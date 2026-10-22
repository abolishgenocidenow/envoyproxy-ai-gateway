// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package libraryconsumer

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/envoyproxy/ai-gateway/mcp"
)

type transportFunc func(mcp.BackendRequest) (*http.Response, error)

func (f transportFunc) RoundTrip(r mcp.BackendRequest) (*http.Response, error) { return f(r) }

func newProxy(t *testing.T, transport mcp.Transport, selection mcp.Selection) *mcp.Proxy {
	t.Helper()
	crypto, err := mcp.NewSessionCrypto("fixture", 100)
	noError(t, err)
	proxy, err := mcp.New(mcp.Options{Transport: transport, SessionCrypto: crypto, Select: func(*http.Request) (mcp.Selection, error) { return selection, nil }})
	noError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		noError(t, proxy.Close(ctx))
	})
	return proxy
}

func TestMCPPrepareAndConcurrentReload(t *testing.T) {
	proxy := newProxy(t, transportFunc(func(mcp.BackendRequest) (*http.Response, error) {
		return nil, errors.New("unexpected outbound request")
	}), mcp.Selection{Route: "route"})
	routes := []mcp.Route{{Name: "route", Backends: []mcp.Backend{{Name: "backend"}}}}
	noError(t, proxy.Configure(t.Context(), routes))
	// Mutating the supplied graph after Configure must not mutate serving state.
	routes[0].Name = "caller-mutated"
	request := func() int {
		r := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader("invalid-json"))
		w := httptest.NewRecorder()
		proxy.ServeHTTP(w, r)
		return w.Code
	}
	check(t, request() == 400, "caller mutated the active config")
	invalidCEL := "invalid syntax ("
	err := proxy.Configure(t.Context(), []mcp.Route{{Name: "route", Authorization: &mcp.Authorization{DefaultAction: "Deny", Rules: []mcp.Rule{{Action: "Allow", CEL: &invalidCEL}}}}})
	check(t, err != nil, "invalid candidate accepted")
	check(t, request() == 400, "rejected candidate displaced serving route")
	var wg sync.WaitGroup
	failures := make(chan string, 8)
	for range 4 {
		wg.Go(func() {
			for range 30 {
				if request() != 400 {
					failures <- "request lost serving route"
					return
				}
			}
		})
	}
	for range 2 {
		wg.Go(func() {
			for range 20 {
				if err := proxy.Configure(t.Context(), []mcp.Route{{Name: "route", Backends: []mcp.Backend{{Name: "backend"}}}}); err != nil {
					failures <- err.Error()
					return
				}
			}
		})
	}
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	noError(t, proxy.Configure(t.Context(), nil))
	check(t, request() == 404, "withdrawal still admitted route")
}

func TestMCPCloseCancelsActiveTransport(t *testing.T) {
	entered := make(chan struct{})
	proxy := newProxy(t, transportFunc(func(r mcp.BackendRequest) (*http.Response, error) {
		close(entered)
		<-r.Request.Context().Done()
		return nil, r.Request.Context().Err()
	}), mcp.Selection{Route: "route"})
	noError(t, proxy.Configure(t.Context(), []mcp.Route{{Name: "route", Backends: []mcp.Backend{{Name: "backend"}}}}))
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		r := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}}`))
		proxy.ServeHTTP(httptest.NewRecorder(), r)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("transport never started")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	noError(t, proxy.Close(ctx))
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("handler survived close")
	}
	w := httptest.NewRecorder()
	proxy.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/mcp", nil))
	check(t, w.Code == 503, "closed proxy accepted request")
	check(t, proxy.Configure(t.Context(), nil) != nil, "closed proxy accepted configuration")
}

func TestMCPRequiresVerifiedContext(t *testing.T) {
	proxy := newProxy(t, transportFunc(func(mcp.BackendRequest) (*http.Response, error) {
		return nil, errors.New("unexpected outbound request")
	}), mcp.Selection{Route: "route", Subject: "claimed"})
	noError(t, proxy.Configure(t.Context(), []mcp.Route{{Name: "route", Authorization: &mcp.Authorization{DefaultAction: "Allow", VerifiedJWT: true}}}))
	r := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader("invalid-json"))
	r.Header.Set("x-ai-eg-mcp-subject", "forged")
	w := httptest.NewRecorder()
	proxy.ServeHTTP(w, r)
	check(t, w.Code == 401, "unverified context satisfied verified policy")
}

func TestMCPInvalidConfigurationRejected(t *testing.T) {
	proxy := newProxy(t, transportFunc(func(mcp.BackendRequest) (*http.Response, error) { return nil, errors.New("unexpected") }), mcp.Selection{Route: "route"})
	for _, route := range []mcp.Route{
		{Name: "route", PrefixMode: "Never"},
		{Name: "route", Authorization: &mcp.Authorization{DefaultAction: "typo"}},
		{Name: "route", Backends: []mcp.Backend{{Name: "backend", ForwardHeaders: []mcp.HeaderForward{{Name: "authorization", BackendHeader: "x-ai-eg-mcp-route"}}}}},
	} {
		check(t, proxy.Configure(t.Context(), []mcp.Route{route}) != nil, "unsafe configuration accepted")
	}
}

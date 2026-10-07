// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package mainlib_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/envoyproxy/ai-gateway/cmd/extproc/mainlib"
)

// httpWatcher illustrates a downstream transport, with no internal imports.
// The caller owns client TLS/authentication, redirects and credential rotation.
// LLM and MCP subscriptions can share the client but keep independent checkpoints.
// Run cancels its requests; it does not close the application-owned client.
type httpWatcher struct {
	client   *http.Client
	endpoint string
	maxBytes int64
	interval time.Duration
}

func (w *httpWatcher) fetch(ctx context.Context, etag string) ([]byte, string, bool, error) {
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, w.endpoint, nil)
	if err != nil {
		return nil, "", false, err
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, "", false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified && etag != "" {
		return nil, etag, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", false, fmt.Errorf("configuration endpoint status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, w.maxBytes+1))
	if err != nil {
		return nil, "", false, err
	}
	if int64(len(body)) > w.maxBytes {
		return nil, "", false, errors.New("configuration too large")
	}
	return body, resp.Header.Get("ETag"), true, nil
}

func (w *httpWatcher) Run(ctx context.Context, apply mainlib.ApplyConfig) error {
	var etag string
	var hash [32]byte
	var applied bool
	for {
		body, nextETag, changed, err := w.fetch(ctx, etag)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil && changed {
			nextHash := sha256.Sum256(body)
			if !applied || nextHash != hash {
				err = apply(body)
			}
			if err == nil {
				etag, hash, applied = nextETag, nextHash, true
			}
		}
		// Production pollers should add jitter, capped backoff and safe diagnostics.
		// Do not log rejected documents, response bodies or unredacted apply errors.
		timer := time.NewTimer(w.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func ExampleMainWithOptions() {
	// A downstream binary parses its own URL/authentication flags first, then
	// passes only extproc flags to mainlib. A custom watcher needs no bundle path.
	// The application supplies a configured client: HTTPS/mTLS, authentication,
	// proxy, redirect policy and connection timeouts stay outside mainlib. See the
	// README for mTLS setup. The factory captures this shared client; each call
	// creates only the state for one scoped subscription.
	run := func(ctx context.Context, args []string, client *http.Client) error {
		opts := mainlib.Options{
			MaxConfigBytes: 128 << 20,
			ConfigWatcherFactory: func(o mainlib.WatchOptions) (mainlib.ConfigWatcher, error) {
				endpoint, _ := url.Parse("https://config.example/extproc-config")
				query := endpoint.Query()
				query.Set("payloadVersion", o.PayloadVersion)
				switch o.Target {
				case mainlib.ConfigTargetLLM, mainlib.ConfigTargetMCP:
					query.Set("scope", string(o.Target))
				default:
					return nil, fmt.Errorf("unsupported target %q", o.Target)
				}
				endpoint.RawQuery = query.Encode()
				return &httpWatcher{client: client, endpoint: endpoint.String(), maxBytes: o.MaxConfigBytes, interval: 5 * time.Second}, nil
			},
		}
		return mainlib.MainWithOptions(ctx, args, io.Discard, opts)
	}
	// Call run from the application with its signal-aware context and client.
	// The application closes shared transport resources after run returns.
	_ = run
}

func TestHTTPWatcherCheckpoint(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var requests atomic.Int32
	var applied atomic.Int32
	confirmed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		validator := r.Header.Get("If-None-Match")
		if n <= 2 {
			if validator != "" {
				t.Error("checkpoint advanced before successful application")
			}
			w.Header().Set("ETag", `"generation-1"`)
			_, _ = io.WriteString(w, "config")
		} else {
			if validator != `"generation-1"` {
				t.Errorf("unexpected validator %q", validator)
			}
			w.WriteHeader(http.StatusNotModified)
			if n == 3 {
				close(confirmed)
			}
		}
	}))
	defer server.Close()
	watcher := httpWatcher{client: server.Client(), endpoint: server.URL, maxBytes: 1024, interval: time.Millisecond}
	done := make(chan error, 1)
	go func() {
		done <- watcher.Run(ctx, func([]byte) error {
			if applied.Add(1) == 1 {
				return errors.New("retry application")
			}
			return nil
		})
	}()
	select {
	case <-confirmed:
	case <-time.After(5 * time.Second):
		t.Fatal("poller did not confirm accepted ETag")
	}
	cancel()
	require.NoError(t, <-done)
	require.EqualValues(t, 2, applied.Load(), "304 must not reapply")
}

func TestHTTPWatcherBoundsAndStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"oversized", http.StatusOK, "12345"},
		{"not found", http.StatusNotFound, ""},
		{"unconditional 304", http.StatusNotModified, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			watcher := httpWatcher{client: server.Client(), endpoint: server.URL, maxBytes: 4}
			_, _, _, err := watcher.fetch(t.Context(), "")
			require.Error(t, err)
		})
	}
}

type startupWriter struct {
	ready chan struct{}
	once  sync.Once
}

func (w *startupWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("AI Gateway External Processor is ready")) {
		w.once.Do(func() { close(w.ready) })
	}
	return len(p), nil
}

func TestMainWithOptionsHTTP(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var unavailable atomic.Bool
	unavailable.Store(true)
	fetched := make(chan struct{})
	var first sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first.Do(func() { close(fetched) })
		if unavailable.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		scope := r.URL.Query().Get("scope")
		if scope != "llm" && scope != "mcp" {
			t.Errorf("unexpected scope %q", scope)
		}
		_, _ = fmt.Fprintf(w, "version: %s\nuuid: %s\n", r.URL.Query().Get("version"), scope)
		if scope == "mcp" {
			_, _ = io.WriteString(w, "mcpConfig: {}\n")
		}
	}))
	defer server.Close()
	output := &startupWriter{ready: make(chan struct{})}
	var targets []mainlib.ConfigTarget
	done := make(chan error, 1)
	go func() {
		done <- mainlib.MainWithOptions(ctx, []string{"--extProcAddr", ":0", "--adminPort", "0", "--mcpAddr", "127.0.0.1:0"}, output, mainlib.Options{
			MaxConfigBytes: 1024,
			ConfigWatcherFactory: func(o mainlib.WatchOptions) (mainlib.ConfigWatcher, error) {
				targets = append(targets, o.Target)
				return &httpWatcher{client: server.Client(), endpoint: server.URL + "?scope=" + string(o.Target) + "&version=" + url.QueryEscape(o.PayloadVersion), maxBytes: o.MaxConfigBytes, interval: time.Millisecond}, nil
			},
		})
	}()
	select {
	case <-fetched:
	case err := <-done:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("initial query not made")
	}
	select {
	case <-output.ready:
		t.Fatal("served before initial configuration")
	default:
	}
	unavailable.Store(false)
	select {
	case <-output.ready:
	case err := <-done:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("initial load did not recover")
	}
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("extproc did not stop")
	}
	require.Equal(t, []mainlib.ConfigTarget{mainlib.ConfigTargetLLM, mainlib.ConfigTargetMCP}, targets)
}

type exampleWatcherFunc func(context.Context, mainlib.ApplyConfig) error

func (f exampleWatcherFunc) Run(ctx context.Context, apply mainlib.ApplyConfig) error {
	return f(ctx, apply)
}

func TestMainWithOptionsWatcherFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{{"unexpected return", nil}, {"canceled while live", context.Canceled}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fail := make(chan struct{})
			output := &startupWriter{ready: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				done <- mainlib.MainWithOptions(ctx, []string{"--extProcAddr", ":0", "--adminPort", "0"}, output, mainlib.Options{
					MaxConfigBytes: 1024,
					ConfigWatcherFactory: func(o mainlib.WatchOptions) (mainlib.ConfigWatcher, error) {
						return exampleWatcherFunc(func(ctx context.Context, apply mainlib.ApplyConfig) error {
							if err := apply([]byte(fmt.Sprintf("version: %s\n", o.PayloadVersion))); err != nil {
								return err
							}
							select {
							case <-ctx.Done():
								return nil
							case <-fail:
								return tc.err
							}
						}), nil
					},
				})
			}()
			select {
			case <-output.ready:
			case err := <-done:
				t.Fatal(err)
			case <-time.After(5 * time.Second):
				t.Fatal("extproc did not initialize")
			}
			close(fail)
			select {
			case err := <-done:
				require.ErrorContains(t, err, "llm configuration watcher stopped")
				if tc.err != nil {
					require.ErrorIs(t, err, tc.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("watcher failure did not stop extproc")
			}
		})
	}
}

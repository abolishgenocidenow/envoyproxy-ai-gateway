// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package mcpproxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/envoyproxy/ai-gateway/execution"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
)

type BackendRequest struct {
	Metadata       execution.Metadata
	Route, Backend string
	Request        *http.Request
}

type BackendTransport interface {
	RoundTrip(BackendRequest) (*http.Response, error)
}

// NewBackendTransport adapts logical destinations to a host-owned transport.
// The host context cancels requests and session cleanup when its owner stops.
func NewBackendTransport(ctx context.Context, destination BackendTransport) http.RoundTripper {
	return backendTransport{destination: destination, stop: ctx}
}

type backendTransport struct {
	destination BackendTransport
	stop        context.Context
}

func (t backendTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	route, backend := r.Header.Get(internalapi.MCPRouteHeader), r.Header.Get(internalapi.MCPBackendHeader)
	if route == "" || backend == "" {
		return nil, errors.New("missing backend selection")
	}
	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(t.stop, cancel)
	request := r.Clone(ctx)
	for key := range request.Header {
		if strings.HasPrefix(strings.ToLower(key), internalapi.EnvoyAIGatewayHeaderPrefix) {
			request.Header.Del(key)
		}
	}
	request.URL.Scheme = ""
	request.URL.Host = ""
	request.Host = ""
	metadata := execution.FromContext(ctx)
	metadata.Route, metadata.Backend = route, backend
	request = request.WithContext(execution.WithMetadata(ctx, metadata))
	response, err := t.destination.RoundTrip(BackendRequest{Metadata: metadata, Route: route, Backend: backend, Request: request})
	if err != nil || response == nil || response.Body == nil {
		stop()
		cancel()
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		if ctx.Err() != nil && err != nil {
			return nil, errors.New("backend request canceled or failed")
		}
		return nil, errors.New("backend transport failed")
	}
	response.Body = &responseBody{ReadCloser: response.Body, cancel: cancel, stop: stop}
	return response, nil
}

type responseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
	stop   func() bool
}

func (b *responseBody) Close() error { b.stop(); b.cancel(); return b.ReadCloser.Close() }

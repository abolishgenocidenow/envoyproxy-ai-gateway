// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

// Package mcp embeds the Agent Router MCP execution engine without an Envoy
// listener. Hosts supply authenticated selection and a backend transport.
package mcp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/metric/noop"

	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	"github.com/envoyproxy/ai-gateway/internal/mcpproxy"
	"github.com/envoyproxy/ai-gateway/internal/metrics"
	"github.com/envoyproxy/ai-gateway/internal/tracing/tracingapi"
)

// Configuration values reuse the execution engine's existing schema. They are
// not Dataplane API resources and contain no listener address or secret material.
type (
	Route          = filterapi.MCPRoute
	Backend        = filterapi.MCPBackend
	ToolSelector   = filterapi.MCPToolSelector
	PromptSelector = filterapi.MCPPromptSelector
	HeaderForward  = filterapi.MCPHeaderForward
	Authorization  = filterapi.MCPRouteAuthorization
	Rule           = filterapi.MCPRouteAuthorizationRule
	Source         = filterapi.MCPAuthorizationSource
	JWTSource      = filterapi.JWTSource
	JWTClaim       = filterapi.JWTClaim
	Target         = filterapi.MCPAuthorizationTarget
	ToolCall       = filterapi.ToolCall
	Selection      = mcpproxy.Selection
	SessionCrypto  = mcpproxy.SessionCrypto
)

// BackendRequest carries a trusted logical destination separately from HTTP.
// Request has no routable URL; the transport must resolve Route/Backend, honor
// cancellation, and return a response whose body the engine will close.
type BackendRequest = mcpproxy.BackendRequest

// Transport sends requests to logical destinations selected by the host.
type Transport = mcpproxy.BackendTransport

// Options has no default transport or selector: accidentally falling back to an
// Envoy listener or trusting client routing headers is not allowed.
type Options struct {
	Select             func(*http.Request) (Selection, error)
	Transport          Transport
	SessionCrypto      SessionCrypto
	MaxRequestBodySize int64
}

// Proxy owns request cancellation and compiled configuration, not network listeners.
type Proxy struct {
	engine  *mcpproxy.ProxyConfig
	handler http.Handler
	mu      sync.Mutex
	closed  bool
	active  sync.WaitGroup
	stop    context.Context
	cancel  context.CancelFunc
	done    chan struct{}
}

// New creates an empty proxy. Configure must install at least one route to serve.
func New(options Options) (*Proxy, error) {
	if options.Select == nil || options.Transport == nil || options.SessionCrypto == nil {
		return nil, errors.New("selector, transport and session crypto are required")
	}
	if options.MaxRequestBodySize < 0 {
		return nil, errors.New("negative body limit")
	}
	if options.MaxRequestBodySize == 0 {
		options.MaxRequestBodySize = 4 * 1024 * 1024
	}
	stop, cancel := context.WithCancel(context.Background())
	engine, handler, err := mcpproxy.NewHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.NewMCP(noop.NewMeterProvider().Meter("mcp"), nil), tracingapi.NoopMCPTracer{}, options.SessionCrypto, nil, mcpproxy.HandlerOptions{
		Select: options.Select, MaxRequestBodySize: options.MaxRequestBodySize,
		Client: http.Client{Transport: mcpproxy.NewBackendTransport(stop, options.Transport), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	})
	if err != nil {
		cancel()
		return nil, err
	}
	return &Proxy{engine: engine, handler: handler, stop: stop, cancel: cancel, done: make(chan struct{})}, nil
}

// NewSessionCrypto derives session encryption keys. Keep the seed stable across
// restarts when sessions should survive; store it outside exported configuration.
func NewSessionCrypto(seed string, iterations int) (SessionCrypto, error) {
	if seed == "" || iterations < 1 {
		return nil, errors.New("nonempty session seed and positive iterations required")
	}
	return mcpproxy.NewPBKDF2AesGcmSessionCrypto(seed, iterations), nil
}

// Configure validates/prepares a whole replacement before publishing it. Active
// HTTP operations retain their captured configuration; later operations use the new
// policy, including operations on previously established sessions.
func (p *Proxy) Configure(ctx context.Context, routes []Route) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, route := range routes {
		if route.Name == "" || strings.Contains(route.Name, "@") || seen[route.Name] {
			return errors.New("invalid or duplicate route name")
		}
		seen[route.Name] = true
		if route.PrefixMode != "" && route.PrefixMode != filterapi.PrefixModeAlways {
			return errors.New("library requires Always prefix mode")
		}
		for _, policy := range []*Authorization{route.Authorization, route.BackendSelector} {
			if policy == nil {
				continue
			}
			if policy.DefaultAction != "Allow" && policy.DefaultAction != "Deny" {
				return errors.New("invalid authorization default action")
			}
			for _, rule := range policy.Rules {
				if rule.Action != "Allow" && rule.Action != "Deny" {
					return errors.New("invalid authorization rule action")
				}
				if rule.Source != nil {
					for _, claim := range rule.Source.JWT.Claims {
						if claim.ValueType != "String" && claim.ValueType != "StringArray" {
							return errors.New("invalid JWT claim value type")
						}
					}
				}
			}
		}
		backends := map[string]bool{}
		for _, backend := range route.Backends {
			if backend.Name == "" || strings.ContainsAny(backend.Name, "@,:") || backends[backend.Name] {
				return errors.New("invalid or duplicate backend name")
			}
			backends[backend.Name] = true
			if backend.PrefixMode != "" && backend.PrefixMode != filterapi.PrefixModeAlways {
				return errors.New("library requires Always prefix mode")
			}
			for _, header := range backend.ForwardHeaders {
				if reserved(header.Name) || reserved(header.ForwardName()) {
					return errors.New("reserved forwarding header")
				}
			}
		}
		for _, header := range route.ForwardHeaders {
			if reserved(header) {
				return errors.New("reserved forwarding header")
			}
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("proxy closed")
	}
	return p.engine.LoadConfig(ctx, &filterapi.Config{MCPConfig: &filterapi.MCPConfig{BackendListenerAddr: "http://execution.invalid", Routes: routes}})
}

func reserved(header string) bool {
	return strings.HasPrefix(strings.ToLower(header), internalapi.EnvoyAIGatewayHeaderPrefix)
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		http.Error(w, "proxy closed", http.StatusServiceUnavailable)
		return
	}
	p.active.Add(1)
	p.mu.Unlock()
	defer p.active.Done()
	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(p.stop, cancel)
	defer stop()
	defer cancel()
	p.handler.ServeHTTP(w, r.WithContext(ctx))
}

// Close stops admission, cancels active requests and waits up to ctx's deadline.
// It does not close the caller-owned Transport.
func (p *Proxy) Close(ctx context.Context) error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		p.cancel()
		go func() { p.active.Wait(); close(p.done) }()
	}
	p.mu.Unlock()
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

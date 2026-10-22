// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package extproc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	runtimeapi "github.com/envoyproxy/ai-gateway/api/runtime"
	"github.com/envoyproxy/ai-gateway/internal/endpointspec"
	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/tracing/tracingapi"
)

type hostRouting struct {
	resolve func(context.Context, *extprocv3.ProcessingRequest) (runtimeapi.ExtProcRequest, error)
	handoff func(context.Context, runtimeapi.RoutingHandoff) (*structpb.Struct, error)
}

func (h hostRouting) Resolve(ctx context.Context, r *extprocv3.ProcessingRequest) (runtimeapi.ExtProcRequest, error) {
	return h.resolve(ctx, r)
}

// Handoff implements the public value-copy contract.
//
//nolint:gocritic // The interface deliberately passes an owned value.
func (h hostRouting) Handoff(ctx context.Context, r runtimeapi.RoutingHandoff) (*structpb.Struct, error) {
	return h.handoff(ctx, r)
}

type hostStream struct {
	grpc.ServerStream
	ctx       context.Context
	cancel    context.CancelFunc
	requests  chan *extprocv3.ProcessingRequest
	responses chan *extprocv3.ProcessingResponse
	done      chan error
}

func (s *hostStream) Context() context.Context { return s.ctx }
func (s *hostStream) Recv() (*extprocv3.ProcessingRequest, error) {
	select {
	case r, ok := <-s.requests:
		if !ok {
			return nil, io.EOF
		}
		return r, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *hostStream) Send(r *extprocv3.ProcessingResponse) error {
	select {
	case s.responses <- r:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func startHostStream(t *testing.T, server *Server) *hostStream {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	s := &hostStream{ctx: ctx, cancel: cancel, requests: make(chan *extprocv3.ProcessingRequest, 1), responses: make(chan *extprocv3.ProcessingResponse, 1), done: make(chan error, 1)}
	go func() { s.done <- server.Process(s) }()
	t.Cleanup(cancel)
	return s
}

func (s *hostStream) exchange(t *testing.T, r *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse {
	t.Helper()
	s.requests <- r
	select {
	case response := <-s.responses:
		return response
	case err := <-s.done:
		t.Fatalf("stream failed: %v", err)
	case <-s.ctx.Done():
		t.Fatal(s.ctx.Err())
	}
	return nil
}

func (s *hostStream) finish(t *testing.T) {
	t.Helper()
	close(s.requests)
	select {
	case err := <-s.done:
		require.NoError(t, err)
	case <-s.ctx.Done():
		t.Fatal(s.ctx.Err())
	}
}

func hostHeaders(selection map[string]any) *extprocv3.ProcessingRequest {
	md, _ := structpb.NewStruct(selection)
	return &extprocv3.ProcessingRequest{
		MetadataContext: &corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{"host": md}},
		Attributes:      map[string]*structpb.Struct{"spoofed": {}},
		Request: &extprocv3.ProcessingRequest_RequestHeaders{RequestHeaders: &extprocv3.HttpHeaders{Headers: &corev3.HeaderMap{Headers: []*corev3.HeaderValue{
			{Key: ":path", RawValue: []byte("/spoofed")},
			{Key: "content-type", RawValue: []byte("application/json")},
			{Key: internalReqIDHeader, RawValue: []byte("spoofed-id")},
			{Key: originalPathHeader, RawValue: []byte("/spoofed")},
			{Key: "x-ai-eg-model", RawValue: []byte("spoofed-model")},
		}}}},
	}
}

func newHostTestServer(t *testing.T) *Server {
	t.Helper()
	routing := hostRouting{
		resolve: func(_ context.Context, r *extprocv3.ProcessingRequest) (runtimeapi.ExtProcRequest, error) {
			fields := r.MetadataContext.FilterMetadata["host"].Fields
			// The test's metadata represents trusted host input. A production adapter
			// must authenticate its transport before making the same assertion.
			return runtimeapi.ExtProcRequest{Path: "/v1/chat/completions", Upstream: fields["upstream"].GetBoolValue(), RequestID: fields["id"].GetStringValue(), ConfigID: fields["config"].GetStringValue(), Backend: fields["backend"].GetStringValue(), Route: "route"}, nil
		},
		handoff: func(_ context.Context, h runtimeapi.RoutingHandoff) (*structpb.Struct, error) {
			return structpb.NewStruct(map[string]any{"host": map[string]any{"id": h.RequestID, "config": h.ConfigID, "model": h.Model, "body_ready": h.BodyReady}})
		},
	}
	s, err := NewServerWithOptions(slog.New(slog.NewTextHandler(io.Discard, nil)), false, ServerOptions{Routing: routing})
	require.NoError(t, err)
	require.NoError(t, s.LoadConfig(t.Context(), &filterapi.Config{UUID: "v1", Backends: []filterapi.Backend{
		{Name: "a", Schema: filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}, ModelNameOverride: "served-a"},
		{Name: "b", Schema: filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}, ModelNameOverride: "served-b"},
	}}))
	s.Register("/v1/chat/completions", NewFactory(&mockMetricsFactory{}, tracingapi.NoopChatCompletionTracer{}, endpointspec.ChatCompletionsEndpointSpec{}))
	return s
}

func TestHostRoutingRetainsGenerationAndRetryInput(t *testing.T) {
	s := newHostTestServer(t)
	downstream := startHostStream(t, s)
	first := downstream.exchange(t, hostHeaders(nil))
	fields := first.DynamicMetadata.Fields["host"].GetStructValue().Fields
	id := fields["id"].GetStringValue()
	generation := fields["config"].GetStringValue()
	require.NotEmpty(t, id)
	require.NotEqual(t, "spoofed-id", id)
	body := downstream.exchange(t, &extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_RequestBody{RequestBody: &extprocv3.HttpBody{Body: []byte(`{"model":"public","messages":[{"role":"user","content":"original"}]}`), EndOfStream: true}}})
	require.Equal(t, "public", body.DynamicMetadata.Fields["host"].GetStructValue().Fields["model"].GetStringValue())
	require.False(t, body.GetRequestBody().Response.ClearRouteCache)
	for _, h := range body.GetRequestBody().Response.HeaderMutation.SetHeaders {
		require.False(t, hostRoutingHeader(h.Header.Key))
	}
	require.NoError(t, s.LoadConfig(t.Context(), &filterapi.Config{UUID: "v1", Backends: []filterapi.Backend{{Name: "a", Schema: filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}, ModelNameOverride: "new-model"}}}))
	selection := func(backend, config string) map[string]any {
		return map[string]any{"upstream": true, "id": id, "config": config, "backend": backend}
	}
	reject := func(values map[string]any, code codes.Code) {
		stream := startHostStream(t, s)
		stream.requests <- hostHeaders(values)
		select {
		case err := <-stream.done:
			require.Equal(t, code, status.Code(err))
		case <-stream.ctx.Done():
			t.Fatal(stream.ctx.Err())
		}
	}
	reject(selection("a", "v1/2"), codes.FailedPrecondition)
	reject(selection("unknown", generation), codes.NotFound)
	reject(selection("a", ""), codes.InvalidArgument)
	for _, backend := range []string{"a", "b"} {
		upstream := startHostStream(t, s)
		response := upstream.exchange(t, hostHeaders(selection(backend, generation)))
		wire := string(response.GetRequestHeaders().Response.GetBodyMutation().GetBody())
		require.Contains(t, wire, "served-"+backend)
		require.Contains(t, wire, "original")
		require.NotContains(t, wire, "new-model")
		reject(selection("a", generation), codes.FailedPrecondition)
		upstream.finish(t)
	}
	downstream.finish(t)
	reject(selection("a", generation), codes.NotFound)
	require.Empty(t, s.hostRequests)
}

func TestHostRoutingSanitizesCallbackFailures(t *testing.T) {
	for _, stage := range []string{"resolve", "handoff", "oversize"} {
		t.Run(stage, func(t *testing.T) {
			s := newHostTestServer(t)
			h := s.routing.(hostRouting)
			if stage == "resolve" {
				h.resolve = func(context.Context, *extprocv3.ProcessingRequest) (runtimeapi.ExtProcRequest, error) {
					return runtimeapi.ExtProcRequest{}, errors.New("secret-value")
				}
			} else {
				h.handoff = func(context.Context, runtimeapi.RoutingHandoff) (*structpb.Struct, error) {
					if stage == "oversize" {
						return structpb.NewStruct(map[string]any{"data": strings.Repeat("x", 65536)})
					}
					return nil, errors.New("secret-value")
				}
			}
			s.routing = h
			stream := startHostStream(t, s)
			stream.requests <- hostHeaders(nil)
			select {
			case err := <-stream.done:
				require.Error(t, err)
				require.NotContains(t, err.Error(), "secret-value")
			case <-stream.ctx.Done():
				t.Fatal(stream.ctx.Err())
			}
			require.Empty(t, s.hostRequests)
		})
	}
}

func TestHostRoutingCancellationReleasesRequest(t *testing.T) {
	s := newHostTestServer(t)
	stream := startHostStream(t, s)
	stream.exchange(t, hostHeaders(nil))
	stream.cancel()
	select {
	case <-stream.done:
	case <-time.After(time.Second):
		t.Fatal("canceled stream did not stop")
	}
	require.Empty(t, s.hostRequests)
}

func TestHostRoutingConcurrentPublication(t *testing.T) {
	s := newHostTestServer(t)
	stream := startHostStream(t, s)
	first := stream.exchange(t, hostHeaders(nil))
	id := first.GetDynamicMetadata().GetFields()["host"].GetStructValue().GetFields()["id"].GetStringValue()
	done := make(chan error, 1)
	go func() {
		for range 100 {
			if err := s.LoadConfig(t.Context(), &filterapi.Config{UUID: "reused"}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for range 100 {
		s.hostRequestsMu.Lock()
		captured := s.hostRequests[id]
		s.hostRequestsMu.Unlock()
		captured.mu.Lock()
		require.Equal(t, "v1", captured.config.UUID)
		require.Contains(t, captured.config.Backends, "b")
		captured.mu.Unlock()
	}
	require.NoError(t, <-done)
	stream.finish(t)
}

func TestHostRoutingRejectsUnidentifiedConfiguration(t *testing.T) {
	s := newHostTestServer(t)
	before, generation := s.currentHostConfig()
	require.ErrorContains(t, s.LoadConfig(t.Context(), &filterapi.Config{}), "configuration UUID")
	after, current := s.currentHostConfig()
	require.Same(t, before, after)
	require.Equal(t, generation, current)
}

func TestHostRoutingCallbackCancellation(t *testing.T) {
	s := newHostTestServer(t)
	host := s.routing.(hostRouting)
	entered := make(chan struct{})
	host.handoff = func(ctx context.Context, _ runtimeapi.RoutingHandoff) (*structpb.Struct, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	s.routing = host
	stream := startHostStream(t, s)
	stream.requests <- hostHeaders(nil)
	select {
	case <-entered:
	case <-stream.ctx.Done():
		t.Fatal(stream.ctx.Err())
	}
	stream.cancel()
	select {
	case <-stream.done:
	case <-time.After(time.Second):
		t.Fatal("callback did not cancel")
	}
	require.Empty(t, s.hostRequests)
}

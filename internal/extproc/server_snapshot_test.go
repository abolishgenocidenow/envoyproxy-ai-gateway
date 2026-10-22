// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
package extproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

type snapshotStream struct {
	mockExternalProcessingStream
	requests  chan *extprocv3.ProcessingRequest
	responses chan *extprocv3.ProcessingResponse
}

func (s *snapshotStream) Recv() (*extprocv3.ProcessingRequest, error) {
	select {
	case <-s.ctx.Done():
		return nil, io.EOF
	case request := <-s.requests:
		return request, nil
	}
}
func (s *snapshotStream) Send(response *extprocv3.ProcessingResponse) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.responses <- response:
		return nil
	}
}

type snapshotProcessor struct {
	passThroughProcessor
	backends chan *filterapi.RuntimeBackend
}

func (p snapshotProcessor) SetBackend(_ context.Context, backend *filterapi.RuntimeBackend, _ string, _ Processor) error {
	p.backends <- backend
	return nil
}

func TestServerRequestSnapshotSurvivesReloadAndRetry(t *testing.T) {
	for _, pin := range []bool{false, true} {
		t.Run(fmt.Sprintf("pin=%t", pin), func(t *testing.T) { testRequestSnapshot(t, pin) })
	}
}
func testRequestSnapshot(t *testing.T, pin bool) {
	server, err := NewServerWithOptions(slog.New(slog.NewTextHandler(io.Discard, nil)), false, ServerOptions{PinRequestConfig: pin})
	require.NoError(t, err)
	server.uuidFn = func() string { return "unique" }
	config := func(id string) *filterapi.Config {
		return &filterapi.Config{UUID: id, Backends: []filterapi.Backend{{Name: "stable"}}}
	}
	require.NoError(t, server.LoadConfig(t.Context(), config("initial")))
	retained := server.currentConfig()
	selected := make(chan *filterapi.RuntimeConfig, 2)
	backends := make(chan *filterapi.RuntimeBackend, 2)
	server.Register("/", func(c *filterapi.RuntimeConfig, _ map[string]string, _ *slog.Logger, upstream, _ bool) (Processor, error) {
		if upstream {
			selected <- c
		}
		return snapshotProcessor{backends: backends}, nil
	})
	start := func(request *extprocv3.ProcessingRequest) (context.CancelFunc, chan error) {
		ctx, cancel := context.WithCancel(t.Context())
		stream := &snapshotStream{mockExternalProcessingStream: mockExternalProcessingStream{ctx: ctx}, requests: make(chan *extprocv3.ProcessingRequest, 1), responses: make(chan *extprocv3.ProcessingResponse, 1)}
		stream.requests <- request
		done := make(chan error, 1)
		go func() { done <- server.Process(stream) }()
		select {
		case <-stream.responses:
		case err := <-done:
			cancel()
			t.Fatal(err)
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatal("stream did not respond")
		}
		return cancel, done
	}
	downstream := &extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_RequestHeaders{RequestHeaders: &extprocv3.HttpHeaders{Headers: &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: ":path", RawValue: []byte("/")}, {Key: "x-request-id", RawValue: []byte("request")}}}}}}
	cancelRouter, routerDone := start(downstream)
	defer cancelRouter()
	for _, generation := range []string{"replacement", "another-replacement"} {
		require.NoError(t, server.LoadConfig(t.Context(), config(generation)))
		upstream := &extprocv3.ProcessingRequest{Attributes: map[string]*structpb.Struct{"envoy.filters.http.ext_proc": {Fields: map[string]*structpb.Value{internalapi.XDSUpstreamHostMetadataBackendNamePath: structpb.NewStringValue("stable")}}}, Request: &extprocv3.ProcessingRequest_RequestHeaders{RequestHeaders: &extprocv3.HttpHeaders{Headers: &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: originalPathHeader, RawValue: []byte("/")}, {Key: internalReqIDHeader, RawValue: []byte("request-unique")}}}}}}
		cancelAttempt, attemptDone := start(upstream)
		want := server.currentConfig()
		if pin {
			want = retained
		}
		require.Same(t, want, <-selected, "upstream processor crossed configuration generations")
		require.Same(t, want.Backends["stable"], <-backends, "backend crossed configuration generations")
		cancelAttempt()
		err := <-attemptDone
		require.True(t, err == nil || errors.Is(err, context.Canceled), "%v", err)
	}
	cancelRouter()
	err = <-routerDone
	require.True(t, err == nil || errors.Is(err, context.Canceled), "%v", err)
}

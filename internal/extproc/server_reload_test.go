// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package extproc

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
)

// Exercise publication concurrently with processor construction. A processor
// retains its snapshot after reload; an invalid candidate must not replace it.
func TestServer_ConcurrentReload(t *testing.T) {
	s, err := NewServer(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})), false)
	require.NoError(t, err)
	makeConfig := func(version string) *filterapi.Config {
		return &filterapi.Config{UUID: version, Backends: []filterapi.Backend{{Name: version}, {Name: "stable"}}}
	}
	require.NoError(t, s.LoadConfig(t.Context(), makeConfig("initial")))
	retained := s.currentConfig()
	s.Register("/", func(c *filterapi.RuntimeConfig, _ map[string]string, _ *slog.Logger, _, _ bool) (Processor, error) {
		if c.Backends[c.UUID] == nil {
			return nil, fmt.Errorf("partial configuration: %s", c.UUID)
		}
		return passThroughProcessor{}, nil
	})
	backendRequest := &extprocv3.ProcessingRequest{Attributes: map[string]*structpb.Struct{
		"envoy.filters.http.ext_proc": {Fields: map[string]*structpb.Value{
			internalapi.XDSUpstreamHostMetadataBackendNamePath: structpb.NewStringValue("stable"),
		}},
	}}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			<-start
			for range 1000 {
				if _, err := s.processorForPath(map[string]string{":path": "/"}, false, s.logger); err != nil {
					t.Error(err)
					return
				}
				if err := s.Process(mockExternalProcessingStream{ctx: t.Context(), retErr: io.EOF}); err != nil {
					t.Error(err)
				}
				// No matching router processor: resolve the backend without invoking a
				// mutable processor, then check the expected request correlation failure.
				err := s.setBackend(t.Context(), passThroughProcessor{}, "missing", false, backendRequest)
				if err == nil || !strings.Contains(err.Error(), "no router processor found") {
					t.Errorf("backend lookup: %v", err)
				}
			}
		})
	}
	close(start)
	for i := range 1000 {
		require.NoError(t, s.LoadConfig(t.Context(), makeConfig(fmt.Sprint(i))))
	}
	wg.Wait()
	require.Equal(t, "initial", retained.UUID)
	require.Contains(t, retained.Backends, "initial")
	applied := s.currentConfig()
	invalid := &filterapi.Config{LLMRequestCosts: []filterapi.LLMRequestCost{{}}}
	require.Error(t, s.LoadConfig(t.Context(), invalid))
	require.Same(t, applied, s.currentConfig())
}

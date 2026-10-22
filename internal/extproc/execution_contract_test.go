// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package extproc

import (
	"bytes"
	"io"
	"log/slog"
	"maps"
	"os"
	"strconv"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/envoyproxy/ai-gateway/internal/endpointspec"
	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	"github.com/envoyproxy/ai-gateway/internal/json"
	"github.com/envoyproxy/ai-gateway/internal/tracing/tracingapi"
)

// These fixtures describe wire behavior, not the future shared engine's API.
// Keep the input and expected output when replacing this extproc driver.
func TestExecutionContract_RetryAndStreaming(t *testing.T) {
	var fixture struct {
		Request  json.RawMessage `json:"request"`
		Attempts []struct {
			Prefix  string                  `json:"prefix"`
			Name    string                  `json:"name"`
			Schema  filterapi.APISchemaName `json:"schema"`
			Version string                  `json:"version"`
			Model   string                  `json:"model"`
			Path    string                  `json:"path"`
			Body    json.RawMessage         `json:"body"`
		} `json:"attempts"`
		Response string `json:"response"`
	}
	raw, err := os.ReadFile("testdata/execution/retry.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &fixture))
	for _, chunkSize := range []int{1, 17, len(fixture.Response)} {
		t.Run("chunk_size_"+strconv.Itoa(chunkSize), func(t *testing.T) {
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			config := &filterapi.RuntimeConfig{Backends: map[string]*filterapi.RuntimeBackend{}}
			headers := map[string]string{":path": "/v1/chat/completions", "content-type": "application/json"}
			factory := NewFactory(&mockMetricsFactory{}, tracingapi.NoopChatCompletionTracer{}, endpointspec.ChatCompletionsEndpointSpec{})
			router, err := factory(config, maps.Clone(headers), logger, false, false)
			require.NoError(t, err)
			parsed, err := router.ProcessRequestBody(t.Context(), &extprocv3.HttpBody{Body: bytes.Clone(fixture.Request), EndOfStream: true})
			require.NoError(t, err)
			for _, h := range parsed.GetRequestBody().GetResponse().GetHeaderMutation().GetSetHeaders() {
				headers[h.Header.Key] = string(h.Header.RawValue)
			}
			server := &Server{config: config, routerProcessorsPerReqID: map[string]Processor{"request": router}}
			var last *mockMetrics
			var previous []*mockMetrics
			for _, attempt := range fixture.Attempts {
				backend := &filterapi.RuntimeBackend{Backend: &filterapi.Backend{Name: attempt.Name, Schema: filterapi.VersionedAPISchema{Name: attempt.Schema, Version: attempt.Version, Prefix: attempt.Prefix}, ModelNameOverride: attempt.Model}}
				config.Backends[attempt.Name] = backend
				var upstream Processor
				upstream, err = factory(config, maps.Clone(headers), logger, true, false)
				require.NoError(t, err)
				// Exercise real metadata decoding and request-to-attempt association.
				err = server.setBackend(t.Context(), upstream, "request", false, &extprocv3.ProcessingRequest{Attributes: map[string]*structpb.Struct{
					"envoy.filters.http.ext_proc": {Fields: map[string]*structpb.Value{
						internalapi.XDSUpstreamHostMetadataBackendNamePath: structpb.NewStringValue(attempt.Name),
						internalapi.XDSRouteMetadataRouteNamePath:          structpb.NewStringValue("fixture-route"),
					}},
				}})
				require.NoError(t, err)
				var response *extprocv3.ProcessingResponse
				response, err = upstream.ProcessRequestHeaders(t.Context(), nil)
				require.NoError(t, err)
				require.Nil(t, response.GetImmediateResponse())
				require.Equal(t, attempt.Name, response.GetDynamicMetadata().GetFields()[internalapi.AIGatewayFilterMetadataNamespace].GetStructValue().GetFields()["backend_name"].GetStringValue())
				mutation := response.GetRequestHeaders().GetResponse()
				require.Equal(t, extprocv3.CommonResponse_CONTINUE_AND_REPLACE, mutation.GetStatus())
				require.JSONEq(t, string(attempt.Body), string(mutation.GetBodyMutation().GetBody()), attempt.Name)
				gotHeaders := map[string]string{}
				for _, h := range mutation.GetHeaderMutation().GetSetHeaders() {
					gotHeaders[h.Header.Key] = string(h.Header.RawValue)
				}
				require.Equal(t, attempt.Path, gotHeaders[":path"], attempt.Name)
				require.Contains(t, mutation.GetHeaderMutation().GetRemoveHeaders(), internalapi.UpstreamHostHeader)
				require.Equal(t, strconv.Itoa(len(mutation.GetBodyMutation().GetBody())), gotHeaders["content-length"])
				// Carry the already-mutated headers into the retry, like the Envoy adapter.
				for k, v := range gotHeaders {
					headers[k] = v
				}
				last = upstream.(*chatCompletionProcessorUpstreamFilter).metrics.(*mockMetrics)
				last.RequireSelectedBackend(t, attempt.Name)
				last.RequireRequestNotCompleted(t)
				previous = append(previous, last)
			}
			_, err = router.ProcessResponseHeaders(t.Context(), &corev3.HeaderMap{Headers: []*corev3.HeaderValue{
				{Key: ":status", RawValue: []byte("200")}, {Key: "content-type", RawValue: []byte("text/event-stream")},
			}})
			require.NoError(t, err)
			var downstream bytes.Buffer
			for offset := 0; offset < len(fixture.Response); offset += chunkSize {
				end := min(offset+chunkSize, len(fixture.Response))
				chunk := []byte(fixture.Response[offset:end])
				response, err := router.ProcessResponseBody(t.Context(), &extprocv3.HttpBody{Body: chunk, EndOfStream: end == len(fixture.Response)})
				require.NoError(t, err)
				mutation := response.GetResponseBody().GetResponse().GetBodyMutation()
				if mutation == nil {
					downstream.Write(chunk)
				} else {
					downstream.Write(mutation.GetBody())
				}
				if end < len(fixture.Response) {
					last.RequireRequestNotCompleted(t)
					require.Zero(t, last.inputTokenCount)
					require.Zero(t, last.outputTokenCount)
				}
			}
			require.Equal(t, fixture.Response, downstream.String())
			last.RequireRequestSuccess(t)
			require.Equal(t, 7, last.inputTokenCount)
			require.Equal(t, 3, last.outputTokenCount)
			require.Equal(t, "served-model", last.responseModel)
			require.Equal(t, "public-model", last.originalModel)
			for _, earlier := range previous[:len(previous)-1] {
				earlier.RequireRequestNotCompleted(t)
				require.Zero(t, earlier.inputTokenCount)
			}
		})
	}
}

func TestExecutionContract_MetadataSelection(t *testing.T) {
	for _, tc := range []struct {
		name, host, cluster, route, want, wantRoute string
		picker, fail                                bool
	}{
		{name: "host wins", host: "host", cluster: "cluster", route: "route", want: "host", wantRoute: "route"},
		{name: "picker uses cluster", host: "host", cluster: "cluster", picker: true, want: "cluster"},
		{name: "cluster fallback", cluster: "cluster", want: "cluster"},
		{name: "picker cannot use host", host: "host", picker: true, fail: true},
		{name: "missing metadata", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attributes := &structpb.Struct{Fields: map[string]*structpb.Value{}}
			for key, value := range map[string]string{internalapi.XDSUpstreamHostMetadataBackendNamePath: tc.host, internalapi.XDSClusterMetadataBackendNamePath: tc.cluster, internalapi.XDSRouteMetadataRouteNamePath: tc.route} {
				if value != "" {
					attributes.Fields[key] = structpb.NewStringValue(value)
				}
			}
			got, err := resolveBackendName(tc.picker, attributes)
			if tc.fail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
			require.Equal(t, tc.wantRoute, resolveRouteName(attributes))
			headers := map[string]string{internalapi.UpstreamHostHeader: "client-spoof"}
			setUpstreamHostAttributes(headers, attributes)
			require.NotContains(t, headers, internalapi.UpstreamHostHeader)
			attributes.Fields[internalapi.XDSUpstreamHostMetadataUpstreamHostPath] = structpb.NewStringValue("trusted.example")
			setUpstreamHostAttributes(headers, attributes)
			require.Equal(t, "trusted.example", headers[internalapi.UpstreamHostHeader])
		})
	}
}

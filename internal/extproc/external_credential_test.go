// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package extproc

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/require"

	runtimeapi "github.com/envoyproxy/ai-gateway/api/runtime"
	"github.com/envoyproxy/ai-gateway/internal/apischema/openai"
	"github.com/envoyproxy/ai-gateway/internal/backendauth"
	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/json"
)

func TestExternalCredentialBoundary(t *testing.T) {
	previous := LogRequestHeaderAttributes
	LogRequestHeaderAttributes = map[string]string{"x-provider-token": "provider_token"}
	t.Cleanup(func() { LogRequestHeaderAttributes = previous })
	for _, replacement := range [][]byte{nil, []byte("translated payload")} {
		name := "passthrough"
		if replacement != nil {
			name = "replacement"
		}
		t.Run(name, func(t *testing.T) {
			var output bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
			raw := bodyFromModel(t, "some-model", false, nil)
			var original openai.ChatCompletionRequest
			require.NoError(t, json.Unmarshal(raw, &original))
			var seenBody []byte
			handler, err := backendauth.NewHandlerWithResolver(t.Context(), &filterapi.BackendAuth{ExternalCredential: "authorized-ref"},
				func(_ context.Context, a runtimeapi.CredentialAttempt) ([]runtimeapi.CredentialHeader, error) {
					seenBody = a.Body
					return []runtimeapi.CredentialHeader{{Name: "x-provider-token", Value: "private-provider-credential"}}, nil
				})
			require.NoError(t, err)
			p := &chatCompletionProcessorUpstreamFilter{
				parent: &chatCompletionProcessorRouterFilter{
					config: &filterapi.RuntimeConfig{}, originalRequestBodyRaw: raw,
					originalRequestBody: &original, originalModel: "some-model", logger: logger,
				},
				requestHeaders:     map[string]string{":path": "/v1/chat/completions"},
				metrics:            &mockMetrics{},
				translator:         &mockTranslator{t: t, expRequestBody: &original, retBodyMutation: replacement},
				handler:            handler,
				externalCredential: true,
				logger:             logger,
			}
			s, err := NewServer(logger, false)
			require.NoError(t, err)
			ctx := context.WithValue(t.Context(), loggerContextKey, logger)
			resp, err := s.processMsg(ctx, p, &extprocv3.ProcessingRequest{
				Request: &extprocv3.ProcessingRequest_RequestHeaders{RequestHeaders: &extprocv3.HttpHeaders{
					Headers: &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: ":path", Value: "/v1/chat/completions"}}},
				}},
			}, "review-request", true)
			require.NoError(t, err)
			if replacement == nil {
				require.Equal(t, extprocv3.CommonResponse_CONTINUE, resp.GetRequestHeaders().GetResponse().Status)
			} else {
				require.Equal(t, extprocv3.CommonResponse_CONTINUE_AND_REPLACE, resp.GetRequestHeaders().GetResponse().Status)
			}
			require.Equal(t, replacement, seenBody, "passthrough has no known replacement body")
			require.NotContains(t, output.String(), "private-provider-credential")
			require.NotContains(t, resp.GetDynamicMetadata().String(), "private-provider-credential")
			require.NotContains(t, p.requestHeaders, "x-provider-token")
			require.Equal(t, []byte("private-provider-credential"), resp.GetRequestHeaders().GetResponse().GetHeaderMutation().GetSetHeaders()[0].Header.RawValue)
			// A later upstream invocation may carry a credential injected earlier.
			// The retained header identity must protect that input log as well.
			_, err = s.processMsg(ctx, p, &extprocv3.ProcessingRequest{
				Request: &extprocv3.ProcessingRequest_RequestHeaders{RequestHeaders: &extprocv3.HttpHeaders{
					Headers: &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: "X-Provider-Token", RawValue: []byte("private-provider-credential")}}},
				}},
			}, "review-request", true)
			require.NoError(t, err)
			require.NotContains(t, output.String(), "private-provider-credential")
		})
	}
}

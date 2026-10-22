// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0

package backendauth

import (
	"bytes"
	"context"
	"golang.org/x/net/http/httpguts"
	"maps"

	runtimeapi "github.com/envoyproxy/ai-gateway/api/runtime"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"google.golang.org/protobuf/proto"
)

type externalCredentialHandler struct {
	reference string
	resolve   runtimeapi.CredentialResolver
}

func (h *externalCredentialHandler) Do(ctx context.Context, headers map[string]string, body []byte) ([]internalapi.Header, error) {
	var metadata *corev3.Metadata
	if md := envoyMetadataFromContext(ctx); md != nil {
		metadata = proto.Clone(md).(*corev3.Metadata)
	}
	resolved, err := h.resolve(ctx, runtimeapi.CredentialAttempt{Reference: h.reference, Headers: maps.Clone(headers), Body: bytes.Clone(body), Metadata: metadata})
	if err != nil || len(resolved) == 0 || len(resolved) > 32 {
		return nil, ErrCredentialMissing
	}
	result := make([]internalapi.Header, 0, len(resolved))
	for _, v := range resolved {
		if !httpguts.ValidHeaderFieldName(v.Name) || !httpguts.ValidHeaderFieldValue(v.Value) || len(v.Name) > 256 || len(v.Value) > 16384 {
			return nil, ErrCredentialMissing
		}
		result = append(result, internalapi.Header{v.Name, v.Value})
	}
	// Validate every mutation before touching the request.
	for _, v := range resolved {
		headers[v.Name] = v.Value
	}
	return result, nil
}

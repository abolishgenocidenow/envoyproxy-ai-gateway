// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

// Package llmrequest contains request preparation shared by execution adapters.
package llmrequest

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"

	"github.com/envoyproxy/ai-gateway/internal/bodymutator"
	"github.com/envoyproxy/ai-gateway/internal/endpointspec"
	"github.com/envoyproxy/ai-gateway/internal/headermutator"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	"github.com/envoyproxy/ai-gateway/internal/translator"
)

// Capture parses an owned copy so no translator retains an adapter's borrowed buffer.
func Capture[Req, Resp, Chunk any](spec endpointspec.Spec[Req, Resp, Chunk], raw []byte, contentType string, costs bool) (string, *Req, bool, []byte, bool, error) {
	raw = bytes.Clone(raw)
	var model string
	var body *Req
	var stream bool
	var changed []byte
	var err error
	if strings.HasPrefix(strings.ToLower(contentType), "multipart/form-data") {
		model, body, stream, changed, err = spec.ParseMultipartBody(raw, contentType, costs)
	} else {
		model, body, stream, changed, err = spec.ParseBody(raw, costs)
	}
	if err != nil {
		return "", nil, false, nil, false, err
	}
	force := changed != nil
	if force {
		raw = changed
	}
	return strings.Clone(model), body, stream, raw, force, nil
}

// Mutation preserves the distinction between no replacement and an empty body.
type Mutation struct {
	Headers       []internalapi.Header
	RemoveHeaders []string
	Body          []byte
	ReplaceBody   bool
}

// Prepare translates the original request and applies per-attempt policies.
// Transport authentication and transport-specific metadata remain adapter concerns.
func Prepare[Req, Span any](tr translator.Translator[Req, Span], original []byte, parsed *Req, headers map[string]string, retry, force bool, hm *headermutator.HeaderMutator, bm *bodymutator.BodyMutator, logger *slog.Logger, strict bool) (Mutation, error) {
	sets, body, err := tr.RequestBody(bytes.Clone(original), parsed, retry || force)
	if err != nil {
		return Mutation{}, err
	}
	result := Mutation{Headers: sets, Body: body, ReplaceBody: body != nil || retry || force || (bm != nil && bm.HasMutations())}
	if hm != nil {
		changed, removed := hm.Mutate(headers, retry)
		result.Headers = append(result.Headers, changed...)
		result.RemoveHeaders = removed
	}
	if result.ReplaceBody && bm != nil {
		source := body
		if source == nil {
			source = original
		}
		if body == nil || len(body) > 0 {
			changed, mutationErr := bm.Mutate(bytes.Clone(source))
			if mutationErr != nil {
				if strict {
					return Mutation{}, errors.New("body policy preparation failed")
				}
				logger.Error("failed to apply body mutation", "error", mutationErr)
			} else {
				result.Body = changed
			}
		}
	}
	for _, h := range result.Headers {
		headers[h.Key()] = h.Value()
	}
	return result, nil
}

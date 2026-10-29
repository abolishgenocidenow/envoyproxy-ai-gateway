// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

// Package llm prepares provider requests and translates decoded responses without
// starting servers or selecting upstreams. A caller supplies each selected backend.
package llm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"strconv"
	"strings"

	"github.com/envoyproxy/ai-gateway/execution"
	"github.com/envoyproxy/ai-gateway/internal/bodymutator"
	"github.com/envoyproxy/ai-gateway/internal/endpointspec"
	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/headermutator"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	"github.com/envoyproxy/ai-gateway/internal/json"
	"github.com/envoyproxy/ai-gateway/internal/llmrequest"
	"github.com/envoyproxy/ai-gateway/internal/tracing/tracingapi"
	"github.com/envoyproxy/ai-gateway/internal/translator"
	"github.com/envoyproxy/ai-gateway/sse"
	"github.com/envoyproxy/ai-gateway/usage"
)

// These execution values reuse existing definitions; they are not API resources.
type (
	Header       = internalapi.Header
	Schema       = filterapi.VersionedAPISchema
	SchemaName   = filterapi.APISchemaName
	HeaderPolicy = filterapi.HTTPHeaderMutation
	HeaderValue  = filterapi.HTTPHeader
	BodyPolicy   = filterapi.HTTPBodyMutation
	BodyField    = filterapi.HTTPBodyField
	Usage        = usage.TokenUsage
	Mutation     = llmrequest.Mutation
)

// Input uses lower-case header names, including :path when required by a provider.
// NewRequest owns copies of its input. Bodies are decoded, not compressed.
type Input struct {
	Headers      map[string]string
	Body         []byte
	IncludeUsage bool
	// SSELimit bounds a retained SSE event or AWS binary frame. Nonpositive values are unbounded.
	// Hosts that need a cap must set a positive limit explicitly.
	SSELimit int
	// StrictSSE rejects non-JSON data, incomplete events and data after a terminal event.
	StrictSSE bool
}

// Backend is one trusted selection supplied by pick or another host adapter.
// Authenticate resolves/injects credentials at attempt preparation time. It must
// honor ctx and must not mutate the captured input or retain its arguments.
type Backend struct {
	// Metadata is the complete trusted selection for this attempt.
	Metadata     execution.Metadata
	Schema       Schema
	Model        string
	Headers      *HeaderPolicy
	Body         *BodyPolicy
	Authenticate func(ctx context.Context, headers map[string]string, body []byte) ([]Header, error)
}

// Request captures a parsed input. Each retry creates a new Attempt from it.
// Request and Attempt are single-owner objects, not concurrent-use objects.
type Request struct {
	model         string
	streaming     bool
	selectBackend func(context.Context, Backend) (*Attempt, Mutation, error)
}

func (r *Request) Model() string   { return r.model }
func (r *Request) Streaming() bool { return r.streaming }

// NewAttempt prepares a fresh attempt without performing network I/O itself.
//
//nolint:gocritic // The small selection value is intentionally copied at the public boundary.
func (r *Request) NewAttempt(ctx context.Context, backend Backend) (*Attempt, Mutation, error) {
	if err := ctx.Err(); err != nil {
		return nil, Mutation{}, err
	}
	return r.selectBackend(execution.WithMetadata(ctx, backend.Metadata), backend)
}

// NewRequest selects an existing endpoint parser. Unsupported operations fail.
// Models listing is configuration-owned and is not a translation operation.
func NewRequest(operation string, input Input) (*Request, error) {
	switch operation {
	case "/v1/chat/completions":
		return capture(endpointspec.ChatCompletionsEndpointSpec{}, input)
	case "/v1/completions":
		return capture(endpointspec.CompletionsEndpointSpec{}, input)
	case "/v1/embeddings":
		return capture(endpointspec.EmbeddingsEndpointSpec{}, input)
	case "/v1/responses":
		return capture(endpointspec.ResponsesEndpointSpec{}, input)
	case "/v1/decisions":
		return capture(endpointspec.DecisionsEndpointSpec{}, input)
	case "/v1/responses/input_tokens":
		return capture(endpointspec.ResponsesInputTokensEndpointSpec{}, input)
	case "/v1/audio/speech":
		return capture(endpointspec.SpeechEndpointSpec{}, input)
	case "/v1/audio/transcriptions":
		return capture(endpointspec.TranscriptionEndpointSpec{}, input)
	case "/v1/audio/translations":
		return capture(endpointspec.TranslationEndpointSpec{}, input)
	case "/v1/images/generations":
		return capture(endpointspec.ImageGenerationEndpointSpec{}, input)
	case "/v2/rerank":
		return capture(endpointspec.RerankEndpointSpec{}, input)
	case "/v1/systemone":
		return capture(endpointspec.SystemOneEndpointSpec{}, input)
	case "/v1/messages":
		return capture(endpointspec.MessagesEndpointSpec{}, input)
	case "/tokenize":
		return capture(endpointspec.TokenizeEndpointSpec{}, input)
	case "/v1/messages/count_tokens":
		return capture(endpointspec.MessagesCountTokensEndpointSpec{}, input)
	default:
		return nil, fmt.Errorf("unsupported operation %q", operation)
	}
}

func capture[Req, Resp, Chunk any](spec endpointspec.Spec[Req, Resp, Chunk], input Input) (*Request, error) {
	model, parsed, stream, raw, force, err := llmrequest.Capture(spec, input.Body, input.Headers["content-type"], input.IncludeUsage)
	if err != nil {
		return nil, err
	}
	originalHeaders := maps.Clone(input.Headers)
	if originalHeaders == nil {
		originalHeaders = map[string]string{}
	}
	attempts := 0
	request := &Request{model: model, streaming: stream}
	request.selectBackend = func(ctx context.Context, backend Backend) (*Attempt, Mutation, error) {
		tr, err := spec.GetTranslator(backend.Schema, backend.Model)
		if err != nil {
			return nil, Mutation{}, err
		}
		if setter, ok := tr.(interface{ SetStreamLimit(int) }); ok {
			limit := input.SSELimit
			if limit <= 0 {
				limit = -1
			}
			setter.SetStreamLimit(limit)
		}
		headers := maps.Clone(originalHeaders)
		if setter, ok := tr.(translator.ContentTypeSetter); ok {
			setter.SetContentType(headers["content-type"])
		}
		if setter, ok := tr.(translator.RequestHeadersSetter); ok {
			setter.SetRequestHeaders(headers)
		}
		mutation, err := llmrequest.Prepare(tr, raw, parsed, headers, attempts > 0, force, headermutator.NewHeaderMutator(backend.Headers, originalHeaders), bodymutator.NewBodyMutator(backend.Body, raw), slog.New(slog.NewTextHandler(io.Discard, nil)), true)
		if err != nil {
			return nil, Mutation{}, err
		}
		if backend.Body != nil {
			var checked any
			if json.Unmarshal(mutation.Body, &checked) != nil {
				return nil, Mutation{}, errors.New("body policy must produce valid JSON")
			}
		}
		if mutation.ReplaceBody {
			length := strconv.Itoa(len(mutation.Body))
			mutation.Headers = append(mutation.Headers, Header{"content-length", length})
			headers["content-length"] = length
		}
		for _, name := range mutation.RemoveHeaders {
			delete(headers, name)
		}
		for _, h := range mutation.Headers {
			headers[h.Key()] = h.Value()
		}
		if backend.Authenticate != nil {
			authBody := mutation.Body
			if !mutation.ReplaceBody {
				authBody = bytes.Clone(raw)
			}
			authHeaders, authErr := backend.Authenticate(ctx, headers, authBody)
			if authErr != nil {
				if ctx.Err() != nil {
					return nil, Mutation{}, ctx.Err()
				}
				return nil, Mutation{}, fmt.Errorf("upstream authentication failed")
			}
			mutation.Headers = append(mutation.Headers, authHeaders...)
		}
		if err = ctx.Err(); err != nil {
			return nil, Mutation{}, err
		}
		attempts++
		limit := input.SSELimit
		if limit <= 0 {
			limit = -1
		}
		attempt := &Attempt{streaming: stream, reader: sse.NewReader(limit), decoder: usage.Decoder{Schema: string(backend.Schema.Name)}}
		switch any(spec).(type) {
		case endpointspec.ChatCompletionsEndpointSpec, endpointspec.CompletionsEndpointSpec, endpointspec.ResponsesEndpointSpec, endpointspec.MessagesEndpointSpec:
			attempt.requireTerminal = true
			attempt.observeUsage = true
		case endpointspec.EmbeddingsEndpointSpec, endpointspec.ImageGenerationEndpointSpec, endpointspec.DecisionsEndpointSpec:
			attempt.observeUsage = true
		}
		attempt.strictSSE = input.StrictSSE
		attempt.reader.Strict = input.StrictSSE
		attempt.headers = tr.ResponseHeaders
		attempt.body = func(headers map[string]string, body []byte, end bool) ([]Header, []byte, Usage, string, error) {
			return tr.ResponseBody(headers, bytes.NewReader(body), end, tracingapi.Span[Resp, Chunk](nil))
		}
		attempt.responseError = func(headers map[string]string, body []byte) ([]Header, []byte, error) {
			return tr.ResponseError(headers, bytes.NewReader(body))
		}
		return attempt, mutation, nil
	}
	return request, nil
}

// Result is a translation result. A nil Body means retain the caller's input;
// a non-nil empty Body means remove it. Usage is cumulative for this attempt.
// Apply any returned body before handling a terminal provider streaming error.
type Result struct {
	Headers []Header
	Body    []byte
	Usage   Usage
	Model   string
	Done    bool
	// UsageError reports invalid usage without interrupting forwarding.
	// It is sticky for the attempt. Hosts must treat Usage as invalid when it is non-nil.
	UsageError error
}

// Attempt owns provider-specific streaming state and usage for a single selection.
type Attempt struct {
	headers         func(map[string]string) ([]Header, error)
	body            func(map[string]string, []byte, bool) ([]Header, []byte, Usage, string, error)
	responseError   func(map[string]string, []byte) ([]Header, []byte, error)
	responseHeaders map[string]string
	status          int
	done            bool
	usage           Usage
	streaming       bool
	reader          *sse.Reader
	decoder         usage.Decoder
	streamDone      bool
	terminalSeen    bool
	requireTerminal bool
	observeUsage    bool
	strictSSE       bool
	usageError      error
}

// ResponseHeaders starts response translation. The host must decode content
// encoding before supplying bodies; no Envoy or HTTP client is owned here.
func (a *Attempt) ResponseHeaders(status int, headers map[string]string) ([]Header, error) {
	if a.responseHeaders != nil {
		return nil, errors.New("response headers already supplied")
	}
	if status < 100 || status > 599 {
		return nil, errors.New("invalid response status")
	}
	if encoding := headers["content-encoding"]; encoding != "" && encoding != "identity" {
		return nil, errors.New("decode content encoding before response translation")
	}
	a.responseHeaders = maps.Clone(headers)
	if a.responseHeaders == nil {
		a.responseHeaders = map[string]string{}
	}
	a.status = status
	a.responseHeaders[":status"] = strconv.Itoa(status)
	return a.headers(a.responseHeaders)
}

// ResponseBody consumes decoded bytes in order. Non-success bodies must be
// buffered by the caller and supplied with end=true, as in the extproc adapter.
func (a *Attempt) ResponseBody(body []byte, end bool) (Result, error) {
	if a.responseHeaders == nil {
		return Result{}, errors.New("response headers required")
	}
	if a.done {
		return Result{}, errors.New("attempt already completed")
	}
	if a.status < 200 || a.status >= 300 {
		if !end {
			return Result{}, errors.New("error response must be buffered")
		}
		headers, changed, err := a.responseError(a.responseHeaders, body)
		a.done = true
		return Result{Headers: headers, Body: changed, Done: true}, err
	}
	if a.streaming && strings.Contains(a.responseHeaders["content-type"], "text/event-stream") {
		return a.responseSSE(body, end)
	}
	headers, changed, usage, model, err := a.body(a.responseHeaders, body, end)
	if !a.streaming && a.observeUsage && err != nil {
		// A legacy translator may reject malformed usage before translating content.
		// Retry without usage, retaining the original bytes for passthrough responses.
		clean := withoutUsage(body)
		if !bytes.Equal(clean, body) {
			h, b, u, m, retryErr := a.body(a.responseHeaders, clean, end)
			if retryErr == nil {
				headers, changed, usage, model, err = h, b, u, m, nil
			}
		}
	}
	if !a.streaming && a.observeUsage && err == nil {
		observed, observeErr := a.decoder.Observe(body)
		usage.Override(observed)
		if observeErr != nil {
			a.usageError = observeErr
		}
	}
	a.done = end || err != nil
	a.usage.Override(usage)
	return Result{Headers: headers, Body: changed, Usage: a.usage, Model: model, Done: a.done, UsageError: a.usageError}, err
}

// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package libraryconsumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/envoyproxy/ai-gateway/execution"
	"github.com/envoyproxy/ai-gateway/llm"
)

func check(t *testing.T, ok bool, format string, args ...any) {
	t.Helper()
	if !ok {
		t.Fatalf(format, args...)
	}
}

func noError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func equalJSON(t *testing.T, want, got []byte) {
	t.Helper()
	var a, b any
	noError(t, json.Unmarshal(want, &a))
	noError(t, json.Unmarshal(got, &b))
	check(t, reflect.DeepEqual(a, b), "JSON mismatch\nwant: %s\ngot: %s", want, got)
}

func TestExecutionContract_LLM(t *testing.T) {
	var fixture struct {
		Request  json.RawMessage
		Attempts []struct {
			Name, Schema, Version, Prefix, Model, Path string
			Body                                       json.RawMessage
		}
		Response string
	}
	raw, err := os.ReadFile("../../internal/extproc/testdata/execution/retry.json")
	noError(t, err)
	noError(t, json.Unmarshal(raw, &fixture))
	for _, size := range []int{1, 17, len(fixture.Response)} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			input := bytes.Clone(fixture.Request)
			request, err := llm.NewRequest("/v1/chat/completions", llm.Input{Body: input, Headers: map[string]string{"content-type": "application/json"}})
			noError(t, err)
			// The library must own input even if an adapter immediately reuses its buffer.
			clear(input)
			check(t, request.Model() == "public-model" && request.Streaming(), "wrong captured request")
			var attempt *llm.Attempt
			for _, step := range fixture.Attempts {
				var mutation llm.Mutation
				attempt, mutation, err = request.NewAttempt(t.Context(), llm.Backend{Schema: llm.Schema{Name: llm.SchemaName(step.Schema), Version: step.Version, Prefix: step.Prefix}, Model: step.Model})
				noError(t, err)
				check(t, mutation.ReplaceBody, "attempt must replace body")
				equalJSON(t, step.Body, mutation.Body)
				headers := map[string]string{}
				for _, h := range mutation.Headers {
					headers[h.Key()] = h.Value()
				}
				check(t, headers[":path"] == step.Path, "wrong path: %s", headers[":path"])
			}
			_, err = attempt.ResponseHeaders(200, map[string]string{"content-type": "text/event-stream"})
			noError(t, err)
			var got bytes.Buffer
			var final llm.Result
			for offset := 0; offset < len(fixture.Response); offset += size {
				end := min(offset+size, len(fixture.Response))
				part := []byte(fixture.Response[offset:end])
				final, err = attempt.ResponseBody(part, end == len(fixture.Response))
				noError(t, err)
				if final.Body == nil {
					got.Write(part)
				} else {
					got.Write(final.Body)
				}
				check(t, final.Done == (end == len(fixture.Response)), "premature or missing completion")
			}
			check(t, got.String() == fixture.Response, "stream bytes changed")
			in, ok := final.Usage.InputTokens()
			check(t, ok && in == 7, "wrong input usage")
			out, ok := final.Usage.OutputTokens()
			check(t, ok && out == 3, "wrong output usage")
			check(t, final.Model == "served-model", "wrong served model")
			_, err = attempt.ResponseBody(nil, true)
			check(t, err != nil, "duplicate completion accepted")
		})
	}
}

func TestLLMErrorsAndPolicies(t *testing.T) {
	_, err := llm.NewRequest("/unknown", llm.Input{})
	check(t, err != nil, "unknown operation accepted")
	_, err = llm.NewRequest("/v1/chat/completions", llm.Input{Body: []byte("not-json")})
	check(t, err != nil, "bad JSON accepted")
	newRequest := func() *llm.Request {
		r, err := llm.NewRequest("/v1/chat/completions", llm.Input{Body: []byte(`{"model":"test","messages":[]}`), Headers: map[string]string{"authorization": "incoming"}})
		noError(t, err)
		return r
	}
	request := newRequest()
	_, _, err = request.NewAttempt(t.Context(), llm.Backend{Schema: llm.Schema{Name: "unsupported"}})
	check(t, err != nil, "unsupported provider accepted")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = request.NewAttempt(ctx, llm.Backend{})
	check(t, errors.Is(err, context.Canceled), "cancellation ignored")
	backend := llm.Backend{Schema: llm.Schema{Name: "OpenAI", Prefix: "v1"}, Model: "selected", Headers: &llm.HeaderPolicy{Remove: []string{"authorization"}}, Body: &llm.BodyPolicy{Set: []llm.BodyField{{Path: "temperature", Value: "0.5"}}}}
	backend.Authenticate = func(_ context.Context, headers map[string]string, body []byte) ([]llm.Header, error) {
		check(t, headers["authorization"] == "", "authentication saw removed client credential")
		equalJSON(t, []byte(`{"model":"selected","messages":[],"temperature":0.5}`), body)
		return []llm.Header{{"authorization", "Bearer fixture"}}, nil
	}
	attempt, mutation, err := request.NewAttempt(t.Context(), backend)
	noError(t, err)
	check(t, len(mutation.RemoveHeaders) == 1 && mutation.RemoveHeaders[0] == "authorization", "policy removal missing")
	check(t, mutation.Headers[len(mutation.Headers)-1].Value() == "Bearer fixture", "auth missing")
	_, err = attempt.ResponseHeaders(429, map[string]string{"content-type": "application/json"})
	noError(t, err)
	response := []byte(`{"error":{"message":"limited","type":"rate_limit_error"}}`)
	result, err := attempt.ResponseBody(response, true)
	noError(t, err)
	if result.Body == nil {
		result.Body = response
	}
	equalJSON(t, response, result.Body)
	backend.Authenticate = func(context.Context, map[string]string, []byte) ([]llm.Header, error) {
		return nil, errors.New("secret-sentinel")
	}
	_, _, err = newRequest().NewAttempt(t.Context(), backend)
	check(t, err != nil && !strings.Contains(err.Error(), "secret-sentinel"), "auth error leaked")
	attempt, _, err = newRequest().NewAttempt(t.Context(), llm.Backend{Schema: llm.Schema{Name: "OpenAI"}})
	noError(t, err)
	_, err = attempt.ResponseHeaders(200, map[string]string{"content-encoding": "gzip"})
	check(t, err != nil, "encoded body silently accepted")
}

func TestLLMPolicyDoesNotMutateCapturedBody(t *testing.T) {
	original := []byte(`{"model":"public","messages":[],"temperature":0.9}`)
	request, err := llm.NewRequest("/v1/chat/completions", llm.Input{Body: original})
	noError(t, err)
	_, changed, err := request.NewAttempt(t.Context(), llm.Backend{Schema: llm.Schema{Name: "OpenAI"}, Body: &llm.BodyPolicy{Set: []llm.BodyField{{Path: "temperature", Value: "0.1"}}}})
	noError(t, err)
	equalJSON(t, []byte(`{"model":"public","messages":[],"temperature":0.1}`), changed.Body)
	_, replayed, err := request.NewAttempt(t.Context(), llm.Backend{Schema: llm.Schema{Name: "OpenAI"}})
	noError(t, err)
	equalJSON(t, original, replayed.Body)
}

func TestLLMInvalidBodyPolicyRejected(t *testing.T) {
	request, err := llm.NewRequest("/v1/chat/completions", llm.Input{Body: []byte(`{"model":"test","messages":[]}`)})
	noError(t, err)
	_, _, err = request.NewAttempt(t.Context(), llm.Backend{Schema: llm.Schema{Name: "OpenAI"}, Body: &llm.BodyPolicy{Set: []llm.BodyField{{Path: "temperature", Value: "1.2.3"}}}})
	check(t, err != nil, "invalid policy output accepted")
}

func TestUniversalMetadata(t *testing.T) {
	request, err := llm.NewRequest("/v1/chat/completions", llm.Input{Body: []byte(`{"model":"test","messages":[]}`)})
	noError(t, err)
	metadata := execution.Metadata{Route: "route", Backend: "first", Attributes: map[string]string{"tenant": "fixture"}}
	backend := llm.Backend{Schema: llm.Schema{Name: "OpenAI"}, Metadata: metadata}
	backend.Authenticate = func(ctx context.Context, _ map[string]string, _ []byte) ([]llm.Header, error) {
		got := execution.FromContext(ctx)
		check(t, got.Backend == backend.Metadata.Backend && got.Attributes["tenant"] == "fixture", "selection metadata lost")
		got.Attributes["tenant"] = "mutated"
		check(t, execution.FromContext(ctx).Attributes["tenant"] == "fixture", "callback mutated captured metadata")
		return nil, nil
	}
	_, _, err = request.NewAttempt(t.Context(), backend)
	noError(t, err)
	backend.Metadata.Backend = "retry"
	_, _, err = request.NewAttempt(t.Context(), backend)
	noError(t, err)
	check(t, metadata.Attributes["tenant"] == "fixture", "caller metadata mutated")
}

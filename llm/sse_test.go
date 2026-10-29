// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package llm

import (
	"errors"
	"strings"
	"testing"

	"github.com/envoyproxy/ai-gateway/sse"
)

func streamAttempt(t *testing.T, operation, schema string, limit int) *Attempt {
	t.Helper()
	r, err := NewRequest(operation, Input{Body: []byte(`{"model":"test","stream":true,"messages":[{"role":"user","content":"hi"}],"max_tokens":20}`), SSELimit: limit})
	if err != nil {
		t.Fatal(err)
	}
	a, _, err := r.NewAttempt(t.Context(), Backend{Schema: Schema{Name: SchemaName(schema)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.ResponseHeaders(200, map[string]string{"content-type": "text/event-stream"}); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestSSEUsageAndFraming(t *testing.T) {
	for _, newline := range []string{"\n", "\r", "\r\n"} {
		t.Run(strings.ReplaceAll(strings.ReplaceAll(newline, "\r", "CR"), "\n", "LF"), func(t *testing.T) {
			a := streamAttempt(t, "/v1/chat/completions", "OpenAI", 0)
			input := "\xef\xbb\xbf: comment" + newline + "data: {\"usage\": {" + newline + "data: \"prompt_tokens\":9}}" + newline + newline + "data: {\"usage\":{\"prompt_tokens\":0}}" + newline + newline + "data: [DONE]" + newline + newline
			var result Result
			for i := range len(input) {
				var err error
				result, err = a.ResponseBody([]byte(input[i:i+1]), i == len(input)-1)
				if err != nil {
					t.Fatal(err)
				}
				if result.Body != nil {
					t.Fatal("passthrough bytes must remain unchanged")
				}
			}
			if n, ok := result.Usage.InputTokens(); !ok || n != 0 {
				t.Fatalf("input %d %v", n, ok)
			}
			if _, ok := result.Usage.OutputTokens(); ok {
				t.Fatal("missing output became zero")
			}
		})
	}
}

func TestSSETerminalErrors(t *testing.T) {
	for _, kind := range []string{"error", "response.failed", "response.incomplete"} {
		t.Run(kind, func(t *testing.T) {
			a := streamAttempt(t, "/v1/chat/completions", "OpenAI", 0)
			result, err := a.ResponseBody([]byte("event: "+kind+"\ndata: {}\n\n"), false)
			var failure *StreamError
			if !errors.As(err, &failure) || !result.Done {
				t.Fatalf("got %v %+v", err, result)
			}
		})
	}
}

func TestSSELimitsAndDone(t *testing.T) {
	for _, tt := range []struct {
		name, input string
		end         bool
		want        error
	}{
		{"after done", "data: [DONE]\n\ndata: {}\n\n", false, ErrDataAfterDone},
		{"missing terminal", "data: {}\n\n", true, ErrMissingTerminal},
		{"partial eof", "data: {}\n", true, sse.ErrIncomplete},
		{"oversize", "data: " + strings.Repeat("x", 100), false, sse.ErrOverflow},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := streamAttempt(t, "/v1/chat/completions", "OpenAI", 32)
			r, err := a.ResponseBody([]byte(tt.input), tt.end)
			if !errors.Is(err, tt.want) || !r.Done {
				t.Fatalf("got %v done=%v", err, r.Done)
			}
		})
	}
}

func TestSSETranslationDoesNotLeakPartialInput(t *testing.T) {
	a := streamAttempt(t, "/v1/chat/completions", "GCPAnthropic", 0)
	r, err := a.ResponseBody([]byte("event: message_start\ndata: {"), false)
	if err != nil {
		t.Fatal(err)
	}
	if r.Body == nil || len(r.Body) != 0 {
		t.Fatalf("partial translation body: %q", r.Body)
	}
}

func TestSSEProviderMatrix(t *testing.T) {
	anthropicStream := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"test\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	for _, tt := range []struct{ name, operation, schema, stream string }{
		{"anthropic", "/v1/messages", "Anthropic", anthropicStream},
		{"vertex anthropic", "/v1/chat/completions", "GCPAnthropic", anthropicStream},
		{"gemini", "/v1/chat/completions", "GCPVertexAI", "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"hello\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":2,\"totalTokenCount\":3}}\n\n"},
		{"openai to anthropic", "/v1/messages", "OpenAI", "data: {\"id\":\"m\",\"model\":\"test\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hello\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\ndata: [DONE]\n\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := streamAttempt(t, tt.operation, tt.schema, 0)
			input := strings.ReplaceAll(tt.stream, "\n", "\r\n")
			var r Result
			for i := range len(input) {
				var err error
				r, err = a.ResponseBody([]byte(input[i:i+1]), i == len(input)-1)
				if err != nil {
					t.Fatalf("byte %d: %v", i, err)
				}
			}
			if n, ok := r.Usage.InputTokens(); !ok || n != 1 {
				t.Fatalf("input %d %v", n, ok)
			}
			if n, ok := r.Usage.OutputTokens(); !ok || n != 2 {
				t.Fatalf("output %d %v", n, ok)
			}
		})
	}
}

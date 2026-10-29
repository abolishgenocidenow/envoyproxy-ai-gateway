// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package llm

import (
	"bytes"
	"errors"
	"testing"
)

func TestReviewPassthroughError(t *testing.T) {
	a := streamAttempt(t, "/v1/messages", "Anthropic", 0)
	raw := []byte("event: error\r\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}\r\n\r\n")
	result, err := a.ResponseBody(raw, false)
	var failure *StreamError
	if !errors.As(err, &failure) || !bytes.Equal(raw, result.Body) {
		t.Fatalf("body=%q error=%v", result.Body, err)
	}
}
func TestReviewTolerantSSE(t *testing.T) {
	for _, tc := range []struct{ name, raw string }{
		{"done at eof", "data: [DONE]\n"},
		{"responses done", "event: response.completed\ndata: {\"type\":\"response.completed\"}\n\ndata: [DONE]\n\n"},
		{"data after done", "data: [DONE]\n\ndata: {}\n\n"},
		{"trailing comment", "data: [DONE]\n\n: tail\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := streamAttempt(t, "/v1/chat/completions", "OpenAI", 0)
			_, err := a.ResponseBody([]byte(tc.raw), true)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestReviewInvalidUsageDoesNotAbort(t *testing.T) {
	for _, value := range []string{"-1", `"bad"`} {
		t.Run(value, func(t *testing.T) {
			a := streamAttempt(t, "/v1/chat/completions", "OpenAI", 0)
			r, err := a.ResponseBody([]byte("data: {\"usage\":{\"prompt_tokens\":"+value+"}}\n\ndata: [DONE]\n\n"), true)
			if err != nil {
				t.Fatal(err)
			}
			if r.UsageError == nil {
				t.Fatal("usage not marked invalid")
			}
		})
	}
}

func TestReviewTranslatedInvalidUsageDoesNotAbort(t *testing.T) {
	a := streamAttempt(t, "/v1/chat/completions", "GCPVertexAI", 0)
	r, err := a.ResponseBody([]byte("data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"hello\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":\"bad\"}}\n\n"), true)
	if err != nil {
		t.Fatal(err)
	}
	if r.UsageError == nil || !bytes.Contains(r.Body, []byte("hello")) {
		t.Fatalf("body %q usage error %v", r.Body, r.UsageError)
	}
}
func TestReviewStrictAllowsResponsesDone(t *testing.T) {
	a := streamAttempt(t, "/v1/chat/completions", "OpenAI", 0, true)
	_, err := a.ResponseBody([]byte("data: {\"type\":\"response.completed\"}\n\ndata: [DONE]\n"), true)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReviewLibraryDiscardsIncomplete(t *testing.T) {
	for _, raw := range []string{"data: {", "data: {}\n"} {
		t.Run(raw, func(t *testing.T) {
			a := streamAttempt(t, "/v1/chat/completions", "OpenAI", 0)
			_, err := a.ResponseBody([]byte(raw), true)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReviewCleanEOFMissingTerminal(t *testing.T) {
	a := streamAttempt(t, "/v1/chat/completions", "OpenAI", 0)
	_, err := a.ResponseBody([]byte("data: {}\n\n"), true)
	if !errors.Is(err, ErrMissingTerminal) {
		t.Fatalf("clean EOF: %v", err)
	}
}

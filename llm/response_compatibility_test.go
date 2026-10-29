// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package llm

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func newOpenAITestAttempt(t *testing.T, operation string, stream bool) *Attempt {
	t.Helper()
	body := `{"model":"test","messages":[{"role":"user","content":"hi"}],"input":"hi","max_tokens":20}`
	if stream {
		body = strings.TrimSuffix(body, "}") + `,"stream":true}`
	}
	r, err := NewRequest(operation, Input{Body: []byte(body)})
	if err != nil {
		t.Fatal(err)
	}
	a, _, err := r.NewAttempt(t.Context(), Backend{Schema: Schema{Name: "OpenAI"}})
	if err != nil {
		t.Fatal(err)
	}
	ct := "application/json"
	if stream {
		ct = "text/event-stream"
	}
	if _, err = a.ResponseHeaders(200, map[string]string{"content-type": ct}); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestBufferedForwarding(t *testing.T) {
	for _, field := range []string{`"usage":{"prompt_tokens":-1}`, `"response":"x"`, `"message":"x"`, `"usage":"x"`, `"usageMetadata":"x"`} {
		t.Run(field, func(t *testing.T) {
			a := newOpenAITestAttempt(t, "/v1/chat/completions", false)
			body := []byte(`{"choices":[],` + field + `}`)
			r, err := a.ResponseBody(body, true)
			if err != nil {
				t.Fatal(err)
			}
			if r.Body != nil && !bytes.Equal(r.Body, body) {
				t.Fatalf("body changed: %s", r.Body)
			}
		})
	}
}

func TestOpaqueData(t *testing.T) {
	a := newOpenAITestAttempt(t, "/v1/chat/completions", true)
	body := []byte("data: hello\n\ndata: [DONE]\n\n")
	r, err := a.ResponseBody(body, true)
	if err != nil {
		t.Fatal(err)
	}
	if r.Body != nil && !bytes.Equal(r.Body, body) {
		t.Fatalf("body changed: %s", r.Body)
	}
}

func TestLargeResponses(t *testing.T) {
	a := newOpenAITestAttempt(t, "/v1/responses", true)
	body := []byte("event: response.completed\ndata: " + `{"type":"response.completed","response":{"output":[{"type":"image_generation_call","result":"` + strings.Repeat("x", 2<<20) + `"}],"usage":{"input_tokens":7}}}` + "\n\n")
	r, err := a.ResponseBody(body, true)
	if err != nil {
		t.Fatal(err)
	}
	if r.Body != nil && !bytes.Equal(r.Body, body) {
		t.Fatal("large response changed")
	}
}

func TestResponsesErrorUsage(t *testing.T) {
	a := newOpenAITestAttempt(t, "/v1/responses", true)
	if _, err := a.ResponseBody([]byte("data: {\"type\":\"response.in_progress\",\"response\":{\"usage\":{\"input_tokens\":7}}}\n\n"), false); err != nil {
		t.Fatal(err)
	}
	body := []byte("event: error\ndata: {\"type\":\"error\",\"message\":\"server failed\",\"code\":\"server_error\"}\n\n")
	r, err := a.ResponseBody(body, true)
	var failure *StreamError
	if !errors.As(err, &failure) {
		t.Fatalf("terminal failure: %v", err)
	}
	if r.UsageError != nil {
		t.Fatal(r.UsageError)
	}
	if n, ok := r.Usage.InputTokens(); !ok || n != 7 {
		t.Fatalf("usage %d/%v", n, ok)
	}
	if !bytes.Equal(r.Body, body) {
		t.Fatal("error body lost")
	}
}

func TestObservationErrors(t *testing.T) {
	a := newOpenAITestAttempt(t, "/v1/chat/completions", false)
	r, err := a.ResponseBody([]byte(`{"choices":[],"usage":{"prompt_tokens":-1}}`), true)
	if err != nil || r.UsageError == nil {
		t.Fatalf("response=%v usage=%v", err, r.UsageError)
	}
	a = newOpenAITestAttempt(t, "/v1/chat/completions", true)
	r, err = a.ResponseBody([]byte("data: hello\n\ndata: [DONE]\n\n"), true)
	if err != nil || r.UsageError == nil {
		t.Fatalf("response=%v usage=%v", err, r.UsageError)
	}
	a = streamAttempt(t, "/v1/chat/completions", "OpenAI", 0, true)
	if _, err = a.ResponseBody([]byte("data: hello\n\n"), true); err == nil {
		t.Fatal("strict accepted invalid JSON")
	}
}

func TestOpaqueTranslatedData(t *testing.T) {
	a := streamAttempt(t, "/v1/chat/completions", "GCPAnthropic", 0)
	body := []byte("data: hello\n\n")
	r, err := a.ResponseBody(body, false)
	if err != nil || r.UsageError == nil || !bytes.Equal(r.Body, body) {
		t.Fatalf("body=%q response=%v usage=%v", r.Body, err, r.UsageError)
	}
}

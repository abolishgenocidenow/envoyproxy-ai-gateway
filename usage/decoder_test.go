// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usage

import "testing"

func TestDecoderPresence(t *testing.T) {
	for _, tc := range []struct{ name, schema, first, zero string }{
		{"openai chat", "OpenAI", `{"usage":{"prompt_tokens":9,"completion_tokens":4}}`, `{"usage":{"completion_tokens":0}}`},
		{"responses", "OpenAI", `{"response":{"usage":{"input_tokens":9,"output_tokens":4}}}`, `{"response":{"usage":{"output_tokens":0}}}`},
		{"anthropic", "Anthropic", `{"message":{"usage":{"input_tokens":9,"output_tokens":4}}}`, `{"usage":{"output_tokens":0}}`},
		{"vertex", "GCPVertexAI", `{"usageMetadata":{"promptTokenCount":9,"candidatesTokenCount":4}}`, `{"usageMetadata":{"candidatesTokenCount":0}}`},
		{"gemini", "Gemini", `{"usageMetadata":{"promptTokenCount":9,"candidatesTokenCount":4}}`, `{"usageMetadata":{"candidatesTokenCount":0}}`},
		{"bedrock", "AWSBedrock", `{"metadata":{"usage":{"inputTokens":9,"outputTokens":4}}}`, `{"usage":{"outputTokens":0}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := Decoder{Schema: tc.schema}
			u, err := d.Observe([]byte(`{"usage":{}}`))
			if err != nil || !u.IsZero() {
				t.Fatalf("empty usage = %+v, %v", u, err)
			}
			for _, data := range []string{tc.first, tc.first, `{"usage":{"output_tokens":null}}`, tc.zero} {
				if _, err = d.Observe([]byte(data)); err != nil {
					t.Fatal(err)
				}
			}
			u = d.Snapshot()
			if n, ok := u.InputTokens(); !ok || n != 9 {
				t.Fatalf("input = %d, %v", n, ok)
			}
			if n, ok := u.OutputTokens(); !ok || n != 0 {
				t.Fatalf("output = %d, %v", n, ok)
			}
			if n, ok := u.CachedInputTokens(); ok {
				t.Fatalf("absent cache = %d, %v", n, ok)
			}
		})
	}
}

func TestDecoderAnthropicCacheSnapshots(t *testing.T) {
	d := Decoder{Schema: "Anthropic"}
	for _, data := range []string{
		`{"message":{"usage":{"input_tokens":10,"cache_read_input_tokens":3,"cache_creation_input_tokens":2}}}`,
		`{"usage":{"output_tokens":8}}`,
		`{"usage":{"cache_read_input_tokens":0,"output_tokens":4}}`,
		`{"usage":{"cache_read_input_tokens":0,"output_tokens":4}}`,
	} {
		if _, err := d.Observe([]byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	u := d.Snapshot()
	if n, ok := u.InputTokens(); !ok || n != 12 {
		t.Fatalf("input = %d, %v", n, ok)
	}
	if n, ok := u.OutputTokens(); !ok || n != 4 {
		t.Fatalf("output = %d, %v", n, ok)
	}
	if n, ok := u.TotalTokens(); !ok || n != 16 {
		t.Fatalf("total = %d, %v", n, ok)
	}
	if n, ok := u.CachedInputTokens(); !ok || n != 0 {
		t.Fatalf("cache = %d, %v", n, ok)
	}
}

func TestDecoderRejectsInvalidCounts(t *testing.T) {
	for _, tc := range []struct{ name, data string }{
		{"negative", `{"usage":{"input_tokens":-1}}`},
		{"overflow", `{"usage":{"input_tokens":4294967296}}`},
		{"fraction", `{"usage":{"input_tokens":1.5}}`},
		{"normalized overflow", `{"usage":{"input_tokens":4294967295,"cache_read_input_tokens":1}}`},
		{"total overflow", `{"usage":{"input_tokens":4294967295,"output_tokens":1}}`},
		{"invalid JSON", `{"usage":`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := Decoder{Schema: "Anthropic"}
			before, err := d.Observe([]byte(`{"usage":{"input_tokens":3,"output_tokens":2}}`))
			if err != nil {
				t.Fatal(err)
			}
			after, err := d.Observe([]byte(tc.data))
			if err == nil {
				t.Fatal("expected error")
			}
			if before != after || d.Snapshot() != before {
				t.Fatal("failed observation changed snapshot")
			}
		})
	}
}

func TestDecoderDoesNotInventInput(t *testing.T) {
	d := Decoder{Schema: "Anthropic"}
	u, err := d.Observe([]byte(`{"usage":{"output_tokens":0,"cache_read_input_tokens":3}}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := u.InputTokens(); ok {
		t.Fatal("missing input reported")
	}
	if _, ok := u.TotalTokens(); ok {
		t.Fatal("missing total reported")
	}
	if n, ok := u.OutputTokens(); !ok || n != 0 {
		t.Fatalf("output = %d, %v", n, ok)
	}
}

func TestOverridePreservesPresence(t *testing.T) {
	var first, next TokenUsage
	first.SetInputTokens(7)
	first.SetOutputTokens(9)
	next.SetOutputTokens(0)
	first.Override(next)
	if n, ok := first.InputTokens(); !ok || n != 7 {
		t.Fatalf("input = %d, %v", n, ok)
	}
	if n, ok := first.OutputTokens(); !ok || n != 0 {
		t.Fatalf("output = %d, %v", n, ok)
	}
}

func BenchmarkDecoderObserve(b *testing.B) {
	data := []byte(`{"usage":{"prompt_tokens":12,"completion_tokens":3,"total_tokens":15}}`)
	d := Decoder{Schema: "OpenAI"}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := d.Observe(data); err != nil {
			b.Fatal(err)
		}
	}
}

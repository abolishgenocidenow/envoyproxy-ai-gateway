// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package llm

import "testing"

func TestReviewBufferedUsage(t *testing.T) {
	for _, tc := range []struct {
		name, op, schema, model, request, response string
		input, output, total                       uint32
		hasOutput                                  bool
	}{
		{name: "/v1/chat/completions AWSAnthropic", op: "/v1/chat/completions", schema: "AWSAnthropic", model: "test", request: `{"messages":[{"role":"user","content":"hi"}],"max_tokens":20}`, response: `{"id":"m","type":"message","role":"assistant","model":"test","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":7,"output_tokens":2,"cache_read_input_tokens":3,"cache_creation_input_tokens":4}}`, input: 14, output: 2, total: 16, hasOutput: true},
		{name: "/v1/chat/completions GCPAnthropic", op: "/v1/chat/completions", schema: "GCPAnthropic", model: "test", request: `{"messages":[{"role":"user","content":"hi"}],"max_tokens":20}`, response: `{"id":"m","type":"message","role":"assistant","model":"test","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":7,"output_tokens":2,"cache_read_input_tokens":3,"cache_creation_input_tokens":4}}`, input: 14, output: 2, total: 16, hasOutput: true},
		{name: "/v1/messages Anthropic", op: "/v1/messages", schema: "Anthropic", model: "test", request: `{"messages":[{"role":"user","content":"hi"}],"max_tokens":20}`, response: `{"id":"m","type":"message","role":"assistant","model":"test","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":7,"output_tokens":2,"cache_read_input_tokens":3,"cache_creation_input_tokens":4}}`, input: 14, output: 2, total: 16, hasOutput: true},
		{name: "/v1/messages GCPAnthropic", op: "/v1/messages", schema: "GCPAnthropic", model: "test", request: `{"messages":[{"role":"user","content":"hi"}],"max_tokens":20}`, response: `{"id":"m","type":"message","role":"assistant","model":"test","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":7,"output_tokens":2,"cache_read_input_tokens":3,"cache_creation_input_tokens":4}}`, input: 14, output: 2, total: 16, hasOutput: true},
		{name: "/v1/messages AWSAnthropic", op: "/v1/messages", schema: "AWSAnthropic", model: "test", request: `{"messages":[{"role":"user","content":"hi"}],"max_tokens":20}`, response: `{"id":"m","type":"message","role":"assistant","model":"test","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":7,"output_tokens":2,"cache_read_input_tokens":3,"cache_creation_input_tokens":4}}`, input: 14, output: 2, total: 16, hasOutput: true},
		{name: "/v1/chat/completions Bedrock converse", op: "/v1/chat/completions", schema: "AWSBedrock", model: "test", request: `{"messages":[{"role":"user","content":"hi"}],"max_tokens":20}`, response: `{"output":{"message":{"role":"assistant","content":[{"text":"hi"}]}},"stopReason":"end_turn","usage":{"inputTokens":7,"outputTokens":2,"totalTokens":9,"cacheReadInputTokens":3,"cacheWriteInputTokens":4}}`, input: 14, output: 2, total: 16, hasOutput: true},
		{name: "/v1/messages Bedrock converse", op: "/v1/messages", schema: "AWSBedrock", model: "test", request: `{"messages":[{"role":"user","content":"hi"}],"max_tokens":20}`, response: `{"output":{"message":{"role":"assistant","content":[{"text":"hi"}]}},"stopReason":"end_turn","usage":{"inputTokens":7,"outputTokens":2,"totalTokens":9,"cacheReadInputTokens":3,"cacheWriteInputTokens":4}}`, input: 14, output: 2, total: 16, hasOutput: true},
		{name: "responses metadata", op: "/v1/responses", schema: "OpenAI", model: "test", request: `{"input":"hi"}`, response: `{"id":"r","metadata":{},"usage":{"input_tokens":7,"output_tokens":2,"total_tokens":9}}`, input: 7, output: 2, total: 9, hasOutput: true},
		{name: "responses user metadata", op: "/v1/responses", schema: "OpenAI", model: "test", request: `{"input":"hi"}`, response: `{"id":"r","metadata":{"usage":"tenant-a"},"usage":{"input_tokens":7,"output_tokens":2,"total_tokens":9}}`, input: 7, output: 2, total: 9, hasOutput: true},
		{name: "titan", op: "/v1/embeddings", schema: "AWSBedrock", model: "amazon.titan-embed-text-v2:0", request: `{"input":"hi"}`, response: `{"embedding":[0.1],"inputTextTokenCount":7}`, input: 7, total: 7},
		{name: "vertex predict", op: "/v1/embeddings", schema: "GCPVertexAI", model: "text-embedding-004", request: `{"input":"hi"}`, response: `{"predictions":[{"embeddings":{"values":[0.1],"statistics":{"token_count":3}}},{"embeddings":{"values":[0.2],"statistics":{"token_count":4}}}]}`, input: 7, total: 7},
		{name: "vertex embed content", op: "/v1/embeddings", schema: "GCPVertexAI", model: "gemini-embedding-2", request: `{"input":"hi"}`, response: `{"embedding":{"values":[0.1]},"usageMetadata":{"promptTokenCount":7}}`, input: 7, total: 7},
		{name: "vertex thinking", op: "/v1/chat/completions", schema: "GCPVertexAI", model: "gemini-2.5-pro", request: `{"messages":[{"role":"user","content":"hi"}]}`, response: `{"candidates":[{"content":{"role":"model","parts":[{"text":"hi"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":2,"thoughtsTokenCount":3,"totalTokenCount":12}}`, input: 7, output: 5, total: 12, hasOutput: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"model":"` + tc.model + `",` + tc.request[1:])
			r, err := NewRequest(tc.op, Input{Body: body})
			if err != nil {
				t.Fatal(err)
			}
			schema := Schema{Name: SchemaName(tc.schema)}
			if tc.schema == "GCPAnthropic" {
				schema.Version = "vertex-2023-10-16"
			}
			a, _, err := r.NewAttempt(t.Context(), Backend{Schema: schema})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = a.ResponseHeaders(200, map[string]string{"content-type": "application/json"}); err != nil {
				t.Fatal(err)
			}
			result, err := a.ResponseBody([]byte(tc.response), true)
			if err != nil {
				t.Fatal(err)
			}
			in, ip := result.Usage.InputTokens()
			out, op := result.Usage.OutputTokens()
			total, tp := result.Usage.TotalTokens()
			if !ip || in != tc.input || out != tc.output || op != tc.hasOutput || !tp || total != tc.total {
				t.Fatalf("input %d/%v output %d/%v total %d/%v", in, ip, out, op, total, tp)
			}
		})
	}
}

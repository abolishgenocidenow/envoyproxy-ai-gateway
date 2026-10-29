// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usage

import "testing"

func TestReviewMetadataAndThinking(t *testing.T) {
	for _, tc := range []struct {
		name, schema, body string
		input, output      uint32
	}{
		{"responses empty metadata", "OpenAI", `{"metadata":{},"usage":{"input_tokens":7,"output_tokens":2}}`, 7, 2},
		{"responses user metadata", "OpenAI", `{"metadata":{"usage":"tenant-a"},"usage":{"input_tokens":7,"output_tokens":2}}`, 7, 2},
		{"gemini thoughts", "Gemini", `{"usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":2,"thoughtsTokenCount":3}}`, 7, 5},
		{"vertex thoughts", "GCPVertexAI", `{"usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":2,"thoughtsTokenCount":3}}`, 7, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := Decoder{Schema: tc.schema}
			u, err := d.Observe([]byte(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			in, ip := u.InputTokens()
			out, op := u.OutputTokens()
			if !ip || !op || in != tc.input || out != tc.output {
				t.Fatalf("input=%d/%v output=%d/%v", in, ip, out, op)
			}
		})
	}
}

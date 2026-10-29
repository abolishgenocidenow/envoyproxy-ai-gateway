// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usage

import "testing"

func TestSecondLenientUsage(t *testing.T) {
	for _, body := range []string{`{"message":"failed"}`, `{"response":"x"}`, `{"usage":[]}`, `{"usageMetadata":false}`, `{"usage":{"prompt_tokens":"unknown","prompt_tokens_details":[]}}`, `{"usage":{"prompt_tokens_details":{"cached_tokens":false}}}`, `{"usageMetadata":{"promptTokenCount":"unknown"}}`, `{"metadata":"x"}`} {
		t.Run(body, func(t *testing.T) {
			d := Decoder{Schema: "AWSBedrock"}
			if _, err := d.Observe([]byte(`{"usage":{"input_tokens":7}}`)); err != nil {
				t.Fatal(err)
			}
			got, err := d.Observe([]byte(body))
			if err != nil {
				t.Fatal(err)
			}
			if n, ok := got.InputTokens(); n != 7 || !ok {
				t.Fatalf("input %d/%v", n, ok)
			}
			if _, ok := got.CachedInputTokens(); ok {
				t.Fatal("invented cache count")
			}
		})
	}
}

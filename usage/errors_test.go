// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usage

import (
	"errors"
	"testing"
)

func TestCheckError(t *testing.T) {
	for _, tc := range []struct{ name, event, data, want string }{
		{"named error", "error", "overloaded", "error"},
		{"named failure", "response.failed", "{}", "response.failed"},
		{"named incomplete", "response.incomplete", "{}", "response.incomplete"},
		{"typed failure", "", `{"type":"response.failed"}`, "response.failed"},
		{"error object", "", `{"error":{"message":"failure"}}`, "error"},
		{"nested error", "", `{"response":{"error":{}}}`, "error"},
		{"nested status", "", `{"response":{"status":"incomplete"}}`, "response.incomplete"},
		{"null error", "", `{"error":null}`, ""},
		{"empty", "ping", "", ""},
		{"done", "", "[DONE]", ""},
		{"normal data", "", `{"type":"response.completed","response":{"error":null}}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckError(tc.event, []byte(tc.data))
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var streamErr *StreamError
			if !errors.As(err, &streamErr) || streamErr.Type != tc.want {
				t.Fatalf("got %v, want %s", err, tc.want)
			}
		})
	}
}

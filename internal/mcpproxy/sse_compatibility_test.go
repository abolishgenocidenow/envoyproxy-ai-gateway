// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package mcpproxy

import (
	"strings"
	"testing"
)

func TestLargeAndFinalMCP(t *testing.T) {
	for _, tc := range []struct{ name, raw string }{
		{"large", `data: {"jsonrpc":"2.0","id":1,"result":{"text":"` + strings.Repeat("x", 2<<20) + "\"}}\n\n"},
		{"done line before response", "data: [DONE]\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}"},
		{"final", `data: {"jsonrpc":"2.0","id":1,"result":{}}`},
		{"done before response", "data: [DONE]\n\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newSSEEventParser(strings.NewReader(tc.raw), "")
			e, err := p.next()
			if err != nil {
				t.Fatal(err)
			}
			if len(e.messages) != 1 {
				t.Fatal(e)
			}
		})
	}
	body, err := readResponseBody(strings.NewReader(strings.Repeat("x", 2<<20)))
	if err != nil || len(body) != 2<<20 {
		t.Fatalf("body length=%d error=%v", len(body), err)
	}
}

func TestResponseBeforeBadFollowingEvent(t *testing.T) {
	body, err := readSSEJSONResponse(strings.NewReader("data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n\ndata: invalid\n\n"))
	if err != nil || len(body) == 0 {
		t.Fatalf("body=%q error=%v", body, err)
	}
}

// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package mcpproxy

import (
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func TestSkipNonRPC(t *testing.T) {
	for _, prefix := range []string{"ping", `{"hello":"world"}`, `[1,2]`} {
		t.Run(prefix, func(t *testing.T) {
			for _, split := range []bool{false, true} {
				var input io.Reader = strings.NewReader("data: " + prefix + "\n\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n\n")
				if split {
					input = iotest.OneByteReader(input)
				}
				got, err := readSSEJSONResponse(input)
				if err != nil || !strings.Contains(string(got), `"result"`) {
					t.Fatalf("%s %v", got, err)
				}
			}
		})
	}
}

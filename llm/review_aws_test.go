// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package llm

import (
	"bytes"
	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
	"testing"
)

func TestReviewAWSMissingTerminal(t *testing.T) {
	for _, schema := range []string{"AWSBedrock", "AWSAnthropic"} {
		t.Run(schema, func(t *testing.T) {
			r, err := NewRequest("/v1/chat/completions", Input{Body: []byte(`{"model":"test","stream":true,"messages":[{"role":"user","content":"hi"}],"max_tokens":20}`)})
			if err != nil {
				t.Fatal(err)
			}
			a, _, err := r.NewAttempt(t.Context(), Backend{Schema: Schema{Name: SchemaName(schema)}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = a.ResponseHeaders(200, map[string]string{"content-type": "application/vnd.amazon.eventstream"}); err != nil {
				t.Fatal(err)
			}
			var wire bytes.Buffer
			payload := []byte(`{}`)
			if schema == "AWSAnthropic" {
				payload = []byte(`{"bytes":"eyJ0eXBlIjoicGluZyJ9"}`)
			}
			if err = eventstream.NewEncoder().Encode(&wire, eventstream.Message{Headers: eventstream.Headers{{Name: ":message-type", Value: eventstream.StringValue("event")}, {Name: ":event-type", Value: eventstream.StringValue("chunk")}}, Payload: payload}); err != nil {
				t.Fatal(err)
			}
			_, err = a.ResponseBody(wire.Bytes(), true)
			if err == nil {
				t.Fatal("clean EOF without terminal accepted")
			}
		})
	}
}

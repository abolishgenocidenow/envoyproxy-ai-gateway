// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package llm

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
)

func TestAWSMissingTerminal(t *testing.T) {
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

func TestAWSOpaqueTrailingFrame(t *testing.T) {
	for _, schema := range []string{"AWSBedrock", "AWSAnthropic"} {
		for _, path := range []string{"/v1/chat/completions", "/v1/messages"} {
			t.Run(schema+path, func(t *testing.T) {
				var wire bytes.Buffer
				eventType := "messageStop"
				payload := []byte(`{"stopReason":"end_turn"}`)
				if schema == "AWSAnthropic" {
					eventType = "chunk"
					payload = []byte(`{"bytes":"eyJ0eXBlIjoibWVzc2FnZV9zdG9wIn0="}`)
				}
				encoder := eventstream.NewEncoder()
				if err := encoder.Encode(&wire, eventstream.Message{Headers: eventstream.Headers{{Name: ":event-type", Value: eventstream.StringValue(eventType)}}, Payload: payload}); err != nil {
					t.Fatal(err)
				}
				if err := encoder.Encode(&wire, eventstream.Message{Headers: eventstream.Headers{{Name: "event-type", Value: eventstream.StringValue("end")}}, Payload: []byte("this-is-end")}); err != nil {
					t.Fatal(err)
				}
				for split := 0; split <= wire.Len(); split++ {
					r, err := NewRequest(path, Input{Body: []byte(`{"model":"test","stream":true,"messages":[{"role":"user","content":"hi"}],"max_tokens":20}`)})
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
					if _, err = a.ResponseBody(wire.Bytes()[:split], false); err != nil {
						t.Fatalf("split %d: %v", split, err)
					}
					if _, err = a.ResponseBody(wire.Bytes()[split:], true); err != nil {
						t.Fatalf("split %d: %v", split, err)
					}
				}
			})
		}
	}
}

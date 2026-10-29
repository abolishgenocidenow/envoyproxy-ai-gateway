// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package llm

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
)

func TestSecondCachePresence(t *testing.T) {
	for _, provider := range []string{"chat", "responses", "aws-chat", "aws-messages"} {
		for _, present := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/present=%v", provider, present), func(t *testing.T) {
				var a *Attempt
				var body []byte
				fields := ""
				if present {
					fields = `,"prompt_tokens_details":{"cached_tokens":0,"cache_write_tokens":0}`
				}
				switch provider {
				case "chat":
					a = secondAttempt(t, "/v1/chat/completions", true)
					body = []byte("data: " + `{"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9` + fields + `}}` + "\n\ndata: [DONE]\n\n")
				case "responses":
					a = secondAttempt(t, "/v1/responses", true)
					fields = ""
					if present {
						fields = `,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0}`
					}
					body = []byte("data: " + `{"type":"response.completed","response":{"usage":{"input_tokens":7,"output_tokens":2,"total_tokens":9` + fields + `}}}` + "\n\n")
				default:
					operation := "/v1/chat/completions"
					if provider == "aws-messages" {
						operation = "/v1/messages"
					}
					r, err := NewRequest(operation, Input{Body: []byte(`{"model":"test","stream":true,"messages":[{"role":"user","content":"hi"}],"max_tokens":20}`)})
					if err != nil {
						t.Fatal(err)
					}
					a, _, err = r.NewAttempt(t.Context(), Backend{Schema: Schema{Name: "AWSAnthropic"}})
					if err != nil {
						t.Fatal(err)
					}
					if _, err = a.ResponseHeaders(200, map[string]string{"content-type": "application/vnd.amazon.eventstream"}); err != nil {
						t.Fatal(err)
					}
					fields = ""
					if present {
						fields = `,"cache_read_input_tokens":0,"cache_creation_input_tokens":0`
					}
					var wire bytes.Buffer
					for _, payload := range []string{`{"type":"message_start","message":{"id":"m","type":"message","role":"assistant","model":"test","content":[],"usage":{"input_tokens":7,"output_tokens":1` + fields + `}}}`, `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`, `{"type":"message_stop"}`} {
						err = eventstream.NewEncoder().Encode(&wire, eventstream.Message{Headers: eventstream.Headers{{Name: ":message-type", Value: eventstream.StringValue("event")}, {Name: ":event-type", Value: eventstream.StringValue("chunk")}}, Payload: []byte(`{"bytes":"` + base64.StdEncoding.EncodeToString([]byte(payload)) + `"}`)})
						if err != nil {
							t.Fatal(err)
						}
					}
					body = wire.Bytes()
				}
				result, err := a.ResponseBody(body, true)
				if err != nil {
					t.Fatal(err)
				}
				if n, ok := result.Usage.CachedInputTokens(); n != 0 || ok != present {
					t.Fatalf("cached %d/%v want present=%v", n, ok, present)
				}
				if n, ok := result.Usage.CacheCreationInputTokens(); n != 0 || ok != present {
					t.Fatalf("write %d/%v want present=%v", n, ok, present)
				}
			})
		}
	}
}

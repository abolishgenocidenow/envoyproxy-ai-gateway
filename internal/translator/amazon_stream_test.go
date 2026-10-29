// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package translator

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"

	"github.com/envoyproxy/ai-gateway/internal/json"
	"github.com/envoyproxy/ai-gateway/usage"
)

func amazonTestFrame(t testing.TB, typ string, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	err := eventstream.NewEncoder().Encode(&buf, eventstream.Message{Headers: eventstream.Headers{{Name: ":message-type", Value: eventstream.StringValue(typ)}}, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestAmazonStreamSplitBoundaries(t *testing.T) {
	frame := amazonTestFrame(t, "event", []byte(`{"usage":{"inputTokens":1}}`))
	for split := 0; split <= len(frame); split++ {
		var pending []byte
		count := 0
		emit := func(msg eventstream.Message) error {
			count++
			if !bytes.Equal(msg.Payload, []byte(`{"usage":{"inputTokens":1}}`)) {
				t.Fatal("wrong payload")
			}
			return nil
		}
		if err := readAmazonEventStream(bytes.NewReader(frame[:split]), &pending, len(frame), false, emit); err != nil {
			t.Fatal(err)
		}
		if err := readAmazonEventStream(bytes.NewReader(frame[split:]), &pending, len(frame), true, emit); err != nil {
			t.Fatal(err)
		}
		if count != 1 || len(pending) != 0 || cap(pending) > len(frame) {
			t.Fatalf("split %d: count %d, retained %d/%d", split, count, len(pending), cap(pending))
		}
	}
}

func TestAmazonStreamErrors(t *testing.T) {
	frame := amazonTestFrame(t, "event", []byte(`{}`))
	crc := bytes.Clone(frame)
	crc[len(crc)-1] ^= 1
	oversized := make([]byte, 4)
	binary.BigEndian.PutUint32(oversized, 1<<21)
	for _, tc := range []struct {
		name string
		data []byte
		want error
	}{
		{"partial EOF", frame[:len(frame)-1], io.ErrUnexpectedEOF},
		{"CRC", crc, nil},
		{"oversized header", oversized, ErrAmazonEventTooLarge},
		{"invalid length", []byte{0, 0, 0, 1}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var pending []byte
			err := readAmazonEventStream(bytes.NewReader(tc.data), &pending, 1<<20, true, func(eventstream.Message) error { t.Fatal("invalid frame emitted"); return nil })
			if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestAmazonStreamEarlyException(t *testing.T) {
	frame := amazonTestFrame(t, "exception", []byte(`{"message":"overloaded"}`))
	var pending []byte
	err := readAmazonEventStream(bytes.NewReader(frame), &pending, 1024, false, func(eventstream.Message) error { t.Fatal("exception emitted as data"); return nil })
	var terminal *usage.StreamError
	if !errors.As(err, &terminal) {
		t.Fatalf("expected terminal error, got %v", err)
	}
}

func TestAmazonStreamManyEventsWithinLimit(t *testing.T) {
	frame := amazonTestFrame(t, "event", []byte(`{}`))
	var pending []byte
	count := 0
	if err := readAmazonEventStream(bytes.NewReader(bytes.Repeat(frame, 10000)), &pending, len(frame), true, func(eventstream.Message) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 10000 || cap(pending) > len(frame) {
		t.Fatalf("count=%d, capacity=%d", count, cap(pending))
	}
}

func FuzzAmazonStream(f *testing.F) {
	f.Add([]byte{0, 0, 0, 16}, uint8(1))
	f.Add(amazonTestFrame(f, "event", []byte(`{}`)), uint8(7))
	f.Fuzz(func(t *testing.T, data []byte, step uint8) {
		run := func(chunk int) (int, string) {
			var pending []byte
			count := 0
			for offset := 0; offset < len(data); offset += chunk {
				end := min(offset+chunk, len(data))
				err := readAmazonEventStream(bytes.NewReader(data[offset:end]), &pending, 1024, end == len(data), func(eventstream.Message) error { count++; return nil })
				if cap(pending) > 1024 {
					t.Fatal("retained buffer exceeded limit")
				}
				if err != nil {
					return count, err.Error()
				}
			}
			return count, ""
		}
		one, errOne := run(max(1, len(data)))
		split, errSplit := run(int(step) + 1)
		if one != split || errOne != errSplit {
			t.Fatalf("chunk dependent result: %d/%q vs %d/%q", one, errOne, split, errSplit)
		}
	})
}

func TestAWSStreamingUsagePresence(t *testing.T) {
	for _, name := range []string{"openai bedrock", "anthropic bedrock", "openai anthropic", "anthropic anthropic"} {
		t.Run(name, func(t *testing.T) {
			var consume func([]byte, bool) (usage.TokenUsage, error)
			anthropic := false
			switch name {
			case "openai bedrock":
				tr := &openAIToAWSBedrockTranslatorV1ChatCompletion{stream: true}
				consume = func(data []byte, end bool) (usage.TokenUsage, error) {
					_, _, u, _, err := tr.ResponseBody(nil, bytes.NewReader(data), end, nil)
					return u, err
				}
			case "anthropic bedrock":
				tr := &anthropicToAWSBedrockTranslator{stream: true}
				consume = func(data []byte, end bool) (usage.TokenUsage, error) {
					_, _, u, _, err := tr.ResponseBody(nil, bytes.NewReader(data), end, nil)
					return u, err
				}
			case "openai anthropic":
				anthropic = true
				tr := &openAIToAWSAnthropicTranslatorV1ChatCompletion{streamParser: newAnthropicStreamParser("test")}
				consume = func(data []byte, end bool) (usage.TokenUsage, error) {
					_, _, u, _, err := tr.ResponseBody(nil, bytes.NewReader(data), end, nil)
					return u, err
				}
			case "anthropic anthropic":
				anthropic = true
				tr := &anthropicToAWSAnthropicTranslator{anthropicToAnthropicTranslator: anthropicToAnthropicTranslator{stream: true}}
				consume = func(data []byte, end bool) (usage.TokenUsage, error) {
					_, _, u, _, err := tr.ResponseBody(nil, bytes.NewReader(data), end, nil)
					return u, err
				}
			}
			payloads := [][]byte{[]byte(`{"usage":{"outputTokens":9}}`), []byte(`{"usage":{"outputTokens":0}}`)}
			if anthropic {
				payloads = [][]byte{[]byte(`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":9}}`), []byte(`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":0}}`)}
			}
			for i, payload := range payloads {
				if anthropic {
					var err error
					payload, err = json.Marshal(struct {
						Bytes []byte `json:"bytes"`
					}{payload})
					if err != nil {
						t.Fatal(err)
					}
				}
				frame := amazonTestFrame(t, "event", payload)
				u, err := consume(frame, false)
				if err != nil {
					t.Fatal(err)
				}
				want := uint32(9)
				if i == 1 {
					want = 0
				}
				if n, ok := u.OutputTokens(); !ok || n != want {
					t.Fatalf("output %d/%v, want %d", n, ok, want)
				}
				if _, ok := u.InputTokens(); ok {
					t.Fatal("absent input reported")
				}
				if _, ok := u.TotalTokens(); ok {
					t.Fatal("absent total reported")
				}
			}
			if _, err := consume([]byte{0, 0}, true); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("partial EOF: %v", err)
			}
		})
	}
}

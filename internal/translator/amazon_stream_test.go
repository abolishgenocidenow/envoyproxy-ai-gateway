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
	f.Add([]byte{0, 0, 0, 16}, []byte{1, 3})
	f.Add(amazonTestFrame(f, "event", []byte(`{}`)), []byte{7, 1, 9})
	f.Fuzz(func(t *testing.T, data, splits []byte) {
		// Whole-input oracle uses frame lengths and the SDK decoder directly.
		count := 0
		var wantErr error
		for rest := data; len(rest) > 0; {
			if len(rest) < 4 {
				wantErr = io.ErrUnexpectedEOF
				break
			}
			size := binary.BigEndian.Uint32(rest[:4])
			if size < 16 {
				wantErr = errors.New("invalid frame")
				break
			}
			if size > 1024 {
				wantErr = ErrAmazonEventTooLarge
				break
			}
			if int(size) > len(rest) {
				wantErr = io.ErrUnexpectedEOF
				break
			}
			msg, err := eventstream.NewDecoder().Decode(bytes.NewReader(rest[:size]), nil)
			if err != nil {
				wantErr = err
				break
			}
			if typ := msg.Headers.Get(":message-type"); typ != nil && (typ.String() == "error" || typ.String() == "exception") {
				wantErr = &usage.StreamError{Type: typ.String()}
				break
			}
			count++
			rest = rest[size:]
		}
		var pending []byte
		got := 0
		var gotErr error
		for pos, i := 0, 0; pos < len(data); i++ {
			width := len(data) - pos
			if len(splits) > 0 {
				width = int(splits[i%len(splits)]) + 1
			}
			end := min(pos+width, len(data))
			gotErr = readAmazonEventStream(bytes.NewReader(data[pos:end]), &pending, 1024, end == len(data), func(eventstream.Message) error { got++; return nil })
			if cap(pending) > 1024 {
				t.Fatal("limit exceeded")
			}
			if gotErr != nil {
				break
			}
			pos = end
		}
		category := func(err error) string {
			if err == nil {
				return "ok"
			}
			if errors.Is(err, ErrAmazonEventTooLarge) {
				return "overflow"
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return "incomplete"
			}
			var provider *usage.StreamError
			if errors.As(err, &provider) {
				return "provider"
			}
			return "invalid"
		}
		if count != got || category(wantErr) != category(gotErr) {
			t.Fatalf("oracle %d/%v incremental %d/%v", count, wantErr, got, gotErr)
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

func TestAmazonStreamUnboundedLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 2<<20)
	frame := amazonTestFrame(t, "event", payload)
	for _, tc := range []struct {
		name  string
		limit int
		want  error
	}{
		{"unbounded", -1, nil},
		{"explicit cap", 1 << 20, ErrAmazonEventTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var pending []byte
			seen := false
			err := readAmazonEventStream(bytes.NewReader(frame), &pending, tc.limit, true, func(msg eventstream.Message) error {
				seen = true
				if !bytes.Equal(msg.Payload, payload) {
					t.Fatal("payload changed")
				}
				return nil
			})
			if !errors.Is(err, tc.want) || seen != (tc.want == nil) {
				t.Fatalf("seen=%v error=%v", seen, err)
			}
		})
	}
}

// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package sse

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestReaderFraming(t *testing.T) {
	for _, ending := range []string{"\n", "\r", "\r\n"} {
		t.Run(strings.ReplaceAll(strings.ReplaceAll(ending, "\r", "CR"), "\n", "LF"), func(t *testing.T) {
			input := "\xef\xbb\xbf: comment" + ending + "event: sample" + ending + "data: one" + ending + "data:  two" + ending + ending
			for split := 0; split <= len(input); split++ {
				r := NewReader(1024)
				var got []string
				emit := func(e Event) error { got = append(got, e.Type+":"+string(e.Data)); return nil }
				if err := r.Feed([]byte(input[:split]), emit); err != nil {
					t.Fatal(err)
				}
				if err := r.Feed([]byte(input[split:]), emit); err != nil {
					t.Fatal(err)
				}
				if err := r.End(); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, []string{"sample:one\n two"}) {
					t.Fatalf("split %d: %q", split, got)
				}
			}
		})
	}
}

func TestReaderBoundsAndEOF(t *testing.T) {
	for _, tt := range []struct {
		name, input string
		limit       int
		want        error
	}{
		{"overflow", "data: " + strings.Repeat("x", 100), 16, ErrOverflow},
		{"partial line", "data: x", 16, ErrIncomplete},
		{"partial event", "data: x\n", 16, ErrIncomplete},
		{"complete", "data: x\n\n", 16, nil},
		{"many events", strings.Repeat("data: x\n\n", 1000), 16, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.limit)
			r.Strict = true
			err := r.Feed([]byte(tt.input), func(Event) error { return nil })
			if err == nil {
				err = r.End()
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v want %v", err, tt.want)
			}
			if cap(r.buffer) > tt.limit {
				t.Fatal("retained buffer exceeds limit")
			}
		})
	}
}

// referenceEvents is a whole-input parser independent of Reader's buffering.
func referenceEvents(input string) []string {
	input = strings.TrimPrefix(input, "\xef\xbb\xbf")
	input = strings.ReplaceAll(strings.ReplaceAll(input, "\r\n", "\n"), "\r", "\n")
	var out, data []string
	var kind, id string
	dispatch := func() {
		if len(data) > 0 {
			out = append(out, kind+"\x00"+id+"\x00"+strings.Join(data, "\n"))
		}
		data = nil
		kind = ""
	}
	lines := strings.Split(input, "\n")
	for _, line := range lines[:len(lines)-1] {
		if line == "" {
			dispatch()
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "data":
			data = append(data, value)
		case "event":
			kind = value
		case "id":
			if !strings.ContainsRune(value, 0) {
				id = value
			}
		}
	}
	// The library accepts a complete final terminal marker without a blank line.
	if (lines[len(lines)-1] == "" || strings.HasPrefix(lines[len(lines)-1], ":")) && len(data) == 1 && data[0] == "[DONE]" {
		dispatch()
	}
	return out
}

func FuzzReaderChunks(f *testing.F) {
	for _, s := range []string{"data: one\r\n\r\n", "\xef\xbb\xbfdata: x\rdata: y\r\r", "id: a\ndata:\n\ndata: two\n\n", ": comment\ndata: [DONE]\n"} {
		f.Add(s, []byte{1, 7, 2})
	}
	f.Fuzz(func(t *testing.T, input string, splits []byte) {
		if len(input) > 16384 {
			t.Skip()
		}
		r := NewReader(-1)
		var got []string
		emit := func(e Event) error { got = append(got, e.Type+"\x00"+e.ID+"\x00"+string(e.Data)); return nil }
		for pos, i := 0, 0; pos < len(input); i++ {
			width := len(input) - pos
			if len(splits) > 0 {
				width = int(splits[i%len(splits)]) + 1
			}
			end := min(pos+width, len(input))
			if err := r.Feed([]byte(input[pos:end]), emit); err != nil {
				t.Fatal(err)
			}
			pos = end
		}
		if err := r.End(emit); err != nil {
			t.Fatal(err)
		}
		if want := referenceEvents(input); !reflect.DeepEqual(want, got) {
			t.Fatalf("oracle %q, incremental %q", want, got)
		}
	})
}

func BenchmarkReaderChunk(b *testing.B) {
	r := NewReader(1024)
	chunk := []byte("data: {\"text\":\"hello\"}\n\n")
	emit := func(Event) error { return nil }
	b.ReportAllocs()
	for b.Loop() {
		if err := r.Feed(chunk, emit); err != nil {
			b.Fatal(err)
		}
	}
}

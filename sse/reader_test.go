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

func FuzzReaderChunks(f *testing.F) {
	for _, s := range []string{"data: one\r\n\r\n", "\xef\xbb\xbfdata: x\rdata: y\r\r", "data:\n\n", "event: error\ndata: {}\n\n"} {
		f.Add(s, uint16(1))
	}
	f.Fuzz(func(t *testing.T, input string, width uint16) {
		parse := func(chunk int) ([]string, error) {
			r := NewReader(4096)
			var out []string
			emit := func(e Event) error { out = append(out, e.Type+"\x00"+string(e.Data)); return nil }
			for i := 0; i < len(input); i += chunk {
				if err := r.Feed([]byte(input[i:min(i+chunk, len(input))]), emit); err != nil {
					return out, err
				}
			}
			return out, r.End()
		}
		want, we := parse(max(1, len(input)))
		got, ge := parse(int(width)%128 + 1)
		if !reflect.DeepEqual(want, got) || !errors.Is(we, ge) {
			t.Fatalf("chunk-dependent result: %q %v / %q %v", want, we, got, ge)
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

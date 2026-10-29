// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

// Package sse provides bounded incremental Server-Sent Events framing.
package sse

import (
	"bytes"
	"errors"
)

const DefaultLimit = 1 << 20

var (
	ErrOverflow   = errors.New("sse event exceeds retained payload limit")
	ErrIncomplete = errors.New("incomplete sse event at end of stream")
)

// Event contains one dispatched event. Data is valid only during the callback.
// An omitted event field is represented by an empty Type.
type Event struct {
	Type string
	ID   string
	Data []byte
}

// Reader is single-owner state. Its zero value uses DefaultLimit. It retains at
// most the configured limit of bytes of framing payload, independent of stream length or chunk size.
// Callback allocations and caller-owned data are not included in this limit.
// Errors are sticky. EOF never dispatches an unterminated event.
type Reader struct {
	limit     int
	buffer    []byte
	lineStart int
	cr        bool
	started   bool
	err       error
}

// NewReader sets the maximum retained event size. Nonpositive limits use DefaultLimit.
func NewReader(limit int) *Reader { return &Reader{limit: limit} }

// Feed consumes bytes synchronously. It supports LF, CR, split CRLF, an initial
// UTF-8 BOM, comments, and multiline data. No bytes are retained from p itself.
func (r *Reader) Feed(p []byte, emit func(Event) error) error {
	if r.err != nil {
		return r.err
	}
	limit := r.limit
	if limit <= 0 {
		limit = DefaultLimit
	}
	for _, b := range p {
		if r.cr {
			r.cr = false
			if b == '\n' {
				continue
			}
		}
		if b == '\r' || b == '\n' {
			r.cr = b == '\r'
			if !r.started {
				r.started = true
				r.buffer = bytes.TrimPrefix(r.buffer, []byte{0xef, 0xbb, 0xbf})
			}
			if len(r.buffer) == r.lineStart {
				r.err = r.dispatch(emit)
				r.buffer = r.buffer[:0]
				r.lineStart = 0
				if r.err != nil {
					return r.err
				}
				continue
			}
			b = '\n'
		}
		if len(r.buffer) >= limit {
			r.err = ErrOverflow
			return r.err
		}
		if len(r.buffer) == cap(r.buffer) {
			capacity := max(64, cap(r.buffer)*2)
			capacity = min(capacity, limit)
			next := make([]byte, len(r.buffer), capacity)
			copy(next, r.buffer)
			r.buffer = next
		}
		r.buffer = append(r.buffer, b)
		if b == '\n' {
			r.lineStart = len(r.buffer)
		}
	}
	return nil
}

func (r *Reader) dispatch(emit func(Event) error) error {
	event := Event{Data: r.buffer[:0]}
	var hasData bool
	for line := range bytes.SplitSeq(r.buffer, []byte{'\n'}) {
		field, value, _ := bytes.Cut(line, []byte{':'})
		value = bytes.TrimPrefix(value, []byte{' '})
		switch string(field) {
		case "id":
			if !bytes.ContainsRune(value, 0) {
				event.ID = string(value)
			}
		case "event":
			event.Type = string(value)
		case "data":
			if hasData {
				event.Data = append(event.Data, '\n')
			}
			event.Data = append(event.Data, value...)
			hasData = true
		}
	}
	if hasData {
		return emit(event)
	}
	return nil
}

// End checks for a partial event. Complete comments and unknown fields do not
// dispatch events, but an unfinished event is reported rather than fabricated.
func (r *Reader) End() error {
	if r.err != nil {
		return r.err
	}
	if len(r.buffer) != 0 {
		r.err = ErrIncomplete
	}
	return r.err
}

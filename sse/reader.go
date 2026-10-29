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
// Errors are sticky. EOF behavior is controlled by Strict and FlushFinal.
type Reader struct {
	// Strict reports incomplete events at EOF instead of discarding them.
	Strict bool
	// FlushFinal dispatches a final event at EOF for tolerant protocols such as MCP.
	FlushFinal bool
	id         string
	comment    bool
	limit      int
	buffer     []byte
	lineStart  int
	cr         bool
	started    bool
	err        error
}

// NewReader sets the maximum retained event size. Zero uses DefaultLimit. A negative limit disables the cap.
func NewReader(limit int) *Reader { return &Reader{limit: limit} }

// Feed consumes bytes synchronously. It supports LF, CR, split CRLF, an initial
// UTF-8 BOM, comments, and multiline data. No bytes are retained from p itself.
func (r *Reader) Feed(p []byte, emit func(Event) error) error {
	if r.err != nil {
		return r.err
	}
	limit := r.limit
	if limit == 0 {
		limit = DefaultLimit
	}
	for _, b := range p {
		if r.cr {
			r.cr = false
			if b == '\n' {
				continue
			}
		}
		if r.comment {
			if b == '\r' || b == '\n' {
				r.comment = false
				r.cr = b == '\r'
			}
			continue
		}
		if b == ':' && (len(r.buffer) == r.lineStart || (!r.started && bytes.Equal(r.buffer, []byte{0xef, 0xbb, 0xbf}))) {
			if !r.started {
				r.started = true
				r.buffer = r.buffer[:0]
			}
			r.comment = true
			continue
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
		if limit > 0 && len(r.buffer) >= limit {
			r.err = ErrOverflow
			return r.err
		}
		if len(r.buffer) == cap(r.buffer) {
			capacity := max(64, cap(r.buffer)*2)
			if limit > 0 {
				capacity = min(capacity, limit)
			}
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
	event := Event{ID: r.id, Data: r.buffer[:0]}
	var hasData bool
	for line := range bytes.SplitSeq(r.buffer, []byte{'\n'}) {
		field, value, _ := bytes.Cut(line, []byte{':'})
		value = bytes.TrimPrefix(value, []byte{' '})
		switch string(field) {
		case "id":
			if !bytes.ContainsRune(value, 0) {
				event.ID = string(value)
				r.id = event.ID
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

// End finishes the stream. By default unfinished events are discarded. A complete
// trailing [DONE] data line is dispatched when a callback is supplied. FlushFinal
// also dispatches other final events. Strict reports discarded partial events.
func (r *Reader) End(callback ...func(Event) error) error {
	if r.err != nil {
		return r.err
	}
	emit := func(Event) error { return nil }
	if len(callback) > 0 {
		emit = callback[0]
	}
	dataLines := 0
	completeDone := false
	for line := range bytes.SplitSeq(r.buffer, []byte{'\n'}) {
		field, value, _ := bytes.Cut(line, []byte{':'})
		if bytes.Equal(field, []byte("data")) {
			dataLines++
			completeDone = bytes.Equal(bytes.TrimPrefix(value, []byte{' '}), []byte("[DONE]"))
		}
	}
	completeDone = completeDone && dataLines == 1 && r.lineStart == len(r.buffer)
	if len(r.buffer) > 0 && (r.FlushFinal || completeDone) {
		r.err = r.dispatch(emit)
	} else if len(r.buffer) > 0 && r.Strict {
		r.err = ErrIncomplete
	}
	r.buffer = r.buffer[:0]
	r.lineStart = 0
	return r.err
}

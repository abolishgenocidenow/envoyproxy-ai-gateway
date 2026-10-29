// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package translator

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
	"github.com/tidwall/gjson"

	"github.com/envoyproxy/ai-gateway/internal/json"
	"github.com/envoyproxy/ai-gateway/usage"
)

const defaultAmazonEventLimit = 1 << 20

// ErrAmazonEventTooLarge reports a binary frame larger than the caller limit.
var ErrAmazonEventTooLarge = errors.New("amazon event exceeds configured limit")

// readAmazonEventStream retains only the incomplete frame, never a complete
// response. The limit includes the binary envelope. CRC errors are fatal and a
// partial frame at transport EOF is distinguished from a callback boundary.
func readAmazonEventStream(r io.Reader, pending *[]byte, limit int, end bool, emit func(eventstream.Message) error) error {
	if limit < 0 {
		limit = int(^uint(0) >> 1)
	}
	if limit == 0 {
		limit = defaultAmazonEventLimit
	}
	var scratch [4096]byte
	decoder := eventstream.NewDecoder()
	for {
		n, readErr := r.Read(scratch[:])
		data := scratch[:n]
		for len(data) > 0 {
			target := 4
			if len(*pending) >= 4 {
				length := binary.BigEndian.Uint32((*pending)[:4])
				if length < 16 {
					*pending = nil
					return errors.New("invalid amazon event length")
				}
				if uint64(length) > uint64(limit) {
					*pending = nil
					return ErrAmazonEventTooLarge
				}
				target = int(length)
			}
			take := min(target-len(*pending), len(data))
			if len(*pending)+take > limit {
				*pending = nil
				return ErrAmazonEventTooLarge
			}
			if len(*pending)+take > cap(*pending) {
				capacity := min(limit, max(len(*pending)+take, 2*cap(*pending)))
				grown := make([]byte, len(*pending), capacity)
				copy(grown, *pending)
				*pending = grown
			}
			*pending = append(*pending, data[:take]...)
			data = data[take:]
			if len(*pending) < 4 {
				continue
			}
			length := binary.BigEndian.Uint32((*pending)[:4])
			if length < 16 {
				*pending = nil
				return errors.New("invalid amazon event length")
			}
			if uint64(length) > uint64(limit) {
				*pending = nil
				return ErrAmazonEventTooLarge
			}
			if len(*pending) != int(length) {
				continue
			}
			msg, err := decoder.Decode(bytes.NewReader(*pending), nil)
			*pending = (*pending)[:0]
			if err != nil {
				return fmt.Errorf("decode amazon event: %w", err)
			}
			if typ := msg.Headers.Get(":message-type"); typ != nil && (typ.String() == "exception" || typ.String() == "error") {
				return &usage.StreamError{Type: typ.String()}
			}
			if err = emit(msg); err != nil {
				return err
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return fmt.Errorf("read amazon event: %w", readErr)
			}
			break
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	if end && len(*pending) > 0 {
		*pending = nil
		return io.ErrUnexpectedEOF
	}
	return nil
}

// amazonStreamState tracks provider completion across binary frame boundaries.
type amazonStreamState struct{ terminal bool }

var ErrMissingTerminal = errors.New("stream ended without a terminal event")

func (s *amazonStreamState) read(r io.Reader, pending *[]byte, limit int, end bool, emit func(eventstream.Message) error) error {
	err := readAmazonEventStream(r, pending, limit, end, func(msg eventstream.Message) error {
		// Preserve the translators' tolerance for opaque event payloads. Frame
		// bounds, CRCs, and provider exceptions are checked before this callback.
		// An ignored payload must not count as provider completion.
		if !gjson.ValidBytes(msg.Payload) {
			return nil
		}
		if typ := msg.Headers.Get(":event-type"); typ != nil && (typ.String() == "messageStop" || typ.String() == "metadata") {
			s.terminal = true
		}
		var envelope struct {
			Bytes []byte `json:"bytes"`
			Type  string `json:"type"`
		}
		if json.Unmarshal(msg.Payload, &envelope) == nil {
			if envelope.Type == "message_stop" {
				s.terminal = true
			}
			if len(envelope.Bytes) > 0 {
				var event struct {
					Type string `json:"type"`
				}
				if json.Unmarshal(envelope.Bytes, &event) == nil && event.Type == "message_stop" {
					s.terminal = true
				}
			}
		}
		return emit(msg)
	})

	return err
}

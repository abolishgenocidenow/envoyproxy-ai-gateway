// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package mcpproxy

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"

	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/sse"
)

var (
	sseEventPrefixSpace = []byte("event: ")
	sseIDPrefixSpace    = []byte("id: ")
	sseDataPrefixSpace  = []byte("data: ")

	sseLF   = []byte{'\n'}
	sseLFLF = []byte{'\n', '\n'}

	// utf8BOM is the UTF-8 Byte Order Mark (U+FEFF). Some backends prepend this
	// invisible sequence to response bodies, which breaks JSON decoding.
	utf8BOM = []byte{0xEF, 0xBB, 0xBF}
)

// sseEventParser reads bytes from a reader and parses the SSE Events gracefully
// handling the different line terminations: CR, LF, CRLF.
type sseEventParser struct {
	backend filterapi.MCPBackendName
	r       io.Reader
	readBuf [4096]byte
	framing sse.Reader
	pos, n  int
	readErr error
}

func newSSEEventParser(r io.Reader, backend filterapi.MCPBackendName) sseEventParser {
	return sseEventParser{r: r, backend: backend}
}

// tryDecodeJSONRPCMessage attempts to decode the body as a single JSON-RPC message.
// It strips a leading UTF-8 BOM and whitespace before decoding.
// Returns the decoded message and true on success, or nil and false if the body
// is not valid JSON-RPC (e.g. the backend sent SSE despite a JSON content type).
func tryDecodeJSONRPCMessage(body []byte) (jsonrpc.Message, bool) {
	body = bytes.TrimSpace(body)
	body = bytes.TrimPrefix(body, utf8BOM)
	msg, err := jsonrpc.DecodeMessage(body)
	if err != nil {
		return nil, false
	}
	return msg, true
}

// isJSONContentType reports whether value has application/json as its media
// type, regardless of optional parameters such as charset.
func isJSONContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && strings.EqualFold(mediaType, "application/json")
}

// next reads the next SSE event from the stream.
func (s *sseEventParser) next() (*sseEvent, error) {
	var event *sseEvent
	emit := func(e sse.Event) error {
		ret := &sseEvent{backend: s.backend, event: e.Type, id: e.ID}
		if len(bytes.TrimSpace(e.Data)) != 0 {
			msg, err := jsonrpc.DecodeMessage(e.Data)
			if err != nil {
				return fmt.Errorf("failed to decode jsonrpc message from sse data: %w", err)
			}
			ret.messages = []jsonrpc.Message{msg}
		}
		event = ret
		return nil
	}
	emptyReads := 0
	for {
		// Stop after one event without accumulating an unbounded queue of messages.
		for s.pos < s.n {
			b := s.readBuf[s.pos : s.pos+1]
			s.pos++
			if err := s.framing.Feed(b, emit); err != nil {
				return nil, err
			}
			if event != nil {
				return event, nil
			}
		}
		if s.readErr != nil {
			if errors.Is(s.readErr, io.EOF) {
				if err := s.framing.End(); err != nil {
					return nil, err
				}
			}
			return nil, s.readErr
		}
		s.n, s.readErr = s.r.Read(s.readBuf[:])
		s.pos = 0
		if s.n == 0 && s.readErr == nil {
			emptyReads++
			if emptyReads >= 100 {
				return nil, io.ErrNoProgress
			}
		} else {
			emptyReads = 0
		}
	}
}

// sseEvent represents a parsed Server-Sent Event.
// This struct contains only SSE protocol data and the backend it originated from.
type sseEvent struct {
	event, id string
	messages  []jsonrpc.Message
	backend   filterapi.MCPBackendName
}

func (s *sseEvent) writeAndMaybeFlush(w io.Writer) {
	if s.event != "" {
		_, _ = w.Write(sseEventPrefixSpace)
		_, _ = w.Write([]byte(s.event))
		_, _ = w.Write(sseLF)
	}
	if s.id != "" {
		_, _ = w.Write(sseIDPrefixSpace)
		_, _ = w.Write([]byte(s.id))
		_, _ = w.Write(sseLF)
	}
	for _, msg := range s.messages {
		_, _ = w.Write(sseDataPrefixSpace)
		data, _ := jsonrpc.EncodeMessage(msg)
		_, _ = w.Write(data)
		_, _ = w.Write(sseLF)
	}
	_, _ = w.Write(sseLFLF)

	// Flush the response writer to ensure the event is sent immediately.
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// readResponseBody bounds buffered JSON and error bodies independently of transport limits.
func readResponseBody(r io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, sse.DefaultLimit))
	if err != nil {
		return nil, err
	}
	var probe [1]byte
	n, err := io.ReadFull(r, probe[:])
	if n != 0 {
		return nil, sse.ErrOverflow
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return body, nil
}

// readSSEJSONResponse returns the first complete JSON-RPC response. Notifications
// are consumed incrementally. The terminal response need not wait for HTTP EOF.
func readSSEJSONResponse(r io.Reader) ([]byte, error) {
	parser := newSSEEventParser(r, "")
	for {
		event, err := parser.next()
		if err != nil {
			return nil, fmt.Errorf("read MCP SSE response: %w", err)
		}
		for _, msg := range event.messages {
			if response, ok := msg.(*jsonrpc.Response); ok {
				return jsonrpc.EncodeMessage(response)
			}
		}
	}
}

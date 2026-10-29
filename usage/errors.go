// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usage

import (
	"bytes"
	"encoding/json" //nolint:depguard // This public observer package intentionally uses only the standard library.
	"fmt"
)

// StreamError reports a terminal provider failure in a successful HTTP stream.
// Type identifies the event. Provider payloads are deliberately not retained.
type StreamError struct{ Type string }

func (e *StreamError) Error() string { return "terminal provider event: " + e.Type }

// CheckError classifies a complete SSE event or provider JSON object. Call it
// when an event arrives, not at transport EOF. It never retains data. Usage in a
// failure event can still be obtained independently through Decoder.Observe.
// Non-JSON terminal events are recognized from eventType alone. Other non-JSON
// payloads return a decoding error. Empty payloads and [DONE] are not errors.
func CheckError(eventType string, data []byte) error {
	terminal := func(typ string) bool {
		return typ == "error" || typ == "response.failed" || typ == "response.incomplete"
	}
	if terminal(eventType) {
		return &StreamError{Type: eventType}
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
		return nil
	}
	var object struct {
		Type     string          `json:"type"`
		Error    json.RawMessage `json:"error"`
		Response *struct {
			Error  json.RawMessage `json:"error"`
			Status string          `json:"status"`
		} `json:"response"`
	}
	if err := json.Unmarshal(data, &object); err != nil {
		return fmt.Errorf("decode provider event: %w", err)
	}
	if terminal(object.Type) {
		return &StreamError{Type: object.Type}
	}
	nonnull := func(raw json.RawMessage) bool {
		return len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
	}
	if nonnull(object.Error) {
		return &StreamError{Type: "error"}
	}
	if object.Response != nil {
		if nonnull(object.Response.Error) {
			return &StreamError{Type: "error"}
		}
		if object.Response.Status == "failed" || object.Response.Status == "incomplete" {
			return &StreamError{Type: "response." + object.Response.Status}
		}
	}
	return nil
}

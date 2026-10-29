// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package llm

import (
	"bytes"
	"encoding/json" //nolint:depguard // Compact is not exposed by internal/json.
	"errors"

	"github.com/envoyproxy/ai-gateway/internal/translator"
	"github.com/envoyproxy/ai-gateway/sse"
	"github.com/envoyproxy/ai-gateway/usage"
)

// ErrAmazonEventTooLarge indicates an AWS binary frame larger than Input.SSELimit.
var ErrAmazonEventTooLarge = translator.ErrAmazonEventTooLarge

// ErrDataAfterDone indicates provider data after the terminal [DONE] marker.
var ErrDataAfterDone = errors.New("sse data after terminal event")

// ErrMissingTerminal means transport EOF arrived without provider completion.
var ErrMissingTerminal = translator.ErrMissingTerminal

// StreamError is a terminal provider failure received inside a successful HTTP response.
// Body bytes already sent cannot be recalled. Hosts should classify the attempt
// as failed and apply any Result.Body before handling this error.
type StreamError = usage.StreamError

func (a *Attempt) responseSSE(body []byte, end bool) (result Result, err error) {
	merge := func(headers []Header, changed []byte, model string) {
		result.Headers = append(result.Headers, headers...)
		if changed != nil {
			if result.Body == nil {
				result.Body = []byte{}
			}
			result.Body = append(result.Body, changed...)
		}
		if model != "" {
			result.Model = model
		}
	}
	// Preserve each translator's nil-versus-empty body contract while waiting
	// for a complete event. Translating streams must not leak partial input.
	headers, changed, _, model, initErr := a.body(a.responseHeaders, nil, false)
	merge(headers, changed, model)
	if initErr != nil {
		a.done = true
		result.Done = true
		return result, initErr
	}
	emit := func(event sse.Event) error {
		done := bytes.Equal(event.Data, []byte("[DONE]"))
		if a.streamDone && !done && a.strictSSE {
			return ErrDataAfterDone
		}
		var terminalErr error
		if done || json.Valid(event.Data) || a.strictSSE {
			terminalErr = usage.CheckError(event.Type, event.Data)
		}
		invalidUsage := false
		if !done {
			if _, observeErr := a.decoder.Observe(event.Data); observeErr != nil {
				a.usageError = observeErr
				invalidUsage = true
			}
		}

		// Feed each complete event separately. Legacy translators see canonical LF
		// framing and one JSON data line, even when upstream JSON spans data fields.
		translationData := event.Data
		if changed != nil {
			translationData = withLenientUsage(event.Data)
		}
		if invalidUsage && changed != nil {
			translationData = withoutUsage(event.Data)
		}
		var canonical bytes.Buffer
		if event.Type != "" {
			canonical.WriteString("event: ")
			canonical.WriteString(event.Type)
			canonical.WriteByte('\n')
		}
		canonical.WriteString("data: ")
		if done {
			canonical.Write(event.Data)
		} else if compactErr := json.Compact(&canonical, translationData); compactErr != nil {
			if a.strictSSE {
				return compactErr
			}
			// Opaque data is an observation failure, not a forwarding failure.
			canonical.Write(translationData)
		}
		canonical.WriteString("\n\n")
		if !done && !json.Valid(event.Data) {
			// Translation cannot interpret opaque provider data. Keep the event in the
			// output while the observer records why its usage could not be inspected.
			if changed != nil {
				merge(nil, canonical.Bytes(), "")
			}
			return nil
		}
		var translateErr error
		if !invalidUsage || changed != nil {
			headers, translated, _, model, bodyErr := a.body(a.responseHeaders, canonical.Bytes(), false)
			merge(headers, translated, model)
			translateErr = bodyErr
		}
		a.streamDone = a.streamDone || done
		var terminal struct {
			Type       string `json:"type"`
			Candidates []struct {
				FinishReason string `json:"finishReason"`
			} `json:"candidates"`
		}
		if !done {
			if decodeErr := json.Unmarshal(event.Data, &terminal); decodeErr != nil {
				if a.strictSSE {
					return decodeErr
				}
			}
		}
		switch terminal.Type {
		case "message_stop", "response.completed":
			a.streamDone = true
		}
		switch event.Type {
		case "message_stop", "response.completed":
			a.streamDone = true
		}
		a.terminalSeen = a.terminalSeen || a.streamDone
		for _, candidate := range terminal.Candidates {
			if candidate.FinishReason != "" {
				a.terminalSeen = true
			}
		}
		if translateErr != nil {
			return translateErr
		}
		if terminalErr != nil {
			return terminalErr
		}
		return nil
	}
	err = a.reader.Feed(body, emit)
	if err == nil && end {
		err = a.reader.End(emit)
	}
	if err == nil && end && !a.terminalSeen {
		// Audio SSE endpoints use their own terminal event names. Completion checks
		// here apply to the chat/messages/responses providers served by this path.
		if a.requireTerminal && !a.reader.DiscardedIncomplete() {
			err = ErrMissingTerminal
		}
	}
	if err == nil && end {
		headers, changed, _, model, translateErr := a.body(a.responseHeaders, nil, true)
		merge(headers, changed, model)
		err = translateErr
	}
	a.done = end || err != nil
	a.usage = a.decoder.Snapshot()
	result.Usage, result.Done, result.UsageError = a.usage, a.done, a.usageError
	var providerErr *StreamError
	if errors.As(err, &providerErr) && result.Body == nil {
		result.Body = bytes.Clone(body)
	}
	return result, err
}

// withoutUsage lets content translation continue after a bad usage observation.
// The original event bytes remain untouched for passthrough responses.
func withoutUsage(data []byte) []byte {
	var object map[string]json.RawMessage
	if json.Unmarshal(data, &object) != nil {
		return data
	}
	delete(object, "usage")
	delete(object, "usageMetadata")
	for _, key := range []string{"message", "response", "metadata"} {
		if raw, ok := object[key]; ok {
			object[key] = withoutUsage(raw)
		}
	}
	clean, err := json.Marshal(object)
	if err != nil {
		return data
	}
	return clean
}

// withLenientUsage removes nonnumeric usage fields before legacy typed
// translators inspect them. The observer still receives the original payload.
func withLenientUsage(data []byte) []byte {
	var object map[string]json.RawMessage
	if json.Unmarshal(data, &object) != nil {
		return data
	}
	for _, key := range []string{"usage", "usageMetadata"} {
		if raw, ok := object[key]; ok {
			var fields map[string]json.RawMessage
			if json.Unmarshal(raw, &fields) != nil {
				delete(object, key)
				continue
			}
			cleanUsageFields(fields)
			clean, err := json.Marshal(fields)
			if err != nil {
				return data
			}
			object[key] = clean
		}
	}
	for _, key := range []string{"message", "response", "metadata"} {
		if raw, ok := object[key]; ok {
			object[key] = withLenientUsage(raw)
		}
	}
	clean, err := json.Marshal(object)
	if err != nil {
		return data
	}
	return clean
}

func cleanUsageFields(fields map[string]json.RawMessage) {
	for key, raw := range fields {
		var nested map[string]json.RawMessage
		if len(raw) > 0 && raw[0] == '{' && json.Unmarshal(raw, &nested) == nil {
			cleanUsageFields(nested)
			if clean, err := json.Marshal(nested); err == nil {
				fields[key] = clean
			}
			continue
		}
		var count *uint32
		if json.Unmarshal(raw, &count) != nil {
			delete(fields, key)
		}
	}
}

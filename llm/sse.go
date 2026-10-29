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
var ErrMissingTerminal = errors.New("sse stream ended without a terminal event")

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
	err = a.reader.Feed(body, func(event sse.Event) error {
		if a.streamDone {
			return ErrDataAfterDone
		}
		done := bytes.Equal(event.Data, []byte("[DONE]"))
		terminalErr := usage.CheckError(event.Type, event.Data)
		if !done {
			if _, observeErr := a.decoder.Observe(event.Data); observeErr != nil {
				if terminalErr != nil {
					return terminalErr
				}
				return observeErr
			}
		}

		// Feed each complete event separately. Legacy translators see canonical LF
		// framing and one JSON data line, even when upstream JSON spans data fields.
		var canonical bytes.Buffer
		if event.Type != "" {
			canonical.WriteString("event: ")
			canonical.WriteString(event.Type)
			canonical.WriteByte('\n')
		}
		canonical.WriteString("data: ")
		if done {
			canonical.Write(event.Data)
		} else if compactErr := json.Compact(&canonical, event.Data); compactErr != nil {
			return compactErr
		}
		canonical.WriteString("\n\n")
		headers, changed, _, model, translateErr := a.body(a.responseHeaders, canonical.Bytes(), false)
		merge(headers, changed, model)
		a.streamDone = done
		var terminal struct {
			Type       string `json:"type"`
			Candidates []struct {
				FinishReason string `json:"finishReason"`
			} `json:"candidates"`
		}
		if !done {
			if decodeErr := json.Unmarshal(event.Data, &terminal); decodeErr != nil {
				return decodeErr
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
	})
	if err == nil && end {
		err = a.reader.End()
	}
	if err == nil && end && !a.terminalSeen {
		// Audio SSE endpoints use their own terminal event names. Completion checks
		// here apply to the chat/messages/responses providers served by this path.
		if a.requireTerminal {
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
	result.Usage, result.Done = a.usage, a.done
	return result, err
}

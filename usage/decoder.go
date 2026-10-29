// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usage

import (
	"bytes"
	"encoding/json" //nolint:depguard // This public observer package intentionally uses only the standard library.
	"fmt"
	"math"
	"strings"
)

// Decoder accumulates cumulative provider snapshots. Each present count replaces
// its previous value, including zero. Missing or null counts leave it unchanged.
// It retains only counters, never response payloads, and is not concurrency safe.
// Schema accepts gateway names such as OpenAI, Anthropic, AWSBedrock,
// AWSAnthropic, GCPAnthropic, GCPVertexAI and Gemini.
// Bedrock input must be decoded event JSON, not the binary event-stream envelope.
type Decoder struct {
	Schema      string
	snapshot    TokenUsage
	rawInput    uint32
	rawInputSet bool
}

// Snapshot returns the most recently observed usage.
func (d *Decoder) Snapshot() TokenUsage { return d.snapshot }

type counts struct {
	Prompt             *uint32  `json:"prompt_tokens"`
	Completion         *uint32  `json:"completion_tokens"`
	Input              *uint32  `json:"input_tokens"`
	Output             *uint32  `json:"output_tokens"`
	Total              *uint32  `json:"total_tokens"`
	CacheRead          *uint32  `json:"cache_read_input_tokens"`
	CacheCreate        *uint32  `json:"cache_creation_input_tokens"`
	PromptDetails      *details `json:"prompt_tokens_details"`
	CompletionDetails  *details `json:"completion_tokens_details"`
	InputDetails       *details `json:"input_tokens_details"`
	OutputDetails      *details `json:"output_tokens_details"`
	BedrockInput       *uint32  `json:"inputTokens"`
	BedrockOutput      *uint32  `json:"outputTokens"`
	BedrockTotal       *uint32  `json:"totalTokens"`
	BedrockCacheRead   *uint32  `json:"cacheReadInputTokens"`
	BedrockCacheCreate *uint32  `json:"cacheWriteInputTokens"`
}

type details struct {
	Cached      *uint32 `json:"cached_tokens"`
	CacheWrite  *uint32 `json:"cache_write_tokens"`
	CacheCreate *uint32 `json:"cache_creation_input_tokens"`
	Reasoning   *uint32 `json:"reasoning_tokens"`
	Thinking    *uint32 `json:"thinking_tokens"`
}

type envelope struct {
	Usage    *counts         `json:"usage"`
	Message  *envelope       `json:"message"`
	Response *envelope       `json:"response"`
	Metadata json.RawMessage `json:"metadata"`
	Vertex   *struct {
		Input     *uint32 `json:"promptTokenCount"`
		Output    *uint32 `json:"candidatesTokenCount"`
		Total     *uint32 `json:"totalTokenCount"`
		Cached    *uint32 `json:"cachedContentTokenCount"`
		Reasoning *uint32 `json:"thoughtsTokenCount"`
	} `json:"usageMetadata"`
}

// Observe consumes one complete provider JSON object. Invalid JSON, negative
// counts, values larger than uint32 and normalized-count overflow return errors
// without changing the previous snapshot. Unknown fields are ignored.
func (d *Decoder) Observe(data []byte) (TokenUsage, error) {
	var e envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return d.snapshot, fmt.Errorf("decode provider usage: %w", err)
	}
	next := *d
	if err := next.observe(e); err != nil {
		return d.snapshot, err
	}
	*d = next
	return d.snapshot, nil
}

func set(value *uint32, setter func(uint32)) {
	if value != nil {
		setter(*value)
	}
}

func (d *Decoder) observe(e envelope) error {
	c := e.Usage
	if e.Response != nil {
		c = e.Response.Usage
	}
	if e.Message != nil {
		c = e.Message.Usage
	}
	if d.Schema == "AWSBedrock" && len(e.Metadata) != 0 {
		var metadata struct {
			Usage json.RawMessage `json:"usage"`
		}
		if err := json.Unmarshal(e.Metadata, &metadata); err != nil {
			return err
		}
		if raw := bytes.TrimSpace(metadata.Usage); len(raw) > 0 && raw[0] == '{' {
			var bedrock counts
			if err := json.Unmarshal(raw, &bedrock); err != nil {
				return err
			}
			c = &bedrock
		}
	}
	if e.Vertex != nil {
		v := e.Vertex
		set(v.Input, d.snapshot.SetInputTokens)
		if v.Output != nil || v.Reasoning != nil {
			var output uint64
			if v.Output != nil {
				output += uint64(*v.Output)
			}
			if v.Reasoning != nil {
				output += uint64(*v.Reasoning)
			}
			if output > math.MaxUint32 {
				return fmt.Errorf("output token count exceeds uint32")
			}
			d.snapshot.SetOutputTokens(uint32(output))
		}
		set(v.Total, d.snapshot.SetTotalTokens)
		set(v.Cached, d.snapshot.SetCachedInputTokens)
		set(v.Reasoning, d.snapshot.SetReasoningTokens)
		return nil
	}
	if c == nil {
		return nil
	}
	input, output, total := c.Input, c.Output, c.Total
	if c.Prompt != nil {
		input = c.Prompt
	}
	if c.Completion != nil {
		output = c.Completion
	}
	if c.BedrockInput != nil {
		input = c.BedrockInput
	}
	if c.BedrockOutput != nil {
		output = c.BedrockOutput
	}
	if c.BedrockTotal != nil {
		total = c.BedrockTotal
	}
	set(c.CacheRead, d.snapshot.SetCachedInputTokens)
	set(c.CacheCreate, d.snapshot.SetCacheCreationInputTokens)
	set(c.BedrockCacheRead, d.snapshot.SetCachedInputTokens)
	set(c.BedrockCacheCreate, d.snapshot.SetCacheCreationInputTokens)
	for _, detail := range []*details{c.PromptDetails, c.InputDetails, c.CompletionDetails, c.OutputDetails} {
		if detail == nil {
			continue
		}
		set(detail.Cached, d.snapshot.SetCachedInputTokens)
		write := detail.CacheWrite
		if detail.CacheCreate != nil && (write == nil || *detail.CacheCreate > *write) {
			write = detail.CacheCreate
		}
		set(write, d.snapshot.SetCacheCreationInputTokens)
		set(detail.Reasoning, d.snapshot.SetReasoningTokens)
		set(detail.Thinking, d.snapshot.SetReasoningTokens)
	}
	explicitCache := strings.Contains(d.Schema, "Anthropic") || d.Schema == "AWSBedrock"
	if explicitCache {
		if input != nil {
			d.rawInput, d.rawInputSet = *input, true
		}
		if d.rawInputSet {
			cached, _ := d.snapshot.CachedInputTokens()
			created, _ := d.snapshot.CacheCreationInputTokens()
			normalized := uint64(d.rawInput) + uint64(cached) + uint64(created)
			if normalized > math.MaxUint32 {
				return fmt.Errorf("normalized input token count exceeds uint32")
			}
			d.snapshot.SetInputTokens(uint32(normalized))
		}
	} else {
		set(input, d.snapshot.SetInputTokens)
	}
	set(output, d.snapshot.SetOutputTokens)
	set(total, d.snapshot.SetTotalTokens)
	if explicitCache {
		in, inSet := d.snapshot.InputTokens()
		out, outSet := d.snapshot.OutputTokens()
		if inSet && outSet {
			sum := uint64(in) + uint64(out)
			if sum > math.MaxUint32 {
				return fmt.Errorf("normalized total token count exceeds uint32")
			}
			d.snapshot.SetTotalTokens(uint32(sum))
		}
	}
	return nil
}

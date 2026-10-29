// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

// Package usage decodes provider token snapshots without transport or telemetry dependencies.
package usage

// TokenUsage represents the token usage reported usually by the backend API in the response body.
//
// Fields are not exported to control the optionality of each field via the accompanying boolean flags.
type TokenUsage struct {
	// InputTokens is the number of tokens consumed from the input.
	inputTokens uint32
	// OutputTokens is the number of tokens consumed from the output.
	outputTokens uint32
	// TotalTokens is the total number of tokens consumed.
	totalTokens uint32
	// CachedInputTokens is the total number of tokens read from cache.
	cachedInputTokens uint32
	// CacheCreationInputTokens is the total number of tokens written to cache.
	cacheCreationInputTokens uint32
	// ReasoningTokens is the number of reasoning tokens consumed.
	reasoningTokens uint32

	inputTokenSet, outputTokenSet, totalTokenSet, cachedInputTokenSet, cacheCreationInputTokenSet, reasoningTokenSet bool
}

// InputTokens returns the number of input tokens and whether it was set.
func (u *TokenUsage) InputTokens() (uint32, bool) {
	return u.inputTokens, u.inputTokenSet
}

// OutputTokens returns the number of output tokens and whether it was set.
func (u *TokenUsage) OutputTokens() (uint32, bool) {
	return u.outputTokens, u.outputTokenSet
}

// TotalTokens returns the number of total tokens and whether it was set.
func (u *TokenUsage) TotalTokens() (uint32, bool) {
	return u.totalTokens, u.totalTokenSet
}

// CachedInputTokens returns the number of cached input tokens and whether it was set.
func (u *TokenUsage) CachedInputTokens() (uint32, bool) {
	return u.cachedInputTokens, u.cachedInputTokenSet
}

// CacheCreationInputTokens returns the number of cache creation input tokens and whether it was set.
func (u *TokenUsage) CacheCreationInputTokens() (uint32, bool) {
	return u.cacheCreationInputTokens, u.cacheCreationInputTokenSet
}

// SetInputTokens sets the number of input tokens and marks the field as set.
func (u *TokenUsage) SetInputTokens(tokens uint32) {
	u.inputTokens = tokens
	u.inputTokenSet = true
}

// SetOutputTokens sets the number of output tokens and marks the field as set.
func (u *TokenUsage) SetOutputTokens(tokens uint32) {
	u.outputTokens = tokens
	u.outputTokenSet = true
}

// SetTotalTokens sets the number of total tokens and marks the field as set.
func (u *TokenUsage) SetTotalTokens(tokens uint32) {
	u.totalTokens = tokens
	u.totalTokenSet = true
}

// SetCachedInputTokens sets the number of cached input tokens and marks the field as set.
func (u *TokenUsage) SetCachedInputTokens(tokens uint32) {
	u.cachedInputTokens = tokens
	u.cachedInputTokenSet = true
}

// SetCacheCreationInputTokens sets the number of cache creation input tokens and marks the field as set.
func (u *TokenUsage) SetCacheCreationInputTokens(tokens uint32) {
	u.cacheCreationInputTokens = tokens
	u.cacheCreationInputTokenSet = true
}

// ReasoningTokens returns the number of reasoning tokens and whether it was set.
func (u *TokenUsage) ReasoningTokens() (uint32, bool) {
	return u.reasoningTokens, u.reasoningTokenSet
}

// SetReasoningTokens sets the number of reasoning tokens and marks the field as set.
func (u *TokenUsage) SetReasoningTokens(tokens uint32) {
	u.reasoningTokens = tokens
	u.reasoningTokenSet = true
}

// AddInputTokens increments the recorded input tokens and marks the field as set.
func (u *TokenUsage) AddInputTokens(tokens uint32) {
	u.inputTokenSet = true
	u.inputTokens += tokens
}

// AddOutputTokens increments the recorded output tokens and marks the field as set.
func (u *TokenUsage) AddOutputTokens(tokens uint32) {
	u.outputTokenSet = true
	u.outputTokens += tokens
}

// AddCachedInputTokens increments the recorded cached input tokens and marks the field as set.
func (u *TokenUsage) AddCachedInputTokens(tokens uint32) {
	u.cachedInputTokenSet = true
	u.cachedInputTokens += tokens
}

// AddCacheCreationInputTokens increments the recorded cache creation input tokens and marks the field as set.
func (u *TokenUsage) AddCacheCreationInputTokens(tokens uint32) {
	u.cacheCreationInputTokenSet = true
	u.cacheCreationInputTokens += tokens
}

// AddReasoningTokens increments the recorded reasoning tokens and marks the field as set.
func (u *TokenUsage) AddReasoningTokens(tokens uint32) {
	u.reasoningTokenSet = true
	u.reasoningTokens += tokens
}

// IsZero reports whether no token usage field has been set.
func (u *TokenUsage) IsZero() bool {
	return !u.inputTokenSet && !u.outputTokenSet && !u.totalTokenSet &&
		!u.cachedInputTokenSet && !u.cacheCreationInputTokenSet && !u.reasoningTokenSet
}

// Override updates the TokenUsage fields with values from another TokenUsage instance.
// Only fields that are marked as set in the other instance will override the current values.
func (u *TokenUsage) Override(other TokenUsage) {
	if other.inputTokenSet {
		u.inputTokens = other.inputTokens
		u.inputTokenSet = true
	}
	if other.outputTokenSet {
		u.outputTokens = other.outputTokens
		u.outputTokenSet = true
	}
	if other.totalTokenSet {
		u.totalTokens = other.totalTokens
		u.totalTokenSet = true
	}
	if other.cachedInputTokenSet {
		u.cachedInputTokens = other.cachedInputTokens
		u.cachedInputTokenSet = true
	}
	if other.cacheCreationInputTokenSet {
		u.cacheCreationInputTokens = other.cacheCreationInputTokens
		u.cacheCreationInputTokenSet = true
	}
	if other.reasoningTokenSet {
		u.reasoningTokens = other.reasoningTokens
		u.reasoningTokenSet = true
	}
}

// ExtractTokenUsageFromExplicitCaching extracts the correct token usage from upstream Anthropic or AWS Bedrock token usage response.
// The total input tokens is the summation of:
// input_tokens + cache_creation_input_tokens + cache_read_input_tokens
// This is to unify the usage response returned by envoy ai gateway for both explicit and implicit caching.
//
// This function works for both streaming and non-streaming responses by accepting
// the common usage fields that exist from anthropic or AWS bedrock usage structures.
func ExtractTokenUsageFromExplicitCaching(inputTokens, outputTokens int64, cacheReadTokens, cacheCreationTokens *int64) TokenUsage {
	var usage TokenUsage
	totalInputTokens := inputTokens
	if cacheCreationTokens != nil {
		totalInputTokens += *cacheCreationTokens
		usage.SetCacheCreationInputTokens(uint32(*cacheCreationTokens)) //nolint:gosec
	}
	if cacheReadTokens != nil {
		totalInputTokens += *cacheReadTokens
		usage.SetCachedInputTokens(uint32(*cacheReadTokens)) //nolint:gosec
	}
	usage.SetInputTokens(uint32(totalInputTokens))                //nolint:gosec
	usage.SetOutputTokens(uint32(outputTokens))                   //nolint:gosec
	usage.SetTotalTokens(uint32(totalInputTokens + outputTokens)) //nolint:gosec
	return usage
}

// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

// Package ptr provides small pointer helpers without controller dependencies.
package ptr

// To returns a pointer to a copy of v.
func To[T any](v T) *T { return &v }

// Deref returns the referenced value or fallback for a nil pointer.
func Deref[T any](v *T, fallback T) T {
	if v == nil {
		return fallback
	}
	return *v
}

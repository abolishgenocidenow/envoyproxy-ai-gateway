// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

// Package execution defines host-neutral inputs shared by execution adapters.
package execution

import (
	"context"
	"maps"
)

// Metadata is trusted host input, separate from client headers. Hosts must verify
// identity before setting Subject or VerifiedJWT. Attributes are opaque strings;
// they do not grant authorization and must not contain secrets. Backend identifies
// a logical selection, not an Envoy cluster or a routable network address.
type Metadata struct {
	Route       string
	Backend     string
	Subject     string
	VerifiedJWT bool
	Attributes  map[string]string
}

// Clone returns an independently owned snapshot.
func (m Metadata) Clone() Metadata { m.Attributes = maps.Clone(m.Attributes); return m }

type metadataKey struct{}

// WithMetadata captures metadata for propagation through host callbacks.
func WithMetadata(ctx context.Context, metadata Metadata) context.Context {
	return context.WithValue(ctx, metadataKey{}, metadata.Clone())
}

// FromContext returns a copy, or empty metadata when no host input was supplied.
func FromContext(ctx context.Context) Metadata {
	metadata, _ := ctx.Value(metadataKey{}).(Metadata)
	return metadata.Clone()
}

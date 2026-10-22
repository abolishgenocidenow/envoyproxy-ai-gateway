// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// Package controller defines application-owned controller integration hooks.
package controller

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// BackendAuthorization relates a serialized runtime backend to its policy and
// route. The application can resolve the policy's credential references without
// depending on internal naming functions or inferring identity from key bytes.
type BackendAuthorization struct {
	RuntimeBackend string
	Route          corev1.ObjectReference
	Policy         corev1.ObjectReference
}

// ConfigPublication is a complete document built by the elected controller.
// JSON is an isolated copy and may contain credentials. It must not be logged.
// Cached inputs are not authoritative freshness grants. Gateway names can be
// reused; applications must validate UID and assignment before granting access.
type ConfigPublication struct {
	// Sources records object UID/resourceVersion pairs observed while building.
	// It is evidence to validate against the authoritative API, not a grant. List
	// absence and membership semantics still require application-specific checks.
	Sources        []corev1.ObjectReference
	Authorizations []BackendAuthorization
	Gateway        types.NamespacedName
	JSON           []byte
}

// ConfigPublisher runs before legacy bundle publication. Returning an error
// retries reconciliation without updating the bundle. It must be idempotent,
// honor context cancellation and bound any retained state. It does not activate
// routes or establish consumer readiness; applications coordinate those separately.
type ConfigPublisher func(context.Context, ConfigPublication) error

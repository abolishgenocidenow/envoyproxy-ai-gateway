// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// Package controller defines application-owned controller integration hooks.
package controller

import (
	"context"
	"k8s.io/apimachinery/pkg/types"
)

// ConfigPublication is a complete document built by the elected controller.
// JSON is an isolated copy and may contain credentials. It must not be logged.
// The cached inputs are not an authoritative credential freshness grant.
// Gateway names can be reused; applications must resolve and validate the UID
// against their assignment and authoritative source before granting access.
type ConfigPublication struct {
	Gateway types.NamespacedName
	JSON    []byte
}

// ConfigPublisher runs before legacy bundle publication. Returning an error
// retries reconciliation without updating the bundle. It must be idempotent,
// honor context cancellation and bound any retained state. It does not activate
// routes or establish consumer readiness; applications coordinate those separately.
type ConfigPublisher func(context.Context, ConfigPublication) error

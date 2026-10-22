// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
package controller

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// GatewayPod is a pod after standard extproc injection. The callback owns its
// admission-local copy. ContainerName identifies the injected container in
// either Containers or InitContainers; applications need not import internals.
type GatewayPod struct {
	Gateway       types.NamespacedName
	Pod           *corev1.Pod
	ContainerName string
}

// GatewayPodCustomizer supplies application-owned delivery mounts and flags.
// Revision is pure, stable and contains no secrets. An empty revision means no
// customization for this Gateway. A changed revision participates in the usual
// workload drift hash. Customize must be idempotent, honor context cancellation
// and preserve unrelated pod fields. Errors reject admission without persistence.
type GatewayPodCustomizer interface {
	Revision(types.NamespacedName) string
	Customize(context.Context, GatewayPod) error
}

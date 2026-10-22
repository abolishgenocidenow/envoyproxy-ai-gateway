// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
package controller

import (
	"context"
	"errors"
	"testing"

	controllerapi "github.com/envoyproxy/ai-gateway/api/controller"
	aigv1 "github.com/envoyproxy/ai-gateway/api/v1beta1"
	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

type testPodCustomizer struct {
	gateway   types.NamespacedName
	revision  string
	customize func(context.Context, controllerapi.GatewayPod) error
}

func (c *testPodCustomizer) Revision(gateway types.NamespacedName) string {
	if gateway == c.gateway {
		return c.revision
	}
	return ""
}
func (c *testPodCustomizer) Customize(ctx context.Context, pod controllerapi.GatewayPod) error {
	return c.customize(ctx, pod)
}

func TestApplicationPodRevisionPreservesUnassignedHash(t *testing.T) {
	customization := &testPodCustomizer{gateway: types.NamespacedName{Namespace: "ns", Name: "gw"}, revision: "one"}
	plain := newExtProcBuilder(&Options{}, false, ctrl.Log)
	extended := newExtProcBuilder(&Options{GatewayPodCustomizer: customization}, false, ctrl.Log)
	input := extProcContainerInput{gateway: types.NamespacedName{Namespace: "ns", Name: "unassigned"}}
	require.Equal(t, plain.extProcContainerHash(input), extended.extProcContainerHash(input))
	input.gateway = customization.gateway
	one := extended.extProcContainerHash(input)
	require.NotEqual(t, plain.extProcContainerHash(input), one)
	customization.revision = "two"
	require.NotEqual(t, one, extended.extProcContainerHash(input))
}

func TestApplicationPodCustomizationFollowsStandardInjection(t *testing.T) {
	client := requireNewFakeClientWithIndexes(t)
	kube := kubefake.NewClientset()
	mutator := newTestGatewayMutator(client, kube, nil, nil, nil, nil, "", "", "", false)
	gateway := types.NamespacedName{Namespace: "ns", Name: "gw"}
	require.NoError(t, client.Create(t.Context(), &aigv1.AIGatewayRoute{ObjectMeta: metav1.ObjectMeta{Namespace: gateway.Namespace, Name: "route"}, Spec: aigv1.AIGatewayRouteSpec{ParentRefs: []gwapiv1.ParentReference{{Name: gwapiv1.ObjectName(gateway.Name), Kind: ptr.To(gwapiv1.Kind("Gateway")), Group: ptr.To(gwapiv1.Group("gateway.networking.k8s.io"))}}, Rules: []aigv1.AIGatewayRouteRule{{BackendRefs: []aigv1.AIGatewayRouteRuleBackendRef{{Name: "backend"}}}}}}))
	index, err := filterapi.MarshalConfigBundleIndex(&filterapi.ConfigBundleIndex{Checksum: "abc", Parts: []filterapi.ConfigBundlePart{{Name: "part", Path: "parts/000"}}})
	require.NoError(t, err)
	_, err = kube.CoreV1().Secrets("ns").Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: FilterConfigBundleIndexSecretName("gw", "ns")}, Data: map[string][]byte{FilterConfigBundleIndexKey: index}}, metav1.CreateOptions{})
	require.NoError(t, err)
	calls := 0
	customization := &testPodCustomizer{gateway: gateway, revision: "one", customize: func(_ context.Context, p controllerapi.GatewayPod) error {
		calls++
		require.Equal(t, gateway, p.Gateway)
		require.Len(t, p.Pod.Spec.Containers, 2)
		require.Equal(t, p.ContainerName, p.Pod.Spec.Containers[1].Name)
		require.Contains(t, p.Pod.Spec.Containers[1].Args, "-configBundlePath")
		p.Pod.Annotations = map[string]string{"application-config": "one"}
		return nil
	}}
	mutator.gatewayPodCustomizer = customization
	fresh := func() *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "envoy"}}}}
	}
	pod := fresh()
	require.NoError(t, mutator.mutatePod(t.Context(), pod, "gw", "ns"))
	require.Equal(t, "one", pod.Annotations["application-config"])
	require.Equal(t, 1, calls)
	sentinel := errors.New("application rejected pod")
	customization.customize = func(context.Context, controllerapi.GatewayPod) error { return sentinel }
	require.ErrorIs(t, mutator.mutatePod(t.Context(), fresh(), "gw", "ns"), sentinel)
	customization.revision = ""
	require.NoError(t, mutator.mutatePod(t.Context(), fresh(), "gw", "ns"))
}

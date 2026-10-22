// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	controllerapi "github.com/envoyproxy/ai-gateway/api/controller"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	fakekube "k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"
)

func TestPublicationFailureDoesNotWriteLegacyBundle(t *testing.T) {
	cli := requireNewFakeClientWithIndexes(t)
	kube := fakekube.NewClientset()
	sentinel := errors.New("publication rejected")
	calls := 0
	opts := &Options{PublishConfig: func(ctx context.Context, p controllerapi.ConfigPublication) error {
		calls++
		require.Equal(t, types.NamespacedName{Namespace: "ns", Name: "gw"}, p.Gateway)
		require.True(t, json.Valid(p.JSON))
		return sentinel
	}}
	c := NewGatewayController(cli, kube, ctrl.Log, "system", true, nil, opts, false)
	_, err := c.reconcileFilterConfigSecret(t.Context(), "gw", "ns", "system", nil, nil, "first", nil, nil)
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, 1, calls)
	secrets, err := kube.CoreV1().Secrets("system").List(t.Context(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, secrets.Items)
	// The callback owns its isolated JSON copy: modifying it must not corrupt
	// the independently encoded legacy bundle.
	c.publishConfig = func(_ context.Context, p controllerapi.ConfigPublication) error {
		for i := range p.JSON {
			p.JSON[i] = '!'
		}
		return nil
	}
	_, err = c.reconcileFilterConfigSecret(t.Context(), "gw", "ns", "system", nil, nil, "second", nil, nil)
	require.NoError(t, err)
	index, err := kube.CoreV1().Secrets("system").Get(t.Context(), FilterConfigBundleIndexSecretName("gw", "ns"), metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, index.StringData[FilterConfigBundleIndexKey])
	c.publishConfig = func(_ context.Context, _ controllerapi.ConfigPublication) error { return sentinel }
	_, err = c.reconcileFilterConfigSecret(t.Context(), "gw", "ns", "system", nil, nil, "third", nil, nil)
	require.ErrorIs(t, err, sentinel)
	after, err := kube.CoreV1().Secrets("system").Get(t.Context(), index.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, index.Data, after.Data)
	require.Equal(t, index.StringData, after.StringData)
}

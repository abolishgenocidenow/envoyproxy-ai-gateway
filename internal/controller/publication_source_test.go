// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
package controller

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPublicationSourceObservationsDetectMixedVersions(t *testing.T) {
	base := fake.NewClientBuilder().WithScheme(Scheme).WithObjects(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "one", UID: "uid-one"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "two", UID: "uid-two"}},
	).Build()
	reader := publicationClient{base}
	ctx, sources := publicationContext(t.Context())
	first := &corev1.Secret{}
	require.NoError(t, reader.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "one"}, first))
	require.NoError(t, reader.List(ctx, &corev1.SecretList{}, client.InNamespace("ns")))
	refs, err := sources.snapshot()
	require.NoError(t, err)
	require.Len(t, refs, 2)
	require.Equal(t, "uid-one", string(refs[0].UID))
	require.Equal(t, first.ResourceVersion, refs[0].ResourceVersion)
	require.Equal(t, "v1", refs[0].APIVersion)
	require.Equal(t, "Secret", refs[0].Kind)
	first.Data = map[string][]byte{"key": []byte("updated")}
	require.NoError(t, base.Update(ctx, first))
	require.NoError(t, reader.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "one"}, first))
	_, err = sources.snapshot()
	require.ErrorContains(t, err, "source changed")
	// A separate reconciliation observes the final state independently.
	next, nextSources := publicationContext(context.Background())
	require.NoError(t, reader.Get(next, client.ObjectKey{Namespace: "ns", Name: "one"}, first))
	refs, err = nextSources.snapshot()
	require.NoError(t, err)
	require.Len(t, refs, 1)
	require.Equal(t, first.ResourceVersion, refs[0].ResourceVersion)
}

func TestPublicationSourcesBoundedAndCancellation(t *testing.T) {
	reader := publicationClient{fake.NewClientBuilder().WithScheme(Scheme).Build()}
	ctx, sources := publicationContext(t.Context())
	for i := 0; i < maxPublicationSources; i++ {
		require.NoError(t, reader.record(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: strconv.Itoa(i), UID: "uid", ResourceVersion: "1"}}))
	}
	require.NoError(t, reader.record(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "0", UID: "uid", ResourceVersion: "1"}}))
	require.ErrorContains(t, reader.record(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "overflow"}}), "limit exceeded")
	require.Len(t, sources.objects, maxPublicationSources)
	_, err := sources.snapshot()
	require.ErrorContains(t, err, "limit exceeded")
	canceled, cancel := context.WithCancel(t.Context())
	canceled, fresh := publicationContext(canceled)
	cancel()
	require.ErrorIs(t, reader.record(canceled, &corev1.Secret{}), context.Canceled)
	require.Empty(t, fresh.objects)
}

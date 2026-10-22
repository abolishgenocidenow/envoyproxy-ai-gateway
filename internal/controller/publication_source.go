// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
package controller

import (
	"context"
	"errors"
	"sort"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

const maxPublicationSources = 100000

type publicationContextKey struct{}
type publicationSources struct {
	mu       sync.Mutex
	objects  map[string]corev1.ObjectReference
	changed  bool
	overflow bool
}

func publicationContext(ctx context.Context) (context.Context, *publicationSources) {
	if sources, ok := ctx.Value(publicationContextKey{}).(*publicationSources); ok {
		return ctx, sources
	}
	sources := &publicationSources{objects: map[string]corev1.ObjectReference{}}
	return context.WithValue(ctx, publicationContextKey{}, sources), sources
}

type publicationClient struct{ client.Client }

func (c publicationClient) record(ctx context.Context, obj client.Object) error {
	sources, ok := ctx.Value(publicationContextKey{}).(*publicationSources)
	if !ok {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	gvk, err := apiutil.GVKForObject(obj, c.Scheme())
	if err != nil {
		return err
	}
	// Workload health objects are not configuration inputs.
	// Their lifetime/readiness is coordinated separately by the embedding app.
	switch gvk.Kind {
	case "Pod", "Deployment", "DaemonSet", "ReplicaSet":
		return nil
	}
	ref := corev1.ObjectReference{APIVersion: gvk.GroupVersion().String(), Kind: gvk.Kind, Namespace: obj.GetNamespace(), Name: obj.GetName(), UID: obj.GetUID(), ResourceVersion: obj.GetResourceVersion()}
	key := ref.APIVersion + "/" + ref.Kind + "/" + ref.Namespace + "/" + ref.Name
	sources.mu.Lock()
	defer sources.mu.Unlock()
	if previous, ok := sources.objects[key]; ok {
		if previous != ref {
			sources.changed = true
		}
	} else if len(sources.objects) >= maxPublicationSources {
		sources.overflow = true
		return errors.New("configuration source observation limit exceeded")
	}
	sources.objects[key] = ref
	return nil
}

func (c publicationClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := c.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	return c.record(ctx, obj)
}
func (c publicationClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := c.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if _, ok := ctx.Value(publicationContextKey{}).(*publicationSources); !ok {
		return nil
	}
	objects, err := meta.ExtractList(list)
	if err != nil {
		return err
	}
	for _, obj := range objects {
		if item, ok := obj.(client.Object); ok {
			if err := c.record(ctx, item); err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *publicationSources) snapshot() ([]corev1.ObjectReference, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.overflow {
		return nil, errors.New("configuration source observation limit exceeded")
	}
	if s.changed {
		return nil, errors.New("configuration source changed during reconciliation")
	}
	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	refs := make([]corev1.ObjectReference, 0, len(keys))
	for _, key := range keys {
		refs = append(refs, s.objects[key])
	}
	return refs, nil
}

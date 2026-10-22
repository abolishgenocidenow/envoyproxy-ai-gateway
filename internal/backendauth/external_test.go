// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0

package backendauth

import (
	"context"
	"errors"
	"testing"

	runtimeapi "github.com/envoyproxy/ai-gateway/api/runtime"
	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/stretchr/testify/require"
)

func TestExternalCredentialEveryAttemptAndNoFallback(t *testing.T) {
	calls := 0
	denied := false
	resolve := func(_ context.Context, a runtimeapi.CredentialAttempt) ([]runtimeapi.CredentialHeader, error) {
		calls++
		require.Equal(t, "trusted-reference", a.Reference)
		require.Equal(t, "spoofed", a.Headers["externalCredential"])
		a.Headers["private"] = "changed"
		a.Body[0] = 'x'
		if denied {
			return nil, errors.New("secret must not appear in returned error")
		}
		return []runtimeapi.CredentialHeader{{Name: "Authorization", Value: "Bearer current"}}, nil
	}
	config := &filterapi.BackendAuth{ExternalCredential: "trusted-reference"}
	_, err := NewHandler(t.Context(), config)
	require.Error(t, err)
	handler, err := NewHandlerWithResolver(t.Context(), config, resolve)
	require.NoError(t, err)
	headers := map[string]string{"externalCredential": "spoofed"}
	body := []byte("body")
	for range 2 {
		_, err = handler.Do(t.Context(), headers, body)
		require.NoError(t, err)
		require.Equal(t, "body", string(body))
		require.NotContains(t, headers, "private")
	}
	denied = true
	_, err = handler.Do(t.Context(), headers, body)
	require.ErrorIs(t, err, ErrCredentialMissing)
	require.NotContains(t, err.Error(), "secret")
	require.Equal(t, 3, calls)
	config.APIKey = &filterapi.APIKeyAuth{Key: "fallback"}
	_, err = NewHandlerWithResolver(t.Context(), config, resolve)
	require.Error(t, err)
}

func TestExternalCredentialInvalidMutationIsAtomic(t *testing.T) {
	for _, name := range []string{"", ":authority", "bad name", "bad\rname"} {
		t.Run(name, func(t *testing.T) {
			handler, err := NewHandlerWithResolver(t.Context(), &filterapi.BackendAuth{ExternalCredential: "ref"}, func(context.Context, runtimeapi.CredentialAttempt) ([]runtimeapi.CredentialHeader, error) {
				return []runtimeapi.CredentialHeader{{Name: "Authorization", Value: "new"}, {Name: name, Value: "invalid"}}, nil
			})
			require.NoError(t, err)
			headers := map[string]string{"Authorization": "unchanged"}
			_, err = handler.Do(t.Context(), headers, nil)
			require.ErrorIs(t, err, ErrCredentialMissing)
			require.Equal(t, map[string]string{"Authorization": "unchanged"}, headers)
		})
	}
}

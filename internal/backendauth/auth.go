// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package backendauth

import (
	"context"
	"errors"

	runtimeapi "github.com/envoyproxy/ai-gateway/api/runtime"
	"github.com/envoyproxy/ai-gateway/internal/filterapi"
)

// NewHandler returns a new implementation of [filterapi.BackendAuthHandler] based on the configuration.
// When config.CredentialOverride is set, the returned handler sources the upstream credential
// per-request and falls back to the static credential (or returns 401) as configured.
func NewHandler(ctx context.Context, config *filterapi.BackendAuth) (filterapi.BackendAuthHandler, error) {
	return NewHandlerWithResolver(ctx, config, nil)
}

// NewHandlerWithResolver handles explicit application references without fallback.
func NewHandlerWithResolver(ctx context.Context, config *filterapi.BackendAuth, resolver runtimeapi.CredentialResolver) (filterapi.BackendAuthHandler, error) {
	if config.ExternalCredential != "" {
		if resolver == nil {
			return nil, errors.New("external credential resolver is not configured")
		}
		if len(config.ExternalCredential) > 4096 || config.APIKey != nil || config.AWSAuth != nil || config.AzureAPIKey != nil || config.AnthropicAPIKey != nil || config.AzureAuth != nil || config.GCPAuth != nil || config.CredentialOverride != nil {
			return nil, errors.New("external credential cannot be combined with another credential source")
		}
		return &externalCredentialHandler{reference: config.ExternalCredential, resolve: resolver}, nil
	}

	var (
		inner   filterapi.BackendAuthHandler
		applyFn applyCredentialFn
		err     error
	)

	switch {
	case config.AWSAuth != nil:
		// AWS has its own handler: SigV4 takes three values, not one, so applyCredentialFn does not fit.
		awsInner, awsErr := newAWSHandler(ctx, config.AWSAuth)
		if awsErr != nil {
			return nil, awsErr
		}
		if config.CredentialOverride == nil {
			return awsInner, nil
		}
		return &awsCredentialOverrideHandler{inner: awsInner, config: config.CredentialOverride}, nil
	case config.APIKey != nil:
		inner, err = newAPIKeyHandler(config.APIKey)
		applyFn = applyBearerCredential
	case config.AzureAPIKey != nil:
		inner, err = newAzureAPIKeyHandler(config.AzureAPIKey)
		applyFn = applyAzureAPIKeyCredential
	case config.AzureAuth != nil:
		inner, err = newAzureHandler(config.AzureAuth)
		applyFn = applyBearerCredential
	case config.GCPAuth != nil:
		inner, err = newGCPHandler(ctx, config.GCPAuth)
		applyFn = makeGCPApplyFn(config.GCPAuth.Region, config.GCPAuth.ProjectName)
	case config.AnthropicAPIKey != nil:
		inner, err = newAnthropicAPIKeyHandler(config.AnthropicAPIKey)
		applyFn = applyAnthropicCredential
	default:
		return nil, errors.New("no backend auth handler found")
	}

	if err != nil {
		return nil, err
	}

	if config.CredentialOverride != nil {
		return &credentialOverrideHandler{
			inner:   inner,
			config:  config.CredentialOverride,
			applyFn: applyFn,
		}, nil
	}
	return inner, nil
}

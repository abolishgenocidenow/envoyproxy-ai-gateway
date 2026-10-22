// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0

// Package runtime defines extension contracts for applications embedding the
// AI Gateway runtime. These contracts do not expose internal configuration types.
package runtime

import (
	"context"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
)

// CredentialAttempt describes one upstream authentication attempt. Reference is
// supplied by accepted backend configuration, never by a request header. Headers
// and Body are request input and must not be treated as authorization. Metadata
// is supplied by Envoy; the application must enforce its own trusted namespace,
// authenticated-principal and route/connection binding policy.
// All mutable values are private copies owned by this call.
type CredentialAttempt struct {
	Reference string
	Headers   map[string]string
	// Body contains the replacement payload produced by the processor. Nil means
	// unavailable, not an empty outgoing body: passthrough can preserve bytes
	// changed by another Envoy filter. A body-dependent signer must reject nil.
	Body     []byte
	Metadata *corev3.Metadata
}

// CredentialHeader is an upstream authentication header mutation.
type CredentialHeader struct{ Name, Value string }

// CredentialResolver authorizes and supplies authentication for each attempt,
// including retries. Implementations must recheck current authorization and
// freshness before using either credential material or a cached derived token.
// A failure denies the attempt without using configured fallback credentials.
// Do not retain request data after returning. The runtime redacts returned errors.
type CredentialResolver func(context.Context, CredentialAttempt) ([]CredentialHeader, error)

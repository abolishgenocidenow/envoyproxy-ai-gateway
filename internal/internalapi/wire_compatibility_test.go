// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package internalapi_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	aigv1b1 "github.com/envoyproxy/ai-gateway/api/v1beta1"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
)

func TestWireCompatibility(t *testing.T) {
	require.Equal(t, aigv1b1.AIModelHeaderKey, internalapi.ModelNameHeaderKeyDefault)
	require.Equal(t, aigv1b1.AIGatewayFilterMetadataNamespace, internalapi.AIGatewayFilterMetadataNamespace)
}

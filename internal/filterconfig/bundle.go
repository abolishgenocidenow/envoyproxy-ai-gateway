// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package filterconfig

import (
	"fmt"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/envoyproxy/ai-gateway/internal/filterapi"
)

func MarshalConfigBundleIndex(index *filterapi.ConfigBundleIndex) ([]byte, error) {
	return yaml.Marshal(index)
}

func UnmarshalConfigBundleIndex(raw []byte) (*filterapi.ConfigBundleIndex, error) {
	var index filterapi.ConfigBundleIndex
	if err := yaml.Unmarshal(raw, &index); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config bundle index: %w", err)
	}
	if index.Checksum == "" {
		return nil, fmt.Errorf("invalid config bundle index: empty checksum value")
	}
	if len(index.Parts) == 0 {
		return nil, fmt.Errorf("invalid config bundle index: empty parts")
	}
	return &index, nil
}

// ReassembleBundleConfig rebuilds a full filter config from sharded parts and verifies integrity.
func ReassembleBundleConfig(index *filterapi.ConfigBundleIndex, readPart func(part filterapi.ConfigBundlePart) ([]byte, error)) (*filterapi.Config, error) {
	var payload []byte
	for i := 0; i < len(index.Parts); i++ {
		p := filterapi.ConfigBundlePart{
			Name: index.Parts[i].Name,
			Path: index.Parts[i].Path,
		}
		b, err := readPart(p)
		if err != nil {
			return nil, fmt.Errorf("failed to read bundle part %q: %w", p.Path, err)
		}
		payload = append(payload, b...)
	}

	expected := strings.ToLower(index.Checksum)
	actual := filterapi.ConfigBundleChecksum(payload)
	if actual != expected {
		return nil, fmt.Errorf("%w: expected %s got %s", filterapi.ErrBundleChecksumMismatch, expected, actual)
	}

	var cfg filterapi.Config
	if err := yaml.Unmarshal(payload, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bundled config: %w", err)
	}
	return &cfg, nil
}

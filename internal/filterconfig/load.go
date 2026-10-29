// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

// Package filterconfig loads and watches serialized host configuration.
package filterconfig

import (
	"os"

	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/envoyproxy/ai-gateway/internal/filterapi"
)

// UnmarshalConfigYaml reads the file at the given path and unmarshals it into a Config struct.
func UnmarshalConfigYaml(path string) (*filterapi.Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg filterapi.Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

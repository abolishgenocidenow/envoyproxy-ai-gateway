// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package filterapi

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

const ConfigBundleIndexFileName = "index.yaml"

var ErrBundleChecksumMismatch = errors.New("bundle checksum mismatch")

// ConfigBundleIndex describes where to find sharded filter config parts and how to validate them.
type ConfigBundleIndex struct {
	Version   string             `json:"version" yaml:"version"`
	UUID      string             `json:"uuid" yaml:"uuid"`
	Checksum  string             `json:"checksum" yaml:"checksum"`
	Parts     []ConfigBundlePart `json:"parts" yaml:"parts"`
	CreatedAt *time.Time         `json:"createdAt,omitempty" yaml:"createdAt,omitempty"`
}

type ConfigBundlePart struct {
	Name      string `json:"name" yaml:"name"`
	Path      string `json:"path" yaml:"path"`
	SizeBytes int    `json:"sizeBytes,omitempty" yaml:"sizeBytes,omitempty"`
}

func ConfigBundlePartPath(idx int) string {
	return fmt.Sprintf("parts/%03d", idx)
}

func ConfigBundleChecksum(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

#!/usr/bin/env bash
# Copyright Envoy AI Gateway Authors
# SPDX-License-Identifier: Apache-2.0
# The full text of the Apache license is available in the LICENSE file at
# the root of the repo.

set -euo pipefail

source_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
consumer_dir="$(mktemp -d)"
trap 'rm -rf "$consumer_dir"' EXIT
export GOWORK=off

cp "$source_root/cmd/extproc/mainlib/testdata/consumer_test.go" "$consumer_dir/consumer_test.go"
cat > "$consumer_dir/go.mod" <<'EOF'
module example.com/dataplane-mainlib-consumer

go 1.27.2

require github.com/envoyproxy/ai-gateway v0.0.0
EOF

cd "$consumer_dir"
go mod edit "-replace=github.com/envoyproxy/ai-gateway=$source_root"
go test -mod=mod -race -count=1 -timeout=30s ./...
go vet ./...

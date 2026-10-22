#!/usr/bin/env bash
# Copyright Envoy AI Gateway Authors
# SPDX-License-Identifier: Apache-2.0
# The full text of the Apache license is available in the LICENSE file at
# the root of the repo.
set -euo pipefail
cd "$(dirname "$0")/.."
export GOWORK=off
if [[ $# -eq 0 ]]; then
  set -- -race -count=1 -timeout=2m
fi

deps=$(go list -deps ./llm ./mcp)
forbidden='github.com/envoyproxy/(gateway|go-control-plane)/|github.com/envoyproxy/ai-gateway/(api/|internal/(extproc|controller|extensionserver))|github.com/dio/transit|dynamic.modules'
if printf '%s\n' "$deps" | grep -E "$forbidden"; then
  echo 'Execution library imports a forbidden control-plane or transport dependency.' >&2
  exit 1
fi

go test "$@" ./llm ./mcp ./internal/llmrequest ./internal/extproc ./internal/mcpproxy ./internal/translator ./internal/filterapi ./internal/internalapi
# A distinct Go module proves consumers do not need access to internal packages.
(cd tests/libraryconsumer && go test -mod=readonly "$@" ./... && go vet ./...)
go vet ./execution ./llm ./mcp

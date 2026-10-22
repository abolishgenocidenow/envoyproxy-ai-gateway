# AGN integration fork

`main` mirrors `theagentrouter/agent-router` without downstream changes.
`agn/integration` carries the maintained configuration-watcher, synchronized
runtime-publication and external-credential integration. Feature branches remain
independent review units. Consumers pin an exact qualified commit, not a moving
branch. The `github.com/envoyproxy/ai-gateway` module identity is unchanged.

The `execution`, `llm` and `mcp` packages provide reusable execution without
starting listeners. These APIs incorporate the library boundary from
`dio/agent-router` commits `ef434b55` and `922c05cb`; their public imports retain
the original module identity. Mainlib and execution-library consumers can share
one dependency tree in a single Go binary.

## Qualification

The AGN Integration workflow runs on pushes and pull requests targeting
`agn/integration`. It requires Linux and macOS unit/race tests, boundary vet,
Linux real-Envoy LLM and MCP tests, lint and spelling. Real-provider tests that
require credentials may skip; they are not live-provider acceptance evidence.
The workflow has read-only repository permission and does not publish artifacts
to registries. Envoy is pinned to the upstream unit-test version, 1.39.2, with
the downloader's checksum verification retained.

Inherited Build and Test, Docker Build Job and Release workflows are disabled
in this fork's repository settings because they include upstream registry
publication. This setting does not modify the mirrored main branch. A successful
upstream build is distinct from qualification of the downstream integration.

## Embedding in another Go project

Import `github.com/envoyproxy/ai-gateway/cmd/extproc/mainlib` and use
`MainWithOptions`. Credential callback types live in
`github.com/envoyproxy/ai-gateway/api/runtime`. A consuming module retains that
module identity and replaces it with an exact qualified fork version:

```go
require github.com/envoyproxy/ai-gateway v0.0.0

replace github.com/envoyproxy/ai-gateway => github.com/abolishgenocidenow/envoyproxy-ai-gateway <qualified-pseudo-version>
```

Use Go 1.27.2 or newer. Mainlib owns processor listeners, telemetry and shutdown;
the embedding application owns its context, watcher transport and credential
resolver. This is the runtime dependency closure, not a lightweight SDK.

One build selects one replacement for this module identity. This fork supplies
both mainlib and the execution-library APIs, so the consuming application's
replacement covers both. A dependency's own replacement directives are not
inherited. Default mainlib options retain upstream header/xDS behavior; see
[host composition](docs/design/host-composition.md) for injected routing and MCP
transport contracts.

`bash tools/check-mainlib-consumer.sh` creates an independent temporary module and
qualifies public imports, option construction, initial watcher application and
startup/shutdown with the race detector. CI runs this on Linux and macOS.

## Updating upstream

Fetch upstream main and origin, then merge upstream main into a new topic branch
based on `agn/integration`. Preserve the patch history; never reset the integration
branch to upstream. Open a pull request targeting `agn/integration` and require
`AGN required` before merging. Fast-forward the mirror main without force.
If upstream has rewritten history, investigate rather than forcing the mirror.

Changes to watcher acceptance, request-generation retention, credential ownership
or cancellation require tests at those boundaries. Do not weaken checksum checks,
skip failing tests or add real-provider credentials to make CI green.

Repository policy should require `AGN required` on `agn/integration`, disallow
force pushes and deletion, and allow no publishing automation by default.
Periodic upstream sync is an explicit maintenance operation; no scheduled merge
or dependency-pin update is implied.

# AGN integration fork

`main` mirrors `theagentrouter/agent-router` without downstream changes.
`agn/integration` carries the maintained configuration-watcher, synchronized
runtime-publication and external-credential integration. Feature branches remain
independent review units. Consumers pin an exact qualified commit, not a moving
branch. The `github.com/envoyproxy/ai-gateway` module identity is unchanged.

The execution-library fork at `dio/agent-router` is a separate dependency. Its
`execution`, `llm` and `mcp` APIs are not provided by this integration branch.

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

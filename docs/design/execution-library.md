# Reusable LLM and MCP execution

The `llm` and `mcp` packages expose the existing execution implementation to Go
hosts. The extproc and MCP HTTP adapters remain available. This is the library
boundary for a later Envoy shared-object adapter; it does not implement that ABI,
RTDS delivery, host selection, or a new Dataplane backend.

## Ownership

| Owner                            | Responsibility                                                                                                                                            |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Dataplane API and native runtime | Intent, validation, SQLite acceptance, immutable selection, deployment identity and recovery. Unchanged by this extraction.                               |
| Host / pick                      | Trusted route and backend selection, endpoint resolution, retries, HTTP transport, TLS, credential resolution, readiness and lifecycle.                   |
| `llm`                            | Capture original input, prepare each selected attempt with existing provider translators and mutations, translate decoded responses and accumulate usage. |
| `mcp`                            | Compile execution routes, session encryption, protocol dispatch, fan-out, tool filtering and authorization using the existing MCP engine.                 |
| Existing extproc adapter         | Envoy protobufs, downstream/upstream correlation, metadata precedence, credential integration, tracing and metrics.                                       |

These packages import neither the Envoy control-plane protobufs nor the gateway
controller/API packages. The verification script enforces that dependency boundary.
Existing utility dependencies remain, including Kubernetes utility packages; this
is not yet a minimal dependency module. The public configuration aliases the
existing execution types and is not a replacement for `dataplane/api`.

## LLM host contract

Import `github.com/envoyproxy/ai-gateway/llm`. The module path is intentionally
unchanged in the fork. `tests/libraryconsumer/go.mod` demonstrates consuming the
local fork with `replace`; pin a reviewed fork revision for a deployed consumer.

Call `NewRequest(operation, Input)` once with decoded body bytes and lower-case
headers. The library owns copies of that input. Call `NewAttempt(ctx, Backend)` for
each trusted backend selection. Apply removals before sets in the returned
`Mutation`, preserving the distinction between no replacement and an empty body.
Header entries are ordered; the last value wins. Send the result using the host's
transport. Request and attempt objects are single-owner, not concurrent-use objects.

The authentication callback receives the prepared body and headers, resolves
credentials and returns final header mutations. The library neither persists
credentials nor owns their cache. Authentication callback failures are sanitized.
The host must keep credential-bearing mutations out of logs and receipts. Body
policies must produce valid JSON; policy failure rejects preparation. Legacy
extproc retains its existing mutation-error behavior.

Feed each response through `ResponseHeaders` followed by ordered `ResponseBody`
calls. Bodies must be decoded; compressed headers are rejected. Buffer non-success
responses before passing `end=true`. Preserve a nil output body as pass-through,
and apply returned output even when a terminal provider stream error accompanies
it. Usage is cumulative per attempt; the host owns success accounting and retries.

The facade dispatches the 15 existing translation endpoint specs, including Chat
Completions, Responses, embeddings, audio, images, rerank and Messages. Provider
support is determined by each existing endpoint translator; unsupported pairs
return errors. `/models` is configuration-owned and is not a translation operation.
The shared acceptance corpus exercises Chat Completions across Bedrock Anthropic
and OpenAI, not all endpoint/provider combinations through the public facade.

## MCP host contract

Import `github.com/envoyproxy/ai-gateway/mcp`. `New(Options)` requires a selector,
a backend transport and session crypto. It creates no listener and has no default
network destination. Mount the returned `http.Handler` on a host-owned server.

`Select` supplies the authenticated route, subject and JWT-verification evidence.
It must verify signature, issuer and audience before setting `VerifiedJWT`; the
library does not verify bearer tokens. Reserved `x-ai-eg-` client headers are removed
before selection and trusted internal inputs are established by the adapter.
`Transport.RoundTrip` receives logical route/backend IDs separately from an HTTP
request with no routable URL. It must resolve the destination, honor cancellation,
apply backend credentials/TLS, and return a response body the engine can close.
Reserved routing headers are removed before transport invocation; redirects are
not followed by the engine. This is an HTTP embedding boundary, not an MCP SDK
callback API.

`Configure` deep-copies and compiles a whole route set before publishing it.
Preparation failure retains the old set. In-flight HTTP operations retain their
captured configuration; subsequent operations, including existing-session calls,
use the new policy. Admission can pause during compilation. Configuration updates
retain the host process. A successful `Configure` acknowledges local installation,
not listener readiness or a deployment-wide activation identity.

This first public surface supports default/`Always` tool and prompt prefixes.
`Never` is explicitly rejected because its uniqueness rules currently depend on
controller admission. Invalid authorization actions, claim value types, duplicate
names and reserved forwarding headers are also rejected. Existing adapters retain
their full prior configuration surface.

`Close(ctx)` stops admission, cancels active operations and waits for HTTP handlers
within the supplied deadline. The host still owns listeners and transport cleanup.
This is cancellation, not graceful streaming drain. No uninterrupted streaming or
session survival across process replacement is claimed. A stable session seed is
necessary for decrypting sessions after restart; backend session survival is a
separate concern. Background cleanup uses cancellation but is not a separately
tracked drain guarantee.

## Verification and remaining work

```sh
make test-execution-library
```

This runs dependency-boundary checks, adapter/provider regression tests and an
external Go module importing only the public libraries, all with the race detector.
Both adapter and library drivers read the same checked-in retry/streaming and MCP
session/authorization fixtures. Additional public tests exercise input ownership,
policy rejection, authentication-error redaction, concurrent reload, rejection
retaining serving state, spoofed headers, verified context and shutdown cancellation.
All upstreams in these tests are in-memory fakes.

The full upstream unit gate is `make test GO_TEST_ARGS='-race -p 2 -count=1 -timeout=5m'`.
Run it in an environment without provider credentials: existing credential tests
otherwise inherit host AWS configuration. Execution reports record the isolated
runner used for this change.

Next work is a separate host adapter: define Envoy callback lifetimes and body
ownership, connect pick's endpoint/transport identity, then run the same corpus
through an actual shared object. Dataplane snapshot publication, stale/duplicate
protection, incarnation acknowledgment, RTDS and restart reconciliation remain
outside this library milestone. So do an MCP SDK adapter and configurable library
telemetry; the public MCP entry point currently uses no-op telemetry.

## Host-neutral metadata

`execution.Metadata` is the common trusted input for both libraries: logical
`Route` and `Backend`, authenticated `Subject`, explicit `VerifiedJWT` evidence,
and opaque string `Attributes`. It contains no Envoy protobufs, cluster names,
header conventions or TLS configuration. Attributes never confer authorization
and must not contain credentials. Hosts must establish trust before constructing
this value; client headers must not be copied into identity fields blindly.

LLM hosts supply the complete `Backend.Metadata` for each attempt, including
repicks. Authentication callbacks read a snapshot with
`execution.FromContext(ctx)`. MCP selectors return the same type through the
`mcp.Selection` alias. Each outbound `BackendRequest.Metadata` carries it, with
route/backend set to the actual fan-out destination; the transport context carries
the same values. Session cleanup receives the captured metadata too. Metadata is
not automatically forwarded as HTTP headers, persisted in sessions, or logged.

Context capture and reads clone the attribute map. A callback can modify its own
copy without changing another attempt or fan-out operation. These are flat string
attributes, not arbitrary mutable objects or a parallel public configuration API.
Envoy host/cluster precedence, metadata namespaces and upstream/downstream
correlation remain adapter concerns. The existing extproc adapter continues using
its established decoding; a future shared-object adapter supplies the common input
at its library boundary.

## Fork development and CI

The durable fork's `main` branch carries this experimental library surface.
The upstream remote remains the reference for upstream synchronization. Public import paths remain `github.com/envoyproxy/ai-gateway`; consumers
must pin the fork revision with a Go module replacement.

The Execution Library workflow runs on pushes and pull requests targeting `main` or `lib`. It runs the public consumer and adapter tests with the race detector on
Linux and macOS, the full unit suite, dependency consistency, lint/spelling and
reachable-vulnerability checks for the public packages. No provider credentials,
image publication or chart publication are part of this workflow.

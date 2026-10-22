# Execution library behavior contract

The extraction of LLM and MCP execution into reusable packages must preserve the
observable behavior of the existing extproc and MCP HTTP adapters. The fixtures
in this change capture a bounded baseline before changing those boundaries. They
are not a new runtime configuration format or public API.

Run the focused contract:

```sh
make test-execution-contract GO_TEST_ARGS='-race -count=1 -timeout=2m'
```

Run the surrounding unit regression suite:

```sh
make test GO_TEST_ARGS='-race -p 2 -count=1 -timeout=5m'
```

## LLM request, attempt and response ownership

`internal/extproc/testdata/execution/retry.json` contains one original streaming
Chat Completions request, three ordered upstream attempts, and a fixed SSE response.
`TestExecutionContract_RetryAndStreaming` drives real processors and translators;
only metrics are recorded through the existing test implementation.

| Boundary               | Required behavior                                                                                                                                                |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Request capture        | Retain the original model, user content and unknown OpenAI extension field.                                                                                      |
| First selection        | Translate OpenAI input to Bedrock Anthropic, including its path and body shape.                                                                                  |
| Repick/retry           | Translate the original request to OpenAI with a new model; do not translate the preceding Anthropic body.                                                        |
| Retry without override | Replay the original body, removing the previous attempt's model override.                                                                                        |
| Request mutation       | Emit the appropriate replacement status, path, exact content length and removal of the internal upstream-host header.                                            |
| Backend context        | Decode backend and route attributes and associate each upstream attempt with the captured router request.                                                        |
| Response ownership     | Route downstream response processing through the latest valid attempt. Earlier attempts receive no successful completion or token accounting from that response. |
| Streaming              | Preserve the SSE bytes with one-byte, 17-byte and whole-response chunks.                                                                                         |
| Accounting             | Emit final usage at end of stream once, keeping original and served model identities distinct.                                                                   |

`TestExecutionContract_MetadataSelection` additionally pins host-versus-cluster
metadata precedence, endpoint-picker behavior, missing-metadata rejection, the
legacy empty route name, and replacement/removal of a spoofed upstream-host header.

The driver supplies each attempt explicitly. It does not prove that Envoy retries,
selects hosts or forwards the emitted mutations correctly. A future module adapter
must run the same inputs and compare semantic outputs while replacing the extproc
messages and request-correlation mechanism. Do not preserve protobufs or metadata
key names as the shared execution API merely to keep this driver unchanged.

## MCP conversation and authorization

`internal/mcpproxy/testdata/execution/session.json` describes a single ordered
conversation. `TestExecutionContract_MCPSession` enters the production HTTP mux,
uses real session encryption and config loading, and replaces only outbound HTTP
transport with an in-memory fake. No socket, provider, environment credential or
external service is involved.

The sequence establishes these requirements:

- Initialize negotiates the supported protocol and returns an opaque gateway
  session distinct from the upstream session. Upstream initialization is followed
  by `notifications/initialized`.
- `tools/list` filters unauthorized tools and prefixes visible tool names. The
  currently wired legacy POST path returns this result as one SSE data event.
- An allowed call removes the backend prefix, forwards the expected arguments and
  upstream session, and returns the JSON result with the original JSON-RPC ID.
- A denied call returns 403 without issuing any upstream request.
- Another authenticated subject cannot use the session (401); corrupted or missing
  sessions are rejected (400), without upstream calls.
- A sequential authorization reload affects the existing session: listing hides
  newly denied tools, and subsequent calls cannot retain the old authorization.

The route and subject headers supplied by the driver model trusted Envoy inputs.
This fixture does not authenticate those headers or verify JWT signatures. Existing
`TestAuthorizeRequest` cases cover the distinction between verified and unverified
JWT context. A new adapter must establish equivalent trust before supplying that
context to shared execution.

Encrypted session values and generated SSE event IDs are intentionally not golden
bytes. Assert their behavior and presence rather than snapshotting random values.
The conversation runs as one test with named, logged steps; a failure stops the
conversation before subsequent operations can use invalid state.

## Existing coverage remains part of acceptance

The new fixtures complement, rather than replace:

- `internal/translator`: provider translation, provider errors, token accounting,
  tool messages, and streaming parser fixtures.
- `internal/extproc/processor_impl_test.go`: body replacement versus continuation,
  local replies, header/body policy mutations, compressed responses, credential
  failures and route-scoped metadata/cost behavior.
- `internal/mcpproxy`: authorization/CEL, backend and tool selectors, session
  encoding, fan-out, SSE parsing and configuration change notifications.

Keep these tests passing while moving implementation. Add a second driver for the
new execution adapter before deleting any old driver. The same semantic fixtures
must pass through both consumers; normalizing an adapter-specific representation
must not erase statuses, messages, tool authorization, usage or stream content.

## Limits requiring later qualification

These tests do not establish dynamic-module ABI support, Envoy filter-state trust,
actual host selection, retry scheduling, mTLS/pool isolation, gRPC stream cleanup,
concurrent configuration publication, cancellation across SDK callbacks or draining
of live requests across replacement. The MCP reload in this fixture is sequential;
it does not endorse the current mutable config pointer as concurrency-safe.

They also do not qualify the modern MCP helpers as production behavior: the current
mux still dispatches POST to the legacy path. Full provider/protocol parity remains
an extraction milestone, not a conclusion from this bounded fixture set.

## Public library driver

`tests/libraryconsumer` now consumes both public packages from a separate Go module
and runs the same two fixture files. `make test-execution-library` runs both
consumers plus the surrounding provider tests and checks the dependency boundary.
The MCP engine now captures a synchronized immutable configuration per HTTP
operation; the public driver also tests concurrent reload and cancellation.
See [the library boundary](execution-library.md) for the implemented surface and
remaining deployment qualification work.

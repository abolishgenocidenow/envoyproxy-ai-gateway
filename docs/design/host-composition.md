# Host composition

Mainlib owns listeners, telemetry and supervised lifetime. `llm` and `mcp` expose
execution independently of those listeners. All packages share the original Go
module identity; a consuming binary pins one qualified fork replacement.

## Options

`mainlib.Options` supplies `ConfigWatcherFactory`, `CredentialResolver`, `Routing`
and `MCP`. Nil values retain the existing upstream behavior for each boundary.
The execution libraries do not own xDS, provider discovery, endpoint selection,
credential caches or retry scheduling.

`Routing` implements `api/runtime.ExtProcRouting`. `Resolve` receives an owned
copy of the first ext_proc request-headers message. The host establishes trust
before interpreting its metadata; a client-supplied header is not authority.
A downstream resolution supplies the original operation path. The server creates
a unique request ID and captures a configuration generation. `ConfigUUID` names
the host-owned immutable input for selecting the matching Pick policy; `ConfigID`
is the opaque serving token echoed by an upstream resolution. Host mode requires
a nonempty configuration UUID at load time; invalid initial input cannot satisfy
watcher readiness. `Handoff` runs after
headers and after successful body parsing and returns dynamic metadata for the
host to propagate. `BodyReady` distinguishes those phases. Metadata is limited to
64 KiB and must not contain secrets. The default model, original-path and internal
request-ID headers are removed; host mode does not clear Envoy's route cache.
Tracing and other processor output remain intact.

An upstream resolution supplies the retained request ID, opaque configuration ID,
logical backend and route. The original operation path comes from retained state;
a supplied conflicting path is rejected. `UpstreamHost` supplies a trusted signing
authority, separately from the logical backend. Backends resolve only against the
captured configuration. Config IDs distinguish publications even when source UUIDs
are reused. A retry retains the ID and generation and selects a fresh backend;
translation starts from the original public request. Concurrent upstream attempts
are rejected because response ownership supports sequential retries. Closing the
downstream stream removes its request state and prevents new attempts.

Callbacks support concurrent independent requests, honor cancellation and must
not reenter the same request. Errors returned by callbacks are sanitized. The
server copies callback input and returned metadata. Missing, expired, unknown or
mismatched host selections fail closed; there is no fallback to client headers or
xDS after a custom routing strategy is installed.

Without `Routing`, phase detection, request correlation, operation lookup,
backend/route metadata, routing headers and route-cache behavior use the upstream
adapter. This default is covered by the same ext_proc and real-Envoy tests.

`MCP` supplies a complete `mcp.Options` set: selector, logical backend transport,
session crypto and optional body limit. `--mcpAddr` enables its listener. Mainlib
retains metrics, tracing, config delivery and server supervision. The same host
transport adapter is used by the public MCP library. Reserved routing headers are
removed before selection and egress, transport receives logical destination IDs
with a non-routable HTTP URL, and redirects are not followed. The embedding host
owns destination resolution, credentials and TLS. Mainlib does not close the
caller-owned transport. Nil MCP options use upstream Envoy transport and
flag-configured session crypto.

## Dataplane consumption

Dataplane's Pick supplies a logical selection to Adapt. Adapt consumes `llm`
directly, retaining the original public request and preparing a fresh attempt
with the selected backend, authority and credential callback. It need not emulate
a pair of ext_proc streams in-process. A host that uses ext_proc transport supplies
the equivalent trusted handoff through mainlib's routing option.

The callbacks do not install an Envoy filter-state bridge, create clusters, rewrite
xDS or acknowledge a Dataplane activation. Hosts must implement and qualify that
transport handoff, installation ordering and exact activation evidence. The
execution libraries remain independent of Envoy control-plane protobufs.

## Verification

`tools/verify-execution-library.sh` checks dependency boundaries and shared
LLM/MCP semantic fixtures through both internal and external consumers.
`tools/check-mainlib-consumer.sh` creates an independent module and drives real
gRPC/HTTP listeners with injected routing and MCP transport. These run on both
Linux and macOS in AGN Integration. Existing real-Envoy LLM and MCP tests qualify
the default adapter path; they do not establish provider acceptance or the
host-specific Envoy metadata bridge.

To qualify an owned Envoy artifact, set `ENVOY_BIN` to its verified executable and
run:

```sh
go test -count=1 -p 1 -timeout=20m ./tests/data-plane ./tests/data-plane/vcr ./tests/data-plane-mcp
```

The shared test harness executes that binary directly, without replacing func-e's
cache. An invalid override fails;
it never falls back to a downloaded binary. Without the override, the default
version selection is unchanged. The separate MCP benchmark launcher uses
`aigw run` and is not covered by this override.

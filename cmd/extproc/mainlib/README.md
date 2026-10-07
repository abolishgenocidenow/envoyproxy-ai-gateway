# Extending configuration delivery

`Main(ctx, args, stderr)` keeps the existing sharded bundle-file watcher and
`--configBundlePath` flag. Downstream binaries can instead call
`MainWithOptions(ctx, args, stderr, opts)` with a `ConfigWatcherFactory`. No store
or network protocol is required by mainlib.

The factory receives `WatchOptions` once for each enabled consumer:

| Target            | Consumer                               |
| ----------------- | -------------------------------------- |
| `ConfigTargetAI`  | AI external processor (always enabled) |
| `ConfigTargetMCP` | MCP proxy (when `--mcpAddr` is set)    |

These targets let a source query and watch the corresponding configuration
scope. They do not change which servers are enabled. Keep independent checkpoints
for each consumer, even if both fetch the same document. A successful AI apply
does not acknowledge an MCP apply.

Implement `ConfigWatcher.Run(ctx, apply)` to fetch the initial document and then
supply complete replacements. Documents use the existing extproc YAML/JSON
configuration format with `version` equal to `WatchOptions.PayloadVersion`.
Mainlib decodes and version-checks each document, serializes calls to that
consumer's existing configuration loader, and returns its result. It does not
expose the internal Go configuration type or change the loader's semantics.
For MCP, include `mcpConfig` in the document; omitting it is an existing loader
no-op, not a request to remove MCP configuration. An empty `mcpConfig: {}`
replaces the MCP configuration with an empty one.

- Set a positive `Options.MaxConfigBytes` and omit `--configBundlePath` when
  installing a custom factory. Enforce the same limit while fetching; mainlib
  checks the byte count before decoding. This is not a runtime memory limit.
- Advance a source revision, ETag or cursor only after `apply` succeeds. Retry
  transient fetch and apply failures inside `Run`. Mainlib adds no retries or
  expiration policy for previously accepted configuration.
- Every custom watcher must successfully apply an initial document before
  extproc starts serving. The default file watcher's startup behavior is
  unchanged, including retries for incomplete bundles.
- `Run` must block, honor cancellation, and join its callback goroutines before
  returning. Returning while its context is live, including returning nil,
  stops extproc and causes `MainWithOptions` to return an error.
- Construct independent watcher state in the factory, but acquire and release
  transport resources in `Run`. Mainlib constructs all watchers before starting
  any of them. Applied credential handlers receive the process context, never
  an individual fetch's deadline.

[The HTTP polling example](configwatcher_example_test.go) compiles as an external
package using only the public API. It makes an initial GET, polls with a
per-consumer ETag, bounds response bytes, and advances the ETag only after a
successful apply. Its `scope` and `payloadVersion` query parameters are an
example endpoint contract, not a protocol imposed by mainlib. A downstream
factory can capture gateway identity and other source-specific options.

A production transport also needs appropriate authentication, TLS, redirect
policy, retry backoff/jitter and diagnostics. Configuration can contain
credentials: do not log documents, response bodies or unredacted apply errors.

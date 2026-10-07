# Extending configuration delivery

`Main(ctx, args, stderr)` keeps the existing sharded bundle-file watcher and
`--configBundlePath` flag. Downstream binaries can instead call
`MainWithOptions(ctx, args, stderr, opts)` with a `ConfigWatcherFactory`. No store
or network protocol is required by mainlib.

The factory receives `WatchOptions` once for each enabled consumer:

| Target            | Consumer                                |
| ----------------- | --------------------------------------- |
| `ConfigTargetLLM` | LLM external processor (always enabled) |
| `ConfigTargetMCP` | MCP proxy (when `--mcpAddr` is set)     |

These targets let a source query and watch the corresponding configuration
scope. They do not change which servers are enabled. Keep independent checkpoints
for each consumer, even if both fetch the same document. A successful LLM apply
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
  watcher-owned transport resources in `Run`. A shared client can instead be
  injected by the application, which owns its cleanup. Mainlib constructs all
  watchers before starting any of them. Applied credential handlers receive the
  process context, never an individual fetch's deadline.

[The HTTP polling example](configwatcher_example_test.go) compiles as an external
package using only the public API. It makes an initial GET, polls with a
per-consumer ETag, bounds response bytes, and advances the ETag only after a
successful apply. Its `scope` and `payloadVersion` query parameters are an
example endpoint contract, not a protocol imposed by mainlib. A downstream
factory can capture gateway identity and other source-specific options.

## Connection configuration and client injection

Connection setup belongs to the downstream application and its transport
adapter. Inject a configured client into the adapter through the factory
closure; mainlib only receives the resulting `ConfigWatcher`. This avoids a
protocol-specific client field or TLS/authentication flags in mainlib.

| Owner       | Responsibilities                                                                                                              |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Application | Endpoint, client, trust roots, mTLS identity, authentication, proxies, redirects, connection timeouts and credential rotation |
| Adapter     | Scoped queries, polling or streaming, retries, bounded reads and per-consumer checkpoints                                     |
| Mainlib     | Decoding, version checks, serialized consumer application and subscription lifecycle                                          |

For HTTPS with mTLS, the application can construct the client below after
loading `serverRoots` as an `*x509.CertPool` and `clientCertificate` as a
`tls.Certificate`. These values come from the application's own configuration
or credential provider; mainlib does not load certificate files or credentials.

```go
transport := http.DefaultTransport.(*http.Transport).Clone()
transport.TLSClientConfig = &tls.Config{
	MinVersion:   tls.VersionTLS12,
	RootCAs:      serverRoots,
	Certificates: []tls.Certificate{clientCertificate},
}
client := &http.Client{
	Transport: transport,
	Timeout:   10 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}
defer client.CloseIdleConnections()
// Pass client to the run function in ExampleMainWithOptions.
// Its factory captures client and supplies it to each httpWatcher.
```

The example accepts this client as an argument. The same client and connection
pool can serve both LLM and MCP subscriptions, while each subscription retains
its own ETag and apply result. Sharing a client does not coalesce polling: the
example still makes a separate scoped query for each enabled consumer.

The application closes shared resources after `MainWithOptions` returns,
including on startup failure. Each watcher's `Run` cancels its own requests and
releases its response bodies or streams; it must not close a client shared with
another subscription. The example's request timeout remains independent of the
context passed to installed runtime configuration.

Authentication can be implemented by an application-owned `http.RoundTripper`.
Rotating mTLS identities can use a concurrency-safe `GetClientCertificate`
callback; do not mutate a TLS configuration already in use. A TCP adapter could
capture a dialer, and a gRPC adapter could capture a generated client. Protocol
details, including any ordering or delivery guarantees needed for datagrams,
remain the adapter's responsibility.

A production transport also needs appropriate authentication, TLS, redirect
policy, retry backoff/jitter and diagnostics. Configuration can contain
credentials: do not log documents, response bodies or unredacted apply errors.

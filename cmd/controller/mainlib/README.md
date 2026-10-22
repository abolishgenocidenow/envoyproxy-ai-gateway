# Embedded controller

`Main(ctx, args, Options{})` runs the same controller as `cmd/controller`.
The caller owns signal handling and process exit. Startup and runtime errors
return to the caller; manager-owned services stop with the supplied context.

`Options.Configure` can register application services with the manager before
startup. Services declare whether they require leader election. Existing
controller reconcilers remain leader-elected; extension serving and the rate-limit
configuration server run on every replica, as before.

`Options.PublishConfig` receives an isolated JSON document before the elected
controller writes its legacy bundle. Returning an error preserves the old bundle
and causes reconciliation to retry. Leaving it nil preserves default behavior.
The callback must be idempotent, bounded and context-aware. Its payload can contain
credentials and must not be logged.

This hook is not an authoritative freshness grant, workload assignment, route
activation or credential authorization API. Those guarantees belong to the
embedding application and must not be inferred from successful publication.

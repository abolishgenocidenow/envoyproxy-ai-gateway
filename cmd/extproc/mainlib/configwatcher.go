// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package mainlib

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/version"
)

// ConfigTarget identifies a configuration consumer, not which servers to enable.
type ConfigTarget string

const (
	// ConfigTargetLLM selects the LLM external processor.
	ConfigTargetLLM ConfigTarget = "llm"
	// ConfigTargetMCP selects the MCP proxy, when enabled.
	// Custom documents must include mcpConfig; mcpConfig: {} clears its configuration.
	ConfigTargetMCP ConfigTarget = "mcp"
)

// ApplyConfig decodes, validates and applies a complete YAML or JSON document.
// Nil acknowledges acceptance by this receiver only. On error, the watcher
// should retry without advancing its checkpoint. The caller must not mutate
// payload until the call returns. Errors may contain sensitive input and must
// not be logged without redaction. Each consumer may impose additional snapshot
// requirements before its configuration loader is called.
type ApplyConfig func(payload []byte) error

// WatchOptions describes the consumer to a configuration watcher.
type WatchOptions struct {
	Target ConfigTarget
	// PayloadVersion is the exact configuration version accepted by this binary.
	PayloadVersion string
	// MaxConfigBytes bounds a serialized document, not decoded runtime memory.
	MaxConfigBytes int64
	Logger         *slog.Logger
}

// ConfigWatcher supplies an initial configuration and subsequent replacements.
type ConfigWatcher interface {
	// Run blocks until cancellation or a fatal error. Retry transient fetch and
	// apply errors here. Returning while ctx is live, even with nil, stops extproc.
	// Run must honor cancellation and join any goroutines using apply before
	// returning. Each instance serves a single consumer.
	Run(ctx context.Context, apply ApplyConfig) error
}

// ConfigWatcherFactory constructs independent watcher state per enabled
// consumer. A factory can capture an application-owned client shared by its
// watchers; the application cleans it up after MainWithOptions returns. Acquire
// watcher-owned transport resources in Run so construction does not require
// cleanup if a later factory call fails.
type ConfigWatcherFactory func(WatchOptions) (ConfigWatcher, error)

// Options configures optional extensions to Main. Its zero value preserves the
// bundle-file watcher, including its startup behavior.
type Options struct {
	ConfigWatcherFactory ConfigWatcherFactory
	// MaxConfigBytes must be positive when a custom factory is supplied.
	// Watchers should enforce it while fetching; mainlib checks it before decoding.
	MaxConfigBytes int64
}

var errConfigWatcherStopped = errors.New("configuration watcher stopped")

type configReceiver struct {
	target   ConfigTarget
	receiver filterapi.ConfigReceiver
	// validate checks scoped snapshots from custom sources before loading them.
	// File subscriptions retain their existing loader semantics.
	validate func(*filterapi.Config) error
}

// configSubscription signals initialized according to its source's startup
// policy, then blocks until cancellation or a fatal error. Only internal adapters
// control this signal; public ConfigWatchers initialize through a successful apply.
type configSubscription func(ctx context.Context, initialized chan struct{}) error

type configSubscriptionFactory func(configReceiver) (configSubscription, error)

// resolveConfigSubscriptionFactory selects the default file adapter once. Both
// adapters use the same supervisor without imposing custom-source readiness or
// document decoding on the existing bundle watcher.
func resolveConfigSubscriptionFactory(runtimeCtx context.Context, opts Options, bundlePath string, logger *slog.Logger) configSubscriptionFactory {
	if opts.ConfigWatcherFactory == nil {
		return func(receiver configReceiver) (configSubscription, error) {
			return func(ctx context.Context, initialized chan struct{}) error {
				return filterapi.RunConfigBundleWatcher(ctx, bundlePath, receiver.receiver,
					logger.With("config_target", receiver.target), 5*time.Second, func() { close(initialized) })
			}, nil
		}
	}
	return func(receiver configReceiver) (configSubscription, error) {
		watcher, err := opts.ConfigWatcherFactory(WatchOptions{
			Target: receiver.target, PayloadVersion: version.Parse(), MaxConfigBytes: opts.MaxConfigBytes,
			Logger: logger.With("config_target", receiver.target),
		})
		if err != nil {
			return nil, err
		}
		if watcher == nil {
			return nil, errors.New("nil watcher")
		}
		return func(ctx context.Context, initialized chan struct{}) error {
			return watcher.Run(ctx, configApplier(ctx, runtimeCtx, opts.MaxConfigBytes, receiver, initialized))
		}, nil
	}
}

// startConfigWatchers starts and supervises scoped subscriptions supplied by the
// resolved factory. MainWithOptions registers its enabled configuration consumers;
// this helper manages one subscription per registration, independently of the
// consumer's identity or whether it is optional. It does not merge configuration
// sources or consolidate transport connections. Watchers may share
// an application-owned client, but this helper does not deduplicate their reads.
// File and custom subscriptions share this startup and shutdown path.
//
// Each receiver gets its own subscription because acceptance is independent:
// accepting an update for one consumer must not acknowledge another's update.
// Checkpoints and retries belong to the watcher. The custom callback serializes
// applies to its receiver, decodes and version-checks documents, invokes any
// validation registered by that consumer, and reports the existing loader's
// result. It neither orders source revisions nor makes updates across consumers
// atomic; watchers must deliver replacements in source order.
//
// Construction finishes for every consumer before any subscription starts. A
// factory error therefore leaves no running subscriptions. Subscriptions then run
// concurrently, and startup waits for each adapter's initialization signal. Custom
// watchers signal only after a successful LoadConfig. The file adapter preserves
// legacy startup: a missing part or checksum mismatch starts background retries
// without an initial apply; other initial errors are fatal. This is a one-time
// startup gate, not a freshness check. Each source retains its retry policy.
// MainWithOptions starts serving only after this function returns successfully.
//
// ctx and fail must come from the same context.WithCancelCause call. A watcher
// returning while its context is live, even with nil, calls fail to cancel
// MainWithOptions and its other subscriptions. Startup cancellation joins them
// before returning the cause. On success, the caller owns stop, which cancels the watch
// context and waits for subscriptions to finish. Run must honor cancellation and join
// its callback goroutines; stop cannot forcibly terminate a misbehaving watcher.
//
// Custom-source runtime configuration receives ctx, not the narrower watch context
// or a fetch deadline: credential handlers may retain it for later requests.
// Stopping subscriptions alone does not cancel that runtime context; process
// shutdown does. Shared transport cleanup remains the application's responsibility
// after MainWithOptions returns.
func startConfigWatchers(ctx context.Context, fail context.CancelCauseFunc, factory configSubscriptionFactory,
	receivers []configReceiver,
) (stop func(), err error) {
	// Factories construct state only; opening watcher-owned resources in Run
	// avoids needing a separate cleanup contract for partial construction.
	watchers := make([]configSubscription, len(receivers))
	for i, receiver := range receivers {
		watchers[i], err = factory(receiver)
		if err != nil {
			return nil, fmt.Errorf("create %s configuration watcher: %w", receiver.target, err)
		}
		if watchers[i] == nil {
			return nil, fmt.Errorf("create %s configuration watcher: nil watcher", receiver.target)
		}
	}
	watchCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	stop = func() { cancel(); wg.Wait() }
	ready := make([]<-chan struct{}, len(receivers))
	for i, receiver := range receivers {
		initialized := make(chan struct{})
		ready[i] = initialized
		watcher := watchers[i]
		wg.Go(func() {
			runErr := watcher(watchCtx, initialized)
			if watchCtx.Err() == nil {
				if runErr == nil {
					runErr = errors.New("watcher returned before cancellation")
				}
				fail(fmt.Errorf("%s %w: %w", receiver.target, errConfigWatcherStopped, runErr))
			}
		})
	}
	// Runs are already concurrent. Waiting for all receivers must not let the
	// first successful scope make another scope appear initialized.
	for _, initialized := range ready {
		select {
		case <-ctx.Done():
			stop()
			return nil, context.Cause(ctx)
		case <-initialized:
		}
	}
	if ctx.Err() != nil {
		stop()
		return nil, context.Cause(ctx)
	}
	return stop, nil
}

func configApplier(watchCtx, runtimeCtx context.Context, maxBytes int64, receiver configReceiver,
	initialized chan struct{},
) ApplyConfig {
	var mu sync.Mutex
	var ready bool
	return func(payload []byte) error {
		mu.Lock()
		defer mu.Unlock()
		if err := watchCtx.Err(); err != nil {
			return err
		}
		if int64(len(payload)) > maxBytes {
			return fmt.Errorf("configuration exceeds %d byte limit", maxBytes)
		}
		var cfg filterapi.Config
		if err := yaml.Unmarshal(payload, &cfg); err != nil {
			return fmt.Errorf("decode configuration: %w", err)
		}
		if cfg.Version != version.Parse() {
			return fmt.Errorf("config version mismatch: expected %q, got %q", version.Parse(), cfg.Version)
		}
		if receiver.validate != nil {
			if err := receiver.validate(&cfg); err != nil {
				return fmt.Errorf("validate configuration: %w", err)
			}
		}
		// Runtime credential handlers may retain this context. A fetch deadline must
		// not cancel state that was successfully installed for subsequent requests.
		if err := receiver.receiver.LoadConfig(runtimeCtx, &cfg); err != nil {
			return fmt.Errorf("apply configuration: %w", err)
		}
		if !ready {
			ready = true
			close(initialized)
		}
		return nil
	}
}

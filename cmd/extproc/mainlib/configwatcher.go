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
	ConfigTargetMCP ConfigTarget = "mcp"
)

// ApplyConfig decodes, validates and applies a complete YAML or JSON document.
// Nil acknowledges acceptance by this receiver only. On error, the watcher
// should retry without advancing its checkpoint. The caller must not mutate
// payload until the call returns. Errors may contain sensitive input and must
// not be logged without redaction.
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
// consumer. Acquire transport resources in Run so construction does not require
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
}

func startConfigWatchers(ctx context.Context, fail context.CancelCauseFunc, opts Options, logger *slog.Logger,
	receivers []configReceiver,
) (stop func(), err error) {
	watchers := make([]ConfigWatcher, len(receivers))
	for i, receiver := range receivers {
		watchers[i], err = opts.ConfigWatcherFactory(WatchOptions{
			Target: receiver.target, PayloadVersion: version.Parse(), MaxConfigBytes: opts.MaxConfigBytes,
			Logger: logger.With("config_target", receiver.target),
		})
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
		apply := configApplier(watchCtx, ctx, opts.MaxConfigBytes, receiver.receiver, initialized)
		wg.Add(1)
		go func(watcher ConfigWatcher, target ConfigTarget) {
			defer wg.Done()
			runErr := watcher.Run(watchCtx, apply)
			if watchCtx.Err() == nil {
				if runErr == nil {
					runErr = errors.New("watcher returned before cancellation")
				}
				fail(fmt.Errorf("%s %w: %w", target, errConfigWatcherStopped, runErr))
			}
		}(watchers[i], receiver.target)
	}
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

func configApplier(watchCtx, runtimeCtx context.Context, maxBytes int64, receiver filterapi.ConfigReceiver,
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
		// Runtime credential handlers may retain this context. A fetch deadline must
		// not cancel state that was successfully installed for subsequent requests.
		if err := receiver.LoadConfig(runtimeCtx, &cfg); err != nil {
			return fmt.Errorf("apply configuration: %w", err)
		}
		if !ready {
			ready = true
			close(initialized)
		}
		return nil
	}
}

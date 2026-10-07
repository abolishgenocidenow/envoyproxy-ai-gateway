// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package mainlib

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/version"
)

type watcherFunc func(context.Context, ApplyConfig) error

func (f watcherFunc) Run(ctx context.Context, apply ApplyConfig) error { return f(ctx, apply) }

type receiverFunc func(context.Context, *filterapi.Config) error

func (f receiverFunc) LoadConfig(ctx context.Context, cfg *filterapi.Config) error {
	return f(ctx, cfg)
}

func configPayload(uuid string) []byte {
	return []byte(fmt.Sprintf("version: %s\nuuid: %s\n", version.Parse(), uuid))
}

func TestConfigWatcherOptions(t *testing.T) {
	factory := func(WatchOptions) (ConfigWatcher, error) { return nil, nil }
	for _, tc := range []struct {
		name string
		args []string
		opts Options
		want string
	}{
		{name: "default requires file", want: "configBundlePath must be provided"},
		{name: "default file", args: []string{"--configBundlePath", "/bundle"}},
		{name: "custom", opts: Options{ConfigWatcherFactory: factory, MaxConfigBytes: 1024}},
		{name: "conflict", args: []string{"--configBundlePath", "/bundle"}, opts: Options{ConfigWatcherFactory: factory, MaxConfigBytes: 1024}, want: "cannot be combined"},
		{name: "missing limit", opts: Options{ConfigWatcherFactory: factory}, want: "MaxConfigBytes must be positive"},
		{name: "negative limit", opts: Options{ConfigWatcherFactory: factory, MaxConfigBytes: -1}, want: "MaxConfigBytes must be positive"},
		{name: "other flags still validated", args: []string{"--logLevel", "invalid"}, opts: Options{ConfigWatcherFactory: factory, MaxConfigBytes: 1024}, want: "failed to unmarshal log level"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseAndValidateFlagsWithOptions(tc.args, tc.opts)
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfigApplier(t *testing.T) {
	runtimeCtx, cancelRuntime := context.WithCancel(t.Context())
	defer cancelRuntime()
	watchCtx, cancelWatch := context.WithCancel(runtimeCtx)
	defer cancelWatch()
	initialized := make(chan struct{})
	var calls int
	var current string
	var savedCtx context.Context
	rejected := errors.New("receiver rejected candidate")
	apply := configApplier(watchCtx, runtimeCtx, 128, receiverFunc(func(ctx context.Context, cfg *filterapi.Config) error {
		calls++
		if cfg.UUID == "rejected" {
			return rejected
		}
		current, savedCtx = cfg.UUID, ctx
		return nil
	}), initialized)
	for _, payload := range [][]byte{[]byte(strings.Repeat("x", 129)), []byte("[broken"), []byte("version: incompatible"), nil} {
		require.Error(t, apply(payload))
	}
	require.Zero(t, calls)
	require.ErrorIs(t, apply(configPayload("rejected")), rejected)
	select {
	case <-initialized:
		t.Fatal("rejected config initialized receiver")
	default:
	}
	require.NoError(t, apply(configPayload("first")))
	<-initialized
	require.Equal(t, "first", current)
	require.ErrorIs(t, apply(configPayload("rejected")), rejected)
	require.Equal(t, "first", current)
	require.NoError(t, apply([]byte(fmt.Sprintf(`{"version":%q,"uuid":"second"}`, version.Parse()))))
	require.Equal(t, "second", current)
	cancelWatch()
	require.NoError(t, savedCtx.Err(), "watch/fetch lifetime must not cancel installed runtime")
	require.ErrorIs(t, apply(configPayload("late")), context.Canceled)
	require.Equal(t, "second", current)
	cancelRuntime()
	require.ErrorIs(t, savedCtx.Err(), context.Canceled)
}

func TestConfigApplierSerializesReceiver(t *testing.T) {
	var active, calls atomic.Int32
	apply := configApplier(t.Context(), t.Context(), 1024, receiverFunc(func(context.Context, *filterapi.Config) error {
		if active.Add(1) != 1 {
			return errors.New("concurrent receiver application")
		}
		defer active.Add(-1)
		calls.Add(1)
		return nil
	}), make(chan struct{}))
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for range 20 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- apply(configPayload("update")) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, 20, calls.Load())
}

func TestConfigWatchersIndependentInitialization(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	llmApplied, mcpRejected := make(chan struct{}), make(chan struct{})
	retryMCP := make(chan struct{})
	var llmCalls, mcpCalls atomic.Int32
	var observed []WatchOptions
	opts := Options{MaxConfigBytes: 1024, ConfigWatcherFactory: func(o WatchOptions) (ConfigWatcher, error) {
		observed = append(observed, o)
		return watcherFunc(func(ctx context.Context, apply ApplyConfig) error {
			if o.Target == ConfigTargetLLM {
				if err := apply(configPayload("llm")); err != nil {
					return err
				}
				close(llmApplied)
			} else {
				if err := apply(configPayload("rejected")); err == nil {
					return errors.New("expected receiver rejection")
				}
				close(mcpRejected)
				select {
				case <-ctx.Done():
					return nil
				case <-retryMCP:
				}
				if err := apply(configPayload("mcp")); err != nil {
					return err
				}
			}
			<-ctx.Done()
			return nil
		}), nil
	}}
	receivers := []configReceiver{
		{ConfigTargetLLM, receiverFunc(func(context.Context, *filterapi.Config) error { llmCalls.Add(1); return nil })},
		{ConfigTargetMCP, receiverFunc(func(_ context.Context, cfg *filterapi.Config) error {
			mcpCalls.Add(1)
			if cfg.UUID == "rejected" {
				return errors.New("retry")
			}
			return nil
		})},
	}
	started := make(chan func(), 1)
	startErr := make(chan error, 1)
	go func() {
		stop, err := startConfigWatchers(ctx, cancel, opts, slog.New(slog.NewTextHandler(io.Discard, nil)), receivers)
		if err != nil {
			startErr <- err
			return
		}
		started <- stop
	}()
	select {
	case <-llmApplied:
	case <-time.After(5 * time.Second):
		t.Fatal("LLM not applied")
	}
	select {
	case <-mcpRejected:
	case <-time.After(5 * time.Second):
		t.Fatal("MCP not attempted")
	}
	select {
	case <-started:
		t.Fatal("started before MCP accepted")
	case err := <-startErr:
		t.Fatal(err)
	default:
	}
	close(retryMCP)
	var stop func()
	select {
	case stop = <-started:
	case err := <-startErr:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("startup blocked")
	}
	defer stop()
	require.EqualValues(t, 1, llmCalls.Load())
	require.EqualValues(t, 2, mcpCalls.Load())
	require.Len(t, observed, 2)
	for _, o := range observed {
		require.Equal(t, version.Parse(), o.PayloadVersion)
		require.EqualValues(t, 1024, o.MaxConfigBytes)
		require.NotNil(t, o.Logger)
	}
	cancel(nil)
}

func TestConfigWatchersTermination(t *testing.T) {
	for _, tc := range []struct {
		name   string
		runErr error
	}{{"nil", nil}, {"fatal", errors.New("broken source")}, {"canceled while live", context.Canceled}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			_, err := startConfigWatchers(ctx, cancel, Options{MaxConfigBytes: 1024, ConfigWatcherFactory: func(WatchOptions) (ConfigWatcher, error) {
				return watcherFunc(func(context.Context, ApplyConfig) error { return tc.runErr }), nil
			}}, slog.Default(), []configReceiver{{ConfigTargetLLM, receiverFunc(func(context.Context, *filterapi.Config) error { return nil })}})
			require.ErrorContains(t, err, "llm configuration watcher stopped")
			if tc.runErr != nil {
				require.ErrorIs(t, err, tc.runErr)
			}
		})
	}
}

func TestConfigWatchersFactoryFailure(t *testing.T) {
	for _, factoryErr := range []error{nil, errors.New("cannot construct")} {
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)
		var started bool
		_, err := startConfigWatchers(ctx, cancel, Options{MaxConfigBytes: 1024, ConfigWatcherFactory: func(o WatchOptions) (ConfigWatcher, error) {
			if o.Target == ConfigTargetMCP {
				return nil, factoryErr
			}
			return watcherFunc(func(context.Context, ApplyConfig) error { started = true; return nil }), nil
		}}, slog.Default(), []configReceiver{{target: ConfigTargetLLM}, {target: ConfigTargetMCP}})
		require.Error(t, err)
		require.False(t, started, "do not start watchers until every factory succeeds")
	}
}

func TestConfigWatchersCancelBeforeInitialConfig(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	entered := make(chan struct{})
	exited := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := startConfigWatchers(ctx, cancel, Options{MaxConfigBytes: 1024, ConfigWatcherFactory: func(WatchOptions) (ConfigWatcher, error) {
			return watcherFunc(func(ctx context.Context, _ ApplyConfig) error {
				close(entered)
				<-ctx.Done()
				close(exited)
				return nil
			}), nil
		}}, slog.Default(), []configReceiver{{target: ConfigTargetLLM}})
		result <- err
	}()
	<-entered
	cancel(nil)
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not stop watcher")
	}
	select {
	case <-exited:
	default:
		t.Fatal("watcher not joined")
	}
}

func TestConfigWatcherShutdownWithActiveStream(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	defer listener.Close()
	server := grpc.NewServer()
	defer server.Stop()
	grpc_health_v1.RegisterHealthServer(server, health.NewServer())
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	defer conn.Close()
	stream, err := grpc_health_v1.NewHealthClient(conn).Watch(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err, "stream must be active before shutdown")
	shutdownCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stopped := make(chan struct{})
	go func() { stopGRPCServer(shutdownCtx, server); close(stopped) }()
	cancel()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("active stream blocked shutdown")
	}
	_, err = stream.Recv()
	require.Error(t, err)
	require.NoError(t, <-served)
}

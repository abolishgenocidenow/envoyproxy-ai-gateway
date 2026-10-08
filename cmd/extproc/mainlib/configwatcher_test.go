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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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

func mcpPayload(uuid string) []byte {
	return append(configPayload(uuid), []byte("mcpConfig: {}\n")...)
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
	apply := configApplier(watchCtx, runtimeCtx, 128, configReceiver{target: ConfigTargetLLM, receiver: receiverFunc(func(ctx context.Context, cfg *filterapi.Config) error {
		calls++
		if cfg.UUID == "rejected" {
			return rejected
		}
		current, savedCtx = cfg.UUID, ctx
		return nil
	})}, initialized)
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
	apply := configApplier(t.Context(), t.Context(), 1024, configReceiver{target: ConfigTargetLLM, receiver: receiverFunc(func(context.Context, *filterapi.Config) error {
		if active.Add(1) != 1 {
			return errors.New("concurrent receiver application")
		}
		defer active.Add(-1)
		calls.Add(1)
		return nil
	})}, make(chan struct{}))
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

func TestConfigApplierConsumerValidation(t *testing.T) {
	initialized := make(chan struct{})
	rejected := errors.New("consumer rejected snapshot")
	var loaded string
	apply := configApplier(t.Context(), t.Context(), 1024, configReceiver{
		target: ConfigTarget("additional-consumer"),
		validate: func(cfg *filterapi.Config) error {
			if cfg.UUID != "accepted" {
				return rejected
			}
			return nil
		},
		receiver: receiverFunc(func(_ context.Context, cfg *filterapi.Config) error {
			loaded = cfg.UUID
			return nil
		}),
	}, initialized)
	require.ErrorIs(t, apply(configPayload("rejected")), rejected)
	require.Empty(t, loaded, "validation must precede the loader")
	select {
	case <-initialized:
		t.Fatal("validation failure satisfied startup readiness")
	default:
	}
	require.NoError(t, apply(configPayload("accepted")))
	select {
	case <-initialized:
	default:
		t.Fatal("accepted configuration did not satisfy startup readiness")
	}
	require.ErrorIs(t, apply(configPayload("rejected-again")), rejected)
	require.Equal(t, "accepted", loaded, "rejection must preserve the accepted configuration")
}

func TestConfigApplierMCPRequiresConfiguration(t *testing.T) {
	initialized := make(chan struct{})
	var calls int
	var current *filterapi.MCPConfig
	apply := configApplier(t.Context(), t.Context(), 1024, configReceiver{
		target: ConfigTargetMCP, validate: validateMCPConfig,
		receiver: receiverFunc(func(_ context.Context, cfg *filterapi.Config) error {
			calls++
			current = cfg.MCPConfig
			return nil
		}),
	}, initialized)
	for _, payload := range [][]byte{configPayload("missing"), append(configPayload("null"), []byte("mcpConfig: null\n")...)} {
		require.ErrorContains(t, apply(payload), "must include mcpConfig")
	}
	require.Zero(t, calls)
	select {
	case <-initialized:
		t.Fatal("missing MCP section satisfied initial configuration gate")
	default:
	}
	// An explicit empty section is a real replacement, including for revocation.
	require.NoError(t, apply(mcpPayload("empty")))
	<-initialized
	require.NotNil(t, current)
	accepted := current
	require.ErrorContains(t, apply(configPayload("missing-again")), "must include mcpConfig")
	require.Equal(t, 1, calls, "rejection must not reach the loader or acknowledge an update")
	require.Same(t, accepted, current)
}

func FuzzConfigApplier(f *testing.F) {
	for _, payload := range [][]byte{
		nil, []byte("[broken"), []byte("null"), []byte("version: incompatible\n"),
		configPayload("llm"), mcpPayload("mcp"),
		append(configPayload("null-mcp"), []byte("mcpConfig: null\n")...),
		[]byte(fmt.Sprintf(`{"version":%q,"mcpConfig":{}}`, version.Parse())),
		append(mcpPayload("at-limit"), []byte(strings.Repeat(" ", 1024-len(mcpPayload("at-limit"))))...),
		[]byte(strings.Repeat("x", 1025)),
	} {
		for _, mcp := range []bool{false, true} {
			for _, reject := range []bool{false, true} {
				f.Add(payload, mcp, reject)
			}
		}
	}
	f.Fuzz(func(t *testing.T, payload []byte, mcp, reject bool) {
		watchCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		target := ConfigTargetLLM
		var validate func(*filterapi.Config) error
		if mcp {
			target = ConfigTargetMCP
			validate = validateMCPConfig
		}
		initialized := make(chan struct{})
		rejected := errors.New("receiver rejected candidate")
		var calls int
		apply := configApplier(watchCtx, t.Context(), 1024, configReceiver{target: target, validate: validate, receiver: receiverFunc(func(ctx context.Context, cfg *filterapi.Config) error {
			calls++
			require.NoError(t, ctx.Err())
			require.LessOrEqual(t, len(payload), 1024, "oversized input reached the loader")
			require.Equal(t, version.Parse(), cfg.Version, "incompatible input reached the loader")
			if mcp {
				require.NotNil(t, cfg.MCPConfig, "missing MCP section reached the loader")
			}
			if reject {
				return rejected
			}
			return nil
		})}, initialized)
		// Repeated delivery must neither acknowledge rejection nor close the
		// initialization channel twice after a successful apply.
		for range 2 {
			before := calls
			err := apply(payload)
			switch {
			case calls == before:
				require.Error(t, err, "acknowledged input without calling the loader")
			case reject:
				require.ErrorIs(t, err, rejected)
			default:
				require.NoError(t, err)
			}
			select {
			case <-initialized:
				require.NoError(t, err, "rejected input satisfied startup readiness")
			default:
				require.Error(t, err, "accepted input did not satisfy startup readiness")
			}
		}
		cancel()
		before := calls
		require.ErrorIs(t, apply(payload), context.Canceled)
		require.Equal(t, before, calls, "canceled subscription called the loader")
	})
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
				if err := apply(mcpPayload("rejected")); err == nil {
					return errors.New("expected receiver rejection")
				}
				close(mcpRejected)
				select {
				case <-ctx.Done():
					return nil
				case <-retryMCP:
				}
				if err := apply(mcpPayload("mcp")); err != nil {
					return err
				}
			}
			<-ctx.Done()
			return nil
		}), nil
	}}
	receivers := []configReceiver{
		{target: ConfigTargetLLM, receiver: receiverFunc(func(context.Context, *filterapi.Config) error { llmCalls.Add(1); return nil })},
		{target: ConfigTargetMCP, validate: validateMCPConfig, receiver: receiverFunc(func(_ context.Context, cfg *filterapi.Config) error {
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
		factory := resolveConfigSubscriptionFactory(ctx, opts, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
		stop, err := startConfigWatchers(ctx, cancel, factory, receivers)
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
			opts := Options{MaxConfigBytes: 1024, ConfigWatcherFactory: func(WatchOptions) (ConfigWatcher, error) {
				return watcherFunc(func(context.Context, ApplyConfig) error { return tc.runErr }), nil
			}}
			factory := resolveConfigSubscriptionFactory(ctx, opts, "", slog.Default())
			_, err := startConfigWatchers(ctx, cancel, factory, []configReceiver{{target: ConfigTargetLLM, receiver: receiverFunc(func(context.Context, *filterapi.Config) error { return nil })}})
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
		opts := Options{MaxConfigBytes: 1024, ConfigWatcherFactory: func(o WatchOptions) (ConfigWatcher, error) {
			if o.Target == ConfigTargetMCP {
				return nil, factoryErr
			}
			return watcherFunc(func(context.Context, ApplyConfig) error { started = true; return nil }), nil
		}}
		factory := resolveConfigSubscriptionFactory(ctx, opts, "", slog.Default())
		_, err := startConfigWatchers(ctx, cancel, factory, []configReceiver{{target: ConfigTargetLLM}, {target: ConfigTargetMCP}})
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
		opts := Options{MaxConfigBytes: 1024, ConfigWatcherFactory: func(WatchOptions) (ConfigWatcher, error) {
			return watcherFunc(func(ctx context.Context, _ ApplyConfig) error {
				close(entered)
				<-ctx.Done()
				close(exited)
				return nil
			}), nil
		}}
		factory := resolveConfigSubscriptionFactory(ctx, opts, "", slog.Default())
		_, err := startConfigWatchers(ctx, cancel, factory, []configReceiver{{target: ConfigTargetLLM}})
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

func TestDefaultConfigSubscriptions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		partial bool
		wantErr string
	}{
		{name: "complete"},
		{name: "missing part", partial: true},
		{name: "checksum mismatch", partial: true},
		{name: "missing index", wantErr: "failed to load initial bundled config"},
		{name: "invalid payload", wantErr: "failed to unmarshal bundled config"},
		{name: "wrong version", wantErr: "config version mismatch"},
		{name: "receiver rejection", wantErr: "receiver rejected configuration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(t.Context())
				defer cancel(nil)
				bundlePath := t.TempDir()
				payload := configPayload("initial")
				switch tc.name {
				case "invalid payload":
					payload = []byte("[broken")
				case "wrong version":
					payload = []byte("version: incompatible\n")
				}
				part := filterapi.ConfigBundlePart{Name: "config", Path: filterapi.ConfigBundlePartPath(0)}
				partPath := filepath.Join(bundlePath, part.Path)
				require.NoError(t, os.MkdirAll(filepath.Dir(partPath), 0o700))
				if tc.name != "missing part" {
					raw := payload
					if tc.name == "checksum mismatch" {
						raw = configPayload("old-generation")
					}
					require.NoError(t, os.WriteFile(partPath, raw, 0o600))
				}
				index, err := filterapi.MarshalConfigBundleIndex(&filterapi.ConfigBundleIndex{
					Checksum: filterapi.ConfigBundleChecksum(payload), Parts: []filterapi.ConfigBundlePart{part},
				})
				require.NoError(t, err)
				if tc.name != "missing index" {
					require.NoError(t, os.WriteFile(filepath.Join(bundlePath, filterapi.ConfigBundleIndexFileName), index, 0o600))
				}
				var llmCalls, mcpCalls atomic.Int32
				receivers := []configReceiver{
					{target: ConfigTargetLLM, receiver: receiverFunc(func(context.Context, *filterapi.Config) error {
						if tc.name == "receiver rejection" {
							return errors.New("receiver rejected configuration")
						}
						llmCalls.Add(1)
						return nil
					})},
					// File mode keeps the existing loader semantics, including an
					// absent MCP section; custom-source validation must not run here.
					{target: ConfigTargetMCP, validate: validateMCPConfig, receiver: receiverFunc(func(context.Context, *filterapi.Config) error {
						mcpCalls.Add(1)
						return nil
					})},
				}
				factory := resolveConfigSubscriptionFactory(ctx, Options{}, bundlePath, slog.New(slog.NewTextHandler(io.Discard, nil)))
				stop, err := startConfigWatchers(ctx, cancel, factory, receivers)
				if tc.wantErr != "" {
					require.ErrorContains(t, err, tc.wantErr)
					require.Nil(t, stop)
					return
				}
				require.NoError(t, err)
				defer stop()
				if tc.partial {
					require.Zero(t, llmCalls.Load(), "legacy file startup must allow a partial bundle")
					require.Zero(t, mcpCalls.Load())
					require.NoError(t, os.WriteFile(partPath, payload, 0o600))
					time.Sleep(5 * time.Second)
					synctest.Wait()
				}
				require.EqualValues(t, 1, llmCalls.Load())
				require.EqualValues(t, 1, mcpCalls.Load())
				stop()
				require.NoError(t, ctx.Err(), "stopping subscriptions must not cancel their caller")
				// A changed file after stop must not reach either receiver.
				stat, err := os.Stat(partPath)
				require.NoError(t, err)
				later := stat.ModTime().Add(time.Second)
				require.NoError(t, os.Chtimes(partPath, later, later))
				time.Sleep(5 * time.Second)
				synctest.Wait()
				require.EqualValues(t, 1, llmCalls.Load())
				require.EqualValues(t, 1, mcpCalls.Load())
			})
		})
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestMainDrainsActiveStream(t *testing.T) {
	// Use a short directory to stay within Unix socket path limits on macOS.
	socketDir, err := os.MkdirTemp("/tmp", "watcher-drain-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(socketDir)) })
	endpoint := "unix://" + filepath.Join(socketDir, "extproc.sock")
	bundlePath := t.TempDir()
	payload := configPayload("drain")
	part := filterapi.ConfigBundlePart{Name: "config", Path: filterapi.ConfigBundlePartPath(0), SizeBytes: len(payload)}
	partPath := filepath.Join(bundlePath, part.Path)
	require.NoError(t, os.MkdirAll(filepath.Dir(partPath), 0o700))
	require.NoError(t, os.WriteFile(partPath, payload, 0o600))
	index, err := filterapi.MarshalConfigBundleIndex(&filterapi.ConfigBundleIndex{
		Checksum: filterapi.ConfigBundleChecksum(payload), Parts: []filterapi.ConfigBundlePart{part},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(bundlePath, filterapi.ConfigBundleIndexFileName), index, 0o600))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ready := make(chan struct{})
	var once sync.Once
	done := make(chan error, 1)
	go func() {
		done <- Main(ctx, []string{"--configBundlePath", bundlePath, "--extProcAddr", endpoint, "--adminPort", "0"},
			writerFunc(func(p []byte) (int, error) {
				if strings.Contains(string(p), "AI Gateway External Processor is ready") {
					once.Do(func() { close(ready) })
				}
				return len(p), nil
			}))
	}()
	select {
	case <-ready:
	case mainErr := <-done:
		t.Fatalf("startup failed: %v", mainErr)
	case <-time.After(5 * time.Second):
		t.Fatal("extproc did not initialize")
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	streamCtx, cancelStream := context.WithCancel(t.Context())
	defer cancelStream()
	stream, err := extprocv3.NewExternalProcessorClient(conn).Process(streamCtx)
	require.NoError(t, err)
	request := &extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_RequestBody{RequestBody: &extprocv3.HttpBody{Body: []byte("probe")}}}
	require.NoError(t, stream.Send(request))
	_, err = stream.Recv()
	require.NoError(t, err, "stream must be active before shutdown")
	cancel()
	// Exceed the removed five-second deadline, then verify the RPC still works.
	select {
	case mainErr := <-done:
		t.Fatalf("Main returned before the active stream drained: %v", mainErr)
	case <-time.After(5200 * time.Millisecond):
	}
	require.NoError(t, stream.Send(request))
	_, err = stream.Recv()
	require.NoError(t, err, "shutdown must not force-close an active stream")
	cancelStream()
	select {
	case mainErr := <-done:
		require.NoError(t, mainErr)
	case <-time.After(5 * time.Second):
		t.Fatal("Main did not return after the stream drained")
	}
}

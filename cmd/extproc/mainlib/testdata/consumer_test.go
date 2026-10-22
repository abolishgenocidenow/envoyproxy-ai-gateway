// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package consumer_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	runtimeapi "github.com/envoyproxy/ai-gateway/api/runtime"
	"github.com/envoyproxy/ai-gateway/cmd/extproc/mainlib"
)

type watcher struct{ version string }

func (w watcher) Run(ctx context.Context, apply mainlib.ApplyConfig) error {
	if err := apply([]byte(fmt.Sprintf("version: %s\nuuid: external-consumer\n", w.version))); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

type readyWriter struct{ cancel context.CancelFunc }

func (w readyWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "External Processor is ready") {
		w.cancel()
	}
	return len(p), nil
}

func TestEmbedMainlib(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "true")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	applied := false
	err := mainlib.MainWithOptions(ctx, []string{"--extProcAddr", "127.0.0.1:0", "--adminPort", "0"}, readyWriter{cancel}, mainlib.Options{
		MaxConfigBytes: 1024,
		CredentialResolver: func(context.Context, runtimeapi.CredentialAttempt) ([]runtimeapi.CredentialHeader, error) {
			return nil, io.EOF
		},
		ConfigWatcherFactory: func(o mainlib.WatchOptions) (mainlib.ConfigWatcher, error) {
			if o.Target != mainlib.ConfigTargetLLM {
				t.Errorf("unexpected consumer %s", o.Target)
			}
			applied = true
			return watcher{o.PayloadVersion}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("watcher was not constructed")
	}
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatal("startup did not reach readiness")
	}
}

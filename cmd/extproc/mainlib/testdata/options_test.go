// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package consumer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimeapi "github.com/envoyproxy/ai-gateway/api/runtime"
	"github.com/envoyproxy/ai-gateway/cmd/extproc/mainlib"
	"github.com/envoyproxy/ai-gateway/mcp"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

type hostOptionsRouting struct{}

func (hostOptionsRouting) Resolve(_ context.Context, r *extprocv3.ProcessingRequest) (runtimeapi.ExtProcRequest, error) {
	fields := r.GetMetadataContext().GetFilterMetadata()["host"].GetFields()
	return runtimeapi.ExtProcRequest{Path: "/v1/chat/completions", Upstream: fields["upstream"].GetBoolValue(), RequestID: fields["id"].GetStringValue(), ConfigID: fields["config"].GetStringValue(), Backend: "provider", Route: "route"}, nil
}
func (hostOptionsRouting) Handoff(_ context.Context, h runtimeapi.RoutingHandoff) (*structpb.Struct, error) {
	return structpb.NewStruct(map[string]any{"host": map[string]any{"id": h.RequestID, "config": h.ConfigID, "model": h.Model}})
}

type optionsWatcher struct{ options mainlib.WatchOptions }

func (w optionsWatcher) Run(ctx context.Context, apply mainlib.ApplyConfig) error {
	section := "backends:\n- name: provider\n  schema:\n    name: OpenAI\n  modelNameOverride: served-model\n"
	if w.options.Target == mainlib.ConfigTargetMCP {
		section = "mcpConfig:\n  routes:\n  - name: route\n    backends:\n    - name: backend\n"
	}
	if err := apply([]byte(fmt.Sprintf("version: %s\nuuid: generation-1\n%s", w.options.PayloadVersion, section))); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

type readySignal struct {
	once  sync.Once
	ready chan struct{}
}

func (w *readySignal) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "External Processor is ready") {
		w.once.Do(func() { close(w.ready) })
	}
	return len(p), nil
}

type optionsTransport struct{ calls atomic.Int32 }

func (t *optionsTransport) RoundTrip(r mcp.BackendRequest) (*http.Response, error) {
	if r.Route != "route" || r.Backend != "backend" || r.Request.URL.Host != "" || r.Request.URL.Scheme != "" {
		return nil, fmt.Errorf("unexpected logical destination")
	}
	var message struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if err := json.NewDecoder(r.Request.Body).Decode(&message); err != nil {
		return nil, err
	}
	t.calls.Add(1)
	code := 200
	body := ""
	headers := http.Header{"Content-Type": []string{"application/json"}}
	switch message.Method {
	case "initialize":
		headers.Set("Mcp-Session-Id", "backend-session")
		body = fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"host","version":"1"}}}`, message.ID)
	case "notifications/initialized":
		code = 202
	default:
		return nil, fmt.Errorf("unexpected method")
	}
	return &http.Response{StatusCode: code, Header: headers, Body: io.NopCloser(strings.NewReader(body))}, nil
}

// A distinct module drives actual gRPC and HTTP listeners to prove option
// propagation through mainlib, using no Envoy or provider network endpoint.
func TestMainlibHostOptions(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "true")
	dir, err := os.MkdirTemp("/tmp", "agn-host-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	grpcPath, mcpPath := filepath.Join(dir, "grpc.sock"), filepath.Join(dir, "mcp.sock")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	crypto, err := mcp.NewSessionCrypto("test-only", 100)
	if err != nil {
		t.Fatal(err)
	}
	transport := &optionsTransport{}
	ready := &readySignal{ready: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- mainlib.MainWithOptions(ctx, []string{"--extProcAddr", "unix://" + grpcPath, "--adminPort", "0", "--mcpAddr", "unix://" + mcpPath}, ready, mainlib.Options{
			Routing: hostOptionsRouting{}, MaxConfigBytes: 4096,
			ConfigWatcherFactory: func(o mainlib.WatchOptions) (mainlib.ConfigWatcher, error) { return optionsWatcher{o}, nil },
			MCP: &mcp.Options{SessionCrypto: crypto, Transport: transport, Select: func(r *http.Request) (mcp.Selection, error) {
				if r.Header.Get("x-ai-eg-mcp-route") != "" {
					return mcp.Selection{}, fmt.Errorf("spoofed routing header survived")
				}
				return mcp.Selection{Route: "route", Subject: "trusted"}, nil
			}},
		})
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("mainlib did not stop")
		}
	}()
	select {
	case <-ready.ready:
	case err := <-done:
		done <- err
		t.Fatalf("mainlib startup: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	conn, err := grpc.NewClient("unix://"+grpcPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := extprocv3.NewExternalProcessorClient(conn)
	downstream, err := client.Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	headers := func(md *structpb.Struct) *extprocv3.ProcessingRequest {
		return &extprocv3.ProcessingRequest{MetadataContext: &corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{"host": md}}, Request: &extprocv3.ProcessingRequest_RequestHeaders{RequestHeaders: &extprocv3.HttpHeaders{Headers: &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: ":path", RawValue: []byte("/client-spoof")}, {Key: "content-type", RawValue: []byte("application/json")}}}}}}
	}
	if err = downstream.Send(headers(nil)); err != nil {
		t.Fatal(err)
	}
	first, err := downstream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	generation := first.GetDynamicMetadata().GetFields()["host"].GetStructValue().GetFields()["config"].GetStringValue()
	id := first.GetDynamicMetadata().GetFields()["host"].GetStructValue().GetFields()["id"].GetStringValue()
	if id == "" {
		t.Fatal("host handoff missing")
	}
	if err = downstream.Send(&extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_RequestBody{RequestBody: &extprocv3.HttpBody{Body: []byte(`{"model":"public-model","messages":[{"role":"user","content":"hello"}]}`), EndOfStream: true}}}); err != nil {
		t.Fatal(err)
	}
	parsed, err := downstream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if parsed.GetRequestBody().GetResponse().GetClearRouteCache() {
		t.Fatal("host mode cleared Envoy route cache")
	}
	metadata, _ := structpb.NewStruct(map[string]any{"upstream": true, "id": id, "config": generation})
	upstream, err := client.Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = upstream.Send(headers(metadata)); err != nil {
		t.Fatal(err)
	}
	response, err := upstream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(response.GetRequestHeaders().GetResponse().GetBodyMutation().GetBody()), "served-model") {
		t.Fatal("host backend selection was not applied")
	}
	_ = upstream.CloseSend()
	_, _ = upstream.Recv()
	_ = downstream.CloseSend()
	_, _ = downstream.Recv()
	httpTransport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", mcpPath)
	}}
	defer httpTransport.CloseIdleConnections()
	httpClient := &http.Client{Transport: httpTransport}
	request, err := http.NewRequestWithContext(ctx, "POST", "http://host/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("x-ai-eg-mcp-route", "spoofed")
	result, err := httpClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Body.Close()
	payload, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatal(err)
	}
	if result.StatusCode != 200 || transport.calls.Load() != 2 {
		t.Fatalf("MCP options not applied: status=%d calls=%d body=%s", result.StatusCode, transport.calls.Load(), payload)
	}
}

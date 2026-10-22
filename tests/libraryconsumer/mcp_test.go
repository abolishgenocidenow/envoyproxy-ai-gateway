// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package libraryconsumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/envoyproxy/ai-gateway/mcp"
)

// The only fake is the upstream transport. Requests enter the production mux,
// including its actual legacy POST dispatch, session crypto and authorization.
func TestExecutionContract_MCPSession(t *testing.T) {
	var steps []struct {
		Name           string          `json:"name"`
		ContentType    string          `json:"contentType"`
		Request        json.RawMessage `json:"request"`
		Status         int             `json:"status"`
		UpstreamCalls  int             `json:"upstreamCalls"`
		Tools          []string        `json:"tools"`
		Result         json.RawMessage `json:"result"`
		Error          string          `json:"error"`
		Subject        string          `json:"subject"`
		CorruptSession bool            `json:"corruptSession"`
		OmitSession    bool            `json:"omitSession"`
		DenyAll        bool            `json:"denyAll"`
	}
	raw, err := os.ReadFile("../../internal/mcpproxy/testdata/execution/session.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &steps))
	crypto, err := mcp.NewSessionCrypto("fixture-only", 100)
	require.NoError(t, err)
	upstream := &executionUpstream{}
	proxy, err := mcp.New(mcp.Options{SessionCrypto: crypto, Transport: upstream, Select: func(r *http.Request) (mcp.Selection, error) {
		selected, ok := r.Context().Value(selectionKey{}).(mcp.Selection)
		if !ok {
			return mcp.Selection{}, fmt.Errorf("missing trusted context")
		}
		require.Empty(t, r.Header.Get("x-ai-eg-mcp-route"))
		return selected, nil
	}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proxy.Close(context.Background())) })
	routes := []mcp.Route{{Name: "fixture-route", Backends: []mcp.Backend{{Name: "backend"}}, Authorization: &mcp.Authorization{DefaultAction: "Deny", Rules: []mcp.Rule{{Action: "Allow", Target: &mcp.Target{Tools: []mcp.ToolCall{{Backend: "backend", Tool: "allowed"}}}}}}}}
	require.NoError(t, proxy.Configure(t.Context(), routes))

	var sessionID string
	for _, step := range steps {
		// Each step belongs to this conversation, not an independently runnable subtest.
		t.Log(step.Name)
		func() {
			if step.DenyAll {
				routes[0].Authorization.Rules = nil
				require.NoError(t, proxy.Configure(t.Context(), routes))
			}
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/mcp", bytes.NewReader(step.Request))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "application/json, text/event-stream")
			// These model trusted Envoy inputs, not client-controlled authentication.
			request.Header.Set("x-ai-eg-mcp-route", "spoofed-route")
			subject := step.Subject
			if subject == "" {
				subject = "fixture-user"
			}
			request = request.WithContext(context.WithValue(request.Context(), selectionKey{}, mcp.Selection{Route: "fixture-route", Subject: subject, Attributes: map[string]string{"tenant": "fixture"}}))
			request.Header.Set("x-ai-eg-mcp-subject", "spoofed-subject")
			if sessionID != "" && !step.OmitSession {
				request.Header.Set(sessionIDHeader, sessionID)
			}
			if step.CorruptSession {
				request.Header.Set(sessionIDHeader, "invalid-session")
			}
			before := upstream.count()
			recorder := httptest.NewRecorder()
			proxy.ServeHTTP(recorder, request)
			require.Equal(t, step.Status, recorder.Code, recorder.Body.String())
			require.Equal(t, step.UpstreamCalls, upstream.count()-before)
			if step.Error != "" {
				require.Contains(t, recorder.Body.String(), step.Error)
				return
			}
			var response struct {
				ID     json.RawMessage `json:"id"`
				Result json.RawMessage `json:"result"`
			}
			payload := recorder.Body.Bytes()
			require.Equal(t, step.ContentType, recorder.Header().Get("Content-Type"))
			if step.ContentType == "text/event-stream" {
				require.Contains(t, recorder.Body.String(), "event: message\n")
				require.Regexp(t, `(?m)^id: .+$`, recorder.Body.String())
				var data []string
				for _, line := range strings.Split(recorder.Body.String(), "\n") {
					if strings.HasPrefix(line, "data: ") {
						data = append(data, strings.TrimPrefix(line, "data: "))
					}
				}
				require.Len(t, data, 1, "one JSON-RPC result event")
				payload = []byte(data[0])
			}
			require.NoError(t, json.Unmarshal(payload, &response))
			var input struct {
				ID json.RawMessage `json:"id"`
			}
			require.NoError(t, json.Unmarshal(step.Request, &input))
			require.JSONEq(t, string(input.ID), string(response.ID))
			if step.Name == "initialize" {
				sessionID = recorder.Header().Get(sessionIDHeader)
				require.NotEmpty(t, sessionID)
				require.NotEqual(t, "upstream-session", sessionID)
				var result struct {
					ProtocolVersion string `json:"protocolVersion"`
				}
				require.NoError(t, json.Unmarshal(response.Result, &result))
				require.Equal(t, "2025-06-18", result.ProtocolVersion)
			}
			if step.Tools != nil {
				var result struct {
					Tools []struct {
						Name string `json:"name"`
					} `json:"tools"`
				}
				require.NoError(t, json.Unmarshal(response.Result, &result))
				names := make([]string, 0, len(result.Tools))
				for _, tool := range result.Tools {
					names = append(names, tool.Name)
				}
				require.ElementsMatch(t, step.Tools, names)
			}
			if step.Result != nil {
				require.JSONEq(t, string(step.Result), string(response.Result))
			}
		}()
	}
}

// executionUpstream never opens sockets and rejects unexpected destinations,
// session identities and tool calls. No environment credentials are consulted.
type executionUpstream struct {
	mu    sync.Mutex
	calls int
}

func (u *executionUpstream) count() int { u.mu.Lock(); defer u.mu.Unlock(); return u.calls }

func (u *executionUpstream) RoundTrip(selected mcp.BackendRequest) (*http.Response, error) {
	r := selected.Request
	if selected.Metadata.Route != "fixture-route" || selected.Metadata.Backend != "backend" || selected.Metadata.Attributes["tenant"] != "fixture" {
		return nil, fmt.Errorf("trusted metadata lost")
	}
	selected.Metadata.Attributes["tenant"] = "changed-by-transport"
	u.mu.Lock()
	defer u.mu.Unlock()
	u.calls++
	defer r.Body.Close()
	if selected.Route != "fixture-route" || selected.Backend != "backend" || r.URL.Host != "" {
		return nil, fmt.Errorf("unexpected upstream destination")
	}
	for key := range r.Header {
		if strings.HasPrefix(strings.ToLower(key), "x-ai-eg-") {
			return nil, fmt.Errorf("internal header leaked")
		}
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	var message struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err = json.Unmarshal(body, &message); err != nil {
		return nil, err
	}
	headers := http.Header{"Content-Type": []string{"application/json"}}
	status := http.StatusOK
	var result string
	switch message.Method {
	case "initialize":
		if r.Header.Get(sessionIDHeader) != "" {
			return nil, fmt.Errorf("unexpected initial session")
		}
		headers.Set(sessionIDHeader, "upstream-session")
		result = `{"protocolVersion":"2025-06-18","capabilities":{"tools":{"listChanged":true}},"serverInfo":{"name":"fixture","version":"1"}}`
	case "notifications/initialized":
		status = http.StatusAccepted
	case "tools/list":
		result = `{"tools":[{"name":"allowed","inputSchema":{"type":"object"}},{"name":"denied","inputSchema":{"type":"object"}}]}`
	case "tools/call":
		var params struct {
			Name      string            `json:"name"`
			Arguments map[string]string `json:"arguments"`
		}
		if err = json.Unmarshal(message.Params, &params); err != nil {
			return nil, err
		}
		if params.Name != "allowed" || params.Arguments["value"] != "hello" {
			return nil, fmt.Errorf("unexpected tool call: %s", message.Params)
		}
		result = `{"content":[{"type":"text","text":"hello"}]}`
	default:
		return nil, fmt.Errorf("unexpected method: %s", message.Method)
	}
	if message.Method != "initialize" && r.Header.Get(sessionIDHeader) != "upstream-session" {
		return nil, fmt.Errorf("wrong upstream session")
	}
	response := ""
	if result != "" {
		response = fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":%s}`, message.ID, result)
	}
	return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(response)), Request: r}, nil
}

type selectionKey struct{}

const sessionIDHeader = "Mcp-Session-Id"

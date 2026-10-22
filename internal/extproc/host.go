// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package extproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	runtimeapi "github.com/envoyproxy/ai-gateway/api/runtime"
	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
)

// hostRequest retains one generation and serializes access to the shared router
// and current attempt. Concurrent upstream attempts are rejected: response
// ownership in the existing processor supports sequential retries only.
type hostRequest struct {
	mu               sync.Mutex
	router           Processor
	config           *filterapi.RuntimeConfig
	handoff          runtimeapi.RoutingHandoff
	closed, upstream bool
}

func (s *Server) processWithHost(stream extprocv3.ExternalProcessor_ProcessServer) error {
	ctx := context.WithValue(stream.Context(), loggerContextKey, s.logger)
	var state *hostRequest
	var p Processor = passThroughProcessor{}
	var selected runtimeapi.ExtProcRequest
	initialized := false
	defer func() {
		if state == nil {
			return
		}
		if !selected.Upstream {
			s.hostRequestsMu.Lock()
			delete(s.hostRequests, state.handoff.RequestID)
			s.hostRequestsMu.Unlock()
		}
		state.mu.Lock()
		defer state.mu.Unlock()
		if selected.Upstream {
			state.upstream = false
		} else {
			state.closed = true
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
			return nil
		}
		if err != nil {
			return status.Error(codes.Unknown, "cannot receive processing request")
		}
		if req.GetRequestHeaders() != nil {
			if initialized {
				return status.Error(codes.InvalidArgument, "duplicate request headers")
			}
			initialized = true
			selected, err = s.routing.Resolve(ctx, proto.Clone(req).(*extprocv3.ProcessingRequest))
			if canceled := ctx.Err(); canceled != nil {
				return canceled
			}
			if err != nil {
				return status.Error(codes.PermissionDenied, "host selection rejected")
			}
			headers := headersToMap(req.GetRequestHeaders().GetHeaders())
			for key := range headers {
				if hostRoutingHeader(key) {
					delete(headers, key)
				}
			}
			if selected.Upstream {
				if selected.RequestID == "" || selected.ConfigID == "" || selected.Backend == "" {
					return status.Error(codes.InvalidArgument, "incomplete host selection")
				}
				s.hostRequestsMu.Lock()
				retained := s.hostRequests[selected.RequestID]
				s.hostRequestsMu.Unlock()
				if retained == nil {
					return status.Error(codes.NotFound, "unknown host request")
				}
				retained.mu.Lock()
				if retained.closed || retained.upstream || !retained.handoff.BodyReady {
					retained.mu.Unlock()
					return status.Error(codes.FailedPrecondition, "host request is not available for an attempt")
				}
				if selected.ConfigID != retained.handoff.ConfigID || (selected.Path != "" && selected.Path != retained.handoff.Path) {
					retained.mu.Unlock()
					return status.Error(codes.FailedPrecondition, "host request generation or operation mismatch")
				}
				backend := retained.config.Backends[selected.Backend]
				if backend == nil {
					retained.mu.Unlock()
					return status.Error(codes.NotFound, "unknown backend in captured configuration")
				}
				headers[originalPathHeader] = retained.handoff.Path
				if selected.UpstreamHost != "" {
					headers[internalapi.UpstreamHostHeader] = selected.UpstreamHost
				}
				p, err = s.hostProcessor(retained.config, headers, retained.handoff.Path, true, retained.handoff.RequestID)
				if err == nil {
					err = p.SetBackend(ctx, backend, selected.Route, retained.router)
				}
				if err == nil {
					retained.upstream = true
					state = retained
				}
				retained.mu.Unlock()
				if err != nil {
					return status.Error(codes.Internal, "cannot prepare host attempt")
				}
			} else {
				if !strings.HasPrefix(selected.Path, "/") || len(selected.Path) > 4096 || selected.RequestID != "" {
					return status.Error(codes.InvalidArgument, "invalid host operation")
				}
				captured, generation := s.currentHostConfig()
				if captured == nil || captured.UUID == "" {
					return status.Error(codes.Unavailable, "host configuration is not ready")
				}
				headers[":path"] = selected.Path
				requestID := s.uuidFn()
				p, err = s.hostProcessor(captured, headers, selected.Path, false, requestID)
				if err != nil {
					return status.Error(codes.NotFound, "unsupported host operation")
				}
				state = &hostRequest{router: p, config: captured, handoff: runtimeapi.RoutingHandoff{
					RequestID: requestID, ConfigID: generation, ConfigUUID: captured.UUID, Path: selected.Path,
				}}
				s.hostRequestsMu.Lock()
				if _, exists := s.hostRequests[state.handoff.RequestID]; exists {
					s.hostRequestsMu.Unlock()
					state = nil
					return status.Error(codes.AlreadyExists, "host request identity collision")
				}
				s.hostRequests[state.handoff.RequestID] = state
				s.hostRequestsMu.Unlock()
			}
			ctx = context.WithValue(ctx, loggerContextKey, s.logger.With("request_id", state.handoff.RequestID, "is_upstream_filter", selected.Upstream))
		}
		var resp *extprocv3.ProcessingResponse
		if state != nil {
			state.mu.Lock()
			if state.closed {
				state.mu.Unlock()
				return status.Error(codes.Canceled, "host request closed")
			}
			resp, err = s.processMsg(ctx, p, req, state.handoff.RequestID, selected.Upstream)
			if err == nil && !selected.Upstream {
				err = s.hostHandoff(ctx, state, resp)
			}
			if err != nil && !selected.Upstream {
				state.closed = true
			}
			state.mu.Unlock()
		} else {
			resp, err = s.processMsg(ctx, p, req, "", false)
		}
		if err != nil {
			return status.Error(codes.Internal, "host processing failed")
		}
		if err = stream.Send(resp); err != nil {
			return status.Error(codes.Unknown, "cannot send processing response")
		}
	}
}

func (s *Server) hostProcessor(config *filterapi.RuntimeConfig, headers map[string]string, path string, upstream bool, requestID string) (Processor, error) {
	path, _, _ = strings.Cut(path, "?")
	factory := s.processorFactories[path]
	if factory == nil {
		return nil, errNoProcessor
	}
	return factory(config, headers, s.logger.With("request_id", requestID, "is_upstream_filter", upstream, "config_uuid", config.UUID), upstream, s.enableRedaction)
}

func hostRoutingHeader(name string) bool {
	switch strings.ToLower(name) {
	case internalReqIDHeader, originalPathHeader, internalapi.EnvoyOriginalPathHeader,
		internalapi.ModelNameHeaderKeyDefault, internalapi.UpstreamHostHeader, internalapi.EndpointPickerHeaderKey:
		return true
	}
	return false
}

func (s *Server) hostHandoff(ctx context.Context, state *hostRequest, resp *extprocv3.ProcessingResponse) error {
	if resp == nil {
		return nil
	}
	var common *extprocv3.CommonResponse
	switch value := resp.Response.(type) {
	case *extprocv3.ProcessingResponse_RequestHeaders:
		if value.RequestHeaders == nil {
			value.RequestHeaders = &extprocv3.HeadersResponse{}
		}
		if value.RequestHeaders.Response == nil {
			value.RequestHeaders.Response = &extprocv3.CommonResponse{}
		}
		common = value.RequestHeaders.Response
	case *extprocv3.ProcessingResponse_RequestBody:
		if value.RequestBody == nil {
			value.RequestBody = &extprocv3.BodyResponse{}
		}
		if value.RequestBody.Response == nil {
			value.RequestBody.Response = &extprocv3.CommonResponse{}
		}
		common = value.RequestBody.Response
		state.handoff.BodyReady = true
	}
	if common == nil {
		return nil
	} // Immediate replies do not become routing decisions.
	if common.HeaderMutation == nil {
		common.HeaderMutation = &extprocv3.HeaderMutation{}
	}
	mutation := common.HeaderMutation
	kept := mutation.SetHeaders[:0]
	for _, h := range mutation.SetHeaders {
		if strings.EqualFold(h.GetHeader().GetKey(), internalapi.ModelNameHeaderKeyDefault) {
			state.handoff.Model = string(h.GetHeader().GetRawValue())
		}
		if !hostRoutingHeader(h.GetHeader().GetKey()) {
			kept = append(kept, h)
		}
	}
	mutation.SetHeaders = kept
	// Remove spoofed copies from the wire as well as the processor's input map.
	mutation.RemoveHeaders = append(mutation.RemoveHeaders, internalReqIDHeader, originalPathHeader,
		internalapi.EnvoyOriginalPathHeader, internalapi.ModelNameHeaderKeyDefault, internalapi.UpstreamHostHeader, internalapi.EndpointPickerHeaderKey)
	common.ClearRouteCache = false
	metadata, err := s.routing.Handoff(ctx, state.handoff)
	if err != nil {
		return errors.New("host handoff rejected")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if metadata == nil {
		return nil
	}
	if proto.Size(metadata) > 64*1024 {
		return errors.New("host metadata exceeds limit")
	}
	copied := proto.Clone(metadata).(*structpb.Struct)
	if resp.DynamicMetadata == nil {
		resp.DynamicMetadata = copied
	} else {
		if resp.DynamicMetadata.Fields == nil {
			resp.DynamicMetadata.Fields = make(map[string]*structpb.Value)
		}
		for key, value := range copied.Fields {
			resp.DynamicMetadata.Fields[key] = value
		}
	}
	return nil
}

func (s *Server) currentHostConfig() (*filterapi.RuntimeConfig, string) {
	s.configMutex.RLock()
	defer s.configMutex.RUnlock()
	if s.config == nil {
		return nil, ""
	}
	return s.config, fmt.Sprintf("%s/%d", s.config.UUID, s.configGeneration)
}

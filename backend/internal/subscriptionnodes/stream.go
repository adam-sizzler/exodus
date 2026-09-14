package subscriptionnodes

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	subscriptionapi "exodus/internal/httpapi/subscription"
	systemapi "exodus/internal/httpapi/system"
	"exodus/internal/proto"
	srscore "exodus/internal/srslists"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (sm *SubNodeMonitor) receiveStream(state *subNodeState) {
	for {
		state.mutex.RLock()
		stream := state.stream
		state.mutex.RUnlock()
		if stream == nil {
			sm.handleDisconnect(state, "Stream unavailable")
			return
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			sm.handleDisconnect(state, "Stream closed")
			return
		}
		if err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Canceled && state.ctx.Err() != nil {
					return
				}
				if st.Code() == codes.Unavailable {
					sm.handleDisconnect(state, "Node unavailable")
					return
				}
			}
			sm.handleDisconnect(state, fmt.Sprintf("Stream error: %v", err))
			return
		}

		sm.markStreamActivity(state)

		if err := sm.processResponse(state, resp); err != nil {
			sm.handleDisconnect(state, err.Error())
			return
		}
	}
}

func (sm *SubNodeMonitor) processResponse(state *subNodeState, resp *proto.NodeDataResponse) error {
	switch payload := resp.Response.(type) {
	case *proto.NodeDataResponse_Stats:
		sm.updateRuntimeFromStats(state.nodeName, payload.Stats.GetStats())
		return nil
	case *proto.NodeDataResponse_SubscriptionRequest:
		response := sm.handleSubscriptionBridgeRequest(state, payload.SubscriptionRequest)
		err := sm.sendNodeRequest(state, &proto.NodeDataRequest{
			Request: &proto.NodeDataRequest_SubscriptionResponse{
				SubscriptionResponse: response,
			},
		})
		if err != nil {
			sm.cfg.Logger.Warn(
				"Failed to send subscription bridge response to node",
				"node", state.nodeName,
				"error", err,
			)
			return err
		}
		return nil
	default:
		return nil
	}
}

func (sm *SubNodeMonitor) handleSubscriptionBridgeRequest(state *subNodeState, req *proto.SubscriptionBridgeRequest) *proto.SubscriptionBridgeResponse {
	if req == nil {
		return &proto.SubscriptionBridgeResponse{
			RequestId:  "",
			StatusCode: http.StatusBadRequest,
			Error:      "empty bridge request",
		}
	}

	requestID := strings.TrimSpace(req.GetRequestId())
	operation := strings.ToLower(strings.TrimSpace(req.GetOperation()))

	ctx, cancel := context.WithTimeout(state.ctx, 15*time.Second)
	defer cancel()

	switch operation {
	case bridgeOperationSubscriptionInfo:
		shortUUID := strings.TrimSpace(req.GetShortUuid())
		if shortUUID == "" {
			return &proto.SubscriptionBridgeResponse{RequestId: requestID, StatusCode: http.StatusBadRequest, Error: "short_uuid is required"}
		}
		path := "/api/sub/" + shortUUID + "/info"
		statusCode, payload, headers := sm.performInternalAPIRequest(ctx, http.MethodGet, path, nil, sm.bridgeHeadersWithClientIP(req))
		return buildBridgeResponse(requestID, statusCode, payload, headers)

	case bridgeOperationSubscriptionContent:
		shortUUID := strings.TrimSpace(req.GetShortUuid())
		if shortUUID == "" {
			return &proto.SubscriptionBridgeResponse{RequestId: requestID, StatusCode: http.StatusBadRequest, Error: "short_uuid is required"}
		}
		clientType := strings.TrimSpace(req.GetClientType())
		path := "/api/sub/" + shortUUID
		if clientType != "" {
			path += "/" + clientType
		}
		statusCode, payload, headers := sm.performInternalAPIRequest(ctx, http.MethodGet, path, nil, sm.bridgeHeadersWithClientIP(req))
		return buildBridgeResponse(requestID, statusCode, payload, headers)

	case bridgeOperationSubpageByShortUUID:
		shortUUID := strings.TrimSpace(req.GetShortUuid())
		if shortUUID == "" {
			return &proto.SubscriptionBridgeResponse{RequestId: requestID, StatusCode: http.StatusBadRequest, Error: "short_uuid is required"}
		}
		path := "/api/subscriptions/subpage-config/" + shortUUID
		payload := req.GetPayload()
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		statusCode, body, headers := sm.performInternalAPIRequest(ctx, http.MethodPost, path, payload, sm.bridgeHeadersWithClientIP(req))
		return buildBridgeResponse(requestID, statusCode, body, headers)

	case bridgeOperationSubpageByUUID:
		uuidValue := strings.TrimSpace(req.GetSubpageConfigUuid())
		if uuidValue == "" {
			return &proto.SubscriptionBridgeResponse{RequestId: requestID, StatusCode: http.StatusBadRequest, Error: "subpage_config_uuid is required"}
		}
		configPayload, err := sm.fetchSubpageConfigRaw(ctx, uuidValue)
		if err != nil {
			if err == sql.ErrNoRows {
				return &proto.SubscriptionBridgeResponse{RequestId: requestID, StatusCode: http.StatusNotFound, Error: "subpage config not found"}
			}
			return &proto.SubscriptionBridgeResponse{RequestId: requestID, StatusCode: http.StatusInternalServerError, Error: err.Error()}
		}
		return &proto.SubscriptionBridgeResponse{
			RequestId:  requestID,
			StatusCode: http.StatusOK,
			Payload:    configPayload,
			Headers: []*proto.Header{
				{Key: "Content-Type", Value: "application/json; charset=utf-8"},
			},
		}
	default:
		return &proto.SubscriptionBridgeResponse{
			RequestId:  requestID,
			StatusCode: http.StatusNotImplemented,
			Error:      "unknown bridge operation",
		}
	}
}

func (sm *SubNodeMonitor) bridgeHeadersWithClientIP(req *proto.SubscriptionBridgeRequest) []*proto.Header {
	headersMap := sm.flattenRequestHeaders(req.GetHeaders())
	clientIP := strings.TrimSpace(req.GetClientIp())
	if clientIP != "" {
		headersMap[exodusRealIPHeader] = clientIP
	}

	result := make([]*proto.Header, 0, len(headersMap))
	for k, v := range headersMap {
		result = append(result, &proto.Header{Key: k, Value: v})
	}
	return result
}

func (sm *SubNodeMonitor) flattenRequestHeaders(headers []*proto.Header) map[string]string {
	result := make(map[string]string)
	for _, header := range headers {
		if header == nil {
			continue
		}
		key := strings.TrimSpace(header.GetKey())
		if key == "" {
			continue
		}
		if _, exists := result[key]; exists {
			continue
		}
		result[key] = header.GetValue()
	}
	return result
}

func (sm *SubNodeMonitor) performInternalAPIRequest(
	ctx context.Context,
	method,
	path string,
	body []byte,
	headers []*proto.Header,
) (int, []byte, http.Header) {
	handler := sm.resolveInternalHandler(path)
	if handler == nil {
		return http.StatusNotFound, []byte(`{"error":"not found"}`), http.Header{}
	}

	request, err := http.NewRequestWithContext(ctx, method, path, bytes.NewReader(body))
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error()), http.Header{}
	}
	if len(body) > 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	for _, header := range headers {
		if header == nil {
			continue
		}
		key := strings.TrimSpace(header.GetKey())
		if key == "" {
			continue
		}
		request.Header.Add(key, header.GetValue())
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	result := recorder.Result()
	defer result.Body.Close()

	responseBody, readErr := io.ReadAll(result.Body)
	if readErr != nil {
		return http.StatusInternalServerError, []byte(readErr.Error()), result.Header.Clone()
	}

	return result.StatusCode, responseBody, result.Header.Clone()
}

func (sm *SubNodeMonitor) resolveInternalHandler(path string) http.HandlerFunc {
	switch {
	case path == "/api/system/metadata":
		return systemapi.MetadataHandler(sm.cfg)
	case strings.HasPrefix(path, "/api/subscriptions/subpage-config/") || path == "/api/subscriptions/subpage-config":
		return subscriptionapi.SubpageConfigPublicHandler(sm.db, sm.db, sm.cfg)
	case strings.HasPrefix(path, "/api/sub/") || path == "/api/sub" || strings.HasPrefix(path, "/api/subscriptions/") || path == "/api/subscriptions":
		return subscriptionapi.SubscriptionPublicHandler(sm.db, sm.db, sm.cfg)
	default:
		return nil
	}
}

func (sm *SubNodeMonitor) fetchSubpageConfigRaw(ctx context.Context, uuidValue string) ([]byte, error) {
	var payload string
	err := sm.db.QueryRowContext(ctx, `
		SELECT config
		FROM subscription_page_config
		WHERE uuid = $1
		LIMIT 1
	`, uuidValue).Scan(&payload)
	if err != nil {
		return nil, err
	}

	raw := []byte(strings.TrimSpace(payload))
	if len(raw) == 0 || !json.Valid(raw) {
		return nil, fmt.Errorf("invalid subpage config payload")
	}

	return raw, nil
}

func buildBridgeResponse(requestID string, statusCode int, body []byte, headers http.Header) *proto.SubscriptionBridgeResponse {
	response := &proto.SubscriptionBridgeResponse{
		RequestId:  requestID,
		StatusCode: int32(statusCode),
		Payload:    body,
		Headers:    toProtoHeaders(headers),
	}
	if statusCode < 200 || statusCode >= 300 {
		trimmed := strings.TrimSpace(string(body))
		if trimmed == "" {
			trimmed = http.StatusText(statusCode)
		}
		response.Error = trimmed
	}
	return response
}

func toProtoHeaders(headers http.Header) []*proto.Header {
	if len(headers) == 0 {
		return nil
	}
	result := make([]*proto.Header, 0, len(headers))
	for key, values := range headers {
		for _, value := range values {
			result = append(result, &proto.Header{Key: key, Value: value})
		}
	}
	return result
}

func (sm *SubNodeMonitor) updateRuntimeFromStats(nodeName string, stats []*proto.Stat) {
	cleanNodeName := strings.TrimSpace(nodeName)
	if cleanNodeName == "" || len(stats) == 0 {
		return
	}

	var (
		subNodeVersion string
		nodeVersion    string
		singboxVersion string
		subNodeUptime  string
		singboxUptime  string
		cpuCountStr    string
		cpuModelStr    string
		totalRAMStr    string
		hasAny         bool
	)

	for _, stat := range stats {
		if stat == nil {
			continue
		}
		name := strings.TrimSpace(stat.GetName())
		if name == "" {
			continue
		}
		val := strings.TrimSpace(stat.GetValue())

		switch {
		case strings.EqualFold(name, subNodeRuntimeStatVersion):
			subNodeVersion = val
			hasAny = true
		case strings.EqualFold(name, "node_version"):
			nodeVersion = val
			hasAny = true
		case strings.EqualFold(name, "singbox_version"):
			singboxVersion = val
			hasAny = true
		case strings.EqualFold(name, subNodeRuntimeStatUptime):
			subNodeUptime = val
			hasAny = true
		case strings.EqualFold(name, "singbox_uptime"):
			singboxUptime = val
			hasAny = true
		case strings.EqualFold(name, subNodeRuntimeStatCPUCount):
			cpuCountStr = val
			hasAny = true
		case strings.EqualFold(name, subNodeRuntimeStatCPUModel):
			cpuModelStr = val
			hasAny = true
		case strings.EqualFold(name, subNodeRuntimeStatTotalRAM):
			totalRAMStr = val
			hasAny = true
		}
	}

	if !hasAny {
		return
	}

	sm.runtimeMu.Lock()
	runtime := sm.runtimeByNodeName[cleanNodeName]
	if runtime.SingboxUptime == "" {
		runtime.SingboxUptime = "0"
	}

	if version, ok := normalizeSubNodeRuntimeVersion(firstNonEmptyString(subNodeVersion, nodeVersion)); ok {
		runtime.NodeVersion = new(version)
	}

	if sbVersion, ok := normalizeSubNodeRuntimeVersion(singboxVersion); ok {
		runtime.SingboxVersion = new(sbVersion)
	}

	if uptime, ok := normalizeSubNodeRuntimeUptime(firstNonEmptyString(subNodeUptime, singboxUptime)); ok {
		runtime.SingboxUptime = uptime
	}

	if cpuCount, ok := parseOptionalIntValue(cpuCountStr); ok {
		runtime.CPUCount = new(cpuCount)
	}

	if cpuModel, ok := parseOptionalStringValue(cpuModelStr); ok {
		runtime.CPUModel = new(cpuModel)
	}

	if totalRAM, ok := parseOptionalStringValue(totalRAMStr); ok {
		runtime.TotalRAM = new(totalRAM)
	}

	sm.runtimeByNodeName[cleanNodeName] = runtime
	sm.runtimeMu.Unlock()
}

func (sm *SubNodeMonitor) handleDisconnect(state *subNodeState, reason string) {
	state.mutex.Lock()
	wasConnected := state.isConnected
	streamCancel := state.streamCancel
	conn := state.conn
	state.isConnected = false
	state.isConnecting = false
	state.lastError = reason
	state.lastResponseAt = time.Time{}
	state.stream = nil
	state.streamCancel = nil
	state.conn = nil
	state.mutex.Unlock()

	if streamCancel != nil {
		streamCancel()
	}
	if conn != nil {
		_ = conn.Close()
	}

	if wasConnected {
		sm.updateConnectionStatus(state.nodeName, false, false, reason)
		if sm != nil && sm.cfg != nil {
			sm.cfg.Logger.Info("Subscription node disconnected", "node", state.nodeName, "reason", reason)
		}
	}
}

func (sm *SubNodeMonitor) syncSRSListsToConnectedNodes(requestedNodeUUIDs []string) {
	if sm == nil {
		return
	}

	srsLists, err := srscore.LoadNodeSyncItems(context.Background(), sm.db)
	if err != nil {
		sm.cfg.Logger.Warn("Failed to load SRS lists for subscription node sync", "error", err)
		return
	}

	payload, err := json.Marshal(map[string]any{
		"srs_lists": srsLists,
	})
	if err != nil {
		sm.cfg.Logger.Warn("Failed to marshal SRS sync payload for subscription nodes", "error", err)
		return
	}

	targetFilter := make(map[string]struct{}, len(requestedNodeUUIDs))
	for _, item := range requestedNodeUUIDs {
		uuid := strings.TrimSpace(item)
		if uuid == "" {
			continue
		}
		targetFilter[uuid] = struct{}{}
	}

	sm.nodesLock.RLock()
	states := make([]*subNodeState, 0, len(sm.nodes))
	for _, state := range sm.nodes {
		if state != nil {
			states = append(states, state)
		}
	}
	sm.nodesLock.RUnlock()

	matchedTargets := 0
	readyTargets := 0

	type srsTarget struct {
		name   string
		client proto.NodeServiceClient
	}
	var targets []srsTarget

	for _, state := range states {
		state.mutex.RLock()
		nodeUUID := state.nodeUUID
		nodeName := state.nodeName
		ready := state.isConnected && state.client != nil
		client := state.client
		state.mutex.RUnlock()
		if len(targetFilter) > 0 {
			if _, ok := targetFilter[nodeUUID]; !ok {
				continue
			}
		}
		matchedTargets++
		if !ready {
			continue
		}
		readyTargets++
		targets = append(targets, srsTarget{name: nodeName, client: client})
	}

	var sentTargets int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, subNodeDeployConcurrency)

	for _, target := range targets {
		wg.Add(1)
		sem <- struct{}{}

		go func(t srsTarget) {
			defer wg.Done()
			defer func() { <-sem }()

			ctxBase := sm.globalCtx
			if ctxBase == nil {
				ctxBase = context.Background()
			}
			ctx, cancel := context.WithTimeout(ctxBase, 30*time.Second)
			resp, submitErr := t.client.SubmitTask(ctx, &proto.NodeTask{
				TaskId:    fmt.Sprintf("sync-srs-%d", time.Now().UnixNano()),
				Operation: "sync_srs_lists",
				Payload:   payload,
			})
			cancel()

			if submitErr != nil {
				sm.cfg.Logger.Warn("SRS sync task failed on subscription node", "node", t.name, "error", submitErr)
				return
			}
			if resp == nil || resp.Code != int32(codes.OK) {
				if resp == nil {
					sm.cfg.Logger.Warn("SRS sync returned nil status from subscription node", "node", t.name)
				} else {
					sm.cfg.Logger.Warn("SRS sync rejected by subscription node", "node", t.name, "code", resp.Code, "message", resp.Message)
				}
				return
			}

			atomic.AddInt64(&sentTargets, 1)
			sm.cfg.Logger.Info("SRS lists synced to subscription node", "node", t.name, "lists", len(srsLists), "message", resp.Message)
		}(target)
	}

	wg.Wait()

	sm.cfg.Logger.Debug(
		"SRS subscription node sync processed",
		"target_filter_count", len(targetFilter),
		"matched_targets", matchedTargets,
		"ready_targets", readyTargets,
		"sent_targets", int(sentTargets),
		"lists", len(srsLists),
	)
}

const subNodeDeployConcurrency = 10

func (sm *SubNodeMonitor) deployToConnectedNodes(requestedNodeUUIDs []string) {
	targetFilter := make(map[string]struct{}, len(requestedNodeUUIDs))
	for _, item := range requestedNodeUUIDs {
		uuid := strings.TrimSpace(item)
		if uuid == "" {
			continue
		}
		targetFilter[uuid] = struct{}{}
	}

	sm.nodesLock.RLock()
	states := make([]*subNodeState, 0, len(sm.nodes))
	for _, state := range sm.nodes {
		if state != nil {
			states = append(states, state)
		}
	}
	sm.nodesLock.RUnlock()

	var wg sync.WaitGroup
	sem := make(chan struct{}, subNodeDeployConcurrency)

	for _, state := range states {
		state.mutex.RLock()
		nodeUUID := state.nodeUUID
		ready := state.isConnected && state.stream != nil
		state.mutex.RUnlock()
		if !ready {
			continue
		}
		if len(targetFilter) > 0 {
			if _, ok := targetFilter[nodeUUID]; !ok {
				continue
			}
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(st *subNodeState) {
			defer wg.Done()
			defer func() { <-sem }()

			st.mutex.RLock()
			nodeName := st.nodeName
			st.mutex.RUnlock()

			err := sm.sendNodeRequest(st, &proto.NodeDataRequest{
				Request: &proto.NodeDataRequest_Config{Config: &proto.StreamConfig{IntervalSeconds: 15}},
			})
			if err != nil {
				sm.cfg.Logger.Warn("Failed to push subscription config over stream", "node", nodeName, "error", err)
				sm.handleDisconnect(st, fmt.Sprintf("Config push failed: %v", err))
				return
			}
			sm.pushAssignedSubpageConfig(st)
			sm.cfg.Logger.Info("Subscription config push sent", "node", nodeName)
		}(state)
	}

	wg.Wait()
}

func (sm *SubNodeMonitor) pushAssignedSubpageConfig(state *subNodeState) {
	if state == nil {
		return
	}

	state.mutex.RLock()
	nodeName := state.nodeName
	nodeUUID := state.nodeUUID
	subpageConfigUUID := strings.TrimSpace(state.subpageConfigUUID)
	ready := state.isConnected && state.stream != nil
	state.mutex.RUnlock()

	if !ready || subpageConfigUUID == "" {
		return
	}

	configPayload, err := sm.fetchSubpageConfigRaw(state.ctx, subpageConfigUUID)
	if err != nil {
		sm.cfg.Logger.Warn(
			"Failed to fetch assigned subpage config for subscription node",
			"node", nodeName,
			"node_uuid", nodeUUID,
			"subpage_config_uuid", subpageConfigUUID,
			"error", err,
		)
		return
	}

	err = sm.sendNodeRequest(state, &proto.NodeDataRequest{
		Request: &proto.NodeDataRequest_SubpageConfigUpdate{SubpageConfigUpdate: &proto.SubpageConfigUpdate{
			Uuid:   subpageConfigUUID,
			Config: configPayload,
		}},
	})
	if err != nil {
		sm.cfg.Logger.Warn(
			"Failed to push assigned subpage config to subscription node",
			"node", nodeName,
			"node_uuid", nodeUUID,
			"subpage_config_uuid", subpageConfigUUID,
			"error", err,
		)
		sm.handleDisconnect(state, fmt.Sprintf("Subpage config push failed: %v", err))
		return
	}

	sm.cfg.Logger.Info(
		"Assigned subpage config push sent",
		"node", nodeName,
		"node_uuid", nodeUUID,
		"subpage_config_uuid", subpageConfigUUID,
	)
}

func (sm *SubNodeMonitor) pushSubpageConfigToConnectedNodes(command subpageConfigPushCommand) {
	targetFilter := make(map[string]struct{}, len(command.targetUUIDs))
	for _, item := range command.targetUUIDs {
		uuid := strings.TrimSpace(item)
		if uuid == "" {
			continue
		}
		targetFilter[uuid] = struct{}{}
	}

	sm.nodesLock.RLock()
	states := make([]*subNodeState, 0, len(sm.nodes))
	for _, state := range sm.nodes {
		if state != nil {
			states = append(states, state)
		}
	}
	sm.nodesLock.RUnlock()

	matchedTargets := 0
	readyTargets := 0
	sentTargets := 0

	for _, state := range states {
		state.mutex.RLock()
		nodeUUID := state.nodeUUID
		nodeName := state.nodeName
		ready := state.isConnected && state.stream != nil
		state.mutex.RUnlock()
		if len(targetFilter) > 0 {
			if _, ok := targetFilter[nodeUUID]; !ok {
				continue
			}
		}
		matchedTargets++
		if !ready {
			continue
		}
		readyTargets++

		err := sm.sendNodeRequest(state, &proto.NodeDataRequest{
			Request: &proto.NodeDataRequest_SubpageConfigUpdate{SubpageConfigUpdate: &proto.SubpageConfigUpdate{
				Uuid:   command.uuid,
				Config: command.config,
			}},
		})
		if err != nil {
			sm.cfg.Logger.Warn("Failed to push subpage config update", "node", nodeName, "error", err)
			sm.handleDisconnect(state, fmt.Sprintf("Subpage config push failed: %v", err))
			continue
		}
		sentTargets++
		sm.cfg.Logger.Info("Subpage config push sent", "node", nodeName, "uuid", command.uuid)
	}

	sm.cfg.Logger.Debug(
		"Subpage config push processed",
		"uuid", command.uuid,
		"target_filter_count", len(targetFilter),
		"matched_targets", matchedTargets,
		"ready_targets", readyTargets,
		"sent_targets", sentTargets,
		"payload_bytes", len(command.config),
	)
}

func normalizeSubNodeRuntimeVersion(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", false
	}

	lower := strings.ToLower(trimmed)
	switch lower {
	case "unknown", "latest", "(devel)", "sub":
		return "", false
	}

	if subNodeVersionPattern.MatchString(trimmed) {
		if strings.HasPrefix(lower, "v") {
			return "v" + strings.TrimSpace(trimmed[1:]), true
		}
		return trimmed, true
	}

	return trimmed, true
}

func normalizeSubNodeRuntimeUptime(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", false
	}

	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || parsed < 0 {
		return "", false
	}

	return strconv.FormatInt(parsed, 10), true
}

func parseOptionalIntValue(value string) (int, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, false
	}

	parsed, err := strconv.Atoi(trimmed)
	if err != nil {
		return 0, false
	}

	return parsed, true
}

func parseOptionalStringValue(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", false
	}
	return trimmed, true
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

package users

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"exodus/internal/jobqueue"
	"exodus/internal/nodehotcache"
	"exodus/internal/notifications"
	"exodus/internal/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const nodeHeartbeatThrottleInterval = 5 * time.Minute

func (nm *NodeMonitor) shouldUpdateIdleHeartbeat(nodeName string, now time.Time) bool {
	if val, ok := nm.lastIdleHeartbeatUpdate.Load(nodeName); ok {
		if lastTime, ok := val.(time.Time); ok && now.Sub(lastTime) < nodeHeartbeatThrottleInterval {
			return false
		}
	}
	return true
}

// receiveStream receives and processes stream data.
func (nm *NodeMonitor) receiveStream(state *nodeState) {
	for {
		state.mutex.RLock()
		stream := state.stream
		state.mutex.RUnlock()
		if stream == nil {
			nm.handleDisconnect(state, "Stream unavailable")
			return
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			nm.cfg.Logger.Warn("Stream closed by node", "node", state.nodeName)
			nm.handleDisconnect(state, "Stream closed")
			return
		}
		if err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Canceled && state.ctx.Err() != nil {
					nm.cfg.Logger.Debug("Stream canceled", "node", state.nodeName)
					return
				}
				if st.Code() == codes.Unavailable {
					reason := formatNodeConnectionError(err)
					if reason == err.Error() {
						if strings.TrimSpace(st.Message()) != "" {
							reason = fmt.Sprintf("Node unavailable: %s", st.Message())
						} else {
							reason = "Node unavailable"
						}
					}
					nm.cfg.Logger.Warn(
						"Node unavailable",
						"node", state.nodeName,
						"code", st.Code().String(),
						"message", st.Message(),
					)
					nm.handleDisconnect(state, reason)
					return
				}
			}
			friendlyErr := formatNodeConnectionError(err)
			if friendlyErr == err.Error() {
				friendlyErr = fmt.Sprintf("Stream error: %v", err)
			}
			nm.cfg.Logger.Error("Stream error", "node", state.nodeName, "error", friendlyErr)
			nm.handleDisconnect(state, friendlyErr)
			return
		}

		nm.markStreamActivity(state)
		nm.processResponse(state, resp)
	}
}

// handleDisconnect handles node disconnection.
func (nm *NodeMonitor) handleDisconnect(state *nodeState, reason string) {
	state.mutex.Lock()
	wasConnected := state.isConnected
	state.isConnected = false
	state.isConnecting = false
	state.lastError = reason
	state.hasSentStaticInfo = false
	state.lastStaticInfoSentAt = time.Time{}
	state.lastSingboxVer = ""
	state.lastNodeVer = ""
	if state.streamCancel != nil {
		state.streamCancel()
		state.streamCancel = nil
	}
	if state.stream != nil {
		state.stream = nil
	}
	if state.conn != nil {
		_ = state.conn.Close()
		state.conn = nil
	}
	state.client = nil
	state.lastResponseAt = time.Time{}
	nodeUUID := state.nodeUUID
	state.mutex.Unlock()

	// Unconditionally delete all node transient keys from Redis hot cache immediately
	if nm.hotCache != nil && nodeUUID != "" {
		_ = nm.hotCache.DeleteTransient(context.Background(), nodeUUID)
	}

	if nodeUUID != "" {
		nm.metricsLock.Lock()
		if s, ok := nm.metricsByNodeUUID[nodeUUID]; ok && s != nil {
			s.UsersOnline = 0
			s.UpdatedAt = time.Now().UTC()
		}
		nm.metricsLock.Unlock()
	}

	if wasConnected {
		nm.updateConnectionStatus(state.nodeName, false, false, reason)
		if nm != nil && nm.cfg != nil && nm.cfg.Logger != nil {
			nm.cfg.Logger.Warn(fmt.Sprintf("Lost connection to Node %s (%s:%d), message: %s", state.nodeName, state.address, state.port, reason))
		}
	} else {
		if nm != nil && nm.cfg != nil && nm.cfg.Logger != nil {
			nm.cfg.Logger.Warn(fmt.Sprintf("Connection attempt failed for Node %s (%s:%d), message: %s", state.nodeName, state.address, state.port, reason))
		}
	}
}

// processResponse processes node response data.
func (nm *NodeMonitor) processResponse(state *nodeState, resp *proto.NodeDataResponse) {
	if state == nil {
		return
	}
	switch payload := resp.Response.(type) {
	case *proto.NodeDataResponse_Stats:
		nm.cfg.Logger.Trace("Node stats received", "node", state.nodeName)
		nm.updateNodeRuntimeFromStats(state, payload.Stats.GetStats())
	case *proto.NodeDataResponse_Users:
		nm.cfg.Logger.Trace("Node users received", "node", state.nodeName)
	case *proto.NodeDataResponse_LogData:
		nm.cfg.Logger.Trace("Node log data received", "node", state.nodeName)
	default:
		nm.cfg.Logger.Trace("Node message received", "node", state.nodeName)
	}
}

func (nm *NodeMonitor) updateNodeRuntimeFromStats(state *nodeState, stats []*proto.Stat) {
	if len(stats) == 0 || state == nil {
		return
	}
	nodeName := state.nodeName

	rt, trafficDelta := parseNodeStatsStream(stats)

	rawCoreStatus := rt.rawCoreStatus
	rawCoreError := rt.rawCoreError
	rawSingboxVersion := rt.rawSingboxVersion
	rawNodeVersion := rt.rawNodeVersion
	rawSingboxUptime := rt.rawSingboxUptime
	rawSystemInfo := rt.rawSystemInfo
	rawSystemStats := rt.rawSystemStats

	coreStatus := strings.ToLower(strings.TrimSpace(rawCoreStatus))
	coreError := strings.TrimSpace(rawCoreError)
	switch coreStatus {
	case "running", "ok", "healthy":
		nm.updateConnectionStatus(nodeName, true, false, "")
	case "error", "failed", "unhealthy", "stopped":
		nm.updateConnectionStatus(nodeName, false, false, coreError)
	}

	singboxVersion := firstNonEmptyString(rawSingboxVersion)
	nodeVersion := firstNonEmptyString(rawNodeVersion)
	singboxUptime, hasSingboxUptime := parseOptionalUptimeSeconds(rawSingboxUptime)
	systemInfo := parseOptionalJSONRaw(rawSystemInfo)
	systemStats := parseOptionalJSONRaw(rawSystemStats)
	usersOnline := trafficDelta.UsersOnline

	now := time.Now()
	state.mutex.Lock()
	needStaticInfo := !state.hasSentStaticInfo ||
		now.Sub(state.lastStaticInfoSentAt) > 30*time.Minute ||
		(singboxVersion != "" && singboxVersion != state.lastSingboxVer) ||
		(nodeVersion != "" && nodeVersion != state.lastNodeVer)

	if needStaticInfo {
		state.hasSentStaticInfo = true
		state.lastStaticInfoSentAt = now
		if singboxVersion != "" {
			state.lastSingboxVer = singboxVersion
		}
		if nodeVersion != "" {
			state.lastNodeVer = nodeVersion
		}
	}
	state.mutex.Unlock()

	var (
		cachedSystemInfo json.RawMessage
		cachedSingboxVer string
		cachedNodeVer    string
	)
	if needStaticInfo {
		cachedSystemInfo = systemInfo
		cachedSingboxVer = singboxVersion
		cachedNodeVer = nodeVersion
	}

	persistedNodeUUID := ""
	firstConnectedEvents := make([]notifications.Event, 0)

	nodeUUID, nodeID, consumptionMultiplier, nodeConsumptionMultiplier, err := nm.getNodeMetadata(nodeName)
	if err != nil {
		nm.cfg.Logger.Warn("Failed to query node metadata", "node", nodeName, "error", err)
		return
	}
	persistedNodeUUID = nodeUUID

	streamDBContext := context.Background()
	if nm.globalCtx != nil {
		streamDBContext = nm.globalCtx
	}

	if trafficDelta.TotalUploadBytes > 0 || trafficDelta.TotalDownloadBytes > 0 {
		totalBytes := trafficDelta.TotalUploadBytes + trafficDelta.TotalDownloadBytes
		nodeUsageBytes := applyConsumptionMultiplier(totalBytes, nodeConsumptionMultiplier)
		if _, execErr := nm.db.Exec(streamDBContext, `
			WITH ins AS (
				INSERT INTO nodes_usage_history (node_uuid, download_bytes, upload_bytes, total_bytes)
				VALUES ($1, $2, $3, $4)
				ON CONFLICT (node_uuid, created_at)
				DO UPDATE SET
					download_bytes = nodes_usage_history.download_bytes + EXCLUDED.download_bytes,
					upload_bytes = nodes_usage_history.upload_bytes + EXCLUDED.upload_bytes,
					total_bytes = nodes_usage_history.total_bytes + EXCLUDED.total_bytes,
					updated_at = now()
			)
			UPDATE nodes
			SET traffic_used_bytes = COALESCE(traffic_used_bytes, 0) + $5, updated_at = CURRENT_TIMESTAMP
			WHERE uuid = $1
		`, nodeUUID, trafficDelta.TotalDownloadBytes, trafficDelta.TotalUploadBytes, totalBytes, nodeUsageBytes); execErr != nil {
			nm.cfg.Logger.Warn("Failed to record node usage and traffic", "node", nodeName, "error", execErr)
			return
		}
		nm.lastIdleHeartbeatUpdate.Store(nodeName, time.Now())
	} else {
		now := time.Now()
		if nm.shouldUpdateIdleHeartbeat(nodeName, now) {
			if _, execErr := nm.db.Exec(streamDBContext, `
				UPDATE nodes
				SET updated_at = CURRENT_TIMESTAMP
				WHERE name = $1`, nodeName); execErr != nil {
				nm.cfg.Logger.Warn("Failed to update node updated_at", "node", nodeName, "error", execErr)
				return
			}
			nm.lastIdleHeartbeatUpdate.Store(nodeName, now)
		}
	}

	if len(trafficDelta.UserBytesByName) > 0 {
		usageDeltas := make([]userUsageDelta, 0, len(trafficDelta.UserBytesByName))
		for rawKey, rawBytes := range trafficDelta.UserBytesByName {
			parsedID, err := strconv.ParseInt(strings.TrimSpace(rawKey), 10, 64)
			if err != nil || parsedID <= 0 {
				continue
			}
			if nm.cfg != nil && nm.cfg.Redis.UserUsageIgnoreBelowBytes > 0 && rawBytes < nm.cfg.Redis.UserUsageIgnoreBelowBytes {
				continue
			}

			effectiveBytes := applyConsumptionMultiplier(rawBytes, consumptionMultiplier)
			if effectiveBytes <= 0 {
				continue
			}

			usageDeltas = append(usageDeltas, userUsageDelta{
				UserID:       parsedID,
				TotalBytes:   effectiveBytes,
				HistoryBytes: rawBytes,
			})
			nm.cfg.Logger.Trace("Recorded user traffic delta", "node", nodeName, "user", parsedID, "bytes", effectiveBytes)
		}

		if len(usageDeltas) > 0 {
			bulkCtx := nm.globalCtx
			if bulkCtx == nil {
				bulkCtx = context.Background()
			}

			firstConnectedIDs, execErr := bulkUpsertUserTraffic(bulkCtx, nm.db, usageDeltas, nodeUUID)
			if execErr != nil {
				nm.cfg.Logger.Warn("Failed to upsert user traffic", "node", nodeName, "error", execErr)
			}
			if len(firstConnectedIDs) > 0 {
				var unqueuedIDs []int64
				for _, id := range firstConnectedIDs {
					queued, _ := jobqueue.EnqueueUserEvent(bulkCtx, jobqueue.FireUserEventPayload{
						UserID:    id,
						UserEvent: notifications.EventUserFirstConnected,
						NodeUUID:  nodeUUID,
						NodeName:  nodeName,
					})
					if !queued {
						unqueuedIDs = append(unqueuedIDs, id)
					}
				}

				if len(unqueuedIDs) > 0 {
					userMap := make(map[int64]struct {
						username          string
						uuid              string
						shortUUID         string
						status            string
						trafficLimitBytes int64
						usedTrafficBytes  int64
						expireAt          time.Time
					}, len(unqueuedIDs))

					rows, queryErr := nm.db.Query(bulkCtx, `
						SELECT u.id, u.username, u.uuid::text, u.short_uuid, u.status,
						       u.traffic_limit_bytes, COALESCE(ut.used_traffic_bytes, 0), u.expire_at
						FROM users u
						LEFT JOIN user_traffic ut ON ut.id = u.id
						WHERE u.id = ANY($1)
					`, unqueuedIDs)
					if queryErr != nil {
						nm.cfg.Logger.Warn("Failed to query user details for first connected event", "error", queryErr)
					} else {
						for rows.Next() {
							var (
								uID       int64
								uName     string
								uUUID     string
								shortUUID string
								status    string
								tLimit    int64
								used      int64
								expireAt  time.Time
							)
							if scanErr := rows.Scan(&uID, &uName, &uUUID, &shortUUID, &status, &tLimit, &used, &expireAt); scanErr == nil {
								userMap[uID] = struct {
									username          string
									uuid              string
									shortUUID         string
									status            string
									trafficLimitBytes int64
									usedTrafficBytes  int64
									expireAt          time.Time
								}{
									username:          uName,
									uuid:              uUUID,
									shortUUID:         shortUUID,
									status:            status,
									trafficLimitBytes: tLimit,
									usedTrafficBytes:  used,
									expireAt:          expireAt,
								}
							}
						}
						_ = rows.Err()
						rows.Close()
					}

					for _, id := range unqueuedIDs {
						data := map[string]any{
							"id":       id,
							"nodeUuid": nodeUUID,
							"nodeName": nodeName,
						}
						if u, ok := userMap[id]; ok {
							data["username"] = u.username
							data["uuid"] = u.uuid
							data["shortUuid"] = u.shortUUID
							data["status"] = u.status
							data["trafficLimitBytes"] = u.trafficLimitBytes
							data["usedTrafficBytes"] = u.usedTrafficBytes
							data["expireAt"] = u.expireAt.UTC().Format(time.RFC3339)
							data["userTraffic"] = map[string]any{
								"usedTrafficBytes": u.usedTrafficBytes,
							}
						}
						firstConnectedEvents = append(firstConnectedEvents, notifications.Event{
							Scope: notifications.ScopeUser,
							Event: notifications.EventUserFirstConnected,
							Data:  data,
						})
					}
				}
			}

			if recordErr := nm.recordNodeUserUsageHistory(bulkCtx, nm.db, nodeID, usageDeltas); recordErr != nil {
				nm.cfg.Logger.Warn("Failed to record node user usage history", "node", nodeName, "error", recordErr)
			}
		}
	}

	for _, event := range firstConnectedEvents {
		notifications.Emit(context.Background(), nm.cfg, event)
	}

	nm.updateNodeMetricsSnapshot(persistedNodeUUID, usersOnline, trafficDelta)
	nm.updateHotCacheNodeRuntime(nodeName, persistedNodeUUID, cachedSingboxVer, cachedNodeVer, hasSingboxUptime, singboxUptime, usersOnline, cachedSystemInfo, systemStats, trafficDelta)
}

func (nm *NodeMonitor) updateHotCacheNodeRuntime(
	_ string,
	nodeUUID string,
	singboxVersion string,
	nodeVersion string,
	hasSingboxUptime bool,
	singboxUptime int64,
	usersOnline int,
	systemInfo json.RawMessage,
	systemStats json.RawMessage,
	_ trafficStatsDelta,
) {
	if nm.hotCache == nil {
		return
	}
	ctx := nm.globalCtx
	if ctx == nil {
		ctx = context.Background()
	}

	if nm.hotCache != nil && strings.TrimSpace(nodeUUID) != "" {
		_ = nm.hotCache.SetNodeRuntimeState(ctx, nodeUUID, nodehotcache.NodeRuntimeUpdate{
			SystemInfo:       systemInfo,
			SystemStats:      systemStats,
			SingboxVersion:   singboxVersion,
			NodeVersion:      nodeVersion,
			HasSingboxUptime: hasSingboxUptime,
			SingboxUptime:    singboxUptime,
			UsersOnline:      usersOnline,
		})
	}
}

func applyConsumptionMultiplier(bytes int64, multiplier int64) int64 {
	if bytes <= 0 || multiplier <= 0 {
		return 0
	}
	if multiplier == 1_000_000_000 {
		return bytes
	}
	// Prevent int64 overflow on bytes * multiplier (overflows at ~9.22 GB when multiplied by 10^9).
	// Uses float64 scaling matching upstream fromNanoToNumber(multiplier) * totalBytes.
	return int64(float64(bytes) * (float64(multiplier) / 1_000_000_000.0))
}

func parseOptionalUptimeSeconds(raw string) (int64, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, false
	}
	val, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || val < 0 {
		return 0, false
	}
	return val, true
}

func parseOptionalJSONRaw(raw string) json.RawMessage {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	b := []byte(trimmed)
	if !json.Valid(b) {
		return nil
	}
	return json.RawMessage(b)
}

type nodeRuntimeData struct {
	rawCoreStatus     string
	rawCoreError      string
	rawSingboxVersion string
	rawNodeVersion    string
	rawSingboxUptime  string
	rawSystemInfo     string
	rawSystemStats    string
}

func parseNodeStatsStream(stats []*proto.Stat) (nodeRuntimeData, trafficStatsDelta) {
	var (
		rt    nodeRuntimeData
		delta trafficStatsDelta
	)
	userCap := 0
	if len(stats) > 16 {
		userCap = len(stats) / 2
	}
	delta.InboundByTag = make(map[string]TagTrafficCounters, 8)
	delta.OutboundByTag = make(map[string]TagTrafficCounters, 8)
	delta.UserBytesByName = make(map[string]int64, userCap)

	for _, stat := range stats {
		if stat == nil {
			continue
		}
		rawKey := strings.TrimSpace(stat.GetName())
		if len(rawKey) == 0 {
			continue
		}

		switch rawKey[0] {
		case 'u', 'U':
			if strings.HasPrefix(rawKey, "user>>>") {
				rest := rawKey[7:]
				idx := strings.Index(rest, ">>>")
				if idx > 0 && strings.HasPrefix(rest[idx+3:], "traffic>>>") {
					username := rest[:idx]
					val, _ := strconv.ParseInt(strings.TrimSpace(stat.GetValue()), 10, 64)
					if val > 0 {
						delta.UserBytesByName[username] += val
					}
				}
				continue
			}
			key := strings.ToLower(rawKey)
			if key == "users_online" {
				val, _ := strconv.ParseInt(strings.TrimSpace(stat.GetValue()), 10, 64)
				if val >= 0 && val <= math.MaxInt {
					delta.UsersOnline = int(val)
				}
				continue
			}
			if strings.HasPrefix(key, "user_") {
				username := strings.TrimPrefix(key, "user_")
				username = strings.TrimSuffix(username, "_bytes")
				username = strings.TrimSpace(username)
				if username != "" {
					val, _ := strconv.ParseInt(strings.TrimSpace(stat.GetValue()), 10, 64)
					if val > 0 {
						delta.UserBytesByName[username] += val
					}
				}
				continue
			}

		case 'i', 'I':
			if strings.HasPrefix(rawKey, "inbound>>>") {
				rest := rawKey[10:]
				idx := strings.Index(rest, ">>>")
				if idx > 0 && strings.HasPrefix(rest[idx+3:], "traffic>>>") {
					tag := rest[:idx]
					dir := rest[idx+3+10:]
					val, _ := strconv.ParseInt(strings.TrimSpace(stat.GetValue()), 10, 64)
					counters := delta.InboundByTag[tag]
					if strings.EqualFold(dir, "uplink") {
						counters.UploadBytes += val
					} else if strings.EqualFold(dir, "downlink") {
						counters.DownloadBytes += val
					}
					delta.InboundByTag[tag] = counters
				}
				continue
			}
			key := strings.ToLower(rawKey)
			if strings.HasPrefix(key, "inbound_") {
				parts := strings.Split(key, "_")
				if len(parts) >= 3 {
					direction := parts[len(parts)-1]
					tag := strings.Join(parts[1:len(parts)-1], "_")
					val, _ := strconv.ParseInt(strings.TrimSpace(stat.GetValue()), 10, 64)
					counters := delta.InboundByTag[tag]
					switch direction {
					case "down", "download":
						counters.DownloadBytes += val
					case "up", "upload":
						counters.UploadBytes += val
					}
					delta.InboundByTag[tag] = counters
				}
				continue
			}

		case 'o', 'O':
			if strings.HasPrefix(rawKey, "outbound>>>") {
				rest := rawKey[11:]
				idx := strings.Index(rest, ">>>")
				if idx > 0 && strings.HasPrefix(rest[idx+3:], "traffic>>>") {
					tag := rest[:idx]
					dir := rest[idx+3+10:]
					val, _ := strconv.ParseInt(strings.TrimSpace(stat.GetValue()), 10, 64)
					counters := delta.OutboundByTag[tag]
					if strings.EqualFold(dir, "uplink") {
						delta.TotalUploadBytes += val
						counters.UploadBytes += val
					} else if strings.EqualFold(dir, "downlink") {
						delta.TotalDownloadBytes += val
						counters.DownloadBytes += val
					}
					delta.OutboundByTag[tag] = counters
				}
				continue
			}
			key := strings.ToLower(rawKey)
			if strings.HasPrefix(key, "outbound_") {
				parts := strings.Split(key, "_")
				if len(parts) >= 3 {
					direction := parts[len(parts)-1]
					tag := strings.Join(parts[1:len(parts)-1], "_")
					val, _ := strconv.ParseInt(strings.TrimSpace(stat.GetValue()), 10, 64)
					counters := delta.OutboundByTag[tag]
					switch direction {
					case "down", "download":
						counters.DownloadBytes += val
					case "up", "upload":
						counters.UploadBytes += val
					}
					delta.OutboundByTag[tag] = counters
				}
				continue
			}

		case 't', 'T':
			key := strings.ToLower(rawKey)
			if key == "total_download_bytes" {
				val, _ := strconv.ParseInt(strings.TrimSpace(stat.GetValue()), 10, 64)
				delta.TotalDownloadBytes = val
				continue
			}
			if key == "total_upload_bytes" {
				val, _ := strconv.ParseInt(strings.TrimSpace(stat.GetValue()), 10, 64)
				delta.TotalUploadBytes = val
				continue
			}

		case 'c', 'C':
			key := strings.ToLower(rawKey)
			if key == "core_status" {
				rt.rawCoreStatus = stat.GetValue()
				continue
			}
			if key == "core_error" {
				rt.rawCoreError = stat.GetValue()
				continue
			}

		case 's', 'S':
			key := strings.ToLower(rawKey)
			switch key {
			case "singbox_version":
				rt.rawSingboxVersion = stat.GetValue()
			case "singbox_uptime":
				rt.rawSingboxUptime = stat.GetValue()
			case "system_info":
				rt.rawSystemInfo = stat.GetValue()
			case "system_stats":
				rt.rawSystemStats = stat.GetValue()
			}
			continue

		case 'n', 'N':
			key := strings.ToLower(rawKey)
			if key == "node_version" {
				rt.rawNodeVersion = stat.GetValue()
				continue
			}
		}
	}

	if delta.UsersOnline == 0 {
		delta.UsersOnline = len(delta.UserBytesByName)
	}

	return rt, delta
}

func extractTrafficStatsDelta(stats []*proto.Stat) trafficStatsDelta {
	_, delta := parseNodeStatsStream(stats)
	return delta
}

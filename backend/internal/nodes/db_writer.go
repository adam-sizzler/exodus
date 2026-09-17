package users

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"exodus/internal/db"
	"exodus/internal/notifications"

	"github.com/jackc/pgx/v5"
)

// updateConnectionStatus updates node connection status in database (only on change).
func (nm *NodeMonitor) updateConnectionStatus(nodeName string, isConnected, isConnecting bool, message string) {
	if nm == nil || nm.db == nil {
		return
	}
	message, messageDBValue := optionalStatusMessage(message)

	var (
		nodeUUID          string
		nodeAddress       string
		nodePort          *int64
		currentConnected  bool
		currentConnecting bool
		currentMessage    *string
	)

	ctx := context.Background()
	err := nm.db.QueryRow(ctx, `SELECT uuid, address, port, is_connected, is_connecting, last_status_message FROM nodes WHERE name = $1`, nodeName).
		Scan(&nodeUUID, &nodeAddress, &nodePort, &currentConnected, &currentConnecting, &currentMessage)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			nm.cfg.Logger.Debug("Node not found in DB", "node", nodeName)
			return
		}
		nm.cfg.Logger.Warn("Failed to query node status from DB", "node", nodeName, "error", err)
		return
	}

	msgStr := ""
	if currentMessage != nil {
		msgStr = *currentMessage
	}

	if currentConnected == isConnected && currentConnecting == isConnecting && msgStr == message {
		if !isConnected {
			_ = nm.clearDisconnectedNodeRuntimeFields(context.Background(), nodeUUID)
		}
		return
	}

	query := `
		UPDATE nodes
		SET is_connected = $1,
		    is_connecting = $2,
		    last_status_message = $3,
		    last_status_change = CURRENT_TIMESTAMP
		WHERE name = $4`

	if _, execErr := nm.db.Exec(ctx, query, isConnected, isConnecting, messageDBValue, nodeName); execErr != nil {
		nm.cfg.Logger.Warn("Failed to update node status in DB", "node", nodeName, "error", execErr)
		return
	}

	if !isConnected {
		if err := nm.clearDisconnectedNodeRuntimeFields(context.Background(), nodeUUID); err != nil {
			nm.cfg.Logger.Warn("Failed to clear runtime cache fields on node status change", "node", nodeName, "error", err)
		}
	}

	if currentConnected != isConnected {
		eventName := notifications.EventNodeConnectionRestored
		if !isConnected {
			eventName = notifications.EventNodeConnectionLost
		}
		var portVal int64
		if nodePort != nil {
			portVal = *nodePort
		}
		notificationEvent := notifications.Event{
			Scope: notifications.ScopeNode,
			Event: eventName,
			Data: map[string]any{
				"uuid":        nodeUUID,
				"name":        nodeName,
				"address":     nodeAddress,
				"port":        portVal,
				"isConnected": isConnected,
				"message":     message,
			},
		}
		notifications.Emit(context.Background(), nm.cfg, notificationEvent)
	}
}

func (nm *NodeMonitor) recordNodeUserUsageHistory(ctx context.Context, dbConn db.DBTX, nodeID int64, usageDeltas []userUsageDelta) error {
	if len(usageDeltas) == 0 {
		return nil
	}
	if nm.usageRecorder != nil {
		userBytes := make(map[int64]int64, len(usageDeltas))
		for _, delta := range usageDeltas {
			if delta.UserID <= 0 || delta.HistoryBytes <= 0 {
				continue
			}
			userBytes[delta.UserID] += delta.HistoryBytes
		}
		if len(userBytes) == 0 {
			return nil
		}
		if err := nm.usageRecorder.RecordNodeUserUsage(ctx, nodeID, userBytes); err == nil {
			return nil
		} else if nm.cfg != nil && nm.cfg.Logger != nil {
			nm.cfg.Logger.Warn("Failed to enqueue node user usage history in Redis, falling back to direct database write", "error", err)
		}
	}
	return bulkUpsertNodeUserUsageHistory(ctx, dbConn, nodeID, usageDeltas)
}

func bulkUpsertUserTraffic(ctx context.Context, dbConn db.DBTX, usageDeltas []userUsageDelta, nodeUUID string) ([]int64, error) {
	const chunkSize = 1000
	var firstConnectedIDs []int64

	for start := 0; start < len(usageDeltas); start += chunkSize {
		end := min(start+chunkSize, len(usageDeltas))
		chunk := usageDeltas[start:end]

		var query strings.Builder
		query.Grow(len(chunk)*48 + 512)
		args := make([]any, 0, len(chunk)*3)

		query.WriteString(`
			INSERT INTO user_traffic (
				id, used_traffic_bytes, lifetime_used_traffic_bytes,
				online_at, last_connected_node_uuid, first_connected_at
			)
			SELECT
				v.id,
				v.total_bytes,
				v.total_bytes,
				now(),
				v.last_connected_node_uuid,
				now()
			FROM (VALUES `)

		idx := 1
		for i, delta := range chunk {
			if i > 0 {
				query.WriteString(", ")
			}
			writePlaceholder3(&query, idx, "uuid")
			args = append(args, delta.UserID, delta.TotalBytes, nodeUUID)
			idx += 3
		}

		query.WriteString(`) AS v(id, total_bytes, last_connected_node_uuid)
			ON CONFLICT (id)
			DO UPDATE SET
				used_traffic_bytes = user_traffic.used_traffic_bytes + EXCLUDED.used_traffic_bytes,
				lifetime_used_traffic_bytes = user_traffic.lifetime_used_traffic_bytes + EXCLUDED.lifetime_used_traffic_bytes,
				online_at = now(),
				last_connected_node_uuid = EXCLUDED.last_connected_node_uuid,
				first_connected_at = COALESCE(user_traffic.first_connected_at, now())
			RETURNING id, (user_traffic.first_connected_at IS NULL OR user_traffic.first_connected_at = user_traffic.online_at) AS is_first_connection
		`)

		rows, err := dbConn.Query(ctx, query.String(), args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var (
				id                int64
				isFirstConnection bool
			)
			if scanErr := rows.Scan(&id, &isFirstConnection); scanErr == nil && isFirstConnection {
				firstConnectedIDs = append(firstConnectedIDs, id)
			}
		}
		_ = rows.Err()
		rows.Close()
	}

	return firstConnectedIDs, nil
}

func bulkUpsertNodeUserUsageHistory(ctx context.Context, dbConn db.DBTX, nodeID int64, usageDeltas []userUsageDelta) error {
	const chunkSize = 1000

	for start := 0; start < len(usageDeltas); start += chunkSize {
		end := min(start+chunkSize, len(usageDeltas))
		chunk := usageDeltas[start:end]

		var query strings.Builder
		query.Grow(len(chunk)*48 + 256)
		args := make([]any, 0, len(chunk)*3)

		query.WriteString(`
			INSERT INTO nodes_user_usage_history (node_id, user_id, total_bytes)
			VALUES `)

		idx := 1
		for i, delta := range chunk {
			if i > 0 {
				query.WriteString(", ")
			}
			writePlaceholder3(&query, idx, "bigint")
			args = append(args, nodeID, delta.UserID, delta.HistoryBytes)
			idx += 3
		}

		query.WriteString(`
			ON CONFLICT (node_id, created_at, user_id)
			DO UPDATE SET
				total_bytes = nodes_user_usage_history.total_bytes + EXCLUDED.total_bytes,
				updated_at = now()
		`)

		if _, err := dbConn.Exec(ctx, query.String(), args...); err != nil {
			return err
		}
	}

	return nil
}

func (nm *NodeMonitor) clearDisconnectedNodeRuntimeFields(ctx context.Context, nodeUUID string) error {
	if nm.hotCache == nil {
		return nil
	}
	if err := nm.hotCache.DeleteTransient(ctx, nodeUUID); err != nil {
		return fmt.Errorf("clear disconnected node runtime fields: %w", err)
	}
	return nil
}

func optionalStatusMessage(message string) (string, any) {
	trimmed := strings.TrimSpace(message)
	if trimmed == "" {
		return "", nil
	}
	return trimmed, trimmed
}

func writePlaceholder3(b *strings.Builder, idx int, type3 string) {
	var buf [16]byte
	b.WriteString("($")
	b.Write(strconv.AppendInt(buf[:0], int64(idx), 10))
	b.WriteString("::bigint, $")
	b.Write(strconv.AppendInt(buf[:0], int64(idx+1), 10))
	b.WriteString("::bigint, $")
	b.Write(strconv.AppendInt(buf[:0], int64(idx+2), 10))
	b.WriteString("::")
	b.WriteString(type3)
	b.WriteByte(')')
}

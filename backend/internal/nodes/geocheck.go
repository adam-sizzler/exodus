package users

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"exodus/internal/proto"

	"google.golang.org/grpc/codes"
)

const geocheckOperation = "geocheck"

type GeocheckRequestPayload struct {
	IP        string `json:"ip,omitempty"`
	Interface string `json:"interface,omitempty"`
}

func (nm *NodeMonitor) ExecuteGeocheck(ctx context.Context, nodeUUID string, ip string, iface string) (string, error) {
	if nm == nil {
		return "", fmt.Errorf("node monitor is not ready")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var (
		client         proto.NodeServiceClient
		targetNodeName string
	)

	nm.nodesLock.RLock()
	for name, state := range nm.nodes {
		if state == nil {
			continue
		}
		if state.nodeUUID == nodeUUID {
			targetNodeName = name
			state.mutex.RLock()
			if state.isConnected && state.client != nil {
				client = state.client
			}
			state.mutex.RUnlock()
			break
		}
	}
	nm.nodesLock.RUnlock()

	if targetNodeName == "" {
		err := nm.db.QueryRow(ctx, `SELECT name FROM nodes WHERE uuid = $1`, nodeUUID).Scan(&targetNodeName)
		if err != nil {
			return "", fmt.Errorf("node not found: %s", nodeUUID)
		}
	}

	if client == nil {
		return "", fmt.Errorf("node %s is not connected", targetNodeName)
	}

	payloadBytes, _ := json.Marshal(GeocheckRequestPayload{
		IP:        ip,
		Interface: iface,
	})

	taskCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	resp, err := client.SubmitTask(taskCtx, &proto.NodeTask{
		TaskId:    fmt.Sprintf("geocheck-%d", time.Now().UnixNano()),
		Operation: geocheckOperation,
		Payload:   payloadBytes,
	})
	if err != nil {
		return "", fmt.Errorf("geocheck task error: %w", err)
	}
	if resp == nil {
		return "", fmt.Errorf("geocheck returned nil status")
	}
	if resp.Code != int32(codes.OK) {
		return "", fmt.Errorf("%s", resp.Message)
	}

	return resp.Message, nil
}

func RequestGeocheck(ctx context.Context, nodeUUID string, ip string, iface string) (string, error) {
	globalMonitorMu.RLock()
	nm := globalMonitor
	globalMonitorMu.RUnlock()
	if nm == nil {
		return "", fmt.Errorf("node monitor is not ready")
	}
	return nm.ExecuteGeocheck(ctx, nodeUUID, ip, iface)
}

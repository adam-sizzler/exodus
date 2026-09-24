package users

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"exodus/internal/proto"

	"google.golang.org/grpc/codes"
)

// UserSyncItem represents a lightweight user synchronization delta.
type UserSyncItem struct {
	Action            string   `json:"action"` // "add", "delete", "disable"
	Identifier        string   `json:"identifier"`
	Username          string   `json:"username"`
	UUID              string   `json:"uuid,omitempty"`
	TrojanPassword    string   `json:"trojan_password,omitempty"`
	SSPassword        string   `json:"ss_password,omitempty"`
	Hysteria2Password string   `json:"hysteria2_password,omitempty"`
	Flow              string   `json:"flow,omitempty"`
	InboundTags       []string `json:"inbound_tags,omitempty"`
}

// SyncUsersTaskPayload wraps multiple user synchronization items for the node.
type SyncUsersTaskPayload struct {
	Users []UserSyncItem `json:"users"`
}

// RequestSyncUser dispatches a single user sync operation to connected nodes asynchronously.
func (nm *NodeMonitor) RequestSyncUser(user UserSyncItem, nodeUUIDs ...string) {
	nm.RequestSyncUsers([]UserSyncItem{user}, nodeUUIDs...)
}

// RequestSyncUsers dispatches a batch of user sync operations to connected nodes asynchronously.
func (nm *NodeMonitor) RequestSyncUsers(users []UserSyncItem, nodeUUIDs ...string) {
	if nm == nil || len(users) == 0 {
		return
	}
	go nm.syncUsersToConnectedNodes(users, nodeUUIDs)
}

func (nm *NodeMonitor) syncUsersToConnectedNodes(users []UserSyncItem, requestedNodeUUIDs []string) {
	if nm == nil || len(users) == 0 {
		return
	}

	targetFilter := make(map[string]struct{}, len(requestedNodeUUIDs))
	for _, nodeUUID := range requestedNodeUUIDs {
		trimmed := strings.TrimSpace(nodeUUID)
		if trimmed != "" {
			targetFilter[trimmed] = struct{}{}
		}
	}

	type targetItem struct {
		name   string
		uuid   string
		client proto.NodeServiceClient
	}

	nm.nodesLock.RLock()
	targets := make([]targetItem, 0, len(nm.nodes))
	for nodeName, state := range nm.nodes {
		if state == nil {
			continue
		}
		state.mutex.RLock()
		client := state.client
		isConnected := state.isConnected
		uuid := state.nodeUUID
		state.mutex.RUnlock()

		if client == nil || !isConnected || uuid == "" {
			continue
		}
		if len(targetFilter) > 0 {
			if _, allowed := targetFilter[uuid]; !allowed {
				continue
			}
		}
		targets = append(targets, targetItem{
			name:   nodeName,
			uuid:   uuid,
			client: client,
		})
	}
	nm.nodesLock.RUnlock()

	if len(targets) == 0 {
		return
	}

	payloadBytes, err := json.Marshal(SyncUsersTaskPayload{Users: users})
	if err != nil {
		if nm.cfg != nil && nm.cfg.Logger != nil {
			nm.cfg.Logger.Error("Failed to marshal sync_users payload", "error", err)
		}
		return
	}

	ctxBase := nm.globalCtx
	if ctxBase == nil {
		ctxBase = context.Background()
	}

	for _, target := range targets {
		ctx, cancel := context.WithTimeout(ctxBase, 15*time.Second)
		resp, submitErr := target.client.SubmitTask(ctx, &proto.NodeTask{
			TaskId:    fmt.Sprintf("sync-users-%d", time.Now().UnixNano()),
			Operation: "sync_users",
			Payload:   payloadBytes,
		})
		cancel()

		if submitErr != nil {
			if nm.cfg != nil && nm.cfg.Logger != nil {
				nm.cfg.Logger.Warn("Fast user sync failed on node; triggering full deploy fallback", "node", target.name, "error", submitErr)
			}
			nm.RequestDeploy(true, target.uuid)
			continue
		}

		if resp != nil && resp.Code == int32(codes.FailedPrecondition) {
			// Node reported base config is not yet deployed
			if nm.cfg != nil && nm.cfg.Logger != nil {
				nm.cfg.Logger.Info("Node requires full initial deploy before delta sync", "node", target.name)
			}
			nm.RequestDeploy(true, target.uuid)
			continue
		}

		if nm.cfg != nil && nm.cfg.Logger != nil {
			nm.cfg.Logger.Debug("Fast user sync applied on node", "node", target.name, "users_count", len(users))
		}
	}
}

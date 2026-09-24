package users

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"exodus/internal/scheduler"
)

var (
	globalMonitorMu sync.RWMutex
	globalMonitor   *NodeMonitor
)

func init() {
	scheduler.SetNodeDeployCallback(RequestNodeDeploy)
}

// RegisterGlobalNodeMonitor stores a global reference for API-triggered syncs.
func RegisterGlobalNodeMonitor(nm *NodeMonitor) {
	globalMonitorMu.Lock()
	globalMonitor = nm
	globalMonitorMu.Unlock()
	scheduler.SetNodeDeployCallback(RequestNodeDeploy)
}

// RequestNodeSync triggers an immediate sync if a monitor is registered.
func RequestNodeSync() {
	globalMonitorMu.RLock()
	nm := globalMonitor
	globalMonitorMu.RUnlock()
	if nm != nil {
		nm.RequestSync()
	}
}

// RequestNodeDeploy triggers config deploy to connected nodes.
func RequestNodeDeploy(restart bool, nodeUUIDs ...string) {
	globalMonitorMu.RLock()
	nm := globalMonitor
	globalMonitorMu.RUnlock()
	if nm != nil {
		nm.RequestDeploy(restart, nodeUUIDs...)
	}
}

// RequestNodeDeployWithForce triggers config deploy and forces core reload on target nodes.
func RequestNodeDeployWithForce(restart bool, forceRestart bool, nodeUUIDs ...string) {
	globalMonitorMu.RLock()
	nm := globalMonitor
	globalMonitorMu.RUnlock()
	if nm != nil {
		nm.RequestDeployWithForce(restart, forceRestart, nodeUUIDs...)
	}
}

// RequestSyncUser sends a fast incremental user sync to target connected nodes.
func RequestSyncUser(user UserSyncItem, nodeUUIDs ...string) {
	globalMonitorMu.RLock()
	nm := globalMonitor
	globalMonitorMu.RUnlock()
	if nm != nil {
		nm.RequestSyncUser(user, nodeUUIDs...)
	}
}

// RequestSyncUsers sends a batch of user sync items to target connected nodes.
func RequestSyncUsers(users []UserSyncItem, nodeUUIDs ...string) {
	globalMonitorMu.RLock()
	nm := globalMonitor
	globalMonitorMu.RUnlock()
	if nm != nil {
		nm.RequestSyncUsers(users, nodeUUIDs...)
	}
}

// RequestNodePluginExecutor sends a node-plugin runtime command to connected nodes.
func RequestNodePluginExecutor(command json.RawMessage, nodeUUIDs ...string) error {
	globalMonitorMu.RLock()
	nm := globalMonitor
	globalMonitorMu.RUnlock()
	if nm == nil {
		return fmt.Errorf("node monitor is not ready")
	}
	return nm.ExecuteNodePluginCommand(context.Background(), command, nodeUUIDs)
}

// GetNodeMetricsSnapshot returns current per-node traffic metrics snapshots.
func GetNodeMetricsSnapshot() map[string]NodeMetricsSnapshot {
	globalMonitorMu.RLock()
	nm := globalMonitor
	globalMonitorMu.RUnlock()
	if nm == nil {
		return map[string]NodeMetricsSnapshot{}
	}
	return nm.SnapshotNodeMetrics()
}

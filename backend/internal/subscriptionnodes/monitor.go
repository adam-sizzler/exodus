package subscriptionnodes

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"

	"exodus/internal/config"
	"exodus/internal/scheduler"

	"github.com/jackc/pgx/v5/pgxpool"
)

type SubNodeMonitor struct {
	db    *pgxpool.Pool
	sqlDB *sql.DB // temporary bridge for SubpageConfigPublicHandler and SubscriptionPublicHandler until Stage 8
	cfg   *config.BackendConfig

	nodes     map[string]*subNodeState
	nodesLock sync.RWMutex

	runtimeByNodeName map[string]SubNodeRuntimeSnapshot
	runtimeMu         sync.RWMutex

	globalCtx    context.Context
	globalCancel context.CancelFunc

	syncNow        chan struct{}
	deployNow      chan []string
	subpagePushNow chan subpageConfigPushCommand
	srsSyncNow     chan []string
}

func NewSubNodeMonitor(db *pgxpool.Pool, sqlDB *sql.DB, cfg *config.BackendConfig) *SubNodeMonitor {
	return &SubNodeMonitor{
		db:                db,
		sqlDB:             sqlDB,
		cfg:               cfg,
		nodes:             make(map[string]*subNodeState),
		runtimeByNodeName: make(map[string]SubNodeRuntimeSnapshot),
		syncNow:           make(chan struct{}, 1),
		deployNow:         make(chan []string, 1),
		subpagePushNow:    make(chan subpageConfigPushCommand, 1),
		srsSyncNow:        make(chan []string, 1),
	}
}

func (sm *SubNodeMonitor) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	sm.globalCtx = ctx
	sm.globalCancel = func() {
		sm.nodesLock.RLock()
		defer sm.nodesLock.RUnlock()
		for _, state := range sm.nodes {
			state.cancel()
		}
	}

	sm.cfg.Logger.Trace("Subscription monitor initial sync")
	sm.syncNodes()

	// Periodic sync every 30 seconds
	syncTicker := time.NewTicker(scheduler.RecordNodeUsageInterval)
	defer syncTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			sm.cfg.Logger.Info("Subscription monitor stopping")
			sm.stopAll()
			return
		case <-sm.syncNow:
			sm.cfg.Logger.Debug("Subscription monitor manual sync requested")
			sm.syncNodes()
		case targets := <-sm.deployNow:
			sm.deployToConnectedNodes(targets)
		case push := <-sm.subpagePushNow:
			sm.pushSubpageConfigToConnectedNodes(push)
		case targets := <-sm.srsSyncNow:
			sm.cfg.Logger.Info("Subscription node SRS sync requested", "node_targets", len(targets))
			sm.syncSRSListsToConnectedNodes(targets)
		case <-syncTicker.C:
			sm.syncNodes()
		}
	}
}

func (sm *SubNodeMonitor) syncNodes() {
	dbNodes, err := sm.loadActiveNodes()
	if err != nil {
		sm.cfg.Logger.Warn("Failed to load subscription nodes from DB", "error", err)
		return
	}

	desired := make(map[string]dbSubNode, len(dbNodes))
	for _, n := range dbNodes {
		desired[n.Name] = n
	}

	sm.nodesLock.Lock()

	toStart := make(map[string]dbSubNode)
	for name, state := range sm.nodes {
		desiredNode, exists := desired[name]
		if !exists {
			sm.cfg.Logger.Debug("Subscription node removed from DB, stopping monitor", "node", name)
			state.cancel()
			if state.conn != nil {
				_ = state.conn.Close()
			}
			delete(sm.nodes, name)
			sm.deleteRuntimeSnapshot(name)
			continue
		}
		if subNodeConfigChanged(state, desiredNode) {
			sm.cfg.Logger.Info(
				"Subscription node config changed, restarting monitor",
				"node", name,
				"old_address", state.address,
				"new_address", desiredNode.Address,
				"old_port", state.port,
				"new_port", desiredNode.Port,
				"old_schema", state.apiSchema,
				"new_schema", desiredNode.APISchema,
				"old_path", state.apiPath,
				"new_path", desiredNode.APIPath,
				"old_subpage_config_uuid", state.subpageConfigUUID,
				"new_subpage_config_uuid", desiredNode.SubpageConfigUUID,
			)
			state.cancel()
			if state.conn != nil {
				_ = state.conn.Close()
			}
			delete(sm.nodes, name)
			toStart[name] = desiredNode
		}
	}

	for name, node := range desired {
		if _, exists := sm.nodes[name]; !exists {
			toStart[name] = node
		}
	}

	tasks := make([]subNodeStartupTask, 0, len(toStart))
	for name, node := range toStart {
		tasks = append(tasks, sm.registerSubNodeState(name, node))
	}

	sm.nodesLock.Unlock()

	sm.launchSubNodes(tasks)
}

// subNodeStartupConcurrency bounds how many subscription nodes we mark "connecting" in
// Postgres (and hand off to a monitor goroutine) at the same time during a sync.
const subNodeStartupConcurrency = 10

type subNodeStartupTask struct {
	name  string
	state *subNodeState
}

func (sm *SubNodeMonitor) registerSubNodeState(name string, dbNode dbSubNode) subNodeStartupTask {
	ctx, cancel := context.WithCancel(sm.globalCtx)
	state := &subNodeState{
		nodeUUID:          dbNode.UUID,
		nodeName:          dbNode.Name,
		address:           dbNode.Address,
		port:              dbNode.Port,
		apiSchema:         dbNode.APISchema,
		apiPath:           dbNode.APIPath,
		grpcAuthToken:     dbNode.GRPCAuthToken,
		subpageConfigUUID: dbNode.SubpageConfigUUID,
		ctx:               ctx,
		cancel:            cancel,
	}

	sm.nodes[name] = state

	return subNodeStartupTask{name: name, state: state}
}

func (sm *SubNodeMonitor) launchSubNodes(tasks []subNodeStartupTask) {
	if len(tasks) == 0 {
		return
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, subNodeStartupConcurrency)

	for _, task := range tasks {
		wg.Add(1)
		sem <- struct{}{}

		go func(task subNodeStartupTask) {
			defer wg.Done()
			defer func() { <-sem }()

			if task.state == nil || (task.state.ctx != nil && task.state.ctx.Err() != nil) {
				return
			}

			// Mark as connecting in DB
			sm.updateConnectionStatus(task.name, false, true, "Connecting...")

			if task.state.ctx != nil && task.state.ctx.Err() != nil {
				return
			}

			go sm.monitorNode(task.state)

			sm.cfg.Logger.Debug(
				"Started monitoring subscription node",
				"node", task.name,
				"address", task.state.address,
				"port", task.state.port,
				"schema", task.state.apiSchema,
				"path", task.state.apiPath,
				"subpage_config_uuid", task.state.subpageConfigUUID,
			)
		}(task)
	}

	wg.Wait()
}

func (sm *SubNodeMonitor) stopAll() {
	sm.nodesLock.Lock()
	defer sm.nodesLock.Unlock()

	for name, state := range sm.nodes {
		sm.cfg.Logger.Trace("Canceling subscription node monitor context", "node", name)
		state.cancel()
		if state.conn != nil {
			_ = state.conn.Close()
		}
		delete(sm.nodes, name)
		sm.deleteRuntimeSnapshot(name)
	}
}

func (sm *SubNodeMonitor) RuntimeSnapshot(nodeName string) (SubNodeRuntimeSnapshot, bool) {
	if sm == nil {
		return SubNodeRuntimeSnapshot{}, false
	}

	cleanNodeName := strings.TrimSpace(nodeName)
	if cleanNodeName == "" {
		return SubNodeRuntimeSnapshot{}, false
	}

	sm.runtimeMu.RLock()
	snapshot, ok := sm.runtimeByNodeName[cleanNodeName]
	sm.runtimeMu.RUnlock()
	if !ok {
		return SubNodeRuntimeSnapshot{}, false
	}

	return cloneRuntimeSnapshot(snapshot), true
}

func (sm *SubNodeMonitor) deleteRuntimeSnapshot(nodeName string) {
	if sm == nil {
		return
	}

	cleanNodeName := strings.TrimSpace(nodeName)
	if cleanNodeName == "" {
		return
	}

	sm.runtimeMu.Lock()
	delete(sm.runtimeByNodeName, cleanNodeName)
	sm.runtimeMu.Unlock()
}

func (sm *SubNodeMonitor) Stop() {
	if sm != nil && sm.globalCancel != nil {
		sm.globalCancel()
	}
}

func (sm *SubNodeMonitor) RequestSync() {
	if sm == nil {
		return
	}
	select {
	case sm.syncNow <- struct{}{}:
	default:
	}
}

func (sm *SubNodeMonitor) RequestDeploy(nodeUUIDs ...string) {
	if sm == nil {
		return
	}
	normalized := normalizeSubNodeUUIDTargets(nodeUUIDs)
	select {
	case sm.deployNow <- normalized:
	default:
		select {
		case prev := <-sm.deployNow:
			normalized = mergeSubNodeTargets(prev, normalized)
		default:
		}
		sm.deployNow <- normalized
	}
}

func (sm *SubNodeMonitor) RequestSRSDeploy(nodeUUIDs ...string) {
	if sm == nil {
		return
	}
	normalized := normalizeSubNodeUUIDTargets(nodeUUIDs)
	select {
	case sm.srsSyncNow <- normalized:
	default:
		select {
		case prev := <-sm.srsSyncNow:
			normalized = mergeSubNodeTargets(prev, normalized)
		default:
		}
		sm.srsSyncNow <- normalized
	}
}

func (sm *SubNodeMonitor) RequestSubpageConfigPush(uuid string, config []byte, nodeUUIDs ...string) {
	if sm == nil {
		return
	}

	payload := make([]byte, len(config))
	copy(payload, config)
	command := subpageConfigPushCommand{
		uuid:        strings.TrimSpace(uuid),
		config:      payload,
		targetUUIDs: normalizeSubNodeUUIDTargets(nodeUUIDs),
	}
	sm.cfg.Logger.Debug(
		"Subpage config push queued",
		"uuid", command.uuid,
		"target_nodes_count", len(command.targetUUIDs),
		"payload_bytes", len(command.config),
	)

	select {
	case sm.subpagePushNow <- command:
	default:
		select {
		case prev := <-sm.subpagePushNow:
			if prev.uuid == command.uuid {
				command.targetUUIDs = mergeSubNodeTargets(prev.targetUUIDs, command.targetUUIDs)
			}
		default:
		}
		sm.subpagePushNow <- command
	}
}

func mergeSubNodeTargets(a, b []string) []string {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	result := make([]string, 0, len(a)+len(b))
	for _, id := range a {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			result = append(result, id)
		}
	}
	for _, id := range b {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			result = append(result, id)
		}
	}
	return result
}

func normalizeSubNodeUUIDTargets(raw []string) []string {
	if len(raw) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(raw))
	result := make([]string, 0, len(raw))
	for _, item := range raw {
		uuid := strings.TrimSpace(item)
		if uuid == "" {
			continue
		}
		if _, exists := seen[uuid]; exists {
			continue
		}
		seen[uuid] = struct{}{}
		result = append(result, uuid)
	}
	return result
}

func subNodeConfigChanged(state *subNodeState, desired dbSubNode) bool {
	if state.address != desired.Address {
		return true
	}
	if state.port != desired.Port {
		return true
	}
	if strings.TrimSpace(state.apiSchema) != strings.TrimSpace(desired.APISchema) {
		return true
	}
	if strings.TrimSpace(state.apiPath) != strings.TrimSpace(desired.APIPath) {
		return true
	}
	if strings.TrimSpace(state.grpcAuthToken) != strings.TrimSpace(desired.GRPCAuthToken) {
		return true
	}
	if normalizeAssignedSubpageConfigUUID(state.subpageConfigUUID) != normalizeAssignedSubpageConfigUUID(desired.SubpageConfigUUID) {
		return true
	}
	return false
}

func cloneRuntimeSnapshot(snapshot SubNodeRuntimeSnapshot) SubNodeRuntimeSnapshot {
	cloned := SubNodeRuntimeSnapshot{
		SingboxUptime: snapshot.SingboxUptime,
	}

	if snapshot.SingboxVersion != nil {
		cloned.SingboxVersion = new(*snapshot.SingboxVersion)
	}
	if snapshot.NodeVersion != nil {
		cloned.NodeVersion = new(*snapshot.NodeVersion)
	}
	if snapshot.CPUCount != nil {
		cloned.CPUCount = new(*snapshot.CPUCount)
	}
	if snapshot.CPUModel != nil {
		cloned.CPUModel = new(*snapshot.CPUModel)
	}
	if snapshot.TotalRAM != nil {
		cloned.TotalRAM = new(*snapshot.TotalRAM)
	}
	if cloned.SingboxUptime == "" {
		cloned.SingboxUptime = "0"
	}

	return cloned
}

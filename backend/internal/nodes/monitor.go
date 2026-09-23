package users

import (
	"context"
	"strings"
	"sync"
	"time"

	"exodus/internal/config"
	"exodus/internal/db"
	"exodus/internal/nodehotcache"
	"exodus/internal/scheduler"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NodeMonitor dynamically manages node monitoring with status tracking.
type NodeMonitor struct {
	db  *pgxpool.Pool
	cfg *config.BackendConfig

	// Active node contexts
	nodes     map[string]*nodeState
	nodesLock sync.RWMutex

	// Global context for shutdown
	globalCtx    context.Context
	globalCancel context.CancelFunc

	// Manual sync trigger
	syncNow chan struct{}
	// Manual deploy trigger
	deployNow chan deployRequest
	// Runtime traffic metrics snapshots by node UUID.
	metricsByNodeUUID map[string]*NodeMetricsSnapshot
	metricsLock       sync.RWMutex

	usageRecorder NodeUserUsageRecorder
	hotCache                *nodehotcache.Cache
	statusLock              sync.Mutex
	nodeMetaCache           sync.Map
	lastIdleHeartbeatUpdate sync.Map
}

type nodeMetadata struct {
	UUID                      string
	ID                        int64
	ConsumptionMultiplier     int64
	NodeConsumptionMultiplier int64
}

func (nm *NodeMonitor) getNodeMetadata(nodeName string) (string, int64, int64, int64, error) {
	if val, ok := nm.nodeMetaCache.Load(nodeName); ok {
		meta := val.(nodeMetadata)
		return meta.UUID, meta.ID, meta.ConsumptionMultiplier, meta.NodeConsumptionMultiplier, nil
	}

	var meta nodeMetadata
	ctx := context.Background()
	if nm.globalCtx != nil {
		ctx = nm.globalCtx
	}
	if err := nm.db.QueryRow(ctx, `SELECT uuid, id, consumption_multiplier, node_consumption_multiplier FROM nodes WHERE name = $1`, nodeName).Scan(
		&meta.UUID, &meta.ID, &meta.ConsumptionMultiplier, &meta.NodeConsumptionMultiplier,
	); err != nil {
		return "", 0, 0, 0, err
	}

	nm.nodeMetaCache.Store(nodeName, meta)
	return meta.UUID, meta.ID, meta.ConsumptionMultiplier, meta.NodeConsumptionMultiplier, nil
}

// NewNodeMonitor creates a new NodeMonitor.
func NewNodeMonitor(db *pgxpool.Pool, cfg *config.BackendConfig) *NodeMonitor {
	return &NodeMonitor{
		db:                db,
		cfg:               cfg,
		nodes:             make(map[string]*nodeState),
		syncNow:           make(chan struct{}, 1),
		deployNow:         make(chan deployRequest, 1),
		metricsByNodeUUID: make(map[string]*NodeMetricsSnapshot),
		hotCache:          nodehotcache.Default(cfg),
	}
}

func (nm *NodeMonitor) SetNodeUserUsageRecorder(recorder NodeUserUsageRecorder) {
	nm.usageRecorder = recorder
}

// Start begins the node monitoring loop.
func (nm *NodeMonitor) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// Store global context
	nm.globalCtx = ctx

	// Initialize cancel function for Stop()
	nm.globalCancel = func() {
		// Cancel all node contexts
		nm.nodesLock.RLock()
		defer nm.nodesLock.RUnlock()
		for _, state := range nm.nodes {
			state.mutex.Lock()
			if state.cancel != nil {
				state.cancel()
			}
			state.mutex.Unlock()
		}
	}

	// Initial load and start
	nm.cfg.Logger.Trace("Node monitor initial sync")
	nm.syncNodes()
	go nm.cleanupInactiveNodesHotCache()

	// Periodic sync every 30 seconds
	syncTicker := time.NewTicker(scheduler.RecordNodeUsageInterval)
	defer syncTicker.Stop()

	// Periodic watchdog for disconnected active nodes every NodeHealthCheckInterval (10 seconds)
	watchdogTicker := time.NewTicker(scheduler.NodeHealthCheckInterval)
	defer watchdogTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			nm.cfg.Logger.Info("Node monitor stopping")
			nm.stopAll()
			return
		case <-nm.syncNow:
			nm.cfg.Logger.Debug("Node monitor manual sync requested")
			nm.syncNodes()
		case deployReq := <-nm.deployNow:
			nm.cfg.Logger.Debug(
				"Node monitor deploy requested",
				"restart", deployReq.Restart,
				"force_restart", deployReq.ForceRestart,
				"node_targets", len(deployReq.NodeUUIDs),
			)
			nm.deployToConnectedNodes(deployReq.Restart, deployReq.ForceRestart, deployReq.NodeUUIDs)
		case <-syncTicker.C:
			nm.syncNodes()
		case <-watchdogTicker.C:
			nm.retryFailedNodes()
		}
	}
}

// retryFailedNodes automatically re-attempts deploy for active nodes whose core failed to start or is disconnected in DB.
func (nm *NodeMonitor) retryFailedNodes() {
	if nm == nil || nm.db == nil {
		return
	}

	// Fast in-memory pre-check: only nodes with an active client connection can be redeployed.
	candidates := make(map[string]string) // name -> uuid
	nm.nodesLock.RLock()
	for name, state := range nm.nodes {
		if state == nil {
			continue
		}
		state.mutex.RLock()
		hasClient := state.client != nil && state.isConnected
		uuid := state.nodeUUID
		state.mutex.RUnlock()
		if hasClient && uuid != "" {
			candidates[name] = uuid
		}
	}
	nm.nodesLock.RUnlock()

	if len(candidates) == 0 {
		return
	}

	ctx := nm.globalCtx
	if ctx == nil {
		ctx = context.Background()
	}

	candidateNames := make([]string, 0, len(candidates))
	for name := range candidates {
		candidateNames = append(candidateNames, name)
	}

	rows, err := nm.db.Query(ctx, `SELECT uuid::text FROM nodes WHERE is_disabled = false AND is_connected = false AND name = ANY($1)`, candidateNames)
	if err != nil {
		nm.cfg.Logger.Debug("Failed to query disconnected nodes for watchdog retry", "error", err)
		return
	}
	defer rows.Close()

	var failedTargets []string
	for rows.Next() {
		var uuid string
		if scanErr := rows.Scan(&uuid); scanErr == nil && uuid != "" {
			failedTargets = append(failedTargets, uuid)
		}
	}
	if err := rows.Err(); err != nil {
		nm.cfg.Logger.Debug("Error iterating disconnected nodes", "error", err)
	}

	if len(failedTargets) > 0 {
		nm.cfg.Logger.Debug("Node watchdog retrying deploy for active disconnected nodes", "count", len(failedTargets), "node_uuids", failedTargets)
		nm.deployToConnectedNodes(true, false, failedTargets)
	}
}

// syncNodes synchronizes monitored nodes with database.
func (nm *NodeMonitor) syncNodes() {
	// Load active nodes from DB
	dbNodes, err := nm.loadActiveNodes()
	if err != nil {
		nm.cfg.Logger.Warn("Failed to load nodes from DB", "error", err)
		return
	}

	// Build desired state
	desired := make(map[string]db.DBNode)
	for _, n := range dbNodes {
		desired[n.Name] = n
	}
	nm.cfg.Logger.Debug("Node monitor sync complete", "nodes", len(desired))

	nm.nodesLock.Lock()

	toStart := make(map[string]db.DBNode)

	// Stop removed or changed nodes
	for name, state := range nm.nodes {
		desiredNode, exists := desired[name]
		if !exists {
			nm.cfg.Logger.Debug("Node removed from DB, stopping monitor", "node", name)
			nm.closeNodeState(state)
			if state.nodeUUID != "" {
				nm.removeNodeMetrics(state.nodeUUID)
			}
			nm.nodeMetaCache.Delete(name)
			delete(nm.nodes, name)
			continue
		}
		if nodeConfigChanged(state, desiredNode) {
			nm.cfg.Logger.Info(
				"Node config changed, restarting monitor",
				"node", name,
				"old_address", state.address, "new_address", desiredNode.Address,
				"old_port", state.port, "new_port", desiredNode.Port,
				"old_proxy_url", state.proxyURL, "new_proxy_url", desiredNode.ProxyURL,
				"old_schema", state.apiSchema, "new_schema", desiredNode.APISchema,
				"old_path", state.apiPath, "new_path", desiredNode.APIPath,
			)
			nm.closeNodeState(state)
			nm.nodeMetaCache.Delete(name)
			delete(nm.nodes, name)
			toStart[name] = desiredNode
		}
	}

	// Start new nodes
	for name, dbNode := range desired {
		if _, exists := nm.nodes[name]; !exists {
			toStart[name] = dbNode
		}
	}

	// Register in-memory state for every node we're about to (re)start while
	// still holding the write lock — this part is pure map manipulation, no
	// I/O, so it's cheap even for a large batch. The slow part (marking the
	// node as connecting in Postgres, one row at a time) is deferred to
	// launchNodes, which runs after the lock is released — see its comment
	// for why.
	tasks := make([]nodeStartupTask, 0, len(toStart))
	for name, dbNode := range toStart {
		tasks = append(tasks, nm.registerNodeState(name, dbNode))
	}

	nm.nodesLock.Unlock()

	nm.launchNodes(tasks)
}

// loadActiveNodes loads enabled nodes from database.
func (nm *NodeMonitor) loadActiveNodes() ([]db.DBNode, error) {
	ctx := nm.globalCtx
	if ctx == nil {
		ctx = context.Background()
	}
	nodes, err := db.LoadNodesFromDB(ctx, nm.db, nm.cfg)
	if err != nil {
		return nil, err
	}

	for i := range nodes {
		nodes[i].APISchema = normalizeNodeSchema(nodes[i].APISchema)
		nodes[i].APIPath = normalizeNodePath(nodes[i].APIPath)
	}
	return nodes, nil
}

// nodeStartupConcurrency bounds how many nodes we mark "connecting" in
// Postgres (and hand off to a monitor goroutine) at the same time during a
// single sync. Without this, a large batch — e.g. the very first sync after
// startup with hundreds/thousands of nodes already in the DB — would fire
// that many sequential blocking SQL round-trips back to back. Mirrors the
// bounded-concurrency pattern used for the equivalent work on the upstream
// worker side (concurrency 40 for node health checks, 20 for starting all
// nodes in a config profile).
const nodeStartupConcurrency = 32

// nodeStartupTask carries a node whose in-memory state has already been
// registered (see registerNodeState) and just needs its DB status write and
// monitor goroutine.
type nodeStartupTask struct {
	name  string
	state *nodeState
}

// registerNodeState creates a node's in-memory state and adds it to
// nm.nodes. The caller must hold nm.nodesLock for writing; this only touches
// the in-memory map, no I/O, so it's safe to do for an entire batch without
// releasing the lock in between.
func (nm *NodeMonitor) registerNodeState(name string, dbNode db.DBNode) nodeStartupTask {
	ctx, cancel := context.WithCancel(nm.globalCtx)

	state := &nodeState{
		nodeUUID:      dbNode.UUID,
		nodeName:      dbNode.Name,
		address:       dbNode.Address,
		port:          dbNode.Port,
		proxyURL:      dbNode.ProxyURL,
		apiSchema:     dbNode.APISchema,
		apiPath:       dbNode.APIPath,
		grpcAuthToken: dbNode.GRPCAuthToken,
		ctx:           ctx,
		cancel:        cancel,
	}

	nm.nodes[name] = state

	return nodeStartupTask{name: name, state: state}
}

// launchNodes marks tasks' nodes as connecting in the DB in a single batch query
// and starts their monitor goroutines. It is called without nm.nodesLock held.
func (nm *NodeMonitor) launchNodes(tasks []nodeStartupTask) {
	if len(tasks) == 0 {
		return
	}

	names := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if task.name != "" {
			names = append(names, task.name)
		}
	}

	if len(names) > 0 && nm.db != nil {
		ctx := nm.globalCtx
		if ctx == nil {
			ctx = context.Background()
		}
		_, err := nm.db.Exec(ctx, `
			UPDATE nodes
			SET is_connected = false,
			    is_connecting = true,
			    last_status_message = NULL,
			    updated_at = CURRENT_TIMESTAMP
			WHERE name = ANY($1)
		`, names)
		if err != nil {
			nm.cfg.Logger.Warn("Failed to batch update nodes connecting status", "error", err, "nodes_count", len(names))
		}
	}

	for _, task := range tasks {
		if task.state == nil || (task.state.ctx != nil && task.state.ctx.Err() != nil) {
			continue
		}

		go nm.monitorNode(task.state)

		nm.cfg.Logger.Debug(
			"Started monitoring node",
			"node", task.name,
			"address", task.state.address,
			"port", task.state.port,
			"schema", task.state.apiSchema,
			"path", task.state.apiPath,
		)
	}
}

func (nm *NodeMonitor) Stop() {
	if nm != nil && nm.globalCancel != nil {
		nm.globalCancel()
	}
}

// closeNodeState safely cancels contexts and closes grpc connection under state.mutex.
func (nm *NodeMonitor) closeNodeState(state *nodeState) {
	if state == nil {
		return
	}
	state.mutex.Lock()
	if state.cancel != nil {
		state.cancel()
	}
	if state.streamCancel != nil {
		state.streamCancel()
		state.streamCancel = nil
	}
	state.stream = nil
	if state.conn != nil {
		_ = state.conn.Close()
		state.conn = nil
	}
	state.client = nil
	state.isConnected = false
	state.isConnecting = false
	state.hasSentStaticInfo = false
	state.lastStaticInfoSentAt = time.Time{}
	state.lastSingboxVer = ""
	state.lastNodeVer = ""
	nodeUUID := state.nodeUUID
	state.mutex.Unlock()

	if nm.hotCache != nil && nodeUUID != "" {
		_ = nm.hotCache.DeleteTransient(context.Background(), nodeUUID)
	}
}

// cleanupInactiveNodesHotCache removes transient Redis hot cache data for offline or disabled nodes.
func (nm *NodeMonitor) cleanupInactiveNodesHotCache() {
	if nm == nil || nm.hotCache == nil || nm.db == nil {
		return
	}
	ctx := nm.globalCtx
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := nm.db.Query(ctx, `SELECT uuid::text FROM nodes WHERE is_disabled = true OR is_connected = false`)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err == nil && u != "" {
			_ = nm.hotCache.DeleteTransient(ctx, u)
		}
	}
}

// stopAll cancels and stops all connections.
func (nm *NodeMonitor) stopAll() {
	nm.nodesLock.Lock()
	defer nm.nodesLock.Unlock()

	for name, state := range nm.nodes {
		nm.cfg.Logger.Trace("Canceling node monitor context", "node", name)
		nm.closeNodeState(state)
		delete(nm.nodes, name)
	}
}

// RefreshNodes manually triggers a sync loop.
func (nm *NodeMonitor) RefreshNodes() {
	select {
	case nm.syncNow <- struct{}{}:
	default:
	}
}

// IsNodeConnected checks if a node is connected.
func (nm *NodeMonitor) IsNodeConnected(nodeName string) bool {
	nm.nodesLock.RLock()
	defer nm.nodesLock.RUnlock()

	state, exists := nm.nodes[nodeName]
	if !exists || state == nil {
		return false
	}

	state.mutex.RLock()
	defer state.mutex.RUnlock()
	return state.isConnected
}

// GetMetricsSnapshot returns metrics for a node.
func (nm *NodeMonitor) GetMetricsSnapshot(nodeUUID string) *NodeMetricsSnapshot {
	nm.metricsLock.RLock()
	defer nm.metricsLock.RUnlock()
	return nm.metricsByNodeUUID[nodeUUID]
}

func (nm *NodeMonitor) updateNodeMetricsSnapshot(nodeUUID string, usersOnline int, delta trafficStatsDelta) {
	if nm == nil || strings.TrimSpace(nodeUUID) == "" {
		return
	}

	nm.metricsLock.Lock()
	defer nm.metricsLock.Unlock()

	snapshot, exists := nm.metricsByNodeUUID[nodeUUID]
	if !exists || snapshot == nil {
		snapshot = &NodeMetricsSnapshot{
			NodeUUID:  nodeUUID,
			Inbounds:  make(map[string]TagTrafficCounters),
			Outbounds: make(map[string]TagTrafficCounters),
		}
		nm.metricsByNodeUUID[nodeUUID] = snapshot
	}

	snapshot.UsersOnline = usersOnline
	snapshot.UpdatedAt = time.Now().UTC()

	for tag, item := range delta.InboundByTag {
		current := snapshot.Inbounds[tag]
		current.UploadBytes += item.UploadBytes
		current.DownloadBytes += item.DownloadBytes
		snapshot.Inbounds[tag] = current
	}

	for tag, item := range delta.OutboundByTag {
		current := snapshot.Outbounds[tag]
		current.UploadBytes += item.UploadBytes
		current.DownloadBytes += item.DownloadBytes
		snapshot.Outbounds[tag] = current
	}
}

func (nm *NodeMonitor) removeNodeMetrics(nodeUUID string) {
	if nm == nil || strings.TrimSpace(nodeUUID) == "" {
		return
	}
	nm.metricsLock.Lock()
	defer nm.metricsLock.Unlock()
	delete(nm.metricsByNodeUUID, nodeUUID)
}

func (nm *NodeMonitor) SnapshotNodeMetrics() map[string]NodeMetricsSnapshot {
	if nm == nil {
		return map[string]NodeMetricsSnapshot{}
	}

	nm.metricsLock.RLock()
	defer nm.metricsLock.RUnlock()

	result := make(map[string]NodeMetricsSnapshot, len(nm.metricsByNodeUUID))
	for nodeUUID, source := range nm.metricsByNodeUUID {
		if source == nil {
			continue
		}

		copySnapshot := NodeMetricsSnapshot{
			NodeUUID:    source.NodeUUID,
			UsersOnline: source.UsersOnline,
			Inbounds:    make(map[string]TagTrafficCounters, len(source.Inbounds)),
			Outbounds:   make(map[string]TagTrafficCounters, len(source.Outbounds)),
			UpdatedAt:   source.UpdatedAt,
		}
		for tag, item := range source.Inbounds {
			copySnapshot.Inbounds[tag] = item
		}
		for tag, item := range source.Outbounds {
			copySnapshot.Outbounds[tag] = item
		}

		result[nodeUUID] = copySnapshot
	}
	return result
}

func (nm *NodeMonitor) RequestSync() {
	if nm == nil {
		return
	}
	select {
	case nm.syncNow <- struct{}{}:
	default:
	}
}

func (nm *NodeMonitor) RequestDeploy(restart bool, nodeUUIDs ...string) {
	nm.RequestDeployWithForce(restart, false, nodeUUIDs...)
}

func (nm *NodeMonitor) RequestDeployWithForce(restart bool, forceRestart bool, nodeUUIDs ...string) {
	if nm == nil {
		return
	}
	normalizedTargets := normalizeNodeUUIDTargets(nodeUUIDs)
	req := deployRequest{
		Restart:      restart,
		ForceRestart: forceRestart,
		NodeUUIDs:    normalizedTargets,
	}
	if nm.cfg != nil && nm.cfg.Logger != nil {
		nm.cfg.Logger.Debug("Node deploy requested", "restart", restart, "force_restart", forceRestart, "node_targets", len(normalizedTargets))
	}
	select {
	case nm.deployNow <- req:
		if nm.cfg != nil && nm.cfg.Logger != nil {
			nm.cfg.Logger.Debug("Node deploy queue accepted request", "restart", restart, "force_restart", forceRestart, "node_targets", len(normalizedTargets))
		}
	default:
		// Drain and merge pending request to prevent lost deploy targets
		select {
		case prev := <-nm.deployNow:
			req = mergeDeployRequests(prev, req)
		default:
		}
		select {
		case nm.deployNow <- req:
			if nm.cfg != nil && nm.cfg.Logger != nil {
				nm.cfg.Logger.Debug("Node deploy queue merged pending request", "restart", req.Restart, "force_restart", req.ForceRestart, "node_targets", len(req.NodeUUIDs))
			}
		default:
			if nm.cfg != nil && nm.cfg.Logger != nil {
				nm.cfg.Logger.Warn("Node deploy queue full, dropping deploy request", "restart", req.Restart, "node_targets", len(req.NodeUUIDs))
			}
		}
	}
}

func mergeDeployRequests(a, b deployRequest) deployRequest {
	merged := deployRequest{
		Restart:      a.Restart || b.Restart,
		ForceRestart: a.ForceRestart || b.ForceRestart,
	}
	if len(a.NodeUUIDs) == 0 || len(b.NodeUUIDs) == 0 {
		merged.NodeUUIDs = nil
		return merged
	}
	seen := make(map[string]struct{}, len(a.NodeUUIDs)+len(b.NodeUUIDs))
	mergedTargets := make([]string, 0, len(a.NodeUUIDs)+len(b.NodeUUIDs))
	for _, id := range a.NodeUUIDs {
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			mergedTargets = append(mergedTargets, id)
		}
	}
	for _, id := range b.NodeUUIDs {
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			mergedTargets = append(mergedTargets, id)
		}
	}
	merged.NodeUUIDs = mergedTargets
	return merged
}

func normalizeNodeUUIDTargets(raw []string) []string {
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
	if len(result) == 0 {
		return nil
	}
	return result
}

func nodeConfigChanged(state *nodeState, desired db.DBNode) bool {
	if state.address != desired.Address {
		return true
	}
	if state.port != desired.Port {
		return true
	}
	if strings.TrimSpace(state.proxyURL) != strings.TrimSpace(desired.ProxyURL) {
		return true
	}
	if normalizeNodeSchema(state.apiSchema) != normalizeNodeSchema(desired.APISchema) {
		return true
	}
	if normalizeNodePath(state.apiPath) != normalizeNodePath(desired.APIPath) {
		return true
	}
	if strings.TrimSpace(state.grpcAuthToken) != strings.TrimSpace(desired.GRPCAuthToken) {
		return true
	}
	return false
}

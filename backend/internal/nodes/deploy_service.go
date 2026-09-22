package users

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"exodus/internal/logger"
	"exodus/internal/proto"

	"google.golang.org/grpc/codes"
)

// nodeDeployConcurrency bounds how many nodes we deploy configurations to
// concurrently. Mirrors the bounded-concurrency pattern (concurrency 20)
// used for starting all nodes in a profile.
const nodeDeployConcurrency = 20

func (nm *NodeMonitor) deployToConnectedNodes(restart bool, forceRestart bool, requestedNodeUUIDs []string) {
	if nm == nil {
		return
	}
	nm.cfg.Logger.Debug("Deploying node configs", "restart", restart, "force_restart", forceRestart, "requested_node_targets", len(requestedNodeUUIDs))

	dbNodes, err := nm.loadActiveNodes()
	if err != nil {
		nm.cfg.Logger.Warn("Failed to load nodes for deploy", "error", err)
		return
	}

	nodesByName := make(map[string]string, len(dbNodes))
	for _, n := range dbNodes {
		nodesByName[n.Name] = n.UUID
	}

	targetFilter := make(map[string]struct{}, len(requestedNodeUUIDs))
	for _, nodeUUID := range requestedNodeUUIDs {
		trimmed := strings.TrimSpace(nodeUUID)
		if trimmed == "" {
			continue
		}
		targetFilter[trimmed] = struct{}{}
	}

	targets := make([]deployTarget, 0)
	nm.nodesLock.RLock()
	for nodeName, state := range nm.nodes {
		if state == nil {
			continue
		}
		state.mutex.RLock()
		client := state.client
		isConnected := state.isConnected
		state.mutex.RUnlock()
		if client == nil || !isConnected {
			continue
		}
		nodeUUID, ok := nodesByName[nodeName]
		if !ok {
			continue
		}
		if len(targetFilter) > 0 {
			if _, allowed := targetFilter[nodeUUID]; !allowed {
				continue
			}
		}
		targets = append(targets, deployTarget{
			name:   nodeName,
			uuid:   nodeUUID,
			client: client,
		})
	}
	nm.nodesLock.RUnlock()
	nm.cfg.Logger.Debug("Prepared deploy targets", "active_nodes", len(dbNodes), "connected_targets", len(targets), "requested_node_targets", len(targetFilter), "restart", restart, "force_restart", forceRestart)

	if len(targets) == 0 {
		nm.cfg.Logger.Warn("No connected nodes to deploy")
		return
	}

	// Node-independent data: identical for every target in this deploy cycle,
	// so it's loaded once instead of once per node (was a per-target N+1).
	sharedLists := nm.loadSharedLists(nm.globalCtx)
	snippets := nm.loadConfigSnippets(nm.globalCtx)
	profileCache := nm.newDeployProfileCache(snippets)

	batchStart := time.Now()
	var (
		wg              sync.WaitGroup
		profileMu       sync.Mutex
		lastProfileUUID string
	)
	sem := make(chan struct{}, nodeDeployConcurrency)

	for _, target := range targets {
		wg.Add(1)
		sem <- struct{}{}

		go func(target deployTarget) {
			defer wg.Done()
			defer func() { <-sem }()

			profileUUID := nm.deployNodeTarget(target, sharedLists, snippets, profileCache, restart, forceRestart)
			if profileUUID != "" {
				profileMu.Lock()
				lastProfileUUID = profileUUID
				profileMu.Unlock()
			}
		}(target)
	}

	wg.Wait()

	if lastProfileUUID != "" {
		batchDuration := time.Since(batchStart).Milliseconds()
		nm.cfg.Logger.RoleService(logger.RoleWorkers, "StartAllNodesByProfileQueueProcessor").Info(fmt.Sprintf("Started all nodes with profile %s in %dms", lastProfileUUID, batchDuration))
	}
}

func (nm *NodeMonitor) deployNodeTarget(
	target deployTarget,
	sharedLists resolvedSharedLists,
	snippets *resolvedConfigSnippets,
	profileCache *deployProfileCache,
	restart bool,
	forceRestart bool,
) string {
	start := time.Now()
	var (
		configJSON    json.RawMessage
		internals     *deployInternalsBlock
		profileUUID   string
		inboundsCount int
		err           error
	)
	if profileCache != nil {
		configJSON, internals, profileUUID, inboundsCount, err = profileCache.buildNodeConfigForDeploy(nm.globalCtx, target.uuid)
	} else {
		configJSON, internals, profileUUID, inboundsCount, err = nm.buildNodeConfigForDeploy(nm.globalCtx, target.uuid, snippets)
	}
	if err != nil {
		nm.cfg.Logger.Warn("Failed to build node deploy config", "node", target.name, "node_uuid", target.uuid, "error", err)
		return ""
	}

	nm.cfg.Logger.RoleService(logger.RoleWorkers, "StartAllNodesByProfileQueueProcessor").Info(fmt.Sprintf("Node %s has %d active inbounds.", target.uuid, inboundsCount))

	genDuration := time.Since(start).Milliseconds()
	nm.cfg.Logger.RoleService(logger.RoleWorkers, "StartAllNodesByProfileQueueProcessor").Info(fmt.Sprintf("Generated config for nodes by Profile in %dms", genDuration))

	pluginConfig, modulesErr := nm.loadNodePluginRuntimeConfig(nm.globalCtx, target.uuid)
	if modulesErr != nil {
		nm.cfg.Logger.Warn("Failed to load node plugin settings for deploy payload", "node", target.name, "node_uuid", target.uuid, "error", modulesErr)
	}
	haproxyInboundTags := normalizeHaproxyInboundTags(pluginConfig.HaproxyAuth.InboundTags)

	ingressIPs, ingressASNs := resolvePluginFilters(pluginConfig.IngressFilter.BlockedIPs, pluginConfig.IngressFilter.BlockedASNs, sharedLists)
	egressIPs, egressASNs := resolvePluginFilters(pluginConfig.EgressFilter.BlockedIPs, pluginConfig.EgressFilter.BlockedASNs, sharedLists)

	modules := &deployModulesTaskBlock{
		IngressFilter: deployIngressFilterBlock{
			Enabled:     pluginConfig.IngressFilter.Enabled,
			BlockedIPs:  ingressIPs,
			BlockedASNs: ingressASNs,
		},
		EgressFilter: deployEgressFilterBlock{
			Enabled:      pluginConfig.EgressFilter.Enabled,
			BlockedIPs:   egressIPs,
			BlockedPorts: normalizePortSlice(pluginConfig.EgressFilter.BlockedPorts),
			BlockedASNs:  egressASNs,
		},
	}
	if pluginConfig.PreStart.Enabled {
		modules.PreStart.Enabled = true
		if pluginConfig.PreStart.CleanupSockets.Enabled && len(pluginConfig.PreStart.CleanupSockets.Files) > 0 {
			modules.PreStart.CleanupSockets = &deployCleanupSocketsBlock{
				Enabled: true,
				Files:   normalizeStringSlice(pluginConfig.PreStart.CleanupSockets.Files),
			}
		}
	}
	if pluginConfig.HaproxyAuth.Enabled {
		haproxyUsers, haproxyEnabled, usersErr := nm.loadNodeHaproxyUsers(nm.globalCtx, target.uuid, haproxyInboundTags)
		if usersErr != nil {
			nm.cfg.Logger.Warn("Failed to load node users for HAPROXY payload", "node", target.name, "node_uuid", target.uuid, "error", usersErr)
		} else {
			modules.HaproxyEnabled = haproxyEnabled
			modules.HaproxyUsers = haproxyUsers
		}
	}

	restartFlag := restart
	forceRestartFlag := forceRestart
	taskPayload, err := json.Marshal(deployTaskPayload{
		Config:       configJSON,
		Restart:      &restartFlag,
		ForceRestart: &forceRestartFlag,
		Modules:      modules,
		Internals:    internals,
	})
	if err != nil {
		nm.cfg.Logger.Warn("Failed to serialize deploy payload", "node", target.name, "error", err)
		return profileUUID
	}

	if err := nm.submitDeployTask(target, taskPayload, restart, forceRestart); err != nil {
		return profileUUID
	}
	return profileUUID
}

func (nm *NodeMonitor) submitDeployTask(
	target deployTarget,
	taskPayload []byte,
	restart bool,
	forceRestart bool,
) error {
	ctxBase := nm.globalCtx
	if ctxBase == nil {
		ctxBase = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctxBase, 60*time.Second)
	defer cancel()

	if nm.cfg != nil && nm.cfg.Logger != nil {
		nm.cfg.Logger.Debug("Submitting deploy task", "node", target.name, "payload_bytes", len(taskPayload), "restart", restart, "force_restart", forceRestart)
	}

	resp, err := target.client.SubmitTask(ctx, &proto.NodeTask{
		TaskId:    fmt.Sprintf("deploy-%d", time.Now().UnixNano()),
		Operation: "deploy_config",
		Payload:   taskPayload,
	})

	if err != nil {
		if nm.cfg != nil && nm.cfg.Logger != nil {
			nm.cfg.Logger.Warn("Deploy task failed", "node", target.name, "error", err)
		}
		errStr := err.Error()
		if strings.Contains(errStr, "the client connection is closing") ||
			strings.Contains(errStr, "connection is closing") ||
			strings.Contains(errStr, "code = Canceled") {
			return err
		}
		friendlyErr := formatNodeConnectionError(err)
		if friendlyErr == errStr {
			friendlyErr = fmt.Sprintf("Deploy transport error: %v", err)
		}
		nm.updateConnectionStatus(target.name, false, false, friendlyErr)
		return err
	}
	if resp == nil || resp.Code != int32(codes.OK) {
		if resp == nil {
			if nm.cfg != nil && nm.cfg.Logger != nil {
				nm.cfg.Logger.Warn("Deploy task returned nil status", "node", target.name)
			}
			nm.updateConnectionStatus(target.name, false, false, "Deploy task returned nil status")
			return fmt.Errorf("deploy task returned nil status")
		}
		if nm.cfg != nil && nm.cfg.Logger != nil {
			nm.cfg.Logger.Warn("Deploy task rejected", "node", target.name, "code", resp.Code, "message", resp.Message)
		}
		nm.updateConnectionStatus(target.name, false, false, firstNonEmptyString(resp.Message, "Deploy task rejected"))
		return fmt.Errorf("deploy task rejected: %s", resp.Message)
	}

	if hasCoreReady, coreReady, coreMessage := parseDeployCoreState(resp.Message); hasCoreReady {
		if coreReady {
			nm.updateConnectionStatus(target.name, true, false, "")
		} else {
			nm.updateConnectionStatus(target.name, false, false, coreMessage)
		}
	} else {
		nm.updateConnectionStatus(target.name, false, true, "")
	}

	if nm.cfg != nil && nm.cfg.Logger != nil {
		nm.cfg.Logger.Debug("Node config deployed", "node", target.name, "restart", restart, "force_restart", forceRestart, "message", resp.Message)
	}
	return nil
}

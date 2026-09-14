package users

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"exodus/internal/proto"

	"google.golang.org/grpc/codes"
)

const (
	nodePluginExecutorOperation   = "node_plugin_executor"
	nodePluginExecutorConcurrency = 20
)

func (nm *NodeMonitor) ExecuteNodePluginCommand(ctx context.Context, command json.RawMessage, requestedNodeUUIDs []string) error {
	if nm == nil {
		return nil
	}
	if len(command) == 0 {
		return fmt.Errorf("node plugin executor command is empty")
	}
	if ctx == nil {
		ctx = nm.globalCtx
	}
	if ctx == nil {
		ctx = context.Background()
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
		isReady := state.isConnected && state.client != nil
		client := state.client
		nodeUUID := state.nodeUUID
		state.mutex.RUnlock()
		if !isReady || nodeUUID == "" {
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

	if len(targets) == 0 {
		return fmt.Errorf("no connected nodes matched plugin executor targets")
	}

	var (
		wg      sync.WaitGroup
		errsMu  sync.Mutex
		errList []error
	)
	sem := make(chan struct{}, nodePluginExecutorConcurrency)

	for _, target := range targets {
		wg.Add(1)
		sem <- struct{}{}

		go func(target deployTarget) {
			defer wg.Done()
			defer func() { <-sem }()

			taskCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			resp, submitErr := target.client.SubmitTask(taskCtx, &proto.NodeTask{
				TaskId:    fmt.Sprintf("node-plugin-%d", time.Now().UnixNano()),
				Operation: nodePluginExecutorOperation,
				Payload:   command,
			})
			cancel()

			if submitErr != nil {
				err := fmt.Errorf("node plugin executor failed on %s: %w", target.name, submitErr)
				nm.cfg.Logger.Warn("Node plugin executor failed", "node", target.name, "error", submitErr)
				errsMu.Lock()
				errList = append(errList, err)
				errsMu.Unlock()
				return
			}
			if resp == nil {
				err := fmt.Errorf("node plugin executor returned nil status on %s", target.name)
				nm.cfg.Logger.Warn("Node plugin executor returned nil status", "node", target.name)
				errsMu.Lock()
				errList = append(errList, err)
				errsMu.Unlock()
				return
			}
			if resp.Code != int32(codes.OK) {
				err := fmt.Errorf("node plugin executor rejected on %s: code=%d message=%s", target.name, resp.Code, resp.Message)
				nm.cfg.Logger.Warn("Node plugin executor rejected", "node", target.name, "code", resp.Code, "message", resp.Message)
				errsMu.Lock()
				errList = append(errList, err)
				errsMu.Unlock()
				return
			}

			nm.cfg.Logger.Info("Node plugin executor command applied", "node", target.name, "node_uuid", target.uuid, "message", resp.Message)
		}(target)
	}

	wg.Wait()

	if len(errList) > 0 {
		return errors.Join(errList...)
	}

	return nil
}

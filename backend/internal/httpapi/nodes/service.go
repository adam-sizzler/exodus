package nodes

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"exodus/internal/config"
	"exodus/internal/nodehotcache"
	monitor "exodus/internal/nodes"
	"exodus/internal/notifications"
	"exodus/internal/security"

	"github.com/google/uuid"
)

type NodeService struct {
	repo *NodeRepository
	cfg  *config.BackendConfig
}

func NewNodeService(repo *NodeRepository, cfg *config.BackendConfig) *NodeService {
	return &NodeService{
		repo: repo,
		cfg:  cfg,
	}
}

func (s *NodeService) EnableNode(ctx context.Context, nodeUUID string) error {
	node, err := s.repo.getNodeByUUID(ctx, nodeUUID)
	if err != nil {
		return err
	}

	inboundsMap, err := s.repo.getNodeInbounds(ctx, []string{nodeUUID})
	if err != nil {
		return err
	}

	err = s.repo.enableNodeRecord(ctx, nodeUUID, node, inboundsMap[nodeUUID])
	if err != nil {
		return err
	}

	monitor.RequestNodeSync()
	monitor.RequestNodeDeploy(true, nodeUUID)

	if updated, loadErr := s.repo.getNodeByUUID(ctx, nodeUUID); loadErr == nil {
		emitNodeNotification(ctx, s.cfg, notifications.EventNodeEnabled, updated, nil)
	}
	return nil
}

func (s *NodeService) DisableNode(ctx context.Context, nodeUUID string) error {
	node, err := s.repo.getNodeByUUID(ctx, nodeUUID)
	if err != nil {
		return err
	}

	inboundsMap, err := s.repo.getNodeInbounds(ctx, []string{nodeUUID})
	if err != nil {
		return err
	}

	err = s.repo.disableNodeRecord(ctx, nodeUUID, node, inboundsMap[nodeUUID])
	if err != nil {
		return err
	}

	monitor.RequestNodeSync()
	monitor.RequestNodeDeploy(true, nodeUUID)
	_ = nodehotcache.Default(s.cfg).DeleteTransient(ctx, nodeUUID)

	if updated, loadErr := s.repo.getNodeByUUID(ctx, nodeUUID); loadErr == nil {
		emitNodeNotification(ctx, s.cfg, notifications.EventNodeDisabled, updated, nil)
	}
	return nil
}

func (s *NodeService) RestartNode(ctx context.Context, nodeUUID string, forceRestart bool) error {
	node, err := s.repo.getNodeByUUID(ctx, nodeUUID)
	if err != nil {
		return err
	}
	if node.IsDisabled {
		return errors.New("node is disabled")
	}

	monitor.RequestNodeSync()
	monitor.RequestNodeDeployWithForce(true, forceRestart, nodeUUID)
	return nil
}

func (s *NodeService) RestartAllNodes(ctx context.Context, forceRestart bool) error {
	var enabledCount int
	err := s.repo.db.QueryRow(ctx, `SELECT COUNT(*) FROM nodes WHERE is_disabled = false`).Scan(&enabledCount)
	if err != nil {
		return err
	}
	if enabledCount == 0 {
		return errNoEnabledNodes
	}

	monitor.RequestNodeSync()
	monitor.RequestNodeDeployWithForce(true, forceRestart)
	return nil
}

func (s *NodeService) ResetNodeTraffic(ctx context.Context, nodeUUID string) error {
	node, err := s.repo.getNodeByUUID(ctx, nodeUUID)
	if err != nil {
		return err
	}

	return s.repo.resetNodeTraffic(ctx, nodeUUID, node)
}

func (s *NodeService) ReorderNodes(ctx context.Context, items []reorderNodeItem) error {
	return s.repo.reorderNodes(ctx, items)
}

func (s *NodeService) CreateNode(ctx context.Context, req createNodeRequest) (nodeRecord, error) {
	grpcAuthToken := ""
	if req.GRPCAuthToken != nil {
		grpcAuthToken = *req.GRPCAuthToken
	}
	resolvedToken, err := security.ResolveGRPCAuthToken(grpcAuthToken)
	if err != nil {
		return nodeRecord{}, err
	}

	nodeUUID := uuid.NewString()
	now := time.Now().UTC()
	err = s.repo.createNode(ctx, nodeUUID, req, resolvedToken, now)
	if err != nil {
		return nodeRecord{}, err
	}

	monitor.RequestNodeSync()
	monitor.RequestNodeDeploy(true, nodeUUID)

	node, err := s.repo.getNodeByUUID(ctx, nodeUUID)
	if err != nil {
		return nodeRecord{}, err
	}

	emitNodeNotification(ctx, s.cfg, notifications.EventNodeCreated, node, nil)
	return node, nil
}

func (s *NodeService) UpdateNode(ctx context.Context, req updateNodeRequest) (nodeRecord, error) {
	if req.Name != nil {
		trimmed := strings.TrimSpace(*req.Name)
		req.Name = &trimmed
	}
	if req.Address != nil {
		trimmed := strings.TrimSpace(*req.Address)
		req.Address = &trimmed
	}

	var grpcAuthToken *string
	if req.GRPCAuthToken != nil {
		token, err := security.ResolveGRPCAuthToken(*req.GRPCAuthToken)
		if err != nil {
			return nodeRecord{}, err
		}
		grpcAuthToken = &token
	}

	err := s.repo.updateNode(ctx, req, grpcAuthToken)
	if err != nil {
		return nodeRecord{}, err
	}

	monitor.RequestNodeSync()

	node, err := s.repo.getNodeByUUID(ctx, req.UUID)
	if err != nil {
		return nodeRecord{}, err
	}

	if !node.IsDisabled && (req.ConfigProfile != nil || req.ProxyURL.Set || req.Port != nil || req.Address != nil || req.ActivePluginUUID.Set || req.GRPCAuthToken != nil) {
		monitor.RequestNodeDeploy(true, req.UUID)
	}

	emitNodeNotification(ctx, s.cfg, notifications.EventNodeModified, node, nil)
	return node, nil
}

func (s *NodeService) DeleteNode(ctx context.Context, nodeUUID string) error {
	node, nodeErr := s.repo.getNodeByUUID(ctx, nodeUUID)
	if nodeErr != nil && !errors.Is(nodeErr, pgx.ErrNoRows) {
		return nodeErr
	}

	err := s.repo.deleteNode(ctx, nodeUUID)
	if err != nil {
		return err
	}

	_ = nodehotcache.Default(s.cfg).DeleteTransient(ctx, nodeUUID)

	monitor.RequestNodeSync()
	monitor.RequestNodeDeploy(true, nodeUUID)

	if nodeErr == nil {
		emitNodeNotification(ctx, s.cfg, notifications.EventNodeDeleted, node, nil)
	}
	return nil
}

func (s *NodeService) BulkProfileModification(ctx context.Context, req bulkProfileModificationRequest) error {
	err := s.repo.bulkProfileModification(ctx, req.UUIDs, req.ConfigProfile.ActiveConfigProfileUUID, req.ConfigProfile.ActiveInbounds)
	if err != nil {
		return err
	}

	monitor.RequestNodeSync()
	monitor.RequestNodeDeploy(true, req.UUIDs...)
	return nil
}

func (s *NodeService) BulkNodesUpdate(ctx context.Context, req bulkUpdateNodesRequest, clauses []string, args []any) error {
	err := s.repo.bulkUpdateNodes(ctx, req.UUIDs, clauses, args)
	if err != nil {
		return err
	}

	monitor.RequestNodeSync()
	if req.Fields.ActivePluginUUID.Set {
		monitor.RequestNodeDeploy(true, req.UUIDs...)
	}
	emitNodesByUUIDsNotification(ctx, s.repo, s.cfg, notifications.EventNodeModified, req.UUIDs)
	return nil
}

func (s *NodeService) BulkNodesActions(ctx context.Context, req bulkNodesActionsRequest) error {
	for _, nodeUUID := range req.UUIDs {
		switch req.Action {
		case "ENABLE":
			node, err := s.repo.getNodeByUUID(ctx, nodeUUID)
			if err != nil {
				return err
			}
			inboundsMap, err := s.repo.getNodeInbounds(ctx, []string{nodeUUID})
			if err != nil {
				return err
			}
			err = s.repo.enableNodeRecord(ctx, nodeUUID, node, inboundsMap[nodeUUID])
			if err != nil {
				return err
			}
		case "DISABLE":
			node, err := s.repo.getNodeByUUID(ctx, nodeUUID)
			if err != nil {
				return err
			}
			inboundsMap, err := s.repo.getNodeInbounds(ctx, []string{nodeUUID})
			if err != nil {
				return err
			}
			err = s.repo.disableNodeRecord(ctx, nodeUUID, node, inboundsMap[nodeUUID])
			if err != nil {
				return err
			}
		case "RESTART":
			if _, err := s.repo.getNodeByUUID(ctx, nodeUUID); err != nil {
				return err
			}
		case "RESET_TRAFFIC":
			node, err := s.repo.getNodeByUUID(ctx, nodeUUID)
			if err != nil {
				return err
			}
			err = s.repo.resetNodeTraffic(ctx, nodeUUID, node)
			if err != nil {
				return err
			}
		default:
			return errors.New("invalid bulk action")
		}
	}

	monitor.RequestNodeSync()
	monitor.RequestNodeDeploy(true, req.UUIDs...)

	switch req.Action {
	case "ENABLE":
		emitNodesByUUIDsNotification(ctx, s.repo, s.cfg, notifications.EventNodeEnabled, req.UUIDs)
	case "DISABLE":
		emitNodesByUUIDsNotification(ctx, s.repo, s.cfg, notifications.EventNodeDisabled, req.UUIDs)
		cache := nodehotcache.Default(s.cfg)
		for _, nodeUUID := range req.UUIDs {
			_ = cache.DeleteTransient(ctx, nodeUUID)
		}
	case "RESET_TRAFFIC":
		emitNodesByUUIDsNotification(ctx, s.repo, s.cfg, notifications.EventNodeModified, req.UUIDs)
	}

	return nil
}

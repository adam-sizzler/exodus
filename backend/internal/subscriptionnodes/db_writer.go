package subscriptionnodes

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (sm *SubNodeMonitor) loadActiveNodes() ([]dbSubNode, error) {
	ctx := sm.globalCtx
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := sm.db.Query(ctx, `
		SELECT n.uuid, n.name, n.address, n.port, n.api_schema, n.api_path, n.grpc_auth_token,
		       sns.subpage_config_uuid
		FROM sub_nodes n
		LEFT JOIN sub_nodes_to_subscription_page_config sns ON sns.node_uuid = n.uuid
		WHERE n.is_disabled = false
		ORDER BY n.view_position ASC, n.name ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query sub_nodes: %w", err)
	}
	defer rows.Close()

	nodes := make([]dbSubNode, 0)
	for rows.Next() {
		var (
			n             dbSubNode
			port          *int64
			grpcAuthToken *string
			subpageConfig *string
		)
		if err := rows.Scan(&n.UUID, &n.Name, &n.Address, &port, &n.APISchema, &n.APIPath, &grpcAuthToken, &subpageConfig); err != nil {
			return nil, fmt.Errorf("scan sub_node: %w", err)
		}
		if port != nil {
			n.Port = int(*port)
		} else {
			n.Port = 2222
		}
		n.APISchema = normalizeSubSchema(n.APISchema)
		if grpcAuthToken != nil {
			n.GRPCAuthToken = strings.TrimSpace(*grpcAuthToken)
		}
		if subpageConfig != nil {
			n.SubpageConfigUUID = normalizeAssignedSubpageConfigUUID(*subpageConfig)
		}
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

func (sm *SubNodeMonitor) updateConnectionStatus(nodeName string, isConnected, isConnecting bool, message string) {
	if sm == nil || sm.db == nil {
		return
	}
	var (
		currentConnected  bool
		currentConnecting bool
		currentMessage    *string
	)
	ctx := context.Background()
	if sm.globalCtx != nil {
		ctx = sm.globalCtx
	}
	err := sm.db.QueryRow(ctx, `SELECT is_connected, is_connecting, last_status_message FROM sub_nodes WHERE name = $1`, nodeName).
		Scan(&currentConnected, &currentConnecting, &currentMessage)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) && sm.cfg != nil && sm.cfg.Logger != nil {
			sm.cfg.Logger.Warn("Failed to query subscription node status", "node", nodeName, "error", err)
		}
		return
	}

	msgStr := ""
	if currentMessage != nil {
		msgStr = *currentMessage
	}
	if currentConnected == isConnected && currentConnecting == isConnecting && msgStr == message {
		return
	}

	_, err = sm.db.Exec(ctx, `
		UPDATE sub_nodes
		SET is_connected = $1,
		    is_connecting = $2,
		    last_status_message = $3,
		    last_status_change = CURRENT_TIMESTAMP,
		    updated_at = CURRENT_TIMESTAMP
		WHERE name = $4
	`, isConnected, isConnecting, message, nodeName)
	if err != nil {
		sm.cfg.Logger.Warn("Failed to update subscription node status", "node", nodeName, "error", err)
	}
}

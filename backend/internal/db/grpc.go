package db

import (
	"context"
	"fmt"

	"exodus/internal/config"
)

// DBNode represents a node loaded from database.
type DBNode struct {
	UUID                      string
	Name                      string
	Address                   string
	Port                      int
	ProxyURL                  string
	APISchema                 string
	APIPath                   string
	GRPCAuthToken             string
	IsDisabled                bool
	ConsumptionMultiplier     int64
	NodeConsumptionMultiplier int64
	IsTrafficTrackingActive   bool
	TrafficResetDay           int
	TrafficLimitBytes         int64
	NotifyPercent             int
	ViewPosition              int
	CountryCode               string
	Tags                      []string
}

const activeNodesQuery = `
	SELECT uuid, name, address,
	       COALESCE(port, 9253),
	       COALESCE(proxy_url, ''),
	       COALESCE(api_schema, 'grpc'),
	       COALESCE(api_path, ''),
	       COALESCE(grpc_auth_token, ''),
	       is_disabled,
	       COALESCE(consumption_multiplier, 1000000000),
	       COALESCE(node_consumption_multiplier, 1000000000),
	       COALESCE(is_traffic_tracking_active, true),
	       COALESCE(traffic_reset_day, 1),
	       COALESCE(traffic_limit_bytes, 0),
	       COALESCE(notify_percent, 80),
	       COALESCE(view_position, 0),
	       COALESCE(country_code, ''),
	       COALESCE(tags, '{}')
	FROM nodes
	WHERE is_disabled = false
	ORDER BY view_position ASC, name ASC`

// LoadNodesFromDB loads all active nodes from the database using the native pgx DBTX interface.
func LoadNodesFromDB(ctx context.Context, db DBTX, cfg *config.BackendConfig) ([]DBNode, error) {
	rows, err := db.Query(ctx, activeNodesQuery)
	if err != nil {
		return nil, fmt.Errorf("query nodes: %w", err)
	}
	defer rows.Close()

	nodes := make([]DBNode, 0, 32)
	for rows.Next() {
		var n DBNode
		err := rows.Scan(
			&n.UUID, &n.Name, &n.Address, &n.Port, &n.ProxyURL, &n.APISchema, &n.APIPath, &n.GRPCAuthToken,
			&n.IsDisabled, &n.ConsumptionMultiplier, &n.NodeConsumptionMultiplier, &n.IsTrafficTrackingActive,
			&n.TrafficResetDay, &n.TrafficLimitBytes, &n.NotifyPercent, &n.ViewPosition,
			&n.CountryCode, &n.Tags,
		)
		if err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		if n.Tags == nil {
			n.Tags = []string{}
		}
		nodes = append(nodes, n)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	if cfg != nil && cfg.Logger != nil {
		cfg.Logger.Debug("Loaded nodes from database via pgx", "count", len(nodes))
	}
	return nodes, nil
}

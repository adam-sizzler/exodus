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
	SELECT uuid, name, address, port, proxy_url, api_schema, api_path, grpc_auth_token,
	       is_disabled, consumption_multiplier, node_consumption_multiplier, is_traffic_tracking_active,
	       traffic_reset_day, traffic_limit_bytes, notify_percent,
	       view_position, country_code, tags
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
		var port *int
		var proxyURL, apiSchema, apiPath, grpcAuthToken, countryCode *string
		var tags []string
		var consumptionMultiplier, nodeConsumptionMultiplier, trafficLimitBytes, trafficResetDay, notifyPercent, viewPosition *int64
		var isTrafficTrackingActive *bool

		err := rows.Scan(
			&n.UUID, &n.Name, &n.Address, &port, &proxyURL, &apiSchema, &apiPath, &grpcAuthToken,
			&n.IsDisabled, &consumptionMultiplier, &nodeConsumptionMultiplier, &isTrafficTrackingActive,
			&trafficResetDay, &trafficLimitBytes, &notifyPercent, &viewPosition,
			&countryCode, &tags,
		)
		if err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}

		if port != nil {
			n.Port = *port
		} else {
			n.Port = 9253
		}
		if proxyURL != nil {
			n.ProxyURL = *proxyURL
		}
		if apiSchema != nil {
			n.APISchema = *apiSchema
		} else {
			n.APISchema = "grpc"
		}
		if apiPath != nil {
			n.APIPath = *apiPath
		}
		if grpcAuthToken != nil {
			n.GRPCAuthToken = *grpcAuthToken
		}
		if consumptionMultiplier != nil {
			n.ConsumptionMultiplier = *consumptionMultiplier
		} else {
			n.ConsumptionMultiplier = 1_000_000_000
		}
		if nodeConsumptionMultiplier != nil {
			n.NodeConsumptionMultiplier = *nodeConsumptionMultiplier
		} else {
			n.NodeConsumptionMultiplier = 1_000_000_000
		}
		if isTrafficTrackingActive != nil {
			n.IsTrafficTrackingActive = *isTrafficTrackingActive
		} else {
			n.IsTrafficTrackingActive = true
		}
		if trafficResetDay != nil {
			n.TrafficResetDay = int(*trafficResetDay)
		} else {
			n.TrafficResetDay = 1
		}
		if trafficLimitBytes != nil {
			n.TrafficLimitBytes = *trafficLimitBytes
		}
		if notifyPercent != nil {
			n.NotifyPercent = int(*notifyPercent)
		} else {
			n.NotifyPercent = 80
		}
		if viewPosition != nil {
			n.ViewPosition = int(*viewPosition)
		}
		if countryCode != nil {
			n.CountryCode = *countryCode
		}

		if tags != nil {
			n.Tags = tags
		} else {
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

package db

import (
	"context"
	"database/sql"
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
		var port sql.NullInt64
		var proxyURL, apiSchema, apiPath, grpcAuthToken, countryCode sql.NullString
		var tags StringArray
		var consumptionMultiplier, nodeConsumptionMultiplier, trafficLimitBytes, trafficResetDay, notifyPercent, viewPosition sql.NullInt64
		var isTrafficTrackingActive sql.NullBool

		err := rows.Scan(
			&n.UUID, &n.Name, &n.Address, &port, &proxyURL, &apiSchema, &apiPath, &grpcAuthToken,
			&n.IsDisabled, &consumptionMultiplier, &nodeConsumptionMultiplier, &isTrafficTrackingActive,
			&trafficResetDay, &trafficLimitBytes, &notifyPercent, &viewPosition,
			&countryCode, &tags,
		)
		if err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}

		if port.Valid {
			n.Port = int(port.Int64)
		} else {
			n.Port = 9253
		}
		if proxyURL.Valid {
			n.ProxyURL = proxyURL.String
		}
		if apiSchema.Valid {
			n.APISchema = apiSchema.String
		} else {
			n.APISchema = "grpc"
		}
		if apiPath.Valid {
			n.APIPath = apiPath.String
		} else {
			n.APIPath = ""
		}
		if grpcAuthToken.Valid {
			n.GRPCAuthToken = grpcAuthToken.String
		}
		if consumptionMultiplier.Valid {
			n.ConsumptionMultiplier = consumptionMultiplier.Int64
		} else {
			n.ConsumptionMultiplier = 1_000_000_000
		}
		if nodeConsumptionMultiplier.Valid {
			n.NodeConsumptionMultiplier = nodeConsumptionMultiplier.Int64
		} else {
			n.NodeConsumptionMultiplier = 1_000_000_000
		}
		if isTrafficTrackingActive.Valid {
			n.IsTrafficTrackingActive = isTrafficTrackingActive.Bool
		} else {
			n.IsTrafficTrackingActive = true
		}
		if trafficResetDay.Valid {
			n.TrafficResetDay = int(trafficResetDay.Int64)
		} else {
			n.TrafficResetDay = 1
		}
		if trafficLimitBytes.Valid {
			n.TrafficLimitBytes = trafficLimitBytes.Int64
		}
		if notifyPercent.Valid {
			n.NotifyPercent = int(notifyPercent.Int64)
		} else {
			n.NotifyPercent = 80
		}
		if viewPosition.Valid {
			n.ViewPosition = int(viewPosition.Int64)
		}
		if countryCode.Valid {
			n.CountryCode = countryCode.String
		}

		if len(tags) > 0 {
			n.Tags = tags.Slice()
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

// LoadNodesFromSqlDB is a temporary bridge for legacy *sql.DB callers until Stage 4.
func LoadNodesFromSqlDB(ctx context.Context, db *sql.DB, cfg *config.BackendConfig) ([]DBNode, error) {
	rows, err := db.QueryContext(ctx, activeNodesQuery)
	if err != nil {
		return nil, fmt.Errorf("query nodes: %w", err)
	}
	defer rows.Close()

	nodes := make([]DBNode, 0, 32)
	for rows.Next() {
		var n DBNode
		var port sql.NullInt64
		var proxyURL, apiSchema, apiPath, grpcAuthToken, countryCode sql.NullString
		var tags StringArray
		var consumptionMultiplier, nodeConsumptionMultiplier, trafficLimitBytes, trafficResetDay, notifyPercent, viewPosition sql.NullInt64
		var isTrafficTrackingActive sql.NullBool

		err := rows.Scan(
			&n.UUID, &n.Name, &n.Address, &port, &proxyURL, &apiSchema, &apiPath, &grpcAuthToken,
			&n.IsDisabled, &consumptionMultiplier, &nodeConsumptionMultiplier, &isTrafficTrackingActive,
			&trafficResetDay, &trafficLimitBytes, &notifyPercent, &viewPosition,
			&countryCode, &tags,
		)
		if err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}

		if port.Valid {
			n.Port = int(port.Int64)
		} else {
			n.Port = 9253
		}
		if proxyURL.Valid {
			n.ProxyURL = proxyURL.String
		}
		if apiSchema.Valid {
			n.APISchema = apiSchema.String
		} else {
			n.APISchema = "grpc"
		}
		if apiPath.Valid {
			n.APIPath = apiPath.String
		} else {
			n.APIPath = ""
		}
		if grpcAuthToken.Valid {
			n.GRPCAuthToken = grpcAuthToken.String
		}
		if consumptionMultiplier.Valid {
			n.ConsumptionMultiplier = consumptionMultiplier.Int64
		} else {
			n.ConsumptionMultiplier = 1_000_000_000
		}
		if nodeConsumptionMultiplier.Valid {
			n.NodeConsumptionMultiplier = nodeConsumptionMultiplier.Int64
		} else {
			n.NodeConsumptionMultiplier = 1_000_000_000
		}
		if isTrafficTrackingActive.Valid {
			n.IsTrafficTrackingActive = isTrafficTrackingActive.Bool
		} else {
			n.IsTrafficTrackingActive = true
		}
		if trafficResetDay.Valid {
			n.TrafficResetDay = int(trafficResetDay.Int64)
		} else {
			n.TrafficResetDay = 1
		}
		if trafficLimitBytes.Valid {
			n.TrafficLimitBytes = trafficLimitBytes.Int64
		}
		if notifyPercent.Valid {
			n.NotifyPercent = int(notifyPercent.Int64)
		} else {
			n.NotifyPercent = 80
		}
		if viewPosition.Valid {
			n.ViewPosition = int(viewPosition.Int64)
		}
		if countryCode.Valid {
			n.CountryCode = countryCode.String
		}

		if len(tags) > 0 {
			n.Tags = tags.Slice()
		} else {
			n.Tags = []string{}
		}

		nodes = append(nodes, n)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	if cfg != nil && cfg.Logger != nil {
		cfg.Logger.Debug("Loaded nodes from database", "count", len(nodes))
	}
	return nodes, nil
}

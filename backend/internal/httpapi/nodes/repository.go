package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	exodusdb "exodus/internal/db"
	"exodus/internal/httpapi/shared"
	"exodus/internal/util"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type NodeRepository struct {
	db *pgxpool.Pool
}

func NewNodeRepository(db *pgxpool.Pool) *NodeRepository {
	return &NodeRepository{db: db}
}

func (r *NodeRepository) getAllNodeRecords(ctx context.Context) ([]nodeRecord, error) {
	rows, err := r.db.Query(ctx, `
		SELECT
			uuid, id, name, address, port, proxy_url, api_schema, api_path, grpc_auth_token, active_config_profile_uuid, active_plugin_uuid,
			is_connected, is_connecting, is_disabled, last_status_change, last_status_message,
			consumption_multiplier, node_consumption_multiplier,
			is_traffic_tracking_active, traffic_reset_day, traffic_limit_bytes, traffic_used_bytes,
			notify_percent, provider_uuid, view_position, country_code, tags, ips, note,
			created_at, updated_at
		FROM nodes
		ORDER BY view_position ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []nodeRecord
	for rows.Next() {
		node, scanErr := scanNodeRecord(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (r *NodeRepository) getNodeByUUID(ctx context.Context, nodeUUID string) (nodeRecord, error) {
	row := r.db.QueryRow(ctx, `
		SELECT
			uuid, id, name, address, port, proxy_url, api_schema, api_path, grpc_auth_token, active_config_profile_uuid, active_plugin_uuid,
			is_connected, is_connecting, is_disabled, last_status_change, last_status_message,
			consumption_multiplier, node_consumption_multiplier,
			is_traffic_tracking_active, traffic_reset_day, traffic_limit_bytes, traffic_used_bytes,
			notify_percent, provider_uuid, view_position, country_code, tags, ips, note,
			created_at, updated_at
		FROM nodes
		WHERE uuid = $1
	`, nodeUUID)
	return scanNodeRecord(row)
}

func scanNodeRecord(scanner shared.RowScanner) (nodeRecord, error) {
	var node nodeRecord
	var id *int64
	var port *int
	var proxyURL *string
	var activeConfigProfileUUID *string
	var activePluginUUID *string
	var lastStatusChange *time.Time
	var lastStatusMessage *string
	var trafficResetDay *int
	var trafficLimitBytes *int64
	var trafficUsedBytes *int64
	var notifyPercent *int
	var providerUUID *string
	var tags []string
	var ipsRaw []byte
	var note *string

	err := scanner.Scan(
		&node.UUID,
		&id,
		&node.Name,
		&node.Address,
		&port,
		&proxyURL,
		&node.APISchema,
		&node.APIPath,
		&node.GRPCAuthToken,
		&activeConfigProfileUUID,
		&activePluginUUID,
		&node.IsConnected,
		&node.IsConnecting,
		&node.IsDisabled,
		&lastStatusChange,
		&lastStatusMessage,
		&node.ConsumptionMultiplier,
		&node.NodeConsumptionMultiplier,
		&node.IsTrafficTrackingActive,
		&trafficResetDay,
		&trafficLimitBytes,
		&trafficUsedBytes,
		&notifyPercent,
		&providerUUID,
		&node.ViewPosition,
		&node.CountryCode,
		&tags,
		&ipsRaw,
		&note,
		&node.CreatedAt,
		&node.UpdatedAt,
	)
	if err != nil {
		return node, err
	}

	if len(ipsRaw) > 0 {
		_ = json.Unmarshal(ipsRaw, &node.IPs)
	}
	if node.IPs == nil {
		node.IPs = []NodeIPItem{}
	}

	node.ID = id
	node.Port = port
	node.ProxyURL = proxyURL
	node.ActiveConfigProfileUUID = activeConfigProfileUUID
	node.ActivePluginUUID = activePluginUUID
	node.LastStatusChange = lastStatusChange
	node.LastStatusMessage = lastStatusMessage
	node.TrafficResetDay = trafficResetDay
	node.TrafficLimitBytes = trafficLimitBytes
	node.TrafficUsedBytes = trafficUsedBytes
	node.NotifyPercent = notifyPercent
	node.ProviderUUID = providerUUID
	if tags != nil {
		node.Tags = tags
	} else {
		node.Tags = []string{}
	}
	node.Note = note

	return node, nil
}

func (r *NodeRepository) getNodeInbounds(ctx context.Context, nodeUUIDs []string) (map[string][]configProfileInboundResponse, error) {
	result := make(map[string][]configProfileInboundResponse)
	if len(nodeUUIDs) == 0 {
		return result, nil
	}
	rows, err := r.db.Query(ctx, `
		SELECT
			cpitn.node_uuid,
			cpi.uuid, cpi.profile_uuid, cpi.tag, cpi.type, cpi.network, cpi.security, cpi.port, cpi.raw_inbound
		FROM config_profile_inbounds_to_nodes cpitn
		JOIN config_profile_inbounds cpi ON cpi.uuid = cpitn.config_profile_inbound_uuid
		WHERE cpitn.node_uuid = ANY($1)
		ORDER BY cpi.tag ASC
	`, nodeUUIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var nodeUUID string
		var inbound configProfileInboundResponse
		var network *string
		var security *string
		var port *int
		var rawInbound []byte
		if err := rows.Scan(
			&nodeUUID,
			&inbound.UUID,
			&inbound.ProfileUUID,
			&inbound.Tag,
			&inbound.Type,
			&network,
			&security,
			&port,
			&rawInbound,
		); err != nil {
			return nil, err
		}
		inbound.Network = network
		inbound.Security = security
		inbound.Port = port
		inbound.RawInbound = json.RawMessage(rawInbound)
		result[nodeUUID] = append(result[nodeUUID], inbound)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *NodeRepository) getProviders(ctx context.Context, providerUUIDs []string) (map[string]*providerResponse, error) {
	result := make(map[string]*providerResponse)
	if len(providerUUIDs) == 0 {
		return result, nil
	}
	rows, err := r.db.Query(ctx, `
		SELECT uuid, name, favicon_link, login_url, created_at, updated_at
		FROM infra_providers
		WHERE uuid = ANY($1)
	`, providerUUIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var item providerResponse
		var favicon *string
		var loginURL *string
		var createdAt time.Time
		var updatedAt time.Time
		if err := rows.Scan(&item.UUID, &item.Name, &favicon, &loginURL, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		item.FaviconLink = favicon
		item.LoginURL = loginURL
		item.CreatedAt = &createdAt
		item.UpdatedAt = &updatedAt
		result[item.UUID] = &item
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *NodeRepository) replaceNodeInboundsTx(ctx context.Context, tx pgx.Tx, nodeUUID string, inboundUUIDs []string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM config_profile_inbounds_to_nodes WHERE node_uuid = $1`, nodeUUID); err != nil {
		return err
	}
	deduped := dedupeStrings(inboundUUIDs)
	if len(deduped) > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO config_profile_inbounds_to_nodes (config_profile_inbound_uuid, node_uuid)
			SELECT unnest($1::uuid[]), $2::uuid
		`, deduped, nodeUUID); err != nil {
			return err
		}
	}
	return nil
}

func (r *NodeRepository) getNodeTags(ctx context.Context) ([]string, error) {
	rows, err := r.db.Query(ctx, `SELECT DISTINCT unnest(tags) AS tag FROM nodes ORDER BY tag ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tags := make([]string, 0)
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, err
		}
		if tag != "" {
			tags = append(tags, tag)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(tags)
	return dedupeStrings(tags), nil
}

func (r *NodeRepository) createNode(ctx context.Context, nodeUUID string, req createNodeRequest, grpcAuthToken string, now time.Time) error {
	ipsJSON, _ := json.Marshal(normalizeNodeIPs(req.IPs))
	return exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO nodes (
				uuid, name, address, port, proxy_url, api_schema, api_path, grpc_auth_token, active_config_profile_uuid, active_plugin_uuid,
				is_connected, is_connecting, is_disabled, last_status_change, last_status_message,
				consumption_multiplier, node_consumption_multiplier,
				is_traffic_tracking_active, traffic_reset_day, traffic_limit_bytes, traffic_used_bytes,
				notify_percent, provider_uuid, country_code, tags, ips, note, created_at, updated_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
				$11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
				$21, $22, $23, $24, $25, $26, $27, $28, $29
			)
		`,
			nodeUUID,
			strings.TrimSpace(req.Name),
			strings.TrimSpace(req.Address),
			req.Port,
			normalizeNullableString(req.ProxyURL),
			normalizeAPISchema(req.APISchema),
			normalizeAPIPath(req.APIPath),
			grpcAuthToken,
			req.ConfigProfile.ActiveConfigProfileUUID,
			normalizeNullableString(req.ActivePluginUUID),
			false,
			false,
			false,
			nil,
			nil,
			toNanoMultiplier(util.Coalesce(req.ConsumptionMultiplier, 1)),
			toNanoMultiplier(util.Coalesce(req.NodeConsumptionMultiplier, 1)),
			util.Coalesce(req.IsTrafficTrackingActive, false),
			util.Coalesce(req.TrafficResetDay, 1),
			util.Coalesce(req.TrafficLimitBytes, int64(0)),
			0,
			util.Coalesce(req.NotifyPercent, 0),
			normalizeNullableString(req.ProviderUUID),
			normalizeCountryCode(req.CountryCode),
			normalizeTags(req.Tags),
			string(ipsJSON),
			normalizeNullableString(req.Note),
			now,
			now,
		)
		if err != nil {
			if util.IsUniqueViolation(err, "nodes_name_key") {
				return fmt.Errorf("node with this name already exists")
			}
			if util.IsUniqueViolation(err, "nodes_address_key") {
				return fmt.Errorf("node with this address already exists")
			}
			return err
		}

		return r.replaceNodeInboundsTx(ctx, tx, nodeUUID, req.ConfigProfile.ActiveInbounds)
	})
}

func (r *NodeRepository) updateNode(ctx context.Context, req updateNodeRequest, grpcAuthToken *string) error {
	clauses := make([]string, 0)
	args := make([]any, 0)
	idx := 1
	add := func(column string, value any) {
		clauses = append(clauses, fmt.Sprintf("%s = $%d", column, idx))
		args = append(args, value)
		idx++
	}

	if req.Name != nil {
		add("name", strings.TrimSpace(*req.Name))
	}
	if req.Address != nil {
		add("address", strings.TrimSpace(*req.Address))
	}
	if req.Port != nil {
		add("port", *req.Port)
	}
	if req.ProxyURL.Set {
		if req.ProxyURL.Value == nil || strings.TrimSpace(*req.ProxyURL.Value) == "" {
			clauses = append(clauses, "proxy_url = NULL")
		} else {
			add("proxy_url", strings.TrimSpace(*req.ProxyURL.Value))
		}
	}
	if req.APISchema != nil {
		add("api_schema", normalizeAPISchema(req.APISchema))
	}
	if req.APIPath != nil {
		add("api_path", normalizeAPIPath(req.APIPath))
	}
	if grpcAuthToken != nil {
		add("grpc_auth_token", *grpcAuthToken)
	}
	if req.IsTrafficTrackingActive != nil {
		add("is_traffic_tracking_active", *req.IsTrafficTrackingActive)
	}
	if req.TrafficLimitBytes != nil {
		add("traffic_limit_bytes", *req.TrafficLimitBytes)
	}
	if req.NotifyPercent != nil {
		add("notify_percent", *req.NotifyPercent)
	}
	if req.TrafficResetDay != nil {
		add("traffic_reset_day", *req.TrafficResetDay)
	}
	if req.CountryCode != nil {
		add("country_code", strings.ToUpper(strings.TrimSpace(*req.CountryCode)))
	}
	if req.ConsumptionMultiplier != nil {
		add("consumption_multiplier", toNanoMultiplier(*req.ConsumptionMultiplier))
	}
	if req.NodeConsumptionMultiplier != nil {
		add("node_consumption_multiplier", toNanoMultiplier(*req.NodeConsumptionMultiplier))
	}
	if req.Tags != nil {
		add("tags", normalizeTags(*req.Tags))
	}
	if req.IPs != nil {
		ipsJSON, _ := json.Marshal(normalizeNodeIPs(*req.IPs))
		add("ips", string(ipsJSON))
	}
	if req.Note.Set {
		if req.Note.Value == nil || strings.TrimSpace(*req.Note.Value) == "" {
			clauses = append(clauses, "note = NULL")
		} else {
			add("note", strings.TrimSpace(*req.Note.Value))
		}
	}
	if req.ProviderUUID.Set {
		if req.ProviderUUID.Value == nil || strings.TrimSpace(*req.ProviderUUID.Value) == "" {
			clauses = append(clauses, "provider_uuid = NULL")
		} else {
			add("provider_uuid", strings.TrimSpace(*req.ProviderUUID.Value))
		}
	}
	if req.ActivePluginUUID.Set {
		if req.ActivePluginUUID.Value == nil || strings.TrimSpace(*req.ActivePluginUUID.Value) == "" {
			clauses = append(clauses, "active_plugin_uuid = NULL")
		} else {
			add("active_plugin_uuid", strings.TrimSpace(*req.ActivePluginUUID.Value))
		}
	}
	if req.ConfigProfile != nil {
		add("active_config_profile_uuid", req.ConfigProfile.ActiveConfigProfileUUID)
	}

	return exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		if len(clauses) > 0 {
			updateArgs := append(args, req.UUID)
			query := fmt.Sprintf("UPDATE nodes SET %s, updated_at = CURRENT_TIMESTAMP WHERE uuid = $%d", strings.Join(clauses, ", "), idx)
			tag, err := tx.Exec(ctx, query, updateArgs...)
			if err != nil {
				if util.IsUniqueViolation(err, "nodes_name_key") {
					return fmt.Errorf("node with this name already exists")
				}
				if util.IsUniqueViolation(err, "nodes_address_key") {
					return fmt.Errorf("node with this address already exists")
				}
				return err
			}
			if tag.RowsAffected() == 0 {
				return pgx.ErrNoRows
			}
		}

		if req.ConfigProfile != nil {
			if err := r.replaceNodeInboundsTx(ctx, tx, req.UUID, req.ConfigProfile.ActiveInbounds); err != nil {
				return err
			}
		}

		return nil
	})
}

func (r *NodeRepository) deleteNode(ctx context.Context, nodeUUID string) error {
	tag, err := r.db.Exec(ctx, `DELETE FROM nodes WHERE uuid = $1`, nodeUUID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *NodeRepository) resetNodeTraffic(ctx context.Context, nodeUUID string, node nodeRecord) error {
	return exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO nodes_traffic_usage_history (node_uuid, traffic_bytes, reset_at)
			VALUES ($1, $2, $3)
		`, nodeUUID, util.Coalesce(node.TrafficUsedBytes, int64(0)), time.Now().UTC())
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE nodes SET traffic_used_bytes = 0, updated_at = CURRENT_TIMESTAMP WHERE uuid = $1`, nodeUUID)
		return err
	})
}

func (r *NodeRepository) reorderNodes(ctx context.Context, items []reorderNodeItem) error {
	if len(items) == 0 {
		return nil
	}

	uuids := make([]string, len(items))
	positions := make([]int32, len(items))
	for i, item := range items {
		uuids[i] = item.UUID
		positions[i] = int32(item.ViewPosition)
	}

	return exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE nodes AS n
			SET view_position = v.view_position
			FROM (
				SELECT unnest($1::uuid[]) AS uuid, unnest($2::int[]) AS view_position
			) AS v
			WHERE n.uuid = v.uuid
		`, uuids, positions); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT setval('nodes_view_position_seq', (SELECT COALESCE(MAX(view_position), 0) FROM nodes) + 1)`); err != nil {
			return err
		}
		return nil
	})
}

func (r *NodeRepository) enableNodeRecord(ctx context.Context, nodeUUID string, node nodeRecord, inbounds []configProfileInboundResponse) error {
	if node.ActiveConfigProfileUUID == nil || len(inbounds) == 0 {
		_, execErr := r.db.Exec(ctx, `
			UPDATE nodes
			SET is_disabled = true, active_config_profile_uuid = NULL, is_connecting = false,
				is_connected = false, last_status_message = NULL, last_status_change = $1
			WHERE uuid = $2
		`, time.Now().UTC(), nodeUUID)
		return execErr
	}
	_, execErr := r.db.Exec(ctx, `UPDATE nodes SET is_disabled = false, updated_at = CURRENT_TIMESTAMP WHERE uuid = $1`, nodeUUID)
	return execErr
}

func (r *NodeRepository) disableNodeRecord(ctx context.Context, nodeUUID string, node nodeRecord, inbounds []configProfileInboundResponse) error {
	if node.ActiveConfigProfileUUID == nil || len(inbounds) == 0 {
		if _, execErr := r.db.Exec(ctx, `UPDATE nodes SET active_config_profile_uuid = NULL WHERE uuid = $1`, nodeUUID); execErr != nil {
			return execErr
		}
	}
	_, execErr := r.db.Exec(ctx, `
		UPDATE nodes
		SET is_disabled = true, is_connecting = false, is_connected = false,
			last_status_message = NULL, last_status_change = $1,
			updated_at = CURRENT_TIMESTAMP
		WHERE uuid = $2
	`, time.Now().UTC(), nodeUUID)
	return execErr
}

func (r *NodeRepository) bulkProfileModification(ctx context.Context, uuids []string, activeConfigProfileUUID string, activeInbounds []string) error {
	return exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		if len(uuids) == 0 {
			return nil
		}
		if _, err := tx.Exec(ctx, `UPDATE nodes SET active_config_profile_uuid = $1, updated_at = CURRENT_TIMESTAMP WHERE uuid = ANY($2)`, activeConfigProfileUUID, uuids); err != nil {
			return err
		}
		for _, nodeUUID := range uuids {
			if err := r.replaceNodeInboundsTx(ctx, tx, nodeUUID, activeInbounds); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *NodeRepository) bulkUpdateNodes(ctx context.Context, uuids []string, clauses []string, args []any) error {
	if len(clauses) == 0 {
		return nil
	}
	args = append(args, uuids)
	query := fmt.Sprintf("UPDATE nodes SET %s, updated_at = CURRENT_TIMESTAMP WHERE uuid = ANY($%d)", strings.Join(clauses, ", "), len(args))
	_, execErr := r.db.Exec(ctx, query, args...)
	return execErr
}

func (r *NodeRepository) ensureConfigProfileInbounds(ctx context.Context, profileUUID string, inboundUUIDs []string) error {
	var exists int
	if err := r.db.QueryRow(ctx, `SELECT 1 FROM config_profiles WHERE uuid = $1`, profileUUID).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errConfigProfileNotFound
		}
		return err
	}

	found := make(map[string]struct{}, len(inboundUUIDs))
	rows, err := r.db.Query(ctx, `
		SELECT uuid
		FROM config_profile_inbounds
		WHERE profile_uuid = $1 AND uuid = ANY($2)
	`, profileUUID, inboundUUIDs)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var inboundUUID string
		if err := rows.Scan(&inboundUUID); err != nil {
			return err
		}
		found[inboundUUID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, inboundUUID := range inboundUUIDs {
		if _, ok := found[inboundUUID]; !ok {
			return errConfigProfileInboundInvalid
		}
	}
	return nil
}

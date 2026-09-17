package subscriptionconnections

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	exodusdb "exodus/internal/db"
	"exodus/internal/httpapi/shared"
)

type SubscriptionConnectionRepository struct {
	db *pgxpool.Pool
}

func NewSubscriptionConnectionRepository(db *pgxpool.Pool) *SubscriptionConnectionRepository {
	return &SubscriptionConnectionRepository{db: db}
}

func (r *SubscriptionConnectionRepository) getAllNodeRecords(ctx context.Context) ([]nodeRecord, error) {
	rows, err := r.db.Query(ctx, `
		SELECT
			n.uuid, n.id, n.name, n.address, n.public_domain, n.port, n.api_schema, n.api_path, n.grpc_auth_token, sns.subpage_config_uuid,
			n.is_connected, n.is_connecting, n.is_disabled,
			n.last_status_change, n.last_status_message,
			n.provider_uuid, n.view_position, n.tags, n.created_at, n.updated_at
		FROM sub_nodes n
		LEFT JOIN sub_nodes_to_subscription_page_config sns ON sns.node_uuid = n.uuid
		ORDER BY n.view_position ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []nodeRecord
	for rows.Next() {
		node, scanErr := r.scanNodeRecord(rows)
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

func (r *SubscriptionConnectionRepository) getNodeByUUID(ctx context.Context, nodeUUID string) (nodeRecord, error) {
	row := r.db.QueryRow(ctx, `
		SELECT
			n.uuid, n.id, n.name, n.address, n.public_domain, n.port, n.api_schema, n.api_path, n.grpc_auth_token, sns.subpage_config_uuid,
			n.is_connected, n.is_connecting, n.is_disabled,
			n.last_status_change, n.last_status_message,
			n.provider_uuid, n.view_position, n.tags, n.created_at, n.updated_at
		FROM sub_nodes n
		LEFT JOIN sub_nodes_to_subscription_page_config sns ON sns.node_uuid = n.uuid
		WHERE n.uuid = $1
	`, nodeUUID)
	return r.scanNodeRecord(row)
}

func (r *SubscriptionConnectionRepository) scanNodeRecord(scanner shared.RowScanner) (nodeRecord, error) {
	var node nodeRecord
	var id *int64
	var port *int
	var publicDomain *string
	var lastStatusChange *time.Time
	var lastStatusMessage *string
	var providerUUID *string
	var subpageConfigUUID *string
	var tags []string

	err := scanner.Scan(
		&node.UUID,
		&id,
		&node.Name,
		&node.Address,
		&publicDomain,
		&port,
		&node.APISchema,
		&node.APIPath,
		&node.GRPCAuthToken,
		&subpageConfigUUID,
		&node.IsConnected,
		&node.IsConnecting,
		&node.IsDisabled,
		&lastStatusChange,
		&lastStatusMessage,
		&providerUUID,
		&node.ViewPosition,
		&tags,
		&node.CreatedAt,
		&node.UpdatedAt,
	)
	if err != nil {
		return node, err
	}

	node.ID = id
	node.Port = port
	if publicDomain != nil {
		val := strings.TrimSpace(*publicDomain)
		if val != "" {
			node.PublicDomain = &val
		}
	}
	node.LastStatusChange = lastStatusChange
	node.LastStatusMessage = lastStatusMessage
	node.ProviderUUID = providerUUID
	if subpageConfigUUID != nil {
		val := strings.TrimSpace(*subpageConfigUUID)
		if val != "" {
			node.SubpageConfigUUID = &val
		}
	}
	node.APISchema = normalizeSubNodeSchema(node.APISchema)
	node.SingboxVersion = nil
	node.NodeVersion = nil
	node.SingboxUptime = "0"
	node.CPUCount = nil
	node.CPUModel = nil
	node.TotalRAM = nil
	node.Tags = tags
	if node.Tags == nil {
		node.Tags = []string{}
	}
	node.CountryCode = "XX"
	node.ConsumptionMultiplier = 1_000_000_000
	node.IsTrafficTrackingActive = false

	return node, nil
}

func (r *SubscriptionConnectionRepository) getProviders(ctx context.Context, providerUUIDs []string) (map[string]*providerResponse, error) {
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

func (r *SubscriptionConnectionRepository) getNodeTags(ctx context.Context) ([]string, error) {
	rows, err := r.db.Query(ctx, `SELECT DISTINCT unnest(tags) AS tag FROM sub_nodes ORDER BY tag ASC`)
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

func (r *SubscriptionConnectionRepository) subpageConfigExists(ctx context.Context, configUUID string) (bool, error) {
	var count int
	if err := r.db.QueryRow(ctx, `SELECT COUNT(*) FROM subscription_page_config WHERE uuid = $1`, configUUID).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *SubscriptionConnectionRepository) fetchSubpageConfigRaw(ctx context.Context, configUUID string) ([]byte, error) {
	var payload string
	err := r.db.QueryRow(ctx, `
		SELECT config
		FROM subscription_page_config
		WHERE uuid = $1
		LIMIT 1
	`, configUUID).Scan(&payload)
	if err != nil {
		return nil, err
	}

	raw := []byte(strings.TrimSpace(payload))
	if len(raw) == 0 || !json.Valid(raw) {
		return nil, fmt.Errorf("invalid subpage config payload")
	}

	return raw, nil
}

func (r *SubscriptionConnectionRepository) countEnabledNodes(ctx context.Context) (int, error) {
	var enabledCount int
	err := r.db.QueryRow(ctx, `SELECT COUNT(*) FROM sub_nodes WHERE is_disabled = false`).Scan(&enabledCount)
	return enabledCount, err
}

func (r *SubscriptionConnectionRepository) createNode(ctx context.Context, nodeUUID string, req createNodeRequest, schema, grpcAuthToken string, subpageConfigUUID *string, now time.Time) error {
	return exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO sub_nodes (
				uuid, name, address, public_domain, port, api_schema, api_path, grpc_auth_token,
				is_connected, is_connecting, is_disabled, provider_uuid, tags, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		`,
			nodeUUID,
			strings.TrimSpace(req.Name),
			strings.TrimSpace(req.Address),
			normalizePublicDomain(req.PublicDomain),
			req.Port,
			schema,
			normalizeAPIPath(req.APIPath),
			grpcAuthToken,
			false,
			false,
			false,
			normalizeNullableString(req.ProviderUUID),
			normalizeTags(req.Tags),
			now,
			now,
		)
		if err != nil {
			return err
		}
		if subpageConfigUUID != nil {
			if _, err := tx.Exec(ctx, `
				INSERT INTO sub_nodes_to_subscription_page_config (node_uuid, subpage_config_uuid)
				VALUES ($1, $2)
				ON CONFLICT (node_uuid) DO UPDATE
				SET subpage_config_uuid = EXCLUDED.subpage_config_uuid
			`, nodeUUID, *subpageConfigUUID); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *SubscriptionConnectionRepository) updateNode(ctx context.Context, nodeUUID string, clauses []string, args []any, subpageConfigSet bool, finalSubpageConfigUUID *string) error {
	return exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		if len(clauses) > 0 {
			updateArgs := append([]any{}, args...)
			updateArgs = append(updateArgs, nodeUUID)
			query := fmt.Sprintf("UPDATE sub_nodes SET %s, updated_at = CURRENT_TIMESTAMP WHERE uuid = $%d", strings.Join(clauses, ", "), len(updateArgs))
			result, err := tx.Exec(ctx, query, updateArgs...)
			if err != nil {
				return err
			}
			if result.RowsAffected() == 0 {
				return pgx.ErrNoRows
			}
		}
		if subpageConfigSet {
			if finalSubpageConfigUUID == nil {
				if _, err := tx.Exec(ctx, `DELETE FROM sub_nodes_to_subscription_page_config WHERE node_uuid = $1`, nodeUUID); err != nil {
					return err
				}
			} else {
				if _, err := tx.Exec(ctx, `
					INSERT INTO sub_nodes_to_subscription_page_config (node_uuid, subpage_config_uuid)
					VALUES ($1, $2)
					ON CONFLICT (node_uuid) DO UPDATE
					SET subpage_config_uuid = EXCLUDED.subpage_config_uuid
				`, nodeUUID, *finalSubpageConfigUUID); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (r *SubscriptionConnectionRepository) deleteNode(ctx context.Context, nodeUUID string) error {
	result, err := r.db.Exec(ctx, `DELETE FROM sub_nodes WHERE uuid = $1`, nodeUUID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *SubscriptionConnectionRepository) setNodeDisabled(ctx context.Context, nodeUUID string, disabled bool) error {
	var query string
	if disabled {
		query = `
			UPDATE sub_nodes
			SET is_disabled = true, is_connecting = false, is_connected = false, updated_at = CURRENT_TIMESTAMP
			WHERE uuid = $1
		`
	} else {
		query = `
			UPDATE sub_nodes
			SET is_disabled = false, updated_at = CURRENT_TIMESTAMP
			WHERE uuid = $1
		`
	}
	result, err := r.db.Exec(ctx, query, nodeUUID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *SubscriptionConnectionRepository) reorderNodes(ctx context.Context, items []reorderNodeItem) error {
	if len(items) == 0 {
		return nil
	}

	return exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		uuids := make([]string, len(items))
		positions := make([]int32, len(items))
		for i, item := range items {
			uuids[i] = item.UUID
			positions[i] = int32(item.ViewPosition)
		}

		if _, err := tx.Exec(ctx, `
			UPDATE sub_nodes AS s
			SET view_position = v.view_position
			FROM (
				SELECT unnest($1::uuid[]) AS uuid, unnest($2::int[]) AS view_position
			) AS v
			WHERE s.uuid = v.uuid
		`, uuids, positions); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT setval('sub_nodes_view_position_seq', (SELECT COALESCE(MAX(view_position), 0) FROM sub_nodes) + 1)`); err != nil {
			return err
		}
		return nil
	})
}

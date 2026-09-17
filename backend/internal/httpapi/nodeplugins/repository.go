package nodeplugins

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"exodus/internal/httpapi/shared"
	monitor "exodus/internal/nodes"
	"exodus/internal/util"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type responseEnvelope[T any] struct {
	Response T `json:"response"`
}

type nodePlugin struct {
	UUID         string          `json:"uuid"`
	Name         string          `json:"name"`
	Tags         []string        `json:"tags"`
	PluginConfig json.RawMessage `json:"pluginConfig"`
	ViewPosition int             `json:"viewPosition"`
	CreatedAt    time.Time       `json:"createdAt"`
	UpdatedAt    time.Time       `json:"updatedAt"`
}

type listPayload struct {
	NodePlugins []nodePlugin `json:"nodePlugins"`
	Total       int          `json:"total"`
}

type createRequest struct {
	Name         string          `json:"name"`
	Tags         []string        `json:"tags,omitempty"`
	PluginConfig json.RawMessage `json:"pluginConfig,omitempty"`
}

type updateRequest struct {
	UUID         *string          `json:"uuid,omitempty"`
	Name         *string          `json:"name,omitempty"`
	Tags         []string         `json:"tags,omitempty"`
	PluginConfig *json.RawMessage `json:"pluginConfig,omitempty"`
	ViewPosition *int             `json:"viewPosition,omitempty"`
}

type reorderRequest struct {
	Items []struct {
		UUID         string `json:"uuid"`
		ViewPosition int    `json:"viewPosition"`
	} `json:"items"`
}

type cloneRequest struct {
	CloneFromUUID string  `json:"cloneFromUuid"`
	Name          *string `json:"name,omitempty"`
}

type executorRequest struct {
	Command     executorCommand `json:"command"`
	TargetNodes struct {
		Target    string   `json:"target"`
		NodeUUIDs []string `json:"nodeUuids"`
	} `json:"targetNodes"`
}

type executorCommand struct {
	Command string          `json:"command"`
	Raw     json.RawMessage `json:"-"`
}

func (c *executorCommand) UnmarshalJSON(data []byte) error {
	type alias executorCommand
	var decoded alias
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*c = executorCommand(decoded)
	c.Raw = append(c.Raw[:0], data...)
	return nil
}

var defaultPluginConfig = json.RawMessage(`{"ingressFilter":{"enabled":false,"blockedIps":[]},"egressFilter":{"enabled":false,"blockedIps":[],"blockedPorts":[]},"haproxyAuth":{"enabled":false,"inboundTags":[]}}`)

const haproxyAllInboundTags = "*"

func normalizePluginConfig(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return append(json.RawMessage(nil), defaultPluginConfig...), nil
	}

	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("pluginConfig must be valid JSON object")
	}
	if obj == nil {
		return append(json.RawMessage(nil), defaultPluginConfig...), nil
	}
	haproxyAuth, err := normalizeHaproxyAuthConfig(obj["haproxyAuth"])
	if err != nil {
		return nil, err
	}
	obj["haproxyAuth"] = haproxyAuth

	normalized, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("pluginConfig cannot be encoded")
	}
	return normalized, nil
}

func normalizeHaproxyAuthConfig(raw any) (map[string]any, error) {
	result := map[string]any{
		"enabled":     false,
		"inboundTags": []string{},
	}
	if raw == nil {
		return result, nil
	}

	obj, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("haproxyAuth must be a JSON object")
	}

	if enabled, ok := obj["enabled"].(bool); ok {
		result["enabled"] = enabled
	}

	if rawTags, ok := obj["inboundTags"]; ok {
		values, ok := rawTags.([]any)
		if !ok {
			return nil, fmt.Errorf("haproxyAuth.inboundTags must be an array")
		}

		tags := make([]string, 0, len(values))
		for _, item := range values {
			value, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("haproxyAuth.inboundTags items must be strings")
			}
			tags = append(tags, value)
		}
		result["inboundTags"] = tags
		if _, hasEnabled := obj["enabled"]; !hasEnabled && len(tags) > 0 {
			result["enabled"] = true
		}
		return result, nil
	}

	if enabled, ok := obj["enabled"].(bool); ok && enabled {
		result["inboundTags"] = []string{"*"}
	}
	return result, nil
}

func normalizeHaproxyInboundTags(raw []string) []string {
	seen := make(map[string]struct{}, len(raw))
	result := make([]string, 0, len(raw))
	for _, item := range raw {
		value := strings.TrimSpace(item)
		if value == "" {
			continue
		}
		if value == haproxyAllInboundTags {
			return []string{haproxyAllInboundTags}
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func loadPlugins(ctx context.Context, db *pgxpool.Pool) ([]nodePlugin, error) {
	rows, err := db.Query(ctx, `
		SELECT uuid::text, name, tags, plugin_config::text, view_position, created_at, updated_at
		FROM node_plugin
		ORDER BY view_position ASC, created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	plugins := make([]nodePlugin, 0)
	for rows.Next() {
		plugin, scanErr := scanPlugin(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		plugins = append(plugins, plugin)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return plugins, nil
}

func loadPluginByUUID(ctx context.Context, db *pgxpool.Pool, pluginUUID string) (nodePlugin, error) {
	var plugin nodePlugin
	row := db.QueryRow(ctx, `
		SELECT uuid::text, name, tags, plugin_config::text, view_position, created_at, updated_at
		FROM node_plugin
		WHERE uuid::text = $1
	`, pluginUUID)
	err := scanPluginRow(row, &plugin)
	return plugin, err
}

func createPlugin(ctx context.Context, db *pgxpool.Pool, name string, tags []string, configJSON json.RawMessage) (nodePlugin, error) {
	sanitized := shared.SanitizeTags(tags)
	var plugin nodePlugin
	row := db.QueryRow(ctx, `
		INSERT INTO node_plugin (name, tags, plugin_config)
		VALUES ($1, $2::text[], $3::jsonb)
		RETURNING uuid::text, name, tags, plugin_config::text, view_position, created_at, updated_at
	`, name, shared.PostgresTextArrayLiteral(sanitized), string(configJSON))
	err := scanPluginRow(row, &plugin)
	return plugin, err
}

func updatePlugin(ctx context.Context, db *pgxpool.Pool, pluginUUID string, name *string, tags []string, configJSON *json.RawMessage, viewPosition *int) (nodePlugin, error) {
	current, err := loadPluginByUUID(ctx, db, pluginUUID)
	if err != nil {
		return nodePlugin{}, err
	}
	nextName := current.Name
	if name != nil {
		nextName = *name
	}
	nextTags := current.Tags
	if tags != nil {
		nextTags = shared.SanitizeTags(tags)
	}
	nextConfig := current.PluginConfig
	if configJSON != nil {
		nextConfig = *configJSON
	}
	nextPosition := current.ViewPosition
	if viewPosition != nil {
		nextPosition = *viewPosition
	}

	var plugin nodePlugin
	row := db.QueryRow(ctx, `
		UPDATE node_plugin
		SET name = $1, tags = $2::text[], plugin_config = $3::jsonb, view_position = $4, updated_at = CURRENT_TIMESTAMP
		WHERE uuid::text = $5
		RETURNING uuid::text, name, tags, plugin_config::text, view_position, created_at, updated_at
	`, nextName, shared.PostgresTextArrayLiteral(nextTags), string(nextConfig), nextPosition, pluginUUID)
	err = scanPluginRow(row, &plugin)
	return plugin, err
}

func deletePlugin(ctx context.Context, db *pgxpool.Pool, pluginUUID string) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()
	if _, err = tx.Exec(ctx, `UPDATE nodes SET active_plugin_uuid = NULL WHERE active_plugin_uuid::text = $1`, pluginUUID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM node_plugin WHERE uuid::text = $1`, pluginUUID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func reorderPlugins(ctx context.Context, db *pgxpool.Pool, req reorderRequest) error {
	if len(req.Items) == 0 {
		return nil
	}

	uuids := make([]string, len(req.Items))
	positions := make([]int32, len(req.Items))
	for i, item := range req.Items {
		if _, err := uuid.Parse(item.UUID); err != nil {
			return fmt.Errorf("invalid uuid")
		}
		uuids[i] = item.UUID
		positions[i] = int32(item.ViewPosition)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	// Single batched UPDATE via UNNEST instead of one round-trip per plugin.
	if _, err := tx.Exec(ctx, `
		UPDATE node_plugin AS p
		SET view_position = v.view_position, updated_at = CURRENT_TIMESTAMP
		FROM (
			SELECT unnest($1::uuid[]) AS uuid, unnest($2::int[]) AS view_position
		) AS v
		WHERE p.uuid = v.uuid
	`, uuids, positions); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func clonePlugin(ctx context.Context, db *pgxpool.Pool, req cloneRequest) (nodePlugin, error) {
	source, err := loadPluginByUUID(ctx, db, strings.TrimSpace(req.CloneFromUUID))
	if err != nil {
		return nodePlugin{}, err
	}
	name := source.Name + " copy"
	if req.Name != nil && strings.TrimSpace(*req.Name) != "" {
		name = strings.TrimSpace(*req.Name)
	} else {
		name = fmt.Sprintf("%s copy %s", source.Name, time.Now().UTC().Format("20060102150405"))
	}
	return createPlugin(ctx, db, name, source.Tags, source.PluginConfig)
}

func getAllTags(ctx context.Context, db *pgxpool.Pool) ([]string, error) {
	rows, err := db.Query(ctx, `
		SELECT DISTINCT unnest(tags) AS tag
		FROM node_plugin
		WHERE tags IS NOT NULL AND cardinality(tags) > 0
		ORDER BY tag ASC
	`)
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
		if trimmed := strings.TrimSpace(tag); trimmed != "" {
			tags = append(tags, trimmed)
		}
	}
	return tags, rows.Err()
}

func setTags(ctx context.Context, db *pgxpool.Pool, pluginUUID string, tags []string) error {
	sanitized := shared.SanitizeTags(tags)
	result, err := db.Exec(ctx, `
		UPDATE node_plugin
		SET tags = $1::text[], updated_at = CURRENT_TIMESTAMP
		WHERE uuid::text = $2
	`, shared.PostgresTextArrayLiteral(sanitized), pluginUUID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func ensureNodesExist(ctx context.Context, db *pgxpool.Pool, nodeUUIDs []string) error {
	var count int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM nodes WHERE uuid::text = ANY($1)`, nodeUUIDs).Scan(&count); err != nil {
		return err
	}
	if count != len(nodeUUIDs) {
		return pgx.ErrNoRows
	}
	return nil
}

func normalizeUUIDList(raw []string) []string {
	return util.NormalizeUUIDs(raw)
}

type pluginScanner interface {
	Scan(dest ...any) error
}

func scanPlugin(rows pgx.Rows) (nodePlugin, error) {
	var plugin nodePlugin
	err := scanPluginRow(rows, &plugin)
	return plugin, err
}

func scanPluginRow(scanner pluginScanner, plugin *nodePlugin) error {
	var rawConfig string
	var tags []string
	if err := scanner.Scan(
		&plugin.UUID,
		&plugin.Name,
		&tags,
		&rawConfig,
		&plugin.ViewPosition,
		&plugin.CreatedAt,
		&plugin.UpdatedAt,
	); err != nil {
		return err
	}
	plugin.Tags = tags
	if plugin.Tags == nil {
		plugin.Tags = []string{}
	}
	plugin.PluginConfig = json.RawMessage(rawConfig)
	return nil
}

func syncPlugin(ctx context.Context, db *pgxpool.Pool, pluginUUID string) error {
	var id string
	if err := db.QueryRow(ctx, `SELECT uuid::text FROM node_plugin WHERE uuid::text = $1`, pluginUUID).Scan(&id); err != nil {
		return err
	}

	rows, err := db.Query(ctx, `SELECT uuid::text FROM nodes WHERE active_plugin_uuid::text = $1 AND is_disabled = false`, pluginUUID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var nodeUUIDs []string
	for rows.Next() {
		var nodeUUID string
		if err := rows.Scan(&nodeUUID); err == nil && nodeUUID != "" {
			nodeUUIDs = append(nodeUUIDs, nodeUUID)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(nodeUUIDs) > 0 {
		monitor.RequestNodeDeploy(false, nodeUUIDs...)
	}
	return nil
}

package subscriptionpageconfigs

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
	"exodus/internal/notifications"
)

const (
	subpageConfigsBasePath    = "/api/subscription-page-configs"
	subpageConfigsActionsPath = "/api/subscription-page-configs/actions/"
	defaultSubpageConfigUUID  = "00000000-0000-0000-0000-000000000000"
)

var subpageConfigNameRegex = regexp.MustCompile(`^[A-Za-z0-9_\s-]+$`)

type SubscriptionPageConfig struct {
	UUID         string          `json:"uuid"`
	ViewPosition int             `json:"viewPosition"`
	Name         string          `json:"name"`
	Tags         []string        `json:"tags"`
	Config       json.RawMessage `json:"config"`
	CreatedAt    time.Time       `json:"createdAt"`
	UpdatedAt    time.Time       `json:"updatedAt"`
}

type subscriptionPageConfigsListResponse struct {
	Total   int                      `json:"total"`
	Configs []SubscriptionPageConfig `json:"configs"`
}

type subpageConfigCreateRequest struct {
	Name string   `json:"name"`
	Tags []string `json:"tags,omitempty"`
}

type subpageConfigUpdateRequest struct {
	UUID   string           `json:"uuid"`
	Name   *string          `json:"name,omitempty"`
	Tags   []string         `json:"tags,omitempty"`
	Config *json.RawMessage `json:"config,omitempty"`
}

type subpageConfigReorderItem struct {
	UUID         string `json:"uuid"`
	ViewPosition int    `json:"viewPosition"`
}

type subpageConfigReorderRequest struct {
	Items []subpageConfigReorderItem `json:"items"`
}

type subpageConfigCloneRequest struct {
	CloneFromUUID string `json:"cloneFromUuid"`
}

func fetchSubscriptionPageConfig(ctx context.Context, dbConn *pgxpool.Pool, uuidStr string, withConfig bool) (SubscriptionPageConfig, error) {
	var cfgItem SubscriptionPageConfig
	query := `
		SELECT uuid, view_position, name, tags, created_at, updated_at
		FROM subscription_page_config
		WHERE uuid = $1
		LIMIT 1
	`
	if withConfig {
		query = `
			SELECT uuid, view_position, name, tags, config, created_at, updated_at
			FROM subscription_page_config
			WHERE uuid = $1
			LIMIT 1
		`
	}

	row := dbConn.QueryRow(ctx, query, uuidStr)
	var viewPosition *int
	var configStr *string
	var tags []string
	if withConfig {
		if err := row.Scan(&cfgItem.UUID, &viewPosition, &cfgItem.Name, &tags, &configStr, &cfgItem.CreatedAt, &cfgItem.UpdatedAt); err != nil {
			return cfgItem, err
		}
		if configStr != nil {
			cfgItem.Config = json.RawMessage(*configStr)
		}
	} else {
		if err := row.Scan(&cfgItem.UUID, &viewPosition, &cfgItem.Name, &tags, &cfgItem.CreatedAt, &cfgItem.UpdatedAt); err != nil {
			return cfgItem, err
		}
	}

	if viewPosition != nil {
		cfgItem.ViewPosition = *viewPosition
	}
	cfgItem.Tags = tags
	if cfgItem.Tags == nil {
		cfgItem.Tags = []string{}
	}
	return cfgItem, nil
}

func emitSubpageConfigChanged(ctx context.Context, cfg *config.BackendConfig, action string, item SubscriptionPageConfig, extra map[string]any) {
	data := map[string]any{
		"action": action,
	}
	if item.UUID != "" {
		data["uuid"] = item.UUID
	}
	if item.Name != "" {
		data["name"] = item.Name
	}
	if item.ViewPosition != 0 {
		data["viewPosition"] = item.ViewPosition
	}
	for key, value := range extra {
		data[key] = value
	}
	notifications.Emit(ctx, cfg, notifications.Event{
		Scope: notifications.ScopeService,
		Event: notifications.EventServiceSubpageChanged,
		Data:  data,
	})
}

func fetchDefaultSubpageConfig(ctx context.Context, db *pgxpool.Pool) (json.RawMessage, error) {
	row := db.QueryRow(ctx, `SELECT config FROM subscription_page_config WHERE uuid = $1`, defaultSubpageConfigUUID)
	var cfgStr *string
	if err := row.Scan(&cfgStr); err == nil && cfgStr != nil {
		return json.RawMessage(*cfgStr), nil
	}

	return json.RawMessage("{}"), nil
}

func getSubNodeUUIDsBySubpageConfigUUID(ctx context.Context, db *pgxpool.Pool, subpageConfigUUID string) ([]string, error) {
	rows, err := db.Query(ctx, `
		SELECT node_uuid
		FROM sub_nodes_to_subscription_page_config
		WHERE subpage_config_uuid = $1
	`, subpageConfigUUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nodeUUIDs := make([]string, 0)
	for rows.Next() {
		var nodeUUID string
		if err := rows.Scan(&nodeUUID); err != nil {
			return nil, err
		}
		nodeUUID = strings.TrimSpace(nodeUUID)
		if nodeUUID != "" {
			nodeUUIDs = append(nodeUUIDs, nodeUUID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return nodeUUIDs, nil
}

func normalizeSubpageConfigName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if len(name) < 2 {
		return "", fmt.Errorf("name must be at least 2 characters")
	}
	if len(name) > 30 {
		return "", fmt.Errorf("name must be less than 30 characters")
	}
	if !subpageConfigNameRegex.MatchString(name) {
		return "", fmt.Errorf("name can only contain letters, numbers, underscores, dashes and spaces")
	}
	return name, nil
}

func randomSuffix(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "00000"
	}
	for i := range buf {
		buf[i] = letters[int(buf[i])%len(letters)]
	}
	return string(buf)
}

func getAllTags(ctx context.Context, db *pgxpool.Pool) ([]string, error) {
	rows, err := db.Query(ctx, `
		SELECT DISTINCT unnest(tags) AS tag
		FROM subscription_page_config
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

func setTags(ctx context.Context, db *pgxpool.Pool, configUUID string, tags []string) error {
	sanitized := shared.SanitizeTags(tags)
	result, err := db.Exec(ctx, `
		UPDATE subscription_page_config
		SET tags = $1::text[], updated_at = CURRENT_TIMESTAMP
		WHERE uuid = $2
	`, shared.PostgresTextArrayLiteral(sanitized), configUUID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

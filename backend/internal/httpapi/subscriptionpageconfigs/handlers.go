package subscriptionpageconfigs

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	exodusdb "exodus/internal/db"
	"exodus/internal/httpapi/shared"
	monitor "exodus/internal/subscriptionnodes"
	"exodus/internal/util"

	"github.com/google/uuid"
)

// SubscriptionPageConfigsHandler godoc
// @Summary      Manage subscription page configs
// @Description  List, create (201), or update subscription page configs
// @Tags         Subscription Page Configs Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Subscription page config parameters"
// @Success      200   {object}  map[string]any
// @Success      201   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /subscription-page-configs [get]
// @Router       /subscription-page-configs [post]
// @Router       /subscription-page-configs [patch]
func SubscriptionPageConfigsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetSubscriptionPageConfigs(w, r, db, cfg)
		case http.MethodPost:
			handleCreateSubscriptionPageConfig(w, r, db, cfg)
		case http.MethodPatch:
			handleUpdateSubscriptionPageConfig(w, r, db, cfg)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// SubscriptionPageConfigsActionsHandler godoc
// @Summary      Subscription page config actions
// @Description  Reorder or clone subscription page configurations
// @Tags         Subscription Page Configs Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Action payload"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /subscription-page-configs/actions/reorder [post]
// @Router       /subscription-page-configs/actions/clone [post]
func SubscriptionPageConfigsActionsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		path := strings.TrimPrefix(r.URL.Path, subpageConfigsActionsPath)
		path = strings.Trim(path, "/")
		switch path {
		case "reorder":
			handleReorderSubscriptionPageConfigs(w, r, db, cfg)
		case "clone":
			handleCloneSubscriptionPageConfig(w, r, db, cfg)
		default:
			http.NotFound(w, r)
		}
	}
}

// SubscriptionPageConfigByUUIDHandler godoc
// @Summary      Subscription page config by UUID
// @Description  Get details or delete subscription page configuration by UUID
// @Tags         Subscription Page Configs Controller
// @Produce      json
// @Security     BearerAuth
// @Param        uuid  path      string  true  "Configuration UUID" format(uuid)
// @Success      200   {object}  map[string]any
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      404   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /subscription-page-configs/{uuid} [get]
// @Router       /subscription-page-configs/{uuid} [delete]
func SubscriptionPageConfigByUUIDHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		uuidStr := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, subpageConfigsBasePath+"/"))
		if uuidStr == "" {
			switch r.Method {
			case http.MethodGet:
				handleGetSubscriptionPageConfigs(w, r, db, cfg)
			case http.MethodPost:
				handleCreateSubscriptionPageConfig(w, r, db, cfg)
			case http.MethodPatch:
				handleUpdateSubscriptionPageConfig(w, r, db, cfg)
			default:
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			}
			return
		}
		if _, err := uuid.Parse(uuidStr); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid UUID format", err, cfg)
			return
		}

		switch r.Method {
		case http.MethodGet:
			handleGetSubscriptionPageConfigByUUID(w, r, db, cfg, uuidStr)
		case http.MethodDelete:
			handleDeleteSubscriptionPageConfig(w, r, db, cfg, uuidStr)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handleGetSubscriptionPageConfigs(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	ctx := r.Context()
	rows, err := db.Query(ctx, `
		SELECT uuid, view_position, name, tags, created_at, updated_at
		FROM subscription_page_config
		ORDER BY view_position ASC
	`)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetAllSubpageConfigsFailed.WithCause(err), cfg)
		return
	}
	defer rows.Close()

	configs := []SubscriptionPageConfig{}
	for rows.Next() {
		var cfgItem SubscriptionPageConfig
		var viewPosition *int
		var tags []string
		if err := rows.Scan(&cfgItem.UUID, &viewPosition, &cfgItem.Name, &tags, &cfgItem.CreatedAt, &cfgItem.UpdatedAt); err != nil {
			shared.SendAPIError(w, shared.ErrGetAllSubpageConfigsFailed.WithCause(err), cfg)
			return
		}
		if viewPosition != nil {
			cfgItem.ViewPosition = *viewPosition
		}
		cfgItem.Tags = tags
		if cfgItem.Tags == nil {
			cfgItem.Tags = []string{}
		}
		configs = append(configs, cfgItem)
	}
	if err := rows.Err(); err != nil {
		shared.SendAPIError(w, shared.ErrGetAllSubpageConfigsFailed.WithCause(err), cfg)
		return
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": subscriptionPageConfigsListResponse{
			Total:   len(configs),
			Configs: configs,
		},
	})
}

func handleGetSubscriptionPageConfigByUUID(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, uuidStr string) {
	ctx := r.Context()
	cfgItem, err := fetchSubscriptionPageConfig(ctx, db, uuidStr, true)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrSubpageConfigNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrGetSubpageConfigByUUIDFailed.WithCause(err), cfg)
		return
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": cfgItem,
	})
}

func handleCreateSubscriptionPageConfig(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req subpageConfigCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	name, err := normalizeSubpageConfigName(req.Name)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
		return
	}

	ctx := r.Context()
	defaultConfig, err := fetchDefaultSubpageConfig(ctx, db)
	if err != nil {
		shared.SendAPIError(w, shared.ErrCreateSubpageConfigFailed.WithCause(err), cfg)
		return
	}

	tags := shared.SanitizeTags(req.Tags)

	row := db.QueryRow(ctx, `
		INSERT INTO subscription_page_config (name, tags, config)
		VALUES ($1, $2::text[], $3)
		RETURNING uuid, view_position, name, tags, config, created_at, updated_at
	`, name, shared.PostgresTextArrayLiteral(tags), string(defaultConfig))

	var created SubscriptionPageConfig
	var viewPosition *int
	var configStr *string
	var tagsArr []string
	if err := row.Scan(&created.UUID, &viewPosition, &created.Name, &tagsArr, &configStr, &created.CreatedAt, &created.UpdatedAt); err != nil {
		if util.IsUniqueViolation(err) {
			shared.SendAPIError(w, shared.ErrSubpageConfigNameAlreadyExists, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrCreateSubpageConfigFailed.WithCause(err), cfg)
		return
	}
	if viewPosition != nil {
		created.ViewPosition = *viewPosition
	}
	created.Tags = tagsArr
	if created.Tags == nil {
		created.Tags = []string{}
	}
	if configStr != nil {
		created.Config = json.RawMessage(*configStr)
	}

	shared.WriteJSON(w, http.StatusCreated, map[string]any{
		"response": created,
	})
	emitSubpageConfigChanged(ctx, cfg, "created", created, nil)
	targetNodeUUIDs, targetErr := getSubNodeUUIDsBySubpageConfigUUID(ctx, db, created.UUID)
	if targetErr != nil {
		cfg.Logger.Warn("Failed to resolve sub nodes for created subpage config push", "subpage_config_uuid", created.UUID, "error", targetErr)
		return
	}
	if len(targetNodeUUIDs) == 0 {
		return
	}
	monitor.RequestSubNodeSubpageConfigPush(created.UUID, created.Config, targetNodeUUIDs...)
}

func handleUpdateSubscriptionPageConfig(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req subpageConfigUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	if _, err := uuid.Parse(req.UUID); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid UUID format", err, cfg)
		return
	}

	if req.Name == nil && req.Config == nil && req.Tags == nil {
		shared.SendError(w, http.StatusBadRequest, "no fields to update", nil, cfg)
		return
	}

	updates := []string{}
	args := []any{}
	idx := 1

	if req.Name != nil {
		name, err := normalizeSubpageConfigName(*req.Name)
		if err != nil {
			shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
			return
		}
		updates = append(updates, fmt.Sprintf("name = $%d", idx))
		args = append(args, name)
		idx++
	}

	if req.Tags != nil {
		sanitized := shared.SanitizeTags(req.Tags)
		updates = append(updates, fmt.Sprintf("tags = $%d::text[]", idx))
		args = append(args, shared.PostgresTextArrayLiteral(sanitized))
		idx++
	}

	if req.Config != nil {
		cleaned, err := normalizeAndValidateSubpageConfig(*req.Config)
		if err != nil {
			shared.SendError(w, http.StatusBadRequest, err.Error(), err, cfg)
			return
		}
		configJSON, err := json.Marshal(cleaned)
		if err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid config", err, cfg)
			return
		}
		updates = append(updates, fmt.Sprintf("config = $%d", idx))
		args = append(args, string(configJSON))
		idx++
	}

	updates = append(updates, "updated_at = CURRENT_TIMESTAMP")
	args = append(args, req.UUID)

	ctx := r.Context()
	query := fmt.Sprintf(`
		UPDATE subscription_page_config
		SET %s
		WHERE uuid = $%d
		RETURNING uuid, view_position, name, tags, config, created_at, updated_at
	`, strings.Join(updates, ", "), idx)

	row := db.QueryRow(ctx, query, args...)
	var updated SubscriptionPageConfig
	var viewPosition *int
	var configStr *string
	var tagsArr []string
	if err := row.Scan(&updated.UUID, &viewPosition, &updated.Name, &tagsArr, &configStr, &updated.CreatedAt, &updated.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrSubpageConfigNotFound, cfg)
			return
		}
		if util.IsUniqueViolation(err) {
			shared.SendAPIError(w, shared.ErrSubpageConfigNameAlreadyExists, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrUpdateSubpageConfigFailed.WithCause(err), cfg)
		return
	}
	if viewPosition != nil {
		updated.ViewPosition = *viewPosition
	}
	updated.Tags = tagsArr
	if updated.Tags == nil {
		updated.Tags = []string{}
	}
	if configStr != nil {
		updated.Config = json.RawMessage(*configStr)
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": updated,
	})
	emitSubpageConfigChanged(ctx, cfg, "updated", updated, nil)
	targetNodeUUIDs, targetErr := getSubNodeUUIDsBySubpageConfigUUID(ctx, db, updated.UUID)
	if targetErr != nil {
		cfg.Logger.Warn("Failed to resolve sub nodes for updated subpage config push", "subpage_config_uuid", updated.UUID, "error", targetErr)
		return
	}
	if len(targetNodeUUIDs) == 0 {
		cfg.Logger.Debug("No linked subscription nodes for updated subpage config push", "subpage_config_uuid", updated.UUID)
		return
	}
	cfg.Logger.Info(
		"Dispatching updated subpage config push",
		"subpage_config_uuid", updated.UUID,
		"target_nodes_count", len(targetNodeUUIDs),
		"target_node_uuids", targetNodeUUIDs,
	)
	monitor.RequestSubNodeSubpageConfigPush(updated.UUID, updated.Config, targetNodeUUIDs...)
}

func handleDeleteSubscriptionPageConfig(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, uuidStr string) {
	if uuidStr == defaultSubpageConfigUUID {
		shared.SendAPIError(w, shared.ErrReservedSubpageConfigCantBeDeleted, cfg)
		return
	}

	ctx := r.Context()
	targetNodeUUIDs, targetErr := getSubNodeUUIDsBySubpageConfigUUID(ctx, db, uuidStr)
	if targetErr != nil {
		shared.SendAPIError(w, shared.ErrDeleteSubpageConfigFailed.WithCause(targetErr), cfg)
		return
	}

	result, err := db.Exec(ctx, `DELETE FROM subscription_page_config WHERE uuid = $1`, uuidStr)
	if err != nil {
		shared.SendAPIError(w, shared.ErrDeleteSubpageConfigFailed.WithCause(err), cfg)
		return
	}

	rows := result.RowsAffected()
	if rows == 0 {
		shared.SendAPIError(w, shared.ErrSubpageConfigNotFound, cfg)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	emitSubpageConfigChanged(ctx, cfg, "deleted", SubscriptionPageConfig{UUID: uuidStr}, nil)
	if len(targetNodeUUIDs) == 0 {
		return
	}
	monitor.RequestSubNodeSubpageConfigPush(uuidStr, nil, targetNodeUUIDs...)
}

func handleReorderSubscriptionPageConfigs(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req subpageConfigReorderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	if len(req.Items) == 0 {
		shared.SendError(w, http.StatusBadRequest, "items is required", nil, cfg)
		return
	}

	seen := map[string]struct{}{}
	for _, item := range req.Items {
		if _, err := uuid.Parse(item.UUID); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid UUID format", err, cfg)
			return
		}
		if _, ok := seen[item.UUID]; ok {
			shared.SendError(w, http.StatusBadRequest, "duplicate uuid in items", nil, cfg)
			return
		}
		seen[item.UUID] = struct{}{}
	}

	ctx := r.Context()
	uuids := make([]string, len(req.Items))
	positions := make([]int32, len(req.Items))
	for i, item := range req.Items {
		uuids[i] = item.UUID
		positions[i] = int32(item.ViewPosition)
	}

	err := exodusdb.WithRetryTx(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE subscription_page_config AS c
			SET view_position = v.view_position
			FROM (
				SELECT unnest($1::uuid[]) AS uuid, unnest($2::int[]) AS view_position
			) AS v
			WHERE c.uuid = v.uuid
		`, uuids, positions); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT setval('subscription_page_config_view_position_seq', (SELECT COALESCE(MAX(view_position), 0) FROM subscription_page_config) + 1)`); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		shared.SendAPIError(w, shared.ErrReorderSubpageConfigsFailed.WithCause(err), cfg)
		return
	}

	emitSubpageConfigChanged(ctx, cfg, "reordered", SubscriptionPageConfig{}, map[string]any{"items": len(req.Items)})
	handleGetSubscriptionPageConfigs(w, r, db, cfg)
}

func handleCloneSubscriptionPageConfig(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req subpageConfigCloneRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	if _, err := uuid.Parse(req.CloneFromUUID); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid UUID format", err, cfg)
		return
	}

	ctx := r.Context()
	var cfgItem SubscriptionPageConfig
	row := db.QueryRow(ctx, `
		SELECT uuid, view_position, name, config, created_at, updated_at
		FROM subscription_page_config
		WHERE uuid = $1
		LIMIT 1
	`, req.CloneFromUUID)

	var viewPosition *int
	var configStr *string
	if err := row.Scan(&cfgItem.UUID, &viewPosition, &cfgItem.Name, &configStr, &cfgItem.CreatedAt, &cfgItem.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrSubpageConfigNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrCloneSubpageConfigFailed.WithCause(err), cfg)
		return
	}
	if viewPosition != nil {
		cfgItem.ViewPosition = *viewPosition
	}
	if configStr != nil {
		cfgItem.Config = json.RawMessage(*configStr)
	}

	cloneName := fmt.Sprintf("Clone %s", randomSuffix(5))
	insertRow := db.QueryRow(ctx, `
		INSERT INTO subscription_page_config (name, config)
		VALUES ($1, $2)
		RETURNING uuid, view_position, name, config, created_at, updated_at
	`, cloneName, string(cfgItem.Config))

	var created SubscriptionPageConfig
	var insertViewPosition *int
	var insertConfigStr *string
	if err := insertRow.Scan(&created.UUID, &insertViewPosition, &created.Name, &insertConfigStr, &created.CreatedAt, &created.UpdatedAt); err != nil {
		shared.SendAPIError(w, shared.ErrCloneSubpageConfigFailed.WithCause(err), cfg)
		return
	}
	if insertViewPosition != nil {
		created.ViewPosition = *insertViewPosition
	}
	if insertConfigStr != nil {
		created.Config = json.RawMessage(*insertConfigStr)
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": created,
	})
	emitSubpageConfigChanged(ctx, cfg, "cloned", created, map[string]any{"cloneFromUuid": req.CloneFromUUID})
	targetNodeUUIDs, targetErr := getSubNodeUUIDsBySubpageConfigUUID(ctx, db, created.UUID)
	if targetErr != nil {
		cfg.Logger.Warn("Failed to resolve sub nodes for cloned subpage config push", "subpage_config_uuid", created.UUID, "error", targetErr)
		return
	}
	if len(targetNodeUUIDs) == 0 {
		return
	}
	monitor.RequestSubNodeSubpageConfigPush(created.UUID, created.Config, targetNodeUUIDs...)
}

// SubscriptionPageConfigsTagsHandler godoc
// @Summary      Manage subscription page config tags
// @Description  Get unique subscription page config tags or set tags for a subscription page config
// @Tags         Subscription Page Configs Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /subscription-page-configs/tags [get]
// @Router       /subscription-page-configs/tags [patch]
func SubscriptionPageConfigsTagsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetSubscriptionPageConfigTags(w, r, db, cfg)
		case http.MethodPatch:
			handleSetSubscriptionPageConfigTags(w, r, db, cfg)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handleGetSubscriptionPageConfigTags(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	tags, err := getAllTags(r.Context(), db)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetAllSubpageConfigsFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"tags": tags,
		},
	})
}

func handleSetSubscriptionPageConfigTags(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req shared.SetEntityTagsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	if _, err := uuid.Parse(strings.TrimSpace(req.UUID)); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
		return
	}

	if err := setTags(r.Context(), db, req.UUID, req.Tags); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrSubpageConfigNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrUpdateSubpageConfigFailed.WithCause(err), cfg)
		return
	}

	sanitized := shared.SanitizeTags(req.Tags)
	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"uuid": req.UUID,
			"tags": sanitized,
		},
	})
}

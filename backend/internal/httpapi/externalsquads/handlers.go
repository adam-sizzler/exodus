package externalsquads

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
	"exodus/internal/util"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ExternalSquadsHandler godoc
// @Summary      Manage external squads
// @Description  List, create (201), or update external squads
// @Tags         External Squads Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "External squad create/update payload"
// @Success      200   {object}  map[string]any
// @Success      201   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /external-squads [get]
// @Router       /external-squads [post]
// @Router       /external-squads [patch]
func ExternalSquadsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetExternalSquads(w, r, db, cfg)
		case http.MethodPost:
			handleCreateExternalSquad(w, r, db, cfg)
		case http.MethodPatch:
			handleUpdateExternalSquad(w, r, db, cfg)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// ExternalSquadByUUIDHandler godoc
// @Summary      External squad by UUID
// @Description  Get, update, or delete external squad by UUID
// @Tags         External Squads Controller
// @Produce      json
// @Security     BearerAuth
// @Param        uuid  path      string  true  "External squad UUID" format(uuid)
// @Success      200   {object}  map[string]any
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      404   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /external-squads/{uuid} [get]
// @Router       /external-squads/{uuid} [delete]
func ExternalSquadByUUIDHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimSpace(trimExternalSquadsPath(r.URL.Path))

		if path == "" {
			switch r.Method {
			case http.MethodGet:
				handleGetExternalSquads(w, r, db, cfg)
			case http.MethodPost:
				handleCreateExternalSquad(w, r, db, cfg)
			case http.MethodPatch:
				handleUpdateExternalSquad(w, r, db, cfg)
			default:
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			}
			return
		}

		parts := strings.Split(path, "/")
		squadUUID := strings.TrimSpace(parts[0])

		if _, err := uuid.Parse(squadUUID); err != nil {
			if r.Method == http.MethodPatch {
				handleUpdateExternalSquad(w, r, db, cfg)
				return
			}
			shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
			return
		}

		if len(parts) > 1 {
			if len(parts) == 3 && parts[1] == "bulk-actions" && parts[2] == "add-users" {
				if r.Method != http.MethodPost {
					shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
					return
				}
				handleBulkAddUsersToExternalSquad(w, r, db, cfg, squadUUID)
				return
			}
			if len(parts) == 3 && parts[1] == "bulk-actions" && parts[2] == "remove-users" {
				if r.Method != http.MethodDelete && r.Method != http.MethodPost {
					shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
					return
				}
				handleBulkRemoveUsersFromExternalSquad(w, r, db, cfg, squadUUID)
				return
			}
			shared.SendAPIError(w, shared.ErrNotFound, cfg)
			return
		}

		switch r.Method {
		case http.MethodGet:
			handleGetExternalSquadByUUID(w, r, db, cfg, squadUUID)
		case http.MethodPatch:
			handleUpdateExternalSquad(w, r, db, cfg)
		case http.MethodDelete:
			handleDeleteExternalSquad(w, r, db, cfg, squadUUID)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func trimExternalSquadsPath(p string) string {
	p = strings.TrimPrefix(p, "/api/external-squads")
	return strings.TrimPrefix(p, "/")
}

// ExternalSquadsReorderHandler godoc
// @Summary      Reorder external squads
// @Description  Update view position of external squads
// @Tags         External Squads Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Reorder payload"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /external-squads/actions/reorder [post]
func ExternalSquadsReorderHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var req ReorderExternalSquadsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
			return
		}

		if len(req.Squads) == 0 {
			req.Squads = req.Items
		}

		if len(req.Squads) == 0 {
			shared.SendError(w, http.StatusBadRequest, "items cannot be empty", nil, cfg)
			return
		}

		for _, item := range req.Squads {
			if _, err := uuid.Parse(item.UUID); err != nil {
				shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
				return
			}
		}

		tx, err := db.Begin(r.Context())
		if err != nil {
			shared.SendAPIError(w, shared.ErrReorderExternalSquadsFailed.WithCause(err), cfg)
			return
		}
		defer func() { _ = tx.Rollback(r.Context()) }()

		for _, item := range req.Squads {
			if _, err := tx.Exec(r.Context(),
				`UPDATE external_squads SET view_position = $1 WHERE uuid = $2`,
				item.ViewPosition, item.UUID); err != nil {
				shared.SendAPIError(w, shared.ErrReorderExternalSquadsFailed.WithCause(err), cfg)
				return
			}
		}

		if err := tx.Commit(r.Context()); err != nil {
			shared.SendAPIError(w, shared.ErrReorderExternalSquadsFailed.WithCause(err), cfg)
			return
		}

		handleGetExternalSquads(w, r, db, cfg)
	}
}

func handleGetExternalSquads(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	records, err := getExternalSquads(r.Context(), db)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetExternalSquadsFailed.WithCause(err), cfg)
		return
	}

	result := make([]ExternalSquadAPI, 0, len(records))
	squadUUIDs := make([]string, 0, len(records))
	for _, rec := range records {
		api, err := convertExternalSquadToAPI(rec)
		if err != nil {
			cfg.Logger.Error("Failed to convert external squad", "uuid", rec.UUID, "error", err)
			continue
		}
		result = append(result, api)
		squadUUIDs = append(squadUUIDs, api.UUID)
	}

	// Batch-load members count and templates for all squads in 2 queries total,
	// instead of 2 queries per squad (was N+1 across the whole list).
	membersCount, err := getExternalSquadsMembersCount(r.Context(), db, squadUUIDs)
	if err != nil {
		cfg.Logger.Error("Failed to batch-load external squads members count", "error", err)
	}
	templatesBySquad, err := getExternalSquadsTemplates(r.Context(), db, squadUUIDs)
	if err != nil {
		cfg.Logger.Error("Failed to batch-load external squads templates", "error", err)
	}

	for i := range result {
		result[i].Info.MembersCount = membersCount[result[i].UUID]
		if t, ok := templatesBySquad[result[i].UUID]; ok && t != nil {
			result[i].Templates = t
		} else {
			result[i].Templates = make([]ExternalSquadTemplate, 0)
		}
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"total":          len(result),
			"externalSquads": result,
		},
	})
}

func handleGetExternalSquadByUUID(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, squadUUID string) {
	record, err := getExternalSquadByUUID(r.Context(), db, squadUUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrExternalSquadNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrGetExternalSquadByUUIDFailed.WithCause(err), cfg)
		return
	}

	api, err := convertExternalSquadToAPI(record)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetExternalSquadByUUIDFailed.WithCause(err), cfg)
		return
	}

	_ = db.QueryRow(r.Context(), `SELECT COUNT(*) FROM users WHERE external_squad_uuid = $1`, api.UUID).Scan(&api.Info.MembersCount)
	rows, err := db.Query(r.Context(), `SELECT template_uuid, template_type FROM external_squads_templates WHERE external_squad_uuid = $1`, api.UUID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var t ExternalSquadTemplate
			if err := rows.Scan(&t.TemplateUUID, &t.TemplateType); err == nil {
				api.Templates = append(api.Templates, t)
			}
		}
		_ = rows.Err()
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{"response": api})
}

func handleCreateExternalSquad(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, "failed to read body", err, cfg)
		return
	}

	var req CreateExternalSquadRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}

	if err := req.Validate(); err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
		return
	}

	squadUUID := uuid.NewString()

	tx, err := db.Begin(r.Context())
	if err != nil {
		shared.SendAPIError(w, shared.ErrCreateExternalSquadFailed.WithCause(err), cfg)
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	subSettings, err := marshalJSON(req.SubscriptionSettings)
	if err != nil {
		shared.SendAPIError(w, shared.ErrCreateExternalSquadFailed.WithCause(err), cfg)
		return
	}
	hostOverrides, err := marshalJSON(req.HostOverrides)
	if err != nil {
		shared.SendAPIError(w, shared.ErrCreateExternalSquadFailed.WithCause(err), cfg)
		return
	}
	respHeadersAdd, err := marshalJSON(req.ResponseHeadersAdd)
	if err != nil {
		shared.SendAPIError(w, shared.ErrCreateExternalSquadFailed.WithCause(err), cfg)
		return
	}
	respHeadersRemove := shared.PostgresTextArrayLiteral(req.ResponseHeadersRemove)
	hwidSettings, err := marshalJSON(req.HWIDSettings)
	if err != nil {
		shared.SendAPIError(w, shared.ErrCreateExternalSquadFailed.WithCause(err), cfg)
		return
	}
	customRemarks, err := marshalJSON(req.CustomRemarks)
	if err != nil {
		shared.SendAPIError(w, shared.ErrCreateExternalSquadFailed.WithCause(err), cfg)
		return
	}

	tags := shared.SanitizeTags(req.Tags)

	_, err = tx.Exec(r.Context(), `
		INSERT INTO external_squads (
			uuid, view_position, name, tags,
			subscription_settings, host_overrides, response_headers_add, response_headers_remove,
			hwid_settings, custom_remarks, subpage_config_uuid
		) VALUES ($1, $2, $3, $4::text[], $5, $6, $7, $8, $9, $10, $11)
	`,
		squadUUID,
		util.Coalesce(req.ViewPosition, 0),
		strings.TrimSpace(req.Name),
		shared.PostgresTextArrayLiteral(tags),
		subSettings,
		hostOverrides,
		respHeadersAdd,
		respHeadersRemove,
		hwidSettings,
		customRemarks,
		normalizeStringPtr(req.SubpageConfigUUID),
	)
	if err != nil {
		if util.IsUniqueViolation(err, "external_squads_name_key") {
			shared.SendAPIError(w, shared.ErrExternalSquadNameAlreadyExists, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrCreateExternalSquadFailed.WithCause(err), cfg)
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		shared.SendAPIError(w, shared.ErrCreateExternalSquadFailed.WithCause(err), cfg)
		return
	}

	handleGetExternalSquadByUUID(w, r, db, cfg, squadUUID)
}

func handleUpdateExternalSquad(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, "failed to read body", err, cfg)
		return
	}

	var req UpdateExternalSquadRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}

	if err := req.Validate(); err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
		return
	}

	if _, err := uuid.Parse(req.UUID); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
		return
	}

	tx, err := db.Begin(r.Context())
	if err != nil {
		shared.SendAPIError(w, shared.ErrUpdateExternalSquadFailed.WithCause(err), cfg)
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

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
	if req.ViewPosition != nil {
		add("view_position", *req.ViewPosition)
	}
	if req.Tags != nil {
		sanitized := shared.SanitizeTags(req.Tags)
		add("tags", shared.PostgresTextArrayLiteral(sanitized))
	}

	if req.SubscriptionSettings != nil {
		if string(req.SubscriptionSettings) == "null" {
			add("subscription_settings", nil)
		} else {
			add("subscription_settings", string(req.SubscriptionSettings))
		}
	}
	if req.HostOverrides != nil {
		if string(req.HostOverrides) == "null" {
			add("host_overrides", nil)
		} else {
			add("host_overrides", string(req.HostOverrides))
		}
	}
	if req.ResponseHeadersAdd != nil {
		if string(req.ResponseHeadersAdd) == "null" {
			add("response_headers_add", nil)
		} else {
			add("response_headers_add", string(req.ResponseHeadersAdd))
		}
	}
	if req.ResponseHeadersRemove != nil {
		if string(req.ResponseHeadersRemove) == "null" {
			add("response_headers_remove", "{}")
		} else {
			items := shared.ParsePgTextArray(string(req.ResponseHeadersRemove))
			add("response_headers_remove", shared.PostgresTextArrayLiteral(items))
		}
	}
	if req.HWIDSettings != nil {
		if string(req.HWIDSettings) == "null" {
			add("hwid_settings", nil)
		} else {
			add("hwid_settings", string(req.HWIDSettings))
		}
	}
	if req.CustomRemarks != nil {
		if string(req.CustomRemarks) == "null" {
			add("custom_remarks", nil)
		} else {
			add("custom_remarks", string(req.CustomRemarks))
		}
	}
	if req.SubpageConfigUUID != nil {
		add("subpage_config_uuid", normalizeStringPtr(req.SubpageConfigUUID))
	}

	if len(clauses) > 0 {
		clauses = append(clauses, "updated_at = CURRENT_TIMESTAMP")
		args = append(args, req.UUID)

		query := fmt.Sprintf(`
			UPDATE external_squads
			SET %s
			WHERE uuid = $%d
		`, strings.Join(clauses, ", "), idx)

		if _, err := tx.Exec(r.Context(), query, args...); err != nil {
			if util.IsUniqueViolation(err, "external_squads_name_key") {
				shared.SendAPIError(w, shared.ErrExternalSquadNameAlreadyExists, cfg)
				return
			}
			shared.SendAPIError(w, shared.ErrUpdateExternalSquadFailed.WithCause(err), cfg)
			return
		}
	} else if req.Templates != nil {
		_, err := tx.Exec(r.Context(), `
			UPDATE external_squads 
			SET updated_at = CURRENT_TIMESTAMP 
			WHERE uuid = $1
		`, req.UUID)
		if err != nil {
			shared.SendAPIError(w, shared.ErrUpdateExternalSquadFailed.WithCause(err), cfg)
			return
		}
	}

	if req.Templates != nil {
		_, err = tx.Exec(r.Context(), `DELETE FROM external_squads_templates WHERE external_squad_uuid = $1`, req.UUID)
		if err != nil {
			shared.SendAPIError(w, shared.ErrUpdateExternalSquadFailed.WithCause(err), cfg)
			return
		}

		for _, t := range *req.Templates {
			_, err = tx.Exec(r.Context(), `
				INSERT INTO external_squads_templates (external_squad_uuid, template_uuid, template_type)
				VALUES ($1, $2, $3)
			`, req.UUID, t.TemplateUUID, t.TemplateType)
			if err != nil {
				shared.SendAPIError(w, shared.ErrUpdateExternalSquadFailed.WithCause(err), cfg)
				return
			}
		}
	}

	if len(clauses) == 0 && req.Templates == nil {
		shared.SendError(w, http.StatusBadRequest, "no fields to update", nil, cfg)
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		shared.SendAPIError(w, shared.ErrUpdateExternalSquadFailed.WithCause(err), cfg)
		return
	}

	if OnSquadUpdated != nil {
		OnSquadUpdated(req.UUID)
	}

	handleGetExternalSquadByUUID(w, r, db, cfg, req.UUID)
}

func handleDeleteExternalSquad(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, squadUUID string) {
	result, err := db.Exec(r.Context(), `DELETE FROM external_squads WHERE uuid = $1`, squadUUID)
	if err != nil {
		shared.SendAPIError(w, shared.ErrDeleteExternalSquadFailed.WithCause(err), cfg)
		return
	}
	if result.RowsAffected() == 0 {
		shared.SendAPIError(w, shared.ErrExternalSquadNotFound, cfg)
		return
	}

	if OnSquadUpdated != nil {
		OnSquadUpdated(squadUUID)
	}

	w.WriteHeader(http.StatusNoContent)
}

// OnSquadUpdated is invoked whenever external squad overrides are modified.
var OnSquadUpdated func(uuid string)

func handleBulkAddUsersToExternalSquad(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, squadUUID string) {
	var req BulkUsersRequest
	bodyBytes, err := io.ReadAll(r.Body)

	hasSpecificUsers := false
	if err == nil && len(bodyBytes) > 0 {
		if jsonErr := json.Unmarshal(bodyBytes, &req); jsonErr == nil && len(req.UserUUIDs) > 0 {
			if validateErr := req.Validate(); validateErr == nil {
				hasSpecificUsers = true
			}
		}
	}

	var affected int64
	var exists int
	if err := db.QueryRow(r.Context(), `SELECT 1 FROM external_squads WHERE uuid = $1`, squadUUID).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrExternalSquadNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrAddUsersToExternalSquadFailed.WithCause(err), cfg)
		return
	}

	if hasSpecificUsers {
		placeholders := make([]string, len(req.UserUUIDs))
		args := make([]any, 0, len(req.UserUUIDs)+1)
		args = append(args, squadUUID)

		for i, u := range req.UserUUIDs {
			placeholders[i] = fmt.Sprintf("$%d", i+2)
			args = append(args, u)
		}

		query := fmt.Sprintf(`
			UPDATE users
			SET external_squad_uuid = $1, updated_at = CURRENT_TIMESTAMP
			WHERE uuid IN (%s)
		`, strings.Join(placeholders, ", "))

		result, err := db.Exec(r.Context(), query, args...)
		if err != nil {
			shared.SendAPIError(w, shared.ErrAddUsersToExternalSquadFailed.WithCause(err), cfg)
			return
		}
		affected = result.RowsAffected()
	} else {
		result, err := db.Exec(r.Context(), `
			UPDATE users
			SET external_squad_uuid = $1::uuid, updated_at = CURRENT_TIMESTAMP
			WHERE external_squad_uuid IS DISTINCT FROM $1::uuid
		`, squadUUID)
		if err != nil {
			shared.SendAPIError(w, shared.ErrAddUsersToExternalSquadFailed.WithCause(err), cfg)
			return
		}
		affected = result.RowsAffected()
	}

	cfg.Logger.Info("Users added to external squad", "squad_uuid", squadUUID, "affected_rows", affected)
	w.WriteHeader(http.StatusAccepted)
}

func handleBulkRemoveUsersFromExternalSquad(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, squadUUID string) {
	var req BulkUsersRequest
	bodyBytes, err := io.ReadAll(r.Body)

	hasSpecificUsers := false
	if err == nil && len(bodyBytes) > 0 {
		if jsonErr := json.Unmarshal(bodyBytes, &req); jsonErr == nil && len(req.UserUUIDs) > 0 {
			if validateErr := req.Validate(); validateErr == nil {
				hasSpecificUsers = true
			}
		}
	}

	var affected int64
	var exists int
	if err := db.QueryRow(r.Context(), `SELECT 1 FROM external_squads WHERE uuid = $1`, squadUUID).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrExternalSquadNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrRemoveUsersFromExternalSquadFailed.WithCause(err), cfg)
		return
	}

	if hasSpecificUsers {
		placeholders := make([]string, len(req.UserUUIDs))
		args := make([]any, 0, len(req.UserUUIDs)+1)

		for i, u := range req.UserUUIDs {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args = append(args, u)
		}
		args = append(args, squadUUID)

		query := fmt.Sprintf(`
			UPDATE users
			SET external_squad_uuid = NULL, updated_at = CURRENT_TIMESTAMP
			WHERE uuid IN (%s) AND external_squad_uuid = $%d
		`, strings.Join(placeholders, ", "), len(req.UserUUIDs)+1)

		result, err := db.Exec(r.Context(), query, args...)
		if err != nil {
			shared.SendAPIError(w, shared.ErrRemoveUsersFromExternalSquadFailed.WithCause(err), cfg)
			return
		}
		affected = result.RowsAffected()
	} else {
		result, err := db.Exec(r.Context(), `
			UPDATE users
			SET external_squad_uuid = NULL, updated_at = CURRENT_TIMESTAMP
			WHERE external_squad_uuid = $1
		`, squadUUID)
		if err != nil {
			shared.SendAPIError(w, shared.ErrRemoveUsersFromExternalSquadFailed.WithCause(err), cfg)
			return
		}
		affected = result.RowsAffected()
	}

	cfg.Logger.Info("Users removed from external squad", "squad_uuid", squadUUID, "affected_rows", affected)

	w.WriteHeader(http.StatusAccepted)
}

// ExternalSquadsTagsHandler godoc
// @Summary      Manage external squad tags
// @Description  Get unique external squad tags or set tags for an external squad
// @Tags         External Squads Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /external-squads/tags [get]
// @Router       /external-squads/tags [patch]
func ExternalSquadsTagsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetExternalSquadTags(w, r, db, cfg)
		case http.MethodPatch:
			handleSetExternalSquadTags(w, r, db, cfg)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handleGetExternalSquadTags(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	tags, err := getAllTags(r.Context(), db)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetExternalSquadsFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"tags": tags,
		},
	})
}

func handleSetExternalSquadTags(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
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
			shared.SendAPIError(w, shared.ErrExternalSquadNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrUpdateExternalSquadFailed.WithCause(err), cfg)
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

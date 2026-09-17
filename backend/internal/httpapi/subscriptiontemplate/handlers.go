package subscriptiontemplate

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/db"
	exodusdb "exodus/internal/db"
	"exodus/internal/httpapi/shared"
	"exodus/internal/util"

	"github.com/google/uuid"
)

// SubscriptionTemplatesHandler godoc
// @Summary      Manage subscription templates
// @Description  List, create (201), or update subscription templates
// @Tags         Subscription Template Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Subscription template creation/update fields"
// @Success      200   {object}  map[string]any
// @Success      201   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /subscription-templates [get]
// @Router       /subscription-templates [post]
// @Router       /subscription-templates [patch]
func SubscriptionTemplatesHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetSubscriptionTemplates(w, r, db, cfg)
		case http.MethodPost:
			handleCreateSubscriptionTemplate(w, r, db, cfg)
		case http.MethodPatch:
			handleUpdateSubscriptionTemplate(w, r, db, cfg)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// SubscriptionTemplatesActionsHandler godoc
// @Summary      Subscription template actions
// @Description  Reorder subscription templates
// @Tags         Subscription Template Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Reorder payload"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /subscription-templates/actions/reorder [post]
func SubscriptionTemplatesActionsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		path := strings.TrimPrefix(r.URL.Path, subscriptionTemplatesActionsPath)
		path = strings.Trim(path, "/")
		switch path {
		case "reorder":
			handleReorderSubscriptionTemplates(w, r, db, cfg)
		default:
			http.NotFound(w, r)
		}
	}
}

// SubscriptionTemplateByUUIDHandler godoc
// @Summary      Subscription template by UUID
// @Description  Get details or delete subscription template by UUID
// @Tags         Subscription Template Controller
// @Produce      json
// @Security     BearerAuth
// @Param        uuid  path      string  true  "Template UUID" format(uuid)
// @Success      200   {object}  map[string]any
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      404   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /subscription-templates/{uuid} [get]
// @Router       /subscription-templates/{uuid} [delete]
func SubscriptionTemplateByUUIDHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		uuidStr := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, subscriptionTemplatesBasePath+"/"))
		if uuidStr == "" {
			switch r.Method {
			case http.MethodGet:
				handleGetSubscriptionTemplates(w, r, db, cfg)
			case http.MethodPost:
				handleCreateSubscriptionTemplate(w, r, db, cfg)
			case http.MethodPatch:
				handleUpdateSubscriptionTemplate(w, r, db, cfg)
			default:
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			}
			return
		}
		if _, err := uuid.Parse(uuidStr); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
			return
		}

		switch r.Method {
		case http.MethodGet:
			handleGetSubscriptionTemplateByUUID(w, r, db, cfg, uuidStr)
		case http.MethodDelete:
			handleDeleteSubscriptionTemplate(w, r, db, cfg, uuidStr)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handleGetSubscriptionTemplates(w http.ResponseWriter, r *http.Request, dbConn *pgxpool.Pool, cfg *config.BackendConfig) {
	rows, err := dbConn.Query(r.Context(), `
		SELECT uuid, view_position, name, tags, template_type
		FROM subscription_templates
		ORDER BY view_position ASC, template_type ASC`)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetAllSubTemplatesFailed.WithCause(err), cfg)
		return
	}
	defer rows.Close()

	templates := make([]SubscriptionTemplate, 0)
	for rows.Next() {
		var rec subscriptionTemplateRecord
		var tags []string
		if scanErr := rows.Scan(&rec.UUID, &rec.ViewPosition, &rec.Name, &tags, &rec.TemplateType); scanErr != nil {
			shared.SendAPIError(w, shared.ErrGetAllSubTemplatesFailed.WithCause(scanErr), cfg)
			return
		}
		if tags == nil {
			tags = []string{}
		}
		templates = append(templates, SubscriptionTemplate{
			UUID:               rec.UUID,
			ViewPosition:       rec.ViewPosition,
			Name:               rec.Name,
			Tags:               tags,
			TemplateType:       rec.TemplateType,
			TemplateJSON:       nil,
			EncodedTemplateYML: nil,
		})
	}
	if err := rows.Err(); err != nil {
		shared.SendAPIError(w, shared.ErrGetAllSubTemplatesFailed.WithCause(err), cfg)
		return
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": subscriptionTemplatesListResponse{
			Total:     len(templates),
			Templates: templates,
		},
	})
}

func handleGetSubscriptionTemplateByUUID(w http.ResponseWriter, r *http.Request, dbConn *pgxpool.Pool, cfg *config.BackendConfig, templateUUID string) {
	var rec subscriptionTemplateRecord
	row := dbConn.QueryRow(r.Context(), `
		SELECT uuid, view_position, name, tags, template_type, template_yaml, template_json
		FROM subscription_templates
		WHERE uuid = $1`, templateUUID)
	if err := scanSubscriptionTemplateRecord(row, &rec); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrSubTemplateNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrGetSubTemplateByUUIDFailed.WithCause(err), cfg)
		return
	}

	resp := mapSubscriptionTemplateRecord(rec, true)
	shared.WriteJSON(w, http.StatusOK, map[string]any{"response": resp})
}

func handleCreateSubscriptionTemplate(w http.ResponseWriter, r *http.Request, dbConn *pgxpool.Pool, cfg *config.BackendConfig) {
	var req subscriptionTemplateCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}

	req.Name = strings.TrimSpace(req.Name)
	req.TemplateType = strings.TrimSpace(strings.ToUpper(req.TemplateType))

	if err := validateTemplateName(req.Name); err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
		return
	}
	if req.Name == "Default" {
		shared.SendAPIError(w, shared.ErrReservedSubTemplateName, cfg)
		return
	}
	if !isAllowedTemplateType(req.TemplateType) {
		shared.SendError(w, http.StatusBadRequest, "invalid templateType", nil, cfg)
		return
	}
	if req.TemplateType == "XRAY_BASE64" {
		shared.SendAPIError(w, shared.ErrSubTemplateTypeNotAllowed, cfg)
		return
	}

	defaultTmpl, ok := db.DefaultSubscriptionTemplateByType(req.TemplateType)
	if !ok {
		shared.SendError(w, http.StatusBadRequest, "unknown templateType", nil, cfg)
		return
	}

	newUUID := uuid.NewString()

	viewPosition := 0
	row := dbConn.QueryRow(r.Context(), `SELECT COALESCE(MAX(view_position), 0) + 1 FROM subscription_templates`)
	if scanErr := row.Scan(&viewPosition); scanErr != nil {
		shared.SendAPIError(w, shared.ErrCreateSubTemplateFailed.WithCause(scanErr), cfg)
		return
	}

	tags := shared.SanitizeTags(req.Tags)

	_, execErr := dbConn.Exec(r.Context(), `
		INSERT INTO subscription_templates (
			uuid, view_position, name, tags, template_type, template_yaml, template_json
		) VALUES ($1, $2, $3, $4::text[], $5, $6, $7)`,
		newUUID,
		viewPosition,
		req.Name,
		shared.PostgresTextArrayLiteral(tags),
		req.TemplateType,
		defaultTmpl.TemplateYAML,
		defaultTmpl.TemplateJSON,
	)
	if execErr != nil {
		if util.IsUniqueViolation(execErr) {
			shared.SendAPIError(w, shared.ErrSubTemplateNameAlreadyExistsForThisType, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrCreateSubTemplateFailed.WithCause(execErr), cfg)
		return
	}

	created := subscriptionTemplateRecord{
		UUID:         newUUID,
		ViewPosition: viewPosition,
		Name:         req.Name,
		Tags:         tags,
		TemplateType: req.TemplateType,
		TemplateYAML: defaultTmpl.TemplateYAML,
		TemplateJSON: defaultTmpl.TemplateJSON,
	}

	shared.WriteJSON(w, http.StatusCreated, map[string]any{"response": mapSubscriptionTemplateRecord(created, true)})
}

func handleUpdateSubscriptionTemplate(w http.ResponseWriter, r *http.Request, dbConn *pgxpool.Pool, cfg *config.BackendConfig) {
	var req subscriptionTemplateUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	if _, err := uuid.Parse(req.UUID); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
		return
	}

	if req.Name != nil {
		trimmed := strings.TrimSpace(*req.Name)
		if err := validateTemplateName(trimmed); err != nil {
			shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
			return
		}
		if trimmed == "Default" {
			shared.SendAPIError(w, shared.ErrReservedSubTemplateName, cfg)
			return
		}
		req.Name = &trimmed
	}

	if req.TemplateJSON != nil {
		if err := ensureJSONObject(*req.TemplateJSON); err != nil {
			shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
			return
		}
	}

	if req.TemplateJSON != nil && req.EncodedTemplateYML != nil {
		shared.SendAPIError(w, shared.ErrSubTemplateJsonAndYamlCannotBeUpdatedSimultaneously, cfg)
		return
	}

	var template subscriptionTemplateRecord
	row := dbConn.QueryRow(r.Context(), `
		SELECT uuid, view_position, name, tags, template_type, template_yaml, template_json
		FROM subscription_templates
		WHERE uuid = $1`, req.UUID)
	if scanErr := scanSubscriptionTemplateRecord(row, &template); scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrSubTemplateNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrGetSubTemplateByUUIDFailed.WithCause(scanErr), cfg)
		return
	}

	isYamlTemplate := template.TemplateType == "MIHOMO" || template.TemplateType == "STASH" || template.TemplateType == "CLASH"
	isJsonTemplate := template.TemplateType == "XRAY_JSON" || template.TemplateType == "SINGBOX"

	if isYamlTemplate && req.TemplateJSON != nil {
		shared.SendAPIError(w, shared.ErrSubTemplateJsonNotAllowedForYaml, cfg)
		return
	}
	if isJsonTemplate && req.EncodedTemplateYML != nil {
		shared.SendAPIError(w, shared.ErrSubTemplateYamlNotAllowedForJson, cfg)
		return
	}

	var decodedYAML *string
	if req.EncodedTemplateYML != nil {
		decoded, decodeErr := base64.StdEncoding.DecodeString(*req.EncodedTemplateYML)
		if decodeErr != nil {
			shared.SendError(w, http.StatusBadRequest, "encodedTemplateYaml must be base64", decodeErr, cfg)
			return
		}
		decodedStr := string(decoded)
		decodedYAML = &decodedStr
	}

	var clauses []string
	var args []any
	idx := 1
	add := func(column string, value any) {
		clauses = append(clauses, fmt.Sprintf("%s = $%d", column, idx))
		args = append(args, value)
		idx++
	}

	if req.Name != nil {
		add("name", *req.Name)
	}
	if req.Tags != nil {
		sanitized := shared.SanitizeTags(req.Tags)
		add("tags", shared.PostgresTextArrayLiteral(sanitized))
	}
	if req.TemplateJSON != nil {
		add("template_json", []byte(*req.TemplateJSON))
	}
	if req.EncodedTemplateYML != nil {
		add("template_yaml", decodedYAML)
	}

	if len(clauses) == 0 {
		shared.SendError(w, http.StatusBadRequest, "no fields to update", nil, cfg)
		return
	}

	args = append(args, req.UUID)
	query := fmt.Sprintf("UPDATE subscription_templates SET %s, updated_at = CURRENT_TIMESTAMP WHERE uuid = $%d", strings.Join(clauses, ", "), idx)

	result, execErr := dbConn.Exec(r.Context(), query, args...)
	if execErr != nil {
		if util.IsUniqueViolation(execErr) {
			shared.SendAPIError(w, shared.ErrSubTemplateNameAlreadyExistsForThisType, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrUpdateSubTemplateFailed.WithCause(execErr), cfg)
		return
	}
	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		shared.SendAPIError(w, shared.ErrSubTemplateNotFound, cfg)
		return
	}

	if req.Name != nil {
		template.Name = *req.Name
	}
	if req.Tags != nil {
		template.Tags = shared.SanitizeTags(req.Tags)
	}
	if req.TemplateJSON != nil {
		template.TemplateJSON = *req.TemplateJSON
	}
	if req.EncodedTemplateYML != nil {
		template.TemplateYAML = decodedYAML
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{"response": mapSubscriptionTemplateRecord(template, true)})
}

func handleDeleteSubscriptionTemplate(w http.ResponseWriter, r *http.Request, dbConn *pgxpool.Pool, cfg *config.BackendConfig, templateUUID string) {
	var templateName string
	row := dbConn.QueryRow(r.Context(), `SELECT name FROM subscription_templates WHERE uuid = $1`, templateUUID)
	if err := row.Scan(&templateName); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrSubTemplateNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrGetSubTemplateByUUIDFailed.WithCause(err), cfg)
		return
	}

	if templateName == "Default" {
		shared.SendAPIError(w, shared.ErrReservedSubTemplateCannotBeDeleted, cfg)
		return
	}

	if _, execErr := dbConn.Exec(r.Context(), `DELETE FROM subscription_templates WHERE uuid = $1`, templateUUID); execErr != nil {
		shared.SendAPIError(w, shared.ErrDeleteSubTemplateFailed.WithCause(execErr), cfg)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func handleReorderSubscriptionTemplates(w http.ResponseWriter, r *http.Request, dbConn *pgxpool.Pool, cfg *config.BackendConfig) {
	var req subscriptionTemplateReorderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	if len(req.Items) == 0 {
		shared.SendError(w, http.StatusBadRequest, "items cannot be empty", nil, cfg)
		return
	}
	for _, item := range req.Items {
		if _, err := uuid.Parse(item.UUID); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
			return
		}
	}

	uuids := make([]string, len(req.Items))
	positions := make([]int32, len(req.Items))
	for i, item := range req.Items {
		uuids[i] = item.UUID
		positions[i] = int32(item.ViewPosition)
	}

	err := exodusdb.WithRetryTx(r.Context(), dbConn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(r.Context(), `
			UPDATE subscription_templates AS t
			SET view_position = v.view_position
			FROM (
				SELECT unnest($1::uuid[]) AS uuid, unnest($2::int[]) AS view_position
			) AS v
			WHERE t.uuid = v.uuid
		`, uuids, positions); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		shared.SendAPIError(w, shared.ErrReorderSubscriptionTemplatesFailed.WithCause(err), cfg)
		return
	}

	handleGetSubscriptionTemplates(w, r, dbConn, cfg)
}

// SubscriptionTemplateTagsHandler godoc
// @Summary      Manage subscription template tags
// @Description  Get unique subscription template tags or set tags for a subscription template
// @Tags         Subscription Template Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /subscription-templates/tags [get]
// @Router       /subscription-templates/tags [patch]
func SubscriptionTemplateTagsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetSubscriptionTemplateTags(w, r, db, cfg)
		case http.MethodPatch:
			handleSetSubscriptionTemplateTags(w, r, db, cfg)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handleGetSubscriptionTemplateTags(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	tags, err := getAllTags(r.Context(), db)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetAllSubTemplatesFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"tags": tags,
		},
	})
}

func handleSetSubscriptionTemplateTags(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
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
			shared.SendAPIError(w, shared.ErrSubTemplateNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrUpdateSubTemplateFailed.WithCause(err), cfg)
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

func getAllTags(ctx context.Context, db *pgxpool.Pool) ([]string, error) {
	rows, err := db.Query(ctx, `
		SELECT DISTINCT unnest(tags) AS tag
		FROM subscription_templates
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

func setTags(ctx context.Context, db *pgxpool.Pool, templateUUID string, tags []string) error {
	sanitized := shared.SanitizeTags(tags)
	result, err := db.Exec(ctx, `
		UPDATE subscription_templates
		SET tags = $1::text[], updated_at = CURRENT_TIMESTAMP
		WHERE uuid = $2
	`, shared.PostgresTextArrayLiteral(sanitized), templateUUID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

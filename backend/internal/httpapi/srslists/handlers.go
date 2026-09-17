package srslists

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
	srscore "exodus/internal/srslists"
	monitor "exodus/internal/subscriptionnodes"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SRSListsHandler godoc
// @Summary      Manage SRS rule lists
// @Description  List, create (201), or update sing-box binary rule-set lists
// @Tags         SRS Lists Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "SRS rule list payload"
// @Success      200   {object}  map[string]any
// @Success      201   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /srs-lists [get]
// @Router       /srs-lists [post]
// @Router       /srs-lists [patch]
func SRSListsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetSRSLists(w, r, db, cfg)
		case http.MethodPost:
			handleCreateSRSLists(w, r, db, cfg)
		case http.MethodPatch:
			handleUpdateSRSList(w, r, db, cfg)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// SRSListByUUIDHandler godoc
// @Summary      SRS rule list by UUID
// @Description  Get or delete SRS rule list by UUID
// @Tags         SRS Lists Controller
// @Produce      json
// @Security     BearerAuth
// @Param        uuid  path      string  true  "SRS rule list UUID" format(uuid)
// @Success      200   {object}  map[string]any
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      404   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /srs-lists/{uuid} [get]
// @Router       /srs-lists/{uuid} [delete]
func SRSListByUUIDHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		uuidStr := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/api/srs-lists/"))
		if uuidStr == "" {
			SRSListsHandler(db, cfg)(w, r)
			return
		}
		if _, err := uuid.Parse(uuidStr); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
			return
		}

		switch r.Method {
		case http.MethodDelete:
			handleDeleteSRSList(w, r, db, cfg, uuidStr)
		case http.MethodGet:
			handleGetSRSList(w, r, db, cfg, uuidStr)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// SRSListsActionsHandler godoc
// @Summary      SRS rule list actions
// @Description  Reorder, check or trigger subscription node sync for SRS rule lists
// @Tags         SRS Lists Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Action payload"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /srs-lists/actions/reorder [post]
// @Router       /srs-lists/actions/check [post]
// @Router       /srs-lists/actions/sync [post]
func SRSListsActionsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/srs-lists/actions/"), "/")
		switch path {
		case "reorder":
			handleReorderSRSLists(w, r, db, cfg)
		case "check":
			handleCheckSRSLists(w, r, db, cfg)
		case "sync":
			monitor.RequestSubNodeSRSDeploy()
			shared.WriteJSON(w, http.StatusOK, map[string]any{"response": map[string]any{"queued": true}})
		default:
			http.NotFound(w, r)
		}
	}
}

// SRSListsBulkHandler godoc
// @Summary      Bulk SRS rule list operations
// @Description  Bulk delete, enable, disable, or update download interval for SRS lists
// @Tags         SRS Lists Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Bulk payload"
// @Success      200   {object}  map[string]any
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /srs-lists/bulk/delete [post]
// @Router       /srs-lists/bulk/enable [post]
// @Router       /srs-lists/bulk/disable [post]
// @Router       /srs-lists/bulk/set-interval [post]
func SRSListsBulkHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/srs-lists/bulk/"), "/")
		switch path {
		case "delete":
			handleBulkDeleteSRSLists(w, r, db, cfg)
		case "enable":
			handleBulkEnableSRSLists(w, r, db, cfg, true)
		case "disable":
			handleBulkEnableSRSLists(w, r, db, cfg, false)
		case "set-interval":
			handleBulkSetIntervalSRSLists(w, r, db, cfg)
		default:
			http.NotFound(w, r)
		}
	}
}

func handleGetSRSLists(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	items, err := srscore.LoadAll(r.Context(), db)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetSrsListsFailed.WithCause(err), cfg)
		return
	}
	apiItems := make([]srsListAPI, 0, len(items))
	for _, item := range items {
		tags := item.Tags
		if tags == nil {
			tags = []string{}
		}
		apiItems = append(apiItems, srsListAPI{
			UUID:           item.UUID,
			Tags:           tags,
			Format:         item.Format,
			URL:            item.URL,
			UpdateInterval: item.UpdateInterval,
			Path:           item.Path,
			FileName:       item.FileName,
			ShortName:      item.FileName,
			ViewPosition:   item.ViewPosition,
			IsEnabled:      item.IsEnabled,
			IsAvailable:    item.IsAvailable,
			LastCheckedAt:  item.LastCheckedAt,
			LastError:      item.LastError,
			CreatedAt:      item.CreatedAt,
			UpdatedAt:      item.UpdatedAt,
		})
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{"response": map[string]any{"srsLists": apiItems}})
}

func handleGetSRSList(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, listUUID string) {
	items, err := srscore.LoadAll(r.Context(), db)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetSrsListByUUIDFailed.WithCause(err), cfg)
		return
	}
	for _, item := range items {
		if item.UUID == listUUID {
			tags := item.Tags
			if tags == nil {
				tags = []string{}
			}
			shared.WriteJSON(w, http.StatusOK, map[string]any{"response": srsListAPI{
				UUID:           item.UUID,
				Tags:           tags,
				Format:         item.Format,
				URL:            item.URL,
				UpdateInterval: item.UpdateInterval,
				Path:           item.Path,
				FileName:       item.FileName,
				ShortName:      item.FileName,
				ViewPosition:   item.ViewPosition,
				IsEnabled:      item.IsEnabled,
				IsAvailable:    item.IsAvailable,
				LastCheckedAt:  item.LastCheckedAt,
				LastError:      item.LastError,
				CreatedAt:      item.CreatedAt,
				UpdatedAt:      item.UpdatedAt,
			}})
			return
		}
	}
	shared.SendAPIError(w, shared.ErrSrsListNotFound, cfg)
}

func handleCreateSRSLists(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	reqStarted := time.Now()
	defer func() {
		cfg.Logger.Debug("SRS create request completed", "duration_ms", time.Since(reqStarted).Milliseconds())
	}()

	var req createSRSListsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid request payload", err, cfg)
		return
	}

	rawURLs := make([]string, 0, len(req.URLs)+1)
	if strings.TrimSpace(req.URL) != "" {
		rawURLs = append(rawURLs, req.URL)
	}
	rawURLs = append(rawURLs, req.URLs...)

	if len(rawURLs) == 0 {
		shared.WriteJSONError(w, http.StatusBadRequest, "url or urls[] is required")
		return
	}

	type createItem struct {
		Tags           []string
		Format         string
		URL            string
		UpdateInterval string
		Path           *string
		FileName       string
		IsEnabled      bool
	}

	toCreate := make([]createItem, 0, len(rawURLs))
	seenURL := make(map[string]struct{}, len(rawURLs))
	for _, raw := range rawURLs {
		cleanURL := strings.TrimSpace(raw)
		if cleanURL == "" {
			continue
		}
		if _, exists := seenURL[cleanURL]; exists {
			continue
		}
		seenURL[cleanURL] = struct{}{}

		fileName, err := srscore.DeriveFileNameFromURL(cleanURL)
		if err != nil {
			shared.SendError(w, http.StatusBadRequest, fmt.Sprintf("invalid url %q", cleanURL), err, cfg)
			return
		}
		itemFormat := strings.ToLower(strings.TrimSpace(req.Format))
		if itemFormat == "" {
			itemFormat = "binary"
		}

		updateInterval := strings.TrimSpace(req.UpdateInterval)
		if updateInterval == "" {
			updateInterval = "1d"
		}

		itemTags := shared.SanitizeTags(req.Tags)
		if itemTags == nil {
			itemTags = []string{}
		}

		var pathValue *string
		if p := strings.TrimSpace(req.Path); p != "" {
			pathValue = &p
		}
		isEnabled := true
		if req.IsEnabled != nil {
			isEnabled = *req.IsEnabled
		}

		toCreate = append(toCreate, createItem{
			Tags:           itemTags,
			Format:         itemFormat,
			URL:            cleanURL,
			UpdateInterval: updateInterval,
			Path:           pathValue,
			FileName:       fileName,
			IsEnabled:      isEnabled,
		})
	}

	if len(toCreate) == 0 {
		shared.WriteJSONError(w, http.StatusBadRequest, "no valid urls provided")
		return
	}

	dbStarted := time.Now()
	tx, err := db.Begin(r.Context())
	if err != nil {
		shared.SendAPIError(w, shared.ErrCreateSRSListFailed.WithCause(err), cfg)
		return
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	var maxPos *int64
	if err := tx.QueryRow(r.Context(), `SELECT COALESCE(MAX(view_position), -1) FROM srs_lists`).Scan(&maxPos); err != nil {
		shared.SendAPIError(w, shared.ErrCreateSRSListFailed.WithCause(err), cfg)
		return
	}
	currentPos := int64(-1)
	if maxPos != nil {
		currentPos = *maxPos
	}

	for _, item := range toCreate {
		currentPos++
		if _, err := tx.Exec(r.Context(), `
			INSERT INTO srs_lists (tags, format, url, update_interval, path, file_name, view_position, is_enabled, is_available, created_at, updated_at)
			VALUES ($1::text[], $2, $3, $4, $5, $6, $7, $8, false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		`, shared.PostgresTextArrayLiteral(item.Tags), item.Format, item.URL, item.UpdateInterval, item.Path, item.FileName, currentPos, item.IsEnabled); err != nil {
			shared.SendAPIError(w, shared.ErrCreateSRSListFailed.WithCause(err), cfg)
			return
		}
	}

	if err := tx.Commit(r.Context()); err != nil {
		shared.SendAPIError(w, shared.ErrCreateSRSListFailed.WithCause(err), cfg)
		return
	}

	cfg.Logger.Debug("SRS create DB write completed", "duration_ms", time.Since(dbStarted).Milliseconds(), "created", len(toCreate))

	checkStarted := time.Now()
	if _, err := srscore.CheckAndUpdateAvailability(context.Background(), db, cfg); err != nil {
		cfg.Logger.Warn("Failed to check SRS lists right after create", "error", err)
	}
	cfg.Logger.Debug("SRS create availability check completed", "duration_ms", time.Since(checkStarted).Milliseconds())

	handleGetSRSLists(w, r, db, cfg)
}

func handleUpdateSRSList(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	reqStarted := time.Now()
	defer func() {
		cfg.Logger.Debug("SRS update request completed", "duration_ms", time.Since(reqStarted).Milliseconds())
	}()

	var req updateSRSListRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid request payload", err, cfg)
		return
	}
	if strings.TrimSpace(req.UUID) == "" {
		shared.SendError(w, http.StatusBadRequest, "uuid is required", nil, cfg)
		return
	}
	if _, err := uuid.Parse(strings.TrimSpace(req.UUID)); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid uuid", nil, cfg)
		return
	}

	updates := make([]string, 0, 8)
	args := make([]any, 0, 9)
	idx := 1

	var fileName string
	if req.URL != nil {
		urlValue := strings.TrimSpace(*req.URL)
		if urlValue == "" {
			shared.SendError(w, http.StatusBadRequest, "url cannot be empty", nil, cfg)
			return
		}
		derivedName, err := srscore.DeriveFileNameFromURL(urlValue)
		if err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid url", err, cfg)
			return
		}
		fileName = derivedName
		updates = append(updates, fmt.Sprintf("url = $%d", idx), fmt.Sprintf("file_name = $%d", idx+1), "is_available = false", "last_error = NULL")
		args = append(args, urlValue, fileName)
		idx += 2
	}

	if req.Tags != nil {
		sanitized := shared.SanitizeTags(req.Tags)
		updates = append(updates, fmt.Sprintf("tags = $%d", idx))
		args = append(args, shared.PostgresTextArrayLiteral(sanitized))
		idx++
	}

	if req.Format != nil {
		formatValue := strings.ToLower(strings.TrimSpace(*req.Format))
		if formatValue == "" {
			formatValue = "binary"
		}
		updates = append(updates, fmt.Sprintf("format = $%d", idx))
		args = append(args, formatValue)
		idx++
	}

	if req.UpdateInterval != nil {
		val := strings.TrimSpace(*req.UpdateInterval)
		if val == "" {
			val = "1d"
		}
		updates = append(updates, fmt.Sprintf("update_interval = $%d", idx))
		args = append(args, val)
		idx++
	}

	if req.Path != nil {
		pathValue := strings.TrimSpace(*req.Path)
		if pathValue == "" {
			updates = append(updates, "path = NULL")
		} else {
			updates = append(updates, fmt.Sprintf("path = $%d", idx))
			args = append(args, pathValue)
			idx++
		}
	}
	if req.IsEnabled != nil {
		updates = append(updates, fmt.Sprintf("is_enabled = $%d", idx))
		args = append(args, *req.IsEnabled)
		idx++
	}

	if len(updates) == 0 {
		shared.SendError(w, http.StatusBadRequest, "nothing to update", nil, cfg)
		return
	}

	updates = append(updates, "updated_at = CURRENT_TIMESTAMP")
	args = append(args, strings.TrimSpace(req.UUID))

	query := fmt.Sprintf("UPDATE srs_lists SET %s WHERE uuid = $%d", strings.Join(updates, ", "), idx)

	dbStarted := time.Now()
	res, execErr := db.Exec(r.Context(), query, args...)
	if execErr != nil {
		shared.SendAPIError(w, shared.ErrUpdateSRSListFailed.WithCause(execErr), cfg)
		return
	}
	if res.RowsAffected() == 0 {
		shared.SendAPIError(w, shared.ErrSrsListNotFound, cfg)
		return
	}

	cfg.Logger.Debug("SRS update DB write completed", "duration_ms", time.Since(dbStarted).Milliseconds(), "uuid", strings.TrimSpace(req.UUID))

	checkStarted := time.Now()
	if _, err := srscore.CheckAndUpdateAvailability(context.Background(), db, cfg); err != nil {
		cfg.Logger.Warn("Failed to check SRS list after update", "error", err)
	}
	cfg.Logger.Debug("SRS update availability check completed", "duration_ms", time.Since(checkStarted).Milliseconds(), "uuid", strings.TrimSpace(req.UUID))

	handleGetSRSLists(w, r, db, cfg)
}

func handleDeleteSRSList(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, listUUID string) {
	res, execErr := db.Exec(r.Context(), `DELETE FROM srs_lists WHERE uuid = $1`, listUUID)
	if execErr != nil {
		shared.SendAPIError(w, shared.ErrDeleteSRSListFailed.WithCause(execErr), cfg)
		return
	}
	if res.RowsAffected() == 0 {
		shared.SendAPIError(w, shared.ErrSrsListNotFound, cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{"response": map[string]any{"deleted": true}})
}

func handleBulkDeleteSRSLists(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req bulkDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid request payload", err, cfg)
		return
	}
	cleanUUIDs, err := normalizeUUIDs(req.UUIDs)
	if len(cleanUUIDs) == 0 {
		shared.SendError(w, http.StatusBadRequest, "uuids are required", nil, cfg)
		return
	}
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, "one or more uuids are invalid", nil, cfg)
		return
	}

	_, execErr := db.Exec(r.Context(), `DELETE FROM srs_lists WHERE uuid = ANY($1)`, cleanUUIDs)
	if execErr != nil {
		shared.SendAPIError(w, shared.ErrDeleteSRSListFailed.WithCause(execErr), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{"response": map[string]any{"deleted": true}})
}

func handleBulkEnableSRSLists(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, enabled bool) {
	var req bulkEnableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid request payload", err, cfg)
		return
	}
	cleanUUIDs, err := normalizeUUIDs(req.UUIDs)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, "one or more uuids are invalid", nil, cfg)
		return
	}
	if len(cleanUUIDs) == 0 {
		shared.SendError(w, http.StatusBadRequest, "uuids are required", nil, cfg)
		return
	}

	_, execErr := db.Exec(r.Context(), `
		UPDATE srs_lists
		SET is_enabled = $1, updated_at = CURRENT_TIMESTAMP
		WHERE uuid = ANY($2)
	`, enabled, cleanUUIDs)
	if execErr != nil {
		shared.SendAPIError(w, shared.ErrUpdateSRSListFailed.WithCause(execErr), cfg)
		return
	}

	handleGetSRSLists(w, r, db, cfg)
}

func handleBulkSetIntervalSRSLists(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req bulkSetIntervalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid request payload", err, cfg)
		return
	}
	cleanUUIDs, err := normalizeUUIDs(req.UUIDs)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, "one or more uuids are invalid", nil, cfg)
		return
	}
	if len(cleanUUIDs) == 0 {
		shared.SendError(w, http.StatusBadRequest, "uuids are required", nil, cfg)
		return
	}
	interval := strings.TrimSpace(req.UpdateInterval)
	if interval == "" {
		shared.SendError(w, http.StatusBadRequest, "updateInterval is required", nil, cfg)
		return
	}

	_, execErr := db.Exec(r.Context(), `
		UPDATE srs_lists
		SET update_interval = $1, updated_at = CURRENT_TIMESTAMP
		WHERE uuid = ANY($2)
	`, interval, cleanUUIDs)
	if execErr != nil {
		shared.SendAPIError(w, shared.ErrUpdateSRSListFailed.WithCause(execErr), cfg)
		return
	}

	handleGetSRSLists(w, r, db, cfg)
}

func handleReorderSRSLists(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req reorderSRSListsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid request payload", err, cfg)
		return
	}
	if len(req.Items) == 0 {
		shared.WriteJSONError(w, http.StatusBadRequest, "items are required")
		return
	}

	tx, err := db.Begin(r.Context())
	if err != nil {
		shared.SendAPIError(w, shared.ErrReorderSRSListsFailed.WithCause(err), cfg)
		return
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	uuids := make([]string, len(req.Items))
	positions := make([]int32, len(req.Items))
	for i, item := range req.Items {
		trimmed := strings.TrimSpace(item.UUID)
		if _, err := uuid.Parse(trimmed); err != nil {
			shared.SendError(w, http.StatusBadRequest, fmt.Sprintf("invalid uuid: %s", item.UUID), nil, cfg)
			return
		}
		uuids[i] = trimmed
		positions[i] = int32(item.ViewPosition)
	}

	// Single batched UPDATE via UNNEST instead of one round-trip per item.
	if _, err := tx.Exec(r.Context(), `
		UPDATE srs_lists AS s
		SET view_position = v.view_position, updated_at = CURRENT_TIMESTAMP
		FROM (
			SELECT unnest($1::uuid[]) AS uuid, unnest($2::int[]) AS view_position
		) AS v
		WHERE s.uuid = v.uuid
	`, uuids, positions); err != nil {
		shared.SendAPIError(w, shared.ErrReorderSRSListsFailed.WithCause(err), cfg)
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		shared.SendAPIError(w, shared.ErrReorderSRSListsFailed.WithCause(err), cfg)
		return
	}

	handleGetSRSLists(w, r, db, cfg)
}

func handleCheckSRSLists(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	started := time.Now()
	defer func() {
		cfg.Logger.Debug("SRS check request completed", "duration_ms", time.Since(started).Milliseconds())
	}()

	var req checkListsRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	if len(req.UUIDs) > 0 {
		if err := checkSelectedLists(r.Context(), db, cfg, req.UUIDs); err != nil {
			shared.SendAPIError(w, shared.ErrGetAllSRSListsFailed.WithCause(err), cfg)
			return
		}
	} else {
		if _, err := srscore.CheckAndUpdateAvailability(r.Context(), db, cfg); err != nil {
			shared.SendAPIError(w, shared.ErrGetAllSRSListsFailed.WithCause(err), cfg)
			return
		}
	}
	handleGetSRSLists(w, r, db, cfg)
}

// SRSListsTagsHandler godoc
// @Summary      Manage SRS list tags
// @Description  Get unique SRS list tags or set tags for an SRS list
// @Tags         SRS Lists Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /srs-lists/tags [get]
// @Router       /srs-lists/tags [patch]
func SRSListsTagsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetSRSListTags(w, r, db, cfg)
		case http.MethodPatch:
			handleSetSRSListTags(w, r, db, cfg)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handleGetSRSListTags(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	tags, err := getAllTags(r.Context(), db)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetSrsListsFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"tags": tags,
		},
	})
}

func handleSetSRSListTags(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req shared.SetEntityTagsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	if _, err := uuid.Parse(strings.TrimSpace(req.UUID)); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
		return
	}

	sanitized := shared.SanitizeTags(req.Tags)
	if err := setTags(r.Context(), db, req.UUID, sanitized); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrSrsListNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrUpdateSrsListFailed.WithCause(err), cfg)
		return
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"uuid": req.UUID,
			"tags": sanitized,
		},
	})
}

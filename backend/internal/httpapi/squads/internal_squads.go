package squads

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// InternalSquadsHandler godoc
// @Summary      Manage internal squads
// @Description  List, create (201), or update internal squads
// @Tags         Internal Squads Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Squad parameters"
// @Success      200   {object}  map[string]any
// @Success      201   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /internal-squads [get]
// @Router       /internal-squads [post]
// @Router       /internal-squads [patch]
func InternalSquadsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewSquadRepository(db)
	service := NewSquadService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetInternalSquads(w, r, service)
		case http.MethodPost:
			handleCreateInternalSquad(w, r, service)
		case http.MethodPatch:
			handleUpdateInternalSquad(w, r, service)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// InternalSquadsTagsHandler godoc
// @Summary      Manage internal squad tags
// @Description  Get unique internal squad tags or set tags for an internal squad
// @Tags         Internal Squads Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /internal-squads/tags [get]
// @Router       /internal-squads/tags [patch]
func InternalSquadsTagsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewSquadRepository(db)
	service := NewSquadService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetInternalSquadTags(w, r, service)
		case http.MethodPatch:
			handleSetInternalSquadTags(w, r, service)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// InternalSquadsReorderHandler godoc
// @Summary      Reorder internal squads
// @Description  Update view position of internal squads
// @Tags         Internal Squads Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Reorder payload"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /internal-squads/actions/reorder [post]
func InternalSquadsReorderHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewSquadRepository(db)
	service := NewSquadService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		handleReorderInternalSquads(w, r, service)
	}
}

// InternalSquadByUUIDHandler godoc
// @Summary      Internal squad by UUID
// @Description  Get or delete internal squad by UUID, or batch add/remove users
// @Tags         Internal Squads Controller
// @Produce      json
// @Security     BearerAuth
// @Param        uuid  path      string  true  "Squad UUID" format(uuid)
// @Success      200   {object}  map[string]any
// @Success      202
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      404   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /internal-squads/{uuid} [get]
// @Router       /internal-squads/{uuid} [delete]
// @Router       /internal-squads/{uuid}/bulk-actions/add-many-users [post]
// @Router       /internal-squads/{uuid}/bulk-actions/remove-many-users [post]
func InternalSquadByUUIDHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewSquadRepository(db)
	service := NewSquadService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/internal-squads/"), "/")
		if path == "" {
			InternalSquadsHandler(db, cfg)(w, r)
			return
		}
		parts := strings.Split(path, "/")
		squadUUID := parts[0]
		if _, err := uuid.Parse(squadUUID); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
			return
		}

		if len(parts) > 1 {
			if len(parts) == 3 && parts[1] == "bulk-actions" {
				switch parts[2] {
				case "add-users":
					if r.Method != http.MethodPost {
						shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
						return
					}
					handleBulkAddUsersToInternalSquad(w, r, db, cfg, squadUUID)
					return
				case "remove-users":
					if r.Method != http.MethodDelete && r.Method != http.MethodPost {
						shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
						return
					}
					handleBulkRemoveUsersFromInternalSquad(w, r, db, cfg, squadUUID)
					return
				case "add-many-users":
					if r.Method != http.MethodPost {
						shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
						return
					}
					handleBulkAddManyUsersToInternalSquad(w, r, db, cfg, squadUUID)
					return
				case "remove-many-users":
					if r.Method != http.MethodDelete && r.Method != http.MethodPost {
						shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
						return
					}
					handleBulkRemoveManyUsersFromInternalSquad(w, r, db, cfg, squadUUID)
					return
				}
			}
			if len(parts) == 2 && parts[1] == "usage" && r.Method == http.MethodGet {
				handleGetInternalSquadUsage(w, r, db, cfg, squadUUID)
				return
			}
			http.NotFound(w, r)
			return
		}

		switch r.Method {
		case http.MethodGet:
			handleGetInternalSquad(w, r, service, squadUUID)
		case http.MethodDelete:
			handleDeleteInternalSquad(w, r, service, squadUUID)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handleBulkAddUsersToInternalSquad(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, squadUUID string) {
	ctx := r.Context()
	var exists int
	if err := db.QueryRow(ctx, `SELECT 1 FROM internal_squads WHERE uuid = $1`, squadUUID).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrInternalSquadNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrAddUsersToInternalSquadFailed.WithCause(err), cfg)
		return
	}

	_, err := db.Exec(ctx, `
		INSERT INTO internal_squad_members (internal_squad_uuid, user_id)
		SELECT $1, id FROM users
		ON CONFLICT (internal_squad_uuid, user_id) DO NOTHING
	`, squadUUID)
	if err != nil {
		shared.SendAPIError(w, shared.ErrAddUsersToInternalSquadFailed.WithCause(err), cfg)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func handleBulkRemoveUsersFromInternalSquad(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, squadUUID string) {
	ctx := r.Context()
	var exists int
	if err := db.QueryRow(ctx, `SELECT 1 FROM internal_squads WHERE uuid = $1`, squadUUID).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrInternalSquadNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrRemoveUsersFromInternalSquadFailed.WithCause(err), cfg)
		return
	}

	_, err := db.Exec(ctx, `
		DELETE FROM internal_squad_members WHERE internal_squad_uuid = $1
	`, squadUUID)
	if err != nil {
		shared.SendAPIError(w, shared.ErrRemoveUsersFromInternalSquadFailed.WithCause(err), cfg)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func handleBulkAddManyUsersToInternalSquad(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, squadUUID string) {
	ctx := r.Context()
	var exists int
	if err := db.QueryRow(ctx, `SELECT 1 FROM internal_squads WHERE uuid = $1`, squadUUID).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrInternalSquadNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrAddUsersToInternalSquadFailed.WithCause(err), cfg)
		return
	}

	var req struct {
		UserIDs []int64 `json:"userIds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.UserIDs) == 0 || len(req.UserIDs) > 1000 {
		shared.SendError(w, http.StatusBadRequest, "invalid request body (userIds array of 1..1000 required)", nil, cfg)
		return
	}

	_, err := db.Exec(ctx, `
		INSERT INTO internal_squad_members (internal_squad_uuid, user_id)
		SELECT $1, unnest($2::bigint[])
		ON CONFLICT (internal_squad_uuid, user_id) DO NOTHING
	`, squadUUID, req.UserIDs)
	if err != nil {
		shared.SendAPIError(w, shared.ErrAddUsersToInternalSquadFailed.WithCause(err), cfg)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func handleBulkRemoveManyUsersFromInternalSquad(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, squadUUID string) {
	ctx := r.Context()
	var exists int
	if err := db.QueryRow(ctx, `SELECT 1 FROM internal_squads WHERE uuid = $1`, squadUUID).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrInternalSquadNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrRemoveUsersFromInternalSquadFailed.WithCause(err), cfg)
		return
	}

	var req struct {
		UserIDs []int64 `json:"userIds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.UserIDs) == 0 || len(req.UserIDs) > 1000 {
		shared.SendError(w, http.StatusBadRequest, "invalid request body (userIds array of 1..1000 required)", nil, cfg)
		return
	}

	_, err := db.Exec(ctx, `
		DELETE FROM internal_squad_members
		WHERE internal_squad_uuid = $1 AND user_id = ANY($2::bigint[])
	`, squadUUID, req.UserIDs)
	if err != nil {
		shared.SendAPIError(w, shared.ErrRemoveUsersFromInternalSquadFailed.WithCause(err), cfg)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func handleGetInternalSquadUsage(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, squadUUID string) {
	q := r.URL.Query()
	limit := 250
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed >= 1 {
			limit = parsed
		}
	}
	var cursor int64 = 0
	if cursorStr := q.Get("cursor"); cursorStr != "" {
		if parsed, err := strconv.ParseInt(cursorStr, 10, 64); err == nil && parsed >= 0 {
			cursor = parsed
		}
	}
	var minTotalBytes int64 = 0
	if minStr := q.Get("minTotalBytes"); minStr != "" {
		if parsed, err := strconv.ParseInt(minStr, 10, 64); err == nil && parsed > 0 {
			minTotalBytes = parsed
		}
	}

	var total int64
	_ = db.QueryRow(r.Context(), `SELECT COUNT(*) FROM internal_squad_members WHERE internal_squad_uuid = $1`, squadUUID).Scan(&total)

	rows, err := db.Query(r.Context(), `
		SELECT u.id, COALESCE(SUM(nuh.total_bytes), 0) AS total_bytes
		FROM internal_squad_members ism
		JOIN users u ON ism.user_id = u.id
		LEFT JOIN internal_squad_inbounds isi ON isi.internal_squad_uuid = ism.internal_squad_uuid
		LEFT JOIN config_profile_inbounds_to_nodes cpin ON cpin.config_profile_inbound_uuid = isi.inbound_uuid
		LEFT JOIN nodes n ON n.uuid = cpin.node_uuid
		LEFT JOIN nodes_user_usage_history nuh ON nuh.user_id = u.id AND nuh.node_id = n.id
		WHERE ism.internal_squad_uuid = $1 AND u.id > $2
		GROUP BY u.id
		HAVING COALESCE(SUM(nuh.total_bytes), 0) >= $3
		ORDER BY u.id ASC
		LIMIT $4
	`, squadUUID, cursor, minTotalBytes, limit)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetInternalSquadByUUIDFailed.WithCause(err), cfg)
		return
	}
	defer rows.Close()

	type squadUserUsageItem struct {
		ID         int64 `json:"id"`
		TotalBytes int64 `json:"totalBytes"`
	}

	users := make([]squadUserUsageItem, 0)
	var lastID int64 = 0
	for rows.Next() {
		var item squadUserUsageItem
		if scanErr := rows.Scan(&item.ID, &item.TotalBytes); scanErr != nil {
			shared.SendAPIError(w, shared.ErrGetInternalSquadByUUIDFailed.WithCause(scanErr), cfg)
			return
		}
		users = append(users, item)
		lastID = item.ID
	}
	if err := rows.Err(); err != nil {
		shared.SendAPIError(w, shared.ErrGetInternalSquadByUUIDFailed.WithCause(err), cfg)
		return
	}

	var nextCursor *string
	hasMore := len(users) == limit
	if hasMore {
		cStr := fmt.Sprintf("%d", lastID)
		nextCursor = &cStr
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"squadUuid":  squadUUID,
			"users":      users,
			"nextCursor": nextCursor,
			"hasMore":    hasMore,
			"total":      total,
		},
	})
}

// BandwidthStatsInternalSquadsHandler godoc
// @Summary      Internal squads bandwidth statistics
// @Description  Get user usage within internal squad, or user daily usage breakdown by squad nodes
// @Tags         Internal Squads Controller
// @Produce      json
// @Security     BearerAuth
// @Param        uuid    path      string  true   "Internal Squad UUID" format(uuid)
// @Param        userId  path      int     false  "Numeric User ID"
// @Param        start   query     string  false  "Start date (YYYY-MM-DD)"
// @Param        end     query     string  false  "End date (YYYY-MM-DD)"
// @Success      200     {object}  map[string]any
// @Failure      400     {object}  shared.ErrorResponse
// @Failure      404     {object}  shared.ErrorResponse
// @Failure      500     {object}  shared.ErrorResponse
// @Router       /bandwidth-stats/internal-squads/{uuid}/usage [get]
// @Router       /bandwidth-stats/internal-squads/{uuid}/users/{userId}/usage [get]
func BandwidthStatsInternalSquadsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/bandwidth-stats/internal-squads")
		path = strings.Trim(path, "/")
		parts := strings.Split(path, "/")
		if len(parts) == 0 || parts[0] == "" {
			shared.SendAPIError(w, shared.ErrNotFound, cfg)
			return
		}

		squadUUID := parts[0]
		if len(parts) == 2 && parts[1] == "usage" {
			handleGetInternalSquadUsage(w, r, db, cfg, squadUUID)
			return
		}
		if len(parts) == 4 && parts[1] == "users" && parts[3] == "usage" {
			userID, parseErr := strconv.ParseInt(parts[2], 10, 64)
			if parseErr != nil {
				shared.SendError(w, http.StatusBadRequest, "userId must be numeric", parseErr, cfg)
				return
			}
			handleGetInternalSquadUserUsage(w, r, db, cfg, squadUUID, userID)
			return
		}

		shared.SendAPIError(w, shared.ErrNotFound, cfg)
	}
}

func handleGetInternalSquadUserUsage(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, squadUUID string, userID int64) {
	q := r.URL.Query()
	startStr := q.Get("start")
	endStr := q.Get("end")
	startDate, err := time.Parse("2006-01-02", startStr)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, "start must be a valid date (YYYY-MM-DD)", err, cfg)
		return
	}
	endDate, err := time.Parse("2006-01-02", endStr)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, "end must be a valid date (YYYY-MM-DD)", err, cfg)
		return
	}
	if endDate.Before(startDate) {
		shared.SendError(w, http.StatusBadRequest, "end must not be before start", nil, cfg)
		return
	}

	var squadExists bool
	if err := db.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM internal_squads WHERE uuid = $1)`, squadUUID).Scan(&squadExists); err != nil {
		shared.SendAPIError(w, shared.ErrGetInternalSquadByUUIDFailed.WithCause(err), cfg)
		return
	}
	if !squadExists {
		shared.SendAPIError(w, shared.ErrInternalSquadNotFound, cfg)
		return
	}

	type dayNode struct {
		UUID       string `json:"uuid"`
		TotalBytes int64  `json:"totalBytes"`
	}
	type dayUsage struct {
		Date  string    `json:"date"`
		Nodes []dayNode `json:"nodes"`
	}

	dates := make([]string, 0)
	for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
		dates = append(dates, d.Format("2006-01-02"))
	}

	nodeRows, err := db.Query(r.Context(), `
		SELECT DISTINCT n.id, n.uuid
		FROM internal_squad_inbounds isi
		JOIN config_profile_inbounds_to_nodes cpin ON cpin.config_profile_inbound_uuid = isi.inbound_uuid
		JOIN nodes n ON n.uuid = cpin.node_uuid
		WHERE isi.internal_squad_uuid = $1
	`, squadUUID)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetInternalSquadAccessibleNodesFailed.WithCause(err), cfg)
		return
	}

	nodeUUIDByID := make(map[int64]string)
	nodeIDs := make([]int64, 0)
	for nodeRows.Next() {
		var id int64
		var nodeUUID string
		if scanErr := nodeRows.Scan(&id, &nodeUUID); scanErr != nil {
			nodeRows.Close()
			shared.SendAPIError(w, shared.ErrGetInternalSquadAccessibleNodesFailed.WithCause(scanErr), cfg)
			return
		}
		nodeUUIDByID[id] = nodeUUID
		nodeIDs = append(nodeIDs, id)
	}
	nodeRows.Close()
	if err := nodeRows.Err(); err != nil {
		shared.SendAPIError(w, shared.ErrGetInternalSquadAccessibleNodesFailed.WithCause(err), cfg)
		return
	}

	byDate := make(map[string][]dayNode)
	if len(nodeIDs) > 0 {
		usageRows, err := db.Query(r.Context(), `
			SELECT nuh.created_at, nuh.node_id, COALESCE(SUM(nuh.total_bytes), 0) AS total_bytes
			FROM nodes_user_usage_history nuh
			WHERE nuh.user_id = $1 AND nuh.node_id = ANY($2::bigint[]) AND nuh.created_at >= $3 AND nuh.created_at <= $4
			GROUP BY nuh.created_at, nuh.node_id
		`, userID, nodeIDs, startDate, endDate)
		if err != nil {
			shared.SendAPIError(w, shared.ErrInternalServerError.WithCause(err), cfg)
			return
		}
		for usageRows.Next() {
			var createdAt time.Time
			var nodeID, totalBytes int64
			if scanErr := usageRows.Scan(&createdAt, &nodeID, &totalBytes); scanErr != nil {
				usageRows.Close()
				shared.SendAPIError(w, shared.ErrInternalServerError.WithCause(scanErr), cfg)
				return
			}
			nodeUUID, ok := nodeUUIDByID[nodeID]
			if !ok {
				continue
			}
			dateKey := createdAt.UTC().Format("2006-01-02")
			byDate[dateKey] = append(byDate[dateKey], dayNode{UUID: nodeUUID, TotalBytes: totalBytes})
		}
		if err := usageRows.Err(); err != nil {
			usageRows.Close()
			shared.SendAPIError(w, shared.ErrInternalServerError.WithCause(err), cfg)
			return
		}
		usageRows.Close()
	}

	days := make([]dayUsage, 0, len(dates))
	for _, date := range dates {
		nodes := byDate[date]
		if nodes == nil {
			nodes = []dayNode{}
		}
		days = append(days, dayUsage{Date: date, Nodes: nodes})
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"days": days,
		},
	})
}

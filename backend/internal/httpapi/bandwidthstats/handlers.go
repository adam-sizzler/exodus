package bandwidthstats

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
)

// NodesHandler godoc
// @Summary      Get nodes bandwidth stats
// @Description  Get nodes overall usage, realtime metrics, or user usage breakdown per node
// @Tags         Bandwidth Stats Controller
// @Produce      json
// @Security     BearerAuth
// @Param        start          query     string  false  "Start date (YYYY-MM-DD)"
// @Param        end            query     string  false  "End date (YYYY-MM-DD)"
// @Param        topNodesLimit  query     int     false  "Limit top nodes (default 20)"
// @Param        nodeUuid       path      string  false  "Node UUID for GET /bandwidth-stats/nodes/{nodeUuid}/users" format(uuid)
// @Success      200            {object}  map[string]any
// @Failure      400            {object}  shared.ErrorResponse
// @Failure      500            {object}  shared.ErrorResponse
// @Router       /bandwidth-stats/nodes [get]
// @Router       /bandwidth-stats/nodes/realtime [get]
// @Router       /bandwidth-stats/nodes/{nodeUuid}/users [get]
// @Router       /bandwidth-stats/nodes/usage [post]
// @Router       /bandwidth-stats/nodes/users [post]
func NodesHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/bandwidth-stats/nodes")
		path = strings.Trim(path, "/")

		if path == "usage" {
			if r.Method != http.MethodPost {
				w.Header().Set("Allow", http.MethodPost)
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			handlePostNodesUsage(w, r, db, cfg)
			return
		}

		if path == "users" {
			if r.Method != http.MethodPost {
				w.Header().Set("Allow", http.MethodPost)
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			handleGetNodesUsersUsage(w, r, db, cfg)
			return
		}

		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		switch {
		case path == "":
			handleGetNodesUsage(w, r, db, cfg)
		case path == "realtime":
			handleGetNodesRealtimeUsage(w, r, db, cfg)
		case strings.HasSuffix(path, "/users"):
			nodeUUID := strings.TrimSuffix(path, "/users")
			handleGetNodeUsersUsage(w, r, db, cfg, nodeUUID)
		default:
			shared.SendAPIError(w, shared.ErrNotFound, cfg)
		}
	}
}

// UsersHandler godoc
// @Summary      Get user bandwidth stats
// @Description  Get daily bandwidth usage and top connected nodes for a specific user ID
// @Tags         Bandwidth Stats Controller
// @Produce      json
// @Security     BearerAuth
// @Param        userId         path      int     true   "Numeric User ID"
// @Param        start          query     string  false  "Start date (YYYY-MM-DD)"
// @Param        end            query     string  false  "End date (YYYY-MM-DD)"
// @Param        topNodesLimit  query     int     false  "Limit top nodes (default 20)"
// @Success      200            {object}  map[string]any
// @Failure      400            {object}  shared.ErrorResponse
// @Failure      404            {object}  shared.ErrorResponse
// @Failure      500            {object}  shared.ErrorResponse
// @Router       /bandwidth-stats/users/{userId} [get]
func UsersHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/api/bandwidth-stats/users")
		path = strings.Trim(path, "/")
		if path == "" {
			shared.SendAPIError(w, shared.ErrNotFound, cfg)
			return
		}

		userID, parseErr := strconv.ParseInt(path, 10, 64)
		if parseErr != nil {
			shared.SendError(w, http.StatusBadRequest, "userId must be numeric", parseErr, cfg)
			return
		}

		handleGetUserUsage(w, r, db, cfg, userID)
	}
}

func handleGetNodesRealtimeUsage(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	rows, err := db.Query(r.Context(), `
WITH nodes_latest_updates AS (
	SELECT
		node_uuid,
		SUM(download_bytes) AS current_download_bytes,
		SUM(upload_bytes) AS current_upload_bytes,
		SUM(total_bytes) AS current_total_bytes,
		MAX(updated_at) AS latest_update_time
	FROM nodes_usage_history
	WHERE created_at = date_trunc('hour', NOW())
	GROUP BY node_uuid
)
SELECT
	n.uuid AS node_uuid,
	n.name AS node_name,
	n.country_code,
	l.current_download_bytes,
	l.current_upload_bytes,
	l.current_total_bytes,
	COALESCE(CAST(l.current_download_bytes / NULLIF(EXTRACT(EPOCH FROM (l.latest_update_time - date_trunc('hour', l.latest_update_time))), 0) AS BIGINT), 0) AS download_speed_bps,
	COALESCE(CAST(l.current_upload_bytes / NULLIF(EXTRACT(EPOCH FROM (l.latest_update_time - date_trunc('hour', l.latest_update_time))), 0) AS BIGINT), 0) AS upload_speed_bps,
	COALESCE(CAST(l.current_total_bytes / NULLIF(EXTRACT(EPOCH FROM (l.latest_update_time - date_trunc('hour', l.latest_update_time))), 0) AS BIGINT), 0) AS total_speed_bps
FROM nodes_latest_updates l
JOIN nodes n ON n.uuid = l.node_uuid
ORDER BY total_speed_bps DESC
	`)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetNodesRealtimeUsageFailed.WithCause(err), cfg)
		return
	}
	defer rows.Close()

	items := make([]nodeRealtimeUsage, 0)
	for rows.Next() {
		var it nodeRealtimeUsage
		if scanErr := rows.Scan(
			&it.NodeUUID, &it.NodeName, &it.CountryCode,
			&it.DownloadBytes, &it.UploadBytes, &it.TotalBytes,
			&it.DownloadSpeedBps, &it.UploadSpeedBps, &it.TotalSpeedBps,
		); scanErr != nil {
			shared.SendAPIError(w, shared.ErrGetNodesRealtimeUsageFailed.WithCause(scanErr), cfg)
			return
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		shared.SendAPIError(w, shared.ErrGetNodesRealtimeUsageFailed.WithCause(err), cfg)
		return
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{"response": items})
}

func handleGetNodesUsage(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	startDate, endDate, dates, ok := parseDateRange(w, r)
	if !ok {
		return
	}
	topLimit := parsePositiveIntWithDefault(r.URL.Query().Get("topNodesLimit"), 20)

	batch := &pgx.Batch{}
	batch.Queue(`
WITH daily_traffic AS (
	SELECT DATE_TRUNC('day', created_at AT TIME ZONE 'UTC')::date AS date, SUM(total_bytes) AS bytes
	FROM nodes_usage_history
	WHERE created_at >= $1 AND created_at <= $2
	GROUP BY DATE_TRUNC('day', created_at AT TIME ZONE 'UTC')
)
SELECT COALESCE(dt.bytes, 0) AS value
FROM unnest($3::date[]) WITH ORDINALITY AS d(date, ord)
LEFT JOIN daily_traffic dt ON dt.date = d.date
ORDER BY d.ord
	`, startDate, endDate, pgDateArrayLiteral(dates))

	batch.Queue(`
WITH daily_usage AS (
	SELECT
		n.uuid, n.name, n.country_code,
		DATE_TRUNC('day', h.created_at)::date AS date,
		SUM(h.total_bytes) AS bytes
	FROM nodes n
	INNER JOIN nodes_usage_history h ON h.node_uuid = n.uuid
	WHERE h.created_at >= $1 AND h.created_at <= $2
	GROUP BY n.uuid, n.name, n.country_code, DATE_TRUNC('day', h.created_at)
),
nodes_with_totals AS (
	SELECT uuid, name, country_code, SUM(bytes) AS total_bytes
	FROM daily_usage
	GROUP BY uuid, name, country_code
)
SELECT
	nt.uuid, nt.name, nt.country_code, nt.total_bytes,
	ARRAY_AGG(COALESCE(du.bytes, 0) ORDER BY d.ord) AS data
FROM nodes_with_totals nt
CROSS JOIN unnest($3::date[]) WITH ORDINALITY AS d(date, ord)
LEFT JOIN daily_usage du ON du.uuid = nt.uuid AND du.date = d.date
GROUP BY nt.uuid, nt.name, nt.country_code, nt.total_bytes
ORDER BY nt.total_bytes DESC
	`, startDate, endDate, pgDateArrayLiteral(dates))

	batch.Queue(`
SELECT n.uuid, n.name, n.country_code, COALESCE(SUM(h.total_bytes), 0) AS total
FROM nodes n
INNER JOIN nodes_usage_history h ON h.node_uuid = n.uuid
WHERE h.created_at >= $1 AND h.created_at <= $2
GROUP BY n.uuid, n.name, n.country_code
ORDER BY total DESC
LIMIT $3
	`, startDate, endDate, topLimit)

	br := db.SendBatch(r.Context(), batch)
	defer br.Close()

	sparkRows, err := br.Query()
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetNodesSparklineFailed.WithCause(err), cfg)
		return
	}
	sparkline := make([]int64, 0, len(dates))
	for sparkRows.Next() {
		var v int64
		if scanErr := sparkRows.Scan(&v); scanErr != nil {
			sparkRows.Close()
			shared.SendAPIError(w, shared.ErrGetNodesSparklineFailed.WithCause(scanErr), cfg)
			return
		}
		sparkline = append(sparkline, v)
	}
	if err := sparkRows.Err(); err != nil {
		sparkRows.Close()
		shared.SendAPIError(w, shared.ErrGetNodesSparklineFailed.WithCause(err), cfg)
		return
	}
	sparkRows.Close()

	seriesRows, err := br.Query()
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetNodesUsageFailed.WithCause(err), cfg)
		return
	}
	series := make([]usageSeries, 0)
	for seriesRows.Next() {
		var s usageSeries
		if scanErr := seriesRows.Scan(&s.UUID, &s.Name, &s.CountryCode, &s.Total, &s.Data); scanErr != nil {
			seriesRows.Close()
			shared.SendAPIError(w, shared.ErrGetNodesUsageFailed.WithCause(scanErr), cfg)
			return
		}
		s.Color = colorFromUUID(s.UUID)
		series = append(series, s)
	}
	if err := seriesRows.Err(); err != nil {
		seriesRows.Close()
		shared.SendAPIError(w, shared.ErrGetNodesUsageFailed.WithCause(err), cfg)
		return
	}
	seriesRows.Close()

	topRows, err := br.Query()
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetTopNodesFailed.WithCause(err), cfg)
		return
	}
	topNodes := make([]topNode, 0)
	for topRows.Next() {
		var t topNode
		if scanErr := topRows.Scan(&t.UUID, &t.Name, &t.CountryCode, &t.Total); scanErr != nil {
			topRows.Close()
			shared.SendAPIError(w, shared.ErrGetTopNodesFailed.WithCause(scanErr), cfg)
			return
		}
		t.Color = colorFromUUID(t.UUID)
		topNodes = append(topNodes, t)
	}
	if err := topRows.Err(); err != nil {
		topRows.Close()
		shared.SendAPIError(w, shared.ErrGetTopNodesFailed.WithCause(err), cfg)
		return
	}
	topRows.Close()

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"categories":    dates,
			"sparklineData": sparkline,
			"topNodes":      topNodes,
			"series":        series,
		},
	})
}

func handleGetNodeUsersUsage(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, nodeUUID string) {
	startDate, endDate, dates, ok := parseDateRange(w, r)
	if !ok {
		return
	}
	topLimit := parsePositiveIntWithDefault(r.URL.Query().Get("topUsersLimit"), 100)

	var nodeID int64
	err := db.QueryRow(r.Context(), `SELECT id FROM nodes WHERE uuid = $1`, nodeUUID).Scan(&nodeID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrNodeNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrGetOneNodeFailed.WithCause(err), cfg)
		return
	}

	batch := &pgx.Batch{}
	batch.Queue(`
WITH daily_traffic AS (
	SELECT created_at::date AS date, SUM(total_bytes) AS bytes
	FROM nodes_user_usage_history
	WHERE node_id = $1 AND created_at >= $2::date AND created_at <= $3::date
	GROUP BY created_at
)
SELECT COALESCE(dt.bytes, 0) AS value
FROM unnest($4::date[]) WITH ORDINALITY AS d(date, ord)
LEFT JOIN daily_traffic dt ON dt.date = d.date::date
ORDER BY d.ord
	`, nodeID, startDate, endDate, pgDateArrayLiteral(dates))

	batch.Queue(`
SELECT u.uuid, u.username, COALESCE(SUM(nuh.total_bytes), 0) AS total
FROM users u
INNER JOIN nodes_user_usage_history nuh ON nuh.user_id = u.id
WHERE nuh.node_id = $1 AND nuh.created_at >= $2 AND nuh.created_at <= $3
GROUP BY u.uuid, u.username
ORDER BY total DESC
LIMIT $4
	`, nodeID, startDate, endDate, topLimit)

	br := db.SendBatch(r.Context(), batch)
	defer br.Close()

	sparkRows, err := br.Query()
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetNodeUsersSparklineFailed.WithCause(err), cfg)
		return
	}
	sparkline := make([]int64, 0, len(dates))
	for sparkRows.Next() {
		var v int64
		if scanErr := sparkRows.Scan(&v); scanErr != nil {
			sparkRows.Close()
			shared.SendAPIError(w, shared.ErrGetNodeUsersSparklineFailed.WithCause(scanErr), cfg)
			return
		}
		sparkline = append(sparkline, v)
	}
	if err := sparkRows.Err(); err != nil {
		sparkRows.Close()
		shared.SendAPIError(w, shared.ErrGetNodeUsersSparklineFailed.WithCause(err), cfg)
		return
	}
	sparkRows.Close()

	topRows, err := br.Query()
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetTopUsersFailed.WithCause(err), cfg)
		return
	}
	topUsers := make([]topUser, 0)
	for topRows.Next() {
		var userUUID, username string
		var total int64
		if scanErr := topRows.Scan(&userUUID, &username, &total); scanErr != nil {
			topRows.Close()
			shared.SendAPIError(w, shared.ErrGetTopUsersFailed.WithCause(scanErr), cfg)
			return
		}
		topUsers = append(topUsers, topUser{
			Color:    colorFromUUID(userUUID),
			Username: username,
			Total:    total,
		})
	}
	if err := topRows.Err(); err != nil {
		topRows.Close()
		shared.SendAPIError(w, shared.ErrGetTopUsersFailed.WithCause(err), cfg)
		return
	}
	topRows.Close()

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"categories":    dates,
			"sparklineData": sparkline,
			"topUsers":      topUsers,
		},
	})
}

func handleGetNodesUsersUsage(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	startDate, endDate, dates, ok := parseDateRange(w, r)
	if !ok {
		return
	}
	topLimit := parsePositiveIntWithDefault(r.URL.Query().Get("topUsersLimit"), 100)

	var req getNodesUsersUsageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if len(req.NodesUUIDs) == 0 {
		shared.WriteJSONError(w, http.StatusBadRequest, "nodesUuids: must be at least 1 node UUID")
		return
	}

	nodeRows, err := db.Query(r.Context(), `SELECT id FROM nodes WHERE uuid = ANY($1)`, req.NodesUUIDs)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetAllNodesFailed.WithCause(err), cfg)
		return
	}
	defer nodeRows.Close()

	var nodeIDs []int64
	for nodeRows.Next() {
		var id int64
		if scanErr := nodeRows.Scan(&id); scanErr != nil {
			shared.SendAPIError(w, shared.ErrGetAllNodesFailed.WithCause(scanErr), cfg)
			return
		}
		nodeIDs = append(nodeIDs, id)
	}
	if err := nodeRows.Err(); err != nil {
		shared.SendAPIError(w, shared.ErrGetAllNodesFailed.WithCause(err), cfg)
		return
	}
	if len(nodeIDs) == 0 {
		shared.SendAPIError(w, shared.ErrNodeNotFound, cfg)
		return
	}

	batch := &pgx.Batch{}
	batch.Queue(`
WITH daily_traffic AS (
	SELECT created_at::date AS date, SUM(total_bytes) AS bytes
	FROM nodes_user_usage_history
	WHERE node_id = ANY($1) AND created_at >= $2::date AND created_at <= $3::date
	GROUP BY created_at
)
SELECT COALESCE(dt.bytes, 0) AS value
FROM unnest($4::date[]) WITH ORDINALITY AS d(date, ord)
LEFT JOIN daily_traffic dt ON dt.date = d.date::date
ORDER BY d.ord
	`, nodeIDs, startDate, endDate, pgDateArrayLiteral(dates))

	batch.Queue(`
SELECT u.uuid, u.username, COALESCE(SUM(nuh.total_bytes), 0) AS total
FROM users u
INNER JOIN nodes_user_usage_history nuh ON nuh.user_id = u.id
WHERE nuh.node_id = ANY($1) AND nuh.created_at >= $2 AND nuh.created_at <= $3
GROUP BY u.uuid, u.username
ORDER BY total DESC
LIMIT $4
	`, nodeIDs, startDate, endDate, topLimit)

	br := db.SendBatch(r.Context(), batch)
	defer br.Close()

	sparkRows, err := br.Query()
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetNodeUsersSparklineFailed.WithCause(err), cfg)
		return
	}
	sparkline := make([]int64, 0, len(dates))
	for sparkRows.Next() {
		var v int64
		if scanErr := sparkRows.Scan(&v); scanErr != nil {
			sparkRows.Close()
			shared.SendAPIError(w, shared.ErrGetNodeUsersSparklineFailed.WithCause(scanErr), cfg)
			return
		}
		sparkline = append(sparkline, v)
	}
	if err := sparkRows.Err(); err != nil {
		sparkRows.Close()
		shared.SendAPIError(w, shared.ErrGetNodeUsersSparklineFailed.WithCause(err), cfg)
		return
	}
	sparkRows.Close()

	topRows, err := br.Query()
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetTopUsersFailed.WithCause(err), cfg)
		return
	}
	topUsers := make([]topUser, 0)
	for topRows.Next() {
		var userUUID, username string
		var total int64
		if scanErr := topRows.Scan(&userUUID, &username, &total); scanErr != nil {
			topRows.Close()
			shared.SendAPIError(w, shared.ErrGetTopUsersFailed.WithCause(scanErr), cfg)
			return
		}
		topUsers = append(topUsers, topUser{
			Color:    colorFromUUID(userUUID),
			Username: username,
			Total:    total,
		})
	}
	if err := topRows.Err(); err != nil {
		topRows.Close()
		shared.SendAPIError(w, shared.ErrGetTopUsersFailed.WithCause(err), cfg)
		return
	}
	topRows.Close()

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"categories":    dates,
			"sparklineData": sparkline,
			"topUsers":      topUsers,
		},
	})
}

func handleGetUserUsage(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, userID int64) {
	startDate, endDate, dates, ok := parseDateRange(w, r)
	if !ok {
		return
	}
	topLimit := parsePositiveIntWithDefault(r.URL.Query().Get("topNodesLimit"), 20)

	var userExists bool
	if err := db.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)`, userID).Scan(&userExists); err != nil {
		shared.SendAPIError(w, shared.ErrGetUserStatsFailed.WithCause(err), cfg)
		return
	}
	if !userExists {
		shared.SendAPIError(w, shared.ErrUserNotFound, cfg)
		return
	}

	batch := &pgx.Batch{}
	batch.Queue(`
WITH daily_traffic AS (
	SELECT created_at::date AS date, SUM(total_bytes) AS bytes
	FROM nodes_user_usage_history
	WHERE user_id = $1 AND created_at >= $2::date AND created_at <= $3::date
	GROUP BY created_at
)
SELECT COALESCE(dt.bytes, 0) AS value
FROM unnest($4::date[]) WITH ORDINALITY AS d(date, ord)
LEFT JOIN daily_traffic dt ON dt.date = d.date::date
ORDER BY d.ord
	`, userID, startDate, endDate, pgDateArrayLiteral(dates))

	batch.Queue(`
WITH daily_usage AS (
	SELECT
		n.uuid, n.name, n.country_code,
		nuh.created_at::date AS date,
		SUM(nuh.total_bytes) AS bytes
	FROM nodes n
	INNER JOIN nodes_user_usage_history nuh ON nuh.node_id = n.id
	WHERE nuh.user_id = $1 AND nuh.created_at >= $2::date AND nuh.created_at <= $3::date
	GROUP BY n.uuid, n.name, n.country_code, nuh.created_at
),
nodes_with_totals AS (
	SELECT uuid, name, country_code, SUM(bytes) AS total_bytes
	FROM daily_usage
	GROUP BY uuid, name, country_code
)
SELECT
	nt.uuid, nt.name, nt.country_code, nt.total_bytes,
	ARRAY_AGG(COALESCE(du.bytes, 0) ORDER BY d.ord) AS data
FROM nodes_with_totals nt
CROSS JOIN unnest($4::date[]) WITH ORDINALITY AS d(date, ord)
LEFT JOIN daily_usage du ON du.uuid = nt.uuid AND du.date = d.date::date
GROUP BY nt.uuid, nt.name, nt.country_code, nt.total_bytes
ORDER BY nt.total_bytes DESC
	`, userID, startDate, endDate, pgDateArrayLiteral(dates))

	batch.Queue(`
SELECT n.uuid, n.name, n.country_code, COALESCE(SUM(nuh.total_bytes), 0) AS total
FROM nodes n
INNER JOIN nodes_user_usage_history nuh ON nuh.node_id = n.id
WHERE nuh.user_id = $1 AND nuh.created_at >= $2 AND nuh.created_at <= $3
GROUP BY n.uuid, n.name, n.country_code
ORDER BY total DESC
LIMIT $4
	`, userID, startDate, endDate, topLimit)

	br := db.SendBatch(r.Context(), batch)
	defer br.Close()

	sparkRows, err := br.Query()
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetUserSparklineFailed.WithCause(err), cfg)
		return
	}
	sparkline := make([]int64, 0, len(dates))
	for sparkRows.Next() {
		var v int64
		if scanErr := sparkRows.Scan(&v); scanErr != nil {
			sparkRows.Close()
			shared.SendAPIError(w, shared.ErrGetUserSparklineFailed.WithCause(scanErr), cfg)
			return
		}
		sparkline = append(sparkline, v)
	}
	if err := sparkRows.Err(); err != nil {
		sparkRows.Close()
		shared.SendAPIError(w, shared.ErrGetUserSparklineFailed.WithCause(err), cfg)
		return
	}
	sparkRows.Close()

	seriesRows, err := br.Query()
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetUserNodesSeriesFailed.WithCause(err), cfg)
		return
	}
	series := make([]usageSeries, 0)
	for seriesRows.Next() {
		var s usageSeries
		if scanErr := seriesRows.Scan(&s.UUID, &s.Name, &s.CountryCode, &s.Total, &s.Data); scanErr != nil {
			seriesRows.Close()
			shared.SendAPIError(w, shared.ErrGetUserNodesSeriesFailed.WithCause(scanErr), cfg)
			return
		}
		s.Color = colorFromUUID(s.UUID)
		series = append(series, s)
	}
	if err := seriesRows.Err(); err != nil {
		seriesRows.Close()
		shared.SendAPIError(w, shared.ErrGetUserNodesSeriesFailed.WithCause(err), cfg)
		return
	}
	seriesRows.Close()

	topRows, err := br.Query()
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetUserTopNodesFailed.WithCause(err), cfg)
		return
	}
	topNodes := make([]topNode, 0)
	for topRows.Next() {
		var t topNode
		if scanErr := topRows.Scan(&t.UUID, &t.Name, &t.CountryCode, &t.Total); scanErr != nil {
			topRows.Close()
			shared.SendAPIError(w, shared.ErrGetUserTopNodesFailed.WithCause(scanErr), cfg)
			return
		}
		t.Color = colorFromUUID(t.UUID)
		topNodes = append(topNodes, t)
	}
	if err := topRows.Err(); err != nil {
		topRows.Close()
		shared.SendAPIError(w, shared.ErrGetUserTopNodesFailed.WithCause(err), cfg)
		return
	}
	topRows.Close()

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"categories":    dates,
			"sparklineData": sparkline,
			"topNodes":      topNodes,
			"series":        series,
		},
	})
}

func handlePostNodesUsage(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	startDate, endDate, _, ok := parseDateRange(w, r)
	if !ok {
		return
	}
	minTotalBytesStr := r.URL.Query().Get("minTotalBytes")
	var minTotalBytes int64 = 0
	if minTotalBytesStr != "" {
		if parsed, err := strconv.ParseInt(minTotalBytesStr, 10, 64); err == nil && parsed > 0 {
			minTotalBytes = parsed
		}
	}

	var req struct {
		NodesUUIDs []string `json:"nodesUuids"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	if len(req.NodesUUIDs) == 0 {
		shared.WriteJSON(w, http.StatusOK, map[string]any{"response": map[string]any{"nodes": []any{}}})
		return
	}

	type nodeUsageUser struct {
		ID         int64 `json:"id"`
		TotalBytes int64 `json:"totalBytes"`
	}
	type nodeUsageItem struct {
		UUID  string          `json:"uuid"`
		Users []nodeUsageUser `json:"users"`
	}

	rows, err := db.Query(r.Context(), `
		SELECT n.uuid,
		       nuh.user_id,
		       CASE WHEN COALESCE(SUM(nuh.total_bytes), 0) >= $4 THEN COALESCE(SUM(nuh.total_bytes), 0) ELSE 0 END AS total_bytes,
		       CASE WHEN nuh.user_id IS NOT NULL AND COALESCE(SUM(nuh.total_bytes), 0) >= $4 THEN true ELSE false END AS meets_threshold
		FROM nodes n
		LEFT JOIN nodes_user_usage_history nuh ON nuh.node_id = n.id AND nuh.created_at >= $2 AND nuh.created_at <= $3
		WHERE n.uuid = ANY($1)
		GROUP BY n.uuid, nuh.user_id
	`, req.NodesUUIDs, startDate, endDate, minTotalBytes)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetNodesUsageFailed.WithCause(err), cfg)
		return
	}
	defer rows.Close()

	nodesByUUID := make(map[string]*nodeUsageItem)
	orderedUUIDs := make([]string, 0)
	for rows.Next() {
		var nodeUUID string
		var userID *int64
		var totalBytes int64
		var meetsThreshold bool
		if scanErr := rows.Scan(&nodeUUID, &userID, &totalBytes, &meetsThreshold); scanErr != nil {
			shared.SendAPIError(w, shared.ErrGetNodesUsageFailed.WithCause(scanErr), cfg)
			return
		}
		item, exists := nodesByUUID[nodeUUID]
		if !exists {
			item = &nodeUsageItem{UUID: nodeUUID, Users: make([]nodeUsageUser, 0)}
			nodesByUUID[nodeUUID] = item
			orderedUUIDs = append(orderedUUIDs, nodeUUID)
		}
		if meetsThreshold && userID != nil {
			item.Users = append(item.Users, nodeUsageUser{ID: *userID, TotalBytes: totalBytes})
		}
	}
	if err := rows.Err(); err != nil {
		shared.SendAPIError(w, shared.ErrGetNodesUsageFailed.WithCause(err), cfg)
		return
	}

	nodes := make([]nodeUsageItem, 0, len(orderedUUIDs))
	for _, nodeUUID := range orderedUUIDs {
		nodes = append(nodes, *nodesByUUID[nodeUUID])
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{"response": map[string]any{"nodes": nodes}})
}

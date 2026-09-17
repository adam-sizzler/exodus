package hwiduserdevices

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
	"exodus/internal/httpapi/subscription"
	"exodus/internal/notifications"
)

type hwidCompatDevice struct {
	HWID        string  `json:"hwid"`
	UserID      int64   `json:"userId"`
	Platform    *string `json:"platform"`
	OSVersion   *string `json:"osVersion"`
	DeviceModel *string `json:"deviceModel"`
	UserAgent   *string `json:"userAgent"`
	RequestIP   *string `json:"requestIp"`
	CreatedAt   string  `json:"createdAt"`
	UpdatedAt   string  `json:"updatedAt"`
}

// HWIDDevicesResponseEnvelope wraps list of HWID devices.
type HWIDDevicesResponseEnvelope struct {
	Response struct {
		Devices []hwidCompatDevice `json:"devices"`
		Total   int                `json:"total"`
	} `json:"response"`
}

// HWIDStatsResponseEnvelope wraps HWID statistics response.
type HWIDStatsResponseEnvelope struct {
	Response struct {
		ByPlatform []map[string]any `json:"byPlatform"`
		Stats      struct {
			TotalUniqueDevices        int     `json:"totalUniqueDevices"`
			TotalHwidDevices          int     `json:"totalHwidDevices"`
			AverageHwidDevicesPerUser float64 `json:"averageHwidDevicesPerUser"`
		} `json:"stats"`
	} `json:"response"`
}

// HWIDTopUserItem represents a user with top HWID device count.
type HWIDTopUserItem struct {
	ID           int64  `json:"id"`
	Username     string `json:"username"`
	DevicesCount int    `json:"devicesCount"`
}

// HWIDTopUsersResponseEnvelope wraps top users by HWID devices.
type HWIDTopUsersResponseEnvelope struct {
	Response struct {
		Users []HWIDTopUserItem `json:"users"`
		Total int               `json:"total"`
	} `json:"response"`
}

type deleteAllUserHWIDDevicesRequest struct {
	UserID int64 `json:"userId"`
}

type createUserHWIDDeviceRequest struct {
	HWID        string  `json:"hwid"`
	UserID      int64   `json:"userId"`
	Platform    *string `json:"platform"`
	OSVersion   *string `json:"osVersion"`
	DeviceModel *string `json:"deviceModel"`
	UserAgent   *string `json:"userAgent"`
	RequestIP   *string `json:"requestIp"`
}

type deleteUserHWIDDeviceRequest struct {
	HWID   string `json:"hwid"`
	UserID int64  `json:"userId"`
}

type tableFilter struct {
	ID    string `json:"id"`
	Value any    `json:"value"`
}

type tableSorting struct {
	ID   string `json:"id"`
	Desc bool   `json:"desc"`
}

// HWIDCompatDevicesHandler godoc
// @Summary      Manage HWID devices
// @Description  List, create, or delete HWID devices (GET /hwid/devices, GET /hwid/devices/{userId}, POST /hwid/devices, POST /hwid/devices/delete)
// @Tags         HWID User Devices Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        start    query     int                          false  "Pagination start index"
// @Param        size     query     int                          false  "Page size (1-1000, default 25)"
// @Param        userId   path      int                          false  "Numeric User ID (for GET /hwid/devices/{userId})"
// @Param        body     body      createUserHWIDDeviceRequest  false  "Creation or deletion payload"
// @Success      200      {object}  HWIDDevicesResponseEnvelope
// @Failure      400      {object}  shared.ErrorResponse
// @Failure      404      {object}  shared.ErrorResponse
// @Failure      409      {object}  shared.ErrorResponse
// @Failure      500      {object}  shared.ErrorResponse
// @Router       /hwid/devices [get]
// @Router       /hwid/devices/{userId} [get]
// @Router       /hwid/devices [post]
// @Router       /hwid/devices/delete [post]
func HWIDCompatDevicesHandler(db *pgxpool.Pool, sqlDB *sql.DB, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/hwid/devices"), "/")

		if path == "delete" {
			if r.Method != http.MethodPost {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			handleHWIDCompatDeleteUserDevice(w, r, db, cfg)
			return
		}

		if r.Method == http.MethodPost && path == "" {
			handleHWIDCompatCreateUserDevice(w, r, db, sqlDB, cfg)
			return
		}

		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		if path != "" {
			userID, parseErr := strconv.ParseInt(path, 10, 64)
			if parseErr != nil {
				shared.SendError(w, http.StatusBadRequest, "userId must be numeric", parseErr, cfg)
				return
			}
			handleHWIDCompatGetUserDevices(w, r, db, cfg, userID)
			return
		}

		start, size := parseStartSize(r, 0, 25, 1000)
		devices, total, err := getHWIDCompatDevices(r.Context(), db, start, size, r)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetAllHwidDevicesFailed.WithCause(err), cfg)
			return
		}

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{
				"devices": devices,
				"total":   total,
			},
		})
	}
}

func handleHWIDCompatGetUserDevices(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, userID int64) {
	devices, err := getHWIDCompatDevicesByUserID(r.Context(), db, userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrUserNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrGetUserHwidDevicesFailed.WithCause(err), cfg)
		return
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"devices": devices,
			"total":   len(devices),
		},
	})
}

func handleHWIDCompatCreateUserDevice(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, sqlDB *sql.DB, cfg *config.BackendConfig) {
	var req createUserHWIDDeviceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	hwid := strings.TrimSpace(req.HWID)
	if hwid == "" {
		shared.SendError(w, http.StatusBadRequest, "hwid is required", nil, cfg)
		return
	}
	if req.UserID <= 0 {
		shared.SendError(w, http.StatusBadRequest, "userId is required", nil, cfg)
		return
	}
	userID := req.UserID
	ctx := r.Context()

	var hwidDeviceLimit *int
	var externalSquadUUID *string
	err := db.QueryRow(ctx, `SELECT hwid_device_limit, external_squad_uuid FROM users WHERE id = $1`, userID).
		Scan(&hwidDeviceLimit, &externalSquadUUID)
	if errors.Is(err, pgx.ErrNoRows) {
		shared.SendAPIError(w, shared.ErrUserNotFound, cfg)
		return
	}
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetUserByError.WithCause(err), cfg)
		return
	}

	var deviceExists bool
	if err := db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM hwid_user_devices WHERE user_id = $1 AND hwid = $2)`, userID, hwid).
		Scan(&deviceExists); err != nil {
		shared.SendAPIError(w, shared.ErrCreateUserHwidDeviceFailed.WithCause(err), cfg)
		return
	}
	if deviceExists {
		shared.SendAPIError(w, shared.ErrUserHwidDeviceExists, cfg)
		return
	}

	renderService := subscription.NewRenderService(sqlDB, sqlDB, cfg)
	settings, err := renderService.LoadSubscriptionSettings(ctx)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetSubscriptionSettingsFailed.WithCause(err), cfg)
		return
	}
	if externalSquadUUID != nil {
		overrides, _ := renderService.LoadExternalSquadOverrides(ctx, *externalSquadUUID)
		settings = renderService.MergeSettings(settings, overrides)
	}

	if settings.HwidSettings.Enabled {
		var existingCount int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM hwid_user_devices WHERE user_id = $1`, userID).
			Scan(&existingCount); err != nil {
			shared.SendAPIError(w, shared.ErrCreateUserHwidDeviceFailed.WithCause(err), cfg)
			return
		}
		deviceLimit := settings.HwidSettings.FallbackDeviceLimit
		if hwidDeviceLimit != nil {
			deviceLimit = *hwidDeviceLimit
		}
		if existingCount >= deviceLimit {
			shared.SendAPIError(w, shared.ErrUserHwidDeviceLimitReached, cfg)
			return
		}
	}

	now := time.Now().UTC()
	_, err = db.Exec(ctx, `
		INSERT INTO hwid_user_devices (hwid, user_id, platform, os_version, device_model, user_agent, request_ip, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
		ON CONFLICT (hwid, user_id) DO UPDATE SET
			platform = EXCLUDED.platform,
			os_version = EXCLUDED.os_version,
			device_model = EXCLUDED.device_model,
			user_agent = EXCLUDED.user_agent,
			request_ip = EXCLUDED.request_ip,
			updated_at = EXCLUDED.updated_at
	`, hwid, userID, req.Platform, req.OSVersion, req.DeviceModel, req.UserAgent, req.RequestIP, now)
	if err != nil {
		shared.SendAPIError(w, shared.ErrCreateUserHwidDeviceFailed.WithCause(err), cfg)
		return
	}

	devices, err := getHWIDCompatDevicesByUserID(ctx, db, userID)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetUserHwidDevicesFailed.WithCause(err), cfg)
		return
	}

	var createdDevice hwidCompatDevice
	for _, d := range devices {
		if d.HWID == hwid && d.UserID == userID {
			createdDevice = d
			break
		}
	}
	emitHWIDNotification(ctx, cfg, notifications.EventUserHWIDDeviceAdded, hwidCompatNotificationData(createdDevice), nil)

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"devices": devices,
			"total":   len(devices),
		},
	})
}

func handleHWIDCompatDeleteUserDevice(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req deleteUserHWIDDeviceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	hwid := strings.TrimSpace(req.HWID)
	if hwid == "" {
		shared.SendError(w, http.StatusBadRequest, "hwid is required", nil, cfg)
		return
	}
	if req.UserID <= 0 {
		shared.SendError(w, http.StatusBadRequest, "userId is required", nil, cfg)
		return
	}
	userID := req.UserID

	var userExists bool
	if err := db.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)`, userID).Scan(&userExists); err != nil {
		shared.SendAPIError(w, shared.ErrGetUserByError.WithCause(err), cfg)
		return
	}
	if !userExists {
		shared.SendAPIError(w, shared.ErrUserNotFound, cfg)
		return
	}

	row := db.QueryRow(r.Context(), `
		SELECT h.hwid, h.user_id, h.platform, h.os_version, h.device_model, h.user_agent, h.request_ip, h.created_at, h.updated_at
		FROM hwid_user_devices h
		WHERE h.hwid = $1 AND h.user_id = $2
		LIMIT 1
	`, hwid, userID)
	deletedDevice, err := scanHWIDCompatDevice(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrHwidDeviceNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrDeleteUserHwidDeviceFailed.WithCause(err), cfg)
		return
	}

	result, err := db.Exec(r.Context(), `DELETE FROM hwid_user_devices WHERE hwid = $1 AND user_id = $2`, hwid, userID)
	if err != nil {
		shared.SendAPIError(w, shared.ErrDeleteUserHwidDeviceFailed.WithCause(err), cfg)
		return
	}
	if result.RowsAffected() == 0 {
		shared.SendAPIError(w, shared.ErrHwidDeviceNotFound, cfg)
		return
	}

	devices, err := getHWIDCompatDevicesByUserID(r.Context(), db, userID)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetUserHwidDevicesFailed.WithCause(err), cfg)
		return
	}
	emitHWIDNotification(r.Context(), cfg, notifications.EventUserHWIDDeviceDeleted, hwidCompatNotificationData(deletedDevice), nil)
	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"devices": devices,
			"total":   len(devices),
		},
	})
}

// HWIDCompatDeleteAllUserDevicesHandler godoc
// @Summary      Delete all HWID devices by user ID
// @Description  Delete all registered HWID devices for a specific user ID
// @Tags         HWID User Devices Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      deleteAllUserHWIDDevicesRequest  true  "User ID"
// @Success      200   {object}  HWIDDevicesResponseEnvelope
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      404   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /hwid/devices/delete-all [post]
func HWIDCompatDeleteAllUserDevicesHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var req deleteAllUserHWIDDevicesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
			return
		}
		if req.UserID <= 0 {
			shared.SendError(w, http.StatusBadRequest, "userId is required", nil, cfg)
			return
		}
		userID := req.UserID

		var userExists bool
		if err := db.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)`, userID).Scan(&userExists); err != nil {
			shared.SendAPIError(w, shared.ErrGetUserByError.WithCause(err), cfg)
			return
		}
		if !userExists {
			shared.SendAPIError(w, shared.ErrUserNotFound, cfg)
			return
		}

		result, err := db.Exec(r.Context(), `DELETE FROM hwid_user_devices WHERE user_id = $1`, userID)
		if err != nil {
			shared.SendAPIError(w, shared.ErrDeleteUserHwidDeviceFailed.WithCause(err), cfg)
			return
		}
		deletedCount := result.RowsAffected()

		if deletedCount > 0 {
			emitHWIDNotification(r.Context(), cfg, notifications.EventUserHWIDDeviceDeleted, map[string]any{
				"userId":      userID,
				"deletedAll":  true,
				"deletedRows": deletedCount,
			}, map[string]any{"bulk": true})
		}
		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{
				"devices": []hwidCompatDevice{},
				"total":   0,
			},
		})
	}
}

// HWIDCompatStatsHandler godoc
// @Summary      Get HWID user devices stats
// @Description  Get aggregate HWID device stats broken down by platform and client app
// @Tags         HWID User Devices Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  HWIDStatsResponseEnvelope
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /hwid/devices/stats [get]
func HWIDCompatStatsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var totalDevices, uniqueDevices, uniqueUsers int
		row := db.QueryRow(r.Context(), `
			SELECT COUNT(*), COUNT(DISTINCT hwid), COUNT(DISTINCT user_id)
			FROM hwid_user_devices
		`)
		if err := row.Scan(&totalDevices, &uniqueDevices, &uniqueUsers); err != nil {
			shared.SendAPIError(w, shared.ErrGetHwidStatsFailed.WithCause(err), cfg)
			return
		}

		byPlatform := make([]map[string]any, 0)
		rows, err := db.Query(r.Context(), `
			SELECT COALESCE(NULLIF(platform, ''), 'Unknown') AS platform, COUNT(*) AS count
			FROM hwid_user_devices
			GROUP BY platform
			ORDER BY count DESC
		`)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetHwidStatsFailed.WithCause(err), cfg)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var platform string
			var count int
			if scanErr := rows.Scan(&platform, &count); scanErr != nil {
				shared.SendAPIError(w, shared.ErrGetHwidStatsFailed.WithCause(scanErr), cfg)
				return
			}
			byPlatform = append(byPlatform, map[string]any{"platform": platform, "count": count, "byApp": []map[string]any{}})
		}
		if err := rows.Err(); err != nil {
			shared.SendAPIError(w, shared.ErrGetHwidStatsFailed.WithCause(err), cfg)
			return
		}

		byAppByPlatform := map[string][]map[string]any{}
		rows2, err := db.Query(r.Context(), `
			SELECT
				COALESCE(NULLIF(platform, ''), 'Unknown') AS platform,
				COALESCE(NULLIF(SPLIT_PART(user_agent, '/', 1), ''), 'Unknown') AS app,
				COUNT(*) AS count
			FROM hwid_user_devices
			GROUP BY platform, app
			ORDER BY platform ASC, count DESC
		`)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetHwidStatsFailed.WithCause(err), cfg)
			return
		}
		defer rows2.Close()

		for rows2.Next() {
			var platform string
			var app string
			var count int
			if scanErr := rows2.Scan(&platform, &app, &count); scanErr != nil {
				shared.SendAPIError(w, shared.ErrGetHwidStatsFailed.WithCause(scanErr), cfg)
				return
			}
			if strings.HasPrefix(app, "https:") {
				continue
			}
			byAppByPlatform[platform] = append(byAppByPlatform[platform], map[string]any{"app": app, "count": count})
		}
		if err := rows2.Err(); err != nil {
			shared.SendAPIError(w, shared.ErrGetHwidStatsFailed.WithCause(err), cfg)
			return
		}

		for i := range byPlatform {
			p := byPlatform[i]["platform"].(string)
			if apps, ok := byAppByPlatform[p]; ok {
				byPlatform[i]["byApp"] = apps
			}
		}

		avg := 0.0
		if uniqueUsers > 0 {
			avg = float64(totalDevices) / float64(uniqueUsers)
		}

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{
				"byPlatform": byPlatform,
				"stats": map[string]any{
					"totalUniqueDevices":        uniqueDevices,
					"totalHwidDevices":          totalDevices,
					"averageHwidDevicesPerUser": avg,
				},
			},
		})
	}
}

// HWIDCompatTopUsersHandler godoc
// @Summary      Get top users by HWID devices
// @Description  Get paginated list of users with highest HWID device counts
// @Tags         HWID User Devices Controller
// @Produce      json
// @Security     BearerAuth
// @Param        start  query     int  false  "Pagination start index"
// @Param        size   query     int  false  "Page size (1-100, default 5)"
// @Success      200    {object}  HWIDTopUsersResponseEnvelope
// @Failure      500    {object}  shared.ErrorResponse
// @Router       /hwid/devices/top-users [get]
func HWIDCompatTopUsersHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		start, size := parseStartSize(r, 0, 5, 100)
		type item struct {
			ID           int64  `json:"id"`
			Username     string `json:"username"`
			DevicesCount int    `json:"devicesCount"`
		}
		var total int
		if err := db.QueryRow(r.Context(), `SELECT COUNT(DISTINCT user_id) FROM hwid_user_devices`).Scan(&total); err != nil {
			shared.SendAPIError(w, shared.ErrGetHwidStatsFailed.WithCause(err), cfg)
			return
		}

		rows, err := db.Query(r.Context(), `
			SELECT u.id, u.username, COUNT(*) AS devices_count
			FROM hwid_user_devices h
			JOIN users u ON u.id = h.user_id
			GROUP BY u.id, u.username
			ORDER BY devices_count DESC, u.id ASC
			OFFSET $1 LIMIT $2
		`, start, size)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetHwidStatsFailed.WithCause(err), cfg)
			return
		}
		defer rows.Close()

		users := make([]item, 0)
		for rows.Next() {
			var v item
			if scanErr := rows.Scan(&v.ID, &v.Username, &v.DevicesCount); scanErr != nil {
				shared.SendAPIError(w, shared.ErrGetHwidStatsFailed.WithCause(scanErr), cfg)
				return
			}
			users = append(users, v)
		}
		if err := rows.Err(); err != nil {
			shared.SendAPIError(w, shared.ErrGetHwidStatsFailed.WithCause(err), cfg)
			return
		}

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{
				"users": users,
				"total": total,
			},
		})
	}
}

func getHWIDCompatDevices(ctx context.Context, db *pgxpool.Pool, start, size int, r *http.Request) ([]hwidCompatDevice, int, error) {
	columns := map[string]string{
		"hwid":        "h.hwid",
		"userId":      "u.id",
		"platform":    "h.platform",
		"osVersion":   "h.os_version",
		"deviceModel": "h.device_model",
		"userAgent":   "h.user_agent",
		"requestIp":   "h.request_ip",
		"createdAt":   "h.created_at",
		"updatedAt":   "h.updated_at",
	}
	whereSQL, whereArgs := buildTableWhereClause(r, columns)
	orderSQL := buildTableOrderClause(r, columns, "h.created_at DESC")

	var total int
	countQuery := `SELECT COUNT(*) FROM hwid_user_devices h JOIN users u ON u.id = h.user_id` + whereSQL
	if err := db.QueryRow(ctx, countQuery, whereArgs...).Scan(&total); err != nil {
		return nil, 0, err
	}

	args := append(append([]any{}, whereArgs...), start, size)
	query := fmt.Sprintf(`
		SELECT h.hwid, h.user_id, h.platform, h.os_version, h.device_model, h.user_agent, h.request_ip, h.created_at, h.updated_at
		FROM hwid_user_devices h
		JOIN users u ON u.id = h.user_id
	`+whereSQL+orderSQL+` OFFSET $%d LIMIT $%d`, len(whereArgs)+1, len(whereArgs)+2)

	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items := make([]hwidCompatDevice, 0)
	for rows.Next() {
		item, scanErr := scanHWIDCompatDevice(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func getHWIDCompatDevicesByUserID(ctx context.Context, db *pgxpool.Pool, userID int64) ([]hwidCompatDevice, error) {
	var userExists bool
	if err := db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)`, userID).Scan(&userExists); err != nil {
		return nil, err
	}
	if !userExists {
		return nil, pgx.ErrNoRows
	}

	rows, err := db.Query(ctx, `
		SELECT h.hwid, h.user_id, h.platform, h.os_version, h.device_model, h.user_agent, h.request_ip, h.created_at, h.updated_at
		FROM hwid_user_devices h
		WHERE h.user_id = $1
		ORDER BY h.created_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]hwidCompatDevice, 0)
	for rows.Next() {
		item, scanErr := scanHWIDCompatDevice(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

type hwidCompatScanner interface {
	Scan(dest ...any) error
}

func scanHWIDCompatDevice(scanner hwidCompatScanner) (hwidCompatDevice, error) {
	var item hwidCompatDevice
	var createdAt, updatedAt *time.Time
	if err := scanner.Scan(&item.HWID, &item.UserID, &item.Platform, &item.OSVersion, &item.DeviceModel, &item.UserAgent, &item.RequestIP, &createdAt, &updatedAt); err != nil {
		return item, err
	}
	if createdAt != nil {
		item.CreatedAt = createdAt.UTC().Format("2006-01-02T15:04:05.000Z")
	}
	if updatedAt != nil {
		item.UpdatedAt = updatedAt.UTC().Format("2006-01-02T15:04:05.000Z")
	}
	return item, nil
}

func buildTableWhereClause(r *http.Request, columns map[string]string) (string, []any) {
	filters := parseTableFilters(r.URL.Query().Get("filters"))
	if len(filters) == 0 {
		return "", nil
	}

	parts := make([]string, 0, len(filters))
	args := make([]any, 0, len(filters))
	idx := 1
	for _, filter := range filters {
		column, ok := columns[filter.ID]
		if !ok {
			continue
		}
		value := tableFilterValue(filter.Value)
		if value == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("LOWER(COALESCE(%s::text, '')) LIKE LOWER($%d)", column, idx))
		args = append(args, "%"+value+"%")
		idx++
	}
	if len(parts) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(parts, " AND "), args
}

func buildTableOrderClause(r *http.Request, columns map[string]string, fallback string) string {
	sorting := parseTableSorting(r.URL.Query().Get("sorting"))
	if len(sorting) == 0 {
		return " ORDER BY " + fallback
	}

	parts := make([]string, 0, len(sorting))
	for _, sort := range sorting {
		column, ok := columns[sort.ID]
		if !ok {
			continue
		}
		direction := "ASC"
		if sort.Desc {
			direction = "DESC"
		}
		parts = append(parts, column+" "+direction+" NULLS LAST")
	}
	if len(parts) == 0 {
		return " ORDER BY " + fallback
	}
	return " ORDER BY " + strings.Join(parts, ", ")
}

func parseTableFilters(raw string) []tableFilter {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "[]" {
		return nil
	}
	var filters []tableFilter
	if err := json.Unmarshal([]byte(raw), &filters); err != nil {
		return nil
	}
	return filters
}

func parseTableSorting(raw string) []tableSorting {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "[]" {
		return nil
	}
	var sorting []tableSorting
	if err := json.Unmarshal([]byte(raw), &sorting); err != nil {
		return nil
	}
	return sorting
}

func tableFilterValue(value any) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func parseStartSize(r *http.Request, defaultStart, defaultSize, maxSize int) (int, int) {
	start := defaultStart
	size := defaultSize
	if raw := strings.TrimSpace(r.URL.Query().Get("start")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v >= 0 {
			start = v
		}
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("size")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			size = v
		}
	}
	if size > maxSize {
		size = maxSize
	}
	return start, size
}

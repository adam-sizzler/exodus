package scheduler

import (
	"context"
	"strings"
	"sync"
	"time"

	"exodus/internal/db"
	"exodus/internal/notifications"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	nodeDeployMu       sync.RWMutex
	nodeDeployCallback func(restart bool, nodeUUIDs ...string)
)

// SetNodeDeployCallback registers a callback to trigger node deploy on user state changes.
func SetNodeDeployCallback(cb func(restart bool, nodeUUIDs ...string)) {
	nodeDeployMu.Lock()
	defer nodeDeployMu.Unlock()
	nodeDeployCallback = cb
}

func triggerNodeDeploy(restart bool, nodeUUIDs ...string) {
	nodeDeployMu.RLock()
	cb := nodeDeployCallback
	nodeDeployMu.RUnlock()
	if cb != nil {
		cb(restart, nodeUUIDs...)
	}
}

type StatusUpdateResult struct {
	Users     int64
	NodeUUIDs []string
}

func (s *Scheduler) runExpiredUsersReview(ctx context.Context) error {
	result, users, err := UpdateExpiredUsersWithRecords(ctx, s.db)
	if err != nil {
		s.handleStatusUpdateResult("expired", result, err)
		return err
	}
	if len(users) >= 10000 {
		s.cfg.Logger.Info("More than 10,000 expired users found, skipping webhook/telegram events.")
		if len(result.NodeUUIDs) > 0 {
			triggerNodeDeploy(true, result.NodeUUIDs...)
		} else {
			triggerNodeDeploy(true)
		}
		return nil
	}
	s.handleStatusUpdateResult("expired", result, nil)
	if len(users) > 0 {
		skipTelegram := len(users) >= 500
		for _, user := range users {
			meta := map[string]any{}
			if skipTelegram {
				meta["skipTelegramNotification"] = true
			}
			notifications.Emit(ctx, s.cfg, notifications.Event{
				Scope: notifications.ScopeUser,
				Event: notifications.EventUserExpired,
				Data:  user.notificationData(),
				Meta:  meta,
			})
		}
	}
	return nil
}

func (s *Scheduler) runExceededUsersReview(ctx context.Context) error {
	result, users, err := UpdateExceededTrafficUsersWithRecords(ctx, s.db)
	if err != nil {
		s.handleStatusUpdateResult("limited", result, err)
		return err
	}
	if len(users) >= 10000 {
		s.cfg.Logger.Info("More than 10,000 exceeded traffic usage users found, skipping webhook/telegram events.")
		if len(result.NodeUUIDs) > 0 {
			triggerNodeDeploy(true, result.NodeUUIDs...)
		} else {
			triggerNodeDeploy(true)
		}
		return nil
	}
	s.handleStatusUpdateResult("limited", result, nil)
	if len(users) > 0 {
		skipTelegram := len(users) >= 500
		for _, user := range users {
			meta := map[string]any{}
			if skipTelegram {
				meta["skipTelegramNotification"] = true
			}
			notifications.Emit(ctx, s.cfg, notifications.Event{
				Scope: notifications.ScopeUser,
				Event: notifications.EventUserLimited,
				Data:  user.notificationData(),
				Meta:  meta,
			})
		}
	}
	return nil
}

func (s *Scheduler) handleStatusUpdateResult(label string, result StatusUpdateResult, err error) {
	if err != nil {
		s.cfg.Logger.Error("User status update review failed", "type", label, "error", err)
		return
	}
	if result.Users == 0 {
		s.cfg.Logger.Debug("User status update review: no users affected", "type", label)
		return
	}
	s.cfg.Logger.Info("User status update review completed", "type", label, "users", result.Users, "node_targets", len(result.NodeUUIDs))
	if len(result.NodeUUIDs) > 0 {
		triggerNodeDeploy(true, result.NodeUUIDs...)
	}
}

func UpdateExpiredUsers(ctx context.Context, dbConn db.DBTX) (StatusUpdateResult, error) {
	res, _, err := UpdateExpiredUsersWithRecords(ctx, dbConn)
	return res, err
}

func isNilDB(dbConn db.DBTX) bool {
	if dbConn == nil {
		return true
	}
	switch v := dbConn.(type) {
	case *pgxpool.Pool:
		return v == nil
	case *pgx.Conn:
		return v == nil
	}
	return false
}

func UpdateExpiredUsersWithRecords(ctx context.Context, dbConn db.DBTX) (StatusUpdateResult, []userNotificationRecord, error) {
	result := StatusUpdateResult{NodeUUIDs: []string{}}
	if isNilDB(dbConn) {
		return result, nil, nil
	}

	rows, err := dbConn.Query(ctx, `
		WITH affected_users AS (
			UPDATE users
			SET status = 'EXPIRED', updated_at = CURRENT_TIMESTAMP
			WHERE status IN ('ACTIVE', 'LIMITED')
			  AND expire_at < CURRENT_TIMESTAMP
			RETURNING id, uuid::text, username, short_uuid, status,
			          traffic_limit_bytes, expire_at, last_triggered_threshold, created_at
		),
		affected_nodes AS (
			SELECT DISTINCT cpitn.node_uuid::text AS node_uuid
			FROM affected_users au
			JOIN internal_squad_members ism ON ism.user_id = au.id
			JOIN internal_squad_inbounds isi ON isi.internal_squad_uuid = ism.internal_squad_uuid
			JOIN config_profile_inbounds_to_nodes cpitn ON cpitn.config_profile_inbound_uuid = isi.inbound_uuid
		)
		SELECT
			au.id, au.uuid, au.username, au.short_uuid, au.status,
			au.traffic_limit_bytes, COALESCE(ut.used_traffic_bytes, 0),
			au.expire_at, au.last_triggered_threshold, au.created_at,
			COALESCE((SELECT array_agg(node_uuid) FROM affected_nodes), '{}'::text[]) AS node_uuids
		FROM affected_users au
		LEFT JOIN user_traffic ut ON ut.id = au.id
		ORDER BY au.created_at ASC
	`)
	if err != nil {
		return result, nil, err
	}
	defer rows.Close()

	var users []userNotificationRecord
	var nodeUUIDs []string
	for rows.Next() {
		var u userNotificationRecord
		var rowNodes []string
		if err := rows.Scan(
			&u.ID, &u.UUID, &u.Username, &u.ShortUUID, &u.Status,
			&u.TrafficLimitBytes, &u.UsedTrafficBytes,
			&u.ExpireAt, &u.LastTriggeredThreshold, &u.CreatedAt,
			&rowNodes,
		); err != nil {
			return result, nil, err
		}
		users = append(users, u)
		if len(nodeUUIDs) == 0 && len(rowNodes) > 0 {
			nodeUUIDs = rowNodes
		}
	}
	if err := rows.Err(); err != nil {
		return result, nil, err
	}

	result.Users = int64(len(users))
	if len(nodeUUIDs) > 0 {
		result.NodeUUIDs = dedupeStrings(nodeUUIDs)
	}
	return result, users, nil
}

func UpdateExceededTrafficUsers(ctx context.Context, dbConn db.DBTX) (StatusUpdateResult, error) {
	res, _, err := UpdateExceededTrafficUsersWithRecords(ctx, dbConn)
	return res, err
}

func UpdateExceededTrafficUsersWithRecords(ctx context.Context, dbConn db.DBTX) (StatusUpdateResult, []userNotificationRecord, error) {
	result := StatusUpdateResult{NodeUUIDs: []string{}}
	if isNilDB(dbConn) {
		return result, nil, nil
	}

	rows, err := dbConn.Query(ctx, `
		WITH affected_users AS (
			UPDATE users AS u
			SET status = 'LIMITED', updated_at = CURRENT_TIMESTAMP
			FROM user_traffic AS ut
			WHERE ut.id = u.id
			  AND u.status = 'ACTIVE'
			  AND u.traffic_limit_bytes <> 0
			  AND ut.used_traffic_bytes >= u.traffic_limit_bytes
			RETURNING u.id, u.uuid::text, u.username, u.short_uuid, u.status,
			          u.traffic_limit_bytes, u.expire_at, u.last_triggered_threshold, u.created_at,
			          ut.used_traffic_bytes
		),
		affected_nodes AS (
			SELECT DISTINCT cpitn.node_uuid::text AS node_uuid
			FROM affected_users au
			JOIN internal_squad_members ism ON ism.user_id = au.id
			JOIN internal_squad_inbounds isi ON isi.internal_squad_uuid = ism.internal_squad_uuid
			JOIN config_profile_inbounds_to_nodes cpitn ON cpitn.config_profile_inbound_uuid = isi.inbound_uuid
		)
		SELECT
			id, uuid, username, short_uuid, status,
			traffic_limit_bytes, used_traffic_bytes,
			expire_at, last_triggered_threshold, created_at,
			COALESCE((SELECT array_agg(node_uuid) FROM affected_nodes), '{}'::text[]) AS node_uuids
		FROM affected_users
		ORDER BY created_at ASC
	`)
	if err != nil {
		return result, nil, err
	}
	defer rows.Close()

	var users []userNotificationRecord
	var nodeUUIDs []string
	for rows.Next() {
		var u userNotificationRecord
		var rowNodes []string
		if err := rows.Scan(
			&u.ID, &u.UUID, &u.Username, &u.ShortUUID, &u.Status,
			&u.TrafficLimitBytes, &u.UsedTrafficBytes,
			&u.ExpireAt, &u.LastTriggeredThreshold, &u.CreatedAt,
			&rowNodes,
		); err != nil {
			return result, nil, err
		}
		users = append(users, u)
		if len(nodeUUIDs) == 0 && len(rowNodes) > 0 {
			nodeUUIDs = rowNodes
		}
	}
	if err := rows.Err(); err != nil {
		return result, nil, err
	}

	result.Users = int64(len(users))
	if len(nodeUUIDs) > 0 {
		result.NodeUUIDs = dedupeStrings(nodeUUIDs)
	}
	return result, users, nil
}

func ResetTrafficByStrategy(ctx context.Context, pool *pgxpool.Pool, strategy string) (StatusUpdateResult, error) {
	return ResetTrafficByStrategyAt(ctx, pool, strategy, time.Now())
}

func ResetTrafficByStrategyAt(ctx context.Context, pool *pgxpool.Pool, strategy string, now time.Time) (StatusUpdateResult, error) {
	normalizedStrategy := strings.ToUpper(strings.TrimSpace(strategy))
	result := StatusUpdateResult{NodeUUIDs: []string{}}
	if pool == nil {
		return result, nil
	}

	boundary, ok := resetPeriodBoundary(normalizedStrategy, now)
	if !ok {
		return result, nil
	}

	var nodeUUIDs []string
	err := pool.QueryRow(ctx, `
		WITH affected_users AS (
			UPDATE users
			SET last_traffic_reset_at = CURRENT_TIMESTAMP,
			    last_triggered_threshold = 0,
			    status = CASE WHEN status = 'LIMITED' THEN 'ACTIVE' ELSE status END,
			    updated_at = CURRENT_TIMESTAMP
			WHERE traffic_limit_strategy = $1
			  AND COALESCE(last_traffic_reset_at, created_at) < $2
			  AND (
			      $3 <> 'MONTH_ROLLING'
			      OR (
			          (created_at + interval '1 month')::date <= $4::date
			          AND LEAST(
			              EXTRACT(DAY FROM created_at),
			              EXTRACT(DAY FROM date_trunc('month', $5::timestamp) + interval '1 month - 1 day')
			          ) = EXTRACT(DAY FROM $6::timestamp)
			      )
			  )
			RETURNING id, (status = 'LIMITED') AS was_limited
		),
		reset_traffic AS (
			INSERT INTO user_traffic (id, used_traffic_bytes, lifetime_used_traffic_bytes)
			SELECT id, 0, 0 FROM affected_users
			ON CONFLICT (id)
			DO UPDATE SET used_traffic_bytes = 0
			RETURNING id
		),
		affected_nodes AS (
			SELECT DISTINCT cpitn.node_uuid::text AS node_uuid
			FROM affected_users au
			JOIN internal_squad_members ism ON ism.user_id = au.id
			JOIN internal_squad_inbounds isi ON isi.internal_squad_uuid = ism.internal_squad_uuid
			JOIN config_profile_inbounds_to_nodes cpitn ON cpitn.config_profile_inbound_uuid = isi.inbound_uuid
			WHERE au.was_limited = true
		)
		SELECT
			(SELECT COUNT(*) FROM reset_traffic)::bigint AS users_count,
			COALESCE((SELECT array_agg(node_uuid) FROM affected_nodes), '{}'::text[]) AS node_uuids
	`, normalizedStrategy, boundary, normalizedStrategy, now, now, now).Scan(&result.Users, &nodeUUIDs)
	if err != nil {
		return result, err
	}

	result.NodeUUIDs = dedupeStrings(nodeUUIDs)
	return result, nil
}

func resetPeriodBoundary(strategy string, now time.Time) (time.Time, bool) {
	loc := now.Location()
	dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)

	switch strings.ToUpper(strings.TrimSpace(strategy)) {
	case "DAY":
		return dayStart, true
	case "WEEK":
		daysSinceMonday := (int(now.Weekday()) - int(time.Monday) + 7) % 7
		return dayStart.AddDate(0, 0, -daysSinceMonday), true
	case "MONTH":
		return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, loc), true
	case "MONTH_ROLLING":
		return dayStart, true
	default:
		return time.Time{}, false
	}
}

func updateUsersAndCollectNodes(ctx context.Context, dbConn db.DBTX, query string) (StatusUpdateResult, error) {
	result := StatusUpdateResult{NodeUUIDs: []string{}}
	if dbConn == nil {
		return result, nil
	}

	rows, err := dbConn.Query(ctx, query)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	nodeUUIDs := make([]string, 0)
	for rows.Next() {
		var (
			users    int64
			nodeUUID *string
		)
		if err := rows.Scan(&users, &nodeUUID); err != nil {
			return result, err
		}
		result.Users = users
		if nodeUUID != nil && strings.TrimSpace(*nodeUUID) != "" {
			nodeUUIDs = append(nodeUUIDs, strings.TrimSpace(*nodeUUID))
		}
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	result.NodeUUIDs = dedupeStrings(nodeUUIDs)
	return result, nil
}

func dedupeStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

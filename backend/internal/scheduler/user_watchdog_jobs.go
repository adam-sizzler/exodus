package scheduler

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"

	"exodus/internal/notifications"
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

func (s *Scheduler) handleStatusUpdateResult(statusName string, result StatusUpdateResult, err error) {
	if err != nil {
		s.cfg.Logger.Warn("User status review failed", "status", statusName, "error", err)
		return
	}
	if result.Users == 0 {
		s.cfg.Logger.Debug("User status review found no users", "status", statusName)
		return
	}

	s.cfg.Logger.Info("User status review updated users", "status", statusName, "users", result.Users, "node_targets", len(result.NodeUUIDs))
	if len(result.NodeUUIDs) > 0 {
		triggerNodeDeploy(true, result.NodeUUIDs...)
	}
}

func UpdateExpiredUsers(ctx context.Context, db *sql.DB) (StatusUpdateResult, error) {
	res, _, err := UpdateExpiredUsersWithRecords(ctx, db)
	return res, err
}

func UpdateExpiredUsersWithRecords(ctx context.Context, db *sql.DB) (StatusUpdateResult, []userNotificationRecord, error) {
	result := StatusUpdateResult{NodeUUIDs: []string{}}
	if db == nil {
		return result, nil, nil
	}

	rows, err := db.QueryContext(ctx, `
		WITH affected_users AS (
			UPDATE users
			SET status = 'EXPIRED', updated_at = CURRENT_TIMESTAMP
			WHERE status IN ('ACTIVE', 'LIMITED')
			  AND expire_at < CURRENT_TIMESTAMP
			RETURNING id, uuid::text, username, short_uuid, status,
			          traffic_limit_bytes, expire_at, last_triggered_threshold, created_at
		)
		SELECT
			au.id, au.uuid, au.username, au.short_uuid, au.status,
			au.traffic_limit_bytes, COALESCE(ut.used_traffic_bytes, 0),
			au.expire_at, au.last_triggered_threshold, au.created_at
		FROM affected_users au
		LEFT JOIN user_traffic ut ON ut.id = au.id
		ORDER BY au.created_at ASC
	`)
	if err != nil {
		return result, nil, err
	}
	defer rows.Close()

	var users []userNotificationRecord
	var userIDs []int64
	for rows.Next() {
		var u userNotificationRecord
		if err := rows.Scan(
			&u.ID, &u.UUID, &u.Username, &u.ShortUUID, &u.Status,
			&u.TrafficLimitBytes, &u.UsedTrafficBytes,
			&u.ExpireAt, &u.LastTriggeredThreshold, &u.CreatedAt,
		); err != nil {
			return result, nil, err
		}
		users = append(users, u)
		userIDs = append(userIDs, u.ID)
	}
	if err := rows.Err(); err != nil {
		return result, nil, err
	}
	rows.Close()

	result.Users = int64(len(users))
	if len(userIDs) == 0 {
		return result, users, nil
	}

	nodeRows, err := db.QueryContext(ctx, `
		SELECT DISTINCT cpitn.node_uuid::text AS node_uuid
		FROM internal_squad_members ism
		JOIN internal_squad_inbounds isi ON isi.internal_squad_uuid = ism.internal_squad_uuid
		JOIN config_profile_inbounds_to_nodes cpitn ON cpitn.config_profile_inbound_uuid = isi.inbound_uuid
		WHERE ism.user_id = ANY($1)
	`, userIDs)
	if err != nil {
		return result, users, err
	}
	defer nodeRows.Close()

	for nodeRows.Next() {
		var nodeUUID sql.NullString
		if err := nodeRows.Scan(&nodeUUID); err != nil {
			return result, users, err
		}
		if nodeUUID.Valid && strings.TrimSpace(nodeUUID.String) != "" {
			result.NodeUUIDs = append(result.NodeUUIDs, nodeUUID.String)
		}
	}
	return result, users, nodeRows.Err()
}

func UpdateExceededTrafficUsers(ctx context.Context, db *sql.DB) (StatusUpdateResult, error) {
	res, _, err := UpdateExceededTrafficUsersWithRecords(ctx, db)
	return res, err
}

func UpdateExceededTrafficUsersWithRecords(ctx context.Context, db *sql.DB) (StatusUpdateResult, []userNotificationRecord, error) {
	result := StatusUpdateResult{NodeUUIDs: []string{}}
	if db == nil {
		return result, nil, nil
	}

	rows, err := db.QueryContext(ctx, `
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
		)
		SELECT
			id, uuid, username, short_uuid, status,
			traffic_limit_bytes, used_traffic_bytes,
			expire_at, last_triggered_threshold, created_at
		FROM affected_users
		ORDER BY created_at ASC
	`)
	if err != nil {
		return result, nil, err
	}
	defer rows.Close()

	var users []userNotificationRecord
	var userIDs []int64
	for rows.Next() {
		var u userNotificationRecord
		if err := rows.Scan(
			&u.ID, &u.UUID, &u.Username, &u.ShortUUID, &u.Status,
			&u.TrafficLimitBytes, &u.UsedTrafficBytes,
			&u.ExpireAt, &u.LastTriggeredThreshold, &u.CreatedAt,
		); err != nil {
			return result, nil, err
		}
		users = append(users, u)
		userIDs = append(userIDs, u.ID)
	}
	if err := rows.Err(); err != nil {
		return result, nil, err
	}
	rows.Close()

	result.Users = int64(len(users))
	if len(userIDs) == 0 {
		return result, users, nil
	}

	nodeRows, err := db.QueryContext(ctx, `
		SELECT DISTINCT cpitn.node_uuid::text AS node_uuid
		FROM internal_squad_members ism
		JOIN internal_squad_inbounds isi ON isi.internal_squad_uuid = ism.internal_squad_uuid
		JOIN config_profile_inbounds_to_nodes cpitn ON cpitn.config_profile_inbound_uuid = isi.inbound_uuid
		WHERE ism.user_id = ANY($1)
	`, userIDs)
	if err != nil {
		return result, users, err
	}
	defer nodeRows.Close()

	for nodeRows.Next() {
		var nodeUUID sql.NullString
		if err := nodeRows.Scan(&nodeUUID); err != nil {
			return result, users, err
		}
		if nodeUUID.Valid && strings.TrimSpace(nodeUUID.String) != "" {
			result.NodeUUIDs = append(result.NodeUUIDs, nodeUUID.String)
		}
	}
	return result, users, nodeRows.Err()
}

func ResetTrafficByStrategy(ctx context.Context, db *sql.DB, strategy string) (StatusUpdateResult, error) {
	return ResetTrafficByStrategyAt(ctx, db, strategy, time.Now())
}

func ResetTrafficByStrategyAt(ctx context.Context, db *sql.DB, strategy string, now time.Time) (StatusUpdateResult, error) {
	normalizedStrategy := strings.ToUpper(strings.TrimSpace(strategy))
	result := StatusUpdateResult{NodeUUIDs: []string{}}

	boundary, ok := resetPeriodBoundary(normalizedStrategy, now)
	if !ok {
		return result, nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback() }()

	nodeUUIDs, err := queryLimitedUserNodeUUIDsByStrategyTx(ctx, tx, normalizedStrategy, boundary, now)
	if err != nil {
		return result, err
	}

	err = tx.QueryRowContext(ctx, `
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
				RETURNING id
			),
		reset_traffic AS (
			INSERT INTO user_traffic (id, used_traffic_bytes, lifetime_used_traffic_bytes)
			SELECT id, 0, 0 FROM affected_users
			ON CONFLICT (id)
			DO UPDATE SET used_traffic_bytes = 0
			RETURNING id
		)
		SELECT COUNT(*)::bigint FROM reset_traffic
	`, normalizedStrategy, boundary, normalizedStrategy, now, now, now).Scan(&result.Users)
	if err != nil {
		return result, err
	}

	result.NodeUUIDs = nodeUUIDs
	if err := tx.Commit(); err != nil {
		return result, err
	}
	return result, nil
}

func resetPeriodBoundary(strategy string, now time.Time) (time.Time, bool) {
	local := now.Local()
	dayStart := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, local.Location())

	switch strings.ToUpper(strings.TrimSpace(strategy)) {
	case "DAY":
		return dayStart, true
	case "WEEK":
		daysSinceMonday := (int(local.Weekday()) - int(time.Monday) + 7) % 7
		return dayStart.AddDate(0, 0, -daysSinceMonday), true
	case "MONTH":
		return time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, local.Location()), true
	case "MONTH_ROLLING":
		return dayStart, true
	default:
		return time.Time{}, false
	}
}

func updateUsersAndCollectNodes(ctx context.Context, db *sql.DB, query string) (StatusUpdateResult, error) {
	result := StatusUpdateResult{NodeUUIDs: []string{}}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	nodeUUIDs := make([]string, 0)
	for rows.Next() {
		var (
			users    int64
			nodeUUID sql.NullString
		)
		if err := rows.Scan(&users, &nodeUUID); err != nil {
			return result, err
		}
		result.Users = users
		if nodeUUID.Valid {
			nodeUUIDs = append(nodeUUIDs, strings.TrimSpace(nodeUUID.String))
		}
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	result.NodeUUIDs = dedupeStrings(nodeUUIDs)
	return result, nil
}

func queryLimitedUserNodeUUIDsByStrategyTx(ctx context.Context, tx *sql.Tx, strategy string, boundary time.Time, now time.Time) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT cpitn.node_uuid::text AS node_uuid
		FROM users u
		JOIN internal_squad_members ism ON ism.user_id = u.id
		JOIN internal_squad_inbounds isi ON isi.internal_squad_uuid = ism.internal_squad_uuid
		JOIN config_profile_inbounds_to_nodes cpitn ON cpitn.config_profile_inbound_uuid = isi.inbound_uuid
		WHERE u.status = 'LIMITED'
		  AND u.traffic_limit_strategy = $1
		  AND COALESCE(u.last_traffic_reset_at, u.created_at) < $2
		  AND (
		      $3 <> 'MONTH_ROLLING'
		      OR (
		          (u.created_at + interval '1 month')::date <= $4::date
		          AND LEAST(
		              EXTRACT(DAY FROM u.created_at),
		              EXTRACT(DAY FROM date_trunc('month', $5::timestamp) + interval '1 month - 1 day')
		          ) = EXTRACT(DAY FROM $6::timestamp)
		      )
		  )
	`, strategy, boundary, strategy, now, now, now)
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
		nodeUUIDs = append(nodeUUIDs, nodeUUID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return dedupeStrings(nodeUUIDs), nil
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

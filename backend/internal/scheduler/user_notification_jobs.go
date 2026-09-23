package scheduler

import (
	"context"
	"fmt"
	"sort"
	"time"

	"exodus/internal/notifications"
)

type userNotificationRecord struct {
	ID                     int64
	UUID                   string
	Username               string
	ShortUUID              string
	Status                 string
	TrafficLimitBytes      int64
	UsedTrafficBytes       int64
	ExpireAt               time.Time
	LastTriggeredThreshold int
	CreatedAt              time.Time
}

func (s *Scheduler) findUsersForExpireNotifications(ctx context.Context) error {
	if !s.cfg.Scheduler.NotificationsEnabled || !s.cfg.Scheduler.ExpirationNotificationsEnabled {
		return nil
	}

	if len(s.cfg.Scheduler.ExpirationNotifications) == 0 {
		s.cfg.Logger.Warn("Expiration notifications enabled without valid intervals")
		return nil
	}

	now := time.Now().UTC()
	for _, interval := range s.cfg.Scheduler.ExpirationNotifications {
		target := now.Add(-time.Duration(interval) * time.Hour)
		start := target.Truncate(time.Minute)
		end := start.Add(time.Minute)

		users, err := s.usersByExpireAt(ctx, start, end)
		if err != nil {
			return err
		}
		if len(users) > 0 {
			s.cfg.Logger.Info("Users found for expiration notification", "users", len(users), "expiration_hours", interval)
			skipTelegram := len(users) >= 500
			for _, user := range users {
				meta := map[string]any{"expiration": interval}
				if skipTelegram {
					meta["skipTelegramNotification"] = true
				}
				notifications.Emit(ctx, s.cfg, notifications.Event{
					Scope: notifications.ScopeUser,
					Event: notifications.EventUserExpiration,
					Data:  user.notificationData(),
					Meta:  meta,
				})
			}
		}
	}
	return nil
}

func (s *Scheduler) usersByExpireAt(ctx context.Context, start, end time.Time) ([]userNotificationRecord, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			u.id, u.uuid::text, u.username, u.short_uuid, u.status,
			u.traffic_limit_bytes, COALESCE(ut.used_traffic_bytes, 0),
			u.expire_at, u.last_triggered_threshold, u.created_at
		FROM users u
		LEFT JOIN user_traffic ut ON ut.id = u.id
		WHERE u.expire_at >= $1 AND u.expire_at < $2
		ORDER BY u.created_at ASC
	`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := make([]userNotificationRecord, 0)
	for rows.Next() {
		var user userNotificationRecord
		if scanErr := rows.Scan(
			&user.ID,
			&user.UUID,
			&user.Username,
			&user.ShortUUID,
			&user.Status,
			&user.TrafficLimitBytes,
			&user.UsedTrafficBytes,
			&user.ExpireAt,
			&user.LastTriggeredThreshold,
			&user.CreatedAt,
		); scanErr != nil {
			return nil, scanErr
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

func (s *Scheduler) findUsersForThresholdNotification(ctx context.Context) error {
	if !s.cfg.Scheduler.NotificationsEnabled || !s.cfg.Scheduler.BandwidthUsageNotificationsEnabled {
		return nil
	}

	thresholds := normalizeThresholds(s.cfg.Scheduler.BandwidthUsageNotificationsThreshold)
	if len(thresholds) == 0 {
		s.cfg.Logger.Warn("Bandwidth usage notifications enabled without valid thresholds")
		return nil
	}

	total := 0
	for {
		users, err := s.triggerThresholdNotifications(ctx, thresholds)
		if err != nil {
			return err
		}
		if len(users) == 0 {
			break
		}
		total += len(users)
		skipTelegram := len(users) >= 500
		if skipTelegram {
			notifications.Emit(ctx, s.cfg, notifications.Event{
				Scope: notifications.ScopeErrors,
				Event: notifications.EventBandwidthMaxNotification,
				Data: map[string]any{
					"description":      fmt.Sprintf("Telegram notifications were skipped because one bandwidth threshold batch contains %d users. Webhook delivery is still allowed.", len(users)),
					"batchSize":        len(users),
					"totalProcessed":   total,
					"thresholds":       thresholds,
					"telegramBatchMax": 500,
				},
			})
		}
		for _, user := range users {
			meta := map[string]any{}
			if skipTelegram {
				meta["skipTelegramNotification"] = true
			}
			notifications.Emit(ctx, s.cfg, notifications.Event{
				Scope: notifications.ScopeUser,
				Event: notifications.EventUserBandwidthThreshold,
				Data:  user.notificationData(),
				Meta:  meta,
			})
		}
		if len(users) < 5000 {
			break
		}
	}
	if total > 0 {
		s.cfg.Logger.Info("Users found for bandwidth threshold notification", "users", total, "thresholds", thresholds)
	}
	return nil
}

func (s *Scheduler) triggerThresholdNotifications(ctx context.Context, thresholds []int) ([]userNotificationRecord, error) {
	if len(thresholds) == 0 {
		return nil, nil
	}

	const query = `
		WITH thresholds(pct) AS (SELECT unnest($1::int[])),
		candidates AS (
			SELECT
				u.id,
				MIN(u.created_at) AS created_at_for_order,
				MAX(t.pct)::int AS new_threshold,
				MAX(ut.used_traffic_bytes)::bigint AS used_traffic_bytes
			FROM users u
			INNER JOIN user_traffic ut ON ut.id = u.id
			INNER JOIN thresholds t
				ON u.status = 'ACTIVE'
				AND u.traffic_limit_bytes > 0
				AND ut.used_traffic_bytes >= ((u.traffic_limit_bytes * t.pct::bigint) / 100)
				AND u.last_triggered_threshold < t.pct
			GROUP BY u.id
			ORDER BY created_at_for_order
			LIMIT 5000
		)
		UPDATE users AS u
		SET last_triggered_threshold = c.new_threshold,
		    updated_at = CURRENT_TIMESTAMP
		FROM candidates c
		WHERE u.id = c.id
		RETURNING
			u.id,
			u.uuid::text,
			u.username,
			u.short_uuid,
			u.status,
			u.traffic_limit_bytes,
			c.used_traffic_bytes,
			u.expire_at,
			u.last_triggered_threshold,
			u.created_at
	`

	rows, err := s.db.Query(ctx, query, thresholds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := make([]userNotificationRecord, 0)
	for rows.Next() {
		var user userNotificationRecord
		if scanErr := rows.Scan(
			&user.ID,
			&user.UUID,
			&user.Username,
			&user.ShortUUID,
			&user.Status,
			&user.TrafficLimitBytes,
			&user.UsedTrafficBytes,
			&user.ExpireAt,
			&user.LastTriggeredThreshold,
			&user.CreatedAt,
		); scanErr != nil {
			return nil, scanErr
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

func (s *Scheduler) findNotConnectedUsersNotification(ctx context.Context) error {
	if !s.cfg.Scheduler.NotificationsEnabled || !s.cfg.Scheduler.NotConnectedUsersNotificationsEnabled {
		return nil
	}

	intervals := normalizeNotificationIntervals(s.cfg.Scheduler.NotConnectedUsersNotificationsAfterHours)
	if len(intervals) == 0 {
		s.cfg.Logger.Warn("Not-connected users notifications enabled without valid intervals")
		return nil
	}

	now := time.Now().UTC()
	for _, interval := range intervals {
		target := now.Add(-time.Duration(interval) * time.Hour)
		start := target.Add(-10 * time.Minute)
		end := target

		users, err := s.notConnectedUsers(ctx, start, end)
		if err != nil {
			return err
		}
		if len(users) > 0 {
			s.cfg.Logger.Info("Users found for not-connected notification", "users", len(users), "after_hours", interval)
			skipTelegram := len(users) >= 500
			for _, user := range users {
				meta := map[string]any{"notConnectedAfterHours": interval}
				if skipTelegram {
					meta["skipTelegramNotification"] = true
				}
				notifications.Emit(ctx, s.cfg, notifications.Event{
					Scope: notifications.ScopeUser,
					Event: notifications.EventUserNotConnected,
					Data:  user.notificationData(),
					Meta:  meta,
				})
			}
		}
	}
	return nil
}

func (s *Scheduler) notConnectedUsers(ctx context.Context, start, end time.Time) ([]userNotificationRecord, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			u.id, u.uuid::text, u.username, u.short_uuid, u.status,
			u.traffic_limit_bytes, COALESCE(ut.used_traffic_bytes, 0),
			u.expire_at, u.last_triggered_threshold, u.created_at
		FROM users u
		INNER JOIN user_traffic ut ON ut.id = u.id
		WHERE u.status = 'ACTIVE'
		  AND ut.first_connected_at IS NULL
		  AND ut.online_at IS NULL
		  AND u.created_at >= $1
		  AND u.created_at < $2
		ORDER BY u.created_at ASC
	`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := make([]userNotificationRecord, 0)
	for rows.Next() {
		var user userNotificationRecord
		if scanErr := rows.Scan(
			&user.ID,
			&user.UUID,
			&user.Username,
			&user.ShortUUID,
			&user.Status,
			&user.TrafficLimitBytes,
			&user.UsedTrafficBytes,
			&user.ExpireAt,
			&user.LastTriggeredThreshold,
			&user.CreatedAt,
		); scanErr != nil {
			return nil, scanErr
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

func (u userNotificationRecord) notificationData() map[string]any {
	return map[string]any{
		"id":                     u.ID,
		"uuid":                   u.UUID,
		"username":               u.Username,
		"shortUuid":              u.ShortUUID,
		"status":                 u.Status,
		"trafficLimitBytes":      u.TrafficLimitBytes,
		"usedTrafficBytes":       u.UsedTrafficBytes,
		"expireAt":               u.ExpireAt.UTC().Format(time.RFC3339),
		"lastTriggeredThreshold": u.LastTriggeredThreshold,
		"createdAt":              u.CreatedAt.UTC().Format(time.RFC3339),
		"userTraffic": map[string]any{
			"usedTrafficBytes": u.UsedTrafficBytes,
		},
	}
}

func normalizeThresholds(input []int) []int {
	if len(input) == 0 {
		return nil
	}
	seen := make(map[int]struct{}, len(input))
	result := make([]int, 0, len(input))
	for _, value := range input {
		if value < 25 || value > 95 {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Ints(result)
	return result
}

func normalizeNotificationIntervals(input []int) []int {
	if len(input) == 0 {
		return nil
	}
	seen := make(map[int]struct{}, len(input))
	result := make([]int, 0, len(input))
	for _, value := range input {
		if value <= 0 {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Ints(result)
	return result
}

package scheduler

import (
	"context"
	"strings"
	"time"

	"exodus/internal/notifications"
)

type infraBillingNotificationWindow struct {
	Name  string
	Start time.Time
	End   time.Time
}

type infraBillingNotificationRecord struct {
	NodeName      string
	ProviderName  string
	LoginURL      string
	NextBillingAt time.Time
}

func (s *Scheduler) infraBillingNodesNotifications(ctx context.Context) error {
	if !s.cfg.Scheduler.NotificationsEnabled {
		return nil
	}

	now := time.Now().Local()
	windows := []infraBillingNotificationWindow{
		{Name: notifications.EventInfraBillingIn7Days, Start: startOfDay(now.AddDate(0, 0, 7)), End: endOfDayExclusive(now.AddDate(0, 0, 7))},
		{Name: notifications.EventInfraBillingIn48Hours, Start: startOfDay(now.AddDate(0, 0, 2)), End: endOfDayExclusive(now.AddDate(0, 0, 2))},
		{Name: notifications.EventInfraBillingIn24Hours, Start: startOfDay(now.AddDate(0, 0, 1)), End: endOfDayExclusive(now.AddDate(0, 0, 1))},
		{Name: notifications.EventInfraBillingDueToday, Start: startOfDay(now), End: endOfDayExclusive(now)},
		{Name: notifications.EventInfraBillingOverdue24Hours, Start: startOfDay(now.AddDate(0, 0, -1)), End: endOfDayExclusive(now.AddDate(0, 0, -1))},
		{Name: notifications.EventInfraBillingOverdue48Hours, Start: startOfDay(now.AddDate(0, 0, -2)), End: endOfDayExclusive(now.AddDate(0, 0, -2))},
		{Name: notifications.EventInfraBillingOverdue7Days, Start: startOfDay(now.AddDate(0, 0, -7)), End: endOfDayExclusive(now.AddDate(0, 0, -7))},
	}

	total := 0
	for _, window := range windows {
		items, err := s.getInfraBillingNotifications(ctx, window)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			continue
		}
		total += len(items)
		for _, item := range items {
			s.cfg.Logger.Info(
				"Infra billing notification",
				"event", window.Name,
				"node", item.NodeName,
				"provider", item.ProviderName,
				"login_url", item.LoginURL,
				"next_billing_at", item.NextBillingAt.Format(time.RFC3339),
			)
			notifications.Emit(ctx, s.cfg, notifications.Event{
				Scope: notifications.ScopeCRM,
				Event: window.Name,
				Data: map[string]any{
					"providerName":  item.ProviderName,
					"nodeName":      item.NodeName,
					"loginUrl":      item.LoginURL,
					"nextBillingAt": item.NextBillingAt.UTC().Format(time.RFC3339),
				},
			})
		}
	}
	if total > 0 {
		s.cfg.Logger.Info("Infra billing notification scan completed", "notifications", total)
	}
	return nil
}

func (s *Scheduler) getInfraBillingNotifications(ctx context.Context, window infraBillingNotificationWindow) ([]infraBillingNotificationRecord, error) {
	rows, err := s.db.Query(ctx, `
		SELECT COALESCE(n.name, ibn.name), ip.name, ip.login_url, ibn.next_billing_at
		FROM infra_billing_nodes ibn
		LEFT JOIN nodes n ON n.uuid = ibn.node_uuid
		INNER JOIN infra_providers ip ON ip.uuid = ibn.provider_uuid
		WHERE ibn.next_billing_at >= $1
		  AND ibn.next_billing_at < $2
		ORDER BY ibn.next_billing_at ASC, COALESCE(n.name, ibn.name) ASC
	`, window.Start, window.End)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]infraBillingNotificationRecord, 0)
	for rows.Next() {
		var (
			item     infraBillingNotificationRecord
			loginURL *string
		)
		if scanErr := rows.Scan(&item.NodeName, &item.ProviderName, &loginURL, &item.NextBillingAt); scanErr != nil {
			return nil, scanErr
		}
		if loginURL != nil {
			item.LoginURL = strings.TrimSpace(*loginURL)
		}
		if item.LoginURL == "" {
			item.LoginURL = "https://docs.ex"
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

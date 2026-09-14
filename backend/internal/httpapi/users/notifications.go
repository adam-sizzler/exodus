package users

import (
	"context"
	"strings"
	"time"

	"exodus/internal/config"
	"exodus/internal/notifications"
)

func emitUserNotification(ctx context.Context, repo *UserRepository, cfg *config.BackendConfig, event string, record userRecord, meta map[string]any) {
	data := userRecordNotificationData(ctx, repo, cfg, record)
	if userNotificationNeedsInternalSquads(event) {
		enrichUserNotificationInternalSquads(ctx, repo, record.UUID, data)
	}

	notifications.Emit(ctx, cfg, notifications.Event{
		Scope: notifications.ScopeUser,
		Event: event,
		Data:  data,
		Meta:  meta,
	})
}

func userNotificationNeedsInternalSquads(event string) bool {
	switch event {
	case notifications.EventUserCreated, notifications.EventUserModified, notifications.EventUserRevoked:
		return true
	default:
		return false
	}
}

func enrichUserNotificationInternalSquads(ctx context.Context, repo *UserRepository, userUUID string, data map[string]any) {
	if repo == nil || strings.TrimSpace(userUUID) == "" || data == nil {
		return
	}

	squadsByUser, err := repo.getUsersActiveInternalSquads(ctx, []string{userUUID})
	if err != nil {
		return
	}

	squads := squadsByUser[userUUID]
	squadObjects := make([]map[string]any, 0, len(squads))
	names := make([]string, 0, len(squads))
	for _, squad := range squads {
		name := strings.TrimSpace(squad.Name)
		if name != "" {
			names = append(names, name)
			squadObjects = append(squadObjects, map[string]any{
				"uuid": squad.UUID,
				"name": squad.Name,
			})
		}
	}

	data["activeInternalSquads"] = squadObjects
	data["internalSquads"] = names
}

func enrichUsersNotificationInternalSquads(ctx context.Context, repo *UserRepository, userUUIDs []string, dataByUUID map[string]map[string]any) {
	if repo == nil || len(userUUIDs) == 0 || len(dataByUUID) == 0 {
		return
	}

	squadsByUser, err := repo.getUsersActiveInternalSquads(ctx, userUUIDs)
	if err != nil {
		return
	}

	for userUUID, squads := range squadsByUser {
		data, exists := dataByUUID[userUUID]
		if !exists || data == nil {
			continue
		}
		squadObjects := make([]map[string]any, 0, len(squads))
		names := make([]string, 0, len(squads))
		for _, squad := range squads {
			name := strings.TrimSpace(squad.Name)
			if name != "" {
				names = append(names, name)
				squadObjects = append(squadObjects, map[string]any{
					"uuid": squad.UUID,
					"name": squad.Name,
				})
			}
		}
		data["activeInternalSquads"] = squadObjects
		data["internalSquads"] = names
	}
}

func emitUsersByUUIDsNotification(ctx context.Context, repo *UserRepository, cfg *config.BackendConfig, event string, userUUIDs []string) {
	clean := dedupeStrings(userUUIDs)
	if len(clean) == 0 {
		return
	}
	if len(clean) >= 10000 {
		if cfg != nil && cfg.Logger != nil {
			cfg.Logger.Info("More than 10,000 users in batch, skipping webhook/telegram events", "count", len(clean), "event", event)
		}
		return
	}
	records, err := repo.getUserRecordsByUUIDs(ctx, clean)
	if err != nil || len(records) == 0 {
		return
	}

	subBase := ""
	if repo != nil {
		subBase = resolveUsersSubscriptionBaseFromNode(ctx, repo.db)
	}

	foundUUIDs := make([]string, 0, len(records))
	dataByUUID := make(map[string]map[string]any, len(records))
	for _, record := range records {
		foundUUIDs = append(foundUUIDs, record.UUID)
		dataByUUID[record.UUID] = userRecordNotificationData(ctx, repo, cfg, record, subBase)
	}

	if userNotificationNeedsInternalSquads(event) {
		enrichUsersNotificationInternalSquads(ctx, repo, foundUUIDs, dataByUUID)
	}

	skipTelegram := len(records) >= 500
	for _, record := range records {
		meta := map[string]any{"bulk": true}
		if skipTelegram {
			meta["skipTelegramNotification"] = true
		}
		notifications.Emit(ctx, cfg, notifications.Event{
			Scope: notifications.ScopeUser,
			Event: event,
			Data:  dataByUUID[record.UUID],
			Meta:  meta,
		})
	}
}

func emitUsersNotificationFromRecords(ctx context.Context, repo *UserRepository, cfg *config.BackendConfig, event string, userUUIDs []string, records map[string]userRecord) {
	clean := dedupeStrings(userUUIDs)
	if len(clean) == 0 || len(records) == 0 {
		return
	}
	if len(records) >= 10000 {
		if cfg != nil && cfg.Logger != nil {
			cfg.Logger.Info("More than 10,000 users in batch, skipping webhook/telegram events", "count", len(records), "event", event)
		}
		return
	}

	subBase := ""
	if repo != nil {
		subBase = resolveUsersSubscriptionBaseFromNode(ctx, repo.db)
	}

	foundUUIDs := make([]string, 0, len(records))
	dataByUUID := make(map[string]map[string]any, len(records))
	for _, userUUID := range clean {
		if record, ok := records[userUUID]; ok {
			foundUUIDs = append(foundUUIDs, record.UUID)
			dataByUUID[record.UUID] = userRecordNotificationData(ctx, repo, cfg, record, subBase)
		}
	}

	if len(foundUUIDs) == 0 {
		return
	}

	if userNotificationNeedsInternalSquads(event) {
		enrichUsersNotificationInternalSquads(ctx, repo, foundUUIDs, dataByUUID)
	}

	skipTelegram := len(foundUUIDs) >= 500
	for _, userUUID := range foundUUIDs {
		meta := map[string]any{"bulk": true}
		if skipTelegram {
			meta["skipTelegramNotification"] = true
		}
		notifications.Emit(ctx, cfg, notifications.Event{
			Scope: notifications.ScopeUser,
			Event: event,
			Data:  dataByUUID[userUUID],
			Meta:  meta,
		})
	}
}

func userRecordNotificationData(ctx context.Context, repo *UserRepository, cfg *config.BackendConfig, record userRecord, optSubBase ...string) map[string]any {
	subBase := ""
	if len(optSubBase) > 0 && optSubBase[0] != "" {
		subBase = optSubBase[0]
	} else if repo != nil {
		subBase = resolveUsersSubscriptionBaseFromNode(ctx, repo.db)
	}
	if subBase == "" {
		subBase = "https://localhost/"
	}
	subscriptionURL := strings.TrimRight(subBase, "/") + "/" + record.ShortUUID

	return map[string]any{
		"id":                     record.ID,
		"uuid":                   record.UUID,
		"shortUuid":              record.ShortUUID,
		"username":               record.Username,
		"status":                 record.Status,
		"trafficLimitBytes":      record.TrafficLimitBytes,
		"trafficLimitStrategy":   record.TrafficLimitStrategy,
		"expireAt":               record.ExpireAt.UTC().Format(time.RFC3339),
		"telegramId":             record.TelegramID,
		"email":                  record.Email,
		"description":            record.Description,
		"tag":                    record.Tag,
		"hwidDeviceLimit":        record.HwidDeviceLimit,
		"externalSquadUuid":      record.ExternalSquadUUID,
		"trojanPassword":         record.TrojanPassword,
		"vlessUuid":              record.VlessUUID,
		"ssPassword":             record.SSPassword,
		"naivePassword":          protocolCredentialString(record.NaivePassword, ""),
		"shadowtlsPassword":      protocolCredentialString(record.ShadowtlsPassword, ""),
		"hysteria2Password":      protocolCredentialString(record.Hysteria2Password, ""),
		"anytlsPassword":         protocolCredentialString(record.AnytlsPassword, ""),
		"lastTriggeredThreshold": record.LastTriggeredThreshold,
		"subRevokedAt":           optionalTimeString(record.SubRevokedAt),
		"lastTrafficResetAt":     optionalTimeString(record.LastTrafficResetAt),
		"createdAt":              record.CreatedAt.UTC().Format(time.RFC3339),
		"updatedAt":              record.UpdatedAt.UTC().Format(time.RFC3339),
		"subscriptionUrl":        subscriptionURL,
		"activeInternalSquads":   []map[string]any{},
		"userTraffic": map[string]any{
			"usedTrafficBytes":         record.UsedTrafficBytes,
			"lifetimeUsedTrafficBytes": record.LifetimeUsedTrafficBytes,
			"onlineAt":                 optionalTimeString(record.OnlineAt),
			"firstConnectedAt":         optionalTimeString(record.FirstConnectedAt),
			"lastConnectedNodeUuid":    record.LastConnectedNodeUUID,
		},
	}
}

func optionalTimeString(value *time.Time) *string {
	if value == nil {
		return nil
	}
	formatted := value.UTC().Format(time.RFC3339)
	return &formatted
}

func userStatusChangedNotification(previous, next string) string {
	if strings.EqualFold(previous, next) {
		return ""
	}
	switch strings.ToUpper(strings.TrimSpace(next)) {
	case "ACTIVE":
		return notifications.EventUserEnabled
	case "DISABLED":
		return notifications.EventUserDisabled
	case "LIMITED":
		return notifications.EventUserLimited
	case "EXPIRED":
		return notifications.EventUserExpired
	default:
		return ""
	}
}

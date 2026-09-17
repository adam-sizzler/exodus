package users

import (
	"fmt"
	"strings"
	"time"

	"exodus/internal/util"

	"github.com/google/uuid"
)

func validateCreateUserRequest(req createUserRequest) error {
	username := strings.TrimSpace(req.Username)
	if len(username) < 3 || len(username) > 36 {
		return fmt.Errorf("username must be between 3 and 36 characters")
	}
	if !userUsernameRegex.MatchString(username) {
		return fmt.Errorf("username can only contain letters, numbers, underscores and dashes")
	}
	if req.Status != nil && !isValidUserStatus(*req.Status) {
		return fmt.Errorf("invalid status")
	}
	if req.ExpireAt == "" {
		return fmt.Errorf("expireAt is required")
	}
	if _, err := time.Parse(time.RFC3339, req.ExpireAt); err != nil {
		return fmt.Errorf("expireAt must be RFC3339")
	}
	if req.CreatedAt != nil && strings.TrimSpace(*req.CreatedAt) != "" {
		if _, err := time.Parse(time.RFC3339, strings.TrimSpace(*req.CreatedAt)); err != nil {
			return fmt.Errorf("createdAt must be RFC3339")
		}
	}
	if req.LastTrafficResetAt != nil && strings.TrimSpace(*req.LastTrafficResetAt) != "" {
		if _, err := time.Parse(time.RFC3339, strings.TrimSpace(*req.LastTrafficResetAt)); err != nil {
			return fmt.Errorf("lastTrafficResetAt must be RFC3339")
		}
	}
	if req.TrafficLimitBytes != nil && *req.TrafficLimitBytes < 0 {
		return fmt.Errorf("trafficLimitBytes must be non-negative")
	}
	if req.TrafficLimitStrategy != nil && !isValidTrafficStrategy(*req.TrafficLimitStrategy) {
		return fmt.Errorf("invalid trafficLimitStrategy")
	}
	if req.Tag != nil && strings.TrimSpace(*req.Tag) != "" {
		tag := strings.ToUpper(strings.TrimSpace(*req.Tag))
		if len(tag) > 16 || !userTagRegex.MatchString(tag) {
			return fmt.Errorf("tag can only contain uppercase letters, numbers, underscores and be up to 16 chars")
		}
	}
	if req.Email != nil && strings.TrimSpace(*req.Email) != "" && !strings.Contains(strings.TrimSpace(*req.Email), "@") {
		return fmt.Errorf("invalid email")
	}
	if req.HwidDeviceLimit != nil && *req.HwidDeviceLimit < 0 {
		return fmt.Errorf("hwidDeviceLimit must be non-negative")
	}
	if req.TelegramID != nil && *req.TelegramID < 0 {
		return fmt.Errorf("telegramId must be non-negative")
	}
	if err := util.ValidateUUIDsAllowEmpty(req.ActiveInternalSquads); err != nil {
		return err
	}
	if req.VlessUUID != nil && strings.TrimSpace(*req.VlessUUID) != "" {
		if _, err := uuid.Parse(strings.TrimSpace(*req.VlessUUID)); err != nil {
			return fmt.Errorf("invalid vlessUuid")
		}
	}
	for name, value := range map[string]*string{
		"trojanPassword":    req.TrojanPassword,
		"ssPassword":        req.SSPassword,
		"naivePassword":     req.NaivePassword,
		"shadowtlsPassword": req.ShadowtlsPassword,
		"hysteria2Password": req.Hysteria2Password,
		"anytlsPassword":    req.AnytlsPassword,
	} {
		if value != nil && len(strings.TrimSpace(*value)) > 256 {
			return fmt.Errorf("%s must be less than 256 characters", name)
		}
	}
	if req.ExternalSquadUUID != nil && strings.TrimSpace(*req.ExternalSquadUUID) != "" {
		if _, err := uuid.Parse(strings.TrimSpace(*req.ExternalSquadUUID)); err != nil {
			return fmt.Errorf("invalid externalSquadUuid")
		}
	}
	return nil
}

func validateUpdateUserRequest(req updateUserRequest) error {
	if req.ID == nil && req.UUID == nil && req.Username == nil {
		return fmt.Errorf("either id, uuid or username must be provided")
	}
	if req.UUID != nil && strings.TrimSpace(*req.UUID) != "" {
		if _, err := uuid.Parse(strings.TrimSpace(*req.UUID)); err != nil {
			return fmt.Errorf("invalid uuid")
		}
	}
	if req.Status != nil && !isValidUserStatus(*req.Status) {
		return fmt.Errorf("invalid status")
	}
	if req.TrafficLimitBytes != nil && *req.TrafficLimitBytes < 0 {
		return fmt.Errorf("trafficLimitBytes must be non-negative")
	}
	if req.TrafficLimitStrategy != nil && !isValidTrafficStrategy(*req.TrafficLimitStrategy) {
		return fmt.Errorf("invalid trafficLimitStrategy")
	}
	if req.ExpireAt != nil {
		if _, err := time.Parse(time.RFC3339, strings.TrimSpace(*req.ExpireAt)); err != nil {
			return fmt.Errorf("expireAt must be RFC3339")
		}
	}
	if req.Tag.Set && req.Tag.Value != nil && strings.TrimSpace(*req.Tag.Value) != "" {
		tag := strings.ToUpper(strings.TrimSpace(*req.Tag.Value))
		if len(tag) > 16 || !userTagRegex.MatchString(tag) {
			return fmt.Errorf("tag can only contain uppercase letters, numbers, underscores and be up to 16 chars")
		}
	}
	if req.Email.Set && req.Email.Value != nil && strings.TrimSpace(*req.Email.Value) != "" && !strings.Contains(strings.TrimSpace(*req.Email.Value), "@") {
		return fmt.Errorf("invalid email")
	}
	if req.HwidDeviceLimit.Set && req.HwidDeviceLimit.Value != nil && *req.HwidDeviceLimit.Value < 0 {
		return fmt.Errorf("hwidDeviceLimit must be non-negative")
	}
	if req.TelegramID.Set && req.TelegramID.Value != nil && *req.TelegramID.Value < 0 {
		return fmt.Errorf("telegramId must be non-negative")
	}
	if req.ActiveInternalSquads != nil {
		if err := util.ValidateUUIDsAllowEmpty(*req.ActiveInternalSquads); err != nil {
			return err
		}
	}
	if req.VlessUUID.Set {
		if req.VlessUUID.Value == nil || strings.TrimSpace(*req.VlessUUID.Value) == "" {
			return fmt.Errorf("vlessUuid cannot be empty")
		}
		if _, err := uuid.Parse(strings.TrimSpace(*req.VlessUUID.Value)); err != nil {
			return fmt.Errorf("invalid vlessUuid")
		}
	}
	for name, field := range map[string]OptionalString{
		"trojanPassword":    req.TrojanPassword,
		"ssPassword":        req.SSPassword,
		"naivePassword":     req.NaivePassword,
		"shadowtlsPassword": req.ShadowtlsPassword,
		"hysteria2Password": req.Hysteria2Password,
		"anytlsPassword":    req.AnytlsPassword,
	} {
		if !field.Set {
			continue
		}
		if field.Value == nil || strings.TrimSpace(*field.Value) == "" {
			return fmt.Errorf("%s cannot be empty", name)
		}
		if len(strings.TrimSpace(*field.Value)) > 256 {
			return fmt.Errorf("%s must be less than 256 characters", name)
		}
	}
	if req.ExternalSquadUUID.Set && req.ExternalSquadUUID.Value != nil && strings.TrimSpace(*req.ExternalSquadUUID.Value) != "" {
		if _, err := uuid.Parse(strings.TrimSpace(*req.ExternalSquadUUID.Value)); err != nil {
			return fmt.Errorf("invalid externalSquadUuid")
		}
	}
	return nil
}

func validateBulkUpdateUsersFields(fields bulkUpdateUsersFields) error {
	hasUpdate := fields.Status != nil ||
		fields.TrafficLimitBytes != nil ||
		fields.TrafficLimitStrategy != nil ||
		fields.ExpireAt != nil ||
		fields.Description.Set ||
		fields.Tag.Set ||
		fields.TelegramID.Set ||
		fields.Email.Set ||
		fields.HwidDeviceLimit.Set ||
		fields.ExternalSquadUUID.Set
	if !hasUpdate {
		return fmt.Errorf("at least one field must be provided")
	}
	if fields.Status != nil {
		status := strings.ToUpper(strings.TrimSpace(*fields.Status))
		if !isValidUserStatus(status) {
			return fmt.Errorf("invalid status")
		}
		if status == "EXPIRED" || status == "LIMITED" {
			return fmt.Errorf("invalid status")
		}
	}
	if fields.TrafficLimitBytes != nil && *fields.TrafficLimitBytes < 0 {
		return fmt.Errorf("trafficLimitBytes must be non-negative")
	}
	if fields.TrafficLimitStrategy != nil && !isValidTrafficStrategy(*fields.TrafficLimitStrategy) {
		return fmt.Errorf("invalid trafficLimitStrategy")
	}
	if fields.ExpireAt != nil {
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(*fields.ExpireAt))
		if err != nil {
			return fmt.Errorf("expireAt must be RFC3339")
		}
		if !parsed.After(time.Now().UTC()) {
			return fmt.Errorf("expireAt must be in the future")
		}
	}
	if fields.Tag.Set && fields.Tag.Value != nil && strings.TrimSpace(*fields.Tag.Value) != "" {
		tag := strings.ToUpper(strings.TrimSpace(*fields.Tag.Value))
		if len(tag) > 16 || !userTagRegex.MatchString(tag) {
			return fmt.Errorf("tag can only contain uppercase letters, numbers, underscores and be up to 16 chars")
		}
	}
	if fields.Email.Set && fields.Email.Value != nil && strings.TrimSpace(*fields.Email.Value) != "" && !strings.Contains(strings.TrimSpace(*fields.Email.Value), "@") {
		return fmt.Errorf("invalid email")
	}
	if fields.HwidDeviceLimit.Set && fields.HwidDeviceLimit.Value != nil && *fields.HwidDeviceLimit.Value < 0 {
		return fmt.Errorf("hwidDeviceLimit must be non-negative")
	}
	if fields.TelegramID.Set && fields.TelegramID.Value != nil && *fields.TelegramID.Value < 0 {
		return fmt.Errorf("telegramId must be non-negative")
	}
	if fields.ExternalSquadUUID.Set && fields.ExternalSquadUUID.Value != nil && strings.TrimSpace(*fields.ExternalSquadUUID.Value) != "" {
		if _, err := uuid.Parse(strings.TrimSpace(*fields.ExternalSquadUUID.Value)); err != nil {
			return fmt.Errorf("invalid externalSquadUuid")
		}
	}
	return nil
}

func buildBulkUpdateUserClauses(fields bulkUpdateUsersFields) ([]string, []any) {
	clauses := make([]string, 0)
	args := make([]any, 0)
	idx := 1
	add := func(column string, value any) {
		clauses = append(clauses, fmt.Sprintf("%s = $%d", column, idx))
		args = append(args, value)
		idx++
	}

	if fields.Status != nil {
		add("status", strings.ToUpper(strings.TrimSpace(*fields.Status)))
	}
	if fields.TrafficLimitBytes != nil {
		add("traffic_limit_bytes", *fields.TrafficLimitBytes)
	}
	if fields.TrafficLimitStrategy != nil {
		add("traffic_limit_strategy", strings.ToUpper(strings.TrimSpace(*fields.TrafficLimitStrategy)))
	}
	if fields.ExpireAt != nil {
		parsed, _ := time.Parse(time.RFC3339, strings.TrimSpace(*fields.ExpireAt))
		add("expire_at", parsed.UTC())
	}
	if fields.Description.Set {
		if fields.Description.Value == nil || strings.TrimSpace(*fields.Description.Value) == "" {
			clauses = append(clauses, "description = NULL")
		} else {
			add("description", strings.TrimSpace(*fields.Description.Value))
		}
	}
	if fields.Tag.Set {
		if fields.Tag.Value == nil || strings.TrimSpace(*fields.Tag.Value) == "" {
			clauses = append(clauses, "tag = NULL")
		} else {
			add("tag", strings.ToUpper(strings.TrimSpace(*fields.Tag.Value)))
		}
	}
	if fields.TelegramID.Set {
		if fields.TelegramID.Value == nil {
			clauses = append(clauses, "telegram_id = NULL")
		} else {
			add("telegram_id", *fields.TelegramID.Value)
		}
	}
	if fields.Email.Set {
		if fields.Email.Value == nil || strings.TrimSpace(*fields.Email.Value) == "" {
			clauses = append(clauses, "email = NULL")
		} else {
			add("email", strings.TrimSpace(*fields.Email.Value))
		}
	}
	if fields.HwidDeviceLimit.Set {
		if fields.HwidDeviceLimit.Value == nil {
			clauses = append(clauses, "hwid_device_limit = NULL")
		} else {
			add("hwid_device_limit", *fields.HwidDeviceLimit.Value)
		}
	}
	if fields.ExternalSquadUUID.Set {
		if fields.ExternalSquadUUID.Value == nil || strings.TrimSpace(*fields.ExternalSquadUUID.Value) == "" {
			clauses = append(clauses, "external_squad_uuid = NULL")
		} else {
			add("external_squad_uuid", strings.TrimSpace(*fields.ExternalSquadUUID.Value))
		}
	}

	return clauses, args
}

func validateBulkAllUpdateUsersRequest(req bulkAllUpdateUsersRequest) error {
	hasUpdate := req.Status != nil ||
		req.TrafficLimitBytes != nil ||
		req.TrafficLimitStrategy != nil ||
		req.ExpireAt != nil ||
		req.Description.Set ||
		req.Tag.Set ||
		req.TelegramID.Set ||
		req.Email.Set ||
		req.HwidDeviceLimit.Set
	if !hasUpdate {
		return fmt.Errorf("at least one field must be provided")
	}
	if req.Status != nil {
		status := strings.ToUpper(strings.TrimSpace(*req.Status))
		if !isValidUserStatus(status) {
			return fmt.Errorf("invalid status")
		}
		if status == "EXPIRED" || status == "LIMITED" {
			return fmt.Errorf("status EXPIRED and LIMITED are set by the system and cannot be assigned manually")
		}
	}
	if req.TrafficLimitBytes != nil && *req.TrafficLimitBytes < 0 {
		return fmt.Errorf("trafficLimitBytes must be non-negative")
	}
	if req.TrafficLimitStrategy != nil && !isValidTrafficStrategy(*req.TrafficLimitStrategy) {
		return fmt.Errorf("invalid trafficLimitStrategy")
	}
	if req.ExpireAt != nil {
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(*req.ExpireAt))
		if err != nil {
			return fmt.Errorf("expireAt must be RFC3339")
		}
		if !parsed.After(time.Now().UTC()) {
			return fmt.Errorf("expireAt must be in the future")
		}
	}
	if req.Tag.Set && req.Tag.Value != nil && strings.TrimSpace(*req.Tag.Value) != "" {
		tag := strings.ToUpper(strings.TrimSpace(*req.Tag.Value))
		if len(tag) > 16 || !userTagRegex.MatchString(tag) {
			return fmt.Errorf("tag can only contain uppercase letters, numbers, underscores and be up to 16 chars")
		}
	}
	if req.Email.Set && req.Email.Value != nil && strings.TrimSpace(*req.Email.Value) != "" && !strings.Contains(strings.TrimSpace(*req.Email.Value), "@") {
		return fmt.Errorf("invalid email")
	}
	if req.HwidDeviceLimit.Set && req.HwidDeviceLimit.Value != nil && *req.HwidDeviceLimit.Value < 0 {
		return fmt.Errorf("hwidDeviceLimit must be non-negative")
	}
	if req.TelegramID.Set && req.TelegramID.Value != nil && *req.TelegramID.Value < 0 {
		return fmt.Errorf("telegramId must be non-negative")
	}
	return nil
}

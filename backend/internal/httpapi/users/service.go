package users

import (
	"context"
	"errors"
	"strings"
	"time"

	"exodus/internal/config"
	monitor "exodus/internal/nodes"
	"exodus/internal/notifications"
)

type UserService struct {
	repo *UserRepository
	cfg  *config.BackendConfig
}

func NewUserService(repo *UserRepository, cfg *config.BackendConfig) *UserService {
	return &UserService{
		repo: repo,
		cfg:  cfg,
	}
}

func buildUserSyncItem(action string, record userRecord, tags []string) monitor.UserSyncItem {
	hy2Password := ""
	if record.Hysteria2Password != nil {
		hy2Password = *record.Hysteria2Password
	}
	identifier := ""
	if record.ID > 0 {
		identifier = monitor.FormatUserID(record.ID)
	} else {
		identifier = record.Username
	}
	return monitor.UserSyncItem{
		Action:            action,
		Identifier:        identifier,
		Username:          record.Username,
		UUID:              record.VlessUUID,
		TrojanPassword:    record.TrojanPassword,
		SSPassword:        record.SSPassword,
		Hysteria2Password: hy2Password,
		InboundTags:       tags,
	}
}

func (s *UserService) EnableUser(ctx context.Context, identifier string) (userRecord, error) {
	record, nodeUUIDs, err := s.repo.updateUserStatus(ctx, identifier, "ACTIVE")
	if err != nil {
		return userRecord{}, err
	}
	if len(nodeUUIDs) > 0 {
		monitor.RequestSyncUser(buildUserSyncItem("add", record, nil), nodeUUIDs...)
	}
	emitUserNotification(ctx, s.repo, s.cfg, notifications.EventUserEnabled, record, nil)
	return record, nil
}

func (s *UserService) DisableUser(ctx context.Context, identifier string) (userRecord, error) {
	record, nodeUUIDs, err := s.repo.updateUserStatus(ctx, identifier, "DISABLED")
	if err != nil {
		return userRecord{}, err
	}
	if len(nodeUUIDs) > 0 {
		monitor.RequestSyncUser(buildUserSyncItem("disable", record, nil), nodeUUIDs...)
	}
	emitUserNotification(ctx, s.repo, s.cfg, notifications.EventUserDisabled, record, nil)
	return record, nil
}

func (s *UserService) ResetUserTraffic(ctx context.Context, identifier string) (userRecord, error) {
	record, nodeUUIDs, reactivated, err := s.repo.resetUserTraffic(ctx, identifier)
	if err != nil {
		return userRecord{}, err
	}
	if len(nodeUUIDs) > 0 {
		monitor.RequestSyncUser(buildUserSyncItem("add", record, nil), nodeUUIDs...)
	}
	emitUserNotification(ctx, s.repo, s.cfg, notifications.EventUserTrafficReset, record, nil)
	if reactivated {
		emitUserNotification(ctx, s.repo, s.cfg, notifications.EventUserEnabled, record, nil)
	}
	return record, nil
}

func (s *UserService) RevokeUserSubscription(ctx context.Context, userUUID string, req revokeUserSubscriptionRequest) error {
	shortUUIDLength := 16
	if s != nil && s.cfg != nil && s.cfg.Backend.ShortUUIDLength >= 16 {
		shortUUIDLength = s.cfg.Backend.ShortUUIDLength
	}
	shortUUID := generateSubscriptionShortUUID(shortUUIDLength)
	credentials, err := newUserProtocolCredentials(nil, nil, nil, nil, nil, nil, nil)
	if shortUUID == "" || err != nil {
		return err
	}

	nodeUUIDs, err := s.repo.revokeUserSubscription(ctx, userUUID, shortUUID, credentials, req.RevokeOnlyPasswords)
	if err != nil {
		return err
	}

	if len(nodeUUIDs) > 0 {
		monitor.RequestNodeDeploy(true, nodeUUIDs...)
	}
	if record, loadErr := s.repo.getUserRecordByUUID(ctx, userUUID); loadErr == nil {
		emitUserNotification(ctx, s.repo, s.cfg, notifications.EventUserRevoked, record, nil)
	}
	return nil
}

func (s *UserService) CreateUser(ctx context.Context, req createUserRequest) (userRecord, error) {
	userUUID := coalesceUUID(nil)
	shortUUID := coalesceShortUUID(req.ShortUUID, s.cfg)
	credentials, err := newUserProtocolCredentials(
		req.TrojanPassword,
		req.VlessUUID,
		req.SSPassword,
		req.NaivePassword,
		req.ShadowtlsPassword,
		req.Hysteria2Password,
		req.AnytlsPassword,
	)
	if err != nil {
		return userRecord{}, err
	}

	expireAt, _ := time.Parse(time.RFC3339, req.ExpireAt)
	createdAt := time.Now().UTC()
	if req.CreatedAt != nil && strings.TrimSpace(*req.CreatedAt) != "" {
		createdAt, _ = time.Parse(time.RFC3339, strings.TrimSpace(*req.CreatedAt))
	}
	var lastTrafficResetAt any
	if req.LastTrafficResetAt != nil && strings.TrimSpace(*req.LastTrafficResetAt) != "" {
		if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(*req.LastTrafficResetAt)); err == nil {
			lastTrafficResetAt = parsed
		}
	}

	_, internalSquadNodeUUIDs, err := s.repo.createUser(ctx, userUUID, shortUUID, req, credentials, expireAt, createdAt, lastTrafficResetAt)
	if err != nil {
		return userRecord{}, err
	}

	record, err := s.repo.getUserRecordByUUID(ctx, userUUID)
	if err != nil {
		return userRecord{}, err
	}

	if strings.EqualFold(normalizeUserStatus(req.Status), "ACTIVE") && len(internalSquadNodeUUIDs) > 0 {
		monitor.RequestSyncUser(buildUserSyncItem("add", record, nil), internalSquadNodeUUIDs...)
	}
	emitUserNotification(ctx, s.repo, s.cfg, notifications.EventUserCreated, record, nil)
	return record, nil
}

func (s *UserService) UpdateUser(ctx context.Context, req updateUserRequest) (userRecord, error) {
	targetUUID, err := s.repo.resolveUserUUIDForUpdate(ctx, req.ID, req.UUID, req.Username)
	if err != nil {
		return userRecord{}, err
	}

	record, err := s.repo.getUserRecordByUUID(ctx, targetUUID)
	if err != nil {
		return userRecord{}, err
	}

	statusToSet, shouldSetStatus := plannedUserStatusForUpdate(record, req, time.Now().UTC())
	statusDeployRequired := shouldSetStatus && userConfigPresenceChanges(record.Status, statusToSet)
	plannedCredentials := recordProtocolCredentials(record)
	plannedCredentials.TrojanPassword = applyOptionalProtocolCredential(plannedCredentials.TrojanPassword, req.TrojanPassword)
	plannedCredentials.VlessUUID = applyOptionalProtocolCredential(plannedCredentials.VlessUUID, req.VlessUUID)
	plannedCredentials.SSPassword = applyOptionalProtocolCredential(plannedCredentials.SSPassword, req.SSPassword)
	plannedCredentials.NaivePassword = applyOptionalProtocolCredential(plannedCredentials.NaivePassword, req.NaivePassword)
	plannedCredentials.ShadowtlsPassword = applyOptionalProtocolCredential(plannedCredentials.ShadowtlsPassword, req.ShadowtlsPassword)
	plannedCredentials.Hysteria2Password = applyOptionalProtocolCredential(plannedCredentials.Hysteria2Password, req.Hysteria2Password)
	plannedCredentials.AnytlsPassword = applyOptionalProtocolCredential(plannedCredentials.AnytlsPassword, req.AnytlsPassword)
	if err := plannedCredentials.validateUnique(); err != nil {
		return userRecord{}, err
	}

	updatedRecord, statusNodeUUIDs, internalSquadNodeUUIDs, internalSquadsChanged, err := s.repo.updateUserRecord(ctx, targetUUID, record, req, statusToSet, shouldSetStatus, statusDeployRequired)
	if err != nil {
		return userRecord{}, err
	}

	deployNodeUUIDs := dedupeStrings(append(statusNodeUUIDs, internalSquadNodeUUIDs...))
	if internalSquadsChanged && len(deployNodeUUIDs) > 0 {
		monitor.RequestNodeDeploy(true, deployNodeUUIDs...)
	} else if statusDeployRequired && len(deployNodeUUIDs) > 0 {
		action := "add"
		if strings.EqualFold(updatedRecord.Status, "DISABLED") ||
			strings.EqualFold(updatedRecord.Status, "EXPIRED") ||
			strings.EqualFold(updatedRecord.Status, "LIMITED") {
			action = "disable"
		}
		monitor.RequestSyncUser(buildUserSyncItem(action, updatedRecord, nil), deployNodeUUIDs...)
	}
	emitUserNotification(ctx, s.repo, s.cfg, notifications.EventUserModified, updatedRecord, nil)
	if statusChanged := userStatusChangedNotification(record.Status, updatedRecord.Status); statusChanged != "" {
		emitUserNotification(ctx, s.repo, s.cfg, statusChanged, updatedRecord, nil)
	}

	return updatedRecord, nil
}

func (s *UserService) DeleteUser(ctx context.Context, userUUID string) error {
	record, recordErr := s.repo.getUserRecordByUUID(ctx, userUUID)
	if recordErr != nil && !errors.Is(recordErr, errUserNotFound) {
		return recordErr
	}

	internalSquadNodeUUIDs, err := s.repo.deleteUserRecord(ctx, record.UUID)
	if err != nil {
		return err
	}

	if len(internalSquadNodeUUIDs) > 0 {
		monitor.RequestSyncUser(buildUserSyncItem("delete", record, nil), internalSquadNodeUUIDs...)
	}
	if recordErr == nil {
		emitUserNotification(ctx, s.repo, s.cfg, notifications.EventUserDeleted, record, nil)
	}
	return nil
}

func (s *UserService) BulkDeleteUsers(ctx context.Context, targets []string) error {
	notificationRecords, err := s.repo.getUserRecordsByUUIDs(ctx, targets)
	if err != nil {
		return err
	}

	internalSquadNodeUUIDs, err := s.repo.deleteUsersRecord(ctx, targets)
	if err != nil {
		return err
	}

	if len(internalSquadNodeUUIDs) > 0 {
		syncItems := make([]monitor.UserSyncItem, 0, len(notificationRecords))
		for _, rec := range notificationRecords {
			syncItems = append(syncItems, buildUserSyncItem("delete", rec, nil))
		}
		monitor.RequestSyncUsers(syncItems, internalSquadNodeUUIDs...)
	}
	emitUsersNotificationFromRecords(ctx, s.repo, s.cfg, notifications.EventUserDeleted, targets, notificationRecords)
	return nil
}

func (s *UserService) BulkDeleteUsersByStatus(ctx context.Context, status string) (int64, error) {
	affectedRows, internalSquadNodeUUIDs, err := s.repo.deleteUsersByStatus(ctx, status)
	if err != nil {
		return 0, err
	}

	if strings.EqualFold(status, "ACTIVE") && len(internalSquadNodeUUIDs) > 0 {
		monitor.RequestNodeDeploy(true, internalSquadNodeUUIDs...)
	}
	return affectedRows, nil
}

func (s *UserService) BulkResetUsersTraffic(ctx context.Context, uuids []string) (int64, error) {
	cleanUUIDs := dedupeStrings(uuids)
	affectedRows, nodeUUIDs, reactivatedUUIDs, err := s.repo.resetUsersTrafficByUUIDs(ctx, cleanUUIDs)
	if err != nil {
		return 0, err
	}
	if len(nodeUUIDs) > 0 {
		monitor.RequestNodeDeploy(true, nodeUUIDs...)
	}
	emitUsersByUUIDsNotification(ctx, s.repo, s.cfg, notifications.EventUserTrafficReset, cleanUUIDs)
	if len(reactivatedUUIDs) > 0 {
		emitUsersByUUIDsNotification(ctx, s.repo, s.cfg, notifications.EventUserEnabled, reactivatedUUIDs)
	}
	return affectedRows, nil
}

func (s *UserService) BulkAllResetUsersTraffic(ctx context.Context) (int64, error) {
	affectedRows, nodeUUIDs, reactivatedUUIDs, err := s.repo.resetAllUsersTraffic(ctx)
	if err != nil {
		return 0, err
	}
	if len(nodeUUIDs) > 0 {
		monitor.RequestNodeDeploy(true, nodeUUIDs...)
	}
	if len(reactivatedUUIDs) >= 10000 {
		if s.cfg != nil && s.cfg.Logger != nil {
			s.cfg.Logger.Info("More than 10,000 users reactivated after resetAllUserTraffic, skipping webhook/telegram events", "count", len(reactivatedUUIDs))
		}
	} else if len(reactivatedUUIDs) > 0 {
		emitUsersByUUIDsNotification(ctx, s.repo, s.cfg, notifications.EventUserEnabled, reactivatedUUIDs)
	}
	return affectedRows, nil
}

func (s *UserService) ExtendUserExpirationDate(ctx context.Context, identifier string, extendDays int) error {
	affectedRows, nodeUUIDs, err := s.repo.extendUsersExpirationByUUIDs(ctx, []string{identifier}, extendDays)
	if err != nil {
		return err
	}
	if affectedRows == 0 {
		return errUserNotFound
	}
	if len(nodeUUIDs) > 0 {
		monitor.RequestNodeDeploy(true, nodeUUIDs...)
	}
	emitUsersByUUIDsNotification(ctx, s.repo, s.cfg, notifications.EventUserModified, []string{identifier})
	return nil
}

func (s *UserService) BulkExtendUsersExpirationDate(ctx context.Context, uuids []string, extendDays int) (int64, error) {
	affectedRows, nodeUUIDs, err := s.repo.extendUsersExpirationByUUIDs(ctx, uuids, extendDays)
	if err != nil {
		return 0, err
	}
	if len(nodeUUIDs) > 0 {
		monitor.RequestNodeDeploy(true, nodeUUIDs...)
	}
	return affectedRows, nil
}

func (s *UserService) BulkAllExtendUsersExpirationDate(ctx context.Context, extendDays int) (int64, error) {
	affectedRows, nodeUUIDs, err := s.repo.extendAllUsersExpiration(ctx, extendDays)
	if err != nil {
		return 0, err
	}
	if len(nodeUUIDs) > 0 {
		monitor.RequestNodeDeploy(true, nodeUUIDs...)
	}
	return affectedRows, nil
}

func (s *UserService) BulkUpdateUsers(ctx context.Context, uuids []string, fields bulkUpdateUsersFields) (int64, error) {
	clauses, args := buildBulkUpdateUserClauses(fields)
	if len(clauses) == 0 {
		return 0, errors.New("at least one field must be provided")
	}

	cleanUUIDs := dedupeStrings(uuids)
	affectedRows, nodeUUIDs, err := s.repo.bulkUpdateUsers(ctx, cleanUUIDs, clauses, args)
	if err != nil {
		return 0, err
	}

	if affectedRows > 0 {
		if len(nodeUUIDs) > 0 {
			monitor.RequestNodeDeploy(true, nodeUUIDs...)
		}
		emitUsersByUUIDsNotification(ctx, s.repo, s.cfg, notifications.EventUserModified, cleanUUIDs)
	}
	return affectedRows, nil
}

func (s *UserService) BulkUpdateUsersSquads(ctx context.Context, uuids []string, activeInternalSquads []string) (int64, error) {
	cleanUserUUIDs := dedupeStrings(uuids)
	requestedSquads := dedupeStrings(activeInternalSquads)
	affectedRows, nodeUUIDs, err := s.repo.bulkUpdateUsersSquads(ctx, cleanUserUUIDs, requestedSquads)
	if err != nil {
		return 0, err
	}

	if affectedRows > 0 {
		if len(nodeUUIDs) > 0 {
			monitor.RequestNodeDeploy(true, nodeUUIDs...)
		}
	}
	return affectedRows, nil
}

func (s *UserService) BulkAllUpdateUsers(ctx context.Context, req bulkAllUpdateUsersRequest) (int64, error) {
	clauses, args := buildBulkUpdateUserClauses(bulkUpdateUsersFields{
		Status:               req.Status,
		TrafficLimitBytes:    req.TrafficLimitBytes,
		TrafficLimitStrategy: req.TrafficLimitStrategy,
		ExpireAt:             req.ExpireAt,
		Description:          req.Description,
		Tag:                  req.Tag,
		TelegramID:           req.TelegramID,
		Email:                req.Email,
		HwidDeviceLimit:      req.HwidDeviceLimit,
	})
	if len(clauses) == 0 {
		return 0, errors.New("at least one field must be provided")
	}

	affectedRows, err := s.repo.bulkAllUpdateUsers(ctx, clauses, args)
	if err != nil {
		return 0, err
	}

	if affectedRows > 0 {
		monitor.RequestNodeDeploy(true)
	}
	return affectedRows, nil
}

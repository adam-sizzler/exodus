package users

import (
	"context"
	"errors"
	"fmt"
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

func (s *UserService) EnableUser(ctx context.Context, userUUID string) error {
	nodeUUIDs, err := s.repo.updateUserStatus(ctx, userUUID, "ACTIVE")
	if err != nil {
		return err
	}
	if len(nodeUUIDs) > 0 {
		monitor.RequestNodeDeploy(true, nodeUUIDs...)
	}
	if record, loadErr := s.repo.getUserRecordByUUID(ctx, userUUID); loadErr == nil {
		emitUserNotification(ctx, s.repo, s.cfg, notifications.EventUserEnabled, record, nil)
	}
	return nil
}

func (s *UserService) DisableUser(ctx context.Context, userUUID string) error {
	nodeUUIDs, err := s.repo.updateUserStatus(ctx, userUUID, "DISABLED")
	if err != nil {
		return err
	}
	if len(nodeUUIDs) > 0 {
		monitor.RequestNodeDeploy(true, nodeUUIDs...)
	}
	if record, loadErr := s.repo.getUserRecordByUUID(ctx, userUUID); loadErr == nil {
		emitUserNotification(ctx, s.repo, s.cfg, notifications.EventUserDisabled, record, nil)
	}
	return nil
}

func (s *UserService) ResetUserTraffic(ctx context.Context, userUUID string) error {
	affected, nodeUUIDs, reactivatedUUIDs, err := s.repo.resetUsersTrafficByUUIDs(ctx, []string{userUUID})
	if err != nil {
		return err
	}
	if len(nodeUUIDs) > 0 {
		monitor.RequestNodeDeploy(true, nodeUUIDs...)
	}
	if record, loadErr := s.repo.getUserRecordByUUID(ctx, userUUID); loadErr == nil {
		emitUserNotification(ctx, s.repo, s.cfg, notifications.EventUserTrafficReset, record, nil)
		if len(reactivatedUUIDs) > 0 && affected > 0 {
			emitUserNotification(ctx, s.repo, s.cfg, notifications.EventUserEnabled, record, nil)
		}
	}
	return nil
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

	tx, err := s.repo.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	nodeUUIDs, nodeTargetsErr := s.repo.resolveNodeUUIDsForUserUUIDsTx(ctx, tx, []string{userUUID})
	if nodeTargetsErr != nil {
		return nodeTargetsErr
	}

	query := `
		UPDATE users
		SET trojan_password = $1,
		    vless_uuid = $2,
		    ss_password = $3,
		    naive_password = $4,
		    shadowtls_password = $5,
		    hysteria2_password = $6,
		    anytls_password = $7,
		    sub_revoked_at = CURRENT_TIMESTAMP,
		    updated_at = CURRENT_TIMESTAMP`
	args := []any{
		credentials.TrojanPassword,
		credentials.VlessUUID,
		credentials.SSPassword,
		credentials.NaivePassword,
		credentials.ShadowtlsPassword,
		credentials.Hysteria2Password,
		credentials.AnytlsPassword,
	}
	if !req.RevokeOnlyPasswords {
		query += `, short_uuid = $8`
		args = append(args, shortUUID)
	}
	query += fmt.Sprintf(` WHERE uuid = $%d`, len(args)+1)
	args = append(args, userUUID)

	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return mapUserWriteError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return errUserNotFound
	}
	if err := tx.Commit(); err != nil {
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
		monitor.RequestNodeDeploy(true, internalSquadNodeUUIDs...)
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
	if (internalSquadsChanged || statusDeployRequired) && len(deployNodeUUIDs) > 0 {
		monitor.RequestNodeDeploy(true, deployNodeUUIDs...)
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
		monitor.RequestNodeDeploy(true, internalSquadNodeUUIDs...)
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
		monitor.RequestNodeDeploy(true, internalSquadNodeUUIDs...)
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

package users

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	exodusdb "exodus/internal/db"
	"exodus/internal/httpapi/shared"
	"exodus/internal/util"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type UserRepository struct {
	db *pgxpool.Pool
}

func NewUserRepository(db *pgxpool.Pool) *UserRepository {
	return &UserRepository{db: db}
}

// getUsersTableRecords fetches one page of the users table with filtering,
// sorting and pagination applied in SQL (WHERE/ORDER BY/LIMIT/OFFSET), instead
// of loading the whole users table into memory and slicing it in Go.
func (r *UserRepository) getUsersTableRecords(ctx context.Context, whereSQL, orderSQL string, whereArgs []any, start, size int) ([]userRecord, int64, error) {
	if size <= 0 || size > 1000 {
		size = 25
	}
	if start < 0 {
		start = 0
	}

	baseFrom := `FROM users u LEFT JOIN user_traffic ut ON ut.id = u.id ` + whereSQL

	var total int64
	if err := r.db.QueryRow(ctx, "SELECT COUNT(*) "+baseFrom, whereArgs...).Scan(&total); err != nil {
		return nil, 0, err
	}

	limitIdx := len(whereArgs) + 1
	offsetIdx := len(whereArgs) + 2
	args := append(append([]any{}, whereArgs...), size, start)

	query := fmt.Sprintf(`
		SELECT
			u.id, u.uuid, u.short_uuid, u.username, u.status, u.traffic_limit_bytes,
			u.traffic_limit_strategy, u.expire_at, u.last_traffic_reset_at,
			u.sub_revoked_at, u.trojan_password, u.vless_uuid, u.ss_password,
			u.naive_password, u.shadowtls_password, u.hysteria2_password, u.anytls_password,
			u.description, u.tag, u.telegram_id, u.email, u.hwid_device_limit, u.external_squad_uuid,
			u.last_triggered_threshold, u.created_at, u.updated_at,
			COALESCE(ut.used_traffic_bytes, 0), COALESCE(ut.lifetime_used_traffic_bytes, 0),
			ut.online_at, ut.last_connected_node_uuid, ut.first_connected_at
		%s
		%s
		LIMIT $%d OFFSET $%d
	`, baseFrom, orderSQL, limitIdx, offsetIdx)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	records := make([]userRecord, 0, size)
	for rows.Next() {
		record, scanErr := scanUserRecord(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	return records, total, nil
}

func (r *UserRepository) getUserRecordByID(ctx context.Context, id int64) (userRecord, error) {
	query := `
		SELECT
			u.id, u.uuid, u.short_uuid, u.username, u.status, u.traffic_limit_bytes,
			u.traffic_limit_strategy, u.expire_at, u.last_traffic_reset_at,
			u.sub_revoked_at, u.trojan_password, u.vless_uuid, u.ss_password,
			u.naive_password, u.shadowtls_password, u.hysteria2_password, u.anytls_password,
			u.description, u.tag, u.telegram_id, u.email, u.hwid_device_limit, u.external_squad_uuid,
			u.last_triggered_threshold, u.created_at, u.updated_at,
			COALESCE(ut.used_traffic_bytes, 0), COALESCE(ut.lifetime_used_traffic_bytes, 0),
			ut.online_at, ut.last_connected_node_uuid, ut.first_connected_at
		FROM users u
		LEFT JOIN user_traffic ut ON ut.id = u.id
		WHERE u.id = $1
	`
	row := r.db.QueryRow(ctx, query, id)
	record, scanErr := scanUserRecord(row)
	if errors.Is(scanErr, pgx.ErrNoRows) {
		return record, errUserNotFound
	}
	return record, scanErr
}

func (r *UserRepository) getUserRecordByUUID(ctx context.Context, identifier string) (userRecord, error) {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return userRecord{}, errUserNotFound
	}

	query := `
		SELECT
			u.id, u.uuid, u.short_uuid, u.username, u.status, u.traffic_limit_bytes,
			u.traffic_limit_strategy, u.expire_at, u.last_traffic_reset_at,
			u.sub_revoked_at, u.trojan_password, u.vless_uuid, u.ss_password,
			u.naive_password, u.shadowtls_password, u.hysteria2_password, u.anytls_password,
			u.description, u.tag, u.telegram_id, u.email, u.hwid_device_limit, u.external_squad_uuid,
			u.last_triggered_threshold, u.created_at, u.updated_at,
			COALESCE(ut.used_traffic_bytes, 0), COALESCE(ut.lifetime_used_traffic_bytes, 0),
			ut.online_at, ut.last_connected_node_uuid, ut.first_connected_at
		FROM users u
		LEFT JOIN user_traffic ut ON ut.id = u.id
	`

	var row pgx.Row
	if idNum, err := strconv.ParseInt(identifier, 10, 64); err == nil {
		row = r.db.QueryRow(ctx, query+` WHERE u.id = $1 OR u.uuid::text = $2 OR u.short_uuid = $2 OR u.username = $2`, idNum, identifier)
	} else {
		row = r.db.QueryRow(ctx, query+` WHERE u.uuid::text = $1 OR u.short_uuid = $1 OR u.username = $1`, identifier)
	}

	record, scanErr := scanUserRecord(row)
	if errors.Is(scanErr, pgx.ErrNoRows) {
		return record, errUserNotFound
	}
	return record, scanErr
}

func (r *UserRepository) getUserRecordsByUUIDs(ctx context.Context, userUUIDs []string) (map[string]userRecord, error) {
	clean := dedupeStrings(userUUIDs)
	records := make(map[string]userRecord, len(clean))
	if len(clean) == 0 {
		return records, nil
	}

	rows, err := r.db.Query(ctx, `
		SELECT
			u.id, u.uuid, u.short_uuid, u.username, u.status, u.traffic_limit_bytes,
			u.traffic_limit_strategy, u.expire_at, u.last_traffic_reset_at,
			u.sub_revoked_at, u.trojan_password, u.vless_uuid, u.ss_password,
			u.naive_password, u.shadowtls_password, u.hysteria2_password, u.anytls_password,
			u.description, u.tag, u.telegram_id, u.email, u.hwid_device_limit, u.external_squad_uuid,
			u.last_triggered_threshold, u.created_at, u.updated_at,
			COALESCE(ut.used_traffic_bytes, 0), COALESCE(ut.lifetime_used_traffic_bytes, 0),
			ut.online_at, ut.last_connected_node_uuid, ut.first_connected_at
		FROM users u
		LEFT JOIN user_traffic ut ON ut.id = u.id
		WHERE u.uuid = ANY($1)
	`, clean)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		record, scanErr := scanUserRecord(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		records[record.UUID] = record
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func (r *UserRepository) resolveUUIDsByUserIDs(ctx context.Context, userIDs []int64) ([]string, error) {
	if len(userIDs) == 0 {
		return []string{}, nil
	}
	rows, err := r.db.Query(ctx, `SELECT uuid::text FROM users WHERE id = ANY($1)`, userIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	uuids := make([]string, 0, len(userIDs))
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, err
		}
		uuids = append(uuids, u)
	}
	return uuids, rows.Err()
}

func scanUserRecord(scanner shared.RowScanner) (userRecord, error) {
	var record userRecord

	err := scanner.Scan(
		&record.ID,
		&record.UUID,
		&record.ShortUUID,
		&record.Username,
		&record.Status,
		&record.TrafficLimitBytes,
		&record.TrafficLimitStrategy,
		&record.ExpireAt,
		&record.LastTrafficResetAt,
		&record.SubRevokedAt,
		&record.TrojanPassword,
		&record.VlessUUID,
		&record.SSPassword,
		&record.NaivePassword,
		&record.ShadowtlsPassword,
		&record.Hysteria2Password,
		&record.AnytlsPassword,
		&record.Description,
		&record.Tag,
		&record.TelegramID,
		&record.Email,
		&record.HwidDeviceLimit,
		&record.ExternalSquadUUID,
		&record.LastTriggeredThreshold,
		&record.CreatedAt,
		&record.UpdatedAt,
		&record.UsedTrafficBytes,
		&record.LifetimeUsedTrafficBytes,
		&record.OnlineAt,
		&record.LastConnectedNodeUUID,
		&record.FirstConnectedAt,
	)
	return record, err
}

func (r *UserRepository) getUsersActiveInternalSquads(ctx context.Context, userUUIDs []string) (map[string][]internalSquadResponse, error) {
	result := make(map[string][]internalSquadResponse, len(userUUIDs))
	if len(userUUIDs) == 0 {
		return result, nil
	}
	for _, userUUID := range userUUIDs {
		result[userUUID] = emptyInternalSquads
	}

	rows, err := r.db.Query(ctx, `
		SELECT u.uuid, s.uuid, s.name
		FROM users u
		INNER JOIN internal_squad_members ism ON ism.user_id = u.id
		INNER JOIN internal_squads s ON s.uuid = ism.internal_squad_uuid
		WHERE u.uuid = ANY($1)
		ORDER BY s.view_position ASC, s.name ASC
	`, userUUIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var userUUID, squadUUID, squadName string
		if err := rows.Scan(&userUUID, &squadUUID, &squadName); err != nil {
			return nil, err
		}
		result[userUUID] = append(result[userUUID], internalSquadResponse{
			UUID: squadUUID,
			Name: squadName,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (r *UserRepository) getAllUserTags(ctx context.Context) ([]string, error) {
	rows, err := r.db.Query(ctx, `
		SELECT DISTINCT tag
		FROM users
		WHERE tag IS NOT NULL AND tag <> ''
		ORDER BY tag ASC
		LIMIT 1000
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tags := make([]string, 0)
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, err
		}
		tags = append(tags, tag)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tags, nil
}

func (r *UserRepository) replaceUserInternalSquadsTx(ctx context.Context, tx pgx.Tx, userID int64, squadUUIDs []string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM internal_squad_members WHERE user_id = $1`, userID); err != nil {
		return err
	}
	cleanSquads := make([]string, 0, len(squadUUIDs))
	for _, squadUUID := range dedupeStrings(squadUUIDs) {
		if clean := strings.TrimSpace(squadUUID); clean != "" {
			cleanSquads = append(cleanSquads, clean)
		}
	}
	if len(cleanSquads) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO internal_squad_members (internal_squad_uuid, user_id)
		SELECT s::uuid, $2
		FROM unnest($1::text[]) AS s
		ON CONFLICT (internal_squad_uuid, user_id) DO NOTHING
	`, cleanSquads, userID)
	return err
}

func (r *UserRepository) resolveUserUUIDForUpdate(ctx context.Context, id *int64, userUUID *string, username *string) (string, error) {
	if id != nil {
		record, err := r.getUserRecordByID(ctx, *id)
		if err != nil {
			return "", err
		}
		return record.UUID, nil
	}
	if userUUID != nil && strings.TrimSpace(*userUUID) != "" {
		return strings.TrimSpace(*userUUID), nil
	}
	if username == nil || strings.TrimSpace(*username) == "" {
		return "", fmt.Errorf("either id, uuid or username must be provided")
	}

	var resolved string
	err := r.db.QueryRow(ctx, `SELECT uuid FROM users WHERE username = $1`, strings.TrimSpace(*username)).Scan(&resolved)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", errUserNotFound
	}
	return resolved, err
}

func (r *UserRepository) resolveUser(ctx context.Context, req resolveUserRequest) (resolveUserResponse, error) {
	var response resolveUserResponse
	clause := ""
	var arg any
	switch {
	case req.ID != nil:
		clause = "id = $1"
		arg = *req.ID
	case req.ShortUUID != nil:
		clause = "short_uuid = $1"
		arg = strings.TrimSpace(*req.ShortUUID)
	case req.Username != nil:
		clause = "username = $1"
		arg = strings.TrimSpace(*req.Username)
	default:
		return response, fmt.Errorf("missing user lookup field")
	}

	var dummyUUID string
	err := r.db.QueryRow(ctx, fmt.Sprintf(`
		SELECT uuid, id, short_uuid, username
		FROM users
		WHERE %s
	`, clause), arg).Scan(&dummyUUID, &response.ID, &response.ShortUUID, &response.Username)
	if errors.Is(err, pgx.ErrNoRows) {
		return response, errUserNotFound
	}
	return response, err
}

func (r *UserRepository) createUser(ctx context.Context, userUUID, shortUUID string, req createUserRequest, credentials userProtocolCredentials, expireAt, createdAt time.Time, lastTrafficResetAt any) (int64, []string, error) {
	var userID int64
	var internalSquadNodeUUIDs []string

	err := exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		insertErr := tx.QueryRow(ctx, `
			INSERT INTO users (
				uuid, short_uuid, username, status, traffic_limit_bytes, traffic_limit_strategy,
				expire_at, last_traffic_reset_at, sub_revoked_at,
				trojan_password, vless_uuid, ss_password, naive_password, shadowtls_password, hysteria2_password, anytls_password,
				description, tag, telegram_id, email,
				hwid_device_limit, external_squad_uuid, last_triggered_threshold, created_at, updated_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, NULL, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, 0, $22, $23
			)
			RETURNING id
		`,
			userUUID,
			shortUUID,
			strings.TrimSpace(req.Username),
			normalizeUserStatus(req.Status),
			util.Coalesce(req.TrafficLimitBytes, 0),
			normalizeTrafficStrategy(req.TrafficLimitStrategy),
			expireAt.UTC(),
			lastTrafficResetAt,
			credentials.TrojanPassword,
			credentials.VlessUUID,
			credentials.SSPassword,
			credentials.NaivePassword,
			credentials.ShadowtlsPassword,
			credentials.Hysteria2Password,
			credentials.AnytlsPassword,
			normalizeNullableString(req.Description),
			normalizeUserTag(req.Tag),
			req.TelegramID,
			normalizeNullableString(req.Email),
			req.HwidDeviceLimit,
			normalizeNullableString(req.ExternalSquadUUID),
			createdAt.UTC(),
			createdAt.UTC(),
		).Scan(&userID)
		if insertErr != nil {
			return mapUserWriteError(insertErr)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO user_traffic (
				id, used_traffic_bytes, lifetime_used_traffic_bytes, online_at,
				last_connected_node_uuid, first_connected_at
			) VALUES ($1, 0, 0, NULL, NULL, NULL)
		`, userID); err != nil {
			return err
		}

		if err := r.replaceUserInternalSquadsTx(ctx, tx, userID, req.ActiveInternalSquads); err != nil {
			return err
		}

		requestedSquads := dedupeStrings(req.ActiveInternalSquads)
		if len(requestedSquads) > 0 {
			nodeUUIDs, nodeTargetsErr := r.resolveNodeUUIDsForInternalSquadsTx(ctx, tx, requestedSquads)
			if nodeTargetsErr != nil {
				return nodeTargetsErr
			}
			internalSquadNodeUUIDs = nodeUUIDs
		} else {
			internalSquadNodeUUIDs = make([]string, 0)
		}

		return nil
	})
	if err != nil {
		return 0, nil, err
	}

	return userID, internalSquadNodeUUIDs, nil
}

func (r *UserRepository) revokeUserSubscription(ctx context.Context, userUUID string, shortUUID string, credentials userProtocolCredentials, revokeOnlyPasswords bool) ([]string, error) {
	var nodeUUIDs []string

	err := exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		resolvedNodeUUIDs, nodeTargetsErr := r.resolveNodeUUIDsForUserUUIDsTx(ctx, tx, []string{userUUID})
		if nodeTargetsErr != nil {
			return nodeTargetsErr
		}
		nodeUUIDs = resolvedNodeUUIDs

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
		if !revokeOnlyPasswords {
			query += `, short_uuid = $8`
			args = append(args, shortUUID)
		}
		query += fmt.Sprintf(` WHERE uuid = $%d`, len(args)+1)
		args = append(args, userUUID)

		tag, err := tx.Exec(ctx, query, args...)
		if err != nil {
			return mapUserWriteError(err)
		}
		if tag.RowsAffected() == 0 {
			return errUserNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return nodeUUIDs, nil
}

func (r *UserRepository) updateUserRecord(ctx context.Context, targetUUID string, record userRecord, req updateUserRequest, statusToSet string, shouldSetStatus, statusDeployRequired bool) (userRecord, []string, []string, bool, error) {
	var statusNodeUUIDs []string
	var internalSquadNodeUUIDs []string
	var internalSquadsChanged bool

	err := exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		clauses := make([]string, 0)
		args := make([]any, 0)
		idx := 1
		add := func(column string, value any) {
			clauses = append(clauses, fmt.Sprintf("%s = $%d", column, idx))
			args = append(args, value)
			idx++
		}

		if statusDeployRequired {
			nodeUUIDs, nodeTargetsErr := r.resolveNodeUUIDsForUserUUIDsTx(ctx, tx, []string{targetUUID})
			if nodeTargetsErr != nil {
				return nodeTargetsErr
			}
			statusNodeUUIDs = nodeUUIDs
		} else {
			statusNodeUUIDs = make([]string, 0)
		}

		if shouldSetStatus {
			add("status", statusToSet)
		}
		if req.TrafficLimitBytes != nil {
			add("traffic_limit_bytes", *req.TrafficLimitBytes)
		}
		if req.TrafficLimitStrategy != nil {
			add("traffic_limit_strategy", strings.ToUpper(strings.TrimSpace(*req.TrafficLimitStrategy)))
		}
		if req.ExpireAt != nil {
			parsed, _ := time.Parse(time.RFC3339, strings.TrimSpace(*req.ExpireAt))
			add("expire_at", parsed.UTC())
		}
		if req.Description.Set {
			if req.Description.Value == nil || strings.TrimSpace(*req.Description.Value) == "" {
				clauses = append(clauses, "description = NULL")
			} else {
				add("description", strings.TrimSpace(*req.Description.Value))
			}
		}
		if req.Tag.Set {
			if req.Tag.Value == nil || strings.TrimSpace(*req.Tag.Value) == "" {
				clauses = append(clauses, "tag = NULL")
			} else {
				add("tag", strings.ToUpper(strings.TrimSpace(*req.Tag.Value)))
			}
		}
		if req.TelegramID.Set {
			if req.TelegramID.Value == nil {
				clauses = append(clauses, "telegram_id = NULL")
			} else {
				add("telegram_id", *req.TelegramID.Value)
			}
		}
		if req.Email.Set {
			if req.Email.Value == nil || strings.TrimSpace(*req.Email.Value) == "" {
				clauses = append(clauses, "email = NULL")
			} else {
				add("email", strings.TrimSpace(*req.Email.Value))
			}
		}
		if req.HwidDeviceLimit.Set {
			if req.HwidDeviceLimit.Value == nil {
				clauses = append(clauses, "hwid_device_limit = NULL")
			} else {
				add("hwid_device_limit", *req.HwidDeviceLimit.Value)
			}
		}

		addOptionalCredential := func(field OptionalString, column string, nullable bool) {
			if !field.Set {
				return
			}
			if field.Value == nil {
				if nullable {
					clauses = append(clauses, fmt.Sprintf("%s = NULL", column))
				}
				return
			}
			add(column, strings.TrimSpace(*field.Value))
		}
		addOptionalCredential(req.TrojanPassword, "trojan_password", false)
		addOptionalCredential(req.VlessUUID, "vless_uuid", false)
		addOptionalCredential(req.SSPassword, "ss_password", false)
		addOptionalCredential(req.NaivePassword, "naive_password", false)
		addOptionalCredential(req.ShadowtlsPassword, "shadowtls_password", false)
		addOptionalCredential(req.Hysteria2Password, "hysteria2_password", false)
		addOptionalCredential(req.AnytlsPassword, "anytls_password", false)

		if req.ExternalSquadUUID.Set {
			if req.ExternalSquadUUID.Value == nil || strings.TrimSpace(*req.ExternalSquadUUID.Value) == "" {
				clauses = append(clauses, "external_squad_uuid = NULL")
			} else {
				add("external_squad_uuid", strings.TrimSpace(*req.ExternalSquadUUID.Value))
			}
		}

		if len(clauses) > 0 {
			args = append(args, targetUUID)
			query := fmt.Sprintf("UPDATE users SET %s, updated_at = CURRENT_TIMESTAMP WHERE uuid = $%d", strings.Join(clauses, ", "), idx)
			if _, err := tx.Exec(ctx, query, args...); err != nil {
				return mapUserWriteError(err)
			}
		}

		internalSquadsChanged = false
		internalSquadNodeUUIDs = make([]string, 0)
		if req.ActiveInternalSquads != nil {
			currentSquads, loadErr := r.getUserInternalSquadsTx(ctx, tx, record.ID)
			if loadErr != nil {
				return loadErr
			}
			requestedSquads := dedupeStrings(*req.ActiveInternalSquads)
			if internalSquadSetsDiffer(currentSquads, requestedSquads) {
				affectedSquads := dedupeStrings(append(append([]string{}, currentSquads...), requestedSquads...))
				nodeUUIDs, nodeTargetsErr := r.resolveNodeUUIDsForInternalSquadsTx(ctx, tx, affectedSquads)
				if nodeTargetsErr != nil {
					return nodeTargetsErr
				}
				if err := r.replaceUserInternalSquadsTx(ctx, tx, record.ID, requestedSquads); err != nil {
					return err
				}
				internalSquadNodeUUIDs = nodeUUIDs
				internalSquadsChanged = true
			}
		}

		return nil
	})
	if err != nil {
		return userRecord{}, nil, nil, false, err
	}

	updatedRecord, err := r.getUserRecordByUUID(ctx, targetUUID)
	return updatedRecord, statusNodeUUIDs, internalSquadNodeUUIDs, internalSquadsChanged, err
}

func (r *UserRepository) deleteUserRecord(ctx context.Context, userUUID string) ([]string, error) {
	var nodeUUIDs []string

	err := exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		var userID int64
		if err := tx.QueryRow(ctx, `SELECT id FROM users WHERE uuid = $1`, userUUID).Scan(&userID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return errUserNotFound
			}
			return err
		}

		currentSquads, loadErr := r.getUserInternalSquadsTx(ctx, tx, userID)
		if loadErr != nil {
			return loadErr
		}

		resolvedNodeUUIDs, nodeTargetsErr := r.resolveNodeUUIDsForInternalSquadsTx(ctx, tx, currentSquads)
		if nodeTargetsErr != nil {
			return nodeTargetsErr
		}
		nodeUUIDs = resolvedNodeUUIDs

		tag, err := tx.Exec(ctx, `DELETE FROM users WHERE uuid = $1`, userUUID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return errUserNotFound
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return nodeUUIDs, nil
}

func (r *UserRepository) updateUserStatus(ctx context.Context, userUUID string, status string) ([]string, error) {
	var nodeUUIDs []string

	err := exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		resolvedNodeUUIDs, nodeTargetsErr := r.resolveNodeUUIDsForUserUUIDsTx(ctx, tx, []string{userUUID})
		if nodeTargetsErr != nil {
			return nodeTargetsErr
		}
		nodeUUIDs = resolvedNodeUUIDs

		tag, err := tx.Exec(ctx, `
			UPDATE users
			SET status = $1, updated_at = CURRENT_TIMESTAMP
			WHERE uuid = $2
		`, status, userUUID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return errUserNotFound
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return nodeUUIDs, nil
}

func (r *UserRepository) deleteUsersRecord(ctx context.Context, uuids []string) ([]string, error) {
	var nodeUUIDs []string

	err := exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		resolvedNodeUUIDs, nodeTargetsErr := r.resolveNodeUUIDsForUserUUIDsTx(ctx, tx, uuids)
		if nodeTargetsErr != nil {
			return nodeTargetsErr
		}
		nodeUUIDs = resolvedNodeUUIDs

		if _, err := tx.Exec(ctx, `DELETE FROM users WHERE uuid = ANY($1)`, uuids); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return nodeUUIDs, nil
}

func (r *UserRepository) deleteUsersByStatus(ctx context.Context, status string) (int64, []string, error) {
	var affectedRows int64
	var nodeUUIDs []string

	err := exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		rows, queryErr := tx.Query(ctx,
			`SELECT DISTINCT cpitn.node_uuid
			   FROM users u
			   JOIN internal_squad_members ism ON ism.user_id = u.id
			   JOIN internal_squad_inbounds isi ON isi.internal_squad_uuid = ism.internal_squad_uuid
			   JOIN config_profile_inbounds_to_nodes cpitn ON cpitn.config_profile_inbound_uuid = isi.inbound_uuid
			  WHERE u.status = $1`, status)
		if queryErr != nil {
			return queryErr
		}
		defer rows.Close()

		nodeUUIDs = make([]string, 0)
		for rows.Next() {
			var nodeUUID string
			if scanErr := rows.Scan(&nodeUUID); scanErr != nil {
				return scanErr
			}
			nodeUUIDs = append(nodeUUIDs, nodeUUID)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return rowsErr
		}

		const deleteBatchSize = 30000
		affectedRows = 0
		for {
			tag, execErr := tx.Exec(ctx, `
				DELETE FROM users
				WHERE id IN (
					SELECT id FROM users
					WHERE status = $1
					LIMIT $2
				)
			`, status, deleteBatchSize)
			if execErr != nil {
				return execErr
			}
			deleted := tag.RowsAffected()
			affectedRows += deleted
			if deleted < deleteBatchSize {
				break
			}
		}

		return nil
	})
	if err != nil {
		return 0, nil, err
	}

	return affectedRows, nodeUUIDs, nil
}

func (r *UserRepository) bulkUpdateUsers(ctx context.Context, cleanUUIDs []string, clauses []string, args []any) (int64, []string, error) {
	var affectedRows int64
	var nodeUUIDs []string

	err := exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		resolvedNodeUUIDs, nodeTargetsErr := r.resolveNodeUUIDsForUserUUIDsTx(ctx, tx, cleanUUIDs)
		if nodeTargetsErr != nil {
			return nodeTargetsErr
		}
		nodeUUIDs = resolvedNodeUUIDs

		queryArgs := append(args, cleanUUIDs)
		query := fmt.Sprintf("UPDATE users SET %s, updated_at = CURRENT_TIMESTAMP WHERE uuid = ANY($%d)", strings.Join(clauses, ", "), len(queryArgs))
		tag, execErr := tx.Exec(ctx, query, queryArgs...)
		if execErr != nil {
			return mapUserWriteError(execErr)
		}
		affectedRows = tag.RowsAffected()

		return nil
	})
	if err != nil {
		return 0, nil, err
	}

	return affectedRows, nodeUUIDs, nil
}

func (r *UserRepository) bulkUpdateUsersSquads(ctx context.Context, cleanUserUUIDs []string, requestedSquads []string) (int64, []string, error) {
	var userIDs []int64
	var nodeUUIDs []string

	err := exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		targets, nodeTargetsErr := r.resolveNodeUUIDsForUserUUIDsTx(ctx, tx, cleanUserUUIDs)
		if nodeTargetsErr != nil {
			return nodeTargetsErr
		}
		squadTargets, squadTargetsErr := r.resolveNodeUUIDsForInternalSquadsTx(ctx, tx, requestedSquads)
		if squadTargetsErr != nil {
			return squadTargetsErr
		}
		nodeUUIDs = dedupeStrings(append(targets, squadTargets...))

		rows, err := tx.Query(ctx, `SELECT id FROM users WHERE uuid = ANY($1)`, cleanUserUUIDs)
		if err != nil {
			return err
		}
		defer rows.Close()

		userIDs = make([]int64, 0, len(cleanUserUUIDs))
		for rows.Next() {
			var userID int64
			if err := rows.Scan(&userID); err != nil {
				return err
			}
			userIDs = append(userIDs, userID)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		if len(userIDs) > 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM internal_squad_members WHERE user_id = ANY($1)`, userIDs); err != nil {
				return err
			}

			cleanSquads := make([]string, 0, len(requestedSquads))
			for _, sq := range dedupeStrings(requestedSquads) {
				if clean := strings.TrimSpace(sq); clean != "" {
					cleanSquads = append(cleanSquads, clean)
				}
			}

			if len(cleanSquads) > 0 {
				if _, err := tx.Exec(ctx, `
					INSERT INTO internal_squad_members (internal_squad_uuid, user_id)
					SELECT s::uuid, u
					FROM unnest($1::text[]) AS s
					CROSS JOIN unnest($2::bigint[]) AS u
					ON CONFLICT (internal_squad_uuid, user_id) DO NOTHING
				`, cleanSquads, userIDs); err != nil {
					return err
				}
			}

			if _, err := tx.Exec(ctx, `UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE uuid = ANY($1)`, cleanUserUUIDs); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return 0, nil, err
	}

	return int64(len(userIDs)), nodeUUIDs, nil
}

func (r *UserRepository) bulkAllUpdateUsers(ctx context.Context, clauses []string, args []any) (int64, error) {
	query := fmt.Sprintf("UPDATE users SET %s, updated_at = CURRENT_TIMESTAMP", strings.Join(clauses, ", "))
	tag, execErr := r.db.Exec(ctx, query, args...)
	if execErr != nil {
		return 0, mapUserWriteError(execErr)
	}
	return tag.RowsAffected(), nil
}

func (r *UserRepository) confirmUserExistsByID(ctx context.Context, userID int64) error {
	var exists bool
	if err := r.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)`, userID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return errUserNotFound
	}
	return nil
}

func (r *UserRepository) getUserSubscriptionRequestHistory(ctx context.Context, userID int64) ([]userSubscriptionRequestHistoryRecord, error) {
	if err := r.confirmUserExistsByID(ctx, userID); err != nil {
		return nil, err
	}

	rows, err := r.db.Query(ctx, `
		SELECT id, user_id, COALESCE(srr_response_type, 'UNKNOWN'), srr_rule_name, request_ip, user_agent, request_at
		FROM user_subscription_request_history
		WHERE user_id = $1
		ORDER BY request_at DESC
		LIMIT 24
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]userSubscriptionRequestHistoryRecord, 0)
	for rows.Next() {
		var item userSubscriptionRequestHistoryRecord
		var requestAt time.Time
		if scanErr := rows.Scan(&item.ID, &item.UserID, &item.SRRResponseType, &item.SRRRuleName, &item.RequestIP, &item.UserAgent, &requestAt); scanErr != nil {
			return nil, scanErr
		}
		item.RequestAt = requestAt.UTC().Format("2006-01-02T15:04:05.000Z")
		records = append(records, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func (r *UserRepository) getUserAccessibleNodes(ctx context.Context, userID int64) ([]userAccessibleNode, error) {
	if err := r.confirmUserExistsByID(ctx, userID); err != nil {
		return nil, err
	}

	rows, err := r.db.Query(ctx, `
		SELECT
			n.uuid,
			n.name,
			n.country_code,
			cp.uuid,
			cp.name,
			sq.uuid,
			sq.name,
			cpi.tag
		FROM nodes n
		INNER JOIN config_profiles cp ON cp.uuid = n.active_config_profile_uuid
		INNER JOIN config_profile_inbounds cpi ON cpi.profile_uuid = cp.uuid
		INNER JOIN config_profile_inbounds_to_nodes cpin
			ON cpin.config_profile_inbound_uuid = cpi.uuid
			AND cpin.node_uuid = n.uuid
		INNER JOIN internal_squad_inbounds isi ON isi.inbound_uuid = cpi.uuid
		INNER JOIN internal_squads sq ON sq.uuid = isi.internal_squad_uuid
		INNER JOIN internal_squad_members ism
			ON ism.internal_squad_uuid = sq.uuid
			AND ism.user_id = $1
		ORDER BY n.view_position ASC, sq.view_position ASC, cpi.tag ASC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	activeNodes := make([]userAccessibleNode, 0)
	nodeIndexes := make(map[string]int)
	squadIndexesByNode := make(map[string]map[string]int)
	for rows.Next() {
		var nodeUUID, nodeName, countryCode, profileUUID, profileName string
		var squadUUID, squadName, inboundTag string
		if scanErr := rows.Scan(&nodeUUID, &nodeName, &countryCode, &profileUUID, &profileName, &squadUUID, &squadName, &inboundTag); scanErr != nil {
			return nil, scanErr
		}

		nodeIndex, ok := nodeIndexes[nodeUUID]
		if !ok {
			activeNodes = append(activeNodes, userAccessibleNode{
				UUID:              nodeUUID,
				NodeName:          nodeName,
				CountryCode:       countryCode,
				ConfigProfileUUID: profileUUID,
				ConfigProfileName: profileName,
				ActiveSquads:      make([]userAccessibleSquad, 0),
			})
			nodeIndex = len(activeNodes) - 1
			nodeIndexes[nodeUUID] = nodeIndex
			squadIndexesByNode[nodeUUID] = make(map[string]int)
		}

		squadIndexes := squadIndexesByNode[nodeUUID]
		squadIndex, ok := squadIndexes[squadUUID]
		if !ok {
			activeNodes[nodeIndex].ActiveSquads = append(activeNodes[nodeIndex].ActiveSquads, userAccessibleSquad{
				SquadName:      squadName,
				ActiveInbounds: make([]string, 0),
			})
			squadIndex = len(activeNodes[nodeIndex].ActiveSquads) - 1
			squadIndexes[squadUUID] = squadIndex
		}

		activeNodes[nodeIndex].ActiveSquads[squadIndex].ActiveInbounds = append(
			activeNodes[nodeIndex].ActiveSquads[squadIndex].ActiveInbounds,
			inboundTag,
		)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return activeNodes, nil
}

func (r *UserRepository) getUsersStream(ctx context.Context, cursor int64, size int, telegramID, email, tag, status, trafficLimitStrategy, externalSquadUUID string) ([]userRecord, *string, bool, error) {
	whereClauses := []string{"u.id > $1"}
	args := []any{cursor}
	argIdx := 2

	if telegramID != "" {
		if parsedTelegramID, err := strconv.ParseInt(telegramID, 10, 64); err == nil {
			whereClauses = append(whereClauses, fmt.Sprintf("telegram_id = $%d", argIdx))
			args = append(args, parsedTelegramID)
			argIdx++
		}
	}
	if email != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("email = $%d", argIdx))
		args = append(args, email)
		argIdx++
	}
	if tag != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("tag = $%d", argIdx))
		args = append(args, tag)
		argIdx++
	}
	if status != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, strings.ToUpper(status))
		argIdx++
	}
	if trafficLimitStrategy != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("traffic_limit_strategy = $%d", argIdx))
		args = append(args, strings.ToUpper(trafficLimitStrategy))
		argIdx++
	}
	if externalSquadUUID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("external_squad_uuid = $%d", argIdx))
		args = append(args, externalSquadUUID)
		argIdx++
	}

	whereStmt := strings.Join(whereClauses, " AND ")
	args = append(args, size+1)
	limitIdx := argIdx

	query := fmt.Sprintf(`
		SELECT
			u.id, u.uuid, u.short_uuid, u.username, u.status, u.traffic_limit_bytes,
			u.traffic_limit_strategy, u.expire_at, u.last_traffic_reset_at,
			u.sub_revoked_at, u.trojan_password, u.vless_uuid, u.ss_password,
			u.naive_password, u.shadowtls_password, u.hysteria2_password, u.anytls_password,
			u.description, u.tag, u.telegram_id, u.email, u.hwid_device_limit, u.external_squad_uuid,
			u.last_triggered_threshold, u.created_at, u.updated_at,
			COALESCE(ut.used_traffic_bytes, 0), COALESCE(ut.lifetime_used_traffic_bytes, 0),
			ut.online_at, ut.last_connected_node_uuid, ut.first_connected_at
		FROM users u
		LEFT JOIN user_traffic ut ON ut.id = u.id
		WHERE %s
		ORDER BY u.id ASC
		LIMIT $%d
	`, whereStmt, limitIdx)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, false, err
	}
	defer rows.Close()

	records := make([]userRecord, 0)
	for rows.Next() {
		rec, scanErr := scanUserRecord(rows)
		if scanErr != nil {
			return nil, nil, false, scanErr
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, false, err
	}

	hasMore := len(records) > size
	if hasMore {
		records = records[:size]
	}

	var nextCursor *string
	if hasMore && len(records) > 0 {
		c := strconv.FormatInt(records[len(records)-1].ID, 10)
		nextCursor = &c
	}

	return records, nextCursor, hasMore, nil
}

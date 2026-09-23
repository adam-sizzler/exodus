package users

import (
	"context"
	"errors"
	"strings"

	exodusdb "exodus/internal/db"

	"github.com/jackc/pgx/v5"
)

func (r *UserRepository) queryLimitedUserNodeUUIDsTx(ctx context.Context, tx pgx.Tx, userUUIDs []string) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT cpitn.node_uuid
		FROM users u
		JOIN internal_squad_members ism ON ism.user_id = u.id
		JOIN internal_squad_inbounds isi ON isi.internal_squad_uuid = ism.internal_squad_uuid
		JOIN config_profile_inbounds_to_nodes cpitn ON cpitn.config_profile_inbound_uuid = isi.inbound_uuid
		WHERE u.status = 'LIMITED' AND u.uuid = ANY($1)
	`, dedupeStrings(userUUIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanNodeUUIDRows(rows)
}

func (r *UserRepository) queryAllLimitedUserNodeUUIDsTx(ctx context.Context, tx pgx.Tx) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT cpitn.node_uuid
		FROM users u
		JOIN internal_squad_members ism ON ism.user_id = u.id
		JOIN internal_squad_inbounds isi ON isi.internal_squad_uuid = ism.internal_squad_uuid
		JOIN config_profile_inbounds_to_nodes cpitn ON cpitn.config_profile_inbound_uuid = isi.inbound_uuid
		WHERE u.status = 'LIMITED'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanNodeUUIDRows(rows)
}

func (r *UserRepository) queryReactivatedExpiredUserNodeUUIDsTx(ctx context.Context, tx pgx.Tx, userUUIDs []string, extendDays int) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT cpitn.node_uuid
		FROM users u
		JOIN internal_squad_members ism ON ism.user_id = u.id
		JOIN internal_squad_inbounds isi ON isi.internal_squad_uuid = ism.internal_squad_uuid
		JOIN config_profile_inbounds_to_nodes cpitn ON cpitn.config_profile_inbound_uuid = isi.inbound_uuid
		WHERE u.status = 'EXPIRED'
		  AND u.uuid = ANY($1)
		  AND u.expire_at + ($2::int * INTERVAL '1 day') > CURRENT_TIMESTAMP
	`, dedupeStrings(userUUIDs), extendDays)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanNodeUUIDRows(rows)
}

func (r *UserRepository) queryAllReactivatedExpiredUserNodeUUIDsTx(ctx context.Context, tx pgx.Tx, extendDays int) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT cpitn.node_uuid
		FROM users u
		JOIN internal_squad_members ism ON ism.user_id = u.id
		JOIN internal_squad_inbounds isi ON isi.internal_squad_uuid = ism.internal_squad_uuid
		JOIN config_profile_inbounds_to_nodes cpitn ON cpitn.config_profile_inbound_uuid = isi.inbound_uuid
		WHERE u.status = 'EXPIRED'
		  AND u.expire_at + ($1::int * INTERVAL '1 day') > CURRENT_TIMESTAMP
	`, extendDays)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanNodeUUIDRows(rows)
}

func scanNodeUUIDRows(rows pgx.Rows) ([]string, error) {
	nodeUUIDs := make([]string, 0)
	for rows.Next() {
		var nodeUUID string
		if err := rows.Scan(&nodeUUID); err != nil {
			return nil, err
		}
		nodeUUIDs = append(nodeUUIDs, strings.TrimSpace(nodeUUID))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return dedupeStrings(nodeUUIDs), nil
}

func (r *UserRepository) queryLimitedUserUUIDsTx(ctx context.Context, tx pgx.Tx, userUUIDs []string) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT uuid::text FROM users WHERE status = 'LIMITED' AND uuid = ANY($1)`, dedupeStrings(userUUIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStringRows(rows)
}

func (r *UserRepository) queryAllLimitedUserUUIDsTx(ctx context.Context, tx pgx.Tx) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT uuid::text FROM users WHERE status = 'LIMITED'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStringRows(rows)
}

func scanStringRows(rows pgx.Rows) ([]string, error) {
	items := make([]string, 0)
	for rows.Next() {
		var item string
		if err := rows.Scan(&item); err != nil {
			return nil, err
		}
		items = append(items, strings.TrimSpace(item))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return dedupeStrings(items), nil
}

func (r *UserRepository) resetUserTraffic(ctx context.Context, identifier string) (userRecord, []string, bool, error) {
	idNum, _ := parseNumericID(identifier)

	query := `
		WITH target_user AS (
			SELECT id, uuid, status
			FROM users
			WHERE (id = $1 OR uuid::text = $2 OR short_uuid = $2 OR username = $2)
			LIMIT 1
		),
		node_targets AS (
			SELECT DISTINCT cpitn.node_uuid
			FROM target_user tu
			JOIN internal_squad_members ism ON ism.user_id = tu.id
			JOIN internal_squad_inbounds isi ON isi.internal_squad_uuid = ism.internal_squad_uuid
			JOIN config_profile_inbounds_to_nodes cpitn ON cpitn.config_profile_inbound_uuid = isi.inbound_uuid
			WHERE tu.status = 'LIMITED'
		),
		upd_users AS (
			UPDATE users u
			SET last_traffic_reset_at = CURRENT_TIMESTAMP,
				last_triggered_threshold = 0,
				status = CASE WHEN u.status = 'LIMITED' THEN 'ACTIVE' ELSE u.status END,
				updated_at = CURRENT_TIMESTAMP
			FROM target_user tu
			WHERE u.id = tu.id
			RETURNING u.id, u.uuid, u.short_uuid, u.username, u.status, u.traffic_limit_bytes,
			          u.traffic_limit_strategy, u.expire_at, u.last_traffic_reset_at,
			          u.sub_revoked_at, u.trojan_password, u.vless_uuid, u.ss_password,
			          u.naive_password, u.shadowtls_password, u.hysteria2_password, u.anytls_password,
			          u.description, u.tag, u.telegram_id, u.email, u.hwid_device_limit, u.external_squad_uuid,
			          u.last_triggered_threshold, u.created_at, u.updated_at,
			          tu.status AS old_status
		),
		upd_traffic AS (
			UPDATE user_traffic ut
			SET used_traffic_bytes = 0
			FROM upd_users uu
			WHERE ut.id = uu.id
			RETURNING ut.id, ut.used_traffic_bytes, ut.lifetime_used_traffic_bytes,
			          ut.online_at, ut.last_connected_node_uuid, ut.first_connected_at
		)
		SELECT 
			uu.id, uu.uuid, uu.short_uuid, uu.username, uu.status, uu.traffic_limit_bytes,
			uu.traffic_limit_strategy, uu.expire_at, uu.last_traffic_reset_at,
			uu.sub_revoked_at, uu.trojan_password, uu.vless_uuid, uu.ss_password,
			uu.naive_password, uu.shadowtls_password, uu.hysteria2_password, uu.anytls_password,
			uu.description, uu.tag, uu.telegram_id, uu.email, uu.hwid_device_limit, uu.external_squad_uuid,
			uu.last_triggered_threshold, uu.created_at, uu.updated_at,
			COALESCE(ut.used_traffic_bytes, 0), COALESCE(ut.lifetime_used_traffic_bytes, 0),
			ut.online_at, ut.last_connected_node_uuid, ut.first_connected_at,
			uu.old_status,
			COALESCE((SELECT array_agg(node_uuid) FROM node_targets), '{}'::text[]) AS node_uuids
		FROM upd_users uu
		LEFT JOIN upd_traffic ut ON ut.id = uu.id;
	`

	var record userRecord
	var oldStatus string
	var rawNodeUUIDs []string

	row := r.db.QueryRow(ctx, query, idNum, identifier)
	err := row.Scan(
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
		&oldStatus,
		&rawNodeUUIDs,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return userRecord{}, nil, false, errUserNotFound
	}
	if err != nil {
		return userRecord{}, nil, false, err
	}

	nodeUUIDs := dedupeStrings(rawNodeUUIDs)
	reactivated := oldStatus == "LIMITED"
	return record, nodeUUIDs, reactivated, nil
}

func (r *UserRepository) resetUsersTrafficByUUIDs(ctx context.Context, userUUIDs []string) (int64, []string, []string, error) {
	var affectedRows int64
	var nodeUUIDs []string
	var reactivatedUUIDs []string

	err := exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		var err error
		nodeUUIDs, err = r.queryLimitedUserNodeUUIDsTx(ctx, tx, userUUIDs)
		if err != nil {
			return err
		}

		reactivatedUUIDs, err = r.queryLimitedUserUUIDsTx(ctx, tx, userUUIDs)
		if err != nil {
			return err
		}

		result, err := tx.Exec(ctx, `
			WITH upd_users AS (
				UPDATE users
				SET last_traffic_reset_at = CURRENT_TIMESTAMP,
					last_triggered_threshold = 0,
					status = CASE WHEN status = 'LIMITED' THEN 'ACTIVE' ELSE status END,
					updated_at = CURRENT_TIMESTAMP
				WHERE uuid = ANY($1)
				RETURNING id
			)
			UPDATE user_traffic
			SET used_traffic_bytes = 0
			FROM upd_users
			WHERE user_traffic.id = upd_users.id
		`, dedupeStrings(userUUIDs))
		if err != nil {
			return err
		}
		affectedRows = result.RowsAffected()

		return nil
	})
	if err != nil {
		return 0, nil, nil, err
	}

	return affectedRows, nodeUUIDs, reactivatedUUIDs, nil
}

func (r *UserRepository) resetAllUsersTraffic(ctx context.Context) (int64, []string, []string, error) {
	var affectedRows int64
	var nodeUUIDs []string
	var reactivatedUUIDs []string

	err := exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		var err error
		nodeUUIDs, err = r.queryAllLimitedUserNodeUUIDsTx(ctx, tx)
		if err != nil {
			return err
		}

		reactivatedUUIDs, err = r.queryAllLimitedUserUUIDsTx(ctx, tx)
		if err != nil {
			return err
		}

		result, err := tx.Exec(ctx, `
			WITH upd_users AS (
				UPDATE users
				SET last_traffic_reset_at = CURRENT_TIMESTAMP,
					last_triggered_threshold = 0,
					status = CASE WHEN status = 'LIMITED' THEN 'ACTIVE' ELSE status END,
					updated_at = CURRENT_TIMESTAMP
				RETURNING id
			)
			UPDATE user_traffic
			SET used_traffic_bytes = 0
			FROM upd_users
			WHERE user_traffic.id = upd_users.id
		`)
		if err != nil {
			return err
		}
		affectedRows = result.RowsAffected()

		return nil
	})
	if err != nil {
		return 0, nil, nil, err
	}

	return affectedRows, nodeUUIDs, reactivatedUUIDs, nil
}

func (r *UserRepository) extendUsersExpirationByUUIDs(ctx context.Context, userUUIDs []string, extendDays int) (int64, []string, error) {
	var affectedRows int64
	var nodeUUIDs []string

	err := exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		var err error
		nodeUUIDs, err = r.queryReactivatedExpiredUserNodeUUIDsTx(ctx, tx, userUUIDs, extendDays)
		if err != nil {
			return err
		}

		result, err := tx.Exec(ctx, `
			UPDATE users
			SET expire_at = CASE
					WHEN status = 'EXPIRED' THEN CURRENT_TIMESTAMP + ($1::int * INTERVAL '1 day')
					ELSE expire_at + ($1::int * INTERVAL '1 day')
				END,
				status = CASE
					WHEN status = 'EXPIRED' THEN 'ACTIVE'
					ELSE status
				END,
				updated_at = CURRENT_TIMESTAMP
			WHERE uuid = ANY($2)
		`, extendDays, dedupeStrings(userUUIDs))
		if err != nil {
			return err
		}
		affectedRows = result.RowsAffected()

		return nil
	})
	if err != nil {
		return 0, nil, err
	}

	return affectedRows, nodeUUIDs, nil
}

func (r *UserRepository) extendAllUsersExpiration(ctx context.Context, extendDays int) (int64, []string, error) {
	var affectedRows int64
	var nodeUUIDs []string

	err := exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		var err error
		nodeUUIDs, err = r.queryAllReactivatedExpiredUserNodeUUIDsTx(ctx, tx, extendDays)
		if err != nil {
			return err
		}

		result, err := tx.Exec(ctx, `
			UPDATE users
			SET expire_at = expire_at + ($1::int * INTERVAL '1 day'),
				status = CASE
					WHEN status = 'EXPIRED' AND expire_at + ($2::int * INTERVAL '1 day') > CURRENT_TIMESTAMP THEN 'ACTIVE'
					ELSE status
				END,
				updated_at = CURRENT_TIMESTAMP
		`, extendDays, extendDays)
		if err != nil {
			return err
		}
		affectedRows = result.RowsAffected()

		return nil
	})
	if err != nil {
		return 0, nil, err
	}

	return affectedRows, nodeUUIDs, nil
}

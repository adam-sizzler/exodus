package configprofiles

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	exodusdb "exodus/internal/db"
	"exodus/internal/httpapi/shared"
)

type ConfigProfileRepository struct {
	db *sql.DB
}

func NewConfigProfileRepository(db *sql.DB) *ConfigProfileRepository {
	return &ConfigProfileRepository{db: db}
}

func (r *ConfigProfileRepository) getAllConfigProfileRecords(ctx context.Context) ([]configProfileRecord, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT uuid, view_position, name, tags, config, created_at, updated_at
		FROM config_profiles
		ORDER BY view_position ASC, name ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]configProfileRecord, 0)
	for rows.Next() {
		record, scanErr := r.scanConfigProfileRecord(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func (r *ConfigProfileRepository) getConfigProfileRecordByUUID(ctx context.Context, profileUUID string) (configProfileRecord, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT uuid, view_position, name, tags, config, created_at, updated_at
		FROM config_profiles
		WHERE uuid = $1
	`, profileUUID)
	record, scanErr := r.scanConfigProfileRecord(row)
	if scanErr == sql.ErrNoRows {
		return record, errConfigProfileNotFound
	}
	return record, scanErr
}

func (r *ConfigProfileRepository) scanConfigProfileRecord(scanner shared.RowScanner) (configProfileRecord, error) {
	var record configProfileRecord
	var viewPosition sql.NullInt64
	var configRaw []byte
	var tags exodusdb.StringArray
	if err := scanner.Scan(&record.UUID, &viewPosition, &record.Name, &tags, &configRaw, &record.CreatedAt, &record.UpdatedAt); err != nil {
		return record, err
	}
	if viewPosition.Valid {
		record.ViewPosition = int(viewPosition.Int64)
	}
	record.Tags = tags.Slice()
	if record.Tags == nil {
		record.Tags = []string{}
	}
	record.Config = json.RawMessage(configRaw)
	return record, nil
}

func (r *ConfigProfileRepository) getConfigProfileInboundsMap(ctx context.Context, profileUUIDs []string) (map[string][]ConfigProfileInbound, error) {
	result := make(map[string][]ConfigProfileInbound, len(profileUUIDs))
	if len(profileUUIDs) == 0 {
		return result, nil
	}
	for _, profileUUID := range profileUUIDs {
		result[profileUUID] = make([]ConfigProfileInbound, 0)
	}

	activeSquadsByInbound := make(map[string][]string)
	rows, err := r.db.QueryContext(ctx, `
		SELECT isi.inbound_uuid, isi.internal_squad_uuid
		FROM internal_squad_inbounds isi
		JOIN config_profile_inbounds cpi ON cpi.uuid = isi.inbound_uuid
		WHERE cpi.profile_uuid = ANY($1)
	`, profileUUIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var inboundUUID, squadUUID string
		if err := rows.Scan(&inboundUUID, &squadUUID); err != nil {
			return nil, err
		}
		activeSquadsByInbound[inboundUUID] = append(activeSquadsByInbound[inboundUUID], squadUUID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	inboundRows, err := r.db.QueryContext(ctx, `
		SELECT uuid, profile_uuid, tag, type, network, security, port, raw_inbound
		FROM config_profile_inbounds
		WHERE profile_uuid = ANY($1)
		ORDER BY tag ASC
	`, profileUUIDs)
	if err != nil {
		return nil, err
	}
	defer inboundRows.Close()

	for inboundRows.Next() {
		var (
			inbound     ConfigProfileInbound
			networkVal  sql.NullString
			securityVal sql.NullString
			portVal     sql.NullInt64
			rawInbound  []byte
		)
		if err := inboundRows.Scan(&inbound.UUID, &inbound.ProfileUUID, &inbound.Tag, &inbound.Type, &networkVal, &securityVal, &portVal, &rawInbound); err != nil {
			return nil, err
		}
		if networkVal.Valid {
			inbound.Network = &networkVal.String
		}
		if securityVal.Valid {
			inbound.Security = &securityVal.String
		}
		if portVal.Valid {
			portInt := int(portVal.Int64)
			inbound.Port = &portInt
		}
		inbound.RawInbound = json.RawMessage(rawInbound)
		if squads, ok := activeSquadsByInbound[inbound.UUID]; ok {
			inbound.ActiveSquads = dedupeStrings(squads)
		} else {
			inbound.ActiveSquads = []string{}
		}
		result[inbound.ProfileUUID] = append(result[inbound.ProfileUUID], inbound)
	}
	if err := inboundRows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *ConfigProfileRepository) getConfigProfileNodesMap(ctx context.Context, profileUUIDs []string) (map[string][]ConfigProfileNode, error) {
	result := make(map[string][]ConfigProfileNode, len(profileUUIDs))
	if len(profileUUIDs) == 0 {
		return result, nil
	}
	for _, profileUUID := range profileUUIDs {
		result[profileUUID] = make([]ConfigProfileNode, 0)
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT active_config_profile_uuid, uuid, name, country_code
		FROM nodes
		WHERE active_config_profile_uuid = ANY($1)
		ORDER BY view_position ASC, name ASC
	`, profileUUIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var profileUUID string
		var node ConfigProfileNode
		if err := rows.Scan(&profileUUID, &node.UUID, &node.Name, &node.CountryCode); err != nil {
			return nil, err
		}
		result[profileUUID] = append(result[profileUUID], node)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *ConfigProfileRepository) createConfigProfile(ctx context.Context, profileUUID string, req createConfigProfileRequest) error {
	tags := shared.SanitizeTags(req.Tags)
	return exodusdb.WithRetrySqlTx(ctx, r.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO config_profiles (uuid, name, tags, config, created_at, updated_at)
			VALUES ($1, $2, $3::text[], $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		`, profileUUID, strings.TrimSpace(req.Name), shared.PostgresTextArrayLiteral(tags), req.Config); err != nil {
			return err
		}

		if _, err := exodusdb.SyncConfigProfileInboundsSqlTx(ctx, tx, profileUUID, req.Config); err != nil {
			return err
		}
		return nil
	})
}

func (r *ConfigProfileRepository) getAllTags(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT DISTINCT unnest(tags) AS tag
		FROM config_profiles
		WHERE tags IS NOT NULL AND cardinality(tags) > 0
		ORDER BY tag ASC
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
		if trimmed := strings.TrimSpace(tag); trimmed != "" {
			tags = append(tags, trimmed)
		}
	}
	return tags, rows.Err()
}

func (r *ConfigProfileRepository) setTags(ctx context.Context, profileUUID string, tags []string) error {
	sanitized := shared.SanitizeTags(tags)
	result, err := r.db.ExecContext(ctx, `
		UPDATE config_profiles
		SET tags = $1::text[], updated_at = CURRENT_TIMESTAMP
		WHERE uuid = $2
	`, shared.PostgresTextArrayLiteral(sanitized), profileUUID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return errConfigProfileNotFound
	}
	return nil
}

func (r *ConfigProfileRepository) updateConfigProfile(ctx context.Context, profileUUID string, clauses []string, args []any, updateConfig *json.RawMessage) error {
	return exodusdb.WithRetrySqlTx(ctx, r.db, func(tx *sql.Tx) error {
		if len(clauses) > 0 {
			txArgs := append(append([]any{}, args...), profileUUID)
			result, err := tx.ExecContext(ctx, fmt.Sprintf(`
				UPDATE config_profiles
				SET %s, updated_at = CURRENT_TIMESTAMP
				WHERE uuid = $%d
			`, strings.Join(clauses, ", "), len(txArgs)), txArgs...)
			if err != nil {
				return err
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if rows == 0 {
				return errConfigProfileNotFound
			}
		}

		if updateConfig != nil {
			if _, err := exodusdb.SyncConfigProfileInboundsSqlTx(ctx, tx, profileUUID, *updateConfig); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *ConfigProfileRepository) deleteConfigProfile(ctx context.Context, profileUUID string) error {
	result, err := r.db.ExecContext(ctx, `DELETE FROM config_profiles WHERE uuid = $1`, profileUUID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return errConfigProfileNotFound
	}
	return nil
}

func (r *ConfigProfileRepository) reorderConfigProfiles(ctx context.Context, items []reorderConfigProfilesItem) error {
	if len(items) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// Single batched UPDATE via UNNEST instead of one round-trip per profile.
	uuids := make([]string, len(items))
	positions := make([]int32, len(items))
	for i, item := range items {
		uuids[i] = item.UUID
		positions[i] = int32(item.ViewPosition)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE config_profiles AS c
		SET view_position = v.view_position
		FROM (
			SELECT unnest($1::uuid[]) AS uuid, unnest($2::int[]) AS view_position
		) AS v
		WHERE c.uuid = v.uuid
	`, uuids, positions); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `SELECT setval('config_profiles_view_position_seq', (SELECT COALESCE(MAX(view_position), 0) FROM config_profiles) + 1)`); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *ConfigProfileRepository) SyncConfigProfileInboundsTx(ctx context.Context, tx *sql.Tx, profileUUID string, configJSON json.RawMessage) (int, error) {
	return exodusdb.SyncConfigProfileInboundsSqlTx(ctx, tx, profileUUID, configJSON)
}

func (r *ConfigProfileRepository) getSnippets(ctx context.Context) ([]ConfigProfileSnippet, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT name, snippet, created_at
		FROM config_profile_snippets
		ORDER BY name ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]ConfigProfileSnippet, 0)
	for rows.Next() {
		var snippet ConfigProfileSnippet
		if scanErr := rows.Scan(&snippet.Name, &snippet.Snippet, &snippet.CreatedAt); scanErr != nil {
			return nil, scanErr
		}
		result = append(result, snippet)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *ConfigProfileRepository) createSnippet(ctx context.Context, req configProfileSnippetRequest) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO config_profile_snippets (name, snippet)
		VALUES ($1, $2::jsonb)`, req.Name, string(req.Snippet))
	return err
}

func (r *ConfigProfileRepository) updateSnippet(ctx context.Context, req configProfileSnippetRequest) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE config_profile_snippets
		SET snippet = $1::jsonb
		WHERE name = $2`, string(req.Snippet), req.Name)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return errConfigProfileSnippetNotFound
	}
	return nil
}

func (r *ConfigProfileRepository) deleteSnippet(ctx context.Context, name string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM config_profile_snippets WHERE name = $1`, name)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return errConfigProfileSnippetNotFound
	}
	return nil
}

package externalsquads

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"exodus/internal/httpapi/shared"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ExternalSquadRecord struct {
	UUID                  string          `json:"uuid"`
	ViewPosition          int             `json:"view_position"`
	Name                  string          `json:"name"`
	Tags                  []string        `json:"tags"`
	SubscriptionSettings  json.RawMessage `json:"subscription_settings,omitempty"`
	HostOverrides         json.RawMessage `json:"host_overrides,omitempty"`
	ResponseHeadersAdd    json.RawMessage `json:"response_headers_add,omitempty"`
	ResponseHeadersRemove json.RawMessage `json:"response_headers_remove,omitempty"`
	HWIDSettings          json.RawMessage `json:"hwid_settings,omitempty"`
	CustomRemarks         json.RawMessage `json:"custom_remarks,omitempty"`
	SubpageConfigUUID     *string         `json:"subpage_config_uuid,omitempty"`
	CreatedAt             time.Time       `json:"created_at"`
	UpdatedAt             time.Time       `json:"updated_at"`
}

func getExternalSquads(ctx context.Context, dbConn *pgxpool.Pool) ([]ExternalSquadRecord, error) {
	rows, err := dbConn.Query(ctx, `
		SELECT uuid, view_position, name, tags,
			subscription_settings, host_overrides, response_headers_add,
			array_to_json(COALESCE(response_headers_remove, ARRAY[]::text[]))::text AS response_headers_remove,
			hwid_settings, custom_remarks, subpage_config_uuid,
			created_at, updated_at
		FROM external_squads
		ORDER BY view_position ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]ExternalSquadRecord, 0)
	for rows.Next() {
		rec, err := scanExternalSquad(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}

	return records, rows.Err()
}

// getExternalSquadsMembersCount batch-loads member counts for a set of external squads
// in a single query, instead of one COUNT(*) query per squad.
func getExternalSquadsMembersCount(ctx context.Context, db *pgxpool.Pool, squadUUIDs []string) (map[string]int, error) {
	result := make(map[string]int, len(squadUUIDs))
	if len(squadUUIDs) == 0 {
		return result, nil
	}

	rows, err := db.Query(ctx, `
		SELECT external_squad_uuid, COUNT(*)
		FROM users
		WHERE external_squad_uuid = ANY($1)
		GROUP BY external_squad_uuid
	`, squadUUIDs)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	for rows.Next() {
		var uuid string
		var count int
		if err := rows.Scan(&uuid, &count); err != nil {
			return result, err
		}
		result[uuid] = count
	}
	return result, rows.Err()
}

// getExternalSquadsTemplates batch-loads templates for a set of external squads
// in a single query, instead of one query per squad.
func getExternalSquadsTemplates(ctx context.Context, db *pgxpool.Pool, squadUUIDs []string) (map[string][]ExternalSquadTemplate, error) {
	result := make(map[string][]ExternalSquadTemplate, len(squadUUIDs))
	for _, id := range squadUUIDs {
		result[id] = make([]ExternalSquadTemplate, 0)
	}
	if len(squadUUIDs) == 0 {
		return result, nil
	}

	rows, err := db.Query(ctx, `
		SELECT external_squad_uuid, template_uuid, template_type
		FROM external_squads_templates
		WHERE external_squad_uuid = ANY($1)
	`, squadUUIDs)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	for rows.Next() {
		var squadUUID string
		var t ExternalSquadTemplate
		if err := rows.Scan(&squadUUID, &t.TemplateUUID, &t.TemplateType); err != nil {
			return result, err
		}
		result[squadUUID] = append(result[squadUUID], t)
	}
	return result, rows.Err()
}

func getExternalSquadByUUID(ctx context.Context, dbConn *pgxpool.Pool, squadUUID string) (ExternalSquadRecord, error) {
	row := dbConn.QueryRow(ctx, `
		SELECT uuid, view_position, name, tags,
			subscription_settings, host_overrides, response_headers_add,
			array_to_json(COALESCE(response_headers_remove, ARRAY[]::text[]))::text AS response_headers_remove,
			hwid_settings, custom_remarks, subpage_config_uuid,
			created_at, updated_at
		FROM external_squads
		WHERE uuid = $1
	`, squadUUID)

	return scanExternalSquad(row)
}

func getAllTags(ctx context.Context, dbConn *pgxpool.Pool) ([]string, error) {
	rows, err := dbConn.Query(ctx, `
		SELECT DISTINCT unnest(tags) AS tag
		FROM external_squads
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

func setTags(ctx context.Context, dbConn *pgxpool.Pool, squadUUID string, tags []string) error {
	sanitized := shared.SanitizeTags(tags)
	result, err := dbConn.Exec(ctx, `
		UPDATE external_squads
		SET tags = $1::text[], updated_at = CURRENT_TIMESTAMP
		WHERE uuid = $2
	`, shared.PostgresTextArrayLiteral(sanitized), squadUUID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func scanExternalSquad(scanner shared.RowScanner) (ExternalSquadRecord, error) {
	var rec ExternalSquadRecord
	var tags []string
	var subSettings, hostOverrides, respHeadersAdd, respHeadersRemove, hwidSettings, customRemarks *string
	var subpageConfigUUID *string

	err := scanner.Scan(
		&rec.UUID,
		&rec.ViewPosition,
		&rec.Name,
		&tags,
		&subSettings,
		&hostOverrides,
		&respHeadersAdd,
		&respHeadersRemove,
		&hwidSettings,
		&customRemarks,
		&subpageConfigUUID,
		&rec.CreatedAt,
		&rec.UpdatedAt,
	)
	if err != nil {
		return rec, err
	}

	rec.Tags = tags
	if rec.Tags == nil {
		rec.Tags = []string{}
	}

	rec.SubscriptionSettings = parseJSONRaw(subSettings)
	rec.HostOverrides = parseJSONRaw(hostOverrides)
	rec.ResponseHeadersAdd = parseJSONRaw(respHeadersAdd)
	rec.ResponseHeadersRemove = parseJSONRaw(respHeadersRemove)
	rec.HWIDSettings = parseJSONRaw(hwidSettings)
	rec.CustomRemarks = parseJSONRaw(customRemarks)
	rec.SubpageConfigUUID = subpageConfigUUID

	return rec, nil
}

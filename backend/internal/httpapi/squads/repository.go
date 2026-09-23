package squads

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	exodusdb "exodus/internal/db"
	"exodus/internal/httpapi/shared"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SquadRepository struct {
	db *pgxpool.Pool
}

func NewSquadRepository(db *pgxpool.Pool) *SquadRepository {
	return &SquadRepository{db: db}
}

func (r *SquadRepository) getSquads(ctx context.Context) ([]InternalSquad, error) {
	rows, err := r.db.Query(ctx, `
		SELECT uuid, view_position, name, tags, created_at, updated_at
		FROM internal_squads
		ORDER BY view_position ASC, name ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	squads := []InternalSquad{}
	for rows.Next() {
		squad, err := scanInternalSquad(rows)
		if err != nil {
			return nil, err
		}
		squads = append(squads, squad)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return squads, nil
}

func (r *SquadRepository) getSquadByUUID(ctx context.Context, squadUUID string) (InternalSquad, error) {
	row := r.db.QueryRow(ctx, `
		SELECT uuid, view_position, name, tags, created_at, updated_at
		FROM internal_squads WHERE uuid = $1
	`, squadUUID)
	return scanInternalSquad(row)
}

func (r *SquadRepository) getSquadMembersCount(ctx context.Context, squadUUID string) (int, error) {
	var count int
	err := r.db.QueryRow(ctx, `
		SELECT COUNT(*) FROM internal_squad_members WHERE internal_squad_uuid = $1
	`, squadUUID).Scan(&count)
	return count, err
}

func (r *SquadRepository) getSquadInbounds(ctx context.Context, squadUUID string) ([]InternalSquadInboundAPI, error) {
	rows, err := r.db.Query(ctx, `
		SELECT cpi.uuid, cpi.profile_uuid, cpi.tag, cpi.type, cpi.network, cpi.security, cpi.port, cpi.raw_inbound
		FROM internal_squad_inbounds isi
		JOIN config_profile_inbounds cpi ON cpi.uuid = isi.inbound_uuid
		WHERE isi.internal_squad_uuid = $1
		ORDER BY cpi.tag ASC
	`, squadUUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	inbounds := []InternalSquadInboundAPI{}
	for rows.Next() {
		var inbound InternalSquadInboundAPI
		var network, security, rawInbound *string
		var port *int
		if scanErr := rows.Scan(
			&inbound.UUID,
			&inbound.ProfileUUID,
			&inbound.Tag,
			&inbound.Type,
			&network,
			&security,
			&port,
			&rawInbound,
		); scanErr != nil {
			return nil, scanErr
		}
		inbound.Network = network
		inbound.Security = security
		inbound.Port = port
		if rawInbound != nil && *rawInbound != "" {
			inbound.RawInbound = json.RawMessage(*rawInbound)
		}
		inbounds = append(inbounds, inbound)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return inbounds, nil
}

func (r *SquadRepository) getSquadAccessibleNodes(ctx context.Context, squadUUID string) ([]InternalSquadAccessibleNode, error) {
	var exists int
	if err := r.db.QueryRow(ctx, `SELECT 1 FROM internal_squads WHERE uuid = $1`, squadUUID).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errInternalSquadNotFound
		}
		return nil, err
	}

	rows, err := r.db.Query(ctx, `
		SELECT
			n.uuid,
			n.name,
			n.country_code,
			cp.uuid,
			cp.name,
			cpi.tag
		FROM internal_squad_inbounds isi
		INNER JOIN config_profile_inbounds cpi ON cpi.uuid = isi.inbound_uuid
		INNER JOIN config_profiles cp ON cp.uuid = cpi.profile_uuid
		INNER JOIN config_profile_inbounds_to_nodes cpin
			ON cpin.config_profile_inbound_uuid = cpi.uuid
		INNER JOIN nodes n
			ON n.uuid = cpin.node_uuid
			AND n.active_config_profile_uuid = cp.uuid
		WHERE isi.internal_squad_uuid = $1
		ORDER BY n.view_position ASC, n.name ASC, cpi.tag ASC
	`, squadUUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nodes := make([]InternalSquadAccessibleNode, 0)
	indexByNode := make(map[string]int)
	inboundSeenByNode := make(map[string]map[string]bool)
	for rows.Next() {
		var nodeUUID, nodeName, countryCode, profileUUID, profileName, inboundTag string
		if err := rows.Scan(&nodeUUID, &nodeName, &countryCode, &profileUUID, &profileName, &inboundTag); err != nil {
			return nil, err
		}
		idx, ok := indexByNode[nodeUUID]
		if !ok {
			nodes = append(nodes, InternalSquadAccessibleNode{
				UUID:              nodeUUID,
				NodeName:          nodeName,
				CountryCode:       countryCode,
				ConfigProfileUUID: profileUUID,
				ConfigProfileName: profileName,
				ActiveInbounds:    make([]string, 0),
			})
			idx = len(nodes) - 1
			indexByNode[nodeUUID] = idx
			inboundSeenByNode[nodeUUID] = make(map[string]bool)
		}
		if !inboundSeenByNode[nodeUUID][inboundTag] {
			nodes[idx].ActiveInbounds = append(nodes[idx].ActiveInbounds, inboundTag)
			inboundSeenByNode[nodeUUID][inboundTag] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (r *SquadRepository) getConfigProfilesWithInbounds(ctx context.Context) ([]ConfigProfileWithInbounds, error) {
	profileRows, err := r.db.Query(ctx, `
		SELECT uuid, view_position, name, config, created_at, updated_at
		FROM config_profiles
		ORDER BY view_position ASC, name ASC
	`)
	if err != nil {
		return nil, err
	}
	defer profileRows.Close()

	type tempProfile struct {
		UUID         string
		ViewPosition int
		Name         string
		Config       json.RawMessage
		CreatedAt    time.Time
		UpdatedAt    time.Time
	}
	var tempProfiles []tempProfile

	for profileRows.Next() {
		var tp tempProfile
		var viewPosition *int
		var configStr *string

		if err := profileRows.Scan(&tp.UUID, &viewPosition, &tp.Name, &configStr, &tp.CreatedAt, &tp.UpdatedAt); err != nil {
			return nil, err
		}

		if viewPosition != nil {
			tp.ViewPosition = *viewPosition
		}
		if configStr != nil {
			tp.Config = json.RawMessage(*configStr)
		}

		tempProfiles = append(tempProfiles, tp)
	}
	if err := profileRows.Err(); err != nil {
		return nil, err
	}

	inboundRows, err := r.db.Query(ctx, `
		SELECT uuid, profile_uuid, tag, type, network, security, port, raw_inbound
		FROM config_profile_inbounds
		ORDER BY profile_uuid, tag
	`)
	if err != nil {
		return nil, err
	}
	defer inboundRows.Close()

	inboundsByProfile := make(map[string][]InboundInfo)
	for inboundRows.Next() {
		var ib InboundInfo
		var profileUUID string
		var network, security, rawInbound *string
		var port *int

		if err := inboundRows.Scan(&ib.UUID, &profileUUID, &ib.Tag, &ib.Type, &network, &security, &port, &rawInbound); err != nil {
			return nil, err
		}

		ib.Network = network
		ib.Security = security
		ib.Port = port
		if rawInbound != nil && *rawInbound != "" {
			ib.RawInbound = json.RawMessage(*rawInbound)
		}

		inboundsByProfile[profileUUID] = append(inboundsByProfile[profileUUID], ib)
	}
	if err := inboundRows.Err(); err != nil {
		return nil, err
	}

	var profiles []ConfigProfileWithInbounds
	for _, tp := range tempProfiles {
		ibs := inboundsByProfile[tp.UUID]
		if ibs == nil {
			ibs = make([]InboundInfo, 0)
		}
		profiles = append(profiles, ConfigProfileWithInbounds{
			UUID:         tp.UUID,
			Name:         tp.Name,
			ViewPosition: tp.ViewPosition,
			Config:       tp.Config,
			Inbounds:     ibs,
			CreatedAt:    tp.CreatedAt,
			UpdatedAt:    tp.UpdatedAt,
		})
	}

	return profiles, nil
}

func (r *SquadRepository) getInboundAssignments(ctx context.Context, nodeUUID string) ([]ConfigProfileInboundToNode, error) {
	var query string
	var args []interface{}

	if nodeUUID != "" {
		query = `
			SELECT config_profile_inbound_uuid, node_uuid, created_at
			FROM config_profile_inbounds_to_nodes
			WHERE node_uuid = $1
			ORDER BY config_profile_inbound_uuid`
		args = []interface{}{nodeUUID}
	} else {
		query = `
			SELECT config_profile_inbound_uuid, node_uuid, created_at
			FROM config_profile_inbounds_to_nodes
			ORDER BY node_uuid, config_profile_inbound_uuid`
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var assignments []ConfigProfileInboundToNode
	for rows.Next() {
		var a ConfigProfileInboundToNode
		if err := rows.Scan(&a.ConfigProfileInboundUUID, &a.NodeUUID, &a.CreatedAt); err != nil {
			return nil, err
		}
		assignments = append(assignments, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return assignments, nil
}

func (r *SquadRepository) createSquad(ctx context.Context, squadUUID string, req InternalSquadCreateRequest) error {
	tags := shared.SanitizeTags(req.Tags)
	query := `
		INSERT INTO internal_squads (
			uuid, view_position, name, tags, created_at, updated_at
		) VALUES ($1, $2, $3, $4::text[], CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
	_, err := r.db.Exec(ctx, query, squadUUID, req.ViewPosition, req.Name, shared.PostgresTextArrayLiteral(tags))
	return err
}

func (r *SquadRepository) getAllTags(ctx context.Context) ([]string, error) {
	rows, err := r.db.Query(ctx, `
		SELECT DISTINCT unnest(tags) AS tag
		FROM internal_squads
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

func (r *SquadRepository) setTags(ctx context.Context, squadUUID string, tags []string) error {
	sanitized := shared.SanitizeTags(tags)
	tag, err := r.db.Exec(ctx, `
		UPDATE internal_squads
		SET tags = $1::text[], updated_at = CURRENT_TIMESTAMP
		WHERE uuid = $2
	`, shared.PostgresTextArrayLiteral(sanitized), squadUUID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errInternalSquadNotFound
	}
	return nil
}

func (r *SquadRepository) updateSquad(ctx context.Context, squadUUID string, clauses []string, args []any, inboundUUIDs []string) error {
	return exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		if len(clauses) > 0 {
			updateArgs := append(args, squadUUID)
			query := fmt.Sprintf("UPDATE internal_squads SET %s, updated_at = CURRENT_TIMESTAMP WHERE uuid = $%d", strings.Join(clauses, ", "), len(updateArgs))
			tag, err := tx.Exec(ctx, query, updateArgs...)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				return pgx.ErrNoRows
			}
		}

		if inboundUUIDs != nil {
			if err := r.replaceSquadInbounds(ctx, tx, squadUUID, inboundUUIDs); err != nil {
				return err
			}
		}

		return nil
	})
}

// replaceSquadInbounds clears and re-inserts a squad's inbounds within an existing
// transaction. Existence is validated in a single batch query and rows are inserted
// in a single batch statement.
func (r *SquadRepository) replaceSquadInbounds(ctx context.Context, tx pgx.Tx, squadUUID string, inboundUUIDs []string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM internal_squad_inbounds WHERE internal_squad_uuid = $1`, squadUUID); err != nil {
		return fmt.Errorf("failed to clear existing inbounds: %w", err)
	}

	cleaned := make([]string, 0, len(inboundUUIDs))
	seen := make(map[string]struct{}, len(inboundUUIDs))
	for _, inboundUUID := range inboundUUIDs {
		trimmed := strings.TrimSpace(inboundUUID)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		cleaned = append(cleaned, trimmed)
	}
	if len(cleaned) == 0 {
		return nil
	}

	rows, err := tx.Query(ctx, `SELECT uuid FROM config_profile_inbounds WHERE uuid = ANY($1)`, cleaned)
	if err != nil {
		return err
	}
	existing := make(map[string]struct{}, len(cleaned))
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			rows.Close()
			return err
		}
		existing[u] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, u := range cleaned {
		if _, ok := existing[u]; !ok {
			return fmt.Errorf("inbound not found: %s", u)
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO internal_squad_inbounds (internal_squad_uuid, inbound_uuid)
		SELECT $1, unnest($2::uuid[])
	`, squadUUID, cleaned); err != nil {
		return fmt.Errorf("failed to insert inbounds: %w", err)
	}

	return nil
}

func (r *SquadRepository) deleteSquad(ctx context.Context, squadUUID string) (string, error) {
	var squadName string
	if err := r.db.QueryRow(ctx, "SELECT name FROM internal_squads WHERE uuid = $1", squadUUID).Scan(&squadName); err != nil {
		return "", err
	}
	if _, err := r.db.Exec(ctx, "DELETE FROM internal_squads WHERE uuid = $1", squadUUID); err != nil {
		return "", err
	}
	return squadName, nil
}

func (r *SquadRepository) reorderSquads(ctx context.Context, items []reorderSquadItem) error {
	if len(items) == 0 {
		return nil
	}

	uuids := make([]string, len(items))
	positions := make([]int32, len(items))
	for i, item := range items {
		uuids[i] = item.UUID
		positions[i] = int32(item.ViewPosition)
	}

	return exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE internal_squads AS s
			SET view_position = v.view_position
			FROM (
				SELECT unnest($1::uuid[]) AS uuid, unnest($2::int[]) AS view_position
			) AS v
			WHERE s.uuid = v.uuid
		`, uuids, positions); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT setval('internal_squads_view_position_seq', (SELECT COALESCE(MAX(view_position), 0) FROM internal_squads) + 1)`); err != nil {
			return err
		}
		return nil
	})
}

func (r *SquadRepository) setInboundAssignments(ctx context.Context, nodeUUID string, inboundUUIDs []string) error {
	return exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM config_profile_inbounds_to_nodes WHERE node_uuid = $1`, nodeUUID); err != nil {
			return err
		}

		cleaned := make([]string, 0, len(inboundUUIDs))
		seen := make(map[string]struct{}, len(inboundUUIDs))
		for _, inboundUUID := range inboundUUIDs {
			trimmed := strings.TrimSpace(inboundUUID)
			if trimmed == "" {
				continue
			}
			if _, ok := seen[trimmed]; ok {
				continue
			}
			seen[trimmed] = struct{}{}
			cleaned = append(cleaned, trimmed)
		}

		if len(cleaned) > 0 {
			if _, err := tx.Exec(ctx, `
				INSERT INTO config_profile_inbounds_to_nodes (config_profile_inbound_uuid, node_uuid)
				SELECT unnest($1::uuid[]), $2::uuid
			`, cleaned, nodeUUID); err != nil {
				return err
			}
		}

		return nil
	})
}

func (r *SquadRepository) getSquadInboundBindings(ctx context.Context, squadUUID string) ([]InternalSquadInbound, error) {
	var query string
	var args []interface{}

	if squadUUID != "" {
		query = `
			SELECT internal_squad_uuid, inbound_uuid
			FROM internal_squad_inbounds
			WHERE internal_squad_uuid = $1
			ORDER BY inbound_uuid`
		args = []interface{}{squadUUID}
	} else {
		query = `
			SELECT internal_squad_uuid, inbound_uuid
			FROM internal_squad_inbounds
			ORDER BY internal_squad_uuid, inbound_uuid`
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var squadInbounds []InternalSquadInbound
	for rows.Next() {
		var si InternalSquadInbound
		if err := rows.Scan(&si.InternalSquadUUID, &si.InboundUUID); err != nil {
			return nil, err
		}
		squadInbounds = append(squadInbounds, si)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return squadInbounds, nil
}

func (r *SquadRepository) setSquadInbounds(ctx context.Context, squadUUID string, inboundUUIDs []string) error {
	return exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		var squadID string
		err := tx.QueryRow(ctx, "SELECT uuid FROM internal_squads WHERE uuid = $1", squadUUID).Scan(&squadID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("squad not found")
			}
			return err
		}

		return r.replaceSquadInbounds(ctx, tx, squadUUID, inboundUUIDs)
	})
}

func (r *SquadRepository) getSquadMembers(ctx context.Context, squadUUID string) ([]InternalSquadMember, error) {
	var query string
	var args []interface{}

	if squadUUID != "" {
		query = `
			SELECT m.internal_squad_uuid, m.user_id, u.username
			FROM internal_squad_members m
			JOIN users u ON m.user_id = u.id
			WHERE m.internal_squad_uuid = $1
			ORDER BY m.user_id`
		args = []interface{}{squadUUID}
	} else {
		query = `
			SELECT m.internal_squad_uuid, m.user_id, u.username
			FROM internal_squad_members m
			JOIN users u ON m.user_id = u.id
			ORDER BY m.internal_squad_uuid, m.user_id`
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var squadMembers []InternalSquadMember
	for rows.Next() {
		var sm InternalSquadMember
		if err := rows.Scan(&sm.InternalSquadUUID, &sm.UserID, &sm.Username); err != nil {
			return nil, err
		}
		squadMembers = append(squadMembers, sm)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return squadMembers, nil
}

func (r *SquadRepository) setSquadMembers(ctx context.Context, squadUUID string, userIDs []int64) error {
	return exodusdb.WithRetryTx(ctx, r.db, func(tx pgx.Tx) error {
		var squadID string
		err := tx.QueryRow(ctx, "SELECT uuid FROM internal_squads WHERE uuid = $1", squadUUID).Scan(&squadID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("squad not found")
			}
			return err
		}

		_, err = tx.Exec(ctx, "DELETE FROM internal_squad_members WHERE internal_squad_uuid = $1", squadUUID)
		if err != nil {
			return fmt.Errorf("failed to clear existing members: %w", err)
		}

		if len(userIDs) == 0 {
			return nil
		}

		rows, err := tx.Query(ctx, `SELECT id FROM users WHERE id = ANY($1)`, userIDs)
		if err != nil {
			return err
		}
		existing := make(map[int64]struct{}, len(userIDs))
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			existing[id] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return err
		}

		for _, userID := range userIDs {
			if _, ok := existing[userID]; !ok {
				return fmt.Errorf("user not found: %d", userID)
			}
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO internal_squad_members (internal_squad_uuid, user_id)
			SELECT $1, unnest($2::bigint[])
		`, squadUUID, userIDs); err != nil {
			return fmt.Errorf("failed to insert members: %w", err)
		}

		return nil
	})
}

func (r *SquadRepository) getSquadDetails(ctx context.Context, squadUUID string) (SquadDetails, error) {
	var squad SquadDetails
	query := `
		SELECT uuid, view_position, name, created_at, updated_at
		FROM internal_squads
		WHERE uuid = $1`

	var viewPosition *int
	err := r.db.QueryRow(ctx, query, squadUUID).Scan(
		&squad.UUID, &viewPosition, &squad.Name, &squad.CreatedAt, &squad.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return squad, fmt.Errorf("squad not found")
		}
		return squad, err
	}

	if viewPosition != nil {
		squad.ViewPosition = *viewPosition
	}

	squad.Inbounds = []InboundInfo{}
	squad.Members = []SquadMemberInfo{}

	inboundQuery := `
		SELECT i.uuid, i.tag, i.type, i.network, i.security, i.port, i.raw_inbound
		FROM internal_squad_inbounds si
		JOIN config_profile_inbounds i ON si.inbound_uuid = i.uuid
		WHERE si.internal_squad_uuid = $1
		ORDER BY i.tag`

	rows, err := r.db.Query(ctx, inboundQuery, squadUUID)
	if err != nil {
		return squad, err
	}
	defer rows.Close()

	for rows.Next() {
		var inbound InboundInfo
		var network, security *string
		var port *int
		var rawInbound string

		if err := rows.Scan(&inbound.UUID, &inbound.Tag, &inbound.Type, &network, &security, &port, &rawInbound); err != nil {
			return squad, err
		}

		inbound.Network = network
		inbound.Security = security
		inbound.Port = port
		inbound.RawInbound = json.RawMessage(rawInbound)

		squad.Inbounds = append(squad.Inbounds, inbound)
	}
	if err := rows.Err(); err != nil {
		return squad, err
	}

	memberQuery := `
		SELECT m.user_id, u.username
		FROM internal_squad_members m
		JOIN users u ON m.user_id = u.id
		WHERE m.internal_squad_uuid = $1
		ORDER BY u.username`

	memberRows, err := r.db.Query(ctx, memberQuery, squadUUID)
	if err != nil {
		return squad, err
	}
	defer memberRows.Close()

	for memberRows.Next() {
		var member SquadMemberInfo
		if err := memberRows.Scan(&member.UserID, &member.Username); err != nil {
			return squad, err
		}
		squad.Members = append(squad.Members, member)
	}
	if err := memberRows.Err(); err != nil {
		return squad, err
	}

	return squad, nil
}

func (r *SquadRepository) getAllSquadsSummary(ctx context.Context) ([]SquadSummary, error) {
	query := `
		SELECT 
			s.uuid,
			s.view_position,
			s.name,
			COUNT(DISTINCT m.user_id) as members_count,
			COUNT(DISTINCT si.inbound_uuid) as inbounds_count
		FROM internal_squads s
		LEFT JOIN internal_squad_members m ON s.uuid = m.internal_squad_uuid
		LEFT JOIN internal_squad_inbounds si ON s.uuid = si.internal_squad_uuid
		GROUP BY s.uuid, s.view_position, s.name
		ORDER BY s.view_position ASC, s.name ASC`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var squads []SquadSummary
	for rows.Next() {
		var squad SquadSummary
		var viewPosition, membersCount, inboundsCount *int64

		if err := rows.Scan(&squad.UUID, &viewPosition, &squad.Name, &membersCount, &inboundsCount); err != nil {
			return nil, err
		}

		if viewPosition != nil {
			squad.ViewPosition = int(*viewPosition)
		}
		if membersCount != nil {
			squad.MembersCount = int(*membersCount)
		}
		if inboundsCount != nil {
			squad.InboundsCount = int(*inboundsCount)
		}

		squads = append(squads, squad)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return squads, nil
}

func (r *SquadRepository) getNodesWithConfig(ctx context.Context) ([]NodeWithConfig, error) {
	query := `
		SELECT
			n.uuid,
			n.id,
			n.name,
			n.address,
			n.port,
			n.active_config_profile_uuid,
			COALESCE(cp.name, '') as config_profile_name,
			n.is_connected,
			n.is_disabled,
			n.view_position,
			n.country_code,
			n.tags as tags
		FROM nodes n
		LEFT JOIN config_profiles cp ON n.active_config_profile_uuid = cp.uuid
		ORDER BY n.view_position ASC, n.name ASC`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []NodeWithConfig
	for rows.Next() {
		var node NodeWithConfig
		var id *int64
		var activeConfigProfileUUID, configProfileName *string
		var tags []string

		if err := rows.Scan(&node.UUID, &id, &node.Name, &node.Address, &node.Port,
			&activeConfigProfileUUID, &configProfileName, &node.IsConnected, &node.IsDisabled,
			&node.ViewPosition, &node.CountryCode, &tags); err != nil {
			return nil, err
		}

		node.ID = id
		if activeConfigProfileUUID != nil && *activeConfigProfileUUID != "" {
			node.ActiveConfigProfileUUID = activeConfigProfileUUID
			if configProfileName != nil && *configProfileName != "" {
				node.ConfigProfileName = configProfileName
			}
		}

		if len(tags) > 0 {
			node.Tags = tags
		} else {
			node.Tags = []string{}
		}

		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (r *SquadRepository) getInboundsWithProfiles(ctx context.Context) ([]InboundWithProfile, error) {
	query := `
		SELECT 
			p.uuid AS profile_uuid,
			p.name AS profile_name,
			i.uuid AS inbound_uuid,
			i.tag AS inbound_tag,
			i.type AS inbound_type,
			i.port AS inbound_port
		FROM config_profile_inbounds i
		JOIN config_profiles p ON p.uuid = i.profile_uuid
		ORDER BY p.name, i.tag`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var inbounds []InboundWithProfile
	for rows.Next() {
		var ib InboundWithProfile
		var port *int

		if err := rows.Scan(&ib.ProfileUUID, &ib.ProfileName, &ib.InboundUUID, &ib.InboundTag, &ib.InboundType, &port); err != nil {
			return nil, err
		}

		ib.InboundPort = port
		inbounds = append(inbounds, ib)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return inbounds, nil
}

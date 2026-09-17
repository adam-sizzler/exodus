package users

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (r *UserRepository) getUserInternalSquadsTx(ctx context.Context, tx pgx.Tx, userID int64) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT internal_squad_uuid FROM internal_squad_members WHERE user_id = $1`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	current := make([]string, 0)
	for rows.Next() {
		var squadUUID string
		if err := rows.Scan(&squadUUID); err != nil {
			return nil, err
		}
		current = append(current, strings.TrimSpace(squadUUID))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return dedupeStrings(current), nil
}

func internalSquadSetsDiffer(current []string, requested []string) bool {
	nextSet := make(map[string]struct{})
	for _, squadUUID := range dedupeStrings(requested) {
		nextSet[squadUUID] = struct{}{}
	}
	if len(current) != len(nextSet) {
		return true
	}
	for _, squadUUID := range current {
		if _, ok := nextSet[squadUUID]; !ok {
			return true
		}
	}
	return false
}

func (r *UserRepository) resolveNodeUUIDsForInternalSquadsTx(ctx context.Context, tx pgx.Tx, squadUUIDs []string) ([]string, error) {
	cleanSquadUUIDs := dedupeStrings(squadUUIDs)
	if len(cleanSquadUUIDs) == 0 {
		return []string{}, nil
	}

	rows, err := tx.Query(ctx, `
		SELECT DISTINCT cpitn.node_uuid
		FROM internal_squad_inbounds isi
		JOIN config_profile_inbounds_to_nodes cpitn ON cpitn.config_profile_inbound_uuid = isi.inbound_uuid
		WHERE isi.internal_squad_uuid = ANY($1)
	`, cleanSquadUUIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

func (r *UserRepository) resolveNodeUUIDsForUserUUIDsTx(ctx context.Context, tx pgx.Tx, userUUIDs []string) ([]string, error) {
	cleanUserUUIDs := dedupeStrings(userUUIDs)
	if len(cleanUserUUIDs) == 0 {
		return []string{}, nil
	}

	rows, err := tx.Query(ctx, `
		SELECT DISTINCT cpitn.node_uuid
		FROM users u
		JOIN internal_squad_members ism ON ism.user_id = u.id
		JOIN internal_squad_inbounds isi ON isi.internal_squad_uuid = ism.internal_squad_uuid
		JOIN config_profile_inbounds_to_nodes cpitn ON cpitn.config_profile_inbound_uuid = isi.inbound_uuid
		WHERE u.uuid = ANY($1)
	`, cleanUserUUIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

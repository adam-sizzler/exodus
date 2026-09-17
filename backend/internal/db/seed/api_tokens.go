package seed

import (
	"context"
	"encoding/json"
	"strings"

	"exodus/internal/config"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func ensureValidAPITokens(ctx context.Context, tx pgx.Tx, cfg *config.BackendConfig) error {
	rows, err := tx.Query(ctx, `
		SELECT uuid, name, array_to_json(COALESCE(scopes, ARRAY['*']::text[]))::text
		FROM api_tokens
	`)
	if err != nil {
		return nil
	}
	defer rows.Close()

	type tokenItem struct {
		uuid      string
		name      string
		scopesRaw string
	}

	var tokens []tokenItem
	for rows.Next() {
		var item tokenItem
		if err := rows.Scan(&item.uuid, &item.name, &item.scopesRaw); err != nil {
			continue
		}
		tokens = append(tokens, item)
	}
	if rowsErr := rows.Err(); rowsErr != nil && cfg != nil && cfg.Logger != nil {
		cfg.Logger.Warn("Error iterating API tokens", "error", rowsErr)
	}

	for _, token := range tokens {
		tokenUUID := strings.TrimSpace(token.uuid)
		tokenName := strings.TrimSpace(token.name)

		// Validate UUID format
		if _, err := uuid.Parse(tokenUUID); err != nil {
			if cfg != nil && cfg.Logger != nil {
				cfg.Logger.Warn("Invalid UUID for API token; deleting token", "name", token.name, "uuid", token.uuid)
			}
			_, _ = tx.Exec(ctx, `DELETE FROM api_tokens WHERE uuid = $1`, token.uuid)
			continue
		}

		// Validate name
		if tokenName == "" {
			if cfg != nil && cfg.Logger != nil {
				cfg.Logger.Warn("Invalid empty name for API token; deleting token", "uuid", token.uuid)
			}
			_, _ = tx.Exec(ctx, `DELETE FROM api_tokens WHERE uuid = $1`, token.uuid)
			continue
		}

		// Validate scopes array
		var scopes []string
		if err := json.Unmarshal([]byte(token.scopesRaw), &scopes); err != nil {
			if cfg != nil && cfg.Logger != nil {
				cfg.Logger.Warn("Invalid scopes format for API token; deleting token", "uuid", token.uuid)
			}
			_, _ = tx.Exec(ctx, `DELETE FROM api_tokens WHERE uuid = $1`, token.uuid)
			continue
		}
	}

	return nil
}

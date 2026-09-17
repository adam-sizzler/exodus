package seed

import (
	"context"
	"fmt"

	"exodus/internal/config"

	"github.com/jackc/pgx/v5"
)

func ensureDefaultConfigProfile(ctx context.Context, tx pgx.Tx, _ *config.BackendConfig) error {
	fmt.Println("◐ Seeding default config profile...")

	var count int
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM config_profiles`).Scan(&count); err != nil {
		return fmt.Errorf("count config_profiles: %w", err)
	}
	if count > 0 {
		fmt.Println("ℹ Default config profile already seeded")
		return nil
	}

	query := `
		INSERT INTO config_profiles (
			uuid, view_position, name, config, created_at, updated_at
		) VALUES ($1, $2, $3, $4::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`
	if _, err := tx.Exec(ctx, query, "00000000-0000-0000-0000-000000000000", 1, "Default-Profile", defaultSingboxConfig); err != nil {
		return fmt.Errorf("insert default config_profile: %w", err)
	}

	fmt.Println("✔ Default config profile seeded")
	return nil
}

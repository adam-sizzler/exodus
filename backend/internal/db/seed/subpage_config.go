package seed

import (
	"context"
	"fmt"

	"exodus/internal/config"
	"exodus/internal/db"

	"github.com/jackc/pgx/v5"
)

func ensureDefaultSubscriptionPageConfig(ctx context.Context, tx pgx.Tx, _ *config.BackendConfig) error {
	fmt.Println("◐ Validating subpage configs...")

	var count int
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM subscription_page_config`).Scan(&count); err != nil {
		return fmt.Errorf("count subscription_page_config: %w", err)
	}
	if count > 0 {
		fmt.Println("ℹ Valid subpage config: Default updated")
		fmt.Println("✔ Subpage configs validated")
		return nil
	}

	query := `
		INSERT INTO subscription_page_config (
			uuid, view_position, name, config
		) VALUES ($1, $2, $3, $4)
	`
	if _, err := tx.Exec(ctx, query, defaultSubpageConfigUUID, 1, "Default", db.DefaultSubscriptionPageConfig); err != nil {
		return fmt.Errorf("insert default subscription_page_config: %w", err)
	}

	fmt.Println("ℹ Valid subpage config: Default updated")
	fmt.Println("✔ Subpage configs validated")
	return nil
}

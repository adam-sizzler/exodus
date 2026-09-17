package seed

import (
	"context"
	"fmt"

	"exodus/internal/config"

	"github.com/jackc/pgx/v5"
)

func ensureSingleAdmin(ctx context.Context, tx pgx.Tx, _ *config.BackendConfig) error {
	var count int
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM admin`).Scan(&count); err != nil {
		return fmt.Errorf("count admin rows: %w", err)
	}

	if count <= 1 {
		return nil
	}

	tag, err := tx.Exec(ctx, `
		DELETE FROM admin
		WHERE uuid NOT IN (
			SELECT uuid FROM admin ORDER BY created_at ASC LIMIT 1
		)
	`)
	if err != nil {
		return fmt.Errorf("deduplicate admin records: %w", err)
	}

	deleted := tag.RowsAffected()
	fmt.Printf("⚠️ Multiple admin records found (%d), retained oldest admin\n", deleted)
	return nil
}

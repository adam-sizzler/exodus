package seed

import (
	"context"
	"encoding/json"
	"fmt"

	"exodus/internal/config"
	"exodus/internal/db"

	"github.com/jackc/pgx/v5"
)

func resyncConfigProfileInbounds(ctx context.Context, tx pgx.Tx, _ *config.BackendConfig) (int, error) {
	fmt.Println("◐ Syncing inbounds...")

	rows, err := tx.Query(ctx, `SELECT uuid, name, config FROM config_profiles`)
	if err != nil {
		return 0, fmt.Errorf("list config profiles: %w", err)
	}
	defer rows.Close()

	type profile struct {
		uuid   string
		name   string
		config json.RawMessage
	}
	var profiles []profile
	for rows.Next() {
		var p profile
		if err := rows.Scan(&p.uuid, &p.name, &p.config); err != nil {
			return 0, err
		}
		profiles = append(profiles, p)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, p := range profiles {
		fmt.Printf("◐ Syncing %s...\n", p.name)
		if _, err := db.SyncConfigProfileInboundsTx(ctx, tx, p.uuid, p.config); err != nil {
			return 0, fmt.Errorf("sync inbounds for profile %s: %w", p.uuid, err)
		}
	}

	fmt.Println("✔ Inbounds synced successfully")
	return len(profiles), nil
}

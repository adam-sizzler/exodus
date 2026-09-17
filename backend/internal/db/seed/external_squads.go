package seed

import (
	"context"
	"fmt"

	"exodus/internal/config"

	"github.com/jackc/pgx/v5"
)

func checkupExternalSquads(ctx context.Context, tx pgx.Tx, _ *config.BackendConfig) (int64, error) {
	fmt.Println("◐ Checking up external squads...")

	res, err := tx.Exec(ctx, `
		UPDATE external_squads SET
			subscription_settings = CASE 
				WHEN subscription_settings::text IN ('{}', 'null', '[]') THEN NULL 
				ELSE subscription_settings 
			END,
			host_overrides = CASE 
				WHEN host_overrides::text IN ('{}', 'null', '[]') THEN NULL 
				ELSE host_overrides 
			END,
			response_headers_add = CASE 
				WHEN response_headers_add IS NULL OR response_headers_add::text IN ('null', '[]') THEN '{}'::jsonb 
				ELSE response_headers_add 
			END,
			response_headers_remove = CASE 
				WHEN response_headers_remove IS NULL THEN ARRAY[]::text[] 
				ELSE response_headers_remove 
			END,
			hwid_settings = CASE 
				WHEN hwid_settings::text IN ('{}', 'null', '[]') OR (hwid_settings IS NOT NULL AND jsonb_typeof(hwid_settings) != 'object') THEN NULL 
				ELSE hwid_settings 
			END,
			custom_remarks = CASE 
				WHEN custom_remarks::text IN ('{}', 'null', '[]') OR (custom_remarks IS NOT NULL AND jsonb_typeof(custom_remarks) != 'object') THEN NULL 
				ELSE custom_remarks 
			END
		WHERE 
			subscription_settings::text IN ('{}', 'null', '[]')
			OR host_overrides::text IN ('{}', 'null', '[]')
			OR response_headers_add IS NULL OR response_headers_add::text IN ('null', '[]')
			OR response_headers_remove IS NULL
			OR hwid_settings::text IN ('{}', 'null', '[]') OR (hwid_settings IS NOT NULL AND jsonb_typeof(hwid_settings) != 'object')
			OR custom_remarks::text IN ('{}', 'null', '[]') OR (custom_remarks IS NOT NULL AND jsonb_typeof(custom_remarks) != 'object')
	`)
	if err != nil {
		return 0, fmt.Errorf("checkup external squads: %w", err)
	}

	rows := res.RowsAffected()
	if rows > 0 {
		fmt.Printf("✔ Fixed external squads: %d\n", rows)
	} else {
		fmt.Println("✔ Nothing to fix")
	}

	return rows, nil
}

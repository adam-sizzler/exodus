package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func resetSuperadmin(resources *rescueResources, reader *bufio.Reader) error {
	answer, err := promptConfirm(reader, "Are you sure you want to delete the superadmin?")
	if err != nil {
		return err
	}
	if !answer {
		return errors.New("aborted")
	}

	printStatus("◐", "🔄 Deleting superadmin...")

	ctx := context.Background()

	var (
		adminUUID string
		username  string
	)

	err = resources.db.QueryRow(ctx, `
		SELECT uuid, username
		FROM admin
		ORDER BY created_at ASC
		LIMIT 1
	`).Scan(&adminUUID, &username)

	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("superadmin not found")
	}
	if err != nil {
		return fmt.Errorf("find superadmin: %w", err)
	}

	if _, err := resources.db.Exec(ctx, "DELETE FROM admin WHERE uuid = $1", adminUUID); err != nil {
		return fmt.Errorf("delete superadmin: %w", err)
	}

	printStatus("✔", fmt.Sprintf("✅ Superadmin %s deleted successfully.", username))

	return nil
}

func enablePasswordAuth(resources *rescueResources, reader *bufio.Reader) error {
	printStatus("◐", "🔄 Enabling password authentication...")

	answer, err := promptConfirm(reader, "Are you sure you want to enable password authentication?")
	if err != nil {
		return err
	}
	if !answer {
		return errors.New("aborted")
	}

	ctx := context.Background()

	if _, err := resources.db.Exec(ctx, `
		UPDATE exodus_settings
		SET password_settings = jsonb_set(
			COALESCE(password_settings, '{}'::jsonb),
			'{enabled}',
			'true'::jsonb,
			true
		)
		WHERE id = 1
	`); err != nil {
		return fmt.Errorf("enable password authentication: %w", err)
	}

	printStatus("✔", "✅ Password authentication enabled successfully.")

	return nil
}

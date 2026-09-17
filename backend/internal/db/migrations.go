package db

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"exodus/internal/config"

	"github.com/jackc/pgx/v5"
)

//go:embed prisma/migrations/*/migration.sql
var migrationsFS embed.FS

const migrationsAdvisoryLockKey int64 = 2203092601

// fixedMigrationChecksums — миграции, чей файл был задним числом отредактирован
// после релиза. Ключ — migration_name, значение — {старый, новый} checksum.
var fixedMigrationChecksums = map[string]struct{ Old, New string }{
	// "20260724000000_delete_legacy": {Old: "abc123...", New: "def456..."},
}

// fixedMigrationDeletions — устаревшие записи миграций, которые нужно удалить из истории
var fixedMigrationDeletions = []string{
	// "20251230045744_drop_is_custom_remark",
}

// retiredMigrations — устаревшие миграции, файлы которых были удалены с диска
// при бейзлайнинге схемы. Наличие этих имен в истории БД игнорируется.
var retiredMigrations = map[string]struct{}{
	"20260302031100_init_schema":                                 {},
	"20260309163000_probe_users_created_at_index":                {},
	"20260312230000_srs_lists":                                   {},
	"20260313002000_srs_lists_enabled_drop_type":                 {},
	"20260316000100_add_modules_settings":                        {},
	"20260316003000_create_modules_settings":                     {},
	"20260319170000_rename_xray_columns_to_singbox":              {},
	"20260322030000_hosts_mux_per_core_and_selector_toggle":      {},
	"20260325010000_preserve_json_key_order":                     {},
	"20260325223000_subscription_connections":                    {},
	"20260326000100_sub_nodes_table":                             {},
	"20260326010100_sub_nodes_runtime_columns":                   {},
	"20260326023000_sub_nodes_panel_api_token":                   {},
	"20260326220000_sub_nodes_subpage_config_uuid":               {},
	"20260326233000_sub_nodes_grpc_auth_token":                   {},
	"20260326234000_drop_sub_nodes_panel_api_token":              {},
	"20260327000100_sub_nodes_cleanup_and_keygen_grpc_token":     {},
	"20260327020000_sub_nodes_subpage_join_and_cleanup":          {},
	"20260327030000_drop_sub_nodes_to_subpage_timestamps":        {},
	"20260413190000_rename_cerberus_settings_to_exodus_settings": {},
	"20260413201500_drop_hosts_xhttp_extra_params":               {},
	"20260419170000_sub_nodes_public_domain":                     {},
	"20260518013000_drop_config_profile_snippets":                {},
}

// ApplyMigrations applies pending schema migrations using a native *pgx.Conn and advisory lock.
func ApplyMigrations(ctx context.Context, conn *pgx.Conn, cfg *config.BackendConfig) error {
	if conn == nil {
		return fmt.Errorf("database connection is nil")
	}

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationsAdvisoryLockKey); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	defer func() {
		if _, unlockErr := conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationsAdvisoryLockKey); unlockErr != nil {
			cfg.Logger.Warn("Failed to release migration advisory lock", "error", unlockErr)
		}
	}()

	if _, err := conn.Exec(ctx, `
		CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

		CREATE TABLE IF NOT EXISTS public.schema_migrations (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			migration_name TEXT NOT NULL UNIQUE,
			checksum TEXT NOT NULL,
			started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			finished_at TIMESTAMPTZ,
			applied_steps_count INTEGER NOT NULL DEFAULT 1,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`); err != nil {
		return fmt.Errorf("initialize schema_migrations table: %w", err)
	}

	// 1. Fix old migration checksums & delete legacy migration records
	for name, sums := range fixedMigrationChecksums {
		res, err := conn.Exec(ctx,
			`UPDATE public.schema_migrations SET checksum = $1 WHERE migration_name = $2 AND checksum = $3`,
			sums.New, name, sums.Old)
		if err != nil {
			return fmt.Errorf("fix checksum for %s: %w", name, err)
		}
		if res.RowsAffected() > 0 {
			cfg.Logger.Info("Fixed migration checksum", "name", name)
		}
	}

	for _, name := range fixedMigrationDeletions {
		res, err := conn.Exec(ctx,
			`DELETE FROM public.schema_migrations WHERE migration_name = $1`, name)
		if err != nil {
			return fmt.Errorf("delete old migration record for %s: %w", name, err)
		}
		if res.RowsAffected() > 0 {
			cfg.Logger.Info("Deleted old migration record", "name", name)
		}
	}

	dirs, err := fs.ReadDir(migrationsFS, "prisma/migrations")
	if err != nil {
		return fmt.Errorf("read migrations directory: %w", err)
	}

	var names []string
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		names = append(names, dir.Name())
	}
	sort.Strings(names)

	knownMigrations := make(map[string]struct{}, len(names))
	for _, name := range names {
		knownMigrations[name] = struct{}{}
	}

	appliedRows, err := conn.Query(ctx, `SELECT migration_name FROM public.schema_migrations ORDER BY migration_name`)
	if err != nil {
		return fmt.Errorf("read applied migrations: %w", err)
	}

	var legacyCount int
	var legacyMigrations []string
	for appliedRows.Next() {
		var appliedName string
		if err := appliedRows.Scan(&appliedName); err != nil {
			appliedRows.Close()
			return fmt.Errorf("scan applied migration: %w", err)
		}
		if _, ok := knownMigrations[appliedName]; ok {
			continue
		}
		if _, ok := retiredMigrations[appliedName]; ok || appliedName < "20260506000000_initial_schema" {
			legacyCount++
			continue
		}
		legacyMigrations = append(legacyMigrations, appliedName)
	}
	if err := appliedRows.Err(); err != nil {
		appliedRows.Close()
		return fmt.Errorf("iterate applied migrations: %w", err)
	}
	appliedRows.Close()

	if len(legacyMigrations) > 0 {
		return fmt.Errorf(
			"unsupported legacy migration history detected: %s; this release supports only the %s baseline, use a clean database or prepare the database for the new baseline",
			strings.Join(legacyMigrations, ", "),
			strings.Join(names, ", "),
		)
	}

	// Auto-baseline for legacy databases: if legacy migration records exist or public.admin table already exists,
	// ensure baseline initial_schema is marked as applied so it doesn't fail trying to re-create existing tables.
	var adminTableExists bool
	_ = conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='admin')`).Scan(&adminTableExists)

	if legacyCount > 0 || adminTableExists {
		baselineName := "20260506000000_initial_schema"
		var existsCount int
		_ = conn.QueryRow(ctx, `SELECT count(*) FROM public.schema_migrations WHERE migration_name = $1`, baselineName).Scan(&existsCount)
		if existsCount == 0 {
			sqlPath := fmt.Sprintf("prisma/migrations/%s/migration.sql", baselineName)
			if sqlBytes, err := migrationsFS.ReadFile(sqlPath); err == nil {
				checksum := sha256.Sum256(sqlBytes)
				checksumHex := hex.EncodeToString(checksum[:])
				_, _ = conn.Exec(ctx, `
					INSERT INTO public.schema_migrations (
						migration_name, checksum, started_at, finished_at, applied_steps_count, applied_at
					) VALUES ($1, $2, now(), now(), 1, now()) ON CONFLICT (migration_name) DO NOTHING
				`, baselineName, checksumHex)
				cfg.Logger.Info("Auto-marked baseline migration as applied for legacy database", "name", baselineName)
			}
		}
	}

	for _, name := range names {
		sqlPath := fmt.Sprintf("prisma/migrations/%s/migration.sql", name)
		sqlBytes, err := migrationsFS.ReadFile(sqlPath)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		sqlText := strings.TrimSpace(string(sqlBytes))
		if sqlText == "" {
			continue
		}

		checksum := sha256.Sum256(sqlBytes)
		checksumHex := hex.EncodeToString(checksum[:])

		var existing string
		err = conn.QueryRow(ctx, `SELECT checksum FROM public.schema_migrations WHERE migration_name = $1`, name).Scan(&existing)
		switch {
		case err == nil:
			if existing != checksumHex {
				return fmt.Errorf("migration %s checksum mismatch: stored=%s current=%s", name, existing, checksumHex)
			}
			continue
		case errors.Is(err, pgx.ErrNoRows):
			// apply
		default:
			return fmt.Errorf("check migration %s: %w", name, err)
		}

		fmt.Printf("Applying migration: %s\n", name)
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, sqlText); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO public.schema_migrations (
				migration_name, checksum, started_at, finished_at, applied_steps_count, applied_at
			) VALUES ($1, $2, now(), now(), 1, now())
		`, name, checksumHex); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}
	}

	return nil
}

// ApplyMigrationsDSN connects directly to PostgreSQL using a dedicated *pgx.Conn, applies migrations, and closes the connection.
func ApplyMigrationsDSN(ctx context.Context, dsn string, cfg *config.BackendConfig) error {
	conn, err := pgx.Connect(ctx, strings.TrimSpace(dsn))
	if err != nil {
		return fmt.Errorf("connect for migrations: %w", err)
	}
	defer conn.Close(ctx)

	return ApplyMigrations(ctx, conn, cfg)
}

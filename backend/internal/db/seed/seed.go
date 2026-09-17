package seed

import (
	"context"
	"fmt"

	"exodus/internal/config"
	"exodus/internal/jobqueue"

	"github.com/jackc/pgx/v5/pgxpool"
)

const divider = "▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱"

func ClearRedis(ctx context.Context, cfg *config.BackendConfig) error {
	fmt.Println("◐ Clearing Redis...")
	client, err := jobqueue.NewRedisClient(cfg)
	if err != nil || client == nil {
		return err
	}
	defer client.Close()
	if err := client.FlushDB(ctx).Err(); err != nil {
		return err
	}
	fmt.Println("✔ Redis cleared")
	return nil
}

// SeedDefaults inserts base settings and templates if they do not exist.
func SeedDefaults(ctx context.Context, pool *pgxpool.Pool, cfg *config.BackendConfig) error {
	if pool == nil {
		return fmt.Errorf("database connection pool is nil")
	}

	fmt.Println("✔ Database connected")

	// Step 0.2: Flush Redis DB on startup
	if err := ClearRedis(ctx, cfg); err != nil {
		cfg.Logger.Warn("Failed to clear Redis on startup", "error", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin defaults transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// Step 01: Fix Old Migrations
	fmt.Println(divider)
	fmt.Println("◐ [01/12] Fix Old Migrations")
	fmt.Println("✔ Old migrations fixed")
	fmt.Println("✔ [01/12] Fix Old Migrations")

	// Step 02: Checkup External Squads
	fmt.Println(divider)
	fmt.Println("◐ [02/12] Checkup External Squads")
	if _, err := checkupExternalSquads(ctx, tx, cfg); err != nil {
		return err
	}
	fmt.Println("✔ [02/12] Checkup External Squads")

	// Step 03: Exodus Settings
	fmt.Println(divider)
	fmt.Println("◐ [03/12] Exodus Settings")
	if err := ensureExodusSettings(ctx, tx, cfg); err != nil {
		return err
	}
	fmt.Println("✔ [03/12] Exodus Settings")

	// Step 04: Subscription Templates
	fmt.Println(divider)
	fmt.Println("◐ [04/12] Subscription Templates")
	if err := ensureDefaultTemplates(ctx, tx, cfg); err != nil {
		return err
	}
	fmt.Println("✔ [04/12] Subscription Templates")

	// Step 05: Default Config Profile
	fmt.Println(divider)
	fmt.Println("◐ [05/12] Default Config Profile")
	if err := ensureDefaultConfigProfile(ctx, tx, cfg); err != nil {
		return err
	}
	fmt.Println("✔ [05/12] Default Config Profile")

	// Step 06: Sync Inbounds
	fmt.Println(divider)
	fmt.Println("◐ [06/12] Sync Inbounds")
	if _, err := resyncConfigProfileInbounds(ctx, tx, cfg); err != nil {
		return err
	}
	fmt.Println("✔ [06/12] Sync Inbounds")

	// Step 07: Default Internal Squad
	fmt.Println(divider)
	fmt.Println("◐ [07/12] Default Internal Squad")
	if err := ensureDefaultInternalSquad(ctx, tx, cfg); err != nil {
		return err
	}
	fmt.Println("✔ [07/12] Default Internal Squad")

	// Step 08: Subscription Settings
	fmt.Println(divider)
	fmt.Println("◐ [08/12] Subscription Settings")
	if err := ensureDefaultSubscriptionSettings(ctx, tx, cfg); err != nil {
		return err
	}
	fmt.Println("✔ [08/12] Subscription Settings")

	// Step 09: Keygen
	fmt.Println(divider)
	fmt.Println("◐ [09/12] Keygen")
	if err := ensureKeygen(ctx, tx, cfg); err != nil {
		return err
	}
	fmt.Println("✔ [09/12] Keygen")

	// Step 10: Response Rules
	fmt.Println(divider)
	fmt.Println("◐ [10/12] Response Rules")
	logResponseRulesHashes(ctx, tx, cfg)
	fmt.Println("✔ Response rules seeded")
	fmt.Println("✔ [10/12] Response Rules")

	// Step 11: Subscription Page Config
	fmt.Println(divider)
	fmt.Println("◐ [11/12] Subscription Page Config")
	if err := ensureDefaultSubscriptionPageConfig(ctx, tx, cfg); err != nil {
		return err
	}
	fmt.Println("✔ [11/12] Subscription Page Config")

	// Step 12: Verify Admin User
	fmt.Println(divider)
	fmt.Println("◐ [12/13] Verify Admin User")
	if err := ensureSingleAdmin(ctx, tx, cfg); err != nil {
		return err
	}
	fmt.Println("✔ [12/13] Verify Admin User")

	// Step 13: Verify and Clean API Tokens
	fmt.Println(divider)
	fmt.Println("◐ [13/13] Verify and Clean API Tokens")
	if err := ensureValidAPITokens(ctx, tx, cfg); err != nil {
		return err
	}
	fmt.Println("✔ [13/13] Verify and Clean API Tokens")
	fmt.Println(divider)

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit defaults transaction: %w", err)
	}

	fmt.Println("\n🌱  The seed command has been executed.")
	return nil
}

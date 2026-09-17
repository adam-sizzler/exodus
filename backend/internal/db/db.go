package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"exodus/internal/config"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DBTX is the unified database interaction interface satisfied by *pgxpool.Pool, *pgx.Conn, and pgx.Tx.
type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// OpenPgxPool configures and opens a native pgxpool.Pool using high-performance settings.
func OpenPgxPool(ctx context.Context, dsn string, maxConns, minConns int32) (*pgxpool.Pool, error) {
	cleanDSN := strings.TrimSpace(dsn)
	if cleanDSN == "" {
		return nil, fmt.Errorf("database DSN is empty")
	}

	poolCfg, err := pgxpool.ParseConfig(cleanDSN)
	if err != nil {
		return nil, fmt.Errorf("parse pgx pool config: %w", err)
	}

	poolCfg.MaxConns = maxConns
	poolCfg.MinConns = minConns
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnLifetimeJitter = 2 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute
	poolCfg.HealthCheckPeriod = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}

	pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
	defer cancelPing()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping pgx pool: %w", err)
	}

	return pool, nil
}

// EnsurePgCryptoExtension ensures the pgcrypto extension is installed in PostgreSQL via DBTX.
func EnsurePgCryptoExtension(ctx context.Context, db DBTX) error {
	_, err := db.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "pgcrypto"`)
	if err != nil {
		return fmt.Errorf("ensure pgcrypto extension: %w", err)
	}
	return nil
}

// InitDatabase verifies database connectivity and applies pending migrations using native pgx.
func InitDatabase(ctx context.Context, cfg *config.BackendConfig) error {
	dsn := strings.TrimSpace(cfg.Database.URL)
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL is not set")
	}

	for {
		connCtx, cancelConn := context.WithTimeout(ctx, 5*time.Second)
		conn, err := pgx.Connect(connCtx, dsn)
		cancelConn()
		if err == nil {
			pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
			pingErr := conn.Ping(pingCtx)
			cancelPing()
			if pingErr == nil {
				_ = EnsurePgCryptoExtension(ctx, conn)
				_ = conn.Close(ctx)
				break
			}
			_ = conn.Close(ctx)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cfg.Logger.Warn("Database not ready, retrying in 5s", "error", err)
		time.Sleep(5 * time.Second)
	}

	initCtx, cancelInit := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelInit()

	fmt.Println("Migrating database...")
	if err := ApplyMigrationsDSN(initCtx, dsn, cfg); err != nil {
		return err
	}
	fmt.Println("Migrations deployed successfully!")
	fmt.Println("Seeding database...")

	return nil
}

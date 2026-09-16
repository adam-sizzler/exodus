package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"exodus/internal/config"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
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

// OpenAndInitDB opens a PostgreSQL database, applies migrations, and seeds defaults.
// Maintained for backward compatibility during the dual-pool transition phase.
func OpenAndInitDB(cfg *config.BackendConfig) (*sql.DB, error) {
	dsn := strings.TrimSpace(cfg.Database.URL)
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_URL is not set")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(15 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	for {
		pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
		err := db.PingContext(pingCtx)
		cancelPing()
		if err == nil {
			break
		}
		cfg.Logger.Warn("Database not ready, retrying in 5s", "error", err)
		time.Sleep(5 * time.Second)
	}

	execCtx, cancelExec := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelExec()
	if _, err := db.ExecContext(execCtx, `CREATE EXTENSION IF NOT EXISTS "pgcrypto"`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ensure pgcrypto extension: %w", err)
	}

	initCtx, cancelInit := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancelInit()

	fmt.Println("Migrating database...")
	if err := ApplyMigrations(initCtx, db, cfg); err != nil {
		_ = db.Close()
		return nil, err
	}
	fmt.Println("Migrations deployed successfully!")
	fmt.Println("Seeding database...")

	return db, nil
}

// InitDatabase initializes the database and returns a ready connection.
func InitDatabase(cfg *config.BackendConfig) (*sql.DB, error) {
	return OpenAndInitDB(cfg)
}

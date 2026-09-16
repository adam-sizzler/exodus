package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"exodus/internal/config"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pools holds dual connection pools during the migration transition:
// both legacy database/sql pools and native pgxpool.Pool instances for interactive and background work.
type Pools struct {
	// Legacy stdlib pools (to be removed upon completion of all repository migrations):
	Interactive *sql.DB
	Background  *sql.DB

	// Native pgx v5 pools:
	PgxInteractive *pgxpool.Pool
	PgxBackground  *pgxpool.Pool
}

// NewPools configures the interactive pool and opens isolated background and native pgx pools.
func NewPools(ctx context.Context, interactive *sql.DB, cfg *config.BackendConfig) (*Pools, error) {
	// Configure legacy interactive pool
	interactive.SetMaxOpenConns(32)
	interactive.SetMaxIdleConns(16)
	interactive.SetConnMaxLifetime(30 * time.Minute)
	interactive.SetConnMaxIdleTime(5 * time.Minute)

	// Configure legacy background pool
	bg, err := sql.Open("pgx", cfg.Database.URL)
	if err != nil {
		return nil, fmt.Errorf("open legacy background pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := bg.PingContext(pingCtx); err != nil {
		_ = bg.Close()
		return nil, fmt.Errorf("ping legacy background pool: %w", err)
	}

	bg.SetMaxOpenConns(8)
	bg.SetMaxIdleConns(4)
	bg.SetConnMaxLifetime(30 * time.Minute)
	bg.SetConnMaxIdleTime(5 * time.Minute)

	// Initialize native pgx pools (Dual-Pool Transition Mode)
	pgxInteractive, err := OpenPgxPool(ctx, cfg.Database.URL, 32, 16)
	if err != nil {
		_ = bg.Close()
		return nil, fmt.Errorf("initialize pgx interactive pool: %w", err)
	}

	pgxBackground, err := OpenPgxPool(ctx, cfg.Database.URL, 8, 4)
	if err != nil {
		pgxInteractive.Close()
		_ = bg.Close()
		return nil, fmt.Errorf("initialize pgx background pool: %w", err)
	}

	return &Pools{
		Interactive:    interactive,
		Background:     bg,
		PgxInteractive: pgxInteractive,
		PgxBackground:  pgxBackground,
	}, nil
}

// Close closes all database connection pools (both native pgx and legacy stdlib).
func (p *Pools) Close() error {
	var errs []error

	if p.PgxInteractive != nil {
		p.PgxInteractive.Close()
	}
	if p.PgxBackground != nil {
		p.PgxBackground.Close()
	}

	if p.Interactive != nil {
		if err := p.Interactive.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close interactive pool: %w", err))
		}
	}
	if p.Background != nil {
		if err := p.Background.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close background pool: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing pools: %v", errs)
	}
	return nil
}

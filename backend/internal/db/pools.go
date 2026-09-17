package db

import (
	"context"
	"fmt"

	"exodus/internal/config"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pools holds native pgxpool.Pool instances for interactive and background workloads.
type Pools struct {
	PgxInteractive *pgxpool.Pool
	PgxBackground  *pgxpool.Pool
}

// NewPools configures and opens native pgx pools for interactive and background operations.
func NewPools(ctx context.Context, cfg *config.BackendConfig) (*Pools, error) {
	// Initialize native pgx pools
	pgxInteractive, err := OpenPgxPool(ctx, cfg.Database.URL, 32, 16)
	if err != nil {
		return nil, fmt.Errorf("initialize pgx interactive pool: %w", err)
	}

	pgxBackground, err := OpenPgxPool(ctx, cfg.Database.URL, 8, 4)
	if err != nil {
		pgxInteractive.Close()
		return nil, fmt.Errorf("initialize pgx background pool: %w", err)
	}

	return &Pools{
		PgxInteractive: pgxInteractive,
		PgxBackground:  pgxBackground,
	}, nil
}

// Close closes all database connection pools.
func (p *Pools) Close() error {
	if p.PgxInteractive != nil {
		p.PgxInteractive.Close()
	}
	if p.PgxBackground != nil {
		p.PgxBackground.Close()
	}
	return nil
}

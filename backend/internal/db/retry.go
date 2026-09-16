package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WithRetryTx executes fn within a native pgx transaction, retrying up to 3 times
// if a retryable PostgreSQL error (40001 serialization_failure, 40P01 deadlock_detected) occurs.
func WithRetryTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	const maxRetries = 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin retry tx: %w", err)
		}

		err = fn(tx)
		if err == nil {
			if commitErr := tx.Commit(ctx); commitErr != nil {
				if isRetryablePgError(commitErr) && attempt < maxRetries {
					time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
					continue
				}
				return fmt.Errorf("commit retry tx: %w", commitErr)
			}
			return nil
		}

		_ = tx.Rollback(ctx)
		if !isRetryablePgError(err) || attempt == maxRetries {
			return err
		}
		time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
	}
	return nil
}

// WithRetrySqlTx is a temporary bridge for legacy *sql.DB transactions during migration.
func WithRetrySqlTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	const maxRetries = 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		err = fn(tx)
		if err == nil {
			return tx.Commit()
		}
		_ = tx.Rollback()
		if !isRetryablePgError(err) || attempt == maxRetries {
			return err
		}
		time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
	}
	return nil
}

func isRetryablePgError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", "40P01": // serialization_failure, deadlock_detected
			return true
		}
	}
	return false
}

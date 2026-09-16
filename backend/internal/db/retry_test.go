package db

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsRetryablePgError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "generic standard error",
			err:      errors.New("generic connection error"),
			expected: false,
		},
		{
			name: "serialization_failure (40001)",
			err: &pgconn.PgError{
				Code:    "40001",
				Message: "could not serialize access due to concurrent update",
			},
			expected: true,
		},
		{
			name: "deadlock_detected (40P01)",
			err: &pgconn.PgError{
				Code:    "40P01",
				Message: "deadlock detected",
			},
			expected: true,
		},
		{
			name: "foreign_key_violation (23503) not retryable",
			err: &pgconn.PgError{
				Code:    "23503",
				Message: "foreign key violation",
			},
			expected: false,
		},
		{
			name: "unique_violation (23505) not retryable",
			err: &pgconn.PgError{
				Code:    "23505",
				Message: "unique constraint violation",
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := isRetryablePgError(tc.err)
			if actual != tc.expected {
				t.Fatalf("expected %v, got %v for error: %v", tc.expected, actual, tc.err)
			}
		})
	}
}

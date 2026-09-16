package db

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestStringArrayHandling(t *testing.T) {
	var sa StringArray

	// Nil / empty slice
	if len(sa.Slice()) != 0 {
		t.Fatalf("expected empty slice from uninitialized StringArray, got %v", sa.Slice())
	}

	// Scan valid text array string format
	err := sa.Scan("{foo,bar,baz}")
	if err != nil {
		t.Fatalf("unexpected scan error: %v", err)
	}

	slice := sa.Slice()
	if len(slice) != 3 || slice[0] != "foo" || slice[1] != "bar" || slice[2] != "baz" {
		t.Fatalf("unexpected slice values: %v", slice)
	}

	// Scan nil
	err = sa.Scan(nil)
	if err != nil {
		t.Fatalf("unexpected scan nil error: %v", err)
	}
	if len(sa.Slice()) != 0 {
		t.Fatalf("expected empty slice after scanning nil")
	}
}

type mockDBTX struct{}

func (m *mockDBTX) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag(""), nil
}

func (m *mockDBTX) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, nil
}

func (m *mockDBTX) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return nil
}

func TestDBTXInterfaceVerification(t *testing.T) {
	// Verify that mockDBTX satisfies DBTX interface
	var _ DBTX = (*mockDBTX)(nil)
}

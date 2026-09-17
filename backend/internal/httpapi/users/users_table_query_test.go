package users

import (
	"strings"
	"testing"
)

func TestBuildUsersTableQuery_SQLInjectionProtection(t *testing.T) {
	// Attempt SQL injection via sorting column ID and filter operators
	maliciousSort := []usersTableSorting{
		{ID: "username; DROP TABLE users; --", Desc: true},
		{ID: "1=1", Desc: false},
	}

	_, orderSQL, _, err := buildUsersTableQuery(nil, nil, maliciousSort)
	if err != nil {
		t.Fatalf("unexpected error on malicious sort: %v", err)
	}

	// Should safely fallback to default order or ignore malicious column
	if strings.Contains(orderSQL, "DROP TABLE") || strings.Contains(orderSQL, "1=1") {
		t.Fatalf("SQL injection vulnerability detected in order clause: %s", orderSQL)
	}
	if orderSQL != "ORDER BY u.id DESC" {
		t.Fatalf("expected fallback order, got %s", orderSQL)
	}

	// Filter injection attempt
	maliciousFilter := []usersTableFilter{
		{ID: "username", Value: "admin' OR '1'='1"},
	}
	whereSQL, _, args, err := buildUsersTableQuery(maliciousFilter, map[string]string{"username": "equals"}, nil)
	if err != nil {
		t.Fatalf("unexpected error on filter build: %v", err)
	}

	// The raw string must be passed as parameterized arg, never interpolated into whereSQL
	if strings.Contains(whereSQL, "admin'") || strings.Contains(whereSQL, "'1'='1") {
		t.Fatalf("Filter argument was interpolated directly into SQL: %s", whereSQL)
	}
	if len(args) != 1 || args[0] != "admin' OR '1'='1" {
		t.Fatalf("Malicious payload must be safely parameterized in args, got: %v", args)
	}
}

func TestBuildUsersTableQuery_MalformedFilters(t *testing.T) {
	// Filter with unknown field should be safely ignored without breaking SQL
	unknownFilter := []usersTableFilter{
		{ID: "non_existent_column_injection", Value: "something"},
	}
	whereSQL, _, args, err := buildUsersTableQuery(unknownFilter, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error on unknown column: %v", err)
	}
	if whereSQL != "" {
		t.Fatalf("expected empty whereSQL for unknown column, got: %s", whereSQL)
	}
	if len(args) != 0 {
		t.Fatalf("expected 0 args for unknown column, got: %d", len(args))
	}
}

package users

import (
	"strings"
	"testing"
)

func TestBuildUsersTableQuery_Sorting(t *testing.T) {
	tests := []struct {
		name          string
		sorting       []usersTableSorting
		expectedOrder string
	}{
		{
			name:          "Default sorting when empty",
			sorting:       nil,
			expectedOrder: "ORDER BY u.id DESC",
		},
		{
			name: "Sort by username ASC",
			sorting: []usersTableSorting{
				{ID: "username", Desc: false},
			},
			expectedOrder: "ORDER BY u.username ASC NULLS LAST",
		},
		{
			name: "Sort by usedTrafficBytes DESC",
			sorting: []usersTableSorting{
				{ID: "usedTrafficBytes", Desc: true},
			},
			expectedOrder: "ORDER BY ut.used_traffic_bytes DESC NULLS LAST",
		},
		{
			name: "Sort by userTraffic.onlineAt DESC",
			sorting: []usersTableSorting{
				{ID: "userTraffic.onlineAt", Desc: true},
			},
			expectedOrder: "ORDER BY ut.online_at DESC NULLS LAST",
		},
		{
			name: "Sort by snake_case created_at ASC",
			sorting: []usersTableSorting{
				{ID: "created_at", Desc: false},
			},
			expectedOrder: "ORDER BY u.created_at ASC NULLS LAST",
		},
		{
			name: "Sort by usedTrafficPercentage DESC",
			sorting: []usersTableSorting{
				{ID: "usedTrafficPercentage", Desc: true},
			},
			expectedOrder: "ORDER BY CAST(COALESCE(ut.used_traffic_bytes, 0) AS NUMERIC) / NULLIF(u.traffic_limit_bytes, 0) DESC NULLS LAST",
		},
		{
			name: "Unknown sort ID is safely skipped",
			sorting: []usersTableSorting{
				{ID: "someBogusColumn", Desc: true},
			},
			expectedOrder: "ORDER BY u.id DESC",
		},
		{
			name: "Mixed known and unknown sort IDs",
			sorting: []usersTableSorting{
				{ID: "someBogusColumn", Desc: true},
				{ID: "id", Desc: true},
			},
			expectedOrder: "ORDER BY u.id DESC NULLS LAST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, orderSQL, _, err := buildUsersTableQuery(nil, nil, tt.sorting)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if orderSQL != tt.expectedOrder {
				t.Errorf("expected %q, got %q", tt.expectedOrder, orderSQL)
			}
		})
	}
}

func TestBuildUsersTableQuery_Filters(t *testing.T) {
	filters := []usersTableFilter{
		{ID: "username", Value: "test"},
		{ID: "status", Value: []any{"ACTIVE", "DISABLED"}},
	}
	filterModes := map[string]string{
		"username": "contains",
		"status":   "equals",
	}

	whereSQL, _, args, err := buildUsersTableQuery(filters, filterModes, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(whereSQL, "u.username ILIKE $1") {
		t.Errorf("whereSQL missing username clause: %s", whereSQL)
	}
	if !strings.Contains(whereSQL, "u.status = ANY($2)") {
		t.Errorf("whereSQL missing status ANY clause: %s", whereSQL)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}
}

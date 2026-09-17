package scheduler

import (
	"testing"
	"time"
)

func TestLatestNodeTrafficResetBoundary(t *testing.T) {
	location := time.FixedZone("UTC+7", 7*60*60)

	tests := []struct {
		name     string
		now      time.Time
		resetDay int
		want     time.Time
	}{
		{
			name:     "first day already passed",
			now:      time.Date(2026, time.June, 3, 1, 30, 0, 0, location),
			resetDay: 1,
			want:     time.Date(2026, time.June, 1, 1, 0, 0, 0, location),
		},
		{
			name:     "future day uses previous month",
			now:      time.Date(2026, time.June, 3, 1, 30, 0, 0, location),
			resetDay: 15,
			want:     time.Date(2026, time.May, 15, 1, 0, 0, 0, location),
		},
		{
			name:     "day beyond short month falls back to last day (Feb non-leap year)",
			now:      time.Date(2026, time.February, 28, 2, 0, 0, 0, location),
			resetDay: 31,
			want:     time.Date(2026, time.February, 28, 1, 0, 0, 0, location),
		},
		{
			name:     "before reset hour uses previous month",
			now:      time.Date(2026, time.February, 28, 0, 30, 0, 0, location),
			resetDay: 31,
			want:     time.Date(2026, time.January, 31, 1, 0, 0, 0, location),
		},
		{
			name:     "day 31 in 30-day month falls back to 30th (April)",
			now:      time.Date(2026, time.May, 2, 12, 0, 0, 0, location),
			resetDay: 31,
			want:     time.Date(2026, time.April, 30, 1, 0, 0, 0, location),
		},
		{
			name:     "day 29 in Feb leap year (2024)",
			now:      time.Date(2024, time.February, 29, 2, 0, 0, 0, location),
			resetDay: 29,
			want:     time.Date(2024, time.February, 29, 1, 0, 0, 0, location),
		},
		{
			name:     "day 30 in Feb leap year (2024) falls back to Feb 29",
			now:      time.Date(2024, time.February, 29, 2, 0, 0, 0, location),
			resetDay: 30,
			want:     time.Date(2024, time.February, 29, 1, 0, 0, 0, location),
		},
		{
			name:     "reset day clamping below 1 defaults to 1",
			now:      time.Date(2026, time.June, 10, 2, 0, 0, 0, location),
			resetDay: 0,
			want:     time.Date(2026, time.June, 1, 1, 0, 0, 0, location),
		},
		{
			name:     "reset day clamping above 31 clamps to 31",
			now:      time.Date(2026, time.July, 31, 2, 0, 0, 0, location),
			resetDay: 35,
			want:     time.Date(2026, time.July, 31, 1, 0, 0, 0, location),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := latestNodeTrafficResetBoundary(tt.now, tt.resetDay)
			if !got.Equal(tt.want) {
				t.Fatalf("boundary mismatch: got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestNodeTrafficResetDue(t *testing.T) {
	location := time.FixedZone("UTC+7", 7*60*60)
	now := time.Date(2026, time.June, 3, 1, 30, 0, 0, location)
	boundary := time.Date(2026, time.June, 1, 1, 0, 0, 0, location)

	boundaryPlusHour := boundary.Add(time.Hour)

	tests := []struct {
		name        string
		createdAt   time.Time
		lastResetAt *time.Time
		wantDue     bool
	}{
		{
			name:        "due when no reset exists after boundary",
			createdAt:   time.Date(2026, time.May, 1, 0, 0, 0, 0, location),
			lastResetAt: nil,
			wantDue:     true,
		},
		{
			name:        "not due when node was created after boundary",
			createdAt:   time.Date(2026, time.June, 2, 0, 0, 0, 0, location),
			lastResetAt: nil,
			wantDue:     false,
		},
		{
			name:        "not due when reset already happened at boundary",
			createdAt:   time.Date(2026, time.May, 1, 0, 0, 0, 0, location),
			lastResetAt: &boundary,
			wantDue:     false,
		},
		{
			name:        "not due when reset already happened after boundary",
			createdAt:   time.Date(2026, time.May, 1, 0, 0, 0, 0, location),
			lastResetAt: &boundaryPlusHour,
			wantDue:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBoundary, gotDue := nodeTrafficResetDue(now, 1, tt.createdAt, tt.lastResetAt)
			if !gotBoundary.Equal(boundary) {
				t.Fatalf("boundary mismatch: got %s, want %s", gotBoundary, boundary)
			}
			if gotDue != tt.wantDue {
				t.Fatalf("due mismatch: got %t, want %t", gotDue, tt.wantDue)
			}
		})
	}
}

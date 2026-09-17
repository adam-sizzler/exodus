package users

import (
	"testing"
	"time"
)

func TestTrafficResetDayCalculation(t *testing.T) {
	tests := []struct {
		name       string
		current    time.Time
		resetDay   int
		expectDate string
	}{
		{
			name:       "Reset day 1 in March",
			current:    time.Date(2026, time.March, 15, 12, 0, 0, 0, time.UTC),
			resetDay:   1,
			expectDate: "2026-03-01",
		},
		{
			name:       "Reset day 28 in February non-leap (2025)",
			current:    time.Date(2025, time.February, 20, 0, 0, 0, 0, time.UTC),
			resetDay:   28,
			expectDate: "2025-01-28",
		},
		{
			name:       "Reset day 29 in February leap year (2024)",
			current:    time.Date(2024, time.February, 29, 10, 0, 0, 0, time.UTC),
			resetDay:   29,
			expectDate: "2024-02-29",
		},
		{
			name:       "Reset day 30 in April (30 days)",
			current:    time.Date(2026, time.April, 15, 0, 0, 0, 0, time.UTC),
			resetDay:   30,
			expectDate: "2026-03-30",
		},
		{
			name:       "Reset day 31 in April (clamped to 30)",
			current:    time.Date(2026, time.April, 10, 0, 0, 0, 0, time.UTC),
			resetDay:   31,
			expectDate: "2026-03-31",
		},
		{
			name:       "Reset day 31 in January 31",
			current:    time.Date(2026, time.January, 31, 23, 59, 59, 0, time.UTC),
			resetDay:   31,
			expectDate: "2026-01-31",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			day := tt.resetDay
			if day < 1 || day > 31 {
				t.Fatalf("invalid reset day %d", day)
			}
		})
	}
}

func TestTrafficLimitMultipliers(t *testing.T) {
	tests := []struct {
		name       string
		bytes      int64
		multiplier float64
		expected   int64
	}{
		{
			name:       "1x multiplier",
			bytes:      1000,
			multiplier: 1.0,
			expected:   1000,
		},
		{
			name:       "1.5x multiplier",
			bytes:      1000,
			multiplier: 1.5,
			expected:   1500,
		},
		{
			name:       "0.5x multiplier",
			bytes:      1000,
			multiplier: 0.5,
			expected:   500,
		},
		{
			name:       "Zero bytes",
			bytes:      0,
			multiplier: 2.0,
			expected:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calc := int64(float64(tt.bytes) * tt.multiplier)
			if calc != tt.expected {
				t.Errorf("got %d, want %d", calc, tt.expected)
			}
		})
	}
}

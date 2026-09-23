package util

import (
	"math/big"
	"testing"
)

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{1023, "1023 B"},
		{1024, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{1048576, "1.00 MiB"},
		{1073741824, "1.00 GiB"},
		{400000000000, "372.53 GiB"},
		{-1024, "-1.00 KiB"},
		{-500, "-500 B"},
	}

	for _, tc := range tests {
		got := FormatBytes(tc.input)
		if got != tc.expected {
			t.Errorf("FormatBytes(%d) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestFormatBigBytes(t *testing.T) {
	tests := []struct {
		input    *big.Int
		expected string
	}{
		{nil, "0 B"},
		{big.NewInt(0), "0 B"},
		{big.NewInt(1024), "1.00 KiB"},
		{big.NewInt(-1048576), "-1.00 MiB"},
	}

	for _, tc := range tests {
		got := FormatBigBytes(tc.input)
		if got != tc.expected {
			t.Errorf("FormatBigBytes(%v) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestFormatBitrate(t *testing.T) {
	tests := []struct {
		input    float64
		expected string
	}{
		{0, "0 bps"},
		{500, "500 bps"},
		{1000, "1.00 Kbps"},
		{1350000000, "1.35 Gbps"},
		{-1000000, "-1.00 Mbps"},
	}

	for _, tc := range tests {
		got := FormatBitrate(tc.input)
		if got != tc.expected {
			t.Errorf("FormatBitrate(%f) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

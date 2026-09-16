package cmd

import (
	"bufio"
	"strings"
	"testing"
	"time"
)

func TestFormatThousands(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0"},
		{5, "5"},
		{999, "999"},
		{1000, "1,000"},
		{50000, "50,000"},
		{1234567, "1,234,567"},
		{-1000, "-1,000"},
		{-54321, "-54,321"},
	}

	for _, tc := range tests {
		actual := formatThousands(tc.input)
		if actual != tc.expected {
			t.Errorf("formatThousands(%d) = %q, expected %q", tc.input, actual, tc.expected)
		}
	}
}

func TestPromptStrictDate(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("15-08-2024\n"))
	d, err := promptStrictDate(reader, "test", "01-01-2024")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := time.Date(2024, time.August, 15, 0, 0, 0, 0, time.UTC)
	if !d.Equal(expected) {
		t.Errorf("expected date %v, got %v", expected, d)
	}

	// Invalid format
	invalidReader := bufio.NewReader(strings.NewReader("2024-08-15\n"))
	_, err = promptStrictDate(invalidReader, "test", "01-01-2024")
	if err == nil {
		t.Fatalf("expected error on invalid date format, got nil")
	}
}

func TestRescueActionsList(t *testing.T) {
	actions := rescueActions()
	if len(actions) == 0 {
		t.Fatalf("expected non-empty rescue actions list")
	}

	foundExit := false
	for _, a := range actions {
		if a.Value == "exit" {
			foundExit = true
			break
		}
	}
	if !foundExit {
		t.Errorf("expected 'exit' action in rescue actions list")
	}
}

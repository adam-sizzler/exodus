package externalsquads

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"exodus/internal/httpapi/shared"
)

func TestConvertExternalSquadToAPI_ResponseHeadersRemove(t *testing.T) {
	rec := ExternalSquadRecord{
		UUID:                  "11111111-1111-1111-1111-111111111111",
		ViewPosition:          1,
		Name:                  "Test Squad",
		ResponseHeadersAdd:    json.RawMessage(`{"announce":"test"}`),
		ResponseHeadersRemove: json.RawMessage(`["announce","routing"]`),
		CreatedAt:             time.Now(),
		UpdatedAt:             time.Now(),
	}

	api, err := convertExternalSquadToAPI(rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedRemove := []string{"announce", "routing"}
	if !reflect.DeepEqual(api.ResponseHeadersRemove, expectedRemove) {
		t.Fatalf("expected responseHeadersRemove %v, got %v", expectedRemove, api.ResponseHeadersRemove)
	}
	if api.ResponseHeadersAdd["announce"] != "test" {
		t.Fatalf("expected responseHeadersAdd to have announce=test, got %v", api.ResponseHeadersAdd)
	}
}

func strPtr(s string) *string {
	return &s
}

func TestParseJSONRaw(t *testing.T) {
	tests := []struct {
		name     string
		input    *string
		expected string
	}{
		{
			name:     "null string",
			input:    nil,
			expected: "",
		},
		{
			name:     "empty string",
			input:    strPtr(""),
			expected: "",
		},
		{
			name:     "valid json string",
			input:    strPtr(`["announce"]`),
			expected: `["announce"]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := parseJSONRaw(tt.input)
			if string(actual) != tt.expected {
				t.Fatalf("expected %s, got %s", tt.expected, string(actual))
			}
		})
	}
}

func TestFormatAndParsePgArrayIntegration(t *testing.T) {
	headers := []string{"announce", "routing", "support-url"}
	formatted := shared.PostgresTextArrayLiteral(headers)
	parsed := shared.ParsePgTextArray(formatted)

	if !reflect.DeepEqual(parsed, headers) {
		t.Fatalf("roundtrip failed: expected %v, got %v", headers, parsed)
	}
}

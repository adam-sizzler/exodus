package db

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestParseConfigInbounds_Success(t *testing.T) {
	configJSON := json.RawMessage(`{
		"inbounds": [
			{
				"type": "vless",
				"tag": "vless-in",
				"listen_port": 443,
				"transport": {
					"type": "ws"
				},
				"tls": {
					"enabled": true
				}
			},
			{
				"protocol": "vmess",
				"tag": "vmess-in",
				"port": 8080,
				"network": "tcp",
				"security": "auto"
			}
		]
	}`)

	profileUUID := "11111111-1111-1111-1111-111111111111"
	inbounds, err := parseConfigInbounds(profileUUID, configJSON)
	if err != nil {
		t.Fatalf("unexpected error parsing valid inbounds: %v", err)
	}

	if len(inbounds) != 2 {
		t.Fatalf("expected 2 inbounds, got %d", len(inbounds))
	}

	// First inbound assertions
	ib1 := inbounds[0]
	if ib1.ProfileUUID != profileUUID {
		t.Errorf("expected profileUUID %s, got %s", profileUUID, ib1.ProfileUUID)
	}
	if ib1.Tag != "vless-in" {
		t.Errorf("expected tag vless-in, got %s", ib1.Tag)
	}
	if ib1.Type != "vless" {
		t.Errorf("expected type vless, got %s", ib1.Type)
	}
	if ib1.Port == nil || *ib1.Port != 443 {
		t.Errorf("expected port 443, got %v", ib1.Port)
	}
	if ib1.Network == nil || *ib1.Network != "ws" {
		t.Errorf("expected network ws from transport.type, got %v", ib1.Network)
	}
	if ib1.Security == nil || *ib1.Security != "tls" {
		t.Errorf("expected security tls, got %v", ib1.Security)
	}
	if len(ib1.RawInbound) == 0 {
		t.Errorf("expected non-empty raw_inbound")
	}

	// Second inbound assertions
	ib2 := inbounds[1]
	if ib2.Tag != "vmess-in" {
		t.Errorf("expected tag vmess-in, got %s", ib2.Tag)
	}
	if ib2.Type != "vmess" {
		t.Errorf("expected protocol fallback to type vmess, got %s", ib2.Type)
	}
	if ib2.Port == nil || *ib2.Port != 8080 {
		t.Errorf("expected port 8080, got %v", ib2.Port)
	}
	if ib2.Network == nil || *ib2.Network != "tcp" {
		t.Errorf("expected network tcp, got %v", ib2.Network)
	}
	if ib2.Security == nil || *ib2.Security != "auto" {
		t.Errorf("expected security auto, got %v", ib2.Security)
	}
}

func TestParseConfigInbounds_EmptyInbounds(t *testing.T) {
	// Missing inbounds key
	inbounds, err := parseConfigInbounds("prof-1", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("unexpected error on missing inbounds: %v", err)
	}
	if len(inbounds) != 0 {
		t.Fatalf("expected 0 inbounds, got %d", len(inbounds))
	}

	// Empty inbounds array
	inbounds, err = parseConfigInbounds("prof-1", json.RawMessage(`{"inbounds": []}`))
	if err != nil {
		t.Fatalf("unexpected error on empty inbounds array: %v", err)
	}
	if len(inbounds) != 0 {
		t.Fatalf("expected 0 inbounds, got %d", len(inbounds))
	}
}

func TestParseConfigInbounds_ValidationErrors(t *testing.T) {
	tests := []struct {
		name        string
		configJSON  string
		errContains string
	}{
		{
			name:        "invalid json",
			configJSON:  `{not valid json`,
			errContains: "failed to parse config JSON",
		},
		{
			name:        "inbounds is not an array",
			configJSON:  `{"inbounds": "not-an-array"}`,
			errContains: "inbounds must be an array",
		},
		{
			name:        "empty tag",
			configJSON:  `{"inbounds": [{"type": "vless", "tag": "   "}]}`,
			errContains: "non-empty tag",
		},
		{
			name:        "missing tag",
			configJSON:  `{"inbounds": [{"type": "vless"}]}`,
			errContains: "non-empty tag",
		},
		{
			name:        "tag with comma",
			configJSON:  `{"inbounds": [{"type": "vless", "tag": "tag,one"}]}`,
			errContains: "character ',' is not allowed",
		},
		{
			name: "duplicate tags in same profile",
			configJSON: `{
				"inbounds": [
					{"type": "vless", "tag": "my-tag"},
					{"type": "vmess", "tag": "my-tag"}
				]
			}`,
			errContains: "duplicate inbound tag \"my-tag\" found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseConfigInbounds("prof-test", json.RawMessage(tc.configJSON))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errContains)
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Fatalf("expected error containing %q, got %q", tc.errContains, err.Error())
			}
		})
	}
}

func TestExtractInboundHelpers(t *testing.T) {
	// Transport type precedence over network
	m1 := map[string]any{
		"transport": map[string]any{"type": "httpupgrade"},
		"network":   "tcp",
	}
	if net := extractInboundNetwork(m1); net != "httpupgrade" {
		t.Errorf("expected httpupgrade, got %s", net)
	}

	// Direct network fallback
	m2 := map[string]any{
		"network": "ws",
	}
	if net := extractInboundNetwork(m2); net != "ws" {
		t.Errorf("expected ws, got %s", net)
	}

	// TLS enabled precedence
	m3 := map[string]any{
		"tls":      map[string]any{"enabled": true},
		"security": "reality",
	}
	if sec := extractInboundSecurity(m3); sec != "tls" {
		t.Errorf("expected tls, got %s", sec)
	}

	// Security fallback
	m4 := map[string]any{
		"security": "reality",
	}
	if sec := extractInboundSecurity(m4); sec != "reality" {
		t.Errorf("expected reality, got %s", sec)
	}
}

// recordingMockDBTX records executed SQL and parameters for assertion.
type recordingMockDBTX struct {
	executedQueries []string
	executedArgs    [][]any
	mockRows        pgx.Rows
}

func (r *recordingMockDBTX) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	r.executedQueries = append(r.executedQueries, strings.TrimSpace(sql))
	r.executedArgs = append(r.executedArgs, arguments)
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (r *recordingMockDBTX) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	r.executedQueries = append(r.executedQueries, strings.TrimSpace(sql))
	r.executedArgs = append(r.executedArgs, args)
	return r.mockRows, nil
}

func (r *recordingMockDBTX) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return nil
}

func (r *recordingMockDBTX) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return nil
}

// mockEmptyRows satisfies pgx.Rows for zero rows returned
type mockEmptyRows struct{}

func (m *mockEmptyRows) Close()                                       {}
func (m *mockEmptyRows) Err() error                                   { return nil }
func (m *mockEmptyRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (m *mockEmptyRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (m *mockEmptyRows) Next() bool                                   { return false }
func (m *mockEmptyRows) Scan(dest ...any) error                       { return nil }
func (m *mockEmptyRows) Values() ([]any, error)                       { return nil, nil }
func (m *mockEmptyRows) RawValues() [][]byte                          { return nil }
func (m *mockEmptyRows) Conn() *pgx.Conn                              { return nil }
func (m *mockEmptyRows) TypeMap() *pgtype.Map                         { return nil }

func TestSyncConfigProfileInboundsTx_TagIsolationAndCleanup(t *testing.T) {
	ctx := context.Background()
	mockDB := &recordingMockDBTX{
		mockRows: &mockEmptyRows{},
	}

	profileUUID := "prof-aaa"
	configJSON := json.RawMessage(`{
		"inbounds": [
			{"type": "vless", "tag": "inbound-1", "listen_port": 10001}
		]
	}`)

	count, err := SyncConfigProfileInboundsTx(ctx, mockDB, profileUUID, configJSON)
	if err != nil {
		t.Fatalf("unexpected error syncing inbounds: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}

	// Verify that profile_uuid is strictly isolated to the specified profile
	foundSelect := false
	foundInsert := false
	foundDelete := false

	for i, q := range mockDB.executedQueries {
		if strings.Contains(q, "SELECT uuid, tag FROM config_profile_inbounds WHERE profile_uuid = $1") {
			foundSelect = true
			if len(mockDB.executedArgs[i]) > 0 && mockDB.executedArgs[i][0] != profileUUID {
				t.Errorf("SELECT query used wrong profileUUID: %v", mockDB.executedArgs[i][0])
			}
		}
		if strings.Contains(q, "INSERT INTO config_profile_inbounds") {
			foundInsert = true
			// $2 is profile_uuid
			if len(mockDB.executedArgs[i]) > 1 && mockDB.executedArgs[i][1] != profileUUID {
				t.Errorf("INSERT query used wrong profileUUID: %v", mockDB.executedArgs[i][1])
			}
		}
		if strings.Contains(q, "DELETE FROM config_profile_inbounds") && strings.Contains(q, "NOT (tag = ANY($2))") {
			foundDelete = true
			if len(mockDB.executedArgs[i]) > 0 && mockDB.executedArgs[i][0] != profileUUID {
				t.Errorf("DELETE query used wrong profileUUID: %v", mockDB.executedArgs[i][0])
			}
		}
	}

	if !foundSelect {
		t.Errorf("expected SELECT query for existing inbounds was not executed")
	}
	if !foundInsert {
		t.Errorf("expected INSERT query for new inbounds was not executed")
	}
	if !foundDelete {
		t.Errorf("expected DELETE query for obsolete inbounds was not executed")
	}
}

func TestSyncConfigProfileInboundsTx_AtomicRejectionOnDuplicateTags(t *testing.T) {
	ctx := context.Background()
	mockDB := &recordingMockDBTX{
		mockRows: &mockEmptyRows{},
	}

	duplicateConfigJSON := json.RawMessage(`{
		"inbounds": [
			{"type": "vless", "tag": "same-tag", "port": 443},
			{"type": "vmess", "tag": "same-tag", "port": 80}
		]
	}`)

	_, err := SyncConfigProfileInboundsTx(ctx, mockDB, "profile-uuid", duplicateConfigJSON)
	if err == nil {
		t.Fatalf("expected error on duplicate tags, got nil")
	}

	// Verify no SQL queries were executed (zero DB side-effects on config validation failure)
	if len(mockDB.executedQueries) != 0 {
		t.Fatalf("expected 0 queries on invalid config, got %d", len(mockDB.executedQueries))
	}
}

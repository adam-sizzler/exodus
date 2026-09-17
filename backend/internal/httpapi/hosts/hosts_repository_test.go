package hosts

import (
	"testing"
)

type mockRowScanner struct {
	values []any
}

func (m *mockRowScanner) Scan(dest ...any) error {
	for i, d := range dest {
		if i >= len(m.values) {
			break
		}
		val := m.values[i]
		if val == nil {
			continue
		}
		switch target := d.(type) {
		case *string:
			if s, ok := val.(string); ok {
				*target = s
			}
		case **string:
			if s, ok := val.(string); ok {
				*target = &s
			}
		case *int:
			if v, ok := val.(int); ok {
				*target = v
			}
		case **int:
			if v, ok := val.(int); ok {
				*target = &v
			}
		case *bool:
			if b, ok := val.(bool); ok {
				*target = b
			}
		case **bool:
			if b, ok := val.(bool); ok {
				*target = &b
			}
		case *[]byte:
			if b, ok := val.([]byte); ok {
				*target = b
			}
		case *[]string:
			if s, ok := val.([]string); ok {
				*target = s
			}
		}
	}
	return nil
}

func TestScanHostRecord(t *testing.T) {
	mock := &mockRowScanner{
		values: []any{
			"test-uuid",        // uuid
			10,                 // view_position
			"Test Remark",      // remark
			"1.2.3.4",          // address
			443,                // port
			"/vless",           // path
			"sni.example.com",  // sni
			"host.example.com", // host
			"h2",               // alpn
			"chrome",           // fingerprint
			"TLS",              // security_layer
			[]byte("{}"),       // xhttp_extra_params
			[]byte("{}"),       // mux_params
			[]byte(`{"k":"v"}`),// mapper
			[]byte("{}"),       // sockopt_params
			[]byte("{}"),       // final_mask
			false,              // is_disabled
			"server desc",      // server_description
			1,                  // vless_route_id
			"pinned_sha",       // pinned_peer_cert_sha256
			"peer_name",        // verify_peer_cert_by_name
			true,               // shuffle_host
			false,              // mihomo_x25519
			"ipv4",             // mihomo_ip_version
			"tmpl-uuid",        // xray_json_template_uuid
			false,              // keep_sni_blank
			[]string{"tag1"},   // tags
			false,              // is_hidden
			true,               // override_sni_from_address
			"prof-uuid",        // config_profile_uuid
			"inbound-uuid",     // config_profile_inbound_uuid
			[]string{"type1"},  // exclude_from_subscription_types
			"ALLOW_ONLY",       // internal_squads_mode
		},
	}

	rec, err := scanHostRecord(mock)
	if err != nil {
		t.Fatalf("unexpected scan error: %v", err)
	}

	if rec.UUID != "test-uuid" {
		t.Errorf("UUID = %q, want 'test-uuid'", rec.UUID)
	}
	if rec.ViewPosition != 10 {
		t.Errorf("ViewPosition = %d, want 10", rec.ViewPosition)
	}
	if rec.SecurityLayer != "TLS" {
		t.Errorf("SecurityLayer = %q, want 'TLS'", rec.SecurityLayer)
	}
	if rec.InternalSquadsMode != "ALLOW_ONLY" {
		t.Errorf("InternalSquadsMode = %q, want 'ALLOW_ONLY'", rec.InternalSquadsMode)
	}
	if len(rec.Tags) != 1 || rec.Tags[0] != "tag1" {
		t.Errorf("Tags = %v, want ['tag1']", rec.Tags)
	}
	if len(rec.ExcludeTypes) != 1 || rec.ExcludeTypes[0] != "type1" {
		t.Errorf("ExcludeTypes = %v, want ['type1']", rec.ExcludeTypes)
	}
}

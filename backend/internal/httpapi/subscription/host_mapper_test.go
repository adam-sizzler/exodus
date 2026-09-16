package subscription

import (
	"testing"

	"github.com/iancoleman/orderedmap"
)

func TestApplyHostMapperToOrderedMap(t *testing.T) {
	outbound := orderedmap.New()
	outbound.Set("type", "vless")
	outbound.Set("server", "example.com")
	outbound.Set("server_port", 443)

	multiplex := orderedmap.New()
	multiplex.Set("enabled", true)
	outbound.Set("multiplex", multiplex)

	ops := []HostMapperOperation{
		{Op: "set", To: "packet_encoding", Value: "xudp"},
		{Op: "set", To: "tls.utls.fingerprint", Value: "chrome"},
		{Op: "set", To: "min_idle_session", Value: 4},
		{Op: "unset", To: "multiplex"},
	}

	host := SubscriptionHost{
		Address: "example.com",
		Port:    443,
	}

	ApplyHostMapperToOrderedMap(outbound, ops, host)

	// Check set packet_encoding
	if val, ok := outbound.Get("packet_encoding"); !ok || val != "xudp" {
		t.Fatalf("expected packet_encoding = xudp, got %v", val)
	}

	// Check set tls.utls.fingerprint
	if tlsVal, ok := outbound.Get("tls"); !ok {
		t.Fatalf("expected tls object to exist")
	} else if tlsOM, ok := tlsVal.(*orderedmap.OrderedMap); !ok {
		t.Fatalf("expected tls to be OrderedMap")
	} else if utlsVal, ok := tlsOM.Get("utls"); !ok {
		t.Fatalf("expected utls to exist")
	} else if utlsOM, ok := utlsVal.(*orderedmap.OrderedMap); !ok {
		t.Fatalf("expected utls to be OrderedMap")
	} else if fp, ok := utlsOM.Get("fingerprint"); !ok || fp != "chrome" {
		t.Fatalf("expected fingerprint = chrome, got %v", fp)
	}

	// Check min_idle_session
	if val, ok := outbound.Get("min_idle_session"); !ok || val != 4 {
		t.Fatalf("expected min_idle_session = 4, got %v", val)
	}

	// Check unset multiplex
	if _, ok := outbound.Get("multiplex"); ok {
		t.Fatalf("expected multiplex to be unset")
	}
}

func TestApplyHostMapperToMap(t *testing.T) {
	node := map[string]interface{}{
		"name":   "test-node",
		"type":   "trojan",
		"server": "1.2.3.4",
		"port":   443,
		"smux": map[string]interface{}{
			"enabled": true,
		},
	}

	ops := []HostMapperOperation{
		{Op: "set", To: "packet-encoding", Value: "xudp"},
		{Op: "set", To: "grpc-opts.max-connections", Value: 6},
		{Op: "set", To: "idle-session-timeout", Value: 30},
		{Op: "unset", To: "smux"},
		{Op: "copy", From: "$host.address", To: "custom-addr"},
	}

	host := SubscriptionHost{
		Address: "1.2.3.4",
		Port:    443,
	}

	ApplyHostMapperToMap(node, ops, host)

	if node["packet-encoding"] != "xudp" {
		t.Fatalf("expected packet-encoding = xudp, got %v", node["packet-encoding"])
	}

	grpcOpts, ok := node["grpc-opts"].(map[string]interface{})
	if !ok || grpcOpts["max-connections"] != 6 {
		t.Fatalf("expected grpc-opts.max-connections = 6, got %v", grpcOpts)
	}

	if node["idle-session-timeout"] != 30 {
		t.Fatalf("expected idle-session-timeout = 30, got %v", node["idle-session-timeout"])
	}

	if _, ok := node["smux"]; ok {
		t.Fatalf("expected smux to be unset")
	}

	if node["custom-addr"] != "1.2.3.4" {
		t.Fatalf("expected custom-addr = 1.2.3.4, got %v", node["custom-addr"])
	}
}

func TestApplyHostMapperCopyFromRawInbound(t *testing.T) {
	rawInbound := []byte(`{
		"streamSettings": {
			"realitySettings": {
				"serverNames": ["test.domain.com", "other.domain.com"]
			}
		}
	}`)

	node := map[string]interface{}{
		"name": "reality-node",
	}

	ops := []HostMapperOperation{
		{Op: "copy", From: "streamSettings.realitySettings.serverNames.0", To: "servername"},
	}

	host := SubscriptionHost{
		InboundRaw: rawInbound,
	}

	ApplyHostMapperToMap(node, ops, host)

	if node["servername"] != "test.domain.com" {
		t.Fatalf("expected servername = test.domain.com, got %v", node["servername"])
	}
}

func BenchmarkParseHostMapper(b *testing.B) {
	emptyPayload := []byte("{}")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ParseHostMapper(emptyPayload)
	}
}

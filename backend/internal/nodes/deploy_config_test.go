package users

import (
	"encoding/json"
	"testing"

	"github.com/iancoleman/orderedmap"
)

func TestBuildInboundUsersUsesSingboxProtocolCredentials(t *testing.T) {
	users := []inboundUserCredentials{
		{
			Username:       "alice",
			VLESSUUID:      "9f76f8d8-daf1-4db6-b045-0987cd5e09a2",
			TrojanPassword: "trojan-secret",
			Hysteria2Pass:  "hy-secret",
		},
	}

	tests := []struct {
		name      string
		protocol  string
		wantKey   string
		wantValue any
	}{
		{name: "vmess uses uuid", protocol: "vmess", wantKey: "uuid", wantValue: users[0].VLESSUUID},
		{name: "hysteria uses auth_str", protocol: "hysteria", wantKey: "auth_str", wantValue: "hy-secret"},
		{name: "hysteria2 uses password", protocol: "hysteria2", wantKey: "password", wantValue: "hy-secret"},
		{name: "tuic uses password", protocol: "tuic", wantKey: "password", wantValue: "trojan-secret"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			items := buildInboundUsers(tc.protocol, users)
			rawJSON, err := json.Marshal(items)
			if err != nil {
				t.Fatalf("failed to marshal: %v", err)
			}
			var list []map[string]any
			if err := json.Unmarshal(rawJSON, &list); err != nil {
				t.Fatalf("failed to unmarshal: %v", err)
			}
			if len(list) != 1 {
				t.Fatalf("got %d users, want 1", len(list))
			}
			item := list[0]
			if got := item[tc.wantKey]; got != tc.wantValue {
				t.Fatalf("field %s got %#v, want %#v", tc.wantKey, got, tc.wantValue)
			}
		})
	}

	tuicRaw, _ := json.Marshal(buildInboundUsers("tuic", users))
	var tuicList []map[string]any
	_ = json.Unmarshal(tuicRaw, &tuicList)
	if len(tuicList) == 0 || tuicList[0]["uuid"] != users[0].VLESSUUID {
		t.Fatalf("tuic uuid got %#v, want %#v", tuicList[0]["uuid"], users[0].VLESSUUID)
	}
}

func TestParseDeployCoreState(t *testing.T) {
	tests := []struct {
		name        string
		message     string
		wantHas     bool
		wantReady   bool
		wantMessage string
	}{
		{
			name:        "ready core marks node connected",
			message:     "success: users=4 core_ready=true core_process_after=running",
			wantHas:     true,
			wantReady:   true,
			wantMessage: "",
		},
		{
			name:        "failed core reports reload error",
			message:     `success: users=4 core_ready=false reload_error="parse config: unknown outbound"`,
			wantHas:     true,
			wantReady:   false,
			wantMessage: "parse config: unknown outbound",
		},
		{
			name:        "missing core readiness does not imply connected",
			message:     "success: users=4 restarted=true",
			wantHas:     false,
			wantReady:   false,
			wantMessage: "",
		},
		{
			name:        "invalid core readiness is a failed core state",
			message:     "success: core_ready=maybe",
			wantHas:     true,
			wantReady:   false,
			wantMessage: "success: core_ready=maybe",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotHas, gotReady, gotMessage := parseDeployCoreState(tc.message)
			if gotHas != tc.wantHas {
				t.Fatalf("has got %v, want %v", gotHas, tc.wantHas)
			}
			if gotReady != tc.wantReady {
				t.Fatalf("ready got %v, want %v", gotReady, tc.wantReady)
			}
			if gotMessage != tc.wantMessage {
				t.Fatalf("message got %q, want %q", gotMessage, tc.wantMessage)
			}
		})
	}
}

func TestOptionalStatusMessage(t *testing.T) {
	tests := []struct {
		name      string
		message   string
		want      string
		wantDBNil bool
	}{
		{
			name:      "empty message becomes null",
			message:   "",
			want:      "",
			wantDBNil: true,
		},
		{
			name:      "whitespace message becomes null",
			message:   "  ",
			want:      "",
			wantDBNil: true,
		},
		{
			name:      "error message is preserved",
			message:   " Core error: parse config failed ",
			want:      "Core error: parse config failed",
			wantDBNil: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, gotDB := optionalStatusMessage(tc.message)
			if got != tc.want {
				t.Fatalf("message got %q, want %q", got, tc.want)
			}
			if (gotDB == nil) != tc.wantDBNil {
				t.Fatalf("db nil got %v, want %v", gotDB == nil, tc.wantDBNil)
			}
		})
	}
}

func TestHashedSet(t *testing.T) {
	hs := NewHashedSet()
	hs.Add("user1")
	hs.Add("user2")
	hs.Add("user1") // duplicate
	if hs.Size() != 2 {
		t.Fatalf("expected size 2, got %d", hs.Size())
	}
	hash := hs.Hash64String()
	if len(hash) != 16 {
		t.Fatalf("expected 16-char hex hash, got %q", hash)
	}
}

func TestDeleteField(t *testing.T) {
	m := map[string]any{
		"tag":   "vless-in",
		"users": []any{"alice", "bob"},
	}
	cleaned := deleteField(m, "users").(map[string]any)
	if _, ok := cleaned["users"]; ok {
		t.Fatalf("expected users to be deleted")
	}
	if cleaned["tag"] != "vless-in" {
		t.Fatalf("expected tag to be preserved, got %v", cleaned["tag"])
	}
}

func TestRenderNodeConfigFromPrepared(t *testing.T) {
	base := orderedmap.New()
	base.Set("log", map[string]any{"level": "info"})

	hash1 := deployInboundHash{Tag: "vless-in", Hash: "abcdef1234567890", UsersCount: 1}
	hash2 := deployInboundHash{Tag: "ss-in", Hash: "1234567890abcdef", UsersCount: 2}

	prep := &preparedProfileData{
		profileUUID: "profile-1",
		baseParsed:  base,
		inbounds: []preparedInbound{
			{
				tag:          "vless-in",
				normTag:      "vless-in",
				inboundType:  "vless",
				rawWithUsers: map[string]any{"tag": "vless-in", "type": "vless", "users": []any{"user1"}},
				rawEmpty:     map[string]any{"tag": "vless-in", "type": "vless"},
				hash:         &hash1,
				isUnsecure:   false,
			},
			{
				tag:          "ss-in",
				normTag:      "ss-in",
				inboundType:  "shadowsocks",
				rawWithUsers: map[string]any{"tag": "ss-in", "type": "shadowsocks", "users": []any{"user2", "user3"}},
				rawEmpty:     map[string]any{"tag": "ss-in", "type": "shadowsocks"},
				hash:         &hash2,
				isUnsecure:   false,
			},
		},
	}

	nm := &NodeMonitor{}

	// Node 1 only has vless-in
	cfgJSON, internals, profileUUID, inbCount, err := nm.renderNodeConfigFromPrepared("node-1", prep, map[string]struct{}{"vless-in": {}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if profileUUID != "profile-1" {
		t.Fatalf("expected profile-1, got %s", profileUUID)
	}
	if inbCount != 1 {
		t.Fatalf("expected 1 inbound, got %d", inbCount)
	}
	if len(internals.Hashes.Inbounds) != 1 || internals.Hashes.Inbounds[0].Tag != "vless-in" {
		t.Fatalf("expected 1 inbound hash for vless-in, got %v", internals.Hashes.Inbounds)
	}

	var parsed map[string]any
	if err := json.Unmarshal(cfgJSON, &parsed); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	inbounds := parsed["inbounds"].([]any)
	if len(inbounds) != 1 {
		t.Fatalf("expected 1 inbound in JSON, got %d", len(inbounds))
	}

	// Node 2 only has ss-in
	_, internals2, _, inbCount2, err := nm.renderNodeConfigFromPrepared("node-2", prep, map[string]struct{}{"ss-in": {}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inbCount2 != 1 {
		t.Fatalf("expected 1 inbound, got %d", inbCount2)
	}
	if len(internals2.Hashes.Inbounds) != 1 || internals2.Hashes.Inbounds[0].Tag != "ss-in" {
		t.Fatalf("expected 1 inbound hash for ss-in, got %v", internals2.Hashes.Inbounds)
	}
}

func TestRenderNodeConfigPreservesBlockOrder(t *testing.T) {
	base := orderedmap.New()
	base.Set("log", map[string]any{"level": "info"})
	base.Set("dns", map[string]any{"servers": []any{}})
	base.Set("inbounds", []any{})
	base.Set("outbounds", []any{})
	base.Set("route", map[string]any{"rules": []any{}})
	base.Set("experimental", map[string]any{"v2ray_api": map[string]any{}})

	prep := &preparedProfileData{
		profileUUID: "profile-order",
		baseParsed:  base,
		inbounds: []preparedInbound{
			{
				tag:          "trojan-in",
				normTag:      "trojan-in",
				inboundType:  "trojan",
				rawWithUsers: map[string]any{"tag": "trojan-in", "type": "trojan"},
				rawEmpty:     map[string]any{"tag": "trojan-in", "type": "trojan"},
			},
		},
	}

	nm := &NodeMonitor{}
	cfgJSON, _, _, _, err := nm.renderNodeConfigFromPrepared("node-order", prep, map[string]struct{}{"trojan-in": {}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resultMap := orderedmap.New()
	if err := json.Unmarshal(cfgJSON, resultMap); err != nil {
		t.Fatalf("invalid json: %v", err)
	}

	expectedKeys := []string{"log", "dns", "inbounds", "outbounds", "route", "experimental"}
	actualKeys := resultMap.Keys()
	if len(actualKeys) != len(expectedKeys) {
		t.Fatalf("expected %d keys, got %d: %v", len(expectedKeys), len(actualKeys), actualKeys)
	}
	for i, want := range expectedKeys {
		if actualKeys[i] != want {
			t.Fatalf("key at index %d: expected %q, got %q (full order: %v)", i, want, actualKeys[i], actualKeys)
		}
	}
}


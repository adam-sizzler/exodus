package server

import (
	"encoding/json"
	"testing"

	"github.com/iancoleman/orderedmap"
)

func TestPatchSingboxConfigUsers(t *testing.T) {
	initialJSON := `{
  "inbounds": [
    {
      "tag": "vless-in",
      "type": "vless",
      "listen": "0.0.0.0",
      "listen_port": 11129,
      "users": [
        {
          "name": "existing_user",
          "uuid": "11111111-1111-1111-1111-111111111111"
        }
      ]
    },
    {
      "tag": "trojan-in",
      "type": "trojan",
      "listen": "0.0.0.0",
      "listen_port": 11130,
      "users": []
    }
  ],
  "experimental": {
    "v2ray_api": {
      "listen": "127.0.0.1:8080",
      "stats": {
        "enabled": true,
        "users": [
          "existing_user"
        ]
      }
    }
  }
}`

	cfg := orderedmap.New()
	if err := json.Unmarshal([]byte(initialJSON), cfg); err != nil {
		t.Fatalf("failed to unmarshal initial json: %v", err)
	}

	// 1. Add new user to both inbounds
	newUser := SyncUserItem{
		Action:         "add",
		Identifier:     "user_100",
		Username:       "testuser",
		UUID:           "22222222-2222-2222-2222-222222222222",
		TrojanPassword: "secret_trojan",
	}

	changed := patchSingboxConfigUsers(cfg, []SyncUserItem{newUser})
	if !changed {
		t.Fatalf("expected config to be modified when adding user")
	}

	// Verify vless user was added
	inboundsRaw, _ := cfg.Get("inbounds")
	inbounds := inboundsRaw.([]any)
	vlessUsersRaw, _ := getField(inbounds[0], "users")
	vlessUsers := vlessUsersRaw.([]any)
	if len(vlessUsers) != 2 {
		t.Fatalf("expected 2 vless users, got %d", len(vlessUsers))
	}
	if getFieldString(vlessUsers[1], "name") != "user_100" || getFieldString(vlessUsers[1], "uuid") != newUser.UUID {
		t.Fatalf("vless user data mismatch: %+v", vlessUsers[1])
	}

	// Verify trojan user was added
	trojanUsersRaw, _ := getField(inbounds[1], "users")
	trojanUsers := trojanUsersRaw.([]any)
	if len(trojanUsers) != 1 {
		t.Fatalf("expected 1 trojan user, got %d", len(trojanUsers))
	}
	if getFieldString(trojanUsers[0], "name") != "user_100" || getFieldString(trojanUsers[0], "password") != "secret_trojan" {
		t.Fatalf("trojan user data mismatch: %+v", trojanUsers[0])
	}

	// Verify stats user was added
	expRaw, _ := cfg.Get("experimental")
	v2rayRaw, _ := getField(expRaw, "v2ray_api")
	statsRaw, _ := getField(v2rayRaw, "stats")
	statsUsersRaw, _ := getField(statsRaw, "users")
	statsUsers := statsUsersRaw.([]string)
	if len(statsUsers) != 2 || statsUsers[1] != "user_100" {
		t.Fatalf("expected stats users to have user_100, got: %v", statsUsers)
	}

	// 2. Update user credentials in-place
	updatedUser := SyncUserItem{
		Action:         "add",
		Identifier:     "user_100",
		Username:       "testuser",
		UUID:           "33333333-3333-3333-3333-333333333333",
		TrojanPassword: "new_trojan_password",
	}
	changed = patchSingboxConfigUsers(cfg, []SyncUserItem{updatedUser})
	if !changed {
		t.Fatalf("expected config to be modified on update")
	}

	inboundsRaw, _ = cfg.Get("inbounds")
	inbounds = inboundsRaw.([]any)
	vlessUsersRaw, _ = getField(inbounds[0], "users")
	vlessUsers = vlessUsersRaw.([]any)
	if len(vlessUsers) != 2 {
		t.Fatalf("expected still 2 vless users after update, got %d", len(vlessUsers))
	}
	if getFieldString(vlessUsers[1], "uuid") != "33333333-3333-3333-3333-333333333333" {
		t.Fatalf("expected updated uuid, got: %s", getFieldString(vlessUsers[1], "uuid"))
	}

	// 3. Delete user
	deleteUser := SyncUserItem{
		Action:     "delete",
		Identifier: "user_100",
		Username:   "testuser",
	}
	changed = patchSingboxConfigUsers(cfg, []SyncUserItem{deleteUser})
	if !changed {
		t.Fatalf("expected config to be modified on delete")
	}

	inboundsRaw, _ = cfg.Get("inbounds")
	inbounds = inboundsRaw.([]any)
	vlessUsersRaw, _ = getField(inbounds[0], "users")
	vlessUsers = vlessUsersRaw.([]any)
	if len(vlessUsers) != 1 {
		t.Fatalf("expected 1 vless user after delete, got %d", len(vlessUsers))
	}
	trojanUsersRaw, _ = getField(inbounds[1], "users")
	trojanUsers = trojanUsersRaw.([]any)
	if len(trojanUsers) != 0 {
		t.Fatalf("expected 0 trojan users after delete, got %d", len(trojanUsers))
	}

	// Verify stats user was removed
	expRaw, _ = cfg.Get("experimental")
	v2rayRaw, _ = getField(expRaw, "v2ray_api")
	statsRaw, _ = getField(v2rayRaw, "stats")
	statsUsersRaw, _ = getField(statsRaw, "users")
	statsUsers = statsUsersRaw.([]string)
	if len(statsUsers) != 1 || statsUsers[0] != "existing_user" {
		t.Fatalf("expected stats users to only have existing_user, got: %v", statsUsers)
	}
}

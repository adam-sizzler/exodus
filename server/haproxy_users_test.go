package server

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildHaproxyUsersContent(t *testing.T) {
	users := []HaproxyUserEntry{
		{
			Username:       "user1",
			VLESSUUID:      "4e2de032-0775-e465-cff8-8bf4f6338f59",
			TrojanPassword: "my-trojan-pass",
			AnytlsPassword: "my-anytls-pass",
			NaivePassword:  "my-naive-pass",
		},
		{
			Username:      "user2",
			NaivePassword: "pass:with:colons",
		},
		{
			Username: "   ", // Should be ignored
		},
	}

	content := buildHaproxyUsersContent(users)
	lines := strings.Split(strings.TrimRight(content, "\n"), "\n")

	expectedToken1 := base64.StdEncoding.EncodeToString([]byte("user1:my-naive-pass"))
	expectedToken2 := base64.StdEncoding.EncodeToString([]byte("user2:pass:with:colons"))

	expectedLines := []string{
		"user1,4e2de032-0775-e465-cff8-8bf4f6338f59",
		"user1," + normalizeTrojanHash("my-trojan-pass"),
		"user1," + normalizeAnytlsHash("my-anytls-pass"),
		"user1,basic:" + expectedToken1,
		"user2,basic:" + expectedToken2,
	}

	if len(lines) != len(expectedLines) {
		t.Fatalf("expected %d lines, got %d. Content:\n%s", len(expectedLines), len(lines), content)
	}

	for i, expected := range expectedLines {
		if lines[i] != expected {
			t.Errorf("line %d: expected %q, got %q", i, expected, lines[i])
		}
		if strings.HasPrefix(lines[i], "1,") || strings.HasPrefix(lines[i], "0,") {
			t.Errorf("line %d has legacy prefix: %q", i, lines[i])
		}
	}
}

func TestApplyHaproxyModule(t *testing.T) {
	tmpDir := t.TempDir()
	origPath := haproxyUsersFilePath
	haproxyUsersFilePath = filepath.Join(tmpDir, "users.csv")
	defer func() {
		haproxyUsersFilePath = origPath
	}()

	payload := DeployModulesPayload{
		HaproxyEnabled: true,
		HaproxyUsers: []HaproxyUserEntry{
			{
				Username:      "alice",
				VLESSUUID:     "11111111-2222-3333-4444-555555555555",
				NaivePassword: "secretalice",
			},
		},
	}

	// 1. Initial write
	changed, err := applyHaproxyModule(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatalf("expected changed=true on initial write")
	}

	data, err := os.ReadFile(haproxyUsersFilePath)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !strings.Contains(string(data), "alice,11111111-2222-3333-4444-555555555555") {
		t.Errorf("missing vless line in %s", string(data))
	}
	if !strings.Contains(string(data), "alice,basic:") {
		t.Errorf("missing naive line in %s", string(data))
	}

	// 2. Second write with same content -> should return changed=false
	changed, err = applyHaproxyModule(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if changed {
		t.Fatalf("expected changed=false when content is identical")
	}

	// 3. Disable haproxy -> should remove file
	payload.HaproxyEnabled = false
	changed, err = applyHaproxyModule(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatalf("expected changed=true when disabling and removing file")
	}
	if _, err := os.Stat(haproxyUsersFilePath); !os.IsNotExist(err) {
		t.Fatalf("expected file to be removed, stat err: %v", err)
	}
}

func TestComputeHaproxyCSVHash(t *testing.T) {
	content := []byte("alice,uuid-1\nalice,hash-trojan\nbob,hash-anytls\n")
	hash1, count1 := computeHaproxyCSVHash(content)
	if hash1 == "" || count1 != 3 {
		t.Fatalf("expected 3 items and non-empty hash, got %s, count=%d", hash1, count1)
	}

	// Reordered lines should yield exact same hash
	contentReversed := []byte("bob,hash-anytls\nalice,hash-trojan\nalice,uuid-1\n")
	hash2, count2 := computeHaproxyCSVHash(contentReversed)
	if hash1 != hash2 || count1 != count2 {
		t.Fatalf("expected order-independent hash: hash1=%s, hash2=%s", hash1, hash2)
	}

	// Empty content yields empty hash and 0 count
	emptyHash, emptyCount := computeHaproxyCSVHash([]byte(""))
	if emptyHash != "" || emptyCount != 0 {
		t.Fatalf("expected empty hash for empty content, got %s, count=%d", emptyHash, emptyCount)
	}
}

func TestApplyHaproxyModule_HashReconciliation(t *testing.T) {
	tmpDir := t.TempDir()
	origPath := haproxyUsersFilePath
	haproxyUsersFilePath = filepath.Join(tmpDir, "users.csv")
	defer func() {
		haproxyUsersFilePath = origPath
	}()

	payload := DeployModulesPayload{
		HaproxyEnabled: true,
		HaproxyUsers: []HaproxyUserEntry{
			{
				Username:       "2",
				TrojanPassword: "my-trojan-password",
				AnytlsPassword: "my-anytls-password",
			},
		},
	}

	// 1. Initial write without hashes
	changed, err := applyHaproxyModule(payload)
	if err != nil || !changed {
		t.Fatalf("expected initial apply to succeed and change: changed=%t, err=%v", changed, err)
	}

	data, err := os.ReadFile(haproxyUsersFilePath)
	if err != nil {
		t.Fatalf("read users.csv: %v", err)
	}
	hash, count := computeHaproxyCSVHash(data)

	// 2. Deploy with matching hash -> must return changed = false without rewriting
	hashes := &DeployHashesPayload{
		HaproxyUsersHash: hash,
		HaproxyCount:     count,
	}
	changed, err = applyHaproxyModule(payload, hashes)
	if err != nil {
		t.Fatalf("apply with matching hash: %v", err)
	}
	if changed {
		t.Fatalf("expected changed=false when hashes match")
	}

	// 3. Deploy with different hash -> must return changed = true
	hashesDiff := &DeployHashesPayload{
		HaproxyUsersHash: "ffffffffffffffff",
		HaproxyCount:     count,
	}
	// Payload with updated user
	payload.HaproxyUsers[0].TrojanPassword = "new-password"
	changed, err = applyHaproxyModule(payload, hashesDiff)
	if err != nil {
		t.Fatalf("apply with different hash: %v", err)
	}
	if !changed {
		t.Fatalf("expected changed=true when hashes differ and content changed")
	}
}

func TestPatchHaproxyUsersCSV_Granular(t *testing.T) {
	tmpDir := t.TempDir()
	origPath := haproxyUsersFilePath
	haproxyUsersFilePath = filepath.Join(tmpDir, "users.csv")
	defer func() {
		haproxyUsersFilePath = origPath
	}()

	inboundMap := map[string]string{
		"trojan-stable": "trojan",
		"anytls-in":     "anytls",
		"vless-grpc":    "vless",
	}
	haproxyInboundTags := []string{"trojan-stable", "anytls-in"}

	// 1. Add user "alice" with trojan-stable
	addAlice := []SyncUserItem{
		{
			Action:         "add",
			Identifier:     "alice",
			Username:       "alice",
			TrojanPassword: "trojan-pass-alice",
			InboundTags:    []string{"trojan-stable"},
		},
	}
	changed, err := patchHaproxyUsersCSV(addAlice, inboundMap, true, haproxyInboundTags)
	if err != nil || !changed {
		t.Fatalf("add alice trojan failed: changed=%t, err=%v", changed, err)
	}

	data, _ := os.ReadFile(haproxyUsersFilePath)
	expectedTrojanLine := "alice," + normalizeTrojanHash("trojan-pass-alice")
	if !strings.Contains(string(data), expectedTrojanLine) {
		t.Fatalf("missing trojan line in:\n%s", string(data))
	}

	// 2. Add user "alice" with anytls-in (granular add) -> should keep trojan line and add anytls line
	addAliceAnytls := []SyncUserItem{
		{
			Action:         "add",
			Identifier:     "alice",
			Username:       "alice",
			AnytlsPassword: "anytls-pass-alice",
			InboundTags:    []string{"anytls-in"},
		},
	}
	changed, err = patchHaproxyUsersCSV(addAliceAnytls, inboundMap, true, haproxyInboundTags)
	if err != nil || !changed {
		t.Fatalf("add alice anytls failed: changed=%t, err=%v", changed, err)
	}

	data, _ = os.ReadFile(haproxyUsersFilePath)
	expectedAnytlsLine := "alice," + normalizeAnytlsHash("anytls-pass-alice")
	if !strings.Contains(string(data), expectedTrojanLine) {
		t.Fatalf("trojan line was lost after adding anytls:\n%s", string(data))
	}
	if !strings.Contains(string(data), expectedAnytlsLine) {
		t.Fatalf("missing anytls line in:\n%s", string(data))
	}

	// 3. Repeat add with same credentials -> should be no-op (changed = false)
	changed, err = patchHaproxyUsersCSV(addAliceAnytls, inboundMap, true, haproxyInboundTags)
	if err != nil {
		t.Fatalf("duplicate add failed: %v", err)
	}
	if changed {
		t.Fatalf("expected changed=false on duplicate add")
	}

	// 4. Granular delete: delete alice ONLY from trojan-stable
	delAliceTrojan := []SyncUserItem{
		{
			Action:         "delete",
			Identifier:     "alice",
			Username:       "alice",
			TrojanPassword: "trojan-pass-alice",
			InboundTags:    []string{"trojan-stable"},
		},
	}
	changed, err = patchHaproxyUsersCSV(delAliceTrojan, inboundMap, true, haproxyInboundTags)
	if err != nil || !changed {
		t.Fatalf("granular delete trojan failed: changed=%t, err=%v", changed, err)
	}

	data, _ = os.ReadFile(haproxyUsersFilePath)
	if strings.Contains(string(data), expectedTrojanLine) {
		t.Fatalf("trojan line was NOT removed during granular delete:\n%s", string(data))
	}
	if !strings.Contains(string(data), expectedAnytlsLine) {
		t.Fatalf("anytls line was wrongly removed during trojan granular delete:\n%s", string(data))
	}

	// 5. Full delete: delete alice completely (no InboundTags)
	delAliceFull := []SyncUserItem{
		{
			Action:     "delete",
			Identifier: "alice",
			Username:   "alice",
		},
	}
	changed, err = patchHaproxyUsersCSV(delAliceFull, inboundMap, true, haproxyInboundTags)
	if err != nil || !changed {
		t.Fatalf("full delete failed: changed=%t, err=%v", changed, err)
	}

	data, _ = os.ReadFile(haproxyUsersFilePath)
	if strings.Contains(string(data), "alice,") {
		t.Fatalf("user alice still has lines in users.csv:\n%s", string(data))
	}
}


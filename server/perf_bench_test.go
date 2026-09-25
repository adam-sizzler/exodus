package server

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"exodus-node/config"

	"github.com/Adam-Sizzler/lmdb-go/lmdb"
	"github.com/vmihailenco/msgpack/v5"
)

// Sample config payload simulating 500 users
func generateSampleSingboxConfig(usersCount int) []byte {
	type User struct {
		Name     string `json:"name"`
		Password string `json:"password"`
		UUID     string `json:"uuid"`
	}
	type Inbound struct {
		Tag        string `json:"tag"`
		Type       string `json:"type"`
		Listen     string `json:"listen"`
		ListenPort int    `json:"listen_port"`
		Users      []User `json:"users"`
	}
	users := make([]User, usersCount)
	for i := 0; i < usersCount; i++ {
		users[i] = User{
			Name:     fmt.Sprintf("user_%04d", i),
			Password: fmt.Sprintf("pass_%04d_abcdef", i),
			UUID:     fmt.Sprintf("b8c160ee-89b5-4147-9755-a0808efb%04d", i),
		}
	}
	cfg := map[string]any{
		"log": map[string]any{
			"level":     "debug",
			"timestamp": true,
		},
		"inbounds": []Inbound{
			{
				Tag:        "vless-in",
				Type:       "vless",
				Listen:     "0.0.0.0",
				ListenPort: 443,
				Users:      users,
			},
			{
				Tag:        "trojan-in",
				Type:       "trojan",
				Listen:     "0.0.0.0",
				ListenPort: 8443,
				Users:      users,
			},
		},
		"outbounds": []map[string]any{
			{"tag": "direct", "type": "direct"},
			{"tag": "block", "type": "block"},
		},
		"route": map[string]any{
			"rules": []map[string]any{
				{"action": "sniff"},
			},
		},
	}
	data, _ := json.Marshal(cfg)
	return data
}

func BenchmarkBuildSingboxConfig(b *testing.B) {
	raw := generateSampleSingboxConfig(500)
	opts := BuildOptions{
		Listen:  "127.0.0.1:10085",
		Enabled: true,
	}
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, err := BuildSingboxConfigWithV2RayAPI(raw, opts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkComputeEmptyConfigHash(b *testing.B) {
	raw := generateSampleSingboxConfig(500)
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = computeEmptyConfigHash(raw)
	}
}

func BenchmarkBuildHaproxyUsersContent(b *testing.B) {
	users := make([]HaproxyUserEntry, 500)
	for i := 0; i < 500; i++ {
		users[i] = HaproxyUserEntry{
			Username:       fmt.Sprintf("user_%04d", i),
			VLESSUUID:      fmt.Sprintf("b8c160ee-89b5-4147-9755-a0808efb%04d", i),
			TrojanPassword: fmt.Sprintf("trojan_pass_%04d", i),
			AnytlsPassword: fmt.Sprintf("anytls_pass_%04d", i),
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = buildHaproxyUsersContent(users)
	}
}

func BenchmarkComputeHaproxyCSVHash(b *testing.B) {
	users := make([]HaproxyUserEntry, 500)
	for i := 0; i < 500; i++ {
		users[i] = HaproxyUserEntry{
			Username:       fmt.Sprintf("user_%04d", i),
			VLESSUUID:      fmt.Sprintf("b8c160ee-89b5-4147-9755-a0808efb%04d", i),
			TrojanPassword: fmt.Sprintf("trojan_pass_%04d", i),
		}
	}
	content := []byte(buildHaproxyUsersContent(users))

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = computeHaproxyCSVHash(content)
	}
}

func BenchmarkPatchHaproxyUsersCSV(b *testing.B) {
	tmpDir := b.TempDir()
	origPath := haproxyUsersFilePath
	haproxyUsersFilePath = filepath.Join(tmpDir, "users.csv")
	defer func() {
		haproxyUsersFilePath = origPath
	}()

	inboundMap := map[string]string{
		"trojan-stable": "trojan",
		"anytls-in":     "anytls",
	}
	haproxyInboundTags := []string{"trojan-stable", "anytls-in"}

	// Seed 200 users
	seedUsers := make([]SyncUserItem, 200)
	for i := 0; i < 200; i++ {
		seedUsers[i] = SyncUserItem{
			Action:         "add",
			Identifier:     fmt.Sprintf("user_%04d", i),
			Username:       fmt.Sprintf("user_%04d", i),
			TrojanPassword: fmt.Sprintf("pass_%04d", i),
			InboundTags:    []string{"trojan-stable"},
		}
	}
	_, _ = patchHaproxyUsersCSV(seedUsers, inboundMap, true, haproxyInboundTags)

	updateItem := []SyncUserItem{
		{
			Action:         "add",
			Identifier:     "user_0050",
			Username:       "user_0050",
			AnytlsPassword: "new_anytls_pass",
			InboundTags:    []string{"anytls-in"},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = patchHaproxyUsersCSV(updateItem, inboundMap, true, haproxyInboundTags)
	}
}

func BenchmarkNormalizeNftIPs(b *testing.B) {
	rawIPs := make([]string, 200)
	for i := 0; i < 200; i++ {
		if i%2 == 0 {
			rawIPs[i] = fmt.Sprintf("192.168.%d.0/24", i)
		} else {
			rawIPs[i] = fmt.Sprintf("10.0.%d.%d", i, i)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, err := normalizeNftIPs(rawIPs)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTailSingboxLogLines_ExecTail(b *testing.B) {
	tmpDir := b.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	f, err := os.Create(logPath)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		fmt.Fprintf(f, "@4000000062a1b2c312345678 INFO[0001] log line number %d with some text\n", i)
	}
	f.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = TailSingboxLogLines(logPath, 10)
	}
}

func BenchmarkLoggerFormatting(b *testing.B) {
	logger := config.NewExodusLogger(io.Discard, "info")
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		logger.Info("Benchmark log message", "key1", "val1", "key2", 12345, "user", "test_user")
	}
}

func BenchmarkResolveASNs(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "asn-prefixes.lmdb")

	env, err := lmdb.NewEnv()
	if err != nil {
		b.Fatal(err)
	}
	defer env.Close()

	if err := env.SetMapSize(10 * 1024 * 1024); err != nil {
		b.Fatal(err)
	}
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		b.Fatal(err)
	}
	if err := env.Open(dbPath, 0, 0644); err != nil {
		b.Fatal(err)
	}

	asns := []int{13335, 15169, 20940, 8075, 16509, 2906, 46489, 54113, 396982, 14061}
	_ = env.Update(func(txn *lmdb.Txn) error {
		dbi, _ := txn.OpenRoot(0)
		for _, asn := range asns {
			cfData, _ := msgpack.Marshal(AsnPrefixes{
				IPv4: []string{fmt.Sprintf("192.0.%d.0/24", asn%250), fmt.Sprintf("198.51.%d.0/24", asn%250)},
				IPv6: []string{fmt.Sprintf("2001:db8:%x::/48", asn)},
			})
			cfKey := encodeOrderedBinaryNumberKey(uint32(asn))
			_ = txn.Put(dbi, cfKey, cfData, 0)
		}
		return nil
	})
	env.Close()

	b.Setenv("ASN_LMDB_PATH", dbPath)
	service := NewAsnLmdbService(nil)
	defer service.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = service.ResolveASNs(asns)
	}
}

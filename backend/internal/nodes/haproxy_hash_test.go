package users

import (
	"testing"
)

func TestComputeHaproxyUsersHash(t *testing.T) {
	users := []deployHaproxyUserItem{
		{
			Username:       "2",
			TrojanPassword: "test-trojan-password",
			AnytlsPassword: "test-anytls-password",
		},
		{
			Username:  "3",
			VLESSUUID: "4e2de032-0775-e465-cff8-8bf4f6338f59",
		},
	}

	hash1, count1 := computeHaproxyUsersHash(users)
	if hash1 == "" || count1 != 3 {
		t.Fatalf("expected hash non-empty and count=3, got hash=%s, count=%d", hash1, count1)
	}

	// Reorder users - hash must be order-independent
	reversedUsers := []deployHaproxyUserItem{users[1], users[0]}
	hash2, count2 := computeHaproxyUsersHash(reversedUsers)
	if hash1 != hash2 || count1 != count2 {
		t.Fatalf("expected deterministic order-independent hash: hash1=%s, hash2=%s", hash1, hash2)
	}
}

func BenchmarkComputeHaproxyUsersHash(b *testing.B) {
	users := make([]deployHaproxyUserItem, 1000)
	for i := 0; i < 1000; i++ {
		users[i] = deployHaproxyUserItem{
			Username:       FormatUserID(int64(i + 1)),
			VLESSUUID:      "4e2de032-0775-e465-cff8-8bf4f6338f59",
			TrojanPassword: "test-trojan-password-12345",
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = computeHaproxyUsersHash(users)
	}
}


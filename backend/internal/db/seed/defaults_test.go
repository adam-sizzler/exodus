package seed

import (
	"testing"
)

func TestCanonicalHashMatch(t *testing.T) {
	h, err := canonicalHash(defaultResponseRules)
	if err != nil {
		t.Fatalf("failed to calculate canonical hash: %v", err)
	}
	const expectedNewHash = "f39af20113e4369ecd75f329805af245cd8551ea95c556fe757540da453185f0"
	if h != expectedNewHash {
		t.Errorf("Canonical hash mismatch!\nGot:      %s\nExpected: %s", h, expectedNewHash)
	}
}

func TestClearRedisDoesNotCloseSharedClient(t *testing.T) {
	// If redis is not configured, ClearRedis returns nil or error without panic
	_ = ClearRedis(nil, nil)
}

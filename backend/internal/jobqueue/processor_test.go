package jobqueue

import (
	"testing"

	"exodus/internal/config"
)

func TestGetSharedRedisClientNilWhenUnconfigured(t *testing.T) {
	cfg := &config.BackendConfig{}
	client, err := GetSharedRedisClient(cfg)
	if err != nil {
		t.Fatalf("expected nil error for unconfigured redis, got %v", err)
	}
	if client != nil {
		t.Fatalf("expected nil client for unconfigured redis, got %v", client)
	}
}

func TestCloseSharedRedisClientIdempotent(t *testing.T) {
	if err := CloseSharedRedisClient(); err != nil {
		t.Fatalf("expected nil error closing uninitialized shared client, got %v", err)
	}
}

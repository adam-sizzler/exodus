package nodehotcache

import (
	"context"
	"encoding/json"
	"testing"
)

func TestSetNodeRuntimeState_NilSafe(t *testing.T) {
	var c *Cache
	ctx := context.Background()

	// Should not panic on nil receiver
	err := c.SetNodeRuntimeState(ctx, "test-uuid", NodeRuntimeUpdate{
		SystemInfo:       json.RawMessage(`{"cpus":4}`),
		SystemStats:      json.RawMessage(`{"cpu":10}`),
		SingboxVersion:   "1.13.3",
		NodeVersion:      "26.9.9",
		HasSingboxUptime: true,
		SingboxUptime:    1234,
		UsersOnline:      5,
	})
	if err != nil {
		t.Fatalf("expected nil error on nil receiver, got: %v", err)
	}

	// Should not panic on empty uuid
	c = &Cache{}
	err = c.SetNodeRuntimeState(ctx, "", NodeRuntimeUpdate{})
	if err != nil {
		t.Fatalf("expected nil error on empty uuid, got: %v", err)
	}

	err = c.SetNodeRuntimeState(ctx, "   ", NodeRuntimeUpdate{})
	if err != nil {
		t.Fatalf("expected nil error on whitespace uuid, got: %v", err)
	}
}

func TestNodeRuntimeUpdate_Structure(t *testing.T) {
	update := NodeRuntimeUpdate{
		SystemInfo:       json.RawMessage(`{"os":"linux"}`),
		SystemStats:      json.RawMessage(`{"ram":1024}`),
		SingboxVersion:   "1.13.0",
		NodeVersion:      "26.9.1",
		HasSingboxUptime: true,
		SingboxUptime:    5000,
		UsersOnline:      10,
	}

	if string(update.SystemInfo) != `{"os":"linux"}` {
		t.Errorf("unexpected system info: %s", string(update.SystemInfo))
	}
	if string(update.SystemStats) != `{"ram":1024}` {
		t.Errorf("unexpected system stats: %s", string(update.SystemStats))
	}
	if update.SingboxVersion != "1.13.0" || update.NodeVersion != "26.9.1" {
		t.Errorf("unexpected versions")
	}
	if !update.HasSingboxUptime || update.SingboxUptime != 5000 {
		t.Errorf("unexpected uptime")
	}
	if update.UsersOnline != 10 {
		t.Errorf("unexpected users online: %d", update.UsersOnline)
	}
}

func TestDeleteTransient_NilSafe(t *testing.T) {
	var c *Cache
	ctx := context.Background()

	err := c.DeleteTransient(ctx, "test-uuid")
	if err != nil {
		t.Fatalf("expected nil error on nil receiver, got: %v", err)
	}

	c = &Cache{}
	err = c.DeleteTransient(ctx, "")
	if err != nil {
		t.Fatalf("expected nil error on empty uuid, got: %v", err)
	}
}

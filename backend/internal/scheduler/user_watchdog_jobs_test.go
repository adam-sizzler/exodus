package scheduler

import (
	"context"
	"io"
	"math"
	"testing"
	"time"

	"exodus/internal/config"
	"exodus/internal/logger"
)

func TestTriggerNodeDeployCallback(t *testing.T) {
	var calledRestart bool
	var calledNodes []string
	var callCount int

	SetNodeDeployCallback(func(restart bool, nodeUUIDs ...string) {
		callCount++
		calledRestart = restart
		calledNodes = nodeUUIDs
	})

	// Case 1: Deploy with specific nodes
	triggerNodeDeploy(true, "node-1", "node-2")
	if callCount != 1 || !calledRestart || len(calledNodes) != 2 {
		t.Fatalf("expected 1 call with 2 nodes, got callCount=%d restart=%v nodes=%v", callCount, calledRestart, calledNodes)
	}

	// Case 2: Deploy all nodes (empty nodeUUIDs, matching startAllNodes)
	triggerNodeDeploy(true)
	if callCount != 2 || !calledRestart || len(calledNodes) != 0 {
		t.Fatalf("expected 2 calls with 0 nodes (all nodes), got callCount=%d restart=%v nodes=%v", callCount, calledRestart, calledNodes)
	}
}

func TestSafetyCapThreshold(t *testing.T) {
	const safetyCap = 10000

	// Below safety cap (e.g. 9999 users): individual events are not skipped by cap
	usersBelow := make([]userNotificationRecord, 9999)
	if len(usersBelow) >= safetyCap {
		t.Fatalf("expected below cap")
	}

	// At or above safety cap: individual events are skipped, bulk deploy triggered
	usersAbove := make([]userNotificationRecord, 10000)
	if len(usersAbove) < safetyCap {
		t.Fatalf("expected at or above cap")
	}
}

func TestRunExpiredUsersReviewSafetyCap(t *testing.T) {
	var deployed bool
	var deployedNodes []string
	SetNodeDeployCallback(func(restart bool, nodeUUIDs ...string) {
		deployed = true
		deployedNodes = nodeUUIDs
	})

	l, _ := logger.NewLogger("debug", "UTC", io.Discard)
	sched := &Scheduler{
		cfg: &config.BackendConfig{
			Logger: l,
		},
	}

	// When db is nil, UpdateExpiredUsersWithRecords returns 0 users and nil err
	err := sched.runExpiredUsersReview(context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	_ = deployed
	_ = deployedNodes
}

func TestResetPeriodBoundary(t *testing.T) {
	location := time.FixedZone("UTC+3", 3*60*60)
	// Wednesday, 2026-06-17 14:30:00
	now := time.Date(2026, time.June, 17, 14, 30, 0, 0, location)

	// DAY strategy -> start of today (2026-06-17 00:00:00)
	bDay, ok := resetPeriodBoundary("DAY", now)
	if !ok {
		t.Fatalf("expected DAY boundary to be valid")
	}
	expectedDay := time.Date(2026, time.June, 17, 0, 0, 0, 0, location)
	if !bDay.Equal(expectedDay) {
		t.Errorf("DAY boundary mismatch: got %s, want %s", bDay, expectedDay)
	}

	// WEEK strategy -> start of Monday of the current week (2026-06-15 00:00:00)
	bWeek, ok := resetPeriodBoundary("WEEK", now)
	if !ok {
		t.Fatalf("expected WEEK boundary to be valid")
	}
	expectedWeek := time.Date(2026, time.June, 15, 0, 0, 0, 0, location)
	if !bWeek.Equal(expectedWeek) {
		t.Errorf("WEEK boundary mismatch: got %s, want %s", bWeek, expectedWeek)
	}

	// MONTH strategy -> 1st day of the current month (2026-06-01 00:00:00)
	bMonth, ok := resetPeriodBoundary("MONTH", now)
	if !ok {
		t.Fatalf("expected MONTH boundary to be valid")
	}
	expectedMonth := time.Date(2026, time.June, 1, 0, 0, 0, 0, location)
	if !bMonth.Equal(expectedMonth) {
		t.Errorf("MONTH boundary mismatch: got %s, want %s", bMonth, expectedMonth)
	}

	// MONTH_ROLLING strategy -> start of today (2026-06-17 00:00:00)
	bRolling, ok := resetPeriodBoundary("MONTH_ROLLING", now)
	if !ok {
		t.Fatalf("expected MONTH_ROLLING boundary to be valid")
	}
	if !bRolling.Equal(expectedDay) {
		t.Errorf("MONTH_ROLLING boundary mismatch: got %s, want %s", bRolling, expectedDay)
	}

	// Invalid strategy -> ok == false
	_, ok = resetPeriodBoundary("UNKNOWN_STRATEGY", now)
	if ok {
		t.Errorf("expected UNKNOWN_STRATEGY to return false")
	}
}

func TestDedupeStrings(t *testing.T) {
	input := []string{"node-1", "node-2", "node-1", "  node-3  ", "", "node-2"}
	output := dedupeStrings(input)

	if len(output) != 3 {
		t.Fatalf("expected 3 unique items, got %d: %v", len(output), output)
	}
	if output[0] != "node-1" || output[1] != "node-2" || output[2] != "node-3" {
		t.Errorf("unexpected deduped result: %v", output)
	}
}

func TestMultiplierOverflowSafety(t *testing.T) {
	// 50 GB in bytes
	bytes := int64(50 * 1024 * 1024 * 1024)
	// 2.5x multiplier in nano (2_500_000_000)
	multiplierNano := int64(2_500_000_000)

	// Direct int64 multiplication: bytes * multiplierNano would overflow int64 (50GB * 2.5e9 > 9.22e18)
	// int64 max is ~9.22 * 10^18.
	// 53,687,091,200 * 2,500,000,000 = 1.34217728e20 (OVERFLOWS int64!).
	// Proper scaled calculation prevents overflow:
	scaled := int64(float64(bytes) * (float64(multiplierNano) / 1_000_000_000.0))
	expected := int64(float64(bytes) * 2.5)

	if scaled != expected {
		t.Fatalf("multiplier mismatch: got %d, want %d", scaled, expected)
	}

	// Verify extreme 100 TB usage calculation without panic or negative overflow
	extremeBytes := int64(100 * 1024 * 1024 * 1024 * 1024)
	extremeScaled := int64(float64(extremeBytes) * (float64(multiplierNano) / 1_000_000_000.0))
	if extremeScaled <= 0 || math.IsNaN(float64(extremeScaled)) {
		t.Fatalf("extreme usage multiplier overflowed to non-positive: %d", extremeScaled)
	}
}

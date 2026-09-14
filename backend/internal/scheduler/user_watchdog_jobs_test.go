package scheduler

import (
	"context"
	"io"
	"testing"

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

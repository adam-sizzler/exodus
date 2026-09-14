package subscriptionnodes

import (
	"context"
	"testing"
	"time"

	"exodus/internal/proto"
)

func TestNormalizeSubNodeRuntimeVersion(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
		ok       bool
	}{
		{name: "plain semver", input: "26.1.13", expected: "26.1.13", ok: true},
		{name: "prefixed semver", input: "v26.1.13", expected: "v26.1.13", ok: true},
		{name: "prefixed semver uppercase", input: "V26.1.13", expected: "v26.1.13", ok: true},
		{name: "empty", input: "", expected: "", ok: false},
		{name: "unknown", input: "unknown", expected: "", ok: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			value, ok := normalizeSubNodeRuntimeVersion(testCase.input)
			if ok != testCase.ok {
				t.Fatalf("unexpected ok: got %v want %v", ok, testCase.ok)
			}
			if value != testCase.expected {
				t.Fatalf("unexpected value: got %q want %q", value, testCase.expected)
			}
		})
	}
}

func TestUpdateRuntimeFromStatsStoresSnapshot(t *testing.T) {
	monitor := &SubNodeMonitor{
		runtimeByNodeName: make(map[string]SubNodeRuntimeSnapshot),
	}

	monitor.updateRuntimeFromStats("sub-node-1", []*proto.Stat{
		{Name: "sub_node_version", Value: "v26.1.13"},
		{Name: "sub_node_uptime", Value: "123"},
		{Name: "cpu_count", Value: "8"},
		{Name: "cpu_model", Value: "AMD EPYC"},
		{Name: "total_ram", Value: "31.25 GiB"},
	})

	snapshot, ok := monitor.RuntimeSnapshot("sub-node-1")
	if !ok {
		t.Fatal("expected runtime snapshot to be present")
	}

	if snapshot.NodeVersion == nil || *snapshot.NodeVersion != "v26.1.13" {
		t.Fatalf("unexpected node version: %#v", snapshot.NodeVersion)
	}
	if snapshot.SingboxUptime != "123" {
		t.Fatalf("unexpected uptime: %q", snapshot.SingboxUptime)
	}
	if snapshot.CPUCount == nil || *snapshot.CPUCount != 8 {
		t.Fatalf("unexpected cpu count: %#v", snapshot.CPUCount)
	}
	if snapshot.CPUModel == nil || *snapshot.CPUModel != "AMD EPYC" {
		t.Fatalf("unexpected cpu model: %#v", snapshot.CPUModel)
	}
	if snapshot.TotalRAM == nil || *snapshot.TotalRAM != "31.25 GiB" {
		t.Fatalf("unexpected total ram: %#v", snapshot.TotalRAM)
	}
}

func TestWatchStreamHeartbeatCancelsStaleStream(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	canceled := make(chan struct{}, 1)
	monitor := &SubNodeMonitor{}
	state := &subNodeState{
		nodeName:         "AEZA",
		ctx:              ctx,
		streamCancel:     func() { canceled <- struct{}{} },
		streamGeneration: 1,
		lastResponseAt:   time.Now().Add(-time.Minute),
		isConnected:      true,
	}

	done := make(chan struct{})
	go func() {
		monitor.watchStreamHeartbeat(state, 1, 20*time.Millisecond, 5*time.Millisecond)
		close(done)
	}()

	select {
	case <-canceled:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected stale stream to be canceled")
	}

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("watchdog did not exit after canceling stale stream")
	}
}

func TestWatchStreamHeartbeatIgnoresOldGeneration(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	canceled := make(chan struct{}, 1)
	monitor := &SubNodeMonitor{}
	state := &subNodeState{
		nodeName:         "AEZA",
		ctx:              ctx,
		streamCancel:     func() { canceled <- struct{}{} },
		streamGeneration: 2,
		lastResponseAt:   time.Now().Add(-time.Minute),
		isConnected:      true,
	}

	done := make(chan struct{})
	go func() {
		monitor.watchStreamHeartbeat(state, 1, 20*time.Millisecond, 5*time.Millisecond)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected watchdog for old generation to exit")
	}

	select {
	case <-canceled:
		t.Fatal("old generation watchdog must not cancel current stream")
	default:
	}
}

func TestMergeSubNodeTargets(t *testing.T) {
	// Discrete sets merged
	a := []string{"uuid-1", "uuid-2"}
	b := []string{"uuid-2", "uuid-3"}
	merged := mergeSubNodeTargets(a, b)
	if len(merged) != 3 {
		t.Fatalf("expected 3 unique targets, got %d", len(merged))
	}

	// Either empty/nil => all nodes (nil)
	if res := mergeSubNodeTargets(nil, b); res != nil {
		t.Fatalf("expected nil when a is nil, got %v", res)
	}
	if res := mergeSubNodeTargets(a, nil); res != nil {
		t.Fatalf("expected nil when b is nil, got %v", res)
	}
}

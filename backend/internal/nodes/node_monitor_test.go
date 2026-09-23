package users

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"exodus/internal/config"
	"exodus/internal/logger"
	"exodus/internal/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
)

func TestExtractTrafficStatsDelta(t *testing.T) {
	stats := []*proto.Stat{
		{Name: "outbound>>>direct>>>traffic>>>uplink", Value: "10"},
		{Name: "outbound>>>direct>>>traffic>>>downlink", Value: "20"},
		{Name: "user>>>alice>>>traffic>>>uplink", Value: "3"},
		{Name: "user>>>alice>>>traffic>>>downlink", Value: "7"},
		{Name: "user>>>13>>>traffic>>>uplink", Value: "15"},
		{Name: "user>>>13>>>traffic>>>downlink", Value: "25"},
		{Name: "singbox_version", Value: "1.13.3"},
		{Name: "outbound>>>warp>>>traffic>>>uplink", Value: "bad"},
	}

	delta := extractTrafficStatsDelta(stats)

	if delta.TotalUploadBytes != 10 {
		t.Fatalf("unexpected total upload: got %d want %d", delta.TotalUploadBytes, 10)
	}
	if delta.TotalDownloadBytes != 20 {
		t.Fatalf("unexpected total download: got %d want %d", delta.TotalDownloadBytes, 20)
	}

	if got := delta.UserBytesByName["alice"]; got != 10 {
		t.Fatalf("unexpected alice bytes: got %d want %d", got, 10)
	}
	if got := delta.UserBytesByName["13"]; got != 40 {
		t.Fatalf("unexpected user 13 bytes: got %d want %d", got, 40)
	}
	if _, ok := delta.UserBytesByName["bob"]; ok {
		t.Fatalf("bob should not have traffic bytes")
	}

	// Online users are derived only from non-zero traffic during the interval.
	if delta.UsersOnline != 2 {
		t.Fatalf("unexpected users_online: got %d want %d", delta.UsersOnline, 2)
	}
}

func TestApplyConsumptionMultiplier(t *testing.T) {
	if got := applyConsumptionMultiplier(100, 1_000_000_000); got != 100 {
		t.Fatalf("unexpected 1.0 multiplier result: got %d want %d", got, 100)
	}
	if got := applyConsumptionMultiplier(101, 500_000_000); got != 50 {
		t.Fatalf("unexpected 0.5 multiplier result: got %d want %d", got, 50)
	}
	if got := applyConsumptionMultiplier(100, 0); got != 0 {
		t.Fatalf("unexpected 0 multiplier result: got %d want %d", got, 0)
	}
	// Test large traffic (> 9.22 GB) that previously overflowed int64 when multiplied by 10^9
	const fiftyGB = int64(50 * 1024 * 1024 * 1024)
	if got := applyConsumptionMultiplier(fiftyGB, 1_000_000_000); got != fiftyGB {
		t.Fatalf("unexpected 50GB multiplier result: got %d want %d", got, fiftyGB)
	}
	const oneTB = int64(1024 * 1024 * 1024 * 1024)
	if got := applyConsumptionMultiplier(oneTB, 2_000_000_000); got != oneTB*2 {
		t.Fatalf("unexpected 1TB 2.0x multiplier result: got %d want %d", got, oneTB*2)
	}
}

func TestNormalizeNodeConnectionFields(t *testing.T) {
	if got := normalizeNodeSchema("tls"); got != "tls" {
		t.Fatalf("normalizeNodeSchema(tls) = %q", got)
	}
	if got := normalizeNodeSchema("grpcs"); got != "mtls" {
		t.Fatalf("normalizeNodeSchema(grpcs) = %q", got)
	}
	if got := normalizeNodePath("node"); got != "/node" {
		t.Fatalf("normalizeNodePath(node) = %q", got)
	}
	if got := normalizeNodePath("/"); got != "" {
		t.Fatalf("normalizeNodePath(/) = %q", got)
	}
}

func TestWatchStreamHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	state := &nodeState{
		nodeName:         "test-node",
		ctx:              ctx,
		cancel:           cancel,
		streamGeneration: 1,
		isConnected:      true,
		lastResponseAt:   time.Now().Add(-500 * time.Millisecond),
	}

	nm := &NodeMonitor{}

	// Test markStreamActivity
	nm.markStreamActivity(state)
	state.mutex.RLock()
	recentResponse := state.lastResponseAt
	state.mutex.RUnlock()
	if time.Since(recentResponse) > 100*time.Millisecond {
		t.Fatalf("expected recent lastResponseAt, got %v", recentResponse)
	}

	// Now set lastResponseAt to past and verify watchdog disconnects
	state.mutex.Lock()
	state.lastResponseAt = time.Now().Add(-100 * time.Millisecond)
	state.mutex.Unlock()

	watchDone := make(chan struct{})
	go func() {
		nm.watchStreamHeartbeat(state, 1, 50*time.Millisecond, 10*time.Millisecond)
		close(watchDone)
	}()

	select {
	case <-watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("watchStreamHeartbeat timed out without triggering disconnect")
	}

	state.mutex.RLock()
	defer state.mutex.RUnlock()
	if state.isConnected {
		t.Fatalf("expected isConnected = false after watchdog timeout")
	}
	if state.lastError != "Stream idle timeout" {
		t.Fatalf("expected lastError = 'Stream idle timeout', got %q", state.lastError)
	}
}

func TestMergeDeployRequests(t *testing.T) {
	// Case 1: Both have discrete targets
	reqA := deployRequest{Restart: true, ForceRestart: false, NodeUUIDs: []string{"node-1", "node-2"}}
	reqB := deployRequest{Restart: false, ForceRestart: true, NodeUUIDs: []string{"node-2", "node-3"}}
	merged := mergeDeployRequests(reqA, reqB)
	if !merged.Restart || !merged.ForceRestart {
		t.Fatalf("expected Restart and ForceRestart to be true, got restart=%v force=%v", merged.Restart, merged.ForceRestart)
	}
	if len(merged.NodeUUIDs) != 3 {
		t.Fatalf("expected 3 unique targets, got %d", len(merged.NodeUUIDs))
	}

	// Case 2: One targets all nodes (nil)
	reqAll := deployRequest{Restart: false, NodeUUIDs: nil}
	mergedAll := mergeDeployRequests(reqA, reqAll)
	if mergedAll.NodeUUIDs != nil {
		t.Fatalf("expected nil (all nodes), got %v", mergedAll.NodeUUIDs)
	}
	if !mergedAll.Restart {
		t.Fatalf("expected Restart=true")
	}
}

type mockDeployNodeServiceClient struct {
	proto.NodeServiceClient
	submitTaskFunc func(ctx context.Context, in *proto.NodeTask, opts ...grpc.CallOption) (*rpcstatus.Status, error)
}

func (m *mockDeployNodeServiceClient) SubmitTask(ctx context.Context, in *proto.NodeTask, opts ...grpc.CallOption) (*rpcstatus.Status, error) {
	if m.submitTaskFunc != nil {
		return m.submitTaskFunc(ctx, in, opts...)
	}
	return &rpcstatus.Status{Code: int32(codes.OK), Message: "success"}, nil
}

func TestDeployFailureIsolation(t *testing.T) {
	l, _ := logger.NewLogger("debug", "UTC", nil)
	nm := &NodeMonitor{
		cfg: &config.BackendConfig{Logger: l},
	}

	node1Failed := false
	node2Succeeded := false

	target1 := deployTarget{
		name: "node-1-failing",
		uuid: "uuid-1",
		client: &mockDeployNodeServiceClient{
			submitTaskFunc: func(ctx context.Context, in *proto.NodeTask, opts ...grpc.CallOption) (*rpcstatus.Status, error) {
				node1Failed = true
				return nil, fmt.Errorf("connection refused")
			},
		},
	}

	target2 := deployTarget{
		name: "node-2-working",
		uuid: "uuid-2",
		client: &mockDeployNodeServiceClient{
			submitTaskFunc: func(ctx context.Context, in *proto.NodeTask, opts ...grpc.CallOption) (*rpcstatus.Status, error) {
				node2Succeeded = true
				return &rpcstatus.Status{Code: int32(codes.OK), Message: "success: users=1 core_ready=true"}, nil
			},
		},
	}

	// Run concurrent deploy to both targets using waitgroup and bounded concurrency
	var wg sync.WaitGroup
	targets := []deployTarget{target1, target2}
	sem := make(chan struct{}, 2)

	for _, target := range targets {
		wg.Add(1)
		sem <- struct{}{}
		go func(tgt deployTarget) {
			defer wg.Done()
			defer func() { <-sem }()
			// submitDeployTask should not panic or block even if target fails
			_ = nm.submitDeployTask(tgt, []byte("{}"), true, false)
		}(target)
	}

	wg.Wait()

	if !node1Failed {
		t.Fatalf("expected node 1 to attempt deploy and fail")
	}
	if !node2Succeeded {
		t.Fatalf("expected node 2 to complete deploy successfully despite node 1 failure")
	}
}

func TestRequestDeployNonBlockingAndThreadSafe(t *testing.T) {
	nm := NewNodeMonitor(nil, nil)

	// Fill the channel buffer
	nm.RequestDeployWithForce(true, false, "uuid-1")

	// Trigger multiple concurrent requests without reading from deployNow
	// to verify it never deadlocks or blocks indefinitely
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nm.RequestDeployWithForce(false, true, fmt.Sprintf("uuid-%d", id))
		}(i)
	}
	wg.Wait()

	// Drain request from channel
	select {
	case req := <-nm.deployNow:
		if !req.ForceRestart {
			t.Errorf("expected force restart to be merged into request")
		}
	default:
		t.Errorf("expected at least one pending deploy request")
	}
}

func TestCloseNodeStateThreadSafety(t *testing.T) {
	nm := NewNodeMonitor(nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	state := &nodeState{
		nodeUUID: "test-node",
		nodeName: "node-1",
		ctx:      ctx,
		cancel:   cancel,
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nm.closeNodeState(state)
		}()
	}
	wg.Wait()

	state.mutex.RLock()
	defer state.mutex.RUnlock()
	if state.isConnected || state.isConnecting {
		t.Errorf("expected state to be disconnected")
	}
}

func TestHandleDisconnect_CleansMetricsAndStaticState(t *testing.T) {
	nm := NewNodeMonitor(nil, nil)
	const testUUID = "test-uuid-42"

	nm.metricsLock.Lock()
	nm.metricsByNodeUUID[testUUID] = &NodeMetricsSnapshot{
		NodeUUID:    testUUID,
		UsersOnline: 15,
	}
	nm.metricsLock.Unlock()

	state := &nodeState{
		nodeUUID:          testUUID,
		nodeName:          "test-node",
		isConnected:       true,
		hasSentStaticInfo: true,
		lastSingboxVer:    "1.13.3",
		lastNodeVer:       "1.0.0",
	}

	nm.handleDisconnect(state, "Stream closed")

	state.mutex.RLock()
	if state.isConnected {
		t.Errorf("expected isConnected to be false after disconnect")
	}
	if state.hasSentStaticInfo {
		t.Errorf("expected hasSentStaticInfo to be false after disconnect")
	}
	if state.lastSingboxVer != "" || state.lastNodeVer != "" {
		t.Errorf("expected cached versions to be cleared after disconnect")
	}
	state.mutex.RUnlock()

	nm.metricsLock.RLock()
	snap := nm.metricsByNodeUUID[testUUID]
	if snap == nil || snap.UsersOnline != 0 {
		t.Errorf("expected snapshot UsersOnline to be reset to 0, got %v", snap)
	}
	nm.metricsLock.RUnlock()
}

func TestCloseNodeState_ResetsStaticTracking(t *testing.T) {
	nm := NewNodeMonitor(nil, nil)
	state := &nodeState{
		nodeUUID:          "test-uuid-99",
		nodeName:          "node-99",
		isConnected:       true,
		hasSentStaticInfo: true,
		lastSingboxVer:    "1.13.0",
		lastNodeVer:       "2.0.0",
	}

	nm.closeNodeState(state)

	state.mutex.RLock()
	defer state.mutex.RUnlock()
	if state.isConnected {
		t.Errorf("expected isConnected to be false")
	}
	if state.hasSentStaticInfo {
		t.Errorf("expected hasSentStaticInfo to be false")
	}
}

func TestFormatNodeConnectionError(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{
			err:  fmt.Errorf("dial tcp 192.168.1.1:8443: connect: connection refused"),
			want: "connection refused (node agent is not running on target port)",
		},
		{
			err:  fmt.Errorf("rpc error: code = Unavailable desc = connection error: desc = \"transport: Error while dialing: dial tcp 10.0.0.1:443: connect: connection refused\""),
			want: "connection refused (node agent is not running on target port)",
		},
		{
			err:  fmt.Errorf("rpc error: code = Unavailable desc = transport: authentication handshake failed: EOF"),
			want: "remote host closed connection (host unreachable or node agent not running)",
		},
		{
			err:  fmt.Errorf("dial tcp: i/o timeout"),
			want: "connection timed out (host unreachable)",
		},
		{
			err:  fmt.Errorf("no such host: example.invalid"),
			want: "DNS resolution failed (host does not exist)",
		},
	}

	for _, tt := range tests {
		got := formatNodeConnectionError(tt.err)
		if got != tt.want {
			t.Errorf("formatNodeConnectionError(%v) = %q, want %q", tt.err, got, tt.want)
		}
	}
}

func TestDisconnectedNodeStateClientNil(t *testing.T) {
	nm := NewNodeMonitor(nil, nil)
	state := &nodeState{
		nodeUUID:    "test-uuid-client",
		nodeName:    "node-client",
		isConnected: true,
		client:      &mockDeployNodeServiceClient{},
	}

	nm.handleDisconnect(state, "connection refused (node agent is not running on target port)")

	state.mutex.RLock()
	defer state.mutex.RUnlock()
	if state.isConnected {
		t.Errorf("expected isConnected = false")
	}
	if state.client != nil {
		t.Errorf("expected client = nil after disconnect")
	}
	if state.lastError != "connection refused (node agent is not running on target port)" {
		t.Errorf("expected lastError = connection refused..., got %q", state.lastError)
	}
}

func TestShouldUpdateIdleHeartbeat(t *testing.T) {
	nm := NewNodeMonitor(nil, nil)
	nodeName := "test-node-1"
	now := time.Now()

	// Initial call should allow update
	if !nm.shouldUpdateIdleHeartbeat(nodeName, now) {
		t.Fatalf("expected first idle heartbeat update to be allowed")
	}

	// Record update
	nm.lastIdleHeartbeatUpdate.Store(nodeName, now)

	// Call 1 minute later: should be throttled
	if nm.shouldUpdateIdleHeartbeat(nodeName, now.Add(1*time.Minute)) {
		t.Fatalf("expected idle heartbeat update at +1m to be throttled")
	}

	// Call 4 minutes 59 seconds later: should be throttled
	if nm.shouldUpdateIdleHeartbeat(nodeName, now.Add(4*time.Minute+59*time.Second)) {
		t.Fatalf("expected idle heartbeat update at +4m59s to be throttled")
	}

	// Call 5 minutes later: should be allowed
	if !nm.shouldUpdateIdleHeartbeat(nodeName, now.Add(5*time.Minute)) {
		t.Fatalf("expected idle heartbeat update at +5m to be allowed")
	}
}


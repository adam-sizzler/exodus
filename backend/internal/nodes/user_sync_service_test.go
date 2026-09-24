package users

import (
	"context"
	"encoding/json"
	"testing"

	"exodus/internal/config"
	"exodus/internal/logger"
	"exodus/internal/proto"

	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func TestSyncUsersToConnectedNodes(t *testing.T) {
	l, _ := logger.NewLogger("debug", "UTC", nil)
	nm := &NodeMonitor{
		cfg:       &config.BackendConfig{Logger: l},
		nodes:     make(map[string]*nodeState),
		deployNow: make(chan deployRequest, 10),
	}

	taskReceived := false
	var receivedOp string
	var receivedPayload SyncUsersTaskPayload

	mockClient := &mockDeployNodeServiceClient{
		submitTaskFunc: func(ctx context.Context, in *proto.NodeTask, opts ...grpc.CallOption) (*rpcstatus.Status, error) {
			taskReceived = true
			receivedOp = in.Operation
			_ = json.Unmarshal(in.Payload, &receivedPayload)
			return &rpcstatus.Status{Code: int32(codes.OK), Message: "synced"}, nil
		},
	}

	nm.nodes["node-test"] = &nodeState{
		nodeUUID:    "uuid-node-1",
		isConnected: true,
		client:      mockClient,
	}

	testUsers := []UserSyncItem{
		{
			Action:         "add",
			Identifier:     "100",
			Username:       "alice",
			UUID:           "11111111-2222-3333-4444-555555555555",
			TrojanPassword: "password123",
		},
	}

	nm.syncUsersToConnectedNodes(testUsers, []string{"uuid-node-1"})

	if !taskReceived {
		t.Fatalf("expected task to be submitted to connected node")
	}
	if receivedOp != "sync_users" {
		t.Fatalf("expected operation 'sync_users', got %s", receivedOp)
	}
	if len(receivedPayload.Users) != 1 || receivedPayload.Users[0].Username != "alice" {
		t.Fatalf("unexpected received payload: %+v", receivedPayload)
	}
}

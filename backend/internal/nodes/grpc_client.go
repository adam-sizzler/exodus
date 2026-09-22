package users

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"exodus/internal/nodes/grpcauth"
	"exodus/internal/proto"
	scheduler "exodus/internal/scheduler"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
)

const (
	nodeStreamInterval      = 15 * time.Second
	nodeStreamIdleTimeout   = 45 * time.Second
	nodeStreamWatchInterval = 5 * time.Second
)

type nodeState struct {
	nodeUUID         string
	nodeName         string
	address          string
	port             int
	proxyURL         string
	apiSchema        string
	apiPath          string
	grpcAuthToken    string
	ctx              context.Context
	cancel           context.CancelFunc
	conn             *grpc.ClientConn
	client           proto.NodeServiceClient
	stream           proto.NodeService_StreamNodeDataClient
	streamCancel     context.CancelFunc
	streamGeneration uint64
	lastResponseAt   time.Time
	isConnected      bool
	isConnecting     bool
	lastError         string
	hasSentStaticInfo    bool
	lastStaticInfoSentAt time.Time
	lastSingboxVer       string
	lastNodeVer          string
	mutex                sync.RWMutex
}

// monitorNode monitors a single node with reconnection logic.
func (nm *NodeMonitor) monitorNode(state *nodeState) {
	reconnectInterval := scheduler.NodeReconnectInterval
	attempts := 0

	for {
		if state.ctx.Err() != nil {
			nm.cfg.Logger.Debug("Node monitor stopped", "node", state.nodeName)
			return
		}

		connected := nm.connectAndStream(state)
		if state.ctx.Err() != nil {
			nm.cfg.Logger.Debug("Node monitor stopped", "node", state.nodeName)
			return
		}

		if connected {
			attempts = 0
		}

		attempts++

		nm.cfg.Logger.Warn(
			"Node disconnected, scheduling reconnect",
			"node", state.nodeName,
			"address", state.address,
			"port", state.port,
			"attempt", attempts,
			"wait", reconnectInterval.String(),
		)

		timer := time.NewTimer(reconnectInterval)
		select {
		case <-state.ctx.Done():
			timer.Stop()
			nm.cfg.Logger.Debug("Node monitor stopped", "node", state.nodeName)
			return
		case <-timer.C:
		}
	}
}

// connectAndStream establishes connection and starts streaming. Returns true if connection was established.
func (nm *NodeMonitor) connectAndStream(state *nodeState) bool {
	state.mutex.Lock()
	if state.isConnecting {
		state.mutex.Unlock()
		return false
	}
	state.isConnecting = true
	state.mutex.Unlock()

	targetAddr := fmt.Sprintf("%s:%d", state.address, state.port)

	if state.address == "127.0.0.1" || strings.EqualFold(state.address, "localhost") {
		nm.cfg.Logger.Warn("Node address points to panel container loopback; use service name or host IP", "node", state.nodeName, "address", state.address)
	}

	apiSchema := normalizeNodeSchema(state.apiSchema)
	var useMTLS, useTLS, skipVerify bool
	switch apiSchema {
	case "tls":
		if strings.TrimSpace(state.grpcAuthToken) == "" {
			nm.cfg.Logger.Warn("Failed to connect to node: missing global gRPC token", "node", state.nodeName)
			nm.updateConnectionStatus(state.nodeName, false, false, "Missing gRPC token in keygen")
			state.mutex.Lock()
			state.isConnecting = false
			state.mutex.Unlock()
			return false
		}
		useTLS = true
		if nm.cfg != nil && nm.cfg.Backend.AllowInsecureHTTP {
			skipVerify = true
			nm.cfg.Logger.Warn("Node TLS verification is disabled by EXODUS_ALLOW_INSECURE_HTTP", "node", state.nodeName)
		}
		nm.cfg.Logger.Debug("Using TLS + gRPC token for node gRPC", "node", state.nodeName, "address", state.address)
	case "mtls":
		useMTLS = true
		nm.cfg.Logger.Debug("Using mTLS for node gRPC", "node", state.nodeName, "address", state.address)
	default:
		nm.cfg.Logger.Warn("Node gRPC connection is insecure", "node", state.nodeName, "schema", apiSchema)
	}

	cleanPath := normalizeNodePath(state.apiPath)
	if cleanPath != "" {
		nm.cfg.Logger.Debug("Using gRPC path prefix", "node", state.nodeName, "prefix", cleanPath)
	}

	opts, err := grpcauth.GetDialOptions(state.ctx, nm.db, useMTLS, useTLS, skipVerify, cleanPath, state.grpcAuthToken, state.proxyURL)
	if err != nil {
		nm.cfg.Logger.Warn("Failed to build gRPC options for node", "node", state.nodeName, "error", err)
		nm.updateConnectionStatus(state.nodeName, false, false, err.Error())
		state.mutex.Lock()
		state.isConnecting = false
		state.mutex.Unlock()
		return false
	}

	opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")))
	conn, err := grpc.NewClient(targetAddr, opts...)
	if err != nil {
		friendlyErr := formatNodeConnectionError(err)
		nm.cfg.Logger.Error("Failed to connect to node", "node", state.nodeName, "address", targetAddr, "error", friendlyErr)
		nm.updateConnectionStatus(state.nodeName, false, false, friendlyErr)
		state.mutex.Lock()
		state.isConnecting = false
		state.mutex.Unlock()
		return false
	}

	streamCtx, streamCancel := context.WithCancel(state.ctx)

	client := proto.NewNodeServiceClient(conn)

	stream, err := client.StreamNodeData(streamCtx)
	if err != nil {
		streamCancel()
		friendlyErr := formatNodeConnectionError(err)
		nm.cfg.Logger.Error("Failed to connect to node", "node", state.nodeName, "address", targetAddr, "error", friendlyErr)
		conn.Close()
		nm.updateConnectionStatus(state.nodeName, false, false, friendlyErr)
		state.mutex.Lock()
		state.isConnecting = false
		state.mutex.Unlock()
		return false
	}

	if err := stream.Send(&proto.NodeDataRequest{
		Request: &proto.NodeDataRequest_Config{
			Config: &proto.StreamConfig{
				IntervalSeconds: int32(nodeStreamInterval / time.Second),
			},
		},
	}); err != nil {
		streamCancel()
		friendlyErr := formatNodeConnectionError(err)
		nm.cfg.Logger.Warn("Failed to initialize node stream", "node", state.nodeName, "address", targetAddr, "error", friendlyErr)
		conn.Close()
		nm.updateConnectionStatus(state.nodeName, false, false, friendlyErr)
		state.mutex.Lock()
		state.isConnecting = false
		state.mutex.Unlock()
		return false
	}

	state.mutex.Lock()
	state.conn = conn
	state.client = client
	state.stream = stream
	state.streamCancel = streamCancel
	state.streamGeneration++
	generation := state.streamGeneration
	state.lastResponseAt = time.Now()
	state.isConnected = true
	state.isConnecting = false
	state.lastError = ""
	state.hasSentStaticInfo = false
	state.lastStaticInfoSentAt = time.Time{}
	state.lastSingboxVer = ""
	state.lastNodeVer = ""
	state.mutex.Unlock()

	go nm.watchStreamHeartbeat(state, generation, nodeStreamIdleTimeout, nodeStreamWatchInterval)

	nm.updateConnectionStatus(state.nodeName, false, true, "")

	nm.cfg.Logger.Debug("Node control-plane connected", "node", state.nodeName, "address", state.address, "port", state.port)
	nm.RequestDeploy(true, state.nodeUUID)

	nm.receiveStream(state)
	return true
}

func (nm *NodeMonitor) watchStreamHeartbeat(
	state *nodeState,
	generation uint64,
	idleTimeout time.Duration,
	watchInterval time.Duration,
) {
	ticker := time.NewTicker(watchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-state.ctx.Done():
			return
		case <-ticker.C:
			state.mutex.RLock()
			currentGeneration := state.streamGeneration
			lastResponse := state.lastResponseAt
			isConnected := state.isConnected
			state.mutex.RUnlock()

			if currentGeneration != generation || !isConnected {
				return
			}

			if !lastResponse.IsZero() && time.Since(lastResponse) > idleTimeout {
				if nm != nil && nm.cfg != nil {
					nm.cfg.Logger.Warn(
						"Node stream idle timeout exceeded, restarting stream",
						"node", state.nodeName,
						"idle_for", time.Since(lastResponse).String(),
						"timeout", idleTimeout.String(),
					)
				}
				nm.handleDisconnect(state, "Stream idle timeout")
				return
			}
		}
	}
}

func (nm *NodeMonitor) markStreamActivity(state *nodeState) {
	state.mutex.Lock()
	state.lastResponseAt = time.Now()
	state.mutex.Unlock()
}

func normalizeNodeSchema(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "tls":
		return "tls"
	case "mtls", "grpc", "grpcs", "https", "":
		return "mtls"
	default:
		return "mtls"
	}
}

func normalizeNodePath(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed == "/" {
		return ""
	}
	return "/" + strings.Trim(trimmed, "/")
}

func formatNodeConnectionError(err error) string {
	if err == nil {
		return ""
	}
	errStr := err.Error()
	if strings.Contains(errStr, "authentication handshake failed: EOF") || strings.Contains(errStr, "transport: authentication handshake failed") {
		return "remote host closed connection (host unreachable or node agent not running)"
	}
	if strings.Contains(errStr, "connection refused") {
		return "connection refused (node agent is not running on target port)"
	}
	if strings.Contains(errStr, "i/o timeout") || strings.Contains(errStr, "context deadline exceeded") {
		return "connection timed out (host unreachable)"
	}
	if strings.Contains(errStr, "no such host") || strings.Contains(errStr, "lookup") {
		return "DNS resolution failed (host does not exist)"
	}
	if strings.Contains(errStr, "certificate") || strings.Contains(errStr, "tls:") {
		return fmt.Sprintf("TLS verification failed: %v", err)
	}
	return errStr
}

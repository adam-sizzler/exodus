package grpcauth

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestPathPrefixUnaryInterceptor(t *testing.T) {
	interceptor := PathPrefixUnaryInterceptor("/custom-path", "  secret-token-123  ")

	var invokedMethod string
	var capturedMD metadata.MD

	dummyInvoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		invokedMethod = method
		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			capturedMD = md
		}
		return nil
	}

	err := interceptor(context.Background(), "/proto.NodeService/StreamData", nil, nil, nil, dummyInvoker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedMethod := "/custom-path/proto.NodeService/StreamData"
	if invokedMethod != expectedMethod {
		t.Errorf("invokedMethod = %q, want %q", invokedMethod, expectedMethod)
	}

	tokens := capturedMD.Get("x-exodus-grpc-token")
	if len(tokens) != 1 || tokens[0] != "secret-token-123" {
		t.Errorf("expected token %q, got %v", "secret-token-123", tokens)
	}
}

func TestPathPrefixStreamInterceptor(t *testing.T) {
	interceptor := PathPrefixStreamInterceptor("/custom-path", "  stream-token-456  ")

	var invokedMethod string
	var capturedMD metadata.MD

	dummyStreamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		invokedMethod = method
		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			capturedMD = md
		}
		return nil, nil
	}

	_, err := interceptor(context.Background(), nil, nil, "/proto.NodeService/StreamNodeData", dummyStreamer)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedMethod := "/custom-path/proto.NodeService/StreamNodeData"
	if invokedMethod != expectedMethod {
		t.Errorf("invokedMethod = %q, want %q", invokedMethod, expectedMethod)
	}

	tokens := capturedMD.Get("x-exodus-grpc-token")
	if len(tokens) != 1 || tokens[0] != "stream-token-456" {
		t.Errorf("expected token %q, got %v", "stream-token-456", tokens)
	}
}

func TestPathPrefixInterceptor_EmptyToken(t *testing.T) {
	interceptor := PathPrefixUnaryInterceptor("", "   ")

	var capturedMD metadata.MD
	dummyInvoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			capturedMD = md
		}
		return nil
	}

	_ = interceptor(context.Background(), "/test", nil, nil, nil, dummyInvoker)
	if len(capturedMD.Get("x-exodus-grpc-token")) != 0 {
		t.Errorf("expected no token for whitespace auth token")
	}
}

func TestBuildNodeProxyDialer(t *testing.T) {
	// Empty URL -> nil dialer
	dialer, err := BuildNodeProxyDialer("")
	if err != nil || dialer != nil {
		t.Errorf("expected nil dialer for empty URL, got err=%v", err)
	}

	// Invalid scheme -> error
	_, err = BuildNodeProxyDialer("http://127.0.0.1:8080")
	if err == nil {
		t.Errorf("expected error for unsupported scheme http")
	}

	// Valid socks5 -> dialer created
	dialer, err = BuildNodeProxyDialer("socks5://127.0.0.1:1080")
	if err != nil || dialer == nil {
		t.Errorf("expected valid dialer for socks5, got err=%v", err)
	}
}

func BenchmarkPathPrefixUnaryInterceptor(b *testing.B) {
	interceptor := PathPrefixUnaryInterceptor("/prefix", "  token-123456  ")
	ctx := context.Background()
	dummyInvoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = interceptor(ctx, "/method", nil, nil, nil, dummyInvoker)
	}
}

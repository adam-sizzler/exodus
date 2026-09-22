package grpcauth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	neturl "net/url"
	"strings"
	"time"

	"exodus/internal/db"

	"golang.org/x/net/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

func LoadKeygenMTLSConfig(ctx context.Context, dbConn db.DBTX) (*tls.Config, error) {
	var (
		caCertPEM     string
		clientCertPEM string
		clientKeyPEM  string
	)

	err := dbConn.QueryRow(ctx, `
		SELECT ca_cert, client_cert, client_key
		FROM keygen
		ORDER BY created_at ASC
		LIMIT 1
	`).Scan(&caCertPEM, &clientCertPEM, &clientKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("load keygen mTLS material: %w", err)
	}

	clientCert, err := tls.X509KeyPair([]byte(clientCertPEM), []byte(clientKeyPEM))
	if err != nil {
		return nil, fmt.Errorf("parse keygen client certificate/key: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(caCertPEM)) {
		return nil, fmt.Errorf("parse keygen CA certificate")
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      pool,
		Certificates: []tls.Certificate{clientCert},
		ServerName:   "internal.exodus.local",
	}, nil
}

func PathPrefixUnaryInterceptor(prefix, authToken string) grpc.UnaryClientInterceptor {
	token := strings.TrimSpace(authToken)
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if token != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "x-exodus-grpc-token", token)
		}
		return invoker(ctx, prefix+method, req, reply, cc, opts...)
	}
}

func PathPrefixStreamInterceptor(prefix, authToken string) grpc.StreamClientInterceptor {
	token := strings.TrimSpace(authToken)
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if token != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "x-exodus-grpc-token", token)
		}
		return streamer(ctx, desc, cc, prefix+method, opts...)
	}
}

func BuildNodeProxyDialer(rawProxyURL string) (func(context.Context, string) (net.Conn, error), error) {
	rawProxyURL = strings.TrimSpace(rawProxyURL)
	if rawProxyURL == "" {
		return nil, nil
	}

	parsed, err := neturl.Parse(rawProxyURL)
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(parsed.Scheme, "socks5") {
		return nil, fmt.Errorf("unsupported proxy scheme %q", parsed.Scheme)
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return nil, fmt.Errorf("proxy host is required")
	}

	dialer, err := proxy.FromURL(parsed, proxy.Direct)
	if err != nil {
		return nil, err
	}
	if contextDialer, ok := dialer.(proxy.ContextDialer); ok {
		return func(ctx context.Context, address string) (net.Conn, error) {
			return contextDialer.DialContext(ctx, "tcp", address)
		}, nil
	}

	return func(ctx context.Context, address string) (net.Conn, error) {
		type dialResult struct {
			conn net.Conn
			err  error
		}
		resultCh := make(chan dialResult, 1)
		go func() {
			conn, dialErr := dialer.Dial("tcp", address)
			resultCh <- dialResult{conn: conn, err: dialErr}
		}()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case r := <-resultCh:
			return r.conn, r.err
		}
	}, nil
}

func GetDialOptions(ctx context.Context, dbConn db.DBTX, useMTLS bool, useTLS bool, skipVerify bool, cleanPath string, grpcAuthToken string, rawProxyURL string) ([]grpc.DialOption, error) {
	opts := make([]grpc.DialOption, 0)

	if useMTLS {
		tlsCfg, err := LoadKeygenMTLSConfig(ctx, dbConn)
		if err != nil {
			return nil, fmt.Errorf("mTLS config failed: %w", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else if useTLS {
		tlsCfg := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		if skipVerify {
			tlsCfg.InsecureSkipVerify = true
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	opts = append(opts, grpc.WithUnaryInterceptor(PathPrefixUnaryInterceptor(cleanPath, grpcAuthToken)))
	opts = append(opts, grpc.WithStreamInterceptor(PathPrefixStreamInterceptor(cleanPath, grpcAuthToken)))

	kacp := keepalive.ClientParameters{
		Time:                15 * time.Second,
		Timeout:             5 * time.Second,
		PermitWithoutStream: true,
	}
	opts = append(opts, grpc.WithKeepaliveParams(kacp))

	dialer, dialerErr := BuildNodeProxyDialer(rawProxyURL)
	if dialerErr != nil {
		return nil, fmt.Errorf("proxy config failed: %w", dialerErr)
	} else if dialer != nil {
		opts = append(opts, grpc.WithContextDialer(dialer))
	} else {
		netDialer := &net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 15 * time.Second,
		}
		opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return netDialer.DialContext(ctx, "tcp", addr)
		}))
	}

	return opts, nil
}

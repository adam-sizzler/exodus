package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"exodus/internal/config"
)

func TestAuthRateLimiterInMemoryFallback(t *testing.T) {
	limiter := &AuthRateLimiter{
		attempts: make(map[string][]time.Time),
	}

	ip := "192.168.1.100"
	ctx := context.Background()

	// Initial check should allow
	allowed, remaining, _ := limiter.Allow(ctx, ip, nil)
	if !allowed {
		t.Fatalf("expected initial request to be allowed")
	}
	if remaining != AuthRateLimitMaxAttempts {
		t.Fatalf("expected remaining to be %d, got %d", AuthRateLimitMaxAttempts, remaining)
	}

	// Record 4 failed attempts
	for i := 0; i < 4; i++ {
		limiter.RecordFailedAttempt(ctx, ip, nil)
	}

	// 5th attempt check
	allowed, remaining, _ = limiter.Allow(ctx, ip, nil)
	if !allowed {
		t.Fatalf("expected 5th attempt to still be allowed")
	}
	if remaining != 1 {
		t.Fatalf("expected remaining to be 1, got %d", remaining)
	}

	// Record 5th failed attempt (limit reached)
	limiter.RecordFailedAttempt(ctx, ip, nil)

	// 6th attempt should be BLOCKED
	allowed, remaining, retryAfter := limiter.Allow(ctx, ip, nil)
	if allowed {
		t.Fatalf("expected 6th attempt to be blocked")
	}
	if remaining != 0 {
		t.Fatalf("expected remaining to be 0, got %d", remaining)
	}
	if retryAfter <= 0 {
		t.Fatalf("expected retryAfter > 0, got %v", retryAfter)
	}

	// Reset on successful login
	limiter.Reset(ctx, ip, nil)

	// After reset, should be allowed again
	allowed, remaining, _ = limiter.Allow(ctx, ip, nil)
	if !allowed {
		t.Fatalf("expected request after reset to be allowed")
	}
	if remaining != AuthRateLimitMaxAttempts {
		t.Fatalf("expected remaining after reset to be %d, got %d", AuthRateLimitMaxAttempts, remaining)
	}
}

func TestLoginRateLimitKeyPrefersClientIP(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", nil)
	req.Header.Set("X-Forwarded-For", "203.0.113.42, 172.18.0.2")
	req.RemoteAddr = "172.18.0.2:48392"

	key := loginRateLimitKey(req, nil)
	if key != "203.0.113.42" {
		t.Fatalf("expected loginRateLimitKey to resolve to client IP 203.0.113.42, got %q", key)
	}
}

func TestAuthLoginRateLimitHeaders(t *testing.T) {
	cfg := &config.BackendConfig{}
	ip := "198.51.100.99"

	// Exhaust attempts
	for i := 0; i < AuthRateLimitMaxAttempts; i++ {
		globalAuthRateLimiter.RecordFailedAttempt(context.Background(), ip, cfg)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", nil)
	req.RemoteAddr = ip + ":54321"

	rr := httptest.NewRecorder()
	handler := AuthLoginCompatHandler(nil, cfg)
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("expected status 429, got %d", rr.Code)
	}
	if limit := rr.Header().Get("X-RateLimit-Limit"); limit != "5" {
		t.Fatalf("expected X-RateLimit-Limit 5, got %q", limit)
	}
	if remaining := rr.Header().Get("X-RateLimit-Remaining"); remaining != "0" {
		t.Fatalf("expected X-RateLimit-Remaining 0, got %q", remaining)
	}
	if retryAfter := rr.Header().Get("Retry-After"); retryAfter == "" {
		t.Fatalf("expected Retry-After header to be set")
	}

	// Clean up
	globalAuthRateLimiter.Reset(context.Background(), ip, cfg)
}


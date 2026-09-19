package auth

import (
	"context"
	"net/http/httptest"
	"testing"
)

func TestExternalLoginNotificationDataUsesResolvedClientIP(t *testing.T) {
	req := httptest.NewRequest("POST", "/api/auth/oauth2/callback", nil)
	req.RemoteAddr = "172.18.0.8:47242"
	req.Header.Set("X-Forwarded-For", "74.2.54.120, 172.18.0.1")
	req.Header.Set("X-Real-IP", "172.18.0.1")
	req.Header.Set("User-Agent", "Mozilla/5.0")

	data := externalLoginNotificationData(nil, "oauth2", "telegram", "406150372", "admin-uuid", "", req)

	if got := data["ip"]; got != "74.2.54.120" {
		t.Fatalf("data ip got %v, want %q", got, "74.2.54.120")
	}
	loginAttempt, ok := data["loginAttempt"].(map[string]any)
	if !ok {
		t.Fatalf("loginAttempt missing or has invalid type: %T", data["loginAttempt"])
	}
	if got := loginAttempt["ip"]; got != "74.2.54.120" {
		t.Fatalf("loginAttempt ip got %v, want %q", got, "74.2.54.120")
	}
	if _, ok := loginAttempt["remoteAddr"]; ok {
		t.Fatalf("loginAttempt must not expose remoteAddr: %v", loginAttempt["remoteAddr"])
	}
	if got := loginAttempt["username"]; got != "406150372" {
		t.Fatalf("loginAttempt username got %v, want %q", got, "406150372")
	}
}

func TestExternalLoginNotificationUsernameFallback(t *testing.T) {
	if got := externalLoginNotificationUsername("oauth2", "telegram", ""); got != "oauth2:telegram" {
		t.Fatalf("username fallback got %q, want %q", got, "oauth2:telegram")
	}
	if got := externalLoginNotificationUsername("oauth2", "telegram", " 406150372 "); got != "406150372" {
		t.Fatalf("username identifier got %q, want %q", got, "406150372")
	}
}

func TestOAuthStateStoreAndTake(t *testing.T) {
	ctx := context.Background()

	// 1. Store state for User A
	storeOAuthState(ctx, nil, "github", "state-A", "verifier-A")
	// 2. Store state for User B with the same provider
	storeOAuthState(ctx, nil, "github", "state-B", "verifier-B")

	// 3. User A takes state-A -> must return verifier-A without being overwritten by User B
	entryA, ok := takeOAuthState(ctx, nil, "state-A")
	if !ok {
		t.Fatalf("expected state-A to exist")
	}
	if entryA.CodeVerifier != "verifier-A" || entryA.Provider != "github" {
		t.Fatalf("expected verifier-A and provider github, got %+v", entryA)
	}

	// 4. Double take on state-A must fail (one-time use)
	_, ok = takeOAuthState(ctx, nil, "state-A")
	if ok {
		t.Fatalf("state-A must be consumed on first take")
	}

	// 5. User B takes state-B
	entryB, ok := takeOAuthState(ctx, nil, "state-B")
	if !ok {
		t.Fatalf("expected state-B to exist")
	}
	if entryB.CodeVerifier != "verifier-B" || entryB.Provider != "github" {
		t.Fatalf("expected verifier-B and provider github, got %+v", entryB)
	}
}

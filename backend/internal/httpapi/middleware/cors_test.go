package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"exodus/internal/config"
)

func TestWithCORS_CSPHeaders(t *testing.T) {
	cfg := &config.BackendConfig{
		Backend: config.BackendAppConfig{
			AllowInsecureHTTP: true,
		},
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := WithCORS(cfg, next)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)

	handler.ServeHTTP(rec, req)

	csp := rec.Header().Get("Content-Security-Policy")
	if csp == "" {
		t.Fatal("expected Content-Security-Policy header to be set")
	}

	if !strings.Contains(csp, "frame-ancestors 'self'") {
		t.Errorf("expected frame-ancestors 'self', got CSP: %s", csp)
	}

	if !strings.Contains(csp, "default-src 'self'") {
		t.Errorf("expected default-src 'self', got CSP: %s", csp)
	}

	if !strings.Contains(csp, "script-src 'self' 'wasm-unsafe-eval'") {
		t.Errorf("expected script-src 'self' 'wasm-unsafe-eval', got CSP: %s", csp)
	}

	// Verify no '*' wildcard in script-src, connect-src, frame-ancestors, default-src
	for _, directive := range strings.Split(csp, ";") {
		directive = strings.TrimSpace(directive)
		if strings.HasPrefix(directive, "default-src") ||
			strings.HasPrefix(directive, "script-src") ||
			strings.HasPrefix(directive, "connect-src") ||
			strings.HasPrefix(directive, "frame-ancestors") {
			parts := strings.Fields(directive)
			for _, part := range parts[1:] {
				if part == "*" {
					t.Errorf("found unsafe '*' wildcard in directive: %s", directive)
				}
			}
		}
	}
}

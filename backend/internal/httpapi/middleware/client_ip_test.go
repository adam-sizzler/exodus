package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"exodus/internal/config"
)

func TestGetClientIPUsesForwardedForPublicAddressBeforeProxyRealIP(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "172.18.0.8:47242"
	req.Header.Set("X-Forwarded-For", "144.31.119.150, 172.18.0.1")

	if got := GetClientIP(req, nil); got != "144.31.119.150" {
		t.Fatalf("GetClientIP got %q, want %q", got, "144.31.119.150")
	}
}

func TestGetClientIPUsesExodusRealIPHeader(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "172.18.0.8:47242"
	req.Header.Set(ExodusRealIPHeader, "144.31.119.150")
	req.Header.Set("X-Forwarded-For", "172.18.0.1")

	if got := GetClientIP(req, nil); got != "144.31.119.150" {
		t.Fatalf("GetClientIP got %q, want %q", got, "144.31.119.150")
	}
}

func TestGetClientIPIgnoresSpoofedHeadersFromUntrustedRemote(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "198.51.100.5:12345" // Public untrusted IP
	req.Header.Set("X-Forwarded-For", "1.1.1.1")
	req.Header.Set("X-Real-IP", "8.8.8.8")
	req.Header.Set(ExodusRealIPHeader, "9.9.9.9")

	// Untrusted remote address must ignore all spoofed headers
	if got := GetClientIP(req, nil); got != "198.51.100.5" {
		t.Fatalf("GetClientIP got %q, want %q", got, "198.51.100.5")
	}
}

func TestGetClientIPWithExplicitTrustedProxies(t *testing.T) {
	cfg := &config.BackendConfig{}
	cfg.Backend.TrustedProxies = []string{"10.10.0.0/16"}

	// Request from configured trusted proxy: headers must be honored
	reqTrusted := httptest.NewRequest(http.MethodGet, "/", nil)
	reqTrusted.RemoteAddr = "10.10.1.5:43210"
	reqTrusted.Header.Set("X-Real-IP", "203.0.113.195")

	if got := GetClientIP(reqTrusted, cfg); got != "203.0.113.195" {
		t.Fatalf("GetClientIP for trusted proxy got %q, want %q", got, "203.0.113.195")
	}

	// Request from untrusted IP (not in 10.10.0.0/16): headers must be ignored
	reqUntrusted := httptest.NewRequest(http.MethodGet, "/", nil)
	reqUntrusted.RemoteAddr = "172.18.0.5:43210"
	reqUntrusted.Header.Set("X-Real-IP", "203.0.113.195")

	if got := GetClientIP(reqUntrusted, cfg); got != "172.18.0.5" {
		t.Fatalf("GetClientIP for untrusted remote got %q, want %q", got, "172.18.0.5")
	}
}

func TestWithClientIPStoresResolvedIPInContext(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "172.18.0.8:47242"
	req.Header.Set("X-Forwarded-For", "144.31.119.150, 172.18.0.1")

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := GetClientIP(r, nil); got != "144.31.119.150" {
			t.Fatalf("GetClientIP from context got %q, want %q", got, "144.31.119.150")
		}
	})

	WithClientIP(nil, next).ServeHTTP(httptest.NewRecorder(), req)
}

func BenchmarkResolveClientIP(b *testing.B) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "172.18.0.8:47242"
	req.Header.Set("X-Forwarded-For", "144.31.119.150, 172.18.0.1")
	req.Header.Set("X-Real-IP", "172.18.0.1")

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = ResolveClientIP(req, nil)
	}
}

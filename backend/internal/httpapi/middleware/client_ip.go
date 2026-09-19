package middleware

import (
	"context"
	"net"
	"net/http"
	"net/netip"
	"strings"

	"exodus/internal/config"
)

const (
	ExodusRealIPHeader   = "X-Exodus-Real-Ip"
	HeaderCFConnectingIP = "Cf-Connecting-Ip"
	HeaderTrueClientIP   = "True-Client-Ip"
	HeaderXForwardedFor  = "X-Forwarded-For"
	HeaderXRealIP        = "X-Real-Ip"
	HeaderXClientIP      = "X-Client-Ip"
)

type clientIPContextKey struct{}

// WithClientIP resolves the client IP once per request and stores it in the
// request context. Handlers should use GetClientIP instead of reading
// RemoteAddr directly when they need the end-user address.
func WithClientIP(cfg *config.BackendConfig, next http.Handler) http.Handler {
	if next == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientIP := ResolveClientIP(r, cfg)
		ctx := context.WithValue(r.Context(), clientIPContextKey{}, clientIP)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetClientIP returns the request client IP resolved by WithClientIP, or
// resolves it on demand when the middleware has not run for this request.
func GetClientIP(r *http.Request, cfg *config.BackendConfig) string {
	if r == nil {
		return ""
	}
	if value, ok := r.Context().Value(clientIPContextKey{}).(string); ok && strings.TrimSpace(value) != "" {
		return value
	}
	return ResolveClientIP(r, cfg)
}

// ResolveClientIP retrieves the client IP address from an HTTP request.
// If the immediate remote address is a trusted reverse proxy, proxy forwarding headers
// are evaluated in priority order (X-Exodus-Real-IP, Cf-Connecting-IP, True-Client-IP, X-Forwarded-For, X-Real-IP).
// If the remote address is not a trusted proxy, forwarding headers are ignored to prevent IP spoofing.
func ResolveClientIP(r *http.Request, cfg *config.BackendConfig) string {
	if r == nil {
		return ""
	}

	remoteIPStr := extractHost(r.RemoteAddr)
	remoteAddr, remoteOk := normalizeIPAddr(remoteIPStr)
	if !remoteOk {
		return "0.0.0.0"
	}

	// Check if the connecting address is trusted to provide client IP headers.
	if !isTrustedRemoteAddr(remoteAddr, cfg) {
		if cfg != nil && cfg.Logger != nil && cfg.Logger.IsDebugEnabled() {
			cfg.Logger.Debug("Untrusted remote address, ignoring forwarding headers", "remote_addr", r.RemoteAddr)
		}
		return remoteAddr.String()
	}

	// For trusted proxies, evaluate headers using direct canonical map indexing (zero allocations)
	if r.Header != nil {
		// 1. Fast-path: X-Exodus-Real-Ip (trusted upstream/internal header)
		if v := r.Header[ExodusRealIPHeader]; len(v) > 0 && v[0] != "" {
			if candidate, ok := normalizeIPAddr(v[0]); ok && candidate.IsValid() {
				return candidate.String()
			}
		}

		// 2. Cf-Connecting-Ip (Cloudflare edge)
		if v := r.Header[HeaderCFConnectingIP]; len(v) > 0 && v[0] != "" {
			if candidate, ok := normalizeIPAddr(v[0]); ok && candidate.IsValid() {
				return candidate.String()
			}
		}

		// 3. True-Client-Ip (Akamai / Cloudflare Enterprise)
		if v := r.Header[HeaderTrueClientIP]; len(v) > 0 && v[0] != "" {
			if candidate, ok := normalizeIPAddr(v[0]); ok && candidate.IsValid() {
				return candidate.String()
			}
		}

		// 4. X-Forwarded-For: traverse right-to-left to find the first untrusted client IP
		if v := r.Header[HeaderXForwardedFor]; len(v) > 0 && v[0] != "" {
			remaining := v[0]
			var lastValid netip.Addr
			for len(remaining) > 0 {
				var item string
				if idx := strings.LastIndexByte(remaining, ','); idx >= 0 {
					item = remaining[idx+1:]
					remaining = remaining[:idx]
				} else {
					item = remaining
					remaining = ""
				}
				candidate, ok := normalizeIPAddr(item)
				if !ok || !candidate.IsValid() {
					continue
				}
				lastValid = candidate
				if !isTrustedRemoteAddr(candidate, cfg) {
					return candidate.String()
				}
			}
			if lastValid.IsValid() {
				return lastValid.String()
			}
		}

		// 5. X-Real-Ip
		if v := r.Header[HeaderXRealIP]; len(v) > 0 && v[0] != "" {
			if candidate, ok := normalizeIPAddr(v[0]); ok && candidate.IsValid() {
				return candidate.String()
			}
		}

		// 6. X-Client-Ip
		if v := r.Header[HeaderXClientIP]; len(v) > 0 && v[0] != "" {
			if candidate, ok := normalizeIPAddr(v[0]); ok && candidate.IsValid() {
				return candidate.String()
			}
		}
	}

	return remoteAddr.String()
}

func isTrustedRemoteAddr(addr netip.Addr, cfg *config.BackendConfig) bool {
	if !addr.IsValid() {
		return false
	}
	if addr.IsLoopback() {
		return true
	}
	if cfg != nil && cfg.Backend.AllowInsecureHTTP {
		return true
	}

	ip := net.IP(addr.AsSlice())

	// If explicit trusted proxies are configured, check against them
	if cfg != nil && len(cfg.Backend.TrustedProxies) > 0 {
		return cfg.Backend.IsTrustedProxy(ip)
	}

	// In containerized deployments without explicit trusted proxies, private network addresses (Docker bridge) are trusted
	return addr.IsPrivate()
}

func extractHost(remoteAddr string) string {
	trimmed := strings.TrimSpace(remoteAddr)
	if trimmed == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(trimmed); err == nil {
		return host
	}
	return trimmed
}

func normalizeIPAddr(trimmed string) (netip.Addr, bool) {
	trimmed = strings.Trim(strings.TrimSpace(trimmed), "\"'")
	if trimmed == "" {
		return netip.Addr{}, false
	}
	if host, _, err := net.SplitHostPort(trimmed); err == nil {
		trimmed = host
	}
	trimmed = strings.Trim(trimmed, "[]")
	if strings.HasPrefix(strings.ToLower(trimmed), "::ffff:") {
		trimmed = trimmed[7:]
	}
	if zone := strings.LastIndex(trimmed, "%"); zone > -1 {
		trimmed = trimmed[:zone]
	}
	addr, err := netip.ParseAddr(trimmed)
	if err != nil {
		return netip.Addr{}, false
	}
	return addr.Unmap(), true
}

// CanonicalIP returns the canonical string form of an IP literal.
func CanonicalIP(raw string) string {
	addr, ok := normalizeIPAddr(raw)
	if !ok {
		return ""
	}
	return addr.String()
}

package middleware

import (
	"net"
	"net/http"
	"strings"

	"exodus/internal/config"
)

var (
	defaultCORSMethods = []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}
	defaultCORSHeaders = []string{"Content-Type", "Authorization", "X-API-Token", "Cookie"}
)

const (
	defaultCORSMethodsHeader = "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS"
	defaultCORSHeadersHeader = "Content-Type, Authorization, X-API-Token, Cookie"
)

// WithCORS adds CORS headers and Server header to all responses.
func WithCORS(cfg *config.BackendConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cfg != nil && !cfg.Backend.AllowInsecureHTTP && !IsSecureRequest(r, cfg) && !isHealthPath(r.URL.Path) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUpgradeRequired)
			_, _ = w.Write([]byte(`{"error":"https_required","message":"panel requires HTTPS"}`))
			return
		}

		// Prevent MIME-type sniffing.
		w.Header().Set("X-Content-Type-Options", "nosniff")

		// Disable legacy XSS auditor (removed from all browsers; 0 = don't enable broken fallback).
		w.Header().Set("X-XSS-Protection", "0")

		// Allow framing only by same origin (DENY breaks OAuth popup flows).
		w.Header().Set("X-Frame-Options", "SAMEORIGIN")

		// COOP & CORP (Сross-origin isolation headers)
		w.Header().Set("Cross-Origin-Opener-Policy", "same-origin-allow-popups")
		w.Header().Set("Cross-Origin-Resource-Policy", "same-site")

		// Send origin only on same-origin, only the origin (no path) cross-origin.
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")

		// HSTS — also in nginx for preload, here as belt-and-suspenders for direct Go deployments.
		w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")

		// Block search engine indexing of the admin panel.
		w.Header().Set("X-Robots-Tag", "noindex, nofollow, noarchive, nosnippet, noimageindex")

		// Content Security Policy.
		csp := "default-src 'self';" +
			"script-src 'self' 'unsafe-inline' 'unsafe-eval' 'wasm-unsafe-eval';" +
			"img-src 'self' data: https: blob:;" +
			"connect-src 'self' https: http: ws: wss: data: blob: https://raw.githubusercontent.com https://ungh.cc;" +
			"worker-src 'self' blob:;" +
			"frame-src 'self' https://oauth.telegram.org;" +
			"frame-ancestors 'self';" +
			"base-uri 'self';" +
			"font-src 'self' https: data:;" +
			"form-action 'self';" +
			"object-src 'none';" +
			"script-src-attr 'none';" +
			"style-src 'self' https: 'unsafe-inline';"
		if cfg != nil && !cfg.Backend.AllowInsecureHTTP {
			csp += "upgrade-insecure-requests;"
		}
		w.Header().Set("Content-Security-Policy", csp)

		// Use CORS settings from config
		allowedOrigins := cfg.CORS.AllowedOrigins

		// If no allowed origins configured, do not emit CORS headers.
		origin := r.Header.Get("Origin")
		allowOrigin := ""

		// Check if "*" is in the list (allow all)
		for _, o := range allowedOrigins {
			if o == "*" {
				allowOrigin = "*"
				break
			}
			if o == origin {
				allowOrigin = origin
				break
			}
		}

		if allowOrigin != "" {
			w.Header().Set("Access-Control-Allow-Origin", allowOrigin)
			w.Header().Add("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", defaultCORSMethodsHeader)
			w.Header().Set("Access-Control-Allow-Headers", defaultCORSHeadersHeader)
			w.Header().Set("Access-Control-Max-Age", "86400")
		}

		// Handle preflight OPTIONS requests
		if r.Method == http.MethodOptions {
			if allowOrigin != "" {
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(http.StatusForbidden)
			}
			return
		}

		next.ServeHTTP(w, r)
	})
}

func IsSecureRequest(r *http.Request, cfg *config.BackendConfig) bool {
	if r.TLS != nil {
		return true
	}
	if cfg == nil {
		return false
	}

	remoteIP := parseRemoteIP(r.RemoteAddr)
	if remoteIP != nil && remoteIP.IsLoopback() {
		return true
	}
	if remoteIP == nil || !cfg.Backend.IsTrustedProxy(remoteIP) {
		return false
	}

	if proto := firstHeaderValue(r.Header.Get("X-Forwarded-Proto")); proto != "" {
		return strings.EqualFold(proto, "https")
	}
	if proto := firstHeaderValue(r.Header.Get("X-Forwarded-Scheme")); proto != "" {
		return strings.EqualFold(proto, "https")
	}
	if strings.EqualFold(r.Header.Get("X-Forwarded-Ssl"), "on") {
		return true
	}
	if forwarded := r.Header.Get("Forwarded"); forwarded != "" {
		if proto := parseForwardedProto(forwarded); proto != "" {
			return strings.EqualFold(proto, "https")
		}
	}
	return false
}

func parseRemoteIP(remoteAddr string) net.IP {
	if remoteAddr == "" {
		return nil
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		return net.ParseIP(host)
	}
	return net.ParseIP(remoteAddr)
}

func firstHeaderValue(value string) string {
	if value == "" {
		return ""
	}
	if idx := strings.IndexByte(value, ','); idx >= 0 {
		return strings.TrimSpace(value[:idx])
	}
	return strings.TrimSpace(value)
}

func parseForwardedProto(forwarded string) string {
	parts := strings.SplitSeq(forwarded, ",")
	for part := range parts {
		segments := strings.SplitSeq(part, ";")
		for segment := range segments {
			segment = strings.TrimSpace(segment)
			if strings.HasPrefix(strings.ToLower(segment), "proto=") {
				return strings.Trim(strings.TrimPrefix(segment, "proto="), "\"")
			}
		}
	}
	return ""
}

func isHealthPath(path string) bool {
	return path == "/api/health"
}

package middleware

import (
	"context"
	"net"
	"net/http"
	"net/netip"
	"strings"

	"exodus/internal/config"
)

const ExodusRealIPHeader = "X-Exodus-Real-IP"

var clientIPHeaders = [...]string{
	"X-Exodus-Real-Ip",
	"Cf-Connecting-Ip",
	"True-Client-Ip",
	"X-Forwarded-For",
	"X-Real-Ip",
}

type clientIPContextKey struct{}

type clientIPCandidate struct {
	value  string
	addr   netip.Addr
	source string
}

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

// ResolveClientIP retrieves the client IP address from an HTTP request. It
// prefers the Exodus explicit real-IP header, then common proxy/CDN headers,
// then RemoteAddr. If a public address is present in proxy headers, it is
// preferred over private Docker/proxy addresses from the same chain.
func ResolveClientIP(r *http.Request, cfg *config.BackendConfig) string {
	if r == nil {
		return ""
	}

	var candidateBuf [8]clientIPCandidate
	candidates := candidateBuf[:0]

	if r.Header != nil {
		for _, header := range clientIPHeaders {
			values := r.Header[header]
			if len(values) == 0 {
				continue
			}
			for _, value := range values {
				if strings.IndexByte(value, ',') == -1 {
					if candidate, ok := parseIPCandidate(value, header); ok {
						if isPublicClientIP(candidate.addr) {
							if cfg != nil && cfg.Logger != nil && cfg.Logger.IsDebugEnabled() {
								cfg.Logger.Debug("Resolved client IP address", "client_ip", candidate.value, "source", candidate.source, "remote_addr", r.RemoteAddr)
							}
							return candidate.value
						}
						candidates = append(candidates, candidate)
					}
					continue
				}
				remaining := value
				for len(remaining) > 0 {
					var item string
					if idx := strings.IndexByte(remaining, ','); idx >= 0 {
						item = remaining[:idx]
						remaining = remaining[idx+1:]
					} else {
						item = remaining
						remaining = ""
					}
					if candidate, ok := parseIPCandidate(item, header); ok {
						if isPublicClientIP(candidate.addr) {
							if cfg != nil && cfg.Logger != nil && cfg.Logger.IsDebugEnabled() {
								cfg.Logger.Debug("Resolved client IP address", "client_ip", candidate.value, "source", candidate.source, "remote_addr", r.RemoteAddr)
							}
							return candidate.value
						}
						candidates = append(candidates, candidate)
					}
				}
			}
		}
	}

	if candidate, ok := parseIPCandidate(r.RemoteAddr, "RemoteAddr"); ok {
		candidates = append(candidates, candidate)
	}

	selected := selectClientIPCandidate(candidates)
	if selected.value == "" {
		selected.value = "0.0.0.0"
	}
	if cfg != nil && cfg.Logger != nil && cfg.Logger.IsDebugEnabled() {
		cfg.Logger.Debug("Resolved client IP address", "client_ip", selected.value, "source", selected.source, "remote_addr", r.RemoteAddr)
	}
	return selected.value
}

func parseIPCandidate(value, source string) (clientIPCandidate, bool) {
	trimmed := strings.Trim(strings.TrimSpace(value), "\"'")
	if trimmed == "" {
		return clientIPCandidate{}, false
	}

	addr, ok := normalizeIPAddr(trimmed)
	if !ok {
		return clientIPCandidate{}, false
	}
	return clientIPCandidate{value: addr.String(), addr: addr, source: source}, true
}

func normalizeIPAddr(trimmed string) (netip.Addr, bool) {
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

// normalizeIPLiteral strips an optional port, brackets, IPv6 zone suffix,
// and IPv4-mapped ::ffff: prefix from an IP literal and returns its
// canonical net.IP (IPv4 forms are folded to 4-byte form via To4()). It
// performs no DNS resolution: a hostname or other non-IP input returns
// ok=false.
func normalizeIPLiteral(trimmed string) (net.IP, bool) {
	addr, ok := normalizeIPAddr(trimmed)
	if !ok {
		return nil, false
	}
	if addr.Is4() {
		a4 := addr.As4()
		return net.IPv4(a4[0], a4[1], a4[2], a4[3]), true
	}
	a16 := addr.As16()
	ip := make(net.IP, 16)
	copy(ip, a16[:])
	return ip, true
}

// CanonicalIP returns the canonical string form of an IP literal — handling
// an IPv4-mapped ::ffff: prefix, an IPv6 zone suffix, and an optional
// port/brackets — the same way client-IP resolution in this file does.
// Returns "" if the input is not a literal IP address; it never resolves
// DNS. Exported so other packages that need to compare two "is this the
// same address" values (e.g. node-ssh host validation) share one
// normalization implementation instead of re-deriving their own or, worse,
// falling back to a DNS lookup to decide the comparison.
func CanonicalIP(raw string) string {
	addr, ok := normalizeIPAddr(strings.TrimSpace(raw))
	if !ok {
		return ""
	}
	return addr.String()
}

func selectClientIPCandidate(candidates []clientIPCandidate) clientIPCandidate {
	if len(candidates) == 0 {
		return clientIPCandidate{}
	}
	for _, candidate := range candidates {
		if !strings.EqualFold(candidate.source, "RemoteAddr") && isPublicClientIP(candidate.addr) {
			return candidate
		}
	}
	return candidates[0]
}

func isPublicClientIP(addr netip.Addr) bool {
	if !addr.IsValid() {
		return false
	}
	return addr.IsGlobalUnicast() &&
		!addr.IsPrivate() &&
		!addr.IsLoopback() &&
		!addr.IsLinkLocalUnicast() &&
		!addr.IsLinkLocalMulticast() &&
		!addr.IsUnspecified()
}

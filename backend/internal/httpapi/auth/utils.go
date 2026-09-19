package auth

import (
	"context"
	"net"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"exodus/internal/config"
	"exodus/internal/httpapi/middleware"
	"exodus/internal/jobqueue"
	"exodus/internal/notifications"
	"exodus/internal/security"

	"github.com/redis/go-redis/v9"
)

// --- Login brute-force rate limiting -------------------------------------
const (
	AuthRateLimitWindow      = 60 * time.Second
	AuthRateLimitMaxAttempts = 5
	AuthFailedLoginDelay     = 5 * time.Second
)

type AuthRateLimiter struct {
	mu        sync.Mutex
	attempts  map[string][]time.Time
	redis     *redis.Client
	redisOnce sync.Once
}

var globalAuthRateLimiter = &AuthRateLimiter{
	attempts: make(map[string][]time.Time),
}

func (l *AuthRateLimiter) getRedis(cfg *config.BackendConfig) *redis.Client {
	l.redisOnce.Do(func() {
		if cfg != nil {
			client, err := jobqueue.GetSharedRedisClient(cfg)
			if err == nil {
				l.redis = client
			}
		}
	})
	return l.redis
}

func (l *AuthRateLimiter) Allow(ctx context.Context, ip string, cfg *config.BackendConfig) (allowed bool, remaining int, retryAfter time.Duration) {
	if ip == "" {
		return true, AuthRateLimitMaxAttempts, 0
	}

	redisClient := l.getRedis(cfg)
	if redisClient != nil {
		key := "ratelimit:auth:login:" + ip
		val, err := redisClient.Get(ctx, key).Int()
		if err == nil && val >= AuthRateLimitMaxAttempts {
			ttl, _ := redisClient.TTL(ctx, key).Result()
			if ttl <= 0 {
				ttl = AuthRateLimitWindow
			}
			return false, 0, ttl
		}
		if err == nil {
			rem := AuthRateLimitMaxAttempts - val
			if rem < 0 {
				rem = 0
			}
			return true, rem, 0
		}
	}

	// Fallback to in-memory sliding window
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-AuthRateLimitWindow)
	existing := l.attempts[ip]
	kept := existing[:0]
	for _, t := range existing {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	l.attempts[ip] = kept

	if len(kept) >= AuthRateLimitMaxAttempts {
		oldest := kept[0]
		retry := AuthRateLimitWindow - now.Sub(oldest)
		if retry <= 0 {
			retry = time.Second
		}
		return false, 0, retry
	}

	remaining = AuthRateLimitMaxAttempts - len(kept)
	return true, remaining, 0
}

func (l *AuthRateLimiter) RecordFailedAttempt(ctx context.Context, ip string, cfg *config.BackendConfig) {
	if ip == "" {
		return
	}

	redisClient := l.getRedis(cfg)
	if redisClient != nil {
		key := "ratelimit:auth:login:" + ip
		pipe := redisClient.Pipeline()
		pipe.Incr(ctx, key)
		pipe.Expire(ctx, key, AuthRateLimitWindow)
		_, _ = pipe.Exec(ctx)
	}

	// Also record in-memory fallback
	l.mu.Lock()
	defer l.mu.Unlock()
	l.attempts[ip] = append(l.attempts[ip], time.Now())
}

func (l *AuthRateLimiter) Reset(ctx context.Context, ip string, cfg *config.BackendConfig) {
	if ip == "" {
		return
	}

	redisClient := l.getRedis(cfg)
	if redisClient != nil {
		key := "ratelimit:auth:login:" + ip
		_ = redisClient.Del(ctx, key).Err()
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.attempts, ip)
}

func loginRateLimitKey(r *http.Request, cfg *config.BackendConfig) string {
	ip := middleware.GetClientIP(r, cfg)
	if ip != "" {
		return ip
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || host == "" {
		return r.RemoteAddr
	}
	return host
}

// --- Timing Safety -------------------------------------------------------
var (
	dummyPasswordHashOnce sync.Once
	dummyPasswordHashVal  string
)

func getDummyPasswordHash(secret string) string {
	dummyPasswordHashOnce.Do(func() {
		dummyPasswordHashVal = mustDummyPasswordHash(secret)
	})
	return dummyPasswordHashVal
}

func mustDummyPasswordHash(secret string) string {
	hash, err := security.HashPassword("exodus-timing-safety-placeholder-do-not-use", secret)
	if err != nil {
		return "00000000000000000000000000000000:" +
			"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
	}
	return hash
}

// --- Context Principal ---------------------------------------------------
func CurrentAuthPrincipal(ctx context.Context) (*AuthPrincipal, bool) {
	if ctx == nil {
		return nil, false
	}
	val := ctx.Value(authPrincipalContextKey)
	if val == nil {
		return nil, false
	}
	principal, ok := val.(*AuthPrincipal)
	if !ok || principal == nil {
		return nil, false
	}
	return principal, true
}

func WithAuthPrincipal(ctx context.Context, principal *AuthPrincipal) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, authPrincipalContextKey, principal)
}

// --- API Scopes ----------------------------------------------------------
func requireAPITokenScope(principal *AuthPrincipal, r *http.Request, cfg *config.BackendConfig) bool {
	if principal == nil {
		return false
	}
	if principal.TokenType != "jwt_api_token" {
		return true // UI admin sessions bypass API scope validation
	}
	if len(principal.Scopes) == 0 {
		return false
	}
	if hasScope(principal.Scopes, "*") {
		return true
	}

	resource, action, specificEndpointScope := apiTokenScopeForRequest(r.Method, r.URL.Path, cfg)
	if resource == "" {
		return false
	}

	// 1. Resource wildcard check (e.g. "users:*", "users")
	if hasScope(principal.Scopes, resource+":*") || hasScope(principal.Scopes, resource) {
		return true
	}

	// 2. Action scope check (e.g. "users:read", "users:write")
	if action != "" && hasScope(principal.Scopes, resource+":"+action) {
		return true
	}

	// 3. Specific endpoint scope (e.g. "users:list", "users:create", "system:stats", "keygen:read")
	if specificEndpointScope != "" && hasScope(principal.Scopes, specificEndpointScope) {
		return true
	}

	return false
}

func normalizeAPITokenPrincipalScopes(scopes []string) []string {
	seen := make(map[string]struct{}, len(scopes))
	result := make([]string, 0, len(scopes))
	for _, raw := range scopes {
		scope := strings.ToLower(strings.TrimSpace(raw))
		if scope == "" {
			continue
		}
		if _, ok := seen[scope]; ok {
			continue
		}
		seen[scope] = struct{}{}
		result = append(result, scope)
	}
	return result
}

func hasScope(scopes []string, expected string) bool {
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope == "*" || strings.EqualFold(scope, expected) {
			return true
		}
	}
	return false
}

func apiTokenScopeForRequest(method, requestPath string, cfg *config.BackendConfig) (resource, action, specificScope string) {
	clean := cleanPath(requestPath)
	if cfg != nil && cfg.Backend.Trimmed() != "" {
		clean = strings.TrimPrefix(clean, cfg.Backend.Trimmed())
	}
	if idx := strings.Index(clean, "/api/"); idx != -1 {
		clean = clean[idx:]
	}
	if !strings.HasPrefix(clean, "/api/") {
		return "", "", ""
	}
	trimmed := strings.TrimPrefix(clean, "/api/")

	isRead := method == http.MethodGet || method == http.MethodHead
	action = "write"
	if isRead {
		action = "read"
	}

	switch {
	case strings.HasPrefix(trimmed, "users"):
		resource = "users"
		if isRead {
			if trimmed == "users" || trimmed == "users/" {
				specificScope = "users:list"
			} else {
				specificScope = "users:get"
			}
		} else {
			if method == http.MethodPost && (trimmed == "users" || trimmed == "users/") {
				specificScope = "users:create"
			} else if method == http.MethodDelete {
				specificScope = "users:delete"
			} else {
				specificScope = "users:update"
			}
		}

	case strings.HasPrefix(trimmed, "nodes"):
		resource = "nodes"
		if isRead {
			if trimmed == "nodes" || trimmed == "nodes/" {
				specificScope = "nodes:list"
			} else {
				specificScope = "nodes:get"
			}
		} else {
			if method == http.MethodPost && (trimmed == "nodes" || trimmed == "nodes/") {
				specificScope = "nodes:create"
			} else if method == http.MethodDelete {
				specificScope = "nodes:delete"
			} else {
				specificScope = "nodes:update"
			}
		}

	case strings.HasPrefix(trimmed, "hosts"):
		resource = "hosts"
		if isRead {
			if trimmed == "hosts" || trimmed == "hosts/" {
				specificScope = "hosts:list"
			} else {
				specificScope = "hosts:get"
			}
		} else {
			if method == http.MethodPost && (trimmed == "hosts" || trimmed == "hosts/") {
				specificScope = "hosts:create"
			} else if method == http.MethodDelete {
				specificScope = "hosts:delete"
			} else {
				specificScope = "hosts:update"
			}
		}

	case strings.HasPrefix(trimmed, "subscription-connections"):
		resource = "subscription-connections"
		if isRead {
			if trimmed == "subscription-connections" || trimmed == "subscription-connections/" {
				specificScope = "subscription-connections:list"
			} else {
				specificScope = "subscription-connections:get"
			}
		} else {
			if method == http.MethodPost && (trimmed == "subscription-connections" || trimmed == "subscription-connections/") {
				specificScope = "subscription-connections:create"
			} else if method == http.MethodDelete {
				specificScope = "subscription-connections:delete"
			} else {
				specificScope = "subscription-connections:update"
			}
		}

	case strings.HasPrefix(trimmed, "config-profiles") || strings.HasPrefix(trimmed, "snippets"):
		resource = "config-profiles"
		if isRead {
			specificScope = "config-profiles:read"
		} else {
			specificScope = "config-profiles:write"
		}

	case strings.HasPrefix(trimmed, "subscription-page-configs"):
		resource = "subscription-page-configs"
		if isRead {
			if trimmed == "subscription-page-configs" || trimmed == "subscription-page-configs/" {
				specificScope = "subscription-page-configs:list"
			} else {
				specificScope = "subscription-page-configs:get"
			}
		} else {
			if method == http.MethodPost && (trimmed == "subscription-page-configs" || trimmed == "subscription-page-configs/") {
				specificScope = "subscription-page-configs:create"
			} else if method == http.MethodDelete {
				specificScope = "subscription-page-configs:delete"
			} else {
				specificScope = "subscription-page-configs:update"
			}
		}

	case strings.HasPrefix(trimmed, "subscriptions"):
		resource = "subscriptions"
		if isRead {
			specificScope = "subscriptions:read"
		} else {
			specificScope = "subscriptions:write"
		}

	case strings.HasPrefix(trimmed, "subscription-settings"):
		resource = "subscription-settings"
		if isRead {
			specificScope = "subscription-settings:read"
		} else {
			specificScope = "subscription-settings:write"
		}

	case strings.HasPrefix(trimmed, "subscription-templates"):
		resource = "subscription-template"
		if isRead {
			if trimmed == "subscription-templates" || trimmed == "subscription-templates/" {
				specificScope = "subscription-template:list"
			} else {
				specificScope = "subscription-template:get"
			}
		} else {
			if method == http.MethodPost && (trimmed == "subscription-templates" || trimmed == "subscription-templates/") {
				specificScope = "subscription-template:create"
			} else if method == http.MethodDelete {
				specificScope = "subscription-template:delete"
			} else {
				specificScope = "subscription-template:update"
			}
		}

	case strings.HasPrefix(trimmed, "subscription-request-history"):
		resource = "subscription-request-history"
		specificScope = "subscription-request-history:read"

	case strings.HasPrefix(trimmed, "metadata"):
		resource = "metadata"
		if isRead {
			if strings.HasPrefix(trimmed, "metadata/user") {
				specificScope = "metadata:get-user"
			} else {
				specificScope = "metadata:get-node"
			}
		} else {
			if strings.HasPrefix(trimmed, "metadata/user") {
				specificScope = "metadata:upsert-user"
			} else {
				specificScope = "metadata:upsert-node"
			}
		}

	case strings.HasPrefix(trimmed, "node-plugins"):
		resource = "node-plugins"
		if isRead {
			specificScope = "node-plugins:read"
		} else {
			specificScope = "node-plugins:write"
		}

	case strings.HasPrefix(trimmed, "hwid"):
		resource = "hwid-user-devices"
		if isRead {
			specificScope = "hwid-user-devices:read"
		} else {
			specificScope = "hwid-user-devices:write"
		}

	case strings.HasPrefix(trimmed, "bandwidth-stats"):
		resource = "bandwidth-stats"
		specificScope = "bandwidth-stats:read"

	case strings.HasPrefix(trimmed, "srs-lists"):
		resource = "srs-lists"
		if isRead {
			specificScope = "srs-lists:read"
		} else {
			specificScope = "srs-lists:write"
		}

	case strings.HasPrefix(trimmed, "internal-squads"):
		resource = "internal-squads"
		if isRead {
			specificScope = "internal-squads:read"
		} else {
			specificScope = "internal-squads:write"
		}

	case strings.HasPrefix(trimmed, "external-squads"):
		resource = "external-squads"
		if isRead {
			specificScope = "external-squads:read"
		} else {
			specificScope = "external-squads:write"
		}

	case strings.HasPrefix(trimmed, "infra-billing"):
		resource = "infra-billing"
		if isRead {
			specificScope = "infra-billing:read"
		} else {
			specificScope = "infra-billing:write"
		}

	case strings.HasPrefix(trimmed, "keygen"):
		resource = "keygen"
		specificScope = "keygen:read"

	case strings.HasPrefix(trimmed, "passkeys"):
		resource = "passkeys"
		if isRead {
			specificScope = "passkeys:read"
		} else {
			specificScope = "passkeys:write"
		}

	case strings.HasPrefix(trimmed, "connections"):
		resource = "connections"
		if isRead {
			specificScope = "connections:read"
		} else {
			specificScope = "connections:write"
		}

	case strings.HasPrefix(trimmed, "system"):
		resource = "system"
		if isRead {
			specificScope = "system:stats"
		} else {
			specificScope = "system:write"
		}

	case strings.HasPrefix(trimmed, "tokens"), strings.HasPrefix(trimmed, "exodus-settings"):
		resource = "tokens"
		if isRead {
			specificScope = "tokens:read"
		} else {
			specificScope = "tokens:write"
		}

	default:
		parts := strings.Split(trimmed, "/")
		if len(parts) > 0 && parts[0] != "" {
			resource = parts[0]
			specificScope = resource + ":" + action
		}
	}

	return resource, action, specificScope
}

// --- Notifications & Emitters ---------------------------------------------
func emitLoginNotification(ctx context.Context, cfg *config.BackendConfig, event, username, adminUUID, password, reason string, r *http.Request) {
	var (
		clientIP  = notificationClientIP(cfg, r)
		userAgent = strings.TrimSpace(r.Header.Get("User-Agent"))
	)

	notifications.Emit(ctx, cfg, notifications.Event{
		Scope: notifications.ScopeService,
		Event: event,
		Data: map[string]any{
			"username":  username,
			"adminUuid": adminUUID,
			"password":  password,
			"ip":        clientIP,
			"userAgent": userAgent,
			"reason":    reason,
		},
	})
}

func notificationClientIP(cfg *config.BackendConfig, r *http.Request) string {
	return middleware.GetClientIP(r, cfg)
}

func cleanPath(rawPath string) string {
	cleaned := path.Clean(strings.TrimSpace(rawPath))
	if cleaned == "." {
		return "/"
	}
	return cleaned
}

func readBearerToken(authHeader string) string {
	trimmed := strings.TrimSpace(authHeader)
	if trimmed == "" {
		return ""
	}

	parts := strings.SplitN(trimmed, " ", 2)
	if len(parts) == 2 && strings.EqualFold(parts[0], "bearer") {
		return strings.TrimSpace(parts[1])
	}

	return ""
}

func AuthTTLMinutes() int {
	return 2880 // 48 hours
}

func nullableString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

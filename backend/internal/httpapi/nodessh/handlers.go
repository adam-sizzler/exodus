package nodessh

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"exodus/internal/config"
	"exodus/internal/httpapi/auth"
	"exodus/internal/httpapi/middleware"
	"exodus/internal/httpapi/shared"
	"exodus/internal/jobqueue"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// ─── Ticket store ──────────────────────────────────────────────────────────────

// TicketInfo holds data associated with a one-time SSH session ticket.
type TicketInfo struct {
	AdminUUID string
	NodeUUID  string
	ClientIP  string    // #3: IP bound at ticket issuance — must match on WS upgrade
	ExpiresAt time.Time
}

var (
	ticketLock sync.RWMutex
	ticketMap  = make(map[string]TicketInfo)
)

func storeTicket(ticket, adminUUID, nodeUUID, clientIP string, ttl time.Duration) {
	ticketLock.Lock()
	defer ticketLock.Unlock()
	now := time.Now()
	// Inline sweep of expired tickets to prevent map accumulation
	for k, v := range ticketMap {
		if now.After(v.ExpiresAt) {
			delete(ticketMap, k)
		}
	}
	ticketMap[ticket] = TicketInfo{
		AdminUUID: adminUUID,
		NodeUUID:  nodeUUID,
		ClientIP:  clientIP,
		ExpiresAt: now.Add(ttl),
	}
}

// consumeTicket atomically removes and validates the ticket.
// #3: clientIP must match the IP that originally requested the ticket.
func consumeTicket(ticket, clientIP string) (TicketInfo, bool) {
	ticketLock.Lock()
	defer ticketLock.Unlock()
	info, ok := ticketMap[ticket]
	if !ok {
		return TicketInfo{}, false
	}
	delete(ticketMap, ticket)
	if time.Now().After(info.ExpiresAt) {
		return TicketInfo{}, false
	}
	// #3: Reject if stored IP doesn't match request IP.
	if info.ClientIP == "" || info.ClientIP != clientIP {
		return TicketInfo{}, false
	}
	return info, true
}

// ─── UUID extraction helper ─────────────────────────────────────────────────

func extractNodeUUID(path string) string {
	path = strings.Trim(path, "/")
	// Case 1: /api/node-ssh/{uuid}/ticket
	if strings.HasSuffix(path, "/ticket") {
		trimmed := strings.TrimSuffix(path, "/ticket")
		parts := strings.Split(trimmed, "/")
		if len(parts) > 0 {
			return parts[len(parts)-1]
		}
	}
	// Case 2: /api/node-ssh/tickets/{uuid}
	if strings.Contains(path, "tickets/") {
		parts := strings.Split(path, "tickets/")
		if len(parts) > 1 {
			return strings.Trim(parts[1], "/")
		}
	}
	// Case 3: bare uuid
	parts := strings.Split(path, "/")
	if len(parts) > 0 {
		last := parts[len(parts)-1]
		if _, err := uuid.Parse(last); err == nil {
			return last
		}
	}
	return ""
}

// ─── Ticket handler ─────────────────────────────────────────────────────────

// NodeSSHTicketHandler issues a one-time 15-second ticket for SSH web terminal.
// POST /api/node-ssh/:uuid/ticket  or  /api/node-ssh/tickets/:uuid
func NodeSSHTicketHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return auth.RequireAdminRole(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		nodeUUID := extractNodeUUID(r.URL.Path)
		if nodeUUID == "" {
			var body struct {
				NodeUUID string `json:"nodeUuid"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			nodeUUID = body.NodeUUID
		}

		if _, err := uuid.Parse(nodeUUID); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid node UUID format", nil, cfg)
			return
		}

		// Verify node exists
		var exists int
		err := db.QueryRow(r.Context(), `SELECT 1 FROM nodes WHERE uuid = $1`, nodeUUID).Scan(&exists)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				shared.SendAPIError(w, shared.ErrNodeNotFound, cfg)
				return
			}
			shared.SendError(w, http.StatusInternalServerError, "failed to query node", err, cfg)
			return
		}

		// Generate random 32-byte url-safe ticket
		buf := make([]byte, 32)
		if _, err := rand.Read(buf); err != nil {
			shared.SendError(w, http.StatusInternalServerError, "failed to generate ticket", err, cfg)
			return
		}
		ticket := base64.RawURLEncoding.EncodeToString(buf)

		adminUUID := ""
		if principal, ok := auth.CurrentAuthPrincipal(r.Context()); ok && principal != nil {
			adminUUID = principal.AdminUUID
		}

		// #3: Capture client IP to bind to the ticket
		clientIP := middleware.GetClientIP(r, cfg)
		if clientIP == "" {
			host, _, splitErr := net.SplitHostPort(r.RemoteAddr)
			if splitErr == nil && host != "" {
				clientIP = host
			} else {
				clientIP = r.RemoteAddr
			}
		}

		ttl := 15 * time.Second
		storeTicket(ticket, adminUUID, nodeUUID, clientIP, ttl)

		shared.WriteJSON(w, http.StatusCreated, map[string]any{
			"response": CreateSshTicketResponse{
				Ticket:           ticket,
				Path:             "/api/node-ssh/ws",
				ExpiresInSeconds: 15,
			},
		})
	})
}

// ─── Vault rate-limiter (#4) ────────────────────────────────────────────────

const (
	vaultRateLimitWindow      = 15 * time.Minute
	vaultRateLimitMaxAttempts = 10
)

type vaultRateLimiter struct {
	mu       sync.Mutex
	attempts map[string][]time.Time // in-memory sliding-window fallback
	redis    *redis.Client
	once     sync.Once
}

var globalVaultRateLimiter = &vaultRateLimiter{
	attempts: make(map[string][]time.Time),
}

func (l *vaultRateLimiter) getRedis(cfg *config.BackendConfig) *redis.Client {
	l.once.Do(func() {
		if cfg != nil {
			client, err := jobqueue.NewRedisClient(cfg)
			if err == nil {
				l.redis = client
			}
		}
	})
	return l.redis
}

// allow checks (and records) an OPRF evaluate attempt for adminUUID.
// Returns (true, 0) if allowed, (false, retryAfter) if blocked.
func (l *vaultRateLimiter) allow(ctx context.Context, adminUUID string, cfg *config.BackendConfig) (bool, time.Duration) {
	if adminUUID == "" {
		return true, 0
	}

	rc := l.getRedis(cfg)
	if rc != nil {
		key := "ratelimit:vault:eval:" + adminUUID
		pipe := rc.Pipeline()
		incrCmd := pipe.Incr(ctx, key)
		pipe.Expire(ctx, key, vaultRateLimitWindow)
		_, _ = pipe.Exec(ctx)
		count, err := incrCmd.Result()
		if err == nil {
			if count > int64(vaultRateLimitMaxAttempts) {
				ttl, _ := rc.TTL(ctx, key).Result()
				if ttl <= 0 {
					ttl = vaultRateLimitWindow
				}
				return false, ttl
			}
			return true, 0
		}
		// Redis error — fall through to in-memory
	}

	// In-memory sliding-window fallback
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-vaultRateLimitWindow)
	// Sweep inactive admins
	for k, att := range l.attempts {
		if len(att) > 0 && att[len(att)-1].Before(cutoff) {
			delete(l.attempts, k)
		}
	}
	existing := l.attempts[adminUUID]
	kept := existing[:0]
	for _, t := range existing {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	kept = append(kept, now)
	l.attempts[adminUUID] = kept

	if len(kept) > vaultRateLimitMaxAttempts {
		oldest := kept[0]
		retryAfter := vaultRateLimitWindow - now.Sub(oldest)
		if retryAfter <= 0 {
			retryAfter = time.Second
		}
		return false, retryAfter
	}
	return true, 0
}

// ─── Vault evaluate handler ─────────────────────────────────────────────────

// NodeSSHVaultEvaluateHandler handles OPRF evaluation of blinded elements.
// POST /api/node-ssh/vault/evaluate
func NodeSSHVaultEvaluateHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return auth.RequireAdminRole(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		// #4: Rate-limit per admin UUID (10 attempts / 15 minutes)
		adminUUID := ""
		if principal, ok := auth.CurrentAuthPrincipal(r.Context()); ok && principal != nil {
			adminUUID = principal.AdminUUID
		}
		if allowed, retryAfter := globalVaultRateLimiter.allow(r.Context(), adminUUID, cfg); !allowed {
			secs := int(retryAfter.Seconds())
			if secs <= 0 {
				secs = 1
			}
			w.Header().Set("Retry-After", fmt.Sprintf("%d", secs))
			w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", vaultRateLimitMaxAttempts))
			w.Header().Set("X-RateLimit-Remaining", "0")
			shared.WriteJSONError(w, http.StatusTooManyRequests, "vault evaluate rate limit exceeded")
			return
		}

		var req EvaluateVaultRequestBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
			return
		}

		if strings.TrimSpace(req.Blinded) == "" {
			shared.SendError(w, http.StatusBadRequest, "blinded cannot be empty", nil, cfg)
			return
		}

		// #47: Fail explicitly if APP_SECRET is not configured — no silent fallback.
		secret := cfg.JWT.AuthSecret
		if strings.TrimSpace(secret) == "" {
			shared.SendError(w, http.StatusInternalServerError, "vault secret not configured", nil, cfg)
			return
		}

		evaluated, err := EvaluateBlindedElement(secret, req.Blinded)
		if err != nil {
			shared.SendError(w, http.StatusBadRequest, "vault evaluation failed", err, cfg)
			return
		}

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": EvaluateVaultResponse{
				Evaluated: evaluated,
			},
		})
	})
}

// ─── Dispatcher ─────────────────────────────────────────────────────────────

// NodeSSHDispatcherHandler dispatches all /api/node-ssh/ requests.
func NodeSSHDispatcherHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	ticketHandler := NodeSSHTicketHandler(db, cfg)
	vaultHandler := NodeSSHVaultEvaluateHandler(db, cfg)
	wsHandler := NodeSSHWSHandler(db, cfg)

	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if cfg != nil && cfg.Backend.IsCustom() {
			path = strings.TrimPrefix(path, cfg.Backend.Trimmed())
		}

		if path == "/api/node-ssh/vault/evaluate" {
			vaultHandler.ServeHTTP(w, r)
			return
		}
		if path == "/api/node-ssh/ws" {
			wsHandler.ServeHTTP(w, r)
			return
		}
		if strings.HasSuffix(path, "/ticket") || strings.Contains(path, "/tickets") {
			ticketHandler.ServeHTTP(w, r)
			return
		}

		shared.WriteJSONError(w, http.StatusNotFound, "route not found")
	}
}

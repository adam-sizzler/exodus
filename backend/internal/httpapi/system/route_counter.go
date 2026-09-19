package system

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"exodus/internal/config"

	"github.com/redis/go-redis/v9"
)

const (
	redisRouteCounterKey = "route_counter:stats"
	flushIntervalMs      = 5 * time.Second
	maxRouteSlots        = 512
)

type RouteStatItem struct {
	Method string `json:"method"`
	Route  string `json:"route"`
	Count  int64  `json:"count"`
}

type RouteStatsResponse struct {
	Routes []RouteStatItem `json:"routes"`
	Total  int64           `json:"total"`
}

type routePairKey struct {
	method string
	route  string
}

type RouteCounter struct {
	client     *redis.Client
	cfg        *config.BackendConfig
	mu         sync.RWMutex
	keys       []string
	counts     [maxRouteSlots]int64
	slotByKey  map[string]int
	slotByPair map[routePairKey]int
	slotCache  sync.Map
	isFlushing atomic.Bool
	stopCh     chan struct{}
}

func NewRouteCounter(client *redis.Client, cfg *config.BackendConfig) *RouteCounter {
	return &RouteCounter{
		client:     client,
		cfg:        cfg,
		keys:       make([]string, 0, 64),
		slotByKey:  make(map[string]int, 64),
		slotByPair: make(map[routePairKey]int, 64),
		stopCh:     make(chan struct{}),
	}
}

func (rc *RouteCounter) Start(ctx context.Context) {
	ticker := time.NewTicker(flushIntervalMs)
	go func() {
		for {
			select {
			case <-ticker.C:
				rc.flush(context.Background())
			case <-rc.stopCh:
				ticker.Stop()
				rc.flush(context.Background())
				return
			case <-ctx.Done():
				ticker.Stop()
				rc.flush(context.Background())
				return
			}
		}
	}()
}

func (rc *RouteCounter) Stop() {
	close(rc.stopCh)
}

func (rc *RouteCounter) Register(key string) int {
	if val, ok := rc.slotCache.Load(key); ok {
		return val.(int)
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()
	slot, exists := rc.slotByKey[key]
	if exists {
		rc.slotCache.Store(key, slot)
		return slot
	}

	slot = len(rc.keys)
	if slot >= maxRouteSlots {
		return maxRouteSlots - 1
	}
	rc.keys = append(rc.keys, key)
	rc.slotByKey[key] = slot
	rc.slotCache.Store(key, slot)

	spaceIdx := strings.IndexByte(key, ' ')
	if spaceIdx != -1 {
		rc.slotByPair[routePairKey{method: key[:spaceIdx], route: key[spaceIdx+1:]}] = slot
	}

	return slot
}

func (rc *RouteCounter) Increment(key string) {
	slot := rc.Register(key)
	if slot < maxRouteSlots {
		atomic.AddInt64(&rc.counts[slot], 1)
	}
}

func (rc *RouteCounter) IncrementRoute(method, route string) {
	pair := routePairKey{method: method, route: route}
	rc.mu.RLock()
	slot, ok := rc.slotByPair[pair]
	rc.mu.RUnlock()

	if ok {
		if slot < maxRouteSlots {
			atomic.AddInt64(&rc.counts[slot], 1)
		}
		return
	}

	key := method + " " + route
	rc.mu.Lock()
	slot, exists := rc.slotByKey[key]
	if !exists {
		slot = len(rc.keys)
		if slot < maxRouteSlots {
			rc.keys = append(rc.keys, key)
			rc.slotByKey[key] = slot
			rc.slotByPair[pair] = slot
			rc.slotCache.Store(key, slot)
		}
	} else {
		rc.slotByPair[pair] = slot
	}
	rc.mu.Unlock()

	if slot < maxRouteSlots {
		atomic.AddInt64(&rc.counts[slot], 1)
	}
}

func (rc *RouteCounter) GetStats(ctx context.Context) RouteStatsResponse {
	if rc.client != nil {
		hash, err := rc.client.HGetAll(ctx, redisRouteCounterKey).Result()
		if err == nil && len(hash) > 0 {
			var total int64
			routes := make([]RouteStatItem, 0, len(hash))
			for key, valStr := range hash {
				count, parseErr := strconv.ParseInt(valStr, 10, 64)
				if parseErr != nil {
					continue
				}
				spaceIdx := strings.IndexByte(key, ' ')
				var method, route string
				if spaceIdx == -1 {
					method = ""
					route = key
				} else {
					method = key[:spaceIdx]
					route = key[spaceIdx+1:]
				}
				routes = append(routes, RouteStatItem{
					Method: method,
					Route:  route,
					Count:  count,
				})
				total += count
			}

			sort.Slice(routes, func(i, j int) bool {
				return routes[i].Count > routes[j].Count
			})

			return RouteStatsResponse{
				Routes: routes,
				Total:  total,
			}
		}
	}

	rc.mu.RLock()
	defer rc.mu.RUnlock()

	var total int64
	routes := make([]RouteStatItem, 0, len(rc.keys))
	for i, key := range rc.keys {
		count := atomic.LoadInt64(&rc.counts[i])
		if count > 0 {
			spaceIdx := strings.IndexByte(key, ' ')
			var method, route string
			if spaceIdx == -1 {
				method = ""
				route = key
			} else {
				method = key[:spaceIdx]
				route = key[spaceIdx+1:]
			}
			routes = append(routes, RouteStatItem{
				Method: method,
				Route:  route,
				Count:  count,
			})
			total += count
		}
	}

	sort.Slice(routes, func(i, j int) bool {
		return routes[i].Count > routes[j].Count
	})

	return RouteStatsResponse{
		Routes: routes,
		Total:  total,
	}
}

func (rc *RouteCounter) flush(ctx context.Context) {
	if rc.client == nil {
		return
	}
	if !rc.isFlushing.CompareAndSwap(false, true) {
		return
	}
	defer rc.isFlushing.Store(false)

	type drainedItem struct {
		slot  int
		field string
		delta int64
	}

	rc.mu.RLock()
	numSlots := len(rc.keys)
	drained := make([]drainedItem, 0, numSlots)
	for i := 0; i < numSlots; i++ {
		delta := atomic.SwapInt64(&rc.counts[i], 0)
		if delta > 0 {
			drained = append(drained, drainedItem{slot: i, field: rc.keys[i], delta: delta})
		}
	}
	rc.mu.RUnlock()

	if len(drained) == 0 {
		return
	}

	pipe := rc.client.Pipeline()
	for _, item := range drained {
		pipe.HIncrBy(ctx, redisRouteCounterKey, item.field, item.delta)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		for _, item := range drained {
			atomic.AddInt64(&rc.counts[item.slot], item.delta)
		}
		if rc.cfg != nil && rc.cfg.Logger != nil {
			rc.cfg.Logger.Warn("Failed to flush route counters to Redis", "error", err)
		}
	}
}

// NormalizeRoutePattern maps raw request URL paths to bounded route patterns matching upstream.
// All return values are string constants, ensuring zero heap allocations on hot-path.
func NormalizeRoutePattern(path string) string {
	// Exact matches first for static routes
	switch path {
	// Auth
	case "/api/auth/bootstrap":
		return "/api/auth/bootstrap"
	case "/api/auth/setup":
		return "/api/auth/setup"
	case "/api/auth/status":
		return "/api/auth/status"
	case "/api/auth/register":
		return "/api/auth/register"
	case "/api/auth/login":
		return "/api/auth/login"
	case "/api/auth/logout":
		return "/api/auth/logout"
	case "/api/auth/me":
		return "/api/auth/me"
	case "/api/auth/oauth2/authorize":
		return "/api/auth/oauth2/authorize"
	case "/api/auth/oauth2/callback":
		return "/api/auth/oauth2/callback"
	case "/api/auth/passkey/authentication/options":
		return "/api/auth/passkey/authentication/options"
	case "/api/auth/passkey/authentication/verify":
		return "/api/auth/passkey/authentication/verify"

	// Settings & Tokens
	case "/api/exodus-settings", "/api/exodus-settings/":
		return "/api/exodus-settings"
	case "/api/tokens":
		return "/api/tokens"
	case "/api/tokens/scopes":
		return "/api/tokens/scopes"
	case "/api/tokens/ott":
		return "/api/tokens/ott"

	// Nodes
	case "/api/nodes":
		return "/api/nodes"
	case "/api/nodes/tags":
		return "/api/nodes/tags"
	case "/api/nodes/bulk-actions", "/api/nodes/bulk-actions/":
		return "/api/nodes/bulk-actions"

	// Node Plugins & Integrations
	case "/api/node-plugins":
		return "/api/node-plugins"
	case "/api/node-plugins/tags":
		return "/api/node-plugins/tags"
	case "/api/node-integrations":
		return "/api/node-integrations"

	// Node SSH
	case "/api/node-ssh":
		return "/api/node-ssh"
	case "/api/node-ssh/ws":
		return "/api/node-ssh/ws"
	case "/api/node-ssh/tickets":
		return "/api/node-ssh/tickets"
	case "/api/node-ssh/vault/evaluate":
		return "/api/node-ssh/vault/evaluate"

	// Subscription Connections
	case "/api/subscription-connections":
		return "/api/subscription-connections"
	case "/api/subscription-connections/tags":
		return "/api/subscription-connections/tags"
	case "/api/subscription-connections/bulk-actions", "/api/subscription-connections/bulk-actions/":
		return "/api/subscription-connections/bulk-actions"

	// Hosts
	case "/api/hosts":
		return "/api/hosts"
	case "/api/hosts/tags":
		return "/api/hosts/tags"

	// Users
	case "/api/users":
		return "/api/users"
	case "/api/users/tags":
		return "/api/users/tags"

	// Keygen & Passkeys
	case "/api/keygen":
		return "/api/keygen"
	case "/api/passkeys":
		return "/api/passkeys"
	case "/api/passkeys/registration/options":
		return "/api/passkeys/registration/options"
	case "/api/passkeys/registration/verify":
		return "/api/passkeys/registration/verify"

	// Bandwidth Stats
	case "/api/bandwidth-stats/nodes":
		return "/api/bandwidth-stats/nodes"
	case "/api/bandwidth-stats/users":
		return "/api/bandwidth-stats/users"

	// Config Profiles & Snippets
	case "/api/config-profiles":
		return "/api/config-profiles"
	case "/api/config-profiles/tags":
		return "/api/config-profiles/tags"
	case "/api/config-profiles/inbounds":
		return "/api/config-profiles/inbounds"
	case "/api/snippets":
		return "/api/snippets"

	// Squads
	case "/api/internal-squads":
		return "/api/internal-squads"
	case "/api/internal-squads/tags":
		return "/api/internal-squads/tags"
	case "/api/internal-squads/actions/reorder":
		return "/api/internal-squads/actions/reorder"
	case "/api/external-squads":
		return "/api/external-squads"
	case "/api/external-squads/tags":
		return "/api/external-squads/tags"
	case "/api/external-squads/actions/reorder":
		return "/api/external-squads/actions/reorder"

	// SRS Lists (User Custom)
	case "/api/srs-lists":
		return "/api/srs-lists"
	case "/api/srs-lists/tags":
		return "/api/srs-lists/tags"

	// HWID Devices
	case "/api/hwid/devices":
		return "/api/hwid/devices"
	case "/api/hwid/devices/delete":
		return "/api/hwid/devices/delete"
	case "/api/hwid/devices/delete-all":
		return "/api/hwid/devices/delete-all"
	case "/api/hwid/devices/stats":
		return "/api/hwid/devices/stats"
	case "/api/hwid/devices/top-users":
		return "/api/hwid/devices/top-users"

	// Subscription Settings & Templates
	case "/api/subscription-settings":
		return "/api/subscription-settings"
	case "/api/subscription-templates":
		return "/api/subscription-templates"
	case "/api/subscription-templates/tags":
		return "/api/subscription-templates/tags"

	// Infra Billing
	case "/api/infra-billing/providers":
		return "/api/infra-billing/providers"
	case "/api/infra-billing/nodes":
		return "/api/infra-billing/nodes"
	case "/api/infra-billing/history":
		return "/api/infra-billing/history"

	// Subscriptions & Requests
	case "/api/subscriptions":
		return "/api/subscriptions"
	case "/api/subscriptions/subpage-config":
		return "/api/subscriptions/subpage-config"
	case "/api/subscription-page-configs":
		return "/api/subscription-page-configs"
	case "/api/subscription-page-configs/tags":
		return "/api/subscription-page-configs/tags"
	case "/api/subscription-request-history":
		return "/api/subscription-request-history"
	case "/api/subscription-request-history/stats":
		return "/api/subscription-request-history/stats"

	// System
	case "/api/system/stats":
		return "/api/system/stats"
	case "/api/system/configuration":
		return "/api/system/configuration"
	case "/api/system/metadata":
		return "/api/system/metadata"
	case "/api/system/health":
		return "/api/system/health"
	case "/api/system/stats/bandwidth":
		return "/api/system/stats/bandwidth"
	case "/api/system/stats/nodes":
		return "/api/system/stats/nodes"
	case "/api/system/stats/recap":
		return "/api/system/stats/recap"
	case "/api/system/stats/http":
		return "/api/system/stats/http"
	case "/api/system/stats/digest":
		return "/api/system/stats/digest"
	case "/api/system/nodes/metrics":
		return "/api/system/nodes/metrics"
	case "/api/system/testers/srr-matcher":
		return "/api/system/testers/srr-matcher"
	case "/api/system/tools/x25519/generate":
		return "/api/system/tools/x25519/generate"
	case "/api/sub":
		return "/api/sub"
	case "/api/health":
		return "/api/health"
	}

	// Parameterized prefix matching
	switch {
	// Tokens
	case strings.HasPrefix(path, "/api/tokens/"):
		return "/api/tokens/:uuid"

	// Nodes
	case strings.HasPrefix(path, "/api/nodes/actions/"):
		return "/api/nodes/actions/:action"
	case strings.HasPrefix(path, "/api/nodes/"):
		return "/api/nodes/:uuid"

	// Node Plugins & Integrations
	case strings.HasPrefix(path, "/api/node-plugins/"):
		return "/api/node-plugins/:uuid"
	case strings.HasPrefix(path, "/api/node-integrations/"):
		return "/api/node-integrations/:uuid"

	// Connections
	case strings.HasPrefix(path, "/api/connections/"):
		return "/api/connections/:uuid"

	// Node SSH
	case strings.HasPrefix(path, "/api/node-ssh/tickets/"):
		return "/api/node-ssh/tickets/:uuid"
	case strings.HasPrefix(path, "/api/node-ssh/"):
		return "/api/node-ssh/:other"

	// Metadata
	case strings.HasPrefix(path, "/api/metadata/user/"):
		return "/api/metadata/user/:uuid"
	case strings.HasPrefix(path, "/api/metadata/node/"):
		return "/api/metadata/node/:uuid"

	// Subscription Connections
	case strings.HasPrefix(path, "/api/subscription-connections/actions/"):
		return "/api/subscription-connections/actions/:action"
	case strings.HasPrefix(path, "/api/subscription-connections/"):
		return "/api/subscription-connections/:uuid"

	// Hosts
	case strings.HasPrefix(path, "/api/hosts/actions/"):
		return "/api/hosts/actions/:action"
	case strings.HasPrefix(path, "/api/hosts/bulk/"):
		return "/api/hosts/bulk/:action"
	case strings.HasPrefix(path, "/api/hosts/"):
		return "/api/hosts/:uuid"

	// Users
	case strings.HasPrefix(path, "/api/users/bulk/"):
		return "/api/users/bulk/:action"
	case strings.HasPrefix(path, "/api/users/"):
		return "/api/users/:uuid"

	// Keygen & Passkeys
	case strings.HasPrefix(path, "/api/keygen/"):
		return "/api/keygen/:uuid"
	case strings.HasPrefix(path, "/api/passkeys/"):
		return "/api/passkeys/:uuid"

	// Bandwidth Stats
	case strings.HasPrefix(path, "/api/bandwidth-stats/nodes/"):
		return "/api/bandwidth-stats/nodes/:uuid"
	case strings.HasPrefix(path, "/api/bandwidth-stats/users/"):
		return "/api/bandwidth-stats/users/:uuid"
	case strings.HasPrefix(path, "/api/bandwidth-stats/internal-squads/"):
		return "/api/bandwidth-stats/internal-squads/:uuid"

	// Config Profiles & Snippets
	case strings.HasPrefix(path, "/api/config-profiles/actions/"):
		return "/api/config-profiles/actions/:action"
	case strings.HasPrefix(path, "/api/config-profiles/"):
		return "/api/config-profiles/:uuid"
	case strings.HasPrefix(path, "/api/snippets/"):
		return "/api/snippets/:uuid"

	// Squads
	case strings.HasPrefix(path, "/api/internal-squads/"):
		return "/api/internal-squads/:uuid"
	case strings.HasPrefix(path, "/api/external-squads/"):
		return "/api/external-squads/:uuid"

	// SRS Lists (User Custom)
	case strings.HasPrefix(path, "/api/srs-lists/actions/"):
		return "/api/srs-lists/actions/:action"
	case strings.HasPrefix(path, "/api/srs-lists/bulk/"):
		return "/api/srs-lists/bulk/:action"
	case strings.HasPrefix(path, "/api/srs-lists/"):
		return "/api/srs-lists/:uuid"

	// HWID Devices
	case strings.HasPrefix(path, "/api/hwid/devices/"):
		return "/api/hwid/devices/:uuid"

	// Subscription Settings & Templates
	case strings.HasPrefix(path, "/api/subscription-settings/"):
		return "/api/subscription-settings/:uuid"
	case strings.HasPrefix(path, "/api/subscription-templates/actions/"):
		return "/api/subscription-templates/actions/:action"
	case strings.HasPrefix(path, "/api/subscription-templates/"):
		return "/api/subscription-templates/:uuid"

	// Infra Billing
	case strings.HasPrefix(path, "/api/infra-billing/providers/"):
		return "/api/infra-billing/providers/:uuid"
	case strings.HasPrefix(path, "/api/infra-billing/nodes/"):
		return "/api/infra-billing/nodes/:uuid"
	case strings.HasPrefix(path, "/api/infra-billing/history/"):
		return "/api/infra-billing/history/:uuid"

	// Subscriptions
	case strings.HasPrefix(path, "/api/subscriptions/connection-keys/"):
		return "/api/subscriptions/connection-keys/:uuid"
	case strings.HasPrefix(path, "/api/subscriptions/subpage-config/"):
		return "/api/subscriptions/subpage-config/:token"
	case strings.HasPrefix(path, "/api/subscriptions/"):
		return "/api/subscriptions/:uuid"

	// Subscription Page Configs & Request History
	case strings.HasPrefix(path, "/api/subscription-page-configs/actions/"):
		return "/api/subscription-page-configs/actions/:action"
	case strings.HasPrefix(path, "/api/subscription-page-configs/"):
		return "/api/subscription-page-configs/:uuid"
	case strings.HasPrefix(path, "/api/subscription-request-history/"):
		return "/api/subscription-request-history/:uuid"

	// Public Sub
	case strings.HasPrefix(path, "/api/sub/"):
		return "/api/sub/:token"

	// Backend Tools
	case strings.HasPrefix(path, "/api/backend-tools/swagger"):
		return "/api/backend-tools/swagger"
	case strings.HasPrefix(path, "/api/backend-tools/scalar"):
		return "/api/backend-tools/scalar"
	case strings.HasPrefix(path, "/api/backend-tools/queues"):
		return "/api/backend-tools/queues"
	case strings.HasPrefix(path, "/api/backend-tools/"):
		return "/api/backend-tools/:other"

	default:
		return "/api/:other"
	}
}

func Middleware(rc *RouteCounter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if rc != nil {
				path := r.URL.Path
				if rc.cfg != nil {
					path = strings.TrimPrefix(path, rc.cfg.Backend.Trimmed())
				}
				if !strings.HasPrefix(path, "/") {
					path = "/" + path
				}
				if strings.HasPrefix(path, "/api/") {
					pattern := NormalizeRoutePattern(path)
					rc.IncrementRoute(r.Method, pattern)
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

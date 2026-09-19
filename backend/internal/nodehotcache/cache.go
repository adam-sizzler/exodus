package nodehotcache

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"exodus/internal/config"
	"exodus/internal/jobqueue"

	"github.com/redis/go-redis/v9"
)

const (
	systemInfoPrefix    = "node_system_info:"
	systemStatsPrefix   = "node_system_stats:"
	versionsPrefix      = "node_versions:"
	singboxUptimePrefix = "node_singbox_uptime:"
	usersOnlinePrefix   = "node_users_online:"

	systemInfoTTL    = 30 * time.Second
	systemStatsTTL   = 30 * time.Second
	versionsTTL      = 30 * time.Second
	singboxUptimeTTL = 16 * time.Second
	usersOnlineTTL   = 16 * time.Second
)

type NodeSystem struct {
	Info  json.RawMessage
	Stats json.RawMessage
}

type NodeVersions struct {
	Singbox string `json:"singbox"`
	Node    string `json:"node"`
}

type HotCache struct {
	System        *NodeSystem
	Versions      *NodeVersions
	SingboxUptime int64
	UsersOnline   int
}

type Cache struct {
	client *redis.Client
	cfg    *config.BackendConfig
}

var defaultCache struct {
	sync.Mutex
	cache *Cache
	ready bool
}

func Default(cfg *config.BackendConfig) *Cache {
	defaultCache.Lock()
	defer defaultCache.Unlock()

	if defaultCache.ready {
		return defaultCache.cache
	}
	if cfg == nil {
		return nil
	}

	client, err := jobqueue.GetSharedRedisClient(cfg)
	if err != nil || client == nil {
		// Logged, not just swallowed: node system stats (rx/tx speed,
		// uptime, users online, versions) live only in Redis, so if this
		// fails, the dashboard will silently show empty stats for every
		// node for the rest of this process's lifetime — worth knowing
		// about immediately rather than discovering it by staring at a
		// blank "Download speed" widget.
		//
		// Crucially, ready is intentionally NOT marked true here, allowing
		// subsequent calls to retry connecting if Redis is temporarily
		// unavailable during early process initialization.
		if cfg.Logger != nil {
			cfg.Logger.Warn("Node hot cache disabled: failed to connect to Redis", "error", err)
		}
		return nil
	}

	defaultCache.ready = true
	defaultCache.cache = &Cache{client: client, cfg: cfg}
	return defaultCache.cache
}

func New(client *redis.Client) *Cache {
	if client == nil {
		return nil
	}
	return &Cache{client: client}
}

func (c *Cache) GetOne(ctx context.Context, uuid string) (HotCache, error) {
	result, err := c.GetMany(ctx, []string{uuid})
	if err != nil {
		return HotCache{}, err
	}
	return result[uuid], nil
}

func (c *Cache) GetMany(ctx context.Context, uuids []string) (map[string]HotCache, error) {
	result := make(map[string]HotCache, len(uuids))
	for _, uuid := range uuids {
		result[uuid] = HotCache{}
	}
	if c == nil || c.client == nil || len(uuids) == 0 {
		return result, nil
	}

	keys := make([]string, 0, len(uuids)*5)
	for _, uuid := range uuids {
		keys = append(keys,
			key(systemInfoPrefix, uuid),
			key(systemStatsPrefix, uuid),
			key(usersOnlinePrefix, uuid),
			key(versionsPrefix, uuid),
			key(singboxUptimePrefix, uuid),
		)
	}

	values, err := c.client.MGet(ctx, keys...).Result()
	if err != nil && err != redis.Nil {
		// Every caller of GetMany discards its error return (they can't do
		// much about a cache miss except show empty stats), so this is the
		// only place a Redis outage would ever be visible. Log it here
		// rather than failing silently — otherwise "the dashboard shows no
		// speed/uptime for any node" looks identical to "Redis is fine, the
		// nodes just aren't reporting anything."
		if c.cfg != nil && c.cfg.Logger != nil {
			c.cfg.Logger.Warn("Node hot cache MGet failed, returning empty stats", "error", err, "nodes", len(uuids))
		}
		return result, nil
	}

	for i, uuid := range uuids {
		base := i * 5
		if base+4 >= len(values) {
			break
		}

		infoRaw := mgetBytes(values[base])
		statsRaw := mgetBytes(values[base+1])
		onlineRaw := mgetString(values[base+2])
		versionsRaw := mgetBytes(values[base+3])
		uptimeRaw := mgetString(values[base+4])

		hot := HotCache{
			SingboxUptime: parseInt64(uptimeRaw),
			UsersOnline:   int(parseInt64(onlineRaw)),
		}
		if len(infoRaw) > 0 && len(statsRaw) > 0 {
			hot.System = &NodeSystem{
				Info:  infoRaw,
				Stats: statsRaw,
			}
		}
		if len(versionsRaw) > 0 {
			var versions NodeVersions
			if err := json.Unmarshal(versionsRaw, &versions); err == nil {
				hot.Versions = &versions
			}
		}
		result[uuid] = hot
	}

	return result, nil
}

func mgetString(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprint(val)
	}
}

func mgetBytes(v any) []byte {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		return val
	case string:
		return []byte(val)
	default:
		return fmt.Append(nil, val)
	}
}

func (c *Cache) SetSystemInfo(ctx context.Context, uuid string, info json.RawMessage) error {
	return c.setJSON(ctx, key(systemInfoPrefix, uuid), info, systemInfoTTL)
}

func (c *Cache) SetSystemStats(ctx context.Context, uuid string, stats json.RawMessage) error {
	return c.setJSON(ctx, key(systemStatsPrefix, uuid), stats, systemStatsTTL)
}

func (c *Cache) SetVersions(ctx context.Context, uuid, singbox, node string) error {
	if c == nil || c.client == nil || strings.TrimSpace(uuid) == "" {
		return nil
	}
	payload, err := json.Marshal(NodeVersions{
		Singbox: strings.TrimSpace(singbox),
		Node:    strings.TrimSpace(node),
	})
	if err != nil {
		return err
	}
	redisKey := key(versionsPrefix, uuid)
	err = c.client.Set(ctx, redisKey, payload, versionsTTL).Err()
	c.logWriteError("SetVersions", redisKey, err)
	return err
}

func (c *Cache) SetUptime(ctx context.Context, uuid string, seconds int64) error {
	if c == nil || c.client == nil || strings.TrimSpace(uuid) == "" {
		return nil
	}
	redisKey := key(singboxUptimePrefix, uuid)
	err := c.client.Set(ctx, redisKey, strconv.FormatInt(seconds, 10), singboxUptimeTTL).Err()
	c.logWriteError("SetUptime", redisKey, err)
	return err
}

func (c *Cache) SetUsersOnline(ctx context.Context, uuid string, count int) error {
	if c == nil || c.client == nil || strings.TrimSpace(uuid) == "" {
		return nil
	}
	redisKey := key(usersOnlinePrefix, uuid)
	err := c.client.Set(ctx, redisKey, strconv.Itoa(count), usersOnlineTTL).Err()
	c.logWriteError("SetUsersOnline", redisKey, err)
	return err
}

func (c *Cache) Delete(ctx context.Context, uuid string) error {
	if c == nil || c.client == nil || strings.TrimSpace(uuid) == "" {
		return nil
	}
	err := c.client.Del(ctx,
		key(systemInfoPrefix, uuid),
		key(systemStatsPrefix, uuid),
		key(usersOnlinePrefix, uuid),
		key(versionsPrefix, uuid),
		key(singboxUptimePrefix, uuid),
	).Err()
	c.logWriteError("Delete", uuid, err)
	return err
}

func (c *Cache) DeleteTransient(ctx context.Context, uuid string) error {
	if c == nil || c.client == nil || strings.TrimSpace(uuid) == "" {
		return nil
	}
	err := c.client.Del(ctx,
		key(systemInfoPrefix, uuid),
		key(systemStatsPrefix, uuid),
		key(usersOnlinePrefix, uuid),
		key(versionsPrefix, uuid),
		key(singboxUptimePrefix, uuid),
	).Err()
	c.logWriteError("DeleteTransient", uuid, err)
	return err
}

func (c *Cache) setJSON(ctx context.Context, redisKey string, payload json.RawMessage, ttl time.Duration) error {
	if c == nil || c.client == nil || strings.TrimSpace(redisKey) == "" || len(payload) == 0 {
		return nil
	}
	err := c.client.Set(ctx, redisKey, []byte(payload), ttl).Err()
	c.logWriteError("setJSON", redisKey, err)
	return err
}

func (c *Cache) logWriteError(op, redisKey string, err error) {
	if err != nil && c != nil && c.cfg != nil && c.cfg.Logger != nil {
		c.cfg.Logger.Warn("Node hot cache write failed", "op", op, "key", redisKey, "error", err)
	}
}

func key(prefix, uuid string) string {
	return prefix + strings.TrimSpace(uuid)
}

func stringValue(cmd *redis.StringCmd) string {
	if cmd == nil || cmd.Err() != nil {
		return ""
	}
	return strings.TrimSpace(cmd.Val())
}

func parseInt64(raw string) int64 {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0
	}
	return value
}

func validJSON(raw string) bool {
	raw = strings.TrimSpace(raw)
	return raw != "" && json.Valid([]byte(raw))
}

package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"exodus-node/config"

	"github.com/iancoleman/orderedmap"
)

// DeployConfigTaskPayload is JSON payload for SubmitTask(operation=deploy_config).
// It accepts either "config" or "singbox_config" with raw sing-box JSON.
type DeployConfigTaskPayload struct {
	Config            json.RawMessage         `json:"config"`
	SingboxConfig     json.RawMessage         `json:"singbox_config"`
	Listen            string                  `json:"listen"`
	Restart           *bool                   `json:"restart"`
	ForceRestart      *bool                   `json:"force_restart"`
	ForceRestartCamel *bool                   `json:"forceRestart"`
	Stats             DeployConfigTaskStats   `json:"stats"`
	Modules           DeployModulesPayload    `json:"modules"`
	Internals         *DeployInternalsPayload `json:"internals"`
	Hashes            *DeployHashesPayload    `json:"hashes"`
}

type DeployInternalsPayload struct {
	Hashes       DeployHashesPayload `json:"hashes"`
	ForceRestart *bool               `json:"force_restart"`
}

type DeployHashesPayload struct {
	EmptyConfig      string              `json:"emptyConfig"`
	Inbounds         []DeployInboundHash `json:"inbounds"`
	HaproxyUsersHash string              `json:"haproxyUsersHash,omitempty"`
	HaproxyCount     int                 `json:"haproxyCount,omitempty"`
}

type DeployInboundHash struct {
	Tag        string `json:"tag"`
	Hash       string `json:"hash"`
	UsersCount int    `json:"usersCount"`
}

func (task DeployConfigTaskPayload) getHashes() *DeployHashesPayload {
	if task.Hashes != nil {
		return task.Hashes
	}
	if task.Internals != nil {
		return &task.Internals.Hashes
	}
	return nil
}

type DeployConfigTaskStats struct {
	Enabled   *bool    `json:"enabled"`
	Inbounds  []string `json:"inbounds"`
	Outbounds []string `json:"outbounds"`
	Users     []string `json:"users"`
}

type DeployModulesPayload struct {
	HaproxyEnabled     bool                    `json:"haproxy_enabled"`
	HaproxyInboundTags []string                `json:"haproxy_inbound_tags,omitempty"`
	HaproxyUsers       []HaproxyUserEntry      `json:"haproxy_users"`
	IngressFilter      NftIngressFilterPayload `json:"ingress_filter"`
	EgressFilter       NftEgressFilterPayload  `json:"egress_filter"`
	PreStart           PreStartPluginPayload   `json:"pre_start"`
}

type PreStartPluginPayload struct {
	Enabled        bool                         `json:"enabled"`
	CleanupSockets *CleanupSocketsPluginPayload `json:"cleanupSockets,omitempty"`
}

type CleanupSocketsPluginPayload struct {
	Enabled bool     `json:"enabled"`
	Files   []string `json:"files"`
}

type HaproxyUserEntry struct {
	Username       string `json:"username"`
	VLESSUUID      string `json:"vless_uuid"`
	TrojanPassword string `json:"trojan_password"`
	AnytlsPassword string `json:"anytls_password"`
	NaivePassword  string `json:"naive_password,omitempty"`
}

type DeploySummary struct {
	ConfigPath          string
	Listen              string
	Inbounds            int
	Outbounds           int
	Users               int
	Restarted           bool
	ForceRestart        bool
	ConfigChanged       bool
	HaproxyUsersChanged bool
	ReloadError         string
	CoreProcessBefore   string
	CoreProcessAfter    string
	CoreReady           bool
}

// DeployConfig applies sing-box config, injects experimental.v2ray_api stats block and starts/restarts the managed core if requested.
func (s *NodeServer) DeployConfig(ctx context.Context, task DeployConfigTaskPayload) (DeploySummary, error) {
	log := s.Cfg.LoggerFor("SingboxService")
	haproxyLog := s.Cfg.LoggerFor("HAProxyService")
	if ctx == nil {
		ctx = context.Background()
	}
	rawConfig := task.Config
	if len(rawConfig) == 0 {
		rawConfig = task.SingboxConfig
	}
	if len(rawConfig) == 0 {
		return DeploySummary{}, fmt.Errorf("deploy payload does not contain sing-box config")
	}
	log.Debug("DeployConfig started", "config_bytes", len(rawConfig))

	logInternalUserExtraction(s.Cfg.LoggerFor("InternalService"), rawConfig, task.getHashes())

	configPath := config.FixedSingboxConfigPath

	// Do NOT default task.Listen here. Doing so makes opt.Listen always
	// non-empty below, which permanently shadows BuildSingboxConfigWithV2RayAPI's
	// own fallback to the profile config's experimental.v2ray_api.listen -
	// the panel-sent config profile's listen/port was silently discarded and
	// the node always fell back to the fixed default instead. Pass task.Listen
	// through as-is (possibly empty) so Build's priority chain applies:
	// explicit task.Listen > profile config's own listen > fixed default.
	listen := task.Listen

	enabled := true
	if task.Stats.Enabled != nil {
		enabled = *task.Stats.Enabled
	}

	finalConfig, summary, err := BuildSingboxConfigWithV2RayAPI(rawConfig, BuildOptions{
		Listen:          listen,
		Enabled:         enabled,
		ExplicitUsers:   task.Stats.Users,
		ExplicitInTags:  task.Stats.Inbounds,
		ExplicitOutTags: task.Stats.Outbounds,
	})
	if err != nil {
		return DeploySummary{}, fmt.Errorf("build sing-box config: %w", err)
	}

	// Update the target listen address to whatever Build actually resolved
	// (explicit task.Listen, else the profile config's own listen, else the
	// fixed default) - this is always the real effective value regardless of
	// which branch of the priority chain produced it.
	listen = summary.Listen

	// Update the core API client connection to use the new listen address/port
	if host, portStr, err := net.SplitHostPort(listen); err == nil {
		if port, err := strconv.Atoi(portStr); err == nil {
			if err := s.apiService.UpdateAPIClient(host, port); err != nil {
				log.Error("Failed to update core API client", "listen", listen, "error", err)
			}
		}
	}

	log.Debug("Built sing-box config with v2ray_api", "listen", listen, "inbounds", len(summary.Inbounds), "outbounds", len(summary.Outbounds), "users", len(summary.Users))

	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		return DeploySummary{}, fmt.Errorf("create config dir: %w", err)
	}

	finalConfigHash := sha256Hex(finalConfig)
	configChanged := true
	if currentConfig, err := os.ReadFile(configPath); err == nil {
		configChanged = sha256Hex(currentConfig) != finalConfigHash
	} else if !os.IsNotExist(err) {
		return DeploySummary{}, fmt.Errorf("read current config: %w", err)
	}

	if configChanged {
		if err := os.WriteFile(configPath, finalConfig, 0o644); err != nil {
			return DeploySummary{}, fmt.Errorf("write config: %w", err)
		}
		log.Warn("Detected changes in Sing-box Core base configuration")
		log.Debug("Sing-box config written", "path", configPath, "bytes", len(finalConfig))
	} else {
		log.Debug("Sing-box config unchanged, write skipped", "path", configPath)
	}

	s.setHaproxyPluginState(task.Modules.HaproxyEnabled, task.Modules.HaproxyInboundTags)
	haproxyUsersChanged, err := applyHaproxyModule(task.Modules, task.getHashes())
	if err != nil {
		return DeploySummary{}, err
	}
	if err := applyNftablesModule(task.Modules, s.asnService); err != nil {
		return DeploySummary{}, err
	}

	if haproxyUsersChanged {
		reloadResult := reloadHaproxyUsers()
		switch {
		case reloadResult.Reloaded:
			haproxyLog.Log("HAProxy users cache reloaded", "socket", haproxyRuntimeSocketPath, "result", reloadResult.Output)
		case reloadResult.Skipped:
			haproxyLog.Debug("HAProxy users reload skipped", "socket", haproxyRuntimeSocketPath, "warning", reloadResult.Warning)
		default:
			haproxyLog.Warn("HAProxy users reload failed", "socket", haproxyRuntimeSocketPath, "warning", reloadResult.Warning)
		}
	} else {
		haproxyLog.Debug("HAProxy users unchanged, runtime reload skipped")
	}

	shouldRestart := task.restartRequested()
	forceRestart := task.forceRestartRequested()

	restarted := false
	reloadError := ""
	coreProcessBefore := ""
	coreProcessAfter := ""
	coreReady := false
	if shouldRestart {
		if s.shouldRunCoreLifecycle(ctx, task, configChanged) {
			if forceRestart && !configChanged {
				log.Warn("Force restart requested")
			} else {
				log.Debug("Core managed lifecycle requested by deploy payload")
			}

			if task.Modules.PreStart.Enabled && task.Modules.PreStart.CleanupSockets != nil && task.Modules.PreStart.CleanupSockets.Enabled {
				cleanupSocketFiles(log, task.Modules.PreStart.CleanupSockets.Files)
			}

			lifecycle := restartCoreProcessLifecycle(ctx, s.Cfg, s.apiService)
			coreProcessBefore = lifecycle.ProcessBefore
			coreProcessAfter = lifecycle.ProcessAfter
			coreReady = lifecycle.Ready
			if lifecycle.failed() {
				reloadError = lifecycle.Error
			} else {
				restarted = lifecycle.Started
			}
		} else {
			log.Log("Sing-box Core configuration is up-to-date - no restart required")
			coreReady = true
		}
	} else {
		log.Debug("Core lifecycle skipped by deploy payload")
	}

	return DeploySummary{
		ConfigPath:          configPath,
		Listen:              listen,
		Inbounds:            len(summary.Inbounds),
		Outbounds:           len(summary.Outbounds),
		Users:               len(summary.Users),
		Restarted:           restarted,
		ForceRestart:        forceRestart,
		ConfigChanged:       configChanged,
		HaproxyUsersChanged: haproxyUsersChanged,
		ReloadError:         reloadError,
		CoreProcessBefore:   coreProcessBefore,
		CoreProcessAfter:    coreProcessAfter,
		CoreReady:           coreReady,
	}, nil
}

func (task DeployConfigTaskPayload) restartRequested() bool {
	if task.forceRestartRequested() {
		return true
	}
	return task.Restart != nil && *task.Restart
}

func (task DeployConfigTaskPayload) forceRestartRequested() bool {
	if task.ForceRestart != nil {
		return *task.ForceRestart
	}
	return task.ForceRestartCamel != nil && *task.ForceRestartCamel
}

func shouldReloadCore(task DeployConfigTaskPayload, configChanged bool) bool {
	if !task.restartRequested() {
		return false
	}
	return configChanged || task.forceRestartRequested()
}

func (s *NodeServer) shouldRunCoreLifecycle(ctx context.Context, task DeployConfigTaskPayload, configChanged bool) bool {
	if !task.restartRequested() {
		return false
	}
	if configChanged || task.forceRestartRequested() {
		return true
	}
	if s == nil || s.Cfg == nil || s.apiService == nil {
		return true
	}
	log := s.Cfg.LoggerFor("SingboxService")
	if !s.apiService.IsCoreOnline() {
		log.Debug("Core API is not marked online; starting managed core without pre-healthcheck")
		return true
	}

	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.apiService.CheckCoreReady(checkCtx); err != nil {
		s.apiService.MarkCoreOffline()
		log.Warn("Sing-box Core health check failed, restarting...", "error", err)
		return true
	}
	return false
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type BuildOptions struct {
	Listen          string
	Enabled         bool
	ExplicitInTags  []string
	ExplicitOutTags []string
	ExplicitUsers   []string
}

type buildSummary struct {
	Listen    string
	Inbounds  []string
	Outbounds []string
	Users     []string
}

func BuildSingboxConfigWithV2RayAPI(rawConfig json.RawMessage, opt BuildOptions) ([]byte, buildSummary, error) {
	cfg := orderedmap.New()
	if err := json.Unmarshal(rawConfig, cfg); err != nil {
		return nil, buildSummary{}, fmt.Errorf("invalid sing-box JSON: %w", err)
	}
	normalizeLocalRuleSetPathsOrdered(cfg, filepath.Dir(config.FixedSingboxConfigPath))

	inboundsValue, _ := cfg.Get("inbounds")
	outboundsValue, _ := cfg.Get("outbounds")

	inboundTags := dedupeKeepOrder(opt.ExplicitInTags)
	if len(inboundTags) == 0 {
		inboundTags = extractTags(inboundsValue)
	}

	outboundTags := dedupeKeepOrder(opt.ExplicitOutTags)
	if len(outboundTags) == 0 {
		outboundTags = extractTags(outboundsValue)
	}

	users := dedupeKeepOrder(opt.ExplicitUsers)
	if len(users) == 0 {
		users = extractUsers(inboundsValue)
	}

	experimental, _ := orderedMapByKey(cfg, "experimental")

	// 1. Process cache_file config options
	cacheFile, hasCacheFile := orderedMapByKey(&experimental, "cache_file")
	if !hasCacheFile {
		cacheFile.Set("enabled", true)
	} else {
		if _, hasEnabled := cacheFile.Get("enabled"); !hasEnabled {
			cacheFile.Set("enabled", true)
		}
	}
	experimental.Set("cache_file", cacheFile)

	// 2. Process v2ray_api config options
	v2rayAPI, hasV2rayAPI := orderedMapByKey(&experimental, "v2ray_api")

	listen := opt.Listen
	if listen == "" && hasV2rayAPI {
		if lRaw, ok := v2rayAPI.Get("listen"); ok {
			if lStr, ok := lRaw.(string); ok && lStr != "" {
				listen = lStr
			}
		}
	}
	if listen == "" {
		listen = fmt.Sprintf("%s:%d", config.FixedCoreAPIAddress, config.FixedCoreAPIGRPCPort)
	}

	stats, hasStats := orderedMapByKey(&v2rayAPI, "stats")

	statsEnabled := opt.Enabled
	if hasStats {
		if eRaw, ok := stats.Get("enabled"); ok {
			if eBool, ok := eRaw.(bool); ok {
				statsEnabled = eBool
			}
		}
	}

	var statsInbounds []string
	hasExplicitInbounds := false
	if hasStats {
		if rawIn, ok := stats.Get("inbounds"); ok {
			if arr, ok := rawIn.([]any); ok && len(arr) > 0 {
				hasExplicitInbounds = true
				statsInbounds = make([]string, 0, len(arr))
				for _, item := range arr {
					if str, ok := item.(string); ok && str != "" {
						statsInbounds = append(statsInbounds, str)
					}
				}
			}
		}
	}
	if !hasExplicitInbounds {
		statsInbounds = inboundTags
	}

	var statsOutbounds []string
	hasExplicitOutbounds := false
	if hasStats {
		if rawOut, ok := stats.Get("outbounds"); ok {
			if arr, ok := rawOut.([]any); ok && len(arr) > 0 {
				hasExplicitOutbounds = true
				statsOutbounds = make([]string, 0, len(arr))
				for _, item := range arr {
					if str, ok := item.(string); ok && str != "" {
						statsOutbounds = append(statsOutbounds, str)
					}
				}
			}
		}
	}
	if !hasExplicitOutbounds {
		statsOutbounds = outboundTags
	}

	var statsUsers []string
	hasExplicitUsers := false
	if hasStats {
		if rawUsers, ok := stats.Get("users"); ok {
			if arr, ok := rawUsers.([]any); ok && len(arr) > 0 {
				hasExplicitUsers = true
				statsUsers = make([]string, 0, len(arr))
				for _, item := range arr {
					if str, ok := item.(string); ok && str != "" {
						statsUsers = append(statsUsers, str)
					}
				}
			}
		}
	}
	if !hasExplicitUsers {
		statsUsers = users
	}

	stats.Set("enabled", statsEnabled)
	stats.Set("inbounds", nonNilSlice(statsInbounds))
	stats.Set("outbounds", nonNilSlice(statsOutbounds))
	stats.Set("users", nonNilSlice(statsUsers))

	v2rayAPI.Set("listen", listen)
	v2rayAPI.Set("stats", stats)

	experimental.Set("v2ray_api", v2rayAPI)
	cfg.Set("experimental", experimental)

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return nil, buildSummary{}, fmt.Errorf("marshal sing-box JSON: %w", err)
	}
	return data, buildSummary{
		Listen:    listen,
		Inbounds:  statsInbounds,
		Outbounds: statsOutbounds,
		Users:     statsUsers,
	}, nil
}

func orderedMapByKey(container any, key string) (orderedmap.OrderedMap, bool) {
	raw, ok := getField(container, key)
	if !ok {
		return *orderedmap.New(), false
	}
	m, ok := toOrderedMap(raw)
	if !ok {
		return *orderedmap.New(), false
	}
	return m, true
}

func mapAny(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func extractTags(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		tag := getFieldString(item, "tag")
		if tag != "" {
			out = append(out, tag)
		}
	}
	return dedupeKeepOrder(out)
}

func extractUsers(v any) []string {
	inbounds, ok := v.([]any)
	if !ok {
		return nil
	}
	users := make([]string, 0)
	for _, inbound := range inbounds {
		usersRaw, ok := getField(inbound, "users")
		if !ok {
			continue
		}
		usersArr, ok := usersRaw.([]any)
		if !ok {
			continue
		}
		for _, u := range usersArr {
			name := getFieldString(u, "name")
			if name != "" {
				users = append(users, name)
			}
		}
	}
	return dedupeKeepOrder(users)
}

func dedupeKeepOrder(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, item := range in {
		if item == "" {
			continue
		}
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func nonNilSlice(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

// djb2Dual implements 64-bit dual-hash for fast deduplication.
func djb2Dual(str string) (uint32, uint32) {
	var high uint32 = 5381
	var low uint32 = 5387
	for i := 0; i < len(str); i++ {
		c := uint32(str[i])
		high = (high<<5) + high + c
		low = (low<<6) + low + c*37
	}
	return high, low
}

type HashedSet struct {
	seen     map[uint64]struct{}
	hashHigh uint32
	hashLow  uint32
}

func NewHashedSet() *HashedSet {
	return &HashedSet{
		seen: make(map[uint64]struct{}),
	}
}

func (h *HashedSet) Add(str string) {
	if str == "" {
		return
	}
	high, low := djb2Dual(str)
	key := (uint64(high) << 32) | uint64(low)
	if _, ok := h.seen[key]; !ok {
		h.seen[key] = struct{}{}
		h.hashHigh ^= high
		h.hashLow ^= low
	}
}

func (h *HashedSet) AddParts(part1 string, sep byte, part2 []byte) {
	if part1 == "" && len(part2) == 0 {
		return
	}
	var high uint32 = 5381
	var low uint32 = 5387
	for i := 0; i < len(part1); i++ {
		c := uint32(part1[i])
		high = (high << 5) + high + c
		low = (low << 6) + low + c*37
	}
	if sep != 0 {
		c := uint32(sep)
		high = (high << 5) + high + c
		low = (low << 6) + low + c*37
	}
	for i := 0; i < len(part2); i++ {
		c := uint32(part2[i])
		high = (high << 5) + high + c
		low = (low << 6) + low + c*37
	}

	key := (uint64(high) << 32) | uint64(low)
	if _, ok := h.seen[key]; !ok {
		h.seen[key] = struct{}{}
		h.hashHigh ^= high
		h.hashLow ^= low
	}
}

func (h *HashedSet) Hash64String() string {
	return fmt.Sprintf("%08x%08x", h.hashHigh, h.hashLow)
}

func (h *HashedSet) Size() int {
	return len(h.seen)
}

func computeEmptyConfigHash(rawConfig json.RawMessage) string {
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(rawConfig, &parsed); err != nil {
		return sha256Hex(rawConfig)
	}
	inboundsRaw, ok := parsed["inbounds"]
	if !ok || len(inboundsRaw) == 0 {
		return sha256Hex(rawConfig)
	}

	var inbounds []map[string]json.RawMessage
	if err := json.Unmarshal(inboundsRaw, &inbounds); err != nil {
		return sha256Hex(rawConfig)
	}

	hasChanged := false
	for _, inMap := range inbounds {
		if _, hasUsers := inMap["users"]; hasUsers {
			delete(inMap, "users")
			hasChanged = true
		}
		if _, hasClients := inMap["clients"]; hasClients {
			delete(inMap, "clients")
			hasChanged = true
		}
	}
	if !hasChanged {
		return sha256Hex(rawConfig)
	}

	cleanInboundsJSON, err := json.Marshal(inbounds)
	if err != nil {
		return sha256Hex(rawConfig)
	}
	parsed["inbounds"] = cleanInboundsJSON

	cleanJSON, err := json.Marshal(parsed)
	if err != nil {
		return sha256Hex(rawConfig)
	}
	return sha256Hex(cleanJSON)
}

func logInternalUserExtraction(log *config.Logger, rawConfig json.RawMessage, hashes *DeployHashesPayload) {
	if log == nil || !log.Enabled(config.LogLevelInfo) {
		return
	}
	start := time.Now()
	log.Log("Cleaning up internal service.")
	log.Log("Starting user extraction from inbounds...")

	emptyHash := ""
	if hashes != nil && hashes.EmptyConfig != "" {
		emptyHash = hashes.EmptyConfig
	} else {
		emptyHash = computeEmptyConfigHash(rawConfig)
	}
	log.Log(fmt.Sprintf("▸ Empty Config Hash: %s", emptyHash))

	remoteHashes := make(map[string]string)
	if hashes != nil {
		for _, item := range hashes.Inbounds {
			if item.Tag != "" {
				remoteHashes[item.Tag] = item.Hash
			}
		}
	}

	var parsed struct {
		Inbounds []map[string]any `json:"inbounds"`
	}
	if err := json.Unmarshal(rawConfig, &parsed); err == nil {
		for _, in := range parsed.Inbounds {
			tag, _ := in["tag"].(string)
			if tag == "" {
				tag = "inbound"
			}
			usersRaw, ok := in["users"].([]any)
			if !ok || len(usersRaw) == 0 {
				continue
			}
			userSet := NewHashedSet()
			for _, u := range usersRaw {
				if uMap, ok := u.(map[string]any); ok {
					id, _ := uMap["uuid"].(string)
					if id == "" {
						id, _ = uMap["password"].(string)
					}
					if id == "" {
						id, _ = uMap["name"].(string)
					}
					if id != "" {
						userSet.Add(id)
					}
				}
			}
			if userSet.Size() > 0 {
				localHash := userSet.Hash64String()
				remoteHash, hasRemote := remoteHashes[tag]
				if !hasRemote || remoteHash == "" {
					remoteHash = "N/A"
				}
				log.Log(fmt.Sprintf("▸ %s · %d users · %s (%s)", tag, userSet.Size(), localHash, remoteHash))
			}
		}
	}
	log.Log(fmt.Sprintf("User extraction completed in %s", formatDuration(time.Since(start))))
}

func normalizeLocalRuleSetPathsOrdered(cfg *orderedmap.OrderedMap, baseDir string) {
	if cfg == nil || strings.TrimSpace(baseDir) == "" {
		return
	}
	routeRaw, ok := cfg.Get("route")
	if !ok {
		return
	}
	route, ok := toOrderedMap(routeRaw)
	if !ok {
		return
	}
	rawRuleSetsRaw, ok := route.Get("rule_set")
	if !ok {
		return
	}
	rawRuleSets, ok := rawRuleSetsRaw.([]any)
	if !ok || len(rawRuleSets) == 0 {
		return
	}

	changed := false
	for i, raw := range rawRuleSets {
		ruleSet, ok := toOrderedMap(raw)
		if !ok {
			continue
		}
		rsType := getFieldString(ruleSet, "type")
		if strings.ToLower(strings.TrimSpace(rsType)) != "local" {
			continue
		}
		pathValue := getFieldString(ruleSet, "path")
		pathValue = strings.TrimSpace(pathValue)
		if pathValue == "" || filepath.IsAbs(pathValue) {
			continue
		}
		ruleSet.Set("path", filepath.Clean(filepath.Join(baseDir, pathValue)))
		rawRuleSets[i] = ruleSet
		changed = true
	}

	if changed {
		route.Set("rule_set", rawRuleSets)
		cfg.Set("route", route)
	}
}

func toOrderedMap(v any) (orderedmap.OrderedMap, bool) {
	switch m := v.(type) {
	case orderedmap.OrderedMap:
		return m, true
	case *orderedmap.OrderedMap:
		if m == nil {
			return orderedmap.OrderedMap{}, false
		}
		return *m, true
	default:
		return orderedmap.OrderedMap{}, false
	}
}

func getField(v any, key string) (any, bool) {
	switch m := v.(type) {
	case map[string]any:
		value, ok := m[key]
		return value, ok
	case orderedmap.OrderedMap:
		return m.Get(key)
	case *orderedmap.OrderedMap:
		if m == nil {
			return nil, false
		}
		return m.Get(key)
	default:
		return nil, false
	}
}

func getFieldString(v any, key string) string {
	value, ok := getField(v, key)
	if !ok {
		return ""
	}
	s, _ := value.(string)
	return s
}

func cleanupSocketFiles(log interface {
	Debug(string, ...any)
	Info(string, ...any)
	Warn(string, ...any)
}, files []string) {
	for _, pattern := range files {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		matches, err := filepath.Glob(pattern)
		if err != nil {
			if log != nil {
				log.Warn("Failed to glob pattern for cleanupSockets", "pattern", pattern, "error", err)
			}
			continue
		}
		for _, match := range matches {
			info, err := os.Lstat(match)
			if err != nil {
				continue
			}
			// Only remove unix sockets or socket files
			if info.Mode()&os.ModeSocket != 0 || strings.HasSuffix(match, ".sock") {
				if err := os.Remove(match); err != nil {
					if log != nil {
						log.Warn("Failed to remove socket file", "path", match, "error", err)
					}
				} else {
					if log != nil {
						log.Info("Cleaned up stale socket file before core start", "path", match)
					}
				}
			}
		}
	}
}

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/iancoleman/orderedmap"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"

	"exodus-node/config"
)

const (
	coreReloadDebounceDelay = 10 * time.Second
	coreReloadMaxWait       = 30 * time.Second
)

// SyncUserItem represents a user update instruction from the panel.
type SyncUserItem struct {
	Action            string   `json:"action"` // "add", "delete", "disable"
	Identifier        string   `json:"identifier"`
	Username          string   `json:"username"`
	UUID              string   `json:"uuid,omitempty"`
	TrojanPassword    string   `json:"trojan_password,omitempty"`
	SSPassword        string   `json:"ss_password,omitempty"`
	Hysteria2Password string   `json:"hysteria2_password,omitempty"`
	Flow              string   `json:"flow,omitempty"`
	InboundTags       []string `json:"inbound_tags,omitempty"`
}

// SyncUsersTaskPayload wraps multiple user synchronization items.
type SyncUsersTaskPayload struct {
	Users []SyncUserItem `json:"users"`
}

// HandleSyncUsers handles dynamic user addition, updating, or removal directly in Sing-box config.
func (s *NodeServer) HandleSyncUsers(ctx context.Context, operation string, payloadBytes []byte) (*rpcstatus.Status, error) {
	if s == nil || s.Cfg == nil {
		return &rpcstatus.Status{
			Code:    int32(codes.Internal),
			Message: "node server is uninitialized",
		}, nil
	}
	log := s.Cfg.LoggerFor("SingboxService")

	var payload SyncUsersTaskPayload
	if len(payloadBytes) > 0 {
		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			log.Warn("Failed to unmarshal sync_users payload", "error", err)
			return &rpcstatus.Status{
				Code:    int32(codes.InvalidArgument),
				Message: fmt.Sprintf("invalid payload: %v", err),
			}, nil
		}
	}

	if len(payload.Users) == 0 {
		return &rpcstatus.Status{
			Code:    int32(codes.OK),
			Message: "no users to sync",
		}, nil
	}

	// Override action if operation is explicitly "add_users" or "delete_users"
	if operation == "add_users" {
		for i := range payload.Users {
			if payload.Users[i].Action == "" {
				payload.Users[i].Action = "add"
			}
		}
	} else if operation == "delete_users" {
		for i := range payload.Users {
			if payload.Users[i].Action == "" {
				payload.Users[i].Action = "delete"
			}
		}
	}

	s.configLock.Lock()
	defer s.configLock.Unlock()

	configPath := config.FixedSingboxConfigPath
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Warn("Sing-box config does not exist on disk yet; full deploy required", "path", configPath)
		return &rpcstatus.Status{
			Code:    int32(codes.FailedPrecondition),
			Message: "sing-box config not yet deployed on node",
		}, nil
	}

	rawConfig, err := os.ReadFile(configPath)
	if err != nil {
		log.Error("Failed to read current sing-box config", "path", configPath, "error", err)
		return &rpcstatus.Status{
			Code:    int32(codes.Internal),
			Message: fmt.Sprintf("read config: %v", err),
		}, nil
	}

	cfg := orderedmap.New()
	if err := json.Unmarshal(rawConfig, cfg); err != nil {
		log.Error("Failed to parse sing-box JSON", "error", err)
		return &rpcstatus.Status{
			Code:    int32(codes.Internal),
			Message: fmt.Sprintf("parse config json: %v", err),
		}, nil
	}

	modified := patchSingboxConfigUsers(cfg, payload.Users)
	if modified {
		data, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			log.Error("Failed to marshal patched sing-box config", "error", err)
			return &rpcstatus.Status{
				Code:    int32(codes.Internal),
				Message: fmt.Sprintf("marshal config: %v", err),
			}, nil
		}

		tmpPath := configPath + ".tmp"
		if err := os.MkdirAll(filepath.Dir(tmpPath), 0o755); err != nil {
			return &rpcstatus.Status{
				Code:    int32(codes.Internal),
				Message: fmt.Sprintf("create config dir: %v", err),
			}, nil
		}

		if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
			return &rpcstatus.Status{
				Code:    int32(codes.Internal),
				Message: fmt.Sprintf("write tmp config: %v", err),
			}, nil
		}

		if err := os.Rename(tmpPath, configPath); err != nil {
			return &rpcstatus.Status{
				Code:    int32(codes.Internal),
				Message: fmt.Sprintf("rename config: %v", err),
			}, nil
		}

		log.Info("Patched sing-box config on disk with user changes", "users_count", len(payload.Users))
		s.scheduleDebouncedCoreReload()
	} else {
		log.Debug("User changes already reflected in config, reload not scheduled")
	}

	return &rpcstatus.Status{
		Code:    int32(codes.OK),
		Message: fmt.Sprintf("success: synced %d user(s), debounced reload scheduled", len(payload.Users)),
	}, nil
}

func patchSingboxConfigUsers(cfg *orderedmap.OrderedMap, users []SyncUserItem) bool {
	if cfg == nil || len(users) == 0 {
		return false
	}

	inboundsRaw, hasInbounds := cfg.Get("inbounds")
	if !hasInbounds {
		return false
	}

	inboundsArr, ok := inboundsRaw.([]any)
	if !ok {
		return false
	}

	modified := false

	for _, user := range users {
		action := strings.ToLower(strings.TrimSpace(user.Action))
		if action == "" {
			action = "add"
		}

		targetTags := make(map[string]struct{}, len(user.InboundTags))
		for _, t := range user.InboundTags {
			cleaned := strings.TrimSpace(t)
			if cleaned != "" {
				targetTags[cleaned] = struct{}{}
			}
		}

		for idx, inb := range inboundsArr {
			inboundTag := getFieldString(inb, "tag")
			inboundType := normalizeInboundType(inb)

			if len(targetTags) > 0 {
				if _, matches := targetTags[inboundTag]; !matches {
					continue
				}
			}

			// Inbounds without user credential support (e.g. direct, socks) are skipped
			if !inboundSupportsUsers(inboundType) {
				continue
			}

			usersRaw, _ := getField(inb, "users")
			existingUsersArr, _ := usersRaw.([]any)

			switch action {
			case "add":
				updatedUsers, changed := upsertInboundUser(inboundType, existingUsersArr, user)
				if changed {
					inboundsArr[idx] = setField(inb, "users", updatedUsers)
					modified = true
				}
			case "delete", "disable":
				updatedUsers, changed := removeInboundUser(existingUsersArr, user)
				if changed {
					inboundsArr[idx] = setField(inb, "users", updatedUsers)
					modified = true
				}
			}
		}

		// Keep experimental.v2ray_api.stats.users synchronized
		if action == "add" {
			if addStatsUser(cfg, userIdentifier(user)) {
				modified = true
			}
		} else if action == "delete" || action == "disable" {
			if removeStatsUser(cfg, userIdentifier(user), user.Username) {
				modified = true
			}
		}
	}

	if modified {
		cfg.Set("inbounds", inboundsArr)
	}

	return modified
}

func inboundSupportsUsers(inboundType string) bool {
	switch strings.ToLower(strings.TrimSpace(inboundType)) {
	case "vless", "vmess", "trojan", "shadowsocks", "ss", "hysteria2", "hy2", "tuic", "naive", "shadowtls":
		return true
	default:
		return false
	}
}

func normalizeInboundType(inbound any) string {
	t := getFieldString(inbound, "type")
	return strings.ToLower(strings.TrimSpace(t))
}

func userIdentifier(user SyncUserItem) string {
	if strings.TrimSpace(user.Identifier) != "" {
		return strings.TrimSpace(user.Identifier)
	}
	return strings.TrimSpace(user.Username)
}

func upsertInboundUser(inboundType string, users []any, item SyncUserItem) ([]any, bool) {
	identifier := userIdentifier(item)
	if identifier == "" {
		return users, false
	}

	userObj := buildUserObject(inboundType, item)
	if userObj == nil {
		return users, false
	}

	for i, existing := range users {
		name := getFieldString(existing, "name")
		if name == "" {
			name = getFieldString(existing, "username")
		}
		uuidVal := getFieldString(existing, "uuid")

		if (name != "" && (name == identifier || (item.Username != "" && name == item.Username))) ||
			(uuidVal != "" && item.UUID != "" && uuidVal == item.UUID) {
			// Update in-place
			users[i] = userObj
			return users, true
		}
	}

	// User not found, append
	return append(users, userObj), true
}

func removeInboundUser(users []any, item SyncUserItem) ([]any, bool) {
	identifier := userIdentifier(item)
	if len(users) == 0 {
		return users, false
	}

	result := make([]any, 0, len(users))
	changed := false

	for _, existing := range users {
		name := getFieldString(existing, "name")
		if name == "" {
			name = getFieldString(existing, "username")
		}
		uuidVal := getFieldString(existing, "uuid")

		isMatch := (identifier != "" && name == identifier) ||
			(item.Username != "" && name == item.Username) ||
			(item.UUID != "" && uuidVal == item.UUID)

		if isMatch {
			changed = true
			continue
		}
		result = append(result, existing)
	}

	return result, changed
}

func buildUserObject(inboundType string, item SyncUserItem) any {
	identifier := userIdentifier(item)
	m := orderedmap.New()

	switch inboundType {
	case "vless", "vmess":
		m.Set("name", identifier)
		if item.UUID != "" {
			m.Set("uuid", item.UUID)
		}
		if item.Flow != "" && inboundType == "vless" {
			m.Set("flow", item.Flow)
		}
		if inboundType == "vmess" {
			m.Set("alterId", 0)
		}
		return m

	case "trojan":
		m.Set("name", identifier)
		m.Set("password", item.TrojanPassword)
		return m

	case "shadowsocks", "ss":
		m.Set("name", identifier)
		m.Set("password", item.SSPassword)
		return m

	case "hysteria2", "hy2":
		m.Set("name", identifier)
		pwd := item.Hysteria2Password
		if pwd == "" {
			pwd = item.TrojanPassword
		}
		m.Set("password", pwd)
		return m

	case "tuic":
		m.Set("name", identifier)
		if item.UUID != "" {
			m.Set("uuid", item.UUID)
		}
		pwd := item.TrojanPassword
		if pwd == "" {
			pwd = item.Hysteria2Password
		}
		m.Set("password", pwd)
		return m

	case "naive":
		m.Set("username", identifier)
		m.Set("password", item.TrojanPassword)
		return m

	case "shadowtls":
		m.Set("name", identifier)
		m.Set("password", item.TrojanPassword)
		return m

	default:
		m.Set("name", identifier)
		if item.UUID != "" {
			m.Set("uuid", item.UUID)
		}
		if item.TrojanPassword != "" {
			m.Set("password", item.TrojanPassword)
		}
		return m
	}
}

func addStatsUser(cfg *orderedmap.OrderedMap, identifier string) bool {
	if identifier == "" {
		return false
	}
	experimentalRaw, ok := cfg.Get("experimental")
	if !ok {
		return false
	}
	experimental, ok := toOrderedMap(experimentalRaw)
	if !ok {
		return false
	}
	v2rayRaw, ok := experimental.Get("v2ray_api")
	if !ok {
		return false
	}
	v2ray, ok := toOrderedMap(v2rayRaw)
	if !ok {
		return false
	}
	statsRaw, ok := v2ray.Get("stats")
	if !ok {
		return false
	}
	stats, ok := toOrderedMap(statsRaw)
	if !ok {
		return false
	}

	usersRaw, _ := stats.Get("users")
	var usersArr []string
	if rawArr, ok := usersRaw.([]any); ok {
		usersArr = make([]string, 0, len(rawArr)+1)
		for _, u := range rawArr {
			if s, ok := u.(string); ok && s != "" {
				if s == identifier {
					return false // already present
				}
				usersArr = append(usersArr, s)
			}
		}
	} else if strArr, ok := usersRaw.([]string); ok {
		usersArr = make([]string, 0, len(strArr)+1)
		for _, s := range strArr {
			if s == identifier {
				return false
			}
			usersArr = append(usersArr, s)
		}
	}

	usersArr = append(usersArr, identifier)
	stats.Set("users", usersArr)
	v2ray.Set("stats", stats)
	experimental.Set("v2ray_api", v2ray)
	cfg.Set("experimental", experimental)
	return true
}

func removeStatsUser(cfg *orderedmap.OrderedMap, identifiers ...string) bool {
	if len(identifiers) == 0 {
		return false
	}
	experimentalRaw, ok := cfg.Get("experimental")
	if !ok {
		return false
	}
	experimental, ok := toOrderedMap(experimentalRaw)
	if !ok {
		return false
	}
	v2rayRaw, ok := experimental.Get("v2ray_api")
	if !ok {
		return false
	}
	v2ray, ok := toOrderedMap(v2rayRaw)
	if !ok {
		return false
	}
	statsRaw, ok := v2ray.Get("stats")
	if !ok {
		return false
	}
	stats, ok := toOrderedMap(statsRaw)
	if !ok {
		return false
	}

	usersRaw, _ := stats.Get("users")
	var usersArr []string
	changed := false

	matches := make(map[string]struct{}, len(identifiers))
	for _, id := range identifiers {
		if trimmed := strings.TrimSpace(id); trimmed != "" {
			matches[trimmed] = struct{}{}
		}
	}

	if rawArr, ok := usersRaw.([]any); ok {
		usersArr = make([]string, 0, len(rawArr))
		for _, u := range rawArr {
			if s, ok := u.(string); ok && s != "" {
				if _, match := matches[s]; match {
					changed = true
					continue
				}
				usersArr = append(usersArr, s)
			}
		}
	} else if strArr, ok := usersRaw.([]string); ok {
		usersArr = make([]string, 0, len(strArr))
		for _, s := range strArr {
			if _, match := matches[s]; match {
				changed = true
				continue
			}
			usersArr = append(usersArr, s)
		}
	}

	if changed {
		stats.Set("users", usersArr)
		v2ray.Set("stats", stats)
		experimental.Set("v2ray_api", v2ray)
		cfg.Set("experimental", experimental)
	}
	return changed
}

func setField(v any, key string, val any) any {
	switch m := v.(type) {
	case orderedmap.OrderedMap:
		m.Set(key, val)
		return m
	case *orderedmap.OrderedMap:
		if m != nil {
			m.Set(key, val)
		}
		return m
	case map[string]any:
		m[key] = val
		return m
	default:
		return v
	}
}

func (s *NodeServer) scheduleDebouncedCoreReload() {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()

	log := s.Cfg.LoggerFor("SingboxService")

	if s.reloadTimer == nil {
		s.reloadFirst = time.Now()
		s.reloadTimer = time.AfterFunc(coreReloadDebounceDelay, func() {
			s.executeCoreReload()
		})
		log.Info("Scheduled Sing-box Core reload (10s quiet window)")
		return
	}

	if time.Since(s.reloadFirst) < coreReloadMaxWait {
		if !s.reloadTimer.Stop() {
			select {
			case <-s.reloadTimer.C:
			default:
			}
		}
		s.reloadTimer.Reset(coreReloadDebounceDelay)
		log.Debug("Reset Sing-box Core reload timer (+10s window)")
	} else {
		log.Debug("Sing-box Core reload max wait (30s) reached; timer will fire without extension")
	}
}

func (s *NodeServer) executeCoreReload() {
	s.reloadMu.Lock()
	s.reloadTimer = nil
	s.reloadMu.Unlock()

	log := s.Cfg.LoggerFor("SingboxService")
	log.Info("Debounce window expired: executing Sing-box Core reload/restart...")

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	lifecycle := restartCoreProcessLifecycle(ctx, s.Cfg, s.apiService)
	if lifecycle.failed() {
		log.Error("Sing-box Core reload failed", "error", lifecycle.Error)
	} else {
		log.Info("Sing-box Core reloaded successfully with updated users", "process", lifecycle.ProcessAfter)
	}
}

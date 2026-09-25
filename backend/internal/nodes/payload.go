package users

import (
	"encoding/json"
	"strconv"
	"strings"

	"exodus/internal/proto"
)

const haproxyAllInboundTags = "*"

type deployTaskPayload struct {
	Config       json.RawMessage         `json:"config"`
	Restart      *bool                   `json:"restart,omitempty"`
	ForceRestart *bool                   `json:"force_restart,omitempty"`
	Modules      *deployModulesTaskBlock `json:"modules,omitempty"`
	Internals    *deployInternalsBlock   `json:"internals,omitempty"`
}

type deployInternalsBlock struct {
	Hashes deployHashesBlock `json:"hashes"`
}

type deployHashesBlock struct {
	EmptyConfig      string              `json:"emptyConfig"`
	Inbounds         []deployInboundHash `json:"inbounds"`
	HaproxyUsersHash string              `json:"haproxyUsersHash,omitempty"`
	HaproxyCount     int                 `json:"haproxyCount,omitempty"`
}

type deployInboundHash struct {
	Tag        string `json:"tag"`
	Hash       string `json:"hash"`
	UsersCount int    `json:"usersCount"`
}

type deployModulesTaskBlock struct {
	HaproxyEnabled     bool                     `json:"haproxy_enabled"`
	HaproxyInboundTags []string                 `json:"haproxy_inbound_tags,omitempty"`
	HaproxyUsers       []deployHaproxyUserItem  `json:"haproxy_users,omitempty"`
	IngressFilter  deployIngressFilterBlock `json:"ingress_filter"`
	EgressFilter   deployEgressFilterBlock  `json:"egress_filter"`
	PreStart       deployPreStartBlock      `json:"pre_start"`
}

type deployPreStartBlock struct {
	Enabled        bool                       `json:"enabled"`
	CleanupSockets *deployCleanupSocketsBlock `json:"cleanupSockets,omitempty"`
}

type deployCleanupSocketsBlock struct {
	Enabled bool     `json:"enabled"`
	Files   []string `json:"files,omitempty"`
}

type deployHaproxyUserItem struct {
	Username       string `json:"username"`
	VLESSUUID      string `json:"vless_uuid"`
	TrojanPassword string `json:"trojan_password"`
	NaivePassword  string `json:"naive_password,omitempty"`
	AnytlsPassword string `json:"anytls_password,omitempty"`
}

type deployIngressFilterBlock struct {
	Enabled     bool     `json:"enabled"`
	BlockedIPs  []string `json:"blocked_ips,omitempty"`
	BlockedASNs []int    `json:"blocked_asns,omitempty"`
}

type deployEgressFilterBlock struct {
	Enabled      bool     `json:"enabled"`
	BlockedIPs   []string `json:"blocked_ips,omitempty"`
	BlockedPorts []int    `json:"blocked_ports,omitempty"`
	BlockedASNs  []int    `json:"blocked_asns,omitempty"`
}

type activeNodePluginRuntimeConfig struct {
	IngressFilter struct {
		Enabled     bool     `json:"enabled"`
		BlockedIPs  []string `json:"blockedIps"`
		BlockedASNs []int    `json:"blockedAsns,omitempty"`
	} `json:"ingressFilter"`
	EgressFilter struct {
		Enabled      bool     `json:"enabled"`
		BlockedIPs   []string `json:"blockedIps"`
		BlockedPorts []int    `json:"blockedPorts"`
		BlockedASNs  []int    `json:"blockedAsns,omitempty"`
	} `json:"egressFilter"`
	HaproxyAuth struct {
		Enabled     bool     `json:"enabled"`
		InboundTags []string `json:"inboundTags"`
	} `json:"haproxyAuth"`
	PreStart struct {
		Enabled        bool `json:"enabled"`
		CleanupSockets struct {
			Enabled bool     `json:"enabled"`
			Files   []string `json:"files"`
		} `json:"cleanupSockets"`
	} `json:"preStart"`
}

type deployTarget struct {
	name   string
	uuid   string
	client proto.NodeServiceClient
}

type nodeInboundBinding struct {
	InboundUUID string
	Tag         string
}

type inboundUserCredentials struct {
	ID             int64
	Identifier     string
	Username       string
	VLESSUUID      string
	TrojanPassword string
	SSPassword     string
	NaivePassword  string
	ShadowTLSPass  string
	Hysteria2Pass  string
	AnytlsPassword string
}

func normalizeTagValue(tag string) string {
	return strings.TrimSpace(tag)
}

func normalizeHaproxyInboundTags(raw []string) []string {
	seen := make(map[string]struct{}, len(raw))
	result := make([]string, 0, len(raw))
	for _, item := range raw {
		value := strings.TrimSpace(item)
		if value == "" {
			continue
		}
		if value == haproxyAllInboundTags {
			return []string{haproxyAllInboundTags}
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func haproxyUsesAllInboundTags(tags []string) bool {
	return len(tags) == 1 && tags[0] == haproxyAllInboundTags
}

func parseDeployCoreState(message string) (bool, bool, string) {
	coreReadyRaw, ok := deployMessageValue(message, "core_ready")
	if !ok {
		return false, false, ""
	}
	coreReady, err := strconv.ParseBool(strings.TrimSpace(coreReadyRaw))
	if err != nil {
		return true, false, firstNonEmptyString(message, "Core start result is invalid")
	}
	if coreReady {
		return true, true, ""
	}

	reloadErr, _ := deployMessageValue(message, "reload_error")
	reloadErr = strings.TrimSpace(reloadErr)
	if reloadErr != "" {
		return true, false, reloadErr
	}
	return true, false, firstNonEmptyString(message, "Core failed to start")
}

func deployMessageValue(message, key string) (string, bool) {
	needle := key + "="
	idx := strings.Index(message, needle)
	if idx < 0 {
		return "", false
	}
	value := strings.TrimSpace(message[idx+len(needle):])
	if value == "" {
		return "", true
	}
	if strings.HasPrefix(value, "\"") {
		end := 1
		for end < len(value) {
			if value[end] == '\\' {
				end += 2
				continue
			}
			if value[end] == '"' {
				quoted := value[:end+1]
				unquoted, err := strconv.Unquote(quoted)
				if err == nil {
					return unquoted, true
				}
				return strings.Trim(quoted, "\""), true
			}
			end++
		}
	}
	if space := strings.IndexAny(value, " \t\r\n"); space >= 0 {
		value = value[:space]
	}
	return strings.TrimSpace(value), true
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

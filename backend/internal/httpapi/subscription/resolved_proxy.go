package subscription

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/util"
)

type ResolvedProxyConfig struct {
	FinalRemark      string             `json:"finalRemark"`
	Address          string             `json:"address"`
	Port             int                `json:"port"`
	Protocol         string             `json:"protocol"`
	ProtocolOptions  map[string]any     `json:"protocolOptions"`
	Transport        string             `json:"transport"`
	TransportOptions map[string]any     `json:"transportOptions"`
	Security         string             `json:"security"`
	SecurityOptions  map[string]any     `json:"securityOptions,omitempty"`
	StreamOverrides  map[string]any     `json:"streamOverrides"`
	Mux              any                `json:"mux"`
	ClientOverrides  map[string]any     `json:"clientOverrides"`
	Metadata         ProxyEntryMetadata `json:"metadata"`
}

type ProxyEntryMetadata struct {
	UUID                         string   `json:"uuid"`
	Tags                         []string `json:"tags"`
	ExcludeFromSubscriptionTypes []string `json:"excludeFromSubscriptionTypes"`
	InboundTag                   string   `json:"inboundTag"`
	ConfigProfileUUID            *string  `json:"configProfileUuid"`
	ConfigProfileInboundUUID     *string  `json:"configProfileInboundUuid"`
	IsDisabled                   bool     `json:"isDisabled"`
	IsHidden                     bool     `json:"isHidden"`
	ViewPosition                 int      `json:"viewPosition"`
	Remark                       string   `json:"remark"`
	VlessRouteID                 *int     `json:"vlessRouteId"`
	RawInbound                   any      `json:"rawInbound"`
}

func buildResolvedProxyConfigs(hosts []SubscriptionHost, user SubscriptionUser, settings SubscriptionSettingsParsed, subscriptionURL string) []ResolvedProxyConfig {
	if len(hosts) > 0 {
		hosts = applyShuffle(hosts)
	}
	knownRemarks := make(map[string]int, len(hosts))
	result := make([]ResolvedProxyConfig, 0, len(hosts))
	for _, host := range hosts {
		finalRemark := deduplicateRemark(
			resolveTemplateVariables(strings.TrimSpace(host.Remark), user, settings, subscriptionURL),
			knownRemarks,
		)
		resolved := buildResolvedProxyConfig(host, user, finalRemark)
		if resolved != nil {
			result = append(result, *resolved)
		}
	}
	return result
}

func buildResolvedProxyConfig(host SubscriptionHost, user SubscriptionUser, finalRemark string) *ResolvedProxyConfig {
	protocol := normalizedHostProtocol(host)
	if protocol == "" {
		return nil
	}

	protoKey := protocol
	if protoKey == "hysteria2" || protoKey == "hy2" {
		protoKey = "hysteria"
	}

	switch protoKey {
	case "vless", "trojan", "shadowsocks", "hysteria":
		// supported in ResolvedProxyConfig
	default:
		return nil
	}

	defaults := resolveSingboxInboundDefaults(host)
	cred := effectiveProtocolCredential(host, user)

	protocolOptions := make(map[string]any)
	switch protoKey {
	case "vless":
		flow := ""
		if defaults.flow == "xtls-rprx-vision" || defaults.flow == "xtls-rprx-vision-udp443" {
			flow = defaults.flow
		}
		protocolOptions["encryption"] = "none"
		protocolOptions["id"] = cred
		protocolOptions["flow"] = flow
	case "trojan":
		protocolOptions["password"] = cred
	case "shadowsocks":
		method := extractShadowsocksMethod(host.InboundRaw)
		if method == "" {
			method = "aes-128-gcm"
		}
		protocolOptions["method"] = method
		protocolOptions["password"] = cred
		protocolOptions["uot"] = false
		protocolOptions["uotVersion"] = 0
	case "hysteria":
		protocolOptions["version"] = 2
	}

	transport := "tcp"
	if protoKey == "hysteria" {
		transport = "hysteria"
	} else if defaults.network != "" {
		transport = strings.ToLower(defaults.network)
	} else if host.InboundNetwork != nil && *host.InboundNetwork != "" {
		transport = strings.ToLower(*host.InboundNetwork)
	}

	transportOptions := make(map[string]any)
	switch transport {
	case "ws":
		transportOptions["host"] = host.Host
		transportOptions["path"] = host.Path
		transportOptions["headers"] = nil
		transportOptions["heartbeatPeriod"] = nil
	case "grpc":
		transportOptions["authority"] = host.Host
		transportOptions["serviceName"] = host.Path
		transportOptions["multiMode"] = false
	case "httpupgrade":
		transportOptions["host"] = host.Host
		transportOptions["path"] = host.Path
		transportOptions["headers"] = nil
	case "xhttp":
		transportOptions["path"] = host.Path
		transportOptions["host"] = host.Host
		transportOptions["mode"] = "auto"
		transportOptions["extra"] = nil
	case "hysteria":
		transportOptions["version"] = 2
		transportOptions["auth"] = cred
	default:
		transport = "tcp"
		transportOptions["header"] = nil
	}

	security := "none"
	var securityOptions map[string]any
	if defaults.security != "" {
		security = strings.ToLower(defaults.security)
	} else if host.InboundSecurity != nil && *host.InboundSecurity != "" {
		security = strings.ToLower(*host.InboundSecurity)
	} else if strings.EqualFold(host.SecurityLayer, "TLS") {
		security = "tls"
	}

	switch security {
	case "tls":
		var alpn *string
		if defaults.alpn != "" {
			v := defaults.alpn
			alpn = &v
		} else if host.ALPN != nil && *host.ALPN != "" {
			alpn = host.ALPN
		}

		fp := "chrome"
		if defaults.fingerprint != "" {
			fp = defaults.fingerprint
		} else if host.Fingerprint != nil && *host.Fingerprint != "" {
			fp = *host.Fingerprint
		}

		finalSNI := resolveFinalServerName(host, defaults.sni)
		var serverName *string
		if finalSNI != "" {
			serverName = &finalSNI
		}

		securityOptions = map[string]any{
			"pinnedPeerCertSha256":    host.PinnedPeerCertSha256,
			"verifyPeerCertByName":    host.VerifyPeerCertByName,
			"alpn":                    alpn,
			"enableSessionResumption": false,
			"fingerprint":             fp,
			"serverName":              serverName,
			"echConfigList":           nil,
			"echForceQuery":           nil,
			"echSockopt":              nil,
		}
	case "reality":
		fp := "chrome"
		if defaults.fingerprint != "" {
			fp = defaults.fingerprint
		} else if host.Fingerprint != nil && *host.Fingerprint != "" {
			fp = *host.Fingerprint
		}

		serverName := resolveFinalServerName(host, defaults.sni)

		var shortID *string
		if defaults.shortID != "" {
			v := defaults.shortID
			shortID = &v
		}

		securityOptions = map[string]any{
			"fingerprint":   fp,
			"publicKey":     defaults.publicKey,
			"shortId":       shortID,
			"serverName":    serverName,
			"spiderX":       nil,
			"mldsa65Verify": nil,
		}
	default:
		security = "none"
		securityOptions = nil
	}

	remark := finalRemark
	if remark == "" {
		remark = host.Address
	}

	tags := []string{}
	if host.Tag != nil && *host.Tag != "" {
		tags = append(tags, *host.Tag)
	}

	inboundTag := ""
	if host.InboundTag != nil {
		inboundTag = *host.InboundTag
	}

	var sockoptMap any
	if host.SockoptParams != nil && strings.TrimSpace(*host.SockoptParams) != "" {
		_ = json.Unmarshal([]byte(*host.SockoptParams), &sockoptMap)
	}

	var muxMap any
	if host.MuxParams != nil && strings.TrimSpace(*host.MuxParams) != "" {
		_ = json.Unmarshal([]byte(*host.MuxParams), &muxMap)
	}

	var rawInboundMap any
	if len(host.InboundRaw) > 0 {
		_ = json.Unmarshal(host.InboundRaw, &rawInboundMap)
	}

	var finalMaskMap any
	if host.FinalMask != nil && strings.TrimSpace(*host.FinalMask) != "" {
		_ = json.Unmarshal([]byte(*host.FinalMask), &finalMaskMap)
	}

	return &ResolvedProxyConfig{
		FinalRemark:      remark,
		Address:          host.Address,
		Port:             host.Port,
		Protocol:         protoKey,
		ProtocolOptions:  protocolOptions,
		Transport:        transport,
		TransportOptions: transportOptions,
		Security:         security,
		SecurityOptions:  securityOptions,
		StreamOverrides: map[string]any{
			"finalMask": finalMaskMap,
			"sockopt":   sockoptMap,
		},
		Mux: muxMap,
		ClientOverrides: map[string]any{
			"shuffleHost":       host.ShuffleHost,
			"mihomoX25519":      host.MihomoX25519,
			"mihomoIpVersion":   host.MihomoIPVersion,
			"serverDescription": host.ServerDescription,
			"xrayJsonTemplate":  nil,
		},
		Metadata: ProxyEntryMetadata{
			UUID:                         host.UUID,
			Tags:                         tags,
			ExcludeFromSubscriptionTypes: host.ExcludeFromSubscriptionTypes,
			InboundTag:                   inboundTag,
			ConfigProfileUUID:            host.ConfigProfileUUID,
			ConfigProfileInboundUUID:     host.ConfigProfileInboundUUID,
			IsDisabled:                   host.IsDisabled,
			IsHidden:                     host.IsHidden,
			ViewPosition:                 host.ViewPosition,
			Remark:                       host.Remark,
			VlessRouteID:                 nil,
			RawInbound:                   rawInboundMap,
		},
	}
}

type RawSubscriptionResponse struct {
	User                 ExtendedUserDTO       `json:"user"`
	ConvertedUserInfo    ConvertedUserInfoDTO  `json:"convertedUserInfo"`
	Headers              map[string]string     `json:"headers"`
	ResolvedProxyConfigs []ResolvedProxyConfig `json:"resolvedProxyConfigs"`
}

type ConvertedUserInfoDTO struct {
	DaysLeft            int64  `json:"daysLeft"`
	TrafficLimit        string `json:"trafficLimit"`
	TrafficUsed         string `json:"trafficUsed"`
	LifetimeTrafficUsed string `json:"lifetimeTrafficUsed"`
	HwidCheckup         any    `json:"hwidCheckup"`
}

type ExtendedUserDTO struct {
	ID                     int64              `json:"id"`
	ShortUUID              string             `json:"shortUuid"`
	Username               string             `json:"username"`
	Status                 string             `json:"status"`
	TrafficLimitBytes      int64              `json:"trafficLimitBytes"`
	TrafficLimitStrategy   string             `json:"trafficLimitStrategy"`
	ExpireAt               time.Time          `json:"expireAt"`
	TelegramID             *int64             `json:"telegramId"`
	Email                  *string            `json:"email"`
	Description            *string            `json:"description"`
	Tag                    *string            `json:"tag"`
	HwidDeviceLimit        *int               `json:"hwidDeviceLimit"`
	ExternalSquadUUID      *string            `json:"externalSquadUuid"`
	TrojanPassword         string             `json:"trojanPassword"`
	VlessUUID              string             `json:"vlessUuid"`
	SSPassword             string             `json:"ssPassword"`
	NaivePassword          string             `json:"naivePassword"`
	ShadowtlsPassword      string             `json:"shadowtlsPassword"`
	Hysteria2Password      string             `json:"hysteria2Password"`
	AnytlsPassword         string             `json:"anytlsPassword"`
	LastTriggeredThreshold int                `json:"lastTriggeredThreshold"`
	SubRevokedAt           *time.Time         `json:"subRevokedAt"`
	LastTrafficResetAt     *time.Time         `json:"lastTrafficResetAt"`
	CreatedAt              time.Time          `json:"createdAt"`
	UpdatedAt              time.Time          `json:"updatedAt"`
	SubscriptionURL        string             `json:"subscriptionUrl"`
	ActiveInternalSquads   []InternalSquadDTO `json:"activeInternalSquads"`
	UserTraffic            UserTrafficDTO     `json:"userTraffic"`
}

type InternalSquadDTO struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
}

type UserTrafficDTO struct {
	UsedTrafficBytes         int64      `json:"usedTrafficBytes"`
	LifetimeUsedTrafficBytes int64      `json:"lifetimeUsedTrafficBytes"`
	OnlineAt                 *time.Time `json:"onlineAt"`
	FirstConnectedAt         *time.Time `json:"firstConnectedAt"`
	LastConnectedNodeUUID    *string    `json:"lastConnectedNodeUuid"`
}

func buildRawSubscriptionResponse(
	ctx context.Context,
	db *pgxpool.Pool,
	user SubscriptionUser,
	settings SubscriptionSettingsParsed,
	hosts []SubscriptionHost,
	subURL string,
) RawSubscriptionResponse {
	activeSquads := []InternalSquadDTO{}
	if db != nil {
		rows, err := db.Query(ctx, `
			SELECT isq.uuid, isq.name
			FROM internal_squads isq
			JOIN user_internal_squads uis ON uis.internal_squad_uuid = isq.uuid
			WHERE uis.user_uuid = $1
			ORDER BY isq.name ASC
		`, user.UUID)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var sq InternalSquadDTO
				if scanErr := rows.Scan(&sq.UUID, &sq.Name); scanErr == nil {
					activeSquads = append(activeSquads, sq)
				}
			}
			_ = rows.Err()
		}
	}

	daysLeft := int64(0)
	if !user.ExpireAt.IsZero() && user.ExpireAt.After(time.Now()) {
		daysLeft = int64(time.Until(user.ExpireAt).Hours() / 24)
	}

	limitPretty := "0 B"
	if user.TrafficLimitBytes > 0 {
		limitPretty = util.FormatBytes(user.TrafficLimitBytes)
	}

	convertedUserInfo := ConvertedUserInfoDTO{
		DaysLeft:            daysLeft,
		TrafficLimit:        limitPretty,
		TrafficUsed:         util.FormatBytes(user.UsedTrafficBytes),
		LifetimeTrafficUsed: util.FormatBytes(user.LifetimeUsedBytes),
		HwidCheckup:         nil,
	}

	strategy := user.TrafficLimitStrategy
	if strategy == "" {
		strategy = "NO_RESET"
	}

	status := user.Status
	if status == "" {
		status = "ACTIVE"
	}

	createdAt := user.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	updatedAt := createdAt

	expireAt := user.ExpireAt
	if expireAt.IsZero() {
		expireAt = time.Date(2099, 12, 31, 23, 59, 59, 0, time.UTC)
	}

	headers := buildResponseHeaders(user, settings, "application/json", subURL)

	resolvedProxies := buildResolvedProxyConfigs(hosts, user, settings, subURL)

	return RawSubscriptionResponse{
		User: ExtendedUserDTO{
			ID:                     user.ID,
			ShortUUID:              user.ShortUUID,
			Username:               user.Username,
			Status:                 status,
			TrafficLimitBytes:      user.TrafficLimitBytes,
			TrafficLimitStrategy:   strategy,
			ExpireAt:               expireAt,
			TelegramID:             user.TelegramID,
			Email:                  user.Email,
			Description:            user.Description,
			Tag:                    user.Tag,
			HwidDeviceLimit:        user.HwidDeviceLimit,
			ExternalSquadUUID:      user.ExternalSquadUUID,
			TrojanPassword:         user.TrojanPassword,
			VlessUUID:              user.VlessUUID,
			SSPassword:             user.SSPassword,
			NaivePassword:          user.NaivePassword,
			ShadowtlsPassword:      user.ShadowtlsPassword,
			Hysteria2Password:      user.Hysteria2Password,
			AnytlsPassword:         user.AnytlsPassword,
			LastTriggeredThreshold: user.LastTriggeredThreshold,
			SubRevokedAt:           user.SubRevokedAt,
			LastTrafficResetAt:     user.LastTrafficResetAt,
			CreatedAt:              createdAt,
			UpdatedAt:              updatedAt,
			SubscriptionURL:        subURL,
			ActiveInternalSquads:   activeSquads,
			UserTraffic: UserTrafficDTO{
				UsedTrafficBytes:         user.UsedTrafficBytes,
				LifetimeUsedTrafficBytes: user.LifetimeUsedBytes,
				OnlineAt:                 user.OnlineAt,
				FirstConnectedAt:         user.FirstConnectedAt,
				LastConnectedNodeUUID:    user.LastConnectedNodeUUID,
			},
		},
		ConvertedUserInfo:    convertedUserInfo,
		Headers:              headers,
		ResolvedProxyConfigs: resolvedProxies,
	}
}

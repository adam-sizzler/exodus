package subscription

/*
CROSS-CUTTING RULES / НЕЯВНЫЕ ЗАВИСИМОСТИ:
1. Использование host.Path:
   Значение `host.Path` используется как для WebSocket (`ws.path`), так и для gRPC (`grpc.service_name`).
   В билдерах (Mihomo, Xray, Singbox) при проверке пути gRPC нужно использовать `host.Path`.
2. Обработка Reality:
   Для совместимости с xray/mihomo билдерами, Reality принудительно схлопывается в `"tls"`.
*/

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/subscriptionresponserules"
	"exodus/internal/httpapi/subscriptionsettings"
	"exodus/internal/logger"
	"exodus/internal/util"
)

type RenderService struct {
	db           *pgxpool.Pool
	backgroundDB *pgxpool.Pool
	cfg          *config.BackendConfig
}

func NewRenderService(db, backgroundDB *pgxpool.Pool, cfg *config.BackendConfig) *RenderService {
	return &RenderService{db: db, backgroundDB: backgroundDB, cfg: cfg}
}

func applyExternalSquadOverrides(base SubscriptionSettingsParsed, overrides *ExternalSquadOverrides) SubscriptionSettingsParsed {
	if overrides == nil {
		return base
	}

	if overrides.SubscriptionSettings != nil {
		base.Raw = mergeSubscriptionSettings(base.Raw, *overrides.SubscriptionSettings)

		if strings.TrimSpace(base.Raw.ResponseRules) != "" {
			var rules subscriptionresponserules.Config
			if err := json.Unmarshal([]byte(base.Raw.ResponseRules), &rules); err == nil {
				base.ResponseRules = &rules
			}
		}
	}

	if len(overrides.HostOverrides) > 0 {
		base.HostOverrides = overrides.HostOverrides
	}

	if len(overrides.ResponseHeaders) > 0 {
		if base.ResponseHeaders == nil {
			base.ResponseHeaders = make(map[string]string)
		}
		for k, v := range overrides.ResponseHeaders {
			base.ResponseHeaders[k] = v
		}
	}
	if len(overrides.ResponseHeadersRemove) > 0 {
		base.ResponseHeadersRemove = append(base.ResponseHeadersRemove, overrides.ResponseHeadersRemove...)
	}

	if overrides.HwidSettings != nil {
		base.HwidSettings = *overrides.HwidSettings
	}

	if overrides.CustomRemarks != nil {
		base.CustomRemarks = *overrides.CustomRemarks
	}

	return base
}

// mergeSubscriptionSettings applies a squad-level subscription_settings override onto the
// base settings. Matches upstream EXodus's ExternalSquadSubscriptionSettingsSchema, which
// only picks serveJsonAtBaseSubscription/isShowCustomRemarks/randomizeHosts — everything else
// (profile title, announce, routing, support link, update interval, webpage url) is squad-level
// customized exclusively via responseHeadersAdd/responseHeadersRemove, not via this struct.
func mergeSubscriptionSettings(base, squad subscriptionsettings.SubscriptionSettings) subscriptionsettings.SubscriptionSettings {
	base.ServeJSONAtBaseSubscription = squad.ServeJSONAtBaseSubscription
	base.IsShowCustomRemarks = squad.IsShowCustomRemarks
	base.RandomizeHosts = squad.RandomizeHosts
	return base
}

func (s *RenderService) RenderUserSubscription(
	ctx context.Context,
	user SubscriptionUser,
	userAgent string,
	requestedType string,
	requestIP string,
	hwid *HwidHeaders,
) ([]byte, string, map[string]string, error) {
	settings, err := loadSubscriptionSettings(ctx, s.db, s.cfg)
	if err != nil {
		return nil, "", nil, err
	}

	squadOverrides, _ := loadExternalSquadOverrides(ctx, s.db, ptrString(user.ExternalSquadUUID), s.cfg)
	settings = applyExternalSquadOverrides(settings, squadOverrides)

	reqType := strings.ToUpper(strings.TrimSpace(requestedType))
	var matchedRuleName string
	var matchedRuleMods *subscriptionresponserules.RuleModifications

	h := http.Header{}
	if userAgent != "" {
		h.Set("User-Agent", userAgent)
	}
	if settings.ResponseRules != nil {
		matchRes := subscriptionresponserules.MatchRulesDetailed(
			settings.ResponseRules,
			h,
			requestedType,
			func(s string) string { return s },
			defaultResponseType,
		)
		if matchRes.Matched {
			if reqType == "" {
				reqType = matchRes.ResponseType
			}
			if matchRes.MatchedRule != nil {
				matchedRuleName = matchRes.MatchedRule.Name
				matchedRuleMods = matchRes.MatchedRule.ResponseModifications
			}
		}
	}
	if reqType == "" {
		reqType = defaultResponseType
	}

	switch reqType {
	case responseTypeBlock:
		return nil, "", nil, ErrBlocked
	case "STATUS_CODE_404":
		return nil, "", nil, ErrNotFound
	case "STATUS_CODE_451":
		return nil, "", nil, ErrUnavailableForLegalReasons
	}

	// 1. Respond with remarks (SRR respondWithRemarks)
	var earlyExitRemarks []string
	// respondedWithRuleRemarks tracks whether the SRR rule itself asked for
	// remarks (as opposed to disabled/expired/limited-status remarks below).
	// Matches upstream: srrContext.respondWithRemarks triggers an
	// unconditional early return in subscription.service.ts, before the
	// HWID check ever runs — regardless of what isShowCustomRemarks ends up
	// substituting it with. That early-return-before-HWID behavior must be
	// preserved here too, not just the remarks-selection logic.
	respondedWithRuleRemarks := matchedRuleMods != nil && len(matchedRuleMods.RespondWithRemarks) > 0
	if respondedWithRuleRemarks {
		if settings.Raw.IsShowCustomRemarks {
			earlyExitRemarks = matchedRuleMods.RespondWithRemarks
		} else if len(settings.CustomRemarks.EmptyHosts) > 0 {
			earlyExitRemarks = settings.CustomRemarks.EmptyHosts
		} else {
			return nil, "", nil, ErrNoHosts
		}
	} else if settings.Raw.IsShowCustomRemarks {
		if user.Status == "DISABLED" {
			earlyExitRemarks = settings.CustomRemarks.DisabledUsers
		} else if user.Status == "EXPIRED" || (!user.ExpireAt.IsZero() && user.ExpireAt.Before(time.Now())) {
			earlyExitRemarks = settings.CustomRemarks.ExpiredUsers
		} else if user.Status == "LIMITED" {
			earlyExitRemarks = settings.CustomRemarks.LimitedUsers
		}
	}

	if len(earlyExitRemarks) == 0 && (user.Status != "ACTIVE" || (!user.ExpireAt.IsZero() && user.ExpireAt.Before(time.Now()))) {
		return nil, "", nil, ErrUserDisabled
	}

	// 2. Fetch hosts and apply excludeHostsByTags
	var hosts []SubscriptionHost
	if len(earlyExitRemarks) > 0 {
		hosts = createFallbackRemarkHosts(earlyExitRemarks)
	} else {
		withHidden := reqType == responseTypeXrayJSON || reqType == responseTypeMihomo || reqType == responseTypeStash
		userHosts, err := getHostsForUserWithOptions(ctx, s.db, user, false, withHidden)
		if err != nil {
			return nil, "", nil, err
		}

		if matchedRuleMods != nil && len(matchedRuleMods.ExcludeHostsByTags) > 0 {
			excludeSet := make(map[string]struct{}, len(matchedRuleMods.ExcludeHostsByTags))
			for _, tag := range matchedRuleMods.ExcludeHostsByTags {
				trimmed := strings.TrimSpace(tag)
				if trimmed != "" {
					excludeSet[trimmed] = struct{}{}
				}
			}
			filtered := make([]SubscriptionHost, 0, len(userHosts))
			for _, host := range userHosts {
				excluded := false
				for _, tag := range host.Tags {
					if _, ok := excludeSet[strings.TrimSpace(tag)]; ok {
						excluded = true
						break
					}
				}
				if !excluded {
					filtered = append(filtered, host)
				}
			}
			userHosts = filtered
		}

		if len(userHosts) == 0 {
			if settings.Raw.IsShowCustomRemarks && len(settings.CustomRemarks.EmptyHosts) > 0 {
				hosts = createFallbackRemarkHosts(settings.CustomRemarks.EmptyHosts)
			} else {
				return nil, "", nil, ErrNoHosts
			}
		} else {
			hosts = userHosts
		}
	}

	if len(settings.HostOverrides) > 0 && len(hosts) > 0 {
		hosts = applyHostOverrides(hosts, settings.HostOverrides)
	}

	if settings.Raw.RandomizeHosts && len(hosts) > 0 {
		rand.Shuffle(len(hosts), func(i, j int) {
			hosts[i], hosts[j] = hosts[j], hosts[i]
		})
	}

	if len(hosts) > 0 {
		hosts = applyShuffle(hosts)
	}

	subscriptionURL := resolveSubscriptionURL(ctx, s.db, user, settings)

	// 3. HWID check (supports disableHwidCheck)
	var hwidExtraHeaders map[string]string
	hwidSoftLimitHit := false
	disableHwidCheck := matchedRuleMods != nil && matchedRuleMods.DisableHwidCheck

	// respondedWithRuleRemarks already means "return before this point" in
	// upstream, so the whole HWID check (and the device-recording fallback
	// below) is skipped for it too — not just the host/tag/override steps
	// above.
	if !respondedWithRuleRemarks {
		if settings.HwidSettings.Enabled && !disableHwidCheck {
			result, err := checkHwidDeviceLimit(ctx, s.db, user, hwid, settings.HwidSettings)
			if err != nil {
				return nil, "", nil, ErrHwidCheckFailed
			}

			if !result.Allowed {
				hwidSoftLimitHit = true
				hwidExtraHeaders = map[string]string{"x-hwid-limit": "true"}

				if result.MaxDeviceReached && settings.HwidSettings.MaxDevicesAnnounce != nil &&
					*settings.HwidSettings.MaxDevicesAnnounce != "" {
					hwidExtraHeaders["announce"] = formatTemplateValue(
						"exEncodeBase64:"+*settings.HwidSettings.MaxDevicesAnnounce,
						user, settings, subscriptionURL,
					)
				}
				if result.HwidNotSupported {
					hwidExtraHeaders["x-hwid-not-supported"] = "true"
				}
				if result.MaxDeviceReached {
					hwidExtraHeaders["x-hwid-max-devices-reached"] = "true"
				}

				hosts = nil
				if settings.Raw.IsShowCustomRemarks {
					if result.MaxDeviceReached && len(settings.CustomRemarks.HWIDMaxDevicesExceeded) > 0 {
						hosts = createFallbackRemarkHosts(settings.CustomRemarks.HWIDMaxDevicesExceeded)
					} else if result.HwidNotSupported && len(settings.CustomRemarks.HWIDNotSupported) > 0 {
						hosts = createFallbackRemarkHosts(settings.CustomRemarks.HWIDNotSupported)
					}
				}
			}
		} else if hwid != nil {
			_ = enqueueOrUpsertHwidUserDevice(ctx, s.db, user.ID, *hwid)
		}
	}

	if len(hosts) > 0 {
		resolveHostRemarks(hosts, user, settings, subscriptionURL)
	}

	// 4. Auto-upgrade to XRAY_JSON if serveJsonAtBaseSubscription enabled
	ignoreServeJSON := matchedRuleMods != nil && matchedRuleMods.IgnoreServeJsonAtBaseSubscription
	if reqType == defaultResponseType &&
		settings.Raw.ServeJSONAtBaseSubscription &&
		!ignoreServeJSON &&
		isJSONSubscriptionFallbackSupported(userAgent) {
		reqType = responseTypeXrayJSON
	}

	updateSubscriptionRequest(ctx, s.backgroundDB, user.UUID, user.ID, userAgent, requestIP, reqType, matchedRuleName)

	if s.cfg != nil && s.cfg.Logger != nil {
		s.cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceHTTP).Debug(
			"Rendering user subscription",
			"username", user.Username,
			"short_uuid", user.ShortUUID,
			"user_agent", userAgent,
			"client_ip", requestIP,
			"requested_type", requestedType,
			"resolved_type", reqType,
			"used_traffic_bytes", user.UsedTrafficBytes,
			"traffic_limit_bytes", user.TrafficLimitBytes,
			"active_hosts", len(hosts),
		)
	}

	// 5. Subscription template override
	templateType := strings.ToLower(reqType)
	var templateContent []byte
	templateLoaded := false

	if matchedRuleMods != nil && matchedRuleMods.SubscriptionTemplate != nil && strings.TrimSpace(*matchedRuleMods.SubscriptionTemplate) != "" {
		tplType, tplData, err := getSubscriptionTemplateByName(ctx, s.db, strings.TrimSpace(*matchedRuleMods.SubscriptionTemplate))
		if err == nil && len(tplData) > 0 && strings.EqualFold(tplType, reqType) {
			templateContent = tplData
			templateLoaded = true
		}
	}
	if !templateLoaded {
		templateContent, _ = getSubscriptionTemplate(ctx, s.db, templateType)
	}

	// 6. Extended clients & generator settings
	var additionalExtendedRegex []string
	if matchedRuleMods != nil {
		additionalExtendedRegex = matchedRuleMods.AdditionalExtendedClientsRegex
	}
	settings.IsExtendedClient = isExtendedClient(userAgent, additionalExtendedRegex)
	if matchedRuleMods != nil && matchedRuleMods.IgnoreHostXrayJsonTemplate {
		settings.IgnoreHostXrayJsonTemplate = true
	}
	settings.CustomTemplateLoader = func(uuidStr string) ([]byte, error) {
		templateType, data, err := getSubscriptionTemplateByUUID(ctx, s.db, uuidStr)
		if err != nil {
			return nil, err
		}
		// Same defensive check as the whole-response subscriptionTemplate
		// override (getSubscriptionTemplateWithOverride): a UUID that
		// resolves to a template of the wrong type (e.g. a host's
		// xrayJsonTemplateUuid pointing at a SINGBOX template — both types
		// share the template_json column, so a type mix-up wouldn't
		// otherwise fail to load) must not be used; the caller falls back
		// to the default template on any error here.
		if !strings.EqualFold(templateType, responseTypeXrayJSON) {
			return nil, fmt.Errorf("template %s is not of type %s (got %s)", uuidStr, responseTypeXrayJSON, templateType)
		}
		return data, nil
	}

	var outputContent string
	var contentType string

	if hwidSoftLimitHit && len(hosts) == 0 {
		outputContent = ""
		contentType = "text/plain; charset=utf-8"
	} else {
		switch reqType {
		case responseTypeClash:
			gen := NewClashGenerator(s.cfg)
			out, err := gen.Generate(templateContent, user, hosts, settings)
			if err != nil {
				return nil, "", nil, err
			}
			outputContent = out
			contentType = "text/yaml; charset=utf-8"
		case responseTypeMihomo, responseTypeStash:
			gen := NewMihomoGenerator(s.cfg)
			out, err := gen.Generate(templateContent, user, hosts, settings, reqType)
			if err != nil {
				return nil, "", nil, err
			}
			outputContent = out
			contentType = "text/yaml; charset=utf-8"
		case responseTypeSingbox:
			gen := NewSingboxGenerator(s.cfg)
			out, err := gen.Generate(templateContent, user, hosts, settings)
			if err != nil {
				return nil, "", nil, err
			}
			outputContent = out
			contentType = "application/json; charset=utf-8"
		case responseTypeXrayJSON:
			gen := NewXrayGenerator(s.cfg)
			out, err := gen.GenerateJSON(templateContent, user, hosts, settings)
			if err != nil {
				return nil, "", nil, err
			}
			outputContent = out
			contentType = "application/json; charset=utf-8"
		default:
			gen := NewXrayGenerator(s.cfg)
			links, err := gen.GenerateLinks(user, hosts, settings)
			if err != nil {
				return nil, "", nil, err
			}
			outputContent = base64.StdEncoding.EncodeToString([]byte(strings.Join(links, "\n")))
			contentType = "text/plain; charset=utf-8"
		}
	}

	// 7. Response headers (early headers, hwid extra, late headers)
	responseHeaders := buildResponseHeaders(user, settings, contentType, subscriptionURL)

	for k, v := range hwidExtraHeaders {
		responseHeaders[k] = v
	}

	if matchedRuleMods != nil && len(matchedRuleMods.Headers) > 0 {
		for _, hdr := range matchedRuleMods.Headers {
			key := strings.ToLower(strings.TrimSpace(hdr.Key))
			if key == "" {
				continue
			}
			if matchedRuleMods.ApplyHeadersToEnd {
				responseHeaders[key] = hdr.Value
			} else if _, exists := responseHeaders[key]; !exists {
				responseHeaders[key] = hdr.Value
			}
		}
	}

	// 8. Encryption (age1 / age1pq1)
	outputBytes := []byte(outputContent)
	if matchedRuleMods != nil && matchedRuleMods.Encryption != nil {
		encrypted, err := encryptResponseBody(outputBytes, matchedRuleMods.Encryption.Method, matchedRuleMods.Encryption.Key)
		if err != nil {
			return nil, "", nil, fmt.Errorf("srr response encryption failed: %w", err)
		}
		outputBytes = []byte(encrypted)
		contentType = "text/plain; charset=utf-8"
		responseHeaders["content-type"] = contentType
	}

	return outputBytes, contentType, responseHeaders, nil
}

func (s *RenderService) buildSubscriptionInfoResponse(
	user SubscriptionUser,
	settings SubscriptionSettingsParsed,
	hosts []SubscriptionHost,
) SubscriptionInfoResponse {
	domain := strings.TrimSpace(settings.Raw.Address)
	if domain == "" {
		domain = "panel.exodus.dev"
	}
	scheme := strings.TrimSpace(settings.Raw.APISchema)
	if scheme == "" {
		scheme = "https"
	}
	apiPath := strings.Trim(strings.TrimSpace(settings.Raw.APIPath), "/")
	if apiPath == "" {
		apiPath = "api/sub"
	}
	subURL := fmt.Sprintf("%s://%s/%s/%s", scheme, domain, apiPath, user.ShortUUID)

	resolveHostRemarks(hosts, user, settings, subURL)

	links, ssConfLinks := buildSubscriptionLinks(hosts, user)

	usedPretty := util.FormatBytes(user.UsedTrafficBytes)
	limitPretty := "0"
	if user.TrafficLimitBytes > 0 {
		limitPretty = util.FormatBytes(user.TrafficLimitBytes)
	}
	lifetimePretty := util.FormatBytes(user.LifetimeUsedBytes)

	daysLeft := 0
	expiresAt := user.ExpireAt
	if expiresAt.IsZero() || expiresAt.Year() <= 1 {
		expiresAt = time.Date(2099, 12, 31, 23, 59, 59, 0, time.UTC)
	} else if expiresAt.After(time.Now()) {
		daysLeft = int(time.Until(expiresAt).Hours() / 24)
	}

	return SubscriptionInfoResponse{
		IsFound: true,
		User: SubscriptionInfoUser{
			ShortUUID:                user.ShortUUID,
			DaysLeft:                 daysLeft,
			TrafficUsed:              usedPretty,
			TrafficLimit:             limitPretty,
			LifetimeTrafficUsed:      lifetimePretty,
			TrafficUsedBytes:         fmt.Sprintf("%d", user.UsedTrafficBytes),
			TrafficLimitBytes:        fmt.Sprintf("%d", user.TrafficLimitBytes),
			LifetimeTrafficUsedBytes: fmt.Sprintf("%d", user.LifetimeUsedBytes),
			Username:                 user.Username,
			ExpiresAt:                expiresAt,
			IsActive:                 user.Status == "ACTIVE",
			UserStatus:               user.Status,
			TrafficLimitStrategy:     user.TrafficLimitStrategy,
		},
		Links:           links,
		SSConfLinks:     ssConfLinks,
		SubscriptionURL: subURL,
	}
}

func sortHosts(hosts []SubscriptionHost) {
	sort.Slice(hosts, func(i, j int) bool {
		if hosts[i].ViewPosition != hosts[j].ViewPosition {
			return hosts[i].ViewPosition < hosts[j].ViewPosition
		}
		return hosts[i].Remark < hosts[j].Remark
	})
}

func createFallbackRemarkHosts(remarks []string) []SubscriptionHost {
	result := make([]SubscriptionHost, 0, len(remarks))
	for i, remark := range remarks {
		if host, ok := parseFallbackHostFromRemark(remark, i+1); ok {
			result = append(result, host)
			continue
		}
		vlessType := "vless"
		noneSec := "none"
		tcpNet := "tcp"
		result = append(result, SubscriptionHost{
			UUID:            fmt.Sprintf("00000000-0000-0000-0000-00000000%04d", i+1),
			Remark:          remark,
			Address:         "0.0.0.0",
			Port:            1,
			InboundType:     &vlessType,
			InboundSecurity: &noneSec,
			InboundNetwork:  &tcpNet,
		})
	}
	return result
}

func parseFallbackHostFromRemark(remark string, index int) (SubscriptionHost, bool) {
	trimmed := strings.TrimSpace(remark)
	if !strings.HasPrefix(trimmed, "{") {
		return SubscriptionHost{}, false
	}

	var raw map[string]any
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return SubscriptionHost{}, false
	}

	address, _ := raw["address"].(string)
	if address == "" {
		return SubscriptionHost{}, false
	}

	var port int
	switch p := raw["port"].(type) {
	case float64:
		port = int(p)
	case int:
		port = p
	case string:
		port, _ = strconv.Atoi(p)
	}
	if port <= 0 || port > 65535 {
		port = 443
	}

	finalRemark := ""
	if r, ok := raw["finalRemark"].(string); ok && r != "" {
		finalRemark = r
	} else if r, ok := raw["remark"].(string); ok && r != "" {
		finalRemark = r
	} else if meta, ok := raw["metadata"].(map[string]any); ok {
		if r, ok := meta["remark"].(string); ok && r != "" {
			finalRemark = r
		}
	}
	if finalRemark == "" {
		finalRemark = address
	}

	protocol := "vless"
	if p, ok := raw["protocol"].(string); ok && p != "" {
		protocol = p
	} else if p, ok := raw["inboundType"].(string); ok && p != "" {
		protocol = p
	} else if p, ok := raw["type"].(string); ok && p != "" {
		protocol = p
	}

	network := "tcp"
	if n, ok := raw["transport"].(string); ok && n != "" {
		network = n
	} else if n, ok := raw["network"].(string); ok && n != "" {
		network = n
	} else if n, ok := raw["inboundNetwork"].(string); ok && n != "" {
		network = n
	}

	security := "none"
	if s, ok := raw["security"].(string); ok && s != "" {
		security = s
	} else if s, ok := raw["inboundSecurity"].(string); ok && s != "" {
		security = s
	} else if s, ok := raw["securityLayer"].(string); ok && s != "" {
		security = s
	}

	var path *string
	if p, ok := raw["path"].(string); ok && p != "" {
		path = &p
	} else if to, ok := raw["transportOptions"].(map[string]any); ok {
		if p, ok := to["path"].(string); ok && p != "" {
			path = &p
		} else if p, ok := to["serviceName"].(string); ok && p != "" {
			path = &p
		}
	}

	var hostHeader *string
	if h, ok := raw["host"].(string); ok && h != "" {
		hostHeader = &h
	} else if to, ok := raw["transportOptions"].(map[string]any); ok {
		if h, ok := to["host"].(string); ok && h != "" {
			hostHeader = &h
		} else if h, ok := to["authority"].(string); ok && h != "" {
			hostHeader = &h
		}
	}

	var sni *string
	if s, ok := raw["sni"].(string); ok && s != "" {
		sni = &s
	} else if so, ok := raw["securityOptions"].(map[string]any); ok {
		if s, ok := so["serverName"].(string); ok && s != "" {
			sni = &s
		} else if s, ok := so["sni"].(string); ok && s != "" {
			sni = &s
		}
	}

	var alpn *string
	if a, ok := raw["alpn"].(string); ok && a != "" {
		alpn = &a
	} else if so, ok := raw["securityOptions"].(map[string]any); ok {
		if a, ok := so["alpn"].(string); ok && a != "" {
			alpn = &a
		} else if alist, ok := so["alpn"].([]any); ok {
			parts := make([]string, 0, len(alist))
			for _, item := range alist {
				if s, ok := item.(string); ok && s != "" {
					parts = append(parts, s)
				}
			}
			if len(parts) > 0 {
				joined := strings.Join(parts, ",")
				alpn = &joined
			}
		}
	}

	var fingerprint *string
	if f, ok := raw["fingerprint"].(string); ok && f != "" {
		fingerprint = &f
	} else if so, ok := raw["securityOptions"].(map[string]any); ok {
		if f, ok := so["fingerprint"].(string); ok && f != "" {
			fingerprint = &f
		} else if f, ok := so["client-fingerprint"].(string); ok && f != "" {
			fingerprint = &f
		}
	}

	var finalMask *string
	if fm, ok := raw["finalMask"]; ok && fm != nil {
		if fmBytes, err := json.Marshal(fm); err == nil {
			fmStr := string(fmBytes)
			finalMask = &fmStr
		}
	} else if so, ok := raw["streamOverrides"].(map[string]any); ok {
		if fm, ok := so["finalMask"]; ok && fm != nil {
			if fmBytes, err := json.Marshal(fm); err == nil {
				fmStr := string(fmBytes)
				finalMask = &fmStr
			}
		}
	}

	var sockoptParams *string
	if so, ok := raw["sockoptParams"].(string); ok && so != "" {
		sockoptParams = &so
	} else if so, ok := raw["streamOverrides"].(map[string]any); ok {
		if s, ok := so["sockopt"]; ok && s != nil {
			if soBytes, err := json.Marshal(s); err == nil {
				soStr := string(soBytes)
				sockoptParams = &soStr
			}
		}
	}

	var muxParams *string
	if mux, ok := raw["muxParams"].(string); ok && mux != "" {
		muxParams = &mux
	} else if mux, ok := raw["mux"]; ok && mux != nil {
		if muxBytes, err := json.Marshal(mux); err == nil {
			muxStr := string(muxBytes)
			muxParams = &muxStr
		}
	}

	var serverDescription *string
	if co, ok := raw["clientOverrides"].(map[string]any); ok {
		if sd, ok := co["serverDescription"].(string); ok && sd != "" {
			serverDescription = &sd
		}
	}

	uuidStr := fmt.Sprintf("00000000-0000-0000-0000-00000000%04d", index)
	if meta, ok := raw["metadata"].(map[string]any); ok {
		if u, ok := meta["uuid"].(string); ok && u != "" {
			uuidStr = u
		}
	} else if u, ok := raw["uuid"].(string); ok && u != "" {
		uuidStr = u
	}

	return SubscriptionHost{
		UUID:              uuidStr,
		Remark:            finalRemark,
		Address:           address,
		Port:              port,
		Path:              path,
		SNI:               sni,
		Host:              hostHeader,
		ALPN:              alpn,
		Fingerprint:       fingerprint,
		SecurityLayer:     security,
		InboundType:       &protocol,
		InboundNetwork:    &network,
		InboundSecurity:   &security,
		FinalMask:         finalMask,
		SockoptParams:     sockoptParams,
		MuxParams:         muxParams,
		ServerDescription: serverDescription,
	}, true
}

// applyShuffle separates hosts with ShuffleHost=true, shuffles them among themselves,
// and places them at the beginning of the slice, followed by the remaining hosts in order.
// This strictly mirrors upstream applyShuffle in resolve-proxy-config.
func applyShuffle(hosts []SubscriptionHost) []SubscriptionHost {
	hasShuffle := false
	for _, h := range hosts {
		if h.ShuffleHost {
			hasShuffle = true
			break
		}
	}
	if !hasShuffle {
		return hosts
	}

	shuffled := make([]SubscriptionHost, 0, len(hosts))
	remaining := make([]SubscriptionHost, 0, len(hosts))
	for _, h := range hosts {
		if h.ShuffleHost {
			shuffled = append(shuffled, h)
		} else {
			remaining = append(remaining, h)
		}
	}

	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	return append(shuffled, remaining...)
}

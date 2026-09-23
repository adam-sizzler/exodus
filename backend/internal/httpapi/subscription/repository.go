package subscription

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	exodusdb "exodus/internal/db"
	"exodus/internal/httpapi/externalsquads"
	"exodus/internal/httpapi/shared"
	"exodus/internal/httpapi/subscriptionresponserules"
	"exodus/internal/httpapi/subscriptionsettings"
	"exodus/internal/jobqueue"
	"exodus/internal/logger"
	"exodus/internal/util"
)

var (
	subSettingsCacheLock sync.RWMutex
	subSettingsCached    *SubscriptionSettingsParsed
	subSettingsCacheTime time.Time

	squadOverridesCacheLock sync.RWMutex
	squadOverridesCache     = make(map[string]cachedSquadOverride)

	subNodeBaseLock sync.RWMutex
	subNodeBaseVal  string
	subNodeBaseExp  time.Time

	subTemplateLock      sync.RWMutex
	subTemplateTypeCache = make(map[string]cachedTemplate)
	subTemplateNameCache = make(map[string]cachedNamedTemplate)
	subTemplateUUIDCache = make(map[string]cachedNamedTemplate)
)

type cachedSquadOverride struct {
	overrides *ExternalSquadOverrides
	expiresAt time.Time
}

type cachedTemplate struct {
	data      []byte
	expiresAt time.Time
}

type cachedNamedTemplate struct {
	templateType string
	data         []byte
	expiresAt    time.Time
}

const (
	subSettingsCacheTTL    = 1 * time.Hour
	squadOverridesCacheTTL = 1 * time.Hour
	subNodeBaseTTL         = 30 * time.Second
	subTemplateCacheTTL    = 5 * time.Minute
)

func init() {
	subscriptionsettings.OnSettingsUpdated = InvalidateSubscriptionSettingsCache
	externalsquads.OnSquadUpdated = InvalidateExternalSquadCache
}

// InvalidateSubscriptionSettingsCache clears the cached subscription settings.
func InvalidateSubscriptionSettingsCache() {
	subSettingsCacheLock.Lock()
	subSettingsCached = nil
	subSettingsCacheLock.Unlock()
}

// InvalidateExternalSquadCache clears the cached squad overrides.
func InvalidateExternalSquadCache(squadUUID string) {
	squadOverridesCacheLock.Lock()
	if squadUUID == "" {
		squadOverridesCache = make(map[string]cachedSquadOverride)
	} else {
		delete(squadOverridesCache, squadUUID)
	}
	squadOverridesCacheLock.Unlock()
}

func loadSubscriptionSettings(ctx context.Context, dbConn *pgxpool.Pool, _ *config.BackendConfig) (SubscriptionSettingsParsed, error) {
	subSettingsCacheLock.RLock()
	if subSettingsCached != nil && time.Since(subSettingsCacheTime) < subSettingsCacheTTL {
		cached := *subSettingsCached
		subSettingsCacheLock.RUnlock()
		return cached, nil
	}
	subSettingsCacheLock.RUnlock()

	var parsed SubscriptionSettingsParsed

	row := dbConn.QueryRow(ctx, `
		SELECT uuid, address, port, api_schema, api_path,
			   serve_json_at_base_subscription, is_show_custom_remarks, custom_remarks,
			   custom_response_headers, randomize_hosts, response_rules, hwid_settings,
			   created_at, updated_at
		FROM subscription_settings
		ORDER BY created_at ASC
		LIMIT 1
	`)

	settings, err := subscriptionsettings.ScanSubscriptionSettings(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			_, _ = dbConn.Exec(ctx, `
				INSERT INTO subscription_settings (
					uuid, address, port, api_schema, api_path,
					serve_json_at_base_subscription, is_show_custom_remarks, custom_remarks,
					custom_response_headers, randomize_hosts, response_rules, hwid_settings
				) VALUES (
					'00000000-0000-0000-0000-000000000000', '', 9263, 'grpc', '',
					false, true, '{}'::jsonb,
					'{"profile-title":"exEncodeBase64:exodus","support-url":"https://github.com","profile-update-interval":"12"}'::jsonb,
					false, '[]'::jsonb, '{}'::jsonb
				) ON CONFLICT DO NOTHING
			`)
			rowRetry := dbConn.QueryRow(ctx, `
				SELECT uuid, address, port, api_schema, api_path,
					   serve_json_at_base_subscription, is_show_custom_remarks, custom_remarks,
					   custom_response_headers, randomize_hosts, response_rules, hwid_settings,
					   created_at, updated_at
				FROM subscription_settings
				ORDER BY created_at ASC
				LIMIT 1
			`)
			settings, err = subscriptionsettings.ScanSubscriptionSettings(rowRetry)
		}
		if err != nil {
			return parsed, err
		}
	}

	parsed.Raw = settings

	if settings.CustomResponseHeaders != "" {
		var headers map[string]string
		if err := json.Unmarshal([]byte(settings.CustomResponseHeaders), &headers); err == nil {
			parsed.CustomResponseHeaders = headers
		}
	}

	if settings.ResponseRules != "" {
		var rules subscriptionresponserules.Config
		if err := json.Unmarshal([]byte(settings.ResponseRules), &rules); err == nil {
			parsed.ResponseRules = &rules
		}
	}

	if settings.HWIDSettings != "" {
		var hwid HwidSettings
		if err := json.Unmarshal([]byte(settings.HWIDSettings), &hwid); err == nil {
			parsed.HwidSettings = hwid
		}
	}
	if parsed.HwidSettings.FallbackDeviceLimit <= 0 {
		parsed.HwidSettings.FallbackDeviceLimit = 999
	}

	if settings.CustomRemarks != "" {
		var remarks CustomRemarks
		if err := json.Unmarshal([]byte(settings.CustomRemarks), &remarks); err == nil {
			parsed.CustomRemarks = remarks
		}
	}

	subSettingsCacheLock.Lock()
	subSettingsCached = &parsed
	subSettingsCacheTime = time.Now()
	subSettingsCacheLock.Unlock()

	return parsed, nil
}

func loadExternalSquadOverrides(ctx context.Context, dbConn *pgxpool.Pool, squadUUID string, cfg *config.BackendConfig) (*ExternalSquadOverrides, error) {
	if strings.TrimSpace(squadUUID) == "" {
		return nil, nil
	}

	squadOverridesCacheLock.RLock()
	if cached, ok := squadOverridesCache[squadUUID]; ok && time.Now().Before(cached.expiresAt) {
		squadOverridesCacheLock.RUnlock()
		return cached.overrides, nil
	}
	squadOverridesCacheLock.RUnlock()

	log := cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceHTTP)
	overrides := &ExternalSquadOverrides{
		Templates: make(map[string]string),
	}

	var subscriptionSettingsJSON, hostOverridesJSON, responseHeadersAddJSON, hwidSettingsJSON, customRemarksJSON *string
	var responseHeadersRemoveRaw *string

	query := `SELECT subscription_settings, host_overrides, response_headers_add,
			  array_to_json(COALESCE(response_headers_remove, ARRAY[]::text[]))::text AS response_headers_remove,
			  hwid_settings, custom_remarks
			  FROM external_squads WHERE uuid = $1 LIMIT 1`
	row := dbConn.QueryRow(ctx, query, squadUUID)

	if err := row.Scan(&subscriptionSettingsJSON, &hostOverridesJSON, &responseHeadersAddJSON, &responseHeadersRemoveRaw, &hwidSettingsJSON, &customRemarksJSON); err != nil {
		return nil, err
	}
	if responseHeadersRemoveRaw != nil {
		overrides.ResponseHeadersRemove = shared.ParsePgTextArray(*responseHeadersRemoveRaw)
	}

	if subscriptionSettingsJSON != nil && *subscriptionSettingsJSON != "" {
		var ss subscriptionsettings.SubscriptionSettings
		if err := json.Unmarshal([]byte(*subscriptionSettingsJSON), &ss); err == nil {
			overrides.SubscriptionSettings = &ss
			log.Debug("Loaded subscription_settings override")
		}
	}
	if hostOverridesJSON != nil && *hostOverridesJSON != "" {
		var ho map[string]HostOverride
		if err := json.Unmarshal([]byte(*hostOverridesJSON), &ho); err == nil {
			overrides.HostOverrides = ho
			log.Debug("Loaded host_overrides override", "count", len(ho))
		}
	}
	if responseHeadersAddJSON != nil && *responseHeadersAddJSON != "" {
		var rh map[string]string
		if err := json.Unmarshal([]byte(*responseHeadersAddJSON), &rh); err == nil {
			overrides.ResponseHeaders = rh
			log.Debug("Loaded response_headers_add override")
		}
	}
	if hwidSettingsJSON != nil && *hwidSettingsJSON != "" {
		var hs HwidSettings
		if err := json.Unmarshal([]byte(*hwidSettingsJSON), &hs); err == nil {
			overrides.HwidSettings = &hs
			log.Debug("Loaded hwid_settings override")
		}
	}
	if customRemarksJSON != nil && *customRemarksJSON != "" {
		var cr CustomRemarks
		if err := json.Unmarshal([]byte(*customRemarksJSON), &cr); err == nil {
			overrides.CustomRemarks = &cr
			log.Debug("Loaded custom_remarks override")
		}
	}

	tmplQuery := `SELECT template_type, template_yaml, template_json
				  FROM external_squad_subscription_templates
				  WHERE external_squad_uuid = $1`
	rows, err := dbConn.Query(ctx, tmplQuery, squadUUID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var tmplType string
			var tmplYAML, tmplJSON *string
			if err := rows.Scan(&tmplType, &tmplYAML, &tmplJSON); err == nil {
				upperType := strings.ToUpper(tmplType)
				if upperType == responseTypeXrayJSON || upperType == responseTypeSingbox {
					if tmplJSON != nil {
						overrides.Templates[upperType] = *tmplJSON
					}
				} else {
					if tmplYAML != nil {
						overrides.Templates[upperType] = *tmplYAML
					}
				}
			}
		}
	}

	squadOverridesCacheLock.Lock()
	squadOverridesCache[squadUUID] = cachedSquadOverride{
		overrides: overrides,
		expiresAt: time.Now().Add(squadOverridesCacheTTL),
	}
	squadOverridesCacheLock.Unlock()

	return overrides, nil
}

func getSubscriptionUserByShortUUID(ctx context.Context, dbConn *pgxpool.Pool, shortUUID string) (SubscriptionUser, error) {
	return getSubscriptionUserByField(ctx, dbConn, "short_uuid", shortUUID)
}

func getSubscriptionUserByID(ctx context.Context, dbConn *pgxpool.Pool, userID int64) (SubscriptionUser, error) {
	return getSubscriptionUserByField(ctx, dbConn, "id", userID)
}

func getSubscriptionUserByUUID(ctx context.Context, dbConn *pgxpool.Pool, userUUID string) (SubscriptionUser, error) {
	return getSubscriptionUserByField(ctx, dbConn, "uuid", userUUID)
}

func getSubscriptionUserByUsername(ctx context.Context, dbConn *pgxpool.Pool, username string) (SubscriptionUser, error) {
	return getSubscriptionUserByField(ctx, dbConn, "username", username)
}

func stringVal(p *string) string {
	if p != nil {
		return *p
	}
	return ""
}

func getSubscriptionUserByField(ctx context.Context, dbConn *pgxpool.Pool, field string, value any) (SubscriptionUser, error) {
	var user SubscriptionUser

	var where string
	switch field {
	case "id":
		where = "u.id = $1"
	case "short_uuid":
		where = "u.short_uuid = $1"
	case "uuid":
		where = "u.uuid::text = $1 OR u.short_uuid = $1 OR u.username = $1"
	case "username":
		where = "u.username = $1"
	default:
		return user, fmt.Errorf("unsupported search field")
	}

	query := fmt.Sprintf(`
		SELECT u.id, u.uuid, u.short_uuid, u.username, u.status,
			   u.traffic_limit_bytes, u.traffic_limit_strategy, u.expire_at,
			   u.last_traffic_reset_at, u.created_at, u.updated_at, u.sub_revoked_at,
			   u.last_triggered_threshold, u.description, u.tag, u.telegram_id, u.email,
			   u.trojan_password, u.vless_uuid, u.ss_password,
			   u.naive_password, u.shadowtls_password, u.hysteria2_password, u.anytls_password,
			   u.hwid_device_limit, u.external_squad_uuid,
			   COALESCE(ut.used_traffic_bytes, 0), COALESCE(ut.lifetime_used_traffic_bytes, 0),
			   ut.online_at, ut.last_connected_node_uuid, ut.first_connected_at
		FROM users u
		LEFT JOIN user_traffic ut ON ut.id = u.id
		WHERE %s
		LIMIT 1
	`, where)

	row := dbConn.QueryRow(ctx, query, value)

	var lastTrafficReset, subRevokedAt, onlineAt, firstConnectedAt *time.Time
	var updatedAt *time.Time
	var description, tag, email, lastConnectedNodeUUID *string
	var telegramID *int64
	var hwidDeviceLimit *int
	var lastTriggeredThreshold *int
	var externalSquadUUID *string
	var naivePassword, shadowtlsPassword, hysteria2Password, anytlsPassword *string
	if err := row.Scan(
		&user.ID,
		&user.UUID,
		&user.ShortUUID,
		&user.Username,
		&user.Status,
		&user.TrafficLimitBytes,
		&user.TrafficLimitStrategy,
		&user.ExpireAt,
		&lastTrafficReset,
		&user.CreatedAt,
		&updatedAt,
		&subRevokedAt,
		&lastTriggeredThreshold,
		&description,
		&tag,
		&telegramID,
		&email,
		&user.TrojanPassword,
		&user.VlessUUID,
		&user.SSPassword,
		&naivePassword,
		&shadowtlsPassword,
		&hysteria2Password,
		&anytlsPassword,
		&hwidDeviceLimit,
		&externalSquadUUID,
		&user.UsedTrafficBytes,
		&user.LifetimeUsedBytes,
		&onlineAt,
		&lastConnectedNodeUUID,
		&firstConnectedAt,
	); err != nil {
		return user, err
	}

	user.LastTrafficResetAt = lastTrafficReset
	user.SubRevokedAt = subRevokedAt
	if updatedAt != nil {
		user.UpdatedAt = *updatedAt
	} else {
		user.UpdatedAt = user.CreatedAt
	}
	if lastTriggeredThreshold != nil {
		user.LastTriggeredThreshold = *lastTriggeredThreshold
	}
	user.OnlineAt = onlineAt
	user.FirstConnectedAt = firstConnectedAt
	user.LastConnectedNodeUUID = lastConnectedNodeUUID
	user.Description = description
	user.Tag = tag
	user.TelegramID = telegramID
	user.Email = email
	user.HwidDeviceLimit = hwidDeviceLimit
	user.ExternalSquadUUID = externalSquadUUID
	user.NaivePassword = stringVal(naivePassword)
	user.ShadowtlsPassword = stringVal(shadowtlsPassword)
	user.Hysteria2Password = stringVal(hysteria2Password)
	user.AnytlsPassword = stringVal(anytlsPassword)

	return user, nil
}

func getHostsForUser(ctx context.Context, dbConn *pgxpool.Pool, user SubscriptionUser) ([]SubscriptionHost, error) {
	return getHostsForUserWithOptions(ctx, dbConn, user, false, false)
}

func getHostsForUserWithOptions(ctx context.Context, dbConn *pgxpool.Pool, user SubscriptionUser, withDisabled, withHidden bool) ([]SubscriptionHost, error) {
	whereClause := `ism.user_id = $1 AND (
		(COALESCE(h.internal_squads_mode, 'EXCLUDE') = 'ALLOW_ONLY' AND ishl.host_uuid IS NOT NULL)
		OR
		(COALESCE(h.internal_squads_mode, 'EXCLUDE') != 'ALLOW_ONLY' AND ishl.host_uuid IS NULL)
	)`
	if !withDisabled {
		whereClause += " AND NOT COALESCE(h.is_disabled, false)"
	}
	if !withHidden {
		whereClause += " AND NOT COALESCE(h.is_hidden, false)"
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT h.uuid, h.view_position, h.remark, h.address, h.port,
			   h.path, h.sni, h.host, h.alpn, h.fingerprint, h.security_layer,
			   h.xhttp_extra_params, h.mux_params, h.mapper, h.sockopt_params, h.final_mask, h.is_disabled,
			   h.server_description, h.shuffle_host,
			   h.mihomo_x25519, h.mihomo_ip_version, h.xray_json_template_uuid, h.keep_sni_blank,
			   h.exclude_from_subscription_types, h.tags, h.is_hidden, h.override_sni_from_address,
			   h.config_profile_uuid, h.config_profile_inbound_uuid,
			   h.pinned_peer_cert_sha256, h.verify_peer_cert_by_name,
			   cpi.tag, cpi.type, cpi.network, cpi.security, cpi.port, cpi.raw_inbound
		FROM internal_squad_members ism
		JOIN internal_squad_inbounds isi ON ism.internal_squad_uuid = isi.internal_squad_uuid
		JOIN config_profile_inbounds cpi ON isi.inbound_uuid = cpi.uuid
		JOIN hosts h ON h.config_profile_inbound_uuid = cpi.uuid
		LEFT JOIN internal_squad_host_links ishl
			ON ishl.host_uuid = h.uuid AND ishl.squad_uuid = ism.internal_squad_uuid
		WHERE %s
		ORDER BY h.view_position ASC, h.remark ASC
	`, whereClause)

	rows, err := dbConn.Query(ctx, query, user.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	hosts := make([]SubscriptionHost, 0, 16)
	for rows.Next() {
		host, err := scanSubscriptionHost(rows)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, host)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return hosts, nil
}

func scanSubscriptionHost(scanner shared.RowScanner) (SubscriptionHost, error) {
	var h SubscriptionHost
	var viewPosition *int
	var path, sni, host, alpn, fingerprint, securityLayer *string
	var xhttpExtraParams, muxParams, mapper, sockoptParams, finalMask, serverDescription *string
	var xrayJSONTemplateUUID, mihomoIPVersion, configProfileUUID, configProfileInboundUUID, pinnedPeerCertSha256, verifyPeerCertByName *string
	var inboundTag, inboundType, inboundNetwork, inboundSecurity *string
	var inboundPort *int
	var rawInbound *string
	var excludeTypes, hostTags []string
	var isDisabled, shuffleHost, mihomoX25519, keepSNIBlank, isHidden, overrideSNIFromAddress *bool

	err := scanner.Scan(
		&h.UUID,
		&viewPosition,
		&h.Remark,
		&h.Address,
		&h.Port,
		&path,
		&sni,
		&host,
		&alpn,
		&fingerprint,
		&securityLayer,
		&xhttpExtraParams,
		&muxParams,
		&mapper,
		&sockoptParams,
		&finalMask,
		&isDisabled,
		&serverDescription,
		&shuffleHost,
		&mihomoX25519,
		&mihomoIPVersion,
		&xrayJSONTemplateUUID,
		&keepSNIBlank,
		&excludeTypes,
		&hostTags,
		&isHidden,
		&overrideSNIFromAddress,
		&configProfileUUID,
		&configProfileInboundUUID,
		&pinnedPeerCertSha256,
		&verifyPeerCertByName,
		&inboundTag,
		&inboundType,
		&inboundNetwork,
		&inboundSecurity,
		&inboundPort,
		&rawInbound,
	)
	if err != nil {
		return h, err
	}

	h.PinnedPeerCertSha256 = pinnedPeerCertSha256
	h.VerifyPeerCertByName = verifyPeerCertByName
	if viewPosition != nil {
		h.ViewPosition = *viewPosition
	}
	h.Path = path
	h.SNI = sni
	h.Host = host
	h.ALPN = alpn
	h.Fingerprint = fingerprint
	if securityLayer != nil && *securityLayer != "" {
		h.SecurityLayer = *securityLayer
	} else {
		h.SecurityLayer = "DEFAULT"
	}
	h.XHTTPExtraParams = xhttpExtraParams
	h.MuxParams = muxParams
	if mapper != nil && strings.TrimSpace(*mapper) != "" {
		h.Mapper = ParseHostMapper([]byte(*mapper))
	}
	h.SockoptParams = sockoptParams
	h.FinalMask = finalMask
	if isDisabled != nil {
		h.IsDisabled = *isDisabled
	}
	h.ServerDescription = serverDescription
	if shuffleHost != nil {
		h.ShuffleHost = *shuffleHost
	}
	if mihomoX25519 != nil {
		h.MihomoX25519 = *mihomoX25519
	}
	h.MihomoIPVersion = mihomoIPVersion
	h.XrayJSONTemplateUUID = xrayJSONTemplateUUID
	if keepSNIBlank != nil {
		h.KeepSNIBlank = *keepSNIBlank
	}
	h.Tags = hostTags
	if h.Tags == nil {
		h.Tags = []string{}
	}
	if len(h.Tags) > 0 && strings.TrimSpace(h.Tags[0]) != "" {
		firstTag := strings.TrimSpace(h.Tags[0])
		h.Tag = &firstTag
	}
	if isHidden != nil {
		h.IsHidden = *isHidden
	}
	if overrideSNIFromAddress != nil {
		h.OverrideSNIFromAddress = *overrideSNIFromAddress
	}
	h.ConfigProfileUUID = configProfileUUID
	h.ConfigProfileInboundUUID = configProfileInboundUUID
	h.ExcludeFromSubscriptionTypes = excludeTypes
	if h.ExcludeFromSubscriptionTypes == nil {
		h.ExcludeFromSubscriptionTypes = []string{}
	}
	h.InboundTag = inboundTag
	h.InboundType = inboundType
	h.InboundNetwork = inboundNetwork
	h.InboundSecurity = inboundSecurity
	h.InboundPort = inboundPort
	if rawInbound != nil {
		h.InboundRaw = json.RawMessage(*rawInbound)
	}

	return h, nil
}

// checkHwidDeviceLimit ports upstream's checkHwidDeviceLimit(): it never
// treats "device limit reached" and "no X-HWID header sent" as the same
// outcome, and DB failures are surfaced as a real error rather than folded
// into one of the two device-limit reasons.
func checkHwidDeviceLimit(ctx context.Context, dbConn *pgxpool.Pool, user SubscriptionUser, hwid *HwidHeaders, settings HwidSettings) (HwidCheckupResult, error) {
	if user.HwidDeviceLimit != nil && *user.HwidDeviceLimit == 0 {
		if hwid != nil {
			_ = enqueueOrUpsertHwidUserDevice(ctx, dbConn, user.ID, *hwid)
		}
		return HwidCheckupResult{Allowed: true, LimitBypassed: true}, nil
	}

	if hwid == nil {
		return HwidCheckupResult{Allowed: false, HwidNotSupported: true}, nil
	}

	exists, err := hwidDeviceExists(ctx, dbConn, user.ID, hwid.Hwid)
	if err == nil && exists {
		_ = enqueueOrUpsertHwidUserDevice(ctx, dbConn, user.ID, *hwid)
		return HwidCheckupResult{Allowed: true}, nil
	}

	limit := settings.FallbackDeviceLimit
	if user.HwidDeviceLimit != nil {
		limit = *user.HwidDeviceLimit
	}

	allowed, err := createHwidDeviceWithAdvisoryLock(ctx, dbConn, user.ID, *hwid, limit)
	if err != nil {
		return HwidCheckupResult{}, fmt.Errorf("create hwid device: %w", err)
	}
	if !allowed {
		return HwidCheckupResult{Allowed: false, MaxDeviceReached: true}, nil
	}

	return HwidCheckupResult{Allowed: true}, nil
}

const hwidLockPrefix int64 = 900000000

func createHwidDeviceWithAdvisoryLock(ctx context.Context, dbConn *pgxpool.Pool, userID int64, hwid HwidHeaders, deviceLimit int) (bool, error) {
	allowed := false
	err := exodusdb.WithRetryTx(ctx, dbConn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, hwidLockPrefix+userID); err != nil {
			return fmt.Errorf("acquire hwid advisory lock: %w", err)
		}

		var count int
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM hwid_user_devices WHERE user_id = $1`, userID).Scan(&count); err != nil {
			return fmt.Errorf("count hwid devices: %w", err)
		}

		platform := lowerStringPtr(hwid.Platform)

		if count >= deviceLimit {
			res, err := tx.Exec(ctx, `
				UPDATE hwid_user_devices SET
					platform = COALESCE($3, platform),
					os_version = COALESCE($4, os_version),
					device_model = COALESCE($5, device_model),
					user_agent = COALESCE($6, user_agent),
					request_ip = COALESCE($7, request_ip),
					updated_at = now()
				WHERE hwid = $1 AND user_id = $2
			`, hwid.Hwid, userID, platform, hwid.OsVersion, hwid.DeviceModel, hwid.UserAgent, hwid.RequestIP)
			if err != nil {
				return fmt.Errorf("update hwid device: %w", err)
			}
			if res.RowsAffected() > 0 {
				allowed = true
				return nil
			}
			allowed = false
			return nil
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO hwid_user_devices (hwid, user_id, platform, os_version, device_model, user_agent, request_ip)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (hwid, user_id) DO UPDATE SET
				platform = COALESCE(EXCLUDED.platform, hwid_user_devices.platform),
				os_version = COALESCE(EXCLUDED.os_version, hwid_user_devices.os_version),
				device_model = COALESCE(EXCLUDED.device_model, hwid_user_devices.device_model),
				user_agent = COALESCE(EXCLUDED.user_agent, hwid_user_devices.user_agent),
				request_ip = COALESCE(EXCLUDED.request_ip, hwid_user_devices.request_ip),
				updated_at = now()
		`, hwid.Hwid, userID, platform, hwid.OsVersion, hwid.DeviceModel, hwid.UserAgent, hwid.RequestIP); err != nil {
			return fmt.Errorf("insert hwid device: %w", err)
		}

		allowed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return allowed, nil
}

func hwidDeviceExists(ctx context.Context, dbConn *pgxpool.Pool, userID int64, hwid string) (bool, error) {
	var tmp int
	err := dbConn.QueryRow(ctx, `SELECT 1 FROM hwid_user_devices WHERE user_id = $1 AND hwid = $2`, userID, hwid).Scan(&tmp)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func upsertHwidUserDevice(ctx context.Context, dbConn *pgxpool.Pool, userID int64, hwid HwidHeaders) error {
	platform := lowerStringPtr(hwid.Platform)
	_, err := dbConn.Exec(ctx, `
		INSERT INTO hwid_user_devices (hwid, user_id, platform, os_version, device_model, user_agent, request_ip)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (hwid, user_id)
		DO UPDATE SET
			platform = COALESCE(EXCLUDED.platform, hwid_user_devices.platform),
			os_version = COALESCE(EXCLUDED.os_version, hwid_user_devices.os_version),
			device_model = COALESCE(EXCLUDED.device_model, hwid_user_devices.device_model),
			user_agent = COALESCE(EXCLUDED.user_agent, hwid_user_devices.user_agent),
			request_ip = COALESCE(EXCLUDED.request_ip, hwid_user_devices.request_ip),
			updated_at = now()
	`, hwid.Hwid, userID, platform, hwid.OsVersion, hwid.DeviceModel, hwid.UserAgent, hwid.RequestIP)
	return err
}

func enqueueOrUpsertHwidUserDevice(ctx context.Context, dbConn *pgxpool.Pool, userID int64, hwid HwidHeaders) error {
	platform := lowerStringPtr(hwid.Platform)
	queued, err := jobqueue.EnqueueUpsertHwidDevice(ctx, jobqueue.UpsertHwidDevicePayload{
		UserID:      userID,
		Hwid:        hwid.Hwid,
		Platform:    platform,
		OsVersion:   hwid.OsVersion,
		DeviceModel: hwid.DeviceModel,
		UserAgent:   hwid.UserAgent,
		RequestIP:   hwid.RequestIP,
	})
	if err == nil && queued {
		return nil
	}
	return upsertHwidUserDevice(ctx, dbConn, userID, hwid)
}

// subHistoryFallbackSem bounds how many synchronous DB fallback goroutines can run
// concurrently when Redis/jobqueue is unavailable. This prevents connection pool exhaustion.
var subHistoryFallbackSem = make(chan struct{}, 16)

func updateSubscriptionRequest(ctx context.Context, dbConn *pgxpool.Pool, userUUID string, userID int64, userAgent, requestIP, responseType, ruleName string) {
	if responseType == "" {
		responseType = "UNKNOWN"
	}
	var ruleVal *string
	if strings.TrimSpace(ruleName) != "" {
		trimmed := strings.TrimSpace(ruleName)
		ruleVal = &trimmed
	}

	recordQueued, recordErr := jobqueue.EnqueueAddSubscriptionRequestRecord(ctx, jobqueue.AddSubscriptionRequestRecordPayload{
		UserID:          userID,
		RequestIP:       requestIP,
		UserAgent:       userAgent,
		SRRResponseType: responseType,
		SRRRuleName:     ruleVal,
	})
	if recordErr == nil && recordQueued {
		return
	}

	select {
	case subHistoryFallbackSem <- struct{}{}:
		go func() {
			defer func() { <-subHistoryFallbackSem }()

			jobCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			batch := &pgx.Batch{}
			batch.Queue(`
				INSERT INTO user_subscription_request_history (user_id, srr_response_type, srr_rule_name, request_ip, user_agent)
				VALUES ($1, $2, $3, $4, $5)
			`, userID, responseType, ruleVal, requestIP, userAgent)

			batch.Queue(`
				DELETE FROM user_subscription_request_history
				WHERE user_id = $1
				  AND id NOT IN (
					  SELECT id
					  FROM user_subscription_request_history
					  WHERE user_id = $2
					  ORDER BY request_at DESC, id DESC
					  LIMIT 24
				  )
			`, userID, userID)

			br := dbConn.SendBatch(jobCtx, batch)
			for i := 0; i < batch.Len(); i++ {
				if _, err := br.Exec(); err != nil {
					_ = br.Close()
					return
				}
			}
			_ = br.Close()
		}()
	default:
		// Queue is unavailable and all fallback slots are busy; drop non-critical history record to protect PostgreSQL pool
	}
}

func getSubscriptionTemplate(ctx context.Context, dbConn *pgxpool.Pool, templateType string) ([]byte, error) {
	upperType := strings.ToUpper(strings.TrimSpace(templateType))

	subTemplateLock.RLock()
	if cached, ok := subTemplateTypeCache[upperType]; ok && time.Now().Before(cached.expiresAt) {
		subTemplateLock.RUnlock()
		return cached.data, nil
	}
	subTemplateLock.RUnlock()

	row := dbConn.QueryRow(ctx, `
		SELECT template_yaml, template_json
		FROM subscription_templates
		WHERE UPPER(template_type) = $1
		ORDER BY view_position ASC
		LIMIT 1
	`, upperType)

	var templateYAML, templateJSON *string
	if err := row.Scan(&templateYAML, &templateJSON); err != nil {
		return nil, err
	}

	var templateData []byte
	if upperType == responseTypeXrayJSON || upperType == responseTypeSingbox {
		if templateJSON != nil {
			templateData = []byte(*templateJSON)
		}
	} else {
		if templateYAML != nil {
			templateData = []byte(*templateYAML)
		}
	}

	subTemplateLock.Lock()
	subTemplateTypeCache[upperType] = cachedTemplate{
		data:      templateData,
		expiresAt: time.Now().Add(subTemplateCacheTTL),
	}
	subTemplateLock.Unlock()

	return templateData, nil
}

func getSubscriptionTemplateByName(ctx context.Context, dbConn *pgxpool.Pool, name string) (string, []byte, error) {
	subTemplateLock.RLock()
	if cached, ok := subTemplateNameCache[name]; ok && time.Now().Before(cached.expiresAt) {
		subTemplateLock.RUnlock()
		return cached.templateType, cached.data, nil
	}
	subTemplateLock.RUnlock()

	row := dbConn.QueryRow(ctx, `
		SELECT template_type, template_yaml, template_json
		FROM subscription_templates
		WHERE name = $1
		LIMIT 1
	`, name)

	var templateType string
	var templateYAML, templateJSON *string
	if err := row.Scan(&templateType, &templateYAML, &templateJSON); err != nil {
		return "", nil, err
	}

	upperType := strings.ToUpper(strings.TrimSpace(templateType))
	var templateData []byte
	if upperType == responseTypeXrayJSON || upperType == responseTypeSingbox {
		if templateJSON != nil {
			templateData = []byte(*templateJSON)
		}
	} else {
		if templateYAML != nil {
			templateData = []byte(*templateYAML)
		}
	}

	subTemplateLock.Lock()
	subTemplateNameCache[name] = cachedNamedTemplate{
		templateType: templateType,
		data:         templateData,
		expiresAt:    time.Now().Add(subTemplateCacheTTL),
	}
	subTemplateLock.Unlock()

	return templateType, templateData, nil
}

func getSubscriptionTemplateByUUID(ctx context.Context, dbConn *pgxpool.Pool, uuidStr string) (string, []byte, error) {
	subTemplateLock.RLock()
	if cached, ok := subTemplateUUIDCache[uuidStr]; ok && time.Now().Before(cached.expiresAt) {
		subTemplateLock.RUnlock()
		return cached.templateType, cached.data, nil
	}
	subTemplateLock.RUnlock()

	row := dbConn.QueryRow(ctx, `
		SELECT template_type, template_yaml, template_json
		FROM subscription_templates
		WHERE uuid = $1
		LIMIT 1
	`, uuidStr)

	var templateType string
	var templateYAML, templateJSON *string
	if err := row.Scan(&templateType, &templateYAML, &templateJSON); err != nil {
		return "", nil, err
	}

	upperType := strings.ToUpper(strings.TrimSpace(templateType))
	var templateData []byte
	if upperType == responseTypeXrayJSON || upperType == responseTypeSingbox {
		if templateJSON != nil {
			templateData = []byte(*templateJSON)
		}
	} else {
		if templateYAML != nil {
			templateData = []byte(*templateYAML)
		}
	}

	subTemplateLock.Lock()
	subTemplateUUIDCache[uuidStr] = cachedNamedTemplate{
		templateType: templateType,
		data:         templateData,
		expiresAt:    time.Now().Add(subTemplateCacheTTL),
	}
	subTemplateLock.Unlock()

	return templateType, templateData, nil
}

func getUsersWithPagination(ctx context.Context, dbConn *pgxpool.Pool, start, size int) ([]SubscriptionUser, int, error) {
	var total int
	if err := dbConn.QueryRow(ctx, `SELECT COUNT(*) FROM users`).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := dbConn.Query(ctx, `
		SELECT u.id, u.uuid, u.short_uuid, u.username, u.status,
			   u.traffic_limit_bytes, u.traffic_limit_strategy, u.expire_at,
			   u.trojan_password, u.vless_uuid, u.ss_password,
			   u.naive_password, u.shadowtls_password, u.hysteria2_password, u.anytls_password,
			   u.hwid_device_limit, u.external_squad_uuid,
			   COALESCE(ut.used_traffic_bytes, 0), COALESCE(ut.lifetime_used_traffic_bytes, 0)
		FROM users u
		LEFT JOIN user_traffic ut ON ut.id = u.id
		ORDER BY u.created_at DESC
		LIMIT $1 OFFSET $2
	`, size, start)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	users := []SubscriptionUser{}
	for rows.Next() {
		var user SubscriptionUser
		var hwidDeviceLimit *int
		var externalSquadUUID *string
		var naivePassword, shadowtlsPassword, hysteria2Password, anytlsPassword *string

		if err := rows.Scan(
			&user.ID,
			&user.UUID,
			&user.ShortUUID,
			&user.Username,
			&user.Status,
			&user.TrafficLimitBytes,
			&user.TrafficLimitStrategy,
			&user.ExpireAt,
			&user.TrojanPassword,
			&user.VlessUUID,
			&user.SSPassword,
			&naivePassword,
			&shadowtlsPassword,
			&hysteria2Password,
			&anytlsPassword,
			&hwidDeviceLimit,
			&externalSquadUUID,
			&user.UsedTrafficBytes,
			&user.LifetimeUsedBytes,
		); err != nil {
			return nil, 0, err
		}

		user.HwidDeviceLimit = hwidDeviceLimit
		user.ExternalSquadUUID = externalSquadUUID
		user.NaivePassword = stringVal(naivePassword)
		user.ShadowtlsPassword = stringVal(shadowtlsPassword)
		user.Hysteria2Password = stringVal(hysteria2Password)
		user.AnytlsPassword = stringVal(anytlsPassword)

		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	return users, total, nil
}

func getSubpageConfigForUser(ctx context.Context, dbConn *pgxpool.Pool, cfg *config.BackendConfig, shortUUID string, requestHeaders map[string]string) (string, bool, error) {
	log := cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceHTTP)
	user, err := getSubscriptionUserByShortUUID(ctx, dbConn, shortUUID)
	if err != nil {
		return "", false, err
	}

	subpageConfigUUID := ""

	if user.ExternalSquadUUID != nil {
		var squadSubpageUUID *string

		err := dbConn.QueryRow(ctx, `
			SELECT subpage_config_uuid 
			FROM external_squads 
			WHERE uuid = $1`,
			*user.ExternalSquadUUID).Scan(&squadSubpageUUID)

		if err == nil && squadSubpageUUID != nil {
			subpageConfigUUID = *squadSubpageUUID
		} else if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			log.Error(fmt.Sprintf("Failed to load external squad subpage config: %v", err))
		}
	}

	if subpageConfigUUID == "" {
		subpageConfigUUID = defaultSubpageConfigUUID
	}

	settings, err := loadSubscriptionSettings(ctx, dbConn, cfg)
	if err != nil {
		return subpageConfigUUID, false, err
	}

	webpageAllowed := false
	if settings.ResponseRules != nil {
		header := http.Header{}
		for key, value := range requestHeaders {
			header.Set(key, value)
		}
		responseType := matchResponseRules(settings.ResponseRules, header)
		webpageAllowed = responseType == responseTypeBrowser
	}

	log.Debug(
		"Resolved subpage config for user",
		"short_uuid", shortUUID,
		"subpage_config_uuid", subpageConfigUUID,
		"webpage_allowed", webpageAllowed,
	)

	return subpageConfigUUID, webpageAllowed, nil
}

func UpdateExternalSquad(ctx context.Context, dbConn *pgxpool.Pool, squadUUID string, input UpdateExternalSquadInput) error {
	var currentName string
	var currentSubpageConfigUUID *string
	var currentCustomRemarks *string
	var currentHwidSettingsRaw []byte

	err := dbConn.QueryRow(ctx,
		`SELECT name, subpage_config_uuid, custom_remarks, hwid_settings FROM external_squads WHERE uuid = $1`,
		squadUUID).Scan(&currentName, &currentSubpageConfigUUID, &currentCustomRemarks, &currentHwidSettingsRaw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("external squad not found")
		}
		return fmt.Errorf("failed to fetch current external squad: %w", err)
	}

	var columns []string
	var args []interface{}
	idx := 1

	if input.Name != nil {
		columns = append(columns, fmt.Sprintf("name = $%d", idx))
		args = append(args, *input.Name)
		idx++
	}

	if input.SubpageConfigUUID != nil {
		columns = append(columns, fmt.Sprintf("subpage_config_uuid = $%d", idx))
		if *input.SubpageConfigUUID == "" {
			args = append(args, nil)
		} else {
			args = append(args, *input.SubpageConfigUUID)
		}
		idx++
	}

	if input.CustomRemarks != nil {
		columns = append(columns, fmt.Sprintf("custom_remarks = $%d", idx))
		if shared.IsJSONNull(*input.CustomRemarks) {
			args = append(args, nil)
		} else {
			args = append(args, string(*input.CustomRemarks))
		}
		idx++
	}

	if len(input.HwidSettings) > 0 {
		if shared.IsJSONNull(input.HwidSettings) {
			columns = append(columns, fmt.Sprintf("hwid_settings = $%d", idx))
			args = append(args, nil)
			idx++
		} else {
			var hwidInput HwidSettingsInput
			if err := json.Unmarshal(input.HwidSettings, &hwidInput); err != nil {
				return fmt.Errorf("invalid hwidSettings: %w", err)
			}

			var hwid struct {
				Enabled             bool `json:"enabled"`
				MaxDevicesAnnounce  *int `json:"maxDevicesAnnounce"`
				FallbackDeviceLimit int  `json:"fallbackDeviceLimit"`
			}
			hwid.FallbackDeviceLimit = 999
			if len(currentHwidSettingsRaw) > 0 && !bytes.Equal(currentHwidSettingsRaw, []byte("null")) {
				if err := json.Unmarshal(currentHwidSettingsRaw, &hwid); err != nil {
					return fmt.Errorf("failed to unmarshal current hwid settings from database: %w", err)
				}
			}

			if hwidInput.Enabled != nil {
				hwid.Enabled = *hwidInput.Enabled
			}
			if hwidInput.FallbackDeviceLimit != nil {
				hwid.FallbackDeviceLimit = *hwidInput.FallbackDeviceLimit
			}
			if hwidInput.MaxDevicesAnnounce != nil {
				hwid.MaxDevicesAnnounce = hwidInput.MaxDevicesAnnounce
			}

			updatedHwidRaw, err := json.Marshal(hwid)
			if err != nil {
				return fmt.Errorf("failed to marshal merged hwid settings: %w", err)
			}

			columns = append(columns, fmt.Sprintf("hwid_settings = $%d", idx))
			args = append(args, string(updatedHwidRaw))
			idx++
		}
	}

	if len(columns) == 0 {
		return fmt.Errorf("no fields to update")
	}

	args = append(args, squadUUID)
	query := fmt.Sprintf("UPDATE external_squads SET %s WHERE uuid = $%d", strings.Join(columns, ", "), idx)

	_, err = dbConn.Exec(ctx, query, args...)
	return err
}

func applyHostOverrides(hosts []SubscriptionHost, overrides map[string]HostOverride) []SubscriptionHost {
	if len(overrides) == 0 {
		return hosts
	}
	result := make([]SubscriptionHost, len(hosts))
	for i, h := range hosts {
		override, ok := overrides[h.UUID]
		if !ok {
			result[i] = h
			continue
		}
		if override.Address != nil && strings.TrimSpace(*override.Address) != "" {
			h.Address = *override.Address
		}
		if override.Port != nil && *override.Port > 0 {
			h.Port = *override.Port
		}
		if override.Remark != nil && strings.TrimSpace(*override.Remark) != "" {
			h.Remark = *override.Remark
		}
		if override.SNI != nil && strings.TrimSpace(*override.SNI) != "" {
			h.SNI = override.SNI
		}
		if override.Host != nil && strings.TrimSpace(*override.Host) != "" {
			h.Host = override.Host
		}
		if override.Path != nil && strings.TrimSpace(*override.Path) != "" {
			h.Path = override.Path
		}
		result[i] = h
	}
	return result
}

func resolveSubscriptionBaseFromNode(ctx context.Context, dbConn *pgxpool.Pool) string {
	if dbConn == nil {
		return ""
	}

	subNodeBaseLock.RLock()
	if time.Now().Before(subNodeBaseExp) {
		val := subNodeBaseVal
		subNodeBaseLock.RUnlock()
		return val
	}
	subNodeBaseLock.RUnlock()

	var domain *string
	var apiPath *string
	row := dbConn.QueryRow(ctx, `
		SELECT
			COALESCE(NULLIF(BTRIM(public_domain), ''), NULLIF(BTRIM(address), '')) AS domain,
			COALESCE(NULLIF(BTRIM(api_path), ''), '/') AS api_path
		FROM sub_nodes
		ORDER BY is_disabled ASC, view_position ASC, created_at ASC
		LIMIT 1
	`)

	scanErr := row.Scan(&domain, &apiPath)
	if errors.Is(scanErr, pgx.ErrNoRows) || scanErr != nil || domain == nil {
		subNodeBaseLock.Lock()
		subNodeBaseVal = ""
		subNodeBaseExp = time.Now().Add(subNodeBaseTTL)
		subNodeBaseLock.Unlock()
		return ""
	}

	nodeDomain := strings.TrimSpace(strings.Split(*domain, ",")[0])
	if nodeDomain == "" {
		subNodeBaseLock.Lock()
		subNodeBaseVal = ""
		subNodeBaseExp = time.Now().Add(subNodeBaseTTL)
		subNodeBaseLock.Unlock()
		return ""
	}

	if !strings.Contains(nodeDomain, "://") {
		nodeDomain = "https://" + nodeDomain
	}

	parsedDomain, parseErr := url.Parse(nodeDomain)
	if parseErr != nil || strings.TrimSpace(parsedDomain.Host) == "" {
		subNodeBaseLock.Lock()
		subNodeBaseVal = ""
		subNodeBaseExp = time.Now().Add(subNodeBaseTTL)
		subNodeBaseLock.Unlock()
		return ""
	}

	parsedDomain.Path = ""
	parsedDomain.RawQuery = ""
	parsedDomain.Fragment = ""
	parsedDomain.User = nil

	base := strings.TrimRight(parsedDomain.String(), "/")
	apiPathStr := ""
	if apiPath != nil {
		apiPathStr = *apiPath
	}
	path := normalizeSubscriptionAPIPath(apiPathStr)
	res := base + path

	subNodeBaseLock.Lock()
	subNodeBaseVal = res
	subNodeBaseExp = time.Now().Add(subNodeBaseTTL)
	subNodeBaseLock.Unlock()

	return res
}

func normalizeSubscriptionAPIPath(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed == "/" {
		return "/"
	}

	return "/" + strings.Trim(trimmed, "/") + "/"
}

func resolveSubscriptionURL(ctx context.Context, dbConn *pgxpool.Pool, user SubscriptionUser, settings SubscriptionSettingsParsed) string {
	if dbConn != nil {
		if base := resolveSubscriptionBaseFromNode(ctx, dbConn); base != "" {
			return base + user.ShortUUID
		}
	}

	domain := strings.TrimSpace(settings.Raw.Address)
	if domain == "" {
		domain = "panel.exodus.dev"
	}
	scheme := strings.TrimSpace(settings.Raw.APISchema)
	if scheme != "http" && scheme != "https" {
		scheme = "https"
	}
	apiPath := strings.Trim(strings.TrimSpace(settings.Raw.APIPath), "/")
	if apiPath == "" {
		apiPath = "api/sub"
	}
	return fmt.Sprintf("%s://%s/%s/%s", scheme, domain, apiPath, user.ShortUUID)
}

func buildResponseHeaders(user SubscriptionUser, settings SubscriptionSettingsParsed, contentType string, subscriptionURL string) map[string]string {
	headers := make(map[string]string)
	if contentType != "" {
		headers["content-type"] = contentType
	}
	headers["content-disposition"] = fmt.Sprintf("attachment; filename=%s", user.Username)

	headers["subscription-userinfo"] = formatSubscriptionUserInfo(user)

	if refillDate := getSubscriptionRefillDate(user.TrafficLimitStrategy, user.CreatedAt); refillDate != "" {
		headers["subscription-refill-date"] = refillDate
	}

	if settings.HwidSettings.Enabled {
		headers["x-hwid-active"] = "true"
	}

	if len(settings.CustomResponseHeaders) > 0 {
		for k, v := range settings.CustomResponseHeaders {
			headers[strings.ToLower(strings.TrimSpace(k))] = formatTemplateValue(v, user, settings, subscriptionURL)
		}
	} else {
		title := settings.Raw.ProfileTitle
		if title == "" {
			title = user.Username
		} else {
			title = formatTemplateValue(title, user, settings, subscriptionURL)
		}
		headers["profile-title"] = fmt.Sprintf("base64:%s", base64.StdEncoding.EncodeToString([]byte(title)))

		if settings.Raw.SupportLink != "" {
			headers["support-url"] = settings.Raw.SupportLink
		}

		interval := settings.Raw.ProfileUpdateInterval
		if interval <= 0 {
			interval = 24
		}
		headers["profile-update-interval"] = fmt.Sprintf("%d", interval)

		if settings.Raw.HappAnnounce != "" {
			announce := formatTemplateValue(settings.Raw.HappAnnounce, user, settings, subscriptionURL)
			headers["announce"] = fmt.Sprintf("base64:%s", base64.StdEncoding.EncodeToString([]byte(announce)))
		}

		if settings.Raw.HappRouting != "" {
			headers["routing"] = settings.Raw.HappRouting
		}

		if settings.Raw.IsProfileWebpageURLEnabled {
			headers["profile-web-page-url"] = subscriptionURL
		}
	}

	for k, v := range settings.ResponseHeaders {
		headers[strings.ToLower(strings.TrimSpace(k))] = formatTemplateValue(v, user, settings, subscriptionURL)
	}
	for _, k := range settings.ResponseHeadersRemove {
		delete(headers, strings.ToLower(strings.TrimSpace(k)))
	}
	return headers
}

var templateRegex = regexp.MustCompile(`\{\{(\w+)(?::([^{}]*))?\}\}`)

func parseTemplateArgs(rawArgs string) map[string]string {
	args := make(map[string]string)
	for _, pair := range strings.Split(rawArgs, "|") {
		if idx := strings.Index(pair, "="); idx != -1 {
			args[strings.TrimSpace(pair[:idx])] = pair[idx+1:]
		}
	}
	return args
}

func formatTemplateValue(value string, user SubscriptionUser, settings SubscriptionSettingsParsed, subscriptionURL string) string {
	shouldBase64 := false
	if strings.HasPrefix(value, "exEncodeBase64:") {
		shouldBase64 = true
		value = strings.TrimPrefix(value, "exEncodeBase64:")
	} else if strings.HasPrefix(value, "rwEncodeBase64:") {
		shouldBase64 = true
		value = strings.TrimPrefix(value, "rwEncodeBase64:")
	}

	res := resolveTemplateVariables(value, user, settings, subscriptionURL)

	if shouldBase64 {
		return "base64:" + base64.StdEncoding.EncodeToString([]byte(res))
	}
	return res
}

func formatTemplateTrafficBytes(bytes int64) string {
	if bytes < 0 {
		bytes = 0
	}
	return util.FormatBytes(bytes)
}

var (
	dayjsBracketRegex    = regexp.MustCompile(`\[([^\]]*)\]`)
	dayjsFormatReplacer  = strings.NewReplacer(
		"YYYY", "2006",
		"YY", "06",
		"MMMM", "January",
		"MMM", "Jan",
		"MM", "01",
		"M", "1",
		"dddd", "Monday",
		"ddd", "Mon",
		"DD", "02",
		"D", "2",
		"HH", "15",
		"H", "15",
		"hh", "03",
		"h", "3",
		"mm", "04",
		"m", "4",
		"ss", "05",
		"s", "5",
		"SSS", ".000",
		"A", "PM",
		"a", "pm",
		"ZZ", "-0700",
		"Z", "-07:00",
	)
)

func convertDayjsToGoFormat(layout string) string {
	if layout == "" {
		return "02.01.2006"
	}

	var escapes []string
	layout = dayjsBracketRegex.ReplaceAllStringFunc(layout, func(m string) string {
		content := m[1 : len(m)-1]
		escapes = append(escapes, content)
		return fmt.Sprintf("\x00%d\x00", len(escapes)-1)
	})

	res := dayjsFormatReplacer.Replace(layout)

	for i, esc := range escapes {
		res = strings.ReplaceAll(res, fmt.Sprintf("\x00%d\x00", i), esc)
	}

	return res
}

func formatTemplateDate(t *time.Time, args map[string]string) string {
	if t == nil || t.IsZero() {
		return ""
	}
	fmtStr := args["format"]
	layout := convertDayjsToGoFormat(fmtStr)
	return t.Format(layout)
}

func getNextTrafficResetAt(strategy string, createdAt time.Time) *time.Time {
	now := time.Now().UTC()
	createdAt = createdAt.UTC()

	switch strategy {
	case "DAY":
		startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 5, 0, 0, time.UTC)
		if startOfDay.After(now) {
			return &startOfDay
		}
		next := startOfDay.AddDate(0, 0, 1)
		return &next

	case "WEEK":
		daysUntilMonday := (1 - int(now.Weekday()) + 7) % 7
		startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 15, 0, 0, time.UTC)
		next := startOfDay.AddDate(0, 0, daysUntilMonday)
		if next.After(now) {
			return &next
		}
		nextWeek := next.AddDate(0, 0, 7)
		return &nextWeek

	case "MONTH":
		startOfMonth := time.Date(now.Year(), now.Month(), 1, 0, 20, 0, 0, time.UTC)
		if startOfMonth.After(now) {
			return &startOfMonth
		}
		nextMonth := startOfMonth.AddDate(0, 1, 0)
		return &nextMonth

	case "MONTH_ROLLING":
		anchorDay := createdAt.Day()
		daysInMonth := func(year int, month time.Month) int {
			return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
		}

		resetAtIn := func(t time.Time) time.Time {
			dim := daysInMonth(t.Year(), t.Month())
			day := anchorDay
			if day > dim {
				day = dim
			}
			return time.Date(t.Year(), t.Month(), day, 0, 10, 0, 0, time.UTC)
		}

		next := resetAtIn(now)
		var resolved time.Time
		if next.After(now) {
			resolved = next
		} else {
			resolved = resetAtIn(now.AddDate(0, 1, 0))
		}

		firstReset := resetAtIn(createdAt.AddDate(0, 1, 0))
		if resolved.Before(firstReset) {
			return &firstReset
		}
		return &resolved

	default:
		return nil
	}
}

func resolveTemplateVariables(value string, user SubscriptionUser, settings SubscriptionSettingsParsed, subscriptionURL string) string {
	if !strings.Contains(value, "{{") {
		return value
	}
	trafficLeft := int64(0)
	if user.TrafficLimitBytes > 0 {
		if user.TrafficLimitBytes > user.UsedTrafficBytes {
			trafficLeft = user.TrafficLimitBytes - user.UsedTrafficBytes
		}
	}

	daysLeft := int64(0)
	if !user.ExpireAt.IsZero() && user.ExpireAt.After(time.Now()) {
		daysLeft = int64(math.Max(0, time.Until(user.ExpireAt).Hours()/24))
	}

	lastResetUnix := int64(0)
	if user.LastTrafficResetAt != nil && !user.LastTrafficResetAt.IsZero() {
		lastResetUnix = user.LastTrafficResetAt.Unix()
	}

	nextResetAt := getNextTrafficResetAt(user.TrafficLimitStrategy, user.CreatedAt)
	nextResetUnix := int64(0)
	if nextResetAt != nil && !nextResetAt.IsZero() {
		nextResetUnix = nextResetAt.Unix()
	}

	createdAtUnix := int64(0)
	if !user.CreatedAt.IsZero() {
		createdAtUnix = user.CreatedAt.Unix()
	}

	expireUnix := int64(0)
	if !user.ExpireAt.IsZero() {
		expireUnix = user.ExpireAt.Unix()
	}

	email := ""
	if user.Email != nil {
		email = *user.Email
	}

	tag := ""
	if user.Tag != nil {
		tag = *user.Tag
	}

	telegramID := ""
	if user.TelegramID != nil {
		telegramID = strconv.FormatInt(*user.TelegramID, 10)
	}

	description := ""
	if user.Description != nil {
		description = *user.Description
	}

	hwidLimit := 0
	if user.HwidDeviceLimit != nil {
		hwidLimit = *user.HwidDeviceLimit
	} else {
		hwidLimit = settings.HwidSettings.FallbackDeviceLimit
	}

	userStatusLabel := ""
	if len(user.Status) > 0 {
		userStatusLabel = strings.ToUpper(user.Status[:1]) + strings.ToLower(user.Status[1:])
	}

	res := templateRegex.ReplaceAllStringFunc(value, func(match string) string {
		submatches := templateRegex.FindStringSubmatch(match)
		if len(submatches) < 2 {
			return match
		}
		key := submatches[1]
		var args map[string]string
		if len(submatches) >= 3 && submatches[2] != "" {
			args = parseTemplateArgs(submatches[2])
		} else {
			args = make(map[string]string)
		}

		switch key {
		case "DAYS_LEFT":
			return strconv.FormatInt(daysLeft, 10)
		case "TRAFFIC_USED":
			return formatTemplateTrafficBytes(user.UsedTrafficBytes)
		case "TRAFFIC_LEFT":
			return formatTemplateTrafficBytes(trafficLeft)
		case "TOTAL_TRAFFIC":
			return formatTemplateTrafficBytes(user.TrafficLimitBytes)
		case "STATUS":
			if val, ok := args[user.Status]; ok {
				return val
			}
			return userStatusLabel
		case "USERNAME":
			return user.Username
		case "EMAIL":
			return email
		case "TELEGRAM_ID":
			return telegramID
		case "SUBSCRIPTION_URL":
			return subscriptionURL
		case "TAG":
			return tag
		case "EXPIRE_UNIX":
			return strconv.FormatInt(expireUnix, 10)
		case "SHORT_UUID":
			return user.ShortUUID
		case "ID":
			return strconv.FormatInt(user.ID, 10)
		case "TRAFFIC_USED_BYTES":
			return strconv.FormatInt(user.UsedTrafficBytes, 10)
		case "TRAFFIC_LEFT_BYTES":
			return strconv.FormatInt(trafficLeft, 10)
		case "TOTAL_TRAFFIC_BYTES":
			return strconv.FormatInt(user.TrafficLimitBytes, 10)
		case "RESET_STRATEGY":
			if val, ok := args[user.TrafficLimitStrategy]; ok {
				return val
			}
			return user.TrafficLimitStrategy
		case "LIFETIME_USED_BYTES":
			return strconv.FormatInt(user.LifetimeUsedBytes, 10)
		case "CREATED_AT_UNIX":
			return strconv.FormatInt(createdAtUnix, 10)
		case "LAST_TRAFFIC_RESET_AT_UNIX":
			return strconv.FormatInt(lastResetUnix, 10)
		case "LAST_TRAFFIC_RESET_AT":
			return formatTemplateDate(user.LastTrafficResetAt, args)
		case "NEXT_TRAFFIC_RESET_AT_UNIX":
			return strconv.FormatInt(nextResetUnix, 10)
		case "NEXT_TRAFFIC_RESET_AT":
			return formatTemplateDate(nextResetAt, args)
		case "SS_HWID_LIMIT":
			return strconv.Itoa(hwidLimit)
		case "DESCRIPTION":
			return description
		default:
			return match
		}
	})

	return res
}

func resolveHostRemarks(hosts []SubscriptionHost, user SubscriptionUser, settings SubscriptionSettingsParsed, subscriptionURL string) {
	knownRemarks := make(map[string]int, len(hosts))
	for i := range hosts {
		hosts[i].Remark = deduplicateRemark(
			resolveTemplateVariables(hosts[i].Remark, user, settings, subscriptionURL),
			knownRemarks,
		)
	}
}

func deduplicateRemark(remark string, knownRemarks map[string]int) string {
	currentCount := knownRemarks[remark]
	knownRemarks[remark] = currentCount + 1

	if currentCount == 0 {
		return remark
	}

	hasExistingSuffix := strings.Contains(remark, "^~") && strings.HasSuffix(remark, "~^")
	suffix := currentCount + 1
	if hasExistingSuffix {
		suffix = currentCount
	}
	return fmt.Sprintf("%s ^~%d~^", remark, suffix)
}

func formatSubscriptionUserInfo(user SubscriptionUser) string {
	expire := user.ExpireAt.Unix()
	if user.ExpireAt.IsZero() || user.ExpireAt.Year() <= 1 || user.ExpireAt.Year() == 2099 {
		expire = 0
	}

	var buf [128]byte
	b := append(buf[:0], "download="...)
	b = strconv.AppendInt(b, user.UsedTrafficBytes, 10)
	b = append(b, "; expire="...)
	b = strconv.AppendInt(b, expire, 10)
	b = append(b, "; total="...)
	b = strconv.AppendInt(b, user.TrafficLimitBytes, 10)
	b = append(b, "; upload=0"...)
	return string(b)
}

func getSubscriptionUserInfo(user SubscriptionUser) map[string]int64 {
	expire := user.ExpireAt.Unix()
	if user.ExpireAt.IsZero() || user.ExpireAt.Year() <= 1 || user.ExpireAt.Year() == 2099 {
		expire = 0
	}

	return map[string]int64{
		"upload":   0,
		"download": user.UsedTrafficBytes,
		"total":    user.TrafficLimitBytes,
		"expire":   expire,
	}
}

func getSubscriptionRefillDate(strategy string, createdAt time.Time) string {
	next := getNextTrafficResetAt(strategy, createdAt)
	if next == nil {
		return ""
	}
	return strconv.FormatInt(next.Unix(), 10)
}

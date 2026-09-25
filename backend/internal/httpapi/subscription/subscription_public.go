package subscription

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"maps"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/middleware"
	"exodus/internal/httpapi/shared"
	"exodus/internal/httpapi/subscriptionresponserules"
	"exodus/internal/logger"
)

// SubscriptionPublicHandler godoc
// @Summary      Public subscription render
// @Description  Fetch subscription config (sing-box, xray, mihomo, base64) or user subscription info by short UUID
// @Tags         [Public] Subscription Controller
// @Produce      plain
// @Produce      json
// @Param        shortUuid  path      string  true  "User short UUID"
// @Param        client     path      string  false "Client type (e.g. singbox, xray, mihomo, clash, outline)"
// @Success      200        {object}  map[string]any
// @Failure      404        {object}  shared.ErrorResponse
// @Failure      500        {object}  shared.ErrorResponse
// @Router       /sub/{shortUuid} [get]
// @Router       /sub/{shortUuid}/{client} [get]
// @Router       /sub/{shortUuid}/info [get]
func SubscriptionPublicHandler(db, backgroundDB *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/api/sub/")
		path = strings.Trim(path, "/")
		if path == "" {
			http.NotFound(w, r)
			return
		}

		parts := strings.Split(path, "/")
		if len(parts) == 0 {
			http.NotFound(w, r)
			return
		}

		if len(parts) == 2 && parts[1] == "info" {
			handlePublicSubscriptionInfo(w, r, db, backgroundDB, cfg, parts[0])
			return
		}

		if len(parts) >= 4 && parts[0] == "outline" {
			handlePublicOutlineSubscription(w, r, db, backgroundDB, cfg, parts)
			return
		}

		if len(parts) == 2 {
			handlePublicSubscription(w, r, db, backgroundDB, cfg, parts[0], parts[1])
			return
		}

		if len(parts) == 1 {
			handlePublicSubscription(w, r, db, backgroundDB, cfg, parts[0], "")
			return
		}

		http.NotFound(w, r)
	}
}

func handlePublicSubscriptionInfo(w http.ResponseWriter, r *http.Request, db, backgroundDB *pgxpool.Pool, cfg *config.BackendConfig, shortUUID string) {
	ctx := r.Context()
	renderService := NewRenderService(db, backgroundDB, cfg)

	user, err := getSubscriptionUserByShortUUID(ctx, db, shortUUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrUserNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrGetUserByError.WithCause(err), cfg)
		return
	}

	settings, err := loadSubscriptionSettings(ctx, db, cfg)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetSubscriptionSettingsFailed.WithCause(err), cfg)
		return
	}

	hosts, err := getHostsForUser(ctx, db, user)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetAllHostsFailed.WithCause(err), cfg)
		return
	}

	info := renderService.buildSubscriptionInfoResponse(user, settings, hosts)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"response": info,
	})
}

func handlePublicOutlineSubscription(w http.ResponseWriter, r *http.Request, db, backgroundDB *pgxpool.Pool, cfg *config.BackendConfig, parts []string) {
	ctx := r.Context()
	log := cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceHTTP)

	shortUUID := parts[1]
	reqType := parts[2]
	encodedTag := parts[3]

	tagBytes, err := base64.RawURLEncoding.DecodeString(encodedTag)
	if err != nil {
		tagBytes, err = base64.StdEncoding.DecodeString(encodedTag)
		if err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid encoded tag", err, cfg)
			return
		}
	}
	targetTag := string(tagBytes)

	user, err := getSubscriptionUserByShortUUID(ctx, db, shortUUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrUserNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrGetUserByError.WithCause(err), cfg)
		return
	}

	hosts, err := getHostsForUser(ctx, db, user)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetAllHostsFailed.WithCause(err), cfg)
		return
	}

	var targetHost *SubscriptionHost
	for _, h := range hosts {
		if h.InboundTag != nil && *h.InboundTag == targetTag {
			targetHost = &h
			break
		}
	}
	if targetHost == nil {
		shared.SendAPIError(w, shared.ErrHostNotFound, cfg)
		return
	}

	settings, err := loadSubscriptionSettings(ctx, db, cfg)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetSubscriptionSettingsFailed.WithCause(err), cfg)
		return
	}

	renderService := NewRenderService(db, backgroundDB, cfg)

	if reqType == "ss" || reqType == "shadowsocks" {
		links, err := NewXrayGenerator(cfg).GenerateLinks(user, []SubscriptionHost{*targetHost}, settings)
		if err != nil || len(links) == 0 {
			shared.SendAPIError(w, shared.ErrGenerateShadowsocksLinkFailed.WithCause(err), cfg)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte(links[0]))
		return
	}

	content, contentType, headers, err := renderService.RenderUserSubscription(
		ctx, user, r.UserAgent(), reqType, middleware.GetClientIP(r, cfg), ExtractHwidHeaders(r), settings,
	)
	if err != nil {
		if err == ErrBlocked {
			shared.SendAPIError(w, shared.ErrForbidden, cfg)
			return
		}
		if err == ErrNotFound {
			shared.SendAPIError(w, shared.ErrNotFound, cfg)
			return
		}
		if err == ErrUnavailableForLegalReasons {
			shared.SendError(w, http.StatusUnavailableForLegalReasons, "Unavailable For Legal Reasons", nil, cfg)
			return
		}
		log.Warn("Failed to render outline subscription", "short_uuid", shortUUID, "error", err)
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
		return
	}

	for k, v := range headers {
		w.Header().Set(k, v)
	}
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}

	etag := fmt.Sprintf(`W/"%08x-%x"`, crc32.ChecksumIEEE(content), len(content))
	w.Header().Set("ETag", etag)
	if match := r.Header.Get("If-None-Match"); match != "" && (match == etag || match == "*") {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	_, _ = w.Write(content)
}

func handlePublicSubscription(w http.ResponseWriter, r *http.Request, db, backgroundDB *pgxpool.Pool, cfg *config.BackendConfig, shortUUID string, clientType string) {
	ctx := r.Context()
	log := cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceHTTP)

	user, err := getSubscriptionUserByShortUUID(ctx, db, shortUUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrUserNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrGetUserByError.WithCause(err), cfg)
		return
	}

	renderService := NewRenderService(db, backgroundDB, cfg)

	settings, err := renderService.LoadSubscriptionSettings(ctx)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetSubscriptionSettingsFailed.WithCause(err), cfg)
		return
	}
	requestIP := middleware.GetClientIP(r, cfg)
	hwidHeaders := ResolveHwidHeaders(r, user.UUID, requestIP, settings.HwidSettings)

	content, contentType, headers, err := renderService.RenderUserSubscription(
		ctx, user, r.UserAgent(), clientType, requestIP, hwidHeaders, settings,
	)
	if err != nil {
		if err == ErrBlocked {
			shared.SendAPIError(w, shared.ErrForbidden, cfg)
			return
		}
		if err == ErrNotFound {
			shared.SendAPIError(w, shared.ErrNotFound, cfg)
			return
		}
		if err == ErrUnavailableForLegalReasons {
			shared.SendError(w, http.StatusUnavailableForLegalReasons, "Unavailable For Legal Reasons", nil, cfg)
			return
		}
		if err == ErrHwidCheckFailed {
			shared.SendAPIError(w, shared.ErrCheckHwidDeviceLimitFailed, cfg)
			return
		}
		if err == ErrUserDisabled {
			shared.SendAPIError(w, shared.ErrUserIsDisabledOrExpired, cfg)
			return
		}
		if err == ErrNoHosts {
			shared.SendAPIError(w, shared.ErrNoActiveHostsAvailable, cfg)
			return
		}
		log.Warn("Failed to render subscription", "short_uuid", shortUUID, "error", err)
		shared.SendAPIError(w, shared.ErrRenderSubscriptionFailed.WithCause(err), cfg)
		return
	}

	for k, v := range headers {
		w.Header().Set(k, v)
	}
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}

	etag := fmt.Sprintf(`W/"%08x-%x"`, crc32.ChecksumIEEE(content), len(content))
	w.Header().Set("ETag", etag)
	if match := r.Header.Get("If-None-Match"); match != "" && (match == etag || match == "*") {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	_, _ = w.Write(content)
}

func ResolveHwidHeaders(r *http.Request, userUUID, requestIP string, hwidSettings HwidSettings) *HwidHeaders {
	hwidHeaders := extractHwidHeaders(r)
	if hwidHeaders == nil && !hwidSettings.Enabled {
		hwidHeaders = extractSyntheticHwidHeaders(r, userUUID, requestIP)
	}
	if hwidHeaders != nil && hwidHeaders.RequestIP == nil {
		hwidHeaders.RequestIP = stringPtrIfNotEmpty(requestIP)
	}
	return hwidHeaders
}

func ExtractHwidHeaders(r *http.Request) *HwidHeaders {
	return extractHwidHeaders(r)
}

func (s *RenderService) GetSubscriptionUserByShortUUID(ctx context.Context, shortUUID string) (SubscriptionUser, error) {
	return getSubscriptionUserByShortUUID(ctx, s.db, shortUUID)
}

func (s *RenderService) GetHostsForUser(ctx context.Context, user SubscriptionUser) ([]SubscriptionHost, error) {
	return getHostsForUser(ctx, s.db, user)
}

func (s *RenderService) LoadSubscriptionSettings(ctx context.Context) (SubscriptionSettingsParsed, error) {
	return loadSubscriptionSettings(ctx, s.db, s.cfg)
}

func (s *RenderService) LoadExternalSquadOverrides(ctx context.Context, squadUUID string) (*ExternalSquadOverrides, error) {
	return loadExternalSquadOverrides(ctx, s.db, squadUUID, s.cfg)
}

func (s *RenderService) MergeSettings(base SubscriptionSettingsParsed, squad *ExternalSquadOverrides) SubscriptionSettingsParsed {
	return applyExternalSquadOverrides(base, squad)
}

func (s *RenderService) ApplyHostOverrides(hosts []SubscriptionHost, overrides map[string]HostOverride) []SubscriptionHost {
	return applyHostOverrides(hosts, overrides)
}

func (s *RenderService) CheckHwidDeviceLimit(ctx context.Context, user SubscriptionUser, hwid *HwidHeaders, settings HwidSettings) (HwidCheckupResult, error) {
	return checkHwidDeviceLimit(ctx, s.db, user, hwid, settings)
}

func (s *RenderService) UpdateSubscriptionRequest(ctx context.Context, userUUID string, userID int64, userAgent, requestIP, responseType, ruleName string) {
	updateSubscriptionRequest(ctx, s.backgroundDB, userUUID, userID, userAgent, requestIP, responseType, ruleName)
}

func (s *RenderService) GetSubscriptionTemplate(ctx context.Context, templateType string) ([]byte, error) {
	return getSubscriptionTemplate(ctx, s.db, templateType)
}

func (s *RenderService) GetSubscriptionTemplateByName(ctx context.Context, name string) (string, []byte, error) {
	return getSubscriptionTemplateByName(ctx, s.db, name)
}

func (s *RenderService) BuildResponseHeaders(ctx context.Context, user SubscriptionUser, settings SubscriptionSettingsParsed, contentType string) map[string]string {
	subscriptionURL := resolveSubscriptionURL(ctx, s.db, user, settings)
	return buildResponseHeaders(user, settings, contentType, subscriptionURL)
}

func (s *RenderService) MatchResponseRules(rules *subscriptionresponserules.Config, header http.Header) string {
	return matchResponseRules(rules, header)
}

func (s *RenderService) DetectClientType(userAgent string) string {
	return detectClientType(userAgent)
}

func (s *RenderService) BuildSubscriptionInfoResponse(user SubscriptionUser, settings SubscriptionSettingsParsed, hosts []SubscriptionHost) SubscriptionInfoResponse {
	return s.buildSubscriptionInfoResponse(user, settings, hosts)
}

func (s *RenderService) CopyMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	maps.Copy(dst, src)
	return dst
}

// SubpageConfigPublicHandler godoc
// @Summary      Get subpage config for user
// @Description  Evaluate matching subscription page config template based on request headers and user settings
// @Tags         [Public] Subscription Controller
// @Accept       json
// @Produce      json
// @Param        shortUuid  path      string  true  "User short UUID"
// @Param        body       body      object  false "Request headers mapping"
// @Success      200        {object}  map[string]any
// @Failure      400        {object}  shared.ErrorResponse
// @Failure      404        {object}  shared.ErrorResponse
// @Failure      500        {object}  shared.ErrorResponse
// @Router       /subscriptions/subpage-config/{shortUuid} [post]
func SubpageConfigPublicHandler(db, backgroundDB *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/api/subscriptions/subpage-config/")
		shortUUID := strings.Trim(path, "/")
		if shortUUID == "" {
			shared.SendError(w, http.StatusBadRequest, "shortUuid is required", nil, cfg)
			return
		}

		var req struct {
			RequestHeaders map[string]string `json:"requestHeaders"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if len(req.RequestHeaders) == 0 {
			req.RequestHeaders = make(map[string]string)
			for k, v := range r.Header {
				if len(v) > 0 {
					req.RequestHeaders[k] = v[0]
				}
			}
		}

		subpageConfigUUID, webpageAllowed, err := getSubpageConfigForUser(r.Context(), db, cfg, shortUUID, req.RequestHeaders)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				shared.SendAPIError(w, shared.ErrUserNotFound, cfg)
				return
			}
			shared.SendAPIError(w, shared.ErrGetSubpageConfigFailed.WithCause(err), cfg)
			return
		}

		var uuidPtr *string
		if subpageConfigUUID != "" {
			uuidPtr = &subpageConfigUUID
		}

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{
				"subpageConfigUuid": uuidPtr,
				"webpageAllowed":    webpageAllowed,
			},
		})
	}
}

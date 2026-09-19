package auth

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"exodus/internal/config"
	"exodus/internal/httpapi/middleware"
	"exodus/internal/httpapi/shared"
	"exodus/internal/notifications"
	"exodus/internal/panelsettings"
	"exodus/internal/security"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errAdminAlreadyConfigured = errors.New("admin already configured")

// AuthBootstrapHandler godoc
// @Summary      Get auth bootstrap data
// @Description  Get initial branding, password settings and whether an admin is configured
// @Tags         Auth Controller
// @Produce      json
// @Success      200  {object}  BootstrapResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /auth/bootstrap [get]
func AuthBootstrapHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		brandingSettings, passwordSettings, defaultUsername, hasAdmin, err := getBootstrapData(r.Context(), db)
		if err != nil {
			cfg.Logger.Error("Failed to read auth bootstrap data", "error", err)
			shared.SendAPIError(w, shared.ErrGetAuthBootstrapFailed.WithCause(err), cfg)
			return
		}

		shared.WriteJSON(w, http.StatusOK, BootstrapResponse{
			BrandingSettings:   brandingSettings,
			PasswordSettings:   passwordSettings,
			DefaultUsername:    defaultUsername,
			HasAdminConfigured: hasAdmin,
		})
	}
}

// AuthStatusHandler godoc
// @Summary      Get auth methods status
// @Description  Get enabled status for password, passkey and OAuth2 authentication methods
// @Tags         Auth Controller
// @Produce      json
// @Success      200  {object}  AuthStatusResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /auth/status [get]
func AuthStatusHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		brandingSettings, passwordSettings, _, hasAdmin, err := getBootstrapData(r.Context(), db)
		if err != nil {
			cfg.Logger.Error("Failed to load auth status", "error", err)
			shared.SendAPIError(w, shared.ErrGetAuthStatusFailed.WithCause(err), cfg)
			return
		}
		cfg.Logger.Trace("Auth status requested", "has_admin", hasAdmin, "remote_addr", r.RemoteAddr)

		title, _ := brandingSettings["title"].(string)
		logoURL, _ := brandingSettings["logoUrl"].(string)

		if hasAdmin {
			passkeyEnabled, oauth2Providers := getAuthMethodsStatus(r.Context(), db)
			passwordEnabled := resolvePasswordAuthEnabled(
				passwordSettings,
				passkeyEnabled,
				oauth2Providers,
				cfg,
			)

			shared.WriteJSON(w, http.StatusOK, map[string]any{
				"response": map[string]any{
					"isLoginAllowed":    true,
					"isRegisterAllowed": false,
					"authentication": map[string]any{
						"passkey": map[string]any{
							"enabled": passkeyEnabled,
						},
						"oauth2": map[string]any{
							"providers": oauth2Providers,
						},
						"password": map[string]any{
							"enabled": passwordEnabled,
						},
					},
					"branding": map[string]any{
						"title":   nullableString(title),
						"logoUrl": nullableString(logoURL),
					},
				},
			})
			return
		}

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{
				"isLoginAllowed":    false,
				"isRegisterAllowed": true,
				"authentication":    nil,
				"branding": map[string]any{
					"title":   nullableString(title),
					"logoUrl": nullableString(logoURL),
				},
			},
		})
	}
}

// AuthLoginHandler godoc
// @Summary      Admin login
// @Description  Authenticate admin with username and password
// @Tags         Auth Controller
// @Accept       json
// @Produce      json
// @Param        body  body      LoginRequest  true  "Admin login credentials"
// @Success      200   {object}  LoginResponse
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      403   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /auth/login [post]
func AuthLoginCompatHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		rateLimitKey := loginRateLimitKey(r, cfg)
		allowed, remaining, retryAfter := globalAuthRateLimiter.Allow(r.Context(), rateLimitKey, cfg)
		if !allowed {
			retrySeconds := int(math.Ceil(retryAfter.Seconds()))
			if retrySeconds <= 0 {
				retrySeconds = int(AuthRateLimitWindow.Seconds())
			}
			w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(AuthRateLimitMaxAttempts))
			w.Header().Set("X-RateLimit-Remaining", "0")
			cfg.Logger.Warn("Auth login rate limited", "client_ip", rateLimitKey, "retry_after", retrySeconds)
			shared.WriteJSONError(w, http.StatusTooManyRequests, "too many login attempts, please try again later")
			return
		}
		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(AuthRateLimitMaxAttempts))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))

		var req LoginRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			cfg.Logger.Warn("Auth login decode failed", "error", err, "client_ip", rateLimitKey)
			shared.WriteJSONError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}

		username := strings.TrimSpace(req.Username)
		password := req.Password
		if username == "" || password == "" {
			shared.SendError(w, http.StatusBadRequest, "username and password are required", nil, cfg)
			return
		}
		cfg.Logger.Trace("Auth login attempt", "username", username, "client_ip", rateLimitKey)

		_, passwordSettings, _, hasAdmin, err := getBootstrapData(r.Context(), db)
		if err != nil {
			cfg.Logger.Error("Failed to read auth bootstrap for login", "error", err)
			shared.SendAPIError(w, shared.ErrValidateCredentialsFailed.WithCause(err), cfg)
			return
		}

		if !hasAdmin {
			cfg.Logger.Warn("Auth login blocked: no admin configured", "username", username, "client_ip", rateLimitKey)
			emitLoginNotification(r.Context(), cfg, notifications.EventLoginAttemptFailed, username, "", password, "no_admin_configured", r)
			shared.SendAPIError(w, shared.ErrLoginNotAllowed, cfg)
			return
		}

		passkeyEnabled, oauth2Providers := getAuthMethodsStatus(r.Context(), db)
		passwordEnabled := resolvePasswordAuthEnabled(
			passwordSettings,
			passkeyEnabled,
			oauth2Providers,
			cfg,
		)
		if !passwordEnabled {
			cfg.Logger.Warn("Auth login blocked: password auth disabled", "username", username, "client_ip", rateLimitKey)
			emitLoginNotification(r.Context(), cfg, notifications.EventLoginAttemptFailed, username, "", password, "password_auth_disabled", r)
			shared.SendAPIError(w, shared.ErrLoginNotAllowed, cfg)
			return
		}

		var (
			adminUUID          string
			storedPasswordHash string
			role               string
		)

		row := db.QueryRow(r.Context(), `
			SELECT uuid, password_hash, role
			FROM admin
			WHERE username = $1
			LIMIT 1
		`, username)

		scanErr := row.Scan(&adminUUID, &storedPasswordHash, &role)
		if scanErr != nil && !errors.Is(scanErr, pgx.ErrNoRows) {
			cfg.Logger.Error("Failed to read admin credentials", "error", scanErr)
			shared.SendAPIError(w, shared.ErrValidateCredentialsFailed.WithCause(scanErr), cfg)
			return
		}

		if errors.Is(scanErr, pgx.ErrNoRows) {
			_ = security.VerifyPassword(password, cfg.JWT.AuthSecret, getDummyPasswordHash(cfg.JWT.AuthSecret))
			globalAuthRateLimiter.RecordFailedAttempt(r.Context(), rateLimitKey, cfg)
			if remaining > 0 {
				w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining-1))
			} else {
				w.Header().Set("X-RateLimit-Remaining", "0")
			}
			cfg.Logger.Warn("Auth login failed: user not found", "username", username, "client_ip", rateLimitKey)
			emitLoginNotification(r.Context(), cfg, notifications.EventLoginAttemptFailed, username, "", password, "invalid_credentials", r)
			if !delayFailedLogin(r.Context()) {
				return
			}
			shared.SendAPIError(w, shared.ErrInvalidCredentials, cfg)
			return
		}

		if !security.VerifyPassword(password, cfg.JWT.AuthSecret, storedPasswordHash) {
			globalAuthRateLimiter.RecordFailedAttempt(r.Context(), rateLimitKey, cfg)
			if remaining > 0 {
				w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining-1))
			} else {
				w.Header().Set("X-RateLimit-Remaining", "0")
			}
			cfg.Logger.Warn("Auth login failed: wrong password", "username", username, "client_ip", rateLimitKey)
			emitLoginNotification(r.Context(), cfg, notifications.EventLoginAttemptFailed, username, adminUUID, password, "invalid_credentials", r)
			if !delayFailedLogin(r.Context()) {
				return
			}
			shared.SendAPIError(w, shared.ErrInvalidCredentials, cfg)
			return
		}

		accessToken, expiresAt, err := createAdminAccessToken(cfg, username, adminUUID, role)
		if err != nil {
			cfg.Logger.Error("Failed to create JWT auth token", "error", err)
			shared.SendAPIError(w, shared.ErrCreateAuthTokenFailed.WithCause(err), cfg)
			return
		}
		globalAuthRateLimiter.Reset(r.Context(), rateLimitKey, cfg)
		cfg.Logger.Info("Auth login success", "username", username, "admin_uuid", adminUUID, "client_ip", rateLimitKey)
		emitLoginNotification(r.Context(), cfg, notifications.EventLoginAttemptSuccess, username, adminUUID, "", "", r)
		setAuthCookie(w, r, cfg, accessToken, expiresAt)

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{
				"accessToken": accessToken,
			},
		})
	}
}

// AuthRegisterHandler godoc
// @Summary      Register admin
// @Description  Register initial admin account when none exists
// @Tags         Auth Controller
// @Accept       json
// @Produce      json
// @Param        body  body      LoginRequest  true  "Registration payload"
// @Success      201   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      403   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /auth/register [post]
func AuthRegisterHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var req LoginRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			cfg.Logger.Warn("Auth register decode failed", "error", err, "remote_addr", r.RemoteAddr)
			shared.SendError(w, http.StatusBadRequest, "invalid JSON body", err, cfg)
			return
		}

		username := strings.TrimSpace(req.Username)
		password := req.Password
		if username == "" || password == "" {
			shared.SendError(w, http.StatusBadRequest, "username and password are required", nil, cfg)
			return
		}
		cfg.Logger.Trace("Auth register attempt", "username", username, "remote_addr", r.RemoteAddr)

		passwordHash, err := security.HashPassword(password, cfg.JWT.AuthSecret)
		if err != nil {
			shared.SendAPIError(w, shared.ErrCreateAdminAccountFailed.WithCause(err), cfg)
			return
		}

		var adminCount int
		if countErr := db.QueryRow(r.Context(), "SELECT COUNT(*) FROM admin").Scan(&adminCount); countErr != nil {
			shared.SendAPIError(w, shared.ErrCheckAdminStatusFailed.WithCause(countErr), cfg)
			return
		}
		if adminCount > 0 {
			cfg.Logger.Warn("Auth register blocked: admin already configured", "username", username, "remote_addr", r.RemoteAddr)
			shared.SendAPIError(w, shared.ErrRegistrationNotAllowed, cfg)
			return
		}

		adminUUID := uuid.NewString()
		if _, execErr := db.Exec(r.Context(), `
			INSERT INTO admin (uuid, username, password_hash, role)
			VALUES ($1, $2, $3, 'ADMIN')
		`, adminUUID, username, passwordHash); execErr != nil {
			cfg.Logger.Error("Auth register failed", "username", username, "remote_addr", r.RemoteAddr, "error", execErr)
			shared.SendAPIError(w, shared.ErrCreateAdminAccountFailed.WithCause(execErr), cfg)
			return
		}

		accessToken, expiresAt, err := createAdminAccessToken(cfg, username, adminUUID, "ADMIN")
		if err != nil {
			cfg.Logger.Error("Failed to create JWT auth token", "error", err)
			shared.SendAPIError(w, shared.ErrCreateAuthTokenFailed.WithCause(err), cfg)
			return
		}
		cfg.Logger.Info("Auth register success", "username", username, "remote_addr", r.RemoteAddr)
		setAuthCookie(w, r, cfg, accessToken, expiresAt)

		shared.WriteJSON(w, http.StatusCreated, map[string]any{
			"response": map[string]any{
				"accessToken": accessToken,
			},
		})
	}
}

func AuthRegisterCompatHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return AuthRegisterHandler(db, cfg)
}

// AuthSetupHandler godoc
// @Summary      Setup admin
// @Description  Set up first admin account
// @Tags         Auth Controller
// @Accept       json
// @Produce      json
// @Param        body  body      SetupRequest  true  "Admin setup payload"
// @Success      201   {object}  LoginResponse
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      409   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /auth/setup [post]
func AuthSetupHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var req SetupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid JSON body", err, cfg)
			return
		}

		username := strings.TrimSpace(req.Username)
		password := req.Password
		if username == "" || password == "" {
			shared.SendError(w, http.StatusBadRequest, "username and password are required", nil, cfg)
			return
		}

		passwordHash, err := security.HashPassword(password, cfg.JWT.AuthSecret)
		if err != nil {
			cfg.Logger.Error("Failed to hash setup password", "error", err)
			shared.SendAPIError(w, shared.ErrCreateAdminAccountFailed.WithCause(err), cfg)
			return
		}

		var adminCount int
		if countErr := db.QueryRow(r.Context(), "SELECT COUNT(*) FROM admin").Scan(&adminCount); countErr != nil {
			shared.SendAPIError(w, shared.ErrCheckAdminStatusFailed.WithCause(countErr), cfg)
			return
		}
		if adminCount > 0 {
			shared.SendAPIError(w, shared.ErrAdminAlreadyExists, cfg)
			return
		}

		adminUUID := uuid.NewString()
		if _, execErr := db.Exec(r.Context(), `
			INSERT INTO admin (uuid, username, password_hash, role)
			VALUES ($1, $2, $3, 'ADMIN')
		`, adminUUID, username, passwordHash); execErr != nil {
			cfg.Logger.Error("Failed to create initial admin account", "error", execErr)
			shared.SendAPIError(w, shared.ErrCreateAdminAccountFailed.WithCause(execErr), cfg)
			return
		}

		accessToken, expiresAt, err := createAdminAccessToken(cfg, username, adminUUID, "ADMIN")
		if err != nil {
			cfg.Logger.Error("Failed to create JWT auth token", "error", err)
			shared.SendAPIError(w, shared.ErrCreateAuthTokenFailed.WithCause(err), cfg)
			return
		}
		setAuthCookie(w, r, cfg, accessToken, expiresAt)

		brandingSettings, passwordSettings, _, _, bootstrapErr := getBootstrapData(r.Context(), db)
		if bootstrapErr != nil {
			cfg.Logger.Warn("Failed to include bootstrap settings in setup response", "error", bootstrapErr)
			brandingSettings = panelsettings.DefaultBrandingSettings()
			passwordSettings = panelsettings.DefaultPasswordSettings()
		}

		shared.WriteJSON(w, http.StatusCreated, LoginResponse{
			Admin: AuthAdminInfo{
				UUID:              adminUUID,
				Username:          username,
				Role:              "ADMIN",
				SessionTTLMinutes: AuthTTLMinutes(),
			},
			ExpiresAt:        expiresAt,
			BrandingSettings: brandingSettings,
			PasswordSettings: passwordSettings,
			Message:          "admin account created",
		})
	}
}

func AuthLoginHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return AuthLoginCompatHandler(db, cfg)
}

// AuthMeHandler godoc
// @Summary      Get current session
// @Description  Get authenticated admin user profile and settings
// @Tags         Auth Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  LoginResponse
// @Failure      401  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /auth/me [get]
func AuthMeHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		principal, ok := CurrentAuthPrincipal(r.Context())
		if !ok || principal == nil || principal.TokenType != "jwt_auth" {
			shared.SendAPIError(w, shared.ErrUnauthorized, cfg)
			return
		}

		var (
			adminInfo AuthAdminInfo
			expiresAt = principal.ExpiresAt
		)
		row := db.QueryRow(r.Context(), `
			SELECT uuid, username, role
			FROM admin
			WHERE uuid = $1
			LIMIT 1
		`, principal.AdminUUID)

		if scanErr := row.Scan(&adminInfo.UUID, &adminInfo.Username, &adminInfo.Role); scanErr != nil {
			if errors.Is(scanErr, pgx.ErrNoRows) {
				shared.SendAPIError(w, shared.ErrUnauthorized, cfg)
				return
			}
			cfg.Logger.Error("Failed to load auth me payload", "error", scanErr)
			shared.SendAPIError(w, shared.ErrLoadCurrentSessionFailed.WithCause(scanErr), cfg)
			return
		}
		adminInfo.Role = strings.ToUpper(adminInfo.Role)
		adminInfo.SessionTTLMinutes = AuthTTLMinutes()

		brandingSettings, passwordSettings, _, _, err := getBootstrapData(r.Context(), db)
		if err != nil {
			cfg.Logger.Warn("Failed to load branding for auth/me response", "error", err)
			brandingSettings = panelsettings.DefaultBrandingSettings()
			passwordSettings = panelsettings.DefaultPasswordSettings()
		}

		shared.WriteJSON(w, http.StatusOK, LoginResponse{
			Admin:            adminInfo,
			ExpiresAt:        expiresAt,
			BrandingSettings: brandingSettings,
			PasswordSettings: passwordSettings,
			Principal:        principal,
		})
	}
}

// AuthLogoutHandler godoc
// @Summary      Logout admin
// @Description  Clear admin session cookie
// @Tags         Auth Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]string
// @Router       /auth/logout [post]
func AuthLogoutHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		secureCookie := middleware.IsSecureRequest(r, cfg)
		http.SetCookie(w, &http.Cookie{
			Name:     sessionCookieName,
			Value:    "",
			Path:     "/",
			Expires:  time.Unix(0, 0).UTC(),
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
			Secure:   secureCookie,
			MaxAge:   -1,
		})

		toolsCookiePath := cfg.Backend.Trimmed() + "/api/backend-tools"
		http.SetCookie(w, &http.Cookie{
			Name:     "ex-tools",
			Value:    "",
			Path:     toolsCookiePath,
			Expires:  time.Unix(0, 0).UTC(),
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
			Secure:   secureCookie,
			MaxAge:   -1,
		})
		shared.WriteJSON(w, http.StatusOK, map[string]string{"message": "logged out"})
	}
}

// OAuth2AuthorizeHandler godoc
// @Summary      Authorize OAuth2
// @Description  Get authorization URL for third-party OAuth2 provider
// @Tags         Auth Controller
// @Accept       json
// @Produce      json
// @Param        body  body      oauthAuthorizeRequest  true  "OAuth2 provider"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /auth/oauth2/authorize [post]
func OAuth2AuthorizeHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req oauthAuthorizeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
			return
		}
		provider := strings.ToLower(strings.TrimSpace(req.Provider))
		if !isSupportedOAuthProvider(provider) {
			shared.SendAPIError(w, shared.ErrOAuth2ProviderNotFound, cfg)
			return
		}
		if !isLoginAllowed(r.Context(), db) {
			shared.SendAPIError(w, shared.ErrForbidden, cfg)
			return
		}
		settings, err := loadOAuthSettings(r.Context(), db)
		if err != nil {
			shared.SendAPIError(w, shared.ErrOAuth2AuthorizeFailed.WithCause(err), cfg)
			return
		}
		providerSettings := getOAuthProviderSettings(settings, provider)
		if !providerSettings.Enabled {
			shared.SendAPIError(w, shared.ErrOAuth2ProviderDisabled, cfg)
			return
		}

		state, err := security.GenerateRandomToken(32)
		if err != nil {
			shared.SendAPIError(w, shared.ErrOAuth2AuthorizeFailed.WithCause(err), cfg)
			return
		}
		codeVerifier := ""
		authURL, err := buildAuthorizationURL(provider, providerSettings, cfg.Backend.Trimmed(), state, &codeVerifier)
		if err != nil {
			shared.SendAPIError(w, shared.ErrOAuth2AuthorizeFailed.WithCause(err), cfg)
			return
		}
		storeOAuthState(provider, state, codeVerifier)
		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{"authorizationUrl": authURL},
		})
	}
}

func OAuth2CallbackHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req oauthCallbackRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
			return
		}
		provider := strings.ToLower(strings.TrimSpace(req.Provider))
		if !isSupportedOAuthProvider(provider) {
			emitExternalLoginNotification(r.Context(), cfg, notifications.EventLoginAttemptFailed, "oauth2", provider, "", "", "unsupported_provider", r)
			shared.SendAPIError(w, shared.ErrOAuth2ProviderNotFound, cfg)
			return
		}
		stateEntry, ok := takeOAuthState(provider)
		if !ok || stateEntry.State != strings.TrimSpace(req.State) {
			emitExternalLoginNotification(r.Context(), cfg, notifications.EventLoginAttemptFailed, "oauth2", provider, "", "", "state_mismatch", r)
			shared.SendAPIError(w, shared.ErrOAuth2StateMismatch, cfg)
			return
		}
		if !isLoginAllowed(r.Context(), db) {
			emitExternalLoginNotification(r.Context(), cfg, notifications.EventLoginAttemptFailed, "oauth2", provider, "", "", "login_not_allowed", r)
			shared.SendAPIError(w, shared.ErrForbidden, cfg)
			return
		}
		settings, err := loadOAuthSettings(r.Context(), db)
		if err != nil {
			shared.SendAPIError(w, shared.ErrOAuth2CallbackFailed.WithCause(err), cfg)
			return
		}
		providerSettings := getOAuthProviderSettings(settings, provider)
		if !providerSettings.Enabled {
			emitExternalLoginNotification(r.Context(), cfg, notifications.EventLoginAttemptFailed, "oauth2", provider, "", "", "provider_disabled", r)
			shared.SendAPIError(w, shared.ErrOAuth2ProviderDisabled, cfg)
			return
		}
		email, hasCustomClaim, err := exchangeOAuthCode(r.Context(), provider, providerSettings, cfg.Backend.Trimmed(), strings.TrimSpace(req.Code), stateEntry.CodeVerifier)
		if err != nil {
			emitExternalLoginNotification(r.Context(), cfg, notifications.EventLoginAttemptFailed, "oauth2", provider, "", "", "callback_error", r)
			shared.SendAPIError(w, shared.ErrOAuth2CallbackFailed.WithCause(err), cfg)
			return
		}
		if email == "" || !isOAuthPrincipalAllowed(provider, email, hasCustomClaim, providerSettings) {
			emitExternalLoginNotification(r.Context(), cfg, notifications.EventLoginAttemptFailed, "oauth2", provider, email, "", "email_not_allowed", r)
			shared.SendAPIError(w, shared.ErrOAuth2PrincipalNotAllowed, cfg)
			return
		}
		token, adminUUID, err := createFirstAdminSession(w, r, db, cfg)
		if err != nil {
			emitExternalLoginNotification(r.Context(), cfg, notifications.EventLoginAttemptFailed, "oauth2", provider, email, "", "session_create_failed", r)
			shared.SendAPIError(w, shared.ErrOAuth2SessionCreateFailed.WithCause(err), cfg)
			return
		}
		emitExternalLoginNotification(r.Context(), cfg, notifications.EventLoginAttemptSuccess, "oauth2", provider, email, adminUUID, "", r)
		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{"accessToken": token},
		})
	}
}

func delayFailedLogin(ctx context.Context) bool {
	timer := time.NewTimer(AuthFailedLoginDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}


package auth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"exodus/internal/config"
	"exodus/internal/httpapi/middleware"
	"exodus/internal/notifications"
	"exodus/internal/security"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	oauthStateTTL        = 10 * time.Minute
	oauthScope           = "openid email profile"
	customOAuthClaimName = "exodusAccess"
)

var oauthStateCache = struct {
	sync.Mutex
	items map[string]oauthStateEntry
}{
	items: make(map[string]oauthStateEntry),
}

func isLoginAllowed(ctx context.Context, db *pgxpool.Pool) bool {
	var count int
	if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM admin").Scan(&count); err != nil {
		return false
	}
	return count > 0
}

func createFirstAdminSession(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) (string, string, error) {
	var adminUUID, username, role string
	row := db.QueryRow(r.Context(), `
		SELECT uuid, username, role
		FROM admin
		WHERE UPPER(role) = 'ADMIN'
		ORDER BY created_at ASC
		LIMIT 1
	`)
	if err := row.Scan(&adminUUID, &username, &role); err != nil {
		return "", "", err
	}

	accessToken, expiresAt, err := createAdminAccessToken(cfg, username, adminUUID, role)
	if err != nil {
		return "", "", err
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    accessToken,
		Path:     "/",
		Expires:  time.Unix(expiresAt, 0).UTC(),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   middleware.IsSecureRequest(r, cfg),
	})
	return accessToken, adminUUID, nil
}

func emitExternalLoginNotification(ctx context.Context, cfg *config.BackendConfig, event, method, provider, identifier, adminUUID, reason string, r *http.Request) {
	data := externalLoginNotificationData(cfg, method, provider, identifier, adminUUID, reason, r)
	notifications.Emit(ctx, cfg, notifications.Event{
		Scope: notifications.ScopeService,
		Event: event,
		Data:  data,
	})
}

func externalLoginNotificationData(cfg *config.BackendConfig, method, provider, identifier, adminUUID, reason string, r *http.Request) map[string]any {
	username := externalLoginNotificationUsername(method, provider, identifier)
	loginAttempt := map[string]any{
		"username":    username,
		"password":    "",
		"ip":          notificationClientIP(cfg, r),
		"userAgent":   "",
		"description": reason,
		"method":      strings.TrimSpace(method),
		"provider":    strings.TrimSpace(provider),
	}
	data := map[string]any{
		"method":       strings.TrimSpace(method),
		"provider":     strings.TrimSpace(provider),
		"username":     username,
		"ip":           loginAttempt["ip"],
		"description":  reason,
		"loginAttempt": loginAttempt,
	}
	if identifier != "" {
		data["identifier"] = strings.TrimSpace(identifier)
	}
	if adminUUID != "" {
		data["adminUuid"] = adminUUID
		loginAttempt["adminUuid"] = adminUUID
	}
	if reason != "" {
		data["reason"] = reason
		loginAttempt["reason"] = reason
	}
	if r != nil {
		data["remoteAddr"] = r.RemoteAddr
		data["userAgent"] = r.UserAgent()
		data["path"] = r.URL.Path
		loginAttempt["userAgent"] = r.UserAgent()
		loginAttempt["path"] = r.URL.Path
	}
	return data
}

func externalLoginNotificationUsername(method, provider, identifier string) string {
	if trimmed := strings.TrimSpace(identifier); trimmed != "" {
		return trimmed
	}
	provider = strings.TrimSpace(provider)
	method = strings.TrimSpace(method)
	switch {
	case method != "" && provider != "":
		return method + ":" + provider
	case provider != "":
		return provider
	default:
		return method
	}
}

func loadOAuthSettings(ctx context.Context, db *pgxpool.Pool) (oauthSettings, error) {
	var out oauthSettings
	var raw *string
	if err := db.QueryRow(ctx, `SELECT oauth2_settings FROM exodus_settings WHERE id = 1 LIMIT 1`).Scan(&raw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return out, nil
		}
		return out, err
	}
	if raw != nil && strings.TrimSpace(*raw) != "" {
		err := json.Unmarshal([]byte(*raw), &out)
		return out, err
	}
	return out, nil
}

func isSupportedOAuthProvider(provider string) bool {
	switch provider {
	case "github", "pocketid", "yandex", "keycloak", "generic", "telegram":
		return true
	default:
		return false
	}
}

func getOAuthProviderSettings(settings oauthSettings, provider string) oauthProviderSettings {
	switch provider {
	case "github":
		return settings.Github
	case "pocketid":
		return settings.PocketID
	case "yandex":
		return settings.Yandex
	case "keycloak":
		return settings.Keycloak
	case "generic":
		return settings.Generic
	case "telegram":
		return settings.Telegram
	default:
		return oauthProviderSettings{}
	}
}

func buildAuthorizationURL(provider string, settings oauthProviderSettings, basePath, state string, codeVerifier *string) (string, error) {
	switch provider {
	case "github":
		if settings.ClientID == "" || settings.ClientSecret == "" {
			return "", errors.New("github OAuth2 settings are incomplete")
		}
		return makeOAuthURL("https://github.com/login/oauth/authorize", url.Values{
			"client_id": {settings.ClientID},
			"state":     {state},
			"scope":     {"read:user user:email"},
		})
	case "yandex":
		if settings.ClientID == "" || settings.ClientSecret == "" {
			return "", errors.New("yandex OAuth2 settings are incomplete")
		}
		return makeOAuthURL("https://oauth.yandex.ru/authorize", url.Values{
			"response_type": {"code"},
			"client_id":     {settings.ClientID},
			"state":         {state},
			"scope":         {"login:email"},
		})
	case "pocketid":
		if settings.ClientID == "" || settings.ClientSecret == "" || settings.PlainDomain == "" {
			return "", errors.New("pocketid OAuth2 settings are incomplete")
		}
		return makeOAuthURL("https://"+settings.PlainDomain+"/authorize", url.Values{
			"response_type": {"code"},
			"client_id":     {settings.ClientID},
			"state":         {state},
			"scope":         {oauthScope},
		})
	case "keycloak":
		if settings.ClientID == "" || settings.ClientSecret == "" || settings.KeycloakDomain == "" || settings.Realm == "" || settings.FrontendDomain == "" {
			return "", errors.New("keycloak OAuth2 settings are incomplete")
		}
		verifier, challenge, err := generatePKCEPair()
		if err != nil {
			return "", err
		}
		*codeVerifier = verifier
		return makeOAuthURL(fmt.Sprintf("https://%s/realms/%s/protocol/openid-connect/auth", settings.KeycloakDomain, settings.Realm), url.Values{
			"response_type":         {"code"},
			"client_id":             {settings.ClientID},
			"redirect_uri":          {oauthRedirectURI(settings.FrontendDomain, basePath, provider)},
			"state":                 {state},
			"scope":                 {oauthScope},
			"code_challenge_method": {"S256"},
			"code_challenge":        {challenge},
		})
	case "generic":
		if settings.ClientID == "" || settings.ClientSecret == "" || settings.AuthorizationURL == "" || settings.TokenURL == "" || settings.FrontendDomain == "" {
			return "", errors.New("generic OAuth2 settings are incomplete")
		}
		values := url.Values{
			"response_type": {"code"},
			"client_id":     {settings.ClientID},
			"redirect_uri":  {oauthRedirectURI(settings.FrontendDomain, basePath, provider)},
			"state":         {state},
			"scope":         {oauthScope},
		}
		if settings.WithPKCE {
			verifier, challenge, err := generatePKCEPair()
			if err != nil {
				return "", err
			}
			*codeVerifier = verifier
			values.Set("code_challenge_method", "S256")
			values.Set("code_challenge", challenge)
		}
		return makeOAuthURL(settings.AuthorizationURL, values)
	case "telegram":
		if settings.ClientID == "" || settings.ClientSecret == "" || settings.FrontendDomain == "" {
			return "", errors.New("telegram OAuth2 settings are incomplete")
		}
		verifier, challenge, err := generatePKCEPair()
		if err != nil {
			return "", err
		}
		*codeVerifier = verifier
		return makeOAuthURL("https://oauth.telegram.org/auth", url.Values{
			"response_type":         {"code"},
			"client_id":             {settings.ClientID},
			"redirect_uri":          {oauthRedirectURI(settings.FrontendDomain, basePath, provider)},
			"state":                 {state},
			"scope":                 {"openid profile telegram:bot_access"},
			"code_challenge_method": {"S256"},
			"code_challenge":        {challenge},
		})
	default:
		return "", errors.New("unsupported OAuth2 provider")
	}
}

func makeOAuthURL(rawURL string, values url.Values) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	for key, vals := range values {
		if len(vals) > 0 && strings.TrimSpace(vals[0]) != "" {
			q.Set(key, vals[0])
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func exchangeOAuthCode(ctx context.Context, provider string, settings oauthProviderSettings, basePath, code, codeVerifier string) (string, bool, error) {
	switch provider {
	case "github":
		token, err := requestOAuthToken(ctx, "https://github.com/login/oauth/access_token", url.Values{
			"grant_type":    {"authorization_code"},
			"client_id":     {settings.ClientID},
			"client_secret": {settings.ClientSecret},
			"code":          {code},
		})
		if err != nil {
			return "", false, err
		}
		return fetchGithubPrimaryEmail(ctx, token.AccessToken)
	case "yandex":
		token, err := requestOAuthToken(ctx, "https://oauth.yandex.ru/token", url.Values{
			"grant_type":    {"authorization_code"},
			"client_id":     {settings.ClientID},
			"client_secret": {settings.ClientSecret},
			"code":          {code},
		})
		if err != nil {
			return "", false, err
		}
		return fetchYandexEmail(ctx, token.AccessToken)
	case "pocketid":
		token, err := requestOAuthToken(ctx, "https://"+settings.PlainDomain+"/api/oidc/token", url.Values{
			"grant_type":    {"authorization_code"},
			"client_id":     {settings.ClientID},
			"client_secret": {settings.ClientSecret},
			"code":          {code},
		})
		if err != nil {
			return "", false, err
		}
		return extractEmailFromIDToken(token.IDToken)
	case "keycloak":
		token, err := requestOAuthToken(ctx, fmt.Sprintf("https://%s/realms/%s/protocol/openid-connect/token", settings.KeycloakDomain, settings.Realm), url.Values{
			"grant_type":    {"authorization_code"},
			"client_id":     {settings.ClientID},
			"client_secret": {settings.ClientSecret},
			"code":          {code},
			"redirect_uri":  {oauthRedirectURI(settings.FrontendDomain, basePath, provider)},
			"code_verifier": {codeVerifier},
		})
		if err != nil {
			return "", false, err
		}
		return extractEmailFromIDToken(token.IDToken)
	case "generic":
		values := url.Values{
			"grant_type":    {"authorization_code"},
			"client_id":     {settings.ClientID},
			"client_secret": {settings.ClientSecret},
			"code":          {code},
			"redirect_uri":  {oauthRedirectURI(settings.FrontendDomain, basePath, provider)},
		}
		if settings.WithPKCE {
			values.Set("code_verifier", codeVerifier)
		}
		token, err := requestOAuthToken(ctx, settings.TokenURL, values)
		if err != nil {
			return "", false, err
		}
		return extractEmailFromIDToken(token.IDToken)
	case "telegram":
		token, err := requestOAuthToken(ctx, "https://oauth.telegram.org/token", url.Values{
			"grant_type":    {"authorization_code"},
			"client_id":     {settings.ClientID},
			"client_secret": {settings.ClientSecret},
			"code":          {code},
			"redirect_uri":  {oauthRedirectURI(settings.FrontendDomain, basePath, provider)},
			"code_verifier": {codeVerifier},
		})
		if err != nil {
			return "", false, err
		}
		return extractTelegramIDFromIDToken(token.IDToken)
	default:
		return "", false, errors.New("unsupported OAuth2 provider")
	}
}

func requestOAuthToken(ctx context.Context, endpoint string, values url.Values) (oauthTokenResponse, error) {
	var out oauthTokenResponse
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(values.Encode()))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return out, fmt.Errorf("token endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return out, err
	}
	if out.Error != "" {
		return out, fmt.Errorf("%s: %s", out.Error, out.Description)
	}
	return out, nil
}

func fetchGithubPrimaryEmail(ctx context.Context, accessToken string) (string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/user/emails", nil)
	if err != nil {
		return "", false, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("User-Agent", "Exodus")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return "", false, fmt.Errorf("github email endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var emails []struct {
		Email   string `json:"email"`
		Primary bool   `json:"primary"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&emails); err != nil {
		return "", false, err
	}
	for _, item := range emails {
		if item.Primary {
			return item.Email, false, nil
		}
	}
	return "", false, errors.New("github primary email not found")
}

func fetchYandexEmail(ctx context.Context, accessToken string) (string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://login.yandex.ru/info?format=json", nil)
	if err != nil {
		return "", false, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("User-Agent", "Exodus")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return "", false, fmt.Errorf("yandex info endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload struct {
		DefaultEmail string `json:"default_email"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", false, err
	}
	if payload.DefaultEmail == "" {
		return "", false, errors.New("yandex email not found")
	}
	return payload.DefaultEmail, false, nil
}

func extractEmailFromIDToken(idToken string) (string, bool, error) {
	parts := strings.Split(idToken, ".")
	if len(parts) < 2 {
		return "", false, errors.New("invalid id_token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", false, err
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", false, err
	}
	email, _ := claims["email"].(string)
	customClaim, _ := claims[customOAuthClaimName].(bool)
	if email == "" {
		return "", customClaim, errors.New("missing email in id_token")
	}
	return email, customClaim, nil
}

func extractTelegramIDFromIDToken(idToken string) (string, bool, error) {
	parts := strings.Split(idToken, ".")
	if len(parts) < 2 {
		return "", false, errors.New("invalid id_token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", false, err
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", false, err
	}
	switch id := claims["id"].(type) {
	case string:
		id = strings.TrimSpace(id)
		if id == "" {
			return "", false, errors.New("missing telegram id in id_token")
		}
		return id, false, nil
	case float64:
		return strconv.FormatInt(int64(id), 10), false, nil
	default:
		return "", false, errors.New("missing telegram id in id_token")
	}
}

func storeOAuthState(provider, state, codeVerifier string) {
	oauthStateCache.Lock()
	defer oauthStateCache.Unlock()
	now := time.Now()
	for key, item := range oauthStateCache.items {
		if now.After(item.ExpiresAt) {
			delete(oauthStateCache.items, key)
		}
	}
	oauthStateCache.items[provider] = oauthStateEntry{
		State:        state,
		CodeVerifier: codeVerifier,
		ExpiresAt:    now.Add(oauthStateTTL),
	}
}

func takeOAuthState(provider string) (oauthStateEntry, bool) {
	oauthStateCache.Lock()
	defer oauthStateCache.Unlock()
	item, ok := oauthStateCache.items[provider]
	delete(oauthStateCache.items, provider)
	if !ok || time.Now().After(item.ExpiresAt) {
		return oauthStateEntry{}, false
	}
	return item, true
}

func generatePKCEPair() (string, string, error) {
	verifier, err := security.GenerateRandomToken(64)
	if err != nil {
		return "", "", err
	}
	sum := sha256.Sum256([]byte(verifier))
	return verifier, base64.RawURLEncoding.EncodeToString(sum[:]), nil
}

func oauthRedirectURI(frontendDomain, basePath, provider string) string {
	base := strings.TrimSuffix(strings.TrimSpace(basePath), "/")
	if base == "/" {
		base = ""
	}
	return "https://" + strings.TrimSuffix(frontendDomain, "/") + base + "/oauth2/callback/" + provider
}

func isEmailAllowed(email string, allowed []string) bool {
	email = strings.ToLower(strings.TrimSpace(email))
	for _, item := range allowed {
		if strings.ToLower(strings.TrimSpace(item)) == email {
			return true
		}
	}
	return false
}

func isOAuthPrincipalAllowed(provider, principal string, hasCustomClaim bool, settings oauthProviderSettings) bool {
	if provider == "telegram" {
		return containsString(settings.AllowedIDs, principal)
	}
	return hasCustomClaim || isEmailAllowed(principal, settings.AllowedEmails)
}

func containsString(items []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, item := range items {
		if strings.TrimSpace(item) == target {
			return true
		}
	}
	return false
}

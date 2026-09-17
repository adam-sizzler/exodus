package panelsettings

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	panelsettingsDefaults "exodus/internal/panelsettings"
)

type PanelSettingsResponse struct {
	Settings map[string]any `json:"settings"`
}

type APITokenRecord struct {
	UUID      string    `json:"uuid"`
	Token     string    `json:"-"`
	Name      string    `json:"name"`
	ExpireAt  time.Time `json:"expire_at"`
	Scopes    []string  `json:"scopes"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type apiTokenResponse struct {
	UUID      string    `json:"uuid"`
	Name      string    `json:"name"`
	Token     string    `json:"token,omitempty"`
	ExpireAt  time.Time `json:"expireAt"`
	Scopes    []string  `json:"scopes"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func toAPITokensResponse(items []APITokenRecord) []apiTokenResponse {
	out := make([]apiTokenResponse, 0, len(items))
	for _, item := range items {
		out = append(out, toAPITokenResponse(item, false))
	}
	return out
}

func toAPITokenResponse(item APITokenRecord, includeToken bool) apiTokenResponse {
	token := ""
	if includeToken {
		token = item.Token
	}
	return apiTokenResponse{
		UUID:      item.UUID,
		Name:      item.Name,
		Token:     token,
		ExpireAt:  item.ExpireAt,
		Scopes:    normalizeAPITokenScopes(item.Scopes),
		CreatedAt: item.CreatedAt,
		UpdatedAt: item.UpdatedAt,
	}
}

func toExodusSettingsResponse(settings map[string]any) map[string]any {
	return map[string]any{
		"passkeySettings":  settings["passkey_settings"],
		"oauth2Settings":   normalizeOAuth2Settings(settings["oauth2_settings"]),
		"passwordSettings": settings["password_settings"],
		"brandingSettings": settings["branding_settings"],
	}
}

type ioNopCloser struct {
	*strings.Reader
}

func (ioNopCloser) Close() error { return nil }

type responseRecorder struct {
	header http.Header
	status int
	body   []byte
}

func (r *responseRecorder) Header() http.Header { return r.header }
func (r *responseRecorder) Write(p []byte) (int, error) {
	r.body = append(r.body, p...)
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return len(p), nil
}
func (r *responseRecorder) WriteHeader(statusCode int) { r.status = statusCode }

func loadPanelSettings(ctx context.Context, db *pgxpool.Pool) (map[string]any, error) {
	settings := map[string]any{
		"passkey_settings":  panelsettingsDefaults.DefaultPasskeySettings(),
		"oauth2_settings":   panelsettingsDefaults.DefaultOAuth2Settings(),
		"password_settings": panelsettingsDefaults.DefaultPasswordSettings(),
		"branding_settings": panelsettingsDefaults.DefaultBrandingSettings(),
	}

	row := db.QueryRow(ctx, `
		SELECT passkey_settings::text, oauth2_settings::text, password_settings::text, branding_settings::text
		FROM exodus_settings
		WHERE id = 1
		LIMIT 1
	`)

	var passkeyRaw, oauth2Raw, passwordRaw, brandingRaw *string
	if scanErr := row.Scan(&passkeyRaw, &oauth2Raw, &passwordRaw, &brandingRaw); scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			return settings, nil
		}
		return nil, scanErr
	}

	if passkeyRaw != nil {
		mergeJSONObject(settings, "passkey_settings", *passkeyRaw)
	}
	if oauth2Raw != nil {
		mergeJSONObject(settings, "oauth2_settings", *oauth2Raw)
	}
	if passwordRaw != nil {
		mergeJSONObject(settings, "password_settings", *passwordRaw)
	}
	if brandingRaw != nil {
		mergeJSONObject(settings, "branding_settings", *brandingRaw)
	}
	return settings, nil
}

func loadAPITokens(ctx context.Context, db *pgxpool.Pool) ([]APITokenRecord, error) {
	rows, queryErr := db.Query(ctx, `
		SELECT
			uuid,
			name,
			expire_at,
			COALESCE(scopes, ARRAY['*']::text[]),
			created_at,
			updated_at
		FROM api_tokens
		ORDER BY created_at DESC
	`)
	if queryErr != nil {
		return nil, queryErr
	}
	defer rows.Close()

	tokens := make([]APITokenRecord, 0)
	for rows.Next() {
		var row APITokenRecord
		if scanErr := rows.Scan(&row.UUID, &row.Name, &row.ExpireAt, &row.Scopes, &row.CreatedAt, &row.UpdatedAt); scanErr != nil {
			return nil, scanErr
		}
		tokens = append(tokens, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tokens, nil
}

func mergeJSONObject(target map[string]any, key, raw string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return
	}

	if existing, ok := target[key].(map[string]any); ok {
		for k, v := range decoded {
			existing[k] = v
		}
		target[key] = existing
		return
	}

	target[key] = decoded
}

func defaultOAuth2Settings() map[string]any {
	return map[string]any{
		"github": map[string]any{
			"enabled": false, "clientId": nil, "clientSecret": nil, "allowedEmails": []any{},
		},
		"pocketid": map[string]any{
			"enabled": false, "clientId": nil, "clientSecret": nil, "plainDomain": nil, "frontendDomain": nil, "allowedEmails": []any{},
		},
		"yandex": map[string]any{
			"enabled": false, "clientId": nil, "clientSecret": nil, "allowedEmails": []any{},
		},
		"keycloak": map[string]any{
			"enabled": false, "realm": nil, "clientId": nil, "clientSecret": nil, "frontendDomain": nil, "keycloakDomain": nil, "allowedEmails": []any{},
		},
		"generic": map[string]any{
			"enabled": false, "clientId": nil, "clientSecret": nil, "withPkce": false, "authorizationUrl": nil, "tokenUrl": nil, "frontendDomain": nil, "allowedEmails": []any{},
		},
		"telegram": map[string]any{
			"enabled": false, "clientId": nil, "clientSecret": nil, "allowedIds": []any{}, "frontendDomain": nil,
		},
	}
}

func normalizeOAuth2Settings(value any) map[string]any {
	normalized := defaultOAuth2Settings()
	raw, ok := value.(map[string]any)
	if !ok {
		return normalized
	}

	for provider, defaults := range normalized {
		providerRaw, ok := raw[provider].(map[string]any)
		if !ok {
			continue
		}
		providerDefaults, _ := defaults.(map[string]any)
		for key, val := range providerRaw {
			providerDefaults[key] = val
		}
		normalized[provider] = providerDefaults
	}

	return normalized
}

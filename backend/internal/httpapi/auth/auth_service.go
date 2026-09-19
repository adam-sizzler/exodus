package auth

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"exodus/internal/config"
	"exodus/internal/panelsettings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func getBootstrapData(ctx context.Context, db *pgxpool.Pool) (brandingSettings map[string]any, passwordSettings map[string]any, hasAdmin bool, err error) {
	brandingSettings = panelsettings.DefaultBrandingSettings()
	passwordSettings = panelsettings.DefaultPasswordSettings()
	hasAdmin = false

	row := db.QueryRow(ctx, `
		SELECT branding_settings, password_settings
		FROM exodus_settings
		WHERE id = 1
		LIMIT 1
	`)

	var brandingRaw, passwordRaw *string
	if scanErr := row.Scan(&brandingRaw, &passwordRaw); scanErr != nil && !errors.Is(scanErr, pgx.ErrNoRows) {
		return brandingSettings, passwordSettings, hasAdmin, scanErr
	}

	if brandingRaw != nil && strings.TrimSpace(*brandingRaw) != "" {
		var tmp map[string]any
		if json.Unmarshal([]byte(*brandingRaw), &tmp) == nil && len(tmp) > 0 {
			brandingSettings = tmp
		}
	}
	if passwordRaw != nil && strings.TrimSpace(*passwordRaw) != "" {
		var tmp map[string]any
		if json.Unmarshal([]byte(*passwordRaw), &tmp) == nil && len(tmp) > 0 {
			passwordSettings = tmp
		}
	}

	var adminCount int
	if countErr := db.QueryRow(ctx, "SELECT COUNT(*) FROM admin").Scan(&adminCount); countErr != nil {
		return brandingSettings, passwordSettings, hasAdmin, countErr
	}
	hasAdmin = adminCount > 0

	return brandingSettings, passwordSettings, hasAdmin, nil
}

func getAuthMethodsStatus(ctx context.Context, db *pgxpool.Pool) (passkeyEnabled bool, oauth2Providers map[string]bool) {
	passkeyEnabled = false
	oauth2Providers = map[string]bool{
		"github":   false,
		"yandex":   false,
		"generic":  false,
		"keycloak": false,
		"pocketid": false,
		"telegram": false,
	}

	row := db.QueryRow(ctx, `
		SELECT passkey_settings, oauth2_settings
		FROM exodus_settings
		WHERE id = 1
		LIMIT 1
	`)

	var passkeyRaw, oauth2Raw *string
	if err := row.Scan(&passkeyRaw, &oauth2Raw); err != nil {
		return passkeyEnabled, oauth2Providers
	}

	if passkeyRaw != nil && strings.TrimSpace(*passkeyRaw) != "" {
		var passkeyObj map[string]any
		if json.Unmarshal([]byte(*passkeyRaw), &passkeyObj) == nil {
			if enabled, ok := passkeyObj["enabled"].(bool); ok {
				passkeyEnabled = enabled
			}
		}
	}

	if oauth2Raw != nil && strings.TrimSpace(*oauth2Raw) != "" {
		var oauthObj map[string]map[string]any
		if json.Unmarshal([]byte(*oauth2Raw), &oauthObj) == nil {
			for provider := range oauth2Providers {
				if providerCfg, ok := oauthObj[provider]; ok {
					if enabled, ok := providerCfg["enabled"].(bool); ok {
						oauth2Providers[provider] = enabled
					}
				}
			}
		}
	}

	return passkeyEnabled, oauth2Providers
}

func resolvePasswordAuthEnabled(
	passwordSettings map[string]any,
	passkeyEnabled bool,
	oauth2Providers map[string]bool,
	cfg *config.BackendConfig,
) bool {
	passwordEnabled := true
	if raw, ok := passwordSettings["enabled"]; ok {
		if value, ok := raw.(bool); ok {
			passwordEnabled = value
		}
	}

	if passwordEnabled {
		return true
	}

	hasOAuth2Enabled := false
	for _, enabled := range oauth2Providers {
		if enabled {
			hasOAuth2Enabled = true
			break
		}
	}

	if !passkeyEnabled && !hasOAuth2Enabled {
		if cfg != nil && cfg.Logger != nil {
			cfg.Logger.Warn("All authentication methods are disabled. Falling back to password auth enabled=true to prevent lockout.")
		}
		return true
	}

	return false
}

package seed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"exodus/internal/config"

	"github.com/jackc/pgx/v5"
)

type passkeySettingsStruct struct {
	Enabled bool    `json:"enabled"`
	RpID    *string `json:"rpId"`
	Origin  *string `json:"origin"`
}

type passwordSettingsStruct struct {
	Enabled bool `json:"enabled"`
}

func ensureExodusSettings(ctx context.Context, tx pgx.Tx, _ *config.BackendConfig) error {
	var (
		passkeyRaw  *string
		oauth2Raw   *string
		passwordRaw *string
		brandingRaw *string
	)

	err := tx.QueryRow(ctx, `SELECT passkey_settings::text, oauth2_settings::text, password_settings::text, branding_settings::text FROM exodus_settings WHERE id = 1`).Scan(&passkeyRaw, &oauth2Raw, &passwordRaw, &brandingRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		query := `
			INSERT INTO exodus_settings (
				id, passkey_settings, oauth2_settings, password_settings, branding_settings
			) VALUES (1, $1, $2, $3, $4)
		`
		if _, err := tx.Exec(ctx, query, defaultPasskeySettings, defaultOAuth2Settings, defaultPasswordSettings, defaultBrandingSettings); err != nil {
			return fmt.Errorf("insert default exodus_settings row: %w", err)
		}
		fmt.Println("✔ Exodus settings seeded")
		return nil
	} else if err != nil {
		return fmt.Errorf("check exodus_settings row: %w", err)
	}

	var resetFields []string
	updates := make(map[string]string)

	// 1. Validate passkey_settings
	passkeyValid := false
	if passkeyRaw != nil && strings.TrimSpace(*passkeyRaw) != "" && *passkeyRaw != "null" {
		var pk passkeySettingsStruct
		if err := json.Unmarshal([]byte(*passkeyRaw), &pk); err == nil {
			passkeyValid = true
		}
	}
	if !passkeyValid {
		updates["passkey_settings"] = defaultPasskeySettings
		resetFields = append(resetFields, "passkey_settings")
		updates["password_settings"] = defaultPasswordSettings
		resetFields = append(resetFields, "password_settings (forced enabled)")
	}

	// 2. Validate oauth2_settings
	oauth2Valid := false
	if oauth2Raw != nil && strings.TrimSpace(*oauth2Raw) != "" && *oauth2Raw != "null" {
		var m map[string]json.RawMessage
		if err := json.Unmarshal([]byte(*oauth2Raw), &m); err == nil {
			oauth2Valid = true
		}
	}
	if !oauth2Valid {
		updates["oauth2_settings"] = defaultOAuth2Settings
		resetFields = append(resetFields, "oauth2_settings")
	}

	// 3. Validate password_settings
	if _, alreadyReset := updates["password_settings"]; !alreadyReset {
		passwordValid := false
		if passwordRaw != nil && strings.TrimSpace(*passwordRaw) != "" && *passwordRaw != "null" {
			var pw passwordSettingsStruct
			if err := json.Unmarshal([]byte(*passwordRaw), &pw); err == nil {
				passwordValid = true
			}
		}
		if !passwordValid {
			updates["password_settings"] = defaultPasswordSettings
			resetFields = append(resetFields, "password_settings")
		}
	}

	// 4. Validate branding_settings
	brandingValid := false
	if brandingRaw != nil && strings.TrimSpace(*brandingRaw) != "" && *brandingRaw != "null" {
		var m map[string]json.RawMessage
		if err := json.Unmarshal([]byte(*brandingRaw), &m); err == nil {
			brandingValid = true
		}
	}
	if !brandingValid {
		updates["branding_settings"] = defaultBrandingSettings
		resetFields = append(resetFields, "branding_settings")
	}

	if len(updates) > 0 {
		setParts := make([]string, 0, len(updates))
		args := make([]any, 0, len(updates))
		idx := 1
		for col, val := range updates {
			setParts = append(setParts, fmt.Sprintf("%s = $%d::jsonb", col, idx))
			args = append(args, val)
			idx++
		}
		query := fmt.Sprintf("UPDATE exodus_settings SET %s WHERE id = 1", strings.Join(setParts, ", "))
		if _, err := tx.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("reset invalid exodus_settings fields: %w", err)
		}
		fmt.Println("✔ ExodusSettings reset invalid fields: " + strings.Join(resetFields, ", "))
	} else {
		fmt.Println("✔ Exodus settings seeded")
	}

	return nil
}

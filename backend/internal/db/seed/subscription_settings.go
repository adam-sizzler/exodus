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

func ensureDefaultSubscriptionSettings(ctx context.Context, tx pgx.Tx, _ *config.BackendConfig) error {
	fmt.Println("◐ Seeding subscription settings...")

	var (
		subUUID          string
		hwidRaw          *string
		customRemarksRaw *string
		responseRulesRaw *string
	)

	err := tx.QueryRow(ctx, `SELECT uuid, hwid_settings, custom_remarks, response_rules FROM subscription_settings LIMIT 1`).Scan(&subUUID, &hwidRaw, &customRemarksRaw, &responseRulesRaw)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("read subscription_settings: %w", err)
	}

	if err == nil {
		var (
			updates     []string
			args        []any
			resetFields []string
			argIdx      = 1
		)

		needHwidUpdate := hwidRaw == nil || strings.TrimSpace(*hwidRaw) == "" || *hwidRaw == "null"
		if needHwidUpdate {
			updates = append(updates, fmt.Sprintf("hwid_settings = $%d::jsonb", argIdx))
			args = append(args, defaultHWIDSettings)
			argIdx++
			resetFields = append(resetFields, "hwid_settings")
		}

		needRemarksUpdate := false
		var currentRemarks map[string]any
		if customRemarksRaw != nil && strings.TrimSpace(*customRemarksRaw) != "" && *customRemarksRaw != "null" {
			if err := json.Unmarshal([]byte(*customRemarksRaw), &currentRemarks); err != nil {
				needRemarksUpdate = true
			} else {
				if _, ok1 := currentRemarks["HWIDMaxDevicesExceeded"]; !ok1 {
					needRemarksUpdate = true
				}
				if _, ok2 := currentRemarks["HWIDNotSupported"]; !ok2 {
					needRemarksUpdate = true
				}
			}
		} else {
			needRemarksUpdate = true
		}

		if needRemarksUpdate {
			var defaultRemarks map[string]any
			_ = json.Unmarshal([]byte(defaultCustomRemarks), &defaultRemarks)
			if currentRemarks == nil {
				currentRemarks = defaultRemarks
			} else {
				for k, v := range defaultRemarks {
					if _, exists := currentRemarks[k]; !exists {
						currentRemarks[k] = v
					}
				}
			}
			mergedBytes, _ := json.Marshal(currentRemarks)
			updates = append(updates, fmt.Sprintf("custom_remarks = $%d::jsonb", argIdx))
			args = append(args, string(mergedBytes))
			argIdx++
			resetFields = append(resetFields, "custom_remarks")
		}

		var currentRules string
		if responseRulesRaw != nil && strings.TrimSpace(*responseRulesRaw) != "" && *responseRulesRaw != "null" {
			currentRules = *responseRulesRaw
		}

		if currentRules == "" {
			updates = append(updates, fmt.Sprintf("response_rules = $%d::jsonb", argIdx))
			args = append(args, defaultResponseRules)
			argIdx++
			resetFields = append(resetFields, "response_rules")
		} else {
			existingHash, _ := canonicalHash(currentRules)
			defaultHash, _ := canonicalHash(defaultResponseRules)
			if existingHash == PrevResponseRulesHash && defaultHash != PrevResponseRulesHash {
				updates = append(updates, fmt.Sprintf("response_rules = $%d::jsonb", argIdx))
				args = append(args, defaultResponseRules)
				argIdx++
				resetFields = append(resetFields, "response_rules (upgraded)")
			}
		}

		if len(updates) > 0 {
			args = append(args, subUUID)
			query := fmt.Sprintf("UPDATE subscription_settings SET %s WHERE uuid = $%d", strings.Join(updates, ", "), argIdx)
			if _, err := tx.Exec(ctx, query, args...); err != nil {
				return fmt.Errorf("update subscription_settings backfill: %w", err)
			}
			fmt.Println("✔ SubscriptionSettings field backfill applied: " + strings.Join(resetFields, ", "))
		} else {
			fmt.Println("✔ Custom remarks seeded")
		}
		return nil
	}

	query := `
		INSERT INTO subscription_settings (
			uuid, address, port, api_schema, api_path,
			serve_json_at_base_subscription, is_show_custom_remarks, custom_remarks,
			custom_response_headers, randomize_hosts, response_rules, hwid_settings
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`
	_, err = tx.Exec(ctx, query,
		"00000000-0000-0000-0000-000000000000",
		"",
		9263,
		"grpc",
		"",
		false,
		true,
		defaultCustomRemarks,
		`{"profile-title":"exEncodeBase64:exodus","support-url":"https://github.com","profile-update-interval":"12","profile-web-page-url":"{{SUBSCRIPTION_URL}}"}`,
		false,
		defaultResponseRules,
		defaultHWIDSettings,
	)
	if err != nil {
		return fmt.Errorf("insert default subscription_settings: %w", err)
	}

	fmt.Println("✔ Default subscription settings seeded")
	return nil
}

func logResponseRulesHashes(ctx context.Context, tx pgx.Tx, _ *config.BackendConfig) {
	var currentRules *string
	_ = tx.QueryRow(ctx, `SELECT response_rules FROM subscription_settings LIMIT 1`).Scan(&currentRules)

	rulesStr := ""
	if currentRules != nil {
		rulesStr = *currentRules
	}
	existingHash, _ := canonicalHash(rulesStr)
	defaultHash, _ := canonicalHash(defaultResponseRules)

	fmt.Printf("ℹ Existing SRR hash: %s\n", existingHash)
	fmt.Printf("ℹ Default SRR hash: %s\n", defaultHash)
	fmt.Printf("ℹ Previous SRR hash: %s\n", PrevResponseRulesHash)
}

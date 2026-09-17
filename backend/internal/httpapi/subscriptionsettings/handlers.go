package subscriptionsettings

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"

	"github.com/google/uuid"
)

// SubscriptionSettingsHandler godoc
// @Summary      Manage subscription settings
// @Description  Get or update global subscription page and protocol settings
// @Tags         Subscription Settings Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Subscription settings fields to patch"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /subscription-settings [get]
// @Router       /subscription-settings [patch]
func SubscriptionSettingsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetSubscriptionSettingsEXODUS(w, r, db, cfg)
		case http.MethodPatch:
			handlePatchSubscriptionSettingsEXODUS(w, r, db, cfg)
		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	}
}

func handleGetSubscriptionSettingsEXODUS(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	row := db.QueryRow(r.Context(), `
		SELECT
			uuid, address, port, api_schema, api_path,
			serve_json_at_base_subscription, is_show_custom_remarks, custom_remarks,
			custom_response_headers, randomize_hosts, response_rules, hwid_settings,
			created_at, updated_at
		FROM subscription_settings
		ORDER BY created_at DESC
		LIMIT 1`)
	settings, err := ScanSubscriptionSettings(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			_, _ = db.Exec(r.Context(), `
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
			rowRetry := db.QueryRow(r.Context(), `
				SELECT
					uuid, address, port, api_schema, api_path,
					serve_json_at_base_subscription, is_show_custom_remarks, custom_remarks,
					custom_response_headers, randomize_hosts, response_rules, hwid_settings,
					created_at, updated_at
				FROM subscription_settings
				ORDER BY created_at DESC
				LIMIT 1`)
			if sRetry, errRetry := ScanSubscriptionSettings(rowRetry); errRetry == nil {
				settings = sRetry
				err = nil
			}
		}
		if err != nil {
			shared.SendAPIError(w, shared.ErrSubscriptionSettingsNotFound, cfg)
			return
		}
	}

	apiSettings, err := convertSubscriptionSettingsToAPI(settings)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetSubscriptionSettingsFailed.WithCause(err), cfg)
		return
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{"response": apiSettings})
}

func handlePatchSubscriptionSettingsEXODUS(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req SubscriptionSettingsUpdateRequestAPI
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	if _, err := uuid.Parse(req.UUID); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
		return
	}

	if err := validateSubscriptionSettingsUpdate(req); err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
		return
	}

	// Fetch current headers so we can update header keys inside custom_response_headers
	var currentHeadersRaw *string
	_ = db.QueryRow(r.Context(), `SELECT custom_response_headers::text FROM subscription_settings WHERE uuid = $1`, req.UUID).Scan(&currentHeadersRaw)
	headersMap := make(map[string]string)
	if currentHeadersRaw != nil && *currentHeadersRaw != "" {
		_ = json.Unmarshal([]byte(*currentHeadersRaw), &headersMap)
	}

	headersModified := false

	if set, val, err := parseOptionalHeaders(req.CustomResponseHeaders); err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
		return
	} else if set {
		userHeaders := make(map[string]string)
		if val != "" && val != "{}" && val != "null" {
			var rawMap map[string]string
			if err := json.Unmarshal([]byte(val), &rawMap); err == nil {
				for k, v := range rawMap {
					trimmedKey := strings.ToLower(strings.TrimSpace(k))
					if trimmedKey != "" {
						userHeaders[trimmedKey] = v
					}
				}
			}
		}
		headersMap = userHeaders
		headersModified = true
	}

	updates := []string{}
	args := []any{}
	idx := 1
	add := func(column string, value any) {
		updates = append(updates, fmt.Sprintf("%s = $%d", column, idx))
		args = append(args, value)
		idx++
	}

	if headersModified {
		hBytes, _ := json.Marshal(headersMap)
		add("custom_response_headers", string(hBytes))
	}
	if req.ServeJsonAtBaseSubscription != nil {
		add("serve_json_at_base_subscription", *req.ServeJsonAtBaseSubscription)
	}
	if req.IsShowCustomRemarks != nil {
		add("is_show_custom_remarks", *req.IsShowCustomRemarks)
	}
	if req.RandomizeHosts != nil {
		add("randomize_hosts", *req.RandomizeHosts)
	}

	if set, val, err := parseOptionalJSONMap(req.CustomRemarks, true); err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
		return
	} else if set {
		add("custom_remarks", val)
	}

	if set, val, err := parseOptionalJSONMap(req.ResponseRules, true); err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
		return
	} else if set {
		add("response_rules", val)
	}

	if set, val, err := parseOptionalJSONMap(req.HwidSettings, true); err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
		return
	} else if set {
		add("hwid_settings", val)
	}

	if len(updates) == 0 {
		handleGetSubscriptionSettingsEXODUS(w, r, db, cfg)
		return
	}

	updates = append(updates, "updated_at = CURRENT_TIMESTAMP")
	args = append(args, req.UUID)

	query := fmt.Sprintf(`
		UPDATE subscription_settings
		SET %s
		WHERE uuid = $%d
		RETURNING uuid, address, port, api_schema, api_path,
			serve_json_at_base_subscription, is_show_custom_remarks, custom_remarks,
			custom_response_headers, randomize_hosts, response_rules, hwid_settings,
			created_at, updated_at
	`, strings.Join(updates, ", "), idx)
	row := db.QueryRow(r.Context(), query, args...)
	settings, err := ScanSubscriptionSettings(row)
	if err != nil {
		shared.SendAPIError(w, shared.ErrUpdateSubscriptionSettingsFailed.WithCause(err), cfg)
		return
	}

	if OnSettingsUpdated != nil {
		OnSettingsUpdated()
	}

	apiSettings, err := convertSubscriptionSettingsToAPI(settings)
	if err != nil {
		shared.SendAPIError(w, shared.ErrUpdateSubscriptionSettingsFailed.WithCause(err), cfg)
		return
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{"response": apiSettings})
}

// OnSettingsUpdated is invoked whenever subscription settings are modified.
var OnSettingsUpdated func()

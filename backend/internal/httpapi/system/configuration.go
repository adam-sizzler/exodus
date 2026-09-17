package system

import (
	"net/http"
	"os"
	"strconv"
	"strings"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"

	"github.com/jackc/pgx/v5/pgxpool"
)

type SystemConfigurationResponse struct {
	Notifications SystemConfigurationNotifications `json:"notifications"`
	Service       SystemConfigurationService       `json:"service"`
	Misc          SystemConfigurationMisc          `json:"misc"`
}

type SystemConfigurationNotifications struct {
	Webhook                 bool `json:"webhook"`
	BandwidthUsage          bool `json:"bandwidthUsage"`
	NotConnectedAfter       bool `json:"notConnectedAfter"`
	ExpirationNotifications bool `json:"expirationNotifications"`
}

type SystemConfigurationService struct {
	CleanUsageHistory       bool `json:"cleanUsageHistory"`
	DisableUserUsageRecords bool `json:"disableUserUsageRecords"`
	DisableSrhRecords       bool `json:"disableSrhRecords"`
	ExportToRedisStream     bool `json:"exportToRedisStream"`
}

type SystemConfigurationMisc struct {
	ShortUUIDLength           int    `json:"shortUuidLength"`
	UserUsageIgnoreBelowBytes int64  `json:"userUsageIgnoreBelowBytes"`
	SubPublicDomain           string `json:"subPublicDomain"`
}

// ConfigurationHandler godoc
// @Summary      Get system configuration
// @Description  Get public runtime configuration parameters
// @Tags         System Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]SystemConfigurationResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /system/configuration [get]
func ConfigurationHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		userUsageIgnore, _ := strconv.ParseInt(os.Getenv("USER_USAGE_IGNORE_BELOW_BYTES"), 10, 64)
		subDomain := os.Getenv("SUB_PUBLIC_DOMAIN")
		if subDomain == "" && db != nil {
			var domain *string
			_ = db.QueryRow(r.Context(), `
				SELECT COALESCE(NULLIF(BTRIM(public_domain), ''), NULLIF(BTRIM(address), ''))
				FROM sub_nodes
				WHERE is_disabled = false
				ORDER BY view_position ASC, created_at ASC
				LIMIT 1
			`).Scan(&domain)
			if domain != nil && *domain != "" {
				subDomain = strings.TrimSpace(strings.Split(*domain, ",")[0])
			}
		}

		disableUserUsage := os.Getenv("SERVICE_DISABLE_USER_USAGE_RECORDS") == "true"
		disableSrh := os.Getenv("SERVICE_DISABLE_SRH_RECORDS") == "true"

		resp := SystemConfigurationResponse{
			Notifications: SystemConfigurationNotifications{
				Webhook:                 cfg.Notifications.WebhookEnabled,
				BandwidthUsage:          cfg.Scheduler.BandwidthUsageNotificationsEnabled && len(cfg.Scheduler.BandwidthUsageNotificationsThreshold) > 0,
				NotConnectedAfter:       cfg.Scheduler.NotConnectedUsersNotificationsEnabled && len(cfg.Scheduler.NotConnectedUsersNotificationsAfterHours) > 0,
				ExpirationNotifications: cfg.Scheduler.ExpirationNotificationsEnabled && len(cfg.Scheduler.ExpirationNotifications) > 0,
			},
			Service: SystemConfigurationService{
				CleanUsageHistory:       cfg.Scheduler.ServiceCleanUsageHistory,
				DisableUserUsageRecords: disableUserUsage,
				DisableSrhRecords:       disableSrh,
				ExportToRedisStream:     cfg.Redis.ExportToStreamEnabled,
			},
			Misc: SystemConfigurationMisc{
				ShortUUIDLength:           cfg.Backend.ShortUUIDLength,
				UserUsageIgnoreBelowBytes: userUsageIgnore,
				SubPublicDomain:           subDomain,
			},
		}

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": resp,
		})
	}
}

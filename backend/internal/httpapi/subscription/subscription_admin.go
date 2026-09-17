package subscription

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
)

// SubscriptionsHandler godoc
// @Summary      Get all subscriptions
// @Description  Get paginated subscription info for users
// @Tags         [Protected] Subscriptions Controller
// @Produce      json
// @Security     BearerAuth
// @Param        start  query     int  false  "Pagination start index"
// @Param        size   query     int  false  "Page size (1-500, default 25)"
// @Success      200    {object}  map[string]any
// @Failure      500    {object}  shared.ErrorResponse
// @Router       /subscriptions [get]
func SubscriptionsHandler(db, backgroundDB *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	renderService := NewRenderService(db, backgroundDB, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		start, _ := strconv.Atoi(r.URL.Query().Get("start"))
		size, _ := strconv.Atoi(r.URL.Query().Get("size"))
		if start < 0 {
			start = 0
		}
		if size <= 0 {
			size = 25
		}
		if size > 500 {
			size = 500
		}

		ctx := r.Context()

		settings, err := loadSubscriptionSettings(ctx, db, cfg)
		if err != nil {
			shared.SendAPIError(w, shared.ErrSubscriptionSettingsNotFound, cfg)
			return
		}

		users, total, err := getUsersWithPagination(ctx, db, start, size)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetAllUsersFailed.WithCause(err), cfg)
			return
		}

		subscriptions := []SubscriptionInfoResponse{}
		for _, user := range users {
			hosts, err := getHostsForUser(ctx, db, user)
			if err != nil {
				continue
			}
			subscriptions = append(subscriptions, renderService.buildSubscriptionInfoResponse(user, settings, hosts))
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"response": map[string]interface{}{
				"subscriptions": subscriptions,
				"total":         total,
				"start":         start,
				"size":          size,
			},
		})
	}
}

// SubscriptionByUUIDHandler godoc
// @Summary      Get user subscription by identifier
// @Description  Get subscription details or connection keys by ID, short UUID, or username
// @Tags         [Protected] Subscriptions Controller
// @Produce      json
// @Security     BearerAuth
// @Param        userId     path      int     false  "Numeric User ID (for /subscriptions/by-id/{userId} and /subscriptions/connection-keys/{userId})"
// @Param        shortUuid  path      string  false  "Short UUID (for /subscriptions/by-short-uuid/{shortUuid})"
// @Param        username   path      string  false  "Username (for /subscriptions/by-username/{username})"
// @Success      200        {object}  map[string]any
// @Failure      400        {object}  shared.ErrorResponse
// @Failure      404        {object}  shared.ErrorResponse
// @Failure      500        {object}  shared.ErrorResponse
// @Router       /subscriptions/by-id/{userId} [get]
// @Router       /subscriptions/by-short-uuid/{shortUuid} [get]
// @Router       /subscriptions/by-username/{username} [get]
// @Router       /subscriptions/connection-keys/{userId} [get]
func SubscriptionByUUIDHandler(db, backgroundDB *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	renderService := NewRenderService(db, backgroundDB, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/subscriptions/"), "/")
		if path == "" {
			http.NotFound(w, r)
			return
		}

		if strings.HasPrefix(path, "connection-keys/") {
			identifier := strings.TrimPrefix(path, "connection-keys/")
			identifier = strings.TrimSuffix(strings.TrimSpace(identifier), "/raw")
			userID, parseErr := strconv.ParseInt(identifier, 10, 64)
			if parseErr != nil {
				shared.SendError(w, http.StatusBadRequest, "userId must be numeric", parseErr, cfg)
				return
			}
			handleGetConnectionKeysByUserID(w, r, db, cfg, userID)
			return
		}

		isRaw := strings.HasSuffix(strings.TrimSpace(path), "/raw")
		path = strings.TrimSuffix(strings.TrimSpace(path), "/raw")
		ctx := r.Context()

		var user SubscriptionUser
		var err error
		switch {
		case strings.HasPrefix(path, "by-id/"):
			identifier := strings.TrimPrefix(path, "by-id/")
			var userID int64
			userID, err = strconv.ParseInt(identifier, 10, 64)
			if err != nil {
				shared.SendError(w, http.StatusBadRequest, "userId must be numeric", err, cfg)
				return
			}
			user, err = getSubscriptionUserByID(ctx, db, userID)
		case strings.HasPrefix(path, "by-short-uuid/"):
			identifier := strings.TrimPrefix(path, "by-short-uuid/")
			user, err = getSubscriptionUserByShortUUID(ctx, db, identifier)
		case strings.HasPrefix(path, "by-username/"):
			identifier := strings.TrimPrefix(path, "by-username/")
			user, err = getSubscriptionUserByUsername(ctx, db, identifier)
		default:
			user, err = getSubscriptionUserByUUID(ctx, db, path)
		}
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
			shared.SendAPIError(w, shared.ErrSubscriptionSettingsNotFound, cfg)
			return
		}

		if isRaw {
			withDisabledHosts := r.URL.Query().Get("withDisabledHosts") == "true"
			hosts, err := getHostsForUserWithOptions(ctx, db, user, withDisabledHosts, true)
			if err != nil {
				shared.SendAPIError(w, shared.ErrGetAllHostsFailed.WithCause(err), cfg)
				return
			}

			domain := strings.TrimSpace(settings.Raw.Address)
			if domain == "" {
				domain = r.Host
			}
			scheme := strings.TrimSpace(settings.Raw.APISchema)
			if scheme == "" {
				scheme = "https"
			}
			apiPath := strings.Trim(strings.TrimSpace(settings.Raw.APIPath), "/")
			if apiPath == "" {
				apiPath = "api/sub"
			}
			subURL := fmt.Sprintf("%s://%s/%s/%s", scheme, domain, apiPath, user.ShortUUID)

			rawResponse := buildRawSubscriptionResponse(ctx, db, user, settings, hosts, subURL)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"response": rawResponse,
			})
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
}

func handleGetConnectionKeysByUserID(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, userID int64) {
	ctx := r.Context()
	user, err := getSubscriptionUserByID(ctx, db, userID)
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
		shared.SendAPIError(w, shared.ErrSubscriptionSettingsNotFound, cfg)
		return
	}

	squadOverrides, _ := loadExternalSquadOverrides(ctx, db, ptrString(user.ExternalSquadUUID), cfg)
	settings = applyExternalSquadOverrides(settings, squadOverrides)

	hosts, err := getHostsForUserWithOptions(ctx, db, user, true, true)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetAllHostsFailed.WithCause(err), cfg)
		return
	}

	if len(settings.HostOverrides) > 0 {
		hosts = applyHostOverrides(hosts, settings.HostOverrides)
	}

	scheme := strings.TrimSpace(settings.Raw.APISchema)
	if scheme == "" {
		scheme = "https"
	}
	domain := strings.TrimSpace(settings.Raw.Address)
	if domain == "" {
		domain = "panel.exodus.dev"
	}
	apiPath := strings.Trim(strings.TrimSpace(settings.Raw.APIPath), "/")
	if apiPath == "" {
		apiPath = "api/sub"
	}
	subURL := fmt.Sprintf("%s://%s/%s/%s", scheme, domain, apiPath, user.ShortUUID)

	resolveHostRemarks(hosts, user, settings, subURL)

	var enabledHosts, disabledHosts, hiddenHosts []SubscriptionHost
	for _, h := range hosts {
		if h.IsDisabled && !h.IsHidden {
			disabledHosts = append(disabledHosts, h)
		} else if h.IsHidden && !h.IsDisabled {
			hiddenHosts = append(hiddenHosts, h)
		} else if !h.IsDisabled && !h.IsHidden {
			enabledHosts = append(enabledHosts, h)
		}
	}

	enabledKeys, _ := buildSubscriptionLinks(enabledHosts, user)
	disabledKeys, _ := buildSubscriptionLinks(disabledHosts, user)
	hiddenKeys, _ := buildSubscriptionLinks(hiddenHosts, user)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"response": map[string]interface{}{
			"enabledKeys":  enabledKeys,
			"hiddenKeys":   hiddenKeys,
			"disabledKeys": disabledKeys,
		},
	})
}

package users

import (
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
)

func UsersHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewUserRepository(db)
	service := NewUserService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetUsers(w, r, service)
		case http.MethodPost:
			handleCreateUser(w, r, service)
		case http.MethodPatch:
			handleUpdateUser(w, r, service)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func UserByUUIDHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewUserRepository(db)
	service := NewUserService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		path := trimUsersPath(r.URL.Path, "/")
		if path == "" {
			switch r.Method {
			case http.MethodGet:
				handleGetUsers(w, r, service)
			case http.MethodPost:
				handleCreateUser(w, r, service)
			case http.MethodPatch:
				handleUpdateUser(w, r, service)
			default:
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			}
			return
		}

		parts := strings.Split(path, "/")
		if len(parts) == 1 && parts[0] == "stream" {
			if r.Method != http.MethodGet {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			handleGetUsersStream(w, r, service)
			return
		}

		if len(parts) == 1 && parts[0] == "resolve" {
			if r.Method != http.MethodPost {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			handleResolveUser(w, r, service)
			return
		}

		if len(parts) == 2 && parts[0] == "by-short-uuid" {
			if r.Method != http.MethodGet {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			handleGetUser(w, r, service, parts[1])
			return
		}

		if len(parts) == 2 && parts[0] == "by-username" {
			if r.Method != http.MethodGet {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			handleGetUser(w, r, service, parts[1])
			return
		}

		userUUID := parts[0]

		if len(parts) >= 3 && parts[1] == "actions" && r.Method == http.MethodPost {
			switch parts[2] {
			case "enable":
				handleEnableUser(w, r, service, userUUID)
			case "disable":
				handleDisableUser(w, r, service, userUUID)
			case "reset-traffic":
				handleResetUserTraffic(w, r, service, userUUID)
			case "revoke":
				handleRevokeUserSubscription(w, r, service, userUUID)
			case "extend":
				handleExtendUser(w, r, service, userUUID)
			default:
				http.NotFound(w, r)
			}
			return
		}

		if len(parts) == 2 && parts[1] == "subscription-request-history" {
			if r.Method != http.MethodGet {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			handleGetUserSubscriptionRequestHistory(w, r, service, userUUID)
			return
		}

		if len(parts) == 2 && parts[1] == "accessible-nodes" {
			if r.Method != http.MethodGet {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			handleGetUserAccessibleNodes(w, r, service, userUUID)
			return
		}

		if len(parts) != 1 {
			http.NotFound(w, r)
			return
		}

		switch r.Method {
		case http.MethodGet:
			handleGetUser(w, r, service, userUUID)
		case http.MethodDelete:
			handleDeleteUser(w, r, service, userUUID)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func UsersBulkHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewUserRepository(db)
	service := NewUserService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		path := trimUsersPath(r.URL.Path, "/bulk/")
		switch path {
		case "delete":
			handleBulkDeleteUsers(w, r, service)
		case "delete-by-status":
			handleBulkDeleteUsersByStatus(w, r, service)
		case "reset-traffic":
			handleBulkResetUsersTraffic(w, r, service)
		case "revoke-subscription":
			handleBulkRevokeUsersSubscription(w, r, service)
		case "update":
			handleBulkUpdateUsers(w, r, service)
		case "update-squads":
			handleBulkUpdateUsersSquads(w, r, service)
		case "extend-expiration-date":
			handleBulkExtendUsersExpirationDate(w, r, service)
		case "all/reset-traffic":
			handleBulkAllResetUsersTraffic(w, r, service)
		case "all/extend-expiration-date":
			handleBulkAllExtendUsersExpirationDate(w, r, service)
		case "all/update":
			handleBulkAllUpdateUsers(w, r, service)
		default:
			http.NotFound(w, r)
		}
	}
}

// UsersTagsHandler godoc
// @Summary      Get users tags
// @Description  Get all unique user tags
// @Tags         Users Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  UserTagsResponseEnvelope
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /users/tags [get]
func UsersTagsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewUserRepository(db)
	service := NewUserService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		tags, err := service.repo.getAllUserTags(r.Context())
		if err != nil {
			shared.SendAPIError(w, shared.ErrFetchUserTagsFailed.WithCause(err), service.cfg)
			return
		}

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{
				"tags": tags,
			},
		})
	}
}

func trimUsersPath(path string, suffix string) string {
	for _, prefix := range []string{"/api/users"} {
		if strings.HasPrefix(path, prefix+suffix) {
			return strings.Trim(strings.TrimPrefix(path, prefix+suffix), "/")
		}
	}
	return strings.Trim(path, "/")
}

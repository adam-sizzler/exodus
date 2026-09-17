package configprofiles

import (
	"net/http"
	"strings"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ConfigProfilesHandler godoc
// @Summary      Manage configuration profiles
// @Description  List, create (201), or update sing-box/xray configuration profiles
// @Tags         Config Profiles Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Configuration profile payload"
// @Success      200   {object}  map[string]any
// @Success      201   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /config-profiles [get]
// @Router       /config-profiles [post]
// @Router       /config-profiles [patch]
func ConfigProfilesHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewConfigProfileRepository(db)
	service := NewConfigProfileService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetConfigProfiles(w, r, service)
		case http.MethodPost:
			handleCreateConfigProfile(w, r, service)
		case http.MethodPatch:
			handleUpdateConfigProfile(w, r, service)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// ConfigProfileByUUIDHandler godoc
// @Summary      Config profile by UUID
// @Description  Get, delete (204) config profile, or get profile inbounds / computed config
// @Tags         Config Profiles Controller
// @Produce      json
// @Security     BearerAuth
// @Param        uuid  path      string  true  "Configuration profile UUID" format(uuid)
// @Success      200   {object}  map[string]any
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      404   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /config-profiles/{uuid} [get]
// @Router       /config-profiles/{uuid} [delete]
// @Router       /config-profiles/{uuid}/inbounds [get]
// @Router       /config-profiles/{uuid}/computed-config [get]
func ConfigProfileByUUIDHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewConfigProfileRepository(db)
	service := NewConfigProfileService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		path := trimConfigProfilesPath(r.URL.Path, "/")
		if path == "" {
			switch r.Method {
			case http.MethodGet:
				handleGetConfigProfiles(w, r, service)
			case http.MethodPost:
				handleCreateConfigProfile(w, r, service)
			case http.MethodPatch:
				handleUpdateConfigProfile(w, r, service)
			default:
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			}
			return
		}
		parts := strings.Split(path, "/")
		profileUUID := parts[0]
		if _, err := uuid.Parse(profileUUID); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
			return
		}

		if len(parts) == 2 && r.Method == http.MethodGet {
			switch parts[1] {
			case "inbounds":
				handleGetConfigProfileInbounds(w, r, service, profileUUID)
			case "computed-config":
				handleGetComputedConfigProfile(w, r, service, profileUUID)
			default:
				http.NotFound(w, r)
			}
			return
		}

		if len(parts) != 1 {
			http.NotFound(w, r)
			return
		}

		switch r.Method {
		case http.MethodGet:
			handleGetConfigProfile(w, r, service, profileUUID)
		case http.MethodDelete:
			handleDeleteConfigProfile(w, r, service, profileUUID)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// ConfigProfilesActionsHandler godoc
// @Summary      Config profile actions
// @Description  Reorder configuration profiles
// @Tags         Config Profiles Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Reorder payload"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /config-profiles/actions/reorder [post]
func ConfigProfilesActionsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewConfigProfileRepository(db)
	service := NewConfigProfileService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		path := trimConfigProfilesPath(r.URL.Path, "/actions/")
		switch path {
		case "reorder":
			handleReorderConfigProfiles(w, r, service)
		default:
			http.NotFound(w, r)
		}
	}
}

// ConfigProfilesTagsHandler godoc
// @Summary      Manage config profile tags
// @Description  Get unique config profile tags or set tags for a config profile
// @Tags         Config Profiles Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /config-profiles/tags [get]
// @Router       /config-profiles/tags [patch]
func ConfigProfilesTagsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewConfigProfileRepository(db)
	service := NewConfigProfileService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetConfigProfileTags(w, r, service)
		case http.MethodPatch:
			handleSetConfigProfileTags(w, r, service)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// ConfigProfilesInboundsHandler godoc
// @Summary      List all config profile inbounds
// @Description  Get all inbounds across configuration profiles
// @Tags         Config Profiles Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /config-profiles/inbounds [get]
func ConfigProfilesInboundsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewConfigProfileRepository(db)
	service := NewConfigProfileService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		handleGetAllInbounds(w, r, service)
	}
}

// ConfigProfileSnippetsHandler godoc
// @Summary      Manage config profile snippets
// @Description  List, create (201), update, or delete configuration snippets
// @Tags         Snippets Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Snippet payload"
// @Success      200   {object}  map[string]any
// @Success      201   {object}  map[string]any
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /config-profiles/snippets [get]
// @Router       /config-profiles/snippets [post]
// @Router       /config-profiles/snippets [patch]
// @Router       /snippets [get]
// @Router       /snippets [post]
// @Router       /snippets [patch]
// @Router       /snippets [delete]
func ConfigProfileSnippetsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewConfigProfileRepository(db)
	service := NewConfigProfileService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetConfigProfileSnippets(w, r, service)
		case http.MethodPost:
			handleCreateConfigProfileSnippet(w, r, service)
		case http.MethodPatch:
			handleUpdateConfigProfileSnippet(w, r, service)
		case http.MethodDelete:
			handleDeleteConfigProfileSnippet(w, r, service)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func trimConfigProfilesPath(path string, suffix string) string {
	for _, prefix := range []string{"/api/config-profiles"} {
		if strings.HasPrefix(path, prefix+suffix) {
			return strings.Trim(strings.TrimPrefix(path, prefix+suffix), "/")
		}
	}
	return strings.Trim(path, "/")
}

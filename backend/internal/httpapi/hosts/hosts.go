package hosts

import (
	"net/http"
	"strings"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// HostsHandler godoc
// @Summary      Manage hosts
// @Description  List, create (201), or update hosts
// @Tags         Hosts Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Host create/update fields"
// @Success      200   {object}  map[string]any
// @Success      201   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /hosts [get]
// @Router       /hosts [post]
// @Router       /hosts [patch]
func HostsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewHostRepository(db)
	service := NewHostService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetHosts(w, r, service)
		case http.MethodPost:
			handleCreateHost(w, r, service)
		case http.MethodPatch:
			handleUpdateHost(w, r, service)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// HostByUUIDHandler godoc
// @Summary      Host by UUID
// @Description  Get or delete host by UUID
// @Tags         Hosts Controller
// @Produce      json
// @Security     BearerAuth
// @Param        uuid  path      string  true  "Host UUID" format(uuid)
// @Success      200   {object}  map[string]any
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      404   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /hosts/{uuid} [get]
// @Router       /hosts/{uuid} [delete]
func HostByUUIDHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewHostRepository(db)
	service := NewHostService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		uuidStr := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/api/hosts/"))
		if uuidStr == "" {
			switch r.Method {
			case http.MethodGet:
				handleGetHosts(w, r, service)
			case http.MethodPost:
				handleCreateHost(w, r, service)
			case http.MethodPatch:
				handleUpdateHost(w, r, service)
			default:
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			}
			return
		}
		if uuidStr == "clone" && r.Method == http.MethodPost {
			handleCloneHost(w, r, service)
			return
		}
		if _, err := uuid.Parse(uuidStr); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
			return
		}

		switch r.Method {
		case http.MethodGet:
			handleGetHost(w, r, service, uuidStr)
		case http.MethodDelete:
			handleDeleteHost(w, r, service, uuidStr)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// HostsActionsHandler godoc
// @Summary      Host actions
// @Description  Reorder or clone hosts
// @Tags         Hosts Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Action payload"
// @Success      200   {object}  map[string]any
// @Success      201   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /hosts/actions/reorder [post]
// @Router       /hosts/actions/clone [post]
func HostsActionsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewHostRepository(db)
	service := NewHostService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/api/hosts/actions/")
		path = strings.Trim(path, "/")
		switch path {
		case "reorder":
			handleReorderHosts(w, r, service)
		case "clone":
			handleCloneHost(w, r, service)
		default:
			http.NotFound(w, r)
		}
	}
}

// HostsBulkHandler godoc
// @Summary      Bulk host operations
// @Description  Execute bulk enable, disable, delete, set-inbound, set-port, or bulk update on hosts
// @Tags         Hosts Bulk Actions Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Bulk payload"
// @Success      200   {object}  map[string]any
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /hosts/bulk/enable [post]
// @Router       /hosts/bulk/disable [post]
// @Router       /hosts/bulk/delete [post]
// @Router       /hosts/bulk/set-inbound [post]
// @Router       /hosts/bulk/set-port [post]
// @Router       /hosts/bulk/update [patch]
func HostsBulkHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewHostRepository(db)
	service := NewHostService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/hosts/bulk/")
		path = strings.Trim(path, "/")

		requireMethod := func(method string) bool {
			if r.Method != method {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return false
			}
			return true
		}

		switch path {
		case "enable":
			if !requireMethod(http.MethodPost) {
				return
			}
			handleBulkEnableHosts(w, r, service)
		case "disable":
			if !requireMethod(http.MethodPost) {
				return
			}
			handleBulkDisableHosts(w, r, service)
		case "delete":
			if !requireMethod(http.MethodPost) {
				return
			}
			handleBulkDeleteHosts(w, r, service)
		case "set-inbound":
			if !requireMethod(http.MethodPost) {
				return
			}
			handleBulkSetInbound(w, r, service)
		case "set-port":
			if !requireMethod(http.MethodPost) {
				return
			}
			handleBulkSetPort(w, r, service)
		case "update":
			if !requireMethod(http.MethodPatch) {
				return
			}
			handleBulkUpdateHosts(w, r, service)
		default:
			http.NotFound(w, r)
		}
	}
}

// HostsTagsHandler godoc
// @Summary      List host tags
// @Description  Get all unique tags assigned to hosts
// @Tags         Hosts Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /hosts/tags [get]
func HostsTagsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewHostRepository(db)
	service := NewHostService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		tags, err := service.repo.getHostTags(r.Context())
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetAllHostTagsFailed.WithCause(err), cfg)
			return
		}
		shared.WriteJSON(w, http.StatusOK, map[string]any{"response": map[string]any{"tags": tags}})
	}
}

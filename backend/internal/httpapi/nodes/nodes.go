package nodes

import (
	"net/http"
	"strings"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NodesHandler godoc
// @Summary      Manage nodes
// @Description  List, create (201), or update nodes
// @Tags         Nodes Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Node create/update fields"
// @Success      200   {object}  map[string]any
// @Success      201   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /nodes [get]
// @Router       /nodes [post]
// @Router       /nodes [patch]
func NodesHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewNodeRepository(db)
	service := NewNodeService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetNodes(w, r, service)
		case http.MethodPost:
			handleCreateNode(w, r, service)
		case http.MethodPatch:
			handleUpdateNode(w, r, service)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// NodeByUUIDHandler godoc
// @Summary      Node by UUID
// @Description  Get or delete node by UUID, or trigger node lifecycle actions
// @Tags         Nodes Controller
// @Produce      json
// @Security     BearerAuth
// @Param        uuid  path      string  true  "Node UUID" format(uuid)
// @Success      200   {object}  map[string]any
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      404   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /nodes/{uuid} [get]
// @Router       /nodes/{uuid} [delete]
// @Router       /nodes/{uuid}/actions/enable [post]
// @Router       /nodes/{uuid}/actions/disable [post]
// @Router       /nodes/{uuid}/actions/restart [post]
// @Router       /nodes/{uuid}/actions/reset-traffic [post]
func NodeByUUIDHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewNodeRepository(db)
	service := NewNodeService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		path := trimNodesPath(r.URL.Path, "/")
		if path == "" {
			switch r.Method {
			case http.MethodGet:
				handleGetNodes(w, r, service)
			case http.MethodPost:
				handleCreateNode(w, r, service)
			case http.MethodPatch:
				handleUpdateNode(w, r, service)
			default:
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			}
			return
		}

		parts := strings.Split(path, "/")
		if len(parts) == 0 {
			http.NotFound(w, r)
			return
		}
		nodeUUID := parts[0]
		if _, err := uuid.Parse(nodeUUID); err != nil {
			shared.SendAPIError(w, shared.ErrInvalidUUID, cfg)
			return
		}

		if len(parts) >= 3 && parts[1] == "actions" && r.Method == http.MethodPost {
			switch parts[2] {
			case "enable":
				handleEnableNode(w, r, service, nodeUUID)
			case "disable":
				handleDisableNode(w, r, service, nodeUUID)
			case "restart":
				handleRestartNode(w, r, service, nodeUUID)
			case "reset-traffic":
				handleResetNodeTraffic(w, r, service, nodeUUID)
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
			handleGetNode(w, r, service, nodeUUID)
		case http.MethodDelete:
			handleDeleteNode(w, r, service, nodeUUID)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// NodesActionsHandler godoc
// @Summary      Node actions
// @Description  Restart all nodes or reorder nodes
// @Tags         Nodes Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Action payload"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /nodes/actions/restart-all [post]
// @Router       /nodes/actions/reorder [post]
func NodesActionsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewNodeRepository(db)
	service := NewNodeService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		path := trimNodesPath(r.URL.Path, "/actions/")
		switch path {
		case "restart-all":
			handleRestartAllNodes(w, r, service)
		case "reorder":
			handleReorderNodes(w, r, service)
		default:
			http.NotFound(w, r)
		}
	}
}

// NodesBulkActionsHandler godoc
// @Summary      Bulk node actions
// @Description  Execute bulk delete, enable, disable, restart, update or profile modifications
// @Tags         Nodes Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Bulk payload"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /nodes/bulk-actions [post]
// @Router       /nodes/bulk-actions/update [post]
// @Router       /nodes/bulk-actions/profile-modification [post]
func NodesBulkActionsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewNodeRepository(db)
	service := NewNodeService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		path := trimNodesPath(r.URL.Path, "/bulk-actions")
		switch path {
		case "":
			handleBulkNodesActions(w, r, service)
		case "update":
			handleBulkNodesUpdate(w, r, service)
		case "profile-modification":
			handleBulkProfileModification(w, r, service)
		default:
			http.NotFound(w, r)
		}
	}
}

// NodesTagsHandler godoc
// @Summary      Get nodes tags
// @Description  Get all unique tags assigned to nodes
// @Tags         Nodes Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /nodes/tags [get]
func NodesTagsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewNodeRepository(db)
	service := NewNodeService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		tags, err := service.repo.getNodeTags(r.Context())
		if err != nil {
			shared.SendAPIError(w, shared.ErrFetchNodeTagsFailed.WithCause(err), cfg)
			return
		}
		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{
				"tags": tags,
			},
		})
	}
}

func trimNodesPath(path string, suffix string) string {
	for _, prefix := range []string{"/api/nodes"} {
		if strings.HasPrefix(path, prefix+suffix) {
			return strings.Trim(strings.TrimPrefix(path, prefix+suffix), "/")
		}
	}
	return strings.Trim(path, "/")
}

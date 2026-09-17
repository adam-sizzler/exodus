package subscriptionconnections

import (
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"

	"github.com/google/uuid"
)

// NodesHandler godoc
// @Summary      Manage subscription connections (nodes)
// @Description  List, create, or update subscription connection nodes
// @Tags         Connections Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Node creation/update fields"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /subscription-connections [get]
// @Router       /subscription-connections [post]
// @Router       /subscription-connections [patch]
func NodesHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewSubscriptionConnectionRepository(db)
	service := NewSubscriptionConnectionService(repo, cfg)
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
// @Summary      Subscription connection node by UUID
// @Description  Get or delete subscription connection node, or perform node actions
// @Tags         Connections Controller
// @Produce      json
// @Security     BearerAuth
// @Param        uuid  path      string  true  "Node UUID" format(uuid)
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      404   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /subscription-connections/{uuid} [get]
// @Router       /subscription-connections/{uuid} [delete]
// @Router       /subscription-connections/{uuid}/actions/enable [post]
// @Router       /subscription-connections/{uuid}/actions/disable [post]
// @Router       /subscription-connections/{uuid}/actions/restart [post]
// @Router       /subscription-connections/{uuid}/actions/reset-traffic [post]
func NodeByUUIDHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewSubscriptionConnectionRepository(db)
	service := NewSubscriptionConnectionService(repo, cfg)
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
			shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
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
// @Summary      Subscription connection node bulk actions
// @Description  Restart all nodes or reorder nodes
// @Tags         Connections Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Action payload"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /subscription-connections/actions/restart-all [post]
// @Router       /subscription-connections/actions/reorder [post]
func NodesActionsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewSubscriptionConnectionRepository(db)
	service := NewSubscriptionConnectionService(repo, cfg)
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
// @Summary      Subscription connection node batch actions
// @Description  Perform bulk actions or profile modification on multiple connection nodes
// @Tags         Connections Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Bulk action payload"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /subscription-connections/bulk-actions [post]
// @Router       /subscription-connections/bulk-actions/profile-modification [post]
func NodesBulkActionsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewSubscriptionConnectionRepository(db)
	service := NewSubscriptionConnectionService(repo, cfg)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		path := trimNodesPath(r.URL.Path, "/bulk-actions")
		switch path {
		case "":
			handleBulkNodesActions(w, r, service)
		case "profile-modification":
			handleBulkProfileModification(w, r, service)
		default:
			http.NotFound(w, r)
		}
	}
}

// NodesTagsHandler godoc
// @Summary      List subscription connection tags
// @Description  Get all unique tags for subscription connection nodes
// @Tags         Connections Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /subscription-connections/tags [get]
func NodesTagsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewSubscriptionConnectionRepository(db)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		tags, err := repo.getNodeTags(r.Context())
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
	for _, prefix := range []string{"/api/subscription-connections"} {
		if strings.HasPrefix(path, prefix+suffix) {
			return strings.Trim(strings.TrimPrefix(path, prefix+suffix), "/")
		}
	}
	return strings.Trim(path, "/")
}

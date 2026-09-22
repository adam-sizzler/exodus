package nodeplugins

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
	monitor "exodus/internal/nodes"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Handler godoc
// @Summary      Manage node plugins
// @Description  List, create (201), update, delete (204) node plugins or reorder
// @Tags         Node Plugins Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        uuid  path      string  false  "Node plugin UUID" format(uuid)
// @Param        body  body      object  false  "Node plugin payload"
// @Success      200   {object}  map[string]any
// @Success      201   {object}  map[string]any
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      404   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /node-plugins [get]
// @Router       /node-plugins [post]
// @Router       /node-plugins [patch]
// @Router       /node-plugins/{uuid} [get]
// @Router       /node-plugins/{uuid} [delete]
// @Router       /node-plugins/actions/reorder [post]
func Handler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if cfg != nil {
			path = strings.TrimPrefix(path, cfg.Backend.Trimmed())
		}
		path = strings.TrimPrefix(path, "/api/node-plugins")
		path = strings.Trim(path, "/")

		switch {
		case path == "":
			switch r.Method {
			case http.MethodGet:
				handleList(w, r, db, cfg)
			case http.MethodPost:
				handleCreate(w, r, db, cfg)
			case http.MethodPatch:
				handleUpdate(w, r, db, cfg, "")
			default:
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			}
		case path == "executor":
			handleExecutor(w, r, db, cfg)
		case strings.HasPrefix(path, "actions/"):
			handleAction(w, r, db, cfg, strings.TrimPrefix(path, "actions/"))
		case path == "shared-lists" || strings.HasPrefix(path, "shared-lists/"):
			handleSharedLists(w, r, db, cfg, strings.TrimPrefix(strings.TrimPrefix(path, "shared-lists"), "/"))
		default:
			handleByUUID(w, r, db, cfg, path)
		}
	}
}

func handleList(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	plugins, err := loadPlugins(r.Context(), db)
	if err != nil {
		cfg.Logger.Error("Failed to load node plugins", "error", err)
		shared.SendAPIError(w, shared.ErrGetNodePluginsFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, responseEnvelope[listPayload]{
		Response: listPayload{NodePlugins: plugins, Total: len(plugins)},
	})
}

func handleCreate(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON body", err, cfg)
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		shared.SendError(w, http.StatusBadRequest, "name is required", nil, cfg)
		return
	}
	configJSON, err := normalizePluginConfig(req.PluginConfig)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
		return
	}

	plugin, err := createPlugin(r.Context(), db, name, req.Tags, configJSON)
	if err != nil {
		cfg.Logger.Error("Failed to create node plugin", "error", err)
		shared.SendAPIError(w, shared.ErrCreateNodePluginFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusCreated, responseEnvelope[nodePlugin]{Response: plugin})
}

func handleByUUID(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, rawPath string) {
	uuidStr := strings.TrimPrefix(rawPath, "/")
	if uuidStr == "" {
		shared.WriteJSONError(w, http.StatusBadRequest, "missing uuid")
		return
	}
	if _, err := uuid.Parse(uuidStr); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid uuid", nil, cfg)
		return
	}

	switch r.Method {
	case http.MethodGet:
		plugin, err := loadPluginByUUID(r.Context(), db, uuidStr)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				shared.SendAPIError(w, shared.ErrNodePluginNotFound, cfg)
				return
			}
			cfg.Logger.Error("Failed to get node plugin", "error", err)
			shared.SendAPIError(w, shared.ErrGetNodePluginsFailed.WithCause(err), cfg)
			return
		}
		shared.WriteJSON(w, http.StatusOK, responseEnvelope[nodePlugin]{Response: plugin})
	case http.MethodPatch:
		handleUpdate(w, r, db, cfg, uuidStr)
	case http.MethodDelete:
		handleDelete(w, r, db, cfg, uuidStr)
	default:
		shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func handleUpdate(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, urlUUID string) {
	var req updateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON body", err, cfg)
		return
	}
	targetUUID := urlUUID
	if targetUUID == "" && req.UUID != nil {
		targetUUID = *req.UUID
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		shared.SendError(w, http.StatusBadRequest, "uuid is required", nil, cfg)
		return
	}
	if _, err := uuid.Parse(targetUUID); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid uuid", nil, cfg)
		return
	}

	var configJSON *json.RawMessage
	if req.PluginConfig != nil {
		normalized, err := normalizePluginConfig(*req.PluginConfig)
		if err != nil {
			shared.SendError(w, http.StatusBadRequest, err.Error(), nil, cfg)
			return
		}
		configJSON = &normalized
	}

	plugin, err := updatePlugin(r.Context(), db, targetUUID, req.Name, req.Tags, configJSON, req.ViewPosition)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrNodePluginNotFound, cfg)
			return
		}
		cfg.Logger.Error("Failed to update node plugin", "error", err)
		shared.SendAPIError(w, shared.ErrUpdateNodePluginFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, responseEnvelope[nodePlugin]{Response: plugin})
}

func handleDelete(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, pluginUUID string) {
	if err := deletePlugin(r.Context(), db, pluginUUID); err != nil {
		cfg.Logger.Error("Failed to delete node plugin", "error", err)
		shared.SendAPIError(w, shared.ErrDeleteNodePluginFailed.WithCause(err), cfg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleExecutor(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	if r.Method != http.MethodPost {
		shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req executorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON body", err, cfg)
		return
	}
	if shared.IsJSONNull(req.Command.Raw) {
		shared.SendError(w, http.StatusBadRequest, "command is required", nil, cfg)
		return
	}

	target := strings.ToUpper(strings.TrimSpace(req.TargetNodes.Target))
	nodeUUIDs := normalizeUUIDList(req.TargetNodes.NodeUUIDs)
	switch target {
	case "ALL":
	case "SELECTED_NODES":
		if len(nodeUUIDs) == 0 {
			shared.SendError(w, http.StatusBadRequest, "targetNodes.nodeUuids cannot be empty", nil, cfg)
			return
		}
		if err := ensureNodesExist(r.Context(), db, nodeUUIDs); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				shared.SendAPIError(w, shared.ErrNodeNotFound, cfg)
				return
			}
			cfg.Logger.Error("Failed to validate target nodes", "error", err)
			shared.SendAPIError(w, shared.ErrNodeNotFound.WithCause(err), cfg)
			return
		}
	default:
		shared.SendError(w, http.StatusBadRequest, "invalid targetNodes.target", nil, cfg)
		return
	}

	command := strings.TrimSpace(req.Command.Command)
	switch command {
	case "blockIps", "unblockIps", "recreateTables":
	default:
		shared.SendError(w, http.StatusBadRequest, "unsupported executor command", nil, cfg)
		return
	}

	cfg.Logger.Info(
		"Node plugin executor command accepted",
		"command", command,
		"nodes", strings.Join(nodeUUIDs, ","),
	)
	if err := monitor.RequestNodePluginExecutor(req.Command.Raw, nodeUUIDs...); err != nil {
		cfg.Logger.Warn("Failed to send node plugin executor command", "command", command, "error", err)
		shared.SendAPIError(w, shared.ErrExecuteNodePluginFailed.WithCause(err), cfg)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func handleAction(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, action string) {
	if r.Method != http.MethodPost {
		shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	switch action {
	case "executor":
		handleExecutor(w, r, db, cfg)
	case "reorder":
		handleReorder(w, r, db, cfg)
	case "clone":
		handleClone(w, r, db, cfg)
	case "sync":
		handleSync(w, r, db, cfg)
	default:
		shared.SendAPIError(w, shared.ErrNodePluginNotFound, cfg)
	}
}

func handleReorder(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req reorderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON body", err, cfg)
		return
	}
	if len(req.Items) == 0 {
		shared.SendError(w, http.StatusBadRequest, "items are required", nil, cfg)
		return
	}
	if err := reorderPlugins(r.Context(), db, req); err != nil {
		cfg.Logger.Error("Failed to reorder node plugins", "error", err)
		shared.SendAPIError(w, shared.ErrReorderNodePluginsFailed.WithCause(err), cfg)
		return
	}
	plugins, err := loadPlugins(r.Context(), db)
	if err != nil {
		cfg.Logger.Error("Failed to load reordered node plugins", "error", err)
		shared.SendAPIError(w, shared.ErrGetNodePluginsFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, responseEnvelope[listPayload]{Response: listPayload{NodePlugins: plugins, Total: len(plugins)}})
}

func handleClone(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req cloneRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON body", err, cfg)
		return
	}
	plugin, err := clonePlugin(r.Context(), db, req)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrNodePluginNotFound, cfg)
			return
		}
		cfg.Logger.Error("Failed to clone node plugin", "error", err)
		shared.SendAPIError(w, shared.ErrCloneNodePluginFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusCreated, responseEnvelope[nodePlugin]{Response: plugin})
}

func handleSync(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req struct {
		UUID string `json:"uuid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON body", err, cfg)
		return
	}
	targetUUID := strings.TrimSpace(req.UUID)
	if targetUUID == "" {
		shared.SendError(w, http.StatusBadRequest, "uuid is required", nil, cfg)
		return
	}
	if _, err := uuid.Parse(targetUUID); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid uuid", nil, cfg)
		return
	}
	if err := syncPlugin(r.Context(), db, targetUUID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrNodePluginNotFound, cfg)
			return
		}
		cfg.Logger.Error("Failed to sync node plugin", "error", err)
		shared.SendAPIError(w, shared.ErrSyncNodePluginFailed.WithCause(err), cfg)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// NodePluginsTagsHandler godoc
// @Summary      Manage node plugin tags
// @Description  Get unique node plugin tags or set tags for a node plugin
// @Tags         Node Plugins Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /node-plugins/tags [get]
// @Router       /node-plugins/tags [patch]
func NodePluginsTagsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetNodePluginTags(w, r, db, cfg)
		case http.MethodPatch:
			handleSetNodePluginTags(w, r, db, cfg)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handleGetNodePluginTags(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	tags, err := getAllTags(r.Context(), db)
	if err != nil {
		cfg.Logger.Error("Failed to get node plugin tags", "error", err)
		shared.SendAPIError(w, shared.ErrGetNodePluginsFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"tags": tags,
		},
	})
}

func handleSetNodePluginTags(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req shared.SetEntityTagsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	if _, err := uuid.Parse(strings.TrimSpace(req.UUID)); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
		return
	}

	sanitized := shared.SanitizeTags(req.Tags)
	if err := setTags(r.Context(), db, req.UUID, sanitized); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrNodePluginNotFound, cfg)
			return
		}
		cfg.Logger.Error("Failed to set node plugin tags", "error", err)
		shared.SendAPIError(w, shared.ErrUpdateNodePluginFailed.WithCause(err), cfg)
		return
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"uuid": req.UUID,
			"tags": sanitized,
		},
	})
}

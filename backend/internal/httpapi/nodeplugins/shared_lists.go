package nodeplugins

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SharedListRecord struct {
	Name      string          `json:"name"`
	Config    json.RawMessage `json:"config"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
}

type SharedListPreview struct {
	Name       string    `json:"name"`
	Type       string    `json:"type"`
	ItemsCount int       `json:"itemsCount"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

type CreateSharedListRequest struct {
	Name   string          `json:"name"`
	Config json.RawMessage `json:"config"`
}

type UpdateSharedListRequest struct {
	Name   string          `json:"name"`
	Config json.RawMessage `json:"config"`
}

func handleSharedLists(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, subpath string) {
	if subpath == "" {
		switch r.Method {
		case http.MethodGet:
			handleGetAllSharedLists(w, r, db, cfg)
		case http.MethodPost:
			handleCreateSharedList(w, r, db, cfg)
		case http.MethodPatch:
			handleUpdateSharedList(w, r, db, cfg)
		case http.MethodDelete:
			handleDeleteSharedListByBody(w, r, db, cfg)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}

	if subpath == "actions/sync" || strings.HasPrefix(subpath, "actions/sync") {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// "by-name" is a literal route segment: the contract sends the shared
	// list's name as a query parameter (?name=...), not as a path segment.
	if subpath == "by-name" {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		name := strings.TrimSpace(r.URL.Query().Get("name"))
		if name == "" {
			shared.SendError(w, http.StatusBadRequest, "name is required", nil, cfg)
			return
		}
		handleGetSharedListByName(w, r, db, cfg, name)
		return
	}

	name := subpath
	switch r.Method {
	case http.MethodGet:
		handleGetSharedListByName(w, r, db, cfg, name)
	case http.MethodDelete:
		handleDeleteSharedList(w, r, db, cfg, name)
	default:
		shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func handleGetAllSharedLists(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	rows, err := db.Query(r.Context(), `
		SELECT name, config, created_at, updated_at
		FROM shared_lists
		ORDER BY created_at ASC
	`)
	if err != nil {
		if strings.Contains(err.Error(), "relation \"shared_lists\" does not exist") {
			shared.WriteJSON(w, http.StatusOK, map[string]any{
				"response": map[string]any{
					"sharedLists": []SharedListPreview{},
					"total":       0,
				},
			})
			return
		}
		shared.SendAPIError(w, shared.ErrGetAllSharedListsFailed.WithCause(err), cfg)
		return
	}
	defer rows.Close()

	items := make([]SharedListPreview, 0)
	for rows.Next() {
		var name string
		var configBytes []byte
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&name, &configBytes, &createdAt, &updatedAt); err != nil {
			shared.SendAPIError(w, shared.ErrGetAllSharedListsFailed.WithCause(err), cfg)
			return
		}

		var parsed map[string]any
		_ = json.Unmarshal(configBytes, &parsed)
		listType, _ := parsed["type"].(string)
		if listType == "" {
			listType = "ipList"
		}
		itemsList, _ := parsed["items"].([]any)

		items = append(items, SharedListPreview{
			Name:       name,
			Type:       listType,
			ItemsCount: len(itemsList),
			CreatedAt:  createdAt,
			UpdatedAt:  updatedAt,
		})
	}
	if err := rows.Err(); err != nil {
		shared.SendAPIError(w, shared.ErrGetAllSharedListsFailed.WithCause(err), cfg)
		return
	}

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"sharedLists": items,
			"total":       len(items),
		},
	})
}

func handleGetSharedListByName(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, name string) {
	row := db.QueryRow(r.Context(), `
		SELECT name, config, created_at, updated_at
		FROM shared_lists
		WHERE name = $1
	`, name)

	var item SharedListRecord
	var configBytes []byte
	if err := row.Scan(&item.Name, &configBytes, &item.CreatedAt, &item.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrSharedListNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrSharedListNotFound.WithCause(err), cfg)
		return
	}
	item.Config = json.RawMessage(configBytes)

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": item,
	})
}

func handleCreateSharedList(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req CreateSharedListRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON body", err, cfg)
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		shared.SendError(w, http.StatusBadRequest, "name is required", nil, cfg)
		return
	}

	configJSON, _ := json.Marshal(req.Config)
	if len(req.Config) == 0 {
		configJSON = []byte("{}")
	}

	var item SharedListRecord
	var configBytes []byte
	err := db.QueryRow(r.Context(), `
		INSERT INTO shared_lists (name, config, created_at, updated_at)
		VALUES ($1, $2, NOW(), NOW())
		RETURNING name, config, created_at, updated_at
	`, name, configJSON).Scan(&item.Name, &configBytes, &item.CreatedAt, &item.UpdatedAt)
	if err != nil {
		shared.SendAPIError(w, shared.ErrCreateSharedListFailed.WithCause(err), cfg)
		return
	}
	item.Config = json.RawMessage(configBytes)

	shared.WriteJSON(w, http.StatusCreated, map[string]any{
		"response": item,
	})
}

func handleUpdateSharedList(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req UpdateSharedListRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON body", err, cfg)
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		shared.SendError(w, http.StatusBadRequest, "name is required", nil, cfg)
		return
	}

	configJSON, _ := json.Marshal(req.Config)
	if len(req.Config) == 0 {
		configJSON = []byte("{}")
	}

	var item SharedListRecord
	var configBytes []byte
	err := db.QueryRow(r.Context(), `
		UPDATE shared_lists
		SET config = $1, updated_at = NOW()
		WHERE name = $2
		RETURNING name, config, created_at, updated_at
	`, configJSON, name).Scan(&item.Name, &configBytes, &item.CreatedAt, &item.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			shared.SendAPIError(w, shared.ErrSharedListNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrUpdateSharedListFailed.WithCause(err), cfg)
		return
	}
	item.Config = json.RawMessage(configBytes)

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": item,
	})
}

// handleDeleteSharedListByBody handles DELETE /api/node-plugins/shared-lists,
// where the contract (DeleteSharedListCommand.RequestBodySchema) sends the
// target list's name in a JSON body rather than as a URL path segment.
func handleDeleteSharedListByBody(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON body", err, cfg)
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		shared.SendError(w, http.StatusBadRequest, "name is required", nil, cfg)
		return
	}
	handleDeleteSharedList(w, r, db, cfg, name)
}

func handleDeleteSharedList(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, name string) {
	res, err := db.Exec(r.Context(), `DELETE FROM shared_lists WHERE name = $1`, name)
	if err != nil {
		shared.SendAPIError(w, shared.ErrDeleteSharedListFailed.WithCause(err), cfg)
		return
	}
	if res.RowsAffected() == 0 {
		shared.SendAPIError(w, shared.ErrSharedListNotFound, cfg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

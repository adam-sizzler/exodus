package nodeintegrations

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Handler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	repo := NewRepository(db)
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/node-integrations")
		path = strings.Trim(path, "/")

		switch {
		case path == "":
			switch r.Method {
			case http.MethodGet:
				handleGetAll(w, r, repo, cfg)
			case http.MethodPost:
				handleCreate(w, r, repo, cfg)
			case http.MethodPatch:
				handleUpdate(w, r, repo, cfg)
			default:
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			}
		default:
			// /api/node-integrations/:uuid
			itemUUID := path
			if _, err := uuid.Parse(itemUUID); err != nil {
				shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
				return
			}
			switch r.Method {
			case http.MethodGet:
				handleGetByUUID(w, r, repo, cfg, itemUUID)
			case http.MethodDelete:
				handleDelete(w, r, repo, cfg, itemUUID)
			default:
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			}
		}
	}
}

func handleGetAll(w http.ResponseWriter, r *http.Request, repo *Repository, cfg *config.BackendConfig) {
	items, err := repo.GetAll(r.Context())
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetNodeIntegrationsFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"nodeIntegrations": items,
			"total":            len(items),
		},
	})
}

func handleGetByUUID(w http.ResponseWriter, r *http.Request, repo *Repository, cfg *config.BackendConfig, itemUUID string) {
	item, err := repo.GetByUUID(r.Context(), itemUUID)
	if err != nil {
		if errors.Is(err, errIntegrationNotFound) {
			shared.SendAPIError(w, shared.ErrNodeIntegrationNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrGetNodeIntegrationByUUIDFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{"response": item})
}

func handleCreate(w http.ResponseWriter, r *http.Request, repo *Repository, cfg *config.BackendConfig) {
	var req CreateNodeIntegrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		shared.SendError(w, http.StatusBadRequest, "name is required", nil, cfg)
		return
	}

	item, err := repo.Create(r.Context(), req)
	if err != nil {
		shared.SendAPIError(w, shared.ErrCreateNodeIntegrationFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusCreated, map[string]any{"response": item})
}

func handleUpdate(w http.ResponseWriter, r *http.Request, repo *Repository, cfg *config.BackendConfig) {
	var req UpdateNodeIntegrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	if _, err := uuid.Parse(req.UUID); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid UUID format", nil, cfg)
		return
	}

	item, err := repo.Update(r.Context(), req)
	if err != nil {
		if errors.Is(err, errIntegrationNotFound) {
			shared.SendAPIError(w, shared.ErrNodeIntegrationNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrUpdateNodeIntegrationFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{"response": item})
}

func handleDelete(w http.ResponseWriter, r *http.Request, repo *Repository, cfg *config.BackendConfig, itemUUID string) {
	if err := repo.Delete(r.Context(), itemUUID); err != nil {
		if errors.Is(err, errIntegrationNotFound) {
			shared.SendAPIError(w, shared.ErrNodeIntegrationNotFound, cfg)
			return
		}
		shared.SendAPIError(w, shared.ErrDeleteNodeIntegrationFailed.WithCause(err), cfg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

package metadata

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"

	"github.com/google/uuid"
)

type metadataRequest struct {
	Metadata map[string]any `json:"metadata"`
}

// UserHandler godoc
// @Summary      User custom metadata
// @Description  Get or update arbitrary key-value metadata for user by numeric user ID
// @Tags         Metadata Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        userId  path      int              true   "Numeric User ID"
// @Param        body    body      metadataRequest  false  "Metadata JSON object"
// @Success      200     {object}  map[string]any
// @Failure      400     {object}  shared.ErrorResponse
// @Failure      404     {object}  shared.ErrorResponse
// @Failure      500     {object}  shared.ErrorResponse
// @Router       /metadata/user/{userId} [get]
// @Router       /metadata/user/{userId} [put]
func UserHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return entityHandler(db, cfg, "user")
}

// NodeHandler godoc
// @Summary      Node custom metadata
// @Description  Get or update arbitrary key-value metadata for node by UUID
// @Tags         Metadata Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        uuid  path      string           true   "Node UUID" format(uuid)
// @Param        body  body      metadataRequest  false  "Metadata JSON object"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      404   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /metadata/node/{uuid} [get]
// @Router       /metadata/node/{uuid} [put]
func NodeHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return entityHandler(db, cfg, "node")
}

func entityHandler(db *pgxpool.Pool, cfg *config.BackendConfig, entity string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pathParam := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/metadata/"+entity+"/"), "/")
		if entity == "node" {
			if _, err := uuid.Parse(pathParam); err != nil {
				shared.SendError(w, http.StatusBadRequest, "invalid uuid format", nil, cfg)
				return
			}
		} else if entity == "user" {
			if _, err := strconv.ParseInt(pathParam, 10, 64); err != nil {
				shared.SendError(w, http.StatusBadRequest, "invalid userId format", nil, cfg)
				return
			}
		}

		switch r.Method {
		case http.MethodGet:
			metadata, err := getMetadata(r, db, entity, pathParam)
			if err != nil {
				shared.SendAPIError(w, shared.ErrGetMetadataFailed.WithCause(err), cfg)
				return
			}
			shared.WriteJSON(w, http.StatusOK, map[string]any{"response": map[string]any{"metadata": metadata}})
		case http.MethodPut:
			var req metadataRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
				return
			}
			if req.Metadata == nil {
				req.Metadata = map[string]any{}
			}
			metadata, err := upsertMetadata(r, db, entity, pathParam, req.Metadata)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					if entity == "node" {
						shared.SendAPIError(w, shared.ErrNodeNotFound, cfg)
					} else {
						shared.SendAPIError(w, shared.ErrUserNotFound, cfg)
					}
					return
				}
				shared.SendAPIError(w, shared.ErrUpdateMetadataFailed.WithCause(err), cfg)
				return
			}
			shared.WriteJSON(w, http.StatusOK, map[string]any{"response": map[string]any{"metadata": metadata}})
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func getMetadata(r *http.Request, db *pgxpool.Pool, entity, pathParam string) (map[string]any, error) {
	metadata := map[string]any{}
	table, column, id, err := metadataTargetID(r, db, entity, pathParam)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return metadata, nil
		}
		return nil, err
	}
	var raw string
	err = db.QueryRow(r.Context(), "SELECT metadata::text FROM "+table+" WHERE "+column+" = $1", id).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return metadata, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
		return nil, err
	}
	return metadata, nil
}

func upsertMetadata(r *http.Request, db *pgxpool.Pool, entity, pathParam string, metadata map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	table, column, id, err := metadataTargetID(r, db, entity, pathParam)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(r.Context(), `
		INSERT INTO `+table+` (`+column+`, metadata)
		VALUES ($1, $2::jsonb)
		ON CONFLICT (`+column+`) DO UPDATE
		SET metadata = EXCLUDED.metadata
	`, id, string(raw))
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

func metadataTable(entity string) (table string, column string) {
	if entity == "node" {
		return "node_meta", "node_id"
	}
	return "user_meta", "user_id"
}

func metadataTargetID(r *http.Request, db *pgxpool.Pool, entity, pathParam string) (table string, column string, id int64, err error) {
	table, column = metadataTable(entity)
	if entity == "node" {
		err = db.QueryRow(r.Context(), `SELECT id FROM nodes WHERE uuid = $1`, pathParam).Scan(&id)
		return table, column, id, err
	}
	id, err = strconv.ParseInt(pathParam, 10, 64)
	return table, column, id, err
}

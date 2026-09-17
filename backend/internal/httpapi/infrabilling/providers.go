package infrabilling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
)

type providerNode struct {
	Name    string               `json:"name"`
	Details *providerNodeDetails `json:"details"`
}

type providerNodeDetails struct {
	NodeUUID    string `json:"nodeUuid"`
	CountryCode string `json:"countryCode"`
}

type providerRecord struct {
	UUID      string         `json:"uuid"`
	Name      string         `json:"name"`
	Favicon   *string        `json:"faviconLink"`
	LoginURL  *string        `json:"loginUrl"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
	History   map[string]any `json:"billingHistory"`
	Nodes     []providerNode `json:"billingNodes"`
}

type createProviderRequest struct {
	Name       string  `json:"name"`
	FaviconURL *string `json:"faviconLink"`
	LoginURL   *string `json:"loginUrl"`
}

type updateProviderRequest struct {
	UUID       string         `json:"uuid"`
	Name       optionalString `json:"name"`
	FaviconURL optionalString `json:"faviconLink"`
	LoginURL   optionalString `json:"loginUrl"`
}

type optionalString struct {
	Set   bool
	Value *string
}

func (field *optionalString) UnmarshalJSON(data []byte) error {
	field.Set = true
	if string(data) == "null" {
		field.Value = nil
		return nil
	}

	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	field.Value = &value
	return nil
}

// ProvidersHandler godoc
// @Summary      Manage infrastructure billing providers
// @Description  List, create (201), update, get by UUID, or delete (204) cloud/hosting providers
// @Tags         Infra Billing Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Provider payload"
// @Success      200   {object}  map[string]any
// @Success      201   {object}  map[string]any
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      404   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /infra-billing/providers [get]
// @Router       /infra-billing/providers [post]
// @Router       /infra-billing/providers [patch]
// @Router       /infra-billing/providers/{uuid} [get]
// @Router       /infra-billing/providers/{uuid} [delete]
func ProvidersHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		providerUUID := uuidFromPath(r.URL.Path, "/api/infra-billing/providers")

		switch r.Method {
		case http.MethodGet:
			if providerUUID != "" {
				writeProviderResponse(w, r, db, cfg, providerUUID)
				return
			}
			writeProvidersResponse(w, r, db, cfg)
		case http.MethodPost:
			handleCreateProvider(w, r, db, cfg)
		case http.MethodPatch:
			handleUpdateProvider(w, r, db, cfg)
		case http.MethodDelete:
			handleDeleteProvider(w, r, db, cfg, providerUUID)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handleCreateProvider(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req createProviderRequest
	if err := decodeJSONBody(r, &req); err != nil {
		shared.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	name, err := validateProviderName(req.Name)
	if err != nil {
		shared.WriteJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	faviconURL, err := normalizeOptionalURL(req.FaviconURL)
	if err != nil {
		shared.WriteJSONError(w, http.StatusBadRequest, "invalid faviconLink")
		return
	}
	loginURL, err := normalizeOptionalURL(req.LoginURL)
	if err != nil {
		shared.WriteJSONError(w, http.StatusBadRequest, "invalid loginUrl")
		return
	}

	var providerUUID string
	if err := db.QueryRow(r.Context(), `
		INSERT INTO infra_providers (name, favicon_link, login_url)
		VALUES ($1, $2, $3)
		RETURNING uuid
	`, name, nullableString(faviconURL), nullableString(loginURL)).Scan(&providerUUID); err != nil {
		shared.SendAPIError(w, shared.ErrCreateInfraProviderFailed.WithCause(err), cfg)
		return
	}

	writeProviderResponseWithStatus(w, r, db, cfg, providerUUID, http.StatusCreated)
}

func handleUpdateProvider(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req updateProviderRequest
	if err := decodeJSONBody(r, &req); err != nil {
		shared.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.UUID = strings.TrimSpace(req.UUID)
	if req.UUID == "" {
		shared.WriteJSONError(w, http.StatusBadRequest, "uuid is required")
		return
	}

	updates := make([]string, 0, 4)
	args := make([]any, 0, 5)
	idx := 1

	if req.Name.Set {
		if req.Name.Value == nil {
			shared.WriteJSONError(w, http.StatusBadRequest, "name is required")
			return
		}
		name, err := validateProviderName(*req.Name.Value)
		if err != nil {
			shared.WriteJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		updates = append(updates, fmt.Sprintf("name = $%d", idx))
		args = append(args, name)
		idx++
	}

	if req.FaviconURL.Set {
		faviconURL, err := normalizeOptionalURL(req.FaviconURL.Value)
		if err != nil {
			shared.WriteJSONError(w, http.StatusBadRequest, "invalid faviconLink")
			return
		}
		updates = append(updates, fmt.Sprintf("favicon_link = $%d", idx))
		args = append(args, nullableString(faviconURL))
		idx++
	}

	if req.LoginURL.Set {
		loginURL, err := normalizeOptionalURL(req.LoginURL.Value)
		if err != nil {
			shared.WriteJSONError(w, http.StatusBadRequest, "invalid loginUrl")
			return
		}
		updates = append(updates, fmt.Sprintf("login_url = $%d", idx))
		args = append(args, nullableString(loginURL))
		idx++
	}

	if len(updates) > 0 {
		args = append(args, req.UUID)
		query := fmt.Sprintf("UPDATE infra_providers SET %s, updated_at = now() WHERE uuid = $%d", strings.Join(updates, ", "), idx)
		result, err := db.Exec(r.Context(), query, args...)
		if err != nil {
			shared.SendAPIError(w, shared.ErrUpdateInfraProviderFailed.WithCause(err), cfg)
			return
		}
		if result.RowsAffected() == 0 {
			shared.SendAPIError(w, shared.ErrInfraProviderNotFound, cfg)
			return
		}
	}

	writeProviderResponse(w, r, db, cfg, req.UUID)
}

func handleDeleteProvider(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, providerUUID string) {
	if providerUUID == "" {
		shared.WriteJSONError(w, http.StatusBadRequest, "uuid is required")
		return
	}

	result, err := db.Exec(r.Context(), `DELETE FROM infra_providers WHERE uuid = $1`, providerUUID)
	if err != nil {
		shared.SendAPIError(w, shared.ErrDeleteInfraProviderFailed.WithCause(err), cfg)
		return
	}
	if result.RowsAffected() == 0 {
		shared.SendAPIError(w, shared.ErrInfraProviderNotFound, cfg)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func writeProvidersResponse(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	items, err := getProviders(r.Context(), db)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetInfraProvidersFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"total":     len(items),
			"providers": items,
		},
	})
}

func writeProviderResponse(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, providerUUID string) {
	writeProviderResponseWithStatus(w, r, db, cfg, providerUUID, http.StatusOK)
}

func writeProviderResponseWithStatus(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, providerUUID string, status int) {
	item, err := getProvider(r.Context(), db, providerUUID)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetInfraProviderByUUIDFailed.WithCause(err), cfg)
		return
	}
	if item == nil {
		shared.SendAPIError(w, shared.ErrInfraProviderNotFound, cfg)
		return
	}
	shared.WriteJSON(w, status, map[string]any{"response": item})
}

func getProviders(ctx context.Context, db *pgxpool.Pool) ([]providerRecord, error) {
	items := make([]providerRecord, 0)
	historyByProvider := make(map[string]map[string]any)
	nodesByProvider := make(map[string][]providerNode)

	rows, err := db.Query(ctx, `
		SELECT uuid, name, favicon_link, login_url, created_at, updated_at
		FROM infra_providers
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var rec providerRecord
		if scanErr := rows.Scan(&rec.UUID, &rec.Name, &rec.Favicon, &rec.LoginURL, &rec.CreatedAt, &rec.UpdatedAt); scanErr != nil {
			return nil, scanErr
		}
		items = append(items, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	histRows, err := db.Query(ctx, `
		SELECT provider_uuid, COALESCE(ROUND(SUM(amount)::numeric, 2)::float8, 0), COUNT(*)
		FROM infra_billing_history
		GROUP BY provider_uuid
	`)
	if err != nil {
		return nil, err
	}
	defer histRows.Close()

	for histRows.Next() {
		var providerUUID string
		var totalAmount float64
		var totalBills int
		if scanErr := histRows.Scan(&providerUUID, &totalAmount, &totalBills); scanErr != nil {
			return nil, scanErr
		}
		historyByProvider[providerUUID] = map[string]any{
			"totalAmount": totalAmount,
			"totalBills":  totalBills,
		}
	}
	if err := histRows.Err(); err != nil {
		return nil, err
	}

	nodeRows, err := db.Query(ctx, `
		SELECT ibn.provider_uuid, ibn.node_uuid, n.name, n.country_code
		FROM infra_billing_nodes ibn
		JOIN nodes n ON n.uuid = ibn.node_uuid
		ORDER BY n.view_position ASC
	`)
	if err != nil {
		return nil, err
	}
	defer nodeRows.Close()

	for nodeRows.Next() {
		var providerUUID string
		var node providerNode
		var details providerNodeDetails
		if scanErr := nodeRows.Scan(&providerUUID, &details.NodeUUID, &node.Name, &details.CountryCode); scanErr != nil {
			return nil, scanErr
		}
		node.Details = &details
		nodesByProvider[providerUUID] = append(nodesByProvider[providerUUID], node)
	}
	if err := nodeRows.Err(); err != nil {
		return nil, err
	}

	for i := range items {
		history := historyByProvider[items[i].UUID]
		if history == nil {
			history = map[string]any{
				"totalAmount": float64(0),
				"totalBills":  0,
			}
		}
		nodes := nodesByProvider[items[i].UUID]
		if nodes == nil {
			nodes = make([]providerNode, 0)
		}
		items[i].History = history
		items[i].Nodes = nodes
	}

	return items, nil
}

func getProvider(ctx context.Context, db *pgxpool.Pool, providerUUID string) (*providerRecord, error) {
	providerUUID = strings.TrimSpace(providerUUID)
	if providerUUID == "" {
		return nil, nil
	}

	items, err := getProviders(ctx, db)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if items[i].UUID == providerUUID {
			return &items[i], nil
		}
	}
	return nil, nil
}

func validateProviderName(value string) (string, error) {
	name := strings.TrimSpace(value)
	if len(name) < 2 {
		return "", errProviderNameTooShort{}
	}
	if len(name) > 30 {
		return "", errProviderNameTooLong{}
	}
	return name, nil
}

type errProviderNameTooShort struct{}

func (errProviderNameTooShort) Error() string { return "name must be at least 2 characters" }

type errProviderNameTooLong struct{}

func (errProviderNameTooLong) Error() string { return "name must be less than 30 characters" }

func normalizeOptionalURL(value *string) (*string, error) {
	if value == nil {
		return nil, nil
	}

	clean := strings.TrimSpace(*value)
	if clean == "" {
		return nil, nil
	}

	parsed, err := url.ParseRequestURI(clean)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("url must include scheme and host")
	}
	return &clean, nil
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

package infrabilling

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
)

type billingNodeProvider struct {
	UUID       string  `json:"uuid"`
	Name       string  `json:"name"`
	LoginURL   *string `json:"loginUrl"`
	FaviconURL *string `json:"faviconLink"`
}

type billingNodeNode struct {
	UUID        string `json:"uuid"`
	Name        string `json:"name"`
	CountryCode string `json:"countryCode"`
}

type billingNodeRecord struct {
	UUID          string              `json:"uuid"`
	NodeUUID      string              `json:"nodeUuid"`
	Name          *string             `json:"name"`
	ProviderUUID  string              `json:"providerUuid"`
	Provider      billingNodeProvider `json:"provider"`
	Node          billingNodeNode     `json:"node"`
	NextBillingAt time.Time           `json:"nextBillingAt"`
	CreatedAt     time.Time           `json:"createdAt"`
	UpdatedAt     time.Time           `json:"updatedAt"`
}

type availableNodeRecord struct {
	UUID        string `json:"uuid"`
	Name        string `json:"name"`
	CountryCode string `json:"countryCode"`
}

type billingHistoryProvider struct {
	UUID       string  `json:"uuid"`
	Name       string  `json:"name"`
	FaviconURL *string `json:"faviconLink"`
}

type billingHistoryRecord struct {
	UUID         string                 `json:"uuid"`
	ProviderUUID string                 `json:"providerUuid"`
	Amount       float64                `json:"amount"`
	BilledAt     time.Time              `json:"billedAt"`
	Provider     billingHistoryProvider `json:"provider"`
}

type createBillingNodeRequest struct {
	NodeUUID      string  `json:"nodeUuid"`
	ProviderUUID  string  `json:"providerUuid"`
	Name          *string `json:"name"`
	NextBillingAt *string `json:"nextBillingAt"`
}

type updateBillingNodeRequest struct {
	UUIDs         []string `json:"uuids"`
	Name          *string  `json:"name"`
	NextBillingAt string   `json:"nextBillingAt"`
}

type createBillingHistoryRequest struct {
	ProviderUUID string  `json:"providerUuid"`
	Amount       float64 `json:"amount"`
	BilledAt     string  `json:"billedAt"`
}

// BillingNodesHandler godoc
// @Summary      Manage infrastructure billing nodes
// @Description  List, attach (201), update, or detach (204) server nodes to billing providers
// @Tags         Infra Billing Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Billing node payload"
// @Success      200   {object}  map[string]any
// @Success      201   {object}  map[string]any
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /infra-billing/nodes [get]
// @Router       /infra-billing/nodes [post]
// @Router       /infra-billing/nodes [patch]
// @Router       /infra-billing/nodes/{uuid} [delete]
func BillingNodesHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeBillingNodesResponse(w, r, db, cfg, http.StatusOK)
		case http.MethodPost:
			handleCreateBillingNode(w, r, db, cfg)
		case http.MethodPatch:
			handleUpdateBillingNodes(w, r, db, cfg)
		case http.MethodDelete:
			handleDeleteBillingNode(w, r, db, cfg)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

// BillingHistoryHandler godoc
// @Summary      Manage infrastructure billing history
// @Description  List, create (201), or delete (204) billing invoice records
// @Tags         Infra Billing Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        start  query     int     false  "Offset"
// @Param        size   query     int     false  "Limit"
// @Param        body   body      object  false  "Billing history payload"
// @Success      200    {object}  map[string]any
// @Success      201    {object}  map[string]any
// @Success      204
// @Failure      400    {object}  shared.ErrorResponse
// @Failure      500    {object}  shared.ErrorResponse
// @Router       /infra-billing/history [get]
// @Router       /infra-billing/history [post]
// @Router       /infra-billing/history/{uuid} [delete]
func BillingHistoryHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			start, size := historyPaginationFromQuery(r)
			writeBillingHistoryResponse(w, r, db, cfg, http.StatusOK, start, size)
		case http.MethodPost:
			handleCreateBillingHistory(w, r, db, cfg)
		case http.MethodDelete:
			handleDeleteBillingHistory(w, r, db, cfg)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handleCreateBillingNode(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req createBillingNodeRequest
	if err := decodeJSONBody(r, &req); err != nil {
		shared.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if strings.TrimSpace(req.NodeUUID) == "" || strings.TrimSpace(req.ProviderUUID) == "" {
		shared.WriteJSONError(w, http.StatusBadRequest, "nodeUuid and providerUuid are required")
		return
	}

	nextBillingAt := time.Now().UTC()
	if req.NextBillingAt != nil && strings.TrimSpace(*req.NextBillingAt) != "" {
		parsed, err := parseAPITime(*req.NextBillingAt)
		if err != nil {
			shared.WriteJSONError(w, http.StatusBadRequest, "invalid nextBillingAt")
			return
		}
		nextBillingAt = parsed
	}

	var nameArg *string
	if req.Name != nil && strings.TrimSpace(*req.Name) != "" {
		trimmed := strings.TrimSpace(*req.Name)
		nameArg = &trimmed
	}

	_, err := db.Exec(r.Context(), `
		INSERT INTO infra_billing_nodes (node_uuid, provider_uuid, name, next_billing_at)
		VALUES ($1, $2, $3, $4)
	`, req.NodeUUID, req.ProviderUUID, nameArg, nextBillingAt)
	if err != nil {
		shared.SendAPIError(w, shared.ErrCreateInfraBillingNodeFailed.WithCause(err), cfg)
		return
	}

	writeBillingNodesResponse(w, r, db, cfg, http.StatusCreated)
}

func handleUpdateBillingNodes(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req updateBillingNodeRequest
	if err := decodeJSONBody(r, &req); err != nil {
		shared.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if len(req.UUIDs) == 0 {
		shared.WriteJSONError(w, http.StatusBadRequest, "uuids are required")
		return
	}
	nextBillingAt, err := parseAPITime(req.NextBillingAt)
	if err != nil {
		shared.WriteJSONError(w, http.StatusBadRequest, "invalid nextBillingAt")
		return
	}

	for _, nodeUUID := range req.UUIDs {
		nodeUUID = strings.TrimSpace(nodeUUID)
		if nodeUUID == "" {
			continue
		}
		if _, execErr := db.Exec(r.Context(), `
			UPDATE infra_billing_nodes
			SET next_billing_at = $1, updated_at = now()
			WHERE uuid = $2
		`, nextBillingAt, nodeUUID); execErr != nil {
			shared.SendAPIError(w, shared.ErrUpdateInfraBillingNodeFailed.WithCause(execErr), cfg)
			return
		}
	}

	writeBillingNodesResponse(w, r, db, cfg, http.StatusOK)
}

func handleDeleteBillingNode(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	nodeUUID := uuidFromPath(r.URL.Path, "/api/infra-billing/nodes")
	if nodeUUID == "" {
		shared.WriteJSONError(w, http.StatusBadRequest, "uuid is required")
		return
	}
	if _, err := db.Exec(r.Context(), `DELETE FROM infra_billing_nodes WHERE uuid = $1`, nodeUUID); err != nil {
		shared.SendAPIError(w, shared.ErrDeleteInfraBillingNodeFailed.WithCause(err), cfg)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func handleCreateBillingHistory(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	var req createBillingHistoryRequest
	if err := decodeJSONBody(r, &req); err != nil {
		shared.WriteJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if strings.TrimSpace(req.ProviderUUID) == "" {
		shared.WriteJSONError(w, http.StatusBadRequest, "providerUuid is required")
		return
	}
	if req.Amount < 0 {
		shared.WriteJSONError(w, http.StatusBadRequest, "amount must be greater than or equal to 0")
		return
	}
	billedAt, err := parseAPITime(req.BilledAt)
	if err != nil {
		shared.WriteJSONError(w, http.StatusBadRequest, "invalid billedAt")
		return
	}

	_, err = db.Exec(r.Context(), `
		INSERT INTO infra_billing_history (provider_uuid, amount, billed_at)
		VALUES ($1, $2, $3)
	`, req.ProviderUUID, req.Amount, billedAt)
	if err != nil {
		shared.SendAPIError(w, shared.ErrCreateInfraBillingHistoryRecordFailed.WithCause(err), cfg)
		return
	}

	writeBillingHistoryResponse(w, r, db, cfg, http.StatusCreated, 0, 50)
}

func handleDeleteBillingHistory(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	historyUUID := uuidFromPath(r.URL.Path, "/api/infra-billing/history")
	if historyUUID == "" {
		shared.WriteJSONError(w, http.StatusBadRequest, "uuid is required")
		return
	}
	if _, err := db.Exec(r.Context(), `DELETE FROM infra_billing_history WHERE uuid = $1`, historyUUID); err != nil {
		shared.SendAPIError(w, shared.ErrDeleteInfraBillingHistoryRecordFailed.WithCause(err), cfg)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func writeBillingNodesResponse(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, status int) {
	response, err := getBillingNodesResponse(r, db)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetBillingNodesFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, status, map[string]any{"response": response})
}

func writeBillingHistoryResponse(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig, status int, start, size int) {
	response, err := getBillingHistoryResponse(r, db, start, size)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetInfraBillingHistoryRecordsFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, status, map[string]any{"response": response})
}

func getBillingNodesResponse(r *http.Request, db *pgxpool.Pool) (map[string]any, error) {
	billingNodes := make([]billingNodeRecord, 0)
	availableNodes := make([]availableNodeRecord, 0)
	upcomingCount := 0
	currentMonthPayments := float64(0)
	totalSpent := float64(0)

	rows, err := db.Query(r.Context(), `
		SELECT
			ibn.uuid, ibn.node_uuid, ibn.provider_uuid, ibn.next_billing_at, ibn.created_at, ibn.updated_at,
			ip.uuid, ip.name, ip.login_url, ip.favicon_link,
			n.uuid, n.name, n.country_code
		FROM infra_billing_nodes ibn
		JOIN infra_providers ip ON ip.uuid = ibn.provider_uuid
		JOIN nodes n ON n.uuid = ibn.node_uuid
		ORDER BY ibn.next_billing_at ASC, n.name ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var item billingNodeRecord
		if scanErr := rows.Scan(
			&item.UUID, &item.NodeUUID, &item.ProviderUUID, &item.NextBillingAt, &item.CreatedAt, &item.UpdatedAt,
			&item.Provider.UUID, &item.Provider.Name, &item.Provider.LoginURL, &item.Provider.FaviconURL,
			&item.Node.UUID, &item.Node.Name, &item.Node.CountryCode,
		); scanErr != nil {
			return nil, scanErr
		}
		item.Name = &item.Node.Name
		billingNodes = append(billingNodes, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	availRows, err := db.Query(r.Context(), `
		SELECT n.uuid, n.name, n.country_code
		FROM nodes n
		LEFT JOIN infra_billing_nodes ibn ON ibn.node_uuid = n.uuid
		WHERE ibn.node_uuid IS NULL
		ORDER BY n.name ASC
	`)
	if err != nil {
		return nil, err
	}
	defer availRows.Close()

	for availRows.Next() {
		var item availableNodeRecord
		if scanErr := availRows.Scan(&item.UUID, &item.Name, &item.CountryCode); scanErr != nil {
			return nil, scanErr
		}
		availableNodes = append(availableNodes, item)
	}
	if err := availRows.Err(); err != nil {
		return nil, err
	}

	if scanErr := db.QueryRow(r.Context(), `
		SELECT COUNT(*) FROM infra_billing_nodes
		WHERE next_billing_at <= (NOW() + INTERVAL '7 days')
	`).Scan(&upcomingCount); scanErr != nil {
		return nil, scanErr
	}
	if scanErr := db.QueryRow(r.Context(), `
		SELECT COALESCE(SUM(amount), 0)
		FROM infra_billing_history
		WHERE billed_at >= date_trunc('month', NOW())
	`).Scan(&currentMonthPayments); scanErr != nil {
		return nil, scanErr
	}
	if scanErr := db.QueryRow(r.Context(), `
		SELECT COALESCE(SUM(amount), 0)
		FROM infra_billing_history
	`).Scan(&totalSpent); scanErr != nil {
		return nil, scanErr
	}

	return map[string]any{
		"totalBillingNodes":          len(billingNodes),
		"billingNodes":               billingNodes,
		"availableBillingNodes":      availableNodes,
		"totalAvailableBillingNodes": len(availableNodes),
		"stats": map[string]any{
			"upcomingNodesCount":   upcomingCount,
			"currentMonthPayments": currentMonthPayments,
			"totalSpent":           totalSpent,
		},
	}, nil
}

func getBillingHistoryResponse(r *http.Request, db *pgxpool.Pool, start, size int) (map[string]any, error) {
	var total int
	if scanErr := db.QueryRow(r.Context(), `SELECT COUNT(*) FROM infra_billing_history`).Scan(&total); scanErr != nil {
		return nil, scanErr
	}

	rows, err := db.Query(r.Context(), `
		SELECT
			ibh.uuid, ibh.provider_uuid, ibh.amount, ibh.billed_at,
			ip.uuid, ip.name, ip.favicon_link
		FROM infra_billing_history ibh
		JOIN infra_providers ip ON ip.uuid = ibh.provider_uuid
		ORDER BY ibh.billed_at DESC
		OFFSET $1 LIMIT $2
	`, start, size)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]billingHistoryRecord, 0)
	for rows.Next() {
		var item billingHistoryRecord
		if scanErr := rows.Scan(
			&item.UUID, &item.ProviderUUID, &item.Amount, &item.BilledAt,
			&item.Provider.UUID, &item.Provider.Name, &item.Provider.FaviconURL,
		); scanErr != nil {
			return nil, scanErr
		}
		records = append(records, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return map[string]any{
		"records": records,
		"total":   total,
	}, nil
}

func historyPaginationFromQuery(r *http.Request) (int, int) {
	start := parseIntQuery(r.URL.Query().Get("start"), 0, 0, 1_000_000)
	size := parseIntQuery(r.URL.Query().Get("size"), 50, 1, 500)
	return start, size
}

func decodeJSONBody(r *http.Request, target any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(target)
}

func parseAPITime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, strconv.ErrSyntax
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	return time.Parse("2006-01-02", value)
}

func uuidFromPath(path, base string) string {
	rest := strings.TrimPrefix(path, base)
	rest = strings.Trim(rest, "/")
	if rest == "" || strings.Contains(rest, "/") {
		return ""
	}
	return rest
}

func parseIntQuery(raw string, def, min, max int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

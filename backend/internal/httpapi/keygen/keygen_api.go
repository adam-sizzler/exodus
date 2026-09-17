package keygen

import (
	"encoding/base64"
	"encoding/json"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
	"exodus/internal/security"
)

type secretPayload struct {
	NodeCertPem  string `json:"nodeCertPem"`
	NodeKeyPem   string `json:"nodeKeyPem"`
	CaCertPem    string `json:"caCertPem"`
	JWTPublicKey string `json:"jwtPublicKey"`
}

type KeygenResponse struct {
	Response KeygenPayload `json:"response"`
}

type KeygenPayload struct {
	SecretKey string `json:"secretKey"`
	GrpcToken string `json:"grpcToken"`
}

// KeygenHandler godoc
// @Summary      Generate node keys and certs
// @Description  Generate node TLS certificates, public keys, and gRPC auth tokens for node provisioning
// @Tags         Keygen Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  KeygenResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /keygen [get]
func KeygenHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var (
			pubKey string
			caCert string
			caKey  string
		)
		err := db.QueryRow(r.Context(), `
			SELECT pub_key, ca_cert, ca_key
			FROM keygen
			ORDER BY created_at ASC
			LIMIT 1
		`).Scan(&pubKey, &caCert, &caKey)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetKeygenDataFailed.WithCause(err), cfg)
			return
		}

		nodeCert, err := security.GenerateNodeCert(caCert, caKey)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGenerateNodeCertFailed.WithCause(err), cfg)
			return
		}

		raw, err := json.Marshal(secretPayload{
			NodeCertPem:  nodeCert.NodeCertPEM,
			NodeKeyPem:   nodeCert.NodeKeyPEM,
			CaCertPem:    caCert,
			JWTPublicKey: pubKey,
		})
		if err != nil {
			shared.SendAPIError(w, shared.ErrEncodeSecretPayloadFailed.WithCause(err), cfg)
			return
		}
		payload := base64.StdEncoding.EncodeToString(raw)
		grpcToken, err := security.GenerateGRPCAuthToken()
		if err != nil {
			shared.SendAPIError(w, shared.ErrGenerateGRPCTokenFailed.WithCause(err), cfg)
			return
		}

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{
				"secretKey": payload,
				"pubKey":    payload,
				"grpcToken": grpcToken,
			},
		})
	}
}

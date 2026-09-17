package passkeys

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/auth"
	"exodus/internal/httpapi/middleware"
	"exodus/internal/httpapi/shared"

	"github.com/go-webauthn/webauthn/protocol"
	gowebauthn "github.com/go-webauthn/webauthn/webauthn"
)

// PasskeysHandler godoc
// @Summary      Manage passkeys
// @Description  List, rename, or delete (204) registered passkeys for current admin
// @Tags         Passkeys Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  false  "Passkey update/delete payload"
// @Success      200   {object}  map[string]any
// @Success      204
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      401   {object}  shared.ErrorResponse
// @Failure      404   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /passkeys [get]
// @Router       /passkeys [patch]
// @Router       /passkeys [delete]
func PasskeysHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/passkeys" && r.URL.Path != "/api/passkeys/" {
			shared.SendAPIError(w, shared.ErrNotFound, cfg)
			return
		}

		switch r.Method {
		case http.MethodGet:
			handleGetPasskeys(w, r, db, cfg)
		case http.MethodPatch:
			handlePatchPasskey(w, r, db, cfg)
		case http.MethodDelete:
			handleDeletePasskey(w, r, db, cfg)
		default:
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handleGetPasskeys(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	adminUUID, ok := currentAdminUUID(r)
	if !ok {
		shared.SendAPIError(w, shared.ErrUnauthorized, cfg)
		return
	}

	items, err := listPasskeysForAdmin(r.Context(), db, adminUUID)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetPasskeysFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"passkeys": items,
		},
	})
}

func handlePatchPasskey(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	adminUUID, ok := currentAdminUUID(r)
	if !ok {
		shared.SendAPIError(w, shared.ErrUnauthorized, cfg)
		return
	}

	var req passkeyWriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	req.ID = strings.TrimSpace(req.ID)
	req.Name = strings.TrimSpace(req.Name)
	if req.ID == "" {
		shared.SendError(w, http.StatusBadRequest, "id is required", nil, cfg)
		return
	}
	if len(req.Name) < 2 || len(req.Name) > 30 || !passkeyNameRegexp.MatchString(req.Name) {
		shared.SendError(w, http.StatusBadRequest, "invalid passkey name", nil, cfg)
		return
	}

	result, execErr := db.Exec(r.Context(), `
		UPDATE passkeys
		SET passkey_provider = $1, updated_at = CURRENT_TIMESTAMP
		WHERE id = $2 AND admin_uuid = $3
	`, req.Name, req.ID, adminUUID)
	if execErr != nil {
		shared.SendAPIError(w, shared.ErrUpdatePasskeyFailed.WithCause(execErr), cfg)
		return
	}
	if result.RowsAffected() == 0 {
		shared.SendAPIError(w, shared.ErrPasskeyNotFound, cfg)
		return
	}

	items, err := listPasskeysForAdmin(r.Context(), db, adminUUID)
	if err != nil {
		shared.SendAPIError(w, shared.ErrGetPasskeysFailed.WithCause(err), cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"response": map[string]any{
			"passkeys": items,
		},
	})
}

func handleDeletePasskey(w http.ResponseWriter, r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) {
	adminUUID, ok := currentAdminUUID(r)
	if !ok {
		shared.SendAPIError(w, shared.ErrUnauthorized, cfg)
		return
	}

	var req passkeyWriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
		return
	}
	req.ID = strings.TrimSpace(req.ID)
	if req.ID == "" {
		shared.SendError(w, http.StatusBadRequest, "id is required", nil, cfg)
		return
	}

	result, execErr := db.Exec(r.Context(), `DELETE FROM passkeys WHERE id = $1 AND admin_uuid = $2`, req.ID, adminUUID)
	if execErr != nil {
		shared.SendAPIError(w, shared.ErrDeletePasskeyFailed.WithCause(execErr), cfg)
		return
	}
	if result.RowsAffected() == 0 {
		shared.SendAPIError(w, shared.ErrPasskeyNotFound, cfg)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// RegistrationOptionsHandler godoc
// @Summary      Passkey registration options
// @Description  Generate WebAuthn registration credential creation options for current admin
// @Tags         Passkeys Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      401  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /passkeys/registration/options [get]
func RegistrationOptionsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		adminUUID, ok := currentAdminUUID(r)
		if !ok {
			shared.SendAPIError(w, shared.ErrUnauthorized, cfg)
			return
		}

		admin, err := loadWebAuthnAdmin(r.Context(), db, adminUUID)
		if err != nil {
			sendPasskeyError(w, http.StatusInternalServerError, "failed to load admin", err, cfg)
			return
		}

		resolved, err := resolvePasskeySettings(r.Context(), db, r)
		if err != nil {
			sendPasskeySetupError(w, err, cfg)
			return
		}

		wa, err := newWebAuthn(resolved)
		if err != nil {
			sendPasskeyError(w, http.StatusInternalServerError, "failed to initialize passkeys", err, cfg)
			return
		}

		exclusions := make([]protocol.CredentialDescriptor, 0, len(admin.credentials))
		for i := range admin.credentials {
			exclusions = append(exclusions, admin.credentials[i].Descriptor())
		}

		creation, session, err := wa.BeginRegistration(
			admin,
			gowebauthn.WithExclusions(exclusions),
		)
		if err != nil {
			sendPasskeyError(w, http.StatusInternalServerError, "failed to generate passkey registration options", err, cfg)
			return
		}

		passkeySessions.setRegistration(admin.uuid, *session)
		shared.WriteJSON(w, http.StatusOK, map[string]any{"response": creation.Response})
	}
}

// VerifyRegistrationHandler godoc
// @Summary      Verify passkey registration
// @Description  Complete WebAuthn registration and store new passkey credential
// @Tags         Passkeys Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body      object  true  "WebAuthn credential creation response"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      401   {object}  shared.ErrorResponse
// @Failure      403   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /passkeys/registration/verify [post]
func VerifyRegistrationHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		adminUUID, ok := currentAdminUUID(r)
		if !ok {
			shared.SendAPIError(w, shared.ErrUnauthorized, cfg)
			return
		}

		var req verifyRegistrationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
			return
		}
		if len(req.Response) == 0 {
			shared.WriteJSONError(w, http.StatusBadRequest, "response is required")
			return
		}

		admin, err := loadWebAuthnAdmin(r.Context(), db, adminUUID)
		if err != nil {
			sendPasskeyError(w, http.StatusInternalServerError, "failed to load admin", err, cfg)
			return
		}

		session, ok := passkeySessions.popRegistration(admin.uuid)
		if !ok {
			sendPasskeySetupError(w, errChallengeNotFound, cfg)
			return
		}

		resolved, err := resolvePasskeySettings(r.Context(), db, r)
		if err != nil {
			sendPasskeySetupError(w, err, cfg)
			return
		}

		wa, err := newWebAuthn(resolved)
		if err != nil {
			sendPasskeyError(w, http.StatusInternalServerError, "failed to initialize passkeys", err, cfg)
			return
		}

		parsed, err := protocol.ParseCredentialCreationResponseBytes(req.Response)
		if err != nil {
			sendPasskeyError(w, http.StatusBadRequest, "invalid passkey registration response", err, cfg)
			return
		}

		credential, err := wa.CreateCredential(admin, session, parsed)
		if err != nil {
			sendPasskeyError(w, http.StatusForbidden, "failed to verify passkey registration", err, cfg)
			return
		}

		if err := saveNewCredential(r.Context(), db, admin.uuid, credential); err != nil {
			sendPasskeyError(w, http.StatusInternalServerError, "failed to save passkey", err, cfg)
			return
		}

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{
				"verified": true,
			},
		})
	}
}

// AuthenticationOptionsHandler godoc
// @Summary      Passkey authentication options
// @Description  Generate WebAuthn login assertion options
// @Tags         Passkeys Controller
// @Produce      json
// @Success      200  {object}  map[string]any
// @Failure      403  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /auth/passkey/authentication/options [get]
func AuthenticationOptionsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		resolved, err := resolvePasskeySettings(r.Context(), db, r)
		if err != nil {
			sendPasskeySetupError(w, err, cfg)
			return
		}

		admin, err := loadWebAuthnAdmin(r.Context(), db, "")
		if err != nil {
			sendPasskeyError(w, http.StatusForbidden, "passkey authentication is not available", err, cfg)
			return
		}
		if len(admin.credentials) == 0 {
			sendPasskeySetupError(w, errPasskeysNotConfigured, cfg)
			return
		}

		wa, err := newWebAuthn(resolved)
		if err != nil {
			sendPasskeyError(w, http.StatusInternalServerError, "failed to initialize passkeys", err, cfg)
			return
		}

		assertion, session, err := wa.BeginLogin(admin)
		if err != nil {
			sendPasskeyError(w, http.StatusInternalServerError, "failed to generate passkey authentication options", err, cfg)
			return
		}

		passkeySessions.setAuthentication(admin.uuid, *session)
		shared.WriteJSON(w, http.StatusOK, map[string]any{"response": assertion.Response})
	}
}

// VerifyAuthenticationHandler godoc
// @Summary      Verify passkey authentication
// @Description  Complete WebAuthn login assertion and receive admin JWT
// @Tags         Passkeys Controller
// @Accept       json
// @Produce      json
// @Param        body  body      object  true  "WebAuthn assertion response"
// @Success      200   {object}  map[string]any
// @Failure      400   {object}  shared.ErrorResponse
// @Failure      403   {object}  shared.ErrorResponse
// @Failure      500   {object}  shared.ErrorResponse
// @Router       /auth/passkey/authentication/verify [post]
func VerifyAuthenticationHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var req verifyAuthenticationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, cfg)
			return
		}
		if len(req.Response) == 0 {
			shared.WriteJSONError(w, http.StatusBadRequest, "response is required")
			return
		}

		resolved, err := resolvePasskeySettings(r.Context(), db, r)
		if err != nil {
			sendPasskeySetupError(w, err, cfg)
			return
		}

		admin, err := loadWebAuthnAdmin(r.Context(), db, "")
		if err != nil {
			sendPasskeyError(w, http.StatusForbidden, "passkey authentication is not available", err, cfg)
			return
		}

		session, ok := passkeySessions.popAuthentication(admin.uuid)
		if !ok {
			sendPasskeySetupError(w, errChallengeNotFound, cfg)
			return
		}

		wa, err := newWebAuthn(resolved)
		if err != nil {
			sendPasskeyError(w, http.StatusInternalServerError, "failed to initialize passkeys", err, cfg)
			return
		}

		parsed, err := protocol.ParseCredentialRequestResponseBytes(req.Response)
		if err != nil {
			sendPasskeyError(w, http.StatusBadRequest, "invalid passkey authentication response", err, cfg)
			return
		}

		credential, err := wa.ValidateLogin(admin, session, parsed)
		if err != nil {
			sendPasskeyError(w, http.StatusForbidden, "failed to verify passkey authentication", err, cfg)
			return
		}

		if err := updateCredentialUsage(r.Context(), db, admin.uuid, credential); err != nil {
			sendPasskeyError(w, http.StatusInternalServerError, "failed to update passkey usage", err, cfg)
			return
		}

		sessionToken, expiresAt, err := createAdminSession(r.Context(), db, cfg, admin)
		if err != nil {
			sendPasskeyError(w, http.StatusInternalServerError, "failed to create session", err, cfg)
			return
		}

		secureCookie := middleware.IsSecureRequest(r, cfg)
		http.SetCookie(w, &http.Cookie{
			Name:     passkeySessionCookieName,
			Value:    sessionToken,
			Path:     "/",
			Expires:  time.Unix(expiresAt, 0).UTC(),
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
			Secure:   secureCookie,
		})

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{
				"accessToken": sessionToken,
			},
		})
	}
}

func sendPasskeySetupError(w http.ResponseWriter, err error, cfg *config.BackendConfig) {
	switch {
	case errors.Is(err, errPasskeysNotEnabled):
		shared.SendAPIError(w, shared.ErrPasskeysNotEnabled, cfg)
	case errors.Is(err, errPasskeysNotConfigured):
		shared.SendAPIError(w, shared.ErrPasskeysNotConfigured, cfg)
	case errors.Is(err, errChallengeNotFound):
		shared.SendAPIError(w, shared.ErrPasskeyChallengeNotFound, cfg)
	default:
		shared.SendAPIError(w, shared.ErrPasskeySetupFailed.WithCause(err), cfg)
	}
}

func sendPasskeyError(w http.ResponseWriter, status int, msg string, err error, cfg *config.BackendConfig) {
	shared.SendError(w, status, msg, err, cfg)
}

func currentAdminUUID(r *http.Request) (string, bool) {
	principal, ok := auth.CurrentAuthPrincipal(r.Context())
	if !ok || principal == nil {
		return "", false
	}
	adminUUID := strings.TrimSpace(principal.AdminUUID)
	if adminUUID == "" {
		return "", false
	}
	return adminUUID, true
}

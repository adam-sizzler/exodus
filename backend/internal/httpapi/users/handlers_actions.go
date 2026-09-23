package users

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
)

type revokeUserSubscriptionRequest struct {
	RevokeOnlyPasswords bool `json:"revokeOnlyPasswords"`
}

func resolveActionUserUUID(w http.ResponseWriter, r *http.Request, service *UserService, identifier string) (string, bool) {
	id, err := strconv.ParseInt(strings.TrimSpace(identifier), 10, 64)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, "userId must be numeric", err, service.cfg)
		return "", false
	}
	record, err := service.repo.getUserRecordByID(r.Context(), id)
	if err != nil {
		handleUserActionError(w, err, service.cfg, "failed to resolve user")
		return "", false
	}
	return record.UUID, true
}

// handleEnableUser godoc
// @Summary      Enable user
// @Description  Set user status to ACTIVE
// @Tags         Users Controller
// @Produce      json
// @Security     BearerAuth
// @Param        userId  path      int  true  "Numeric User ID"
// @Success      200     {object}  UserResponseEnvelope
// @Failure      400     {object}  shared.ErrorResponse
// @Failure      404     {object}  shared.ErrorResponse
// @Failure      500     {object}  shared.ErrorResponse
// @Router       /users/{userId}/actions/enable [post]
func handleEnableUser(w http.ResponseWriter, r *http.Request, service *UserService, userUUID string) {
	record, err := service.EnableUser(r.Context(), userUUID)
	if err != nil {
		handleUserActionError(w, err, service.cfg, "failed to enable user")
		return
	}
	sendUserRecordResponse(w, r, service, record)
}

// handleDisableUser godoc
// @Summary      Disable user
// @Description  Set user status to DISABLED
// @Tags         Users Controller
// @Produce      json
// @Security     BearerAuth
// @Param        userId  path      int  true  "Numeric User ID"
// @Success      200     {object}  UserResponseEnvelope
// @Failure      400     {object}  shared.ErrorResponse
// @Failure      404     {object}  shared.ErrorResponse
// @Failure      500     {object}  shared.ErrorResponse
// @Router       /users/{userId}/actions/disable [post]
func handleDisableUser(w http.ResponseWriter, r *http.Request, service *UserService, userUUID string) {
	record, err := service.DisableUser(r.Context(), userUUID)
	if err != nil {
		handleUserActionError(w, err, service.cfg, "failed to disable user")
		return
	}
	sendUserRecordResponse(w, r, service, record)
}

// handleResetUserTraffic godoc
// @Summary      Reset user traffic
// @Description  Reset used traffic to 0 for single user
// @Tags         Users Controller
// @Produce      json
// @Security     BearerAuth
// @Param        userId  path      int  true  "Numeric User ID"
// @Success      200     {object}  UserResponseEnvelope
// @Failure      400     {object}  shared.ErrorResponse
// @Failure      404     {object}  shared.ErrorResponse
// @Failure      500     {object}  shared.ErrorResponse
// @Router       /users/{userId}/actions/reset-traffic [post]
func handleResetUserTraffic(w http.ResponseWriter, r *http.Request, service *UserService, userUUID string) {
	record, err := service.ResetUserTraffic(r.Context(), userUUID)
	if err != nil {
		handleUserActionError(w, err, service.cfg, "failed to reset user traffic")
		return
	}
	sendUserRecordResponse(w, r, service, record)
}

// handleRevokeUserSubscription godoc
// @Summary      Revoke user subscription
// @Description  Regenerate subscription credentials and optionally short UUID
// @Tags         Users Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        userId  path      int                            true   "Numeric User ID"
// @Param        body    body      revokeUserSubscriptionRequest  false  "Revocation options"
// @Success      200     {object}  UserResponseEnvelope
// @Failure      400     {object}  shared.ErrorResponse
// @Failure      404     {object}  shared.ErrorResponse
// @Failure      500     {object}  shared.ErrorResponse
// @Router       /users/{userId}/actions/revoke [post]
func handleRevokeUserSubscription(w http.ResponseWriter, r *http.Request, service *UserService, userUUID string) {
	resolvedUUID, ok := resolveActionUserUUID(w, r, service, userUUID)
	if !ok {
		return
	}

	req := revokeUserSubscriptionRequest{}
	if r.Body != nil {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if len(strings.TrimSpace(string(body))) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, service.cfg)
				return
			}
		}
	}

	err := service.RevokeUserSubscription(r.Context(), resolvedUUID, req)
	if err != nil {
		handleUserWriteError(w, err, service.cfg)
		return
	}
	sendUpdatedUserResponse(w, r, service, resolvedUUID)
}

func sendUpdatedUserResponse(w http.ResponseWriter, r *http.Request, service *UserService, userUUID string) {
	record, err := service.repo.getUserRecordByUUID(r.Context(), userUUID)
	if err != nil {
		shared.SendAPIError(w, shared.ErrFetchUpdatedUserFailed.WithCause(err), service.cfg)
		return
	}
	sendUserRecordResponse(w, r, service, record)
}

func sendUserRecordResponse(w http.ResponseWriter, r *http.Request, service *UserService, record userRecord) {
	response, err := buildUserResponses(r.Context(), service.repo, []userRecord{record}, resolveUsersSubscriptionBase(r.Context(), service.repo.db, r, service.cfg))
	if err != nil {
		shared.SendAPIError(w, shared.ErrFetchUpdatedUserFailed.WithCause(err), service.cfg)
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{"response": response[0]})
}

func handleUserActionError(w http.ResponseWriter, err error, cfg *config.BackendConfig, message string) {
	if errors.Is(err, errUserNotFound) {
		shared.SendAPIError(w, shared.ErrUserNotFound, cfg)
		return
	}
	if message != "" {
		shared.SendAPIError(w, shared.ErrUpdateUserFailed.WithCause(fmt.Errorf("%s: %w", message, err)), cfg)
		return
	}
	shared.SendAPIError(w, shared.ErrUpdateUserFailed.WithCause(err), cfg)
}

func plannedUserStatusForUpdate(record userRecord, req updateUserRequest, now time.Time) (string, bool) {
	if req.Status != nil {
		return strings.ToUpper(strings.TrimSpace(*req.Status)), true
	}

	if req.TrafficLimitBytes != nil && strings.EqualFold(record.Status, "LIMITED") {
		if *req.TrafficLimitBytes == 0 || *req.TrafficLimitBytes > record.TrafficLimitBytes {
			return "ACTIVE", true
		}
	}

	if req.ExpireAt != nil && strings.EqualFold(record.Status, "EXPIRED") {
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(*req.ExpireAt))
		if err == nil {
			newExpireAt := parsed.UTC()
			if !newExpireAt.Equal(record.ExpireAt.UTC()) && newExpireAt.After(now.UTC()) {
				return "ACTIVE", true
			}
		}
	}

	return "", false
}

func userConfigPresenceChanges(previousStatus string, nextStatus string) bool {
	previousActive := strings.EqualFold(previousStatus, "ACTIVE")
	nextActive := strings.EqualFold(nextStatus, "ACTIVE")
	return previousActive != nextActive
}

func validateExtendDays(days int) error {
	if days < 1 || days > 9999 {
		return fmt.Errorf("extendDays must be between 1 and 9999")
	}
	return nil
}

// handleExtendUser godoc
// @Summary      Extend user expiration date
// @Description  Extend expireAt for a single user by specified days
// @Tags         Users Controller
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        userId  path      int                          true  "Numeric User ID"
// @Param        body    body      extendUserExpirationRequest  true  "Number of days to extend"
// @Success      200     {object}  UserResponseEnvelope
// @Failure      400     {object}  shared.ErrorResponse
// @Failure      404     {object}  shared.ErrorResponse
// @Failure      500     {object}  shared.ErrorResponse
// @Router       /users/{userId}/actions/extend [post]
func handleExtendUser(w http.ResponseWriter, r *http.Request, service *UserService, userUUID string) {
	var req extendUserExpirationRequest
	resolvedUUID, ok := resolveActionUserUUID(w, r, service, userUUID)
	if !ok {
		return
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, service.cfg)
		return
	}
	days := req.Days
	if days <= 0 {
		days = req.ExtendDays
	}
	if err := validateExtendDays(days); err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, service.cfg)
		return
	}
	err := service.ExtendUserExpirationDate(r.Context(), resolvedUUID, days)
	if err != nil {
		handleUserActionError(w, err, service.cfg, "failed to extend user expiration date")
		return
	}
	sendUpdatedUserResponse(w, r, service, resolvedUUID)
}

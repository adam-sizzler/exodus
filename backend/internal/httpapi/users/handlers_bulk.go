package users

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"exodus/internal/httpapi/shared"
	"exodus/internal/util"
)

func resolveBulkTargetUUIDs(ctx context.Context, repo *UserRepository, userIDs []int64) ([]string, error) {
	if len(userIDs) == 0 {
		return nil, fmt.Errorf("userIds cannot be empty")
	}
	if len(userIDs) > 500 {
		return nil, fmt.Errorf("userIds cannot contain more than 500 items")
	}
	return repo.resolveUUIDsByUserIDs(ctx, userIDs)
}

// handleBulkDeleteUsers godoc
// @Summary      Bulk delete users by IDs
// @Description  Delete multiple users by their IDs
// @Tags         Users Bulk Actions Controller
// @Accept       json
// @Security     BearerAuth
// @Param        body  body  bulkDeleteUsersRequest  true  "User IDs to delete"
// @Success      204
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /users/bulk/delete [post]
func handleBulkDeleteUsers(w http.ResponseWriter, r *http.Request, service *UserService) {
	var req bulkDeleteUsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, service.cfg)
		return
	}
	targets, err := resolveBulkTargetUUIDs(r.Context(), service.repo, req.UserIDs)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, service.cfg)
		return
	}

	err = service.BulkDeleteUsers(r.Context(), targets)
	if err != nil {
		shared.SendAPIError(w, shared.ErrBulkDeleteUsersFailed.WithCause(err), service.cfg)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleBulkRevokeUsersSubscription godoc
// @Summary      Bulk revoke users subscription
// @Description  Revoke subscriptions for multiple users
// @Tags         Users Bulk Actions Controller
// @Accept       json
// @Security     BearerAuth
// @Param        body  body  bulkRevokeUsersSubscriptionRequest  true  "User IDs to revoke"
// @Success      202
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /users/bulk/revoke-subscription [post]
func handleBulkRevokeUsersSubscription(w http.ResponseWriter, r *http.Request, service *UserService) {
	var req bulkRevokeUsersSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, service.cfg)
		return
	}
	targets, err := resolveBulkTargetUUIDs(r.Context(), service.repo, req.UserIDs)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, service.cfg)
		return
	}

	// handled one by one, same as upstream Exodus's bulkRevokeUsersSubscription
	for _, targetUUID := range targets {
		if err := service.RevokeUserSubscription(r.Context(), targetUUID, revokeUserSubscriptionRequest{}); err != nil {
			shared.SendAPIError(w, shared.ErrBulkRevokeSubscriptionFailed.WithCause(err), service.cfg)
			return
		}
	}

	w.WriteHeader(http.StatusAccepted)
}

// handleBulkResetUsersTraffic godoc
// @Summary      Bulk reset users traffic
// @Description  Reset used traffic to 0 for specified users
// @Tags         Users Bulk Actions Controller
// @Accept       json
// @Security     BearerAuth
// @Param        body  body  bulkDeleteUsersRequest  true  "User IDs to reset"
// @Success      202
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /users/bulk/reset-traffic [post]
func handleBulkResetUsersTraffic(w http.ResponseWriter, r *http.Request, service *UserService) {
	var req bulkDeleteUsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, service.cfg)
		return
	}
	targets, err := resolveBulkTargetUUIDs(r.Context(), service.repo, req.UserIDs)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, service.cfg)
		return
	}

	if _, err := service.BulkResetUsersTraffic(r.Context(), targets); err != nil {
		shared.SendAPIError(w, shared.ErrBulkResetTrafficFailed.WithCause(err), service.cfg)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// handleBulkDeleteUsersByStatus godoc
// @Summary      Bulk delete users by status
// @Description  Delete all users having a specific status
// @Tags         Users Bulk Actions Controller
// @Accept       json
// @Security     BearerAuth
// @Param        body  body  bulkDeleteUsersByStatusRequest  true  "Status to delete"
// @Success      202
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /users/bulk/delete-by-status [post]
func handleBulkDeleteUsersByStatus(w http.ResponseWriter, r *http.Request, service *UserService) {
	var req bulkDeleteUsersByStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, service.cfg)
		return
	}

	status := strings.ToUpper(strings.TrimSpace(req.Status))
	if !isValidUserStatus(status) {
		shared.SendError(w, http.StatusBadRequest, "invalid status", nil, service.cfg)
		return
	}

	if _, err := service.BulkDeleteUsersByStatus(r.Context(), status); err != nil {
		shared.SendAPIError(w, shared.ErrBulkDeleteByStatusFailed.WithCause(err), service.cfg)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// handleBulkAllResetUsersTraffic godoc
// @Summary      Reset traffic for all users
// @Description  Reset used traffic to 0 for all users in the system
// @Tags         Users Bulk Actions Controller
// @Security     BearerAuth
// @Success      202
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /users/bulk/all/reset-traffic [post]
func handleBulkAllResetUsersTraffic(w http.ResponseWriter, r *http.Request, service *UserService) {
	if _, err := service.BulkAllResetUsersTraffic(r.Context()); err != nil {
		shared.SendAPIError(w, shared.ErrBulkResetAllTrafficFailed.WithCause(err), service.cfg)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// handleBulkExtendUsersExpirationDate godoc
// @Summary      Bulk extend users expiration date
// @Description  Add days to expireAt for specified users
// @Tags         Users Bulk Actions Controller
// @Accept       json
// @Security     BearerAuth
// @Param        body  body  bulkExtendExpirationDateRequest  true  "User IDs and extension days"
// @Success      204
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /users/bulk/extend-expiration-date [post]
func handleBulkExtendUsersExpirationDate(w http.ResponseWriter, r *http.Request, service *UserService) {
	var req bulkExtendExpirationDateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, service.cfg)
		return
	}
	targets, err := resolveBulkTargetUUIDs(r.Context(), service.repo, req.UserIDs)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, service.cfg)
		return
	}
	if err := validateExtendDays(req.ExtendDays); err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, service.cfg)
		return
	}

	_, err = service.BulkExtendUsersExpirationDate(r.Context(), targets, req.ExtendDays)
	if err != nil {
		shared.SendAPIError(w, shared.ErrBulkExtendExpirationFailed.WithCause(err), service.cfg)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleBulkAllExtendUsersExpirationDate godoc
// @Summary      Extend expiration date for all users
// @Description  Add days to expireAt for all users in the system
// @Tags         Users Bulk Actions Controller
// @Accept       json
// @Security     BearerAuth
// @Param        body  body  bulkAllExtendExpirationDateRequest  true  "Extension days"
// @Success      202
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /users/bulk/all/extend-expiration-date [post]
func handleBulkAllExtendUsersExpirationDate(w http.ResponseWriter, r *http.Request, service *UserService) {
	var req bulkAllExtendExpirationDateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, service.cfg)
		return
	}
	if err := validateExtendDays(req.ExtendDays); err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, service.cfg)
		return
	}

	if _, err := service.BulkAllExtendUsersExpirationDate(r.Context(), req.ExtendDays); err != nil {
		shared.SendAPIError(w, shared.ErrBulkExtendExpirationFailed.WithCause(err), service.cfg)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// handleBulkUpdateUsers godoc
// @Summary      Bulk update users
// @Description  Update fields for multiple users
// @Tags         Users Bulk Actions Controller
// @Accept       json
// @Security     BearerAuth
// @Param        body  body  bulkUpdateUsersRequest  true  "User IDs and fields to update"
// @Success      202
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /users/bulk/update [post]
func handleBulkUpdateUsers(w http.ResponseWriter, r *http.Request, service *UserService) {
	var req bulkUpdateUsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, service.cfg)
		return
	}
	targets, err := resolveBulkTargetUUIDs(r.Context(), service.repo, req.UserIDs)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, service.cfg)
		return
	}
	if err := validateBulkUpdateUsersFields(req.Fields); err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, service.cfg)
		return
	}

	if _, err := service.BulkUpdateUsers(r.Context(), targets, req.Fields); err != nil {
		handleUserWriteError(w, err, service.cfg)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// handleBulkUpdateUsersSquads godoc
// @Summary      Bulk update users squads
// @Description  Assign internal squads for multiple users
// @Tags         Users Bulk Actions Controller
// @Accept       json
// @Security     BearerAuth
// @Param        body  body  bulkUpdateUsersSquadsRequest  true  "User IDs and internal squad UUIDs"
// @Success      204
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /users/bulk/update-squads [post]
func handleBulkUpdateUsersSquads(w http.ResponseWriter, r *http.Request, service *UserService) {
	var req bulkUpdateUsersSquadsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, service.cfg)
		return
	}
	targets, err := resolveBulkTargetUUIDs(r.Context(), service.repo, req.UserIDs)
	if err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, service.cfg)
		return
	}
	if err := util.ValidateUUIDsAllowEmpty(req.ActiveInternalSquads); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid activeInternalSquads", err, service.cfg)
		return
	}

	_, err = service.BulkUpdateUsersSquads(r.Context(), targets, req.ActiveInternalSquads)
	if err != nil {
		handleUserWriteError(w, err, service.cfg)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleBulkAllUpdateUsers godoc
// @Summary      Update all users
// @Description  Update fields for all users in the system
// @Tags         Users Bulk Actions Controller
// @Accept       json
// @Security     BearerAuth
// @Param        body  body  bulkAllUpdateUsersRequest  true  "Fields to update across all users"
// @Success      202
// @Failure      400  {object}  shared.ErrorResponse
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /users/bulk/all/update [post]
func handleBulkAllUpdateUsers(w http.ResponseWriter, r *http.Request, service *UserService) {
	var req bulkAllUpdateUsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		shared.SendError(w, http.StatusBadRequest, "invalid JSON", err, service.cfg)
		return
	}
	if err := validateBulkAllUpdateUsersRequest(req); err != nil {
		shared.SendError(w, http.StatusBadRequest, err.Error(), nil, service.cfg)
		return
	}

	if _, err := service.BulkAllUpdateUsers(r.Context(), req); err != nil {
		handleUserWriteError(w, err, service.cfg)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

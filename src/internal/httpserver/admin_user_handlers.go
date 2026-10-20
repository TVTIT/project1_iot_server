package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
	"iot-platform/internal/usermanagement"
)

// adminUserManagementCacheMiddleware ensures Cache-Control: no-store on all admin user management endpoints,
// and Pragma: no-cache on mutation endpoints, before CORS, auth, and routing dispatch.
func adminUserManagementCacheMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := strings.TrimSuffix(c.Request.URL.Path, "/")
		if isAdminUserManagementRoute(path) {
			c.Header("Cache-Control", "no-store")
			if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead && c.Request.Method != http.MethodOptions {
				c.Header("Pragma", "no-cache")
			}
		}
		c.Next()
	}
}

func isAdminUserManagementRoute(path string) bool {
	if path == "/v1/admin/users" {
		return true
	}
	parts := strings.Split(path, "/")
	// /v1/admin/users/:user_id
	if len(parts) == 5 && parts[1] == "v1" && parts[2] == "admin" && parts[3] == "users" && parts[4] != "" {
		return true
	}
	// /v1/admin/gateways/:gateway_id/users/:user_id
	if len(parts) == 7 && parts[1] == "v1" && parts[2] == "admin" && parts[3] == "gateways" && parts[4] != "" && parts[5] == "users" && parts[6] != "" {
		return true
	}
	// /v1/admin/gateways/:gateway_id/owner
	if len(parts) == 6 && parts[1] == "v1" && parts[2] == "admin" && parts[3] == "gateways" && parts[4] != "" && parts[5] == "owner" {
		return true
	}
	return false
}

// POST /v1/admin/users
func createUserHandler(creator usermanagement.UserCreator, limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Header("Pragma", "no-cache")

		p, ok := auth.PrincipalFrom(c)
		if !ok || p.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if auth.IsNilDependency(creator) {
			httpapi.WriteError(c, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
			return
		}

		input, status := parseCreateUserRequest(c.Request, limit)
		if status != 0 {
			code, message := "invalid_request", "invalid request"
			if status == http.StatusRequestEntityTooLarge {
				code, message = "payload_too_large", "payload too large"
			}
			if status == http.StatusUnsupportedMediaType {
				code, message = "unsupported_media_type", "unsupported media type"
			}
			httpapi.WriteError(c, status, code, message)
			return
		}

		result, err := creator.CreateUser(c.Request.Context(), p.UserID, input)
		if err != nil {
			status, code, message := http.StatusInternalServerError, "internal_error", "internal server error"
			switch {
			case errors.Is(err, usermanagement.ErrUserAlreadyExists):
				status, code, message = http.StatusConflict, "user_already_exists", "user already exists"
			case errors.Is(err, usermanagement.ErrInvalidInput):
				status, code, message = http.StatusBadRequest, "invalid_request", "invalid request"
			case errors.Is(err, usermanagement.ErrActorNotAdmin):
				status, code, message = http.StatusForbidden, "forbidden", "access denied"
			case errors.Is(err, usermanagement.ErrAuthUnavailable):
				status, code, message = http.StatusServiceUnavailable, "service_unavailable", "service unavailable"
			case errors.Is(err, usermanagement.ErrProfileVerificationFailed):
				status, code, message = http.StatusServiceUnavailable, "service_unavailable", "service unavailable"
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				status, code, message = http.StatusServiceUnavailable, "service_unavailable", "service unavailable"
			}
			logArgs := []any{
				"request_id", httpapi.RequestIDFrom(c),
				"actor_id", p.UserID.String(),
				"error_code", code,
			}
			var pve *usermanagement.ProfileVerificationError
			if errors.As(err, &pve) {
				logArgs = append(logArgs, "target_id", pve.TargetID.String())
			}
			slog.ErrorContext(c.Request.Context(), "create user failed", logArgs...)
			httpapi.WriteError(c, status, code, message)
			return
		}

		c.Header("Location", fmt.Sprintf("/v1/admin/users/%s", result.ID))
		c.JSON(http.StatusCreated, result)
	}
}

// GET /v1/admin/users
func listUsersHandler(lister usermanagement.UserLister) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Header("Pragma", "no-cache")

		p, ok := auth.PrincipalFrom(c)
		if !ok || p.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if auth.IsNilDependency(lister) {
			httpapi.WriteError(c, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
			return
		}

		if c.Request.URL.RawQuery != "" || c.Request.URL.ForceQuery {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}

		buf := make([]byte, 1)
		n, err := c.Request.Body.Read(buf)
		if n > 0 || (err != nil && err != io.EOF) {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}

		items, err := lister.ListUsers(c.Request.Context(), p.UserID)
		if err != nil {
			status, code, message := http.StatusInternalServerError, "internal_error", "internal server error"
			switch {
			case errors.Is(err, usermanagement.ErrCapacityExceeded):
				status, code, message = http.StatusConflict, "user_list_capacity_exceeded", "user list capacity exceeded"
			case errors.Is(err, usermanagement.ErrActorNotAdmin):
				status, code, message = http.StatusForbidden, "forbidden", "access denied"
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				status, code, message = http.StatusServiceUnavailable, "service_unavailable", "service unavailable"
			}
			slog.ErrorContext(c.Request.Context(), "list users failed",
				"request_id", httpapi.RequestIDFrom(c),
				"actor_id", p.UserID.String(),
				"error_code", code,
			)
			httpapi.WriteError(c, status, code, message)
			return
		}

		if items == nil {
			items = []usermanagement.UserSummary{}
		}
		c.JSON(http.StatusOK, struct {
			Items []usermanagement.UserSummary `json:"items"`
		}{Items: items})
	}
}

// PATCH /v1/admin/users/:user_id
func updateUserHandler(updater usermanagement.UserUpdater, limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Header("Pragma", "no-cache")

		p, ok := auth.PrincipalFrom(c)
		if !ok || p.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if auth.IsNilDependency(updater) {
			httpapi.WriteError(c, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
			return
		}

		targetID, err := uuid.Parse(c.Param("user_id"))
		if err != nil || targetID == uuid.Nil {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}

		input, status := parseUpdateUserRequest(c.Request, limit)
		if status != 0 {
			code, message := "invalid_request", "invalid request"
			if status == http.StatusRequestEntityTooLarge {
				code, message = "payload_too_large", "payload too large"
			}
			if status == http.StatusUnsupportedMediaType {
				code, message = "unsupported_media_type", "unsupported media type"
			}
			httpapi.WriteError(c, status, code, message)
			return
		}

		if err := usermanagement.CheckSelfDemotion(p.UserID, targetID, input); err != nil {
			httpapi.WriteError(c, http.StatusConflict, "self_admin_demotion_forbidden", "self admin demotion forbidden")
			return
		}

		result, err := updater.UpdateUser(c.Request.Context(), p.UserID, targetID, input)
		if err != nil {
			status, code, message := http.StatusInternalServerError, "internal_error", "internal server error"
			switch {
			case errors.Is(err, usermanagement.ErrSelfDemotionForbidden):
				status, code, message = http.StatusConflict, "self_admin_demotion_forbidden", "self admin demotion forbidden"
			case errors.Is(err, usermanagement.ErrUserNotFound):
				status, code, message = http.StatusNotFound, "not_found", "resource not found"
			case errors.Is(err, usermanagement.ErrInvalidInput):
				status, code, message = http.StatusBadRequest, "invalid_request", "invalid request"
			case errors.Is(err, usermanagement.ErrActorNotAdmin):
				status, code, message = http.StatusForbidden, "forbidden", "access denied"
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				status, code, message = http.StatusServiceUnavailable, "service_unavailable", "service unavailable"
			}
			slog.ErrorContext(c.Request.Context(), "update user failed",
				"request_id", httpapi.RequestIDFrom(c),
				"actor_id", p.UserID.String(),
				"target_id", targetID.String(),
				"error_code", code,
			)
			httpapi.WriteError(c, status, code, message)
			return
		}

		c.JSON(http.StatusOK, result)
	}
}

// PUT /v1/admin/gateways/:gateway_id/users/:user_id
func assignGatewayUserHandler(assigner usermanagement.GatewayUserAssigner, limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Header("Pragma", "no-cache")

		p, ok := auth.PrincipalFrom(c)
		if !ok || p.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if auth.IsNilDependency(assigner) {
			httpapi.WriteError(c, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
			return
		}

		gatewayID := c.Param("gateway_id")
		if gateway.ValidateGatewayID(gatewayID) != nil || gatewayID == "backend_service" {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}

		targetID, err := uuid.Parse(c.Param("user_id"))
		if err != nil || targetID == uuid.Nil {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}

		role, status := parseAssignGatewayUserRequest(c.Request, limit)
		if status != 0 {
			code, message := "invalid_request", "invalid request"
			if status == http.StatusRequestEntityTooLarge {
				code, message = "payload_too_large", "payload too large"
			}
			if status == http.StatusUnsupportedMediaType {
				code, message = "unsupported_media_type", "unsupported media type"
			}
			httpapi.WriteError(c, status, code, message)
			return
		}

		if err := usermanagement.ValidateGatewayRole(role); err != nil {
			if errors.Is(err, usermanagement.ErrOwnerRoleNotAllowed) {
				httpapi.WriteError(c, http.StatusBadRequest, "invalid_role", "owner role requires owner transfer endpoint")
				return
			}
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}

		if err := assigner.AssignGatewayUser(c.Request.Context(), p.UserID, gatewayID, targetID, role); err != nil {
			status, code, message := http.StatusInternalServerError, "internal_error", "internal server error"
			switch {
			case errors.Is(err, usermanagement.ErrOwnerRoleNotAllowed):
				status, code, message = http.StatusBadRequest, "invalid_role", "owner role requires owner transfer endpoint"
			case errors.Is(err, usermanagement.ErrOwnerTransferRequired):
				status, code, message = http.StatusConflict, "owner_transfer_required", "owner transfer required"
			case errors.Is(err, usermanagement.ErrUserNotFound), errors.Is(err, usermanagement.ErrGatewayNotFound):
				status, code, message = http.StatusNotFound, "not_found", "resource not found"
			case errors.Is(err, usermanagement.ErrInvalidRole), errors.Is(err, usermanagement.ErrInvalidInput):
				status, code, message = http.StatusBadRequest, "invalid_request", "invalid request"
			case errors.Is(err, usermanagement.ErrActorNotAdmin):
				status, code, message = http.StatusForbidden, "forbidden", "access denied"
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				status, code, message = http.StatusServiceUnavailable, "service_unavailable", "service unavailable"
			}
			slog.ErrorContext(c.Request.Context(), "assign gateway user failed",
				"request_id", httpapi.RequestIDFrom(c),
				"actor_id", p.UserID.String(),
				"gateway_id", gatewayID,
				"target_id", targetID.String(),
				"error_code", code,
			)
			httpapi.WriteError(c, status, code, message)
			return
		}

		c.JSON(http.StatusOK, struct {
			GatewayID string `json:"gateway_id"`
			UserID    string `json:"user_id"`
			Role      string `json:"role"`
		}{
			GatewayID: gatewayID,
			UserID:    targetID.String(),
			Role:      role,
		})
	}
}

// DELETE /v1/admin/gateways/:gateway_id/users/:user_id
func removeGatewayUserHandler(remover usermanagement.GatewayUserRemover) gin.HandlerFunc {
	return func(c *gin.Context) {
		p, ok := auth.PrincipalFrom(c)
		if !ok || p.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if auth.IsNilDependency(remover) {
			httpapi.WriteError(c, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
			return
		}
		gatewayID := c.Param("gateway_id")
		targetID, err := uuid.Parse(c.Param("user_id"))
		if err != nil || targetID == uuid.Nil || gateway.ValidateGatewayID(gatewayID) != nil || gatewayID == "backend_service" || c.Request.URL.RawQuery != "" || c.Request.URL.ForceQuery {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}
		// Read at most one byte, rejecting even whitespace and chunked bodies.
		if c.Request.Body != nil {
			var one [1]byte
			n, err := io.ReadFull(c.Request.Body, one[:])
			if n != 0 || (err != nil && !errors.Is(err, io.EOF)) {
				httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
				return
			}
		}
		if err := remover.RemoveGatewayUser(c.Request.Context(), p.UserID, gatewayID, targetID); err != nil {
			status, code, message := http.StatusInternalServerError, "internal_error", "internal server error"
			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.Is(err, usermanagement.ErrMembershipUnavailable):
				status, code, message = http.StatusServiceUnavailable, "service_unavailable", "service unavailable"
			case errors.Is(err, usermanagement.ErrActorNotAdmin):
				status, code, message = http.StatusForbidden, "forbidden", "access denied"
			case errors.Is(err, usermanagement.ErrOwnerTransferRequired):
				status, code, message = http.StatusConflict, "owner_transfer_required", "owner transfer required"
			case errors.Is(err, usermanagement.ErrGatewayNotFound):
				status, code, message = http.StatusNotFound, "not_found", "resource not found"
			case errors.Is(err, usermanagement.ErrInvalidInput):
				status, code, message = http.StatusBadRequest, "invalid_request", "invalid request"
			}
			slog.ErrorContext(c.Request.Context(), "remove gateway user failed", "request_id", httpapi.RequestIDFrom(c), "actor_id", p.UserID.String(), "gateway_id", gatewayID, "target_id", targetID.String(), "error_code", code)
			httpapi.WriteError(c, status, code, message)
			return
		}
		slog.InfoContext(c.Request.Context(), "gateway membership revocation completed",
			"request_id", httpapi.RequestIDFrom(c), "actor_id", p.UserID.String(),
			"gateway_id", gatewayID, "target_id", targetID.String(), "outcome", "success")
		c.Status(http.StatusNoContent)
	}
}

// PUT /v1/admin/gateways/:gateway_id/owner
func transferGatewayOwnerHandler(ownerTransferer usermanagement.GatewayOwnerTransferer, limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Header("Pragma", "no-cache")

		p, ok := auth.PrincipalFrom(c)
		if !ok || p.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if auth.IsNilDependency(ownerTransferer) {
			httpapi.WriteError(c, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
			return
		}

		gatewayID := c.Param("gateway_id")
		if gateway.ValidateGatewayID(gatewayID) != nil || gatewayID == "backend_service" {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}

		targetUserID, status := parseTransferGatewayOwnerRequest(c.Request, limit)
		if status != 0 {
			code, message := "invalid_request", "invalid request"
			if status == http.StatusRequestEntityTooLarge {
				code, message = "payload_too_large", "payload too large"
			}
			if status == http.StatusUnsupportedMediaType {
				code, message = "unsupported_media_type", "unsupported media type"
			}
			httpapi.WriteError(c, status, code, message)
			return
		}

		if err := ownerTransferer.TransferGatewayOwner(c.Request.Context(), p.UserID, gatewayID, targetUserID); err != nil {
			status, code, message := http.StatusInternalServerError, "internal_error", "internal server error"
			switch {
			case errors.Is(err, usermanagement.ErrActorNotAdmin):
				status, code, message = http.StatusForbidden, "forbidden", "access denied"
			case errors.Is(err, usermanagement.ErrGatewayOwnerInconsistent):
				status, code, message = http.StatusConflict, "gateway_owner_inconsistent", "gateway owner inconsistent"
			case errors.Is(err, usermanagement.ErrUserNotFound), errors.Is(err, usermanagement.ErrGatewayNotFound):
				status, code, message = http.StatusNotFound, "not_found", "resource not found"
			case errors.Is(err, usermanagement.ErrInvalidInput):
				status, code, message = http.StatusBadRequest, "invalid_request", "invalid request"
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				status, code, message = http.StatusServiceUnavailable, "service_unavailable", "service unavailable"
			}
			slog.ErrorContext(c.Request.Context(), "transfer gateway owner failed",
				"request_id", httpapi.RequestIDFrom(c),
				"actor_id", p.UserID.String(),
				"gateway_id", gatewayID,
				"target_id", targetUserID.String(),
				"error_code", code,
			)
			httpapi.WriteError(c, status, code, message)
			return
		}

		c.JSON(http.StatusOK, struct {
			GatewayID   string `json:"gateway_id"`
			OwnerUserID string `json:"owner_user_id"`
		}{
			GatewayID:   gatewayID,
			OwnerUserID: targetUserID.String(),
		})
	}
}

func parseCreateUserRequest(r *http.Request, limit int64) (usermanagement.CreateUserInput, int) {
	var input usermanagement.CreateUserInput
	status := parseProvisioningObject(r, limit, map[string]any{
		"full_name": &input.FullName,
		"email":     &input.Email,
		"password":  &input.Password,
	}, []string{"full_name", "email", "password"})
	if status != 0 {
		return input, status
	}
	if err := usermanagement.ValidateCreateUserInput(&input); err != nil {
		return input, http.StatusBadRequest
	}
	return input, 0
}

func parseUpdateUserRequest(r *http.Request, limit int64) (usermanagement.UpdateUserInput, int) {
	var input usermanagement.UpdateUserInput
	var rawFullName *string
	var rawRole *string

	entries, status := readStrictJSONObject(r, limit)
	if status != 0 {
		return input, status
	}

	for k, v := range entries {
		switch k {
		case "full_name":
			if string(v) == "null" {
				return input, http.StatusBadRequest
			}
			if err := json.Unmarshal(v, &rawFullName); err != nil {
				return input, http.StatusBadRequest
			}
			input.FullName = rawFullName
		case "platform_role":
			if string(v) == "null" {
				return input, http.StatusBadRequest
			}
			if err := json.Unmarshal(v, &rawRole); err != nil {
				return input, http.StatusBadRequest
			}
			input.PlatformRole = rawRole
		default:
			return input, http.StatusBadRequest
		}
	}

	if err := usermanagement.ValidateUpdateUserInput(input); err != nil {
		return input, http.StatusBadRequest
	}
	return input, 0
}

func parseAssignGatewayUserRequest(r *http.Request, limit int64) (string, int) {
	var role string
	status := parseProvisioningObject(r, limit, map[string]any{
		"role": &role,
	}, []string{"role"})
	return role, status
}

func parseTransferGatewayOwnerRequest(r *http.Request, limit int64) (uuid.UUID, int) {
	var rawUser string
	status := parseProvisioningObject(r, limit, map[string]any{
		"user_id": &rawUser,
	}, []string{"user_id"})
	if status != 0 {
		return uuid.Nil, status
	}
	parsed, err := uuid.Parse(rawUser)
	if err != nil || parsed == uuid.Nil {
		return uuid.Nil, http.StatusBadRequest
	}
	return parsed, 0
}

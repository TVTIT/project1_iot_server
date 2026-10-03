package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
)

// GatewayProvisioner exposes the atomic Gateway provisioning use case.
type GatewayProvisioner interface {
	ProvisionGateway(context.Context, uuid.UUID, gateway.ProvisionGatewayInput) (gateway.ProvisionGatewayResult, error)
}

func provisionGateway(provisioner GatewayProvisioner, limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		p, ok := auth.PrincipalFrom(c)
		if !ok || p.UserID == uuid.Nil {
			httpapi.WriteError(c, 401, "unauthorized", "authentication required")
			return
		}
		input, status := parseGatewayProvisioning(c.Request, limit)
		if status != 0 {
			code, message := "invalid_request", "invalid request"
			if status == 413 {
				code, message = "payload_too_large", "payload too large"
			}
			if status == 415 {
				code, message = "unsupported_media_type", "unsupported media type"
			}
			httpapi.WriteError(c, status, code, message)
			return
		}
		input.GatewayID = c.Param("gateway_id")
		result, err := provisioner.ProvisionGateway(c.Request.Context(), p.UserID, input)
		if err != nil {
			status, code, message := 500, "internal_error", "internal server error"
			switch {
			case errors.Is(err, gateway.ErrInvalidProvisioningInput), errors.Is(err, gateway.ErrInvalidGatewayID):
				status, code, message = 400, "invalid_request", "invalid request"
			case errors.Is(err, gateway.ErrProvisioningNotFound):
				status, code, message = 404, "not_found", "resource not found"
			case errors.Is(err, gateway.ErrProvisioningConflict):
				status, code, message = 409, "conflict", "resource conflict"
			case errors.Is(err, gateway.ErrProvisioningForbidden):
				status, code, message = 403, "forbidden", "access denied"
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.Is(err, gateway.ErrProvisioningAdminLookup):
				status, code, message = 503, "service_unavailable", "service unavailable"
			}
			slog.ErrorContext(c.Request.Context(), "gateway provisioning failed", "request_id", httpapi.RequestIDFrom(c), "error_code", code)
			httpapi.WriteError(c, status, code, message)
			return
		}
		var stamp *time.Time
		if result.CreatedAt != nil {
			utc := result.CreatedAt.UTC()
			stamp = &utc
		}
		status = http.StatusOK
		if result.Created {
			status = http.StatusCreated
		}
		c.JSON(status, struct {
			GatewayID   string     `json:"gateway_id"`
			Name        string     `json:"name"`
			Description *string    `json:"description"`
			OwnerUserID uuid.UUID  `json:"owner_user_id"`
			EntityID    string     `json:"entity_id"`
			CreatedAt   *time.Time `json:"created_at"`
		}{result.GatewayID, result.Name, result.Description, result.OwnerUserID, result.EntityID, stamp})
	}
}

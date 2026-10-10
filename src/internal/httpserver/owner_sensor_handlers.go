package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
)

// isOwnerSensorRoute matches exact path /v1/gateways/:gateway_id/sensors/:sensor_id
func isOwnerSensorRoute(path string) bool {
	// Normalize path by stripping trailing slash
	p := strings.TrimSuffix(path, "/")
	if !strings.HasPrefix(p, "/v1/gateways/") {
		return false
	}
	parts := strings.Split(p, "/")
	// Expected parts: ["", "v1", "gateways", <gateway_id>, "sensors", <sensor_id>]
	return len(parts) == 6 && parts[4] == "sensors" && parts[3] != "" && parts[5] != ""
}

// ownerSensorCacheMiddleware ensures Cache-Control: no-store and Pragma: no-cache
// for owner Sensor provisioning routes, including preflight, CORS errors, auth errors, and 405.
func ownerSensorCacheMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if isOwnerSensorRoute(c.Request.URL.Path) {
			c.Header("Cache-Control", "no-store")
			c.Header("Pragma", "no-cache")
		}
		c.Next()
	}
}

// OwnerSensorProvisioner exposes the owner-specific atomic Sensor provisioning use case.
type OwnerSensorProvisioner interface {
	ProvisionSensorForOwner(context.Context, uuid.UUID, gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error)
}

func provisionSensorForOwner(provisioner OwnerSensorProvisioner, limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		input, status := parseSensorProvisioning(c.Request, limit)
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
		input.GatewayID, input.SensorID = c.Param("gateway_id"), c.Param("sensor_id")

		err := gateway.ValidateProvisionSensorInput(principal.UserID, input)
		var result gateway.ProvisionSensorResult
		if err == nil {
			result, err = provisioner.ProvisionSensorForOwner(c.Request.Context(), principal.UserID, input)
		}
		if err != nil {
			status, code, message := 500, "internal_error", "internal server error"
			switch {
			case errors.Is(err, gateway.ErrInvalidProvisioningInput), errors.Is(err, gateway.ErrInvalidGatewayID), errors.Is(err, gateway.ErrInvalidSensorID):
				status, code, message = 400, "invalid_request", "invalid request"
			case errors.Is(err, gateway.ErrProvisioningNotFound):
				status, code, message = 404, "not_found", "resource not found"
			case errors.Is(err, gateway.ErrProvisioningConflict):
				status, code, message = 409, "conflict", "resource conflict"
			case errors.Is(err, gateway.ErrProvisioningForbidden):
				status, code, message = 403, "forbidden", "access denied"
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.Is(err, gateway.ErrProvisioningAdminLookup), errors.Is(err, gateway.ErrOwnerProvisioningUnavailable):
				status, code, message = 503, "service_unavailable", "service unavailable"
			}
			slog.ErrorContext(c.Request.Context(), "owner sensor provisioning failed", "request_id", httpapi.RequestIDFrom(c), "error_code", code)
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
			GatewayID string     `json:"gateway_id"`
			SensorID  string     `json:"sensor_id"`
			Name      string     `json:"name"`
			Unit      *string    `json:"unit"`
			EntityID  string     `json:"entity_id"`
			CreatedAt *time.Time `json:"created_at"`
		}{result.GatewayID, result.SensorID, result.Name, result.Unit, result.EntityID, stamp})
	}
}

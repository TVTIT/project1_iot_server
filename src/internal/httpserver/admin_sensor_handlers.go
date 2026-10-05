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

// SensorProvisioner exposes the atomic Sensor provisioning use case.
type SensorProvisioner interface {
	ProvisionSensor(context.Context, uuid.UUID, gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error)
}

func provisionSensor(provisioner SensorProvisioner, limit int64) gin.HandlerFunc {
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
		// Validate before calling the service as well, keeping the HTTP contract
		// independent of an injected implementation. No metadata normalization.
		err := gateway.ValidateProvisionSensorInput(principal.UserID, input)
		var result gateway.ProvisionSensorResult
		if err == nil {
			result, err = provisioner.ProvisionSensor(c.Request.Context(), principal.UserID, input)
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
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.Is(err, gateway.ErrProvisioningAdminLookup):
				status, code, message = 503, "service_unavailable", "service unavailable"
			}
			slog.ErrorContext(c.Request.Context(), "sensor provisioning failed", "request_id", httpapi.RequestIDFrom(c), "error_code", code)
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

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

// SensorReader reads sensors only through their parent Gateway membership.
type SensorReader interface {
	ListSensorsForUserAndGateway(context.Context, uuid.UUID, string) ([]gateway.Sensor, error)
}

type sensorDTO struct {
	SensorID  string     `json:"sensor_id"`
	Name      string     `json:"name"`
	Unit      *string    `json:"unit"`
	CreatedAt *time.Time `json:"created_at"`
}

func listSensors(reader SensorReader) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		gatewayID := c.Param("gateway_id")
		if err := gateway.ValidateGatewayID(gatewayID); err != nil {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}
		if _, present := c.Request.URL.Query()["user_id"]; present {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}
		items, err := reader.ListSensorsForUserAndGateway(c.Request.Context(), principal.UserID, gatewayID)
		if err != nil {
			if errors.Is(err, gateway.ErrNotFound) {
				httpapi.WriteError(c, http.StatusNotFound, "not_found", "resource not found")
				return
			}
			category := "database_error"
			status := http.StatusInternalServerError
			code, message := "internal_error", "internal server error"
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				status = http.StatusServiceUnavailable
				code, message = "service_unavailable", "service unavailable"
				category = "timeout"
				if errors.Is(err, context.Canceled) {
					category = "cancelled"
				}
			}
			slog.ErrorContext(c.Request.Context(), "sensor list failed", "request_id", httpapi.RequestIDFrom(c), "operation", "list_sensors", "error_code", category)
			httpapi.WriteError(c, status, code, message)
			return
		}
		response := struct {
			GatewayID string      `json:"gateway_id"`
			Items     []sensorDTO `json:"items"`
		}{GatewayID: gatewayID, Items: make([]sensorDTO, 0, len(items))}
		for _, item := range items {
			var stamp *time.Time
			if item.CreatedAt != nil {
				utc := item.CreatedAt.UTC()
				stamp = &utc
			}
			response.Items = append(response.Items, sensorDTO{SensorID: item.SensorID, Name: item.Name, Unit: item.Unit, CreatedAt: stamp})
		}
		c.JSON(http.StatusOK, response)
	}
}

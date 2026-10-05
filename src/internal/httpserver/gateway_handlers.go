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

// GatewayReader is the read use-case required by the Gateway list handler.
type GatewayReader interface {
	ListGatewaysForUser(context.Context, uuid.UUID) ([]gateway.Gateway, error)
}

type gatewayDTO struct {
	GatewayID   string       `json:"gateway_id"`
	Name        string       `json:"name"`
	Description *string      `json:"description"`
	Role        gateway.Role `json:"role"`
	CreatedAt   *time.Time   `json:"created_at"`
}

func listGateways(reader GatewayReader) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		p, ok := auth.PrincipalFrom(c)
		if !ok || p.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if _, present := c.Request.URL.Query()["user_id"]; present {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}
		items, err := reader.ListGatewaysForUser(c.Request.Context(), p.UserID)
		if err != nil {
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
			slog.ErrorContext(c.Request.Context(), "gateway list failed", "request_id", httpapi.RequestIDFrom(c), "operation", "list_gateways", "error_code", category)
			httpapi.WriteError(c, status, code, message)
			return
		}
		response := struct {
			Items []gatewayDTO `json:"items"`
		}{Items: make([]gatewayDTO, 0, len(items))}
		for _, item := range items {
			var stamp *time.Time
			if item.CreatedAt != nil {
				utc := item.CreatedAt.UTC()
				stamp = &utc
			}
			response.Items = append(response.Items, gatewayDTO{GatewayID: item.GatewayID, Name: item.Name, Description: item.Description, Role: item.Role, CreatedAt: stamp})
		}
		c.JSON(http.StatusOK, response)
	}
}

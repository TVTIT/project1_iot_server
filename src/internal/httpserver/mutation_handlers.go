package httpserver

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/httpapi"
)

// EntityCreator is the narrow application use case required for creating Digital Twin entities.
type EntityCreator interface {
	CreateEntity(ctx context.Context, actor uuid.UUID, input digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error)
}

// GatewayPatcher is the narrow application use case required for patching Gateway metadata.
type GatewayPatcher interface {
	PatchGateway(ctx context.Context, actor uuid.UUID, gatewayID string, input digitaltwin.PatchGatewayInput) (digitaltwin.PatchGatewayResult, error)
}

// SensorPatcher is the narrow application use case required for patching Sensor metadata.
type SensorPatcher interface {
	PatchSensor(ctx context.Context, actor uuid.UUID, gatewayID string, sensorID string, input digitaltwin.PatchSensorInput) (digitaltwin.PatchSensorResult, error)
}

// postDigitalTwin handles POST /v1/digital-twins.
func postDigitalTwin(creator EntityCreator, limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Header("Pragma", "no-cache")

		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}

		if !negotiateAccept(c, mimeJSON) {
			return
		}

		if creator == nil {
			writeMutationError(c, "post_digital_twin", digitaltwin.ErrDependencyUnavailable)
			return
		}

		req, status := parsePostEntityRequest(c.Request, limit)
		if status != 0 {
			code, message := "invalid_request", "invalid request"
			switch status {
			case http.StatusRequestEntityTooLarge:
				code, message = "payload_too_large", "payload too large"
			case http.StatusUnsupportedMediaType:
				code, message = "unsupported_media_type", "unsupported media type"
			}
			httpapi.WriteError(c, status, code, message)
			return
		}

		res, err := creator.CreateEntity(c.Request.Context(), principal.UserID, req.ToDomain())
		if err != nil {
			writeMutationError(c, "post_digital_twin", err)
			return
		}

		resStatus, body, err := NewCreateEntityResponseDTO(res)
		if err != nil {
			writeMutationError(c, "post_digital_twin_projection", digitaltwin.ErrUnexpected)
			return
		}

		if resStatus == http.StatusCreated {
			parsedURN, parseErr := digitaltwin.ParseEntityURN(res.EntityID)
			if parseErr != nil || parsedURN.URN == "" {
				writeMutationError(c, "post_digital_twin_location", digitaltwin.ErrUnexpected)
				return
			}
			c.Header("Location", "/v1/digital-twins/"+url.PathEscape(parsedURN.URN))
		}

		writeReadJSONResponse(c, "post_digital_twin", resStatus, body)
	}
}

// patchDigitalTwin handles PATCH /v1/digital-twins/{entity_id}.
func patchDigitalTwin(gwPatcher GatewayPatcher, sensorPatcher SensorPatcher, limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Header("Pragma", "no-cache")

		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}

		if !negotiateAccept(c, mimeJSON) {
			return
		}

		if gwPatcher == nil || sensorPatcher == nil {
			writeMutationError(c, "patch_digital_twin", digitaltwin.ErrDependencyUnavailable)
			return
		}

		entityID := c.Param("entity_id")
		req, status := parsePatchEntityRequest(c.Request, entityID, limit)
		if status != 0 {
			code, message := "invalid_request", "invalid request"
			switch status {
			case http.StatusRequestEntityTooLarge:
				code, message = "payload_too_large", "payload too large"
			case http.StatusUnsupportedMediaType:
				code, message = "unsupported_media_type", "unsupported media type"
			}
			httpapi.WriteError(c, status, code, message)
			return
		}

		var dto any
		switch req.Target.Type {
		case digitaltwin.EntityTypeGateway:
			if req.Gateway == nil {
				writeMutationError(c, "patch_digital_twin", digitaltwin.ErrInvalidInput)
				return
			}
			gwRes, err := gwPatcher.PatchGateway(c.Request.Context(), principal.UserID, req.Target.GatewayID, *req.Gateway)
			if err != nil {
				writeMutationError(c, "patch_digital_twin", err)
				return
			}
			dto, err = NewPatchEntityResponseDTO(digitaltwin.EntityTypeGateway, &gwRes, nil)
			if err != nil {
				writeMutationError(c, "patch_gateway_projection", digitaltwin.ErrUnexpected)
				return
			}

		case digitaltwin.EntityTypeSensor:
			if req.Sensor == nil {
				writeMutationError(c, "patch_digital_twin", digitaltwin.ErrInvalidInput)
				return
			}
			sensorRes, err := sensorPatcher.PatchSensor(c.Request.Context(), principal.UserID, req.Target.GatewayID, req.Target.SensorID, *req.Sensor)
			if err != nil {
				writeMutationError(c, "patch_digital_twin", err)
				return
			}
			dto, err = NewPatchEntityResponseDTO(digitaltwin.EntityTypeSensor, nil, &sensorRes)
			if err != nil {
				writeMutationError(c, "patch_sensor_projection", digitaltwin.ErrUnexpected)
				return
			}

		default:
			writeMutationError(c, "patch_digital_twin", digitaltwin.ErrInvalidInput)
			return
		}

		writeReadJSONResponse(c, "patch_digital_twin", http.StatusOK, dto)
	}
}

// writeMutationError maps mutation domain, infrastructure, and validation errors
// to standard HTTP responses using ClassifyMutationError.
func writeMutationError(c *gin.Context, operation string, err error) {
	if err == nil {
		return
	}
	status, code, message := ClassifyMutationError(err)
	reqID := httpapi.RequestIDFrom(c)
	if status >= 500 {
		slog.ErrorContext(c.Request.Context(), "mutation operation failed",
			"request_id", reqID,
			"operation", operation,
			"status", status,
			"error_code", code,
		)
	} else {
		slog.WarnContext(c.Request.Context(), "mutation operation rejected",
			"request_id", reqID,
			"operation", operation,
			"status", status,
			"error_code", code,
		)
	}
	httpapi.WriteError(c, status, code, message)
}

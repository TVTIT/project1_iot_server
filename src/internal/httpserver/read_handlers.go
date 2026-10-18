package httpserver

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/httpapi"
	"iot-platform/internal/telemetry"
)

// TelemetryHistoryReader is the narrow use-case required for telemetry history reads.
type TelemetryHistoryReader interface {
	GetTelemetryHistory(ctx context.Context, actor uuid.UUID, q telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error)
}

// TwinListReader is the narrow use-case required for Twin list reads.
type TwinListReader interface {
	ListTwins(ctx context.Context, actor uuid.UUID, q digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error)
}

// TwinDetailReader is the narrow use-case required for Twin detail reads.
type TwinDetailReader interface {
	GetTwinDetail(ctx context.Context, actor uuid.UUID, q digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error)
}

// TwinStateReader is the narrow use-case required for Twin state reads.
type TwinStateReader interface {
	GetTwinState(ctx context.Context, actor uuid.UUID, q digitaltwin.TwinStateQuery) (*digitaltwin.TwinStateResult, error)
}

// TwinHistoryReader is the narrow use-case required for Twin temporal/delegated history reads.
type TwinHistoryReader interface {
	GetTwinHistory(ctx context.Context, actor uuid.UUID, q digitaltwin.TwinHistoryQuery) (*digitaltwin.TwinHistoryResult, error)
}

// TelemetryHistoryQueryParser is the parser dependency for GET /v1/telemetry/history.
type TelemetryHistoryQueryParser interface {
	ParseTelemetryHistoryQuery(rawQuery string) (*telemetry.TelemetryHistoryQuery, error)
}

// TwinListQueryParser is the parser dependency for GET /v1/digital-twins.
type TwinListQueryParser interface {
	ParseTwinListQuery(rawQuery string) (*digitaltwin.TwinListQuery, error)
}

// TwinDetailQueryParser is the parser dependency for GET /v1/digital-twins/{entity_id}.
type TwinDetailQueryParser interface {
	ParseTwinDetailQuery(rawQuery string, entityID string) (*digitaltwin.TwinDetailQuery, error)
}

// TwinHistoryQueryParser is the parser dependency for GET /v1/digital-twins/{entity_id}/history.
type TwinHistoryQueryParser interface {
	ParseTwinHistoryQuery(rawQuery string, entityID string) (*digitaltwin.TwinHistoryQuery, error)
}

// getTelemetryHistory handles GET /v1/telemetry/history.
func getTelemetryHistory(reader TelemetryHistoryReader, parser TelemetryHistoryQueryParser) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if parser == nil || reader == nil {
			writeReadError(c, "get_telemetry_history", digitaltwin.ErrDependencyUnavailable)
			return
		}
		q, err := parser.ParseTelemetryHistoryQuery(c.Request.URL.RawQuery)
		if err != nil {
			writeReadError(c, "get_telemetry_history", err)
			return
		}
		res, err := reader.GetTelemetryHistory(c.Request.Context(), principal.UserID, *q)
		if err != nil {
			writeReadError(c, "get_telemetry_history", err)
			return
		}
		if res == nil {
			writeReadError(c, "get_telemetry_history", digitaltwin.ErrUnexpected)
			return
		}
		dto, err := NewTelemetryHistoryResponseDTO(res)
		if err != nil {
			slog.ErrorContext(c.Request.Context(), "telemetry history dto projection failed",
				"request_id", httpapi.RequestIDFrom(c),
				"error", err.Error(),
			)
			httpapi.WriteError(c, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		writeReadJSONResponse(c, "get_telemetry_history", http.StatusOK, dto)
	}
}

// listDigitalTwins handles GET /v1/digital-twins.
func listDigitalTwins(reader TwinListReader, parser TwinListQueryParser) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if parser == nil || reader == nil {
			writeReadError(c, "list_digital_twins", digitaltwin.ErrDependencyUnavailable)
			return
		}
		q, err := parser.ParseTwinListQuery(c.Request.URL.RawQuery)
		if err != nil {
			writeReadError(c, "list_digital_twins", err)
			return
		}
		res, err := reader.ListTwins(c.Request.Context(), principal.UserID, *q)
		if err != nil {
			writeReadError(c, "list_digital_twins", err)
			return
		}
		if res == nil {
			writeReadError(c, "list_digital_twins", digitaltwin.ErrUnexpected)
			return
		}
		dto, err := NewTwinListResponseDTO(res)
		if err != nil {
			slog.ErrorContext(c.Request.Context(), "twin list dto projection failed",
				"request_id", httpapi.RequestIDFrom(c),
				"error", err.Error(),
			)
			httpapi.WriteError(c, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		writeReadJSONResponse(c, "list_digital_twins", http.StatusOK, dto)
	}
}

// getDigitalTwinDetail handles GET /v1/digital-twins/{entity_id}.
func getDigitalTwinDetail(reader TwinDetailReader, parser TwinDetailQueryParser) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if parser == nil || reader == nil {
			writeReadError(c, "get_digital_twin_detail", digitaltwin.ErrDependencyUnavailable)
			return
		}
		entityID := c.Param("entity_id")
		q, err := parser.ParseTwinDetailQuery(c.Request.URL.RawQuery, entityID)
		if err != nil {
			writeReadError(c, "get_digital_twin_detail", err)
			return
		}
		res, err := reader.GetTwinDetail(c.Request.Context(), principal.UserID, *q)
		if err != nil {
			writeReadError(c, "get_digital_twin_detail", err)
			return
		}
		if res == nil {
			writeReadError(c, "get_digital_twin_detail", digitaltwin.ErrUnexpected)
			return
		}
		dto, err := NewTwinDetailResponseDTO(res)
		if err != nil {
			slog.ErrorContext(c.Request.Context(), "twin detail dto projection failed",
				"request_id", httpapi.RequestIDFrom(c),
				"error", err.Error(),
			)
			httpapi.WriteError(c, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		writeReadJSONLDResponse(c, "get_digital_twin_detail", http.StatusOK, dto)
	}
}

// getDigitalTwinState handles GET /v1/digital-twins/{entity_id}/state.
func getDigitalTwinState(reader TwinStateReader) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if reader == nil {
			writeReadError(c, "get_digital_twin_state", digitaltwin.ErrDependencyUnavailable)
			return
		}
		entityID := c.Param("entity_id")
		q, err := ParseTwinStateQuery(c.Request.URL.RawQuery, entityID)
		if err != nil {
			writeReadError(c, "get_digital_twin_state", err)
			return
		}
		res, err := reader.GetTwinState(c.Request.Context(), principal.UserID, *q)
		if err != nil {
			writeReadError(c, "get_digital_twin_state", err)
			return
		}
		if res == nil {
			writeReadError(c, "get_digital_twin_state", digitaltwin.ErrUnexpected)
			return
		}
		dto, err := NewTwinStateResponseDTO(res)
		if err != nil {
			slog.ErrorContext(c.Request.Context(), "twin state dto projection failed",
				"request_id", httpapi.RequestIDFrom(c),
				"error", err.Error(),
			)
			httpapi.WriteError(c, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		writeReadJSONResponse(c, "get_digital_twin_state", http.StatusOK, dto)
	}
}

// getDigitalTwinHistory handles GET /v1/digital-twins/{entity_id}/history.
func getDigitalTwinHistory(reader TwinHistoryReader, parser TwinHistoryQueryParser) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if parser == nil || reader == nil {
			writeReadError(c, "get_digital_twin_history", digitaltwin.ErrDependencyUnavailable)
			return
		}
		entityID := c.Param("entity_id")
		q, err := parser.ParseTwinHistoryQuery(c.Request.URL.RawQuery, entityID)
		if err != nil {
			writeReadError(c, "get_digital_twin_history", err)
			return
		}
		res, err := reader.GetTwinHistory(c.Request.Context(), principal.UserID, *q)
		if err != nil {
			writeReadError(c, "get_digital_twin_history", err)
			return
		}
		if res == nil {
			writeReadError(c, "get_digital_twin_history", digitaltwin.ErrUnexpected)
			return
		}
		dto, err := NewTwinHistoryResponseDTO(res)
		if err != nil {
			slog.ErrorContext(c.Request.Context(), "twin history dto projection failed",
				"request_id", httpapi.RequestIDFrom(c),
				"error", err.Error(),
			)
			httpapi.WriteError(c, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		writeReadJSONResponse(c, "get_digital_twin_history", http.StatusOK, dto)
	}
}

package httpserver

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

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
	ParseTwinListQuery(rawQuery string) (*digitaltwin.TwinListQuery, *TwinListPresentationOptions, error)
}

// TwinDetailQueryParser is the parser dependency for GET /v1/digital-twins/{entity_id}.
type TwinDetailQueryParser interface {
	ParseTwinDetailQuery(rawQuery string, entityID string) (*digitaltwin.TwinDetailQuery, error)
	ParseDetailFormat(rawQuery string) (string, error)
	QueryTimeout() time.Duration
}

// TwinHistoryQueryParser is the parser dependency for GET /v1/digital-twins/{entity_id}/history.
type TwinHistoryQueryParser interface {
	ParseTwinHistoryQuery(rawQuery string, entityID string) (*digitaltwin.TwinHistoryQuery, error)
}

// TemporalHistoryQueryParser is the parser dependency for GET /v1/digital-twins/{entity_id}/temporal.
type TemporalHistoryQueryParser interface {
	ParseTemporalHistoryQuery(rawQuery string, entityID string) (*digitaltwin.TwinHistoryQuery, *TemporalPresentationOptions, error)
}

// getDigitalTwinTemporal handles GET /v1/digital-twins/{entity_id}/temporal.
func getDigitalTwinTemporal(reader TwinHistoryReader, parser TemporalHistoryQueryParser) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if !negotiateAccept(c, mimeJSONLD) {
			return
		}
		if parser == nil || reader == nil {
			writeReadError(c, "get_digital_twin_temporal", digitaltwin.ErrDependencyUnavailable)
			return
		}
		entityID := c.Param("entity_id")
		q, pres, err := parser.ParseTemporalHistoryQuery(c.Request.URL.RawQuery, entityID)
		if err != nil {
			writeReadError(c, "get_digital_twin_temporal", err)
			return
		}
		res, err := reader.GetTwinHistory(c.Request.Context(), principal.UserID, *q)
		if err != nil {
			writeReadError(c, "get_digital_twin_temporal", err)
			return
		}
		if res == nil {
			writeReadError(c, "get_digital_twin_temporal", digitaltwin.ErrUnexpected)
			return
		}
		dto, err := NewTemporalHistoryResponseDTO(res, pres)
		if err != nil {
			slog.ErrorContext(c.Request.Context(), "temporal history dto projection failed",
				"request_id", httpapi.RequestIDFrom(c),
				"error", err.Error(),
			)
			httpapi.WriteError(c, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		writeReadJSONLDResponse(c, "get_digital_twin_temporal", http.StatusOK, dto)
	}
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
		// Parse the HTTP-only presentation choice before selecting the served media
		// type: the normalized list is JSON-LD, while format=simplified is plain JSON.
		q, pres, err := parser.ParseTwinListQuery(c.Request.URL.RawQuery)
		if err != nil {
			writeReadError(c, "list_digital_twins", err)
			return
		}
		simplified := pres != nil && pres.Format == "simplified"
		format := ""
		if pres != nil {
			format = pres.Format
		}
		targetMime := mimeJSONLD
		if simplified {
			targetMime = mimeJSON
		}
		if !negotiateAccept(c, targetMime) {
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

		if simplified {
			dto, err := NewTwinSimplifiedListResponseDTO(res)
			if err != nil {
				slog.ErrorContext(c.Request.Context(), "twin simplified list dto projection failed",
					"request_id", httpapi.RequestIDFrom(c),
					"error", err.Error(),
				)
				httpapi.WriteError(c, http.StatusInternalServerError, "internal_error", "internal server error")
				return
			}
			// Emit the relative next link only after the projection succeeded, so a
			// failed projection never leaves a partial-success Link header.
			if res.NextCursor != nil {
				c.Header("Link", formatTwinListNextLink(*q, *res.NextCursor, format))
			}
			writeReadJSONResponse(c, "list_digital_twins", http.StatusOK, dto)
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
		if res.NextCursor != nil {
			c.Header("Link", formatTwinListNextLink(*q, *res.NextCursor, format))
		}
		writeReadJSONLDResponse(c, "list_digital_twins", http.StatusOK, dto)
	}
}

// formatTwinListNextLink builds an RFC 8288 Link header value preserving active query filters.
// The header is always a relative reference (never derived from Host/Forwarded) so proxy
// headers cannot rewrite it; format is emitted only for the simplified presentation.
func formatTwinListNextLink(q digitaltwin.TwinListQuery, nextCursor, format string) string {
	var parts []string
	if q.EntityType != nil {
		parts = append(parts, "type="+url.QueryEscape(string(*q.EntityType)))
	}
	if q.GatewayID != nil {
		parts = append(parts, "gateway_id="+url.QueryEscape(*q.GatewayID))
	}
	parts = append(parts, "limit="+strconv.Itoa(q.Limit))
	if format == "simplified" {
		parts = append(parts, "format="+url.QueryEscape(format))
	}
	parts = append(parts, "cursor="+url.QueryEscape(nextCursor))
	return fmt.Sprintf("</v1/digital-twins?%s>; rel=\"next\"", strings.Join(parts, "&"))
}

// getDigitalTwinDetail handles GET /v1/digital-twins/{entity_id}.
func getDigitalTwinDetail(reader TwinDetailReader, stateReader TwinStateReader, parser TwinDetailQueryParser) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		targetMime := mimeJSONLD
		if c.Query("format") == "simplified" {
			targetMime = mimeJSON
		}
		if !negotiateAccept(c, targetMime) {
			return
		}
		if parser == nil || reader == nil || stateReader == nil {
			writeReadError(c, "get_digital_twin_detail", digitaltwin.ErrDependencyUnavailable)
			return
		}

		format, err := parser.ParseDetailFormat(c.Request.URL.RawQuery)
		if err != nil {
			writeReadError(c, "get_digital_twin_detail", err)
			return
		}

		timeout := digitaltwin.ContractDefaultQueryTimeout
		if parser.QueryTimeout() > 0 {
			timeout = parser.QueryTimeout()
		}

		ctx := c.Request.Context()
		var cancel context.CancelFunc
		if deadline, ok := ctx.Deadline(); ok {
			if time.Until(deadline) > timeout {
				ctx, cancel = context.WithTimeout(ctx, timeout)
			} else {
				ctx, cancel = context.WithCancel(ctx)
			}
		} else {
			ctx, cancel = context.WithTimeout(ctx, timeout)
		}
		defer cancel()

		entityID := c.Param("entity_id")

		if format == "simplified" {
			if _, err := digitaltwin.ParseEntityURN(entityID); err != nil {
				writeReadError(c, "get_digital_twin_detail", &QueryError{
					Code:    "invalid_request",
					Message: "invalid entity_id",
					Err:     digitaltwin.ErrInvalidInput,
				})
				return
			}

			res, err := stateReader.GetTwinState(ctx, principal.UserID, digitaltwin.TwinStateQuery{EntityID: entityID})
			if err != nil {
				writeReadError(c, "get_digital_twin_detail", err)
				return
			}
			if res == nil {
				writeReadError(c, "get_digital_twin_detail", digitaltwin.ErrUnexpected)
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
			writeReadJSONResponse(c, "get_digital_twin_detail", http.StatusOK, dto)
			return
		}

		q, err := parser.ParseTwinDetailQuery(c.Request.URL.RawQuery, entityID)
		if err != nil {
			writeReadError(c, "get_digital_twin_detail", err)
			return
		}

		res, err := reader.GetTwinDetail(ctx, principal.UserID, *q)
		if err != nil {
			writeReadError(c, "get_digital_twin_detail", err)
			return
		}
		if res == nil {
			writeReadError(c, "get_digital_twin_detail", digitaltwin.ErrUnexpected)
			return
		}

		var stateRes *digitaltwin.TwinStateResult
		if res.Type == digitaltwin.EntityTypeSensor {
			var err error
			stateRes, err = stateReader.GetTwinState(ctx, principal.UserID, digitaltwin.TwinStateQuery{EntityID: q.EntityID})
			if err != nil {
				writeReadError(c, "get_digital_twin_detail", err)
				return
			}
			if stateRes == nil {
				writeReadError(c, "get_digital_twin_detail", digitaltwin.ErrUnexpected)
				return
			}
		}

		dto, err := NewTwinDetailResponseDTO(res, stateRes)
		if err != nil {
			slog.ErrorContext(c.Request.Context(), "twin detail dto projection failed",
				"request_id", httpapi.RequestIDFrom(c),
				"error", err.Error(),
			)
			httpapi.WriteError(c, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}

		if res.Type == digitaltwin.EntityTypeGateway && res.RelationshipPage != nil && res.RelationshipPage.NextCursor != nil {
			c.Header("Link", formatTwinDetailNextLink(q.EntityID, q.RelationshipLimit, *res.RelationshipPage.NextCursor))
		}

		writeReadJSONLDResponse(c, "get_digital_twin_detail", http.StatusOK, dto)
	}
}

// formatTwinDetailNextLink builds an RFC 8288 Link header for Gateway detail relationship pagination.
func formatTwinDetailNextLink(entityID string, limit int, nextCursor string) string {
	return fmt.Sprintf("</v1/digital-twins/%s?relationship_limit=%d&relationship_after=%s>; rel=\"next\"",
		url.PathEscape(entityID), limit, url.QueryEscape(nextCursor))
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

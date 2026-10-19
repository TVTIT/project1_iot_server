package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
	"iot-platform/internal/telemetry"
)

// writeReadError maps domain, query parser, and dependency errors to safe HTTP responses
// conforming to Stage 4 contract section 11.5 and AGENTS.md rules.
func writeReadError(c *gin.Context, operation string, err error) {
	if err == nil {
		return
	}

	reqID := httpapi.RequestIDFrom(c)

	// 503 Service Unavailable: Check dependency, timeout, cancellation, or commit uncertainty first.
	// This prevents transient or connection errors from being masked as 404 or 400.
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, digitaltwin.ErrDependencyUnavailable) ||
		errors.Is(err, telemetry.ErrHistoryDependency) ||
		errors.Is(err, gateway.ErrGatewayProvisioningUnavailable) ||
		errors.Is(err, gateway.ErrAdminOrOwnerProvisioningUnavailable) ||
		errors.Is(err, digitaltwin.ErrMetadataPatchUnavailable) ||
		errors.Is(err, gateway.ErrAdminLookupFailed) ||
		errors.Is(err, gateway.ErrInventoryUnavailable) {
		slog.ErrorContext(c.Request.Context(), "read operation unavailable",
			"request_id", reqID,
			"operation", operation,
			"error_code", "service_unavailable",
		)
		httpapi.WriteError(c, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
		return
	}

	// 400 Bad Request: QueryError from strict URL / parameter parsers.
	var qe *QueryError
	if errors.As(err, &qe) {
		slog.WarnContext(c.Request.Context(), "read query validation rejected",
			"request_id", reqID,
			"operation", operation,
			"error_code", qe.Code,
		)
		msg := queryErrorMessage(qe.Code)
		httpapi.WriteError(c, http.StatusBadRequest, qe.Code, msg)
		return
	}

	// 404 Not Found: Inaccessible due to tenant isolation or non-existent entity.
	if errors.Is(err, digitaltwin.ErrNotFound) ||
		errors.Is(err, telemetry.ErrHistoryNotFound) ||
		errors.Is(err, gateway.ErrNotFound) {
		slog.WarnContext(c.Request.Context(), "read resource not found",
			"request_id", reqID,
			"operation", operation,
			"error_code", "not_found",
		)
		httpapi.WriteError(c, http.StatusNotFound, "not_found", "resource not found")
		return
	}

	// 403 Forbidden: Insufficient role/capability.
	if errors.Is(err, digitaltwin.ErrForbidden) ||
		errors.Is(err, gateway.ErrProvisioningForbidden) {
		slog.WarnContext(c.Request.Context(), "read resource access forbidden",
			"request_id", reqID,
			"operation", operation,
			"error_code", "forbidden",
		)
		httpapi.WriteError(c, http.StatusForbidden, "forbidden", "access denied")
		return
	}

	// 400 Bad Request: Specific domain validation errors matching contract section 11.5 allowlist.
	switch {
	case errors.Is(err, digitaltwin.ErrInvalidCursor):
		httpapi.WriteError(c, http.StatusBadRequest, "invalid_cursor", "invalid cursor")
		return
	case errors.Is(err, digitaltwin.ErrInvalidProperty):
		httpapi.WriteError(c, http.StatusBadRequest, "invalid_property", "invalid property selector")
		return
	case errors.Is(err, digitaltwin.ErrInconsistentUnit):
		httpapi.WriteError(c, http.StatusBadRequest, "inconsistent_unit", "inconsistent unit")
		return
	case errors.Is(err, digitaltwin.ErrRangeTooLarge) || errors.Is(err, telemetry.ErrHistoryRangeTooLarge):
		httpapi.WriteError(c, http.StatusBadRequest, "range_too_large", "range too large")
		return
	case errors.Is(err, digitaltwin.ErrInvalidResolution) || errors.Is(err, telemetry.ErrHistoryInvalidResolution):
		httpapi.WriteError(c, http.StatusBadRequest, "invalid_resolution", "invalid resolution")
		return
	case errors.Is(err, digitaltwin.ErrRawLimitExceeded) || errors.Is(err, telemetry.ErrHistoryRawLimitExceeded):
		httpapi.WriteError(c, http.StatusBadRequest, "raw_limit_exceeded", "raw limit exceeded")
		return
	case errors.Is(err, digitaltwin.ErrResolutionTooFine) || errors.Is(err, telemetry.ErrHistoryResolutionTooFine):
		httpapi.WriteError(c, http.StatusBadRequest, "resolution_too_fine", "resolution too fine")
		return
	case errors.Is(err, digitaltwin.ErrInvalidInput) || errors.Is(err, telemetry.ErrHistoryInvalidInput):
		httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
		return
	}

	// 500 Internal Server Error: Unexpected system / data / graph faults.
	slog.ErrorContext(c.Request.Context(), "read operation unexpected error",
		"request_id", reqID,
		"operation", operation,
		"error_code", "internal_error",
	)
	httpapi.WriteError(c, http.StatusInternalServerError, "internal_error", "internal server error")
}

// queryErrorMessage maps allowlisted query error codes to generic safe messages.
func queryErrorMessage(code string) string {
	switch code {
	case "invalid_cursor":
		return "invalid cursor"
	case "invalid_resolution":
		return "invalid resolution"
	case "raw_limit_exceeded":
		return "raw limit exceeded"
	case "resolution_too_fine":
		return "resolution too fine"
	case "range_too_large":
		return "range too large"
	case "invalid_property":
		return "invalid property selector"
	case "inconsistent_unit":
		return "inconsistent unit"
	default:
		return "invalid request"
	}
}

// writeReadJSONResponse pre-encodes the complete DTO before writing status to guarantee
// that marshal or nil projection failures abort with 500 internal_error instead of partial 200.
func writeReadJSONResponse(c *gin.Context, operation string, status int, dto any) {
	if dto == nil {
		slog.ErrorContext(c.Request.Context(), "read handler nil dto projection",
			"request_id", httpapi.RequestIDFrom(c),
			"operation", operation,
		)
		httpapi.WriteError(c, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	data, err := json.Marshal(dto)
	if err != nil {
		slog.ErrorContext(c.Request.Context(), "read handler json marshal failed",
			"request_id", httpapi.RequestIDFrom(c),
			"operation", operation,
			"error_code", "marshal_failure",
		)
		httpapi.WriteError(c, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	c.Data(status, "application/json; charset=utf-8", data)
}

// writeReadJSONLDResponse pre-encodes the complete JSON-LD DTO before writing status.
// Used exclusively for GET /v1/digital-twins/{entity_id} detail success.
func writeReadJSONLDResponse(c *gin.Context, operation string, status int, dto any) {
	if dto == nil {
		slog.ErrorContext(c.Request.Context(), "read handler nil json-ld dto projection",
			"request_id", httpapi.RequestIDFrom(c),
			"operation", operation,
		)
		httpapi.WriteError(c, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	data, err := json.Marshal(dto)
	if err != nil {
		slog.ErrorContext(c.Request.Context(), "read handler json-ld marshal failed",
			"request_id", httpapi.RequestIDFrom(c),
			"operation", operation,
			"error_code", "marshal_failure",
		)
		httpapi.WriteError(c, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	c.Data(status, "application/ld+json; charset=utf-8", data)
}

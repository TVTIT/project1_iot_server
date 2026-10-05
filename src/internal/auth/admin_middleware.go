package auth

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"iot-platform/internal/httpapi"
)

// PlatformAdminMiddleware requires database-backed platform-admin membership.
func PlatformAdminMiddleware(checker PlatformAdminChecker, timeout time.Duration, logger *slog.Logger) gin.HandlerFunc {
	if IsNilDependency(checker) || timeout <= 0 {
		panic("invalid platform admin middleware dependencies")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return func(c *gin.Context) {
		principal, ok := PrincipalFrom(c)
		if !ok {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()
		admin, err := checker.IsPlatformAdmin(ctx, principal.UserID)
		if err == nil {
			err = ctx.Err()
		}
		// Release the lookup timer before running downstream business handlers.
		cancel()
		if err != nil {
			category := "database_error"
			if errors.Is(err, context.DeadlineExceeded) {
				category = "timeout"
			} else if errors.Is(err, context.Canceled) {
				category = "cancelled"
			}
			logger.ErrorContext(c.Request.Context(), "platform admin lookup failed", "request_id", httpapi.RequestIDFrom(c), "operation", "platform_admin_lookup", "error_code", category)
			httpapi.WriteError(c, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
			return
		}
		if !admin {
			httpapi.WriteError(c, http.StatusForbidden, "forbidden", "insufficient permissions")
			return
		}
		c.Next()
	}
}

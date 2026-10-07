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
	"iot-platform/internal/httpapi"
)

type permissionsDTO struct {
	IsPlatformAdmin bool `json:"is_platform_admin"`
}

// Runs before authentication so sensitive errors and unsupported methods on this
// endpoint cannot be cached either.
func permissionCacheMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.URL.Path == "/v1/me/permissions" || strings.TrimSuffix(c.Request.URL.Path, "/") == "/v1/me/permissions" {
			c.Header("Cache-Control", "no-store")
			c.Header("Pragma", "no-cache")
		}
		c.Next()
	}
}

func getMyPermissions(checker auth.PlatformAdminChecker, timeout time.Duration) gin.HandlerFunc {
	if auth.IsNilDependency(checker) || timeout <= 0 {
		panic("invalid permissions handler dependencies")
	}

	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Header("Pragma", "no-cache")

		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}

		if _, present := c.Request.URL.Query()["user_id"]; present {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}

		if c.Request.Body != nil && c.Request.Body != http.NoBody {
			var buf [1]byte
			n, _ := c.Request.Body.Read(buf[:])
			if n > 0 {
				httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
				return
			}
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()

		isAdmin, err := checker.IsPlatformAdmin(ctx, principal.UserID)
		if err == nil {
			err = ctx.Err()
		}
		cancel()

		if err != nil {
			category := "database_error"
			if errors.Is(err, context.DeadlineExceeded) {
				category = "timeout"
			} else if errors.Is(err, context.Canceled) {
				category = "cancelled"
			}
			slog.ErrorContext(c.Request.Context(), "platform admin lookup failed", "request_id", httpapi.RequestIDFrom(c), "operation", "get_permissions", "error_code", category)
			httpapi.WriteError(c, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
			return
		}

		c.JSON(http.StatusOK, permissionsDTO{IsPlatformAdmin: isAdmin})
	}
}

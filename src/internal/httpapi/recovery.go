package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

// RecoveryMiddleware recovers panics without logging request headers, bearer
// tokens, panic values, or stack traces.
func RecoveryMiddleware(logger *slog.Logger) gin.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}
	return func(c *gin.Context) {
		defer func() {
			if recover() == nil {
				return
			}
			logger.ErrorContext(c.Request.Context(), "HTTP handler panic recovered",
				"request_id", RequestIDFrom(c),
				"method", c.Request.Method,
				"path", c.Request.URL.Path,
			)
			if !c.Writer.Written() {
				WriteError(c, http.StatusInternalServerError, "internal_error", "internal server error")
			} else {
				c.Abort()
			}
		}()
		c.Next()
	}
}

// Package httpserver defines the backend HTTP transport and system endpoints.
package httpserver

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// ReadinessChecker verifies whether a required dependency is available.
type ReadinessChecker interface {
	Ping(context.Context) error
}

// NewRouter creates the backend HTTP routes.
func NewRouter(database ReadinessChecker, readinessTimeout time.Duration) http.Handler {
	router := gin.New()
	router.Use(gin.Recovery(), requestIDMiddleware())

	router.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})
	router.GET("/readyz", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), readinessTimeout)
		defer cancel()
		if err := database.Ping(ctx); err != nil {
			slog.WarnContext(c.Request.Context(), "readiness check failed", "error", err)
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	router.GET("/v1/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "running",
			"service": "iot-backend",
			"version": "v1",
		})
	})

	// Preserve the documented API surface until each business handler is added.
	notImplemented := func(c *gin.Context) {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "not_implemented"})
	}
	router.GET("/v1/telemetry/history", notImplemented)
	router.GET("/v1/ws", notImplemented)
	router.GET("/v1/digital-twins", notImplemented)

	return router
}

func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = uuid.NewString()
		}
		c.Set("request_id", requestID)
		c.Header("X-Request-ID", requestID)
		c.Next()
	}
}

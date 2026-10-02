// Package httpserver defines the backend HTTP transport and system endpoints.
package httpserver

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"iot-platform/internal/auth"
	"iot-platform/internal/httpapi"
)

// ReadinessChecker verifies whether a required dependency is available.
type ReadinessChecker interface {
	Ping(context.Context) error
}

// RouterDependencies contains the mandatory HTTP boundary dependencies.
type RouterDependencies struct {
	ReadinessChecker     ReadinessChecker
	ReadinessTimeout     time.Duration
	AuthorizationTimeout time.Duration
	TokenVerifier        auth.Verifier
	PlatformAdminChecker auth.PlatformAdminChecker
	GatewayReader        GatewayReader
}

// NewRouter creates the backend HTTP routes and fails closed when an
// authentication dependency is absent.
func NewRouter(deps RouterDependencies) (http.Handler, error) {
	if auth.IsNilDependency(deps.ReadinessChecker) {
		return nil, fmt.Errorf("readiness checker is required")
	}
	if deps.ReadinessTimeout <= 0 {
		return nil, fmt.Errorf("readiness timeout must be positive")
	}
	if auth.IsNilDependency(deps.TokenVerifier) {
		return nil, fmt.Errorf("token verifier is required")
	}
	if auth.IsNilDependency(deps.PlatformAdminChecker) {
		return nil, fmt.Errorf("platform admin checker is required")
	}
	if deps.AuthorizationTimeout <= 0 {
		return nil, fmt.Errorf("authorization timeout must be positive")
	}

	if auth.IsNilDependency(deps.GatewayReader) {
		return nil, fmt.Errorf("gateway reader is required")
	}
	router := gin.New()
	router.HandleMethodNotAllowed = true
	router.Use(httpapi.RequestIDMiddleware(), httpapi.RecoveryMiddleware(slog.Default()))
	router.NoRoute(func(c *gin.Context) {
		httpapi.WriteError(c, http.StatusNotFound, "not_found", "resource not found")
	})
	router.NoMethod(func(c *gin.Context) {
		httpapi.WriteError(c, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	})

	router.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})
	router.GET("/readyz", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), deps.ReadinessTimeout)
		defer cancel()
		if err := deps.ReadinessChecker.Ping(ctx); err != nil {
			slog.WarnContext(c.Request.Context(), "readiness check failed", "error", err)
			httpapi.WriteError(c, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
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
		httpapi.WriteError(c, http.StatusNotImplemented, "not_implemented", "endpoint not implemented")
	}
	groups := newRouteGroups(router, deps)
	authenticated := groups.authenticated
	authenticated.GET("/gateways", listGateways(deps.GatewayReader))
	authenticated.GET("/telemetry/history", notImplemented)
	authenticated.GET("/ws", notImplemented)
	authenticated.GET("/digital-twins", notImplemented)

	return router, nil
}

// Register all future admin handlers through groups.admin, never by rebuilding
// a group with the same URL prefix: Gin does not inherit prefix policies.
type routeGroups struct {
	authenticated *gin.RouterGroup
	admin         *gin.RouterGroup
}

func newRouteGroups(router *gin.Engine, deps RouterDependencies) routeGroups {
	authenticated := router.Group("/v1", auth.AuthenticationMiddleware(deps.TokenVerifier))
	admin := authenticated.Group("/admin", auth.PlatformAdminMiddleware(deps.PlatformAdminChecker, deps.AuthorizationTimeout, slog.Default()))
	return routeGroups{authenticated: authenticated, admin: admin}
}

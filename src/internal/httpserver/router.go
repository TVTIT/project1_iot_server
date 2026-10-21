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
	"iot-platform/internal/config"
	"iot-platform/internal/httpapi"
	"iot-platform/internal/usermanagement"
)

// ReadinessChecker verifies whether a required dependency is available.
type ReadinessChecker interface {
	Ping(context.Context) error
}

// RouterDependencies contains the mandatory HTTP boundary dependencies.
type RouterDependencies struct {
	ReadinessChecker           ReadinessChecker
	ReadinessTimeout           time.Duration
	AuthorizationTimeout       time.Duration
	TokenVerifier              auth.Verifier
	PlatformAdminChecker       auth.PlatformAdminChecker
	GatewayReader              GatewayReader
	SensorReader               SensorReader
	GatewayProvisioner         GatewayProvisioner
	SensorProvisioner          SensorProvisioner
	OwnerSensorProvisioner     OwnerSensorProvisioner
	AdminGatewayLister         AdminGatewayInventoryLister
	AdminInventoryDefaultLimit int
	AdminInventoryMaxLimit     int
	AdminMaxBodyBytes          int64
	CredentialAPIEnabled       bool
	CredentialRequestTimeout   time.Duration
	CredentialManager          CredentialManager
	CredentialStartupReadiness CredentialStartupReadiness
	CORSAllowedOrigins         []string
	TelemetryHistoryReader     TelemetryHistoryReader
	TwinListReader             TwinListReader
	TwinDetailReader           TwinDetailReader
	TwinStateReader            TwinStateReader
	TwinHistoryReader          TwinHistoryReader
	HistoryQueryParser         *HistoryQueryParser
	TwinQueryParser            *TwinQueryParser
	EntityCreator              EntityCreator
	GatewayPatcher             GatewayPatcher
	SensorPatcher              SensorPatcher
	TwinMaxBodyBytes           int64
	UserCreator                usermanagement.UserCreator
	UserLister                 usermanagement.UserLister
	UserUpdater                usermanagement.UserUpdater
	GatewayUserAssigner        usermanagement.GatewayUserAssigner
	GatewayUserRemover         usermanagement.GatewayUserRemover
	GatewayOwnerTransferer     usermanagement.GatewayOwnerTransferer
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
	if auth.IsNilDependency(deps.SensorReader) {
		return nil, fmt.Errorf("sensor reader is required")
	}
	if auth.IsNilDependency(deps.GatewayProvisioner) {
		return nil, fmt.Errorf("gateway provisioner is required")
	}
	if auth.IsNilDependency(deps.SensorProvisioner) {
		return nil, fmt.Errorf("sensor provisioner is required")
	}
	if auth.IsNilDependency(deps.OwnerSensorProvisioner) {
		return nil, fmt.Errorf("owner sensor provisioner is required")
	}
	if auth.IsNilDependency(deps.AdminGatewayLister) {
		return nil, fmt.Errorf("admin gateway lister is required")
	}
	if deps.AdminInventoryDefaultLimit < 0 {
		return nil, fmt.Errorf("admin inventory default limit must be positive")
	}
	if deps.AdminInventoryDefaultLimit == 0 {
		deps.AdminInventoryDefaultLimit = 100
	}
	if deps.AdminInventoryMaxLimit < 0 {
		return nil, fmt.Errorf("admin inventory max limit must be positive")
	}
	if deps.AdminInventoryMaxLimit == 0 {
		deps.AdminInventoryMaxLimit = 200
	}
	if deps.AdminInventoryDefaultLimit > deps.AdminInventoryMaxLimit {
		return nil, fmt.Errorf("admin inventory max limit must be at least default limit")
	}
	if deps.AdminMaxBodyBytes < 1 || deps.AdminMaxBodyBytes > 1048576 {
		return nil, fmt.Errorf("admin body limit must be between 1 and 1048576")
	}
	if deps.CredentialAPIEnabled && (auth.IsNilDependency(deps.CredentialManager) || auth.IsNilDependency(deps.CredentialStartupReadiness) || deps.CredentialRequestTimeout <= 0 || deps.CredentialRequestTimeout > 5*time.Minute) {
		return nil, fmt.Errorf("enabled credential API requires manager, startup barrier and bounded request timeout")
	}

	if auth.IsNilDependency(deps.TelemetryHistoryReader) {
		return nil, fmt.Errorf("telemetry history reader is required")
	}
	if auth.IsNilDependency(deps.TwinListReader) {
		return nil, fmt.Errorf("twin list reader is required")
	}
	if auth.IsNilDependency(deps.TwinDetailReader) {
		return nil, fmt.Errorf("twin detail reader is required")
	}
	if auth.IsNilDependency(deps.TwinStateReader) {
		return nil, fmt.Errorf("twin state reader is required")
	}
	if auth.IsNilDependency(deps.TwinHistoryReader) {
		return nil, fmt.Errorf("twin history reader is required")
	}
	if auth.IsNilDependency(deps.EntityCreator) {
		return nil, fmt.Errorf("entity creator is required")
	}
	if auth.IsNilDependency(deps.GatewayPatcher) {
		return nil, fmt.Errorf("gateway patcher is required")
	}
	if auth.IsNilDependency(deps.SensorPatcher) {
		return nil, fmt.Errorf("sensor patcher is required")
	}
	if auth.IsNilDependency(deps.HistoryQueryParser) {
		return nil, fmt.Errorf("history query parser is required")
	}
	if err := config.ValidateHistoryConfig(deps.HistoryQueryParser.cfg); err != nil {
		return nil, fmt.Errorf("invalid history query parser config: %w", err)
	}
	if auth.IsNilDependency(deps.TwinQueryParser) {
		return nil, fmt.Errorf("twin query parser is required")
	}
	if err := config.ValidateTwinConfig(deps.TwinQueryParser.cfg); err != nil {
		return nil, fmt.Errorf("invalid twin query parser config: %w", err)
	}
	if deps.TwinMaxBodyBytes < 1 || deps.TwinMaxBodyBytes > 1048576 {
		return nil, fmt.Errorf("twin body limit must be between 1 and 1048576")
	}

	router := gin.New()
	router.HandleMethodNotAllowed = true
	router.Use(
		httpapi.RequestIDMiddleware(),
		adminGatewayCacheMiddleware(),
		adminGatewayAuditMiddleware(),
		ownerSensorCacheMiddleware(),
		stage4CacheMiddleware(),
		adminUserManagementCacheMiddleware(),
		corsMiddleware(deps.CORSAllowedOrigins),
		httpapi.RecoveryMiddleware(slog.Default()),
	)
	router.Use(credentialCacheMiddleware())
	router.Use(permissionCacheMiddleware())
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
	registerCredentialRoutes(groups.admin, deps)
	groups.admin.GET("/gateways", listAdminGateways(deps.AdminGatewayLister, deps.AdminInventoryDefaultLimit, deps.AdminInventoryMaxLimit))
	groups.admin.PUT("/gateways/:gateway_id", provisionGateway(deps.GatewayProvisioner, deps.AdminMaxBodyBytes))
	groups.admin.PUT("/gateways/:gateway_id/sensors/:sensor_id", provisionSensor(deps.SensorProvisioner, deps.AdminMaxBodyBytes))
	groups.admin.POST("/users", createUserHandler(deps.UserCreator, deps.AdminMaxBodyBytes))
	groups.admin.GET("/users", listUsersHandler(deps.UserLister))
	groups.admin.PATCH("/users/:user_id", updateUserHandler(deps.UserUpdater, deps.AdminMaxBodyBytes))
	groups.admin.PUT("/gateways/:gateway_id/users/:user_id", assignGatewayUserHandler(deps.GatewayUserAssigner, deps.AdminMaxBodyBytes))
	groups.admin.DELETE("/gateways/:gateway_id/users/:user_id", removeGatewayUserHandler(deps.GatewayUserRemover))
	groups.admin.PUT("/gateways/:gateway_id/owner", transferGatewayOwnerHandler(deps.GatewayOwnerTransferer, deps.AdminMaxBodyBytes))
	authenticated := groups.authenticated
	authenticated.GET("/me/permissions", getMyPermissions(deps.PlatformAdminChecker, deps.AuthorizationTimeout))
	authenticated.GET("/gateways", listGateways(deps.GatewayReader))
	authenticated.GET("/gateways/:gateway_id/sensors", listSensors(deps.SensorReader))
	authenticated.PUT("/gateways/:gateway_id/sensors/:sensor_id", provisionSensorForOwner(deps.OwnerSensorProvisioner, deps.AdminMaxBodyBytes))
	authenticated.GET("/ws", notImplemented)
	authenticated.GET("/digital-twins", listDigitalTwins(deps.TwinListReader, deps.TwinQueryParser))
	authenticated.POST("/digital-twins", postDigitalTwin(deps.EntityCreator, deps.TwinMaxBodyBytes))
	authenticated.GET("/digital-twins/:entity_id", getDigitalTwinDetail(deps.TwinDetailReader, deps.TwinStateReader, deps.TwinQueryParser))
	authenticated.PATCH("/digital-twins/:entity_id/attrs", patchDigitalTwin(deps.GatewayPatcher, deps.SensorPatcher, deps.TwinMaxBodyBytes))
	authenticated.GET("/digital-twins/:entity_id/temporal", getDigitalTwinTemporal(deps.TwinHistoryReader, deps.HistoryQueryParser))

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

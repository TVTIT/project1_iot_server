// Command server runs the IoT backend HTTP service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/gin-gonic/gin"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/database"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpserver"
)

func main() {
	if err := run(); err != nil {
		slog.Error("backend stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadFromEnvironment()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	if cfg.ServerEnv == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// Open database connection
	pool, err := database.Open(
		rootCtx,
		cfg.DatabaseURL,
		cfg.DatabaseMaxConns,
		cfg.DatabaseMinConns,
		cfg.DatabaseConnectTimeout,
	)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer pool.Close()
	credentials, err := composeCredentials(rootCtx, pool, cfg.Credential)
	if err != nil {
		return err
	}

	// One shutdown path, one SHUTDOWN_TIMEOUT budget, shared by HTTP, the
	// telemetry ingress drain and the credential close. It is registered before
	// any component below starts so a startup failure after the ingress starts
	// still tears it down. A sync.Once keeps it idempotent, and components that
	// never came online stay inert.
	var (
		shutdownOnce sync.Once
		shutdownErr  error
		server       *http.Server
		serverReady  bool
		ingress      ingressRuntime
	)
	shutdown := func() error {
		shutdownOnce.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
			defer cancel()

			// Stop admission and readiness first, then stop the reconnect loop
			// while holding the current connection open so workers can still
			// publish post-commit ACKs during the drain.
			ingress.stopIntake()

			httpErrors := make(chan error, 1)
			if serverReady {
				go func() { httpErrors <- server.Shutdown(ctx) }()
			}

			// Drain admitted work within the same budget, then close MQTT. The
			// shared pool is closed by the deferred pool.Close after this returns,
			// so it is never closed while workers are still running.
			if err := ingress.drainAndClose(ctx); err != nil {
				shutdownErr = errors.Join(shutdownErr, fmt.Errorf("telemetry ingress shutdown: %w", err))
			}
			ingress.logSummary()
			if serverReady {
				if err := <-httpErrors; err != nil {
					_ = server.Close()
					shutdownErr = errors.Join(shutdownErr, fmt.Errorf("shutdown HTTP server: %w", err))
				}
			}
			if err := credentials.closeContext(ctx); err != nil {
				shutdownErr = errors.Join(shutdownErr, fmt.Errorf("credential shutdown close unconfirmed: %w", err))
			}
		})
		return shutdownErr
	}
	defer func() {
		if err := shutdown(); err != nil {
			slog.Error("backend shutdown incomplete", "error", err)
			return
		}
		if serverReady {
			slog.Info("backend shutdown complete")
		}
	}()

	if ingress, err = composeIngress(cfg, pool); err != nil {
		return err
	}

	if _, err := cfg.MQTTCredentialRuntime.Start(rootCtx); err != nil {
		return fmt.Errorf("check MQTT credential runtime: %w", err)
	}

	tokenVerifier, err := auth.NewHS256Verifier(auth.VerifierConfig{
		Secret:    cfg.SupabaseJWTSecret,
		Issuer:    cfg.SupabaseJWTIssuer,
		Audience:  cfg.SupabaseJWTAudience,
		ClockSkew: cfg.SupabaseJWTClockSkew,
	})
	if err != nil {
		return fmt.Errorf("create JWT verifier: %w", err)
	}

	adminChecker, err := auth.NewPostgresPlatformAdminChecker(pool)
	if err != nil {
		return fmt.Errorf("create platform admin checker: %w", err)
	}
	gatewayRepository, err := gateway.NewPostgresRepository(pool)
	if err != nil {
		return fmt.Errorf("create gateway repository: %w", err)
	}
	gatewayService, err := gateway.NewService(gatewayRepository, cfg.AuthorizationTimeout)
	if err != nil {
		return fmt.Errorf("create gateway service: %w", err)
	}
	provisioningRepository, err := gateway.NewPostgresProvisioningRepository(pool)
	if err != nil {
		return fmt.Errorf("create provisioning repository: %w", err)
	}
	provisioningService, err := gateway.NewProvisioningService(provisioningRepository, cfg.AuthorizationTimeout)
	if err != nil {
		return fmt.Errorf("create provisioning service: %w", err)
	}
	ownerSensorRepo, err := gateway.NewPostgresOwnerSensorProvisioningRepository(pool)
	if err != nil {
		return fmt.Errorf("create owner sensor provisioning repository: %w", err)
	}
	ownerSensorService, err := gateway.NewOwnerSensorProvisioningService(ownerSensorRepo, cfg.AuthorizationTimeout)
	if err != nil {
		return fmt.Errorf("create owner sensor provisioning service: %w", err)
	}
	adminGatewayRepository, err := gateway.NewPostgresAdminGatewayRepository(pool)
	if err != nil {
		return fmt.Errorf("create admin gateway repository: %w", err)
	}
	adminGatewayService, err := gateway.NewAdminGatewayService(
		adminGatewayRepository,
		cfg.AuthorizationTimeout,
		cfg.AdminInventoryDefaultLimit,
		cfg.AdminInventoryMaxLimit,
	)
	if err != nil {
		return fmt.Errorf("create admin gateway service: %w", err)
	}
	readiness := ingressReadinessChecker(pool, cfg.ReadinessTimeout, ingress)
	router, err := httpserver.NewRouter(httpserver.RouterDependencies{
		CredentialAPIEnabled:       cfg.Credential.Enabled,
		CredentialRequestTimeout:   cfg.Credential.RequestTimeout,
		CredentialManager:          credentials.manager,
		CredentialStartupReadiness: credentials.ready,
		GatewayProvisioner:         provisioningService,
		SensorProvisioner:          provisioningService,
		OwnerSensorProvisioner:     ownerSensorService,
		AdminGatewayLister:         adminGatewayService,
		AdminInventoryDefaultLimit: cfg.AdminInventoryDefaultLimit,
		AdminInventoryMaxLimit:     cfg.AdminInventoryMaxLimit,
		AdminMaxBodyBytes:          int64(cfg.AdminMaxBodyBytes),
		GatewayReader:              gatewayService,
		SensorReader:               gatewayService,
		ReadinessChecker:           readiness,
		ReadinessTimeout:           cfg.ReadinessTimeout,
		AuthorizationTimeout:       cfg.AuthorizationTimeout,
		TokenVerifier:              tokenVerifier,
		PlatformAdminChecker:       adminChecker,
		CORSAllowedOrigins:         cfg.CORSAllowedOrigins,
	})
	if err != nil {
		return fmt.Errorf("create HTTP router: %w", err)
	}

	server = &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.ServerPort),
		Handler:           router,
		ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
		ReadTimeout:       cfg.HTTPReadTimeout,
		WriteTimeout:      cfg.HTTPWriteTimeout,
		IdleTimeout:       cfg.HTTPIdleTimeout,
	}

	if err := ingress.start(); err != nil {
		return fmt.Errorf("start MQTT telemetry ingress: %w", err)
	}

	serverErrors := make(chan error, 1)
	serverReady = true
	go func() {
		slog.Info("backend listening", "port", cfg.ServerPort, "environment", cfg.ServerEnv)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	case <-rootCtx.Done():
		return nil
	}
}

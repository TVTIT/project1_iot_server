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
	"syscall"

	"github.com/gin-gonic/gin"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/database"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/mqttcredential"
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
	// Fence a prior backend lifetime before any database connection attempt.
	if cfg.Credential.Enabled {
		ctrl := mqttcredential.ControllerClient{ControlDir: cfg.Credential.ControlDir, Timeout: cfg.Credential.ClientTimeout, MaxFrameBytes: 4096}
		if ctrl.CloseDrain(rootCtx) != nil {
			return fmt.Errorf("credential startup controller unavailable")
		}
	}

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
	defer func() {
		if err := credentials.close(); err != nil {
			slog.Error("credential shutdown close unconfirmed")
		}
	}()

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
	router, err := httpserver.NewRouter(httpserver.RouterDependencies{
		CredentialAPIEnabled:       cfg.Credential.Enabled,
		CredentialRequestTimeout:   cfg.Credential.RequestTimeout,
		CredentialManager:          credentials.manager,
		CredentialStartupReadiness: credentials.ready,
		GatewayProvisioner:         provisioningService,
		SensorProvisioner:          provisioningService,
		AdminMaxBodyBytes:          int64(cfg.AdminMaxBodyBytes),
		GatewayReader:              gatewayService,
		SensorReader:               gatewayService,
		ReadinessChecker:           pool,
		ReadinessTimeout:           cfg.ReadinessTimeout,
		AuthorizationTimeout:       cfg.AuthorizationTimeout,
		TokenVerifier:              tokenVerifier,
		PlatformAdminChecker:       adminChecker,
		CORSAllowedOrigins:         cfg.CORSAllowedOrigins,
	})
	if err != nil {
		return fmt.Errorf("create HTTP router: %w", err)
	}

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.ServerPort),
		Handler:           router,
		ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
		ReadTimeout:       cfg.HTTPReadTimeout,
		WriteTimeout:      cfg.HTTPWriteTimeout,
		IdleTimeout:       cfg.HTTPIdleTimeout,
	}

	serverErrors := make(chan error, 1)
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
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
		return fmt.Errorf("shutdown HTTP server: %w", err)
	}
	slog.Info("backend shutdown complete")
	return nil
}

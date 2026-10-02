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
	router, err := httpserver.NewRouter(httpserver.RouterDependencies{
		ReadinessChecker:     pool,
		ReadinessTimeout:     cfg.ReadinessTimeout,
		AuthorizationTimeout: cfg.AuthorizationTimeout,
		TokenVerifier:        tokenVerifier,
		PlatformAdminChecker: adminChecker,
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
		return fmt.Errorf("shutdown HTTP server: %w", err)
	}
	slog.Info("backend shutdown complete")
	return nil
}

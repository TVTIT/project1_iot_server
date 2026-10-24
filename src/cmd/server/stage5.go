package main

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/httpserver"
)

// populateStage5Dependencies wires only durable intent/query HTTP dependencies.
// No broker connection, publisher, result receiver or expiry worker is started.
func populateStage5Dependencies(deps *httpserver.RouterDependencies, cfg config.Config, pool *pgxpool.Pool) error {
	if deps == nil {
		return fmt.Errorf("router dependencies are required")
	}
	deps.ControlAPIEnabled = cfg.Stage5.Enabled
	deps.ControlRepository = nil
	deps.ControlRequestTimeout = cfg.Stage5.Control.DBTimeout
	if !cfg.Stage5.Enabled {
		return nil
	}
	if auth.IsNilDependency(pool) {
		return fmt.Errorf("enabled stage 5 requires a database pool")
	}
	if cfg.MetricsEnabled {
		deps.ControlMetrics = &digitaltwin.ControlMetrics{}
	}
	repo, err := digitaltwin.NewPostgresControlRepository(pool, cfg.Stage5.Control, deps.ControlMetrics)
	if err != nil {
		return fmt.Errorf("create stage 5 intent repository: %w", err)
	}
	deps.ControlRepository = repo
	return nil
}

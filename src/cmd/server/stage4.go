package main

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/telemetry"
)

// stage4Dependencies holds the eleven instantiated Stage 4 HTTP dependencies
// constructed from production configuration and database adapters.
type stage4Dependencies struct {
	TelemetryHistoryReader httpserver.TelemetryHistoryReader
	TwinListReader         httpserver.TwinListReader
	TwinDetailReader       httpserver.TwinDetailReader
	TwinStateReader        httpserver.TwinStateReader
	TwinHistoryReader      httpserver.TwinHistoryReader
	HistoryQueryParser     *httpserver.HistoryQueryParser
	TwinQueryParser        *httpserver.TwinQueryParser
	EntityCreator          httpserver.EntityCreator
	GatewayPatcher         httpserver.GatewayPatcher
	SensorPatcher          httpserver.SensorPatcher
	TwinMaxBodyBytes       int64
}

// apply copies the Stage 4 dependencies into the destination RouterDependencies struct.
func (s stage4Dependencies) apply(deps *httpserver.RouterDependencies) {
	if deps == nil {
		return
	}
	deps.TelemetryHistoryReader = s.TelemetryHistoryReader
	deps.TwinListReader = s.TwinListReader
	deps.TwinDetailReader = s.TwinDetailReader
	deps.TwinStateReader = s.TwinStateReader
	deps.TwinHistoryReader = s.TwinHistoryReader
	deps.HistoryQueryParser = s.HistoryQueryParser
	deps.TwinQueryParser = s.TwinQueryParser
	deps.EntityCreator = s.EntityCreator
	deps.GatewayPatcher = s.GatewayPatcher
	deps.SensorPatcher = s.SensorPatcher
	deps.TwinMaxBodyBytes = s.TwinMaxBodyBytes
}

// composeStage4Dependencies instantiates all Stage 4 database adapters, repositories,
// preparation orchestrators, domain services, and query parsers from validated config
// and a bounded database pool. It validates inputs and propagates any constructor or
// configuration errors before the HTTP listener starts.
func composeStage4Dependencies(
	cfg config.Config,
	pool *pgxpool.Pool,
	gwProvisioner digitaltwin.GatewayProvisioner,
) (stage4Dependencies, error) {
	if auth.IsNilDependency(pool) {
		return stage4Dependencies{}, fmt.Errorf("database pool is required")
	}
	if auth.IsNilDependency(gwProvisioner) {
		return stage4Dependencies{}, fmt.Errorf("gateway provisioner is required")
	}
	if cfg.AuthorizationTimeout <= 0 {
		return stage4Dependencies{}, fmt.Errorf("authorization timeout must be positive")
	}
	if cfg.AdminMaxBodyBytes < 1 || cfg.AdminMaxBodyBytes > 1048576 {
		return stage4Dependencies{}, fmt.Errorf("twin body limit must be between 1 and 1048576")
	}
	if err := config.ValidateHistoryConfig(cfg.History); err != nil {
		return stage4Dependencies{}, fmt.Errorf("invalid history config: %w", err)
	}
	if err := config.ValidateTwinConfig(cfg.Twin); err != nil {
		return stage4Dependencies{}, fmt.Errorf("invalid twin config: %w", err)
	}

	telemetryPrepCfg := cfg.History.ToTelemetryPreparationConfig()
	twinPrepCfg := cfg.Twin.ToReadPreparationConfig(cfg.History)

	resolver, err := digitaltwin.NewPostgresResolver(pool)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create postgres resolver: %w", err)
	}

	historyRepo, err := telemetry.NewPostgresHistoryRepository(pool, telemetryPrepCfg)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create telemetry history repository: %w", err)
	}

	telemetryHistoryService, err := telemetry.NewTelemetryHistoryService(resolver, historyRepo, telemetryPrepCfg)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create telemetry history service: %w", err)
	}

	twinPrep, err := digitaltwin.NewReadPreparation(resolver, twinPrepCfg)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create twin read preparation: %w", err)
	}

	twinReader, err := digitaltwin.NewPostgresTwinReader(pool)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create twin reader: %w", err)
	}

	twinReadService, err := digitaltwin.NewReadService(twinPrep, twinReader, telemetryHistoryService)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create twin read service: %w", err)
	}

	adminOrOwnerSensorRepo, err := gateway.NewPostgresAdminOrOwnerSensorProvisioningRepository(pool)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create admin or owner sensor provisioning repository: %w", err)
	}

	adminOrOwnerSensorService, err := gateway.NewAdminOrOwnerSensorProvisioningService(adminOrOwnerSensorRepo, cfg.AuthorizationTimeout)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create admin or owner sensor provisioning service: %w", err)
	}

	entityCreateService, err := digitaltwin.NewCreateService(gwProvisioner, adminOrOwnerSensorService)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create entity create service: %w", err)
	}

	gwPatcher, err := digitaltwin.NewPostgresGatewayMetadataPatcher(pool)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create gateway metadata patcher: %w", err)
	}

	gatewayPatchService, err := digitaltwin.NewGatewayPatchService(gwPatcher, cfg.AuthorizationTimeout)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create gateway patch service: %w", err)
	}

	sensorPatcher, err := digitaltwin.NewPostgresSensorMetadataPatcher(pool)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create sensor metadata patcher: %w", err)
	}

	sensorPatchService, err := digitaltwin.NewSensorPatchService(sensorPatcher, cfg.AuthorizationTimeout)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create sensor patch service: %w", err)
	}

	historyQueryParser, err := httpserver.NewHistoryQueryParser(&cfg.History)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create history query parser: %w", err)
	}

	twinQueryParser, err := httpserver.NewTwinQueryParser(&cfg.Twin, cfg.History.QueryTimeout)
	if err != nil {
		return stage4Dependencies{}, fmt.Errorf("create twin query parser: %w", err)
	}

	return stage4Dependencies{
		TelemetryHistoryReader: telemetryHistoryService,
		TwinListReader:         twinReadService,
		TwinDetailReader:       twinReadService,
		TwinStateReader:        twinReadService,
		TwinHistoryReader:      twinReadService,
		HistoryQueryParser:     historyQueryParser,
		TwinQueryParser:        twinQueryParser,
		EntityCreator:          entityCreateService,
		GatewayPatcher:         gatewayPatchService,
		SensorPatcher:          sensorPatchService,
		TwinMaxBodyBytes:       int64(cfg.AdminMaxBodyBytes),
	}, nil
}

// populateStage4Dependencies builds the Stage 4 dependencies and populates them
// into the provided RouterDependencies struct.
func populateStage4Dependencies(
	deps *httpserver.RouterDependencies,
	cfg config.Config,
	pool *pgxpool.Pool,
	gwProvisioner digitaltwin.GatewayProvisioner,
) error {
	stage4, err := composeStage4Dependencies(cfg, pool, gwProvisioner)
	if err != nil {
		return err
	}
	stage4.apply(deps)
	return nil
}

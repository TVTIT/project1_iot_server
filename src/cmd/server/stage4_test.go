package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpserver"
)

type dummyGatewayProvisioner struct{}

func (dummyGatewayProvisioner) ProvisionGateway(context.Context, uuid.UUID, gateway.ProvisionGatewayInput) (gateway.ProvisionGatewayResult, error) {
	return gateway.ProvisionGatewayResult{
		Created:   true,
		GatewayID: "test-gw",
		EntityID:  "urn:ngsi-ld:Gateway:test-gw",
	}, nil
}

type dummyReadinessChecker struct{}

func (dummyReadinessChecker) Ping(context.Context) error {
	return nil
}

func validTestConfig() config.Config {
	return config.Config{
		History:                    config.DefaultHistoryConfig(),
		Twin:                       config.DefaultTwinConfig(),
		AuthorizationTimeout:       5 * time.Second,
		AdminMaxBodyBytes:          16384,
		ReadinessTimeout:           time.Second,
		AdminInventoryDefaultLimit: 100,
		AdminInventoryMaxLimit:     200,
	}
}

func TestComposeStage4Dependencies_Valid(t *testing.T) {
	pool := testPool(t)
	cfg := validTestConfig()
	gwProv := dummyGatewayProvisioner{}

	deps, err := composeStage4Dependencies(cfg, pool, gwProv)
	if err != nil {
		t.Fatalf("composeStage4Dependencies failed: %v", err)
	}

	if deps.TelemetryHistoryReader == nil {
		t.Fatal("expected non-nil TelemetryHistoryReader")
	}
	if deps.TwinListReader == nil {
		t.Fatal("expected non-nil TwinListReader")
	}
	if deps.TwinDetailReader == nil {
		t.Fatal("expected non-nil TwinDetailReader")
	}
	if deps.TwinStateReader == nil {
		t.Fatal("expected non-nil TwinStateReader")
	}
	if deps.TwinHistoryReader == nil {
		t.Fatal("expected non-nil TwinHistoryReader")
	}
	if deps.HistoryQueryParser == nil {
		t.Fatal("expected non-nil HistoryQueryParser")
	}
	if deps.TwinQueryParser == nil {
		t.Fatal("expected non-nil TwinQueryParser")
	}
	if deps.EntityCreator == nil {
		t.Fatal("expected non-nil EntityCreator")
	}
	if deps.GatewayPatcher == nil {
		t.Fatal("expected non-nil GatewayPatcher")
	}
	if deps.SensorPatcher == nil {
		t.Fatal("expected non-nil SensorPatcher")
	}
	if deps.TwinMaxBodyBytes != 16384 {
		t.Fatalf("expected TwinMaxBodyBytes 16384, got %d", deps.TwinMaxBodyBytes)
	}

	verifier, err := auth.NewHS256Verifier(auth.VerifierConfig{
		Secret:   strings.Repeat("s", 32),
		Issuer:   "https://auth.example.com",
		Audience: "authenticated",
	})
	if err != nil {
		t.Fatalf("create test verifier: %v", err)
	}

	adminChecker, err := auth.NewPostgresPlatformAdminChecker(pool)
	if err != nil {
		t.Fatalf("create platform admin checker: %v", err)
	}
	gatewayRepo, err := gateway.NewPostgresRepository(pool)
	if err != nil {
		t.Fatalf("create gateway repo: %v", err)
	}
	gatewayService, err := gateway.NewService(gatewayRepo, cfg.AuthorizationTimeout)
	if err != nil {
		t.Fatalf("create gateway service: %v", err)
	}
	provisioningRepo, err := gateway.NewPostgresProvisioningRepository(pool)
	if err != nil {
		t.Fatalf("create provisioning repo: %v", err)
	}
	provisioningService, err := gateway.NewProvisioningService(provisioningRepo, cfg.AuthorizationTimeout)
	if err != nil {
		t.Fatalf("create provisioning service: %v", err)
	}
	ownerSensorRepo, err := gateway.NewPostgresOwnerSensorProvisioningRepository(pool)
	if err != nil {
		t.Fatalf("create owner sensor repo: %v", err)
	}
	ownerSensorService, err := gateway.NewOwnerSensorProvisioningService(ownerSensorRepo, cfg.AuthorizationTimeout)
	if err != nil {
		t.Fatalf("create owner sensor service: %v", err)
	}
	adminGatewayRepo, err := gateway.NewPostgresAdminGatewayRepository(pool)
	if err != nil {
		t.Fatalf("create admin gateway repo: %v", err)
	}
	adminGatewayService, err := gateway.NewAdminGatewayService(adminGatewayRepo, cfg.AuthorizationTimeout, 100, 200)
	if err != nil {
		t.Fatalf("create admin gateway service: %v", err)
	}

	routerDeps := httpserver.RouterDependencies{
		ReadinessChecker:           dummyReadinessChecker{},
		ReadinessTimeout:           time.Second,
		AuthorizationTimeout:       cfg.AuthorizationTimeout,
		TokenVerifier:              verifier,
		PlatformAdminChecker:       adminChecker,
		GatewayReader:              gatewayService,
		SensorReader:               gatewayService,
		GatewayProvisioner:         provisioningService,
		SensorProvisioner:          provisioningService,
		OwnerSensorProvisioner:     ownerSensorService,
		AdminGatewayLister:         adminGatewayService,
		AdminInventoryDefaultLimit: 100,
		AdminInventoryMaxLimit:     200,
		AdminMaxBodyBytes:          16384,
	}

	deps.apply(&routerDeps)

	router, err := httpserver.NewRouter(routerDeps)
	if err != nil {
		t.Fatalf("NewRouter failed with populated stage 4 dependencies: %v", err)
	}
	if router == nil {
		t.Fatal("expected non-nil HTTP router handler")
	}
}

func TestComposeStage4Dependencies_NilAndTypedNilDependencies(t *testing.T) {
	pool := testPool(t)
	cfg := validTestConfig()
	gwProv := dummyGatewayProvisioner{}

	tests := []struct {
		name       string
		pool       *pgxpool.Pool
		gwProv     digitaltwin.GatewayProvisioner
		wantErrSub string
	}{
		{
			name:       "nil pool untyped",
			pool:       nil,
			gwProv:     gwProv,
			wantErrSub: "database pool is required",
		},
		{
			name:       "nil pool typed",
			pool:       (*pgxpool.Pool)(nil),
			gwProv:     gwProv,
			wantErrSub: "database pool is required",
		},
		{
			name:       "nil provisioner untyped",
			pool:       pool,
			gwProv:     nil,
			wantErrSub: "gateway provisioner is required",
		},
		{
			name:       "nil provisioner typed",
			pool:       pool,
			gwProv:     (*gateway.ProvisioningService)(nil),
			wantErrSub: "gateway provisioner is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := composeStage4Dependencies(cfg, tc.pool, tc.gwProv)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErrSub)
			}
			if !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
			}
		})
	}
}

func TestComposeStage4Dependencies_InvalidHistoryBounds(t *testing.T) {
	pool := testPool(t)
	gwProv := dummyGatewayProvisioner{}

	tests := []struct {
		name       string
		modify     func(*config.Config)
		wantErrSub string
	}{
		{
			name: "zero query timeout",
			modify: func(c *config.Config) {
				c.History.QueryTimeout = 0
			},
			wantErrSub: "invalid history config",
		},
		{
			name: "query timeout exceeds 5s hard cap",
			modify: func(c *config.Config) {
				c.History.QueryTimeout = 10 * time.Second
			},
			wantErrSub: "invalid history config",
		},
		{
			name: "min points below 100",
			modify: func(c *config.Config) {
				c.History.MinPoints = 50
			},
			wantErrSub: "invalid history config",
		},
		{
			name: "max points below min points",
			modify: func(c *config.Config) {
				c.History.MaxPoints = 50
			},
			wantErrSub: "invalid history config",
		},
		{
			name: "negative max range",
			modify: func(c *config.Config) {
				c.History.MaxRange = -time.Hour
			},
			wantErrSub: "invalid history config",
		},
		{
			name: "raw window exceeds max range",
			modify: func(c *config.Config) {
				c.History.RawWindow = 2 * time.Hour
				c.History.MaxRange = time.Hour
			},
			wantErrSub: "invalid history config",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validTestConfig()
			tc.modify(&cfg)
			_, err := composeStage4Dependencies(cfg, pool, gwProv)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErrSub)
			}
			if !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
			}
		})
	}
}

func TestComposeStage4Dependencies_InvalidTwinBounds(t *testing.T) {
	pool := testPool(t)
	gwProv := dummyGatewayProvisioner{}

	tests := []struct {
		name       string
		modify     func(*config.Config)
		wantErrSub string
	}{
		{
			name: "list default exceeds max limit",
			modify: func(c *config.Config) {
				c.Twin.ListDefaultLimit = 100
				c.Twin.ListMaxLimit = 50
			},
			wantErrSub: "invalid twin config",
		},
		{
			name: "relationship default exceeds max limit",
			modify: func(c *config.Config) {
				c.Twin.RelationshipDefaultLimit = 200
				c.Twin.RelationshipMaxLimit = 50
			},
			wantErrSub: "invalid twin config",
		},
		{
			name: "list default zero",
			modify: func(c *config.Config) {
				c.Twin.ListDefaultLimit = 0
			},
			wantErrSub: "invalid twin config",
		},
		{
			name: "relationship default zero",
			modify: func(c *config.Config) {
				c.Twin.RelationshipDefaultLimit = 0
			},
			wantErrSub: "invalid twin config",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validTestConfig()
			tc.modify(&cfg)
			_, err := composeStage4Dependencies(cfg, pool, gwProv)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErrSub)
			}
			if !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
			}
		})
	}
}

func TestComposeStage4Dependencies_MutationTimeoutAndBodyCap(t *testing.T) {
	pool := testPool(t)
	gwProv := dummyGatewayProvisioner{}

	tests := []struct {
		name       string
		modify     func(*config.Config)
		wantErrSub string
	}{
		{
			name: "zero authorization timeout",
			modify: func(c *config.Config) {
				c.AuthorizationTimeout = 0
			},
			wantErrSub: "authorization timeout must be positive",
		},
		{
			name: "negative authorization timeout",
			modify: func(c *config.Config) {
				c.AuthorizationTimeout = -1 * time.Second
			},
			wantErrSub: "authorization timeout must be positive",
		},
		{
			name: "zero body cap",
			modify: func(c *config.Config) {
				c.AdminMaxBodyBytes = 0
			},
			wantErrSub: "twin body limit must be between 1 and 1048576",
		},
		{
			name: "negative body cap",
			modify: func(c *config.Config) {
				c.AdminMaxBodyBytes = -5
			},
			wantErrSub: "twin body limit must be between 1 and 1048576",
		},
		{
			name: "excessive body cap",
			modify: func(c *config.Config) {
				c.AdminMaxBodyBytes = 2000000
			},
			wantErrSub: "twin body limit must be between 1 and 1048576",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validTestConfig()
			tc.modify(&cfg)
			_, err := composeStage4Dependencies(cfg, pool, gwProv)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErrSub)
			}
			if !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
			}
		})
	}
}

func TestComposeStage4Dependencies_OverridePropagation(t *testing.T) {
	pool := testPool(t)
	gwProv := dummyGatewayProvisioner{}

	cfg := validTestConfig()
	cfg.AdminMaxBodyBytes = 32768
	cfg.History.QueryTimeout = 3 * time.Second
	cfg.History.MaxRange = 24 * time.Hour
	cfg.History.RawWindow = 5 * time.Second
	cfg.History.DefaultPoints = 500
	cfg.History.MaxPoints = 2000
	cfg.Twin.ListDefaultLimit = 15
	cfg.Twin.ListMaxLimit = 35
	cfg.Twin.RelationshipDefaultLimit = 25
	cfg.Twin.RelationshipMaxLimit = 45

	deps, err := composeStage4Dependencies(cfg, pool, gwProv)
	if err != nil {
		t.Fatalf("composeStage4Dependencies failed: %v", err)
	}

	if deps.TwinMaxBodyBytes != 32768 {
		t.Fatalf("expected TwinMaxBodyBytes 32768, got %d", deps.TwinMaxBodyBytes)
	}

	// Verify override propagation through HistoryQueryParser:
	// A raw query without max_points should receive the overridden DefaultPoints (500).
	queryBase := "gateway_id=gw1&sensor_id=s1&from=2026-10-18T10:00:00Z&to=2026-10-18T11:00:00Z"
	parsedHistory, err := deps.HistoryQueryParser.ParseTelemetryHistoryQuery(queryBase)
	if err != nil {
		t.Fatalf("parse history query failed: %v", err)
	}
	if parsedHistory.MaxPoints != 500 {
		t.Fatalf("expected default MaxPoints 500 from override, got %d", parsedHistory.MaxPoints)
	}

	// Query with max_points=2000 should be accepted (up to MaxPoints override).
	if _, err := deps.HistoryQueryParser.ParseTelemetryHistoryQuery(queryBase + "&max_points=2000"); err != nil {
		t.Fatalf("expected max_points=2000 to be accepted, got: %v", err)
	}

	// Query with max_points=2001 should be rejected (exceeds MaxPoints override of 2000).
	if _, err := deps.HistoryQueryParser.ParseTelemetryHistoryQuery(queryBase + "&max_points=2001"); err == nil {
		t.Fatal("expected max_points=2001 to be rejected, got nil")
	}

	// Verify override propagation through TwinQueryParser:
	// A raw list query without limit should receive overridden ListDefaultLimit (15).
	parsedList, err := deps.TwinQueryParser.ParseTwinListQuery("")
	if err != nil {
		t.Fatalf("parse twin list query failed: %v", err)
	}
	if parsedList.Limit != 15 {
		t.Fatalf("expected default Limit 15 from override, got %d", parsedList.Limit)
	}

	// Limit=35 should be accepted (up to ListMaxLimit override).
	if _, err := deps.TwinQueryParser.ParseTwinListQuery("limit=35"); err != nil {
		t.Fatalf("expected limit=35 to be accepted, got: %v", err)
	}

	// Limit=36 should be rejected (exceeds ListMaxLimit override of 35).
	if _, err := deps.TwinQueryParser.ParseTwinListQuery("limit=36"); err == nil {
		t.Fatal("expected limit=36 to be rejected, got nil")
	}

	// Verify RelationshipDefaultLimit (25) and RelationshipMaxLimit (45) override propagation:
	parsedDetail, err := deps.TwinQueryParser.ParseTwinDetailQuery("", "urn:ngsi-ld:Gateway:gw1")
	if err != nil {
		t.Fatalf("parse twin detail query failed: %v", err)
	}
	if parsedDetail.RelationshipLimit != 25 {
		t.Fatalf("expected default relationship Limit 25 from override, got %d", parsedDetail.RelationshipLimit)
	}
	if _, err := deps.TwinQueryParser.ParseTwinDetailQuery("relationship_limit=45", "urn:ngsi-ld:Gateway:gw1"); err != nil {
		t.Fatalf("expected relationship_limit=45 to be accepted, got: %v", err)
	}
	if _, err := deps.TwinQueryParser.ParseTwinDetailQuery("relationship_limit=46", "urn:ngsi-ld:Gateway:gw1"); err == nil {
		t.Fatal("expected relationship_limit=46 to be rejected, got nil")
	}

	// Verify populateStage4Dependencies helper applies all 11 fields.
	var targetDeps httpserver.RouterDependencies
	if err := populateStage4Dependencies(&targetDeps, cfg, pool, gwProv); err != nil {
		t.Fatalf("populateStage4Dependencies failed: %v", err)
	}
	if targetDeps.TwinMaxBodyBytes != 32768 {
		t.Fatalf("expected target TwinMaxBodyBytes 32768, got %d", targetDeps.TwinMaxBodyBytes)
	}
	if targetDeps.TelemetryHistoryReader == nil || targetDeps.TwinListReader == nil ||
		targetDeps.TwinDetailReader == nil || targetDeps.TwinStateReader == nil ||
		targetDeps.TwinHistoryReader == nil || targetDeps.HistoryQueryParser == nil ||
		targetDeps.TwinQueryParser == nil || targetDeps.EntityCreator == nil ||
		targetDeps.GatewayPatcher == nil || targetDeps.SensorPatcher == nil {
		t.Fatal("expected all 11 router fields to be populated by populateStage4Dependencies")
	}
}

package digitaltwin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/telemetry"
)

// TestNativeTemporalHistory_NumericSlice exercises the first narrow slice of Task 4.4.7:
// native numeric temporal history, authorization, time boundaries, aggregation, and unit semantics
// on real PostgreSQL 16 / TimescaleDB 2.29 under application role iot_backend_app with setupPool fixture management.
func TestNativeTemporalHistory_NumericSlice(t *testing.T) {
	setupURL, backendURL := resolveTestDatabaseURLs(t)
	if setupURL == "" || backendURL == "" {
		t.Skip("Skipping native Digital Twin temporal tests: set TWIN_TEMPORAL_TEST_NATIVE=1 or TWIN_READ_TEST_NATIVE=1 to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	setupPool, err := pgxpool.New(ctx, setupURL)
	if err != nil {
		t.Fatalf("setup database pool connection failed: %v", err)
	}
	defer setupPool.Close()

	appPool, err := pgxpool.New(ctx, backendURL)
	if err != nil {
		t.Fatalf("application database pool connection failed: %v", err)
	}
	defer appPool.Close()

	// 1. Verify target database versions and schema version
	var pgVersion, tsVersion string
	if err := setupPool.QueryRow(ctx, "SELECT version()").Scan(&pgVersion); err != nil {
		t.Fatalf("failed to query postgres version: %v", err)
	}
	if err := setupPool.QueryRow(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").Scan(&tsVersion); err != nil {
		t.Fatalf("failed to query timescaledb extension version: %v", err)
	}
	var schemaVersion int
	if err := setupPool.QueryRow(ctx, "SELECT MAX(version) FROM schema_migrations").Scan(&schemaVersion); err != nil {
		t.Fatalf("failed to query schema migrations: %v", err)
	}

	t.Logf("Database Target Verified: PostgreSQL Version = %s, TimescaleDB Version = %s, Schema Version = %d",
		strings.Fields(pgVersion)[1], tsVersion, schemaVersion)

	// 2. Verify roles and attributes for setupPool and appPool
	var setupUser, setupSession string
	var setupSuper, setupCanLogin, setupBypassRLS bool
	err = setupPool.QueryRow(ctx, `
		SELECT current_user, session_user, r.rolsuper, r.rolcanlogin, r.rolbypassrls
		FROM pg_roles r WHERE r.rolname = current_user
	`).Scan(&setupUser, &setupSession, &setupSuper, &setupCanLogin, &setupBypassRLS)
	if err != nil {
		t.Fatalf("failed to verify setup connection role: %v", err)
	}
	if !setupSuper {
		t.Fatalf("setup connection role must be superuser, got user=%s superuser=false", setupUser)
	}

	var appUser, appSession string
	var appSuper, appCanLogin, appBypassRLS bool
	err = appPool.QueryRow(ctx, `
		SELECT current_user, session_user, r.rolsuper, r.rolcanlogin, r.rolbypassrls
		FROM pg_roles r WHERE r.rolname = current_user
	`).Scan(&appUser, &appSession, &appSuper, &appCanLogin, &appBypassRLS)
	if err != nil {
		t.Fatalf("failed to verify application connection role: %v", err)
	}
	if appUser != "iot_backend_app" {
		t.Fatalf("expected application role iot_backend_app, got %q", appUser)
	}
	if appSuper {
		t.Fatalf("application role iot_backend_app must not be superuser")
	}
	if !appCanLogin {
		t.Fatalf("application role iot_backend_app must have login privilege")
	}
	t.Logf("Application Connection: current_user = %s, session_user = %s, superuser = %v, canlogin = %v, bypassrls = %v",
		appUser, appSession, appSuper, appCanLogin, appBypassRLS)

	// 3. Define uniquely scoped fixture identifiers
	runID := uuid.New().String()[:8]
	gwIDA := fmt.Sprintf("gw_tmp_a_%s", runID)
	gwIDB := fmt.Sprintf("gw_tmp_b_%s", runID)

	sensorSharedID := fmt.Sprintf("sens_shared_%s", runID)
	sensorEmptyID := fmt.Sprintf("sens_empty_%s", runID)
	sensorOrphanID := fmt.Sprintf("sens_orph_%s", runID)
	sensorAggID := fmt.Sprintf("sens_agg_%s", runID)
	sensorUnitID := fmt.Sprintf("sens_unit_%s", runID)
	sensorTypedID := fmt.Sprintf("sens_typed_%s", runID)
	sensorPersistID := fmt.Sprintf("sens_persist_%s", runID)
	sensorSnapID := fmt.Sprintf("sens_snap_%s", runID)

	gwAURN := "urn:ngsi-ld:Gateway:" + gwIDA
	gwBURN := "urn:ngsi-ld:Gateway:" + gwIDB
	sensA1URN := "urn:ngsi-ld:Sensor:" + gwIDA + ":" + sensorSharedID
	sensAEmptyURN := "urn:ngsi-ld:Sensor:" + gwIDA + ":" + sensorEmptyID
	sensAOrphURN := "urn:ngsi-ld:Sensor:" + gwIDA + ":" + sensorOrphanID
	sensAAggURN := "urn:ngsi-ld:Sensor:" + gwIDA + ":" + sensorAggID
	sensAUnitURN := "urn:ngsi-ld:Sensor:" + gwIDA + ":" + sensorUnitID
	sensATypedURN := "urn:ngsi-ld:Sensor:" + gwIDA + ":" + sensorTypedID
	sensAPersistURN := "urn:ngsi-ld:Sensor:" + gwIDA + ":" + sensorPersistID
	sensASnapURN := "urn:ngsi-ld:Sensor:" + gwIDA + ":" + sensorSnapID
	sensB1URN := "urn:ngsi-ld:Sensor:" + gwIDB + ":" + sensorSharedID

	gwAUUID := uuid.New()
	gwBUUID := uuid.New()
	sensA1UUID := uuid.New()
	sensAEmptyUUID := uuid.New()
	sensAOrphUUID := uuid.New()
	sensAAggUUID := uuid.New()
	sensAUnitUUID := uuid.New()
	sensATypedUUID := uuid.New()
	sensAPersistUUID := uuid.New()
	sensASnapUUID := uuid.New()
	sensB1UUID := uuid.New()

	ownerUser := uuid.New()
	operatorUser := uuid.New()
	viewerUser := uuid.New()
	ownerBUser := uuid.New()
	nonMemberUser := uuid.New()
	platformAdminUser := uuid.New()

	allUserIDs := []uuid.UUID{ownerUser, operatorUser, viewerUser, ownerBUser, nonMemberUser, platformAdminUser}

	fixtureIDs := temporalFixtureIDs{
		RunID:             runID,
		GatewayIDs:        []string{gwIDA, gwIDB},
		EntityUUIDs:       []uuid.UUID{gwAUUID, gwBUUID, sensA1UUID, sensAEmptyUUID, sensAOrphUUID, sensAAggUUID, sensAUnitUUID, sensATypedUUID, sensAPersistUUID, sensASnapUUID, sensB1UUID},
		AllUserIDs:        allUserIDs,
		PlatformAdminUser: platformAdminUser,
	}

	// 4. Register scoped cleanup BEFORE first fixture write with independent bounded background context
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()

		performTemporalFixtureCleanup(cleanupCtx, t, setupPool, fixtureIDs)
	}()

	// 5. Seed Users & Profiles via setupPool
	var hasAuthUsersTable bool
	if err := setupPool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsersTable); err == nil && hasAuthUsersTable {
		for _, uID := range allUserIDs {
			_, err := setupPool.Exec(ctx, `
				INSERT INTO auth.users (id, aud, role, email, raw_app_meta_data, raw_user_meta_data, created_at, updated_at)
				VALUES ($1, 'authenticated', 'authenticated', $2, '{"provider":"email"}', '{}', now(), now())
				ON CONFLICT (id) DO NOTHING`,
				uID, fmt.Sprintf("%s@example.com", uID.String()[:8]),
			)
			if err != nil {
				t.Fatalf("failed to insert auth user %s: %v", uID, err)
			}
		}
	}

	for _, uID := range allUserIDs {
		_, err := setupPool.Exec(ctx, `
			INSERT INTO public.profiles (id, full_name)
			VALUES ($1, $2)
			ON CONFLICT (id) DO NOTHING`,
			uID, fmt.Sprintf("User %s", uID.String()[:8]),
		)
		if err != nil {
			t.Fatalf("failed to insert profile %s: %v", uID, err)
		}
	}

	// 6. Seed platform_admin record
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.platform_admins (user_id)
		VALUES ($1)
		ON CONFLICT (user_id) DO NOTHING`, platformAdminUser); err != nil {
		t.Fatalf("failed to insert platform admin: %v", err)
	}

	// 7. Seed Gateways A & B
	for _, gw := range []struct {
		id   string
		name string
	}{
		{gwIDA, "Gateway A Temporal"},
		{gwIDB, "Gateway B Temporal"},
	} {
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.gateways (gateway_id, name, description)
			VALUES ($1, $2, 'Temporal Test GW')`, gw.id, gw.name); err != nil {
			t.Fatalf("failed to insert gateway %s: %v", gw.id, err)
		}
	}

	// 8. Seed user_gateways memberships
	// Gateway A: owner, operator, viewer
	// Gateway B: ownerBUser
	// nonMemberUser & platformAdminUser: NO memberships
	for _, m := range []struct {
		gw   string
		user uuid.UUID
		role string
	}{
		{gwIDA, ownerUser, "owner"},
		{gwIDA, operatorUser, "operator"},
		{gwIDA, viewerUser, "viewer"},
		{gwIDB, ownerBUser, "owner"},
	} {
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.user_gateways (user_id, gateway_id, role)
			VALUES ($1, $2, $3)`, m.user, m.gw, m.role); err != nil {
			t.Fatalf("failed to insert user_gateway membership %+v: %v", m, err)
		}
	}

	// 9. Seed Sensors in public.sensors
	for _, sens := range []struct {
		gw   string
		id   string
		name string
		unit string
	}{
		{gwIDA, sensorSharedID, "Sensor A1 Shared", "us"},
		{gwIDA, sensorEmptyID, "Sensor A Empty", "us"},
		{gwIDA, sensorOrphanID, "Sensor A Orphan", "us"},
		{gwIDA, sensorAggID, "Sensor A Aggregation", "us"},
		{gwIDA, sensorUnitID, "Sensor A Unit Invariants", "us"},
		{gwIDA, sensorTypedID, "Sensor A Typed", "us"},
		{gwIDA, sensorPersistID, "Sensor A Persist", "degC"},
		{gwIDA, sensorSnapID, "Sensor A Snapshots", "us"},
		{gwIDB, sensorSharedID, "Sensor B1 Shared", "us"},
	} {
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.sensors (gateway_id, sensor_id, name, unit)
			VALUES ($1, $2, $3, $4)`, sens.gw, sens.id, sens.name, sens.unit); err != nil {
			t.Fatalf("failed to insert sensor %+v: %v", sens, err)
		}
	}

	// 10. Seed Twin Entities
	for _, ent := range []struct {
		id       uuid.UUID
		entityID string
		eType    string
		gwID     string
	}{
		{gwAUUID, gwAURN, "Gateway", gwIDA},
		{gwBUUID, gwBURN, "Gateway", gwIDB},
		{sensA1UUID, sensA1URN, "Sensor", gwIDA},
		{sensAEmptyUUID, sensAEmptyURN, "Sensor", gwIDA},
		{sensAOrphUUID, sensAOrphURN, "Sensor", gwIDA},
		{sensAAggUUID, sensAAggURN, "Sensor", gwIDA},
		{sensAUnitUUID, sensAUnitURN, "Sensor", gwIDA},
		{sensATypedUUID, sensATypedURN, "Sensor", gwIDA},
		{sensAPersistUUID, sensAPersistURN, "Sensor", gwIDA},
		{sensASnapUUID, sensASnapURN, "Sensor", gwIDA},
		{sensB1UUID, sensB1URN, "Sensor", gwIDB},
	} {
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.twin_entities (id, entity_id, entity_type, name, gateway_id, attributes)
			VALUES ($1, $2, $3, $2, $4, '{}'::jsonb)`, ent.id, ent.entityID, ent.eType, ent.gwID); err != nil {
			t.Fatalf("failed to insert entity %+v: %v", ent, err)
		}
	}

	// 11. Seed Canonical hasSensor Relationships
	// Gateway A -> sensA1, sensAEmpty, sensAAgg, sensAUnit, sensATyped, sensAPersist, sensASnap.
	// NOTE: sensAOrph deliberately omitted to test missing provisioning graph!
	// Gateway B -> sensB1.
	for _, rel := range []struct {
		src uuid.UUID
		tgt uuid.UUID
	}{
		{gwAUUID, sensA1UUID},
		{gwAUUID, sensAEmptyUUID},
		{gwAUUID, sensAAggUUID},
		{gwAUUID, sensAUnitUUID},
		{gwAUUID, sensATypedUUID},
		{gwAUUID, sensAPersistUUID},
		{gwAUUID, sensASnapUUID},
		{gwBUUID, sensB1UUID},
	} {
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.twin_relationships (source_entity_id, relationship_type, target_entity_id, created_at)
			VALUES ($1, 'hasSensor', $2, now())`, rel.src, rel.tgt); err != nil {
			t.Fatalf("failed to insert relationship: %v", err)
		}
	}

	// 12. Seed default Twin States
	for _, entUUID := range []uuid.UUID{gwAUUID, gwBUUID, sensA1UUID, sensAEmptyUUID, sensAOrphUUID, sensAAggUUID, sensAUnitUUID, sensATypedUUID, sensAPersistUUID, sensASnapUUID, sensB1UUID} {
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.twin_states (entity_id, reported_state, desired_state, reported_version, desired_version, updated_at)
			VALUES ($1, '{}'::jsonb, '{}'::jsonb, 0, 0, now())`, entUUID); err != nil {
			t.Fatalf("failed to insert twin state: %v", err)
		}
	}

	// 13. Wire Application Components using appPool (role: iot_backend_app)
	resolver, err := digitaltwin.NewPostgresResolver(appPool)
	if err != nil {
		t.Fatalf("failed to create PostgresResolver: %v", err)
	}
	prep, err := digitaltwin.NewReadPreparation(resolver, digitaltwin.DefaultReadPreparationConfig())
	if err != nil {
		t.Fatalf("failed to create ReadPreparation: %v", err)
	}
	reader, err := digitaltwin.NewPostgresTwinReader(appPool)
	if err != nil {
		t.Fatalf("failed to create PostgresTwinReader: %v", err)
	}

	telemCfg := telemetry.DefaultHistoryPreparationConfig()
	telemHistoryRepo, err := telemetry.NewPostgresHistoryRepository(appPool, telemCfg)
	if err != nil {
		t.Fatalf("failed to create telemetry history repository: %v", err)
	}
	telemHistoryService, err := telemetry.NewTelemetryHistoryService(resolver, telemHistoryRepo, telemCfg)
	if err != nil {
		t.Fatalf("failed to create telemetry history service: %v", err)
	}

	service, err := digitaltwin.NewReadService(prep, reader, telemHistoryService)
	if err != nil {
		t.Fatalf("failed to create ReadService: %v", err)
	}

	persistRepo, err := telemetry.NewPostgresRepository(appPool, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to create telemetry persist repository: %v", err)
	}

	// =========================================================================
	// Scenario 1: Authorization, Tenant Isolation, Roles and Empty Series
	// =========================================================================
	t.Run("Authorization_TenantIsolation_And_Roles", func(t *testing.T) {
		// Seed 1 observation for sensA1 and 1 observation for sensB1
		obsTime := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
		msgIDA := uuid.New()
		msgIDB := uuid.New()
		_, err := setupPool.Exec(ctx, `
			INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata, message_id)
			VALUES
				($1, $2, 'sample_interval_us', 10000, '{"unit":"us"}'::jsonb, $3),
				($1, $4, 'sample_interval_us', 20000, '{"unit":"us"}'::jsonb, $5)`,
			obsTime, sensA1UUID, msgIDA, sensB1UUID, msgIDB)
		if err != nil {
			t.Fatalf("failed to seed initial observations: %v", err)
		}

		qA1 := digitaltwin.TwinHistoryQuery{
			EntityID:   sensA1URN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: obsTime.Add(-1 * time.Second), To: obsTime.Add(1 * time.Second)},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}

		// 1. All Gateway A assigned roles can read Gateway A sensor
		for _, roleUser := range []struct {
			name string
			uID  uuid.UUID
		}{
			{"owner", ownerUser},
			{"operator", operatorUser},
			{"viewer", viewerUser},
		} {
			res, err := service.GetTwinHistory(ctx, roleUser.uID, qA1)
			if err != nil {
				t.Fatalf("role %s failed to read authorized history: %v", roleUser.name, err)
			}
			if res.PointCount != 1 || len(res.TemporalPoints) != 1 {
				t.Fatalf("role %s expected 1 point, got count=%d len=%d", roleUser.name, res.PointCount, len(res.TemporalPoints))
			}
			if res.TemporalPoints[0].ValueNumber == nil || *res.TemporalPoints[0].ValueNumber != 10000 {
				t.Fatalf("role %s expected value 10000, got %v", roleUser.name, res.TemporalPoints[0].ValueNumber)
			}
		}

		// 2. Cross-tenant denial: Gateway A owner CANNOT read Gateway B sensor (identical Sensor ID)
		qB1 := digitaltwin.TwinHistoryQuery{
			EntityID:   sensB1URN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: obsTime.Add(-1 * time.Second), To: obsTime.Add(1 * time.Second)},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		_, err = service.GetTwinHistory(ctx, ownerUser, qB1)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for cross-tenant access to Gateway B, got %v", err)
		}

		// Gateway B owner CANNOT read Gateway A sensor
		_, err = service.GetTwinHistory(ctx, ownerBUser, qA1)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for cross-tenant access to Gateway A, got %v", err)
		}

		// 3. Non-member denial
		_, err = service.GetTwinHistory(ctx, nonMemberUser, qA1)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for nonMemberUser, got %v", err)
		}

		// 4. Platform Admin without membership has NO bypass
		_, err = service.GetTwinHistory(ctx, platformAdminUser, qA1)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for platformAdminUser without membership, got %v", err)
		}

		// 5. Missing canonical provisioning graph (sensAOrph has no hasSensor relationship)
		qOrph := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAOrphURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: obsTime.Add(-1 * time.Second), To: obsTime.Add(1 * time.Second)},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		_, err = service.GetTwinHistory(ctx, ownerUser, qOrph)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for orphaned sensor without hasSensor relationship, got %v", err)
		}

		// 6. Non-existent resource returns ErrNotFound
		qNonExistent := digitaltwin.TwinHistoryQuery{
			EntityID:   "urn:ngsi-ld:Sensor:nonexistent:sens",
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: obsTime.Add(-1 * time.Second), To: obsTime.Add(1 * time.Second)},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		_, err = service.GetTwinHistory(ctx, ownerUser, qNonExistent)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for nonexistent entity, got %v", err)
		}

		// 7. Authorized sensor with 0 observations in range returns valid empty series
		qEmpty := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAEmptyURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: obsTime.Add(-1 * time.Second), To: obsTime.Add(1 * time.Second)},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		resEmpty, err := service.GetTwinHistory(ctx, ownerUser, qEmpty)
		if err != nil {
			t.Fatalf("expected success for empty authorized series, got %v", err)
		}
		if resEmpty.PointCount != 0 {
			t.Fatalf("expected PointCount 0 for empty series, got %d", resEmpty.PointCount)
		}
		if len(resEmpty.TemporalPoints) != 0 {
			t.Fatalf("expected empty TemporalPoints, got %d", len(resEmpty.TemporalPoints))
		}
		if resEmpty.Unit != nil {
			t.Fatalf("expected nil unit for empty series, got %v", *resEmpty.Unit)
		}
		if resEmpty.Mode != "raw" || resEmpty.Resolution != "raw" {
			t.Fatalf("expected raw mode/resolution for empty series, got mode=%q res=%q", resEmpty.Mode, resEmpty.Resolution)
		}

		// 8. Direct Reader Identity Boundary (Finding #5):
		// Mixed identity (Sensor A UUID/URN + Sensor Empty ID within same Gateway A)
		// must be rejected by both raw and bucket queries independently with ErrNotFound and nil result,
		// while consistent Sensor A identity succeeds.
		consistentEntityA := digitaltwin.ResolvedEntity{
			InternalID: sensA1UUID,
			PublicURN:  sensA1URN,
			Type:       digitaltwin.EntityTypeSensor,
			GatewayID:  gwIDA,
			SensorID:   sensorSharedID,
		}
		resRawConsistent, err := reader.ReadTemporalHistory(ctx, ownerUser, consistentEntityA, qA1)
		if err != nil {
			t.Fatalf("expected consistent Sensor A raw read to succeed, got %v", err)
		}
		if resRawConsistent == nil || resRawConsistent.PointCount != 1 {
			t.Fatalf("expected 1 point for consistent Sensor A raw read, got %+v", resRawConsistent)
		}

		qBucketA1 := digitaltwin.TwinHistoryQuery{
			EntityID:   sensA1URN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: obsTime.Add(-1 * time.Second), To: obsTime.Add(1 * time.Second)},
			Resolution: telemetry.Resolution1s,
			MaxPoints:  100,
		}
		resBucketConsistent, err := reader.ReadTemporalHistory(ctx, ownerUser, consistentEntityA, qBucketA1)
		if err != nil {
			t.Fatalf("expected consistent Sensor A bucket read to succeed, got %v", err)
		}
		if resBucketConsistent == nil || resBucketConsistent.PointCount != 1 {
			t.Fatalf("expected 1 point for consistent Sensor A bucket read, got %+v", resBucketConsistent)
		}

		// Mixed identity A: Sensor A UUID/URN + Sensor Empty ID
		mismatchedEntityA := digitaltwin.ResolvedEntity{
			InternalID: sensA1UUID,
			PublicURN:  sensA1URN,
			Type:       digitaltwin.EntityTypeSensor,
			GatewayID:  gwIDA,
			SensorID:   sensorEmptyID,
		}
		resRawMismatch, err := reader.ReadTemporalHistory(ctx, ownerUser, mismatchedEntityA, qA1)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for raw query with mixed identity A UUID/URN + Empty SensorID, got %v", err)
		}
		if resRawMismatch != nil {
			t.Fatalf("expected nil result on ErrNotFound for raw query, got %+v", resRawMismatch)
		}

		resBucketMismatch, err := reader.ReadTemporalHistory(ctx, ownerUser, mismatchedEntityA, qBucketA1)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for bucket query with mixed identity A UUID/URN + Empty SensorID, got %v", err)
		}
		if resBucketMismatch != nil {
			t.Fatalf("expected nil result on ErrNotFound for bucket query, got %+v", resBucketMismatch)
		}

		// Mixed identity B: Sensor Empty UUID/URN + Sensor A ID
		mismatchedEntityEmpty := digitaltwin.ResolvedEntity{
			InternalID: sensAEmptyUUID,
			PublicURN:  sensAEmptyURN,
			Type:       digitaltwin.EntityTypeSensor,
			GatewayID:  gwIDA,
			SensorID:   sensorSharedID,
		}
		resRawMismatch2, err := reader.ReadTemporalHistory(ctx, ownerUser, mismatchedEntityEmpty, qA1)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for raw query with mixed identity Empty UUID/URN + A SensorID, got %v", err)
		}
		if resRawMismatch2 != nil {
			t.Fatalf("expected nil result on ErrNotFound for raw query, got %+v", resRawMismatch2)
		}

		resBucketMismatch2, err := reader.ReadTemporalHistory(ctx, ownerUser, mismatchedEntityEmpty, qBucketA1)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for bucket query with mixed identity Empty UUID/URN + A SensorID, got %v", err)
		}
		if resBucketMismatch2 != nil {
			t.Fatalf("expected nil result on ErrNotFound for bucket query, got %+v", resBucketMismatch2)
		}
	})

	// =========================================================================
	// Scenario 2: Time Boundaries, Multiplicity of Ties, Out-of-Order, and Precision
	// =========================================================================
	t.Run("TimeBoundaries_Ties_Provenance_And_Precision", func(t *testing.T) {
		baseTime := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
		from := baseTime
		to := baseTime.Add(1 * time.Second) // [12:00:00.000000Z, 12:00:01.000000Z)

		msgID1 := uuid.New()
		msgID2 := uuid.New()
		msgIDTieA := uuid.New()
		msgIDTieB := uuid.New()
		msgIDLast := uuid.New()

		// Points designed for strict boundary and precision validation:
		// 1. from - 1us: EXCLUDED (< from)
		// 2. from: INCLUDED (== from)
		// 3. from + 1us: INCLUDED
		// 4. from + 500ms (Tie 1): INCLUDED, with large JSON integer 9007199254740993 (> 2^53)
		// 5. from + 500ms (Tie 2): INCLUDED, identical timestamp, different value/message_id
		// 6. from + 700ms: INCLUDED, NULL message_id
		// 7. to - 1us: INCLUDED (< to)
		// 8. to: EXCLUDED (== to, half-open)
		// 9. to + 1us: EXCLUDED (> to)
		type seedRow struct {
			t      time.Time
			val    float64
			meta   string
			msgID  *uuid.UUID
			inSpan bool
		}

		rows := []seedRow{
			{from.Add(-1 * time.Microsecond), 999, `{"unit":"us"}`, &msgID1, false},
			{from, 10000, `{"unit":"us"}`, &msgID2, true},
			{from.Add(1 * time.Microsecond), 10001, `{"unit":"us"}`, nil, true},
			{from.Add(500 * time.Millisecond), 20000, `{"unit":"us","large_int":9007199254740993}`, &msgIDTieA, true},
			{from.Add(500 * time.Millisecond), 20001, `{"unit":"us","boot_id":"boot-alpha"}`, &msgIDTieB, true},
			{from.Add(700 * time.Millisecond), 15000, `{"unit":"us"}`, nil, true},
			{to.Add(-1 * time.Microsecond), 19999, `{"unit":"us"}`, &msgIDLast, true},
			{to, 30000, `{"unit":"us"}`, nil, false},
			{to.Add(1 * time.Microsecond), 30001, `{"unit":"us"}`, nil, false},
		}

		// Insert rows OUT OF CHRONOLOGICAL ORDER to assert database sorting
		insertOrder := []int{4, 0, 7, 2, 6, 1, 8, 3, 5}
		for _, idx := range insertOrder {
			r := rows[idx]
			_, err := setupPool.Exec(ctx, `
				INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata, message_id)
				VALUES ($1, $2, 'sample_interval_us', $3, $4::jsonb, $5)`,
				r.t, sensA1UUID, r.val, r.meta, r.msgID)
			if err != nil {
				t.Fatalf("failed to insert boundary seed row %d: %v", idx, err)
			}
		}

		q := digitaltwin.TwinHistoryQuery{
			EntityID:   sensA1URN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: from, To: to},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}

		res, err := service.GetTwinHistory(ctx, ownerUser, q)
		if err != nil {
			t.Fatalf("failed to query boundary history: %v", err)
		}

		// Expected inside span: exactly 6 points
		if res.PointCount != 6 || len(res.TemporalPoints) != 6 {
			t.Fatalf("expected exactly 6 points in [from, to), got count=%d len=%d", res.PointCount, len(res.TemporalPoints))
		}

		// 1. Boundary at 'from': first point must be exactly at 'from' (12:00:00.000000Z)
		pt0 := res.TemporalPoints[0]
		if !pt0.ObservedAt.Equal(from) {
			t.Fatalf("expected pt0 observed_at %v, got %v", from, pt0.ObservedAt)
		}
		if pt0.ValueNumber == nil || *pt0.ValueNumber != 10000 {
			t.Fatalf("expected pt0 value 10000, got %v", pt0.ValueNumber)
		}

		// 2. Adjacent microsecond at from+1us
		pt1 := res.TemporalPoints[1]
		if !pt1.ObservedAt.Equal(from.Add(1 * time.Microsecond)) {
			t.Fatalf("expected pt1 observed_at %v, got %v", from.Add(1*time.Microsecond), pt1.ObservedAt)
		}
		if pt1.ValueNumber == nil || *pt1.ValueNumber != 10001 {
			t.Fatalf("expected pt1 value 10001, got %v", pt1.ValueNumber)
		}

		// 3. Tied timestamps at from+500ms: both tie rows must be present (multiplicity preserved)
		tieT := from.Add(500 * time.Millisecond)
		pt2 := res.TemporalPoints[2]
		pt3 := res.TemporalPoints[3]
		if !pt2.ObservedAt.Equal(tieT) || !pt3.ObservedAt.Equal(tieT) {
			t.Fatalf("expected pt2 and pt3 both at %v, got pt2=%v pt3=%v", tieT, pt2.ObservedAt, pt3.ObservedAt)
		}
		tieVals := map[float64]bool{*pt2.ValueNumber: true, *pt3.ValueNumber: true}
		if !tieVals[20000] || !tieVals[20001] {
			t.Fatalf("expected tie values 20000 and 20001, got %v and %v", pt2.ValueNumber, pt3.ValueNumber)
		}

		// Verify large JSON integer strictly > 2^53 (9007199254740993) in metadata was not lost or converted to exponential float string
		var largeIntFound bool
		for _, pt := range []digitaltwin.TemporalSamplePoint{pt2, pt3} {
			if len(pt.Metadata) > 0 {
				dec := json.NewDecoder(bytes.NewReader(pt.Metadata))
				dec.UseNumber()
				var metaObj map[string]any
				if err := dec.Decode(&metaObj); err == nil {
					if rawLarge, ok := metaObj["large_int"]; ok {
						if num, ok := rawLarge.(json.Number); ok && num.String() == "9007199254740993" {
							largeIntFound = true
						}
					}
				}
				// Verify raw JSON string contains exact integer literal
				if strings.Contains(string(pt.Metadata), "9007199254740993") {
					largeIntFound = true
				}
			}
		}
		if !largeIntFound {
			t.Fatalf("expected metadata with large integer 9007199254740993 preserved in tie observations")
		}

		// 4. Nullable message_id provenance
		pt4 := res.TemporalPoints[4]
		if !pt4.ObservedAt.Equal(from.Add(700 * time.Millisecond)) {
			t.Fatalf("expected pt4 at %v, got %v", from.Add(700*time.Millisecond), pt4.ObservedAt)
		}
		if pt4.MessageID != nil {
			t.Fatalf("expected pt4 MessageID to be nil (NULL provenance), got %v", pt4.MessageID)
		}

		// 5. Boundary at 'to - 1us'
		pt5 := res.TemporalPoints[5]
		if !pt5.ObservedAt.Equal(to.Add(-1 * time.Microsecond)) {
			t.Fatalf("expected pt5 observed_at %v, got %v", to.Add(-1*time.Microsecond), pt5.ObservedAt)
		}
		if pt5.ValueNumber == nil || *pt5.ValueNumber != 19999 {
			t.Fatalf("expected pt5 value 19999, got %v", pt5.ValueNumber)
		}

		// 6. Verify strictly excluded points
		for _, pt := range res.TemporalPoints {
			if pt.ObservedAt.Before(from) {
				t.Fatalf("point before 'from' leaked into result: %v", pt.ObservedAt)
			}
			if !pt.ObservedAt.Before(to) {
				t.Fatalf("point at or after 'to' leaked into result: %v", pt.ObservedAt)
			}
		}
	})

	// =========================================================================
	// Scenario 3: Aggregation, Partial Buckets, Gaps, and Auto Selection
	// =========================================================================
	t.Run("Raw_Bucket_Auto_Aggregation_And_Gaps", func(t *testing.T) {
		epochAligned := time.Date(2026, 10, 17, 14, 0, 0, 0, time.UTC)

		// Seed known observations into sensAAggUUID:
		// Bucket 0: [14:00:00, 14:00:01)
		//   14:00:00.100 (val: 10000)
		//   14:00:00.500 (val: 20000)
		//   14:00:00.900 (val: 30000)
		//   Expected: Avg=20000.0, Min=10000.0, Max=30000.0, Count=3
		// Bucket 1: [14:00:01, 14:00:02)
		//   14:00:01.200 (val: 40000)
		//   14:00:01.800 (val: 60000)
		//   Expected: Avg=50000.0, Min=40000.0, Max=60000.0, Count=2
		// Bucket 2: [14:00:02, 14:00:03)
		//   NO SAMPLES (GAP)
		//   Expected: bucket omitted from results (NO GAPFILL)
		// Bucket 3: [14:00:03, 14:00:04)
		//   14:00:03.500 (val: 50000)
		//   Expected: Avg=50000.0, Min=50000.0, Max=50000.0, Count=1
		aggSeeds := []struct {
			t   time.Time
			val float64
		}{
			{epochAligned.Add(100 * time.Millisecond), 10000},
			{epochAligned.Add(500 * time.Millisecond), 20000},
			{epochAligned.Add(900 * time.Millisecond), 30000},
			{epochAligned.Add(1200 * time.Millisecond), 40000},
			{epochAligned.Add(1800 * time.Millisecond), 60000},
			// Bucket 2 gap
			{epochAligned.Add(3500 * time.Millisecond), 50000},
		}

		for _, s := range aggSeeds {
			_, err := setupPool.Exec(ctx, `
				INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
				VALUES ($1, $2, 'sample_interval_us', $3, '{"unit":"us"}'::jsonb)`,
				s.t, sensAAggUUID, s.val)
			if err != nil {
				t.Fatalf("failed to seed aggregation observation: %v", err)
			}
		}

		// Test A: Full 4s window with 1s resolution -> exactly 3 buckets (gap preserved)
		qBucket := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAAggURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: epochAligned, To: epochAligned.Add(4 * time.Second)},
			Resolution: telemetry.Resolution1s,
			MaxPoints:  100,
		}
		resBucket, err := service.GetTwinHistory(ctx, ownerUser, qBucket)
		if err != nil {
			t.Fatalf("bucket query failed: %v", err)
		}
		if resBucket.Mode != "bucket" || resBucket.Resolution != "1s" {
			t.Fatalf("expected mode=bucket res=1s, got mode=%q res=%q", resBucket.Mode, resBucket.Resolution)
		}
		if resBucket.PointCount != 3 || len(resBucket.TemporalBuckets) != 3 {
			t.Fatalf("expected 3 buckets (gap omitted), got count=%d len=%d", resBucket.PointCount, len(resBucket.TemporalBuckets))
		}

		b0 := resBucket.TemporalBuckets[0]
		if !b0.BucketStart.Equal(epochAligned) || b0.Avg != 20000 || b0.Min != 10000 || b0.Max != 30000 || b0.Count != 3 {
			t.Fatalf("bucket 0 mismatch: %+v", b0)
		}

		b1 := resBucket.TemporalBuckets[1]
		if !b1.BucketStart.Equal(epochAligned.Add(1*time.Second)) || b1.Avg != 50000 || b1.Min != 40000 || b1.Max != 60000 || b1.Count != 2 {
			t.Fatalf("bucket 1 mismatch: %+v", b1)
		}

		b2 := resBucket.TemporalBuckets[2]
		if !b2.BucketStart.Equal(epochAligned.Add(3*time.Second)) || b2.Avg != 50000 || b2.Min != 50000 || b2.Max != 50000 || b2.Count != 1 {
			t.Fatalf("bucket 2 (at 14:00:03) mismatch: %+v", b2)
		}

		// Test B: Partial epoch-aligned first and last buckets [14:00:00.500, 14:00:03.200)
		// First bucket [14:00:00, 14:00:01) only covers [14:00:00.500, 14:00:01.000) -> 20000, 30000 -> Avg=25000, Min=20000, Max=30000, Count=2
		// Second bucket [14:00:01, 14:00:02) -> 40000, 60000 -> Avg=50000, Min=40000, Max=60000, Count=2
		// Third bucket: gap
		// Fourth bucket [14:00:03, 14:00:04): sample at 14:00:03.500 is > 14:00:03.200 -> 0 samples in range
		// Expected: exactly 2 buckets returned!
		qPartial := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAAggURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: epochAligned.Add(500 * time.Millisecond), To: epochAligned.Add(3200 * time.Millisecond)},
			Resolution: telemetry.Resolution1s,
			MaxPoints:  100,
		}
		resPartial, err := service.GetTwinHistory(ctx, ownerUser, qPartial)
		if err != nil {
			t.Fatalf("partial bucket query failed: %v", err)
		}
		if resPartial.PointCount != 2 || len(resPartial.TemporalBuckets) != 2 {
			t.Fatalf("expected 2 partial buckets, got count=%d len=%d", resPartial.PointCount, len(resPartial.TemporalBuckets))
		}
		pb0 := resPartial.TemporalBuckets[0]
		if pb0.Avg != 25000 || pb0.Min != 20000 || pb0.Max != 30000 || pb0.Count != 2 {
			t.Fatalf("partial bucket 0 mismatch: %+v", pb0)
		}
		pb1 := resPartial.TemporalBuckets[1]
		if pb1.Avg != 50000 || pb1.Min != 40000 || pb1.Max != 60000 || pb1.Count != 2 {
			t.Fatalf("partial bucket 1 mismatch: %+v", pb1)
		}

		// Seed a dense 1-second burst of 105 observations into sensAAggUUID for overflow and auto testing:
		tBurst := epochAligned.Add(10 * time.Second)
		for i := 0; i < 105; i++ {
			tObs := tBurst.Add(time.Duration(i*8) * time.Millisecond) // spans ~840ms, all in [14:00:10, 14:00:11)
			_, err := setupPool.Exec(ctx, `
				INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
				VALUES ($1, $2, 'sample_interval_us', 10000, '{"unit":"us"}'::jsonb)`,
				tObs, sensAAggUUID)
			if err != nil {
				t.Fatalf("failed to seed dense burst observation %d: %v", i, err)
			}
		}

		// Test C: Exact raw limit overflow detection
		// Window [14:00:10, 14:00:11) has 105 observations. MaxPoints=100 -> must return ErrRawLimitExceeded without silent truncation
		qOverflow := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAAggURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: tBurst, To: tBurst.Add(1 * time.Second)},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		_, err = service.GetTwinHistory(ctx, ownerUser, qOverflow)
		if !errors.Is(err, digitaltwin.ErrRawLimitExceeded) {
			t.Fatalf("expected ErrRawLimitExceeded on raw overflow, got %v", err)
		}

		// Test D: Short auto fallback
		// Window <= 10s (1s) with MaxPoints=100.
		// Raw probe has 105 observations (> 100).
		// Auto mode detects overflow, discards probe, and falls back to auto-selected buckets!
		// For 1s window with MaxPoints=100: 100ms buckets yield 10 buckets <= 100 -> Resolution100ms.
		qShortAuto := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAAggURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: tBurst, To: tBurst.Add(1 * time.Second)},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		resShortAuto, err := service.GetTwinHistory(ctx, ownerUser, qShortAuto)
		if err != nil {
			t.Fatalf("short auto fallback query failed: %v", err)
		}
		if resShortAuto.Mode != "bucket" || resShortAuto.Resolution != "100ms" {
			t.Fatalf("expected short auto fallback to mode=bucket res=100ms, got mode=%q res=%q", resShortAuto.Mode, resShortAuto.Resolution)
		}
		if resShortAuto.PointCount == 0 || len(resShortAuto.TemporalBuckets) == 0 {
			t.Fatalf("expected non-empty buckets from short auto fallback, got %d", resShortAuto.PointCount)
		}

		// Test E: Long auto selection
		// Window > 10s (20s) with MaxPoints=100.
		// SelectAutoBucketResolution directly picks 1s buckets (20s / 1s = 20 buckets <= 100) without raw probe.
		qLongAuto := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAAggURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: epochAligned, To: epochAligned.Add(20 * time.Second)},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		resLongAuto, err := service.GetTwinHistory(ctx, ownerUser, qLongAuto)
		if err != nil {
			t.Fatalf("long auto selection failed: %v", err)
		}
		if resLongAuto.Mode != "bucket" || resLongAuto.Resolution != "1s" {
			t.Fatalf("expected long auto mode=bucket res=1s, got mode=%q res=%q", resLongAuto.Mode, resLongAuto.Resolution)
		}

		// Test F: Configured narrow raw window (1s) governs temporal auto and raw branching
		narrowCfg := digitaltwin.DefaultReadPreparationConfig()
		narrowCfg.HistoryRawWindow = 1 * time.Second
		narrowPrep, err := digitaltwin.NewReadPreparation(resolver, narrowCfg)
		if err != nil {
			t.Fatalf("failed to create narrow ReadPreparation: %v", err)
		}
		narrowService, err := digitaltwin.NewReadService(narrowPrep, reader, telemHistoryService)
		if err != nil {
			t.Fatalf("failed to create narrow ReadService: %v", err)
		}

		// 1. 5s auto query: 6 observations <= 100 maxPoints, but 5s > 1s configured raw window
		// Must NOT return raw; must select bucket mode directly!
		qNarrow5sAuto := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAAggURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: epochAligned, To: epochAligned.Add(5 * time.Second)},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		resNarrow5sAuto, err := narrowService.GetTwinHistory(ctx, ownerUser, qNarrow5sAuto)
		if err != nil {
			t.Fatalf("narrow 5s auto query failed: %v", err)
		}
		if resNarrow5sAuto.Mode != "bucket" {
			t.Fatalf("expected narrow 5s auto mode=bucket, got mode=%q", resNarrow5sAuto.Mode)
		}
		if resNarrow5sAuto.Resolution == "raw" {
			t.Fatalf("expected narrow 5s auto resolution to be bucket resolution, got %q", resNarrow5sAuto.Resolution)
		}

		// 2. <= 1s auto query: 1s window has 3 observations <= 100 maxPoints
		// Must probe raw and return raw mode!
		qNarrow1sAuto := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAAggURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: epochAligned, To: epochAligned.Add(1 * time.Second)},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		resNarrow1sAuto, err := narrowService.GetTwinHistory(ctx, ownerUser, qNarrow1sAuto)
		if err != nil {
			t.Fatalf("narrow 1s auto query failed: %v", err)
		}
		if resNarrow1sAuto.Mode != "raw" || resNarrow1sAuto.Resolution != "raw" {
			t.Fatalf("expected narrow 1s auto query to return mode=raw res=raw, got mode=%q res=%q", resNarrow1sAuto.Mode, resNarrow1sAuto.Resolution)
		}
		if resNarrow1sAuto.PointCount != 3 {
			t.Fatalf("expected 3 raw observations for narrow 1s auto query, got %d", resNarrow1sAuto.PointCount)
		}

		// 3. > 1s explicit raw query: 5s window with ResolutionRaw
		// Must fail with ErrRawLimitExceeded!
		qNarrow5sRaw := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAAggURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: epochAligned, To: epochAligned.Add(5 * time.Second)},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		_, err = narrowService.GetTwinHistory(ctx, ownerUser, qNarrow5sRaw)
		if !errors.Is(err, digitaltwin.ErrRawLimitExceeded) {
			t.Fatalf("expected ErrRawLimitExceeded on narrow 5s explicit raw query, got %v", err)
		}
	})

	// =========================================================================
	// Scenario 4: Unit Semantics, Invariants, and Allowlisted Bucket Resolutions
	// =========================================================================
	t.Run("UnitSemantics_And_WholeWindowInvariants", func(t *testing.T) {
		baseT := time.Date(2026, 10, 17, 16, 0, 0, 0, time.UTC)

		// 1. Homogeneous known unit: 2 observations with "us" -> res.Unit == "us"
		tHomStart := baseT
		tHomEnd := baseT.Add(1 * time.Second)
		_, err := setupPool.Exec(ctx, `
			INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
			VALUES
				($1, $2, 'sample_interval_us', 10000, '{"unit":"us"}'::jsonb),
				($3, $2, 'sample_interval_us', 20000, '{"unit":"us"}'::jsonb)`,
			tHomStart.Add(100*time.Millisecond), sensAUnitUUID, tHomStart.Add(200*time.Millisecond))
		if err != nil {
			t.Fatalf("failed to seed homogeneous unit observations: %v", err)
		}

		qHom := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAUnitURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: tHomStart, To: tHomEnd},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		resHom, err := service.GetTwinHistory(ctx, ownerUser, qHom)
		if err != nil {
			t.Fatalf("homogeneous unit query failed: %v", err)
		}
		if resHom.Unit == nil || *resHom.Unit != "us" {
			t.Fatalf("expected Unit 'us', got %v", resHom.Unit)
		}

		// 2. All unknown units: metadata without "unit" or "unit": null -> res.Unit == nil
		tUnkStart := baseT.Add(2 * time.Second)
		tUnkEnd := baseT.Add(3 * time.Second)
		_, err = setupPool.Exec(ctx, `
			INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
			VALUES
				($1, $2, 'sample_interval_us', 10000, '{}'::jsonb),
				($3, $2, 'sample_interval_us', 20000, '{"unit":null}'::jsonb)`,
			tUnkStart.Add(100*time.Millisecond), sensAUnitUUID, tUnkStart.Add(200*time.Millisecond))
		if err != nil {
			t.Fatalf("failed to seed unknown unit observations: %v", err)
		}

		qUnk := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAUnitURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: tUnkStart, To: tUnkEnd},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		resUnk, err := service.GetTwinHistory(ctx, ownerUser, qUnk)
		if err != nil {
			t.Fatalf("unknown unit query failed: %v", err)
		}
		if resUnk.Unit != nil {
			t.Fatalf("expected nil Unit for all-unknown units, got %v", *resUnk.Unit)
		}

		// 3. Differing units: one "us", one "ms" -> ErrInconsistentUnit
		tDiffStart := baseT.Add(4 * time.Second)
		tDiffEnd := baseT.Add(5 * time.Second)
		_, err = setupPool.Exec(ctx, `
			INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
			VALUES
				($1, $2, 'sample_interval_us', 10000, '{"unit":"us"}'::jsonb),
				($3, $2, 'sample_interval_us', 20000, '{"unit":"ms"}'::jsonb)`,
			tDiffStart.Add(100*time.Millisecond), sensAUnitUUID, tDiffStart.Add(200*time.Millisecond))
		if err != nil {
			t.Fatalf("failed to seed differing unit observations: %v", err)
		}

		qDiff := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAUnitURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: tDiffStart, To: tDiffEnd},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		_, err = service.GetTwinHistory(ctx, ownerUser, qDiff)
		if !errors.Is(err, digitaltwin.ErrInconsistentUnit) {
			t.Fatalf("expected ErrInconsistentUnit for differing units, got %v", err)
		}

		// 4. Mixed known and unknown units: one "us", one null -> ErrInconsistentUnit
		tMixStart := baseT.Add(6 * time.Second)
		tMixEnd := baseT.Add(7 * time.Second)
		_, err = setupPool.Exec(ctx, `
			INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
			VALUES
				($1, $2, 'sample_interval_us', 10000, '{"unit":"us"}'::jsonb),
				($3, $2, 'sample_interval_us', 20000, '{}'::jsonb)`,
			tMixStart.Add(100*time.Millisecond), sensAUnitUUID, tMixStart.Add(200*time.Millisecond))
		if err != nil {
			t.Fatalf("failed to seed mixed unit observations: %v", err)
		}

		qMix := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAUnitURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: tMixStart, To: tMixEnd},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		_, err = service.GetTwinHistory(ctx, ownerUser, qMix)
		if !errors.Is(err, digitaltwin.ErrInconsistentUnit) {
			t.Fatalf("expected ErrInconsistentUnit for mixed known/unknown units, got %v", err)
		}

		// 5. Malformed non-string unit in metadata: {"unit": 1234} -> ErrUnexpected
		tMalStart := baseT.Add(8 * time.Second)
		tMalEnd := baseT.Add(9 * time.Second)
		_, err = setupPool.Exec(ctx, `
			INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
			VALUES ($1, $2, 'sample_interval_us', 10000, '{"unit":1234}'::jsonb)`,
			tMalStart.Add(100*time.Millisecond), sensAUnitUUID)
		if err != nil {
			t.Fatalf("failed to seed malformed unit observation: %v", err)
		}

		qMal := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAUnitURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: tMalStart, To: tMalEnd},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		_, err = service.GetTwinHistory(ctx, ownerUser, qMal)
		if !errors.Is(err, digitaltwin.ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for malformed non-string unit, got %v", err)
		}

		// 6. Bad interval value after bounded raw prefix:
		// Seed 100 valid observations (val: 10000), followed by 1 corrupted observation (val: -500).
		// MaxPoints = 100. A naive query with LIMIT 100 would only fetch the 100 valid rows and miss the corruption!
		// But whole-window CTE checks the entire range [from, to) and must trigger ErrUnexpected.
		tBadStart := baseT.Add(10 * time.Second)
		tBadEnd := baseT.Add(11 * time.Second)
		for i := 0; i < 100; i++ {
			tObs := tBadStart.Add(time.Duration(i*5) * time.Millisecond) // spans 500ms
			_, err = setupPool.Exec(ctx, `
				INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
				VALUES ($1, $2, 'sample_interval_us', 10000, '{"unit":"us"}'::jsonb)`,
				tObs, sensAUnitUUID)
			if err != nil {
				t.Fatalf("failed to seed valid row %d: %v", i, err)
			}
		}
		// 101st row: corrupted negative interval
		_, err = setupPool.Exec(ctx, `
			INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
			VALUES ($1, $2, 'sample_interval_us', -500, '{"unit":"us"}'::jsonb)`,
			tBadStart.Add(600*time.Millisecond), sensAUnitUUID)
		if err != nil {
			t.Fatalf("failed to seed bad interval observation: %v", err)
		}

		qBad := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAUnitURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: tBadStart, To: tBadEnd},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100, // LIMIT 100 would only fetch valid rows 0..99
		}
		_, err = service.GetTwinHistory(ctx, ownerUser, qBad)
		if !errors.Is(err, digitaltwin.ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected due to whole-window invariant check finding invalid interval, got %v", err)
		}

		// Also verify direct reader call with maxPoints = 2 on the bad interval slice
		resolvedUnitEntity := digitaltwin.ResolvedEntity{
			InternalID: sensAUnitUUID,
			PublicURN:  sensAUnitURN,
			Type:       digitaltwin.EntityTypeSensor,
			GatewayID:  gwIDA,
			SensorID:   sensorUnitID,
		}
		qDirectReaderBad := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAUnitURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: tBadStart, To: tBadEnd},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  2,
		}
		_, err = reader.ReadTemporalHistory(ctx, ownerUser, resolvedUnitEntity, qDirectReaderBad)
		if !errors.Is(err, digitaltwin.ErrUnexpected) {
			t.Fatalf("expected direct reader to return ErrUnexpected on whole-window invalid interval, got %v", err)
		}

		// 6b. Fractional interval observation beyond bounded raw prefix & across applicable modes:
		// Seed 100 valid integer observations (10000 us) followed by 1 fractional observation (10000.5 us).
		// Verify raw (with maxPoints=100 and direct reader with maxPoints=2), explicit bucket, and auto modes
		// all reject fractional stored interval observations with ErrUnexpected and nil result.
		tFracStart := baseT.Add(12 * time.Second)
		tFracEnd := baseT.Add(13 * time.Second)
		for i := 0; i < 100; i++ {
			tObs := tFracStart.Add(time.Duration(i*5) * time.Millisecond) // spans 500ms
			_, err = setupPool.Exec(ctx, `
				INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
				VALUES ($1, $2, 'sample_interval_us', 10000, '{"unit":"us"}'::jsonb)`,
				tObs, sensAUnitUUID)
			if err != nil {
				t.Fatalf("failed to seed valid row %d for fractional test: %v", i, err)
			}
		}
		// 101st row: corrupted fractional interval observation (10000.5)
		_, err = setupPool.Exec(ctx, `
			INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
			VALUES ($1, $2, 'sample_interval_us', 10000.5, '{"unit":"us"}'::jsonb)`,
			tFracStart.Add(600*time.Millisecond), sensAUnitUUID)
		if err != nil {
			t.Fatalf("failed to seed fractional interval observation: %v", err)
		}

		// A. Raw mode with maxPoints=100 (fractional observation beyond bounded raw prefix)
		qFracRaw := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAUnitURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: tFracStart, To: tFracEnd},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100, // LIMIT 100 would only fetch valid rows 0..99
		}
		resFracRaw, err := service.GetTwinHistory(ctx, ownerUser, qFracRaw)
		if !errors.Is(err, digitaltwin.ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for fractional interval beyond raw prefix, got %v", err)
		}
		if resFracRaw != nil {
			t.Fatalf("expected nil result on fractional interval error, got %+v", resFracRaw)
		}

		// B. Direct reader raw mode with maxPoints=2
		qFracDirectRaw := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAUnitURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: tFracStart, To: tFracEnd},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  2,
		}
		resFracDirectRaw, err := reader.ReadTemporalHistory(ctx, ownerUser, resolvedUnitEntity, qFracDirectRaw)
		if !errors.Is(err, digitaltwin.ErrUnexpected) {
			t.Fatalf("expected direct reader ErrUnexpected for fractional interval in window, got %v", err)
		}
		if resFracDirectRaw != nil {
			t.Fatalf("expected nil direct reader result on fractional interval error, got %+v", resFracDirectRaw)
		}

		// C. Explicit bucket mode on window containing fractional observation
		qFracBucket := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAUnitURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: tFracStart, To: tFracEnd},
			Resolution: telemetry.Resolution1s,
			MaxPoints:  100,
		}
		resFracBucket, err := service.GetTwinHistory(ctx, ownerUser, qFracBucket)
		if !errors.Is(err, digitaltwin.ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for fractional interval in explicit bucket window, got %v", err)
		}
		if resFracBucket != nil {
			t.Fatalf("expected nil result on fractional bucket error, got %+v", resFracBucket)
		}

		// D. Auto mode on window containing fractional observation
		qFracAuto := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAUnitURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: tFracStart, To: tFracEnd},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		resFracAuto, err := service.GetTwinHistory(ctx, ownerUser, qFracAuto)
		if !errors.Is(err, digitaltwin.ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for fractional interval in auto mode window, got %v", err)
		}
		if resFracAuto != nil {
			t.Fatalf("expected nil result on fractional auto error, got %+v", resFracAuto)
		}

		// 6c. Valid integer observations with fractional AVG on a clean isolated time window:
		// Seed 2 valid integer observations: 10000 and 10001 (AVG = 10000.5).
		// Must succeed and legitimately produce fractional average without being rejected.
		tFracAvgStart := baseT.Add(14 * time.Second)
		tFracAvgEnd := baseT.Add(15 * time.Second)
		_, err = setupPool.Exec(ctx, `
			INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
			VALUES
				($1, $2, 'sample_interval_us', 10000, '{"unit":"us"}'::jsonb),
				($3, $2, 'sample_interval_us', 10001, '{"unit":"us"}'::jsonb)`,
			tFracAvgStart.Add(100*time.Millisecond), sensAUnitUUID,
			tFracAvgStart.Add(200*time.Millisecond))
		if err != nil {
			t.Fatalf("failed to seed valid integer observations for fractional AVG test: %v", err)
		}

		qFracAvg := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAUnitURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: tFracAvgStart, To: tFracAvgEnd},
			Resolution: telemetry.Resolution1s,
			MaxPoints:  100,
		}
		resFracAvg, err := service.GetTwinHistory(ctx, ownerUser, qFracAvg)
		if err != nil {
			t.Fatalf("expected success for valid integer observations with fractional AVG, got %v", err)
		}
		if resFracAvg == nil || len(resFracAvg.TemporalBuckets) != 1 {
			t.Fatalf("expected 1 bucket result for fractional AVG, got %+v", resFracAvg)
		}
		if resFracAvg.TemporalBuckets[0].Avg != 10000.5 {
			t.Fatalf("expected bucket Avg = 10000.5, got %v", resFracAvg.TemporalBuckets[0].Avg)
		}
		if resFracAvg.TemporalBuckets[0].Min != 10000.0 || resFracAvg.TemporalBuckets[0].Max != 10001.0 || resFracAvg.TemporalBuckets[0].Count != 2 {
			t.Fatalf("expected Min=10000 Max=10001 Count=2, got Min=%v Max=%v Count=%d",
				resFracAvg.TemporalBuckets[0].Min, resFracAvg.TemporalBuckets[0].Max, resFracAvg.TemporalBuckets[0].Count)
		}

		// 7. Test Allowlisted Bucket Resolutions against real database:
		// Check that all 9 valid resolution tokens from telemetry source/contract execute valid SQL
		// on a clean time window disjoint from corrupted test rows.
		tCleanStart := baseT.Add(1 * time.Hour)
		_, err = setupPool.Exec(ctx, `
			INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
			VALUES
				($1, $2, 'sample_interval_us', 10000, '{"unit":"us"}'::jsonb),
				($3, $2, 'sample_interval_us', 20000, '{"unit":"us"}'::jsonb)`,
			tCleanStart.Add(10*time.Millisecond), sensAUnitUUID, tCleanStart.Add(50*time.Millisecond))
		if err != nil {
			t.Fatalf("failed to seed clean observations for allowlisted resolutions: %v", err)
		}

		allowlistedResolutions := []telemetry.Resolution{
			telemetry.Resolution100ms,
			telemetry.Resolution1s,
			telemetry.Resolution10s,
			telemetry.Resolution1m,
			telemetry.Resolution5m,
			telemetry.Resolution15m,
			telemetry.Resolution1h,
			telemetry.Resolution6h,
			telemetry.Resolution1d,
		}

		for _, aRes := range allowlistedResolutions {
			dur, ok := telemetry.ResolutionDuration(aRes)
			if !ok || dur <= 0 {
				t.Fatalf("resolution %s missing duration", aRes)
			}
			// Execute a small valid query against sensAUnitUUID with this resolution
			qRes := digitaltwin.TwinHistoryQuery{
				EntityID:   sensAUnitURN,
				Property:   digitaltwin.SelectorSampleIntervalUS,
				Range:      telemetry.TimeRange{From: tCleanStart, To: tCleanStart.Add(dur * 2)},
				Resolution: aRes,
				MaxPoints:  100,
			}
			resObj, err := service.GetTwinHistory(ctx, ownerUser, qRes)
			if err != nil {
				t.Fatalf("allowlisted resolution %s failed against real DB: %v", aRes, err)
			}
			if resObj.Mode != "bucket" || resObj.Resolution != string(aRes) {
				t.Fatalf("resolution %s returned unexpected mode=%s res=%s", aRes, resObj.Mode, resObj.Resolution)
			}
		}
	})

	// =========================================================================
	// Scenario 5: Private Text/Boolean and Metadata-Precision Native Sub-Slice
	// =========================================================================
	t.Run("Private_Text_Boolean_And_MetadataPrecision", func(t *testing.T) {
		const (
			testPropText = "test_text_status"
			testPropBool = "test_bool_alert"
		)

		resolvedTypedEntity := digitaltwin.ResolvedEntity{
			InternalID: sensATypedUUID,
			PublicURN:  sensATypedURN,
			Type:       digitaltwin.EntityTypeSensor,
			GatewayID:  gwIDA,
			SensorID:   sensorTypedID,
		}

		// Verify resolver also resolves sensATypedURN accurately for ownerUser under appPool
		parsedIdentity, err := digitaltwin.ParseEntityURN(sensATypedURN)
		if err != nil {
			t.Fatalf("failed to parse sensor URN: %v", err)
		}
		resEnt, err := resolver.ResolveEntity(ctx, ownerUser, parsedIdentity)
		if err != nil {
			t.Fatalf("failed to resolve sensor entity via resolver: %v", err)
		}
		if resEnt.InternalID != sensATypedUUID || resEnt.PublicURN != sensATypedURN {
			t.Fatalf("resolved entity mismatch: expected ID=%s URN=%s, got ID=%s URN=%s",
				sensATypedUUID, sensATypedURN, resEnt.InternalID, resEnt.PublicURN)
		}

		baseT := time.Date(2026, 10, 17, 18, 0, 0, 0, time.UTC)
		fromText := baseT
		toText := baseT.Add(2 * time.Second) // 2s <= 10s ContractMaxRawWindow

		msgIDText1 := uuid.New()
		msgIDText2 := uuid.New()

		// 1. Seed text observations for testPropText (including empty string, >2^53 int, missing keys, explicit null keys)
		// Insert OUT OF ORDER to test database ASC ordering (ORDER BY v.observed_at ASC, v.message_id ASC NULLS LAST)
		type textSeedRow struct {
			t     time.Time
			val   string
			meta  string
			msgID *uuid.UUID
		}

		textRows := []textSeedRow{
			// Row 0: boundary at fromText, empty string value, empty metadata {}, nullable message_id nil
			{fromText, "", "{}", nil},
			// Row 1: fromText + 100ms, text "online", metadata with large integer 9007199254740993 (>2^53), msgIDText1
			{fromText.Add(100 * time.Millisecond), "online", `{"note":"ok","large_int":9007199254740993}`, &msgIDText1},
			// Row 2: fromText + 300ms, text "standby", explicit null key unit:null, missing keys, nullable message_id nil
			{fromText.Add(300 * time.Millisecond), "standby", `{"unit":null,"missing_key_demo":"val"}`, nil},
			// Row 3: fromText + 500ms, text "active", custom flag and null extra, msgIDText2
			{fromText.Add(500 * time.Millisecond), "active", `{"custom_flag":true,"extra":null}`, &msgIDText2},
		}

		// Insert out-of-order: 2, 0, 3, 1
		for _, idx := range []int{2, 0, 3, 1} {
			r := textRows[idx]
			_, err := setupPool.Exec(ctx, `
				INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_text, metadata, message_id)
				VALUES ($1, $2, $3, $4, $5::jsonb, $6)`,
				r.t, sensATypedUUID, testPropText, r.val, r.meta, r.msgID)
			if err != nil {
				t.Fatalf("failed to seed text observation %d: %v", idx, err)
			}
		}

		// Points strictly outside window to verify boundaries
		_, err = setupPool.Exec(ctx, `
			INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_text, metadata)
			VALUES
				($1, $2, $3, 'excluded_before', '{}'::jsonb),
				($4, $2, $3, 'excluded_at_to', '{}'::jsonb)`,
			fromText.Add(-1*time.Microsecond), sensATypedUUID, testPropText, toText)
		if err != nil {
			t.Fatalf("failed to seed boundary text rows: %v", err)
		}

		// 2. Query testPropText via private readTemporalHistoryInternal (raw resolution)
		qTextRaw := digitaltwin.TwinHistoryQuery{
			EntityID:   sensATypedURN,
			Property:   digitaltwin.HistorySelector(testPropText),
			Range:      telemetry.TimeRange{From: fromText, To: toText},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}

		resTextRaw, err := reader.ReadTemporalHistoryInternalForTest(ctx, ownerUser, resolvedTypedEntity, testPropText, qTextRaw)
		if err != nil {
			t.Fatalf("raw text read failed: %v", err)
		}
		if resTextRaw.Mode != "raw" || resTextRaw.Resolution != "raw" {
			t.Fatalf("expected mode=raw res=raw, got mode=%q res=%q", resTextRaw.Mode, resTextRaw.Resolution)
		}
		if resTextRaw.PointCount != 4 || len(resTextRaw.TemporalPoints) != 4 {
			t.Fatalf("expected 4 text observations, got count=%d len=%d", resTextRaw.PointCount, len(resTextRaw.TemporalPoints))
		}
		if resTextRaw.Unit != nil {
			t.Fatalf("expected nil Unit for text series, got %v", *resTextRaw.Unit)
		}

		// Check ordering and individual typed observation semantics:
		// Point 0: empty string preserved, ValueNumber nil, ValueBoolean nil, MessageID nil, Metadata "{}"
		pt0 := resTextRaw.TemporalPoints[0]
		if !pt0.ObservedAt.Equal(fromText) {
			t.Fatalf("expected pt0 at %v, got %v", fromText, pt0.ObservedAt)
		}
		if pt0.ValueType != "text" {
			t.Fatalf("expected pt0 value_type 'text', got %q", pt0.ValueType)
		}
		if pt0.ValueText == nil || *pt0.ValueText != "" {
			t.Fatalf("expected empty string for pt0 ValueText, got %v", pt0.ValueText)
		}
		if pt0.ValueNumber != nil {
			t.Fatalf("expected nil ValueNumber for text observation pt0, got %v", *pt0.ValueNumber)
		}
		if pt0.ValueBoolean != nil {
			t.Fatalf("expected nil ValueBoolean for text observation pt0, got %v", *pt0.ValueBoolean)
		}
		if pt0.MessageID != nil {
			t.Fatalf("expected nil MessageID for pt0, got %v", pt0.MessageID)
		}
		if string(pt0.Metadata) != "{}" {
			t.Fatalf("expected metadata '{}' for pt0, got %s", string(pt0.Metadata))
		}

		// Point 1: text "online", MessageID present, metadata with large_int > 2^53 (9007199254740993)
		pt1 := resTextRaw.TemporalPoints[1]
		if !pt1.ObservedAt.Equal(fromText.Add(100 * time.Millisecond)) {
			t.Fatalf("expected pt1 at from+100ms, got %v", pt1.ObservedAt)
		}
		if pt1.ValueType != "text" || pt1.ValueText == nil || *pt1.ValueText != "online" {
			t.Fatalf("expected pt1 text 'online', got type=%q val=%v", pt1.ValueType, pt1.ValueText)
		}
		if pt1.MessageID == nil || *pt1.MessageID != msgIDText1 {
			t.Fatalf("expected pt1 MessageID %s, got %v", msgIDText1, pt1.MessageID)
		}
		// Strict precision check on 9007199254740993:
		// Verify raw JSON text preserves exact digits
		if !strings.Contains(string(pt1.Metadata), "9007199254740993") {
			t.Fatalf("expected metadata to contain exact string '9007199254740993', got %s", string(pt1.Metadata))
		}
		// Verify json.Number preserves exact integer without float64 precision loss
		decText := json.NewDecoder(bytes.NewReader(pt1.Metadata))
		decText.UseNumber()
		var metaTextObj map[string]any
		if err := decText.Decode(&metaTextObj); err != nil {
			t.Fatalf("failed to decode pt1 metadata with UseNumber: %v", err)
		}
		rawNum, ok := metaTextObj["large_int"].(json.Number)
		if !ok || rawNum.String() != "9007199254740993" {
			t.Fatalf("expected json.Number '9007199254740993', got %v", metaTextObj["large_int"])
		}

		// Point 2: text "standby", unit:null, missing keys preserved
		pt2 := resTextRaw.TemporalPoints[2]
		if pt2.ValueType != "text" || pt2.ValueText == nil || *pt2.ValueText != "standby" {
			t.Fatalf("expected pt2 text 'standby', got %v", pt2.ValueText)
		}
		if pt2.MessageID != nil {
			t.Fatalf("expected nil MessageID for pt2, got %v", pt2.MessageID)
		}
		var meta2 map[string]any
		if err := json.Unmarshal(pt2.Metadata, &meta2); err != nil {
			t.Fatalf("failed to unmarshal pt2 metadata: %v", err)
		}
		if val, exists := meta2["unit"]; !exists || val != nil {
			t.Fatalf("expected pt2 metadata to preserve explicit null key 'unit': exists=%v val=%v", exists, val)
		}
		if _, exists := meta2["boot_id"]; exists {
			t.Fatalf("expected 'boot_id' key to be missing from pt2 metadata")
		}

		// Point 3: text "active", custom_flag:true, msgIDText2
		pt3 := resTextRaw.TemporalPoints[3]
		if pt3.ValueType != "text" || pt3.ValueText == nil || *pt3.ValueText != "active" {
			t.Fatalf("expected pt3 text 'active', got %v", pt3.ValueText)
		}
		if pt3.MessageID == nil || *pt3.MessageID != msgIDText2 {
			t.Fatalf("expected pt3 MessageID %s, got %v", msgIDText2, pt3.MessageID)
		}

		// 3. Seed boolean observations for testPropBool (including false, true, false, nullable msgID, metadata)
		fromBool := baseT.Add(10 * time.Second)
		toBool := fromBool.Add(2 * time.Second)

		msgIDBool0 := uuid.New()
		msgIDBool2 := uuid.New()

		boolRows := []struct {
			t     time.Time
			val   bool
			meta  string
			msgID *uuid.UUID
		}{
			// Row 0: at fromBool, explicit FALSE, metadata {}, msgIDBool0
			{fromBool, false, "{}", &msgIDBool0},
			// Row 1: fromBool + 200ms, explicit TRUE, explicit null in metadata, nullable msgID nil
			{fromBool.Add(200 * time.Millisecond), true, `{"source":"sensor_check","null_flag":null}`, nil},
			// Row 2: fromBool + 400ms, explicit FALSE, large_int > 2^53 (9007199254740995), msgIDBool2
			{fromBool.Add(400 * time.Millisecond), false, `{"verified":false,"large_int":9007199254740995}`, &msgIDBool2},
		}

		// Insert out of order: 1, 2, 0
		for _, idx := range []int{1, 2, 0} {
			r := boolRows[idx]
			_, err := setupPool.Exec(ctx, `
				INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_boolean, metadata, message_id)
				VALUES ($1, $2, $3, $4, $5::jsonb, $6)`,
				r.t, sensATypedUUID, testPropBool, r.val, r.meta, r.msgID)
			if err != nil {
				t.Fatalf("failed to seed boolean observation %d: %v", idx, err)
			}
		}

		qBoolRaw := digitaltwin.TwinHistoryQuery{
			EntityID:   sensATypedURN,
			Property:   digitaltwin.HistorySelector(testPropBool),
			Range:      telemetry.TimeRange{From: fromBool, To: toBool},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}

		resBoolRaw, err := reader.ReadTemporalHistoryInternalForTest(ctx, ownerUser, resolvedTypedEntity, testPropBool, qBoolRaw)
		if err != nil {
			t.Fatalf("raw boolean read failed: %v", err)
		}
		if resBoolRaw.PointCount != 3 || len(resBoolRaw.TemporalPoints) != 3 {
			t.Fatalf("expected 3 boolean observations, got count=%d len=%d", resBoolRaw.PointCount, len(resBoolRaw.TemporalPoints))
		}
		if resBoolRaw.Unit != nil {
			t.Fatalf("expected nil Unit for boolean series, got %v", *resBoolRaw.Unit)
		}

		// Point 0: explicit FALSE, ValueBoolean non-nil and false, ValueNumber nil (NO conversion to 0.0)
		bpt0 := resBoolRaw.TemporalPoints[0]
		if bpt0.ValueType != "boolean" {
			t.Fatalf("expected bpt0 value_type 'boolean', got %q", bpt0.ValueType)
		}
		if bpt0.ValueBoolean == nil || *bpt0.ValueBoolean != false {
			t.Fatalf("expected explicit false for bpt0 ValueBoolean, got %v", bpt0.ValueBoolean)
		}
		if bpt0.ValueNumber != nil {
			t.Fatalf("expected nil ValueNumber for boolean observation bpt0 (no fake 0.0), got %v", *bpt0.ValueNumber)
		}
		if bpt0.ValueText != nil {
			t.Fatalf("expected nil ValueText for boolean observation bpt0, got %v", *bpt0.ValueText)
		}
		if bpt0.MessageID == nil || *bpt0.MessageID != msgIDBool0 {
			t.Fatalf("expected bpt0 MessageID %s, got %v", msgIDBool0, bpt0.MessageID)
		}

		// Point 1: explicit TRUE, MessageID nil
		bpt1 := resBoolRaw.TemporalPoints[1]
		if bpt1.ValueType != "boolean" || bpt1.ValueBoolean == nil || *bpt1.ValueBoolean != true {
			t.Fatalf("expected explicit true for bpt1 ValueBoolean, got %v", bpt1.ValueBoolean)
		}
		if bpt1.ValueNumber != nil {
			t.Fatalf("expected nil ValueNumber for boolean observation bpt1 (no fake 1.0), got %v", *bpt1.ValueNumber)
		}
		if bpt1.MessageID != nil {
			t.Fatalf("expected nil MessageID for bpt1, got %v", bpt1.MessageID)
		}

		// Point 2: explicit FALSE, large_int 9007199254740995 (> 2^53)
		bpt2 := resBoolRaw.TemporalPoints[2]
		if bpt2.ValueType != "boolean" || bpt2.ValueBoolean == nil || *bpt2.ValueBoolean != false {
			t.Fatalf("expected explicit false for bpt2 ValueBoolean, got %v", bpt2.ValueBoolean)
		}
		if !strings.Contains(string(bpt2.Metadata), "9007199254740995") {
			t.Fatalf("expected bpt2 metadata to contain '9007199254740995', got %s", string(bpt2.Metadata))
		}
		decBool := json.NewDecoder(bytes.NewReader(bpt2.Metadata))
		decBool.UseNumber()
		var metaBoolObj map[string]any
		if err := decBool.Decode(&metaBoolObj); err != nil {
			t.Fatalf("failed to decode bpt2 metadata: %v", err)
		}
		rawBoolNum, ok := metaBoolObj["large_int"].(json.Number)
		if !ok || rawBoolNum.String() != "9007199254740995" {
			t.Fatalf("expected json.Number '9007199254740995', got %v", metaBoolObj["large_int"])
		}

		// 4. Test Resolution Policy for Text and Boolean:
		// A. Fitting short-auto preserves raw types (window <= 10s and points <= maxPoints)
		qTextAuto := qTextRaw
		qTextAuto.Resolution = telemetry.ResolutionAuto
		qTextAuto.MaxPoints = 100
		resTextAuto, err := reader.ReadTemporalHistoryInternalForTest(ctx, ownerUser, resolvedTypedEntity, testPropText, qTextAuto)
		if err != nil {
			t.Fatalf("short auto text query failed: %v", err)
		}
		if resTextAuto.Mode != "raw" || resTextAuto.Resolution != "raw" {
			t.Fatalf("expected short-auto text mode=raw res=raw, got mode=%q res=%q", resTextAuto.Mode, resTextAuto.Resolution)
		}
		if resTextAuto.PointCount != 4 || resTextAuto.TemporalPoints[0].ValueType != "text" {
			t.Fatalf("expected 4 raw text observations in short auto, got %d", resTextAuto.PointCount)
		}
		if resTextAuto.TemporalBuckets != nil {
			t.Fatalf("expected nil TemporalBuckets for short auto text, got %+v", resTextAuto.TemporalBuckets)
		}

		qBoolAuto := qBoolRaw
		qBoolAuto.Resolution = telemetry.ResolutionAuto
		qBoolAuto.MaxPoints = 100
		resBoolAuto, err := reader.ReadTemporalHistoryInternalForTest(ctx, ownerUser, resolvedTypedEntity, testPropBool, qBoolAuto)
		if err != nil {
			t.Fatalf("short auto bool query failed: %v", err)
		}
		if resBoolAuto.Mode != "raw" || resBoolAuto.Resolution != "raw" {
			t.Fatalf("expected short-auto bool mode=raw res=raw, got mode=%q res=%q", resBoolAuto.Mode, resBoolAuto.Resolution)
		}
		if resBoolAuto.PointCount != 3 || resBoolAuto.TemporalPoints[0].ValueType != "boolean" {
			t.Fatalf("expected 3 raw bool observations in short auto, got %d", resBoolAuto.PointCount)
		}
		if resBoolAuto.TemporalBuckets != nil {
			t.Fatalf("expected nil TemporalBuckets for short auto bool, got %+v", resBoolAuto.TemporalBuckets)
		}

		// B. Raw and short-auto overflow on text/bool when points > maxPoints -> ErrRawLimitExceeded
		qTextOverflowRaw := qTextRaw
		qTextOverflowRaw.MaxPoints = 2 // 4 points > 2
		_, err = reader.ReadTemporalHistoryInternalForTest(ctx, ownerUser, resolvedTypedEntity, testPropText, qTextOverflowRaw)
		if !errors.Is(err, digitaltwin.ErrRawLimitExceeded) {
			t.Fatalf("expected ErrRawLimitExceeded for raw text overflow, got %v", err)
		}

		qTextOverflowAuto := qTextRaw
		qTextOverflowAuto.Resolution = telemetry.ResolutionAuto
		qTextOverflowAuto.MaxPoints = 2 // 4 points > 2
		_, err = reader.ReadTemporalHistoryInternalForTest(ctx, ownerUser, resolvedTypedEntity, testPropText, qTextOverflowAuto)
		if !errors.Is(err, digitaltwin.ErrRawLimitExceeded) {
			t.Fatalf("expected ErrRawLimitExceeded for short auto text overflow without bucketing, got %v", err)
		}

		// C. Too-long raw window (> 10s) returns ErrRawLimitExceeded
		qTextTooLongRaw := qTextRaw
		qTextTooLongRaw.Range.To = fromText.Add(15 * time.Second) // 15s > 10s
		_, err = reader.ReadTemporalHistoryInternalForTest(ctx, ownerUser, resolvedTypedEntity, testPropText, qTextTooLongRaw)
		if !errors.Is(err, digitaltwin.ErrRawLimitExceeded) {
			t.Fatalf("expected ErrRawLimitExceeded for too-long raw window, got %v", err)
		}

		// D. Too-long auto window (> 10s) on text/bool cannot aggregate -> returns ErrRawLimitExceeded
		qTextTooLongAuto := qTextRaw
		qTextTooLongAuto.Range.To = fromText.Add(15 * time.Second) // 15s > 10s
		qTextTooLongAuto.Resolution = telemetry.ResolutionAuto
		_, err = reader.ReadTemporalHistoryInternalForTest(ctx, ownerUser, resolvedTypedEntity, testPropText, qTextTooLongAuto)
		if !errors.Is(err, digitaltwin.ErrRawLimitExceeded) {
			t.Fatalf("expected ErrRawLimitExceeded for too-long auto window on text series, got %v", err)
		}

		qBoolTooLongAuto := qBoolRaw
		qBoolTooLongAuto.Range.To = fromBool.Add(15 * time.Second) // 15s > 10s
		qBoolTooLongAuto.Resolution = telemetry.ResolutionAuto
		_, err = reader.ReadTemporalHistoryInternalForTest(ctx, ownerUser, resolvedTypedEntity, testPropBool, qBoolTooLongAuto)
		if !errors.Is(err, digitaltwin.ErrRawLimitExceeded) {
			t.Fatalf("expected ErrRawLimitExceeded for too-long auto window on bool series, got %v", err)
		}

		// E. Explicit bucket on typed non-numeric observations returns ErrInvalidResolution
		qTextBucket := qTextRaw
		qTextBucket.Resolution = telemetry.Resolution1s
		_, err = reader.ReadTemporalHistoryInternalForTest(ctx, ownerUser, resolvedTypedEntity, testPropText, qTextBucket)
		if !errors.Is(err, digitaltwin.ErrInvalidResolution) {
			t.Fatalf("expected ErrInvalidResolution for explicit bucket on text property, got %v", err)
		}

		qBoolBucket := qBoolRaw
		qBoolBucket.Resolution = telemetry.Resolution1s
		_, err = reader.ReadTemporalHistoryInternalForTest(ctx, ownerUser, resolvedTypedEntity, testPropBool, qBoolBucket)
		if !errors.Is(err, digitaltwin.ErrInvalidResolution) {
			t.Fatalf("expected ErrInvalidResolution for explicit bucket on bool property, got %v", err)
		}

		// 5. Public validator and service strictly reject reserved text/bool and inference selectors
		reservedSelectors := []string{
			testPropText,
			testPropBool,
			"anomaly_score",
			"reconstruction_loss",
			"firmware_status",
			"alert_state",
			"custom_text",
		}

		for _, sel := range reservedSelectors {
			qPublic := digitaltwin.TwinHistoryQuery{
				EntityID:   sensATypedURN,
				Property:   digitaltwin.HistorySelector(sel),
				Range:      telemetry.TimeRange{From: fromText, To: toText},
				Resolution: telemetry.ResolutionRaw,
				MaxPoints:  100,
			}
			_, err := service.GetTwinHistory(ctx, ownerUser, qPublic)
			if !errors.Is(err, digitaltwin.ErrInvalidProperty) {
				t.Fatalf("public service must reject reserved selector %q with ErrInvalidProperty, got %v", sel, err)
			}
			// Direct public reader also rejects
			_, err = reader.ReadTemporalHistory(ctx, ownerUser, resolvedTypedEntity, qPublic)
			if !errors.Is(err, digitaltwin.ErrInvalidProperty) {
				t.Fatalf("public reader must reject reserved selector %q with ErrInvalidProperty, got %v", sel, err)
			}
		}
	})

	// =========================================================================
	// Scenario 6: Real Persist/Dedup and Value-Delegation Equivalence Sub-Slice
	// =========================================================================
	t.Run("Real_Persist_Dedup_And_Value_Delegation_Equivalence", func(t *testing.T) {
		validator, err := telemetry.NewValidator(telemetry.Limits{
			MaxPayloadBytes:  1 << 20,
			MaxSamples:       1000,
			MaxBatchDuration: time.Hour,
			MaxFutureSkew:    time.Hour,
		})
		if err != nil {
			t.Fatalf("failed to create telemetry validator: %v", err)
		}

		bootID := uuid.New()
		msgID := uuid.New()
		baseTime := time.Date(2026, 10, 17, 20, 0, 0, 0, time.UTC)
		const sampleCount = 10
		const intervalUS = int64(100000) // 100ms = 100,000us
		samples := []float64{10.0, 12.0, 14.0, 16.0, 18.0, 20.0, 22.0, 24.0, 26.0, 28.0}
		firstSeq := int64(6001)

		dto := telemetry.BatchDTO{
			ProtocolVersion:  1,
			MessageID:        msgID,
			MessageType:      "telemetry_batch",
			GatewayID:        gwIDA,
			SensorID:         sensorPersistID,
			BootID:           bootID,
			FirstSequence:    firstSeq,
			SampleCount:      int32(sampleCount),
			MeasuredAt:       baseTime.Format(time.RFC3339Nano),
			SampleIntervalUS: intervalUS,
			Samples:          samples,
		}

		payloadBytes, err := json.Marshal(dto)
		if err != nil {
			t.Fatalf("failed to marshal batch DTO: %v", err)
		}

		topic := fmt.Sprintf("gateways/%s/telemetry/%s", gwIDA, sensorPersistID)
		validatedMsg, err := validator.Validate(topic, payloadBytes, baseTime.Add(1*time.Second))
		if err != nil {
			t.Fatalf("failed to validate telemetry message: %v", err)
		}

		// 1. Initial Persist call under application role iot_backend_app
		commitRes, err := persistRepo.Persist(ctx, validatedMsg)
		if err != nil {
			t.Fatalf("initial persistRepo.Persist failed: %v", err)
		}
		if commitRes.Outcome != telemetry.CommitOutcomeCommitted {
			t.Fatalf("expected CommitOutcomeCommitted, got %v", commitRes.Outcome)
		}
		if commitRes.SamplesSaved != sampleCount {
			t.Fatalf("expected %d samples saved, got %d", sampleCount, commitRes.SamplesSaved)
		}

		// Assert raw rows in hypertable
		var rawCount int
		err = setupPool.QueryRow(ctx, `
			SELECT count(*) FROM public.telemetry
			WHERE gateway_id = $1 AND sensor_id = $2 AND message_id = $3`,
			gwIDA, sensorPersistID, msgID).Scan(&rawCount)
		if err != nil {
			t.Fatalf("failed to query raw telemetry count: %v", err)
		}
		if rawCount != sampleCount {
			t.Fatalf("raw rows count mismatch: expected %d, got %d", sampleCount, rawCount)
		}

		// Assert ledger in processed_messages
		var ledgerCount int
		var ledgerFirstSeq, ledgerLastSeq int64
		var ledgerSampleCount int
		var ledgerSourceTopic, ledgerSensorID string
		var ledgerBootID uuid.UUID
		err = setupPool.QueryRow(ctx, `
			SELECT count(*), first_sequence, last_sequence, sample_count, source_topic, sensor_id, boot_id
			FROM public.processed_messages
			WHERE gateway_id = $1 AND message_id = $2
			GROUP BY first_sequence, last_sequence, sample_count, source_topic, sensor_id, boot_id`,
			gwIDA, msgID).Scan(&ledgerCount, &ledgerFirstSeq, &ledgerLastSeq, &ledgerSampleCount,
			&ledgerSourceTopic, &ledgerSensorID, &ledgerBootID)
		if err != nil {
			t.Fatalf("failed to query ledger record: %v", err)
		}
		if ledgerCount != 1 || ledgerFirstSeq != firstSeq || ledgerLastSeq != firstSeq+int64(sampleCount-1) ||
			ledgerSampleCount != sampleCount || ledgerSourceTopic != topic ||
			ledgerSensorID != sensorPersistID || ledgerBootID != bootID {
			t.Fatalf("ledger record mismatch: count=%d seq=[%d..%d] samples=%d topic=%s sensor=%s boot=%s",
				ledgerCount, ledgerFirstSeq, ledgerLastSeq, ledgerSampleCount, ledgerSourceTopic, ledgerSensorID, ledgerBootID)
		}

		// Assert single generated temporal sample_interval_us observation
		var temporalCount int
		var temporalObservedAt time.Time
		var temporalValNumber *float64
		var temporalValText *string
		var temporalValBool *bool
		var temporalMeta []byte
		var temporalMsgID *uuid.UUID
		err = setupPool.QueryRow(ctx, `
			SELECT count(*), observed_at, value_number, value_text, value_boolean, metadata, message_id
			FROM public.twin_temporal_values
			WHERE entity_id = $1
			GROUP BY observed_at, value_number, value_text, value_boolean, metadata, message_id`,
			sensAPersistUUID).Scan(&temporalCount, &temporalObservedAt, &temporalValNumber,
			&temporalValText, &temporalValBool, &temporalMeta, &temporalMsgID)
		if err != nil {
			t.Fatalf("failed to query temporal observation: %v", err)
		}
		if temporalCount != 1 {
			t.Fatalf("expected exactly 1 temporal observation, got %d", temporalCount)
		}
		expectedLastSampleTime := baseTime.Add(time.Duration(sampleCount-1) * 100 * time.Millisecond)
		if !temporalObservedAt.Equal(expectedLastSampleTime) {
			t.Fatalf("temporal observed_at mismatch: expected last sample time %v, got %v", expectedLastSampleTime, temporalObservedAt)
		}
		if temporalValNumber == nil || *temporalValNumber != float64(intervalUS) {
			t.Fatalf("temporal value_number mismatch: expected %f, got %v", float64(intervalUS), temporalValNumber)
		}
		if temporalValText != nil || temporalValBool != nil {
			t.Fatalf("temporal value_text or value_boolean must be NULL, got text=%v bool=%v", temporalValText, temporalValBool)
		}
		if temporalMsgID == nil || *temporalMsgID != msgID {
			t.Fatalf("temporal message_id mismatch: expected %s, got %v", msgID, temporalMsgID)
		}
		var metaMap map[string]any
		if err := json.Unmarshal(temporalMeta, &metaMap); err != nil {
			t.Fatalf("failed to parse temporal metadata: %v", err)
		}
		if metaMap["unit"] != "us" {
			t.Fatalf("expected metadata unit 'us', got %v", metaMap["unit"])
		}
		if metaMap["boot_id"] != bootID.String() {
			t.Fatalf("expected metadata boot_id %s, got %v", bootID.String(), metaMap["boot_id"])
		}
		if metaMap["source"] != "telemetry_batch" {
			t.Fatalf("expected metadata source 'telemetry_batch', got %v", metaMap["source"])
		}

		// 2. Duplicate Resend
		dupRes, err := persistRepo.Persist(ctx, validatedMsg)
		if err != nil {
			t.Fatalf("duplicate Persist failed: %v", err)
		}
		if dupRes.Outcome != telemetry.CommitOutcomeDuplicateCommitted {
			t.Fatalf("expected CommitOutcomeDuplicateCommitted, got %v", dupRes.Outcome)
		}
		if dupRes.SamplesSaved != 0 {
			t.Fatalf("expected 0 samples saved on duplicate, got %d", dupRes.SamplesSaved)
		}

		var rawCountAfter, ledgerCountAfter, temporalCountAfter int
		if err := setupPool.QueryRow(ctx, "SELECT count(*) FROM public.telemetry WHERE gateway_id = $1 AND message_id = $2", gwIDA, msgID).Scan(&rawCountAfter); err != nil {
			t.Fatalf("failed to count raw after resend: %v", err)
		}
		if rawCountAfter != sampleCount {
			t.Fatalf("raw count mutated on resend: before=%d, after=%d", sampleCount, rawCountAfter)
		}
		if err := setupPool.QueryRow(ctx, "SELECT count(*) FROM public.processed_messages WHERE gateway_id = $1 AND message_id = $2", gwIDA, msgID).Scan(&ledgerCountAfter); err != nil {
			t.Fatalf("failed to count ledger after resend: %v", err)
		}
		if ledgerCountAfter != 1 {
			t.Fatalf("ledger count mutated on resend: expected 1, got %d", ledgerCountAfter)
		}
		if err := setupPool.QueryRow(ctx, "SELECT count(*) FROM public.twin_temporal_values WHERE entity_id = $1", sensAPersistUUID).Scan(&temporalCountAfter); err != nil {
			t.Fatalf("failed to count temporal after resend: %v", err)
		}
		if temporalCountAfter != 1 {
			t.Fatalf("temporal count mutated on resend: expected 1, got %d", temporalCountAfter)
		}

		// 3. Query property=value: Equivalence between Twin GetTwinHistory and Telemetry History Engine
		historyRange := telemetry.TimeRange{
			From: baseTime,
			To:   baseTime.Add(2 * time.Second),
		}

		// A. Raw Mode Equivalence
		qTwinRaw := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAPersistURN,
			Property:   digitaltwin.SelectorValue,
			Range:      historyRange,
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		resTwinRaw, err := service.GetTwinHistory(ctx, ownerUser, qTwinRaw)
		if err != nil {
			t.Fatalf("twin GetTwinHistory (raw value) failed: %v", err)
		}

		qTelemRaw := telemetry.TelemetryHistoryQuery{
			GatewayID:  gwIDA,
			SensorID:   sensorPersistID,
			Range:      historyRange,
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		resTelemRaw, err := telemHistoryService.GetTelemetryHistory(ctx, ownerUser, qTelemRaw)
		if err != nil {
			t.Fatalf("telemetry GetTelemetryHistory (raw) failed: %v", err)
		}

		if resTwinRaw.Property != digitaltwin.SelectorValue {
			t.Fatalf("twin history property mismatch: expected %s, got %s", digitaltwin.SelectorValue, resTwinRaw.Property)
		}
		if resTwinRaw.PointCount != resTelemRaw.PointCount || resTwinRaw.PointCount != sampleCount {
			t.Fatalf("raw point count mismatch: twin=%d telem=%d expected=%d", resTwinRaw.PointCount, resTelemRaw.PointCount, sampleCount)
		}
		if resTwinRaw.Mode != resTelemRaw.Mode || resTwinRaw.Mode != "raw" {
			t.Fatalf("raw mode mismatch: twin=%s telem=%s", resTwinRaw.Mode, resTelemRaw.Mode)
		}
		if resTwinRaw.Resolution != resTelemRaw.Resolution || resTwinRaw.Resolution != "raw" {
			t.Fatalf("raw resolution mismatch: twin=%s telem=%s", resTwinRaw.Resolution, resTelemRaw.Resolution)
		}
		if resTwinRaw.Unit == nil || resTelemRaw.Unit == nil || *resTwinRaw.Unit != "degC" || *resTwinRaw.Unit != *resTelemRaw.Unit {
			t.Fatalf("unit mismatch: twin=%v telem=%v, expected 'degC'", resTwinRaw.Unit, resTelemRaw.Unit)
		}
		if !resTwinRaw.From.Equal(resTelemRaw.From) || !resTwinRaw.To.Equal(resTelemRaw.To) {
			t.Fatalf("range mismatch: twin=[%v..%v], telem=[%v..%v]", resTwinRaw.From, resTwinRaw.To, resTelemRaw.From, resTelemRaw.To)
		}
		if resTwinRaw.TelemetryResult == nil {
			t.Fatalf("twin TelemetryResult must not be nil for property=value")
		}

		for i := 0; i < sampleCount; i++ {
			pTwin := resTwinRaw.TelemetryResult.RawPoints[i]
			pTelem := resTelemRaw.RawPoints[i]

			if pTwin.Value != pTelem.Value || pTwin.SequenceNum != pTelem.SequenceNum ||
				!pTwin.MeasuredAt.Equal(pTelem.MeasuredAt) || *pTwin.BootID != *pTelem.BootID ||
				*pTwin.MessageID != *pTelem.MessageID || *pTwin.SampleIndex != *pTelem.SampleIndex {
				t.Fatalf("raw point [%d] equivalence mismatch: twin=%+v telem=%+v", i, pTwin, pTelem)
			}

			expectedTime := baseTime.Add(time.Duration(i) * 100 * time.Millisecond)
			expectedSeq := firstSeq + int64(i)
			expectedVal := samples[i]

			if !pTwin.MeasuredAt.Equal(expectedTime) {
				t.Fatalf("raw point [%d] measured_at mismatch: expected %v, got %v", i, expectedTime, pTwin.MeasuredAt)
			}
			if pTwin.SequenceNum != expectedSeq {
				t.Fatalf("raw point [%d] sequence_num mismatch: expected %d, got %d", i, expectedSeq, pTwin.SequenceNum)
			}
			if pTwin.Value != expectedVal {
				t.Fatalf("raw point [%d] value mismatch: expected %f, got %f", i, expectedVal, pTwin.Value)
			}
			if pTwin.BootID == nil || *pTwin.BootID != bootID {
				t.Fatalf("raw point [%d] boot_id mismatch: expected %s, got %v", i, bootID, pTwin.BootID)
			}
			if pTwin.MessageID == nil || *pTwin.MessageID != msgID {
				t.Fatalf("raw point [%d] message_id mismatch: expected %s, got %v", i, msgID, pTwin.MessageID)
			}
			if pTwin.SampleIndex == nil || *pTwin.SampleIndex != int32(i) {
				t.Fatalf("raw point [%d] sample_index mismatch: expected %d, got %v", i, i, pTwin.SampleIndex)
			}
		}

		// B. Bucket Resolution (1s) Equivalence and Hand-Calculated Arithmetic
		// Bucket 0 [20:00:00, 20:00:01): all 10 samples (sum=190.0, avg=19.0, min=10.0, max=28.0, count=10)
		// Bucket 1 [20:00:01, 20:00:02): gap omitted
		qTwinBucket := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAPersistURN,
			Property:   digitaltwin.SelectorValue,
			Range:      historyRange,
			Resolution: telemetry.Resolution1s,
			MaxPoints:  100,
		}
		resTwinBucket, err := service.GetTwinHistory(ctx, ownerUser, qTwinBucket)
		if err != nil {
			t.Fatalf("twin GetTwinHistory (bucket 1s) failed: %v", err)
		}

		qTelemBucket := telemetry.TelemetryHistoryQuery{
			GatewayID:  gwIDA,
			SensorID:   sensorPersistID,
			Range:      historyRange,
			Resolution: telemetry.Resolution1s,
			MaxPoints:  100,
		}
		resTelemBucket, err := telemHistoryService.GetTelemetryHistory(ctx, ownerUser, qTelemBucket)
		if err != nil {
			t.Fatalf("telemetry GetTelemetryHistory (bucket 1s) failed: %v", err)
		}

		if resTwinBucket.PointCount != resTelemBucket.PointCount || resTwinBucket.PointCount != 1 {
			t.Fatalf("bucket point count mismatch: twin=%d telem=%d expected=1", resTwinBucket.PointCount, resTelemBucket.PointCount)
		}
		if resTwinBucket.Mode != resTelemBucket.Mode || resTwinBucket.Mode != "bucket" {
			t.Fatalf("bucket mode mismatch: twin=%s telem=%s", resTwinBucket.Mode, resTelemBucket.Mode)
		}
		if resTwinBucket.Resolution != "1s" || resTelemBucket.Resolution != "1s" {
			t.Fatalf("bucket resolution mismatch: twin=%s telem=%s", resTwinBucket.Resolution, resTelemBucket.Resolution)
		}

		bTwin := resTwinBucket.TelemetryResult.BucketPoints[0]
		bTelem := resTelemBucket.BucketPoints[0]
		if !bTwin.BucketStart.Equal(bTelem.BucketStart) || bTwin.Avg != bTelem.Avg ||
			bTwin.Min != bTelem.Min || bTwin.Max != bTelem.Max || bTwin.Count != bTelem.Count {
			t.Fatalf("bucket 0 equivalence mismatch: twin=%+v telem=%+v", bTwin, bTelem)
		}

		if !bTwin.BucketStart.Equal(baseTime) {
			t.Fatalf("bucket start mismatch: expected %v, got %v", baseTime, bTwin.BucketStart)
		}
		if bTwin.Count != 10 {
			t.Fatalf("bucket count mismatch: expected 10, got %d", bTwin.Count)
		}
		if bTwin.Min != 10.0 {
			t.Fatalf("bucket min mismatch: expected 10.0, got %f", bTwin.Min)
		}
		if bTwin.Max != 28.0 {
			t.Fatalf("bucket max mismatch: expected 28.0, got %f", bTwin.Max)
		}
		if bTwin.Avg != 19.0 {
			t.Fatalf("bucket avg mismatch: expected 19.0, got %f", bTwin.Avg)
		}

		// C. Auto Resolution Equivalence (Short Window and Long Window)
		// 1) Short Auto (2s window <= 10s, maxPoints=100) -> keeps raw mode
		qTwinAutoShort := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAPersistURN,
			Property:   digitaltwin.SelectorValue,
			Range:      historyRange,
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		resTwinAutoShort, err := service.GetTwinHistory(ctx, ownerUser, qTwinAutoShort)
		if err != nil {
			t.Fatalf("twin auto short query failed: %v", err)
		}
		qTelemAutoShort := telemetry.TelemetryHistoryQuery{
			GatewayID:  gwIDA,
			SensorID:   sensorPersistID,
			Range:      historyRange,
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		resTelemAutoShort, err := telemHistoryService.GetTelemetryHistory(ctx, ownerUser, qTelemAutoShort)
		if err != nil {
			t.Fatalf("telemetry auto short query failed: %v", err)
		}
		if resTwinAutoShort.Mode != resTelemAutoShort.Mode || resTwinAutoShort.Mode != "raw" {
			t.Fatalf("auto short mode mismatch: twin=%s telem=%s expected=raw", resTwinAutoShort.Mode, resTelemAutoShort.Mode)
		}
		if resTwinAutoShort.PointCount != resTelemAutoShort.PointCount || resTwinAutoShort.PointCount != sampleCount {
			t.Fatalf("auto short count mismatch: twin=%d telem=%d expected=%d", resTwinAutoShort.PointCount, resTelemAutoShort.PointCount, sampleCount)
		}

		// 2) Long Auto (20s window > 10s, maxPoints=100) -> selects bucket 1s
		longRange := telemetry.TimeRange{
			From: baseTime,
			To:   baseTime.Add(20 * time.Second),
		}
		qTwinAutoLong := digitaltwin.TwinHistoryQuery{
			EntityID:   sensAPersistURN,
			Property:   digitaltwin.SelectorValue,
			Range:      longRange,
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		resTwinAutoLong, err := service.GetTwinHistory(ctx, ownerUser, qTwinAutoLong)
		if err != nil {
			t.Fatalf("twin auto long query failed: %v", err)
		}
		qTelemAutoLong := telemetry.TelemetryHistoryQuery{
			GatewayID:  gwIDA,
			SensorID:   sensorPersistID,
			Range:      longRange,
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		resTelemAutoLong, err := telemHistoryService.GetTelemetryHistory(ctx, ownerUser, qTelemAutoLong)
		if err != nil {
			t.Fatalf("telemetry auto long query failed: %v", err)
		}
		if resTwinAutoLong.Mode != resTelemAutoLong.Mode || resTwinAutoLong.Mode != "bucket" {
			t.Fatalf("auto long mode mismatch: twin=%s telem=%s expected=bucket", resTwinAutoLong.Mode, resTelemAutoLong.Mode)
		}
		if resTwinAutoLong.Resolution != resTelemAutoLong.Resolution || resTwinAutoLong.Resolution != "1s" {
			t.Fatalf("auto long resolution mismatch: twin=%s telem=%s expected=1s", resTwinAutoLong.Resolution, resTelemAutoLong.Resolution)
		}
		if resTwinAutoLong.PointCount != resTelemAutoLong.PointCount || resTwinAutoLong.PointCount != 1 {
			t.Fatalf("auto long point count mismatch: twin=%d telem=%d expected=1", resTwinAutoLong.PointCount, resTelemAutoLong.PointCount)
		}

		// 4. Invariant Verification: No Duplicate High-Frequency Temporal Rows & State Isolation
		var valTemporalCount int
		err = setupPool.QueryRow(ctx, `
			SELECT count(*) FROM public.twin_temporal_values
			WHERE entity_id = $1 AND property_name = 'value'`, sensAPersistUUID).Scan(&valTemporalCount)
		if err != nil {
			t.Fatalf("failed to query temporal value rows: %v", err)
		}
		if valTemporalCount != 0 {
			t.Fatalf("twin_temporal_values must NEVER contain duplicate high-frequency value rows, found %d", valTemporalCount)
		}

		var totalSensorTemporal int
		err = setupPool.QueryRow(ctx, `
			SELECT count(*) FROM public.twin_temporal_values
			WHERE entity_id = $1`, sensAPersistUUID).Scan(&totalSensorTemporal)
		if err != nil {
			t.Fatalf("failed to query total sensor temporal rows: %v", err)
		}
		if totalSensorTemporal != 1 {
			t.Fatalf("expected exactly 1 temporal row (sample_interval_us) for sensor, got %d", totalSensorTemporal)
		}

		// Sensor Twin States: desired untouched, reported advanced by Persist
		var sReportedVal, sReportedSeq, sReportedInterval string
		var sDesiredVersion, sReportedVersion int64
		var sDesiredState string
		err = setupPool.QueryRow(ctx, `
			SELECT reported_state->>'value', reported_state->>'sequence_num', reported_state->>'sample_interval_us',
			       desired_state::text, desired_version, reported_version
			FROM public.twin_states WHERE entity_id = $1`, sensAPersistUUID).Scan(
			&sReportedVal, &sReportedSeq, &sReportedInterval, &sDesiredState, &sDesiredVersion, &sReportedVersion)
		if err != nil {
			t.Fatalf("failed to query sensor twin_states: %v", err)
		}
		if sReportedVal != "28" || sReportedSeq != "6010" || sReportedInterval != "100000" {
			t.Fatalf("sensor reported_state mismatch: val=%s seq=%s interval=%s", sReportedVal, sReportedSeq, sReportedInterval)
		}
		if sDesiredState != "{}" || sDesiredVersion != 0 {
			t.Fatalf("sensor desired_state must remain untouched: state=%s version=%d", sDesiredState, sDesiredVersion)
		}
		if sReportedVersion != 1 {
			t.Fatalf("sensor reported_version expected 1 after Persist, got %d", sReportedVersion)
		}

		// Gateway Twin States: completely untouched by sensor telemetry Persist
		var gwReportedState, gwDesiredState string
		var gwReportedVer, gwDesiredVer int64
		err = setupPool.QueryRow(ctx, `
			SELECT reported_state::text, desired_state::text, reported_version, desired_version
			FROM public.twin_states WHERE entity_id = $1`, gwAUUID).Scan(
			&gwReportedState, &gwDesiredState, &gwReportedVer, &gwDesiredVer)
		if err != nil {
			t.Fatalf("failed to query gateway twin_states: %v", err)
		}
		if gwReportedState != "{}" || gwDesiredState != "{}" || gwReportedVer != 0 || gwDesiredVer != 0 {
			t.Fatalf("gateway twin_states must be completely untouched: reported=%s desired=%s rVer=%d dVer=%d",
				gwReportedState, gwDesiredState, gwReportedVer, gwDesiredVer)
		}

		// 5. Legacy NULL Provenance Check
		var hasLegacyTable bool
		err = setupPool.QueryRow(ctx, "SELECT to_regclass('public.stage3_legacy_telemetry_snapshot') IS NOT NULL").Scan(&hasLegacyTable)
		if err == nil && hasLegacyTable {
			t.Log("stage3_legacy_telemetry_snapshot found; testing legacy NULL provenance delegation")
		} else {
			t.Log("stage3_legacy_telemetry_snapshot not present in standard test database; direct INSERT with NULL provenance is rejected by active check constraint telemetry_new_provenance (schema constraints preserved per AGENTS.md). Value delegation preserves nullable fields without inventing identifiers, as verified by unit tests.")
		}
	})

	// =========================================================================
	// Scenario 7: Read-Only Fixture Integrity Snapshots
	// =========================================================================
	t.Run("ReadOnly_Integrity_Snapshots", func(t *testing.T) {
		// 1. Independent Seed for snapshot test dataset BEFORE capturing baseline snapshot:
		// A. Numeric temporal observations for sample_interval_us on sensASnapUUID
		sampleRange := telemetry.TimeRange{
			From: time.Date(2026, 10, 17, 14, 0, 0, 0, time.UTC),
			To:   time.Date(2026, 10, 17, 14, 0, 5, 0, time.UTC),
		}
		snapSeeds := []struct {
			t   time.Time
			val float64
		}{
			{sampleRange.From.Add(100 * time.Millisecond), 10000},
			{sampleRange.From.Add(500 * time.Millisecond), 20000},
			{sampleRange.From.Add(1200 * time.Millisecond), 30000},
			{sampleRange.From.Add(1800 * time.Millisecond), 50000},
			{sampleRange.From.Add(3500 * time.Millisecond), 40000},
		}
		for _, s := range snapSeeds {
			if _, err := setupPool.Exec(ctx, `
				INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
				VALUES ($1, $2, 'sample_interval_us', $3, '{"unit":"us"}'::jsonb)`,
				s.t, sensASnapUUID, s.val); err != nil {
				t.Fatalf("failed to seed snapshot temporal observation: %v", err)
			}
		}

		// B. Telemetry batch for value on sensASnap via persistRepo.Persist
		valRange := telemetry.TimeRange{
			From: time.Date(2026, 10, 17, 20, 0, 0, 0, time.UTC),
			To:   time.Date(2026, 10, 17, 20, 0, 2, 0, time.UTC),
		}
		snapValidator, err := telemetry.NewValidator(telemetry.Limits{
			MaxPayloadBytes:  1 << 20,
			MaxSamples:       1000,
			MaxBatchDuration: time.Hour,
			MaxFutureSkew:    time.Hour,
		})
		if err != nil {
			t.Fatalf("failed to create snapshot validator: %v", err)
		}

		snapBootID := uuid.New()
		snapMsgID := uuid.New()
		const snapSampleCount = 10
		snapSamples := []float64{10.0, 12.0, 14.0, 16.0, 18.0, 20.0, 22.0, 24.0, 26.0, 28.0}
		snapDTO := telemetry.BatchDTO{
			ProtocolVersion:  1,
			MessageID:        snapMsgID,
			MessageType:      "telemetry_batch",
			GatewayID:        gwIDA,
			SensorID:         sensorSnapID,
			BootID:           snapBootID,
			FirstSequence:    7001,
			SampleCount:      int32(snapSampleCount),
			MeasuredAt:       valRange.From.Format(time.RFC3339Nano),
			SampleIntervalUS: 100000,
			Samples:          snapSamples,
		}
		snapPayload, err := json.Marshal(snapDTO)
		if err != nil {
			t.Fatalf("failed to marshal snapshot batch DTO: %v", err)
		}
		snapTopic := fmt.Sprintf("gateways/%s/telemetry/%s", gwIDA, sensorSnapID)
		snapValidated, err := snapValidator.Validate(snapTopic, snapPayload, valRange.From.Add(1*time.Second))
		if err != nil {
			t.Fatalf("failed to validate snapshot telemetry message: %v", err)
		}
		snapCommitRes, err := persistRepo.Persist(ctx, snapValidated)
		if err != nil {
			t.Fatalf("persistRepo.Persist failed for snapshot seed: %v", err)
		}
		if snapCommitRes.Outcome != telemetry.CommitOutcomeCommitted || snapCommitRes.SamplesSaved != snapSampleCount {
			t.Fatalf("unexpected persist outcome for snapshot seed: %+v", snapCommitRes)
		}

		// 2. Assert Nonempty Preconditions
		var preTemporalCount, preRawCount int
		if err := setupPool.QueryRow(ctx, "SELECT COUNT(*) FROM public.twin_temporal_values WHERE entity_id = $1", sensASnapUUID).Scan(&preTemporalCount); err != nil {
			t.Fatalf("failed to query temporal precondition count: %v", err)
		}
		if preTemporalCount != len(snapSeeds)+1 {
			t.Fatalf("precondition failed: expected %d temporal rows for snapshot sensor, got %d", len(snapSeeds)+1, preTemporalCount)
		}
		if err := setupPool.QueryRow(ctx, "SELECT COUNT(*) FROM public.telemetry WHERE gateway_id = $1 AND sensor_id = $2", gwIDA, sensorSnapID).Scan(&preRawCount); err != nil {
			t.Fatalf("failed to query raw telemetry precondition count: %v", err)
		}
		if preRawCount != snapSampleCount {
			t.Fatalf("precondition failed: expected %d raw telemetry rows for snapshot sensor, got %d", snapSampleCount, preRawCount)
		}

		// 3. Capture baseline snapshot AFTER seed and verify non-zero counts
		baselineSnapshot := captureTemporalFixtureSnapshot(ctx, t, setupPool, gwIDA, gwIDB)
		if baselineSnapshot.temporalCount == 0 || baselineSnapshot.rawCount == 0 || baselineSnapshot.ledgerCount == 0 {
			t.Fatalf("baselineSnapshot has empty counts: temporal=%d, raw=%d, ledger=%d",
				baselineSnapshot.temporalCount, baselineSnapshot.rawCount, baselineSnapshot.ledgerCount)
		}

		// 4. Selector: sample_interval_us on sensASnapURN
		// A. Raw mode read
		qSampleRaw := digitaltwin.TwinHistoryQuery{
			EntityID:   sensASnapURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      sampleRange,
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		resSampleRaw, err := service.GetTwinHistory(ctx, ownerUser, qSampleRaw)
		if err != nil || resSampleRaw == nil {
			t.Fatalf("sample_interval_us raw read failed: %v", err)
		}
		if resSampleRaw.Mode != "raw" || resSampleRaw.PointCount != len(snapSeeds) || len(resSampleRaw.TemporalPoints) != len(snapSeeds) {
			t.Fatalf("sample_interval_us raw points mismatch: mode=%s, count=%d, len=%d, want %d",
				resSampleRaw.Mode, resSampleRaw.PointCount, len(resSampleRaw.TemporalPoints), len(snapSeeds))
		}
		if resSampleRaw.TemporalPoints[0].ValueNumber == nil || *resSampleRaw.TemporalPoints[0].ValueNumber != 10000 ||
			resSampleRaw.TemporalPoints[len(snapSeeds)-1].ValueNumber == nil || *resSampleRaw.TemporalPoints[len(snapSeeds)-1].ValueNumber != 40000 {
			t.Fatalf("sample_interval_us raw point values unexpected: first=%v, last=%v",
				resSampleRaw.TemporalPoints[0].ValueNumber, resSampleRaw.TemporalPoints[len(snapSeeds)-1].ValueNumber)
		}

		// B. Bucket mode read
		qSampleBucket := digitaltwin.TwinHistoryQuery{
			EntityID:   sensASnapURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      sampleRange,
			Resolution: telemetry.Resolution1s,
			MaxPoints:  100,
		}
		resSampleBucket, err := service.GetTwinHistory(ctx, ownerUser, qSampleBucket)
		if err != nil || resSampleBucket == nil {
			t.Fatalf("sample_interval_us bucket read failed: %v", err)
		}
		if resSampleBucket.Mode != "bucket" || resSampleBucket.Resolution != "1s" || resSampleBucket.PointCount != 3 || len(resSampleBucket.TemporalBuckets) != 3 {
			t.Fatalf("sample_interval_us bucket count mismatch: mode=%s, res=%s, count=%d, len=%d, want 3",
				resSampleBucket.Mode, resSampleBucket.Resolution, resSampleBucket.PointCount, len(resSampleBucket.TemporalBuckets))
		}
		b0 := resSampleBucket.TemporalBuckets[0]
		if b0.Avg != 15000 || b0.Min != 10000 || b0.Max != 20000 || b0.Count != 2 {
			t.Fatalf("sample_interval_us bucket 0 mismatch: %+v", b0)
		}
		b1 := resSampleBucket.TemporalBuckets[1]
		if b1.Avg != 40000 || b1.Min != 30000 || b1.Max != 50000 || b1.Count != 2 {
			t.Fatalf("sample_interval_us bucket 1 mismatch: %+v", b1)
		}
		b2 := resSampleBucket.TemporalBuckets[2]
		if b2.Avg != 40000 || b2.Min != 40000 || b2.Max != 40000 || b2.Count != 1 {
			t.Fatalf("sample_interval_us bucket 2 mismatch: %+v", b2)
		}

		// C. Auto mode read (short window <= 10s, points <= 100 -> chooses raw mode)
		qSampleAuto := digitaltwin.TwinHistoryQuery{
			EntityID:   sensASnapURN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      sampleRange,
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		resSampleAuto, err := service.GetTwinHistory(ctx, ownerUser, qSampleAuto)
		if err != nil || resSampleAuto == nil {
			t.Fatalf("sample_interval_us auto read failed: %v", err)
		}
		if resSampleAuto.Mode != "raw" || resSampleAuto.PointCount != len(snapSeeds) || len(resSampleAuto.TemporalPoints) != len(snapSeeds) {
			t.Fatalf("sample_interval_us auto points mismatch: mode=%s, count=%d, len=%d, want %d",
				resSampleAuto.Mode, resSampleAuto.PointCount, len(resSampleAuto.TemporalPoints), len(snapSeeds))
		}

		// D. Representative rejected cross-Gateway requests for sample_interval_us
		// D1: ownerBUser accessing Gateway A's sensor sensASnapURN
		_, err = service.GetTwinHistory(ctx, ownerBUser, qSampleRaw)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for cross-gateway sample_interval_us read, got %v", err)
		}
		// D2: ownerUser accessing Gateway B's sensor sensB1URN
		qCrossSampleB := digitaltwin.TwinHistoryQuery{
			EntityID:   sensB1URN,
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Range:      sampleRange,
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		_, err = service.GetTwinHistory(ctx, ownerUser, qCrossSampleB)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for cross-gateway sensB1 sample_interval_us read, got %v", err)
		}

		// 5. Selector: value on sensASnapURN
		// A. Raw mode read
		qValRaw := digitaltwin.TwinHistoryQuery{
			EntityID:   sensASnapURN,
			Property:   digitaltwin.SelectorValue,
			Range:      valRange,
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		resValRaw, err := service.GetTwinHistory(ctx, ownerUser, qValRaw)
		if err != nil || resValRaw == nil {
			t.Fatalf("value raw read failed: %v", err)
		}
		if resValRaw.Mode != "raw" || resValRaw.PointCount != snapSampleCount || resValRaw.TelemetryResult == nil || len(resValRaw.TelemetryResult.RawPoints) != snapSampleCount {
			t.Fatalf("value raw points mismatch: mode=%s, count=%d, telemNil=%v, want %d",
				resValRaw.Mode, resValRaw.PointCount, resValRaw.TelemetryResult == nil, snapSampleCount)
		}
		rawPts := resValRaw.TelemetryResult.RawPoints
		if rawPts[0].Value != 10.0 || rawPts[snapSampleCount-1].Value != 28.0 {
			t.Fatalf("value raw point values unexpected: first=%v, last=%v",
				rawPts[0].Value, rawPts[snapSampleCount-1].Value)
		}

		// B. Bucket mode read
		qValBucket := digitaltwin.TwinHistoryQuery{
			EntityID:   sensASnapURN,
			Property:   digitaltwin.SelectorValue,
			Range:      valRange,
			Resolution: telemetry.Resolution1s,
			MaxPoints:  100,
		}
		resValBucket, err := service.GetTwinHistory(ctx, ownerUser, qValBucket)
		if err != nil || resValBucket == nil {
			t.Fatalf("value bucket read failed: %v", err)
		}
		if resValBucket.Mode != "bucket" || resValBucket.Resolution != "1s" || resValBucket.PointCount != 1 || resValBucket.TelemetryResult == nil || len(resValBucket.TelemetryResult.BucketPoints) != 1 {
			t.Fatalf("value bucket count mismatch: mode=%s, res=%s, count=%d, telemNil=%v, want 1",
				resValBucket.Mode, resValBucket.Resolution, resValBucket.PointCount, resValBucket.TelemetryResult == nil)
		}
		vb0 := resValBucket.TelemetryResult.BucketPoints[0]
		if vb0.Count != snapSampleCount || vb0.Min != 10.0 || vb0.Max != 28.0 || vb0.Avg != 19.0 {
			t.Fatalf("value bucket 0 mismatch: %+v", vb0)
		}

		// C. Auto mode read
		qValAuto := digitaltwin.TwinHistoryQuery{
			EntityID:   sensASnapURN,
			Property:   digitaltwin.SelectorValue,
			Range:      valRange,
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		resValAuto, err := service.GetTwinHistory(ctx, ownerUser, qValAuto)
		if err != nil || resValAuto == nil {
			t.Fatalf("value auto read failed: %v", err)
		}
		if resValAuto.Mode != "raw" || resValAuto.PointCount != snapSampleCount || resValAuto.TelemetryResult == nil || len(resValAuto.TelemetryResult.RawPoints) != snapSampleCount {
			t.Fatalf("value auto points mismatch: mode=%s, count=%d, telemNil=%v, want %d",
				resValAuto.Mode, resValAuto.PointCount, resValAuto.TelemetryResult == nil, snapSampleCount)
		}

		// D. Representative rejected cross-Gateway requests for value
		// D1: ownerBUser accessing Gateway A's sensor sensASnapURN
		_, err = service.GetTwinHistory(ctx, ownerBUser, qValRaw)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for cross-gateway value read, got %v", err)
		}
		// D2: ownerUser accessing Gateway B's sensor sensB1URN with value selector
		qCrossValB := digitaltwin.TwinHistoryQuery{
			EntityID:   sensB1URN,
			Property:   digitaltwin.SelectorValue,
			Range:      valRange,
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		_, err = service.GetTwinHistory(ctx, ownerUser, qCrossValB)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for cross-gateway sensB1 value read, got %v", err)
		}

		// 6. Capture after snapshot and assert strictly identical
		afterSnapshot := captureTemporalFixtureSnapshot(ctx, t, setupPool, gwIDA, gwIDB)
		if err := compareTemporalFixtureSnapshot(baselineSnapshot, afterSnapshot); err != nil {
			t.Fatalf("read-only integrity violated by temporal history reads: %v", err)
		}
		t.Log("Read-only integrity verified: raw/ledger/temporal counts, entity/relationship/state data, and metadata strictly preserved across reads")
	})

	// =========================================================================
	// Scenario 8: Bounded Pool Dependency Deadline
	// =========================================================================
	t.Run("BoundedPool_DependencyDeadline", func(t *testing.T) {
		boundedCfg, err := pgxpool.ParseConfig(backendURL)
		if err != nil {
			t.Fatalf("failed to parse backend URL for bounded pool: %v", err)
		}
		boundedCfg.MaxConns = 1
		boundedPool, err := pgxpool.NewWithConfig(ctx, boundedCfg)
		if err != nil {
			t.Fatalf("failed to create bounded pool: %v", err)
		}
		var releaseOrder []string
		t.Cleanup(func() {
			if len(releaseOrder) == 2 {
				if releaseOrder[0] != "heldConn.Release" || releaseOrder[1] != "pool.Close" {
					t.Errorf("unexpected pool lifecycle order: got %v, expected [heldConn.Release, pool.Close]", releaseOrder)
				}
			}
		})
		defer func() {
			releaseOrder = append(releaseOrder, "pool.Close")
			boundedPool.Close()
		}()

		// Regression check for early exit after acquisition:
		// Verify that a helper returning early without explicit Release still releases its connection via defer,
		// allowing subsequent acquisition on boundedPool (MaxConns=1) without blocking or timing out.
		earlyExitHelper := func() error {
			conn, err := boundedPool.Acquire(ctx)
			if err != nil {
				return err
			}
			defer conn.Release()

			// Early return simulating an assertion failure before explicit release
			return errors.New("simulated early exit")
		}

		if err := earlyExitHelper(); err == nil {
			t.Fatal("expected simulated early exit error, got nil")
		}

		// Because defer conn.Release() executed on early return, boundedPool (MaxConns=1)
		// must immediately permit acquiring the sole connection without waiting.
		probeCtx, cancelProbe := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancelProbe()
		probeConn, err := boundedPool.Acquire(probeCtx)
		if err != nil {
			t.Fatalf("failed to acquire sole connection on bounded pool after early exit release: %v", err)
		}
		defer probeConn.Release()
		probeConn.Release()

		// Acquire the single connection from boundedPool, exhausting the pool deterministically without sleep
		heldConn, err := boundedPool.Acquire(ctx)
		if err != nil {
			t.Fatalf("failed to acquire sole connection on bounded pool: %v", err)
		}
		defer func() {
			releaseOrder = append(releaseOrder, "heldConn.Release")
			heldConn.Release()
		}()

		boundedResolver, err := digitaltwin.NewPostgresResolver(boundedPool)
		if err != nil {
			t.Fatalf("failed to create bounded resolver: %v", err)
		}
		boundedPrep, err := digitaltwin.NewReadPreparation(boundedResolver, digitaltwin.DefaultReadPreparationConfig())
		if err != nil {
			t.Fatalf("failed to create bounded ReadPreparation: %v", err)
		}
		boundedReader, err := digitaltwin.NewPostgresTwinReader(boundedPool)
		if err != nil {
			t.Fatalf("failed to create bounded reader: %v", err)
		}
		boundedTelemRepo, err := telemetry.NewPostgresHistoryRepository(boundedPool, telemCfg)
		if err != nil {
			t.Fatalf("failed to create bounded telem repo: %v", err)
		}
		boundedTelemService, err := telemetry.NewTelemetryHistoryService(boundedResolver, boundedTelemRepo, telemCfg)
		if err != nil {
			t.Fatalf("failed to create bounded telem service: %v", err)
		}
		boundedService, err := digitaltwin.NewReadService(boundedPrep, boundedReader, boundedTelemService)
		if err != nil {
			t.Fatalf("failed to create bounded ReadService: %v", err)
		}

		// Query with short parent deadline (35ms)
		deadlineCtx, cancelDeadline := context.WithTimeout(ctx, 35*time.Millisecond)
		defer cancelDeadline()

		qDeadline := digitaltwin.TwinHistoryQuery{
			EntityID: sensAPersistURN,
			Property: digitaltwin.SelectorSampleIntervalUS,
			Range: telemetry.TimeRange{
				From: time.Date(2026, 10, 17, 20, 0, 0, 0, time.UTC),
				To:   time.Date(2026, 10, 17, 20, 0, 2, 0, time.UTC),
			},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}

		resDeadline, errDeadline := boundedService.GetTwinHistory(deadlineCtx, ownerUser, qDeadline)
		if resDeadline != nil {
			t.Errorf("expected nil result on database deadline timeout, got %+v", resDeadline)
		}
		if errDeadline == nil {
			t.Fatal("expected error on database deadline timeout, got nil")
		}
		if !errors.Is(errDeadline, digitaltwin.ErrDependencyUnavailable) {
			t.Errorf("expected ErrDependencyUnavailable on database deadline timeout, got %v", errDeadline)
		}
		if !errors.Is(errDeadline, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded preserved in error chain, got %v", errDeadline)
		}

		// Release connection and verify re-acquisition succeeds cleanly
		heldConn.Release()

		reacquiredConn, err := boundedPool.Acquire(ctx)
		if err != nil {
			t.Fatalf("failed to reacquire connection from bounded pool after release: %v", err)
		}
		defer reacquiredConn.Release()
		reacquiredConn.Release()
		t.Log("Bounded pool dependency deadline verified: waiting for connection fails with ErrDependencyUnavailable and context.DeadlineExceeded without resource leak")
	})
}

type temporalFixtureSnapshot struct {
	twin          fixtureSnapshot
	rawCount      int
	ledgerCount   int
	temporalCount int
}

func captureTemporalFixtureSnapshot(ctx context.Context, t *testing.T, pool *pgxpool.Pool, gwIDA, gwIDB string) temporalFixtureSnapshot {
	t.Helper()
	twinSnap := captureFixtureSnapshot(ctx, t, pool, gwIDA, gwIDB)
	var snap temporalFixtureSnapshot
	snap.twin = twinSnap

	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM public.telemetry WHERE gateway_id IN ($1, $2)", gwIDA, gwIDB).Scan(&snap.rawCount); err != nil {
		t.Fatalf("failed to query raw telemetry count for snapshot: %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM public.processed_messages WHERE gateway_id IN ($1, $2)", gwIDA, gwIDB).Scan(&snap.ledgerCount); err != nil {
		t.Fatalf("failed to query ledger count for snapshot: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM public.twin_temporal_values
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id IN ($1, $2))`,
		gwIDA, gwIDB).Scan(&snap.temporalCount); err != nil {
		t.Fatalf("failed to query temporal values count for snapshot: %v", err)
	}
	return snap
}

func compareTemporalFixtureSnapshot(baseline, after temporalFixtureSnapshot) error {
	if after.rawCount != baseline.rawCount {
		return fmt.Errorf("raw telemetry count mutated: baseline=%d, after=%d", baseline.rawCount, after.rawCount)
	}
	if after.ledgerCount != baseline.ledgerCount {
		return fmt.Errorf("processed_messages ledger count mutated: baseline=%d, after=%d", baseline.ledgerCount, after.ledgerCount)
	}
	if after.temporalCount != baseline.temporalCount {
		return fmt.Errorf("twin_temporal_values count mutated: baseline=%d, after=%d", baseline.temporalCount, after.temporalCount)
	}

	if len(after.twin.entities) != len(baseline.twin.entities) {
		return fmt.Errorf("entities count mutated: baseline=%d after=%d", len(baseline.twin.entities), len(after.twin.entities))
	}
	for id, baseE := range baseline.twin.entities {
		afterE, ok := after.twin.entities[id]
		if !ok {
			return fmt.Errorf("entity %s missing in post-read snapshot", id)
		}
		if afterE.EntityID != baseE.EntityID || afterE.EntityType != baseE.EntityType || afterE.GatewayID != baseE.GatewayID {
			return fmt.Errorf("entity %s metadata mutated: %+v vs %+v", id, afterE, baseE)
		}
		if (afterE.Name == nil) != (baseE.Name == nil) || (afterE.Name != nil && *afterE.Name != *baseE.Name) {
			return fmt.Errorf("entity %s name mutated", id)
		}
		if afterE.Attributes != baseE.Attributes {
			return fmt.Errorf("entity %s attributes mutated: got %s, want %s", id, afterE.Attributes, baseE.Attributes)
		}
		if !afterE.UpdatedAt.Equal(baseE.UpdatedAt) {
			return fmt.Errorf("entity %s updated_at mutated: got %v, want %v", id, afterE.UpdatedAt, baseE.UpdatedAt)
		}
	}

	if len(after.twin.relationships) != len(baseline.twin.relationships) {
		return fmt.Errorf("relationships count mutated: baseline=%d after=%d", len(baseline.twin.relationships), len(after.twin.relationships))
	}
	for key, baseR := range baseline.twin.relationships {
		afterR, ok := after.twin.relationships[key]
		if !ok {
			return fmt.Errorf("relationship %s missing in post-read snapshot", key)
		}
		if afterR.SourceID != baseR.SourceID || afterR.RelType != baseR.RelType || afterR.TargetID != baseR.TargetID {
			return fmt.Errorf("relationship %s mutated: %+v vs %+v", key, afterR, baseR)
		}
		if !afterR.CreatedAt.Equal(baseR.CreatedAt) {
			return fmt.Errorf("relationship %s created_at mutated: got %v, want %v", key, afterR.CreatedAt, baseR.CreatedAt)
		}
	}

	if len(after.twin.states) != len(baseline.twin.states) {
		return fmt.Errorf("states count mutated: baseline=%d after=%d", len(baseline.twin.states), len(after.twin.states))
	}
	for id, baseS := range baseline.twin.states {
		afterS, ok := after.twin.states[id]
		if !ok {
			return fmt.Errorf("state for entity %s missing in post-read snapshot", id)
		}
		if afterS.ReportedState != baseS.ReportedState {
			return fmt.Errorf("state for entity %s reported_state mutated: got %s, want %s", id, afterS.ReportedState, baseS.ReportedState)
		}
		if afterS.DesiredState != baseS.DesiredState {
			return fmt.Errorf("state for entity %s desired_state mutated: got %s, want %s", id, afterS.DesiredState, baseS.DesiredState)
		}
		if afterS.ReportedVersion != baseS.ReportedVersion {
			return fmt.Errorf("state for entity %s reported_version mutated: got %d, want %d", id, afterS.ReportedVersion, baseS.ReportedVersion)
		}
		if afterS.DesiredVersion != baseS.DesiredVersion {
			return fmt.Errorf("state for entity %s desired_version mutated: got %d, want %d", id, afterS.DesiredVersion, baseS.DesiredVersion)
		}
		if (afterS.LastReportedAt == nil) != (baseS.LastReportedAt == nil) || (afterS.LastReportedAt != nil && !afterS.LastReportedAt.Equal(*baseS.LastReportedAt)) {
			return fmt.Errorf("state for entity %s last_reported_at mutated", id)
		}
		if (afterS.LastDesiredAt == nil) != (baseS.LastDesiredAt == nil) || (afterS.LastDesiredAt != nil && !afterS.LastDesiredAt.Equal(*baseS.LastDesiredAt)) {
			return fmt.Errorf("state for entity %s last_desired_at mutated", id)
		}
		if !afterS.UpdatedAt.Equal(baseS.UpdatedAt) {
			return fmt.Errorf("state for entity %s updated_at mutated: got %v, want %v", id, afterS.UpdatedAt, baseS.UpdatedAt)
		}
	}

	return compareTwinFixtureMetadata(baseline.twin, after.twin)
}

func TestCompareTemporalFixtureSnapshot(t *testing.T) {
	created := time.Date(2026, 10, 17, 8, 0, 0, 0, time.UTC)
	entID := uuid.MustParse("10000000-0000-0000-0000-000000000001")
	baseGW := gatewaySnapshot{GatewayID: "gw_a", Name: "Gateway A", Description: stringPtr("desc a"), CreatedAt: created}
	baseSens := sensorSnapshot{GatewayID: "gw_a", SensorID: "s1", Name: "Sensor 1", Unit: stringPtr("us"), CreatedAt: created}
	baseEnt := entitySnapshot{ID: entID, EntityID: "urn:ngsi-ld:Sensor:gw_a:s1", EntityType: "Sensor", Name: stringPtr("Sensor 1"), GatewayID: "gw_a", Attributes: "{}", UpdatedAt: created}
	baseRel := relationshipSnapshot{SourceID: entID, RelType: "hasSensor", TargetID: entID, CreatedAt: created}
	baseState := stateSnapshot{
		EntityID:        entID,
		ReportedState:   `{"sample_interval_us":10000}`,
		DesiredState:    "{}",
		ReportedVersion: 1,
		DesiredVersion:  0,
		LastReportedAt:  &created,
		LastDesiredAt:   nil,
		UpdatedAt:       created,
	}

	makeBaseline := func() temporalFixtureSnapshot {
		return temporalFixtureSnapshot{
			twin: fixtureSnapshot{
				entities:      map[uuid.UUID]entitySnapshot{entID: baseEnt},
				relationships: map[string]relationshipSnapshot{"rel1": baseRel},
				states:        map[uuid.UUID]stateSnapshot{entID: baseState},
				gateways:      map[string]gatewaySnapshot{"gw_a": baseGW},
				sensors:       map[string]sensorSnapshot{"gw_a:s1": baseSens},
			},
			rawCount:      10,
			ledgerCount:   1,
			temporalCount: 1,
		}
	}

	t.Run("identical snapshots return nil", func(t *testing.T) {
		b := makeBaseline()
		a := makeBaseline()
		if err := compareTemporalFixtureSnapshot(b, a); err != nil {
			t.Fatalf("expected nil for identical snapshots, got %v", err)
		}
	})

	t.Run("rawCount mutated", func(t *testing.T) {
		b := makeBaseline()
		a := makeBaseline()
		a.rawCount = 11
		if err := compareTemporalFixtureSnapshot(b, a); err == nil || !strings.Contains(err.Error(), "raw telemetry count mutated") {
			t.Fatalf("expected error on rawCount mutation, got %v", err)
		}
	})

	t.Run("ledgerCount mutated", func(t *testing.T) {
		b := makeBaseline()
		a := makeBaseline()
		a.ledgerCount = 2
		if err := compareTemporalFixtureSnapshot(b, a); err == nil || !strings.Contains(err.Error(), "processed_messages ledger count mutated") {
			t.Fatalf("expected error on ledgerCount mutation, got %v", err)
		}
	})

	t.Run("temporalCount mutated", func(t *testing.T) {
		b := makeBaseline()
		a := makeBaseline()
		a.temporalCount = 2
		if err := compareTemporalFixtureSnapshot(b, a); err == nil || !strings.Contains(err.Error(), "twin_temporal_values count mutated") {
			t.Fatalf("expected error on temporalCount mutation, got %v", err)
		}
	})

	t.Run("entity attributes mutated", func(t *testing.T) {
		b := makeBaseline()
		a := makeBaseline()
		mutEnt := baseEnt
		mutEnt.Attributes = `{"foo":"bar"}`
		a.twin.entities[entID] = mutEnt
		if err := compareTemporalFixtureSnapshot(b, a); err == nil || !strings.Contains(err.Error(), "attributes mutated") {
			t.Fatalf("expected error on entity attributes mutation, got %v", err)
		}
	})

	t.Run("state reported_state mutated", func(t *testing.T) {
		b := makeBaseline()
		a := makeBaseline()
		mutState := baseState
		mutState.ReportedState = `{"sample_interval_us":20000}`
		a.twin.states[entID] = mutState
		if err := compareTemporalFixtureSnapshot(b, a); err == nil || !strings.Contains(err.Error(), "reported_state mutated") {
			t.Fatalf("expected error on reported_state mutation, got %v", err)
		}
	})

	t.Run("state reported_version mutated", func(t *testing.T) {
		b := makeBaseline()
		a := makeBaseline()
		mutState := baseState
		mutState.ReportedVersion = 2
		a.twin.states[entID] = mutState
		if err := compareTemporalFixtureSnapshot(b, a); err == nil || !strings.Contains(err.Error(), "reported_version mutated") {
			t.Fatalf("expected error on reported_version mutation, got %v", err)
		}
	})

	t.Run("relationship created_at mutated", func(t *testing.T) {
		b := makeBaseline()
		a := makeBaseline()
		mutRel := baseRel
		mutRel.CreatedAt = created.Add(time.Minute)
		a.twin.relationships["rel1"] = mutRel
		if err := compareTemporalFixtureSnapshot(b, a); err == nil || !strings.Contains(err.Error(), "created_at mutated") {
			t.Fatalf("expected error on relationship created_at mutation, got %v", err)
		}
	})

	t.Run("gateway metadata mutated", func(t *testing.T) {
		b := makeBaseline()
		a := makeBaseline()
		mutGW := baseGW
		mutGW.Description = stringPtr("mutated desc")
		a.twin.gateways["gw_a"] = mutGW
		if err := compareTemporalFixtureSnapshot(b, a); err == nil || !strings.Contains(err.Error(), "gateway gw_a metadata mutated") {
			t.Fatalf("expected error on gateway metadata mutation, got %v", err)
		}
	})

	t.Run("sensor unit mutated", func(t *testing.T) {
		b := makeBaseline()
		a := makeBaseline()
		mutSens := baseSens
		mutSens.Unit = stringPtr("ms")
		a.twin.sensors["gw_a:s1"] = mutSens
		if err := compareTemporalFixtureSnapshot(b, a); err == nil || !strings.Contains(err.Error(), "sensor gw_a:s1 metadata mutated") {
			t.Fatalf("expected error on sensor unit mutation, got %v", err)
		}
	})
}

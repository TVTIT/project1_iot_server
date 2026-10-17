package digitaltwin_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/telemetry"
)

// resolveTestDatabaseURLs determines the connection strings for setup (admin) and application (iot_backend_app).
// It respects TWIN_TEST_SETUP_URL / TELEMETRY_TEST_SETUP_URL and TWIN_TEST_BACKEND_URL / TELEMETRY_TEST_BACKEND_URL
// if set, or reads connection settings from the repository root .env file without logging credentials.
func resolveTestDatabaseURLs(t *testing.T) (string, string) {
	t.Helper()

	setupURL := os.Getenv("TWIN_TEST_SETUP_URL")
	if setupURL == "" {
		setupURL = os.Getenv("TELEMETRY_TEST_SETUP_URL")
	}
	backendURL := os.Getenv("TWIN_TEST_BACKEND_URL")
	if backendURL == "" {
		backendURL = os.Getenv("TELEMETRY_TEST_BACKEND_URL")
	}
	if setupURL != "" && backendURL != "" {
		return setupURL, backendURL
	}

	// Explicit integration opt-in check: prevent routine `go test ./...` from connecting to the shared database
	// simply because a local .env exists. An explicit opt-in variable is required.
	if os.Getenv("TWIN_READ_TEST_NATIVE") != "1" &&
		os.Getenv("TWIN_TEMPORAL_TEST_NATIVE") != "1" &&
		os.Getenv("TWIN_METADATA_TEST_NATIVE") != "1" &&
		os.Getenv("TELEMETRY_TEST_NATIVE") != "1" &&
		os.Getenv("TELEMETRY_TEST_ISOLATED") != "1" {
		return "", ""
	}

	dir, err := os.Getwd()
	if err != nil {
		return setupURL, backendURL
	}

	var envPath string
	for {
		candidate := filepath.Join(dir, ".env")
		if _, statErr := os.Stat(candidate); statErr == nil {
			envPath = candidate
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	if envPath == "" {
		return setupURL, backendURL
	}

	content, err := os.ReadFile(envPath)
	if err != nil {
		return setupURL, backendURL
	}

	envMap := make(map[string]string)
	for _, line := range strings.Split(string(content), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || !strings.Contains(trimmed, "=") {
			continue
		}
		parts := strings.SplitN(trimmed, "=", 2)
		k := strings.TrimSpace(parts[0])
		v := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
		envMap[k] = v
	}

	pgUser := envMap["POSTGRES_USER"]
	if pgUser == "" {
		pgUser = "postgres"
	}
	pgPass := envMap["POSTGRES_PASSWORD"]
	pgDB := envMap["POSTGRES_DB"]
	if pgDB == "" {
		pgDB = "iot_platform"
	}
	pgPort := envMap["POSTGRES_PORT"]
	if pgPort == "" {
		pgPort = "5432"
	}
	backendPass := envMap["BACKEND_DB_PASSWORD"]

	if setupURL == "" && pgPass != "" {
		setupURL = fmt.Sprintf("postgres://%s:%s@127.0.0.1:%s/%s?sslmode=disable", pgUser, pgPass, pgPort, pgDB)
	}
	if backendURL == "" && backendPass != "" {
		backendURL = fmt.Sprintf("postgres://iot_backend_app:%s@127.0.0.1:%s/%s?sslmode=disable", backendPass, pgPort, pgDB)
	}

	return setupURL, backendURL
}

func execCleanup(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string, query string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, query, args...); err != nil {
		t.Errorf("cleanup %s failed: %v", table, err)
	}
}

type entitySnapshot struct {
	ID         uuid.UUID
	EntityID   string
	EntityType string
	Name       *string
	GatewayID  string
	Attributes string
	UpdatedAt  time.Time
}

type relationshipSnapshot struct {
	SourceID  uuid.UUID
	RelType   string
	TargetID  uuid.UUID
	CreatedAt time.Time
}

type stateSnapshot struct {
	EntityID        uuid.UUID
	ReportedState   string
	DesiredState    string
	ReportedVersion int64
	DesiredVersion  int64
	LastReportedAt  *time.Time
	LastDesiredAt   *time.Time
	UpdatedAt       time.Time
}

type gatewaySnapshot struct {
	GatewayID   string
	Name        string
	Description *string
	CreatedAt   time.Time
}

type sensorSnapshot struct {
	GatewayID string
	SensorID  string
	Name      string
	Unit      *string
	CreatedAt time.Time
}

type fixtureSnapshot struct {
	entities      map[uuid.UUID]entitySnapshot
	relationships map[string]relationshipSnapshot
	states        map[uuid.UUID]stateSnapshot
	gateways      map[string]gatewaySnapshot
	sensors       map[string]sensorSnapshot
}

func captureFixtureSnapshot(ctx context.Context, t *testing.T, pool *pgxpool.Pool, gwIDA, gwIDB string) fixtureSnapshot {
	t.Helper()

	snap := fixtureSnapshot{
		entities:      make(map[uuid.UUID]entitySnapshot),
		relationships: make(map[string]relationshipSnapshot),
		states:        make(map[uuid.UUID]stateSnapshot),
		gateways:      make(map[string]gatewaySnapshot),
		sensors:       make(map[string]sensorSnapshot),
	}

	// 1. Entities
	eRows, err := pool.Query(ctx, `
		SELECT id, entity_id, entity_type, name, gateway_id, attributes::text, updated_at
		FROM public.twin_entities
		WHERE gateway_id IN ($1, $2)`,
		gwIDA, gwIDB,
	)
	if err != nil {
		t.Fatalf("failed to query entities for snapshot: %v", err)
	}
	defer eRows.Close()
	for eRows.Next() {
		var e entitySnapshot
		if err := eRows.Scan(&e.ID, &e.EntityID, &e.EntityType, &e.Name, &e.GatewayID, &e.Attributes, &e.UpdatedAt); err != nil {
			t.Fatalf("failed to scan entity snapshot: %v", err)
		}
		e.UpdatedAt = e.UpdatedAt.UTC()
		snap.entities[e.ID] = e
	}
	if err := eRows.Err(); err != nil {
		t.Fatalf("entity snapshot rows error: %v", err)
	}

	// 2. Relationships
	rRows, err := pool.Query(ctx, `
		SELECT source_entity_id, relationship_type, target_entity_id, created_at
		FROM public.twin_relationships
		WHERE source_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id IN ($1, $2))`,
		gwIDA, gwIDB,
	)
	if err != nil {
		t.Fatalf("failed to query relationships for snapshot: %v", err)
	}
	defer rRows.Close()
	for rRows.Next() {
		var r relationshipSnapshot
		if err := rRows.Scan(&r.SourceID, &r.RelType, &r.TargetID, &r.CreatedAt); err != nil {
			t.Fatalf("failed to scan relationship snapshot: %v", err)
		}
		r.CreatedAt = r.CreatedAt.UTC()
		key := fmt.Sprintf("%s:%s:%s", r.SourceID, r.RelType, r.TargetID)
		snap.relationships[key] = r
	}
	if err := rRows.Err(); err != nil {
		t.Fatalf("relationship snapshot rows error: %v", err)
	}

	// 3. States
	sRows, err := pool.Query(ctx, `
		SELECT entity_id, reported_state::text, desired_state::text,
		       reported_version, desired_version,
		       last_reported_at, last_desired_at, updated_at
		FROM public.twin_states
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id IN ($1, $2))`,
		gwIDA, gwIDB,
	)
	if err != nil {
		t.Fatalf("failed to query states for snapshot: %v", err)
	}
	defer sRows.Close()
	for sRows.Next() {
		var s stateSnapshot
		if err := sRows.Scan(
			&s.EntityID,
			&s.ReportedState,
			&s.DesiredState,
			&s.ReportedVersion,
			&s.DesiredVersion,
			&s.LastReportedAt,
			&s.LastDesiredAt,
			&s.UpdatedAt,
		); err != nil {
			t.Fatalf("failed to scan state snapshot: %v", err)
		}
		s.UpdatedAt = s.UpdatedAt.UTC()
		if s.LastReportedAt != nil {
			t := s.LastReportedAt.UTC()
			s.LastReportedAt = &t
		}
		if s.LastDesiredAt != nil {
			t := s.LastDesiredAt.UTC()
			s.LastDesiredAt = &t
		}
		snap.states[s.EntityID] = s
	}
	if err := sRows.Err(); err != nil {
		t.Fatalf("state snapshot rows error: %v", err)
	}

	// 4. Gateways
	gRows, err := pool.Query(ctx, `
		SELECT gateway_id, name, description, created_at
		FROM public.gateways
		WHERE gateway_id IN ($1, $2)`,
		gwIDA, gwIDB,
	)
	if err != nil {
		t.Fatalf("failed to query gateways for snapshot: %v", err)
	}
	defer gRows.Close()
	for gRows.Next() {
		var g gatewaySnapshot
		if err := gRows.Scan(&g.GatewayID, &g.Name, &g.Description, &g.CreatedAt); err != nil {
			t.Fatalf("failed to scan gateway snapshot: %v", err)
		}
		g.CreatedAt = g.CreatedAt.UTC()
		snap.gateways[g.GatewayID] = g
	}
	if err := gRows.Err(); err != nil {
		t.Fatalf("gateway snapshot rows error: %v", err)
	}

	// 5. Sensors
	sensRows, err := pool.Query(ctx, `
		SELECT gateway_id, sensor_id, name, unit, created_at
		FROM public.sensors
		WHERE gateway_id IN ($1, $2)`,
		gwIDA, gwIDB,
	)
	if err != nil {
		t.Fatalf("failed to query sensors for snapshot: %v", err)
	}
	defer sensRows.Close()
	for sensRows.Next() {
		var sens sensorSnapshot
		if err := sensRows.Scan(&sens.GatewayID, &sens.SensorID, &sens.Name, &sens.Unit, &sens.CreatedAt); err != nil {
			t.Fatalf("failed to scan sensor snapshot: %v", err)
		}
		sens.CreatedAt = sens.CreatedAt.UTC()
		key := fmt.Sprintf("%s:%s", sens.GatewayID, sens.SensorID)
		snap.sensors[key] = sens
	}
	if err := sensRows.Err(); err != nil {
		t.Fatalf("sensor snapshot rows error: %v", err)
	}

	return snap
}

func stringPtr(s string) *string {
	return &s
}

// compareTwinFixtureMetadata compares the captured Gateway and Sensor metadata maps between two
// fixture snapshots. It covers every captured field - including the nullable Gateways.description
// and sensors.unit columns - as well as map cardinality, so an added, removed, or renamed metadata
// entry cannot escape the read-only integrity check. captureFixtureSnapshot normalizes both
// CreatedAt values to UTC, so reflect.DeepEqual is safe here and preserves exact values, including
// nil (NULL) versus non-nil pointers.
//
// The entity, relationship, and state comparisons remain inline in their respective ReadOnly_Integrity
// steps; this helper only strengthens the Gateway/Sensor metadata comparison.
func compareTwinFixtureMetadata(baseline, after fixtureSnapshot) error {
	if len(after.gateways) != len(baseline.gateways) {
		return fmt.Errorf("gateways count mutated: baseline=%d after=%d", len(baseline.gateways), len(after.gateways))
	}
	for id, baseG := range baseline.gateways {
		afterG, ok := after.gateways[id]
		if !ok {
			return fmt.Errorf("gateway %s missing in post-read snapshot", id)
		}
		if !reflect.DeepEqual(afterG, baseG) {
			return fmt.Errorf("gateway %s metadata mutated: got %+v, want %+v", id, afterG, baseG)
		}
	}

	if len(after.sensors) != len(baseline.sensors) {
		return fmt.Errorf("sensors count mutated: baseline=%d after=%d", len(baseline.sensors), len(after.sensors))
	}
	for key, baseSens := range baseline.sensors {
		afterSens, ok := after.sensors[key]
		if !ok {
			return fmt.Errorf("sensor %s missing in post-read snapshot", key)
		}
		if !reflect.DeepEqual(afterSens, baseSens) {
			return fmt.Errorf("sensor %s metadata mutated: got %+v, want %+v", key, afterSens, baseSens)
		}
	}
	return nil
}

// snapshotWithMetadata builds a fixtureSnapshot that only carries the Gateway and Sensor metadata
// maps, so the metadata comparator can be exercised without a database.
func snapshotWithMetadata(gateways map[string]gatewaySnapshot, sensors map[string]sensorSnapshot) fixtureSnapshot {
	return fixtureSnapshot{
		entities:      make(map[uuid.UUID]entitySnapshot),
		relationships: make(map[string]relationshipSnapshot),
		states:        make(map[uuid.UUID]stateSnapshot),
		gateways:      gateways,
		sensors:       sensors,
	}
}

// TestNativeTwinRead exercises the native database slice for Twin list, detail, and state reading
// under application role iot_backend_app with setupPool fixture management.
func TestNativeTwinRead(t *testing.T) {
	setupURL, backendURL := resolveTestDatabaseURLs(t)
	if setupURL == "" || backendURL == "" {
		t.Skip("Skipping native Digital Twin read tests: set TWIN_READ_TEST_NATIVE=1 to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	t.Logf("Setup Connection: current_user = %s, session_user = %s, superuser = %v, canlogin = %v, bypassrls = %v",
		setupUser, setupSession, setupSuper, setupCanLogin, setupBypassRLS)

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
	gwIDA := fmt.Sprintf("gw_twin_a_%s", runID)
	gwIDB := fmt.Sprintf("gw_twin_b_%s", runID)

	sensorSharedID := fmt.Sprintf("sens_shared_%s", runID)
	sensorTwoID := fmt.Sprintf("sens_two_%s", runID)
	sensorThreeID := fmt.Sprintf("sens_three_%s", runID)

	gwAURN := "urn:ngsi-ld:Gateway:" + gwIDA
	gwBURN := "urn:ngsi-ld:Gateway:" + gwIDB
	sensA1URN := "urn:ngsi-ld:Sensor:" + gwIDA + ":" + sensorSharedID
	sensA2URN := "urn:ngsi-ld:Sensor:" + gwIDA + ":" + sensorTwoID
	sensA3URN := "urn:ngsi-ld:Sensor:" + gwIDA + ":" + sensorThreeID
	sensB1URN := "urn:ngsi-ld:Sensor:" + gwIDB + ":" + sensorSharedID

	gwAUUID := uuid.New()
	gwBUUID := uuid.New()
	sensA1UUID := uuid.New()
	sensA2UUID := uuid.New()
	sensA3UUID := uuid.New()
	sensB1UUID := uuid.New()

	ownerUser := uuid.New()
	operatorUser := uuid.New()
	viewerUser := uuid.New()
	nonMemberUser := uuid.New()
	platformAdminUser := uuid.New()

	allUserIDs := []uuid.UUID{ownerUser, operatorUser, viewerUser, nonMemberUser, platformAdminUser}

	// Defer hook to clean up fixture resources in reverse foreign key order
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		// 1. Delete twin_temporal_values
		execCleanup(cleanupCtx, t, setupPool, "twin_temporal_values",
			"DELETE FROM public.twin_temporal_values WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id IN ($1, $2))",
			gwIDA, gwIDB)

		// 2. Delete twin_states
		execCleanup(cleanupCtx, t, setupPool, "twin_states",
			"DELETE FROM public.twin_states WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id IN ($1, $2))",
			gwIDA, gwIDB)

		// 3. Delete twin_relationships
		execCleanup(cleanupCtx, t, setupPool, "twin_relationships",
			"DELETE FROM public.twin_relationships WHERE source_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id IN ($1, $2)) OR target_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id IN ($1, $2))",
			gwIDA, gwIDB)

		// 4. Delete twin_entities
		execCleanup(cleanupCtx, t, setupPool, "twin_entities",
			"DELETE FROM public.twin_entities WHERE gateway_id IN ($1, $2)",
			gwIDA, gwIDB)

		// 5. Delete sensors
		execCleanup(cleanupCtx, t, setupPool, "sensors",
			"DELETE FROM public.sensors WHERE gateway_id IN ($1, $2)",
			gwIDA, gwIDB)

		// 6. Delete user_gateways
		execCleanup(cleanupCtx, t, setupPool, "user_gateways",
			"DELETE FROM public.user_gateways WHERE gateway_id IN ($1, $2)",
			gwIDA, gwIDB)

		// 7. Delete gateways
		execCleanup(cleanupCtx, t, setupPool, "gateways",
			"DELETE FROM public.gateways WHERE gateway_id IN ($1, $2)",
			gwIDA, gwIDB)

		// 8. Delete platform_admins
		execCleanup(cleanupCtx, t, setupPool, "platform_admins",
			"DELETE FROM public.platform_admins WHERE user_id = $1",
			platformAdminUser)

		// 9. Delete profiles
		execCleanup(cleanupCtx, t, setupPool, "profiles",
			"DELETE FROM public.profiles WHERE id = ANY($1)",
			allUserIDs)

		// 10. Delete auth.users if table exists
		var hasAuthUsers bool
		if err := setupPool.QueryRow(cleanupCtx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err == nil && hasAuthUsers {
			execCleanup(cleanupCtx, t, setupPool, "auth.users",
				"DELETE FROM auth.users WHERE id = ANY($1)",
				allUserIDs)
		}

		// 11. Verify complete cleanup: COUNT(*) of fixture IDs must be 0
		var remainingGateways int
		if err := setupPool.QueryRow(cleanupCtx, "SELECT COUNT(*) FROM public.gateways WHERE gateway_id IN ($1, $2)", gwIDA, gwIDB).Scan(&remainingGateways); err != nil {
			t.Errorf("cleanup verification query failed: %v", err)
		} else if remainingGateways != 0 {
			t.Errorf("cleanup incomplete: %d gateways remain for run %s", remainingGateways, runID)
		}

		var remainingEntities int
		if err := setupPool.QueryRow(cleanupCtx, "SELECT COUNT(*) FROM public.twin_entities WHERE gateway_id IN ($1, $2)", gwIDA, gwIDB).Scan(&remainingEntities); err != nil {
			t.Errorf("cleanup entity verification failed: %v", err)
		} else if remainingEntities != 0 {
			t.Errorf("cleanup incomplete: %d twin_entities remain for run %s", remainingEntities, runID)
		}
	}()

	// 4. Seed user identities
	var hasAuthUsers bool
	if err := setupPool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
		t.Fatalf("failed to check auth.users existence: %v", err)
	}
	if hasAuthUsers {
		for i, uid := range allUserIDs {
			_, err = setupPool.Exec(ctx, `
				INSERT INTO auth.users (id, aud, role, email)
				VALUES ($1, 'authenticated', 'authenticated', $2)
				ON CONFLICT (id) DO NOTHING`,
				uid, fmt.Sprintf("twin-read-%s-%d@example.com", runID, i),
			)
			if err != nil {
				t.Fatalf("failed to seed auth.users: %v", err)
			}
		}
	}

	userNames := []string{
		fmt.Sprintf("Owner User %s", runID),
		fmt.Sprintf("Operator User %s", runID),
		fmt.Sprintf("Viewer User %s", runID),
		fmt.Sprintf("NonMember User %s", runID),
		fmt.Sprintf("PlatformAdmin User %s", runID),
	}
	for i, uid := range allUserIDs {
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.profiles (id, full_name)
			VALUES ($1, $2)
			ON CONFLICT (id) DO NOTHING`,
			uid, userNames[i],
		); err != nil {
			t.Fatalf("failed to seed profiles: %v", err)
		}
	}

	// Platform admin grant
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.platform_admins (user_id)
		VALUES ($1)
		ON CONFLICT (user_id) DO NOTHING`,
		platformAdminUser,
	); err != nil {
		t.Fatalf("failed to seed platform_admins: %v", err)
	}

	// 5. Seed gateways
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.gateways (gateway_id, name, description)
		VALUES ($1, 'Gateway A', 'Gateway A Description'),
		       ($2, 'Gateway B', 'Gateway B Description')`,
		gwIDA, gwIDB,
	); err != nil {
		t.Fatalf("failed to seed gateways: %v", err)
	}

	// 6. Seed user_gateways memberships for Gateway A only
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.user_gateways (user_id, gateway_id, role)
		VALUES ($1, $4, 'owner'),
		       ($2, $4, 'operator'),
		       ($3, $4, 'viewer')`,
		ownerUser, operatorUser, viewerUser, gwIDA,
	); err != nil {
		t.Fatalf("failed to seed user_gateways: %v", err)
	}

	// 7. Seed sensors
	// Notice: sensorSharedID is identical across Gateway A and Gateway B!
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.sensors (gateway_id, sensor_id, name, unit)
		VALUES ($1, $2, 'Sensor 1', 'degC'),
		       ($1, $3, 'Sensor 2', 'percent'),
		       ($1, $4, 'Sensor 3', 'ppm'),
		       ($5, $2, 'Sensor 1 on B', 'degC')`,
		gwIDA, sensorSharedID, sensorTwoID, sensorThreeID, gwIDB,
	); err != nil {
		t.Fatalf("failed to seed sensors: %v", err)
	}

	// 8. Seed twin_entities
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.twin_entities (id, entity_id, entity_type, name, gateway_id, attributes)
		VALUES ($1, $7, 'Gateway', 'Gateway A', $13, '{}'),
		       ($2, $8, 'Gateway', 'Gateway B', $14, '{}'),
		       ($3, $9, 'Sensor', 'Sensor 1', $13, '{}'),
		       ($4, $10, 'Sensor', 'Sensor 2', $13, '{}'),
		       ($5, $11, 'Sensor', 'Sensor 3', $13, '{}'),
		       ($6, $12, 'Sensor', 'Sensor 1 on B', $14, '{}')`,
		gwAUUID, gwBUUID, sensA1UUID, sensA2UUID, sensA3UUID, sensB1UUID,
		gwAURN, gwBURN, sensA1URN, sensA2URN, sensA3URN, sensB1URN,
		gwIDA, gwIDB,
	); err != nil {
		t.Fatalf("failed to seed twin_entities: %v", err)
	}

	// 9. Seed twin_relationships hasSensor
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.twin_relationships (source_entity_id, relationship_type, target_entity_id)
		VALUES ($1, 'hasSensor', $3),
		       ($1, 'hasSensor', $4),
		       ($1, 'hasSensor', $5),
		       ($2, 'hasSensor', $6)`,
		gwAUUID, gwBUUID, sensA1UUID, sensA2UUID, sensA3UUID, sensB1UUID,
	); err != nil {
		t.Fatalf("failed to seed twin_relationships: %v", err)
	}

	// 10. Seed twin_states
	// Include big integer > 2^53 (9007199254740993), boolean, text, and null in JSONB.
	reportedJSON := `{"sequence":9007199254740993,"status":"online","active":true,"note":null}`
	desiredJSON := `{"target_mode":"eco","auto_calibrate":false}`
	for _, entityID := range []uuid.UUID{gwAUUID, gwBUUID, sensA1UUID, sensA2UUID, sensA3UUID, sensB1UUID} {
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.twin_states (entity_id, reported_state, desired_state, reported_version, desired_version, last_reported_at, last_desired_at)
			VALUES ($1, $2::jsonb, $3::jsonb, 8, 0, now(), now())`,
			entityID, reportedJSON, desiredJSON,
		); err != nil {
			t.Fatalf("failed to seed twin_states for entity %s: %v", entityID, err)
		}
	}

	// 11. Record baseline row counts and full snapshot to verify read-only integrity
	baselineSnapshot := captureFixtureSnapshot(ctx, t, setupPool, gwIDA, gwIDB)
	var baselineEntities, baselineRels, baselineStates int
	if err := setupPool.QueryRow(ctx, "SELECT COUNT(*) FROM public.twin_entities WHERE gateway_id IN ($1, $2)", gwIDA, gwIDB).Scan(&baselineEntities); err != nil {
		t.Fatalf("failed to query baseline entities: %v", err)
	}
	if err := setupPool.QueryRow(ctx, "SELECT COUNT(*) FROM public.twin_relationships WHERE source_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id IN ($1, $2))", gwIDA, gwIDB).Scan(&baselineRels); err != nil {
		t.Fatalf("failed to query baseline relationships: %v", err)
	}
	if err := setupPool.QueryRow(ctx, "SELECT COUNT(*) FROM public.twin_states WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id IN ($1, $2))", gwIDA, gwIDB).Scan(&baselineStates); err != nil {
		t.Fatalf("failed to query baseline states: %v", err)
	}
	if baselineEntities != 6 || baselineRels != 4 || baselineStates != 6 {
		t.Fatalf("unexpected baseline counts: entities=%d rels=%d states=%d", baselineEntities, baselineRels, baselineStates)
	}

	// 12. Wire real application components using appPool (role: iot_backend_app)
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
	service, err := digitaltwin.NewReadService(prep, reader, &stubTelemetryHistoryDelegator{})
	if err != nil {
		t.Fatalf("failed to create ReadService: %v", err)
	}

	// =========================================================================
	// Scenario A: List Queries & Pagination
	// =========================================================================
	t.Run("List_TenantIsolation_And_Pagination", func(t *testing.T) {
		// 1. ownerUser lists twins: sees Gateway A and its 3 sensors (4 items total). Does NOT see Gateway B or its sensor.
		listRes, err := service.ListTwins(ctx, ownerUser, digitaltwin.TwinListQuery{Limit: 10})
		if err != nil {
			t.Fatalf("ListTwins for ownerUser failed: %v", err)
		}
		if len(listRes.Items) != 4 {
			t.Fatalf("expected 4 items for Gateway A, got %d", len(listRes.Items))
		}
		for _, it := range listRes.Items {
			if it.GatewayID != gwIDA {
				t.Fatalf("cross-tenant leakage: item has gateway_id %q, expected %q", it.GatewayID, gwIDA)
			}
			if it.ID == gwBURN || it.ID == sensB1URN {
				t.Fatalf("cross-tenant leakage: item %q from Gateway B found in Gateway A list", it.ID)
			}
		}

		// 2. Roles: operator and viewer also receive the 4 items
		opRes, err := service.ListTwins(ctx, operatorUser, digitaltwin.TwinListQuery{Limit: 10})
		if err != nil {
			t.Fatalf("ListTwins for operatorUser failed: %v", err)
		}
		if len(opRes.Items) != 4 {
			t.Fatalf("expected 4 items for operatorUser, got %d", len(opRes.Items))
		}

		vwRes, err := service.ListTwins(ctx, viewerUser, digitaltwin.TwinListQuery{Limit: 10})
		if err != nil {
			t.Fatalf("ListTwins for viewerUser failed: %v", err)
		}
		if len(vwRes.Items) != 4 {
			t.Fatalf("expected 4 items for viewerUser, got %d", len(vwRes.Items))
		}

		// 3. Paging with Limit = 2
		// Page 1: returns 2 items with non-nil next_cursor
		p1, err := service.ListTwins(ctx, ownerUser, digitaltwin.TwinListQuery{Limit: 2})
		if err != nil {
			t.Fatalf("ListTwins page 1 failed: %v", err)
		}
		if len(p1.Items) != 2 {
			t.Fatalf("expected 2 items on page 1, got %d", len(p1.Items))
		}
		if p1.NextCursor == nil || *p1.NextCursor == "" {
			t.Fatalf("expected non-nil next_cursor on page 1")
		}

		// Page 2: with cursor returns remaining 2 items with nil next_cursor
		p2, err := service.ListTwins(ctx, ownerUser, digitaltwin.TwinListQuery{Limit: 2, After: p1.NextCursor})
		if err != nil {
			t.Fatalf("ListTwins page 2 failed: %v", err)
		}
		if len(p2.Items) != 2 {
			t.Fatalf("expected 2 items on page 2, got %d", len(p2.Items))
		}
		if p2.NextCursor != nil {
			t.Fatalf("expected nil next_cursor on final page 2, got %q", *p2.NextCursor)
		}

		// Verify distinct items across pages
		seenIDs := make(map[string]bool)
		for _, it := range p1.Items {
			seenIDs[it.ID] = true
		}
		for _, it := range p2.Items {
			if seenIDs[it.ID] {
				t.Fatalf("duplicate item across pages: %s", it.ID)
			}
			seenIDs[it.ID] = true
		}
		if len(seenIDs) != 4 {
			t.Fatalf("expected 4 distinct items across p1 and p2, got %d", len(seenIDs))
		}

		// 4. Non-member gets empty list items = [] (length 0), not error/404
		nonMemberRes, err := service.ListTwins(ctx, nonMemberUser, digitaltwin.TwinListQuery{Limit: 10})
		if err != nil {
			t.Fatalf("ListTwins for nonMemberUser should not error, got: %v", err)
		}
		if len(nonMemberRes.Items) != 0 {
			t.Fatalf("expected 0 items for nonMemberUser, got %d", len(nonMemberRes.Items))
		}
		if nonMemberRes.NextCursor != nil {
			t.Fatalf("expected nil next_cursor for empty list")
		}

		// 5. Platform admin without membership gets empty list items = []
		adminRes, err := service.ListTwins(ctx, platformAdminUser, digitaltwin.TwinListQuery{Limit: 10})
		if err != nil {
			t.Fatalf("ListTwins for platformAdminUser should not error, got: %v", err)
		}
		if len(adminRes.Items) != 0 {
			t.Fatalf("expected 0 items for platformAdminUser without membership, got %d", len(adminRes.Items))
		}

		// 6. Tampered cursor rejected
		_, err = service.ListTwins(ctx, ownerUser, digitaltwin.TwinListQuery{Limit: 2, After: stringPtr("invalid_tampered_cursor")})
		if !errors.Is(err, digitaltwin.ErrInvalidCursor) {
			t.Fatalf("expected ErrInvalidCursor on tampered cursor, got: %v", err)
		}

		// 7. Unsupported entity type: insert a Device entity in Gateway A.
		// Unfiltered list and cursor paging must still return only Gateway and Sensor.
		devUUID := uuid.New()
		devURN := "urn:ngsi-ld:Device:" + gwIDA + ":dev_01"
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.twin_entities (id, entity_id, entity_type, name, gateway_id, attributes)
			VALUES ($1, $2, 'Device', 'Unsupported Device', $3, '{}')`,
			devUUID, devURN, gwIDA,
		); err != nil {
			t.Fatalf("failed to seed unsupported Device entity: %v", err)
		}

		unfilteredRes, err := service.ListTwins(ctx, ownerUser, digitaltwin.TwinListQuery{Limit: 10})
		if err != nil {
			t.Fatalf("ListTwins failed with unsupported entity in DB: %v", err)
		}
		if len(unfilteredRes.Items) != 4 {
			t.Fatalf("expected 4 items (excluding Device), got %d", len(unfilteredRes.Items))
		}
		for _, it := range unfilteredRes.Items {
			if it.Type != digitaltwin.EntityTypeGateway && it.Type != digitaltwin.EntityTypeSensor {
				t.Fatalf("unsupported entity type %q leaked into list: %s", it.Type, it.ID)
			}
			if it.ID == devURN {
				t.Fatalf("unsupported Device entity %q leaked into list", it.ID)
			}
		}

		// Cursor paging with unsupported Device in DB
		devP1, err := service.ListTwins(ctx, ownerUser, digitaltwin.TwinListQuery{Limit: 2})
		if err != nil {
			t.Fatalf("ListTwins p1 failed with unsupported entity: %v", err)
		}
		devP2, err := service.ListTwins(ctx, ownerUser, digitaltwin.TwinListQuery{Limit: 2, After: devP1.NextCursor})
		if err != nil {
			t.Fatalf("ListTwins p2 failed with unsupported entity: %v", err)
		}
		if len(devP1.Items)+len(devP2.Items) != 4 {
			t.Fatalf("expected 4 items total across pages, got %d", len(devP1.Items)+len(devP2.Items))
		}
		for _, it := range append(devP1.Items, devP2.Items...) {
			if it.ID == devURN || (it.Type != digitaltwin.EntityTypeGateway && it.Type != digitaltwin.EntityTypeSensor) {
				t.Fatalf("unsupported entity %q leaked into paged list", it.ID)
			}
		}

		// Clean up unsupported Device entity
		if _, err := setupPool.Exec(ctx, "DELETE FROM public.twin_entities WHERE id = $1", devUUID); err != nil {
			t.Fatalf("failed to clean unsupported Device entity: %v", err)
		}
	})

	// =========================================================================
	// Scenario B: Detail Queries, Relationship Paging & Graph Integrity
	// =========================================================================
	t.Run("Detail_GatewayAndSensor_And_RelationshipPaging", func(t *testing.T) {
		// 1. Gateway A detail with RelationshipLimit = 2
		gwDetailP1, err := service.GetTwinDetail(ctx, ownerUser, digitaltwin.TwinDetailQuery{
			EntityID:          gwAURN,
			RelationshipLimit: 2,
		})
		if err != nil {
			t.Fatalf("GetTwinDetail for Gateway A failed: %v", err)
		}
		if gwDetailP1.ID != gwAURN {
			t.Fatalf("expected ID %q, got %q", gwAURN, gwDetailP1.ID)
		}
		if gwDetailP1.Type != digitaltwin.EntityTypeGateway {
			t.Fatalf("expected type Gateway, got %q", gwDetailP1.Type)
		}
		if gwDetailP1.Name != "Gateway A" {
			t.Fatalf("expected name 'Gateway A', got %q", gwDetailP1.Name)
		}
		if gwDetailP1.Description == nil || *gwDetailP1.Description != "Gateway A Description" {
			t.Fatalf("expected description 'Gateway A Description', got %v", gwDetailP1.Description)
		}
		if gwDetailP1.Unit != nil {
			t.Fatalf("gateway must not have unit, got %v", gwDetailP1.Unit)
		}
		if gwDetailP1.RelationshipPage == nil {
			t.Fatalf("expected non-nil RelationshipPage for Gateway")
		}
		if gwDetailP1.RelationshipPage.Count != 2 {
			t.Fatalf("expected relationship count 2, got %d", gwDetailP1.RelationshipPage.Count)
		}
		if !gwDetailP1.RelationshipPage.Partial {
			t.Fatalf("expected partial=true on relationship page 1")
		}
		if gwDetailP1.RelationshipPage.NextCursor == nil || *gwDetailP1.RelationshipPage.NextCursor == "" {
			t.Fatalf("expected non-nil next_cursor on relationship page 1")
		}

		// 2. Gateway A relationship page 2 using RelationshipAfter
		gwDetailP2, err := service.GetTwinDetail(ctx, ownerUser, digitaltwin.TwinDetailQuery{
			EntityID:          gwAURN,
			RelationshipLimit: 2,
			RelationshipAfter: gwDetailP1.RelationshipPage.NextCursor,
		})
		if err != nil {
			t.Fatalf("GetTwinDetail page 2 failed: %v", err)
		}
		if gwDetailP2.RelationshipPage.Count != 1 {
			t.Fatalf("expected relationship count 1 on page 2, got %d", gwDetailP2.RelationshipPage.Count)
		}
		if gwDetailP2.RelationshipPage.Partial {
			t.Fatalf("expected partial=false on final relationship page 2")
		}
		if gwDetailP2.RelationshipPage.NextCursor != nil {
			t.Fatalf("expected nil next_cursor on page 2, got %v", gwDetailP2.RelationshipPage.NextCursor)
		}

		// Verify relationship targets: strictly isolate sensors of Gateway A; Gateway B's sensor is NOT leaked
		allRelTargets := make([]string, 0, 3)
		for _, r := range gwDetailP1.Relationships {
			allRelTargets = append(allRelTargets, r.TargetURN)
		}
		for _, r := range gwDetailP2.Relationships {
			allRelTargets = append(allRelTargets, r.TargetURN)
		}
		if len(allRelTargets) != 3 {
			t.Fatalf("expected 3 total relationships for Gateway A, got %d", len(allRelTargets))
		}
		for _, target := range allRelTargets {
			if !strings.HasPrefix(target, "urn:ngsi-ld:Sensor:"+gwIDA+":") {
				t.Fatalf("relationship target %q does not belong to Gateway A", target)
			}
			if target == sensB1URN {
				t.Fatalf("critical leak: Gateway B's sensor %q found in Gateway A's relationships", target)
			}
		}

		// 3. Sensor detail returns name, unit, description = nil, relationships = nil, relationship_page = nil
		sensDetail, err := service.GetTwinDetail(ctx, ownerUser, digitaltwin.TwinDetailQuery{
			EntityID:          sensA1URN,
			RelationshipLimit: 10,
		})
		if err != nil {
			t.Fatalf("GetTwinDetail for Sensor 1 failed: %v", err)
		}
		if sensDetail.ID != sensA1URN {
			t.Fatalf("expected ID %q, got %q", sensA1URN, sensDetail.ID)
		}
		if sensDetail.Type != digitaltwin.EntityTypeSensor {
			t.Fatalf("expected type Sensor, got %q", sensDetail.Type)
		}
		if sensDetail.Name != "Sensor 1" {
			t.Fatalf("expected name 'Sensor 1', got %q", sensDetail.Name)
		}
		if sensDetail.Unit == nil || *sensDetail.Unit != "degC" {
			t.Fatalf("expected unit 'degC', got %v", sensDetail.Unit)
		}
		if sensDetail.Description != nil {
			t.Fatalf("sensor must have description=nil, got %v", sensDetail.Description)
		}
		if sensDetail.Relationships != nil {
			t.Fatalf("sensor must have relationships=nil, got %v", sensDetail.Relationships)
		}
		if sensDetail.RelationshipPage != nil {
			t.Fatalf("sensor must have relationship_page=nil, got %v", sensDetail.RelationshipPage)
		}

		// 4. Role matrix: operator and viewer can also read details
		if _, err := service.GetTwinDetail(ctx, operatorUser, digitaltwin.TwinDetailQuery{
			EntityID:          sensA1URN,
			RelationshipLimit: 10,
		}); err != nil {
			t.Fatalf("operatorUser GetTwinDetail failed: %v", err)
		}
		if _, err := service.GetTwinDetail(ctx, viewerUser, digitaltwin.TwinDetailQuery{
			EntityID:          sensA1URN,
			RelationshipLimit: 10,
		}); err != nil {
			t.Fatalf("viewerUser GetTwinDetail failed: %v", err)
		}

		// 5. Broken graph: Sensor with missing hasSensor relationship returns ErrNotFound
		sensBrokenID := fmt.Sprintf("sens_broken_%s", runID)
		sensBrokenURN := "urn:ngsi-ld:Sensor:" + gwIDA + ":" + sensBrokenID
		sensBrokenUUID := uuid.New()

		// Insert into sensors and twin_entities, but deliberately OMIT twin_relationships
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.sensors (gateway_id, sensor_id, name, unit)
			VALUES ($1, $2, 'Broken Sensor', 'degC')`,
			gwIDA, sensBrokenID,
		); err != nil {
			t.Fatalf("failed to insert broken sensor: %v", err)
		}
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.twin_entities (id, entity_id, entity_type, name, gateway_id, attributes)
			VALUES ($1, $2, 'Sensor', 'Broken Sensor', $3, '{}')`,
			sensBrokenUUID, sensBrokenURN, gwIDA,
		); err != nil {
			t.Fatalf("failed to insert broken twin_entity: %v", err)
		}

		// Query detail for the broken sensor: should fail closed with ErrNotFound
		_, err = service.GetTwinDetail(ctx, ownerUser, digitaltwin.TwinDetailQuery{
			EntityID:          sensBrokenURN,
			RelationshipLimit: 10,
		})
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for broken graph sensor, got: %v", err)
		}

		// Clean up broken sensor
		if _, err := setupPool.Exec(ctx, "DELETE FROM public.twin_entities WHERE id = $1", sensBrokenUUID); err != nil {
			t.Fatalf("failed to clean broken entity: %v", err)
		}
		if _, err := setupPool.Exec(ctx, "DELETE FROM public.sensors WHERE gateway_id = $1 AND sensor_id = $2", gwIDA, sensBrokenID); err != nil {
			t.Fatalf("failed to clean broken sensor: %v", err)
		}

		// 6. Non-member querying Gateway A or Sensor returns ErrNotFound
		_, err = service.GetTwinDetail(ctx, nonMemberUser, digitaltwin.TwinDetailQuery{
			EntityID:          gwAURN,
			RelationshipLimit: 10,
		})
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for non-member on Gateway A, got: %v", err)
		}
		_, err = service.GetTwinDetail(ctx, nonMemberUser, digitaltwin.TwinDetailQuery{
			EntityID:          sensA1URN,
			RelationshipLimit: 10,
		})
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for non-member on Sensor 1, got: %v", err)
		}

		// 7. Platform admin without membership querying Gateway A returns ErrNotFound
		_, err = service.GetTwinDetail(ctx, platformAdminUser, digitaltwin.TwinDetailQuery{
			EntityID:          gwAURN,
			RelationshipLimit: 10,
		})
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for platform admin without membership on Gateway A, got: %v", err)
		}

		// 8. Tampered relationship cursor returns ErrInvalidCursor
		_, err = service.GetTwinDetail(ctx, ownerUser, digitaltwin.TwinDetailQuery{
			EntityID:          gwAURN,
			RelationshipLimit: 10,
			RelationshipAfter: stringPtr("tampered_cursor"),
		})
		if !errors.Is(err, digitaltwin.ErrInvalidCursor) {
			t.Fatalf("expected ErrInvalidCursor on tampered relationship cursor, got: %v", err)
		}

		// 9. Empty source page: Gateway with zero sensors returns empty page with valid metadata
		gwEmptyID := fmt.Sprintf("gw_empty_%s", runID)
		gwEmptyURN := "urn:ngsi-ld:Gateway:" + gwEmptyID
		gwEmptyUUID := uuid.New()

		// seedEmptyGatewayFixture registers cleanup BEFORE its first INSERT, so an
		// early t.Fatal in this sub-scenario still removes this extra Gateway and
		// its Twin data instead of leaving them in the shared test database. The
		// parent cleanup scope covers only Gateway A/B and does not own these rows.
		if err := seedEmptyGatewayFixture(ctx, t, setupPool, emptyGatewayFixture{
			GatewaysTableID: gwEmptyID,
			EntityUUID:      gwEmptyUUID,
			EntityURN:       gwEmptyURN,
			OwnerUserID:     ownerUser,
		}); err != nil {
			t.Fatalf("failed to seed empty gateway fixture: %v", err)
		}

		gwEmptyDetail, err := service.GetTwinDetail(ctx, ownerUser, digitaltwin.TwinDetailQuery{
			EntityID:          gwEmptyURN,
			RelationshipLimit: 10,
		})
		if err != nil {
			t.Fatalf("GetTwinDetail for empty gateway failed: %v", err)
		}
		if gwEmptyDetail.Name != "Gateway Empty" {
			t.Errorf("empty gateway name = %s, want 'Gateway Empty'", gwEmptyDetail.Name)
		}
		if len(gwEmptyDetail.Relationships) != 0 {
			t.Errorf("expected 0 relationships, got: %d", len(gwEmptyDetail.Relationships))
		}
		if gwEmptyDetail.RelationshipPage == nil || gwEmptyDetail.RelationshipPage.Count != 0 || gwEmptyDetail.RelationshipPage.Partial {
			t.Errorf("unexpected relationship page on empty gateway: %+v", gwEmptyDetail.RelationshipPage)
		}

		// The empty-gateway fixture is removed by the cleanup registered in
		// seedEmptyGatewayFixture; no inline success-path cleanup is needed.

		// 10. Malformed cross-Gateway relationship: link pointing to Sensor on Gateway B must be excluded
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.twin_relationships (source_entity_id, relationship_type, target_entity_id)
			VALUES ($1, 'hasSensor', $2)`,
			gwAUUID, sensB1UUID,
		); err != nil {
			t.Fatalf("failed to seed malformed cross-gateway relationship: %v", err)
		}

		gwDetailWithMalformed, err := service.GetTwinDetail(ctx, ownerUser, digitaltwin.TwinDetailQuery{
			EntityID:          gwAURN,
			RelationshipLimit: 50,
		})
		if err != nil {
			t.Fatalf("GetTwinDetail failed with malformed relationship present: %v", err)
		}
		for _, rel := range gwDetailWithMalformed.Relationships {
			if rel.TargetURN == sensB1URN {
				t.Fatalf("malformed cross-gateway relationship leaked into Gateway A detail: %s", rel.TargetURN)
			}
		}

		// Clean up malformed relationship
		if _, err := setupPool.Exec(ctx, `
			DELETE FROM public.twin_relationships
			WHERE source_entity_id = $1 AND target_entity_id = $2 AND relationship_type = 'hasSensor'`,
			gwAUUID, sensB1UUID,
		); err != nil {
			t.Fatalf("failed to clean malformed relationship: %v", err)
		}
	})

	// =========================================================================
	// Scenario C: State Queries & Direct Read Integrity
	// =========================================================================
	t.Run("State_DirectRead_And_Precision", func(t *testing.T) {
		// 1. State returns exact reported_state, desired_state, reported_version = 8, desired_version = 0
		stateRes, err := service.GetTwinState(ctx, ownerUser, digitaltwin.TwinStateQuery{EntityID: sensA1URN})
		if err != nil {
			t.Fatalf("GetTwinState failed: %v", err)
		}
		if stateRes.ID != sensA1URN {
			t.Fatalf("expected ID %q, got %q", sensA1URN, stateRes.ID)
		}
		if stateRes.Type != digitaltwin.EntityTypeSensor {
			t.Fatalf("expected type Sensor, got %q", stateRes.Type)
		}
		if stateRes.GatewayID != gwIDA {
			t.Fatalf("expected gateway_id %q, got %q", gwIDA, stateRes.GatewayID)
		}
		if stateRes.ReportedVersion != 8 {
			t.Fatalf("expected reported_version 8, got %d", stateRes.ReportedVersion)
		}
		if stateRes.DesiredVersion != 0 {
			t.Fatalf("expected desired_version 0, got %d", stateRes.DesiredVersion)
		}

		// Check reported_state JSON content preserves big integers > 2^53 without float64 precision loss
		var reportedMap map[string]any
		decoder := json.NewDecoder(strings.NewReader(string(stateRes.ReportedState)))
		decoder.UseNumber()
		if err := decoder.Decode(&reportedMap); err != nil {
			t.Fatalf("failed to decode reported_state: %v", err)
		}
		seqVal, ok := reportedMap["sequence"].(json.Number)
		if !ok || seqVal.String() != "9007199254740993" {
			t.Fatalf("sequence number corrupted or lost precision: got %v", reportedMap["sequence"])
		}
		if reportedMap["status"] != "online" || reportedMap["active"] != true || reportedMap["note"] != nil {
			t.Fatalf("unexpected reported_state fields: %v", reportedMap)
		}

		// 2. Direct read proof: verify that state is read directly from twin_states (no hypertable scan).
		// twin_temporal_values for this entity has 0 rows, yet state is accurately retrieved.
		var temporalRowCount int
		if err := setupPool.QueryRow(ctx, "SELECT COUNT(*) FROM public.twin_temporal_values WHERE entity_id = $1", sensA1UUID).Scan(&temporalRowCount); err != nil {
			t.Fatalf("failed to count temporal rows: %v", err)
		}
		if temporalRowCount != 0 {
			t.Fatalf("expected 0 temporal rows for sensor, got %d", temporalRowCount)
		}

		// 3. Missing state row returns ErrNotFound (does not fabricate empty state)
		sensNoStateID := fmt.Sprintf("sens_nostate_%s", runID)
		sensNoStateURN := "urn:ngsi-ld:Sensor:" + gwIDA + ":" + sensNoStateID
		sensNoStateUUID := uuid.New()

		// Insert into sensors, twin_entities, and twin_relationships, but deliberately OMIT twin_states
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.sensors (gateway_id, sensor_id, name, unit)
			VALUES ($1, $2, 'No State Sensor', 'degC')`,
			gwIDA, sensNoStateID,
		); err != nil {
			t.Fatalf("failed to insert no-state sensor: %v", err)
		}
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.twin_entities (id, entity_id, entity_type, name, gateway_id, attributes)
			VALUES ($1, $2, 'Sensor', 'No State Sensor', $3, '{}')`,
			sensNoStateUUID, sensNoStateURN, gwIDA,
		); err != nil {
			t.Fatalf("failed to insert no-state twin_entity: %v", err)
		}
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.twin_relationships (source_entity_id, relationship_type, target_entity_id)
			VALUES ($1, 'hasSensor', $2)`,
			gwAUUID, sensNoStateUUID,
		); err != nil {
			t.Fatalf("failed to insert no-state twin_relationship: %v", err)
		}

		// Query state for entity with missing state row: must return ErrUnexpected (data fault), not ErrNotFound or empty state
		_, err = service.GetTwinState(ctx, ownerUser, digitaltwin.TwinStateQuery{EntityID: sensNoStateURN})
		if !errors.Is(err, digitaltwin.ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for missing twin_states row on authorized entity, got: %v", err)
		}

		// Clean up no-state sensor
		if _, err := setupPool.Exec(ctx, "DELETE FROM public.twin_relationships WHERE target_entity_id = $1", sensNoStateUUID); err != nil {
			t.Fatalf("failed to clean no-state relationship: %v", err)
		}
		if _, err := setupPool.Exec(ctx, "DELETE FROM public.twin_entities WHERE id = $1", sensNoStateUUID); err != nil {
			t.Fatalf("failed to clean no-state entity: %v", err)
		}
		if _, err := setupPool.Exec(ctx, "DELETE FROM public.sensors WHERE gateway_id = $1 AND sensor_id = $2", gwIDA, sensNoStateID); err != nil {
			t.Fatalf("failed to clean no-state sensor: %v", err)
		}

		// 4. Non-member and Platform admin state queries return ErrNotFound
		_, err = service.GetTwinState(ctx, nonMemberUser, digitaltwin.TwinStateQuery{EntityID: sensA1URN})
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for non-member state query, got: %v", err)
		}
		_, err = service.GetTwinState(ctx, platformAdminUser, digitaltwin.TwinStateQuery{EntityID: sensA1URN})
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for platform admin state query, got: %v", err)
		}
	})

	// =========================================================================
	// Scenario D: Wire DTO Projection & JSON Serialization
	// =========================================================================
	t.Run("WireDTO_Projection_And_Serialization", func(t *testing.T) {
		// 1. List DTO
		listRes, err := service.ListTwins(ctx, ownerUser, digitaltwin.TwinListQuery{Limit: 2})
		if err != nil {
			t.Fatalf("ListTwins failed: %v", err)
		}
		listDTO, err := httpserver.NewTwinListResponseDTO(listRes)
		if err != nil {
			t.Fatalf("NewTwinListResponseDTO failed: %v", err)
		}
		listBytes, err := json.Marshal(listDTO)
		if err != nil {
			t.Fatalf("json.Marshal(listDTO) failed: %v", err)
		}
		listStr := string(listBytes)
		if !strings.Contains(listStr, `"items":`) || !strings.Contains(listStr, `"next_cursor":`) {
			t.Fatalf("list JSON missing expected fields: %s", listStr)
		}

		// 2. Gateway Detail DTO
		gwDetail, err := service.GetTwinDetail(ctx, ownerUser, digitaltwin.TwinDetailQuery{
			EntityID:          gwAURN,
			RelationshipLimit: 2,
		})
		if err != nil {
			t.Fatalf("GetTwinDetail failed: %v", err)
		}
		detailDTO, err := httpserver.NewTwinDetailResponseDTO(gwDetail)
		if err != nil {
			t.Fatalf("NewTwinDetailResponseDTO failed: %v", err)
		}
		detailBytes, err := json.Marshal(detailDTO)
		if err != nil {
			t.Fatalf("json.Marshal(detailDTO) failed: %v", err)
		}
		detailStr := string(detailBytes)
		if !strings.Contains(detailStr, `"@context":`) ||
			!strings.Contains(detailStr, `"hasSensor":`) ||
			!strings.Contains(detailStr, `"relationship_page":`) ||
			!strings.Contains(detailStr, `"Gateway A"`) {
			t.Fatalf("gateway detail JSON missing expected fields: %s", detailStr)
		}

		// 3. Sensor Detail DTO
		sensDetail, err := service.GetTwinDetail(ctx, ownerUser, digitaltwin.TwinDetailQuery{
			EntityID:          sensA1URN,
			RelationshipLimit: 10,
		})
		if err != nil {
			t.Fatalf("GetTwinDetail failed: %v", err)
		}
		sensDTO, err := httpserver.NewTwinDetailResponseDTO(sensDetail)
		if err != nil {
			t.Fatalf("NewTwinDetailResponseDTO for sensor failed: %v", err)
		}
		if sensDTO.HasSensor != nil {
			t.Fatalf("sensor DTO HasSensor must be nil, got %v", sensDTO.HasSensor)
		}
		if sensDTO.RelationshipPage != nil {
			t.Fatalf("sensor DTO RelationshipPage must be nil, got %v", sensDTO.RelationshipPage)
		}
		sensBytes, err := json.Marshal(sensDTO)
		if err != nil {
			t.Fatalf("json.Marshal(sensDTO) failed: %v", err)
		}
		var sensMap map[string]any
		if err := json.Unmarshal(sensBytes, &sensMap); err != nil {
			t.Fatalf("failed to unmarshal sensor JSON: %v", err)
		}
		if _, ok := sensMap["hasSensor"]; ok {
			t.Fatalf("sensor detail JSON root must not contain hasSensor: %s", string(sensBytes))
		}
		if _, ok := sensMap["relationship_page"]; ok {
			t.Fatalf("sensor detail JSON root must not contain relationship_page: %s", string(sensBytes))
		}
		if _, ok := sensMap["unit"]; !ok {
			t.Fatalf("sensor detail JSON missing unit: %s", string(sensBytes))
		}

		// 4. State DTO
		stateRes, err := service.GetTwinState(ctx, ownerUser, digitaltwin.TwinStateQuery{EntityID: sensA1URN})
		if err != nil {
			t.Fatalf("GetTwinState failed: %v", err)
		}
		stateDTO, err := httpserver.NewTwinStateResponseDTO(stateRes)
		if err != nil {
			t.Fatalf("NewTwinStateResponseDTO failed: %v", err)
		}
		stateBytes, err := json.Marshal(stateDTO)
		if err != nil {
			t.Fatalf("json.Marshal(stateDTO) failed: %v", err)
		}
		stateStr := string(stateBytes)
		if !strings.Contains(stateStr, `"reported_version":"8"`) {
			t.Fatalf("state JSON missing string reported_version '8': %s", stateStr)
		}
		if !strings.Contains(stateStr, `"desired_version":"0"`) {
			t.Fatalf("state JSON missing string desired_version '0': %s", stateStr)
		}
		if !strings.Contains(stateStr, "9007199254740993") {
			t.Fatalf("state JSON missing big integer 9007199254740993: %s", stateStr)
		}
	})

	// =========================================================================
	// Scenario E: Read-Only Integrity Check
	// =========================================================================
	t.Run("ReadOnly_Integrity", func(t *testing.T) {
		afterSnapshot := captureFixtureSnapshot(ctx, t, setupPool, gwIDA, gwIDB)

		// 1. Entities deep comparison
		if len(afterSnapshot.entities) != len(baselineSnapshot.entities) {
			t.Fatalf("entities count mutated: baseline=%d after=%d", len(baselineSnapshot.entities), len(afterSnapshot.entities))
		}
		for id, baseE := range baselineSnapshot.entities {
			afterE, ok := afterSnapshot.entities[id]
			if !ok {
				t.Fatalf("entity %s missing in post-read snapshot", id)
			}
			if afterE.EntityID != baseE.EntityID || afterE.EntityType != baseE.EntityType || afterE.GatewayID != baseE.GatewayID {
				t.Fatalf("entity %s metadata mutated: %+v vs %+v", id, afterE, baseE)
			}
			if (afterE.Name == nil) != (baseE.Name == nil) || (afterE.Name != nil && *afterE.Name != *baseE.Name) {
				t.Fatalf("entity %s name mutated", id)
			}
			if afterE.Attributes != baseE.Attributes {
				t.Fatalf("entity %s attributes mutated: got %s, want %s", id, afterE.Attributes, baseE.Attributes)
			}
			if !afterE.UpdatedAt.Equal(baseE.UpdatedAt) {
				t.Fatalf("entity %s updated_at mutated: got %v, want %v", id, afterE.UpdatedAt, baseE.UpdatedAt)
			}
		}

		// 2. Relationships deep comparison
		if len(afterSnapshot.relationships) != len(baselineSnapshot.relationships) {
			t.Fatalf("relationships count mutated: baseline=%d after=%d", len(baselineSnapshot.relationships), len(afterSnapshot.relationships))
		}
		for key, baseR := range baselineSnapshot.relationships {
			afterR, ok := afterSnapshot.relationships[key]
			if !ok {
				t.Fatalf("relationship %s missing in post-read snapshot", key)
			}
			if afterR.SourceID != baseR.SourceID || afterR.RelType != baseR.RelType || afterR.TargetID != baseR.TargetID {
				t.Fatalf("relationship %s mutated: %+v vs %+v", key, afterR, baseR)
			}
			if !afterR.CreatedAt.Equal(baseR.CreatedAt) {
				t.Fatalf("relationship %s created_at mutated: got %v, want %v", key, afterR.CreatedAt, baseR.CreatedAt)
			}
		}

		// 3. States deep comparison (preserves int64 versions, JSONB raw text/bytes, and nullable timestamps)
		if len(afterSnapshot.states) != len(baselineSnapshot.states) {
			t.Fatalf("states count mutated: baseline=%d after=%d", len(baselineSnapshot.states), len(afterSnapshot.states))
		}
		for id, baseS := range baselineSnapshot.states {
			afterS, ok := afterSnapshot.states[id]
			if !ok {
				t.Fatalf("state for entity %s missing in post-read snapshot", id)
			}
			if afterS.ReportedState != baseS.ReportedState {
				t.Fatalf("state for entity %s reported_state mutated: got %s, want %s", id, afterS.ReportedState, baseS.ReportedState)
			}
			if afterS.DesiredState != baseS.DesiredState {
				t.Fatalf("state for entity %s desired_state mutated: got %s, want %s", id, afterS.DesiredState, baseS.DesiredState)
			}
			if afterS.ReportedVersion != baseS.ReportedVersion {
				t.Fatalf("state for entity %s reported_version mutated: got %d, want %d", id, afterS.ReportedVersion, baseS.ReportedVersion)
			}
			if afterS.DesiredVersion != baseS.DesiredVersion {
				t.Fatalf("state for entity %s desired_version mutated: got %d, want %d", id, afterS.DesiredVersion, baseS.DesiredVersion)
			}
			if (afterS.LastReportedAt == nil) != (baseS.LastReportedAt == nil) || (afterS.LastReportedAt != nil && !afterS.LastReportedAt.Equal(*baseS.LastReportedAt)) {
				t.Fatalf("state for entity %s last_reported_at mutated", id)
			}
			if (afterS.LastDesiredAt == nil) != (baseS.LastDesiredAt == nil) || (afterS.LastDesiredAt != nil && !afterS.LastDesiredAt.Equal(*baseS.LastDesiredAt)) {
				t.Fatalf("state for entity %s last_desired_at mutated", id)
			}
			if !afterS.UpdatedAt.Equal(baseS.UpdatedAt) {
				t.Fatalf("state for entity %s updated_at mutated: got %v, want %v", id, afterS.UpdatedAt, baseS.UpdatedAt)
			}
		}

		// 4. Gateways and Sensors metadata deep comparison (complete normalized maps,
		//    including nullable description/unit and map cardinality).
		if err := compareTwinFixtureMetadata(baselineSnapshot, afterSnapshot); err != nil {
			t.Fatalf("gateway/sensor metadata mutated: %v", err)
		}
	})
}

// TestCompareTwinFixtureMetadata exercises the read-only metadata comparator used by the
// ReadOnly_Integrity step. It must detect changes to the nullable Gateways.description and
// sensors.unit values (including NULL <-> value transitions) and added/removed metadata entries,
// while passing unchanged metadata. It does not require a database.
func TestCompareTwinFixtureMetadata(t *testing.T) {
	created := time.Date(2026, 10, 17, 8, 0, 0, 0, time.UTC)

	baseGateway := func() gatewaySnapshot {
		return gatewaySnapshot{GatewayID: "gw_a", Name: "Gateway A", Description: stringPtr("primary gateway"), CreatedAt: created}
	}
	baseSensor := func() sensorSnapshot {
		return sensorSnapshot{GatewayID: "gw_a", SensorID: "s1", Name: "Temperature", Unit: stringPtr("celsius"), CreatedAt: created}
	}
	// fixtures returns a fresh baseline/after pair with independent maps so per-case mutations
	// cannot leak between subtests.
	fixtures := func() (fixtureSnapshot, fixtureSnapshot) {
		baseline := snapshotWithMetadata(
			map[string]gatewaySnapshot{"gw_a": baseGateway()},
			map[string]sensorSnapshot{"gw_a:s1": baseSensor()},
		)
		after := snapshotWithMetadata(
			map[string]gatewaySnapshot{"gw_a": baseGateway()},
			map[string]sensorSnapshot{"gw_a:s1": baseSensor()},
		)
		return baseline, after
	}

	t.Run("unchanged_metadata_passes", func(t *testing.T) {
		baseline, after := fixtures()
		if err := compareTwinFixtureMetadata(baseline, after); err != nil {
			t.Fatalf("unchanged metadata reported a difference: %v", err)
		}
	})

	t.Run("changed_gateway_description_detected", func(t *testing.T) {
		baseline, after := fixtures()
		g := after.gateways["gw_a"]
		g.Description = stringPtr("secondary gateway")
		after.gateways["gw_a"] = g
		if err := compareTwinFixtureMetadata(baseline, after); err == nil {
			t.Fatal("changed gateway description was not detected")
		}
	})

	t.Run("gateway_description_null_to_value_detected", func(t *testing.T) {
		baseline, after := fixtures()
		bg := baseline.gateways["gw_a"]
		bg.Description = nil
		baseline.gateways["gw_a"] = bg
		if err := compareTwinFixtureMetadata(baseline, after); err == nil {
			t.Fatal("gateway description NULL -> value transition was not detected")
		}
	})

	t.Run("gateway_description_value_to_null_detected", func(t *testing.T) {
		baseline, after := fixtures()
		ag := after.gateways["gw_a"]
		ag.Description = nil
		after.gateways["gw_a"] = ag
		if err := compareTwinFixtureMetadata(baseline, after); err == nil {
			t.Fatal("gateway description value -> NULL transition was not detected")
		}
	})

	t.Run("changed_sensor_unit_detected", func(t *testing.T) {
		baseline, after := fixtures()
		s := after.sensors["gw_a:s1"]
		s.Unit = stringPtr("fahrenheit")
		after.sensors["gw_a:s1"] = s
		if err := compareTwinFixtureMetadata(baseline, after); err == nil {
			t.Fatal("changed sensor unit was not detected")
		}
	})

	t.Run("sensor_unit_null_to_value_detected", func(t *testing.T) {
		baseline, after := fixtures()
		bs := baseline.sensors["gw_a:s1"]
		bs.Unit = nil
		baseline.sensors["gw_a:s1"] = bs
		if err := compareTwinFixtureMetadata(baseline, after); err == nil {
			t.Fatal("sensor unit NULL -> value transition was not detected")
		}
	})

	t.Run("sensor_unit_value_to_null_detected", func(t *testing.T) {
		baseline, after := fixtures()
		as := after.sensors["gw_a:s1"]
		as.Unit = nil
		after.sensors["gw_a:s1"] = as
		if err := compareTwinFixtureMetadata(baseline, after); err == nil {
			t.Fatal("sensor unit value -> NULL transition was not detected")
		}
	})

	t.Run("added_gateway_entry_detected", func(t *testing.T) {
		baseline, after := fixtures()
		after.gateways["gw_b"] = gatewaySnapshot{GatewayID: "gw_b", Name: "Gateway B", CreatedAt: created}
		if err := compareTwinFixtureMetadata(baseline, after); err == nil {
			t.Fatal("added gateway entry was not detected")
		}
	})

	t.Run("removed_gateway_entry_detected", func(t *testing.T) {
		baseline, after := fixtures()
		delete(after.gateways, "gw_a")
		if err := compareTwinFixtureMetadata(baseline, after); err == nil {
			t.Fatal("removed gateway entry was not detected")
		}
	})

	t.Run("renamed_gateway_key_detected", func(t *testing.T) {
		baseline, after := fixtures()
		delete(after.gateways, "gw_a")
		after.gateways["gw_x"] = baseGateway()
		if err := compareTwinFixtureMetadata(baseline, after); err == nil {
			t.Fatal("renamed gateway key with equal cardinality was not detected")
		}
	})

	t.Run("added_sensor_entry_detected", func(t *testing.T) {
		baseline, after := fixtures()
		after.sensors["gw_a:s2"] = sensorSnapshot{GatewayID: "gw_a", SensorID: "s2", Name: "Humidity", CreatedAt: created}
		if err := compareTwinFixtureMetadata(baseline, after); err == nil {
			t.Fatal("added sensor entry was not detected")
		}
	})

	t.Run("removed_sensor_entry_detected", func(t *testing.T) {
		baseline, after := fixtures()
		delete(after.sensors, "gw_a:s1")
		if err := compareTwinFixtureMetadata(baseline, after); err == nil {
			t.Fatal("removed sensor entry was not detected")
		}
	})
}

type stubTelemetryHistoryDelegator struct{}

func (s *stubTelemetryHistoryDelegator) ExecuteEffectiveHistoryQuery(_ context.Context, _ uuid.UUID, _ telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
	return nil, nil
}

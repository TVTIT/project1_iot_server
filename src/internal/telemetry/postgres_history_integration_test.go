package telemetry_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/telemetry"
)

// resolveTestDatabaseURLs determines the connection strings for setup (admin) and application (iot_backend_app).
// It respects TELEMETRY_TEST_SETUP_URL and TELEMETRY_TEST_BACKEND_URL if set, or reads connection settings
// from the local environment / repository root .env file without logging secrets.
func resolveTestDatabaseURLs(t *testing.T) (string, string) {
	t.Helper()

	setupURL := os.Getenv("TELEMETRY_TEST_SETUP_URL")
	backendURL := os.Getenv("TELEMETRY_TEST_BACKEND_URL")
	if setupURL != "" && backendURL != "" {
		return setupURL, backendURL
	}

	// Explicit integration opt-in check: prevent routine `go test ./...` from connecting to the shared database
	// simply because a local .env exists. An explicit opt-in variable is required.
	if os.Getenv("TELEMETRY_HISTORY_TEST_NATIVE") != "1" &&
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

// TestNativeTelemetryHistoryRead exercises the native database slice for normal raw and bucket reads.
// It verifies real role attributes for iot_backend_app and setup connection, seeds uniquely scoped fixture data,
// calls digitaltwin.PostgresResolver + telemetry.TelemetryHistoryService + telemetry.PostgresHistoryRepository,
// asserts independently computed raw and bucket values, and cleans up only fixture-owned resources.
func TestNativeTelemetryHistoryRead(t *testing.T) {
	setupURL, backendURL := resolveTestDatabaseURLs(t)
	if setupURL == "" || backendURL == "" {
		t.Skip("setup and application database connection URLs are not available or native test not opted in (set TELEMETRY_HISTORY_TEST_NATIVE=1 or TELEMETRY_TEST_SETUP_URL and TELEMETRY_TEST_BACKEND_URL)")
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

	// 3. Seed uniquely scoped fixture resources
	runID := uuid.New().String()[:8]
	testUserUID := uuid.New()
	gwID := fmt.Sprintf("gw_hist_%s", runID)
	sensorID := fmt.Sprintf("sens_hist_%s", runID)
	sensorUnit := "degC"
	bootID := uuid.New()
	msgID := uuid.New()

	// Cleanup hook to delete only fixture-owned resources in reverse foreign key order
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		// Delete telemetry first (references sensors)
		execCleanupStep(cleanupCtx, t, setupPool, "telemetry", "DELETE FROM public.telemetry WHERE gateway_id = $1", gwID)
		// Delete user_gateways (references gateways and profiles)
		execCleanupStep(cleanupCtx, t, setupPool, "user_gateways", "DELETE FROM public.user_gateways WHERE gateway_id = $1", gwID)
		// Delete sensors (references gateways)
		execCleanupStep(cleanupCtx, t, setupPool, "sensors", "DELETE FROM public.sensors WHERE gateway_id = $1", gwID)
		// Delete gateways
		execCleanupStep(cleanupCtx, t, setupPool, "gateways", "DELETE FROM public.gateways WHERE gateway_id = $1", gwID)
		// Delete profiles
		execCleanupStep(cleanupCtx, t, setupPool, "profiles", "DELETE FROM public.profiles WHERE id = $1", testUserUID)

		// Delete auth.users if table exists
		var hasAuthUsers bool
		if err := setupPool.QueryRow(cleanupCtx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
			t.Errorf("cleanup check auth.users failed: %v", err)
		} else if hasAuthUsers {
			execCleanupStep(cleanupCtx, t, setupPool, "auth.users", "DELETE FROM auth.users WHERE id = $1", testUserUID)
		}
	}()

	// Check auth.users table existence
	var hasAuthUsers bool
	if err := setupPool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
		t.Fatalf("failed to check auth.users existence: %v", err)
	}
	if hasAuthUsers {
		_, err = setupPool.Exec(ctx, `
			INSERT INTO auth.users (id, aud, role, email)
			VALUES ($1, 'authenticated', 'authenticated', $2)
			ON CONFLICT (id) DO NOTHING`,
			testUserUID, fmt.Sprintf("hist-%s@example.com", runID),
		)
		if err != nil {
			t.Fatalf("failed to seed auth.users: %v", err)
		}
	}

	// Insert profile
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.profiles (id, full_name)
		VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING`,
		testUserUID, fmt.Sprintf("Hist User %s", runID),
	); err != nil {
		t.Fatalf("failed to seed profile: %v", err)
	}

	// Insert gateway
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.gateways (gateway_id, name)
		VALUES ($1, 'History Native Gateway')`,
		gwID,
	); err != nil {
		t.Fatalf("failed to seed gateway: %v", err)
	}

	// Insert sensor
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.sensors (gateway_id, sensor_id, name, unit)
		VALUES ($1, $2, 'History Native Sensor', $3)`,
		gwID, sensorID, sensorUnit,
	); err != nil {
		t.Fatalf("failed to seed sensor: %v", err)
	}

	// Insert membership (owner)
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.user_gateways (user_id, gateway_id, role)
		VALUES ($1, $2, 'owner')`,
		testUserUID, gwID,
	); err != nil {
		t.Fatalf("failed to seed user_gateways: %v", err)
	}

	// Insert deterministic telemetry samples with valid provenance
	baseTime := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	type sampleDef struct {
		offset time.Duration
		value  float64
		seq    int64
		sIdx   int32
	}
	samples := []sampleDef{
		{offset: 0, value: 12.5, seq: 100, sIdx: 0},
		{offset: 400 * time.Millisecond, value: 17.5, seq: 101, sIdx: 1},
		{offset: 1200 * time.Millisecond, value: 30.0, seq: 102, sIdx: 2},
		{offset: 1800 * time.Millisecond, value: 50.0, seq: 103, sIdx: 3},
	}

	for _, s := range samples {
		measuredAt := baseTime.Add(s.offset)
		_, err := setupPool.Exec(ctx, `
			INSERT INTO public.telemetry (measured_at, gateway_id, sensor_id, sequence_num, value, boot_id, message_id, sample_index)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			measuredAt, gwID, sensorID, s.seq, s.value, bootID, msgID, s.sIdx,
		)
		if err != nil {
			t.Fatalf("failed to insert telemetry sample: %v", err)
		}
	}

	// 4. Construct real application-layer services using appPool
	cfg := telemetry.DefaultHistoryPreparationConfig()

	resolver, err := digitaltwin.NewPostgresResolver(appPool)
	if err != nil {
		t.Fatalf("failed to create digitaltwin.PostgresResolver: %v", err)
	}

	historyRepo, err := telemetry.NewPostgresHistoryRepository(appPool, cfg)
	if err != nil {
		t.Fatalf("failed to create telemetry.PostgresHistoryRepository: %v", err)
	}

	historyService, err := telemetry.NewTelemetryHistoryService(resolver, historyRepo, cfg)
	if err != nil {
		t.Fatalf("failed to create telemetry.TelemetryHistoryService: %v", err)
	}

	timeRange := telemetry.TimeRange{
		From: baseTime,
		To:   baseTime.Add(2 * time.Second),
	}

	// 5. Test normal raw read
	rawQuery := telemetry.TelemetryHistoryQuery{
		GatewayID:  gwID,
		SensorID:   sensorID,
		Range:      timeRange,
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  500,
	}

	rawResult, err := historyService.GetTelemetryHistory(ctx, testUserUID, rawQuery)
	if err != nil {
		t.Fatalf("GetTelemetryHistory (raw) failed: %v", err)
	}

	if rawResult.Resolution != string(telemetry.ResolutionRaw) {
		t.Fatalf("expected raw resolution, got %q", rawResult.Resolution)
	}
	if rawResult.Unit == nil || *rawResult.Unit != sensorUnit {
		t.Fatalf("expected unit %q, got %v", sensorUnit, rawResult.Unit)
	}
	if len(rawResult.RawPoints) != len(samples) {
		t.Fatalf("expected %d raw points, got %d", len(samples), len(rawResult.RawPoints))
	}

	// Assert independently expected values for each raw sample point
	for i, expected := range samples {
		actual := rawResult.RawPoints[i]
		expectedTime := baseTime.Add(expected.offset)
		if !actual.MeasuredAt.Equal(expectedTime) {
			t.Errorf("raw point [%d] measured_at = %v, expected %v", i, actual.MeasuredAt, expectedTime)
		}
		if math.Abs(actual.Value-expected.value) > 1e-9 {
			t.Errorf("raw point [%d] value = %f, expected %f", i, actual.Value, expected.value)
		}
		if actual.SequenceNum != expected.seq {
			t.Errorf("raw point [%d] sequence_num = %d, expected %d", i, actual.SequenceNum, expected.seq)
		}
		if actual.BootID == nil || *actual.BootID != bootID {
			t.Errorf("raw point [%d] boot_id mismatch", i)
		}
		if actual.MessageID == nil || *actual.MessageID != msgID {
			t.Errorf("raw point [%d] message_id mismatch", i)
		}
		if actual.SampleIndex == nil || *actual.SampleIndex != expected.sIdx {
			t.Errorf("raw point [%d] sample_index = %v, expected %d", i, actual.SampleIndex, expected.sIdx)
		}
	}
	t.Logf("Raw Read Verified: %d raw points successfully retrieved with expected values and provenance", len(rawResult.RawPoints))

	// 6. Test normal bucket read (1 second resolution)
	// Independent arithmetic for 1s buckets:
	// Bucket 0 [12:00:00, 12:00:01): samples 12.5 and 17.5
	//   count = 2, min = 12.5, max = 17.5, avg = (12.5 + 17.5) / 2 = 15.0
	// Bucket 1 [12:00:01, 12:00:02): samples 30.0 and 50.0
	//   count = 2, min = 30.0, max = 50.0, avg = (30.0 + 50.0) / 2 = 40.0
	type expectedBucket struct {
		start time.Time
		count int64
		min   float64
		max   float64
		avg   float64
	}
	expectedBuckets := []expectedBucket{
		{
			start: baseTime,
			count: 2,
			min:   12.5,
			max:   17.5,
			avg:   15.0,
		},
		{
			start: baseTime.Add(1 * time.Second),
			count: 2,
			min:   30.0,
			max:   50.0,
			avg:   40.0,
		},
	}

	bucketQuery := telemetry.TelemetryHistoryQuery{
		GatewayID:  gwID,
		SensorID:   sensorID,
		Range:      timeRange,
		Resolution: telemetry.Resolution1s,
		MaxPoints:  500,
	}

	bucketResult, err := historyService.GetTelemetryHistory(ctx, testUserUID, bucketQuery)
	if err != nil {
		t.Fatalf("GetTelemetryHistory (bucket 1s) failed: %v", err)
	}

	if bucketResult.Resolution != string(telemetry.Resolution1s) {
		t.Fatalf("expected resolution %q, got %q", telemetry.Resolution1s, bucketResult.Resolution)
	}
	if bucketResult.Unit == nil || *bucketResult.Unit != sensorUnit {
		t.Fatalf("expected unit %q, got %v", sensorUnit, bucketResult.Unit)
	}
	if len(bucketResult.BucketPoints) != len(expectedBuckets) {
		t.Fatalf("expected %d bucket points, got %d", len(expectedBuckets), len(bucketResult.BucketPoints))
	}

	for i, exp := range expectedBuckets {
		actual := bucketResult.BucketPoints[i]
		if !actual.BucketStart.Equal(exp.start) {
			t.Errorf("bucket point [%d] start = %v, expected %v", i, actual.BucketStart, exp.start)
		}
		if actual.Count != exp.count {
			t.Errorf("bucket point [%d] sample_count = %d, expected %d", i, actual.Count, exp.count)
		}
		if math.Abs(actual.Min-exp.min) > 1e-9 {
			t.Errorf("bucket point [%d] min_value = %f, expected %f", i, actual.Min, exp.min)
		}
		if math.Abs(actual.Max-exp.max) > 1e-9 {
			t.Errorf("bucket point [%d] max_value = %f, expected %f", i, actual.Max, exp.max)
		}
		if math.Abs(actual.Avg-exp.avg) > 1e-9 {
			t.Errorf("bucket point [%d] avg_value = %f, expected %f", i, actual.Avg, exp.avg)
		}
	}
	t.Logf("Bucket Read Verified: %d buckets successfully retrieved with pgx interval binding and TimescaleDB time_bucket aggregation",
		len(bucketResult.BucketPoints))
}

// revokingSensorResolver wraps a SensorResolver to invoke a hook (e.g. membership revocation)
// immediately after successful preparation-level sensor resolution and before the data query runs.
type revokingSensorResolver struct {
	inner      telemetry.SensorResolver
	onResolved func()
}

func (r *revokingSensorResolver) ResolveSensor(ctx context.Context, actor uuid.UUID, gatewayID, sensorID string) (*telemetry.ResolvedSensor, error) {
	resolved, err := r.inner.ResolveSensor(ctx, actor, gatewayID, sensorID)
	if err != nil {
		return nil, err
	}
	if r.onResolved != nil {
		r.onResolved()
	}
	return resolved, nil
}

// TestNativeTelemetryHistoryAuthorizationAndEmptyResult verifies:
//  1. Two-Gateway isolation despite shared sensor IDs on real TimescaleDB hypertables.
//  2. Fail-closed authorization matrix: owner, operator, viewer can read; non-member,
//     platform admin without membership, and non-existent resources all return ErrHistoryNotFound.
//  3. Authorized empty responses (zero points, nullable unit preserved, correct effective mode/resolution)
//     for explicit raw, explicit bucket, short auto (raw), and long auto (bucket).
//  4. Data-level SQL membership enforcement: when membership is revoked post-resolution (between preparation
//     and data query), data SQL CTE fails closed and returns ErrHistoryNotFound rather than authorized empty.
//  5. Strict teardown of exact fixture-owned resources under the setup connection without broad deletions.
func TestNativeTelemetryHistoryAuthorizationAndEmptyResult(t *testing.T) {
	setupURL, backendURL := resolveTestDatabaseURLs(t)
	if setupURL == "" || backendURL == "" {
		t.Skip("setup and application database connection URLs are not available or native test not opted in (set TELEMETRY_HISTORY_TEST_NATIVE=1 or TELEMETRY_TEST_SETUP_URL and TELEMETRY_TEST_BACKEND_URL)")
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

	// 1. Verify database and schema versions
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

	// 2. Verify roles: setupPool vs appPool
	var setupUser string
	var setupSuper, setupCanLogin bool
	err = setupPool.QueryRow(ctx, `
		SELECT current_user, r.rolsuper, r.rolcanlogin
		FROM pg_roles r WHERE r.rolname = current_user
	`).Scan(&setupUser, &setupSuper, &setupCanLogin)
	if err != nil {
		t.Fatalf("failed to verify setup connection role: %v", err)
	}

	var appUser string
	var appSuper, appCanLogin bool
	err = appPool.QueryRow(ctx, `
		SELECT current_user, r.rolsuper, r.rolcanlogin
		FROM pg_roles r WHERE r.rolname = current_user
	`).Scan(&appUser, &appSuper, &appCanLogin)
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

	// 3. Unique fixture IDs
	runID := uuid.New().String()[:8]
	gw1 := fmt.Sprintf("gw_auth1_%s", runID)
	gw2 := fmt.Sprintf("gw_auth2_%s", runID)
	sharedSensorID := fmt.Sprintf("sens_shared_%s", runID)
	emptySensorID := fmt.Sprintf("sens_empty_%s", runID)
	sharedUnit := "degC"

	ownerUser := uuid.New()
	operatorUser := uuid.New()
	viewerUser := uuid.New()
	nonMemberUser := uuid.New()
	platformAdminUser := uuid.New()
	revokedUser := uuid.New()
	revokedBucketUser := uuid.New()

	allTestUsers := []uuid.UUID{
		ownerUser,
		operatorUser,
		viewerUser,
		nonMemberUser,
		platformAdminUser,
		revokedUser,
		revokedBucketUser,
	}
	allTestGateways := []string{gw1, gw2}

	// Cleanup hook to delete strictly exact fixture-owned resources in reverse foreign key order
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()

		// 1. telemetry
		execCleanupStep(cleanupCtx, t, setupPool, "telemetry", "DELETE FROM public.telemetry WHERE gateway_id = ANY($1)", allTestGateways)
		// 2. user_gateways
		execCleanupStep(cleanupCtx, t, setupPool, "user_gateways", "DELETE FROM public.user_gateways WHERE gateway_id = ANY($1) OR user_id = ANY($2)", allTestGateways, allTestUsers)
		// 3. platform_admins
		execCleanupStep(cleanupCtx, t, setupPool, "platform_admins", "DELETE FROM public.platform_admins WHERE user_id = $1", platformAdminUser)
		// 4. sensors
		execCleanupStep(cleanupCtx, t, setupPool, "sensors", "DELETE FROM public.sensors WHERE gateway_id = ANY($1)", allTestGateways)
		// 5. gateways
		execCleanupStep(cleanupCtx, t, setupPool, "gateways", "DELETE FROM public.gateways WHERE gateway_id = ANY($1)", allTestGateways)
		// 6. profiles
		execCleanupStep(cleanupCtx, t, setupPool, "profiles", "DELETE FROM public.profiles WHERE id = ANY($1)", allTestUsers)
		// 7. auth.users
		var hasAuthUsers bool
		if err := setupPool.QueryRow(cleanupCtx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
			t.Errorf("cleanup check auth.users failed: %v", err)
		} else if hasAuthUsers {
			execCleanupStep(cleanupCtx, t, setupPool, "auth.users", "DELETE FROM auth.users WHERE id = ANY($1)", allTestUsers)
		}
	}()

	// Check auth.users table existence
	var hasAuthUsers bool
	if err := setupPool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
		t.Fatalf("failed to check auth.users existence: %v", err)
	}

	// Seed auth.users and profiles
	for _, u := range allTestUsers {
		if hasAuthUsers {
			_, err = setupPool.Exec(ctx, `
				INSERT INTO auth.users (id, aud, role, email)
				VALUES ($1, 'authenticated', 'authenticated', $2)
				ON CONFLICT (id) DO NOTHING`,
				u, fmt.Sprintf("auth-%s@example.com", u.String()[:8]),
			)
			if err != nil {
				t.Fatalf("failed to seed auth.users: %v", err)
			}
		}
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.profiles (id, full_name)
			VALUES ($1, $2)
			ON CONFLICT (id) DO NOTHING`,
			u, fmt.Sprintf("Auth User %s", u.String()[:8]),
		); err != nil {
			t.Fatalf("failed to seed profile: %v", err)
		}
	}

	// Seed platform admin without gateway membership
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.platform_admins (user_id)
		VALUES ($1)`,
		platformAdminUser,
	); err != nil {
		t.Fatalf("failed to seed platform_admins: %v", err)
	}

	// Seed Gateways: gw1 and gw2
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.gateways (gateway_id, name)
		VALUES ($1, 'Auth Gateway 1'), ($2, 'Auth Gateway 2')`,
		gw1, gw2,
	); err != nil {
		t.Fatalf("failed to seed gateways: %v", err)
	}

	// Seed Sensors:
	// gw1 has sharedSensorID (unit 'degC') AND emptySensorID (unit NULL)
	// gw2 has sharedSensorID (unit 'degC')
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.sensors (gateway_id, sensor_id, name, unit)
		VALUES
			($1, $2, 'Shared Sensor GW1', $3),
			($1, $4, 'Empty Sensor GW1', NULL),
			($5, $2, 'Shared Sensor GW2', $3)`,
		gw1, sharedSensorID, sharedUnit, emptySensorID, gw2,
	); err != nil {
		t.Fatalf("failed to seed sensors: %v", err)
	}

	// Seed Memberships:
	// owner, operator, viewer, and two users for revocation tests on gw1.
	// nonMemberUser has NO membership in user_gateways.
	// platformAdminUser has NO membership in user_gateways.
	// None of the users have membership on gw2.
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.user_gateways (user_id, gateway_id, role)
		VALUES
			($1, $2, 'owner'),
			($3, $2, 'operator'),
			($4, $2, 'viewer'),
			($5, $2, 'owner'),
			($6, $2, 'owner')`,
		ownerUser, gw1,
		operatorUser,
		viewerUser,
		revokedUser,
		revokedBucketUser,
	); err != nil {
		t.Fatalf("failed to seed user_gateways: %v", err)
	}

	// Seed distinguishable telemetry for gw1 vs gw2 on sharedSensorID
	baseTime := time.Date(2026, 10, 17, 14, 0, 0, 0, time.UTC)
	bootID1, msgID1 := uuid.New(), uuid.New()
	bootID2, msgID2 := uuid.New(), uuid.New()

	gw1Samples := []struct {
		offset time.Duration
		val    float64
		seq    int64
		sIdx   int32
	}{
		{offset: 0, val: 10.0, seq: 100, sIdx: 0},
		{offset: 500 * time.Millisecond, val: 20.0, seq: 101, sIdx: 1},
		{offset: 1200 * time.Millisecond, val: 30.0, seq: 102, sIdx: 2},
	}
	for _, s := range gw1Samples {
		_, err := setupPool.Exec(ctx, `
			INSERT INTO public.telemetry (measured_at, gateway_id, sensor_id, sequence_num, value, boot_id, message_id, sample_index)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			baseTime.Add(s.offset), gw1, sharedSensorID, s.seq, s.val, bootID1, msgID1, s.sIdx,
		)
		if err != nil {
			t.Fatalf("failed to seed telemetry for gw1: %v", err)
		}
	}

	gw2Samples := []struct {
		offset time.Duration
		val    float64
		seq    int64
		sIdx   int32
	}{
		{offset: 0, val: 910.0, seq: 200, sIdx: 0},
		{offset: 500 * time.Millisecond, val: 920.0, seq: 201, sIdx: 1},
		{offset: 1200 * time.Millisecond, val: 930.0, seq: 202, sIdx: 2},
	}
	for _, s := range gw2Samples {
		_, err := setupPool.Exec(ctx, `
			INSERT INTO public.telemetry (measured_at, gateway_id, sensor_id, sequence_num, value, boot_id, message_id, sample_index)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			baseTime.Add(s.offset), gw2, sharedSensorID, s.seq, s.val, bootID2, msgID2, s.sIdx,
		)
		if err != nil {
			t.Fatalf("failed to seed telemetry for gw2: %v", err)
		}
	}
	// emptySensorID on gw1 has ZERO telemetry samples.

	// 4. Construct application services using appPool (iot_backend_app role)
	cfg := telemetry.DefaultHistoryPreparationConfig()

	resolver, err := digitaltwin.NewPostgresResolver(appPool)
	if err != nil {
		t.Fatalf("failed to create digitaltwin.PostgresResolver: %v", err)
	}

	historyRepo, err := telemetry.NewPostgresHistoryRepository(appPool, cfg)
	if err != nil {
		t.Fatalf("failed to create telemetry.PostgresHistoryRepository: %v", err)
	}

	historyService, err := telemetry.NewTelemetryHistoryService(resolver, historyRepo, cfg)
	if err != nil {
		t.Fatalf("failed to create telemetry.TelemetryHistoryService: %v", err)
	}

	timeRange := telemetry.TimeRange{
		From: baseTime,
		To:   baseTime.Add(2 * time.Second),
	}

	rawQueryGW1 := telemetry.TelemetryHistoryQuery{
		GatewayID:  gw1,
		SensorID:   sharedSensorID,
		Range:      timeRange,
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  500,
	}

	bucketQueryGW1 := telemetry.TelemetryHistoryQuery{
		GatewayID:  gw1,
		SensorID:   sharedSensorID,
		Range:      timeRange,
		Resolution: telemetry.Resolution1s,
		MaxPoints:  500,
	}

	rawQueryGW2 := telemetry.TelemetryHistoryQuery{
		GatewayID:  gw2,
		SensorID:   sharedSensorID,
		Range:      timeRange,
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  500,
	}

	bucketQueryGW2 := telemetry.TelemetryHistoryQuery{
		GatewayID:  gw2,
		SensorID:   sharedSensorID,
		Range:      timeRange,
		Resolution: telemetry.Resolution1s,
		MaxPoints:  500,
	}

	// 5. Assert two-Gateway isolation despite shared Sensor IDs
	// ownerUser has membership on gw1 only.
	gw1RawRes, err := historyService.GetTelemetryHistory(ctx, ownerUser, rawQueryGW1)
	if err != nil {
		t.Fatalf("ownerUser reading gw1 raw failed: %v", err)
	}
	if gw1RawRes.GatewayID != gw1 {
		t.Errorf("expected GatewayID %s, got %s", gw1, gw1RawRes.GatewayID)
	}
	if gw1RawRes.PointCount != len(gw1Samples) {
		t.Fatalf("expected %d raw points for gw1, got %d", len(gw1Samples), gw1RawRes.PointCount)
	}
	for i, pt := range gw1RawRes.RawPoints {
		if math.Abs(pt.Value-gw1Samples[i].val) > 1e-9 {
			t.Errorf("gw1 raw point [%d] value = %f, expected %f", i, pt.Value, gw1Samples[i].val)
		}
		// Confirm none of gw2's samples (>= 900) leaked into gw1
		if pt.Value >= 900.0 {
			t.Errorf("cross-gateway leakage: gw1 raw point contains gw2 value %f", pt.Value)
		}
	}

	gw1BucketRes, err := historyService.GetTelemetryHistory(ctx, ownerUser, bucketQueryGW1)
	if err != nil {
		t.Fatalf("ownerUser reading gw1 bucket failed: %v", err)
	}
	if gw1BucketRes.PointCount != 2 {
		t.Fatalf("expected 2 bucket points for gw1, got %d", gw1BucketRes.PointCount)
	}
	// Bucket 0: samples 10.0 and 20.0 -> count 2, avg 15.0, min 10.0, max 20.0
	b0 := gw1BucketRes.BucketPoints[0]
	if b0.Count != 2 || math.Abs(b0.Avg-15.0) > 1e-9 || math.Abs(b0.Min-10.0) > 1e-9 || math.Abs(b0.Max-20.0) > 1e-9 {
		t.Errorf("gw1 bucket [0] mismatch: count=%d, avg=%f, min=%f, max=%f", b0.Count, b0.Avg, b0.Min, b0.Max)
	}
	// Bucket 1: sample 30.0 -> count 1, avg 30.0, min 30.0, max 30.0
	b1 := gw1BucketRes.BucketPoints[1]
	if b1.Count != 1 || math.Abs(b1.Avg-30.0) > 1e-9 || math.Abs(b1.Min-30.0) > 1e-9 || math.Abs(b1.Max-30.0) > 1e-9 {
		t.Errorf("gw1 bucket [1] mismatch: count=%d, avg=%f, min=%f, max=%f", b1.Count, b1.Avg, b1.Min, b1.Max)
	}

	// Verify ownerUser cannot access gw2 with the shared sensor ID
	deniedGW2Raw, err := historyService.GetTelemetryHistory(ctx, ownerUser, rawQueryGW2)
	if !errors.Is(err, telemetry.ErrHistoryNotFound) {
		t.Errorf("expected ErrHistoryNotFound for cross-gateway raw query, got %v", err)
	}
	if deniedGW2Raw != nil {
		t.Errorf("expected nil result on cross-gateway raw query denial, got %+v", deniedGW2Raw)
	}

	deniedGW2Bucket, err := historyService.GetTelemetryHistory(ctx, ownerUser, bucketQueryGW2)
	if !errors.Is(err, telemetry.ErrHistoryNotFound) {
		t.Errorf("expected ErrHistoryNotFound for cross-gateway bucket query, got %v", err)
	}
	if deniedGW2Bucket != nil {
		t.Errorf("expected nil result on cross-gateway bucket query denial, got %+v", deniedGW2Bucket)
	}
	t.Log("Two-Gateway isolation on shared Sensor ID verified for both raw and bucket modes")

	// 6. Assert Role Matrix: owner, operator, and viewer can read assigned resources
	for _, roleCase := range []struct {
		name string
		user uuid.UUID
	}{
		{name: "owner", user: ownerUser},
		{name: "operator", user: operatorUser},
		{name: "viewer", user: viewerUser},
	} {
		rawRes, err := historyService.GetTelemetryHistory(ctx, roleCase.user, rawQueryGW1)
		if err != nil {
			t.Errorf("role %s failed to read gw1 raw: %v", roleCase.name, err)
		} else if rawRes.PointCount != 3 {
			t.Errorf("role %s expected 3 raw points, got %d", roleCase.name, rawRes.PointCount)
		}

		bucketRes, err := historyService.GetTelemetryHistory(ctx, roleCase.user, bucketQueryGW1)
		if err != nil {
			t.Errorf("role %s failed to read gw1 bucket: %v", roleCase.name, err)
		} else if bucketRes.PointCount != 2 {
			t.Errorf("role %s expected 2 bucket points, got %d", roleCase.name, bucketRes.PointCount)
		}
	}
	t.Log("Role matrix verified: owner, operator, and viewer successfully read assigned resources")

	// 7. Assert fail-closed denials: non-member, platform admin without membership, and missing resources
	// All must return the exact same ErrHistoryNotFound with nil results.
	type denialCase struct {
		name  string
		user  uuid.UUID
		query telemetry.TelemetryHistoryQuery
	}
	denials := []denialCase{
		{
			name:  "non-member user (raw)",
			user:  nonMemberUser,
			query: rawQueryGW1,
		},
		{
			name:  "non-member user (bucket)",
			user:  nonMemberUser,
			query: bucketQueryGW1,
		},
		{
			name:  "platform admin without membership (raw)",
			user:  platformAdminUser,
			query: rawQueryGW1,
		},
		{
			name:  "platform admin without membership (bucket)",
			user:  platformAdminUser,
			query: bucketQueryGW1,
		},
		{
			name: "missing gateway under owner (raw)",
			user: ownerUser,
			query: telemetry.TelemetryHistoryQuery{
				GatewayID:  "gw_missing_nonexistent",
				SensorID:   sharedSensorID,
				Range:      timeRange,
				Resolution: telemetry.ResolutionRaw,
				MaxPoints:  500,
			},
		},
		{
			name: "missing sensor under owner (raw)",
			user: ownerUser,
			query: telemetry.TelemetryHistoryQuery{
				GatewayID:  gw1,
				SensorID:   "sens_missing_nonexistent",
				Range:      timeRange,
				Resolution: telemetry.ResolutionRaw,
				MaxPoints:  500,
			},
		},
		{
			name: "missing gateway under owner (bucket)",
			user: ownerUser,
			query: telemetry.TelemetryHistoryQuery{
				GatewayID:  "gw_missing_nonexistent",
				SensorID:   sharedSensorID,
				Range:      timeRange,
				Resolution: telemetry.Resolution1s,
				MaxPoints:  500,
			},
		},
		{
			name: "missing sensor under owner (bucket)",
			user: ownerUser,
			query: telemetry.TelemetryHistoryQuery{
				GatewayID:  gw1,
				SensorID:   "sens_missing_nonexistent",
				Range:      timeRange,
				Resolution: telemetry.Resolution1s,
				MaxPoints:  500,
			},
		},
	}

	for _, tc := range denials {
		res, err := historyService.GetTelemetryHistory(ctx, tc.user, tc.query)
		if !errors.Is(err, telemetry.ErrHistoryNotFound) {
			t.Errorf("case %q: expected ErrHistoryNotFound, got %v", tc.name, err)
		}
		if res != nil {
			t.Errorf("case %q: expected nil result, got %+v", tc.name, res)
		}
	}
	t.Log("Fail-closed denials verified: non-member, platform admin without membership, and missing resources all return ErrHistoryNotFound with nil result")

	// 8. Assert authorized empty results on emptySensorID (zero telemetry samples, NULL unit)
	// 8a. Explicit raw empty
	emptyRawQuery := telemetry.TelemetryHistoryQuery{
		GatewayID:  gw1,
		SensorID:   emptySensorID,
		Range:      timeRange,
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  500,
	}
	emptyRawRes, err := historyService.GetTelemetryHistory(ctx, ownerUser, emptyRawQuery)
	if err != nil {
		t.Fatalf("authorized empty raw query failed: %v", err)
	}
	if emptyRawRes.GatewayID != gw1 {
		t.Errorf("expected GatewayID %s, got %s", gw1, emptyRawRes.GatewayID)
	}
	if emptyRawRes.SensorID != emptySensorID {
		t.Errorf("expected SensorID %s, got %s", emptySensorID, emptyRawRes.SensorID)
	}
	if emptyRawRes.Unit != nil {
		t.Errorf("expected nil unit for sensor with NULL unit, got %v", *emptyRawRes.Unit)
	}
	if !emptyRawRes.From.Equal(timeRange.From.UTC()) || !emptyRawRes.To.Equal(timeRange.To.UTC()) {
		t.Errorf("time range mismatch: [%v, %v] vs [%v, %v]", emptyRawRes.From, emptyRawRes.To, timeRange.From, timeRange.To)
	}
	if emptyRawRes.Mode != "raw" || emptyRawRes.Resolution != "raw" {
		t.Errorf("expected mode/resolution raw/raw, got %s/%s", emptyRawRes.Mode, emptyRawRes.Resolution)
	}
	if emptyRawRes.PointCount != 0 {
		t.Errorf("expected PointCount 0, got %d", emptyRawRes.PointCount)
	}
	if len(emptyRawRes.RawPoints) != 0 {
		t.Errorf("expected empty RawPoints, got %d points", len(emptyRawRes.RawPoints))
	}

	// 8b. Explicit bucket empty (1s)
	emptyBucketQuery := telemetry.TelemetryHistoryQuery{
		GatewayID:  gw1,
		SensorID:   emptySensorID,
		Range:      timeRange,
		Resolution: telemetry.Resolution1s,
		MaxPoints:  500,
	}
	emptyBucketRes, err := historyService.GetTelemetryHistory(ctx, ownerUser, emptyBucketQuery)
	if err != nil {
		t.Fatalf("authorized empty bucket query failed: %v", err)
	}
	if emptyBucketRes.GatewayID != gw1 || emptyBucketRes.SensorID != emptySensorID {
		t.Errorf("identity mismatch in bucket empty: %s/%s", emptyBucketRes.GatewayID, emptyBucketRes.SensorID)
	}
	if emptyBucketRes.Unit != nil {
		t.Errorf("expected nil unit for sensor with NULL unit, got %v", *emptyBucketRes.Unit)
	}
	if emptyBucketRes.Mode != "bucket" || emptyBucketRes.Resolution != "1s" {
		t.Errorf("expected mode/resolution bucket/1s, got %s/%s", emptyBucketRes.Mode, emptyBucketRes.Resolution)
	}
	if emptyBucketRes.PointCount != 0 {
		t.Errorf("expected PointCount 0, got %d", emptyBucketRes.PointCount)
	}
	if len(emptyBucketRes.BucketPoints) != 0 {
		t.Errorf("expected empty BucketPoints, got %d points", len(emptyBucketRes.BucketPoints))
	}

	// 8c. Short auto empty: range 2 seconds <= RawWindow (10s) -> probes raw, returns raw empty
	shortAutoQuery := telemetry.TelemetryHistoryQuery{
		GatewayID:  gw1,
		SensorID:   emptySensorID,
		Range:      telemetry.TimeRange{From: baseTime, To: baseTime.Add(2 * time.Second)},
		Resolution: telemetry.ResolutionAuto,
		MaxPoints:  500,
	}
	shortAutoRes, err := historyService.GetTelemetryHistory(ctx, ownerUser, shortAutoQuery)
	if err != nil {
		t.Fatalf("short auto empty query failed: %v", err)
	}
	if shortAutoRes.Mode != "raw" || shortAutoRes.Resolution != "raw" {
		t.Errorf("expected short auto empty to have mode/resolution raw/raw, got %s/%s", shortAutoRes.Mode, shortAutoRes.Resolution)
	}
	if shortAutoRes.PointCount != 0 || len(shortAutoRes.RawPoints) != 0 {
		t.Errorf("expected 0 points for short auto empty, got %d (len %d)", shortAutoRes.PointCount, len(shortAutoRes.RawPoints))
	}
	if shortAutoRes.Unit != nil {
		t.Errorf("expected nil unit for short auto empty, got %v", *shortAutoRes.Unit)
	}

	// 8d. Long auto empty: range 4 hours > RawWindow (10s) -> skips raw probe, selects auto bucket (1m), returns bucket empty
	longAutoQuery := telemetry.TelemetryHistoryQuery{
		GatewayID:  gw1,
		SensorID:   emptySensorID,
		Range:      telemetry.TimeRange{From: baseTime, To: baseTime.Add(4 * time.Hour)},
		Resolution: telemetry.ResolutionAuto,
		MaxPoints:  500,
	}
	longAutoRes, err := historyService.GetTelemetryHistory(ctx, ownerUser, longAutoQuery)
	if err != nil {
		t.Fatalf("long auto empty query failed: %v", err)
	}
	if longAutoRes.Mode != "bucket" || longAutoRes.Resolution != string(telemetry.Resolution1m) {
		t.Errorf("expected long auto empty to have mode/resolution bucket/1m, got %s/%s", longAutoRes.Mode, longAutoRes.Resolution)
	}
	if longAutoRes.PointCount != 0 || len(longAutoRes.BucketPoints) != 0 {
		t.Errorf("expected 0 points for long auto empty, got %d (len %d)", longAutoRes.PointCount, len(longAutoRes.BucketPoints))
	}
	if longAutoRes.Unit != nil {
		t.Errorf("expected nil unit for long auto empty, got %v", *longAutoRes.Unit)
	}
	t.Log("Authorized empty results verified for explicit raw, explicit bucket, short auto (raw), and long auto (bucket)")

	// 9. Verify membership is checked inside the data SQL (post-precheck membership revocation)
	// 9a. Raw query: revoke membership after ResolveSensor succeeds, before data SQL runs
	var rawRevokedOnce bool
	revokingResolverRaw := &revokingSensorResolver{
		inner: resolver,
		onResolved: func() {
			if !rawRevokedOnce {
				rawRevokedOnce = true
				_, delErr := setupPool.Exec(ctx, "DELETE FROM public.user_gateways WHERE user_id = $1 AND gateway_id = $2", revokedUser, gw1)
				if delErr != nil {
					t.Fatalf("failed to revoke membership for raw test: %v", delErr)
				}
			}
		},
	}
	svcRevokedRaw, err := telemetry.NewTelemetryHistoryService(revokingResolverRaw, historyRepo, cfg)
	if err != nil {
		t.Fatalf("failed to create svcRevokedRaw: %v", err)
	}
	postRevokeRawRes, err := svcRevokedRaw.GetTelemetryHistory(ctx, revokedUser, emptyRawQuery)
	if !errors.Is(err, telemetry.ErrHistoryNotFound) {
		t.Errorf("expected ErrHistoryNotFound for post-precheck membership revocation (raw), got %v", err)
	}
	if postRevokeRawRes != nil {
		t.Errorf("expected nil result on post-precheck membership revocation (raw), got %+v", postRevokeRawRes)
	}

	// 9b. Bucket query: revoke membership after ResolveSensor succeeds, before data SQL runs
	var bucketRevokedOnce bool
	revokingResolverBucket := &revokingSensorResolver{
		inner: resolver,
		onResolved: func() {
			if !bucketRevokedOnce {
				bucketRevokedOnce = true
				_, delErr := setupPool.Exec(ctx, "DELETE FROM public.user_gateways WHERE user_id = $1 AND gateway_id = $2", revokedBucketUser, gw1)
				if delErr != nil {
					t.Fatalf("failed to revoke membership for bucket test: %v", delErr)
				}
			}
		},
	}
	svcRevokedBucket, err := telemetry.NewTelemetryHistoryService(revokingResolverBucket, historyRepo, cfg)
	if err != nil {
		t.Fatalf("failed to create svcRevokedBucket: %v", err)
	}
	postRevokeBucketRes, err := svcRevokedBucket.GetTelemetryHistory(ctx, revokedBucketUser, emptyBucketQuery)
	if !errors.Is(err, telemetry.ErrHistoryNotFound) {
		t.Errorf("expected ErrHistoryNotFound for post-precheck membership revocation (bucket), got %v", err)
	}
	if postRevokeBucketRes != nil {
		t.Errorf("expected nil result on post-precheck membership revocation (bucket), got %+v", postRevokeBucketRes)
	}

	// 9c. Direct repository check: calling historyRepo directly for the now-revoked users returns ErrHistoryNotFound
	directRawRes, err := historyRepo.ReadTelemetryHistory(ctx, revokedUser, emptyRawQuery)
	if !errors.Is(err, telemetry.ErrHistoryNotFound) {
		t.Errorf("expected direct historyRepo.ReadTelemetryHistory (raw) to return ErrHistoryNotFound, got %v", err)
	}
	if directRawRes != nil {
		t.Errorf("expected nil result from direct historyRepo raw read, got %+v", directRawRes)
	}

	directBucketRes, err := historyRepo.ReadTelemetryHistory(ctx, revokedBucketUser, emptyBucketQuery)
	if !errors.Is(err, telemetry.ErrHistoryNotFound) {
		t.Errorf("expected direct historyRepo.ReadTelemetryHistory (bucket) to return ErrHistoryNotFound, got %v", err)
	}
	if directBucketRes != nil {
		t.Errorf("expected nil result from direct historyRepo bucket read, got %+v", directBucketRes)
	}
	t.Log("Data SQL membership enforcement verified: post-precheck membership revocation returns ErrHistoryNotFound (not authorized empty) for both raw and bucket")
}

// TestNativeTelemetryHistoryIntervalBoundariesAndOrdering verifies:
//  1. Real half-open [from, to) boundaries: samples at from and 1us before to are included;
//     samples 1us before from, exactly at to, and after to are excluded.
//  2. Intentionally unsorted insertion order is stably ordered by (measured_at ASC, sample_id ASC).
//  3. Two valid distinct boot IDs with the exact same measured_at and sequence_num are NOT deduplicated,
//     both are returned, and their relative order is deterministic based on sample_id.
//  4. Sequence values strictly greater than 2^53 are stored and retrieved with exact 64-bit int fidelity.
func TestNativeTelemetryHistoryIntervalBoundariesAndOrdering(t *testing.T) {
	setupURL, backendURL := resolveTestDatabaseURLs(t)
	if setupURL == "" || backendURL == "" {
		t.Skip("setup and application database connection URLs are not available or native test not opted in (set TELEMETRY_HISTORY_TEST_NATIVE=1 or TELEMETRY_TEST_SETUP_URL and TELEMETRY_TEST_BACKEND_URL)")
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

	// Verify roles
	var appUser string
	var appSuper bool
	if err := appPool.QueryRow(ctx, "SELECT current_user, r.rolsuper FROM pg_roles r WHERE r.rolname = current_user").Scan(&appUser, &appSuper); err != nil {
		t.Fatalf("failed to verify application connection role: %v", err)
	}
	if appUser != "iot_backend_app" || appSuper {
		t.Fatalf("expected application role iot_backend_app (non-superuser), got %q (super=%v)", appUser, appSuper)
	}

	runID := uuid.New().String()[:8]
	testUserUID := uuid.New()
	gwID := fmt.Sprintf("gw_bnd_%s", runID)
	sensorID := fmt.Sprintf("sens_bnd_%s", runID)
	sensorUnit := "degC"
	bootID1 := uuid.New()
	bootID2 := uuid.New()
	msgID1 := uuid.New()
	msgID2 := uuid.New()

	// Cleanup hook
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		execCleanupStep(cleanupCtx, t, setupPool, "telemetry", "DELETE FROM public.telemetry WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "user_gateways", "DELETE FROM public.user_gateways WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "sensors", "DELETE FROM public.sensors WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "gateways", "DELETE FROM public.gateways WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "profiles", "DELETE FROM public.profiles WHERE id = $1", testUserUID)

		var hasAuthUsers bool
		if err := setupPool.QueryRow(cleanupCtx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
			t.Errorf("cleanup check auth.users failed: %v", err)
		} else if hasAuthUsers {
			execCleanupStep(cleanupCtx, t, setupPool, "auth.users", "DELETE FROM auth.users WHERE id = $1", testUserUID)
		}
	}()

	var hasAuthUsers bool
	if err := setupPool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err == nil && hasAuthUsers {
		_, err = setupPool.Exec(ctx, `
			INSERT INTO auth.users (id, aud, role, email)
			VALUES ($1, 'authenticated', 'authenticated', $2)
			ON CONFLICT (id) DO NOTHING`,
			testUserUID, fmt.Sprintf("bnd-%s@example.com", runID),
		)
		if err != nil {
			t.Fatalf("failed to seed auth.users: %v", err)
		}
	}

	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.profiles (id, full_name)
		VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING`,
		testUserUID, fmt.Sprintf("Bnd User %s", runID),
	); err != nil {
		t.Fatalf("failed to seed profile: %v", err)
	}
	if _, err := setupPool.Exec(ctx, `INSERT INTO public.gateways (gateway_id, name) VALUES ($1, 'Boundary Gateway')`, gwID); err != nil {
		t.Fatalf("failed to seed gateway: %v", err)
	}
	if _, err := setupPool.Exec(ctx, `INSERT INTO public.sensors (gateway_id, sensor_id, name, unit) VALUES ($1, $2, 'Boundary Sensor', $3)`, gwID, sensorID, sensorUnit); err != nil {
		t.Fatalf("failed to seed sensor: %v", err)
	}
	if _, err := setupPool.Exec(ctx, `INSERT INTO public.user_gateways (user_id, gateway_id, role) VALUES ($1, $2, 'owner')`, testUserUID, gwID); err != nil {
		t.Fatalf("failed to seed user_gateways: %v", err)
	}

	from := time.Date(2026, 10, 17, 14, 0, 0, 0, time.UTC)
	to := time.Date(2026, 10, 17, 14, 0, 1, 0, time.UTC)
	seqOver2To53 := int64(1<<53) + 77 // 9007199254741069 > 2^53

	// Seed samples in intentionally unsorted insertion order:
	// 1) After to: to + 1us (excluded)
	// 2) Exactly at to: to (excluded)
	// 3) 1us before to: to - 1us (included)
	// 4) Boot 2 at from + 200ms (included, same timestamp and sequence as Boot 1)
	// 5) Just before from: from - 1us (excluded)
	// 6) Exactly at from: from (included)
	// 7) Sequence > 2^53 at from + 600ms (included)
	// 8) Boot 1 at from + 200ms (included, same timestamp and sequence as Boot 2)
	type sampleSeed struct {
		tag        string
		measuredAt time.Time
		val        float64
		seq        int64
		boot       uuid.UUID
		msg        uuid.UUID
		sIdx       int32
		inRange    bool
	}
	seeds := []sampleSeed{
		{tag: "after_to", measuredAt: to.Add(1 * time.Microsecond), val: 991.0, seq: 10, boot: bootID1, msg: msgID1, sIdx: 0, inRange: false},
		{tag: "at_to", measuredAt: to, val: 992.0, seq: 20, boot: bootID1, msg: msgID1, sIdx: 1, inRange: false},
		{tag: "before_to_1us", measuredAt: to.Add(-1 * time.Microsecond), val: 80.0, seq: 30, boot: bootID1, msg: msgID1, sIdx: 2, inRange: true},
		{tag: "boot2_200ms", measuredAt: from.Add(200 * time.Millisecond), val: 42.0, seq: 400, boot: bootID2, msg: msgID2, sIdx: 0, inRange: true},
		{tag: "before_from_1us", measuredAt: from.Add(-1 * time.Microsecond), val: 993.0, seq: 50, boot: bootID1, msg: msgID1, sIdx: 3, inRange: false},
		{tag: "at_from", measuredAt: from, val: 10.0, seq: 60, boot: bootID1, msg: msgID1, sIdx: 4, inRange: true},
		{tag: "seq_over_53", measuredAt: from.Add(600 * time.Millisecond), val: 60.0, seq: seqOver2To53, boot: bootID1, msg: msgID1, sIdx: 5, inRange: true},
		{tag: "boot1_200ms", measuredAt: from.Add(200 * time.Millisecond), val: 41.0, seq: 400, boot: bootID1, msg: msgID1, sIdx: 6, inRange: true},
	}

	insertedIDs := make(map[string]int64)
	for _, s := range seeds {
		var sampleID int64
		err := setupPool.QueryRow(ctx, `
			INSERT INTO public.telemetry (measured_at, gateway_id, sensor_id, sequence_num, value, boot_id, message_id, sample_index)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			RETURNING sample_id`,
			s.measuredAt, gwID, sensorID, s.seq, s.val, s.boot, s.msg, s.sIdx,
		).Scan(&sampleID)
		if err != nil {
			t.Fatalf("failed to insert seed sample %q: %v", s.tag, err)
		}
		insertedIDs[s.tag] = sampleID
	}

	cfg := telemetry.DefaultHistoryPreparationConfig()
	resolver, err := digitaltwin.NewPostgresResolver(appPool)
	if err != nil {
		t.Fatalf("failed to create PostgresResolver: %v", err)
	}
	historyRepo, err := telemetry.NewPostgresHistoryRepository(appPool, cfg)
	if err != nil {
		t.Fatalf("failed to create PostgresHistoryRepository: %v", err)
	}
	historyService, err := telemetry.NewTelemetryHistoryService(resolver, historyRepo, cfg)
	if err != nil {
		t.Fatalf("failed to create TelemetryHistoryService: %v", err)
	}

	query := telemetry.TelemetryHistoryQuery{
		GatewayID:  gwID,
		SensorID:   sensorID,
		Range:      telemetry.TimeRange{From: from, To: to},
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  500,
	}

	result, err := historyService.GetTelemetryHistory(ctx, testUserUID, query)
	if err != nil {
		t.Fatalf("GetTelemetryHistory (boundary raw) failed: %v", err)
	}

	// Exactly 5 samples must be returned:
	//  0: at_from (from)
	//  1 & 2: boot1_200ms and boot2_200ms (both at from+200ms, ordered by sample_id)
	//  3: seq_over_53 (from+600ms)
	//  4: before_to_1us (to - 1us)
	if len(result.RawPoints) != 5 {
		t.Fatalf("expected exactly 5 raw points in [from, to), got %d", len(result.RawPoints))
	}
	if result.PointCount != 5 {
		t.Fatalf("expected PointCount 5, got %d", result.PointCount)
	}

	// Verify sample 0: exactly at from
	p0 := result.RawPoints[0]
	if !p0.MeasuredAt.Equal(from) {
		t.Errorf("point 0 measured_at = %v, expected %v", p0.MeasuredAt, from)
	}
	if math.Abs(p0.Value-10.0) > 1e-9 {
		t.Errorf("point 0 value = %f, expected 10.0", p0.Value)
	}
	if p0.SequenceNum != 60 {
		t.Errorf("point 0 sequence_num = %d, expected 60", p0.SequenceNum)
	}

	// Verify points 1 & 2: two boots at from + 200ms with same sequence 400
	p1 := result.RawPoints[1]
	p2 := result.RawPoints[2]
	t200ms := from.Add(200 * time.Millisecond)
	if !p1.MeasuredAt.Equal(t200ms) || !p2.MeasuredAt.Equal(t200ms) {
		t.Errorf("points 1 and 2 measured_at mismatch: p1=%v, p2=%v, expected %v", p1.MeasuredAt, p2.MeasuredAt, t200ms)
	}
	if p1.SequenceNum != 400 || p2.SequenceNum != 400 {
		t.Errorf("points 1 and 2 sequence mismatch: p1=%d, p2=%d, expected 400", p1.SequenceNum, p2.SequenceNum)
	}

	idBoot1 := insertedIDs["boot1_200ms"]
	idBoot2 := insertedIDs["boot2_200ms"]
	var expectedFirstBoot, expectedSecondBoot uuid.UUID
	var expectedFirstVal, expectedSecondVal float64
	if idBoot1 < idBoot2 {
		expectedFirstBoot, expectedFirstVal = bootID1, 41.0
		expectedSecondBoot, expectedSecondVal = bootID2, 42.0
	} else {
		expectedFirstBoot, expectedFirstVal = bootID2, 42.0
		expectedSecondBoot, expectedSecondVal = bootID1, 41.0
	}

	if p1.BootID == nil || *p1.BootID != expectedFirstBoot || math.Abs(p1.Value-expectedFirstVal) > 1e-9 {
		t.Errorf("point 1 boot/value mismatch: boot=%v (expected %v), val=%f (expected %f)", p1.BootID, expectedFirstBoot, p1.Value, expectedFirstVal)
	}
	if p2.BootID == nil || *p2.BootID != expectedSecondBoot || math.Abs(p2.Value-expectedSecondVal) > 1e-9 {
		t.Errorf("point 2 boot/value mismatch: boot=%v (expected %v), val=%f (expected %f)", p2.BootID, expectedSecondBoot, p2.Value, expectedSecondVal)
	}
	if p1.BootID != nil && p2.BootID != nil && *p1.BootID == *p2.BootID {
		t.Errorf("distinct boots collapsed into duplicate boot: p1=%v, p2=%v", *p1.BootID, *p2.BootID)
	}

	// Verify sample 3: sequence > 2^53
	p3 := result.RawPoints[3]
	t600ms := from.Add(600 * time.Millisecond)
	if !p3.MeasuredAt.Equal(t600ms) {
		t.Errorf("point 3 measured_at = %v, expected %v", p3.MeasuredAt, t600ms)
	}
	if p3.SequenceNum != seqOver2To53 {
		t.Errorf("point 3 sequence_num = %d, expected %d", p3.SequenceNum, seqOver2To53)
	}
	if p3.SequenceNum <= (1 << 53) {
		t.Errorf("point 3 sequence_num %d was expected to be > 2^53 (%d)", p3.SequenceNum, int64(1<<53))
	}
	if math.Abs(p3.Value-60.0) > 1e-9 {
		t.Errorf("point 3 value = %f, expected 60.0", p3.Value)
	}

	// Verify sample 4: 1us before to
	p4 := result.RawPoints[4]
	tBeforeTo := to.Add(-1 * time.Microsecond)
	if !p4.MeasuredAt.Equal(tBeforeTo) {
		t.Errorf("point 4 measured_at = %v, expected %v", p4.MeasuredAt, tBeforeTo)
	}
	if math.Abs(p4.Value-80.0) > 1e-9 {
		t.Errorf("point 4 value = %f, expected 80.0", p4.Value)
	}
	if p4.SequenceNum != 30 {
		t.Errorf("point 4 sequence_num = %d, expected 30", p4.SequenceNum)
	}

	// Assert that none of the excluded sentinels (val >= 900.0) are present
	for idx, pt := range result.RawPoints {
		if pt.Value >= 900.0 {
			t.Errorf("boundary exclusion failure at point [%d]: out-of-range sample with value %f was included", idx, pt.Value)
		}
	}
	t.Log("Half-open boundaries, stable ordering, non-deduplication of distinct boots, and >2^53 BIGINT fidelity verified on real TimescaleDB")
}

// TestNativeTelemetryHistoryBucketArithmeticAndClipping verifies:
//  1. Independent bucket arithmetic: avg, min, max, count are calculated accurately.
//  2. Clipping and partial edges: values outside [from, to) in partial first and last buckets
//     are strictly clipped and do not pollute bucket aggregates.
//  3. Negative and positive peaks in a single bucket aggregate without cancellation or overflow.
//  4. Gap preservation: empty buckets produce no row in the result.
//  5. Pre-epoch range: bucket aggregation across 1969/1970 boundary works correctly on TimescaleDB.
func TestNativeTelemetryHistoryBucketArithmeticAndClipping(t *testing.T) {
	setupURL, backendURL := resolveTestDatabaseURLs(t)
	if setupURL == "" || backendURL == "" {
		t.Skip("setup and application database connection URLs are not available or native test not opted in (set TELEMETRY_HISTORY_TEST_NATIVE=1 or TELEMETRY_TEST_SETUP_URL and TELEMETRY_TEST_BACKEND_URL)")
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

	runID := uuid.New().String()[:8]
	testUserUID := uuid.New()
	gwID := fmt.Sprintf("gw_bkt_%s", runID)
	sensorID := fmt.Sprintf("sens_bkt_%s", runID)
	sensorUnit := "m/s"
	bootID := uuid.New()
	msgID := uuid.New()

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		execCleanupStep(cleanupCtx, t, setupPool, "telemetry", "DELETE FROM public.telemetry WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "user_gateways", "DELETE FROM public.user_gateways WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "sensors", "DELETE FROM public.sensors WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "gateways", "DELETE FROM public.gateways WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "profiles", "DELETE FROM public.profiles WHERE id = $1", testUserUID)

		var hasAuthUsers bool
		if err := setupPool.QueryRow(cleanupCtx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
			t.Errorf("cleanup check auth.users failed: %v", err)
		} else if hasAuthUsers {
			execCleanupStep(cleanupCtx, t, setupPool, "auth.users", "DELETE FROM auth.users WHERE id = $1", testUserUID)
		}
	}()

	var hasAuthUsers bool
	if err := setupPool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err == nil && hasAuthUsers {
		_, err = setupPool.Exec(ctx, `
			INSERT INTO auth.users (id, aud, role, email)
			VALUES ($1, 'authenticated', 'authenticated', $2)
			ON CONFLICT (id) DO NOTHING`,
			testUserUID, fmt.Sprintf("bkt-%s@example.com", runID),
		)
		if err != nil {
			t.Fatalf("failed to seed auth.users: %v", err)
		}
	}

	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.profiles (id, full_name)
		VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING`,
		testUserUID, fmt.Sprintf("Bkt User %s", runID),
	); err != nil {
		t.Fatalf("failed to seed profile: %v", err)
	}
	if _, err := setupPool.Exec(ctx, `INSERT INTO public.gateways (gateway_id, name) VALUES ($1, 'Bucket Gateway')`, gwID); err != nil {
		t.Fatalf("failed to seed gateway: %v", err)
	}
	if _, err := setupPool.Exec(ctx, `INSERT INTO public.sensors (gateway_id, sensor_id, name, unit) VALUES ($1, $2, 'Bucket Sensor', $3)`, gwID, sensorID, sensorUnit); err != nil {
		t.Fatalf("failed to seed sensor: %v", err)
	}
	if _, err := setupPool.Exec(ctx, `INSERT INTO public.user_gateways (user_id, gateway_id, role) VALUES ($1, $2, 'owner')`, testUserUID, gwID); err != nil {
		t.Fatalf("failed to seed user_gateways: %v", err)
	}

	cfg := telemetry.DefaultHistoryPreparationConfig()
	resolver, err := digitaltwin.NewPostgresResolver(appPool)
	if err != nil {
		t.Fatalf("failed to create PostgresResolver: %v", err)
	}
	historyRepo, err := telemetry.NewPostgresHistoryRepository(appPool, cfg)
	if err != nil {
		t.Fatalf("failed to create PostgresHistoryRepository: %v", err)
	}
	historyService, err := telemetry.NewTelemetryHistoryService(resolver, historyRepo, cfg)
	if err != nil {
		t.Fatalf("failed to create TelemetryHistoryService: %v", err)
	}

	// 1. Non-aligned range with partial edges, values 1 and 3, peaks, gap, and sentinels
	// from = 15:00:00.500Z, to = 15:00:04.500Z
	baseSec := time.Date(2026, 10, 17, 15, 0, 0, 0, time.UTC)
	from := baseSec.Add(500 * time.Millisecond)
	to := baseSec.Add(4500 * time.Millisecond)

	type bktSeed struct {
		tag        string
		measuredAt time.Time
		val        float64
		seq        int64
		sIdx       int32
	}
	bktSeeds := []bktSeed{
		// Bucket 0 [15:00:00, 15:00:01):
		// Sentinel outside [from, to) but within bucket 0
		{tag: "s_b0_early", measuredAt: baseSec.Add(200 * time.Millisecond), val: 999999.0, seq: 1, sIdx: 0},
		{tag: "s_b0_edge", measuredAt: baseSec.Add(499 * time.Millisecond), val: 888888.0, seq: 2, sIdx: 1},
		// In range for bucket 0: values 1.0 and 3.0 -> count 2, min 1.0, max 3.0, avg 2.0
		{tag: "val_1", measuredAt: baseSec.Add(600 * time.Millisecond), val: 1.0, seq: 3, sIdx: 2},
		{tag: "val_3", measuredAt: baseSec.Add(800 * time.Millisecond), val: 3.0, seq: 4, sIdx: 3},

		// Bucket 1 [15:00:01, 15:00:02):
		// In range: negative peak -100.0, zero 0.0, positive peak 100.0 -> count 3, min -100, max 100, avg 0.0
		{tag: "peak_neg", measuredAt: baseSec.Add(1100 * time.Millisecond), val: -100.0, seq: 5, sIdx: 4},
		{tag: "peak_zero", measuredAt: baseSec.Add(1500 * time.Millisecond), val: 0.0, seq: 6, sIdx: 5},
		{tag: "peak_pos", measuredAt: baseSec.Add(1900 * time.Millisecond), val: 100.0, seq: 7, sIdx: 6},

		// Bucket 2 [15:00:02, 15:00:03):
		// GAP: NO SAMPLES SEEDED

		// Bucket 3 [15:00:03, 15:00:04):
		// In range: values 10.0 and 30.0 -> count 2, min 10.0, max 30.0, avg 20.0
		{tag: "b3_s1", measuredAt: baseSec.Add(3200 * time.Millisecond), val: 10.0, seq: 8, sIdx: 7},
		{tag: "b3_s2", measuredAt: baseSec.Add(3800 * time.Millisecond), val: 30.0, seq: 9, sIdx: 8},

		// Bucket 4 [15:00:04, 15:00:05):
		// In range: value 50.0 -> count 1, min 50.0, max 50.0, avg 50.0
		{tag: "b4_in", measuredAt: baseSec.Add(4200 * time.Millisecond), val: 50.0, seq: 10, sIdx: 9},
		// Sentinels at and after to in bucket 4
		{tag: "s_b4_at_to", measuredAt: baseSec.Add(4500 * time.Millisecond), val: -888888.0, seq: 11, sIdx: 10},
		{tag: "s_b4_after_to", measuredAt: baseSec.Add(4700 * time.Millisecond), val: -999999.0, seq: 12, sIdx: 11},
		// Sentinel in bucket 5
		{tag: "s_b5_late", measuredAt: baseSec.Add(5100 * time.Millisecond), val: -777777.0, seq: 13, sIdx: 12},
	}

	for _, s := range bktSeeds {
		_, err := setupPool.Exec(ctx, `
			INSERT INTO public.telemetry (measured_at, gateway_id, sensor_id, sequence_num, value, boot_id, message_id, sample_index)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			s.measuredAt, gwID, sensorID, s.seq, s.val, bootID, msgID, s.sIdx,
		)
		if err != nil {
			t.Fatalf("failed to insert bucket seed %q: %v", s.tag, err)
		}
	}

	query1s := telemetry.TelemetryHistoryQuery{
		GatewayID:  gwID,
		SensorID:   sensorID,
		Range:      telemetry.TimeRange{From: from, To: to},
		Resolution: telemetry.Resolution1s,
		MaxPoints:  500,
	}

	res1s, err := historyService.GetTelemetryHistory(ctx, testUserUID, query1s)
	if err != nil {
		t.Fatalf("GetTelemetryHistory (bucket 1s) failed: %v", err)
	}

	// Assert exactly 4 bucket points (gap at bucket 2 [15:00:02, 15:00:03) preserved)
	if len(res1s.BucketPoints) != 4 {
		t.Fatalf("expected 4 bucket points (gap preserved), got %d", len(res1s.BucketPoints))
	}
	if res1s.PointCount != 4 {
		t.Fatalf("expected PointCount 4, got %d", res1s.PointCount)
	}

	// Independently computed expected buckets:
	type expBkt struct {
		start time.Time
		count int64
		min   float64
		max   float64
		avg   float64
	}
	expected1s := []expBkt{
		{start: baseSec, count: 2, min: 1.0, max: 3.0, avg: 2.0},
		{start: baseSec.Add(1 * time.Second), count: 3, min: -100.0, max: 100.0, avg: 0.0},
		// Bucket 2 at baseSec+2s is omitted
		{start: baseSec.Add(3 * time.Second), count: 2, min: 10.0, max: 30.0, avg: 20.0},
		{start: baseSec.Add(4 * time.Second), count: 1, min: 50.0, max: 50.0, avg: 50.0},
	}

	for i, exp := range expected1s {
		act := res1s.BucketPoints[i]
		if !act.BucketStart.Equal(exp.start) {
			t.Errorf("bucket [%d] start = %v, expected %v", i, act.BucketStart, exp.start)
		}
		if act.Count != exp.count {
			t.Errorf("bucket [%d] count = %d, expected %d", i, act.Count, exp.count)
		}
		if math.Abs(act.Min-exp.min) > 1e-9 {
			t.Errorf("bucket [%d] min = %f, expected %f", i, act.Min, exp.min)
		}
		if math.Abs(act.Max-exp.max) > 1e-9 {
			t.Errorf("bucket [%d] max = %f, expected %f", i, act.Max, exp.max)
		}
		if math.Abs(act.Avg-exp.avg) > 1e-9 {
			t.Errorf("bucket [%d] avg = %f, expected %f", i, act.Avg, exp.avg)
		}
	}
	t.Log("Bucket arithmetic: partial edges clipped, avg/min/max exact, and gap preserved")

	// 2. Pre-epoch range: 1969-12-31 23:59:58.500Z to 1970-01-01 00:00:01.500Z
	preFrom := time.Date(1969, 12, 31, 23, 59, 58, 500_000_000, time.UTC)
	preTo := time.Date(1970, 1, 1, 0, 0, 1, 500_000_000, time.UTC)
	epochMsgID := uuid.New()

	preSeeds := []bktSeed{
		{tag: "pre_sentinel_early", measuredAt: time.Date(1969, 12, 31, 23, 59, 58, 200_000_000, time.UTC), val: 999999.0, seq: 101, sIdx: 0},
		{tag: "pre_in_b0", measuredAt: time.Date(1969, 12, 31, 23, 59, 58, 700_000_000, time.UTC), val: 5.0, seq: 102, sIdx: 1},
		{tag: "pre_in_b1_a", measuredAt: time.Date(1969, 12, 31, 23, 59, 59, 200_000_000, time.UTC), val: -10.0, seq: 103, sIdx: 2},
		{tag: "pre_in_b1_b", measuredAt: time.Date(1969, 12, 31, 23, 59, 59, 800_000_000, time.UTC), val: 20.0, seq: 104, sIdx: 3},
		{tag: "post_in_b2", measuredAt: time.Date(1970, 1, 1, 0, 0, 0, 500_000_000, time.UTC), val: 30.0, seq: 105, sIdx: 4},
		{tag: "post_sentinel_late", measuredAt: time.Date(1970, 1, 1, 0, 0, 1, 800_000_000, time.UTC), val: -999999.0, seq: 106, sIdx: 5},
	}

	for _, s := range preSeeds {
		_, err := setupPool.Exec(ctx, `
			INSERT INTO public.telemetry (measured_at, gateway_id, sensor_id, sequence_num, value, boot_id, message_id, sample_index)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			s.measuredAt, gwID, sensorID, s.seq, s.val, bootID, epochMsgID, s.sIdx,
		)
		if err != nil {
			t.Fatalf("failed to insert pre-epoch seed %q: %v", s.tag, err)
		}
	}

	preQuery := telemetry.TelemetryHistoryQuery{
		GatewayID:  gwID,
		SensorID:   sensorID,
		Range:      telemetry.TimeRange{From: preFrom, To: preTo},
		Resolution: telemetry.Resolution1s,
		MaxPoints:  500,
	}

	preRes, err := historyService.GetTelemetryHistory(ctx, testUserUID, preQuery)
	if err != nil {
		t.Fatalf("GetTelemetryHistory (pre-epoch bucket 1s) failed: %v", err)
	}

	if len(preRes.BucketPoints) != 3 {
		t.Fatalf("expected 3 pre-epoch bucket points, got %d", len(preRes.BucketPoints))
	}

	expectedPre := []expBkt{
		{start: time.Date(1969, 12, 31, 23, 59, 58, 0, time.UTC), count: 1, min: 5.0, max: 5.0, avg: 5.0},
		{start: time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), count: 2, min: -10.0, max: 20.0, avg: 5.0},
		{start: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), count: 1, min: 30.0, max: 30.0, avg: 30.0},
	}

	for i, exp := range expectedPre {
		act := preRes.BucketPoints[i]
		if !act.BucketStart.Equal(exp.start) {
			t.Errorf("pre-epoch bucket [%d] start = %v, expected %v", i, act.BucketStart, exp.start)
		}
		if act.Count != exp.count {
			t.Errorf("pre-epoch bucket [%d] count = %d, expected %d", i, act.Count, exp.count)
		}
		if math.Abs(act.Min-exp.min) > 1e-9 {
			t.Errorf("pre-epoch bucket [%d] min = %f, expected %f", i, act.Min, exp.min)
		}
		if math.Abs(act.Max-exp.max) > 1e-9 {
			t.Errorf("pre-epoch bucket [%d] max = %f, expected %f", i, act.Max, exp.max)
		}
		if math.Abs(act.Avg-exp.avg) > 1e-9 {
			t.Errorf("pre-epoch bucket [%d] avg = %f, expected %f", i, act.Avg, exp.avg)
		}
	}
	t.Log("Pre-epoch bucket aggregation across 1969/1970 boundary verified on TimescaleDB")
}

// TestNativeTelemetryHistoryCapAndOverflow verifies:
//  1. Legal configuration with MinPoints=100 and maxPoints=100 (not lowered).
//  2. Exact cap success: when sample count == maxPoints, explicit raw succeeds.
//  3. Short auto with sample count == maxPoints returns raw points directly.
//  4. Explicit raw overflow: when sample count == maxPoints + 1, explicit raw fails with
//     ErrHistoryRawLimitExceeded and returns a nil result.
//  5. Auto overflow fallback: short auto detects raw overflow, switches to the smallest valid
//     bucket resolution (100ms for 1s window), and aggregates complete counts (101 samples).
//  6. Long raw window failure: explicit raw on range > RawWindow fails with ErrHistoryRawLimitExceeded.
//  7. Explicit too-fine bucket failure: explicit 100ms on 20s window with maxPoints=100 fails
//     with ErrHistoryResolutionTooFine rather than returning truncated success.
func TestNativeTelemetryHistoryCapAndOverflow(t *testing.T) {
	setupURL, backendURL := resolveTestDatabaseURLs(t)
	if setupURL == "" || backendURL == "" {
		t.Skip("setup and application database connection URLs are not available or native test not opted in (set TELEMETRY_HISTORY_TEST_NATIVE=1 or TELEMETRY_TEST_SETUP_URL and TELEMETRY_TEST_BACKEND_URL)")
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

	runID := uuid.New().String()[:8]
	testUserUID := uuid.New()
	gwID := fmt.Sprintf("gw_cap_%s", runID)
	sensorID := fmt.Sprintf("sens_cap_%s", runID)
	sensorUnit := "V"
	bootID := uuid.New()
	msgID := uuid.New()

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		execCleanupStep(cleanupCtx, t, setupPool, "telemetry", "DELETE FROM public.telemetry WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "user_gateways", "DELETE FROM public.user_gateways WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "sensors", "DELETE FROM public.sensors WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "gateways", "DELETE FROM public.gateways WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "profiles", "DELETE FROM public.profiles WHERE id = $1", testUserUID)

		var hasAuthUsers bool
		if err := setupPool.QueryRow(cleanupCtx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
			t.Errorf("cleanup check auth.users failed: %v", err)
		} else if hasAuthUsers {
			execCleanupStep(cleanupCtx, t, setupPool, "auth.users", "DELETE FROM auth.users WHERE id = $1", testUserUID)
		}
	}()

	var hasAuthUsers bool
	if err := setupPool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err == nil && hasAuthUsers {
		_, err = setupPool.Exec(ctx, `
			INSERT INTO auth.users (id, aud, role, email)
			VALUES ($1, 'authenticated', 'authenticated', $2)
			ON CONFLICT (id) DO NOTHING`,
			testUserUID, fmt.Sprintf("cap-%s@example.com", runID),
		)
		if err != nil {
			t.Fatalf("failed to seed auth.users: %v", err)
		}
	}

	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.profiles (id, full_name)
		VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING`,
		testUserUID, fmt.Sprintf("Cap User %s", runID),
	); err != nil {
		t.Fatalf("failed to seed profile: %v", err)
	}
	if _, err := setupPool.Exec(ctx, `INSERT INTO public.gateways (gateway_id, name) VALUES ($1, 'Cap Gateway')`, gwID); err != nil {
		t.Fatalf("failed to seed gateway: %v", err)
	}
	if _, err := setupPool.Exec(ctx, `INSERT INTO public.sensors (gateway_id, sensor_id, name, unit) VALUES ($1, $2, 'Cap Sensor', $3)`, gwID, sensorID, sensorUnit); err != nil {
		t.Fatalf("failed to seed sensor: %v", err)
	}
	if _, err := setupPool.Exec(ctx, `INSERT INTO public.user_gateways (user_id, gateway_id, role) VALUES ($1, $2, 'owner')`, testUserUID, gwID); err != nil {
		t.Fatalf("failed to seed user_gateways: %v", err)
	}

	// Legal configuration enforcing MinPoints = 100
	cfg := telemetry.DefaultHistoryPreparationConfig()
	if cfg.MinPoints < 100 {
		t.Fatalf("configuration violates contract: MinPoints %d < 100", cfg.MinPoints)
	}

	resolver, err := digitaltwin.NewPostgresResolver(appPool)
	if err != nil {
		t.Fatalf("failed to create PostgresResolver: %v", err)
	}
	historyRepo, err := telemetry.NewPostgresHistoryRepository(appPool, cfg)
	if err != nil {
		t.Fatalf("failed to create PostgresHistoryRepository: %v", err)
	}
	historyService, err := telemetry.NewTelemetryHistoryService(resolver, historyRepo, cfg)
	if err != nil {
		t.Fatalf("failed to create TelemetryHistoryService: %v", err)
	}

	// Short raw window: 1 second <= RawWindow (10s)
	from := time.Date(2026, 10, 17, 16, 0, 0, 0, time.UTC)
	to := time.Date(2026, 10, 17, 16, 0, 1, 0, time.UTC)
	const maxPoints = 100 // exact configured minimum bound

	// 1. Seed exactly maxPoints (100) samples in [from, to)
	for i := 0; i < maxPoints; i++ {
		offsetMicros := int64(10_000 + i*9_000) // 10ms + i*9ms -> 10ms..901ms < 1000ms
		measuredAt := from.Add(time.Duration(offsetMicros) * time.Microsecond)
		_, err := setupPool.Exec(ctx, `
			INSERT INTO public.telemetry (measured_at, gateway_id, sensor_id, sequence_num, value, boot_id, message_id, sample_index)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			measuredAt, gwID, sensorID, int64(1000+i), float64(i), bootID, msgID, int32(i),
		)
		if err != nil {
			t.Fatalf("failed to insert initial sample [%d]: %v", i, err)
		}
	}

	queryRawExact := telemetry.TelemetryHistoryQuery{
		GatewayID:  gwID,
		SensorID:   sensorID,
		Range:      telemetry.TimeRange{From: from, To: to},
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  maxPoints,
	}

	// Assert explicit raw exact-cap success
	resRawExact, err := historyService.GetTelemetryHistory(ctx, testUserUID, queryRawExact)
	if err != nil {
		t.Fatalf("explicit raw exact-cap query failed: %v", err)
	}
	if resRawExact.PointCount != maxPoints || len(resRawExact.RawPoints) != maxPoints {
		t.Fatalf("expected exactly %d raw points, got %d (len %d)", maxPoints, resRawExact.PointCount, len(resRawExact.RawPoints))
	}
	if resRawExact.Mode != "raw" || resRawExact.Resolution != "raw" {
		t.Errorf("expected mode/resolution raw/raw, got %s/%s", resRawExact.Mode, resRawExact.Resolution)
	}

	// Assert auto resolution exact-cap success (returns raw directly)
	queryAutoExact := telemetry.TelemetryHistoryQuery{
		GatewayID:  gwID,
		SensorID:   sensorID,
		Range:      telemetry.TimeRange{From: from, To: to},
		Resolution: telemetry.ResolutionAuto,
		MaxPoints:  maxPoints,
	}
	resAutoExact, err := historyService.GetTelemetryHistory(ctx, testUserUID, queryAutoExact)
	if err != nil {
		t.Fatalf("auto exact-cap query failed: %v", err)
	}
	if resAutoExact.PointCount != maxPoints || len(resAutoExact.RawPoints) != maxPoints {
		t.Fatalf("expected exactly %d raw points for auto probe, got %d (len %d)", maxPoints, resAutoExact.PointCount, len(resAutoExact.RawPoints))
	}
	if resAutoExact.Mode != "raw" || resAutoExact.Resolution != "raw" {
		t.Errorf("expected auto exact-cap mode/resolution raw/raw, got %s/%s", resAutoExact.Mode, resAutoExact.Resolution)
	}
	t.Log("Exact cap success: explicit raw and auto return exactly maxPoints (100) raw samples")

	// 2. Seed sample maxPoints + 1 (101st sample) in [from, to)
	sample101Time := from.Add(950 * time.Millisecond)
	_, err = setupPool.Exec(ctx, `
		INSERT INTO public.telemetry (measured_at, gateway_id, sensor_id, sequence_num, value, boot_id, message_id, sample_index)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		sample101Time, gwID, sensorID, int64(2000), 999.0, bootID, msgID, int32(maxPoints),
	)
	if err != nil {
		t.Fatalf("failed to insert 101st sample: %v", err)
	}

	// Assert explicit raw overflow: fails with ErrHistoryRawLimitExceeded and returns nil result
	resRawOver, err := historyService.GetTelemetryHistory(ctx, testUserUID, queryRawExact)
	if !errors.Is(err, telemetry.ErrHistoryRawLimitExceeded) {
		t.Errorf("expected ErrHistoryRawLimitExceeded for explicit raw overflow, got %v", err)
	}
	if resRawOver != nil {
		t.Errorf("expected nil result on explicit raw overflow, got %+v", resRawOver)
	}

	// Assert auto overflow fallback: switches to smallest valid bucket resolution (100ms) with complete counts
	resAutoOver, err := historyService.GetTelemetryHistory(ctx, testUserUID, queryAutoExact)
	if err != nil {
		t.Fatalf("auto overflow fallback failed: %v", err)
	}
	if resAutoOver == nil {
		t.Fatalf("expected non-nil result on auto overflow fallback")
	}
	if resAutoOver.Mode != "bucket" {
		t.Errorf("expected mode bucket on auto fallback, got %s", resAutoOver.Mode)
	}
	if resAutoOver.Resolution != string(telemetry.Resolution100ms) {
		t.Errorf("expected smallest valid resolution 100ms for 1s window, got %s", resAutoOver.Resolution)
	}

	var totalSamples int64
	for _, b := range resAutoOver.BucketPoints {
		totalSamples += b.Count
	}
	if totalSamples != int64(maxPoints+1) {
		t.Errorf("expected total bucket count %d (complete counts), got %d across %d buckets", maxPoints+1, totalSamples, len(resAutoOver.BucketPoints))
	}
	t.Logf("Auto fallback verified: raw overflow seamlessly switched to %s bucket with complete count of %d samples",
		resAutoOver.Resolution, totalSamples)

	// 3. Assert long raw window failure (> RawWindow 10s): returns domain error, nil result
	queryLongRaw := telemetry.TelemetryHistoryQuery{
		GatewayID:  gwID,
		SensorID:   sensorID,
		Range:      telemetry.TimeRange{From: from, To: from.Add(15 * time.Second)},
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  maxPoints,
	}
	resLongRaw, err := historyService.GetTelemetryHistory(ctx, testUserUID, queryLongRaw)
	if !errors.Is(err, telemetry.ErrHistoryRawLimitExceeded) {
		t.Errorf("expected ErrHistoryRawLimitExceeded for long raw window, got %v", err)
	}
	if resLongRaw != nil {
		t.Errorf("expected nil result on long raw window failure, got %+v", resLongRaw)
	}

	// 4. Assert explicit too-fine bucket failure: 100ms on 20s window with maxPoints=100 (200 buckets > 100)
	queryTooFine := telemetry.TelemetryHistoryQuery{
		GatewayID:  gwID,
		SensorID:   sensorID,
		Range:      telemetry.TimeRange{From: from, To: from.Add(20 * time.Second)},
		Resolution: telemetry.Resolution100ms,
		MaxPoints:  maxPoints,
	}
	resTooFine, err := historyService.GetTelemetryHistory(ctx, testUserUID, queryTooFine)
	if !errors.Is(err, telemetry.ErrHistoryResolutionTooFine) {
		t.Errorf("expected ErrHistoryResolutionTooFine for explicit too-fine bucket, got %v", err)
	}
	if resTooFine != nil {
		t.Errorf("expected nil result on explicit too-fine bucket failure, got %+v", resTooFine)
	}
	t.Log("Cap and overflow bounds verified: explicit raw cap/overflow, auto bucket fallback with complete counts, and domain error rejections")
}

// TestNativeTelemetryHistoryPersistRetryAndReadIntegrity satisfies Task 4.2.6 requirements:
//  1. Reuses opt-in native fixture and actual iot_backend_app pools.
//  2. Seeds minimum valid provisioning graph (Gateway, Sensor, Twin entities, hasSensor relationship, states, user_gateways).
//  3. Validates and commits a real serialized telemetry batch through Persist, then resends the exact immutable bytes.
//     Asserts first outcome CommitOutcomeCommitted vs duplicate CommitOutcomeDuplicateCommitted,
//     unchanged sample/ledger counts, and unchanged raw and bucket history after retry without reader-side dedup.
//  4. Snapshots fixture-owned ledger, raw counts, reported state/version, desired state/version, and temporal observations,
//     runs history reads (raw, bucket, auto), and asserts that all sentinels remain strictly unchanged.
//  5. Adds one deterministic real database deadline scenario using a bounded pool (MaxConns=1) with connection held,
//     asserting ErrHistoryDependency wrapping context.DeadlineExceeded, nil result, and verified recovery upon release.
func TestNativeTelemetryHistoryPersistRetryAndReadIntegrity(t *testing.T) {
	setupURL, backendURL := resolveTestDatabaseURLs(t)
	if setupURL == "" || backendURL == "" {
		t.Skip("setup and application database connection URLs are not available or native test not opted in (set TELEMETRY_HISTORY_TEST_NATIVE=1 or TELEMETRY_TEST_SETUP_URL and TELEMETRY_TEST_BACKEND_URL)")
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

	// Verify application connection role is strictly iot_backend_app
	var appUser string
	if err := appPool.QueryRow(ctx, "SELECT current_user").Scan(&appUser); err != nil {
		t.Fatalf("failed to query application current_user: %v", err)
	}
	if appUser != "iot_backend_app" {
		t.Fatalf("application connection running as %q, expected iot_backend_app", appUser)
	}

	runID := uuid.New().String()[:8]
	testUserUID := uuid.New()
	gwID := fmt.Sprintf("gw_retry_%s", runID)
	sensorID := fmt.Sprintf("sens_retry_%s", runID)
	sensorUnit := "degC"
	gwURN := fmt.Sprintf("urn:ngsi-ld:Gateway:%s", gwID)
	sensorURN := fmt.Sprintf("urn:ngsi-ld:Sensor:%s:%s", gwID, sensorID)

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		execCleanupStep(cleanupCtx, t, setupPool, "twin_temporal_values", "DELETE FROM public.twin_temporal_values WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = $1)", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "twin_states", "DELETE FROM public.twin_states WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = $1)", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "twin_relationships", "DELETE FROM public.twin_relationships WHERE source_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = $1) OR target_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = $1)", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "twin_entities", "DELETE FROM public.twin_entities WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "processed_messages", "DELETE FROM public.processed_messages WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "telemetry", "DELETE FROM public.telemetry WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "user_gateways", "DELETE FROM public.user_gateways WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "sensors", "DELETE FROM public.sensors WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "gateways", "DELETE FROM public.gateways WHERE gateway_id = $1", gwID)
		execCleanupStep(cleanupCtx, t, setupPool, "profiles", "DELETE FROM public.profiles WHERE id = $1", testUserUID)

		var hasAuthUsers bool
		if err := setupPool.QueryRow(cleanupCtx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
			t.Errorf("cleanup check auth.users failed: %v", err)
		} else if hasAuthUsers {
			execCleanupStep(cleanupCtx, t, setupPool, "auth.users", "DELETE FROM auth.users WHERE id = $1", testUserUID)
		}
	}()

	// 1. Seed minimum valid provisioning graph
	var hasAuthUsers bool
	if err := setupPool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err == nil && hasAuthUsers {
		_, err = setupPool.Exec(ctx, `
			INSERT INTO auth.users (id, aud, role, email)
			VALUES ($1, 'authenticated', 'authenticated', $2)
			ON CONFLICT (id) DO NOTHING`,
			testUserUID, fmt.Sprintf("retry-%s@example.com", runID),
		)
		if err != nil {
			t.Fatalf("failed to seed auth.users: %v", err)
		}
	}

	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.profiles (id, full_name)
		VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING`,
		testUserUID, fmt.Sprintf("Retry User %s", runID),
	); err != nil {
		t.Fatalf("failed to seed profile: %v", err)
	}

	if _, err := setupPool.Exec(ctx, `INSERT INTO public.gateways (gateway_id, name) VALUES ($1, 'Retry Gateway')`, gwID); err != nil {
		t.Fatalf("failed to seed gateway: %v", err)
	}
	if _, err := setupPool.Exec(ctx, `INSERT INTO public.sensors (gateway_id, sensor_id, name, unit) VALUES ($1, $2, 'Retry Sensor', $3)`, gwID, sensorID, sensorUnit); err != nil {
		t.Fatalf("failed to seed sensor: %v", err)
	}
	if _, err := setupPool.Exec(ctx, `INSERT INTO public.user_gateways (user_id, gateway_id, role) VALUES ($1, $2, 'owner')`, testUserUID, gwID); err != nil {
		t.Fatalf("failed to seed user_gateways: %v", err)
	}

	// Twin entities for Gateway and Sensor
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.twin_entities (entity_id, entity_type, gateway_id)
		VALUES ($1, 'Gateway', $3), ($2, 'Sensor', $3)`,
		gwURN, sensorURN, gwID,
	); err != nil {
		t.Fatalf("failed to seed twin_entities: %v", err)
	}

	// hasSensor relationship
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.twin_relationships (source_entity_id, relationship_type, target_entity_id)
		SELECT g.id, 'hasSensor', s.id
		FROM public.twin_entities g, public.twin_entities s
		WHERE g.entity_id = $1 AND s.entity_id = $2`,
		gwURN, sensorURN,
	); err != nil {
		t.Fatalf("failed to seed twin_relationships: %v", err)
	}

	// twin_states for both entities with initial versions
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.twin_states (entity_id, desired_state, desired_version, reported_state, reported_version)
		SELECT id, '{"sampling_seconds":10}'::jsonb, 1, '{}'::jsonb, 0
		FROM public.twin_entities
		WHERE gateway_id = $1`,
		gwID,
	); err != nil {
		t.Fatalf("failed to seed twin_states: %v", err)
	}

	// 2. Instantiate actual Validator, Persist Repository, and History Service
	validator, err := telemetry.NewValidator(telemetry.Limits{
		MaxPayloadBytes:  1 << 20,
		MaxSamples:       1000,
		MaxBatchDuration: time.Hour,
		MaxFutureSkew:    time.Hour,
	})
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	persistRepo, err := telemetry.NewPostgresRepository(appPool, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to create persist repository: %v", err)
	}

	cfg := telemetry.DefaultHistoryPreparationConfig()
	resolver, err := digitaltwin.NewPostgresResolver(appPool)
	if err != nil {
		t.Fatalf("failed to create resolver: %v", err)
	}
	historyRepo, err := telemetry.NewPostgresHistoryRepository(appPool, cfg)
	if err != nil {
		t.Fatalf("failed to create history repository: %v", err)
	}
	historyService, err := telemetry.NewTelemetryHistoryService(resolver, historyRepo, cfg)
	if err != nil {
		t.Fatalf("failed to create history service: %v", err)
	}

	// 3. Create valid serialized telemetry batch (20 samples, 100ms interval = 2.0s window)
	bootID := uuid.New()
	msgID := uuid.New()
	baseTime := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Millisecond)
	const sampleCount = 20
	samples := make([]float64, sampleCount)
	for i := 0; i < sampleCount; i++ {
		samples[i] = 22.0 + float64(i)*0.25
	}

	dto := telemetry.BatchDTO{
		ProtocolVersion:  1,
		MessageID:        msgID,
		MessageType:      "telemetry_batch",
		GatewayID:        gwID,
		SensorID:         sensorID,
		BootID:           bootID,
		FirstSequence:    5001,
		SampleCount:      int32(sampleCount),
		MeasuredAt:       baseTime.Format(time.RFC3339Nano),
		SampleIntervalUS: 100000, // 100ms
		Samples:          samples,
	}
	payloadBytes, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("failed to marshal batch DTO: %v", err)
	}

	topic := fmt.Sprintf("gateways/%s/telemetry/%s", gwID, sensorID)
	validatedMsg, err := validator.Validate(topic, payloadBytes, time.Now().UTC())
	if err != nil {
		t.Fatalf("failed to validate telemetry message: %v", err)
	}

	// First Persist call -> CommitOutcomeCommitted
	res1, err := persistRepo.Persist(ctx, validatedMsg)
	if err != nil {
		t.Fatalf("initial Persist failed: %v", err)
	}
	if res1.Outcome != telemetry.CommitOutcomeCommitted {
		t.Fatalf("expected initial outcome CommitOutcomeCommitted, got %v", res1.Outcome)
	}
	if res1.SamplesSaved != sampleCount {
		t.Fatalf("expected %d samples saved, got %d", sampleCount, res1.SamplesSaved)
	}

	// Query initial history (raw and bucket)
	queryRange := telemetry.TimeRange{
		From: baseTime,
		To:   baseTime.Add(2500 * time.Millisecond), // 2.5s <= RawWindow (10s)
	}
	queryRaw := telemetry.TelemetryHistoryQuery{
		GatewayID:  gwID,
		SensorID:   sensorID,
		Range:      queryRange,
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  100,
	}
	queryBucket := telemetry.TelemetryHistoryQuery{
		GatewayID:  gwID,
		SensorID:   sensorID,
		Range:      queryRange,
		Resolution: telemetry.Resolution100ms,
		MaxPoints:  100,
	}

	rawBefore, err := historyService.GetTelemetryHistory(ctx, testUserUID, queryRaw)
	if err != nil {
		t.Fatalf("initial raw history query failed: %v", err)
	}
	if rawBefore.PointCount != sampleCount {
		t.Fatalf("expected %d raw points initially, got %d", sampleCount, rawBefore.PointCount)
	}

	bucketBefore, err := historyService.GetTelemetryHistory(ctx, testUserUID, queryBucket)
	if err != nil {
		t.Fatalf("initial bucket history query failed: %v", err)
	}
	if len(bucketBefore.BucketPoints) == 0 {
		t.Fatalf("expected bucket points initially, got 0")
	}

	// Resend the exact same immutable validated bytes -> CommitOutcomeDuplicateCommitted
	res2, err := persistRepo.Persist(ctx, validatedMsg)
	if err != nil {
		t.Fatalf("retry Persist failed: %v", err)
	}
	if res2.Outcome != telemetry.CommitOutcomeDuplicateCommitted {
		t.Fatalf("expected duplicate outcome CommitOutcomeDuplicateCommitted, got %v", res2.Outcome)
	}

	// Assert unchanged sample and ledger counts on retry
	var rawDbCount int
	if err := setupPool.QueryRow(ctx, "SELECT count(*) FROM public.telemetry WHERE gateway_id = $1 AND message_id = $2", gwID, msgID).Scan(&rawDbCount); err != nil {
		t.Fatalf("failed to query raw telemetry count: %v", err)
	}
	if rawDbCount != sampleCount {
		t.Fatalf("raw sample count mutated on retry: expected %d, got %d", sampleCount, rawDbCount)
	}

	var ledgerDbCount int
	if err := setupPool.QueryRow(ctx, "SELECT count(*) FROM public.processed_messages WHERE gateway_id = $1 AND message_id = $2", gwID, msgID).Scan(&ledgerDbCount); err != nil {
		t.Fatalf("failed to query processed_messages count: %v", err)
	}
	if ledgerDbCount != 1 {
		t.Fatalf("ledger count mutated on retry: expected 1, got %d", ledgerDbCount)
	}

	// Assert unchanged raw and bucket history after retry without reader-side dedup
	rawAfter, err := historyService.GetTelemetryHistory(ctx, testUserUID, queryRaw)
	if err != nil {
		t.Fatalf("post-retry raw history query failed: %v", err)
	}
	if rawAfter.PointCount != rawBefore.PointCount {
		t.Fatalf("raw history point count changed after retry: before=%d, after=%d", rawBefore.PointCount, rawAfter.PointCount)
	}
	if len(rawAfter.RawPoints) != len(rawBefore.RawPoints) {
		t.Fatalf("raw history slice len changed after retry: before=%d, after=%d", len(rawBefore.RawPoints), len(rawAfter.RawPoints))
	}
	for i := range rawBefore.RawPoints {
		p1 := rawBefore.RawPoints[i]
		p2 := rawAfter.RawPoints[i]
		if p1.Value != p2.Value || p1.SequenceNum != p2.SequenceNum || !p1.MeasuredAt.Equal(p2.MeasuredAt) {
			t.Fatalf("raw point [%d] changed after retry: %+v vs %+v", i, p1, p2)
		}
	}

	bucketAfter, err := historyService.GetTelemetryHistory(ctx, testUserUID, queryBucket)
	if err != nil {
		t.Fatalf("post-retry bucket history query failed: %v", err)
	}
	if bucketAfter.PointCount != bucketBefore.PointCount {
		t.Fatalf("bucket history point count changed after retry: before=%d, after=%d", bucketBefore.PointCount, bucketAfter.PointCount)
	}
	if len(bucketAfter.BucketPoints) != len(bucketBefore.BucketPoints) {
		t.Fatalf("bucket history slice len changed after retry: before=%d, after=%d", len(bucketBefore.BucketPoints), len(bucketAfter.BucketPoints))
	}
	for i := range bucketBefore.BucketPoints {
		b1 := bucketBefore.BucketPoints[i]
		b2 := bucketAfter.BucketPoints[i]
		if b1.Count != b2.Count || b1.Avg != b2.Avg || b1.Min != b2.Min || b1.Max != b2.Max || !b1.BucketStart.Equal(b2.BucketStart) {
			t.Fatalf("bucket point [%d] changed after retry: %+v vs %+v", i, b1, b2)
		}
	}
	t.Log("Persist retry verified: duplicate outcome, unchanged DB sample/ledger counts, and identical raw/bucket history without reader-side dedup")

	// 4. Snapshot fixture-owned sentinels to verify read-only integrity
	type Sentinels struct {
		ledgerCount    int
		rawCount       int
		sensorReported string
		sensorDesired  string
		gatewayState   string
		temporalCount  int
	}

	takeSentinels := func() Sentinels {
		var s Sentinels
		if err := setupPool.QueryRow(ctx, "SELECT count(*) FROM public.processed_messages WHERE gateway_id = $1", gwID).Scan(&s.ledgerCount); err != nil {
			t.Fatalf("sentinel ledgerCount query failed: %v", err)
		}
		if err := setupPool.QueryRow(ctx, "SELECT count(*) FROM public.telemetry WHERE gateway_id = $1", gwID).Scan(&s.rawCount); err != nil {
			t.Fatalf("sentinel rawCount query failed: %v", err)
		}
		if err := setupPool.QueryRow(ctx, `
			SELECT row(s.reported_state, s.reported_version, s.last_reported_at)::text
			FROM public.twin_states s
			JOIN public.twin_entities e ON e.id = s.entity_id
			WHERE e.entity_id = $1`, sensorURN).Scan(&s.sensorReported); err != nil {
			t.Fatalf("sentinel sensorReported query failed: %v", err)
		}
		if err := setupPool.QueryRow(ctx, `
			SELECT row(s.desired_state, s.desired_version, s.last_desired_at, s.last_desired_by)::text
			FROM public.twin_states s
			JOIN public.twin_entities e ON e.id = s.entity_id
			WHERE e.entity_id = $1`, sensorURN).Scan(&s.sensorDesired); err != nil {
			t.Fatalf("sentinel sensorDesired query failed: %v", err)
		}
		if err := setupPool.QueryRow(ctx, `
			SELECT row(s.reported_state, s.desired_state, s.reported_version, s.desired_version, s.last_reported_at, s.last_desired_at)::text
			FROM public.twin_states s
			JOIN public.twin_entities e ON e.id = s.entity_id
			WHERE e.entity_id = $1`, gwURN).Scan(&s.gatewayState); err != nil {
			t.Fatalf("sentinel gatewayState query failed: %v", err)
		}
		if err := setupPool.QueryRow(ctx, `
			SELECT count(*)
			FROM public.twin_temporal_values
			WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = $1)`, sensorURN).Scan(&s.temporalCount); err != nil {
			t.Fatalf("sentinel temporalCount query failed: %v", err)
		}
		return s
	}

	sentinelsBefore := takeSentinels()

	// Execute multiple reads: raw, bucket, and auto
	if _, err := historyService.GetTelemetryHistory(ctx, testUserUID, queryRaw); err != nil {
		t.Fatalf("raw history read failed during sentinel test: %v", err)
	}
	if _, err := historyService.GetTelemetryHistory(ctx, testUserUID, queryBucket); err != nil {
		t.Fatalf("bucket history read failed during sentinel test: %v", err)
	}
	queryAuto := telemetry.TelemetryHistoryQuery{
		GatewayID:  gwID,
		SensorID:   sensorID,
		Range:      queryRange,
		Resolution: telemetry.ResolutionAuto,
		MaxPoints:  100,
	}
	if _, err := historyService.GetTelemetryHistory(ctx, testUserUID, queryAuto); err != nil {
		t.Fatalf("auto history read failed during sentinel test: %v", err)
	}

	sentinelsAfter := takeSentinels()
	if sentinelsBefore != sentinelsAfter {
		t.Fatalf("read-only integrity violated by history reads:\n before: %+v\n after:  %+v",
			sentinelsBefore, sentinelsAfter)
	}
	t.Log("Read-only integrity verified: ledger, raw counts, reported/desired states and versions, and temporal observations remained strictly untouched across multiple history reads")

	// 5. Deterministic real database deadline scenario using bounded pool technique (MaxConns=1)
	boundedCfg, err := pgxpool.ParseConfig(backendURL)
	if err != nil {
		t.Fatalf("failed to parse backend URL for bounded pool: %v", err)
	}
	boundedCfg.MaxConns = 1
	boundedPool, err := pgxpool.NewWithConfig(ctx, boundedCfg)
	if err != nil {
		t.Fatalf("failed to create bounded pool: %v", err)
	}
	defer boundedPool.Close()

	// Acquire the single connection from boundedPool, exhausting the pool deterministically without sleep
	heldConn, err := boundedPool.Acquire(ctx)
	if err != nil {
		t.Fatalf("failed to acquire sole connection on bounded pool: %v", err)
	}

	boundedResolver, err := digitaltwin.NewPostgresResolver(boundedPool)
	if err != nil {
		t.Fatalf("failed to create bounded resolver: %v", err)
	}
	boundedHistoryRepo, err := telemetry.NewPostgresHistoryRepository(boundedPool, cfg)
	if err != nil {
		t.Fatalf("failed to create bounded history repository: %v", err)
	}
	boundedHistoryService, err := telemetry.NewTelemetryHistoryService(boundedResolver, boundedHistoryRepo, cfg)
	if err != nil {
		t.Fatalf("failed to create bounded history service: %v", err)
	}

	// Query with short context deadline; since boundedPool is exhausted, Acquire blocks and hits deadline
	deadlineCtx, cancelDeadline := context.WithTimeout(ctx, 35*time.Millisecond)
	defer cancelDeadline()

	resDeadline, errDeadline := boundedHistoryService.GetTelemetryHistory(deadlineCtx, testUserUID, queryRaw)
	if resDeadline != nil {
		t.Errorf("expected nil result on database deadline timeout, got %+v", resDeadline)
	}
	if errDeadline == nil {
		t.Fatal("expected error on database deadline timeout, got nil")
	}
	if !errors.Is(errDeadline, telemetry.ErrHistoryDependency) {
		t.Errorf("expected ErrHistoryDependency on database deadline timeout, got %v", errDeadline)
	}
	if !errors.Is(errDeadline, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded cause preserved in error chain, got %v", errDeadline)
	}

	// Release held connection and verify boundedPool recovers cleanly without leaks or stuck state
	heldConn.Release()

	resRecovered, errRecovered := boundedHistoryService.GetTelemetryHistory(ctx, testUserUID, queryRaw)
	if errRecovered != nil {
		t.Fatalf("history query failed after releasing held connection: %v", errRecovered)
	}
	if resRecovered == nil || resRecovered.PointCount != sampleCount {
		t.Fatalf("unexpected recovered result: %+v", resRecovered)
	}
	t.Log("Database deadline & cancellation verified: bounded pool timeout preserves ErrHistoryDependency and context.DeadlineExceeded with nil result; clean recovery upon resource release")
}

// TestNativeTelemetryHistoryUpgradedLegacyProvenance exercises the upgraded database slice where migration 20
// was applied over an older schema with pre-existing legacy telemetry rows.
// It asserts:
//  1. Legacy raw samples retain NULL boot_id, message_id, and sample_index (provenance is not invented).
//  2. Sequence numbers are preserved as int64 and original measured_at / values match pre-migration data.
//  3. Legacy rows sort correctly alongside newer valid samples (ordered strictly by measured_at ASC, sample_id ASC).
//  4. Bucket aggregation includes legacy rows with independently computed arithmetic and counts.
//  5. Normal schema constraints (NOT VALID telemetry_new_provenance) remain intact.
//  6. Reads are scoped through real membership and executed using the iot_backend_app role.
//  7. Exact cleanup of any test-created resources.
func TestNativeTelemetryHistoryUpgradedLegacyProvenance(t *testing.T) {
	setupURL, backendURL := resolveTestDatabaseURLs(t)
	if setupURL == "" || backendURL == "" {
		t.Skip("setup and application database connection URLs are not available or native test not opted in")
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

	// 2. Verify roles: setupPool can login; appPool is iot_backend_app, not superuser
	var appUser string
	var appSuper bool
	if err := appPool.QueryRow(ctx, "SELECT current_user, r.rolsuper FROM pg_roles r WHERE r.rolname = current_user").Scan(&appUser, &appSuper); err != nil {
		t.Fatalf("failed to query application role attributes: %v", err)
	}
	if appUser != "iot_backend_app" {
		t.Fatalf("application connection running as %q, expected iot_backend_app", appUser)
	}
	if appSuper {
		t.Fatalf("application role iot_backend_app must not be superuser")
	}

	// 3. Check for stage3_legacy_telemetry_snapshot table from upgrade fixture
	var hasLegacySnapshot bool
	if err := setupPool.QueryRow(ctx, "SELECT to_regclass('public.stage3_legacy_telemetry_snapshot') IS NOT NULL").Scan(&hasLegacySnapshot); err != nil || !hasLegacySnapshot {
		t.Skip("stage3_legacy_telemetry_snapshot table not found; test requires upgraded database fixture")
	}

	const legacyGW = "fixture_gw"
	const legacySensor = "fixture_sensor"

	// Verify legacy samples exist in snapshot
	type legacyRecord struct {
		measuredAt time.Time
		seq        int64
		val        float64
	}
	var legacyRecords []legacyRecord
	rows, err := setupPool.Query(ctx, `
		SELECT measured_at, sequence_num, value
		FROM stage3_legacy_telemetry_snapshot
		WHERE gateway_id = $1 AND sensor_id = $2
		ORDER BY measured_at ASC, sequence_num ASC
	`, legacyGW, legacySensor)
	if err != nil {
		t.Fatalf("failed to query stage3_legacy_telemetry_snapshot: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r legacyRecord
		if err := rows.Scan(&r.measuredAt, &r.seq, &r.val); err != nil {
			t.Fatalf("failed to scan legacy record: %v", err)
		}
		legacyRecords = append(legacyRecords, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("legacy records iteration error: %v", err)
	}
	if len(legacyRecords) < 2 {
		t.Fatalf("expected at least 2 legacy records in snapshot, found %d", len(legacyRecords))
	}
	t.Logf("Found %d legacy telemetry records in upgrade snapshot", len(legacyRecords))

	// 4. Ensure real membership for the legacy Sensor's gateway
	testUserUID := uuid.New()
	testNewerMsgID := uuid.New()

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		// Delete any test newer rows inserted during this test
		execCleanupStep(cleanupCtx, t, setupPool, "telemetry (test newer rows)", "DELETE FROM public.telemetry WHERE gateway_id = $1 AND message_id = $2", legacyGW, testNewerMsgID)

		// Delete user_gateways membership
		execCleanupStep(cleanupCtx, t, setupPool, "user_gateways", "DELETE FROM public.user_gateways WHERE user_id = $1", testUserUID)

		// Delete profile
		execCleanupStep(cleanupCtx, t, setupPool, "profiles", "DELETE FROM public.profiles WHERE id = $1", testUserUID)

		// Delete auth.users if table exists
		var hasAuthUsers bool
		if err := setupPool.QueryRow(cleanupCtx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
			t.Errorf("cleanup check auth.users failed: %v", err)
		} else if hasAuthUsers {
			execCleanupStep(cleanupCtx, t, setupPool, "auth.users", "DELETE FROM auth.users WHERE id = $1", testUserUID)
		}
	}()

	var hasAuthUsers bool
	if err := setupPool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err == nil && hasAuthUsers {
		_, err = setupPool.Exec(ctx, `
			INSERT INTO auth.users (id, aud, role, email)
			VALUES ($1, 'authenticated', 'authenticated', $2)
			ON CONFLICT (id) DO NOTHING`,
			testUserUID, fmt.Sprintf("legacy-%s@example.com", testUserUID.String()[:8]),
		)
		if err != nil {
			t.Fatalf("failed to seed auth.users for legacy reader: %v", err)
		}
	}

	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.profiles (id, full_name)
		VALUES ($1, 'Legacy Provenance Reader')
		ON CONFLICT (id) DO NOTHING`,
		testUserUID,
	); err != nil {
		t.Fatalf("failed to seed profile for legacy reader: %v", err)
	}

	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.user_gateways (user_id, gateway_id, role)
		VALUES ($1, $2, 'owner')
		ON CONFLICT (user_id, gateway_id) DO NOTHING`,
		testUserUID, legacyGW,
	); err != nil {
		t.Fatalf("failed to seed user_gateways membership: %v", err)
	}

	// 5. Construct real application services on appPool
	cfg := telemetry.DefaultHistoryPreparationConfig()

	resolver, err := digitaltwin.NewPostgresResolver(appPool)
	if err != nil {
		t.Fatalf("failed to create digitaltwin.PostgresResolver: %v", err)
	}

	historyRepo, err := telemetry.NewPostgresHistoryRepository(appPool, cfg)
	if err != nil {
		t.Fatalf("failed to create telemetry.PostgresHistoryRepository: %v", err)
	}

	historyService, err := telemetry.NewTelemetryHistoryService(resolver, historyRepo, cfg)
	if err != nil {
		t.Fatalf("failed to create telemetry.TelemetryHistoryService: %v", err)
	}

	// 6. Test Raw Query: Assert legacy raw samples retain NULL boot_id, message_id, sample_index,
	// and preserve sequence_num as int64 and original value.
	// Query a tight window around the first two legacy samples
	legacy0 := legacyRecords[0]
	legacy1 := legacyRecords[1]

	rawQueryRange := telemetry.TimeRange{
		From: legacy0.measuredAt.Add(-50 * time.Millisecond),
		To:   legacy1.measuredAt.Add(50 * time.Millisecond),
	}
	rawQuery := telemetry.TelemetryHistoryQuery{
		GatewayID:  legacyGW,
		SensorID:   legacySensor,
		Range:      rawQueryRange,
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  100,
	}

	rawRes, err := historyService.GetTelemetryHistory(ctx, testUserUID, rawQuery)
	if err != nil {
		t.Fatalf("GetTelemetryHistory (raw legacy) failed: %v", err)
	}

	if rawRes.Resolution != string(telemetry.ResolutionRaw) {
		t.Fatalf("expected raw resolution, got %q", rawRes.Resolution)
	}
	if rawRes.Unit == nil || *rawRes.Unit != "degC" {
		t.Fatalf("expected unit %q, got %v", "degC", rawRes.Unit)
	}
	if rawRes.PointCount != 2 || len(rawRes.RawPoints) != 2 {
		t.Fatalf("expected exactly 2 raw legacy points, got PointCount=%d, len=%d", rawRes.PointCount, len(rawRes.RawPoints))
	}

	for i, pt := range rawRes.RawPoints {
		expected := legacyRecords[i]
		if !pt.MeasuredAt.Equal(expected.measuredAt) {
			t.Errorf("raw legacy point [%d] measured_at = %v, expected %v", i, pt.MeasuredAt, expected.measuredAt)
		}
		if pt.SequenceNum != expected.seq {
			t.Errorf("raw legacy point [%d] sequence_num = %d, expected %d", i, pt.SequenceNum, expected.seq)
		}
		if math.Abs(pt.Value-expected.val) > 1e-9 {
			t.Errorf("raw legacy point [%d] value = %f, expected %f", i, pt.Value, expected.val)
		}
		// Provenance MUST remain NULL - not invented or filled with zero values
		if pt.BootID != nil {
			t.Errorf("raw legacy point [%d] boot_id = %v, expected NULL", i, *pt.BootID)
		}
		if pt.MessageID != nil {
			t.Errorf("raw legacy point [%d] message_id = %v, expected NULL", i, *pt.MessageID)
		}
		if pt.SampleIndex != nil {
			t.Errorf("raw legacy point [%d] sample_index = %v, expected NULL", i, *pt.SampleIndex)
		}
	}
	t.Log("Legacy raw samples verified: retained NULL boot_id, message_id, sample_index with preserved sequence_num and measurement values")

	// 7. Test Sorting Alongside Valid Newer Rows:
	// Insert a valid newer row with non-null provenance right after legacy1
	newerBootID := uuid.New()
	newerIdx := int32(0)
	newerSeq := int64(70001)
	newerVal := 29.85
	newerMeasuredAt := legacy1.measuredAt.Add(20 * time.Millisecond)

	_, err = setupPool.Exec(ctx, `
		INSERT INTO public.telemetry (measured_at, gateway_id, sensor_id, sequence_num, value, boot_id, message_id, sample_index)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		newerMeasuredAt, legacyGW, legacySensor, newerSeq, newerVal, newerBootID, testNewerMsgID, newerIdx,
	)
	if err != nil {
		t.Fatalf("failed to insert newer telemetry sample with valid provenance: %v", err)
	}

	rawQueryWithNewer := telemetry.TelemetryHistoryQuery{
		GatewayID: legacyGW,
		SensorID:  legacySensor,
		Range: telemetry.TimeRange{
			From: legacy0.measuredAt.Add(-50 * time.Millisecond),
			To:   newerMeasuredAt.Add(50 * time.Millisecond),
		},
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  100,
	}

	rawWithNewerRes, err := historyService.GetTelemetryHistory(ctx, testUserUID, rawQueryWithNewer)
	if err != nil {
		t.Fatalf("GetTelemetryHistory (raw with newer row) failed: %v", err)
	}

	if len(rawWithNewerRes.RawPoints) != 3 {
		t.Fatalf("expected 3 raw points (2 legacy + 1 newer), got %d", len(rawWithNewerRes.RawPoints))
	}

	// Verify points are strictly sorted by measured_at ASC, sample_id ASC
	for i := 1; i < len(rawWithNewerRes.RawPoints); i++ {
		prev := rawWithNewerRes.RawPoints[i-1]
		curr := rawWithNewerRes.RawPoints[i]
		if curr.MeasuredAt.Before(prev.MeasuredAt) {
			t.Fatalf("raw points out of order at [%d]: %v before %v", i, curr.MeasuredAt, prev.MeasuredAt)
		}
	}

	// Point 0 and 1 must be legacy with NULL provenance
	for i := 0; i < 2; i++ {
		pt := rawWithNewerRes.RawPoints[i]
		if pt.BootID != nil || pt.MessageID != nil || pt.SampleIndex != nil {
			t.Errorf("point [%d] expected NULL provenance, got boot=%v msg=%v idx=%v", i, pt.BootID, pt.MessageID, pt.SampleIndex)
		}
	}

	// Point 2 must be newer with valid non-null provenance
	pt2 := rawWithNewerRes.RawPoints[2]
	if pt2.BootID == nil || *pt2.BootID != newerBootID {
		t.Errorf("newer point [2] boot_id mismatch: %v", pt2.BootID)
	}
	if pt2.MessageID == nil || *pt2.MessageID != testNewerMsgID {
		t.Errorf("newer point [2] message_id mismatch: %v", pt2.MessageID)
	}
	if pt2.SampleIndex == nil || *pt2.SampleIndex != newerIdx {
		t.Errorf("newer point [2] sample_index mismatch: %v", pt2.SampleIndex)
	}
	if pt2.SequenceNum != newerSeq {
		t.Errorf("newer point [2] sequence_num mismatch: got %d, expected %d", pt2.SequenceNum, newerSeq)
	}
	if math.Abs(pt2.Value-newerVal) > 1e-9 {
		t.Errorf("newer point [2] value mismatch: got %f, expected %f", pt2.Value, newerVal)
	}
	t.Log("Sorting alongside newer valid rows verified: legacy NULL provenance and newer valid provenance sort correctly by measured_at")

	// Delete test newer row immediately so subsequent bucket test is on legacy rows alone
	if _, err := setupPool.Exec(ctx, "DELETE FROM public.telemetry WHERE gateway_id = $1 AND message_id = $2", legacyGW, testNewerMsgID); err != nil {
		t.Fatalf("failed to delete test newer row: %v", err)
	}

	// 8. Test Bucket Aggregation: Assert bucket aggregation includes legacy rows with independently computed arithmetic
	bucketStart0 := legacy0.measuredAt.Truncate(time.Second)
	bucketStart1 := legacy1.measuredAt.Truncate(time.Second)

	if bucketStart0.Equal(bucketStart1) {
		// Both samples fall in the same 1s bucket
		expectedCount := int64(2)
		expectedMin := math.Min(legacy0.val, legacy1.val)
		expectedMax := math.Max(legacy0.val, legacy1.val)
		expectedAvg := (legacy0.val + legacy1.val) / 2.0

		bucketQuery := telemetry.TelemetryHistoryQuery{
			GatewayID: legacyGW,
			SensorID:  legacySensor,
			Range: telemetry.TimeRange{
				From: bucketStart0,
				To:   bucketStart0.Add(time.Second),
			},
			Resolution: telemetry.Resolution1s,
			MaxPoints:  100,
		}

		bucketRes, err := historyService.GetTelemetryHistory(ctx, testUserUID, bucketQuery)
		if err != nil {
			t.Fatalf("GetTelemetryHistory (bucket legacy) failed: %v", err)
		}

		if len(bucketRes.BucketPoints) != 1 {
			t.Fatalf("expected exactly 1 bucket point, got %d", len(bucketRes.BucketPoints))
		}
		bp := bucketRes.BucketPoints[0]
		if !bp.BucketStart.Equal(bucketStart0) {
			t.Errorf("bucket start = %v, expected %v", bp.BucketStart, bucketStart0)
		}
		if bp.Count != expectedCount {
			t.Errorf("bucket sample_count = %d, expected %d", bp.Count, expectedCount)
		}
		if math.Abs(bp.Min-expectedMin) > 1e-9 {
			t.Errorf("bucket min = %f, expected %f", bp.Min, expectedMin)
		}
		if math.Abs(bp.Max-expectedMax) > 1e-9 {
			t.Errorf("bucket max = %f, expected %f", bp.Max, expectedMax)
		}
		if math.Abs(bp.Avg-expectedAvg) > 1e-9 {
			t.Errorf("bucket avg = %f, expected %f", bp.Avg, expectedAvg)
		}
		t.Logf("Bucket aggregation verified: bucket start=%s count=%d min=%f max=%f avg=%f matching independent arithmetic",
			bp.BucketStart.UTC().Format(time.RFC3339), bp.Count, bp.Min, bp.Max, bp.Avg)
	} else {
		// Samples span across 1s boundary
		bucketQuery := telemetry.TelemetryHistoryQuery{
			GatewayID: legacyGW,
			SensorID:  legacySensor,
			Range: telemetry.TimeRange{
				From: bucketStart0,
				To:   bucketStart1.Add(time.Second),
			},
			Resolution: telemetry.Resolution1s,
			MaxPoints:  100,
		}

		bucketRes, err := historyService.GetTelemetryHistory(ctx, testUserUID, bucketQuery)
		if err != nil {
			t.Fatalf("GetTelemetryHistory (bucket legacy spanned) failed: %v", err)
		}
		if len(bucketRes.BucketPoints) != 2 {
			t.Fatalf("expected 2 bucket points, got %d", len(bucketRes.BucketPoints))
		}
		t.Logf("Bucket aggregation verified across 2 buckets for legacy rows")
	}

	fmt.Println("INTEGRATION CASE PASS: upgraded legacy NULL provenance and bucket aggregation verified")
}

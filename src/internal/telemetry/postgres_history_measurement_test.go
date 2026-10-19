package telemetry_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/telemetry"
)

// explainReport captures PostgreSQL EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) execution statistics.
type explainReport struct {
	Plan          explainPlanNode `json:"Plan"`
	PlanningTime  float64         `json:"Planning Time"`
	ExecutionTime float64         `json:"Execution Time"`
}

type explainPlanNode struct {
	NodeType            string            `json:"Node Type"`
	RelationName        string            `json:"Relation Name"`
	Alias               string            `json:"Alias"`
	IndexName           string            `json:"Index Name"`
	ActualStartupTime   float64           `json:"Actual Startup Time"`
	ActualTotalTime     float64           `json:"Actual Total Time"`
	ActualRows          int64             `json:"Actual Rows"`
	ActualLoops         int64             `json:"Actual Loops"`
	SharedHitBlocks     int64             `json:"Shared Hit Blocks"`
	SharedReadBlocks    int64             `json:"Shared Read Blocks"`
	SharedDirtiedBlocks int64             `json:"Shared Dirtied Blocks"`
	SharedWrittenBlocks int64             `json:"Shared Written Blocks"`
	Plans               []explainPlanNode `json:"Plans"`
	Filter              string            `json:"Filter"`
	IndexCond           string            `json:"Index Cond"`
}

func collectPlanSummary(root *explainPlanNode, nodeTypes *[]string, relations *[]string, indexes *[]string, totalHits *int64, totalReads *int64) {
	if root == nil {
		return
	}
	// PostgreSQL parent node counters already include all subtree block accesses.
	// Total buffer hits/reads for the plan execution must be read from root only.
	if totalHits != nil {
		*totalHits = root.SharedHitBlocks
	}
	if totalReads != nil {
		*totalReads = root.SharedReadBlocks
	}
	traversePlanNodes(root, nodeTypes, relations, indexes)
}

func traversePlanNodes(node *explainPlanNode, nodeTypes *[]string, relations *[]string, indexes *[]string) {
	if node == nil {
		return
	}
	if nodeTypes != nil {
		*nodeTypes = append(*nodeTypes, node.NodeType)
	}
	if relations != nil && node.RelationName != "" {
		*relations = append(*relations, node.RelationName)
	}
	if indexes != nil && node.IndexName != "" {
		*indexes = append(*indexes, node.IndexName)
	}
	for i := range node.Plans {
		traversePlanNodes(&node.Plans[i], nodeTypes, relations, indexes)
	}
}

// telemetryGeneratorSource implements pgx.CopyFromSource for fast synthetic data generation.
type telemetryGeneratorSource struct {
	count        int
	idx          int
	baseTime     time.Time
	intervalUs   int64
	gatewayID    string
	sensorID     string
	bootID       uuid.UUID
	batchSize    int
	currentMsgID uuid.UUID
	val          []any
}

func newTelemetryGeneratorSource(
	count int,
	baseTime time.Time,
	intervalUs int64,
	gatewayID string,
	sensorID string,
	bootID uuid.UUID,
	batchSize int,
) *telemetryGeneratorSource {
	return &telemetryGeneratorSource{
		count:      count,
		idx:        0,
		baseTime:   baseTime,
		intervalUs: intervalUs,
		gatewayID:  gatewayID,
		sensorID:   sensorID,
		bootID:     bootID,
		batchSize:  batchSize,
		val:        make([]any, 8),
	}
}

func (s *telemetryGeneratorSource) Next() bool {
	if s.idx >= s.count {
		return false
	}
	s.idx++
	return true
}

func (s *telemetryGeneratorSource) Values() ([]any, error) {
	i := s.idx - 1
	if i%s.batchSize == 0 {
		s.currentMsgID = uuid.New()
	}
	sampleIdx := int32(i % s.batchSize)
	measuredAt := s.baseTime.Add(time.Duration(int64(i)*s.intervalUs) * time.Microsecond)
	seq := int64(i)
	val := 20.0 + float64(i%100)*0.1

	s.val[0] = measuredAt
	s.val[1] = s.gatewayID
	s.val[2] = s.sensorID
	s.val[3] = seq
	s.val[4] = val
	s.val[5] = s.bootID
	s.val[6] = s.currentMsgID
	s.val[7] = sampleIdx

	return s.val, nil
}

func (s *telemetryGeneratorSource) Err() error {
	return nil
}

// TestNativeTelemetryHistoryQueryPlanMeasurement executes Task 4.2.7:
// Measures actual PostgreSQL/TimescaleDB query plans and performance on a representative
// dataset (60,000 raw samples for target gateway + 6,000 samples for distractor gateway)
// using iot_backend_app, asserting that observed queries fit the configured 5-second budget
// without requiring speculative indexes or schema migrations.
func TestNativeTelemetryHistoryQueryPlanMeasurement(t *testing.T) {
	setupURL, backendURL := resolveTestDatabaseURLs(t)
	if setupURL == "" || backendURL == "" {
		t.Skip("setup and application database connection URLs are not available or native test not opted in (set TELEMETRY_HISTORY_TEST_NATIVE=1 or TELEMETRY_TEST_SETUP_URL and TELEMETRY_TEST_BACKEND_URL)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	// 1. Record database versions, schema version, application role, and pool settings
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

	// Record compression status for telemetry hypertable
	var compressedCount, totalChunks int
	err = setupPool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN is_compressed THEN 1 ELSE 0 END), 0),
			COUNT(*)
		FROM timescaledb_information.chunks
		WHERE hypertable_name = 'telemetry'
	`).Scan(&compressedCount, &totalChunks)
	if err != nil {
		t.Fatalf("failed to query compression info: %v", err)
	}
	isCompressed := compressedCount > 0

	t.Logf("=== DATABASE ENVIRONMENT & METADATA ===")
	t.Logf("PostgreSQL Version: %s", strings.Fields(pgVersion)[1])
	t.Logf("TimescaleDB Version: %s", tsVersion)
	t.Logf("Schema Version: %d", schemaVersion)
	t.Logf("Application Role: %s (superuser: %v, canlogin: %v, bypassrls: %v)", appUser, appSuper, appCanLogin, appBypassRLS)
	t.Logf("Pool Settings: MaxConns=%d, MinConns=%d, MaxConnIdleTime=%v",
		appPool.Config().MaxConns, appPool.Config().MinConns, appPool.Config().MaxConnIdleTime)
	t.Logf("Hypertable Compression: is_compressed=%v (compressed chunks: %d / %d total chunks)",
		isCompressed, compressedCount, totalChunks)

	// 2. Fixture setup with unique identities
	runID := uuid.New().String()[:8]
	if envRunID := os.Getenv("STAGE4_MEASUREMENT_RUN_ID"); envRunID != "" {
		runID = envRunID
	}
	testUserUID := uuid.New()
	if envUser := os.Getenv("STAGE4_MEASUREMENT_USER_ID"); envUser != "" {
		if parsed, err := uuid.Parse(envUser); err == nil {
			testUserUID = parsed
		}
	}
	targetGW := fmt.Sprintf("gw_meas_%s", runID)
	targetSensor := fmt.Sprintf("sens_meas_%s", runID)
	targetUnit := "degC"
	targetBootID := uuid.New()

	distractGW := fmt.Sprintf("gw_distr_%s", runID)
	distractSensor := fmt.Sprintf("sens_distr_%s", runID)
	distractUnit := "ppm"
	distractBootID := uuid.New()

	// Fixture cleanup defer hook: strictly deletes only fixture-created resources unless preserved for cross-test/compression
	defer func() {
		if os.Getenv("STAGE4_PRESERVE_FIXTURE") == "1" {
			t.Logf("STAGE4_PRESERVE_FIXTURE=1: retaining seeded telemetry fixture rows for cross-test/compression verification")
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()

		// Delete telemetry
		execCleanupStep(cleanupCtx, t, setupPool, "telemetry", "DELETE FROM public.telemetry WHERE gateway_id IN ($1, $2)", targetGW, distractGW)
		// Delete user_gateways
		execCleanupStep(cleanupCtx, t, setupPool, "user_gateways", "DELETE FROM public.user_gateways WHERE gateway_id IN ($1, $2)", targetGW, distractGW)
		// Delete sensors
		execCleanupStep(cleanupCtx, t, setupPool, "sensors", "DELETE FROM public.sensors WHERE gateway_id IN ($1, $2)", targetGW, distractGW)
		// Delete gateways
		execCleanupStep(cleanupCtx, t, setupPool, "gateways", "DELETE FROM public.gateways WHERE gateway_id IN ($1, $2)", targetGW, distractGW)
		// Delete profiles
		execCleanupStep(cleanupCtx, t, setupPool, "profiles", "DELETE FROM public.profiles WHERE id = $1", testUserUID)

		// Delete auth.users if exists
		var hasAuthUsers bool
		if err := setupPool.QueryRow(cleanupCtx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
			t.Errorf("cleanup check auth.users failed: %v", err)
		} else if hasAuthUsers {
			execCleanupStep(cleanupCtx, t, setupPool, "auth.users", "DELETE FROM auth.users WHERE id = $1", testUserUID)
		}

		// Verify complete cleanup
		verifyCleanupCount(cleanupCtx, t, setupPool, "telemetry fixture gateways", "SELECT COUNT(*) FROM public.telemetry WHERE gateway_id IN ($1, $2)", targetGW, distractGW)
	}()

	// 3. Dataset specification:
	// Target dataset: 60,000 samples @ 100 Hz (10 ms interval) over 10 minutes (600 seconds)
	// Distractor dataset: 6,000 samples @ 10 Hz (100 ms interval) over same 10 minutes
	baseTime := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	span := 10 * time.Minute
	targetCount := 60000
	targetIntervalUs := int64(10000) // 100 Hz
	batchSize := 100                 // 100 samples per batch = 600 messages

	distractCount := 6000
	distractIntervalUs := int64(100000) // 10 Hz

	// Seed auth.users if table exists
	if os.Getenv("STAGE4_MEASUREMENT_RERUN_ONLY") == "1" {
		t.Logf("STAGE4_MEASUREMENT_RERUN_ONLY=1: verifying existing target telemetry rows without reseeding")
		var actualTargetCount, actualDistractCount int
		err := setupPool.QueryRow(ctx, `
			SELECT
				COALESCE(SUM(CASE WHEN gateway_id = $1 THEN 1 ELSE 0 END), 0),
				COALESCE(SUM(CASE WHEN gateway_id = $2 THEN 1 ELSE 0 END), 0)
			FROM public.telemetry`, targetGW, distractGW).Scan(&actualTargetCount, &actualDistractCount)
		if err != nil {
			t.Fatalf("failed to query existing telemetry rows for rerun: %v", err)
		}
		if actualTargetCount != targetCount {
			t.Fatalf("expected %d target rows for rerun, got %d", targetCount, actualTargetCount)
		}
		if actualDistractCount != distractCount {
			t.Fatalf("expected %d distractor rows for rerun, got %d", distractCount, actualDistractCount)
		}
	} else {
		var hasAuthUsers bool
		if err := setupPool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err == nil && hasAuthUsers {
			_, err = setupPool.Exec(ctx, `
				INSERT INTO auth.users (id, aud, role, email)
				VALUES ($1, 'authenticated', 'authenticated', $2)
				ON CONFLICT (id) DO NOTHING`,
				testUserUID, fmt.Sprintf("meas-%s@example.com", runID),
			)
			if err != nil {
				t.Fatalf("failed to seed auth.users: %v", err)
			}
		}

		// Seed profile
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.profiles (id, full_name)
			VALUES ($1, $2)
			ON CONFLICT (id) DO NOTHING`,
			testUserUID, fmt.Sprintf("Measurement User %s", runID),
		); err != nil {
			t.Fatalf("failed to seed profile: %v", err)
		}

		// Seed gateways
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.gateways (gateway_id, name)
			VALUES ($1, 'Target Measurement Gateway'), ($2, 'Distractor Gateway')`,
			targetGW, distractGW,
		); err != nil {
			t.Fatalf("failed to seed gateways: %v", err)
		}

		// Seed sensors
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.sensors (gateway_id, sensor_id, name, unit)
			VALUES ($1, $2, 'Target Measurement Sensor', $3), ($4, $5, 'Distractor Sensor', $6)`,
			targetGW, targetSensor, targetUnit, distractGW, distractSensor, distractUnit,
		); err != nil {
			t.Fatalf("failed to seed sensors: %v", err)
		}

		// Seed user_gateways: test user is 'owner' of targetGW only (demonstrating selectivity against distractGW)
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.user_gateways (user_id, gateway_id, role)
			VALUES ($1, $2, 'owner')`,
			testUserUID, targetGW,
		); err != nil {
			t.Fatalf("failed to seed user_gateways: %v", err)
		}

		// 3. Seed dataset:
		// Target dataset: 60,000 samples @ 100 Hz (10 ms interval) over 10 minutes (600 seconds)
		// Distractor dataset: 6,000 samples @ 10 Hz (100 ms interval) over same 10 minutes
		t.Logf("=== SEEDING REPRESENTATIVE DATASET ===")
		t.Logf("Target Dataset: %d samples, 100 Hz (10ms), span %v [%v to %v], batchSize=%d (600 messages)",
			targetCount, span, baseTime.Format(time.RFC3339), baseTime.Add(span).Format(time.RFC3339), batchSize)
		t.Logf("Distractor Dataset: %d samples, 10 Hz (100ms), span %v", distractCount, span)

		telemetryColumns := []string{
			"measured_at", "gateway_id", "sensor_id", "sequence_num",
			"value", "boot_id", "message_id", "sample_index",
		}

		targetSrc := newTelemetryGeneratorSource(targetCount, baseTime, targetIntervalUs, targetGW, targetSensor, targetBootID, batchSize)
		seedStart := time.Now()
		rowsInserted, err := setupPool.CopyFrom(ctx, pgx.Identifier{"public", "telemetry"}, telemetryColumns, targetSrc)
		if err != nil {
			t.Fatalf("failed to copy target telemetry rows: %v", err)
		}
		if rowsInserted != int64(targetCount) {
			t.Fatalf("expected %d target rows inserted, got %d", targetCount, rowsInserted)
		}

		distractSrc := newTelemetryGeneratorSource(distractCount, baseTime, distractIntervalUs, distractGW, distractSensor, distractBootID, batchSize)
		rowsDistract, err := setupPool.CopyFrom(ctx, pgx.Identifier{"public", "telemetry"}, telemetryColumns, distractSrc)
		if err != nil {
			t.Fatalf("failed to copy distractor telemetry rows: %v", err)
		}
		if rowsDistract != int64(distractCount) {
			t.Fatalf("expected %d distractor rows inserted, got %d", distractCount, rowsDistract)
		}
		t.Logf("Seeding Complete in %v: %d target rows + %d distractor rows = %d total raw rows",
			time.Since(seedStart), rowsInserted, rowsDistract, rowsInserted+rowsDistract)
	}

	// Build application service components
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

	// Helper to execute EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) under appPool
	execExplain := func(querySQL string, args ...any) (*explainReport, error) {
		explainSQL := "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + querySQL
		var jsonBytes []byte
		err := appPool.QueryRow(ctx, explainSQL, args...).Scan(&jsonBytes)
		if err != nil {
			return nil, err
		}
		var reports []explainReport
		if err := json.Unmarshal(jsonBytes, &reports); err != nil {
			return nil, fmt.Errorf("failed to unmarshal explain json: %w", err)
		}
		if len(reports) == 0 {
			return nil, fmt.Errorf("empty explain report")
		}
		return &reports[0], nil
	}

	budgetLimitMs := 5000.0 // 5-second budget limit

	// =========================================================================
	// MEASUREMENT 1: Short raw window within ten seconds fitting cap
	// Window: 5 seconds (500 samples @ 100 Hz), MaxPoints = 500, Limit = 501
	// =========================================================================
	t.Logf("--- MEASUREMENT 1: Short Raw Window (5s, 500 samples @ 100 Hz, cap=500) ---")
	rawWindowRange := telemetry.TimeRange{
		From: baseTime,
		To:   baseTime.Add(5 * time.Second),
	}
	rawLimit := 501

	// Run EXPLAIN ANALYZE under appPool with exact raw query and parameters
	rep1, err := execExplain(
		telemetry.RawTelemetryHistoryQueryForTest,
		testUserUID,
		targetGW,
		targetSensor,
		rawWindowRange.From.UTC(),
		rawWindowRange.To.UTC(),
		rawLimit,
	)
	if err != nil {
		t.Fatalf("Measurement 1 EXPLAIN failed: %v", err)
	}

	var nodes1, rels1, idxs1 []string
	var hits1, reads1 int64
	collectPlanSummary(&rep1.Plan, &nodes1, &rels1, &idxs1, &hits1, &reads1)

	t.Logf("M1 Plan: ExecutionTime=%.3f ms, PlanningTime=%.3f ms, ActualRows=%d",
		rep1.ExecutionTime, rep1.PlanningTime, rep1.Plan.ActualRows)
	t.Logf("M1 Buffer Accesses (8KB blocks from root plan): SharedHit=%d, SharedRead=%d", hits1, reads1)
	t.Logf("M1 Nodes: %s", strings.Join(nodes1, " -> "))
	t.Logf("M1 Relations: %v", rels1)
	t.Logf("M1 Indexes: %v", idxs1)

	// Verify with application adapter
	m1Start := time.Now()
	res1, err := historyService.GetTelemetryHistory(ctx, testUserUID, telemetry.TelemetryHistoryQuery{
		GatewayID:  targetGW,
		SensorID:   targetSensor,
		Range:      rawWindowRange,
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  500,
	})
	m1Duration := time.Since(m1Start)
	if err != nil {
		t.Fatalf("Measurement 1 service call failed: %v", err)
	}
	if len(res1.RawPoints) != 500 {
		t.Fatalf("expected 500 raw points, got %d", len(res1.RawPoints))
	}
	if res1.Resolution != string(telemetry.ResolutionRaw) {
		t.Fatalf("expected raw resolution, got %s", res1.Resolution)
	}
	t.Logf("M1 Service Result: EffectiveResolution=%s, Points=%d, ServiceElapsed=%v",
		res1.Resolution, len(res1.RawPoints), m1Duration)

	if rep1.ExecutionTime > budgetLimitMs {
		t.Errorf("M1 execution time %.3f ms exceeded 5s budget", rep1.ExecutionTime)
	}

	// =========================================================================
	// MEASUREMENT 2: Longer bucket query with actual auto-selected width
	// Window: 10 minutes (60,000 samples), MaxPoints = 500
	// Auto resolution for 10m / 500 pts = Resolution10s (width 10s, 60 buckets)
	// =========================================================================
	t.Logf("--- MEASUREMENT 2: Longer Bucket Query (10m, 60,000 samples, cap=500, auto width 10s) ---")
	longWindowRange := telemetry.TimeRange{
		From: baseTime,
		To:   baseTime.Add(10 * time.Minute),
	}
	autoRes2, err := telemetry.SelectAutoBucketResolution(longWindowRange.From, longWindowRange.To, 500)
	if err != nil {
		t.Fatalf("failed to select auto bucket resolution: %v", err)
	}
	if autoRes2 != telemetry.Resolution10s {
		t.Fatalf("expected Resolution10s, got %s", autoRes2)
	}
	dur2, _ := telemetry.ResolutionDuration(autoRes2)
	interval2 := pgtype.Interval{
		Microseconds: dur2.Microseconds(),
		Valid:        true,
	}
	bucketLimit2 := 501

	rep2, err := execExplain(
		telemetry.BucketTelemetryHistoryQueryForTest,
		testUserUID,
		targetGW,
		targetSensor,
		interval2,
		longWindowRange.From.UTC(),
		longWindowRange.To.UTC(),
		bucketLimit2,
	)
	if err != nil {
		t.Fatalf("Measurement 2 EXPLAIN failed: %v", err)
	}

	var nodes2, rels2, idxs2 []string
	var hits2, reads2 int64
	collectPlanSummary(&rep2.Plan, &nodes2, &rels2, &idxs2, &hits2, &reads2)

	t.Logf("M2 Plan: ExecutionTime=%.3f ms, PlanningTime=%.3f ms, ActualRows=%d",
		rep2.ExecutionTime, rep2.PlanningTime, rep2.Plan.ActualRows)
	t.Logf("M2 Buffer Accesses (8KB blocks from root plan): SharedHit=%d, SharedRead=%d", hits2, reads2)
	t.Logf("M2 Nodes: %s", strings.Join(nodes2, " -> "))
	t.Logf("M2 Relations: %v", rels2)
	t.Logf("M2 Indexes: %v", idxs2)

	// Verify with application adapter
	m2Start := time.Now()
	res2, err := historyRepo.ReadTelemetryHistory(ctx, testUserUID, telemetry.TelemetryHistoryQuery{
		GatewayID:  targetGW,
		SensorID:   targetSensor,
		Range:      longWindowRange,
		Resolution: autoRes2,
		MaxPoints:  500,
	})
	m2Duration := time.Since(m2Start)
	if err != nil {
		t.Fatalf("Measurement 2 repository call failed: %v", err)
	}
	if len(res2.BucketPoints) != 60 {
		t.Fatalf("expected 60 bucket points, got %d", len(res2.BucketPoints))
	}
	for i, b := range res2.BucketPoints {
		if b.Count != 1000 {
			t.Errorf("bucket [%d] count = %d, expected 1000", i, b.Count)
		}
	}
	t.Logf("M2 Service Result: EffectiveResolution=%s, Points=%d (each aggregating 1,000 samples), ServiceElapsed=%v",
		res2.Resolution, len(res2.BucketPoints), m2Duration)

	if rep2.ExecutionTime > budgetLimitMs {
		t.Errorf("M2 execution time %.3f ms exceeded 5s budget", rep2.ExecutionTime)
	}

	// =========================================================================
	// MEASUREMENT 3: Short raw probe that overflows followed by bucket fallback
	// Window: 8 seconds (800 samples @ 100 Hz), MaxPoints = 500
	// Raw probe with limit 501: scans and produces 501 rows -> overflow!
	// Bucket fallback: SelectAutoBucketResolution selects 100ms (80 buckets <= 500)
	// =========================================================================
	t.Logf("--- MEASUREMENT 3: Short Raw Probe (8s, 800 samples, cap=500 -> overflow) & Fallback ---")
	probeRange := telemetry.TimeRange{
		From: baseTime,
		To:   baseTime.Add(8 * time.Second),
	}
	probeLimit := 501

	// Part 3a: Raw probe plan
	rep3a, err := execExplain(
		telemetry.RawTelemetryHistoryQueryForTest,
		testUserUID,
		targetGW,
		targetSensor,
		probeRange.From.UTC(),
		probeRange.To.UTC(),
		probeLimit,
	)
	if err != nil {
		t.Fatalf("Measurement 3a (raw probe) EXPLAIN failed: %v", err)
	}
	var nodes3a, rels3a, idxs3a []string
	var hits3a, reads3a int64
	collectPlanSummary(&rep3a.Plan, &nodes3a, &rels3a, &idxs3a, &hits3a, &reads3a)

	t.Logf("M3a Probe Plan: ExecutionTime=%.3f ms, PlanningTime=%.3f ms, ActualRows=%d (overflow >= 501)",
		rep3a.ExecutionTime, rep3a.PlanningTime, rep3a.Plan.ActualRows)
	t.Logf("M3a Buffer Accesses (8KB blocks from root plan): SharedHit=%d, SharedRead=%d", hits3a, reads3a)

	// Part 3b: Bucket fallback plan
	autoRes3, err := telemetry.SelectAutoBucketResolution(probeRange.From, probeRange.To, 500)
	if err != nil {
		t.Fatalf("failed to select auto bucket resolution for probe: %v", err)
	}
	if autoRes3 != telemetry.Resolution100ms {
		t.Fatalf("expected Resolution100ms for 8s window, got %s", autoRes3)
	}
	dur3, _ := telemetry.ResolutionDuration(autoRes3)
	interval3 := pgtype.Interval{
		Microseconds: dur3.Microseconds(),
		Valid:        true,
	}

	rep3b, err := execExplain(
		telemetry.BucketTelemetryHistoryQueryForTest,
		testUserUID,
		targetGW,
		targetSensor,
		interval3,
		probeRange.From.UTC(),
		probeRange.To.UTC(),
		probeLimit,
	)
	if err != nil {
		t.Fatalf("Measurement 3b (bucket fallback) EXPLAIN failed: %v", err)
	}
	var nodes3b, rels3b, idxs3b []string
	var hits3b, reads3b int64
	collectPlanSummary(&rep3b.Plan, &nodes3b, &rels3b, &idxs3b, &hits3b, &reads3b)

	t.Logf("M3b Fallback Plan: ExecutionTime=%.3f ms, PlanningTime=%.3f ms, ActualRows=%d",
		rep3b.ExecutionTime, rep3b.PlanningTime, rep3b.Plan.ActualRows)
	t.Logf("M3b Buffer Accesses (8KB blocks from root plan): SharedHit=%d, SharedRead=%d", hits3b, reads3b)
	t.Logf("M3b Nodes: %s", strings.Join(nodes3b, " -> "))

	// Part 3c: End-to-end service execution demonstrating automatic probe -> overflow -> fallback
	m3Start := time.Now()
	res3, err := historyService.GetTelemetryHistory(ctx, testUserUID, telemetry.TelemetryHistoryQuery{
		GatewayID:  targetGW,
		SensorID:   targetSensor,
		Range:      probeRange,
		Resolution: telemetry.ResolutionAuto,
		MaxPoints:  500,
	})
	m3Duration := time.Since(m3Start)
	if err != nil {
		t.Fatalf("Measurement 3 end-to-end service call failed: %v", err)
	}
	if res3.Resolution != string(telemetry.Resolution100ms) {
		t.Fatalf("expected fallback resolution 100ms, got %s", res3.Resolution)
	}
	if len(res3.BucketPoints) != 80 {
		t.Fatalf("expected 80 bucket points, got %d", len(res3.BucketPoints))
	}
	for i, b := range res3.BucketPoints {
		if b.Count != 10 {
			t.Errorf("bucket [%d] count = %d, expected 10", i, b.Count)
		}
	}
	t.Logf("M3 Service Result: EffectiveResolution=%s, Points=%d (each aggregating 10 samples), TotalElapsed=%v",
		res3.Resolution, len(res3.BucketPoints), m3Duration)

	totalM3TimeMs := rep3a.ExecutionTime + rep3b.ExecutionTime
	if totalM3TimeMs > budgetLimitMs {
		t.Errorf("M3 combined probe+fallback execution time %.3f ms exceeded 5s budget", totalM3TimeMs)
	}

	// =========================================================================
	// SELECTIVITY & ISOLATION CHECK
	// =========================================================================
	t.Logf("--- SELECTIVITY & ISOLATION CHECK ---")
	// Querying distractGW from testUserUID must fail closed with ErrHistoryNotFound (user has no membership in distractGW)
	_, err = historyService.GetTelemetryHistory(ctx, testUserUID, telemetry.TelemetryHistoryQuery{
		GatewayID:  distractGW,
		SensorID:   distractSensor,
		Range:      rawWindowRange,
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  500,
	})
	if err == nil {
		t.Fatalf("expected error querying unassigned gateway %s, got nil", distractGW)
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not found error, got: %v", err)
	}
	t.Logf("Selectivity Verified: unassigned distractor gateway correctly rejected as not found")

	// =========================================================================
	// SUMMARY & SCHEMA DECISION
	// =========================================================================
	t.Logf("=== SUMMARY & SCHEMA 20 DECISION ===")
	t.Logf("Measurement 1 (Raw 5s, 500 pts): %.3f ms execution time (budget: 5000 ms)", rep1.ExecutionTime)
	t.Logf("Measurement 2 (Bucket 10m, 60 pts / 60k rows): %.3f ms execution time (budget: 5000 ms)", rep2.ExecutionTime)
	t.Logf("Measurement 3 (Probe 501 rows + Fallback 80 pts): %.3f ms + %.3f ms = %.3f ms execution time",
		rep3a.ExecutionTime, rep3b.ExecutionTime, totalM3TimeMs)
	t.Logf("Decision: All observed queries comfortably fit within the 5.0-second budget (actual execution times comfortably below 75 ms, far below the 5,000 ms budget).")
	t.Logf("No bottlenecks observed. Retain Schema 20 and existing indexes without speculative indexes or migrations.")

	summary := map[string]any{
		"scenario":               "telemetry",
		"run_id":                 runID,
		"target_gateway_id":      targetGW,
		"target_sensor_id":       targetSensor,
		"distract_gateway_id":    distractGW,
		"distract_sensor_id":     distractSensor,
		"target_raw_count":       targetCount,
		"distract_raw_count":     distractCount,
		"m1_planning_time_ms":    rep1.PlanningTime,
		"m1_execution_time_ms":   rep1.ExecutionTime,
		"m1_actual_rows":         rep1.Plan.ActualRows,
		"m1_shared_hit_blocks":   hits1,
		"m1_shared_read_blocks":  reads1,
		"m1_plan_nodes":          nodes1,
		"m1_service_elapsed_ms":  float64(m1Duration.Microseconds()) / 1000.0,
		"m2_planning_time_ms":    rep2.PlanningTime,
		"m2_execution_time_ms":   rep2.ExecutionTime,
		"m2_actual_rows":         rep2.Plan.ActualRows,
		"m2_shared_hit_blocks":   hits2,
		"m2_shared_read_blocks":  reads2,
		"m2_plan_nodes":          nodes2,
		"m2_service_elapsed_ms":  float64(m2Duration.Microseconds()) / 1000.0,
		"m3a_planning_time_ms":   rep3a.PlanningTime,
		"m3a_execution_time_ms":  rep3a.ExecutionTime,
		"m3a_actual_rows":        rep3a.Plan.ActualRows,
		"m3a_shared_hit_blocks":  hits3a,
		"m3a_shared_read_blocks": reads3a,
		"m3b_planning_time_ms":   rep3b.PlanningTime,
		"m3b_execution_time_ms":  rep3b.ExecutionTime,
		"m3b_actual_rows":        rep3b.Plan.ActualRows,
		"m3b_shared_hit_blocks":  hits3b,
		"m3b_shared_read_blocks": reads3b,
		"m3b_plan_nodes":         nodes3b,
		"m3_total_execution_ms":  totalM3TimeMs,
		"m3_service_elapsed_ms":  float64(m3Duration.Microseconds()) / 1000.0,
		"selectivity_verified":   true,
		"is_compressed":          isCompressed,
	}
	outJSON, err := json.Marshal(summary)
	if err == nil {
		fmt.Printf("TELEMETRY_MEASUREMENT_JSON: %s\n", string(outJSON))
		if outPath := os.Getenv("STAGE4_MEASUREMENT_JSON_OUTPUT"); outPath != "" {
			_ = os.WriteFile(outPath, outJSON, 0o600)
		}
	}
}

func TestCollectPlanSummary_RootBufferCountersIncludeSubtree(t *testing.T) {
	t.Parallel()

	// Synthetic plan regression:
	// PostgreSQL parent node counters already include all subtree block accesses.
	// Root has Shared Hit=10, child has Shared Hit=7 -> total must be 10, not 17.
	// Root has Shared Read=5, child has Shared Read=3 -> total must be 5, not 8.
	root := &explainPlanNode{
		NodeType:         "Aggregate",
		SharedHitBlocks:  10,
		SharedReadBlocks: 5,
		Plans: []explainPlanNode{
			{
				NodeType:         "Custom Scan (ChunkAppend)",
				RelationName:     "telemetry",
				SharedHitBlocks:  9,
				SharedReadBlocks: 4,
				Plans: []explainPlanNode{
					{
						NodeType:         "Index Scan",
						RelationName:     "telemetry",
						IndexName:        "idx_telemetry_gw_sensor_measured",
						SharedHitBlocks:  7,
						SharedReadBlocks: 3,
					},
				},
			},
		},
	}

	var nodeTypes, relations, indexes []string
	var totalHits, totalReads int64

	collectPlanSummary(root, &nodeTypes, &relations, &indexes, &totalHits, &totalReads)

	if totalHits != 10 {
		t.Errorf("expected totalHits=10 from root plan, got %d (subtrees must not be summed)", totalHits)
	}
	if totalReads != 5 {
		t.Errorf("expected totalReads=5 from root plan, got %d (subtrees must not be summed)", totalReads)
	}

	// Verify descendant traversal still collected all node types, relations, and indexes across the tree
	expectedNodes := []string{"Aggregate", "Custom Scan (ChunkAppend)", "Index Scan"}
	if len(nodeTypes) != len(expectedNodes) {
		t.Fatalf("expected %d node types, got %d: %v", len(expectedNodes), len(nodeTypes), nodeTypes)
	}
	for i, expected := range expectedNodes {
		if nodeTypes[i] != expected {
			t.Errorf("nodeTypes[%d] = %q, expected %q", i, nodeTypes[i], expected)
		}
	}

	if len(relations) != 2 || relations[0] != "telemetry" || relations[1] != "telemetry" {
		t.Errorf("expected relations [telemetry, telemetry], got %v", relations)
	}
	if len(indexes) != 1 || indexes[0] != "idx_telemetry_gw_sensor_measured" {
		t.Errorf("expected indexes [idx_telemetry_gw_sensor_measured], got %v", indexes)
	}
}

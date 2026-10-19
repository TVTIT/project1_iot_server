package digitaltwin

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/telemetry"
)

// temporalExplainReport captures PostgreSQL EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) execution statistics.
type temporalExplainReport struct {
	Plan          temporalExplainPlanNode `json:"Plan"`
	PlanningTime  float64                 `json:"Planning Time"`
	ExecutionTime float64                 `json:"Execution Time"`
}

type temporalExplainPlanNode struct {
	NodeType            string                    `json:"Node Type"`
	RelationName        string                    `json:"Relation Name"`
	Alias               string                    `json:"Alias"`
	IndexName           string                    `json:"Index Name"`
	ActualStartupTime   float64                   `json:"Actual Startup Time"`
	ActualTotalTime     float64                   `json:"Actual Total Time"`
	ActualRows          int64                     `json:"Actual Rows"`
	ActualLoops         int64                     `json:"Actual Loops"`
	SharedHitBlocks     int64                     `json:"Shared Hit Blocks"`
	SharedReadBlocks    int64                     `json:"Shared Read Blocks"`
	SharedDirtiedBlocks int64                     `json:"Shared Dirtied Blocks"`
	SharedWrittenBlocks int64                     `json:"Shared Written Blocks"`
	Plans               []temporalExplainPlanNode `json:"Plans"`
	Filter              string                    `json:"Filter"`
	IndexCond           string                    `json:"Index Cond"`
}

func collectTemporalPlanSummary(root *temporalExplainPlanNode, nodeTypes *[]string, relations *[]string, indexes *[]string, totalHits *int64, totalReads *int64) {
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
	traverseTemporalPlanNodes(root, nodeTypes, relations, indexes)
}

func traverseTemporalPlanNodes(node *temporalExplainPlanNode, nodeTypes *[]string, relations *[]string, indexes *[]string) {
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
		traverseTemporalPlanNodes(&node.Plans[i], nodeTypes, relations, indexes)
	}
}

// resolveTemporalTestDatabaseURLs determines connection strings for setup (admin) and application (iot_backend_app).
func resolveTemporalTestDatabaseURLs(t *testing.T) (string, string) {
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

	if os.Getenv("TWIN_TEMPORAL_TEST_NATIVE") != "1" &&
		os.Getenv("TELEMETRY_TEST_NATIVE") != "1" &&
		os.Getenv("STAGE4_TEST_NATIVE") != "1" &&
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

	for _, line := range strings.Split(string(content), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || !strings.Contains(trimmed, "=") {
			continue
		}
		parts := strings.SplitN(trimmed, "=", 2)
		k := strings.TrimSpace(parts[0])
		v := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
		if k == "DATABASE_URL" && setupURL == "" {
			setupURL = v
		}
		if k == "BACKEND_DATABASE_URL" && backendURL == "" {
			backendURL = v
		}
	}

	return setupURL, backendURL
}

// temporalGeneratorSource implements pgx.CopyFromSource for generating synthetic twin_temporal_values rows.
type temporalGeneratorSource struct {
	rows [][]any
	idx  int
}

func (s *temporalGeneratorSource) Next() bool {
	if s.idx >= len(s.rows) {
		return false
	}
	s.idx++
	return true
}

func (s *temporalGeneratorSource) Values() ([]any, error) {
	return s.rows[s.idx-1], nil
}

func (s *temporalGeneratorSource) Err() error {
	return nil
}

// TestNativeTemporalHistoryQueryPlanMeasurement executes the temporal query measurement half of Task 4.7.5:
// Measures actual PostgreSQL/TimescaleDB query plans and performance on twin_temporal_values
// using exact adapter SQL (rawTemporalHistoryQuery and bucketTemporalHistoryQuery) under iot_backend_app,
// with deterministic observations including explicit interval counts, rate, unit, and gaps.
func TestNativeTemporalHistoryQueryPlanMeasurement(t *testing.T) {
	setupURL, backendURL := resolveTemporalTestDatabaseURLs(t)
	if setupURL == "" || backendURL == "" {
		t.Skip("setup and application database connection URLs not available (set TWIN_TEMPORAL_TEST_NATIVE=1 or TELEMETRY_TEST_ISOLATED=1)")
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

	// 1. Verify database versions, schema version, application role, and pool settings
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

	// Record compression status for twin_temporal_values hypertable
	var compressedCount, totalChunks int
	err = setupPool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN is_compressed THEN 1 ELSE 0 END), 0),
			COUNT(*)
		FROM timescaledb_information.chunks
		WHERE hypertable_name = 'twin_temporal_values'
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
	t.Logf("Hypertable Compression (twin_temporal_values): is_compressed=%v (compressed: %d / %d total)",
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
	distractUserUID := uuid.New()

	targetGW := fmt.Sprintf("gw_meas_%s", runID)
	targetSensor := fmt.Sprintf("sens_meas_%s", runID)
	targetGWURN := fmt.Sprintf("urn:ngsi-ld:Gateway:%s", targetGW)
	targetSensorURN := fmt.Sprintf("urn:ngsi-ld:Sensor:%s:%s", targetGW, targetSensor)
	targetGWUUID := uuid.New()
	targetSensorUUID := uuid.New()

	distractGW := fmt.Sprintf("gw_distr_%s", runID)
	distractSensor := fmt.Sprintf("sens_distr_%s", runID)
	distractGWURN := fmt.Sprintf("urn:ngsi-ld:Gateway:%s", distractGW)
	distractSensorURN := fmt.Sprintf("urn:ngsi-ld:Sensor:%s:%s", distractGW, distractSensor)
	distractGWUUID := uuid.New()
	distractSensorUUID := uuid.New()

	// Fixture cleanup defer hook: deletes only fixture-created resources unless preserved
	defer func() {
		if os.Getenv("STAGE4_PRESERVE_FIXTURE") == "1" {
			t.Logf("STAGE4_PRESERVE_FIXTURE=1: retaining seeded temporal fixture rows for cross-test/compression verification")
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()

		_, _ = setupPool.Exec(cleanupCtx, "DELETE FROM public.twin_temporal_values WHERE entity_id IN ($1, $2)", targetSensorUUID, distractSensorUUID)
		_, _ = setupPool.Exec(cleanupCtx, "DELETE FROM public.twin_states WHERE entity_id IN ($1, $2, $3, $4)", targetGWUUID, targetSensorUUID, distractGWUUID, distractSensorUUID)
		_, _ = setupPool.Exec(cleanupCtx, "DELETE FROM public.twin_relationships WHERE source_entity_id IN ($1, $2) OR target_entity_id IN ($1, $2)", targetGWUUID, distractGWUUID)
		_, _ = setupPool.Exec(cleanupCtx, "DELETE FROM public.twin_entities WHERE id IN ($1, $2, $3, $4)", targetGWUUID, targetSensorUUID, distractGWUUID, distractSensorUUID)
		_, _ = setupPool.Exec(cleanupCtx, "DELETE FROM public.user_gateways WHERE gateway_id IN ($1, $2)", targetGW, distractGW)
		_, _ = setupPool.Exec(cleanupCtx, "DELETE FROM public.sensors WHERE gateway_id IN ($1, $2)", targetGW, distractGW)
		_, _ = setupPool.Exec(cleanupCtx, "DELETE FROM public.gateways WHERE gateway_id IN ($1, $2)", targetGW, distractGW)
		_, _ = setupPool.Exec(cleanupCtx, "DELETE FROM public.profiles WHERE id IN ($1, $2)", testUserUID, distractUserUID)

		var hasAuthUsers bool
		if err := setupPool.QueryRow(cleanupCtx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err == nil && hasAuthUsers {
			_, _ = setupPool.Exec(cleanupCtx, "DELETE FROM auth.users WHERE id IN ($1, $2)", testUserUID, distractUserUID)
		}
	}()

	// 3. Seed temporal observations:
	// Target dataset: 580 observations of property "sample_interval_us"
	// Value: 10,000.0 us, Unit: "us" in metadata, over 10 minutes (600s).
	// Segment 1: seconds 0..299 (300 observations)
	// Explicit Gap: seconds 300..319 (20s gap, 0 observations)
	// Segment 2: seconds 320..599 (280 observations)
	// Total target = 300 + 280 = 580 observations.
	// Distractor dataset: 60 observations (every 10s: 0, 10, 20..590s) of value 100,000.0 us, unit "us".
	baseTime := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	metaUS := []byte(`{"unit": "us"}`)

	var targetRows [][]any
	// Segment 1: s=0..299
	for s := 0; s < 300; s++ {
		tObs := baseTime.Add(time.Duration(s) * time.Second)
		msgID := uuid.New()
		targetRows = append(targetRows, []any{
			tObs, targetSensorUUID, "sample_interval_us", 10000.0, metaUS, msgID,
		})
	}
	// Gap: s=300..319 (no rows)
	// Segment 2: s=320..599
	for s := 320; s < 600; s++ {
		tObs := baseTime.Add(time.Duration(s) * time.Second)
		msgID := uuid.New()
		targetRows = append(targetRows, []any{
			tObs, targetSensorUUID, "sample_interval_us", 10000.0, metaUS, msgID,
		})
	}

	var distractRows [][]any
	for s := 0; s < 600; s += 10 {
		tObs := baseTime.Add(time.Duration(s) * time.Second)
		msgID := uuid.New()
		distractRows = append(distractRows, []any{
			tObs, distractSensorUUID, "sample_interval_us", 100000.0, metaUS, msgID,
		})
	}

	if os.Getenv("STAGE4_MEASUREMENT_RERUN_ONLY") == "1" {
		t.Logf("STAGE4_MEASUREMENT_RERUN_ONLY=1: querying existing entity UUIDs and verifying existing target temporal rows without reseeding")
		err := setupPool.QueryRow(ctx, "SELECT id FROM public.twin_entities WHERE entity_id = $1", targetSensorURN).Scan(&targetSensorUUID)
		if err != nil {
			t.Fatalf("failed to query existing target sensor UUID: %v", err)
		}
		err = setupPool.QueryRow(ctx, "SELECT id FROM public.twin_entities WHERE entity_id = $1", targetGWURN).Scan(&targetGWUUID)
		if err != nil {
			t.Fatalf("failed to query existing target gateway UUID: %v", err)
		}
		err = setupPool.QueryRow(ctx, "SELECT id FROM public.twin_entities WHERE entity_id = $1", distractSensorURN).Scan(&distractSensorUUID)
		if err != nil {
			t.Fatalf("failed to query existing distractor sensor UUID: %v", err)
		}
		err = setupPool.QueryRow(ctx, "SELECT id FROM public.twin_entities WHERE entity_id = $1", distractGWURN).Scan(&distractGWUUID)
		if err != nil {
			t.Fatalf("failed to query existing distractor gateway UUID: %v", err)
		}
		err = setupPool.QueryRow(ctx, "SELECT user_id FROM public.user_gateways WHERE gateway_id = $1", distractGW).Scan(&distractUserUID)
		if err != nil {
			t.Fatalf("failed to query existing distractor user UUID: %v", err)
		}

		var actualTargetCount, actualDistractCount int
		err = setupPool.QueryRow(ctx, `
			SELECT
				COALESCE(SUM(CASE WHEN e.entity_id = $1 THEN 1 ELSE 0 END), 0),
				COALESCE(SUM(CASE WHEN e.entity_id = $2 THEN 1 ELSE 0 END), 0)
			FROM public.twin_temporal_values v
			JOIN public.twin_entities e ON e.id = v.entity_id`, targetSensorURN, distractSensorURN).Scan(&actualTargetCount, &actualDistractCount)
		if err != nil {
			t.Fatalf("failed to query existing temporal rows for rerun: %v", err)
		}
		if actualTargetCount != len(targetRows) {
			t.Fatalf("expected %d target temporal rows for rerun, got %d", len(targetRows), actualTargetCount)
		}
		if actualDistractCount != len(distractRows) {
			t.Fatalf("expected %d distractor temporal rows for rerun, got %d", len(distractRows), actualDistractCount)
		}
	} else {
		// Seed auth.users if table exists
		var hasAuthUsers bool
		if err := setupPool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err == nil && hasAuthUsers {
			for _, u := range []struct {
				id    uuid.UUID
				email string
			}{
				{testUserUID, fmt.Sprintf("meas-temp-%s@example.com", runID)},
				{distractUserUID, fmt.Sprintf("distr-temp-%s@example.com", runID)},
			} {
				_, err = setupPool.Exec(ctx, `
					INSERT INTO auth.users (id, aud, role, email)
					VALUES ($1, 'authenticated', 'authenticated', $2)
					ON CONFLICT (id) DO NOTHING`,
					u.id, u.email,
				)
				if err != nil {
					t.Fatalf("failed to seed auth.users: %v", err)
				}
			}
		}

		// Seed profiles
		for _, u := range []struct {
			id   uuid.UUID
			name string
		}{
			{testUserUID, fmt.Sprintf("Temporal Target User %s", runID)},
			{distractUserUID, fmt.Sprintf("Temporal Distractor User %s", runID)},
		} {
			if _, err := setupPool.Exec(ctx, `
				INSERT INTO public.profiles (id, full_name)
				VALUES ($1, $2)
				ON CONFLICT (id) DO NOTHING`,
				u.id, u.name,
			); err != nil {
				t.Fatalf("failed to seed profile: %v", err)
			}
		}

		// Seed gateways
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.gateways (gateway_id, name, description)
			VALUES ($1, 'Target Temporal Gateway', 'Measurement GW'), ($2, 'Distractor Temporal Gateway', 'Distractor GW')
			ON CONFLICT (gateway_id) DO NOTHING`,
			targetGW, distractGW,
		); err != nil {
			t.Fatalf("failed to seed gateways: %v", err)
		}

		// Seed sensors: target has unit "us", distractor has unit "us"
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.sensors (gateway_id, sensor_id, name, unit)
			VALUES ($1, $2, 'Target Temporal Sensor', 'us'), ($3, $4, 'Distractor Temporal Sensor', 'us')
			ON CONFLICT (gateway_id, sensor_id) DO NOTHING`,
			targetGW, targetSensor, distractGW, distractSensor,
		); err != nil {
			t.Fatalf("failed to seed sensors: %v", err)
		}

		// Seed twin entities
		for _, ent := range []struct {
			id   uuid.UUID
			urn  string
			typ  string
			gwID string
		}{
			{targetGWUUID, targetGWURN, "Gateway", targetGW},
			{targetSensorUUID, targetSensorURN, "Sensor", targetGW},
			{distractGWUUID, distractGWURN, "Gateway", distractGW},
			{distractSensorUUID, distractSensorURN, "Sensor", distractGW},
		} {
			if _, err := setupPool.Exec(ctx, `
				INSERT INTO public.twin_entities (id, entity_id, entity_type, name, gateway_id, attributes)
				VALUES ($1, $2, $3, $2, $4, '{}'::jsonb)
				ON CONFLICT (id) DO NOTHING`,
				ent.id, ent.urn, ent.typ, ent.gwID,
			); err != nil {
				t.Fatalf("failed to seed twin_entity: %v", err)
			}
		}

		// Seed canonical hasSensor relationships
		for _, rel := range []struct {
			src uuid.UUID
			tgt uuid.UUID
		}{
			{targetGWUUID, targetSensorUUID},
			{distractGWUUID, distractSensorUUID},
		} {
			if _, err := setupPool.Exec(ctx, `
				INSERT INTO public.twin_relationships (source_entity_id, relationship_type, target_entity_id, created_at)
				VALUES ($1, 'hasSensor', $2, now())
				ON CONFLICT (source_entity_id, relationship_type, target_entity_id) DO NOTHING`,
				rel.src, rel.tgt,
			); err != nil {
				t.Fatalf("failed to seed twin_relationship: %v", err)
			}
		}

		// Seed twin_states
		for _, eid := range []uuid.UUID{targetGWUUID, targetSensorUUID, distractGWUUID, distractSensorUUID} {
			if _, err := setupPool.Exec(ctx, `
				INSERT INTO public.twin_states (entity_id, reported_state, desired_state, reported_version, desired_version)
				VALUES ($1, '{}'::jsonb, '{}'::jsonb, 0, 0)
				ON CONFLICT (entity_id) DO NOTHING`,
				eid,
			); err != nil {
				t.Fatalf("failed to seed twin_state: %v", err)
			}
		}

		// Seed user_gateways: testUserUID is owner of targetGW; distractUserUID is owner of distractGW
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.user_gateways (user_id, gateway_id, role)
			VALUES ($1, $2, 'owner'), ($3, $4, 'owner')
			ON CONFLICT (user_id, gateway_id) DO UPDATE SET role = EXCLUDED.role`,
			testUserUID, targetGW, distractUserUID, distractGW,
		); err != nil {
			t.Fatalf("failed to seed user_gateways: %v", err)
		}

		temporalColumns := []string{
			"observed_at", "entity_id", "property_name", "value_number", "metadata", "message_id",
		}

		nTargetInserted, err := setupPool.CopyFrom(ctx, pgx.Identifier{"public", "twin_temporal_values"}, temporalColumns, &temporalGeneratorSource{rows: targetRows})
		if err != nil {
			t.Fatalf("failed to copy target temporal rows: %v", err)
		}
		if nTargetInserted != int64(len(targetRows)) {
			t.Fatalf("expected %d target temporal rows, got %d", len(targetRows), nTargetInserted)
		}

		nDistractInserted, err := setupPool.CopyFrom(ctx, pgx.Identifier{"public", "twin_temporal_values"}, temporalColumns, &temporalGeneratorSource{rows: distractRows})
		if err != nil {
			t.Fatalf("failed to copy distractor temporal rows: %v", err)
		}
		if nDistractInserted != int64(len(distractRows)) {
			t.Fatalf("expected %d distractor temporal rows, got %d", len(distractRows), nDistractInserted)
		}

		t.Logf("Seeding Temporal Complete: %d target rows (580 obs, 20s gap at [300s, 320s)) + %d distractor rows",
			nTargetInserted, nDistractInserted)
	}

	// Helper to execute EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) under appPool
	execExplain := func(querySQL string, args ...any) (*temporalExplainReport, error) {
		explainSQL := "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + querySQL
		var jsonBytes []byte
		err := appPool.QueryRow(ctx, explainSQL, args...).Scan(&jsonBytes)
		if err != nil {
			return nil, err
		}
		var reports []temporalExplainReport
		if err := json.Unmarshal(jsonBytes, &reports); err != nil {
			return nil, fmt.Errorf("failed to unmarshal explain json: %w", err)
		}
		if len(reports) == 0 {
			return nil, fmt.Errorf("empty explain report")
		}
		return &reports[0], nil
	}

	reader, err := NewPostgresTwinReader(appPool)
	if err != nil {
		t.Fatalf("failed to create PostgresTwinReader: %v", err)
	}

	resolvedTarget := ResolvedEntity{
		InternalID: targetSensorUUID,
		PublicURN:  targetSensorURN,
		Type:       EntityTypeSensor,
		GatewayID:  targetGW,
		SensorID:   targetSensor,
	}

	budgetLimitMs := 5000.0

	// =========================================================================
	// MEASUREMENT 4: Temporal Raw Query (Short window 10s -> 10 observations)
	// Window: [baseTime, baseTime + 10s), MaxPoints = 500, Limit = 501
	// =========================================================================
	t.Logf("--- MEASUREMENT 4: Temporal Raw Query (10s, 10 samples @ 1 Hz, cap=500) ---")
	rawWindowFrom := baseTime
	rawWindowTo := baseTime.Add(10 * time.Second)
	rawLimit := 501

	rep4, err := execExplain(
		rawTemporalHistoryQuery,
		targetSensor,
		testUserUID,
		targetSensorUUID,
		targetSensorURN,
		targetGW,
		"sample_interval_us",
		rawWindowFrom.UTC(),
		rawWindowTo.UTC(),
		rawLimit,
	)
	if err != nil {
		t.Fatalf("Measurement 4 EXPLAIN failed: %v", err)
	}

	var nodes4, rels4, idxs4 []string
	var hits4, reads4 int64
	collectTemporalPlanSummary(&rep4.Plan, &nodes4, &rels4, &idxs4, &hits4, &reads4)

	t.Logf("M4 Plan: ExecutionTime=%.3f ms, PlanningTime=%.3f ms, ActualRows=%d",
		rep4.ExecutionTime, rep4.PlanningTime, rep4.Plan.ActualRows)
	t.Logf("M4 Buffer Accesses: SharedHit=%d, SharedRead=%d", hits4, reads4)
	t.Logf("M4 Nodes: %s", strings.Join(nodes4, " -> "))

	// Service adapter verification
	m4Start := time.Now()
	res4, err := reader.readTemporalHistoryInternal(ctx, testUserUID, resolvedTarget, "sample_interval_us", TwinHistoryQuery{
		EntityID:           targetSensorURN,
		Property:           SelectorSampleIntervalUS,
		Range:              telemetry.TimeRange{From: rawWindowFrom, To: rawWindowTo},
		Resolution:         telemetry.ResolutionRaw,
		MaxPoints:          500,
		EffectiveRawWindow: 10 * time.Second,
	})
	m4Duration := time.Since(m4Start)
	if err != nil {
		t.Fatalf("Measurement 4 service call failed: %v", err)
	}
	if len(res4.TemporalPoints) != 10 {
		t.Fatalf("expected 10 raw temporal points, got %d", len(res4.TemporalPoints))
	}
	if res4.Unit == nil || *res4.Unit != "us" {
		t.Fatalf("expected unit 'us', got %v", res4.Unit)
	}
	for i, pt := range res4.TemporalPoints {
		if pt.ValueNumber == nil || *pt.ValueNumber != 10000.0 {
			t.Fatalf("point [%d] expected value 10000.0, got %v", i, pt.ValueNumber)
		}
	}
	t.Logf("M4 Service Result: Points=%d, Unit=%s, ServiceElapsed=%v",
		len(res4.TemporalPoints), *res4.Unit, m4Duration)

	if rep4.ExecutionTime > budgetLimitMs {
		t.Errorf("M4 execution time %.3f ms exceeded 5s budget", rep4.ExecutionTime)
	}

	// =========================================================================
	// MEASUREMENT 5: Temporal Bucket Query (Longer window 10m, resolution 1m)
	// Window: [baseTime, baseTime + 10m), 10 buckets of 60s
	// Explicit gap at seconds 300..319 -> bucket 5 has 40 observations, others have 60
	// =========================================================================
	t.Logf("--- MEASUREMENT 5: Temporal Bucket Query (10m window, 1m resolution, 10 buckets with gap) ---")
	bucketWindowFrom := baseTime
	bucketWindowTo := baseTime.Add(10 * time.Minute)
	bucketInterval := pgtype.Interval{
		Microseconds: 60 * 1000 * 1000, // 1 minute
		Valid:        true,
	}
	bucketLimit := 501

	rep5, err := execExplain(
		bucketTemporalHistoryQuery,
		targetSensor,
		testUserUID,
		targetSensorUUID,
		targetSensorURN,
		targetGW,
		"sample_interval_us",
		bucketWindowFrom.UTC(),
		bucketWindowTo.UTC(),
		bucketInterval,
		bucketLimit,
	)
	if err != nil {
		t.Fatalf("Measurement 5 EXPLAIN failed: %v", err)
	}

	var nodes5, rels5, idxs5 []string
	var hits5, reads5 int64
	collectTemporalPlanSummary(&rep5.Plan, &nodes5, &rels5, &idxs5, &hits5, &reads5)

	t.Logf("M5 Plan: ExecutionTime=%.3f ms, PlanningTime=%.3f ms, ActualRows=%d",
		rep5.ExecutionTime, rep5.PlanningTime, rep5.Plan.ActualRows)
	t.Logf("M5 Buffer Accesses: SharedHit=%d, SharedRead=%d", hits5, reads5)
	t.Logf("M5 Nodes: %s", strings.Join(nodes5, " -> "))

	// Service adapter verification with arithmetic and gap assertions
	m5Start := time.Now()
	res5, err := reader.readTemporalHistoryInternal(ctx, testUserUID, resolvedTarget, "sample_interval_us", TwinHistoryQuery{
		EntityID:           targetSensorURN,
		Property:           SelectorSampleIntervalUS,
		Range:              telemetry.TimeRange{From: bucketWindowFrom, To: bucketWindowTo},
		Resolution:         telemetry.Resolution1m,
		MaxPoints:          500,
		EffectiveRawWindow: 10 * time.Second,
	})
	m5Duration := time.Since(m5Start)
	if err != nil {
		t.Fatalf("Measurement 5 service call failed: %v", err)
	}
	if len(res5.TemporalBuckets) != 10 {
		t.Fatalf("expected 10 temporal buckets, got %d", len(res5.TemporalBuckets))
	}
	if res5.Unit == nil || *res5.Unit != "us" {
		t.Fatalf("expected unit 'us', got %v", res5.Unit)
	}

	var bucketCounts []int64
	var totalSampleSum int64
	for i, b := range res5.TemporalBuckets {
		bucketCounts = append(bucketCounts, b.Count)
		totalSampleSum += b.Count
		// Arithmetic assertions: all samples in target dataset are 10000.0
		if b.Avg != 10000.0 || b.Min != 10000.0 || b.Max != 10000.0 {
			t.Fatalf("bucket [%d] arithmetic mismatch: avg=%.1f min=%.1f max=%.1f", i, b.Avg, b.Min, b.Max)
		}
		// Gap verification: bucket index 5 corresponds to [baseTime+5m, baseTime+6m) = seconds 300..359
		// The 20s gap is at seconds 300..319, so bucket 5 has exactly 40 observations!
		if i == 5 {
			if b.Count != 40 {
				t.Fatalf("bucket [5] expected count 40 (reflecting 20s gap), got %d", b.Count)
			}
		} else {
			if b.Count != 60 {
				t.Fatalf("bucket [%d] expected count 60, got %d", i, b.Count)
			}
		}
	}
	if totalSampleSum != 580 {
		t.Fatalf("expected total aggregated samples 580, got %d", totalSampleSum)
	}

	t.Logf("M5 Service Result: Buckets=%d, Counts=%v, TotalSamples=%d, Arithmetic=PASS, Gap=PASS, ServiceElapsed=%v",
		len(res5.TemporalBuckets), bucketCounts, totalSampleSum, m5Duration)

	if rep5.ExecutionTime > budgetLimitMs {
		t.Errorf("M5 execution time %.3f ms exceeded 5s budget", rep5.ExecutionTime)
	}

	// =========================================================================
	// SELECTIVITY & ISOLATION CHECK
	// =========================================================================
	t.Logf("--- SELECTIVITY & ISOLATION CHECK ---")
	// User testUserUID (owner of targetGW) querying distractSensor must fail closed with ErrNotFound
	resolvedDistract := ResolvedEntity{
		InternalID: distractSensorUUID,
		PublicURN:  distractSensorURN,
		Type:       EntityTypeSensor,
		GatewayID:  distractGW,
		SensorID:   distractSensor,
	}
	_, err = reader.readTemporalHistoryInternal(ctx, testUserUID, resolvedDistract, "sample_interval_us", TwinHistoryQuery{
		EntityID:           distractSensorURN,
		Property:           SelectorSampleIntervalUS,
		Range:              telemetry.TimeRange{From: rawWindowFrom, To: rawWindowTo},
		Resolution:         telemetry.ResolutionRaw,
		MaxPoints:          500,
		EffectiveRawWindow: 10 * time.Second,
	})
	if err == nil {
		t.Fatalf("expected error querying unassigned distractor sensor, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not found error, got: %v", err)
	}
	t.Logf("Selectivity Verified: unassigned distractor sensor correctly rejected as not found")

	// =========================================================================
	// SUMMARY & JSON OUTPUT
	// =========================================================================
	summary := map[string]any{
		"scenario":              "temporal",
		"run_id":                runID,
		"target_gateway_id":     targetGW,
		"target_sensor_id":      targetSensor,
		"target_sensor_urn":     targetSensorURN,
		"distract_gateway_id":   distractGW,
		"distract_sensor_id":    distractSensor,
		"distract_sensor_urn":   distractSensorURN,
		"target_obs_count":      len(targetRows),
		"distract_obs_count":    len(distractRows),
		"m4_planning_time_ms":   rep4.PlanningTime,
		"m4_execution_time_ms":  rep4.ExecutionTime,
		"m4_actual_rows":        rep4.Plan.ActualRows,
		"m4_shared_hit_blocks":  hits4,
		"m4_shared_read_blocks": reads4,
		"m4_plan_nodes":         nodes4,
		"m4_service_elapsed_ms": float64(m4Duration.Microseconds()) / 1000.0,
		"m5_planning_time_ms":   rep5.PlanningTime,
		"m5_execution_time_ms":  rep5.ExecutionTime,
		"m5_actual_rows":        rep5.Plan.ActualRows,
		"m5_shared_hit_blocks":  hits5,
		"m5_shared_read_blocks": reads5,
		"m5_plan_nodes":         nodes5,
		"m5_service_elapsed_ms": float64(m5Duration.Microseconds()) / 1000.0,
		"m5_bucket_counts":      bucketCounts,
		"m5_arithmetic_valid":   true,
		"m5_gap_verified":       true,
		"selectivity_verified":  true,
		"is_compressed":         isCompressed,
	}

	outJSON, err := json.Marshal(summary)
	if err == nil {
		fmt.Printf("TEMPORAL_MEASUREMENT_JSON: %s\n", string(outJSON))
		if outPath := os.Getenv("STAGE4_MEASUREMENT_JSON_OUTPUT_TEMPORAL"); outPath != "" {
			_ = os.WriteFile(outPath, outJSON, 0o600)
		}
	}
}

func TestCollectTemporalPlanSummary_RootBufferCountersIncludeSubtree(t *testing.T) {
	t.Parallel()

	root := &temporalExplainPlanNode{
		NodeType:         "Aggregate",
		SharedHitBlocks:  12,
		SharedReadBlocks: 4,
		Plans: []temporalExplainPlanNode{
			{
				NodeType:         "Custom Scan (ChunkAppend)",
				RelationName:     "twin_temporal_values",
				SharedHitBlocks:  10,
				SharedReadBlocks: 3,
			},
		},
	}

	var nodes, rels, idxs []string
	var hits, reads int64
	collectTemporalPlanSummary(root, &nodes, &rels, &idxs, &hits, &reads)

	if hits != 12 {
		t.Errorf("expected root hits 12, got %d", hits)
	}
	if reads != 4 {
		t.Errorf("expected root reads 4, got %d", reads)
	}
	if len(nodes) != 2 || nodes[0] != "Aggregate" || nodes[1] != "Custom Scan (ChunkAppend)" {
		t.Errorf("unexpected nodes: %v", nodes)
	}
	if len(rels) != 1 || rels[0] != "twin_temporal_values" {
		t.Errorf("unexpected rels: %v", rels)
	}
}

package main

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
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
	if os.Getenv("STAGE4_HTTP_TEST_NATIVE") != "1" &&
		os.Getenv("TWIN_HTTP_TEST_NATIVE") != "1" &&
		os.Getenv("TELEMETRY_TEST_NATIVE") != "1" &&
		os.Getenv("TWIN_METADATA_TEST_NATIVE") != "1" &&
		os.Getenv("TWIN_READ_TEST_NATIVE") != "1" {
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

type stage4SmokeFixtureIDs struct {
	RunID            string
	GatewayIDs       []string
	AllUserIDs       []uuid.UUID
	AuthUsersPresent *bool
}

func performStage4SmokeFixtureCleanup(ctx context.Context, t *testing.T, pool *pgxpool.Pool, f stage4SmokeFixtureIDs) {
	t.Helper()

	execCleanup := func(table, query string, args ...any) {
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Errorf("cleanup failed for table %s: %v", table, err)
		}
	}

	verifyCount := func(table, query string, args ...any) {
		var cnt int
		if err := pool.QueryRow(ctx, query, args...).Scan(&cnt); err != nil {
			t.Errorf("cleanup count verification failed for table %s: %v", table, err)
		} else if cnt != 0 {
			t.Errorf("cleanup incomplete: %d row(s) remain in %s for run %s", cnt, table, f.RunID)
		}
	}

	// 0a. Delete twin_outbox
	execCleanup("twin_outbox", `
		DELETE FROM public.twin_outbox
		WHERE command_id IN (
			SELECT command_id FROM public.twin_commands
			WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))
		)`, f.GatewayIDs)

	// 0b. Delete twin_commands
	execCleanup("twin_commands", `
		DELETE FROM public.twin_commands
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`,
		f.GatewayIDs)

	// 0c. Delete gateway_mqtt_credentials first (FK fk_gateway_mqtt_credentials_last_operation)
	execCleanup("gateway_mqtt_credentials", `
		DELETE FROM public.gateway_mqtt_credentials WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	// 0d. Delete gateway_mqtt_credential_events
	execCleanup("gateway_mqtt_credential_events", `
		DELETE FROM public.gateway_mqtt_credential_events WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	// 0e. Delete telemetry
	execCleanup("telemetry", `
		DELETE FROM public.telemetry WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	// 0f. Delete processed_messages
	execCleanup("processed_messages", `
		DELETE FROM public.processed_messages WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	// 1. Delete twin_temporal_values
	execCleanup("twin_temporal_values", `
		DELETE FROM public.twin_temporal_values
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`,
		f.GatewayIDs)

	// 2. Delete twin_states
	execCleanup("twin_states", `
		DELETE FROM public.twin_states
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`,
		f.GatewayIDs)

	// 3. Delete twin_relationships
	execCleanup("twin_relationships", `
		DELETE FROM public.twin_relationships
		WHERE source_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))
		   OR target_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`,
		f.GatewayIDs)

	// 4. Delete twin_entities
	execCleanup("twin_entities", `DELETE FROM public.twin_entities WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	// 5. Delete sensors
	execCleanup("sensors", `DELETE FROM public.sensors WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	// 6. Delete user_gateways
	execCleanup("user_gateways", `DELETE FROM public.user_gateways WHERE gateway_id = ANY($1) OR user_id = ANY($2)`, f.GatewayIDs, f.AllUserIDs)

	// 7. Delete gateways
	execCleanup("gateways", `DELETE FROM public.gateways WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	// 8. Delete platform_admins
	execCleanup("platform_admins", `DELETE FROM public.platform_admins WHERE user_id = ANY($1)`, f.AllUserIDs)

	// 9. Delete profiles
	execCleanup("profiles", `DELETE FROM public.profiles WHERE id = ANY($1)`, f.AllUserIDs)

	// 10. Delete auth.users if table exists
	var shouldCleanAuth bool
	if f.AuthUsersPresent != nil && *f.AuthUsersPresent {
		shouldCleanAuth = true
	} else if f.AuthUsersPresent == nil {
		var hasAuthUsers bool
		if err := pool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err == nil && hasAuthUsers {
			shouldCleanAuth = true
		}
	}
	if shouldCleanAuth {
		execCleanup("auth.users", `DELETE FROM auth.users WHERE id = ANY($1)`, f.AllUserIDs)
	}

	// 11. Verify zero rows remain across all affected tables for this run
	verifyCount("twin_outbox", `
		SELECT COUNT(*) FROM public.twin_outbox
		WHERE command_id IN (
			SELECT command_id FROM public.twin_commands
			WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))
		)`, f.GatewayIDs)
	verifyCount("twin_commands", `
		SELECT COUNT(*) FROM public.twin_commands
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`, f.GatewayIDs)
	verifyCount("gateway_mqtt_credentials", `
		SELECT COUNT(*) FROM public.gateway_mqtt_credentials WHERE gateway_id = ANY($1)`, f.GatewayIDs)
	verifyCount("gateway_mqtt_credential_events", `
		SELECT COUNT(*) FROM public.gateway_mqtt_credential_events WHERE gateway_id = ANY($1)`, f.GatewayIDs)
	verifyCount("telemetry", `SELECT COUNT(*) FROM public.telemetry WHERE gateway_id = ANY($1)`, f.GatewayIDs)
	verifyCount("processed_messages", `SELECT COUNT(*) FROM public.processed_messages WHERE gateway_id = ANY($1)`, f.GatewayIDs)
	verifyCount("twin_temporal_values", `
		SELECT COUNT(*) FROM public.twin_temporal_values
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`, f.GatewayIDs)
	verifyCount("twin_states", `
		SELECT COUNT(*) FROM public.twin_states
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`, f.GatewayIDs)
	verifyCount("twin_relationships", `
		SELECT COUNT(*) FROM public.twin_relationships
		WHERE source_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))
		   OR target_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`, f.GatewayIDs)
	verifyCount("twin_entities", `SELECT COUNT(*) FROM public.twin_entities WHERE gateway_id = ANY($1)`, f.GatewayIDs)
	verifyCount("sensors", `SELECT COUNT(*) FROM public.sensors WHERE gateway_id = ANY($1)`, f.GatewayIDs)
	verifyCount("user_gateways", `SELECT COUNT(*) FROM public.user_gateways WHERE gateway_id = ANY($1) OR user_id = ANY($2)`, f.GatewayIDs, f.AllUserIDs)
	verifyCount("gateways", `SELECT COUNT(*) FROM public.gateways WHERE gateway_id = ANY($1)`, f.GatewayIDs)
	verifyCount("platform_admins", `SELECT COUNT(*) FROM public.platform_admins WHERE user_id = ANY($1)`, f.AllUserIDs)
	verifyCount("profiles", `SELECT COUNT(*) FROM public.profiles WHERE id = ANY($1)`, f.AllUserIDs)
}

// TestNativeStage4HTTP_Smoke implements Task 4.6.7:
// Runs an opt-in representative native HTTP smoke against real PostgreSQL 16 / TimescaleDB 2.29
// using populateStage4Dependencies, real app-role pool (iot_backend_app), real HS256 verifier with ephemeral secret,
// and httptest.NewServer TCP listener. Exercises all seven Stage 4 endpoints, exact replays, metadata mutation,
// real telemetry persistence, state/history retrieval, and non-member/mutation denials without side effects.
func TestNativeStage4HTTP_Smoke(t *testing.T) {
	setupURL, backendURL := resolveTestDatabaseURLs(t)
	if setupURL == "" || backendURL == "" {
		t.Skip("Skipping native Stage 4 HTTP smoke: set STAGE4_HTTP_TEST_NATIVE=1 to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// 1. Setup connection pools
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

	// 2. Verify target database versions, schema version, and application role
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
	if schemaVersion < 20 {
		t.Fatalf("expected schema version >= 20, got %d", schemaVersion)
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
	t.Logf("Application Connection Verified: user = %s, superuser = %v, canlogin = %v", appUser, appSuper, appCanLogin)

	// 3. Define unique run identifiers and register cleanup BEFORE any writes
	runID := uuid.New().String()[:8]
	gwID := fmt.Sprintf("gw_smoke_%s", runID)
	sensorID := fmt.Sprintf("sens_smoke_%s", runID)

	adminUID := uuid.New()
	ownerUID := uuid.New()
	userBUID := uuid.New() // non-member
	operatorUID := uuid.New()
	viewerUID := uuid.New()

	allUserIDs := []uuid.UUID{adminUID, ownerUID, userBUID, operatorUID, viewerUID}
	fixtureIDs := stage4SmokeFixtureIDs{
		RunID:      runID,
		GatewayIDs: []string{gwID},
		AllUserIDs: allUserIDs,
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		performStage4SmokeFixtureCleanup(cleanupCtx, t, setupPool, fixtureIDs)
	}()

	// 4. Seed user accounts and platform_admins via setupPool
	var hasAuthUsers bool
	if err := setupPool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
		t.Fatalf("failed to check auth.users table existence: %v", err)
	}
	fixtureIDs.AuthUsersPresent = &hasAuthUsers
	if hasAuthUsers {
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

	// Admin user is in platform_admins, NO membership in user_gateways
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.platform_admins (user_id)
		VALUES ($1)
		ON CONFLICT (user_id) DO NOTHING`, adminUID); err != nil {
		t.Fatalf("failed to insert platform admin: %v", err)
	}

	// 5. Generate ephemeral random secret for HS256 verifier (never logged)
	secretBytes := make([]byte, 32)
	if _, err := cryptorand.Read(secretBytes); err != nil {
		t.Fatalf("failed to generate random secret: %v", err)
	}
	jwtSecret := hex.EncodeToString(secretBytes)
	const jwtIssuer = "https://auth.example.com"
	const jwtAudience = "authenticated"

	verifier, err := auth.NewHS256Verifier(auth.VerifierConfig{
		Secret:   jwtSecret,
		Issuer:   jwtIssuer,
		Audience: jwtAudience,
	})
	if err != nil {
		t.Fatalf("create HS256 verifier failed: %v", err)
	}

	signJWT := func(sub uuid.UUID, role string) string {
		now := time.Now()
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"sub":  sub.String(),
			"iss":  jwtIssuer,
			"aud":  jwtAudience,
			"iat":  now.Unix(),
			"exp":  now.Add(5 * time.Minute).Unix(),
			"role": role,
		})
		s, err := tok.SignedString([]byte(jwtSecret))
		if err != nil {
			t.Fatalf("failed to sign test JWT: %v", err)
		}
		return s
	}

	adminToken := signJWT(adminUID, "authenticated")
	ownerToken := signJWT(ownerUID, "authenticated")
	userBToken := signJWT(userBUID, "authenticated")
	operatorToken := signJWT(operatorUID, "authenticated")
	viewerToken := signJWT(viewerUID, "authenticated")

	// 6. Construct production dependencies and HTTP router using appPool and populateStage4Dependencies
	cfg := validTestConfig()
	adminChecker, err := auth.NewPostgresPlatformAdminChecker(appPool)
	if err != nil {
		t.Fatalf("create platform admin checker failed: %v", err)
	}
	gwRepo, err := gateway.NewPostgresRepository(appPool)
	if err != nil {
		t.Fatalf("create gateway repo failed: %v", err)
	}
	gwService, err := gateway.NewService(gwRepo, cfg.AuthorizationTimeout)
	if err != nil {
		t.Fatalf("create gateway service failed: %v", err)
	}
	provRepo, err := gateway.NewPostgresProvisioningRepository(appPool)
	if err != nil {
		t.Fatalf("create provisioning repo failed: %v", err)
	}
	provService, err := gateway.NewProvisioningService(provRepo, cfg.AuthorizationTimeout)
	if err != nil {
		t.Fatalf("create provisioning service failed: %v", err)
	}
	ownerSensorRepo, err := gateway.NewPostgresOwnerSensorProvisioningRepository(appPool)
	if err != nil {
		t.Fatalf("create owner sensor repo failed: %v", err)
	}
	ownerSensorService, err := gateway.NewOwnerSensorProvisioningService(ownerSensorRepo, cfg.AuthorizationTimeout)
	if err != nil {
		t.Fatalf("create owner sensor service failed: %v", err)
	}
	adminGwRepo, err := gateway.NewPostgresAdminGatewayRepository(appPool)
	if err != nil {
		t.Fatalf("create admin gateway repo failed: %v", err)
	}
	adminGwService, err := gateway.NewAdminGatewayService(adminGwRepo, cfg.AuthorizationTimeout, 100, 200)
	if err != nil {
		t.Fatalf("create admin gateway service failed: %v", err)
	}

	routerDeps := httpserver.RouterDependencies{
		ReadinessChecker:           dummyReadinessChecker{},
		ReadinessTimeout:           time.Second,
		AuthorizationTimeout:       cfg.AuthorizationTimeout,
		TokenVerifier:              verifier,
		PlatformAdminChecker:       adminChecker,
		GatewayReader:              gwService,
		SensorReader:               gwService,
		GatewayProvisioner:         provService,
		SensorProvisioner:          provService,
		OwnerSensorProvisioner:     ownerSensorService,
		AdminGatewayLister:         adminGwService,
		AdminInventoryDefaultLimit: 100,
		AdminInventoryMaxLimit:     200,
		AdminMaxBodyBytes:          16384,
	}

	if err := populateStage4Dependencies(&routerDeps, cfg, appPool, provService); err != nil {
		t.Fatalf("populateStage4Dependencies failed: %v", err)
	}

	router, err := httpserver.NewRouter(routerDeps)
	if err != nil {
		t.Fatalf("NewRouter failed: %v", err)
	}

	// 7. Start httptest.NewServer TCP listener
	ts := httptest.NewServer(router)
	defer ts.Close()

	httpClient := &http.Client{Timeout: 10 * time.Second}

	doRequest := func(method, rawURL, token, reqID string, body []byte) (*http.Response, []byte) {
		t.Helper()
		var bodyReader io.Reader
		if body != nil {
			bodyReader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, rawURL, bodyReader)
		if err != nil {
			t.Fatalf("create HTTP request %s %s failed: %v", method, rawURL, err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if reqID != "" {
			req.Header.Set("X-Request-ID", reqID)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("execute HTTP request %s %s failed: %v", method, rawURL, err)
		}
		defer func() { _ = resp.Body.Close() }()

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read response body failed: %v", err)
		}
		return resp, respBody
	}

	gwURN := fmt.Sprintf("urn:ngsi-ld:Gateway:%s", gwID)
	sensorURN := fmt.Sprintf("urn:ngsi-ld:Sensor:%s:%s", gwID, sensorID)

	// -------------------------------------------------------------------------
	// 8. admin POST Gateway with owner -> 201 Created; exact replay -> 200 OK
	// -------------------------------------------------------------------------
	gwCreateBody, err := json.Marshal(map[string]any{
		"entity_type":   "Gateway",
		"gateway_id":    gwID,
		"name":          "Gateway Smoke Target",
		"owner_user_id": ownerUID.String(),
	})
	if err != nil {
		t.Fatalf("marshal gateway create body: %v", err)
	}

	resp, respBytes := doRequest(http.MethodPost, ts.URL+"/v1/digital-twins", adminToken, "req-post-gw-01", gwCreateBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for admin POST Gateway, got %d: %s", resp.StatusCode, string(respBytes))
	}
	if wantLoc := "/v1/digital-twins/" + url.PathEscape(gwURN); resp.Header.Get("Location") != wantLoc {
		t.Errorf("Location = %q, want %q", resp.Header.Get("Location"), wantLoc)
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		t.Errorf("Content-Type = %q, want application/json", resp.Header.Get("Content-Type"))
	}
	if resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Pragma") != "no-cache" {
		t.Errorf("expected no-store/no-cache headers, got Cache-Control=%q, Pragma=%q", resp.Header.Get("Cache-Control"), resp.Header.Get("Pragma"))
	}
	if resp.Header.Get("X-Request-ID") != "req-post-gw-01" {
		t.Errorf("X-Request-ID = %q, want req-post-gw-01", resp.Header.Get("X-Request-ID"))
	}

	var gwCreateResp struct {
		EntityID    string  `json:"entity_id"`
		GatewayID   string  `json:"gateway_id"`
		Name        string  `json:"name"`
		Description *string `json:"description"`
	}
	if err := json.Unmarshal(respBytes, &gwCreateResp); err != nil {
		t.Fatalf("unmarshal gateway create response: %v", err)
	}
	if gwCreateResp.EntityID != gwURN || gwCreateResp.GatewayID != gwID {
		t.Fatalf("unexpected gateway create body: %+v", gwCreateResp)
	}

	// Exact replay POST Gateway -> 200 OK with identical entity metadata
	resp, respBytes = doRequest(http.MethodPost, ts.URL+"/v1/digital-twins", adminToken, "req-post-gw-replay", gwCreateBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for exact replay POST Gateway, got %d: %s", resp.StatusCode, string(respBytes))
	}
	if loc := resp.Header.Get("Location"); loc != "" {
		t.Errorf("Location on replay = %q, want empty", loc)
	}
	var gwReplayResp struct {
		EntityID  string `json:"entity_id"`
		GatewayID string `json:"gateway_id"`
	}
	if err := json.Unmarshal(respBytes, &gwReplayResp); err != nil {
		t.Fatalf("unmarshal gateway replay response: %v", err)
	}
	if gwReplayResp.EntityID != gwURN || gwReplayResp.GatewayID != gwID {
		t.Fatalf("unexpected gateway replay body: %+v", gwReplayResp)
	}

	// Assign operator and viewer memberships in user_gateways for denial testing
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.user_gateways (user_id, gateway_id, role)
		VALUES ($1, $3, 'operator'), ($2, $3, 'viewer')`,
		operatorUID, viewerUID, gwID); err != nil {
		t.Fatalf("failed to insert operator/viewer memberships: %v", err)
	}

	// -------------------------------------------------------------------------
	// 9. owner POST Sensor -> 201 Created; exact replay -> 200 OK
	// -------------------------------------------------------------------------
	sensorCreateBody, err := json.Marshal(map[string]any{
		"entity_type": "Sensor",
		"gateway_id":  gwID,
		"sensor_id":   sensorID,
		"name":        "Smoke Sensor 01",
		"unit":        "celsius",
	})
	if err != nil {
		t.Fatalf("marshal sensor create body: %v", err)
	}

	resp, respBytes = doRequest(http.MethodPost, ts.URL+"/v1/digital-twins", ownerToken, "req-post-sensor-01", sensorCreateBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for owner POST Sensor, got %d: %s", resp.StatusCode, string(respBytes))
	}
	if wantLoc := "/v1/digital-twins/" + url.PathEscape(sensorURN); resp.Header.Get("Location") != wantLoc {
		t.Errorf("Location = %q, want %q", resp.Header.Get("Location"), wantLoc)
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		t.Errorf("Content-Type = %q, want application/json", resp.Header.Get("Content-Type"))
	}
	if resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Pragma") != "no-cache" {
		t.Errorf("expected no-store/no-cache headers, got Cache-Control=%q, Pragma=%q", resp.Header.Get("Cache-Control"), resp.Header.Get("Pragma"))
	}
	if resp.Header.Get("X-Request-ID") != "req-post-sensor-01" {
		t.Errorf("X-Request-ID = %q, want req-post-sensor-01", resp.Header.Get("X-Request-ID"))
	}

	var sensorCreateResp struct {
		EntityID  string  `json:"entity_id"`
		GatewayID string  `json:"gateway_id"`
		SensorID  string  `json:"sensor_id"`
		Name      string  `json:"name"`
		Unit      *string `json:"unit"`
	}
	if err := json.Unmarshal(respBytes, &sensorCreateResp); err != nil {
		t.Fatalf("unmarshal sensor create response: %v", err)
	}
	if sensorCreateResp.EntityID != sensorURN || sensorCreateResp.SensorID != sensorID || sensorCreateResp.GatewayID != gwID {
		t.Fatalf("unexpected sensor create body: %+v", sensorCreateResp)
	}

	// Exact replay POST Sensor -> 200 OK
	resp, respBytes = doRequest(http.MethodPost, ts.URL+"/v1/digital-twins", ownerToken, "req-post-sensor-replay", sensorCreateBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for exact replay POST Sensor, got %d: %s", resp.StatusCode, string(respBytes))
	}
	if loc := resp.Header.Get("Location"); loc != "" {
		t.Errorf("Location on replay = %q, want empty", loc)
	}
	var sensorReplayResp struct {
		EntityID  string `json:"entity_id"`
		GatewayID string `json:"gateway_id"`
		SensorID  string `json:"sensor_id"`
	}
	if err := json.Unmarshal(respBytes, &sensorReplayResp); err != nil {
		t.Fatalf("unmarshal sensor replay response: %v", err)
	}
	if sensorReplayResp.EntityID != sensorURN || sensorReplayResp.SensorID != sensorID {
		t.Fatalf("unexpected sensor replay body: %+v", sensorReplayResp)
	}

	// -------------------------------------------------------------------------
	// 10. Seed small deterministic telemetry batch using persistRepo.Persist
	// -------------------------------------------------------------------------
	persistRepo, err := telemetry.NewPostgresRepository(appPool, 5*time.Second)
	if err != nil {
		t.Fatalf("create telemetry postgres repo failed: %v", err)
	}

	validator, err := telemetry.NewValidator(telemetry.Limits{
		MaxPayloadBytes:  1 << 20,
		MaxSamples:       1000,
		MaxBatchDuration: time.Hour,
		MaxFutureSkew:    time.Hour,
	})
	if err != nil {
		t.Fatalf("create telemetry validator failed: %v", err)
	}

	baseTime := time.Date(2026, 10, 17, 20, 0, 0, 0, time.UTC)
	const sampleCount = 5
	const intervalUS = int64(100000) // 100ms
	samples := []float64{25.1, 25.2, 25.3, 25.4, 25.5}
	firstSeq := int64(5001)
	msgUUID := uuid.New()
	bootUUID := uuid.New()

	batchDTO := telemetry.BatchDTO{
		ProtocolVersion:  1,
		MessageID:        msgUUID,
		MessageType:      "telemetry_batch",
		GatewayID:        gwID,
		SensorID:         sensorID,
		BootID:           bootUUID,
		FirstSequence:    firstSeq,
		SampleCount:      int32(sampleCount),
		MeasuredAt:       baseTime.Format(time.RFC3339Nano),
		SampleIntervalUS: intervalUS,
		Samples:          samples,
	}

	batchBytes, err := json.Marshal(batchDTO)
	if err != nil {
		t.Fatalf("marshal batch DTO failed: %v", err)
	}

	topic := fmt.Sprintf("gateways/%s/telemetry/%s", gwID, sensorID)
	validatedMsg, err := validator.Validate(topic, batchBytes, baseTime.Add(time.Second))
	if err != nil {
		t.Fatalf("validate telemetry message failed: %v", err)
	}

	commitRes, err := persistRepo.Persist(ctx, validatedMsg)
	if err != nil {
		t.Fatalf("persistRepo.Persist failed: %v", err)
	}
	if commitRes.Outcome != telemetry.CommitOutcomeCommitted || commitRes.SamplesSaved != sampleCount {
		t.Fatalf("unexpected persist outcome: outcome=%v, samplesSaved=%d", commitRes.Outcome, commitRes.SamplesSaved)
	}

	// -------------------------------------------------------------------------
	// 11. Exercise all five GET routes via Owner JWT
	// -------------------------------------------------------------------------
	// Raw window must be <= History.RawWindow (default 10s) for auto mode to select raw points
	rawTimeFrom := baseTime.Add(-2 * time.Second).Format(time.RFC3339)
	rawTimeTo := baseTime.Add(2 * time.Second).Format(time.RFC3339)
	bucketTimeFrom := baseTime.Add(-time.Minute).Format(time.RFC3339)
	bucketTimeTo := baseTime.Add(time.Minute).Format(time.RFC3339)

	// Route 1a: GET /v1/digital-twins/{entity_id}/temporal (raw measurement, window <= 10s)
	telemRawURL := fmt.Sprintf("%s/v1/digital-twins/%s/temporal?attrs=measurement&timerel=between&timeAt=%s&endTimeAt=%s",
		ts.URL, url.PathEscape(sensorURN), url.QueryEscape(rawTimeFrom), url.QueryEscape(rawTimeTo))
	resp, respBytes = doRequest(http.MethodGet, telemRawURL, ownerToken, "req-telem-raw-01", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/digital-twins/{entity_id}/temporal raw failed with %d: %s", resp.StatusCode, string(respBytes))
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/ld+json") {
		t.Errorf("Content-Type = %q, want application/ld+json", resp.Header.Get("Content-Type"))
	}
	if resp.Header.Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", resp.Header.Get("Cache-Control"))
	}

	var telemRawResp struct {
		Measurement struct {
			Type   string  `json:"type"`
			Values [][]any `json:"values"`
		} `json:"measurement"`
	}
	if err := json.Unmarshal(respBytes, &telemRawResp); err != nil {
		t.Fatalf("unmarshal telemetry raw history: %v", err)
	}
	if len(telemRawResp.Measurement.Values) != sampleCount {
		t.Fatalf("expected %d raw points in values array, got %d. Body: %s", sampleCount, len(telemRawResp.Measurement.Values), string(respBytes))
	}
	firstTuple := telemRawResp.Measurement.Values[0]
	if len(firstTuple) != 2 || firstTuple[0].(float64) != 25.1 {
		t.Fatalf("unexpected first point tuple: %+v", firstTuple)
	}

	// Verify old route GET /v1/telemetry/history is removed and returns 404
	oldTelemURL := fmt.Sprintf("%s/v1/telemetry/history?gateway_id=%s&sensor_id=%s&from=%s&to=%s",
		ts.URL, gwID, sensorID, url.QueryEscape(rawTimeFrom), url.QueryEscape(rawTimeTo))
	respOldTelem, _ := doRequest(http.MethodGet, oldTelemURL, ownerToken, "req-telem-old", nil)
	if respOldTelem.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for removed /telemetry/history route, got %d", respOldTelem.StatusCode)
	}

	// Route 1b: GET /v1/digital-twins/{entity_id}/temporal (bucket downsampled measurement, PT1S)
	telemBucketURL := fmt.Sprintf("%s/v1/digital-twins/%s/temporal?attrs=measurement&timerel=between&timeAt=%s&endTimeAt=%s&aggrPeriodDuration=PT1S",
		ts.URL, url.PathEscape(sensorURN), url.QueryEscape(bucketTimeFrom), url.QueryEscape(bucketTimeTo))
	resp, respBytes = doRequest(http.MethodGet, telemBucketURL, ownerToken, "req-telem-bucket-01", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/digital-twins/{entity_id}/temporal bucket failed with %d: %s", resp.StatusCode, string(respBytes))
	}
	var telemBucketResp struct {
		Measurement struct {
			Type       string  `json:"type"`
			Avg        [][]any `json:"avg"`
			TotalCount [][]any `json:"totalCount"`
		} `json:"measurement"`
	}
	if err := json.Unmarshal(respBytes, &telemBucketResp); err != nil {
		t.Fatalf("unmarshal telemetry bucket history: %v", err)
	}
	if len(telemBucketResp.Measurement.TotalCount) != 1 {
		t.Fatalf("expected 1 bucket point in totalCount, got %d", len(telemBucketResp.Measurement.TotalCount))
	}
	if totalCountVal := telemBucketResp.Measurement.TotalCount[0][0].(float64); totalCountVal != 5 {
		t.Fatalf("expected count 5 in bucket, got %v", totalCountVal)
	}

	// Route 2: GET /v1/digital-twins (list)
	twinListURL := fmt.Sprintf("%s/v1/digital-twins?gateway_id=%s", ts.URL, gwID)
	resp, respBytes = doRequest(http.MethodGet, twinListURL, ownerToken, "req-twin-list-01", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/digital-twins failed with %d: %s", resp.StatusCode, string(respBytes))
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/ld+json") {
		t.Errorf("Content-Type = %q, want application/ld+json", resp.Header.Get("Content-Type"))
	}
	var twinListResp []struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	}
	if err := json.Unmarshal(respBytes, &twinListResp); err != nil {
		t.Fatalf("unmarshal twin list: %v", err)
	}
	if len(twinListResp) != 2 {
		t.Fatalf("expected 2 items in twin list for gateway, got %d", len(twinListResp))
	}

	// Route 3: GET /v1/digital-twins/{entity_id} (detail)
	// Must return application/ld+json; charset=utf-8 per Stage 4 contract!
	gwDetailURL := fmt.Sprintf("%s/v1/digital-twins/%s", ts.URL, url.PathEscape(gwURN))
	resp, respBytes = doRequest(http.MethodGet, gwDetailURL, ownerToken, "req-twin-detail-gw-01", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/digital-twins/{entity_id} Gateway detail failed with %d: %s", resp.StatusCode, string(respBytes))
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/ld+json") {
		t.Errorf("Content-Type = %q, want application/ld+json", resp.Header.Get("Content-Type"))
	}
	if resp.Header.Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", resp.Header.Get("Cache-Control"))
	}
	var gwDetailResp struct {
		Context any    `json:"@context"`
		ID      string `json:"id"`
		Type    string `json:"type"`
	}
	if err := json.Unmarshal(respBytes, &gwDetailResp); err != nil {
		t.Fatalf("unmarshal gateway detail: %v", err)
	}
	if gwDetailResp.Context == nil || gwDetailResp.ID != gwURN || gwDetailResp.Type != "Gateway" {
		t.Fatalf("unexpected gateway detail representation: %+v", gwDetailResp)
	}

	// Sensor detail
	sensorDetailURL := fmt.Sprintf("%s/v1/digital-twins/%s", ts.URL, url.PathEscape(sensorURN))
	resp, respBytes = doRequest(http.MethodGet, sensorDetailURL, ownerToken, "req-twin-detail-sensor-01", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/digital-twins/{entity_id} Sensor detail failed with %d: %s", resp.StatusCode, string(respBytes))
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/ld+json") {
		t.Errorf("Content-Type = %q, want application/ld+json", resp.Header.Get("Content-Type"))
	}
	var sensorDetailResp struct {
		ID          string `json:"id"`
		Type        string `json:"type"`
		Measurement *struct {
			Type       string  `json:"type"`
			Value      float64 `json:"value"`
			ObservedAt string  `json:"observedAt"`
			UnitCode   string  `json:"unitCode"`
		} `json:"measurement"`
		MeasurementUnit *struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		} `json:"measurementUnit"`
		SampleIntervalUS *struct {
			Type       string  `json:"type"`
			Value      float64 `json:"value"`
			ObservedAt string  `json:"observedAt"`
		} `json:"sample_interval_us"`
	}
	if err := json.Unmarshal(respBytes, &sensorDetailResp); err != nil {
		t.Fatalf("unmarshal sensor detail: %v", err)
	}
	if sensorDetailResp.Measurement == nil {
		t.Fatalf("expected measurement property in sensor detail, got nil")
	}
	if sensorDetailResp.Measurement.Value != 25.5 {
		t.Errorf("sensor measurement.value = %v, want 25.5", sensorDetailResp.Measurement.Value)
	}
	if sensorDetailResp.MeasurementUnit == nil || sensorDetailResp.MeasurementUnit.Value != "celsius" {
		t.Errorf("sensor measurementUnit = %+v, want celsius", sensorDetailResp.MeasurementUnit)
	}
	if sensorDetailResp.SampleIntervalUS == nil {
		t.Fatalf("expected sample_interval_us property in sensor detail, got nil")
	}
	if sensorDetailResp.SampleIntervalUS.Value != 100000 {
		t.Errorf("sensor sample_interval_us.value = %v, want 100000", sensorDetailResp.SampleIntervalUS.Value)
	}

	// Route 4: GET /v1/digital-twins/{entity_id}?format=simplified (state view)
	sensorStateURL := fmt.Sprintf("%s/v1/digital-twins/%s?format=simplified", ts.URL, url.PathEscape(sensorURN))
	resp, respBytes = doRequest(http.MethodGet, sensorStateURL, ownerToken, "req-twin-state-01", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/digital-twins/{entity_id}?format=simplified failed with %d: %s", resp.StatusCode, string(respBytes))
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		t.Errorf("Content-Type = %q, want application/json", resp.Header.Get("Content-Type"))
	}
	var sensorStateResp struct {
		ReportedVersion string         `json:"reported_version"`
		ReportedState   map[string]any `json:"reported_state"`
	}
	if err := json.Unmarshal(respBytes, &sensorStateResp); err != nil {
		t.Fatalf("unmarshal sensor state: %v", err)
	}
	if sensorStateResp.ReportedVersion != "1" {
		t.Fatalf("expected reported_version '1', got %q", sensorStateResp.ReportedVersion)
	}

	// Verify old /state route returns 404
	oldStateURL := fmt.Sprintf("%s/v1/digital-twins/%s/state", ts.URL, url.PathEscape(sensorURN))
	respOld, _ := doRequest(http.MethodGet, oldStateURL, ownerToken, "req-twin-state-old", nil)
	if respOld.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for removed /state route, got %d", respOld.StatusCode)
	}

	// Route 5: GET /v1/digital-twins/{entity_id}/temporal (sample_interval_us, window <= 10s for raw observation)
	twinIntervalHistURL := fmt.Sprintf("%s/v1/digital-twins/%s/temporal?attrs=sample_interval_us&timerel=between&timeAt=%s&endTimeAt=%s",
		ts.URL, url.PathEscape(sensorURN), url.QueryEscape(rawTimeFrom), url.QueryEscape(rawTimeTo))
	resp, respBytes = doRequest(http.MethodGet, twinIntervalHistURL, ownerToken, "req-twin-hist-int-01", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/digital-twins/{entity_id}/temporal property=sample_interval_us failed with %d: %s", resp.StatusCode, string(respBytes))
	}
	var twinIntervalHistResp struct {
		SampleIntervalUS struct {
			Type   string  `json:"type"`
			Values [][]any `json:"values"`
		} `json:"sample_interval_us"`
	}
	if err := json.Unmarshal(respBytes, &twinIntervalHistResp); err != nil {
		t.Fatalf("unmarshal twin interval history: %v", err)
	}
	if len(twinIntervalHistResp.SampleIntervalUS.Values) != 1 {
		t.Fatalf("expected 1 raw point in interval history, got count=%d. Body: %s", len(twinIntervalHistResp.SampleIntervalUS.Values), string(respBytes))
	}
	if intervalObsVal := twinIntervalHistResp.SampleIntervalUS.Values[0][0].(float64); intervalObsVal != float64(intervalUS) {
		t.Fatalf("expected interval observation %f, got %f. Body: %s", float64(intervalUS), intervalObsVal, string(respBytes))
	}

	// Verify old route GET /v1/digital-twins/{entity_id}/history is removed and returns 404
	oldHistURL := fmt.Sprintf("%s/v1/digital-twins/%s/history?property=value&from=%s&to=%s",
		ts.URL, url.PathEscape(sensorURN), url.QueryEscape(rawTimeFrom), url.QueryEscape(rawTimeTo))
	respOldHist, _ := doRequest(http.MethodGet, oldHistURL, ownerToken, "req-twin-hist-old", nil)
	if respOldHist.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for removed /history route, got %d", respOldHist.StatusCode)
	}

	// -------------------------------------------------------------------------
	// 12. PATCH Gateway description as admin and Sensor name as owner; GET metadata again
	// -------------------------------------------------------------------------
	gwAttrsURL := gwDetailURL + "/attrs"
	sensorAttrsURL := sensorDetailURL + "/attrs"

	// Verify old root route /v1/digital-twins/:entity_id returns 405 Method Not Allowed
	respOldPatch, _ := doRequest(http.MethodPatch, gwDetailURL, adminToken, "req-patch-gw-old", []byte(`{"description":{"type":"Property","value":"Old"}}`))
	if respOldPatch.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed for old root PATCH, got %d", respOldPatch.StatusCode)
	}

	patchGwBody, err := json.Marshal(map[string]any{
		"description": map[string]any{
			"type":  "Property",
			"value": "Patched Gateway Description",
		},
	})
	if err != nil {
		t.Fatalf("marshal patch gw body: %v", err)
	}
	resp, respBytes = doRequest(http.MethodPatch, gwAttrsURL, adminToken, "req-patch-gw-01", patchGwBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("admin PATCH Gateway description failed with %d: %s", resp.StatusCode, string(respBytes))
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		t.Errorf("Content-Type = %q, want application/json", resp.Header.Get("Content-Type"))
	}
	if resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Pragma") != "no-cache" {
		t.Errorf("expected no-store/no-cache headers, got Cache-Control=%q, Pragma=%q", resp.Header.Get("Cache-Control"), resp.Header.Get("Pragma"))
	}

	var patchGwResp struct {
		ID          string  `json:"id"`
		Type        string  `json:"type"`
		GatewayID   string  `json:"gateway_id"`
		Name        string  `json:"name"`
		Description *string `json:"description"`
		UpdatedAt   string  `json:"updated_at"`
	}
	if err := json.Unmarshal(respBytes, &patchGwResp); err != nil {
		t.Fatalf("unmarshal patch gw response: %v", err)
	}
	if patchGwResp.Description == nil || *patchGwResp.Description != "Patched Gateway Description" {
		t.Fatalf("unexpected patch gw response: %+v", patchGwResp)
	}

	patchSensorBody, err := json.Marshal(map[string]any{
		"name": map[string]any{
			"type":  "Property",
			"value": "Patched Sensor Name",
		},
	})
	if err != nil {
		t.Fatalf("marshal patch sensor body: %v", err)
	}
	resp, respBytes = doRequest(http.MethodPatch, sensorAttrsURL, ownerToken, "req-patch-sensor-01", patchSensorBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("owner PATCH Sensor name failed with %d: %s", resp.StatusCode, string(respBytes))
	}
	var patchSensorResp struct {
		ID        string `json:"id"`
		Type      string `json:"type"`
		SensorID  string `json:"sensor_id"`
		Name      string `json:"name"`
		UpdatedAt string `json:"updated_at"`
	}
	if err := json.Unmarshal(respBytes, &patchSensorResp); err != nil {
		t.Fatalf("unmarshal patch sensor response: %v", err)
	}
	if patchSensorResp.Name != "Patched Sensor Name" {
		t.Fatalf("unexpected patch sensor response: %+v", patchSensorResp)
	}

	// Verify updated metadata via GET detail again
	resp, respBytes = doRequest(http.MethodGet, gwDetailURL, ownerToken, "req-verify-gw-meta", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET Gateway detail after patch failed with %d: %s", resp.StatusCode, string(respBytes))
	}
	var gwDetailUpdated struct {
		Description struct {
			Value *string `json:"value"`
		} `json:"description"`
	}
	if err := json.Unmarshal(respBytes, &gwDetailUpdated); err != nil {
		t.Fatalf("unmarshal patched gateway detail: %v", err)
	}
	if gwDetailUpdated.Description.Value == nil || *gwDetailUpdated.Description.Value != "Patched Gateway Description" {
		t.Fatalf("expected updated description in detail graph, got %+v", gwDetailUpdated.Description)
	}

	resp, respBytes = doRequest(http.MethodGet, sensorDetailURL, ownerToken, "req-verify-sensor-meta", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET Sensor detail after patch failed with %d: %s", resp.StatusCode, string(respBytes))
	}
	var sensorDetailUpdated struct {
		Name struct {
			Value string `json:"value"`
		} `json:"name"`
	}
	if err := json.Unmarshal(respBytes, &sensorDetailUpdated); err != nil {
		t.Fatalf("unmarshal patched sensor detail: %v", err)
	}
	if sensorDetailUpdated.Name.Value != "Patched Sensor Name" {
		t.Fatalf("expected updated sensor name 'Patched Sensor Name', got %q", sensorDetailUpdated.Name.Value)
	}

	// -------------------------------------------------------------------------
	// 13. Denials: Real user B (non-member) read denial and viewer/operator mutation denial
	// -------------------------------------------------------------------------
	// User B (non-member) GET detail -> 404
	resp, respBytes = doRequest(http.MethodGet, gwDetailURL, userBToken, "req-userb-gw-detail", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for non-member user B, got %d: %s", resp.StatusCode, string(respBytes))
	}
	var userBErr struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(respBytes, &userBErr); err != nil {
		t.Fatalf("unmarshal user B error response: %v", err)
	}
	if userBErr.Error.Code != "not_found" || userBErr.Error.RequestID != "req-userb-gw-detail" {
		t.Fatalf("unexpected user B error envelope: %+v", userBErr)
	}

	// User B (non-member) GET telemetry history -> 404
	resp, _ = doRequest(http.MethodGet, telemRawURL, userBToken, "req-userb-telem-raw", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for non-member user B telemetry history, got %d", resp.StatusCode)
	}

	// Viewer mutation denial:
	// Viewer PATCH Sensor -> 403 Forbidden
	resp, _ = doRequest(http.MethodPatch, sensorAttrsURL, viewerToken, "req-viewer-patch-sensor", []byte(`{"name":{"type":"Property","value":"Hacked Sensor"}}`))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for viewer PATCH sensor, got %d", resp.StatusCode)
	}

	// Viewer POST Sensor -> 403 Forbidden
	resp, _ = doRequest(http.MethodPost, ts.URL+"/v1/digital-twins", viewerToken, "req-viewer-post-sensor", sensorCreateBody)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for viewer POST sensor, got %d", resp.StatusCode)
	}

	// Viewer PATCH Gateway -> 403 Forbidden
	resp, _ = doRequest(http.MethodPatch, gwAttrsURL, viewerToken, "req-viewer-patch-gw", []byte(`{"description":{"type":"Property","value":"Hacked Desc"}}`))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for viewer PATCH gateway, got %d", resp.StatusCode)
	}

	// Operator mutation denial:
	// Operator PATCH Sensor -> 403 Forbidden
	resp, _ = doRequest(http.MethodPatch, sensorAttrsURL, operatorToken, "req-operator-patch-sensor", []byte(`{"name":{"type":"Property","value":"Operator Sensor"}}`))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for operator PATCH sensor, got %d", resp.StatusCode)
	}

	// Operator POST Sensor -> 403 Forbidden
	resp, _ = doRequest(http.MethodPost, ts.URL+"/v1/digital-twins", operatorToken, "req-operator-post-sensor", sensorCreateBody)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for operator POST sensor, got %d", resp.StatusCode)
	}

	// Operator PATCH Gateway -> 403 Forbidden
	resp, _ = doRequest(http.MethodPatch, gwAttrsURL, operatorToken, "req-operator-patch-gw", []byte(`{"description":{"type":"Property","value":"Operator Desc"}}`))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for operator PATCH gateway, got %d", resp.StatusCode)
	}

	// -------------------------------------------------------------------------
	// 14. Target DB side-effect inspection: verify zero state tampering from denied calls
	// -------------------------------------------------------------------------
	var currentSensorName string
	if err := setupPool.QueryRow(ctx, `SELECT name FROM public.sensors WHERE gateway_id = $1 AND sensor_id = $2`, gwID, sensorID).Scan(&currentSensorName); err != nil {
		t.Fatalf("failed to query sensor name after denials: %v", err)
	}
	if currentSensorName != "Patched Sensor Name" {
		t.Fatalf("sensor name was tampered by denied mutations: %q", currentSensorName)
	}

	var currentGwDesc *string
	if err := setupPool.QueryRow(ctx, `SELECT description FROM public.gateways WHERE gateway_id = $1`, gwID).Scan(&currentGwDesc); err != nil {
		t.Fatalf("failed to query gateway description after denials: %v", err)
	}
	if currentGwDesc == nil || *currentGwDesc != "Patched Gateway Description" {
		t.Fatalf("gateway description was tampered by denied mutations: %v", currentGwDesc)
	}

	var sensorCount int
	if err := setupPool.QueryRow(ctx, `SELECT count(*) FROM public.sensors WHERE gateway_id = $1`, gwID).Scan(&sensorCount); err != nil {
		t.Fatalf("failed to query sensor count after denials: %v", err)
	}
	if sensorCount != 1 {
		t.Fatalf("unexpected sensor count after denied creation: %d", sensorCount)
	}

	t.Logf("Task 4.6.7 native HTTP smoke successfully completed with zero side-effects on run %s", runID)
}

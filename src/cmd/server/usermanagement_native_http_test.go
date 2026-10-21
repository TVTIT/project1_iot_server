package main

import (
	"bytes"
	"context"
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
	"iot-platform/internal/config"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/usermanagement"
)

type userManagementNativeTestConfig struct {
	SetupURL       string
	BackendURL     string
	AuthBaseURL    string
	AnonKey        string
	ServiceRoleKey string
	JWTSecret      string
	JWTIssuer      string
	JWTAudience    string
}

// Never attach upstream bodies or reader errors to token-response diagnostics.
func readSafeLoginResponse(status int, body io.Reader) ([]byte, error) {
	if status != http.StatusOK {
		return nil, fmt.Errorf("login failed: HTTP %d", status)
	}
	b, err := io.ReadAll(io.LimitReader(body, (1<<20)+1))
	if err != nil || len(b) > 1<<20 {
		return nil, fmt.Errorf("login response unreadable or oversized")
	}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if json.Unmarshal(b, &token) != nil || token.AccessToken == "" {
		return nil, fmt.Errorf("invalid login token response")
	}
	return b, nil
}

func TestLoginFailureDoesNotExposeTokens(t *testing.T) {
	const sentinel = "secret-token-sentinel"
	for _, tc := range []struct {
		status int
		body   string
	}{
		{200, `{"access_token":"` + sentinel + `",`},
		{200, `{"refresh_token":"` + sentinel + `"}`},
		{503, `{"access_token":"` + sentinel + `"}`},
		{200, strings.Repeat(sentinel, 60000)},
	} {
		b, err := readSafeLoginResponse(tc.status, strings.NewReader(tc.body))
		if err == nil || b != nil {
			t.Fatal("unsafe response accepted")
		}
		if strings.Contains(err.Error(), sentinel) {
			t.Fatal("token leaked in diagnostic")
		}
	}
}

func resolveUserManagementNativeTestConfig(t *testing.T) userManagementNativeTestConfig {
	t.Helper()

	optIn := os.Getenv("USER_MANAGEMENT_HTTP_TEST_NATIVE") == "1" ||
		os.Getenv("ADMIN_USER_HTTP_TEST_NATIVE") == "1" ||
		os.Getenv("USER_MGMT_HTTP_TEST_NATIVE") == "1"

	if !optIn {
		t.Skip("skipping native user management E2E HTTP test: USER_MANAGEMENT_HTTP_TEST_NATIVE=1 not set")
	}

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to determine current directory: %v", err)
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

	envMap := make(map[string]string)
	if envPath != "" {
		if content, err := os.ReadFile(envPath); err == nil {
			for _, line := range strings.Split(string(content), "\n") {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(trimmed, "#") || !strings.Contains(trimmed, "=") {
					continue
				}
				parts := strings.SplitN(trimmed, "=", 2)
				envMap[strings.TrimSpace(parts[0])] = strings.Trim(strings.TrimSpace(parts[1]), `"'`)
			}
		}
	}

	getVal := func(envKey string, defaultVal string) string {
		if v := os.Getenv(envKey); v != "" {
			return v
		}
		if v, ok := envMap[envKey]; ok && v != "" {
			return v
		}
		return defaultVal
	}

	pgUser := getVal("POSTGRES_USER", "postgres")
	pgPass := getVal("POSTGRES_PASSWORD", "")
	pgDB := getVal("POSTGRES_DB", "iot_platform")
	pgPort := getVal("POSTGRES_PORT", "5432")
	backendPass := getVal("BACKEND_DB_PASSWORD", "")

	setupURL := os.Getenv("TWIN_TEST_SETUP_URL")
	if setupURL == "" && pgPass != "" {
		setupURL = fmt.Sprintf("postgres://%s:%s@127.0.0.1:%s/%s?sslmode=disable", pgUser, pgPass, pgPort, pgDB)
	}

	backendURL := os.Getenv("TWIN_TEST_BACKEND_URL")
	if backendURL == "" && backendPass != "" {
		backendURL = fmt.Sprintf("postgres://iot_backend_app:%s@127.0.0.1:%s/%s?sslmode=disable", backendPass, pgPort, pgDB)
	}

	authBaseURL := getVal("AUTH_ADMIN_BASE_URL", "")
	if authBaseURL == "" {
		authBaseURL = getVal("SUPABASE_URL", "http://127.0.0.1/auth/v1")
	}
	if strings.Contains(authBaseURL, "supabase-envoy") {
		// When test runs on host, use Nginx forwarder on 127.0.0.1:80
		authBaseURL = "http://127.0.0.1/auth/v1"
	}

	anonKey := getVal("ANON_KEY", "")
	serviceRoleKey := getVal("SUPABASE_SERVICE_ROLE_KEY", "")
	if serviceRoleKey == "" {
		serviceRoleKey = getVal("SERVICE_ROLE_KEY", "")
	}

	jwtSecret := getVal("JWT_SECRET", "")
	jwtIssuer := getVal("GOTRUE_JWT_ISSUER", "https://auth.example.com")
	jwtAudience := getVal("SUPABASE_JWT_AUDIENCE", "authenticated")

	if setupURL == "" || backendURL == "" {
		t.Fatalf("native test opted in but database URLs unavailable (setup=%t, backend=%t)", setupURL != "", backendURL != "")
	}
	if serviceRoleKey == "" || anonKey == "" {
		t.Fatalf("native test opted in but service role key or anon key unavailable (srk=%t, anon=%t)", serviceRoleKey != "", anonKey != "")
	}
	if jwtSecret == "" {
		t.Fatalf("native test opted in but JWT_SECRET is not configured")
	}

	return userManagementNativeTestConfig{
		SetupURL:       setupURL,
		BackendURL:     backendURL,
		AuthBaseURL:    authBaseURL,
		AnonKey:        anonKey,
		ServiceRoleKey: serviceRoleKey,
		JWTSecret:      jwtSecret,
		JWTIssuer:      jwtIssuer,
		JWTAudience:    jwtAudience,
	}
}

type userManagementSmokeFixtureIDs struct {
	RunID            string
	GatewayIDs       []string
	AllUserIDs       []uuid.UUID
	AllUserEmails    []string
	AuthUsersPresent *bool
}

func performUserManagementSmokeFixtureCleanup(ctx context.Context, t *testing.T, pool *pgxpool.Pool, f userManagementSmokeFixtureIDs) {
	t.Helper()
	// Emails are registered before POST, so response loss cannot orphan accounts.
	for _, email := range f.AllUserEmails {
		rows, err := pool.Query(ctx, "SELECT id FROM auth.users WHERE email = $1", email)
		if err != nil {
			t.Error("resolve owned account cleanup failed")
			continue
		}
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				t.Error("scan owned account cleanup failed")
				break
			}
			f.AllUserIDs = append(f.AllUserIDs, id)
		}
		if rows.Err() != nil {
			t.Error("read owned account cleanup failed")
		}
		rows.Close()
	}

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

	if len(f.GatewayIDs) > 0 {
		execCleanup("twin_outbox", `
			DELETE FROM public.twin_outbox
			WHERE command_id IN (
				SELECT command_id FROM public.twin_commands
				WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))
			)`, f.GatewayIDs)

		execCleanup("twin_commands", `
			DELETE FROM public.twin_commands
			WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`,
			f.GatewayIDs)

		execCleanup("gateway_mqtt_credentials", `
			DELETE FROM public.gateway_mqtt_credentials WHERE gateway_id = ANY($1)`, f.GatewayIDs)

		execCleanup("gateway_mqtt_credential_events", `
			DELETE FROM public.gateway_mqtt_credential_events WHERE gateway_id = ANY($1)`, f.GatewayIDs)

		execCleanup("telemetry", `DELETE FROM public.telemetry WHERE gateway_id = ANY($1)`, f.GatewayIDs)
		execCleanup("processed_messages", `DELETE FROM public.processed_messages WHERE gateway_id = ANY($1)`, f.GatewayIDs)

		execCleanup("twin_temporal_values", `
			DELETE FROM public.twin_temporal_values
			WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`,
			f.GatewayIDs)

		execCleanup("twin_states", `
			DELETE FROM public.twin_states
			WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`,
			f.GatewayIDs)

		execCleanup("twin_relationships", `
			DELETE FROM public.twin_relationships
			WHERE source_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))
			   OR target_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`,
			f.GatewayIDs)

		execCleanup("twin_entities", `DELETE FROM public.twin_entities WHERE gateway_id = ANY($1)`, f.GatewayIDs)
		execCleanup("sensors", `DELETE FROM public.sensors WHERE gateway_id = ANY($1)`, f.GatewayIDs)
	}

	if len(f.GatewayIDs) > 0 || len(f.AllUserIDs) > 0 {
		execCleanup("user_gateways", `DELETE FROM public.user_gateways WHERE gateway_id = ANY($1) OR user_id = ANY($2)`, f.GatewayIDs, f.AllUserIDs)
	}

	if len(f.GatewayIDs) > 0 {
		execCleanup("gateways", `DELETE FROM public.gateways WHERE gateway_id = ANY($1)`, f.GatewayIDs)
	}

	if len(f.AllUserIDs) > 0 {
		execCleanup("platform_admins", `DELETE FROM public.platform_admins WHERE user_id = ANY($1)`, f.AllUserIDs)
		execCleanup("profiles", `DELETE FROM public.profiles WHERE id = ANY($1)`, f.AllUserIDs)

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
	}

	if len(f.GatewayIDs) > 0 {
		verifyCount("gateways", `SELECT COUNT(*) FROM public.gateways WHERE gateway_id = ANY($1)`, f.GatewayIDs)
		verifyCount("sensors", `SELECT COUNT(*) FROM public.sensors WHERE gateway_id = ANY($1)`, f.GatewayIDs)
		verifyCount("twin_entities", `SELECT COUNT(*) FROM public.twin_entities WHERE gateway_id = ANY($1)`, f.GatewayIDs)
	}
	if len(f.AllUserIDs) > 0 {
		verifyCount("platform_admins", `SELECT COUNT(*) FROM public.platform_admins WHERE user_id = ANY($1)`, f.AllUserIDs)
		verifyCount("profiles", `SELECT COUNT(*) FROM public.profiles WHERE id = ANY($1)`, f.AllUserIDs)
		if f.AuthUsersPresent != nil && *f.AuthUsersPresent {
			verifyCount("auth.users", `SELECT COUNT(*) FROM auth.users WHERE id = ANY($1)`, f.AllUserIDs)
		}
	}
	for _, email := range f.AllUserEmails {
		verifyCount("owned auth email", "SELECT count(*) FROM auth.users WHERE email = $1", email)
	}
}

func TestNativeUserManagement_E2EFlow(t *testing.T) {
	testCfg := resolveUserManagementNativeTestConfig(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	setupPool, err := pgxpool.New(ctx, testCfg.SetupURL)
	if err != nil {
		t.Fatalf("setup database pool connection failed: %v", err)
	}
	defer setupPool.Close()

	appPool, err := pgxpool.New(ctx, testCfg.BackendURL)
	if err != nil {
		t.Fatalf("application database pool connection failed: %v", err)
	}
	defer appPool.Close()

	// 1. Verify target database versions, schema version, and application role
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
	if schemaVersion < 22 {
		t.Fatalf("expected schema version >= 22, got %d", schemaVersion)
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

	// 2. Define unique run identifiers and register cleanup BEFORE any writes
	runID := uuid.New().String()[:8]
	gwID := fmt.Sprintf("gw_um_%s", runID)
	gwURN := fmt.Sprintf("urn:ngsi-ld:Gateway:%s", gwID)
	sensorID := fmt.Sprintf("sens_um_%s", runID)

	adminAUID := uuid.New()
	adminBUID := uuid.New()
	ownerOrigUID := uuid.New()
	userRegUID := uuid.New()

	trackedUserIDs := []uuid.UUID{adminAUID, adminBUID, ownerOrigUID, userRegUID}
	fixtureIDs := userManagementSmokeFixtureIDs{
		RunID:      runID,
		GatewayIDs: []string{gwID},
		AllUserIDs: trackedUserIDs,
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		performUserManagementSmokeFixtureCleanup(cleanupCtx, t, setupPool, fixtureIDs)
	}()

	// 3. Seed users into auth.users (if present) and public.profiles
	var hasAuthUsers bool
	if err := setupPool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
		t.Fatalf("failed to check auth.users table existence: %v", err)
	}
	fixtureIDs.AuthUsersPresent = &hasAuthUsers

	if hasAuthUsers {
		for _, uID := range trackedUserIDs {
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

	for _, uID := range trackedUserIDs {
		_, err := setupPool.Exec(ctx, `
			INSERT INTO public.profiles (id, full_name)
			VALUES ($1, $2)
			ON CONFLICT (id) DO NOTHING`,
			uID, fmt.Sprintf("TestUser %s", uID.String()[:8]),
		)
		if err != nil {
			t.Fatalf("failed to insert profile %s: %v", uID, err)
		}
	}

	// AdminA and AdminB are in platform_admins
	for _, adminUID := range []uuid.UUID{adminAUID, adminBUID} {
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.platform_admins (user_id)
			VALUES ($1)
			ON CONFLICT (user_id) DO NOTHING`, adminUID); err != nil {
			t.Fatalf("failed to insert platform admin %s: %v", adminUID, err)
		}
	}

	// 4. Construct production TokenVerifier using real JWTSecret
	verifier, err := auth.NewHS256Verifier(auth.VerifierConfig{
		Secret:   testCfg.JWTSecret,
		Issuer:   testCfg.JWTIssuer,
		Audience: testCfg.JWTAudience,
	})
	if err != nil {
		t.Fatalf("create HS256 verifier failed: %v", err)
	}

	signJWT := func(sub uuid.UUID, role string) string {
		now := time.Now()
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"sub":  sub.String(),
			"iss":  testCfg.JWTIssuer,
			"aud":  testCfg.JWTAudience,
			"iat":  now.Unix(),
			"exp":  now.Add(5 * time.Minute).Unix(),
			"role": role,
		})
		s, err := tok.SignedString([]byte(testCfg.JWTSecret))
		if err != nil {
			t.Fatalf("failed to sign test JWT: %v", err)
		}
		return s
	}

	adminAToken := signJWT(adminAUID, "authenticated")
	adminBToken := signJWT(adminBUID, "authenticated")
	ownerOrigToken := signJWT(ownerOrigUID, "authenticated")
	userRegToken := signJWT(userRegUID, "authenticated")

	// 5. Construct production dependencies and HTTP router using appPool
	cfg := validTestConfig()
	cfg.CORSAllowedOrigins = []string{"http://localhost:3000"}
	cfg.UserManagement = config.UserManagementConfig{
		AuthBaseURL:      testCfg.AuthBaseURL,
		ServiceRoleKey:   testCfg.ServiceRoleKey,
		OperationTimeout: 10 * time.Second,
		MaxBodyBytes:     16384,
		UserList: usermanagement.UserListConfig{
			MaxUsers:         1000,
			MaxMemberships:   10000,
			MaxResponseBytes: 2097152,
		},
	}

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
		CORSAllowedOrigins:         cfg.CORSAllowedOrigins,
	}

	if err := populateStage4Dependencies(&routerDeps, cfg, appPool, provService); err != nil {
		t.Fatalf("populateStage4Dependencies failed: %v", err)
	}
	if err := populateUserManagementDependencies(&routerDeps, cfg, appPool); err != nil {
		t.Fatalf("populateUserManagementDependencies failed: %v", err)
	}

	router, err := httpserver.NewRouter(routerDeps)
	if err != nil {
		t.Fatalf("NewRouter failed: %v", err)
	}

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
			t.Fatalf("HTTP request %s %s failed: %v", method, rawURL, err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		respBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read response body %s %s failed: %v", method, rawURL, err)
		}
		return resp, respBytes
	}

	// -------------------------------------------------------------------------
	// Step A: Negative / Security Baseline across admin user endpoints
	// -------------------------------------------------------------------------
	adminEndpoints := []struct {
		name   string
		method string
		path   string
		body   []byte
	}{
		{
			name:   "create user",
			method: http.MethodPost,
			path:   "/v1/admin/users",
			body:   []byte(`{"full_name":"Test Name","email":"test@example.com","password":"ValidPassword123!"}`),
		},
		{
			name:   "list users",
			method: http.MethodGet,
			path:   "/v1/admin/users",
			body:   nil,
		},
		{
			name:   "update user",
			method: http.MethodPatch,
			path:   "/v1/admin/users/" + uuid.New().String(),
			body:   []byte(`{"full_name":"New Name"}`),
		},
		{
			name:   "assign gateway user",
			method: http.MethodPut,
			path:   "/v1/admin/gateways/" + gwID + "/users/" + uuid.New().String(),
			body:   []byte(`{"role":"viewer"}`),
		},
		{
			name: "remove gateway user", method: http.MethodDelete,
			path: "/v1/admin/gateways/" + gwID + "/users/" + uuid.NewString(),
		},
		{
			name:   "transfer gateway owner",
			method: http.MethodPut,
			path:   "/v1/admin/gateways/" + gwID + "/owner",
			body:   []byte(`{"owner_user_id":"` + uuid.New().String() + `"}`),
		},
	}

	for _, ep := range adminEndpoints {
		var errEnv struct {
			Error struct {
				Code      string `json:"code"`
				Message   string `json:"message"`
				RequestID string `json:"request_id"`
			} `json:"error"`
		}

		// Missing JWT -> 401
		resp, respBytes := doRequest(ep.method, ts.URL+ep.path, "", "req-no-jwt", ep.body)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("[%s] expected 401 for missing JWT, got %d: %s", ep.name, resp.StatusCode, string(respBytes))
		}
		if err := json.Unmarshal(respBytes, &errEnv); err != nil || errEnv.Error.Code != "unauthorized" {
			t.Errorf("[%s] expected unauthorized error envelope, got: %s", ep.name, string(respBytes))
		}
		if resp.Header.Get("Cache-Control") != "no-store" {
			t.Errorf("[%s] expected Cache-Control: no-store on 401, got %q", ep.name, resp.Header.Get("Cache-Control"))
		}

		// Invalid JWT -> 401
		resp, _ = doRequest(ep.method, ts.URL+ep.path, "invalid.jwt.token", "req-invalid-jwt", ep.body)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("[%s] expected 401 for invalid JWT, got %d", ep.name, resp.StatusCode)
		}

		// Non-admin human user -> 403
		resp, respBytes = doRequest(ep.method, ts.URL+ep.path, userRegToken, "req-non-admin", ep.body)
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("[%s] expected 403 for non-admin user, got %d: %s", ep.name, resp.StatusCode, string(respBytes))
		}
		if err := json.Unmarshal(respBytes, &errEnv); err != nil || errEnv.Error.Code != "forbidden" {
			t.Errorf("[%s] expected forbidden error envelope, got: %s", ep.name, string(respBytes))
		}
		if resp.Header.Get("Cache-Control") != "no-store" {
			t.Errorf("[%s] expected Cache-Control: no-store on 403, got %q", ep.name, resp.Header.Get("Cache-Control"))
		}
	}

	// -------------------------------------------------------------------------
	// Step B: Admin HTTP Create User (POST /v1/admin/users)
	// -------------------------------------------------------------------------
	aliceEmail := fmt.Sprintf("alice_%s@example.com", runID)
	alicePassword := "Valid_" + strings.ReplaceAll(uuid.NewString()+uuid.NewString(), "-", "") + "!"
	// Register exact owned email before POST, including uncertain creation outcomes.
	fixtureIDs.AllUserEmails = append(fixtureIDs.AllUserEmails, aliceEmail)

	createBody, _ := json.Marshal(map[string]string{
		"full_name": "Alice Smith",
		"email":     aliceEmail,
		"password":  alicePassword,
	})

	resp, respBytes := doRequest(http.MethodPost, ts.URL+"/v1/admin/users", adminAToken, "req-create-alice", createBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for POST /v1/admin/users, got %d: %s", resp.StatusCode, string(respBytes))
	}

	if resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Pragma") != "no-cache" {
		t.Errorf("expected no-store and no-cache on POST /v1/admin/users, got Cache-Control=%q, Pragma=%q",
			resp.Header.Get("Cache-Control"), resp.Header.Get("Pragma"))
	}

	var aliceCreated struct {
		ID           string    `json:"id"`
		FullName     string    `json:"full_name"`
		Email        string    `json:"email"`
		PlatformRole string    `json:"platform_role"`
		CreatedAt    time.Time `json:"created_at"`
	}
	if err := json.Unmarshal(respBytes, &aliceCreated); err != nil {
		t.Fatalf("unmarshal create user response failed: %v", err)
	}

	aliceUID, err := uuid.Parse(aliceCreated.ID)
	if err != nil || aliceUID == uuid.Nil {
		t.Fatalf("invalid user UUID in response: %q", aliceCreated.ID)
	}

	// Register created Alice for strict cleanup
	fixtureIDs.AllUserIDs = append(fixtureIDs.AllUserIDs, aliceUID)

	if wantLoc := "/v1/admin/users/" + aliceUID.String(); resp.Header.Get("Location") != wantLoc {
		t.Errorf("Location header = %q, want %q", resp.Header.Get("Location"), wantLoc)
	}
	if aliceCreated.FullName != "Alice Smith" {
		t.Errorf("full_name = %q, want canonical trimmed %q", aliceCreated.FullName, "Alice Smith")
	}
	if aliceCreated.Email != aliceEmail {
		t.Errorf("email = %q, want %q", aliceCreated.Email, aliceEmail)
	}
	if aliceCreated.PlatformRole != "user" {
		t.Errorf("platform_role = %q, want user (no initial global admin)", aliceCreated.PlatformRole)
	}
	if aliceCreated.CreatedAt.IsZero() {
		t.Errorf("expected non-zero created_at")
	}

	// Verify body DTO contains NO secrets
	rawCreateStr := string(respBytes)
	for _, forbidden := range []string{"password", "secret", "token"} {
		if strings.Contains(strings.ToLower(rawCreateStr), `"`+forbidden+`"`) {
			t.Errorf("create user response leaked secret field %q: %s", forbidden, rawCreateStr)
		}
	}

	// Verify profile in DB has canonical name and zero initial platform admin or memberships
	var dbName string
	if err := appPool.QueryRow(ctx, "SELECT full_name FROM public.profiles WHERE id = $1", aliceUID).Scan(&dbName); err != nil {
		t.Fatalf("failed to query profile in DB: %v", err)
	}
	if dbName != "Alice Smith" {
		t.Errorf("DB profile full_name = %q, want %q", dbName, "Alice Smith")
	}
	var adminCount, memberCount int
	if err := appPool.QueryRow(ctx, "SELECT count(*) FROM public.platform_admins WHERE user_id = $1", aliceUID).Scan(&adminCount); err != nil {
		t.Fatal("query initial admin count failed")
	}
	if err := appPool.QueryRow(ctx, "SELECT count(*) FROM public.user_gateways WHERE user_id = $1", aliceUID).Scan(&memberCount); err != nil {
		t.Fatal("query initial membership count failed")
	}
	if adminCount != 0 || memberCount != 0 {
		t.Errorf("expected 0 initial admin and memberships, got admin=%d, member=%d", adminCount, memberCount)
	}

	// Negative: Duplicate email creation returns 409 Conflict
	resp, respBytes = doRequest(http.MethodPost, ts.URL+"/v1/admin/users", adminAToken, "req-create-dup", createBody)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for duplicate email, got %d: %s", resp.StatusCode, string(respBytes))
	}
	var dupErr struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(respBytes, &dupErr); err != nil || dupErr.Error.Code != "user_already_exists" {
		t.Errorf("expected user_already_exists error code, got: %s", string(respBytes))
	}

	// -------------------------------------------------------------------------
	// Step C: Confirmed Account Password Login via Envoy (/auth/v1/token?grant_type=password)
	// -------------------------------------------------------------------------
	loginEndpoint := strings.TrimRight(testCfg.AuthBaseURL, "/")
	if !strings.HasSuffix(loginEndpoint, "/auth/v1") {
		loginEndpoint += "/auth/v1"
	}
	loginEndpoint += "/token?grant_type=password"

	loginPayload, _ := json.Marshal(map[string]string{
		"email":    aliceEmail,
		"password": alicePassword,
	})

	loginReq, err := http.NewRequestWithContext(ctx, http.MethodPost, loginEndpoint, bytes.NewReader(loginPayload))
	if err != nil {
		t.Fatalf("create login request failed: %v", err)
	}
	loginReq.Header.Set("Content-Type", "application/json")
	loginReq.Header.Set("apikey", testCfg.AnonKey)

	loginResp, err := httpClient.Do(loginReq)
	if err != nil {
		t.Fatalf("login request failed: %v", err)
	}
	defer func() {
		_ = loginResp.Body.Close()
	}()
	loginBytes, readErr := readSafeLoginResponse(loginResp.StatusCode, loginResp.Body)
	if readErr != nil {
		t.Fatal(readErr)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		User        struct {
			ID    string `json:"id"`
			Email string `json:"email"`
		} `json:"user"`
	}
	if err := json.Unmarshal(loginBytes, &tokenResp); err != nil || tokenResp.AccessToken == "" {
		t.Fatal("invalid login token response")
	}
	if tokenResp.User.ID != aliceUID.String() {
		t.Errorf("login user ID = %q, want %q", tokenResp.User.ID, aliceUID.String())
	}

	// Verify token verifies with production TokenVerifier
	alicePrincipal, err := verifier.Verify(ctx, tokenResp.AccessToken)
	if err != nil {
		t.Fatalf("production TokenVerifier rejected GoTrue issued token: %v", err)
	}
	if alicePrincipal.UserID != aliceUID {
		t.Fatalf("verified principal UserID = %s, want %s", alicePrincipal.UserID, aliceUID)
	}
	aliceLiveToken := tokenResp.AccessToken

	// Simulate losing the successful create response: cleanup knows only the
	// exact email registered before POST, not the returned account UUID.
	lostEmail := "lost_response_" + runID + "@example.com"
	lostFixture := userManagementSmokeFixtureIDs{AllUserEmails: []string{lostEmail}, AuthUsersPresent: &hasAuthUsers}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		performUserManagementSmokeFixtureCleanup(cctx, t, setupPool, lostFixture)
	}()
	lostBody, err := json.Marshal(map[string]string{"full_name": "Lost Response Fixture", "email": lostEmail, "password": alicePassword})
	if err != nil {
		t.Fatal("encode owned fixture request failed")
	}
	lostResp, _ := doRequest(http.MethodPost, ts.URL+"/v1/admin/users", adminAToken, "req-lost-response", lostBody)
	if lostResp.StatusCode != http.StatusCreated {
		t.Fatal("create cleanup fixture failed")
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	performUserManagementSmokeFixtureCleanup(cleanupCtx, t, setupPool, lostFixture)
	cleanupCancel()

	// -------------------------------------------------------------------------
	// Step D: Admin List Users (GET /v1/admin/users)
	// -------------------------------------------------------------------------
	resp, respBytes = doRequest(http.MethodGet, ts.URL+"/v1/admin/users", adminAToken, "req-list-users", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for GET /v1/admin/users, got %d: %s", resp.StatusCode, string(respBytes))
	}
	if resp.Header.Get("Cache-Control") != "no-store" {
		t.Errorf("expected Cache-Control: no-store on GET /v1/admin/users, got %q", resp.Header.Get("Cache-Control"))
	}

	var listResp struct {
		Items []struct {
			ID           string `json:"id"`
			FullName     string `json:"full_name"`
			Email        string `json:"email"`
			PlatformRole string `json:"platform_role"`
			Memberships  []struct {
				GatewayID string `json:"gateway_id"`
				Role      string `json:"role"`
			} `json:"gateway_roles"`
		} `json:"items"`
	}
	if err := json.Unmarshal(respBytes, &listResp); err != nil {
		t.Fatalf("unmarshal list users response failed: %v", err)
	}

	var foundAlice bool
	for _, u := range listResp.Items {
		if u.ID == aliceUID.String() {
			foundAlice = true
			if u.FullName != "Alice Smith" {
				t.Errorf("listed Alice full_name = %q, want 'Alice Smith'", u.FullName)
			}
			if u.Email != aliceEmail {
				t.Errorf("listed Alice email = %q, want %q", u.Email, aliceEmail)
			}
			if u.PlatformRole != "user" {
				t.Errorf("listed Alice platform_role = %q, want 'user'", u.PlatformRole)
			}
			if len(u.Memberships) != 0 {
				t.Errorf("listed Alice memberships count = %d, want 0", len(u.Memberships))
			}
		}
	}
	if !foundAlice {
		t.Fatalf("created user %s not found in GET /v1/admin/users list (%d total users)", aliceUID, len(listResp.Items))
	}

	// -------------------------------------------------------------------------
	// Step E: Rename User (PATCH /v1/admin/users/:user_id)
	// -------------------------------------------------------------------------
	renameBody, _ := json.Marshal(map[string]string{
		"full_name": "Alice Smith Updated",
	})
	resp, respBytes = doRequest(http.MethodPatch, ts.URL+"/v1/admin/users/"+aliceUID.String(), adminAToken, "req-rename-alice", renameBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for rename PATCH, got %d: %s", resp.StatusCode, string(respBytes))
	}
	if resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Pragma") != "no-cache" {
		t.Errorf("expected no-store and no-cache on PATCH, got Cache-Control=%q, Pragma=%q",
			resp.Header.Get("Cache-Control"), resp.Header.Get("Pragma"))
	}
	var aliceRenamed struct {
		FullName     string `json:"full_name"`
		PlatformRole string `json:"platform_role"`
	}
	_ = json.Unmarshal(respBytes, &aliceRenamed)
	if aliceRenamed.FullName != "Alice Smith Updated" {
		t.Errorf("renamed full_name = %q, want 'Alice Smith Updated'", aliceRenamed.FullName)
	}

	// In DB: profile updated
	if err := appPool.QueryRow(ctx, "SELECT full_name FROM public.profiles WHERE id = $1", aliceUID).Scan(&dbName); err != nil {
		t.Fatal("profile assertion query failed")
	}
	if dbName != "Alice Smith Updated" {
		t.Errorf("DB full_name = %q, want 'Alice Smith Updated'", dbName)
	}

	// -------------------------------------------------------------------------
	// Step F: Grant platform_admin from DIFFERENT actor (AdminB)
	// -------------------------------------------------------------------------
	grantBody, _ := json.Marshal(map[string]string{
		"platform_role": "platform_admin",
	})
	resp, respBytes = doRequest(http.MethodPatch, ts.URL+"/v1/admin/users/"+aliceUID.String(), adminBToken, "req-grant-alice", grantBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for grant platform_admin from AdminB, got %d: %s", resp.StatusCode, string(respBytes))
	}
	var aliceGranted struct {
		PlatformRole string `json:"platform_role"`
	}
	_ = json.Unmarshal(respBytes, &aliceGranted)
	if aliceGranted.PlatformRole != "platform_admin" {
		t.Errorf("granted platform_role = %q, want 'platform_admin'", aliceGranted.PlatformRole)
	}

	// In DB: public.platform_admins has row
	var isNowAdmin bool
	if err := appPool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM public.platform_admins WHERE user_id = $1)", aliceUID).Scan(&isNowAdmin); err != nil {
		t.Fatal("admin assertion query failed")
	}
	if !isNowAdmin {
		t.Fatalf("DB public.platform_admins row missing for granted user %s", aliceUID)
	}

	// -------------------------------------------------------------------------
	// Step G: Next request permissions reflect grant immediately
	// -------------------------------------------------------------------------
	resp, respBytes = doRequest(http.MethodGet, ts.URL+"/v1/admin/users", aliceLiveToken, "req-alice-admin-read", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for newly granted admin on next request, got %d: %s", resp.StatusCode, string(respBytes))
	}

	// -------------------------------------------------------------------------
	// Step H: Revoke platform_admin from DIFFERENT actor (AdminB)
	// -------------------------------------------------------------------------
	revokeBody, _ := json.Marshal(map[string]string{
		"platform_role": "user",
	})
	resp, respBytes = doRequest(http.MethodPatch, ts.URL+"/v1/admin/users/"+aliceUID.String(), adminBToken, "req-revoke-alice", revokeBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for revoke platform_admin from AdminB, got %d: %s", resp.StatusCode, string(respBytes))
	}
	var aliceRevoked struct {
		PlatformRole string `json:"platform_role"`
	}
	_ = json.Unmarshal(respBytes, &aliceRevoked)
	if aliceRevoked.PlatformRole != "user" {
		t.Errorf("revoked platform_role = %q, want 'user'", aliceRevoked.PlatformRole)
	}

	// In DB: row removed from platform_admins
	if err := appPool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM public.platform_admins WHERE user_id = $1)", aliceUID).Scan(&isNowAdmin); err != nil {
		t.Fatal("admin assertion query failed")
	}
	if isNowAdmin {
		t.Fatalf("DB public.platform_admins row still present after revoke for %s", aliceUID)
	}

	// -------------------------------------------------------------------------
	// Step I: Next request permissions reflect revoke immediately
	// -------------------------------------------------------------------------
	resp, respBytes = doRequest(http.MethodGet, ts.URL+"/v1/admin/users", aliceLiveToken, "req-alice-revoked-read", nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for revoked admin on next request, got %d: %s", resp.StatusCode, string(respBytes))
	}

	// -------------------------------------------------------------------------
	// Step J: Combined self-demotion atomic denial
	// -------------------------------------------------------------------------
	var adminAOriginalName *string
	if err := appPool.QueryRow(ctx, "SELECT full_name FROM public.profiles WHERE id = $1", adminAUID).Scan(&adminAOriginalName); err != nil {
		t.Fatal("actor assertion query failed")
	}

	selfDemoteBody, _ := json.Marshal(map[string]string{
		"full_name":     "Hacked Admin A",
		"platform_role": "user",
	})
	resp, respBytes = doRequest(http.MethodPatch, ts.URL+"/v1/admin/users/"+adminAUID.String(), adminAToken, "req-self-demote", selfDemoteBody)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for self-demotion, got %d: %s", resp.StatusCode, string(respBytes))
	}
	var selfDemoteErr struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(respBytes, &selfDemoteErr); err != nil || selfDemoteErr.Error.Code != "self_admin_demotion_forbidden" {
		t.Errorf("expected code self_admin_demotion_forbidden, got: %s", string(respBytes))
	}

	// DB verification: AdminA full_name must NOT have changed (atomic rollback)
	var adminACurrentName *string
	if err := appPool.QueryRow(ctx, "SELECT full_name FROM public.profiles WHERE id = $1", adminAUID).Scan(&adminACurrentName); err != nil {
		t.Fatal("actor assertion query failed")
	}
	if (adminACurrentName == nil) != (adminAOriginalName == nil) || (adminACurrentName != nil && adminAOriginalName != nil && *adminACurrentName != *adminAOriginalName) {
		t.Error("AdminA full_name was mutated despite self-demotion rejection")
	}

	// -------------------------------------------------------------------------
	// Step K: Provision Gateway with OwnerOriginal
	// -------------------------------------------------------------------------
	gwCreateBody, _ := json.Marshal(map[string]any{
		"entity_type":   "Gateway",
		"gateway_id":    gwID,
		"name":          "UserMgmt Test Gateway",
		"owner_user_id": ownerOrigUID.String(),
	})
	resp, respBytes = doRequest(http.MethodPost, ts.URL+"/v1/digital-twins", adminAToken, "req-create-gw", gwCreateBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for provisioning gateway, got %d: %s", resp.StatusCode, string(respBytes))
	}

	// -------------------------------------------------------------------------
	// Step L: Assign Viewer / Operator (PUT /v1/admin/gateways/:gateway_id/users/:user_id)
	// -------------------------------------------------------------------------
	// 1. Assign viewer to Alice
	assignViewerBody, _ := json.Marshal(map[string]string{
		"role": "viewer",
	})
	resp, respBytes = doRequest(http.MethodPut, ts.URL+"/v1/admin/gateways/"+gwID+"/users/"+aliceUID.String(), adminAToken, "req-assign-viewer", assignViewerBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for assign viewer, got %d: %s", resp.StatusCode, string(respBytes))
	}
	if resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Pragma") != "no-cache" {
		t.Errorf("expected no-store and no-cache on assign gateway user, got Cache-Control=%q, Pragma=%q",
			resp.Header.Get("Cache-Control"), resp.Header.Get("Pragma"))
	}
	var assignResp struct {
		GatewayID string `json:"gateway_id"`
		UserID    string `json:"user_id"`
		Role      string `json:"role"`
	}
	_ = json.Unmarshal(respBytes, &assignResp)
	if assignResp.Role != "viewer" {
		t.Errorf("assigned role = %q, want 'viewer'", assignResp.Role)
	}

	// In DB: user_gateways row exists with role viewer
	var dbRole string
	var dbCreatedAt time.Time
	if err := appPool.QueryRow(ctx, "SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", gwID, aliceUID).Scan(&dbRole, &dbCreatedAt); err != nil {
		t.Fatalf("failed to query user_gateways: %v", err)
	}
	if dbRole != "viewer" {
		t.Errorf("DB user_gateways role = %q, want 'viewer'", dbRole)
	}

	// 2. Negative: Attempting to assign "owner" role via this endpoint is denied
	assignOwnerBody, _ := json.Marshal(map[string]string{
		"role": "owner",
	})
	resp, respBytes = doRequest(http.MethodPut, ts.URL+"/v1/admin/gateways/"+gwID+"/users/"+aliceUID.String(), adminAToken, "req-assign-owner-bad", assignOwnerBody)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for role 'owner', got %d: %s", resp.StatusCode, string(respBytes))
	}
	var ownerDenyErr struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(respBytes, &ownerDenyErr)
	if ownerDenyErr.Error.Code != "invalid_role" {
		t.Errorf("expected code invalid_role for owner assignment, got %s", ownerDenyErr.Error.Code)
	}

	// 3. Update to operator role
	assignOperatorBody, _ := json.Marshal(map[string]string{
		"role": "operator",
	})
	resp, respBytes = doRequest(http.MethodPut, ts.URL+"/v1/admin/gateways/"+gwID+"/users/"+aliceUID.String(), adminAToken, "req-assign-operator", assignOperatorBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for assign operator, got %d: %s", resp.StatusCode, string(respBytes))
	}
	var dbCreatedAtAfterUpdate time.Time
	if err := appPool.QueryRow(ctx, "SELECT role, created_at FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", gwID, aliceUID).Scan(&dbRole, &dbCreatedAtAfterUpdate); err != nil {
		t.Fatal("membership assertion query failed")
	}
	if dbRole != "operator" {
		t.Errorf("DB role = %q, want 'operator'", dbRole)
	}
	if !dbCreatedAtAfterUpdate.Equal(dbCreatedAt) {
		t.Errorf("created_at was modified on role update: before=%v, after=%v", dbCreatedAt, dbCreatedAtAfterUpdate)
	}

	// -------------------------------------------------------------------------
	// Step M: Transfer Gateway Owner (PUT /v1/admin/gateways/:gateway_id/owner)
	// -------------------------------------------------------------------------
	transferBody, _ := json.Marshal(map[string]string{
		"user_id": aliceUID.String(),
	})
	resp, respBytes = doRequest(http.MethodPut, ts.URL+"/v1/admin/gateways/"+gwID+"/owner", adminAToken, "req-transfer-owner", transferBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for transfer gateway owner, got %d: %s", resp.StatusCode, string(respBytes))
	}
	if resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Pragma") != "no-cache" {
		t.Errorf("expected no-store and no-cache on transfer owner, got Cache-Control=%q, Pragma=%q",
			resp.Header.Get("Cache-Control"), resp.Header.Get("Pragma"))
	}
	var transferResp struct {
		GatewayID   string `json:"gateway_id"`
		OwnerUserID string `json:"owner_user_id"`
	}
	_ = json.Unmarshal(respBytes, &transferResp)
	if transferResp.OwnerUserID != aliceUID.String() {
		t.Errorf("transfer response owner_user_id = %q, want %q", transferResp.OwnerUserID, aliceUID.String())
	}

	// In DB:
	// Alice is now owner
	if err := appPool.QueryRow(ctx, "SELECT role FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", gwID, aliceUID).Scan(&dbRole); err != nil {
		t.Fatal("owner assertion query failed")
	}
	if dbRole != "owner" {
		t.Errorf("Alice role in DB = %q, want 'owner'", dbRole)
	}
	// OwnerOriginal is now viewer
	var origRole string
	if err := appPool.QueryRow(ctx, "SELECT role FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", gwID, ownerOrigUID).Scan(&origRole); err != nil {
		t.Fatal("old owner assertion query failed")
	}
	if origRole != "viewer" {
		t.Errorf("Old owner role in DB = %q, want 'viewer'", origRole)
	}
	// Exactly one owner for gwID
	var ownerCount int
	if err := appPool.QueryRow(ctx, "SELECT count(*) FROM public.user_gateways WHERE gateway_id = $1 AND role = 'owner'", gwID).Scan(&ownerCount); err != nil {
		t.Fatal("owner count assertion query failed")
	}
	resp, respBytes = doRequest(http.MethodGet, ts.URL+"/v1/admin/users", adminAToken, "req-list-owner-readback", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatal("owner membership list readback failed")
	}
	listResp.Items = nil
	if err := json.Unmarshal(respBytes, &listResp); err != nil {
		t.Fatal("owner membership list decode failed")
	}
	matchedOwner, matchedViewer := false, false
	for _, u := range listResp.Items {
		for _, m := range u.Memberships {
			if m.GatewayID == gwID {
				if u.ID == aliceUID.String() && m.Role == "owner" {
					matchedOwner = true
				}
				if u.ID == ownerOrigUID.String() && m.Role == "viewer" {
					matchedViewer = true
				}
			}
		}
	}
	if !matchedOwner || !matchedViewer {
		t.Fatal("gateway_roles does not reflect owner transfer")
	}
	if ownerCount != 1 {
		t.Errorf("expected exactly 1 owner in DB, got %d", ownerCount)
	}

	// -------------------------------------------------------------------------
	// Step N: Read Gateway / Sensor / Twin data as relevant accounts
	// -------------------------------------------------------------------------
	// 1. New sole owner (Alice) can provision sensor
	sensorCreateBody, _ := json.Marshal(map[string]string{
		"name": "Temperature Sensor",
		"unit": "degC",
	})
	resp, respBytes = doRequest(http.MethodPut, ts.URL+"/v1/gateways/"+gwID+"/sensors/"+sensorID, aliceLiveToken, "req-alice-prov-sensor", sensorCreateBody)
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 201/200 for new owner provisioning sensor, got %d: %s", resp.StatusCode, string(respBytes))
	}

	// 2. New owner (Alice) can read Gateway Twin
	resp, respBytes = doRequest(http.MethodGet, ts.URL+"/v1/digital-twins/"+url.PathEscape(gwURN), aliceLiveToken, "req-alice-read-twin", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for Alice reading Gateway Twin, got %d: %s", resp.StatusCode, string(respBytes))
	}
	if !strings.Contains(string(respBytes), sensorID) {
		t.Errorf("Gateway Twin does not contain sensor %s in hasSensor: %s", sensorID, string(respBytes))
	}

	// 3. Old owner (OwnerOriginal, now viewer) retains read access to Gateway Twin
	resp, respBytes = doRequest(http.MethodGet, ts.URL+"/v1/digital-twins/"+url.PathEscape(gwURN), ownerOrigToken, "req-old-owner-read-twin", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for old owner (now viewer) reading Gateway Twin, got %d: %s", resp.StatusCode, string(respBytes))
	}

	// 4. Old owner (now viewer) CANNOT provision a sensor (viewer cannot write)
	deniedSensorID := fmt.Sprintf("sens_denied_%s", runID)
	resp, respBytes = doRequest(http.MethodPut, ts.URL+"/v1/gateways/"+gwID+"/sensors/"+deniedSensorID, ownerOrigToken, "req-old-owner-write-sensor", sensorCreateBody)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for viewer provisioning sensor, got %d: %s", resp.StatusCode, string(respBytes))
	}

	// 5. Non-member (userRegToken) CANNOT read Gateway Twin -> 404 Not Found
	resp, respBytes = doRequest(http.MethodGet, ts.URL+"/v1/digital-twins/"+url.PathEscape(gwURN), userRegToken, "req-non-member-read-twin", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for non-member reading Gateway Twin, got %d: %s", resp.StatusCode, string(respBytes))
	}

	// 6. Admin non-member (adminAToken) CANNOT read Gateway Twin -> 404 Not Found (no blanket admin read bypass)
	resp, respBytes = doRequest(http.MethodGet, ts.URL+"/v1/digital-twins/"+url.PathEscape(gwURN), adminAToken, "req-admin-nonmember-read-twin", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for admin non-member reading Gateway Twin (no blanket admin bypass), got %d: %s", resp.StatusCode, string(respBytes))
	}

	// -------------------------------------------------------------------------
	// Step O: Unknown Fields & Capacity Exceeded Bounds
	// -------------------------------------------------------------------------
	// Unknown field in POST /v1/admin/users -> 400 Bad Request
	badFieldBody, _ := json.Marshal(map[string]any{
		"full_name":   "Bad Request User",
		"email":       "bad@example.com",
		"password":    "ValidPassword123!",
		"extra_field": "disallowed",
	})
	resp, respBytes = doRequest(http.MethodPost, ts.URL+"/v1/admin/users", adminAToken, "req-bad-field", badFieldBody)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for extra field, got %d: %s", resp.StatusCode, string(respBytes))
	}

	// Capacity Exceeded: configure server with MaxUsers: 1
	capacityCfg := cfg
	capacityCfg.UserManagement.UserList.MaxUsers = 1
	capacityDeps, err := composeUserManagementDependencies(capacityCfg, appPool)
	if err != nil {
		t.Fatalf("compose capacity user management deps failed: %v", err)
	}
	capacityRouterDeps := routerDeps
	capacityDeps.apply(&capacityRouterDeps)
	capacityRouter, err := httpserver.NewRouter(capacityRouterDeps)
	if err != nil {
		t.Fatalf("create capacity router failed: %v", err)
	}
	capacityTS := httptest.NewServer(capacityRouter)
	defer capacityTS.Close()

	resp, respBytes = doRequest(http.MethodGet, capacityTS.URL+"/v1/admin/users", adminAToken, "req-cap-overflow", nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for capacity exceeded, got %d: %s", resp.StatusCode, string(respBytes))
	}
	var capErr struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(respBytes, &capErr); err != nil || capErr.Error.Code != "user_list_capacity_exceeded" {
		t.Errorf("expected code user_list_capacity_exceeded, got: %s", string(respBytes))
	}
	if strings.Contains(string(respBytes), `"items"`) {
		t.Errorf("capacity error leaked partial users array: %s", string(respBytes))
	}

	// -------------------------------------------------------------------------
	// Step P: CORS Verification
	// -------------------------------------------------------------------------
	corsReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/v1/admin/users", nil)
	corsReq.Header.Set("Authorization", "Bearer "+adminAToken)
	corsReq.Header.Set("Origin", "http://localhost:3000")
	corsResp, err := httpClient.Do(corsReq)
	if err != nil {
		t.Fatalf("CORS request failed: %v", err)
	}
	_ = corsResp.Body.Close()
	if corsResp.Header.Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Errorf("expected Access-Control-Allow-Origin: http://localhost:3000, got %q", corsResp.Header.Get("Access-Control-Allow-Origin"))
	}

	// Step Q: membership removal does not delete the account or bypass owner policy.
	otherGW := gwID + "_other"
	fixtureIDs.GatewayIDs = append(fixtureIDs.GatewayIDs, otherGW)
	otherBody, err := json.Marshal(map[string]string{"entity_type": "Gateway", "gateway_id": otherGW, "name": "Membership positive control", "owner_user_id": ownerOrigUID.String()})
	if err != nil {
		t.Fatal(err)
	}
	resp, _ = doRequest(http.MethodPost, ts.URL+"/v1/digital-twins", adminAToken, "req-other-gateway", otherBody)
	if resp.StatusCode != 201 {
		t.Fatal("other Gateway creation failed")
	}
	otherMemberURL := ts.URL + "/v1/admin/gateways/" + otherGW + "/users/" + aliceUID.String()
	resp, _ = doRequest(http.MethodPut, otherMemberURL, adminAToken, "req-other-membership", assignViewerBody)
	if resp.StatusCode != 200 {
		t.Fatal("other membership assignment failed")
	}
	memberURL := ts.URL + "/v1/admin/gateways/" + gwID + "/users/" + aliceUID.String()
	if _, err := setupPool.Exec(ctx, `UPDATE public.twin_states SET reported_state='{"membership_sentinel":"reported"}', desired_state='{"membership_sentinel":"desired"}', reported_version=7, desired_version=3 WHERE entity_id IN(SELECT id FROM public.twin_entities WHERE gateway_id=ANY($1))`, fixtureIDs.GatewayIDs); err != nil {
		t.Fatal("state sentinel setup failed")
	}
	protectedSnapshot := func(excludeTarget bool) string {
		t.Helper()
		var snapshot string
		err := setupPool.QueryRow(ctx, `SELECT jsonb_build_object(
		'account',(SELECT to_jsonb(x) FROM auth.users x WHERE id=$1),
		'profile',(SELECT to_jsonb(x) FROM public.profiles x WHERE id=$1),
		'admins',(SELECT jsonb_agg(to_jsonb(x) ORDER BY user_id) FROM public.platform_admins x WHERE user_id=ANY($3)),
		'members',(SELECT jsonb_agg(to_jsonb(x) ORDER BY gateway_id,user_id) FROM public.user_gateways x WHERE gateway_id=ANY($2) AND NOT($5 AND gateway_id=$4 AND user_id=$1)),
		'gateways',(SELECT jsonb_agg(to_jsonb(x) ORDER BY gateway_id) FROM public.gateways x WHERE gateway_id=ANY($2)),
		'sensors',(SELECT jsonb_agg(to_jsonb(x) ORDER BY gateway_id,sensor_id) FROM public.sensors x WHERE gateway_id=ANY($2)),
		'entities',(SELECT jsonb_agg(to_jsonb(x) ORDER BY id) FROM public.twin_entities x WHERE gateway_id=ANY($2)),
		'states',(SELECT jsonb_agg(to_jsonb(x) ORDER BY entity_id) FROM public.twin_states x WHERE entity_id IN(SELECT id FROM public.twin_entities WHERE gateway_id=ANY($2))),
		'relationships',(SELECT jsonb_agg(to_jsonb(x) ORDER BY source_entity_id,relationship_type,target_entity_id) FROM public.twin_relationships x WHERE source_entity_id IN(SELECT id FROM public.twin_entities WHERE gateway_id=ANY($2))),
		'raw',(SELECT jsonb_agg(to_jsonb(x) ORDER BY to_jsonb(x)::text) FROM public.telemetry x WHERE gateway_id=ANY($2)),
		'ledger',(SELECT jsonb_agg(to_jsonb(x) ORDER BY to_jsonb(x)::text) FROM public.processed_messages x WHERE gateway_id=ANY($2)),
		'temporal',(SELECT jsonb_agg(to_jsonb(x) ORDER BY to_jsonb(x)::text) FROM public.twin_temporal_values x WHERE entity_id IN(SELECT id FROM public.twin_entities WHERE gateway_id=ANY($2))),
		'commands',(SELECT jsonb_agg(to_jsonb(x) ORDER BY to_jsonb(x)::text) FROM public.twin_commands x WHERE entity_id IN(SELECT id FROM public.twin_entities WHERE gateway_id=ANY($2))),
		'outbox',(SELECT jsonb_agg(to_jsonb(x) ORDER BY to_jsonb(x)::text) FROM public.twin_outbox x WHERE command_id IN(SELECT command_id FROM public.twin_commands WHERE entity_id IN(SELECT id FROM public.twin_entities WHERE gateway_id=ANY($2)))),
		'credential_events',(SELECT jsonb_agg(to_jsonb(x) ORDER BY to_jsonb(x)::text) FROM public.gateway_mqtt_credential_events x WHERE gateway_id=ANY($2)),
		'credentials',(SELECT jsonb_agg(to_jsonb(x) ORDER BY gateway_id) FROM public.gateway_mqtt_credentials x WHERE gateway_id=ANY($2)))::text`, aliceUID, fixtureIDs.GatewayIDs, fixtureIDs.AllUserIDs, gwID, excludeTarget).Scan(&snapshot)
		if err != nil {
			t.Fatal("protected snapshot failed")
		}
		return snapshot
	}
	assertRead := func(gatewayID, token string, status int) {
		t.Helper()
		for _, path := range []string{"/v1/gateways/" + gatewayID + "/sensors", "/v1/digital-twins/" + url.PathEscape("urn:ngsi-ld:Gateway:"+gatewayID)} {
			r, b := doRequest(http.MethodGet, ts.URL+path, token, "req-membership-read", nil)
			if r.StatusCode != status {
				t.Fatalf("read after membership operation: status=%d want=%d body=%s", r.StatusCode, status, b)
			}
		}
	}
	beforeOwner := protectedSnapshot(false)
	resp, respBytes = doRequest(http.MethodDelete, memberURL, adminAToken, "req-deny-owner-delete", nil)
	if resp.StatusCode != 409 || !strings.Contains(string(respBytes), `"owner_transfer_required"`) {
		t.Fatal("owner removal was not blocked")
	}
	if protectedSnapshot(false) != beforeOwner {
		t.Fatal("owner denial changed protected data")
	}
	assertRead(gwID, aliceLiveToken, 200)
	// Transfer first: old owner becomes viewer, then can be removed.
	transferBack, err := json.Marshal(map[string]string{"user_id": ownerOrigUID.String()})
	if err != nil {
		t.Fatal(err)
	}
	resp, _ = doRequest(http.MethodPut, ts.URL+"/v1/admin/gateways/"+gwID+"/owner", adminAToken, "req-transfer-before-delete", transferBack)
	if resp.StatusCode != 200 {
		t.Fatal("transfer back failed")
	}
	for _, membershipRole := range []string{"viewer", "operator"} {
		roleBody, err := json.Marshal(map[string]string{"role": membershipRole})
		if err != nil {
			t.Fatal(err)
		}
		resp, _ = doRequest(http.MethodPut, memberURL, adminAToken, "req-grant-before-delete", roleBody)
		if resp.StatusCode != 200 {
			t.Fatal("membership assignment failed")
		}
		assertRead(gwID, aliceLiveToken, 200)
		before := protectedSnapshot(true)
		for repeat := 0; repeat < 2; repeat++ {
			resp, respBytes = doRequest(http.MethodDelete, memberURL, adminAToken, "req-remove-membership", nil)
			if resp.StatusCode != 204 || len(respBytes) != 0 || resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Pragma") != "no-cache" {
				t.Fatalf("DELETE %s/repeat=%d: status=%d", membershipRole, repeat, resp.StatusCode)
			}
		}
		if protectedSnapshot(true) != before {
			t.Fatal("membership DELETE changed account/global role/other Gateway/resource data")
		}
		assertRead(gwID, aliceLiveToken, 404)
		assertRead(otherGW, aliceLiveToken, 200)
		assertRead(gwID, ownerOrigToken, 200)
		var remaining int
		if err := appPool.QueryRow(ctx, "SELECT count(*) FROM public.user_gateways WHERE gateway_id=$1 AND user_id=$2", gwID, aliceUID).Scan(&remaining); err != nil || remaining != 0 {
			t.Fatal("membership row remains after DELETE")
		}
		resp, respBytes = doRequest(http.MethodGet, ts.URL+"/v1/admin/users", adminAToken, "req-revocation-list", nil)
		var users struct {
			Items []usermanagement.UserSummary `json:"items"`
		}
		if resp.StatusCode != 200 || json.Unmarshal(respBytes, &users) != nil {
			t.Fatal("admin list readback failed")
		}
		found := false
		for _, u := range users.Items {
			if u.ID == aliceUID {
				found = true
				if u.PlatformRole != "user" || u.Email != aliceEmail || u.FullName == nil || *u.FullName != "Alice Smith Updated" || len(u.GatewayRoles) != 1 || u.GatewayRoles[0].GatewayID != otherGW || u.GatewayRoles[0].Role != "viewer" {
					t.Error("admin user projection incorrect after removal")
				}
			}
		}
		if !found {
			t.Fatal("account disappeared from admin list")
		}
	}
	// A fresh password login still works, not merely a previously issued JWT.
	loginReq, err = http.NewRequestWithContext(ctx, http.MethodPost, loginEndpoint, bytes.NewReader(loginPayload))
	if err != nil {
		t.Fatal("login request creation failed")
	}
	loginReq.Header.Set("Content-Type", "application/json")
	loginReq.Header.Set("apikey", testCfg.AnonKey)
	loginResp, err = httpClient.Do(loginReq)
	if err != nil {
		t.Fatal("post-removal login unavailable")
	}
	loginBytes, readErr = readSafeLoginResponse(loginResp.StatusCode, loginResp.Body)
	_ = loginResp.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if json.Unmarshal(loginBytes, &tokenResp) != nil || tokenResp.User.ID != aliceUID.String() {
		t.Fatal("post-removal login identity mismatch")
	}
	// An admin may revoke their own non-owner membership without losing global authority.
	selfURL := ts.URL + "/v1/admin/gateways/" + gwID + "/users/" + adminAUID.String()
	resp, _ = doRequest(http.MethodPut, selfURL, adminAToken, "req-self-grant", assignViewerBody)
	if resp.StatusCode != 200 {
		t.Fatal("self assignment failed")
	}
	resp, respBytes = doRequest(http.MethodDelete, selfURL, adminAToken, "req-self-remove", nil)
	if resp.StatusCode != 204 || len(respBytes) != 0 {
		t.Fatal("self membership removal failed")
	}
	resp, _ = doRequest(http.MethodGet, ts.URL+"/v1/admin/users", adminAToken, "req-admin-still-authorized", nil)
	if resp.StatusCode != 200 {
		t.Fatal("self removal revoked global admin")
	}
}

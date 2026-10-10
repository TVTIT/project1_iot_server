package httpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
)

func TestGatewayHTTPAuthorizationIntegration(t *testing.T) {
	if os.Getenv("AUTHORIZATION_TEST_ISOLATED") != "1" {
		t.Skip("run sh scripts/test-stage2-authorization.sh")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	setup, err := pgxpool.New(ctx, os.Getenv("AUTHORIZATION_TEST_SETUP_URL"))
	if err != nil {
		t.Fatal("setup pool failed")
	}
	defer setup.Close()
	pool, err := pgxpool.New(ctx, os.Getenv("AUTHORIZATION_TEST_BACKEND_URL"))
	if err != nil {
		t.Fatal("app pool failed")
	}
	defer pool.Close()
	var role string
	if err := pool.QueryRow(ctx, "SELECT current_user").Scan(&role); err != nil || role != "iot_backend_app" {
		t.Fatal("wrong database identity")
	}
	a, b, admin, newUser := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	ga, gb := "fixture_"+uuid.NewString(), "fixture_"+uuid.NewString()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := setup.Exec(ctx, sql, args...); err != nil {
			t.Fatal("fixture setup failed")
		}
	}
	exec("INSERT INTO profiles(id) VALUES ($1),($2),($3),($4)", a, b, admin, newUser)
	defer func() {
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := setup.Exec(c, "DELETE FROM gateways WHERE gateway_id IN ($1,$2)", ga, gb); err != nil {
			t.Error("gateway cleanup failed")
		}
		if _, err := setup.Exec(c, "DELETE FROM profiles WHERE id IN ($1,$2,$3,$4)", a, b, admin, newUser); err != nil {
			t.Error("user cleanup failed")
		}
	}()
	exec("INSERT INTO gateways(gateway_id,name) VALUES ($1,'A'),($2,'B')", ga, gb)
	exec("INSERT INTO user_gateways(user_id,gateway_id,role) VALUES ($1,$2,'owner'),($3,$4,'operator')", a, ga, b, gb)
	exec("INSERT INTO platform_admins(user_id) VALUES ($1)", admin)
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal("random secret failed")
	}
	secret := []byte(hex.EncodeToString(random))
	verifier, err := auth.NewHS256Verifier(auth.VerifierConfig{Secret: string(secret), Issuer: "http://auth.test.local/auth/v1", Audience: "authenticated"})
	if err != nil {
		t.Fatal(err)
	}
	checker, err := auth.NewPostgresPlatformAdminChecker(pool)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := gateway.NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	service, err := gateway.NewService(repo, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	provisionRepo, err := gateway.NewPostgresProvisioningRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	provisionService, err := gateway.NewProvisioningService(provisionRepo, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	adminRepo, err := gateway.NewPostgresAdminGatewayRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	adminService, err := gateway.NewAdminGatewayService(adminRepo, time.Second, 100, 200)
	if err != nil {
		t.Fatal(err)
	}
	ownerSensorRepo, err := gateway.NewPostgresOwnerSensorProvisioningRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	ownerSensorService, err := gateway.NewOwnerSensorProvisioningService(ownerSensorRepo, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	router, err := NewRouter(RouterDependencies{SensorProvisioner: provisionService, GatewayProvisioner: provisionService, OwnerSensorProvisioner: ownerSensorService, AdminGatewayLister: adminService, AdminInventoryDefaultLimit: 100, AdminInventoryMaxLimit: 200, AdminMaxBodyBytes: 16384, ReadinessChecker: pool, ReadinessTimeout: time.Second, AuthorizationTimeout: time.Second, TokenVerifier: verifier, PlatformAdminChecker: checker, GatewayReader: service, SensorReader: service})
	if err != nil {
		t.Fatal(err)
	}
	token := func(id uuid.UUID) string {
		t.Helper()
		now := time.Now()
		s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": id.String(), "role": "authenticated", "iss": "http://auth.test.local/auth/v1", "aud": "authenticated", "iat": now.Unix(), "exp": now.Add(time.Minute).Unix()}).SignedString(secret)
		if err != nil {
			t.Fatal("sign fixture token failed")
		}
		return s
	}
	check := func(id uuid.UUID, want string) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/v1/gateways", nil).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer "+token(id))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("status=%d", rec.Code)
		}
		var response struct {
			Items []gatewayDTO `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal("bad response")
		}
		if want == "" {
			if response.Items == nil || len(response.Items) != 0 {
				t.Fatal("unmapped caller saw gateways")
			}
		} else {
			if len(response.Items) != 1 || response.Items[0].GatewayID != want {
				t.Fatal("caller isolation failed")
			}
		}
	}
	checkSensors := func(id uuid.UUID, parent string, status int, names ...string) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/v1/gateways/"+parent+"/sensors", nil).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer "+token(id))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != status {
			t.Fatalf("sensor status=%d want=%d", rec.Code, status)
		}
		if status != http.StatusOK {
			var body httpapi.APIErrorEnvelope
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal("bad error response")
			}
			if status == http.StatusNotFound && (body.Error.Code != "not_found" || body.Error.Message != "resource not found") {
				t.Fatal("sensor denial distinguishes missing/inaccessible")
			}
			return
		}
		var body struct {
			GatewayID string      `json:"gateway_id"`
			Items     []sensorDTO `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal("bad sensor response")
		}
		if body.GatewayID != parent || body.Items == nil || len(body.Items) != len(names) {
			t.Fatal("wrong sensor parent/count/empty representation")
		}
		for i, name := range names {
			if body.Items[i].Name != name {
				t.Fatal("sensor isolation/order failed")
			}
		}
		if parent == ga && len(body.Items) > 0 && (body.Items[0].Unit != nil || body.Items[0].CreatedAt != nil) {
			t.Fatal("nullable sensor metadata changed")
		}
	}
	checkSensors(a, ga, http.StatusOK) // Authorized Gateway exists before sensors.
	exec("INSERT INTO sensors(gateway_id,sensor_id,name,unit,created_at) VALUES ($1,'z','last','unit',now()),($1,'a','first',NULL,NULL),($2,'a','other','different',now())", ga, gb)
	for _, role := range []string{"owner", "operator", "viewer"} {
		exec("UPDATE user_gateways SET role=$1 WHERE user_id=$2 AND gateway_id=$3", role, a, ga)
		check(a, ga)
		checkSensors(a, ga, http.StatusOK, "first", "last")
	}
	check(b, gb)
	check(admin, "")
	check(newUser, "")
	checkSensors(b, gb, http.StatusOK, "other")
	checkSensors(a, gb, http.StatusNotFound)
	checkSensors(b, ga, http.StatusNotFound)
	checkSensors(a, ga+"_missing", http.StatusNotFound)
	checkSensors(admin, ga, http.StatusNotFound)
	checkSensors(newUser, ga, http.StatusNotFound)
	// Admin sees only explicit membership; it must not turn into a global listing.
	exec("INSERT INTO user_gateways(user_id,gateway_id,role) VALUES ($1,$2,'viewer')", admin, ga)
	check(admin, ga)
	checkSensors(admin, ga, http.StatusOK, "first", "last")
	exec("DELETE FROM user_gateways WHERE user_id=$1 AND gateway_id=$2", a, ga)
	check(a, "")
	check(b, gb)
	checkSensors(a, ga, http.StatusNotFound)
	checkSensors(b, gb, http.StatusOK, "other")
	tx, err := setup.Begin(ctx)
	if err != nil {
		t.Fatal("lock transaction failed")
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(c)
	}()
	if _, err := tx.Exec(ctx, "LOCK TABLE sensors IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal("sensor lock failed")
	}
	checkSensors(b, gb, http.StatusServiceUnavailable)
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal("unlock failed")
	}
	checkSensors(b, gb, http.StatusOK, "other")

	t.Run("admin gateway inventory http", func(t *testing.T) {
		testAdminGatewayInventoryHTTP(ctx, t, setup, pool)
	})
}

func TestAdminGatewayInventoryHTTPAuthorizationIntegration(t *testing.T) {
	if os.Getenv("AUTHORIZATION_TEST_ISOLATED") != "1" {
		t.Skip("run sh scripts/test-stage2-authorization.sh")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	setup, err := pgxpool.New(ctx, os.Getenv("AUTHORIZATION_TEST_SETUP_URL"))
	if err != nil {
		t.Fatal("setup pool failed")
	}
	defer setup.Close()
	pool, err := pgxpool.New(ctx, os.Getenv("AUTHORIZATION_TEST_BACKEND_URL"))
	if err != nil {
		t.Fatal("app pool failed")
	}
	defer pool.Close()

	testAdminGatewayInventoryHTTP(ctx, t, setup, pool)
}

func testAdminGatewayInventoryHTTP(ctx context.Context, t *testing.T, setup *pgxpool.Pool, pool *pgxpool.Pool) {

	admin, nonAdmin := uuid.New(), uuid.New()
	prefix := "z_fixhttpinv_" + uuid.New().String()[:8]
	g0 := prefix + "_0"
	g1 := prefix + "_1"
	gA := prefix + "_A"
	gZ := prefix + "_Z"
	ga := prefix + "_a"

	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := setup.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("fixture setup failed: %v", err)
		}
	}

	exec("INSERT INTO profiles(id) VALUES ($1),($2)", admin, nonAdmin)
	defer func() {
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, _ = setup.Exec(c, "DELETE FROM gateways WHERE gateway_id IN ($1,$2,$3,$4,$5)", g0, g1, gA, gZ, ga)
		_, _ = setup.Exec(c, "DELETE FROM platform_admins WHERE user_id=$1", admin)
		_, _ = setup.Exec(c, "DELETE FROM profiles WHERE id IN ($1,$2)", admin, nonAdmin)
	}()

	exec("INSERT INTO platform_admins(user_id) VALUES ($1)", admin)
	exec("INSERT INTO gateways(gateway_id, name, description, created_at) VALUES ($1,'Gate 0',NULL,NULL),($2,'Gate 1','desc1',now()),($3,'Gate A','descA',now()),($4,'Gate Z','descZ',now()),($5,'Gate a','desca',now())", g0, g1, gA, gZ, ga)
	exec("INSERT INTO user_gateways(user_id, gateway_id, role) VALUES ($1,$2,'owner')", nonAdmin, g0)
	exec("INSERT INTO twin_entities(entity_id, entity_type, name, gateway_id) VALUES ($1, 'Gateway', 'Gate 0', $2)", "urn:ngsi-ld:Gateway:"+g0, g0)
	var g0TwinID uuid.UUID
	if err := setup.QueryRow(ctx, "SELECT id FROM twin_entities WHERE entity_id=$1", "urn:ngsi-ld:Gateway:"+g0).Scan(&g0TwinID); err != nil {
		t.Fatal(err)
	}
	exec("INSERT INTO twin_states(entity_id) VALUES ($1)", g0TwinID)

	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal("random secret failed")
	}
	secret := []byte(hex.EncodeToString(random))
	verifier, err := auth.NewHS256Verifier(auth.VerifierConfig{Secret: string(secret), Issuer: "http://auth.test.local/auth/v1", Audience: "authenticated"})
	if err != nil {
		t.Fatal(err)
	}
	checker, err := auth.NewPostgresPlatformAdminChecker(pool)
	if err != nil {
		t.Fatal(err)
	}
	serviceRepo, err := gateway.NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	service, err := gateway.NewService(serviceRepo, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	provisionRepo, err := gateway.NewPostgresProvisioningRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	provisionService, err := gateway.NewProvisioningService(provisionRepo, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	adminRepo, err := gateway.NewPostgresAdminGatewayRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	adminService, err := gateway.NewAdminGatewayService(adminRepo, 200*time.Millisecond, 100, 200)
	if err != nil {
		t.Fatal(err)
	}
	ownerSensorRepo, err := gateway.NewPostgresOwnerSensorProvisioningRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	ownerSensorService, err := gateway.NewOwnerSensorProvisioningService(ownerSensorRepo, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	router, err := NewRouter(RouterDependencies{
		SensorProvisioner:          provisionService,
		GatewayProvisioner:         provisionService,
		OwnerSensorProvisioner:     ownerSensorService,
		AdminGatewayLister:         adminService,
		AdminInventoryDefaultLimit: 100,
		AdminInventoryMaxLimit:     200,
		AdminMaxBodyBytes:          16384,
		ReadinessChecker:           pool,
		ReadinessTimeout:           time.Second,
		AuthorizationTimeout:       200 * time.Millisecond,
		TokenVerifier:              verifier,
		PlatformAdminChecker:       checker,
		GatewayReader:              service,
		SensorReader:               service,
	})
	if err != nil {
		t.Fatal(err)
	}

	token := func(id uuid.UUID) string {
		t.Helper()
		now := time.Now()
		s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"sub":  id.String(),
			"role": "authenticated",
			"iss":  "http://auth.test.local/auth/v1",
			"aud":  "authenticated",
			"iat":  now.Unix(),
			"exp":  now.Add(time.Minute).Unix(),
		}).SignedString(secret)
		if err != nil {
			t.Fatal("sign fixture token failed")
		}
		return s
	}

	// 1. Non-admin forbidden
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/gateways", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token(nonAdmin))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-admin, got %d", rec.Code)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("expected no-store on 403, got %s", rec.Header().Get("Cache-Control"))
	}

	// 2. Admin calling user endpoint /v1/gateways sees only membership-assigned gateways (0 in this case)
	userReq := httptest.NewRequest(http.MethodGet, "/v1/gateways", nil).WithContext(ctx)
	userReq.Header.Set("Authorization", "Bearer "+token(admin))
	userRec := httptest.NewRecorder()
	router.ServeHTTP(userRec, userReq)
	if userRec.Code != http.StatusOK {
		t.Fatalf("user route status = %d", userRec.Code)
	}
	var userList struct {
		Items []struct {
			GatewayID string `json:"gateway_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(userRec.Body.Bytes(), &userList); err != nil {
		t.Fatal(err)
	}
	if len(userList.Items) != 0 {
		t.Fatalf("admin without membership saw gateways in user route: %+v", userList.Items)
	}

	// 3. Keyset pagination over HTTP for admin
	type invResponse struct {
		Items []struct {
			GatewayID   string     `json:"gateway_id"`
			Name        string     `json:"name"`
			Description *string    `json:"description"`
			CreatedAt   *time.Time `json:"created_at"`
			Role        *string    `json:"role"`
		} `json:"items"`
		NextCursor *string `json:"next_cursor"`
	}

	// Page 1: limit=2&after=<prefix>
	req = httptest.NewRequest(http.MethodGet, "/v1/admin/gateways?limit=2&after="+prefix, nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token(admin))
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("page 1 status = %d body = %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("expected no-store, got %s", rec.Header().Get("Cache-Control"))
	}
	var res1 invResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res1); err != nil {
		t.Fatal(err)
	}
	if len(res1.Items) != 2 || res1.Items[0].GatewayID != g0 || res1.Items[1].GatewayID != g1 {
		t.Fatalf("page 1 items mismatch: %+v", res1.Items)
	}
	if res1.Items[0].Role != nil || res1.Items[1].Role != nil {
		t.Fatal("admin inventory DTO leaked role")
	}
	if res1.Items[0].Description != nil || res1.Items[0].CreatedAt != nil {
		t.Fatal("expected null description and created_at on g0")
	}
	if res1.NextCursor == nil || *res1.NextCursor != g1 {
		t.Fatalf("page 1 cursor mismatch: %v", res1.NextCursor)
	}

	// Page 2: limit=2&after=g1
	req = httptest.NewRequest(http.MethodGet, "/v1/admin/gateways?limit=2&after="+*res1.NextCursor, nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token(admin))
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("page 2 status = %d", rec.Code)
	}
	var res2 invResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res2); err != nil {
		t.Fatal(err)
	}
	if len(res2.Items) != 2 || res2.Items[0].GatewayID != gA || res2.Items[1].GatewayID != gZ {
		t.Fatalf("page 2 items mismatch: %+v", res2.Items)
	}
	if res2.NextCursor == nil || *res2.NextCursor != gZ {
		t.Fatalf("page 2 cursor mismatch: %v", res2.NextCursor)
	}

	// Page 3: limit=2&after=gZ
	req = httptest.NewRequest(http.MethodGet, "/v1/admin/gateways?limit=2&after="+*res2.NextCursor, nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token(admin))
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("page 3 status = %d", rec.Code)
	}
	var res3 invResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res3); err != nil {
		t.Fatal(err)
	}
	if len(res3.Items) != 1 || res3.Items[0].GatewayID != ga {
		t.Fatalf("page 3 items mismatch: %+v", res3.Items)
	}
	if res3.NextCursor != nil {
		t.Fatalf("page 3 expected null next_cursor, got %v", *res3.NextCursor)
	}

	// 4. Strict query error tests
	for _, badQuery := range []string{
		"?unknown=1",
		"?user_id=" + uuid.NewString(),
		"?limit=0",
		"?limit=abc",
		"?limit=201",
		"?limit=10&limit=20",
		"?after=invalid*id",
		"?after=",
	} {
		req = httptest.NewRequest(http.MethodGet, "/v1/admin/gateways"+badQuery, nil).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer "+token(admin))
		rec = httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("query %s got status %d, want 400", badQuery, rec.Code)
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("expected no-store on 400, got %s", rec.Header().Get("Cache-Control"))
		}
	}

	// 5. Admin role revocation in DB immediately takes effect
	exec("DELETE FROM platform_admins WHERE user_id=$1", admin)
	req = httptest.NewRequest(http.MethodGet, "/v1/admin/gateways", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token(admin))
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 after admin revocation, got %d", rec.Code)
	}

	// 6. DB outage/lock returns 503
	exec("INSERT INTO platform_admins(user_id) VALUES ($1)", admin)
	tx, err := setup.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(c)
	}()
	if _, err := tx.Exec(ctx, "LOCK TABLE platform_admins IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodGet, "/v1/admin/gateways", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token(admin))
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on database lock, got %d", rec.Code)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("expected no-store on 503, got %s", rec.Header().Get("Cache-Control"))
	}

	// 7. Owner Sensor Provisioning via HTTP
	// Owner can provision sensor (201 Created)
	ownerSensorBody := `{"name":"Temperature Sensor","unit":"°C"}`
	req = httptest.NewRequest(http.MethodPut, "/v1/gateways/"+g0+"/sensors/temp_http", strings.NewReader(ownerSensorBody)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token(nonAdmin))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("owner sensor provisioning expected 201, got %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("expected no-store on 201, got %s", rec.Header().Get("Cache-Control"))
	}

	// Identical retry returns 200 OK
	req = httptest.NewRequest(http.MethodPut, "/v1/gateways/"+g0+"/sensors/temp_http", strings.NewReader(ownerSensorBody)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token(nonAdmin))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("owner sensor retry expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	// Different metadata returns 409 Conflict
	conflictBody := `{"name":"Different Name","unit":"°C"}`
	req = httptest.NewRequest(http.MethodPut, "/v1/gateways/"+g0+"/sensors/temp_http", strings.NewReader(conflictBody)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token(nonAdmin))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("owner sensor conflict expected 409, got %d body=%s", rec.Code, rec.Body.String())
	}

	// Admin without owner membership on owner route returns 404 (no bypass!)
	req = httptest.NewRequest(http.MethodPut, "/v1/gateways/"+g0+"/sensors/temp_http", strings.NewReader(ownerSensorBody)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token(admin))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("admin without owner membership expected 404 on owner route, got %d body=%s", rec.Code, rec.Body.String())
	}
}

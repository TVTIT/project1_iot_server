package httpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
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
	router, err := NewRouter(RouterDependencies{SensorProvisioner: provisionService, GatewayProvisioner: provisionService, AdminMaxBodyBytes: 16384, ReadinessChecker: pool, ReadinessTimeout: time.Second, AuthorizationTimeout: time.Second, TokenVerifier: verifier, PlatformAdminChecker: checker, GatewayReader: service, SensorReader: service})
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
}

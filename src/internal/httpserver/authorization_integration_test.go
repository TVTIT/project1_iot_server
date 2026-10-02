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
	router, err := NewRouter(RouterDependencies{ReadinessChecker: pool, ReadinessTimeout: time.Second, AuthorizationTimeout: time.Second, TokenVerifier: verifier, PlatformAdminChecker: checker, GatewayReader: service})
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
	for _, role := range []string{"owner", "operator", "viewer"} {
		exec("UPDATE user_gateways SET role=$1 WHERE user_id=$2 AND gateway_id=$3", role, a, ga)
		check(a, ga)
	}
	check(b, gb)
	check(admin, "")
	check(newUser, "")
	// Admin sees only explicit membership; it must not turn into a global listing.
	exec("INSERT INTO user_gateways(user_id,gateway_id,role) VALUES ($1,$2,'viewer')", admin, ga)
	check(admin, ga)
	exec("DELETE FROM user_gateways WHERE user_id=$1 AND gateway_id=$2", a, ga)
	check(a, "")
	check(b, gb)
}

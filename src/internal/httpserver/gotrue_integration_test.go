package httpserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/auth"
	"iot-platform/internal/httpapi"
)

// The isolated harness supplies real login tokens; no admin/debug endpoint is
// added to the deployed backend solely for integration testing.
func TestGoTrueAdminBoundaryIntegration(t *testing.T) {
	if os.Getenv("AUTH_INTEGRATION_ISOLATED") != "1" {
		t.Skip("run sh scripts/test-stage2-auth.sh")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("AUTH_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("failed to create backend database pool")
	}
	defer pool.Close()
	var role string
	if err := pool.QueryRow(ctx, "SELECT current_user").Scan(&role); err != nil || role != "iot_backend_app" {
		t.Fatal("admin integration must use iot_backend_app")
	}
	checker, err := auth.NewPostgresPlatformAdminChecker(pool)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewHS256Verifier(auth.VerifierConfig{
		Secret: os.Getenv("AUTH_TEST_SECRET"), Issuer: os.Getenv("AUTH_TEST_ISSUER"),
		Audience: "authenticated", ClockSkew: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	adminToken, normalToken := os.Getenv("AUTH_TEST_ADMIN_TOKEN"), os.Getenv("AUTH_TEST_NORMAL_TOKEN")
	if adminToken == "" || normalToken == "" {
		t.Fatal("real GoTrue test tokens are required")
	}
	router := gin.New()
	router.Use(httpapi.RequestIDMiddleware(), httpapi.RecoveryMiddleware(nil))
	groups := newRouteGroups(router, RouterDependencies{
		TokenVerifier: verifier, PlatformAdminChecker: checker, AuthorizationTimeout: time.Second,
	})
	handled := 0
	groups.admin.GET("/test-only", func(c *gin.Context) { handled++; c.Status(http.StatusNoContent) })
	for _, tt := range []struct {
		name   string
		token  string
		status int
	}{
		{name: "no token", status: http.StatusUnauthorized},
		{name: "normal user", token: normalToken, status: http.StatusForbidden},
		{name: "platform admin", token: adminToken, status: http.StatusNoContent},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/admin/test-only", nil).WithContext(ctx)
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			if response.Code != tt.status {
				t.Fatalf("status=%d, want %d", response.Code, tt.status)
			}
		})
	}
	if handled != 1 {
		t.Fatalf("admin handler ran %d times, want 1", handled)
	}
}

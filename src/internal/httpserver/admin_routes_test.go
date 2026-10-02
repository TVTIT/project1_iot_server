package httpserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"iot-platform/internal/auth"
	"iot-platform/internal/httpapi"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

func TestAdminGroupWithRealVerifier(t *testing.T) {
	secret := "test-only-admin-route-secret-at-least-32-bytes"
	userID := uuid.New()
	verifier, err := auth.NewHS256Verifier(auth.VerifierConfig{Secret: secret, Issuer: "http://localhost/auth/v1", Audience: "authenticated"})
	if err != nil {
		t.Fatal(err)
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iss": "http://localhost/auth/v1", "aud": "authenticated", "sub": userID.String(), "role": "authenticated", "iat": time.Now().Add(-time.Second).Unix(), "exp": time.Now().Add(time.Hour).Unix()}).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, credential string
		admin            bool
		lookupErr        error
		status           int
		calls            int
	}{
		{name: "missing", status: 401}, {name: "invalid", credential: "invalid", status: 401},
		{name: "ordinary", credential: token, status: 403, calls: 1},
		{name: "admin", credential: token, admin: true, status: 204, calls: 1},
		{name: "db error", credential: token, lookupErr: errors.New("private detail"), status: 503, calls: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls, handled := 0, 0
			deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
			deps.TokenVerifier = verifier
			deps.PlatformAdminChecker = platformAdminCheckerFunc(func(_ context.Context, id uuid.UUID) (bool, error) {
				calls++
				if id != userID {
					t.Fatal("request spoofed principal")
				}
				return tt.admin, tt.lookupErr
			})
			router := gin.New()
			router.Use(httpapi.RequestIDMiddleware())
			groups := newRouteGroups(router, deps)
			groups.admin.GET("/test-only", func(c *gin.Context) { handled++; c.Status(204) })
			req := httptest.NewRequest(http.MethodGet, "/v1/admin/test-only?user_id="+uuid.NewString(), nil)
			if tt.credential != "" {
				req.Header.Set("Authorization", "Bearer "+tt.credential)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != tt.status || calls != tt.calls {
				t.Fatalf("status=%d calls=%d", rec.Code, calls)
			}
			if (handled == 1) != (tt.status == 204) {
				t.Fatalf("handler count=%d", handled)
			}
		})
	}
}

func TestRouterRejectsTypedNilDependencies(t *testing.T) {
	for _, mutate := range []func(*RouterDependencies){
		func(d *RouterDependencies) { var v tokenVerifierFunc; d.TokenVerifier = v },
		func(d *RouterDependencies) { var v platformAdminCheckerFunc; d.PlatformAdminChecker = v },
		func(d *RouterDependencies) { var v readinessCheckerFunc; d.ReadinessChecker = v },
		func(d *RouterDependencies) { d.AuthorizationTimeout = 0 },
	} {
		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		mutate(&deps)
		if _, err := NewRouter(deps); err == nil {
			t.Fatal("invalid dependency accepted")
		}
	}
}

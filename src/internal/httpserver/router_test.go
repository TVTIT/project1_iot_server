package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
)

type readinessCheckerFunc func(context.Context) error

func (f readinessCheckerFunc) Ping(ctx context.Context) error {
	return f(ctx)
}

type tokenVerifierFunc func(context.Context, string) (auth.Principal, error)

func (f tokenVerifierFunc) Verify(ctx context.Context, token string) (auth.Principal, error) {
	return f(ctx, token)
}

type platformAdminCheckerFunc func(context.Context, uuid.UUID) (bool, error)

func (f platformAdminCheckerFunc) IsPlatformAdmin(ctx context.Context, userID uuid.UUID) (bool, error) {
	return f(ctx, userID)
}

func TestHealthzReportsProcessLiveness(t *testing.T) {
	router := mustRouter(t, readinessCheckerFunc(func(context.Context) error {
		return errors.New("database unavailable")
	}))

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if response.Body.String() != "OK" {
		t.Fatalf("body = %q, want %q", response.Body.String(), "OK")
	}
}

func TestReadyzReportsDatabaseAvailability(t *testing.T) {
	tests := []struct {
		name       string
		check      readinessCheckerFunc
		wantStatus int
	}{
		{
			name: "ready",
			check: func(context.Context) error {
				return nil
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not ready",
			check: func(context.Context) error {
				return errors.New("database unavailable")
			},
			wantStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := mustRouter(t, tt.check)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))

			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, tt.wantStatus)
			}
			if response.Header().Get("Content-Type") != "application/json; charset=utf-8" {
				t.Fatalf("Content-Type = %q", response.Header().Get("Content-Type"))
			}
			if tt.wantStatus == http.StatusServiceUnavailable {
				var body httpapi.APIErrorEnvelope
				if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
					t.Fatalf("json.Unmarshal() error = %v", err)
				}
				if body.Error.Code != "service_unavailable" || body.Error.RequestID == "" {
					t.Fatalf("error = %+v", body.Error)
				}
			}
		})
	}
}

func TestUnknownRouteAndMethodUseSafeErrorContract(t *testing.T) {
	router := mustRouter(t, readinessCheckerFunc(func(context.Context) error { return nil }))
	tests := []struct {
		method     string
		path       string
		wantStatus int
		wantCode   string
	}{
		{method: http.MethodGet, path: "/missing", wantStatus: http.StatusNotFound, wantCode: "not_found"},
		{method: http.MethodPost, path: "/healthz", wantStatus: http.StatusMethodNotAllowed, wantCode: "method_not_allowed"},
	}

	for _, tt := range tests {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(tt.method, tt.path, nil))
		if response.Code != tt.wantStatus {
			t.Fatalf("%s %s status = %d, want %d", tt.method, tt.path, response.Code, tt.wantStatus)
		}
		var body httpapi.APIErrorEnvelope
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
		if body.Error.Code != tt.wantCode || body.Error.RequestID == "" {
			t.Errorf("error = %+v", body.Error)
		}
	}
}

func TestPlannedBusinessRoutesRequireAuthenticationBeforeStub(t *testing.T) {
	router := mustRouter(t, readinessCheckerFunc(func(context.Context) error { return nil }))

	for _, path := range []string{"/v1/telemetry/history", "/v1/ws", "/v1/digital-twins"} {
		unauthorized := httptest.NewRecorder()
		router.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, path, nil))
		if unauthorized.Code != http.StatusUnauthorized {
			t.Errorf("GET %s without token status = %d, want %d", path, unauthorized.Code, http.StatusUnauthorized)
		}

		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer valid-token")
		authorized := httptest.NewRecorder()
		router.ServeHTTP(authorized, request)
		if authorized.Code != http.StatusNotImplemented {
			t.Errorf("GET %s with token status = %d, want %d", path, authorized.Code, http.StatusNotImplemented)
		}
	}
}

func TestRequestIDMiddlewarePropagatesHeader(t *testing.T) {
	router := mustRouter(t, readinessCheckerFunc(func(context.Context) error { return nil }))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", "custom-request-id-123")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if got := rec.Header().Get("X-Request-ID"); got != "custom-request-id-123" {
		t.Errorf("X-Request-ID = %q, want %q", got, "custom-request-id-123")
	}
}

func TestNewRouterRejectsMissingSecurityDependencies(t *testing.T) {
	valid := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	tests := []struct {
		name   string
		mutate func(*RouterDependencies)
	}{
		{name: "readiness checker", mutate: func(deps *RouterDependencies) { deps.ReadinessChecker = nil }},
		{name: "readiness timeout", mutate: func(deps *RouterDependencies) { deps.ReadinessTimeout = 0 }},
		{name: "token verifier", mutate: func(deps *RouterDependencies) { deps.TokenVerifier = nil }},
		{name: "admin checker", mutate: func(deps *RouterDependencies) { deps.PlatformAdminChecker = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := valid
			tt.mutate(&deps)
			if _, err := NewRouter(deps); err == nil {
				t.Fatal("NewRouter() error = nil")
			}
		})
	}
}

func mustRouter(t *testing.T, readiness ReadinessChecker) http.Handler {
	t.Helper()
	router, err := NewRouter(testRouterDependencies(readiness))
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}
	return router
}

func testRouterDependencies(readiness ReadinessChecker) RouterDependencies {
	return RouterDependencies{
		SensorReader:         sensorReaderFunc(func(context.Context, uuid.UUID, string) ([]gateway.Sensor, error) { return nil, nil }),
		GatewayReader:        gatewayReaderFunc(func(context.Context, uuid.UUID) ([]gateway.Gateway, error) { return nil, nil }),
		ReadinessChecker:     readiness,
		ReadinessTimeout:     time.Second,
		AuthorizationTimeout: time.Second,
		TokenVerifier: tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
			return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
		}),
		PlatformAdminChecker: platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
			return true, nil
		}),
	}
}

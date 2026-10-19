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
	"iot-platform/internal/config"
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

	// Only /v1/ws remains an unauthenticated-first stub returning 501 when authorized.
	unauthorized := httptest.NewRecorder()
	router.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/v1/ws", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Errorf("GET /v1/ws without token status = %d, want %d", unauthorized.Code, http.StatusUnauthorized)
	}

	request := httptest.NewRequest(http.MethodGet, "/v1/ws", nil)
	request.Header.Set("Authorization", "Bearer valid-token")
	authorized := httptest.NewRecorder()
	router.ServeHTTP(authorized, request)
	if authorized.Code != http.StatusNotImplemented {
		t.Errorf("GET /v1/ws with token status = %d, want %d", authorized.Code, http.StatusNotImplemented)
	}

	// Replaced route /v1/digital-twins requires auth and dispatches to handler (not 501).
	for _, path := range []string{"/v1/digital-twins"} {
		unauth := httptest.NewRecorder()
		router.ServeHTTP(unauth, httptest.NewRequest(http.MethodGet, path, nil))
		if unauth.Code != http.StatusUnauthorized {
			t.Errorf("GET %s without token status = %d, want %d", path, unauth.Code, http.StatusUnauthorized)
		}

		authReq := httptest.NewRequest(http.MethodGet, path, nil)
		authReq.Header.Set("Authorization", "Bearer valid-token")
		authRec := httptest.NewRecorder()
		router.ServeHTTP(authRec, authReq)
		if authRec.Code == http.StatusNotImplemented {
			t.Errorf("GET %s with token unexpectedly returned 501", path)
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
		{name: "telemetry history reader", mutate: func(deps *RouterDependencies) { deps.TelemetryHistoryReader = nil }},
		{name: "twin list reader", mutate: func(deps *RouterDependencies) { deps.TwinListReader = nil }},
		{name: "twin detail reader", mutate: func(deps *RouterDependencies) { deps.TwinDetailReader = nil }},
		{name: "twin state reader", mutate: func(deps *RouterDependencies) { deps.TwinStateReader = nil }},
		{name: "twin history reader", mutate: func(deps *RouterDependencies) { deps.TwinHistoryReader = nil }},
		{name: "entity creator", mutate: func(deps *RouterDependencies) { deps.EntityCreator = nil }},
		{name: "gateway patcher", mutate: func(deps *RouterDependencies) { deps.GatewayPatcher = nil }},
		{name: "sensor patcher", mutate: func(deps *RouterDependencies) { deps.SensorPatcher = nil }},
		{name: "history query parser", mutate: func(deps *RouterDependencies) { deps.HistoryQueryParser = nil }},
		{name: "invalid history parser bounds", mutate: func(deps *RouterDependencies) {
			deps.HistoryQueryParser = &HistoryQueryParser{cfg: config.HistoryConfig{QueryTimeout: -1}}
		}},
		{name: "twin query parser", mutate: func(deps *RouterDependencies) { deps.TwinQueryParser = nil }},
		{name: "invalid twin parser bounds", mutate: func(deps *RouterDependencies) {
			deps.TwinQueryParser = &TwinQueryParser{cfg: config.TwinConfig{ListDefaultLimit: -1}}
		}},
		{name: "twin max body bytes zero", mutate: func(deps *RouterDependencies) { deps.TwinMaxBodyBytes = 0 }},
		{name: "twin max body bytes negative", mutate: func(deps *RouterDependencies) { deps.TwinMaxBodyBytes = -1 }},
		{name: "twin max body bytes excessive", mutate: func(deps *RouterDependencies) { deps.TwinMaxBodyBytes = 2000000 }},
		{name: "typed nil telemetry history reader", mutate: func(deps *RouterDependencies) {
			var nilReader *mockTelemetryHistoryReader
			deps.TelemetryHistoryReader = nilReader
		}},
		{name: "typed nil history query parser", mutate: func(deps *RouterDependencies) {
			var nilParser *HistoryQueryParser
			deps.HistoryQueryParser = nilParser
		}},
		{name: "typed nil twin query parser", mutate: func(deps *RouterDependencies) {
			var nilParser *TwinQueryParser
			deps.TwinQueryParser = nilParser
		}},
		{name: "typed nil entity creator", mutate: func(deps *RouterDependencies) {
			var nilCreator *mockEntityCreator
			deps.EntityCreator = nilCreator
		}},
		{name: "typed nil gateway patcher", mutate: func(deps *RouterDependencies) {
			var nilPatcher *mockGatewayPatcher
			deps.GatewayPatcher = nilPatcher
		}},
		{name: "typed nil sensor patcher", mutate: func(deps *RouterDependencies) {
			var nilPatcher *mockSensorPatcher
			deps.SensorPatcher = nilPatcher
		}},
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
	deps := RouterDependencies{
		SensorProvisioner: sensorProvisionerFunc(func(context.Context, uuid.UUID, gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error) {
			return gateway.ProvisionSensorResult{}, gateway.ErrProvisioningInconsistent
		}),
		OwnerSensorProvisioner: ownerSensorProvisionerFunc(func(context.Context, uuid.UUID, gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error) {
			return gateway.ProvisionSensorResult{}, gateway.ErrProvisioningInconsistent
		}),
		GatewayProvisioner: gatewayProvisionerFunc(func(context.Context, uuid.UUID, gateway.ProvisionGatewayInput) (gateway.ProvisionGatewayResult, error) {
			return gateway.ProvisionGatewayResult{}, gateway.ErrProvisioningInconsistent
		}),
		AdminMaxBodyBytes: 16384,
		SensorReader:      sensorReaderFunc(func(context.Context, uuid.UUID, string) ([]gateway.Sensor, error) { return nil, nil }),
		GatewayReader:     gatewayReaderFunc(func(context.Context, uuid.UUID) ([]gateway.Gateway, error) { return nil, nil }),
		AdminGatewayLister: adminGatewayListerFunc(func(context.Context, uuid.UUID, gateway.AdminGatewayInventoryQuery) (gateway.AdminGatewayInventoryPage, error) {
			return gateway.AdminGatewayInventoryPage{Items: []gateway.AdminGateway{}}, nil
		}),
		AdminInventoryDefaultLimit: 100,
		AdminInventoryMaxLimit:     200,
		ReadinessChecker:           readiness,
		ReadinessTimeout:           time.Second,
		AuthorizationTimeout:       time.Second,
		TokenVerifier: tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
			return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
		}),
		PlatformAdminChecker: platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
			return true, nil
		}),
	}
	return AttachTestStage4Dependencies(deps)
}

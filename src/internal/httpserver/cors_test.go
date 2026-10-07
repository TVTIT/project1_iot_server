package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/gateway"
)

func TestCORSPreflightAllowsBrowserCanonicalOriginFromConfiguration(t *testing.T) {
	values := map[string]string{
		"DATABASE_URL":          "postgres://user:password@database:5432/iot",
		"SUPABASE_JWT_SECRET":   "test-only-jwt-secret-at-least-32-characters",
		"SUPABASE_JWT_ISSUER":   "http://localhost/auth/v1",
		"SUPABASE_JWT_AUDIENCE": "authenticated",
		"CORS_ALLOWED_ORIGINS":  "https://app.example.com:443",
	}
	cfg, err := config.Load(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatalf("config.Load() error = %v", err)
	}

	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.CORSAllowedOrigins = cfg.CORSAllowedOrigins
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodOptions, "/v1/gateways", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "https://app.example.com")
	}
}

func TestCORSPreflightAllowed(t *testing.T) {
	// Panicking mocks verify that preflight does NOT make database or service calls.
	panickingReadiness := readinessCheckerFunc(func(context.Context) error {
		panic("readiness checker must not be called during preflight")
	})
	deps := testRouterDependencies(panickingReadiness)
	deps.CORSAllowedOrigins = []string{"http://localhost:12345", "https://app.example.com"}
	deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
		panic("token verifier must not be called during preflight")
	})
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
		panic("platform admin checker must not be called during preflight")
	})
	deps.GatewayReader = gatewayReaderFunc(func(context.Context, uuid.UUID) ([]gateway.Gateway, error) {
		panic("gateway reader must not be called during preflight")
	})

	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodOptions, "/v1/gateways", nil)
	req.Header.Set("Origin", "http://localhost:12345")
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "Authorization, Content-Type, Idempotency-Key, X-Request-ID")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:12345" {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "http://localhost:12345")
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got != "GET, POST, PUT, PATCH, DELETE, OPTIONS" {
		t.Errorf("Access-Control-Allow-Methods = %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "Authorization, Content-Type, Idempotency-Key, X-Request-ID" {
		t.Errorf("Access-Control-Allow-Headers = %q", got)
	}
	if got := rec.Header().Get("Access-Control-Max-Age"); got != "600" {
		t.Errorf("Access-Control-Max-Age = %q, want 600", got)
	}
	if got := rec.Header().Get("Access-Control-Expose-Headers"); got != "X-Request-ID" {
		t.Errorf("Access-Control-Expose-Headers = %q, want X-Request-ID", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Errorf("Access-Control-Allow-Credentials = %q, want empty", got)
	}
	vary := rec.Header().Get("Vary")
	for _, expectedVary := range []string{"Origin", "Access-Control-Request-Method", "Access-Control-Request-Headers"} {
		if !containsVaryHeader(vary, expectedVary) {
			t.Errorf("Vary = %q does not contain %q", vary, expectedVary)
		}
	}
}

func TestCORSPreflightCaseInsensitiveHeaders(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.CORSAllowedOrigins = []string{"http://localhost:12345"}
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodOptions, "/v1/gateways", nil)
	req.Header.Set("Origin", "http://localhost:12345")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "authorization, CONTENT-TYPE, idempotency-key, x-request-id")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:12345" {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "http://localhost:12345")
	}
	if !containsVaryHeader(rec.Header().Get("Vary"), "Origin") {
		t.Errorf("Vary = %q does not contain Origin", rec.Header().Get("Vary"))
	}
}

func TestCORSPreflightStrictlyParsesRequestHeaders(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.CORSAllowedOrigins = []string{"http://localhost:12345"}
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	tests := []struct {
		name    string
		method  []string
		headers []string
		want    int
	}{
		{name: "uppercase allowed method", method: []string{"GET"}, want: http.StatusNoContent},
		{name: "lowercase method is rejected", method: []string{"get"}, want: http.StatusForbidden},
		{name: "whitespace padded method is rejected", method: []string{" GET "}, want: http.StatusForbidden},
		{name: "malformed method token is rejected", method: []string{"GE T"}, want: http.StatusForbidden},
		{name: "unsupported method is rejected", method: []string{"TRACE"}, want: http.StatusForbidden},
		{name: "header names are case insensitive", method: []string{"POST"}, headers: []string{"authorization, CONTENT-TYPE"}, want: http.StatusNoContent},
		{name: "empty header element after comma is rejected", method: []string{"POST"}, headers: []string{"Authorization,,Content-Type"}, want: http.StatusForbidden},
		{name: "empty header list is rejected", method: []string{"POST"}, headers: []string{""}, want: http.StatusForbidden},
		{name: "disallowed header in later header line is rejected", method: []string{"POST"}, headers: []string{"Authorization", "X-Secret-Token"}, want: http.StatusForbidden},
		{name: "empty element in later header line is rejected", method: []string{"POST"}, headers: []string{"Authorization", ","}, want: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodOptions, "/v1/gateways", nil)
			req.Header.Add("Origin", "http://localhost:12345")
			for _, value := range tt.method {
				req.Header.Add("Access-Control-Request-Method", value)
			}
			for _, value := range tt.headers {
				req.Header.Add("Access-Control-Request-Headers", value)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, tt.want, rec.Body.String())
			}
			if tt.want != http.StatusNoContent && rec.Header().Get("Access-Control-Allow-Origin") != "" {
				t.Errorf("denied preflight returned Access-Control-Allow-Origin = %q", rec.Header().Get("Access-Control-Allow-Origin"))
			}
		})
	}
}

func TestCORSPreflightRejectsMultipleOriginsAndSingletonRequestMethods(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.CORSAllowedOrigins = []string{"http://localhost:12345"}
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	tests := []struct {
		name    string
		origins []string
		methods []string
	}{
		{name: "multiple origin lines", origins: []string{"http://localhost:12345", "http://evil.com"}, methods: []string{"GET"}},
		{name: "comma separated origins", origins: []string{"http://localhost:12345, http://evil.com"}, methods: []string{"GET"}},
		{name: "duplicate method lines", origins: []string{"http://localhost:12345"}, methods: []string{"GET", "TRACE"}},
		{name: "comma separated methods", origins: []string{"http://localhost:12345"}, methods: []string{"GET, POST"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodOptions, "/v1/gateways", nil)
			for _, value := range tt.origins {
				req.Header.Add("Origin", value)
			}
			for _, value := range tt.methods {
				req.Header.Add("Access-Control-Request-Method", value)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
			}
			if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
				t.Errorf("Access-Control-Allow-Origin = %q, want empty", got)
			}
		})
	}
}

func TestCORSPreflightDeniedCases(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.CORSAllowedOrigins = []string{"http://localhost:12345"}
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	tests := []struct {
		name       string
		origin     string
		method     string
		headers    string
		wantStatus int
	}{
		{
			name:       "disallowed origin",
			origin:     "http://evil.com",
			method:     "GET",
			headers:    "Authorization",
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "disallowed method TRACE",
			origin:     "http://localhost:12345",
			method:     "TRACE",
			headers:    "Authorization",
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "disallowed method CONNECT",
			origin:     "http://localhost:12345",
			method:     "CONNECT",
			headers:    "Authorization",
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "disallowed header",
			origin:     "http://localhost:12345",
			method:     "POST",
			headers:    "Authorization, X-Secret-Token",
			wantStatus: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodOptions, "/v1/gateways", nil)
			req.Header.Set("Origin", tt.origin)
			if tt.method != "" {
				req.Header.Set("Access-Control-Request-Method", tt.method)
			}
			if tt.headers != "" {
				req.Header.Set("Access-Control-Request-Headers", tt.headers)
			}

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
				t.Errorf("Access-Control-Allow-Origin should not be set on denied request, got %q", got)
			}
		})
	}
}

func TestCORSPreflightDeniedWhenAllowlistEmpty(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.CORSAllowedOrigins = nil // empty allowlist
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodOptions, "/v1/gateways", nil)
	req.Header.Set("Origin", "http://localhost:12345")
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "Authorization")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin should be empty, got %q", got)
	}
}

func TestCORSSchemeHostPortIsolation(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.CORSAllowedOrigins = []string{"http://localhost:12345"}
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	probes := []struct {
		origin      string
		wantAllowed bool
	}{
		{origin: "http://localhost:12345", wantAllowed: true},
		{origin: "https://localhost:12345", wantAllowed: false}, // scheme mismatch
		{origin: "http://localhost:54321", wantAllowed: false},  // port mismatch
		{origin: "http://127.0.0.1:12345", wantAllowed: false},  // host mismatch
		{origin: "http://evil.com", wantAllowed: false},         // external domain
		{origin: "http://localhost:12345.evil.com", wantAllowed: false},
		{origin: "null", wantAllowed: false},
	}

	for _, probe := range probes {
		t.Run(probe.origin, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodOptions, "/v1/gateways", nil)
			req.Header.Set("Origin", probe.origin)
			req.Header.Set("Access-Control-Request-Method", "GET")

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if probe.wantAllowed {
				if rec.Code != http.StatusNoContent {
					t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
				}
				if got := rec.Header().Get("Access-Control-Allow-Origin"); got != probe.origin {
					t.Fatalf("Access-Control-Allow-Origin = %q, want %q", got, probe.origin)
				}
			} else {
				if rec.Code != http.StatusForbidden {
					t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
				}
				if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
					t.Fatalf("Access-Control-Allow-Origin should not be set, got %q", got)
				}
			}
		})
	}
}

func TestCORSNonPreflightOPTIONS(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.CORSAllowedOrigins = []string{"http://localhost:12345"}
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	// OPTIONS request WITHOUT Access-Control-Request-Method is not a CORS preflight.
	req := httptest.NewRequest(http.MethodOptions, "/healthz", nil)
	req.Header.Set("Origin", "http://localhost:12345")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	// Since /healthz only accepts GET, router returns 405 Method Not Allowed.
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	// For actual request with allowed origin, Access-Control-Allow-Origin is set even on 405.
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:12345" {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "http://localhost:12345")
	}
	if !containsVaryHeader(rec.Header().Get("Vary"), "Origin") {
		t.Errorf("Vary = %q does not contain Origin", rec.Header().Get("Vary"))
	}
}

func TestOrdinaryRequestsWithoutOriginSucceed(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.CORSAllowedOrigins = []string{"http://localhost:12345"}
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	for _, path := range []string{"/healthz", "/readyz"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want %d", path, rec.Code, http.StatusOK)
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("GET %s should not have Access-Control-Allow-Origin, got %q", path, got)
		}
		if !containsVaryHeader(rec.Header().Get("Vary"), "Origin") {
			t.Errorf("GET %s Vary = %q does not contain Origin", path, rec.Header().Get("Vary"))
		}
	}
}

func TestCORSPreservesExistingVaryHeader(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.CORSAllowedOrigins = []string{"http://localhost:12345"}

	// Build a small router with a middleware that sets Vary: Accept-Encoding
	r, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "http://localhost:12345")
	rec := httptest.NewRecorder()
	rec.Header().Set("Vary", "Accept-Encoding")

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	vary := rec.Header().Get("Vary")
	if !containsVaryHeader(vary, "Origin") {
		t.Errorf("Vary = %q does not contain Origin", vary)
	}
	if !containsVaryHeader(vary, "Accept-Encoding") {
		t.Errorf("Vary = %q does not contain Accept-Encoding", vary)
	}
}

func TestCORSHeadersAppliedToErrorResponses(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error {
		return errors.New("db down")
	}))
	deps.CORSAllowedOrigins = []string{"http://localhost:12345"}
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
		return false, nil // non-admin
	})
	deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	tests := []struct {
		name       string
		method     string
		path       string
		token      string
		wantStatus int
	}{
		{
			name:       "401 unauthorized",
			method:     http.MethodGet,
			path:       "/v1/gateways",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "403 forbidden",
			method:     http.MethodPut,
			path:       "/v1/admin/gateways/gw-001",
			token:      "valid-token",
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "404 not found",
			method:     http.MethodGet,
			path:       "/v1/nonexistent",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "405 method not allowed",
			method:     http.MethodDelete,
			path:       "/healthz",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "503 service unavailable",
			method:     http.MethodGet,
			path:       "/readyz",
			wantStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.Header.Set("Origin", "http://localhost:12345")
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("%s %s status = %d, want %d", tt.method, tt.path, rec.Code, tt.wantStatus)
			}
			if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:12345" {
				t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "http://localhost:12345")
			}
			if !containsVaryHeader(rec.Header().Get("Vary"), "Origin") {
				t.Errorf("Vary = %q does not contain Origin", rec.Header().Get("Vary"))
			}
		})
	}
}

func TestPermissionsEndpointWithAllowedOriginAndJWT(t *testing.T) {
	currentUserID := uuid.New()
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.CORSAllowedOrigins = []string{"http://localhost:12345"}
	deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{UserID: currentUserID, Role: "authenticated"}, nil
	})
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
		return true, nil
	})

	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/me/permissions", nil)
	req.Header.Set("Origin", "http://localhost:12345")
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:12345" {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "http://localhost:12345")
	}
	if got := rec.Header().Get("Access-Control-Expose-Headers"); got != "X-Request-ID" {
		t.Errorf("Access-Control-Expose-Headers = %q, want X-Request-ID", got)
	}

	var payload struct {
		IsPlatformAdmin bool `json:"is_platform_admin"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if !payload.IsPlatformAdmin {
		t.Errorf("expected is_platform_admin = true, got %+v", payload)
	}
}

func TestActualRequestWithDisallowedOriginProceedsWithoutCORSHeaders(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.CORSAllowedOrigins = []string{"http://localhost:12345"}
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "http://evil.com")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin should be empty, got %q", got)
	}
	if !containsVaryHeader(rec.Header().Get("Vary"), "Origin") {
		t.Errorf("Vary header %q should contain Origin", rec.Header().Get("Vary"))
	}
}

func TestActualProtectedRequestWithDisallowedOriginStillRequiresJWT(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.CORSAllowedOrigins = []string{"http://localhost:12345"}
	verificationCalls := 0
	deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
		verificationCalls++
		return auth.Principal{}, errors.New("invalid token")
	})
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/me/permissions", nil)
	req.Header.Set("Origin", "http://evil.com")
	req.Header.Set("Authorization", "Bearer invalid-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if verificationCalls != 1 {
		t.Errorf("token verifier calls = %d, want 1", verificationCalls)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want empty", got)
	}
}

func TestAddVaryMergesExistingTokensWithoutDuplicates(t *testing.T) {
	header := make(http.Header)
	header.Add("Vary", "Accept-Encoding, origin")
	header.Add("Vary", "ORIGIN, ACCEPT-encoding")

	addVary(header, "Origin", "Access-Control-Request-Method")

	if got, want := header.Values("Vary"), []string{"Accept-Encoding, origin, Access-Control-Request-Method"}; !equalStrings(got, want) {
		t.Errorf("Vary values = %#v, want %#v", got, want)
	}
}

func TestAddVaryPreservesWildcard(t *testing.T) {
	header := make(http.Header)
	header.Add("Vary", "Accept-Encoding")
	header.Add("Vary", "*")

	addVary(header, "Origin")

	if got, want := header.Values("Vary"), []string{"*"}; !equalStrings(got, want) {
		t.Errorf("Vary values = %#v, want %#v", got, want)
	}
}

func containsVaryHeader(varyHeader, target string) bool {
	for _, part := range strings.Split(varyHeader, ",") {
		if strings.EqualFold(strings.TrimSpace(part), target) {
			return true
		}
	}
	return false
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

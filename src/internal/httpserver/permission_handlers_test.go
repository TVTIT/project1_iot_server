package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/httpapi"
)

type permissionsTestResponse struct {
	IsPlatformAdmin bool `json:"is_platform_admin"`
}

func TestPermissionsAuthenticatedAdminTrue(t *testing.T) {
	adminID := uuid.New()
	checkerCalls := 0

	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.TokenVerifier = tokenVerifierFunc(func(_ context.Context, token string) (auth.Principal, error) {
		if token != "admin-token" {
			return auth.Principal{}, errors.New("invalid token")
		}
		return auth.Principal{UserID: adminID, Role: "authenticated"}, nil
	})
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(_ context.Context, id uuid.UUID) (bool, error) {
		checkerCalls++
		if id != adminID {
			t.Fatalf("checker received user ID %v, want %v", id, adminID)
		}
		return true, nil
	})

	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/me/permissions", nil)
	req.Header.Set("Authorization", "Bearer admin-token")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q, want %q", rec.Header().Get("Cache-Control"), "no-store")
	}

	var resp permissionsTestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !resp.IsPlatformAdmin {
		t.Errorf("is_platform_admin = false, want true")
	}
	if checkerCalls != 1 {
		t.Errorf("checker called %d times, want 1", checkerCalls)
	}
}

func TestPermissionsOrdinaryUserFalse(t *testing.T) {
	userID := uuid.New()
	checkerCalls := 0

	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.TokenVerifier = tokenVerifierFunc(func(_ context.Context, token string) (auth.Principal, error) {
		if token != "user-token" {
			return auth.Principal{}, errors.New("invalid token")
		}
		return auth.Principal{UserID: userID, Role: "authenticated"}, nil
	})
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(_ context.Context, id uuid.UUID) (bool, error) {
		checkerCalls++
		if id != userID {
			t.Fatalf("checker received user ID %v, want %v", id, userID)
		}
		return false, nil
	})

	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/me/permissions", nil)
	req.Header.Set("Authorization", "Bearer user-token")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q, want %q", rec.Header().Get("Cache-Control"), "no-store")
	}

	var resp permissionsTestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.IsPlatformAdmin {
		t.Errorf("is_platform_admin = true, want false")
	}
	if checkerCalls != 1 {
		t.Errorf("checker called %d times, want 1", checkerCalls)
	}
}

func TestPermissionsMissingOrInvalidJWT(t *testing.T) {
	checkerCalls := 0

	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.TokenVerifier = tokenVerifierFunc(func(_ context.Context, token string) (auth.Principal, error) {
		if token == "valid" {
			return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
		}
		return auth.Principal{}, errors.New("invalid token")
	})
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
		checkerCalls++
		return true, nil
	})

	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	tests := []struct {
		name       string
		authHeader string
	}{
		{name: "missing header", authHeader: ""},
		{name: "invalid bearer scheme", authHeader: "Basic dXNlcjpwYXNz"},
		{name: "empty bearer credential", authHeader: "Bearer "},
		{name: "invalid token value", authHeader: "Bearer invalid-jwt-value"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/me/permissions", nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Errorf("Cache-Control = %q, want %q", rec.Header().Get("Cache-Control"), "no-store")
			}
			if rec.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Errorf("WWW-Authenticate = %q, want %q", rec.Header().Get("WWW-Authenticate"), "Bearer")
			}

			var body httpapi.APIErrorEnvelope
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("failed to decode error body: %v", err)
			}
			if body.Error.Code != "unauthorized" {
				t.Errorf("error.code = %q, want %q", body.Error.Code, "unauthorized")
			}
		})
	}

	if checkerCalls != 0 {
		t.Fatalf("unauthenticated requests invoked checker %d times, want 0", checkerCalls)
	}
}

func TestPermissionsMisleadingSignedAdminClaimWithoutDBGrant(t *testing.T) {
	userID := uuid.New()
	checkerCalls := 0

	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	// Verifier returns Role: "admin" (simulating misleading signed admin claim)
	deps.TokenVerifier = tokenVerifierFunc(func(_ context.Context, token string) (auth.Principal, error) {
		if token == "misleading-admin-token" {
			return auth.Principal{UserID: userID, Role: "admin"}, nil
		}
		return auth.Principal{}, errors.New("invalid")
	})
	// DB authoritative check returns false (no platform_admins grant in DB)
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(_ context.Context, id uuid.UUID) (bool, error) {
		checkerCalls++
		if id != userID {
			t.Fatalf("checker received user ID %v, want %v", id, userID)
		}
		return false, nil
	})

	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/me/permissions", nil)
	req.Header.Set("Authorization", "Bearer misleading-admin-token")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q, want %q", rec.Header().Get("Cache-Control"), "no-store")
	}

	var resp permissionsTestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.IsPlatformAdmin {
		t.Errorf("is_platform_admin = true, want false (JWT admin claim without DB grant must not be trusted)")
	}
	if checkerCalls != 1 {
		t.Errorf("checker called %d times, want 1", checkerCalls)
	}
}

func TestPermissionsCheckerFailure503(t *testing.T) {
	userID := uuid.New()

	tests := []struct {
		name       string
		checkerErr error
	}{
		{
			name:       "database error",
			checkerErr: errors.New("SECRET_DB_PASSWORD_FAIL: connection refused"),
		},
		{
			name:       "timeout",
			checkerErr: context.DeadlineExceeded,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
			deps.TokenVerifier = tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
				return auth.Principal{UserID: userID, Role: "authenticated"}, nil
			})
			deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
				return false, tc.checkerErr
			})

			router, err := NewRouter(deps)
			if err != nil {
				t.Fatalf("NewRouter() error = %v", err)
			}

			req := httptest.NewRequest(http.MethodGet, "/v1/me/permissions", nil)
			req.Header.Set("Authorization", "Bearer valid-token")
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Errorf("Cache-Control = %q, want %q", rec.Header().Get("Cache-Control"), "no-store")
			}

			var body httpapi.APIErrorEnvelope
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("failed to decode error body: %v", err)
			}
			if body.Error.Code != "service_unavailable" {
				t.Errorf("error.code = %q, want %q", body.Error.Code, "service_unavailable")
			}
			if body.Error.Message != "service unavailable" {
				t.Errorf("error.message = %q, want %q", body.Error.Message, "service unavailable")
			}
			if body.Error.RequestID == "" {
				t.Errorf("error.request_id is empty")
			}
			// Assert raw error or secrets are never exposed
			if bytes.Contains(rec.Body.Bytes(), []byte("SECRET")) {
				t.Errorf("response exposed raw error/secrets: %s", rec.Body.String())
			}
		})
	}
}

func TestPermissionsPrincipalIdentityPassedToCheckerAndBoundedContext(t *testing.T) {
	expectedUserID := uuid.New()
	var receivedUserID uuid.UUID
	var hasDeadline bool
	var remainingDeadline time.Duration
	checkerCalls := 0

	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.AuthorizationTimeout = 2 * time.Second
	deps.TokenVerifier = tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{UserID: expectedUserID, Role: "authenticated"}, nil
	})
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(ctx context.Context, id uuid.UUID) (bool, error) {
		checkerCalls++
		receivedUserID = id
		if deadline, ok := ctx.Deadline(); ok {
			hasDeadline = true
			remainingDeadline = time.Until(deadline)
		}
		return true, nil
	})

	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/me/permissions", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if checkerCalls != 1 {
		t.Fatalf("checker called %d times, want 1", checkerCalls)
	}
	if receivedUserID != expectedUserID {
		t.Errorf("received user ID %v, want %v", receivedUserID, expectedUserID)
	}
	if !hasDeadline {
		t.Errorf("context had no deadline; expected bounded context with AuthorizationTimeout")
	}
	if remainingDeadline <= 0 || remainingDeadline > 2*time.Second {
		t.Errorf("remaining deadline %v out of expected range (0, 2s]", remainingDeadline)
	}
}

func TestPermissionsRejectsQueryParamAndBodySpoofing(t *testing.T) {
	userID := uuid.New()
	checkerCalls := 0

	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.TokenVerifier = tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{UserID: userID, Role: "authenticated"}, nil
	})
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
		checkerCalls++
		return true, nil
	})

	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "query param user_id",
			path: "/v1/me/permissions?user_id=" + uuid.New().String(),
			body: "",
		},
		{
			name: "body containing spoofed user_id",
			path: "/v1/me/permissions",
			body: `{"user_id":"` + uuid.New().String() + `"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, bytes.NewBufferString(tc.body))
			req.Header.Set("Authorization", "Bearer valid-token")
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Errorf("Cache-Control = %q, want %q", rec.Header().Get("Cache-Control"), "no-store")
			}

			var body httpapi.APIErrorEnvelope
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("failed to decode error body: %v", err)
			}
			if body.Error.Code != "invalid_request" {
				t.Errorf("error.code = %q, want %q", body.Error.Code, "invalid_request")
			}
		})
	}

	if checkerCalls != 0 {
		t.Fatalf("spoofed requests invoked checker %d times, want 0", checkerCalls)
	}
}

func TestPermissionsUnsupportedMethodsHaveCacheControlNoStore(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(method, "/v1/me/permissions", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s /v1/me/permissions status = %d, want %d", method, rec.Code, http.StatusMethodNotAllowed)
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("%s Cache-Control = %q, want %q", method, rec.Header().Get("Cache-Control"), "no-store")
		}
	}
}

package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
)

type adminGatewayListerFunc func(context.Context, uuid.UUID, gateway.AdminGatewayInventoryQuery) (gateway.AdminGatewayInventoryPage, error)

func (f adminGatewayListerFunc) ListAdminGatewayInventory(ctx context.Context, actor uuid.UUID, query gateway.AdminGatewayInventoryQuery) (gateway.AdminGatewayInventoryPage, error) {
	return f(ctx, actor, query)
}

func TestAdminGatewayInventoryHTTPHandler(t *testing.T) {
	adminID := uuid.New()
	normalUserID := uuid.New()

	for _, tc := range []struct {
		name          string
		authenticated bool
		userRole      string
		userID        uuid.UUID
		isAdmin       bool
		urlPath       string
		listerErr     error
		listerPage    gateway.AdminGatewayInventoryPage
		wantStatus    int
		wantCode      string
	}{
		{
			name:          "unauthenticated missing token",
			authenticated: false,
			urlPath:       "/v1/admin/gateways",
			wantStatus:    http.StatusUnauthorized,
			wantCode:      "unauthorized",
		},
		{
			name:          "authenticated non-admin forbidden",
			authenticated: true,
			userRole:      "authenticated",
			userID:        normalUserID,
			isAdmin:       false,
			urlPath:       "/v1/admin/gateways",
			wantStatus:    http.StatusForbidden,
			wantCode:      "forbidden",
		},
		{
			name:          "admin success default pagination",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways",
			listerPage: gateway.AdminGatewayInventoryPage{
				Items: []gateway.AdminGateway{
					{
						GatewayID:   "gw-01",
						Name:        "Gateway 1",
						Description: nil,
						CreatedAt:   nil,
					},
				},
				NextCursor: nil,
			},
			wantStatus: http.StatusOK,
		},
		{
			name:          "admin success empty list",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways",
			listerPage: gateway.AdminGatewayInventoryPage{
				Items:      []gateway.AdminGateway{},
				NextCursor: nil,
			},
			wantStatus: http.StatusOK,
		},
		{
			name:          "unknown query parameter rejected",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?unknown_param=123",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "scope override user_id query rejected",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?user_id=" + uuid.NewString(),
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "duplicate limit query parameter rejected",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?limit=10&limit=20",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "duplicate after query parameter rejected",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?after=gw-1&after=gw-2",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "malformed non-integer limit rejected",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?limit=abc",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "zero limit rejected",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?limit=0",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "negative limit rejected",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?limit=-5",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "limit exceeding max limit rejected",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?limit=201",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "empty after query rejected",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?after=",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "invalid after gateway id rejected",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?after=invalid*identifier",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "reserved after gateway id rejected",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?after=backend_service",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "malformed percent escape in limit value",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?limit=%zz",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "malformed percent escape in after value",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?after=%zz",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "malformed percent escape in unknown query key",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?%zz=1",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "malformed percent escape in unknown query value",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?unknown=%zz",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "semicolon in query string rejected",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?limit=10;after=gw-1",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "valid pair followed by malformed duplicate",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?limit=10&limit=%zz",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "malformed encoded duplicate key",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways?limit=10&%6c%69%6d%69%74=20",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "unauthenticated query with malformed query still 401",
			authenticated: false,
			urlPath:       "/v1/admin/gateways?limit=%zz",
			wantStatus:    http.StatusUnauthorized,
			wantCode:      "unauthorized",
		},
		{
			name:          "nonadmin query with malformed query still 403",
			authenticated: true,
			userRole:      "authenticated",
			userID:        normalUserID,
			isAdmin:       false,
			urlPath:       "/v1/admin/gateways?limit=%zz",
			wantStatus:    http.StatusForbidden,
			wantCode:      "forbidden",
		},
		{
			name:          "database admin check outage returns 503",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways",
			listerErr:     gateway.ErrAdminLookupFailed,
			wantStatus:    http.StatusServiceUnavailable,
			wantCode:      "service_unavailable",
		},
		{
			name:          "service query timeout returns 503",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways",
			listerErr:     context.DeadlineExceeded,
			wantStatus:    http.StatusServiceUnavailable,
			wantCode:      "service_unavailable",
		},
		{
			name:          "service admin recheck forbidden returns 403",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways",
			listerErr:     gateway.ErrAdminForbidden,
			wantStatus:    http.StatusForbidden,
			wantCode:      "forbidden",
		},
		{
			name:          "unexpected error returns safe 500",
			authenticated: true,
			userRole:      "authenticated",
			userID:        adminID,
			isAdmin:       true,
			urlPath:       "/v1/admin/gateways",
			listerErr:     errors.New("FATAL: internal connection leak detail"),
			wantStatus:    http.StatusInternalServerError,
			wantCode:      "internal_error",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
			deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
				if !tc.authenticated {
					return auth.Principal{}, errors.New("unauthenticated")
				}
				return auth.Principal{UserID: tc.userID, Role: tc.userRole}, nil
			})
			deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
				return tc.isAdmin, nil
			})
			deps.AdminGatewayLister = adminGatewayListerFunc(func(_ context.Context, actor uuid.UUID, _ gateway.AdminGatewayInventoryQuery) (gateway.AdminGatewayInventoryPage, error) {
				if actor != adminID {
					t.Fatalf("unexpected actor: %s", actor)
				}
				return tc.listerPage, tc.listerErr
			})
			deps.AdminInventoryDefaultLimit = 100
			deps.AdminInventoryMaxLimit = 200

			router, err := NewRouter(deps)
			if err != nil {
				t.Fatalf("failed to build router: %v", err)
			}

			req := httptest.NewRequest(http.MethodGet, tc.urlPath, nil)
			if tc.authenticated {
				req.Header.Set("Authorization", "Bearer token-fixture")
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d. Body: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}

			// All responses must include no-store and no-cache
			if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
				t.Errorf("Cache-Control = %q, want %q", cc, "no-store")
			}
			if pragma := rec.Header().Get("Pragma"); pragma != "no-cache" {
				t.Errorf("Pragma = %q, want %q", pragma, "no-cache")
			}

			if tc.wantCode != "" {
				var errEnv httpapi.APIErrorEnvelope
				if err := json.Unmarshal(rec.Body.Bytes(), &errEnv); err != nil {
					t.Fatalf("failed to parse error response: %v", err)
				}
				if errEnv.Error.Code != tc.wantCode {
					t.Errorf("error.code = %q, want %q", errEnv.Error.Code, tc.wantCode)
				}
				if strings.Contains(rec.Body.String(), "FATAL") || strings.Contains(rec.Body.String(), "leak") {
					t.Errorf("sensitive internal detail leaked in body: %s", rec.Body.String())
				}
			}

			if tc.wantStatus == http.StatusOK {
				var res struct {
					Items []struct {
						GatewayID   string  `json:"gateway_id"`
						Name        string  `json:"name"`
						Description *string `json:"description"`
						Role        *string `json:"role"`
					} `json:"items"`
					NextCursor *string `json:"next_cursor"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
					t.Fatalf("failed to decode response: %v", err)
				}
				for _, item := range res.Items {
					if item.Role != nil {
						t.Errorf("role must not be present in admin gateway inventory item: %v", item)
					}
				}
			}
		})
	}
}

func TestAdminGatewayInventoryAuditLogging(t *testing.T) {
	adminID := uuid.New()
	nonAdminID := uuid.New()

	t.Run("success 200 exactly one audit event", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
		oldLogger := slog.Default()
		slog.SetDefault(logger)
		defer slog.SetDefault(oldLogger)

		listerCalls := 0
		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
			return auth.Principal{UserID: adminID, Role: "authenticated"}, nil
		})
		deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
			return true, nil
		})
		deps.AdminGatewayLister = adminGatewayListerFunc(func(context.Context, uuid.UUID, gateway.AdminGatewayInventoryQuery) (gateway.AdminGatewayInventoryPage, error) {
			listerCalls++
			return gateway.AdminGatewayInventoryPage{Items: []gateway.AdminGateway{}}, nil
		})
		deps.AdminInventoryDefaultLimit = 100
		deps.AdminInventoryMaxLimit = 200

		router, err := NewRouter(deps)
		if err != nil {
			t.Fatalf("failed to build router: %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/admin/gateways", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("X-Request-ID", "audit-test-request-id-200")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if listerCalls != 1 {
			t.Fatalf("listerCalls = %d, want 1", listerCalls)
		}

		entries := parseAuditEntries(logBuf.String(), "list_admin_gateways")
		if len(entries) != 1 {
			t.Fatalf("expected exactly 1 audit entry for success, got %d: %s", len(entries), logBuf.String())
		}
		entry := entries[0]
		if entry["actor"] != adminID.String() {
			t.Errorf("actor = %v, want %s", entry["actor"], adminID.String())
		}
		if entry["request_id"] != "audit-test-request-id-200" {
			t.Errorf("request_id = %v, want audit-test-request-id-200", entry["request_id"])
		}
		if entry["outcome"] != "success" {
			t.Errorf("outcome = %v, want success", entry["outcome"])
		}
	})

	t.Run("authenticated nonadmin denial 403 exactly one audit event", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
		oldLogger := slog.Default()
		slog.SetDefault(logger)
		defer slog.SetDefault(oldLogger)

		listerCalls := 0
		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
			return auth.Principal{UserID: nonAdminID, Role: "authenticated"}, nil
		})
		deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
			return false, nil
		})
		deps.AdminGatewayLister = adminGatewayListerFunc(func(context.Context, uuid.UUID, gateway.AdminGatewayInventoryQuery) (gateway.AdminGatewayInventoryPage, error) {
			listerCalls++
			return gateway.AdminGatewayInventoryPage{}, nil
		})

		router, err := NewRouter(deps)
		if err != nil {
			t.Fatalf("failed to build router: %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/admin/gateways", nil)
		req.Header.Set("Authorization", "Bearer valid-token-nonadmin")
		req.Header.Set("X-Request-ID", "audit-test-request-id-403")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", rec.Code)
		}
		if listerCalls != 0 {
			t.Fatalf("listerCalls = %d, want 0 on 403 denial", listerCalls)
		}

		entries := parseAuditEntries(logBuf.String(), "list_admin_gateways")
		if len(entries) != 1 {
			t.Fatalf("expected exactly 1 audit entry for 403 denial, got %d: %s", len(entries), logBuf.String())
		}
		entry := entries[0]
		if entry["actor"] != nonAdminID.String() {
			t.Errorf("actor = %v, want %s", entry["actor"], nonAdminID.String())
		}
		if entry["request_id"] != "audit-test-request-id-403" {
			t.Errorf("request_id = %v, want audit-test-request-id-403", entry["request_id"])
		}
		if entry["outcome"] != "forbidden" {
			t.Errorf("outcome = %v, want forbidden", entry["outcome"])
		}
		if strings.Contains(logBuf.String(), "valid-token-nonadmin") {
			t.Errorf("token leaked in log output: %s", logBuf.String())
		}
	})

	t.Run("admin checker outage 503 exactly one audit event", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
		oldLogger := slog.Default()
		slog.SetDefault(logger)
		defer slog.SetDefault(oldLogger)

		listerCalls := 0
		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
			return auth.Principal{UserID: adminID, Role: "authenticated"}, nil
		})
		deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
			return false, errors.New("db outage connection refused: secret_db_detail")
		})
		deps.AdminGatewayLister = adminGatewayListerFunc(func(context.Context, uuid.UUID, gateway.AdminGatewayInventoryQuery) (gateway.AdminGatewayInventoryPage, error) {
			listerCalls++
			return gateway.AdminGatewayInventoryPage{}, nil
		})

		router, err := NewRouter(deps)
		if err != nil {
			t.Fatalf("failed to build router: %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/admin/gateways", nil)
		req.Header.Set("Authorization", "Bearer valid-token-outage")
		req.Header.Set("X-Request-ID", "audit-test-request-id-503")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
		if listerCalls != 0 {
			t.Fatalf("listerCalls = %d, want 0 on 503 outage", listerCalls)
		}

		entries := parseAuditEntries(logBuf.String(), "list_admin_gateways")
		if len(entries) != 1 {
			t.Fatalf("expected exactly 1 audit entry for 503 outage, got %d: %s", len(entries), logBuf.String())
		}
		entry := entries[0]
		if entry["actor"] != adminID.String() {
			t.Errorf("actor = %v, want %s", entry["actor"], adminID.String())
		}
		if entry["request_id"] != "audit-test-request-id-503" {
			t.Errorf("request_id = %v, want audit-test-request-id-503", entry["request_id"])
		}
		if entry["outcome"] != "service_unavailable" {
			t.Errorf("outcome = %v, want service_unavailable", entry["outcome"])
		}
		if strings.Contains(logBuf.String(), "secret_db_detail") && entry["outcome"] == "service_unavailable" {
			// Ensure audit entry doesn't leak driver secrets
			if strings.Contains(fmt.Sprint(entry), "secret_db_detail") {
				t.Errorf("driver secret leaked in audit entry: %v", entry)
			}
		}
	})

	t.Run("missing JWT 401 exactly one audit event with anonymous actor", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
		oldLogger := slog.Default()
		slog.SetDefault(logger)
		defer slog.SetDefault(oldLogger)

		listerCalls := 0
		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.AdminGatewayLister = adminGatewayListerFunc(func(context.Context, uuid.UUID, gateway.AdminGatewayInventoryQuery) (gateway.AdminGatewayInventoryPage, error) {
			listerCalls++
			return gateway.AdminGatewayInventoryPage{}, nil
		})

		router, err := NewRouter(deps)
		if err != nil {
			t.Fatalf("failed to build router: %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/admin/gateways", nil)
		req.Header.Set("X-Request-ID", "audit-test-request-id-401")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rec.Code)
		}
		if listerCalls != 0 {
			t.Fatalf("listerCalls = %d, want 0 on 401", listerCalls)
		}

		entries := parseAuditEntries(logBuf.String(), "list_admin_gateways")
		if len(entries) != 1 {
			t.Fatalf("expected exactly 1 audit entry for 401 missing JWT, got %d: %s", len(entries), logBuf.String())
		}
		entry := entries[0]
		if entry["actor"] != "anonymous" {
			t.Errorf("actor = %v, want anonymous", entry["actor"])
		}
		if entry["request_id"] != "audit-test-request-id-401" {
			t.Errorf("request_id = %v, want audit-test-request-id-401", entry["request_id"])
		}
		if entry["outcome"] != "unauthorized" {
			t.Errorf("outcome = %v, want unauthorized", entry["outcome"])
		}
	})

	t.Run("invalid JWT 401 must never use unverified claims as actor", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
		oldLogger := slog.Default()
		slog.SetDefault(logger)
		defer slog.SetDefault(oldLogger)

		listerCalls := 0
		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
			return auth.Principal{}, errors.New("signature verification failed")
		})
		deps.AdminGatewayLister = adminGatewayListerFunc(func(context.Context, uuid.UUID, gateway.AdminGatewayInventoryQuery) (gateway.AdminGatewayInventoryPage, error) {
			listerCalls++
			return gateway.AdminGatewayInventoryPage{}, nil
		})

		router, err := NewRouter(deps)
		if err != nil {
			t.Fatalf("failed to build router: %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/admin/gateways?spoofed_claim=attacker-uuid", nil)
		req.Header.Set("Authorization", "Bearer forged-token-for-attacker-uuid")
		req.Header.Set("X-Request-ID", "audit-test-request-id-forged")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rec.Code)
		}
		if listerCalls != 0 {
			t.Fatalf("listerCalls = %d, want 0 on 401", listerCalls)
		}

		entries := parseAuditEntries(logBuf.String(), "list_admin_gateways")
		if len(entries) != 1 {
			t.Fatalf("expected exactly 1 audit entry for 401 invalid JWT, got %d: %s", len(entries), logBuf.String())
		}
		entry := entries[0]
		if entry["actor"] == "attacker-uuid" {
			t.Errorf("actor used unverified claim: %v", entry["actor"])
		}
		if entry["actor"] != "anonymous" {
			t.Errorf("actor = %v, want anonymous", entry["actor"])
		}
		if entry["outcome"] != "unauthorized" {
			t.Errorf("outcome = %v, want unauthorized", entry["outcome"])
		}
		if strings.Contains(logBuf.String(), "forged-token-for-attacker-uuid") {
			t.Errorf("token leaked in log: %s", logBuf.String())
		}
	})

	t.Run("lister panic recovers 500 and emits exactly one internal_error audit event", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
		oldLogger := slog.Default()
		slog.SetDefault(logger)
		defer slog.SetDefault(oldLogger)

		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
			return auth.Principal{UserID: adminID, Role: "authenticated"}, nil
		})
		deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
			return true, nil
		})
		deps.AdminGatewayLister = adminGatewayListerFunc(func(context.Context, uuid.UUID, gateway.AdminGatewayInventoryQuery) (gateway.AdminGatewayInventoryPage, error) {
			panic("simulated fatal lister panic with raw secret: db_secret_password")
		})

		router, err := NewRouter(deps)
		if err != nil {
			t.Fatalf("failed to build router: %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/admin/gateways", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("X-Request-ID", "audit-panic-test-500")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", rec.Code)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", cc)
		}

		entries := parseAuditEntries(logBuf.String(), "list_admin_gateways")
		if len(entries) != 1 {
			t.Fatalf("expected exactly 1 inventory audit entry on panic, got %d: %s", len(entries), logBuf.String())
		}
		entry := entries[0]
		if entry["actor"] != adminID.String() {
			t.Errorf("actor = %v, want %s", entry["actor"], adminID.String())
		}
		if entry["request_id"] != "audit-panic-test-500" {
			t.Errorf("request_id = %v, want audit-panic-test-500", entry["request_id"])
		}
		if entry["outcome"] != "internal_error" {
			t.Errorf("outcome = %v, want internal_error", entry["outcome"])
		}
		if strings.Contains(fmt.Sprint(entry), "db_secret_password") || strings.Contains(fmt.Sprint(entry), "simulated fatal") {
			t.Errorf("raw panic value leaked into audit log entry: %v", entry)
		}
	})

	t.Run("response writer panic during render yields recovered 500 and exactly one internal_error audit event", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
		oldLogger := slog.Default()
		slog.SetDefault(logger)
		defer slog.SetDefault(oldLogger)

		rec := httptest.NewRecorder()
		writer := &panicOnRenderResponseWriter{ResponseWriter: rec}

		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
			return auth.Principal{UserID: adminID, Role: "authenticated"}, nil
		})
		deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
			return true, nil
		})
		deps.AdminGatewayLister = adminGatewayListerFunc(func(context.Context, uuid.UUID, gateway.AdminGatewayInventoryQuery) (gateway.AdminGatewayInventoryPage, error) {
			writer.afterLister = true
			return gateway.AdminGatewayInventoryPage{Items: []gateway.AdminGateway{
				{GatewayID: "gw-1", Name: "Gateway 1"},
			}}, nil
		})

		router, err := NewRouter(deps)
		if err != nil {
			t.Fatalf("failed to build router: %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/admin/gateways", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		req.Header.Set("X-Request-ID", "audit-render-panic-test-500")

		router.ServeHTTP(writer, req)

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", rec.Code)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", cc)
		}

		entries := parseAuditEntries(logBuf.String(), "list_admin_gateways")
		for _, e := range entries {
			if e["outcome"] == "success" {
				t.Fatalf("audit event must NEVER be success when render panicked: %v", e)
			}
		}
		if len(entries) != 1 {
			t.Fatalf("expected exactly 1 inventory audit entry on render panic, got %d: %s", len(entries), logBuf.String())
		}
		entry := entries[0]
		if entry["actor"] != adminID.String() {
			t.Errorf("actor = %v, want %s", entry["actor"], adminID.String())
		}
		if entry["request_id"] != "audit-render-panic-test-500" {
			t.Errorf("request_id = %v, want audit-render-panic-test-500", entry["request_id"])
		}
		if entry["outcome"] != "internal_error" {
			t.Errorf("outcome = %v, want internal_error", entry["outcome"])
		}
		if strings.Contains(fmt.Sprint(entry), "raw_writer_secret") || strings.Contains(fmt.Sprint(entry), "simulated response writer") {
			t.Errorf("raw panic value leaked into audit log entry: %v", entry)
		}
	})

	t.Run("OPTIONS CORS preflight does not emit admin business audit log", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
		oldLogger := slog.Default()
		slog.SetDefault(logger)
		defer slog.SetDefault(oldLogger)

		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.CORSAllowedOrigins = []string{"http://localhost:3000"}

		router, err := NewRouter(deps)
		if err != nil {
			t.Fatalf("failed to build router: %v", err)
		}

		req := httptest.NewRequest(http.MethodOptions, "/v1/admin/gateways", nil)
		req.Header.Set("Origin", "http://localhost:3000")
		req.Header.Set("Access-Control-Request-Method", "GET")
		req.Header.Set("Access-Control-Request-Headers", "authorization")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", rec.Code)
		}

		entries := parseAuditEntries(logBuf.String(), "list_admin_gateways")
		if len(entries) != 0 {
			t.Fatalf("OPTIONS preflight unexpectedly emitted audit log: %v", entries)
		}
	})

	t.Run("unrecovered panic in stack without inner recovery propagates unchanged sentinel and emits safe internal_error audit event", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
		oldLogger := slog.Default()
		slog.SetDefault(logger)
		defer slog.SetDefault(oldLogger)

		type sentinelPanic struct {
			token string
			leak  string
		}
		expectedPanic := &sentinelPanic{
			token: "sentinel-unique-pointer-12345",
			leak:  "unrecovered_secret_password_do_not_log",
		}

		gin.SetMode(gin.ReleaseMode)
		engine := gin.New()
		engine.Use(httpapi.RequestIDMiddleware())
		engine.Use(adminGatewayCacheMiddleware())
		engine.Use(adminGatewayAuditMiddleware())
		// NOTE: Intentionally NO RecoveryMiddleware in this engine stack to exercise unrecovered audit defer branch.

		engine.GET("/v1/admin/gateways", func(c *gin.Context) {
			// Populate authenticated principal so actor is verified admin ID, not anonymous.
			auth.SetPrincipal(c, auth.Principal{UserID: adminID, Role: "authenticated"})
			panic(expectedPanic)
		})

		req := httptest.NewRequest(http.MethodGet, "/v1/admin/gateways?token=query_secret", nil)
		req.Header.Set("Authorization", "Bearer sensitive-raw-jwt")
		req.Header.Set("X-Request-ID", "unrecovered-audit-panic-request-id")
		rec := httptest.NewRecorder()

		var caughtPanic any
		func() {
			defer func() {
				caughtPanic = recover()
			}()
			engine.ServeHTTP(rec, req)
		}()

		if caughtPanic == nil {
			t.Fatal("expected panic to be re-thrown by adminGatewayAuditMiddleware, but nothing was caught")
		}
		caughtSentinel, ok := caughtPanic.(*sentinelPanic)
		if !ok || caughtSentinel != expectedPanic {
			t.Fatalf("caught panic identity mismatch: got %v (%T), want exact pointer %p", caughtPanic, caughtPanic, expectedPanic)
		}

		entries := parseAuditEntries(logBuf.String(), "list_admin_gateways")
		if len(entries) != 1 {
			t.Fatalf("expected exactly 1 inventory audit entry on unrecovered panic, got %d: %s", len(entries), logBuf.String())
		}
		entry := entries[0]
		if entry["actor"] != adminID.String() {
			t.Errorf("actor = %v, want %s", entry["actor"], adminID.String())
		}
		if entry["request_id"] != "unrecovered-audit-panic-request-id" {
			t.Errorf("request_id = %v, want unrecovered-audit-panic-request-id", entry["request_id"])
		}
		if entry["outcome"] != "internal_error" {
			t.Errorf("outcome = %v, want internal_error", entry["outcome"])
		}

		fullLog := logBuf.String()
		if strings.Contains(fullLog, "unrecovered_secret_password") || strings.Contains(fullLog, "query_secret") || strings.Contains(fullLog, "sensitive-raw-jwt") || strings.Contains(fullLog, "sentinel-unique-pointer") {
			t.Errorf("sensitive value or panic leak in audit log output: %s", fullLog)
		}
	})

	t.Run("unrecovered panic handled by outer recovery produces 500 no-store and exactly one internal_error audit event", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
		oldLogger := slog.Default()
		slog.SetDefault(logger)
		defer slog.SetDefault(oldLogger)

		type sentinelPanic struct {
			rawSecret string
		}
		outerSentinel := &sentinelPanic{rawSecret: "outer_recovery_unrecovered_secret"}

		gin.SetMode(gin.ReleaseMode)
		engine := gin.New()
		engine.Use(httpapi.RequestIDMiddleware())
		engine.Use(httpapi.RecoveryMiddleware(logger)) // RecoveryMiddleware placed OUTSIDE audit middleware
		engine.Use(adminGatewayCacheMiddleware())
		engine.Use(adminGatewayAuditMiddleware())

		engine.GET("/v1/admin/gateways", func(c *gin.Context) {
			auth.SetPrincipal(c, auth.Principal{UserID: adminID, Role: "authenticated"})
			panic(outerSentinel)
		})

		req := httptest.NewRequest(http.MethodGet, "/v1/admin/gateways", nil)
		req.Header.Set("X-Request-ID", "outer-recovery-panic-req-id")
		rec := httptest.NewRecorder()

		engine.ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", rec.Code)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", cc)
		}

		entries := parseAuditEntries(logBuf.String(), "list_admin_gateways")
		if len(entries) != 1 {
			t.Fatalf("expected exactly 1 inventory audit entry, got %d: %s", len(entries), logBuf.String())
		}
		entry := entries[0]
		if entry["actor"] != adminID.String() {
			t.Errorf("actor = %v, want %s", entry["actor"], adminID.String())
		}
		if entry["request_id"] != "outer-recovery-panic-req-id" {
			t.Errorf("request_id = %v, want outer-recovery-panic-req-id", entry["request_id"])
		}
		if entry["outcome"] != "internal_error" {
			t.Errorf("outcome = %v, want internal_error", entry["outcome"])
		}

		fullLog := logBuf.String()
		if strings.Contains(fullLog, "outer_recovery_unrecovered_secret") {
			t.Errorf("raw panic value leaked into log output: %s", fullLog)
		}
		if strings.Contains(rec.Body.String(), "outer_recovery_unrecovered_secret") {
			t.Errorf("raw panic value leaked into response body: %s", rec.Body.String())
		}
	})
}

func parseAuditEntries(logOutput, action string) []map[string]any {
	var entries []map[string]any
	for _, line := range strings.Split(logOutput, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(trimmed), &m); err == nil {
			if m["action"] == action {
				entries = append(entries, m)
			}
		}
	}
	return entries
}

type panicOnRenderResponseWriter struct {
	http.ResponseWriter
	panicked    bool
	afterLister bool
}

func (w *panicOnRenderResponseWriter) Header() http.Header {
	if w.afterLister && !w.panicked {
		w.panicked = true
		panic("simulated response writer render panic: raw_writer_secret")
	}
	return w.ResponseWriter.Header()
}

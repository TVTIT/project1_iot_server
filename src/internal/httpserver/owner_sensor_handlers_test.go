package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
)

type ownerSensorProvisionerFunc func(context.Context, uuid.UUID, gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error)

func (f ownerSensorProvisionerFunc) ProvisionSensorForOwner(ctx context.Context, actor uuid.UUID, input gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error) {
	return f(ctx, actor, input)
}

func TestOwnerSensorProvisioningHTTPHandler(t *testing.T) {
	ownerID := uuid.New()
	operatorID := uuid.New()
	viewerID := uuid.New()
	adminID := uuid.New()
	nonMemberID := uuid.New()

	for _, tc := range []struct {
		name          string
		authenticated bool
		actorID       uuid.UUID
		urlPath       string
		body          string
		contentType   string
		provisionErr  error
		created       bool
		wantStatus    int
		wantCode      string
	}{
		{
			name:          "unauthenticated missing JWT",
			authenticated: false,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"name":"Temperature","unit":"°C"}`,
			contentType:   "application/json",
			wantStatus:    http.StatusUnauthorized,
			wantCode:      "unauthorized",
		},
		{
			name:          "owner success 201 created",
			authenticated: true,
			actorID:       ownerID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"name":"Temperature","unit":"°C"}`,
			contentType:   "application/json",
			created:       true,
			wantStatus:    http.StatusCreated,
		},
		{
			name:          "owner identical retry 200 ok",
			authenticated: true,
			actorID:       ownerID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"name":"Temperature","unit":"°C"}`,
			contentType:   "application/json",
			created:       false,
			wantStatus:    http.StatusOK,
		},
		{
			name:          "owner different metadata 409 conflict",
			authenticated: true,
			actorID:       ownerID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"name":"Temperature","unit":"F"}`,
			contentType:   "application/json",
			provisionErr:  gateway.ErrProvisioningConflict,
			wantStatus:    http.StatusConflict,
			wantCode:      "conflict",
		},
		{
			name:          "operator on same gateway 403 forbidden",
			authenticated: true,
			actorID:       operatorID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"name":"Temperature","unit":"°C"}`,
			contentType:   "application/json",
			provisionErr:  gateway.ErrProvisioningForbidden,
			wantStatus:    http.StatusForbidden,
			wantCode:      "forbidden",
		},
		{
			name:          "viewer on same gateway 403 forbidden",
			authenticated: true,
			actorID:       viewerID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"name":"Temperature","unit":"°C"}`,
			contentType:   "application/json",
			provisionErr:  gateway.ErrProvisioningForbidden,
			wantStatus:    http.StatusForbidden,
			wantCode:      "forbidden",
		},
		{
			name:          "non-member 404 not found",
			authenticated: true,
			actorID:       nonMemberID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"name":"Temperature","unit":"°C"}`,
			contentType:   "application/json",
			provisionErr:  gateway.ErrProvisioningNotFound,
			wantStatus:    http.StatusNotFound,
			wantCode:      "not_found",
		},
		{
			name:          "admin without owner membership 404 not found",
			authenticated: true,
			actorID:       adminID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"name":"Temperature","unit":"°C"}`,
			contentType:   "application/json",
			provisionErr:  gateway.ErrProvisioningNotFound,
			wantStatus:    http.StatusNotFound,
			wantCode:      "not_found",
		},
		{
			name:          "parent gateway not found 404",
			authenticated: true,
			actorID:       ownerID,
			urlPath:       "/v1/gateways/missing-gw/sensors/temp-01",
			body:          `{"name":"Temperature","unit":"°C"}`,
			contentType:   "application/json",
			provisionErr:  gateway.ErrProvisioningNotFound,
			wantStatus:    http.StatusNotFound,
			wantCode:      "not_found",
		},
		{
			name:          "membership lookup failure 503",
			authenticated: true,
			actorID:       ownerID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"name":"Temperature","unit":"°C"}`,
			contentType:   "application/json",
			provisionErr:  gateway.ErrProvisioningAdminLookup,
			wantStatus:    http.StatusServiceUnavailable,
			wantCode:      "service_unavailable",
		},
		{
			name:          "transaction timeout 503",
			authenticated: true,
			actorID:       ownerID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"name":"Temperature","unit":"°C"}`,
			contentType:   "application/json",
			provisionErr:  context.DeadlineExceeded,
			wantStatus:    http.StatusServiceUnavailable,
			wantCode:      "service_unavailable",
		},
		{
			name:          "malformed JSON 400",
			authenticated: true,
			actorID:       ownerID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{bad json}`,
			contentType:   "application/json",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "missing required name field 400",
			authenticated: true,
			actorID:       ownerID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"unit":"°C"}`,
			contentType:   "application/json",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "null name field 400",
			authenticated: true,
			actorID:       ownerID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"name":null,"unit":"°C"}`,
			contentType:   "application/json",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "unknown field in body rejected 400",
			authenticated: true,
			actorID:       ownerID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"name":"Temperature","reported_state":{"temp":25}}`,
			contentType:   "application/json",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "body spoofing gateway_id rejected 400",
			authenticated: true,
			actorID:       ownerID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"name":"Temperature","gateway_id":"gw-other"}`,
			contentType:   "application/json",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "query parameters rejected 400",
			authenticated: true,
			actorID:       ownerID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01?override=true",
			body:          `{"name":"Temperature"}`,
			contentType:   "application/json",
			wantStatus:    http.StatusBadRequest,
			wantCode:      "invalid_request",
		},
		{
			name:          "unsupported media type 415",
			authenticated: true,
			actorID:       ownerID,
			urlPath:       "/v1/gateways/gw-01/sensors/temp-01",
			body:          `{"name":"Temperature"}`,
			contentType:   "text/plain",
			wantStatus:    http.StatusUnsupportedMediaType,
			wantCode:      "unsupported_media_type",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
			deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
				if !tc.authenticated {
					return auth.Principal{}, errors.New("unauthenticated")
				}
				return auth.Principal{UserID: tc.actorID, Role: "authenticated"}, nil
			})
			deps.OwnerSensorProvisioner = ownerSensorProvisionerFunc(func(_ context.Context, actor uuid.UUID, input gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error) {
				if actor != tc.actorID {
					t.Fatalf("actor mismatch: got %s, want %s", actor, tc.actorID)
				}
				now := time.Now().UTC()
				return gateway.ProvisionSensorResult{
					Created:   tc.created,
					GatewayID: input.GatewayID,
					SensorID:  input.SensorID,
					Name:      input.Name,
					Unit:      input.Unit,
					EntityID:  "urn:ngsi-ld:Sensor:" + input.GatewayID + ":" + input.SensorID,
					CreatedAt: &now,
				}, tc.provisionErr
			})

			router, err := NewRouter(deps)
			if err != nil {
				t.Fatalf("failed to create router: %v", err)
			}

			req := httptest.NewRequest(http.MethodPut, tc.urlPath, strings.NewReader(tc.body))
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			if tc.authenticated {
				req.Header.Set("Authorization", "Bearer valid-token")
			}

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d. Body: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}

			// Must always have Cache-Control: no-store and Pragma: no-cache
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
			}
		})
	}
}

func TestOwnerSensorPreflightAndCacheControlHeaders(t *testing.T) {
	allowedOrigin := "http://localhost:3000"
	disallowedOrigin := "http://untrusted-attacker.com"

	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.CORSAllowedOrigins = []string{allowedOrigin}
	deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{}, errors.New("invalid token")
	})
	provisionCalls := 0
	deps.OwnerSensorProvisioner = ownerSensorProvisionerFunc(func(context.Context, uuid.UUID, gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error) {
		provisionCalls++
		return gateway.ProvisionSensorResult{}, nil
	})

	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	path := "/v1/gateways/gw-01/sensors/temp-01"

	t.Run("allowed origin OPTIONS preflight returns 204 with no-store and does not invoke provisioning", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, path, nil)
		req.Header.Set("Origin", allowedOrigin)
		req.Header.Set("Access-Control-Request-Method", "PUT")
		req.Header.Set("Access-Control-Request-Headers", "authorization,content-type")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("expected 204, got %d", rec.Code)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want %q", cc, "no-store")
		}
		if pragma := rec.Header().Get("Pragma"); pragma != "no-cache" {
			t.Errorf("Pragma = %q, want %q", pragma, "no-cache")
		}
		if provisionCalls != 0 {
			t.Errorf("provisioning called %d times on preflight", provisionCalls)
		}
	})

	t.Run("disallowed origin OPTIONS preflight returns 403 with no-store", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, path, nil)
		req.Header.Set("Origin", disallowedOrigin)
		req.Header.Set("Access-Control-Request-Method", "PUT")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected 403, got %d", rec.Code)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want %q", cc, "no-store")
		}
		if pragma := rec.Header().Get("Pragma"); pragma != "no-cache" {
			t.Errorf("Pragma = %q, want %q", pragma, "no-cache")
		}
		if provisionCalls != 0 {
			t.Errorf("provisioning called %d times on preflight", provisionCalls)
		}
	})

	t.Run("disallowed requested header OPTIONS preflight returns 403 with no-store", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, path, nil)
		req.Header.Set("Origin", allowedOrigin)
		req.Header.Set("Access-Control-Request-Method", "PUT")
		req.Header.Set("Access-Control-Request-Headers", "x-unsupported-header")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected 403, got %d", rec.Code)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want %q", cc, "no-store")
		}
		if pragma := rec.Header().Get("Pragma"); pragma != "no-cache" {
			t.Errorf("Pragma = %q, want %q", pragma, "no-cache")
		}
		if provisionCalls != 0 {
			t.Errorf("provisioning called %d times on preflight", provisionCalls)
		}
	})

	t.Run("invalid JWT actual PUT returns 401 with no-store", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(`{"name":"test"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer invalid-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", rec.Code)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want %q", cc, "no-store")
		}
		if pragma := rec.Header().Get("Pragma"); pragma != "no-cache" {
			t.Errorf("Pragma = %q, want %q", pragma, "no-cache")
		}
	})

	t.Run("unsupported method DELETE returns 405 with no-store", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, path, nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405, got %d", rec.Code)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want %q", cc, "no-store")
		}
		if pragma := rec.Header().Get("Pragma"); pragma != "no-cache" {
			t.Errorf("Pragma = %q, want %q", pragma, "no-cache")
		}
	})

	t.Run("unrelated path does not get owner sensor no-store", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/v1/gateways/gw-01/sensors", nil)
		req.Header.Set("Origin", allowedOrigin)
		req.Header.Set("Access-Control-Request-Method", "GET")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("expected 204, got %d", rec.Code)
		}
		// /v1/gateways/:gateway_id/sensors is not owner sensor provisioning, so ownerSensorCacheMiddleware must not match it
		if cc := rec.Header().Get("Cache-Control"); cc == "no-store" {
			t.Errorf("unrelated path unexpectedly got Cache-Control: no-store")
		}
	})
}

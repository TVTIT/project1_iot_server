package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
)

func TestSensorRouteRequiresAuthenticationAndAdmin(t *testing.T) {
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) { return false, nil })
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatal(err)
	}
	for _, authenticated := range []bool{false, true} {
		req := httptest.NewRequest("PUT", "/v1/admin/gateways/fixture/sensors/test", strings.NewReader(`{"name":"test"}`))
		req.Header.Set("Content-Type", "application/json")
		want := 401
		if authenticated {
			req.Header.Set("Authorization", "Bearer fixture")
			want = 403
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Fatalf("status=%d want=%d body=%s", rec.Code, want, rec.Body.String())
		}
	}
}

type sensorProvisionerFunc func(context.Context, uuid.UUID, gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error)

func (f sensorProvisionerFunc) ProvisionSensor(ctx context.Context, actor uuid.UUID, input gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error) {
	return f(ctx, actor, input)
}

func TestSensorProvisionerMandatory(t *testing.T) {
	for _, value := range []SensorProvisioner{nil, sensorProvisionerFunc(nil)} {
		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.SensorProvisioner = value
		if _, err := NewRouter(deps); err == nil {
			t.Fatal("missing sensor provisioner accepted")
		}
	}
}

func TestRegisteredSensorProvisioning(t *testing.T) {
	actor := uuid.New()
	stamp := time.Date(2026, 10, 3, 12, 0, 0, 0, time.FixedZone("offset", 7*3600))
	for _, tc := range []struct {
		name    string
		err     error
		created bool
		status  int
	}{
		{"created", nil, true, 201}, {"retry", nil, false, 200},
		{"conflict", gateway.ErrProvisioningConflict, false, 409},
		{"not found", gateway.ErrProvisioningNotFound, false, 404},
		{"inconsistent", gateway.ErrProvisioningInconsistent, false, 500},
		{"invalid", gateway.ErrInvalidProvisioningInput, false, 400},
		{"forbidden", gateway.ErrProvisioningForbidden, false, 403},
		{"deadline", context.DeadlineExceeded, false, 503},
		{"cancel", context.Canceled, false, 503},
		{"admin lookup", gateway.ErrProvisioningAdminLookup, false, 503},
		{"database", errors.New("SECRET SQL token"), false, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
			deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
				return auth.Principal{UserID: actor, Role: "authenticated"}, nil
			})
			calls := 0
			deps.SensorProvisioner = sensorProvisionerFunc(func(_ context.Context, a uuid.UUID, i gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error) {
				calls++
				if a != actor || i.GatewayID != "fixture" || i.SensorID != "backend_service" || i.Name != " raw " || i.Unit == nil || *i.Unit != "" {
					t.Fatal("identity/metadata changed")
				}
				return gateway.ProvisionSensorResult{Created: tc.created, GatewayID: i.GatewayID, SensorID: i.SensorID, Name: i.Name, Unit: i.Unit, EntityID: "urn:ngsi-ld:Sensor:fixture:backend_service", CreatedAt: &stamp}, tc.err
			})
			router, err := NewRouter(deps)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("PUT", "/v1/admin/gateways/fixture/sensors/backend_service", strings.NewReader(`{"name":" raw ","unit":""}`))
			req.Header.Set("Authorization", "Bearer fixture")
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != tc.status || calls != 1 {
				t.Fatalf("status=%d calls=%d body=%s", rec.Code, calls, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "SECRET") || rec.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("unsafe response")
			}
			if tc.err == nil {
				var dto map[string]any
				if json.Unmarshal(rec.Body.Bytes(), &dto) != nil || len(dto) != 6 || dto["created_at"] != "2026-10-03T05:00:00Z" || dto["unit"] != "" {
					t.Fatalf("DTO=%s", rec.Body.String())
				}
			}
		})
	}
}

func TestSensorHTTPValidation(t *testing.T) {
	for _, tc := range []struct {
		name, path, body, media string
		limit                   int64
		status                  int
	}{
		{"null unit", "/fixture/sensors/test", `{"name":"test","unit":null}`, "application/json", 16384, 201},
		{"missing unit", "/fixture/sensors/test", `{"name":"test"}`, "application/json", 16384, 201},
		{"unit limit", "/fixture/sensors/test", `{"name":"test","unit":"` + strings.Repeat("u", 64) + `"}`, "application/json", 16384, 201},
		{"unit too long", "/fixture/sensors/test", `{"name":"test","unit":"` + strings.Repeat("u", 65) + `"}`, "application/json", 16384, 400},
		{"name too long", "/fixture/sensors/test", `{"name":"` + strings.Repeat("n", 257) + `"}`, "application/json", 16384, 400},
		{"empty name", "/fixture/sensors/test", `{"name":" "}`, "application/json", 16384, 400},
		{"missing name", "/fixture/sensors/test", `{}`, "application/json", 16384, 400},
		{"null name", "/fixture/sensors/test", `{"name":null}`, "application/json", 16384, 400},
		{"duplicate", "/fixture/sensors/test", `{"name":"a","na\u006de":"b"}`, "application/json", 16384, 400},
		{"unknown actor", "/fixture/sensors/test", `{"name":"a","actor_user_id":"x"}`, "application/json", 16384, 400},
		{"state", "/fixture/sensors/test", `{"name":"a","reported_state":{}}`, "application/json", 16384, 400},
		{"bad unit", "/fixture/sensors/test", `{"name":"a","unit":3}`, "application/json", 16384, 400},
		{"trailing", "/fixture/sensors/test", `{"name":"a"}{}`, "application/json", 16384, 400},
		{"array", "/fixture/sensors/test", `[]`, "application/json", 16384, 400},
		{"UTF8", "/fixture/sensors/test", `{"name":"` + string([]byte{255}) + `"}`, "application/json", 16384, 400},
		{"surrogate", "/fixture/sensors/test", `{"name":"\ud800"}`, "application/json", 16384, 400},
		{"NUL", "/fixture/sensors/test", `{"name":"\u0000"}`, "application/json", 16384, 400},
		{"query", "/fixture/sensors/test?actor=x", `{"name":"a"}`, "application/json", 16384, 400},
		{"bare query", "/fixture/sensors/test?", `{"name":"a"}`, "application/json", 16384, 400},
		{"gateway ID", "/backend_service/sensors/test", `{"name":"a"}`, "application/json", 16384, 400},
		{"sensor ID", "/fixture/sensors/bad.id", `{"name":"a"}`, "application/json", 16384, 400},
		{"media", "/fixture/sensors/test", `{"name":"a"}`, "text/json", 16384, 415},
		{"charset", "/fixture/sensors/test", `{"name":"a"}`, "application/json; charset=latin1", 16384, 415},
		{"body", "/fixture/sensors/test", `{"name":"a"}`, "application/json", 2, 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
			deps.AdminMaxBodyBytes = tc.limit
			calls := 0
			deps.SensorProvisioner = sensorProvisionerFunc(func(_ context.Context, _ uuid.UUID, i gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error) {
				calls++
				return gateway.ProvisionSensorResult{Created: true, GatewayID: i.GatewayID, SensorID: i.SensorID, Name: i.Name, Unit: i.Unit}, nil
			})
			router, err := NewRouter(deps)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("PUT", "/v1/admin/gateways"+tc.path, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer fixture")
			req.Header.Set("Content-Type", tc.media)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", rec.Code, tc.status, rec.Body.String())
			}
			if tc.status != 201 && calls != 0 {
				t.Fatal("invalid input reached service")
			}
			if tc.status == 201 && !strings.Contains(rec.Body.String(), `"created_at":null`) {
				t.Fatal("nullable timestamp")
			}
		})
	}
}

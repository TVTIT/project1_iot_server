package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
)

type sensorReaderFunc func(context.Context, uuid.UUID, string) ([]gateway.Sensor, error)

func (f sensorReaderFunc) ListSensorsForUserAndGateway(ctx context.Context, user uuid.UUID, id string) ([]gateway.Sensor, error) {
	return f(ctx, user, id)
}

func TestSensorListBoundary(t *testing.T) {
	caller := uuid.New()
	calls := 0
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.TokenVerifier = tokenVerifierFunc(func(_ context.Context, token string) (auth.Principal, error) {
		if token != "valid" {
			return auth.Principal{}, errors.New("invalid")
		}
		return auth.Principal{UserID: caller, Role: "authenticated"}, nil
	})
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
		t.Fatal("sensor list queried admin rights")
		return false, nil
	})
	stamp := time.Date(2026, 1, 1, 12, 0, 0, 0, time.FixedZone("fixture", 3600))
	deps.SensorReader = sensorReaderFunc(func(_ context.Context, user uuid.UUID, id string) ([]gateway.Sensor, error) {
		calls++
		if user != caller || id != "fixture" {
			t.Fatal("wrong caller/parent")
		}
		return []gateway.Sensor{{SensorID: "a", Name: "first"}, {SensorID: "z", Name: "last", CreatedAt: &stamp}}, nil
	})
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		token, path string
		status      int
	}{
		{"", "/v1/gateways/fixture/sensors", 401}, {"bad", "/v1/gateways/fixture/sensors", 401},
		{"valid", "/v1/gateways/bad:id/sensors", 400}, {"valid", "/v1/gateways/backend_service/sensors", 400},
		{"valid", "/v1/gateways/" + strings.Repeat("a", 65) + "/sensors", 400},
		{"valid", "/v1/gateways/fixture/sensors?user_id=", 400},
		{"valid", "/v1/gateways/fixture/sensors?user_id=a&user_id=b", 400},
		{"valid", "/v1/gateways/extra/path/sensors", 404}, {"valid", "/v1/gateways/fixture/sensors", 200},
	} {
		req := httptest.NewRequest(http.MethodGet, tt.path, strings.NewReader(`{"user_id":"other"}`))
		if tt.token != "" {
			req.Header.Set("Authorization", "Bearer "+tt.token)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != tt.status {
			t.Fatalf("%s: status=%d want=%d", tt.path, rec.Code, tt.status)
		}
		if tt.status == 200 {
			var body struct {
				GatewayID string           `json:"gateway_id"`
				Items     []map[string]any `json:"items"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.GatewayID != "fixture" || len(body.Items) != 2 || len(body.Items[0]) != 4 || body.Items[0]["sensor_id"] != "a" || body.Items[0]["unit"] != nil || body.Items[0]["created_at"] != nil || body.Items[1]["sensor_id"] != "z" || body.Items[1]["created_at"] != "2026-01-01T11:00:00Z" {
				t.Fatal("wrong sensor DTO/order/nullability/UTC")
			}
			if rec.Header().Get("Cache-Control") != "no-store" || !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
				t.Fatal("wrong headers")
			}
		}
	}
	if calls != 1 {
		t.Fatal("invalid request reached reader")
	}
}

func TestSensorListErrorsAndEmpty(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(old)
	for _, tt := range []struct {
		cause  error
		status int
		code   string
	}{
		{nil, 200, ""}, {gateway.ErrNotFound, 404, "not_found"}, {context.DeadlineExceeded, 503, "service_unavailable"}, {context.Canceled, 503, "service_unavailable"}, {errors.New("SECRET_CONNECTION_STRING_TOKEN"), 500, "internal_error"},
	} {
		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.SensorReader = sensorReaderFunc(func(context.Context, uuid.UUID, string) ([]gateway.Sensor, error) { return nil, tt.cause })
		router, err := NewRouter(deps)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodGet, "/v1/gateways/fixture/sensors", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != tt.status {
			t.Fatalf("status=%d", rec.Code)
		}
		if tt.cause == nil {
			if rec.Body.String() != `{"gateway_id":"fixture","items":[]}` {
				t.Fatal("empty list not array")
			}
		} else {
			var body httpapi.APIErrorEnvelope
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Error.Code != tt.code || body.Error.RequestID == "" || body.Error.RequestID != rec.Header().Get("X-Request-ID") || strings.Contains(rec.Body.String(), "SECRET") {
				t.Fatal("unsafe error")
			}
			if tt.status == 404 && body.Error.Message != "resource not found" {
				t.Fatal("enumerating error")
			}
		}
	}
	if strings.Contains(logs.String(), "SECRET") {
		t.Fatal("raw error leaked into log")
	}
}

func TestSensorHandlerMissingPrincipal(t *testing.T) {
	r := gin.New()
	r.Use(httpapi.RequestIDMiddleware())
	r.GET("/sensors", listSensors(sensorReaderFunc(func(context.Context, uuid.UUID, string) ([]gateway.Sensor, error) {
		t.Fatal("missing principal reached reader")
		return nil, nil
	})))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/sensors", nil))
	if rec.Code != 401 {
		t.Fatal("missing principal accepted")
	}
}

func TestRouterRejectsMissingSensorReader(t *testing.T) {
	var typedNil sensorReaderFunc
	for _, reader := range []SensorReader{nil, typedNil} {
		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.SensorReader = reader
		if _, err := NewRouter(deps); err == nil {
			t.Fatal("accepted nil sensor reader")
		}
	}
}

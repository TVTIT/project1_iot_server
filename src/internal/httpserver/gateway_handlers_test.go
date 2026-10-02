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

type gatewayReaderFunc func(context.Context, uuid.UUID) ([]gateway.Gateway, error)

func (f gatewayReaderFunc) ListGatewaysForUser(c context.Context, u uuid.UUID) ([]gateway.Gateway, error) {
	return f(c, u)
}
func TestGatewayListBoundary(t *testing.T) {
	user := uuid.New()
	calls := 0
	deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
	deps.TokenVerifier = tokenVerifierFunc(func(_ context.Context, token string) (auth.Principal, error) {
		if token != "valid" {
			return auth.Principal{}, errors.New("invalid")
		}
		return auth.Principal{UserID: user, Role: "authenticated"}, nil
	})
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
		t.Fatal("read API queried global admin")
		return false, nil
	})
	stamp := time.Date(2026, 1, 1, 12, 0, 0, 123000000, time.FixedZone("fixture", 3600))
	deps.GatewayReader = gatewayReaderFunc(func(_ context.Context, id uuid.UUID) ([]gateway.Gateway, error) {
		calls++
		if id != user {
			t.Fatal("spoofed user")
		}
		return []gateway.Gateway{{GatewayID: "fixture", Name: "Gateway", Role: gateway.RoleViewer, CreatedAt: &stamp}}, nil
	})
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		token, path, body string
		status            int
	}{{"", "/v1/gateways", "", 401}, {"bad", "/v1/gateways", "", 401}, {"valid", "/v1/gateways?user_id=", "", 400}, {"valid", "/v1/gateways?user_id=a&user_id=b", "", 400}, {"valid", "/v1/gateways", `{"user_id":"other"}`, 200}} {
		req := httptest.NewRequest(http.MethodGet, tt.path, strings.NewReader(tt.body))
		if tt.token != "" {
			req.Header.Set("Authorization", "Bearer "+tt.token)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != tt.status {
			t.Fatalf("status=%d want=%d", rec.Code, tt.status)
		}
		if tt.status == 200 {
			var body map[string][]map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			items := body["items"]
			if len(body) != 1 || len(items) != 1 || len(items[0]) != 5 || items[0]["description"] != nil || items[0]["created_at"] != "2026-01-01T11:00:00.123Z" {
				t.Fatalf("wrong DTO: %s", rec.Body.String())
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("missing no-store")
			}
		}
	}
	if calls != 1 {
		t.Fatal("invalid request reached reader")
	}
}
func TestGatewayListEmptyAndErrors(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(previous)
	for _, tt := range []struct {
		cause  error
		status int
		code   string
	}{{nil, 200, ""}, {context.DeadlineExceeded, 503, "service_unavailable"}, {context.Canceled, 503, "service_unavailable"}, {errors.New("SECRET_DATABASE_FAILURE"), 500, "internal_error"}} {
		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.GatewayReader = gatewayReaderFunc(func(context.Context, uuid.UUID) ([]gateway.Gateway, error) { return nil, tt.cause })
		router, err := NewRouter(deps)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodGet, "/v1/gateways", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != tt.status {
			t.Fatalf("status=%d", rec.Code)
		}
		if tt.cause == nil {
			if rec.Body.String() != `{"items":[]}` {
				t.Fatal("empty list not array")
			}
		} else {
			var body httpapi.APIErrorEnvelope
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Error.Code != tt.code || body.Error.RequestID != rec.Header().Get("X-Request-ID") || body.Error.RequestID == "" || strings.Contains(rec.Body.String(), "SECRET") {
				t.Fatal("unsafe error")
			}
		}
	}
	if strings.Contains(logs.String(), "SECRET_DATABASE_FAILURE") {
		t.Fatal("raw error leaked into logs")
	}
}

func TestGatewayHandlerRejectsAbsentPrincipal(t *testing.T) {
	router := gin.New()
	router.Use(httpapi.RequestIDMiddleware())
	router.GET("/v1/gateways", listGateways(gatewayReaderFunc(func(context.Context, uuid.UUID) ([]gateway.Gateway, error) {
		t.Fatal("missing principal reached reader")
		return nil, nil
	})))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/gateways", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatal("missing principal accepted")
	}
}
func TestRouterRejectsMissingGatewayReader(t *testing.T) {
	var typedNil *nilGatewayReader
	for _, reader := range []GatewayReader{nil, typedNil} {
		deps := testRouterDependencies(readinessCheckerFunc(func(context.Context) error { return nil }))
		deps.GatewayReader = reader
		if _, err := NewRouter(deps); err == nil {
			t.Fatal("accepted nil reader")
		}
	}
}

type nilGatewayReader struct{}

func (*nilGatewayReader) ListGatewaysForUser(context.Context, uuid.UUID) ([]gateway.Gateway, error) {
	return nil, nil
}

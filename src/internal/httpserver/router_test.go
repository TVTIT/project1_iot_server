package httpserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type readinessCheckerFunc func(context.Context) error

func (f readinessCheckerFunc) Ping(ctx context.Context) error {
	return f(ctx)
}

func TestHealthzReportsProcessLiveness(t *testing.T) {
	router := NewRouter(readinessCheckerFunc(func(context.Context) error {
		return errors.New("database unavailable")
	}), time.Second)

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
			router := NewRouter(tt.check, time.Second)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))

			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, tt.wantStatus)
			}
			if response.Header().Get("Content-Type") != "application/json; charset=utf-8" {
				t.Fatalf("Content-Type = %q", response.Header().Get("Content-Type"))
			}
		})
	}
}

func TestPlannedBusinessRoutesRemainExplicitlyUnavailable(t *testing.T) {
	router := NewRouter(readinessCheckerFunc(func(context.Context) error { return nil }), time.Second)

	for _, path := range []string{"/v1/telemetry/history", "/v1/ws", "/v1/digital-twins"} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusNotImplemented {
			t.Errorf("GET %s status = %d, want %d", path, response.Code, http.StatusNotImplemented)
		}
	}
}

func TestRequestIDMiddlewarePropagatesHeader(t *testing.T) {
	router := NewRouter(readinessCheckerFunc(func(context.Context) error { return nil }), time.Second)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", "custom-request-id-123")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if got := rec.Header().Get("X-Request-ID"); got != "custom-request-id-123" {
		t.Errorf("X-Request-ID = %q, want %q", got, "custom-request-id-123")
	}
}

package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRequestIDMiddlewareAcceptsOnlySafeValues(t *testing.T) {
	tests := []struct {
		name      string
		requestID string
		preserved bool
	}{
		{name: "missing"},
		{name: "UUID", requestID: "0195e18c-9fc1-7a42-9064-69ea49e63bf2", preserved: true},
		{name: "safe trace id", requestID: "trace-abc_123.4:5", preserved: true},
		{name: "maximum length", requestID: "a" + strings.Repeat("b", 127), preserved: true},
		{name: "leading underscore", requestID: "_unsafe"},
		{name: "whitespace", requestID: "unsafe id"},
		{name: "unicode", requestID: "trace-đ"},
		{name: "slash", requestID: "trace/id"},
		{name: "query character", requestID: "trace?id"},
		{name: "fragment character", requestID: "trace#id"},
		{name: "too long", requestID: "a" + strings.Repeat("b", 128)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			router.Use(RequestIDMiddleware())
			router.GET("/test", func(c *gin.Context) {
				c.JSON(http.StatusBadRequest, ErrorResponse(c, "bad_request", "invalid request"))
			})

			request := httptest.NewRequest(http.MethodGet, "/test", nil)
			if tt.requestID != "" {
				request.Header.Set("X-Request-ID", tt.requestID)
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			got := response.Header().Get("X-Request-ID")
			if got == "" {
				t.Fatal("X-Request-ID is empty")
			}
			if tt.preserved && got != tt.requestID {
				t.Fatalf("X-Request-ID = %q, want preserved %q", got, tt.requestID)
			}
			if !tt.preserved && tt.requestID != "" && got == tt.requestID {
				t.Fatalf("unsafe X-Request-ID %q was preserved", got)
			}

			var body APIErrorEnvelope
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}
			if body.Error.RequestID != got {
				t.Errorf("body request_id = %q, header = %q", body.Error.RequestID, got)
			}
		})
	}
}

func TestWriteErrorReturnsSafeContract(t *testing.T) {
	router := gin.New()
	router.Use(RequestIDMiddleware())
	router.GET("/test", func(c *gin.Context) {
		WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
	})

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/test", nil))

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.Code)
	}
	if got := response.Header().Get("WWW-Authenticate"); got != "Bearer" {
		t.Errorf("WWW-Authenticate = %q, want Bearer", got)
	}
	var body APIErrorEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if body.Error.Code != "unauthorized" || body.Error.Message != "authentication required" {
		t.Fatalf("error body = %+v", body.Error)
	}
}

func TestRecoveryMiddlewareReturnsSafeErrorWithoutLoggingAuthorization(t *testing.T) {
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	router := gin.New()
	router.Use(RequestIDMiddleware(), RecoveryMiddleware(logger))
	router.GET("/panic", func(*gin.Context) { panic("sensitive panic detail") })

	request := httptest.NewRequest(http.MethodGet, "/panic", nil)
	request.Header.Set("Authorization", "Bearer secret-token-value")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.Code)
	}
	if strings.Contains(response.Body.String(), "sensitive") || strings.Contains(logs.String(), "secret-token-value") || strings.Contains(logs.String(), "sensitive panic detail") {
		t.Fatal("recovery exposed panic or authorization details")
	}
	if !strings.Contains(logs.String(), response.Header().Get("X-Request-ID")) {
		t.Fatal("recovery log is missing request ID")
	}
}

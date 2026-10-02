package httpapi

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestIDRejectsDuplicateAndControlCharacters(t *testing.T) {
	for _, headers := range [][]string{{"safe-first", "safe-second"}, {"unsafe\nvalue"}, {"unsafe\tvalue"}} {
		router := gin.New()
		router.Use(RequestIDMiddleware())
		router.GET("/", func(c *gin.Context) {
			if RequestIDFrom(c) != c.Writer.Header().Get("X-Request-ID") {
				t.Fatal("context/header mismatch")
			}
			c.Status(204)
		})
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		for _, v := range headers {
			req.Header.Add("X-Request-ID", v)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if _, err := uuid.Parse(rec.Header().Get("X-Request-ID")); err != nil {
			t.Fatal("did not generate UUID")
		}
	}
}

func TestRecoveryAfterResponseIsWritten(t *testing.T) {
	router := gin.New()
	router.Use(RequestIDMiddleware(), RecoveryMiddleware(slog.New(slog.NewTextHandler(io.Discard, nil))))
	router.GET("/", func(c *gin.Context) { c.String(http.StatusAccepted, "already written"); panic("private detail") }, func(*gin.Context) { t.Error("handler executed after panic") })
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != 202 || rec.Body.String() != "already written" {
		t.Fatalf("status=%d body=%q", rec.Code, rec.Body.String())
	}
}

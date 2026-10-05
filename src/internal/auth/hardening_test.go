package auth

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/httpapi"
)

func TestAdminCheckerRejectsNil(t *testing.T) {
	var pool *pgxpool.Pool
	for _, db := range []QueryRower{nil, pool} {
		if _, err := NewPostgresPlatformAdminChecker(db); err == nil {
			t.Fatal("nil database accepted")
		}
	}
}

func TestAdminLookupDeadlineAndSafeLog(t *testing.T) {
	var logs strings.Builder
	var received context.Context
	checker := adminCheckerFunc(func(ctx context.Context, _ uuid.UUID) (bool, error) {
		received = ctx
		if _, ok := ctx.Deadline(); !ok {
			t.Error("missing deadline")
		}
		<-ctx.Done()
		return false, ctx.Err()
	})
	router := gin.New()
	router.Use(httpapi.RequestIDMiddleware())
	router.GET("/admin", func(c *gin.Context) { SetPrincipal(c, Principal{UserID: uuid.New(), Role: "authenticated"}); c.Next() },
		PlatformAdminMiddleware(checker, time.Millisecond, slog.New(slog.NewTextHandler(&logs, nil))),
		func(*gin.Context) { t.Error("handler executed after timeout") })
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin", nil))
	if response.Code != 503 || !errors.Is(received.Err(), context.DeadlineExceeded) {
		t.Fatalf("status=%d context=%v", response.Code, received.Err())
	}
	if !strings.Contains(logs.String(), response.Header().Get("X-Request-ID")) {
		t.Fatal("missing request ID log")
	}
}

func TestAdminLookupCancellationAndRedaction(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		var logs strings.Builder
		var received context.Context
		parent, cancel := context.WithCancel(context.Background())
		if cancelled {
			cancel()
		}
		defer cancel()
		checker := adminCheckerFunc(func(ctx context.Context, _ uuid.UUID) (bool, error) {
			received = ctx
			if cancelled {
				return false, ctx.Err()
			}
			return false, errors.New("secret-db-url-and-token")
		})
		router := gin.New()
		router.Use(httpapi.RequestIDMiddleware())
		router.GET("/admin", func(c *gin.Context) { SetPrincipal(c, Principal{UserID: uuid.New(), Role: "authenticated"}); c.Next() }, PlatformAdminMiddleware(checker, time.Second, slog.New(slog.NewTextHandler(&logs, nil))))
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/admin", nil).WithContext(parent)
		router.ServeHTTP(response, request)
		if response.Code != 503 || received.Err() == nil {
			t.Fatal("failed lookup must abort and cancel context")
		}
		if strings.Contains(logs.String()+response.Body.String(), "secret-db-url-and-token") {
			t.Fatal("error leaked")
		}
	}
}

func TestAdminLookupPreservesParentDeadlineAndCancelsAfterSuccess(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	wantDeadline, _ := parent.Deadline()
	var received context.Context
	var logs strings.Builder
	checker := adminCheckerFunc(func(ctx context.Context, _ uuid.UUID) (bool, error) {
		received = ctx
		got, _ := ctx.Deadline()
		if !got.Equal(wantDeadline) {
			t.Error("child extended parent deadline")
		}
		return true, nil
	})
	router := gin.New()
	router.Use(httpapi.RequestIDMiddleware())
	router.GET("/", func(c *gin.Context) { SetPrincipal(c, Principal{UserID: uuid.New(), Role: "authenticated"}); c.Next() }, PlatformAdminMiddleware(checker, 2*time.Minute, slog.New(slog.NewTextHandler(&logs, nil))), func(c *gin.Context) { c.Status(204) })
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(parent))
	if rec.Code != 204 || !errors.Is(received.Err(), context.Canceled) {
		t.Fatal("successful lookup did not cancel child context")
	}
	if logs.Len() != 0 {
		t.Fatal("successful lookup logged an error")
	}
}

func TestOrdinaryUserDenialDoesNotLogDatabaseError(t *testing.T) {
	var logs strings.Builder
	router := gin.New()
	router.Use(httpapi.RequestIDMiddleware())
	router.GET("/", func(c *gin.Context) { SetPrincipal(c, Principal{UserID: uuid.New(), Role: "authenticated"}); c.Next() }, PlatformAdminMiddleware(adminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) { return false, nil }), time.Second, slog.New(slog.NewTextHandler(&logs, nil))))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != 403 || logs.Len() != 0 {
		t.Fatal("ordinary denial must not be logged as a database error")
	}
}

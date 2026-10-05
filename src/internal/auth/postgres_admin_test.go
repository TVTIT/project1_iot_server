package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"iot-platform/internal/httpapi"
)

type queryRowerFunc func(context.Context, string, ...any) pgx.Row

func (f queryRowerFunc) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	return f(ctx, query, args...)
}

type rowFunc func(...any) error

func (f rowFunc) Scan(dest ...any) error { return f(dest...) }

type adminCheckerFunc func(context.Context, uuid.UUID) (bool, error)

func (f adminCheckerFunc) IsPlatformAdmin(ctx context.Context, userID uuid.UUID) (bool, error) {
	return f(ctx, userID)
}

func TestPostgresPlatformAdminCheckerUsesParameterizedMembershipQuery(t *testing.T) {
	userID := uuid.New()
	querier := queryRowerFunc(func(_ context.Context, query string, args ...any) pgx.Row {
		if !strings.Contains(query, "FROM platform_admins") || !strings.Contains(query, "user_id = $1") {
			t.Fatalf("unexpected query: %s", query)
		}
		if len(args) != 1 || args[0] != userID {
			t.Fatalf("query args = %#v, want user UUID", args)
		}
		return rowFunc(func(dest ...any) error {
			*(dest[0].(*bool)) = true
			return nil
		})
	})

	admin, err := mustAdminChecker(t, querier).IsPlatformAdmin(context.Background(), userID)
	if err != nil || !admin {
		t.Fatalf("IsPlatformAdmin() = %v, %v; want true, nil", admin, err)
	}
}

func TestPostgresPlatformAdminCheckerPropagatesDatabaseError(t *testing.T) {
	wantErr := errors.New("database unavailable")
	checker := mustAdminChecker(t, queryRowerFunc(func(context.Context, string, ...any) pgx.Row {
		return rowFunc(func(...any) error { return wantErr })
	}))
	if _, err := checker.IsPlatformAdmin(context.Background(), uuid.New()); !errors.Is(err, wantErr) {
		t.Fatalf("IsPlatformAdmin() error = %v, want database error", err)
	}
}

func TestPostgresPlatformAdminCheckerReturnsFalseForOrdinaryUser(t *testing.T) {
	checker := mustAdminChecker(t, queryRowerFunc(func(context.Context, string, ...any) pgx.Row {
		return rowFunc(func(dest ...any) error {
			*(dest[0].(*bool)) = false
			return nil
		})
	}))
	admin, err := checker.IsPlatformAdmin(context.Background(), uuid.New())
	if err != nil || admin {
		t.Fatalf("IsPlatformAdmin() = %v, %v; want false, nil", admin, err)
	}
}

func TestPlatformAdminMiddleware(t *testing.T) {
	principal := Principal{UserID: uuid.New(), Role: "authenticated"}
	tests := []struct {
		name        string
		principal   *Principal
		checker     adminCheckerFunc
		wantStatus  int
		handlerRuns bool
	}{
		{name: "missing principal", checker: func(context.Context, uuid.UUID) (bool, error) { return true, nil }, wantStatus: http.StatusUnauthorized},
		{name: "ordinary user", principal: &principal, checker: func(context.Context, uuid.UUID) (bool, error) { return false, nil }, wantStatus: http.StatusForbidden},
		{name: "database unavailable", principal: &principal, checker: func(context.Context, uuid.UUID) (bool, error) { return false, errors.New("database unavailable") }, wantStatus: http.StatusServiceUnavailable},
		{name: "platform admin", principal: &principal, checker: func(_ context.Context, got uuid.UUID) (bool, error) { return got == principal.UserID, nil }, wantStatus: http.StatusNoContent, handlerRuns: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerRan := false
			router := gin.New()
			router.Use(httpapi.RequestIDMiddleware())
			router.GET("/admin",
				func(c *gin.Context) {
					if tt.principal != nil {
						SetPrincipal(c, *tt.principal)
					}
					c.Next()
				},
				PlatformAdminMiddleware(tt.checker, time.Second, nil),
				func(c *gin.Context) { handlerRan = true; c.Status(http.StatusNoContent) },
			)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin", nil))
			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, tt.wantStatus)
			}
			if handlerRan != tt.handlerRuns {
				t.Fatalf("handler ran = %v, want %v", handlerRan, tt.handlerRuns)
			}
		})
	}
}

func mustAdminChecker(t *testing.T, db QueryRower) PlatformAdminChecker {
	t.Helper()
	checker, err := NewPostgresPlatformAdminChecker(db)
	if err != nil {
		t.Fatal(err)
	}
	return checker
}

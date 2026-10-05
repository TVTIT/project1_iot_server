package auth

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Only the isolated-container harness opts into this destructive fixture test.
func TestPostgresAdminIntegration(t *testing.T) {
	if os.Getenv("ADMIN_TEST_ISOLATED") != "1" {
		t.Skip("run sh scripts/test-stage2-admin.sh")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	setup, err := pgxpool.New(ctx, os.Getenv("ADMIN_TEST_SETUP_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close()
	app, err := pgxpool.New(ctx, os.Getenv("ADMIN_TEST_BACKEND_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	var role string
	if err := app.QueryRow(ctx, "SELECT current_user").Scan(&role); err != nil || role != "iot_backend_app" {
		t.Fatal("integration must run with backend role")
	}
	checker := mustAdminChecker(t, app)
	adminID, normalID := uuid.New(), uuid.New()
	if _, err := setup.Exec(ctx, "INSERT INTO profiles (id) VALUES ($1), ($2)", adminID, normalID); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := setup.Exec(cleanupCtx, "DELETE FROM profiles WHERE id IN ($1,$2)", adminID, normalID); err != nil {
			t.Error(err)
		}
	}()
	if _, err := setup.Exec(ctx, "INSERT INTO platform_admins (user_id) VALUES ($1)", adminID); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		id   uuid.UUID
		want bool
	}{{adminID, true}, {normalID, false}, {uuid.New(), false}} {
		got, err := checker.IsPlatformAdmin(ctx, tt.id)
		if err != nil || got != tt.want {
			t.Fatalf("membership=%v want=%v error=%v", got, tt.want, err)
		}
	}
	for _, query := range []string{"INSERT INTO platform_admins (user_id) VALUES ($1)", "DELETE FROM platform_admins WHERE user_id=$1"} {
		_, err := app.Exec(ctx, query, normalID)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Fatalf("expected insufficient_privilege, got %v", err)
		}
	}
	if _, err := setup.Exec(ctx, "DELETE FROM platform_admins WHERE user_id=$1", adminID); err != nil {
		t.Fatal(err)
	}
	if got, err := checker.IsPlatformAdmin(ctx, adminID); err != nil || got {
		t.Fatal("revocation not reflected immediately")
	}

	cancelled, cancelNow := context.WithCancel(ctx)
	cancelNow()
	if _, err := checker.IsPlatformAdmin(cancelled, normalID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error=%v", err)
	}

	tx, err := setup.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	if _, err := tx.Exec(ctx, "LOCK TABLE platform_admins IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	deadline, cancelDeadline := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err = checker.IsPlatformAdmin(deadline, normalID)
	cancelDeadline()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("locked lookup must honor deadline: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	app.Close()
	if _, err := checker.IsPlatformAdmin(ctx, normalID); err == nil {
		t.Fatal("closed pool accepted lookup")
	}
}

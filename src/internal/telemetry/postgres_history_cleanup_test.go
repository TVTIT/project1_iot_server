package telemetry_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type testCleanupTB interface {
	Helper()
	Errorf(format string, args ...any)
	Logf(format string, args ...any)
}

type testCleanupExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type testCleanupQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// execCleanupStep executes a single cleanup DELETE or maintenance query.
// If an error occurs, it records a failure via t.Errorf,
// but does not abort, allowing best-effort cleanup of subsequent resources to continue.
func execCleanupStep(ctx context.Context, t testCleanupTB, exec testCleanupExecutor, resourceLabel string, sql string, args ...any) {
	t.Helper()
	if _, err := exec.Exec(ctx, sql, args...); err != nil {
		t.Errorf("cleanup %s failed: %v", resourceLabel, err)
	}
}

// verifyCleanupCount runs a verification SELECT to assert 0 rows remain for fixture-owned resources.
// If the verification query fails (e.g. database error, timeout), t.Errorf is reported and success
// CANNOT be claimed (no false "cleanup verified" log).
// If remaining rows > 0, t.Errorf is reported.
// Only when err == nil and remainingCount == 0 is "cleanup verified" logged.
func verifyCleanupCount(ctx context.Context, t testCleanupTB, querier testCleanupQuerier, resourceLabel string, sql string, args ...any) {
	t.Helper()
	var remainingCount int
	if err := querier.QueryRow(ctx, sql, args...).Scan(&remainingCount); err != nil {
		t.Errorf("cleanup verification query for %s failed: %v", resourceLabel, err)
		return
	}
	if remainingCount > 0 {
		t.Errorf("fixture cleanup incomplete: %d rows remain in %s", remainingCount, resourceLabel)
		return
	}
	t.Logf("Fixture cleanup verified: 0 rows remain for %s", resourceLabel)
}

type fakeCleanupTB struct {
	errors []string
	logs   []string
}

func (f *fakeCleanupTB) Helper() {}

func (f *fakeCleanupTB) Errorf(format string, args ...any) {
	f.errors = append(f.errors, fmt.Sprintf(format, args...))
}

func (f *fakeCleanupTB) Logf(format string, args ...any) {
	f.logs = append(f.logs, fmt.Sprintf(format, args...))
}

type fakeCleanupExecutor struct {
	errToReturn error
	calls       []string
}

func (f *fakeCleanupExecutor) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	f.calls = append(f.calls, sql)
	return pgconn.CommandTag{}, f.errToReturn
}

type fakeCleanupRow struct {
	scanErr error
	count   int
}

func (r *fakeCleanupRow) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if len(dest) > 0 {
		if ptr, ok := dest[0].(*int); ok {
			*ptr = r.count
		}
	}
	return nil
}

type fakeCleanupQuerier struct {
	row *fakeCleanupRow
}

func (q *fakeCleanupQuerier) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return q.row
}

func TestCleanupHelper_ExecutorErrorContinuesBestEffortAndMarksFailed(t *testing.T) {
	t.Parallel()

	fakeT := &fakeCleanupTB{}
	ctx := context.Background()

	execFail := &fakeCleanupExecutor{errToReturn: errors.New("simulated db exec error")}
	execSuccess := &fakeCleanupExecutor{errToReturn: nil}

	// Step 1 fails
	execCleanupStep(ctx, fakeT, execFail, "telemetry", "DELETE FROM public.telemetry WHERE gateway_id = $1", "gw1")

	// Step 2 continues (best effort)
	execCleanupStep(ctx, fakeT, execSuccess, "gateways", "DELETE FROM public.gateways WHERE gateway_id = $1", "gw1")

	if len(fakeT.errors) != 1 {
		t.Fatalf("expected 1 recorded test error, got %d", len(fakeT.errors))
	}
	if !strings.Contains(fakeT.errors[0], "cleanup telemetry failed: simulated db exec error") {
		t.Errorf("unexpected error message: %s", fakeT.errors[0])
	}

	if len(execFail.calls) != 1 || len(execSuccess.calls) != 1 {
		t.Errorf("expected both steps to execute despite step 1 failure")
	}
}

func TestCleanupHelper_VerificationReadFailureCannotClaimSuccess(t *testing.T) {
	t.Parallel()

	fakeT := &fakeCleanupTB{}
	ctx := context.Background()

	// Querier returns scan error
	querier := &fakeCleanupQuerier{
		row: &fakeCleanupRow{scanErr: errors.New("connection closed during verification")},
	}

	verifyCleanupCount(ctx, fakeT, querier, "telemetry", "SELECT COUNT(*) FROM public.telemetry WHERE gateway_id = $1", "gw1")

	if len(fakeT.errors) != 1 {
		t.Fatalf("expected 1 recorded error, got %d", len(fakeT.errors))
	}
	if !strings.Contains(fakeT.errors[0], "cleanup verification query for telemetry failed: connection closed during verification") {
		t.Errorf("unexpected error message: %s", fakeT.errors[0])
	}

	// Crucial: verify that "Fixture cleanup verified" was NOT logged!
	for _, logMsg := range fakeT.logs {
		if strings.Contains(logMsg, "Fixture cleanup verified") {
			t.Fatalf("forbidden 'cleanup verified' logged upon verification read failure: %s", logMsg)
		}
	}
}

func TestCleanupHelper_VerificationIncompleteCannotClaimSuccess(t *testing.T) {
	t.Parallel()

	fakeT := &fakeCleanupTB{}
	ctx := context.Background()

	// Querier returns remainingCount = 3
	querier := &fakeCleanupQuerier{
		row: &fakeCleanupRow{count: 3},
	}

	verifyCleanupCount(ctx, fakeT, querier, "telemetry", "SELECT COUNT(*) FROM public.telemetry WHERE gateway_id = $1", "gw1")

	if len(fakeT.errors) != 1 {
		t.Fatalf("expected 1 recorded error, got %d", len(fakeT.errors))
	}
	if !strings.Contains(fakeT.errors[0], "fixture cleanup incomplete: 3 rows remain in telemetry") {
		t.Errorf("unexpected error message: %s", fakeT.errors[0])
	}

	for _, logMsg := range fakeT.logs {
		if strings.Contains(logMsg, "Fixture cleanup verified") {
			t.Fatalf("forbidden 'cleanup verified' logged upon incomplete cleanup: %s", logMsg)
		}
	}
}

func TestCleanupHelper_VerificationSuccess(t *testing.T) {
	t.Parallel()

	fakeT := &fakeCleanupTB{}
	ctx := context.Background()

	// Querier returns remainingCount = 0
	querier := &fakeCleanupQuerier{
		row: &fakeCleanupRow{count: 0},
	}

	verifyCleanupCount(ctx, fakeT, querier, "telemetry", "SELECT COUNT(*) FROM public.telemetry WHERE gateway_id = $1", "gw1")

	if len(fakeT.errors) != 0 {
		t.Fatalf("expected 0 errors, got %d: %v", len(fakeT.errors), fakeT.errors)
	}
	if len(fakeT.logs) != 1 || !strings.Contains(fakeT.logs[0], "Fixture cleanup verified: 0 rows remain for telemetry") {
		t.Errorf("expected cleanup verified log, got: %v", fakeT.logs)
	}
}

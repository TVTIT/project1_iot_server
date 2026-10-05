package mqttcredential

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func requireCode(t *testing.T, err error, code ErrorCode) {
	t.Helper()
	var de *DomainError
	if !errors.As(err, &de) || de.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func TestPostgresConstructionAndValidation(t *testing.T) {
	var pool *pgxpool.Pool
	if _, err := NewPostgresRepository(pool, time.Second); err == nil {
		t.Fatal("nil pool accepted")
	}
	r := &PostgresRepository{}
	_, err := r.BeginOperation(context.Background(), BeginRequest{})
	requireCode(t, err, CodeInvalidRequest)
	_, err = r.GetMetadata(context.Background(), "backend_service")
	requireCode(t, err, CodeInvalidRequest)
	_, err = r.ResolveCommitAmbiguity(context.Background(), uuid.Nil)
	requireCode(t, err, CodeInvalidRequest)
}

func TestCommitUnknownIsSafe(t *testing.T) {
	err := &CommitOutcomeUnknown{OperationID: uuid.New()}
	if err.Error() != string(CodeFinalizationPending) {
		t.Fatal("unsafe commit error")
	}
	requireCode(t, err, CodeFinalizationPending)
}

func TestPostgresSeparateFinalizationBudget(t *testing.T) {
	pool := &pgxpool.Pool{}
	r, err := NewPostgresRepositoryWithFinalizationTimeout(pool, time.Second, 5*time.Second, 10)
	if err != nil || r.timeout != time.Second || r.finalizeTimeout != 5*time.Second || r.scanLimit != 10 {
		t.Fatal("distinct repository budgets not preserved")
	}
	legacy, err := NewPostgresRepository(pool, time.Second)
	if err != nil || legacy.finalizeTimeout != legacy.timeout {
		t.Fatal("legacy constructor compatibility lost")
	}
	for _, budget := range []time.Duration{0, -time.Second} {
		_, err = NewPostgresRepositoryWithFinalizationTimeout(pool, time.Second, budget)
		requireCode(t, err, CodeInvalidRequest)
	}
}

func TestPostgresReadErrorsAreSafe(t *testing.T) {
	_, err := scanMetadata(errorRow{errors.New("SQL password=never_expose")})
	requireCode(t, err, CodeServiceUnavailable)
	_, err = scanOperation(errorRow{errors.New("SQL password=never_expose")})
	requireCode(t, err, CodeServiceUnavailable)
}

type errorRow struct{ err error }

func (r errorRow) Scan(...any) error { return r.err }

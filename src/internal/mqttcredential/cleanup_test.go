package mqttcredential

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Rollback after commit (including a rejected commit) may return ErrTxClosed.
// Other cleanup failures must fail the fixture without masking its primary error.
func rollbackTestTransaction(t interface {
	Helper()
	Error(...any)
}, tx interface{ Rollback(context.Context) error }) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Error("fixture transaction rollback failed")
	}
}

type rollbackFixture struct {
	err     error
	bounded bool
}

func (f *rollbackFixture) Rollback(ctx context.Context) error {
	_, f.bounded = ctx.Deadline()
	return f.err
}

type cleanupReporter struct{ failed bool }

func (*cleanupReporter) Helper()        {}
func (r *cleanupReporter) Error(...any) { r.failed = true }

func TestRollbackTestTransaction(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		failed bool
	}{
		{"rolled back", nil, false},
		{"already closed", pgx.ErrTxClosed, false},
		{"cleanup failed", errors.New("rollback unavailable"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &rollbackFixture{err: tc.err}
			reporter := &cleanupReporter{}
			rollbackTestTransaction(reporter, tx)
			if reporter.failed != tc.failed || !tx.bounded {
				t.Fatal("rollback must use a bounded context and report unexpected errors")
			}
		})
	}
}

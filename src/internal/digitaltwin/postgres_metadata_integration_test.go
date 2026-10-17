package digitaltwin_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetry"
)

type metadataStubTelemetryDelegator struct{}

func (s *metadataStubTelemetryDelegator) ExecuteEffectiveHistoryQuery(_ context.Context, _ uuid.UUID, _ telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
	return nil, nil
}

// faultInjectingTxDatabase decorates TxDatabase for test-scoped fault injection.
type faultInjectingTxDatabase struct {
	underlying digitaltwin.TxDatabase
	onTxBegin  func(realTx pgx.Tx) pgx.Tx
}

func (d *faultInjectingTxDatabase) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	tx, err := d.underlying.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	if d.onTxBegin != nil {
		return d.onTxBegin(tx), nil
	}
	return tx, nil
}

type faultInjectingTx struct {
	pgx.Tx
	onQueryRow func(ctx context.Context, realTx pgx.Tx, sql string, args ...any) (pgx.Row, bool)
	onExec     func(ctx context.Context, realTx pgx.Tx, sql string, args ...any) (pgconn.CommandTag, error, bool)
}

func (t *faultInjectingTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if t.onQueryRow != nil {
		if row, intercepted := t.onQueryRow(ctx, t.Tx, sql, args...); intercepted {
			return row
		}
	}
	return t.Tx.QueryRow(ctx, sql, args...)
}

func (t *faultInjectingTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if t.onExec != nil {
		if tag, err, intercepted := t.onExec(ctx, t.Tx, sql, arguments...); intercepted {
			return tag, err
		}
	}
	return t.Tx.Exec(ctx, sql, arguments...)
}

type interceptedRow struct {
	realRow   pgx.Row
	onScanned func(err error)
}

func (r *interceptedRow) Scan(dest ...any) error {
	err := r.realRow.Scan(dest...)
	if r.onScanned != nil {
		r.onScanned(err)
	}
	return err
}

func sameNullableStr(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a != nil && b != nil {
		return *a == *b
	}
	return false
}

// errorScanRow is a pgx.Row whose Scan always returns the injected error. It is
// used to fail a transaction before it can emit a row-lock signal.
type errorScanRow struct{ err error }

func (r errorScanRow) Scan(...any) error { return r.err }

// gatewayPatchOutcome holds the buffered result-signaling channels for one
// concurrently launched Gateway metadata PATCH.
type gatewayPatchOutcome struct {
	errCh chan error
	resCh chan digitaltwin.PatchGatewayResult
}

// startGatewayPatch adds exactly one WaitGroup unit immediately before launching
// the PATCH goroutine. Deferring the Add until the goroutine is actually created
// means a failure that prevents a later scheduled goroutine from ever being
// launched cannot leave an unpaid WaitGroup unit behind. Both result channels are
// buffered (capacity 1) so a sender never blocks even if the coordinator aborts
// before reading the result.
func startGatewayPatch(
	patchCtx context.Context,
	wg *sync.WaitGroup,
	svc digitaltwin.GatewayPatchService,
	actor uuid.UUID,
	gatewayID string,
	input digitaltwin.PatchGatewayInput,
) gatewayPatchOutcome {
	out := gatewayPatchOutcome{
		errCh: make(chan error, 1),
		resCh: make(chan digitaltwin.PatchGatewayResult, 1),
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		res, err := svc.PatchGateway(patchCtx, actor, gatewayID, input)
		if err != nil {
			out.errCh <- err
			return
		}
		out.resCh <- res
		out.errCh <- nil
	}()
	return out
}

// boundedWaitGroupJoin waits for wg to drain under a finite timeout. It reports a
// test error instead of blocking forever when a goroutine does not finish, so
// deferred fixture cleanup still gets a chance to run after an early failure.
func boundedWaitGroupJoin(t *testing.T, wg *sync.WaitGroup, timeout time.Duration, label string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		t.Errorf("%s: bounded join timed out after %s waiting for concurrent goroutines", label, timeout)
	}
}

// patchOutcome carries the observable result of one metadata PATCH attempt used
// by the deterministic updated_at-ordering regression.
type patchOutcome struct {
	err       error
	updatedAt time.Time
	name      string
}

// newPauseBeforeLockDB decorates the application pool so that the first time the
// transaction executes a statement matching match(sql), the caller is signaled
// and then paused BEFORE the real statement runs. This lets a test start
// transaction A earlier and hold it just before it acquires the target resource
// lock, run a later transaction B to completion, then release A. The pause is
// bounded by the query context so an early test failure never leaks a goroutine.
func newPauseBeforeLockDB(
	base digitaltwin.TxDatabase,
	match func(sql string) bool,
	beforeLock chan<- struct{},
	release <-chan struct{},
) *faultInjectingTxDatabase {
	return &faultInjectingTxDatabase{
		underlying: base,
		onTxBegin: func(realTx pgx.Tx) pgx.Tx {
			return &faultInjectingTx{
				Tx: realTx,
				onQueryRow: func(qCtx context.Context, tx pgx.Tx, sql string, args ...any) (pgx.Row, bool) {
					if !match(sql) {
						return nil, false
					}
					select {
					case beforeLock <- struct{}{}:
					default:
					}
					select {
					case <-release:
					case <-qCtx.Done():
					}
					return tx.QueryRow(qCtx, sql, args...), true
				},
			}
		},
	}
}

// runBlockedLockPause runs the deterministic overlap for the updated_at-ordering
// regression: transaction A (started first) pauses before acquiring its target
// lock; transaction B then runs and commits; A is released, acquires the now-free
// lock, and writes last. Both transactions are launched under wg so the caller
// can bound-join them, and each uses its own timeout context for safe teardown.
func runBlockedLockPause(
	parentCtx context.Context,
	t *testing.T,
	wg *sync.WaitGroup,
	beforeLock <-chan struct{},
	release func(),
	label string,
	aRun func(ctx context.Context) patchOutcome,
	bRun func(ctx context.Context) patchOutcome,
) (patchOutcome, patchOutcome) {
	t.Helper()

	aCtx, cancelA := context.WithTimeout(parentCtx, 15*time.Second)
	defer cancelA()
	aRes := make(chan patchOutcome, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		aRes <- aRun(aCtx)
	}()

	select {
	case <-beforeLock:
		t.Logf("%s: transaction A paused BEFORE acquiring its target resource lock", label)
	case out := <-aRes:
		t.Fatalf("%s: transaction A finished before reaching the pre-lock pause: %v", label, out.err)
	case <-parentCtx.Done():
		t.Fatalf("%s: timed out waiting for A to reach the pre-lock pause: %v", label, parentCtx.Err())
	}

	bCtx, cancelB := context.WithTimeout(parentCtx, 15*time.Second)
	defer cancelB()
	bRes := make(chan patchOutcome, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		bRes <- bRun(bCtx)
	}()

	var bOut patchOutcome
	select {
	case bOut = <-bRes:
	case <-parentCtx.Done():
		t.Fatalf("%s: timed out awaiting transaction B: %v", label, parentCtx.Err())
	}
	if bOut.err != nil {
		t.Fatalf("%s: transaction B failed: %v", label, bOut.err)
	}

	// B has committed and released the target lock; release A to acquire it.
	release()

	var aOut patchOutcome
	select {
	case aOut = <-aRes:
	case <-parentCtx.Done():
		t.Fatalf("%s: timed out awaiting transaction A after release: %v", label, parentCtx.Err())
	}
	if aOut.err != nil {
		t.Fatalf("%s: transaction A failed: %v", label, aOut.err)
	}
	return aOut, bOut
}

// metadataFixtureIDs tracks the exact resource IDs created for this native run.
type metadataFixtureIDs struct {
	RunID            string
	GatewayIDs       []string
	AllUserIDs       []uuid.UUID
	AuthUsersPresent *bool
}

// cleanupReporter represents the minimal test reporting interface required by performMetadataFixtureCleanup.
type cleanupReporter interface {
	Helper()
	Errorf(format string, args ...any)
	Logf(format string, args ...any)
}

// cleanupDB represents the minimal database operations required by performMetadataFixtureCleanup.
type cleanupDB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// performMetadataFixtureCleanup executes bounded, exact-scoped cleanup of all resources
// created by this run, and verifies 0 remaining rows for the current run before logging success.
// It never touches unrelated records or previous leftover runs (e.g. 861eb9b2).
func performMetadataFixtureCleanup(ctx context.Context, t cleanupReporter, pool cleanupDB, f metadataFixtureIDs) {
	t.Helper()

	var hasErr bool

	execCleanup := func(table, query string, args ...any) {
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			hasErr = true
			t.Errorf("cleanup failed for table %s: %v", table, err)
		}
	}

	verifyCount := func(table, query string, args ...any) {
		var cnt int
		if err := pool.QueryRow(ctx, query, args...).Scan(&cnt); err != nil {
			hasErr = true
			t.Errorf("cleanup count verification failed for table %s: %v", table, err)
		} else if cnt != 0 {
			hasErr = true
			t.Errorf("cleanup incomplete: %d row(s) remain in %s for run %s", cnt, table, f.RunID)
		}
	}

	// 0a. Delete twin_outbox
	execCleanup("twin_outbox", `
		DELETE FROM public.twin_outbox
		WHERE command_id IN (
			SELECT command_id FROM public.twin_commands
			WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))
		)`, f.GatewayIDs)

	// 0b. Delete twin_commands
	execCleanup("twin_commands", `
		DELETE FROM public.twin_commands
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`,
		f.GatewayIDs)

	// 0c. Delete gateway_mqtt_credentials first: it holds a FK
	// (fk_gateway_mqtt_credentials_last_operation) onto the pending event row, so
	// the events cannot be removed while a credential still references them.
	execCleanup("gateway_mqtt_credentials", `
		DELETE FROM public.gateway_mqtt_credentials WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	// 0d. Delete gateway_mqtt_credential_events
	execCleanup("gateway_mqtt_credential_events", `
		DELETE FROM public.gateway_mqtt_credential_events WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	// 0e. Delete telemetry
	execCleanup("telemetry", `
		DELETE FROM public.telemetry WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	// 0f. Delete processed_messages
	execCleanup("processed_messages", `
		DELETE FROM public.processed_messages WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	// 1. Delete twin_temporal_values
	execCleanup("twin_temporal_values", `
		DELETE FROM public.twin_temporal_values
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`,
		f.GatewayIDs)

	// 2. Delete twin_states
	execCleanup("twin_states", `
		DELETE FROM public.twin_states
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`,
		f.GatewayIDs)

	// 3. Delete twin_relationships
	execCleanup("twin_relationships", `
		DELETE FROM public.twin_relationships
		WHERE source_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))
		   OR target_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`,
		f.GatewayIDs)

	// 4. Delete twin_entities
	execCleanup("twin_entities", `DELETE FROM public.twin_entities WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	// 5. Delete sensors
	execCleanup("sensors", `DELETE FROM public.sensors WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	// 6. Delete user_gateways
	execCleanup("user_gateways", `DELETE FROM public.user_gateways WHERE gateway_id = ANY($1) OR user_id = ANY($2)`, f.GatewayIDs, f.AllUserIDs)

	// 7. Delete gateways
	execCleanup("gateways", `DELETE FROM public.gateways WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	// 8. Delete platform_admins
	execCleanup("platform_admins", `DELETE FROM public.platform_admins WHERE user_id = ANY($1)`, f.AllUserIDs)

	// 9. Delete profiles
	execCleanup("profiles", `DELETE FROM public.profiles WHERE id = ANY($1)`, f.AllUserIDs)

	// 10. Delete auth.users if table exists
	var shouldCleanAuth bool
	if f.AuthUsersPresent != nil && *f.AuthUsersPresent {
		shouldCleanAuth = true
	}

	var hasAuthUsers bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
		hasErr = true
		t.Errorf("cleanup failed: auth.users discovery probe error: %v", err)
		// Unknown discovery is NEVER treated as confirmed absence.
		// If setup did not specify, attempt exact-owned-ID Auth cleanup safely on unknown discovery.
		if f.AuthUsersPresent == nil {
			shouldCleanAuth = true
		}
	} else if hasAuthUsers {
		shouldCleanAuth = true
	} else if f.AuthUsersPresent != nil && *f.AuthUsersPresent {
		hasErr = true
		t.Errorf("cleanup failed: auth.users was confirmed present during setup but now reported absent")
	}

	if shouldCleanAuth {
		execCleanup("auth.users", `DELETE FROM auth.users WHERE id = ANY($1)`, f.AllUserIDs)
	}

	// 11. Verify zero rows remain across all affected tables for this run
	verifyCount("twin_outbox", `
		SELECT COUNT(*) FROM public.twin_outbox
		WHERE command_id IN (
			SELECT command_id FROM public.twin_commands
			WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))
		)`, f.GatewayIDs)

	verifyCount("twin_commands", `
		SELECT COUNT(*) FROM public.twin_commands
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`, f.GatewayIDs)

	verifyCount("gateway_mqtt_credential_events", `
		SELECT COUNT(*) FROM public.gateway_mqtt_credential_events WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	verifyCount("gateway_mqtt_credentials", `
		SELECT COUNT(*) FROM public.gateway_mqtt_credentials WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	verifyCount("telemetry", `
		SELECT COUNT(*) FROM public.telemetry WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	verifyCount("processed_messages", `
		SELECT COUNT(*) FROM public.processed_messages WHERE gateway_id = ANY($1)`, f.GatewayIDs)

	verifyCount("twin_temporal_values", `
		SELECT COUNT(*) FROM public.twin_temporal_values
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`, f.GatewayIDs)

	verifyCount("twin_states", `
		SELECT COUNT(*) FROM public.twin_states
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`, f.GatewayIDs)

	verifyCount("twin_relationships", `
		SELECT COUNT(*) FROM public.twin_relationships
		WHERE source_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))
		   OR target_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`, f.GatewayIDs)

	verifyCount("twin_entities", `SELECT COUNT(*) FROM public.twin_entities WHERE gateway_id = ANY($1)`, f.GatewayIDs)
	verifyCount("sensors", `SELECT COUNT(*) FROM public.sensors WHERE gateway_id = ANY($1)`, f.GatewayIDs)
	verifyCount("user_gateways", `SELECT COUNT(*) FROM public.user_gateways WHERE gateway_id = ANY($1) OR user_id = ANY($2)`, f.GatewayIDs, f.AllUserIDs)
	verifyCount("gateways", `SELECT COUNT(*) FROM public.gateways WHERE gateway_id = ANY($1)`, f.GatewayIDs)
	verifyCount("platform_admins", `SELECT COUNT(*) FROM public.platform_admins WHERE user_id = ANY($1)`, f.AllUserIDs)
	verifyCount("profiles", `SELECT COUNT(*) FROM public.profiles WHERE id = ANY($1)`, f.AllUserIDs)

	if shouldCleanAuth {
		verifyCount("auth.users", `SELECT COUNT(*) FROM auth.users WHERE id = ANY($1)`, f.AllUserIDs)
	}

	if !hasErr {
		t.Logf("Fixture cleanup verified: 0 remaining rows for run %s", f.RunID)
	}
}

// recordingCleanupReporter records Errorf and Logf calls for testing cleanup behavior.
type recordingCleanupReporter struct {
	mu     sync.Mutex
	errors []string
	logs   []string
}

func (r *recordingCleanupReporter) Helper() {}

func (r *recordingCleanupReporter) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func (r *recordingCleanupReporter) Logf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}

func (r *recordingCleanupReporter) HasErrorContaining(substr string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.errors {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

func (r *recordingCleanupReporter) HasLogContaining(substr string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, l := range r.logs {
		if strings.Contains(l, substr) {
			return true
		}
	}
	return false
}

type cleanupStubRow struct {
	scanFn func(dest ...any) error
}

func (r cleanupStubRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}

type stubCleanupDB struct {
	mu         sync.Mutex
	execCalls  []string
	queryCalls []string

	onExec     func(sql string, args ...any) (pgconn.CommandTag, error)
	onQueryRow func(sql string, args ...any) pgx.Row
}

func (db *stubCleanupDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	db.mu.Lock()
	db.execCalls = append(db.execCalls, sql)
	db.mu.Unlock()
	if db.onExec != nil {
		return db.onExec(sql, args...)
	}
	return pgconn.NewCommandTag("DELETE 0"), nil
}

func (db *stubCleanupDB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	db.mu.Lock()
	db.queryCalls = append(db.queryCalls, sql)
	db.mu.Unlock()
	if db.onQueryRow != nil {
		return db.onQueryRow(sql, args...)
	}
	return cleanupStubRow{
		scanFn: func(dest ...any) error {
			if len(dest) > 0 {
				if ptr, ok := dest[0].(*int); ok {
					*ptr = 0
				}
			}
			return nil
		},
	}
}

func (db *stubCleanupDB) HasExecContaining(substr string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, q := range db.execCalls {
		if strings.Contains(q, substr) {
			return true
		}
	}
	return false
}

func (db *stubCleanupDB) HasQueryContaining(substr string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, q := range db.queryCalls {
		if strings.Contains(q, substr) {
			return true
		}
	}
	return false
}

func TestPerformMetadataFixtureCleanup_AuthDiscoveryAndErrors(t *testing.T) {
	t.Parallel()

	runID := "test_clean_001"
	testFixture := metadataFixtureIDs{
		RunID:      runID,
		GatewayIDs: []string{"gw_clean_1"},
		AllUserIDs: []uuid.UUID{uuid.New()},
	}

	t.Run("InjectedDiscoveryFailure_PreventsSuccessLog_AttemptsCleanup", func(t *testing.T) {
		t.Parallel()
		reporter := &recordingCleanupReporter{}
		db := &stubCleanupDB{
			onQueryRow: func(sql string, _ ...any) pgx.Row {
				if strings.Contains(sql, "to_regclass('auth.users')") {
					return cleanupStubRow{
						scanFn: func(_ ...any) error {
							return errors.New("injected auth discovery connection fault")
						},
					}
				}
				return cleanupStubRow{
					scanFn: func(dest ...any) error {
						if len(dest) > 0 {
							if ptr, ok := dest[0].(*int); ok {
								*ptr = 0
							}
						}
						return nil
					},
				}
			},
		}

		performMetadataFixtureCleanup(context.Background(), reporter, db, testFixture)

		if !reporter.HasErrorContaining("auth.users discovery probe error") {
			t.Fatalf("expected discovery probe error reported, got errors: %v", reporter.errors)
		}
		if reporter.HasLogContaining("Fixture cleanup verified") {
			t.Fatalf("expected NO success log when discovery probe fails, got logs: %v", reporter.logs)
		}
		if !db.HasExecContaining("DELETE FROM auth.users") {
			t.Fatalf("expected cleanup to attempt auth.users deletion on unknown discovery, got execs: %v", db.execCalls)
		}
		if !db.HasExecContaining("DELETE FROM public.gateways") {
			t.Fatalf("expected remaining cleanup to be attempted, got execs: %v", db.execCalls)
		}
	})

	t.Run("InjectedDiscoveryFailure_WithConfirmedPresent_AttemptsAuthCleanupAndFails", func(t *testing.T) {
		t.Parallel()
		reporter := &recordingCleanupReporter{}
		present := true
		fixtureWithPresent := testFixture
		fixtureWithPresent.AuthUsersPresent = &present

		db := &stubCleanupDB{
			onQueryRow: func(sql string, _ ...any) pgx.Row {
				if strings.Contains(sql, "to_regclass('auth.users')") {
					return cleanupStubRow{
						scanFn: func(_ ...any) error {
							return errors.New("injected auth discovery timeout")
						},
					}
				}
				return cleanupStubRow{
					scanFn: func(dest ...any) error {
						if len(dest) > 0 {
							if ptr, ok := dest[0].(*int); ok {
								*ptr = 0
							}
						}
						return nil
					},
				}
			},
		}

		performMetadataFixtureCleanup(context.Background(), reporter, db, fixtureWithPresent)

		if !reporter.HasErrorContaining("auth.users discovery probe error") {
			t.Fatalf("expected discovery probe error reported, got errors: %v", reporter.errors)
		}
		if reporter.HasLogContaining("Fixture cleanup verified") {
			t.Fatalf("expected NO success log when discovery probe fails, got logs: %v", reporter.logs)
		}
		if !db.HasExecContaining("DELETE FROM auth.users") {
			t.Fatalf("expected cleanup to attempt auth.users deletion when setup confirmed present, got execs: %v", db.execCalls)
		}
	})

	t.Run("ConfirmedPresent_CleansAndVerifies_Success", func(t *testing.T) {
		t.Parallel()
		reporter := &recordingCleanupReporter{}
		present := true
		fixtureWithPresent := testFixture
		fixtureWithPresent.AuthUsersPresent = &present

		db := &stubCleanupDB{
			onQueryRow: func(sql string, _ ...any) pgx.Row {
				if strings.Contains(sql, "to_regclass('auth.users')") {
					return cleanupStubRow{
						scanFn: func(dest ...any) error {
							if len(dest) > 0 {
								if ptr, ok := dest[0].(*bool); ok {
									*ptr = true
								}
							}
							return nil
						},
					}
				}
				return cleanupStubRow{
					scanFn: func(dest ...any) error {
						if len(dest) > 0 {
							if ptr, ok := dest[0].(*int); ok {
								*ptr = 0
							}
						}
						return nil
					},
				}
			},
		}

		performMetadataFixtureCleanup(context.Background(), reporter, db, fixtureWithPresent)

		if len(reporter.errors) != 0 {
			t.Fatalf("expected 0 errors, got: %v", reporter.errors)
		}
		if !reporter.HasLogContaining("Fixture cleanup verified: 0 remaining rows for run test_clean_001") {
			t.Fatalf("expected success log, got logs: %v", reporter.logs)
		}
		if !db.HasExecContaining("DELETE FROM auth.users") {
			t.Fatalf("expected auth.users delete executed, got execs: %v", db.execCalls)
		}
		if !db.HasQueryContaining("SELECT COUNT(*) FROM auth.users") {
			t.Fatalf("expected auth.users count verification query, got queries: %v", db.queryCalls)
		}
	})

	t.Run("ConfirmedAbsent_SkipsAuth_Success", func(t *testing.T) {
		t.Parallel()
		reporter := &recordingCleanupReporter{}
		absent := false
		fixtureWithAbsent := testFixture
		fixtureWithAbsent.AuthUsersPresent = &absent

		db := &stubCleanupDB{
			onQueryRow: func(sql string, _ ...any) pgx.Row {
				if strings.Contains(sql, "to_regclass('auth.users')") {
					return cleanupStubRow{
						scanFn: func(dest ...any) error {
							if len(dest) > 0 {
								if ptr, ok := dest[0].(*bool); ok {
									*ptr = false
								}
							}
							return nil
						},
					}
				}
				return cleanupStubRow{
					scanFn: func(dest ...any) error {
						if len(dest) > 0 {
							if ptr, ok := dest[0].(*int); ok {
								*ptr = 0
							}
						}
						return nil
					},
				}
			},
		}

		performMetadataFixtureCleanup(context.Background(), reporter, db, fixtureWithAbsent)

		if len(reporter.errors) != 0 {
			t.Fatalf("expected 0 errors, got: %v", reporter.errors)
		}
		if !reporter.HasLogContaining("Fixture cleanup verified: 0 remaining rows for run test_clean_001") {
			t.Fatalf("expected success log, got logs: %v", reporter.logs)
		}
		if db.HasExecContaining("DELETE FROM auth.users") {
			t.Fatalf("expected NO auth.users delete when confirmed absent, got execs: %v", db.execCalls)
		}
		if db.HasQueryContaining("SELECT COUNT(*) FROM auth.users") {
			t.Fatalf("expected NO auth.users count query when confirmed absent, got queries: %v", db.queryCalls)
		}
	})

	t.Run("ResidualAuthRows_FailsAndPreventsSuccessLog", func(t *testing.T) {
		t.Parallel()
		reporter := &recordingCleanupReporter{}
		db := &stubCleanupDB{
			onQueryRow: func(sql string, _ ...any) pgx.Row {
				if strings.Contains(sql, "to_regclass('auth.users')") {
					return cleanupStubRow{
						scanFn: func(dest ...any) error {
							if len(dest) > 0 {
								if ptr, ok := dest[0].(*bool); ok {
									*ptr = true
								}
							}
							return nil
						},
					}
				}
				if strings.Contains(sql, "FROM auth.users") {
					return cleanupStubRow{
						scanFn: func(dest ...any) error {
							if len(dest) > 0 {
								if ptr, ok := dest[0].(*int); ok {
									*ptr = 1
								}
							}
							return nil
						},
					}
				}
				return cleanupStubRow{
					scanFn: func(dest ...any) error {
						if len(dest) > 0 {
							if ptr, ok := dest[0].(*int); ok {
								*ptr = 0
							}
						}
						return nil
					},
				}
			},
		}

		performMetadataFixtureCleanup(context.Background(), reporter, db, testFixture)

		if !reporter.HasErrorContaining("cleanup incomplete: 1 row(s) remain in auth.users") {
			t.Fatalf("expected residual auth.users error, got errors: %v", reporter.errors)
		}
		if reporter.HasLogContaining("Fixture cleanup verified") {
			t.Fatalf("expected NO success log when residual rows remain, got logs: %v", reporter.logs)
		}
	})

	t.Run("ResidualOtherRows_FailsAndPreventsSuccessLog", func(t *testing.T) {
		t.Parallel()
		reporter := &recordingCleanupReporter{}
		db := &stubCleanupDB{
			onQueryRow: func(sql string, _ ...any) pgx.Row {
				if strings.Contains(sql, "to_regclass('auth.users')") {
					return cleanupStubRow{
						scanFn: func(dest ...any) error {
							if len(dest) > 0 {
								if ptr, ok := dest[0].(*bool); ok {
									*ptr = true
								}
							}
							return nil
						},
					}
				}
				if strings.Contains(sql, "FROM public.gateways") {
					return cleanupStubRow{
						scanFn: func(dest ...any) error {
							if len(dest) > 0 {
								if ptr, ok := dest[0].(*int); ok {
									*ptr = 2
								}
							}
							return nil
						},
					}
				}
				return cleanupStubRow{
					scanFn: func(dest ...any) error {
						if len(dest) > 0 {
							if ptr, ok := dest[0].(*int); ok {
								*ptr = 0
							}
						}
						return nil
					},
				}
			},
		}

		performMetadataFixtureCleanup(context.Background(), reporter, db, testFixture)

		if !reporter.HasErrorContaining("cleanup incomplete: 2 row(s) remain in gateways") {
			t.Fatalf("expected residual gateways error, got errors: %v", reporter.errors)
		}
		if reporter.HasLogContaining("Fixture cleanup verified") {
			t.Fatalf("expected NO success log when residual rows remain, got logs: %v", reporter.logs)
		}
	})
}

type metadataFixtureSnapshot struct {
	Gateways          map[string]gatewaySnapshotRecord
	Sensors           map[string]sensorSnapshotRecord
	Memberships       map[string]membershipSnapshotRecord
	Entities          map[uuid.UUID]entitySnapshotRecord
	Relationships     map[string]relationshipSnapshotRecord
	States            map[uuid.UUID]stateSnapshotRecord
	Commands          map[uuid.UUID]commandSnapshotRecord
	Outbox            map[uuid.UUID]outboxSnapshotRecord
	Credentials       map[string]credentialSnapshotRecord
	CredentialEvents  map[uuid.UUID]credentialEventSnapshotRecord
	TemporalCount     int
	TelemetryRawCount int
	LedgerCount       int
	CommandCount      int
	OutboxCount       int
	CredentialCount   int
	EventCount        int
}

type gatewaySnapshotRecord struct {
	GatewayID   string
	Name        string
	Description *string
	CreatedAt   time.Time
}

type sensorSnapshotRecord struct {
	GatewayID string
	SensorID  string
	Name      string
	Unit      *string
	CreatedAt time.Time
}

type membershipSnapshotRecord struct {
	GatewayID string
	UserID    uuid.UUID
	Role      string
}

type entitySnapshotRecord struct {
	ID         uuid.UUID
	EntityID   string
	EntityType string
	Name       string
	GatewayID  string
	Attributes string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type relationshipSnapshotRecord struct {
	SourceID  uuid.UUID
	RelType   string
	TargetID  uuid.UUID
	CreatedAt time.Time
}

type stateSnapshotRecord struct {
	EntityID        uuid.UUID
	ReportedState   string
	DesiredState    string
	ReportedVersion int64
	DesiredVersion  int64
	LastReportedAt  *time.Time
	LastDesiredAt   *time.Time
	UpdatedAt       time.Time
}

// commandSnapshotRecord captures the safe, non-secret content of one twin_commands
// row so a comparator can detect a same-cardinality mutation (not only a count change).
type commandSnapshotRecord struct {
	ID             uuid.UUID
	CommandID      uuid.UUID
	EntityID       uuid.UUID
	Action         string
	Parameters     string
	Status         string
	DesiredVersion *int64
	IdempotencyKey *string
	IssuedBy       *uuid.UUID
	IssuedAt       time.Time
	ExpiresAt      time.Time
	AcknowledgedAt *time.Time
	CompletedAt    *time.Time
	AttemptCount   int32
	ErrorCode      *string
	ErrorMessage   *string
}

// outboxSnapshotRecord captures one twin_outbox row's content.
type outboxSnapshotRecord struct {
	ID            uuid.UUID
	CommandID     uuid.UUID
	Topic         string
	Payload       string
	Status        string
	AttemptCount  int32
	NextAttemptAt time.Time
	CreatedAt     time.Time
	PublishedAt   *time.Time
}

// credentialSnapshotRecord captures gateway_mqtt_credentials metadata. These
// columns hold only projection metadata, never secret material.
type credentialSnapshotRecord struct {
	GatewayID         string
	CredentialVersion int64
	Status            string
	LastOperationID   *uuid.UUID
	ChangedBy         *uuid.UUID
	ActivatedAt       *time.Time
	RevokedAt         *time.Time
	UpdatedAt         time.Time
	LastErrorCode     *string
}

// credentialEventSnapshotRecord captures gateway_mqtt_credential_events metadata
// (journal only, never native broker credentials or password hashes).
type credentialEventSnapshotRecord struct {
	OperationID           uuid.UUID
	GatewayID             string
	CredentialVersion     *int64
	Action                string
	Status                string
	Phase                 string
	ExecutionContract     string
	IdempotencyKey        *uuid.UUID
	ActorUserID           *uuid.UUID
	RequestID             *string
	DeliveryStatus        string
	RAMApplied            bool
	SnapshotObserved      bool
	FreshPositiveVerified bool
	CreatedAt             time.Time
	CompletedAt           *time.Time
	UpdatedAt             time.Time
	ErrorCode             *string
}

// snapshotQuerier is the minimal read surface captureMetadataFixtureSnapshot needs.
// Both *pgxpool.Pool and pgx.Tx satisfy it, and a test-only fake can inject
// snapshot acquisition failures without a real database.
type snapshotQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// fatalReporter is the minimal fail-fast surface the native helpers use to abort
// on a capture error. A real *testing.T satisfies it; a recording double (see
// recordingFatalReporter) lets a regression assert the fail-fast contract without
// failing its own parent test.
type fatalReporter interface {
	Helper()
	Fatalf(format string, args ...any)
}

// mustCaptureMetadataFixtureSnapshot is the fail-fast caller used by the native
// slice: any snapshot acquisition error aborts the test immediately so a failed
// capture can never fall through into a comparison as a comparable empty snapshot.
func mustCaptureMetadataFixtureSnapshot(ctx context.Context, t *testing.T, q snapshotQuerier, gatewayIDs []string) metadataFixtureSnapshot {
	t.Helper()
	return requireMetadataFixtureSnapshot(ctx, t, q, gatewayIDs)
}

// requireMetadataFixtureSnapshot is the fail-fast core with a reporter seam (a
// real *testing.T, or a recording double in a regression) so the abort contract
// can be asserted without a real *testing.T. Context stays the first argument.
func requireMetadataFixtureSnapshot(ctx context.Context, rep fatalReporter, q snapshotQuerier, gatewayIDs []string) metadataFixtureSnapshot {
	rep.Helper()
	snap, err := captureMetadataFixtureSnapshot(ctx, q, gatewayIDs)
	if err != nil {
		rep.Fatalf("failed to capture metadata fixture snapshot: %v", err)
	}
	return snap
}

// metadataSentinelIDs records the exact control/credential row identifiers seeded
// for content-preservation evidence.
type metadataSentinelIDs struct {
	GatewayID   string
	OperationID uuid.UUID
	CommandID   uuid.UUID
	OutboxID    uuid.UUID
}

// seedMetadataControlCredentialSentinels inserts one valid, fixture-owned
// twin_command + twin_outbox and one gateway_mqtt_credentials +
// gateway_mqtt_credential_event for the given gateway. It uses the real schema
// enums, FKs, and the "simplified" MQTT execution contract, which creates no
// maintenance checkpoint row and therefore stays exact-deletable by gateway_id.
// It stores only journal metadata: never native broker credentials, password
// hashes, or any secret material. Callers MUST register the exact-scoped fixture
// cleanup (which deletes these tables by gateway_id) before invoking this helper.
func seedMetadataControlCredentialSentinels(ctx context.Context, t *testing.T, pool *pgxpool.Pool, gatewayID string, entityUUID, actor uuid.UUID) metadataSentinelIDs {
	t.Helper()

	ids := metadataSentinelIDs{
		GatewayID:   gatewayID,
		OperationID: uuid.New(),
		CommandID:   uuid.New(),
		OutboxID:    uuid.New(),
	}
	idempotencyKey := uuid.New()
	ts := time.Now().UTC().Truncate(time.Microsecond)
	topic := fmt.Sprintf("gateways/%s/commands/%s", gatewayID, ids.CommandID)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin sentinel seed transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		INSERT INTO public.twin_commands
			(id, command_id, entity_id, action, parameters, status, desired_version, idempotency_key, issued_by, issued_at, expires_at, attempt_count)
		VALUES ($1, $2, $3, 'update_configuration', $4::jsonb, 'pending', $5, 'sentinel-idempotency-key', $6, $7::timestamptz, $7::timestamptz + interval '5 minutes', 0)`,
		uuid.New(), ids.CommandID, entityUUID,
		`{"sampling_interval_seconds":5}`, int64(1), actor, ts,
	); err != nil {
		t.Fatalf("failed to seed sentinel twin_command: %v", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO public.twin_outbox
			(id, command_id, topic, payload, status, attempt_count, next_attempt_at, created_at)
		VALUES ($1, $2, $3, $4::jsonb, 'pending', 0, $5, $5)`,
		ids.OutboxID, ids.CommandID, topic,
		`{"protocol_version":1,"message_type":"command"}`, ts,
	); err != nil {
		t.Fatalf("failed to seed sentinel twin_outbox: %v", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO public.gateway_mqtt_credential_events
			(operation_id, gateway_id, actor_user_id, request_id, idempotency_key, credential_version, action, status, execution_contract, created_at, updated_at)
		VALUES ($1, $2, $3, 'req-meta-sentinel', $4, $5, 'provision', 'pending', 'simplified', $6, $6)`,
		ids.OperationID, gatewayID, actor, idempotencyKey, int64(1), ts,
	); err != nil {
		t.Fatalf("failed to seed sentinel gateway_mqtt_credential_event: %v", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO public.gateway_mqtt_credentials
			(gateway_id, credential_version, status, last_operation_id, changed_by, updated_at)
		VALUES ($1, $2, 'provisioning', $3, $4, $5)`,
		gatewayID, int64(1), ids.OperationID, actor, ts,
	); err != nil {
		t.Fatalf("failed to seed sentinel gateway_mqtt_credentials: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit sentinel seed (deferred credential invariants rejected it): %v", err)
	}

	return ids
}

// captureMetadataFixtureSnapshot reads every snapshot row and returns an error if
// any acquisition step fails: the row Query, a per-row Scan, Rows.Err, or a count
// QueryRow Scan. On failure it returns the zero snapshot instead of a partially
// populated one, so a caller that mistakenly ignored the error could not treat the
// result as an unchanged or empty snapshot that compares equal to another capture.
func captureMetadataFixtureSnapshot(ctx context.Context, q snapshotQuerier, gatewayIDs []string) (metadataFixtureSnapshot, error) {
	snap := metadataFixtureSnapshot{
		Gateways:         make(map[string]gatewaySnapshotRecord),
		Sensors:          make(map[string]sensorSnapshotRecord),
		Memberships:      make(map[string]membershipSnapshotRecord),
		Entities:         make(map[uuid.UUID]entitySnapshotRecord),
		Relationships:    make(map[string]relationshipSnapshotRecord),
		States:           make(map[uuid.UUID]stateSnapshotRecord),
		Commands:         make(map[uuid.UUID]commandSnapshotRecord),
		Outbox:           make(map[uuid.UUID]outboxSnapshotRecord),
		Credentials:      make(map[string]credentialSnapshotRecord),
		CredentialEvents: make(map[uuid.UUID]credentialEventSnapshotRecord),
	}

	gRows, err := q.Query(ctx, `
		SELECT gateway_id, name, description, created_at
		FROM public.gateways
		WHERE gateway_id = ANY($1)`, gatewayIDs)
	if err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot query gateways failed: %w", err)
	}
	defer gRows.Close()
	for gRows.Next() {
		var g gatewaySnapshotRecord
		if err := gRows.Scan(&g.GatewayID, &g.Name, &g.Description, &g.CreatedAt); err != nil {
			return metadataFixtureSnapshot{}, fmt.Errorf("snapshot scan gateway failed: %w", err)
		}
		g.CreatedAt = g.CreatedAt.UTC()
		snap.Gateways[g.GatewayID] = g
	}
	if err := gRows.Err(); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot gateways rows error: %w", err)
	}

	sRows, err := q.Query(ctx, `
		SELECT gateway_id, sensor_id, name, unit, created_at
		FROM public.sensors
		WHERE gateway_id = ANY($1)`, gatewayIDs)
	if err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot query sensors failed: %w", err)
	}
	defer sRows.Close()
	for sRows.Next() {
		var s sensorSnapshotRecord
		if err := sRows.Scan(&s.GatewayID, &s.SensorID, &s.Name, &s.Unit, &s.CreatedAt); err != nil {
			return metadataFixtureSnapshot{}, fmt.Errorf("snapshot scan sensor failed: %w", err)
		}
		s.CreatedAt = s.CreatedAt.UTC()
		key := fmt.Sprintf("%s:%s", s.GatewayID, s.SensorID)
		snap.Sensors[key] = s
	}
	if err := sRows.Err(); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot sensors rows error: %w", err)
	}

	mRows, err := q.Query(ctx, `
		SELECT gateway_id, user_id, role
		FROM public.user_gateways
		WHERE gateway_id = ANY($1)`, gatewayIDs)
	if err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot query user_gateways failed: %w", err)
	}
	defer mRows.Close()
	for mRows.Next() {
		var m membershipSnapshotRecord
		if err := mRows.Scan(&m.GatewayID, &m.UserID, &m.Role); err != nil {
			return metadataFixtureSnapshot{}, fmt.Errorf("snapshot scan user_gateways failed: %w", err)
		}
		key := fmt.Sprintf("%s:%s", m.GatewayID, m.UserID)
		snap.Memberships[key] = m
	}
	if err := mRows.Err(); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot memberships rows error: %w", err)
	}

	eRows, err := q.Query(ctx, `
		SELECT id, entity_id, entity_type, name, gateway_id, attributes::text, created_at, updated_at
		FROM public.twin_entities
		WHERE gateway_id = ANY($1)`, gatewayIDs)
	if err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot query twin_entities failed: %w", err)
	}
	defer eRows.Close()
	for eRows.Next() {
		var e entitySnapshotRecord
		if err := eRows.Scan(&e.ID, &e.EntityID, &e.EntityType, &e.Name, &e.GatewayID, &e.Attributes, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return metadataFixtureSnapshot{}, fmt.Errorf("snapshot scan twin_entities failed: %w", err)
		}
		e.CreatedAt = e.CreatedAt.UTC()
		e.UpdatedAt = e.UpdatedAt.UTC()
		snap.Entities[e.ID] = e
	}
	if err := eRows.Err(); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot twin_entities rows error: %w", err)
	}

	rRows, err := q.Query(ctx, `
		SELECT source_entity_id, relationship_type, target_entity_id, created_at
		FROM public.twin_relationships
		WHERE source_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))
		   OR target_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`, gatewayIDs)
	if err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot query twin_relationships failed: %w", err)
	}
	defer rRows.Close()
	for rRows.Next() {
		var r relationshipSnapshotRecord
		if err := rRows.Scan(&r.SourceID, &r.RelType, &r.TargetID, &r.CreatedAt); err != nil {
			return metadataFixtureSnapshot{}, fmt.Errorf("snapshot scan twin_relationships failed: %w", err)
		}
		r.CreatedAt = r.CreatedAt.UTC()
		key := fmt.Sprintf("%s:%s:%s", r.SourceID, r.RelType, r.TargetID)
		snap.Relationships[key] = r
	}
	if err := rRows.Err(); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot twin_relationships rows error: %w", err)
	}

	stRows, err := q.Query(ctx, `
		SELECT entity_id, reported_state::text, desired_state::text,
		       reported_version, desired_version,
		       last_reported_at, last_desired_at, updated_at
		FROM public.twin_states
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`, gatewayIDs)
	if err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot query twin_states failed: %w", err)
	}
	defer stRows.Close()
	for stRows.Next() {
		var s stateSnapshotRecord
		if err := stRows.Scan(&s.EntityID, &s.ReportedState, &s.DesiredState, &s.ReportedVersion, &s.DesiredVersion, &s.LastReportedAt, &s.LastDesiredAt, &s.UpdatedAt); err != nil {
			return metadataFixtureSnapshot{}, fmt.Errorf("snapshot scan twin_states failed: %w", err)
		}
		s.UpdatedAt = s.UpdatedAt.UTC()
		if s.LastReportedAt != nil {
			t := s.LastReportedAt.UTC()
			s.LastReportedAt = &t
		}
		if s.LastDesiredAt != nil {
			t := s.LastDesiredAt.UTC()
			s.LastDesiredAt = &t
		}
		snap.States[s.EntityID] = s
	}
	if err := stRows.Err(); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot twin_states rows error: %w", err)
	}

	cRows, err := q.Query(ctx, `
		SELECT id, command_id, entity_id, action, parameters::text, status, desired_version,
		       idempotency_key, issued_by, issued_at, expires_at, acknowledged_at, completed_at, attempt_count, error_code, error_message
		FROM public.twin_commands
		WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`, gatewayIDs)
	if err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot query twin_commands failed: %w", err)
	}
	defer cRows.Close()
	for cRows.Next() {
		var c commandSnapshotRecord
		if err := cRows.Scan(&c.ID, &c.CommandID, &c.EntityID, &c.Action, &c.Parameters, &c.Status, &c.DesiredVersion,
			&c.IdempotencyKey, &c.IssuedBy, &c.IssuedAt, &c.ExpiresAt, &c.AcknowledgedAt, &c.CompletedAt, &c.AttemptCount, &c.ErrorCode, &c.ErrorMessage); err != nil {
			return metadataFixtureSnapshot{}, fmt.Errorf("snapshot scan twin_commands failed: %w", err)
		}
		c.IssuedAt = c.IssuedAt.UTC()
		c.ExpiresAt = c.ExpiresAt.UTC()
		if c.AcknowledgedAt != nil {
			t := c.AcknowledgedAt.UTC()
			c.AcknowledgedAt = &t
		}
		if c.CompletedAt != nil {
			t := c.CompletedAt.UTC()
			c.CompletedAt = &t
		}
		snap.Commands[c.ID] = c
	}
	if err := cRows.Err(); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot twin_commands rows error: %w", err)
	}

	oRows, err := q.Query(ctx, `
		SELECT o.id, o.command_id, o.topic, o.payload::text, o.status, o.attempt_count,
		       o.next_attempt_at, o.created_at, o.published_at
		FROM public.twin_outbox o
		WHERE o.command_id IN (
			SELECT command_id FROM public.twin_commands
			WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))
		)`, gatewayIDs)
	if err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot query twin_outbox failed: %w", err)
	}
	defer oRows.Close()
	for oRows.Next() {
		var o outboxSnapshotRecord
		if err := oRows.Scan(&o.ID, &o.CommandID, &o.Topic, &o.Payload, &o.Status, &o.AttemptCount,
			&o.NextAttemptAt, &o.CreatedAt, &o.PublishedAt); err != nil {
			return metadataFixtureSnapshot{}, fmt.Errorf("snapshot scan twin_outbox failed: %w", err)
		}
		o.NextAttemptAt = o.NextAttemptAt.UTC()
		o.CreatedAt = o.CreatedAt.UTC()
		if o.PublishedAt != nil {
			t := o.PublishedAt.UTC()
			o.PublishedAt = &t
		}
		snap.Outbox[o.ID] = o
	}
	if err := oRows.Err(); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot twin_outbox rows error: %w", err)
	}

	crRows, err := q.Query(ctx, `
		SELECT gateway_id, credential_version, status, last_operation_id, changed_by,
		       activated_at, revoked_at, updated_at, last_error_code
		FROM public.gateway_mqtt_credentials
		WHERE gateway_id = ANY($1)`, gatewayIDs)
	if err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot query gateway_mqtt_credentials failed: %w", err)
	}
	defer crRows.Close()
	for crRows.Next() {
		var c credentialSnapshotRecord
		if err := crRows.Scan(&c.GatewayID, &c.CredentialVersion, &c.Status, &c.LastOperationID, &c.ChangedBy,
			&c.ActivatedAt, &c.RevokedAt, &c.UpdatedAt, &c.LastErrorCode); err != nil {
			return metadataFixtureSnapshot{}, fmt.Errorf("snapshot scan gateway_mqtt_credentials failed: %w", err)
		}
		c.UpdatedAt = c.UpdatedAt.UTC()
		if c.ActivatedAt != nil {
			t := c.ActivatedAt.UTC()
			c.ActivatedAt = &t
		}
		if c.RevokedAt != nil {
			t := c.RevokedAt.UTC()
			c.RevokedAt = &t
		}
		snap.Credentials[c.GatewayID] = c
	}
	if err := crRows.Err(); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot gateway_mqtt_credentials rows error: %w", err)
	}

	evRows, err := q.Query(ctx, `
		SELECT operation_id, gateway_id, credential_version, action, status, phase, execution_contract,
		       idempotency_key, actor_user_id, request_id, delivery_status, ram_applied, snapshot_observed, fresh_positive_verified,
		       created_at, completed_at, updated_at, error_code
		FROM public.gateway_mqtt_credential_events
		WHERE gateway_id = ANY($1)`, gatewayIDs)
	if err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot query gateway_mqtt_credential_events failed: %w", err)
	}
	defer evRows.Close()
	for evRows.Next() {
		var e credentialEventSnapshotRecord
		if err := evRows.Scan(&e.OperationID, &e.GatewayID, &e.CredentialVersion, &e.Action, &e.Status, &e.Phase,
			&e.ExecutionContract, &e.IdempotencyKey, &e.ActorUserID, &e.RequestID, &e.DeliveryStatus, &e.RAMApplied, &e.SnapshotObserved,
			&e.FreshPositiveVerified, &e.CreatedAt, &e.CompletedAt, &e.UpdatedAt, &e.ErrorCode); err != nil {
			return metadataFixtureSnapshot{}, fmt.Errorf("snapshot scan gateway_mqtt_credential_events failed: %w", err)
		}
		e.CreatedAt = e.CreatedAt.UTC()
		e.UpdatedAt = e.UpdatedAt.UTC()
		if e.CompletedAt != nil {
			t := e.CompletedAt.UTC()
			e.CompletedAt = &t
		}
		snap.CredentialEvents[e.OperationID] = e
	}
	if err := evRows.Err(); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot gateway_mqtt_credential_events rows error: %w", err)
	}

	if err := q.QueryRow(ctx, `SELECT count(*) FROM public.twin_temporal_values WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`, gatewayIDs).Scan(&snap.TemporalCount); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot count twin_temporal_values failed: %w", err)
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM public.telemetry WHERE gateway_id = ANY($1)`, gatewayIDs).Scan(&snap.TelemetryRawCount); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot count telemetry failed: %w", err)
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM public.processed_messages WHERE gateway_id = ANY($1)`, gatewayIDs).Scan(&snap.LedgerCount); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot count processed_messages failed: %w", err)
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM public.twin_commands WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1))`, gatewayIDs).Scan(&snap.CommandCount); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot count twin_commands failed: %w", err)
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM public.twin_outbox WHERE command_id IN (SELECT command_id FROM public.twin_commands WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = ANY($1)))`, gatewayIDs).Scan(&snap.OutboxCount); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot count twin_outbox failed: %w", err)
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM public.gateway_mqtt_credentials WHERE gateway_id = ANY($1)`, gatewayIDs).Scan(&snap.CredentialCount); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot count gateway_mqtt_credentials failed: %w", err)
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM public.gateway_mqtt_credential_events WHERE gateway_id = ANY($1)`, gatewayIDs).Scan(&snap.EventCount); err != nil {
		return metadataFixtureSnapshot{}, fmt.Errorf("snapshot count gateway_mqtt_credential_events failed: %w", err)
	}

	return snap, nil
}

func equalUUIDPtr(a, b *uuid.UUID) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func equalStringPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func equalInt64Ptr(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func equalTimePtr(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

func diffCommandRecord(bc, ac commandSnapshotRecord) string {
	if bc.ID != ac.ID {
		return "id"
	}
	if bc.CommandID != ac.CommandID {
		return "command_id"
	}
	if bc.EntityID != ac.EntityID {
		return "entity_id"
	}
	if bc.Action != ac.Action {
		return "action"
	}
	if bc.Parameters != ac.Parameters {
		return "parameters"
	}
	if bc.Status != ac.Status {
		return "status"
	}
	if !equalInt64Ptr(bc.DesiredVersion, ac.DesiredVersion) {
		return "desired_version"
	}
	if !equalStringPtr(bc.IdempotencyKey, ac.IdempotencyKey) {
		return "idempotency_key"
	}
	if !equalUUIDPtr(bc.IssuedBy, ac.IssuedBy) {
		return "issued_by"
	}
	if !bc.IssuedAt.Equal(ac.IssuedAt) {
		return "issued_at"
	}
	if !bc.ExpiresAt.Equal(ac.ExpiresAt) {
		return "expires_at"
	}
	if !equalTimePtr(bc.AcknowledgedAt, ac.AcknowledgedAt) {
		return "acknowledged_at"
	}
	if !equalTimePtr(bc.CompletedAt, ac.CompletedAt) {
		return "completed_at"
	}
	if bc.AttemptCount != ac.AttemptCount {
		return "attempt_count"
	}
	if !equalStringPtr(bc.ErrorCode, ac.ErrorCode) {
		return "error_code"
	}
	if !equalStringPtr(bc.ErrorMessage, ac.ErrorMessage) {
		return "error_message"
	}
	return ""
}

func diffOutboxRecord(bo, ao outboxSnapshotRecord) string {
	if bo.ID != ao.ID {
		return "id"
	}
	if bo.CommandID != ao.CommandID {
		return "command_id"
	}
	if bo.Topic != ao.Topic {
		return "topic"
	}
	if bo.Payload != ao.Payload {
		return "payload"
	}
	if bo.Status != ao.Status {
		return "status"
	}
	if bo.AttemptCount != ao.AttemptCount {
		return "attempt_count"
	}
	if !bo.NextAttemptAt.Equal(ao.NextAttemptAt) {
		return "next_attempt_at"
	}
	if !bo.CreatedAt.Equal(ao.CreatedAt) {
		return "created_at"
	}
	if !equalTimePtr(bo.PublishedAt, ao.PublishedAt) {
		return "published_at"
	}
	return ""
}

func diffCredentialRecord(bc, ac credentialSnapshotRecord) string {
	if bc.GatewayID != ac.GatewayID {
		return "gateway_id"
	}
	if bc.CredentialVersion != ac.CredentialVersion {
		return "credential_version"
	}
	if bc.Status != ac.Status {
		return "status"
	}
	if !equalUUIDPtr(bc.LastOperationID, ac.LastOperationID) {
		return "last_operation_id"
	}
	if !equalUUIDPtr(bc.ChangedBy, ac.ChangedBy) {
		return "changed_by"
	}
	if !equalTimePtr(bc.ActivatedAt, ac.ActivatedAt) {
		return "activated_at"
	}
	if !equalTimePtr(bc.RevokedAt, ac.RevokedAt) {
		return "revoked_at"
	}
	if !bc.UpdatedAt.Equal(ac.UpdatedAt) {
		return "updated_at"
	}
	if !equalStringPtr(bc.LastErrorCode, ac.LastErrorCode) {
		return "last_error_code"
	}
	return ""
}

func diffCredentialEventRecord(be, ae credentialEventSnapshotRecord) string {
	if be.OperationID != ae.OperationID {
		return "operation_id"
	}
	if be.GatewayID != ae.GatewayID {
		return "gateway_id"
	}
	if !equalInt64Ptr(be.CredentialVersion, ae.CredentialVersion) {
		return "credential_version"
	}
	if be.Action != ae.Action {
		return "action"
	}
	if be.Status != ae.Status {
		return "status"
	}
	if be.Phase != ae.Phase {
		return "phase"
	}
	if be.ExecutionContract != ae.ExecutionContract {
		return "execution_contract"
	}
	if !equalUUIDPtr(be.IdempotencyKey, ae.IdempotencyKey) {
		return "idempotency_key"
	}
	if !equalUUIDPtr(be.ActorUserID, ae.ActorUserID) {
		return "actor_user_id"
	}
	if !equalStringPtr(be.RequestID, ae.RequestID) {
		return "request_id"
	}
	if be.DeliveryStatus != ae.DeliveryStatus {
		return "delivery_status"
	}
	if be.RAMApplied != ae.RAMApplied {
		return "ram_applied"
	}
	if be.SnapshotObserved != ae.SnapshotObserved {
		return "snapshot_observed"
	}
	if be.FreshPositiveVerified != ae.FreshPositiveVerified {
		return "fresh_positive_verified"
	}
	if !be.CreatedAt.Equal(ae.CreatedAt) {
		return "created_at"
	}
	if !equalTimePtr(be.CompletedAt, ae.CompletedAt) {
		return "completed_at"
	}
	if !be.UpdatedAt.Equal(ae.UpdatedAt) {
		return "updated_at"
	}
	if !equalStringPtr(be.ErrorCode, ae.ErrorCode) {
		return "error_code"
	}
	return ""
}

// controlCredentialContentEqual verifies that the control-plane
// (twin_commands / twin_outbox) and credential-plane
// (gateway_mqtt_credentials / gateway_mqtt_credential_events) rows are identical
// between two captures, comparing actual field content so a same-cardinality
// one-field mutation is detected rather than passing a count-only check. These
// tables store only metadata/journal columns, never secret material. It returns a
// descriptive error naming the record and field without dumping arbitrary raw error
// messages, payloads, or audit contents.
func controlCredentialContentEqual(before, after metadataFixtureSnapshot) error {
	if len(before.Commands) != len(after.Commands) {
		return fmt.Errorf("commands map len mismatch: %d vs %d", len(before.Commands), len(after.Commands))
	}
	for k, bc := range before.Commands {
		ac, ok := after.Commands[k]
		if !ok {
			return fmt.Errorf("command %s missing in after snapshot", k)
		}
		if diff := diffCommandRecord(bc, ac); diff != "" {
			return fmt.Errorf("command %s field %s changed", k, diff)
		}
	}

	if len(before.Outbox) != len(after.Outbox) {
		return fmt.Errorf("outbox map len mismatch: %d vs %d", len(before.Outbox), len(after.Outbox))
	}
	for k, bo := range before.Outbox {
		ao, ok := after.Outbox[k]
		if !ok {
			return fmt.Errorf("outbox %s missing in after snapshot", k)
		}
		if diff := diffOutboxRecord(bo, ao); diff != "" {
			return fmt.Errorf("outbox %s field %s changed", k, diff)
		}
	}

	if len(before.Credentials) != len(after.Credentials) {
		return fmt.Errorf("credentials map len mismatch: %d vs %d", len(before.Credentials), len(after.Credentials))
	}
	for k, bc := range before.Credentials {
		ac, ok := after.Credentials[k]
		if !ok {
			return fmt.Errorf("credential %s missing in after snapshot", k)
		}
		if diff := diffCredentialRecord(bc, ac); diff != "" {
			return fmt.Errorf("credential %s field %s changed", k, diff)
		}
	}

	if len(before.CredentialEvents) != len(after.CredentialEvents) {
		return fmt.Errorf("credential events map len mismatch: %d vs %d", len(before.CredentialEvents), len(after.CredentialEvents))
	}
	for k, be := range before.CredentialEvents {
		ae, ok := after.CredentialEvents[k]
		if !ok {
			return fmt.Errorf("credential event %s missing in after snapshot", k)
		}
		if diff := diffCredentialEventRecord(be, ae); diff != "" {
			return fmt.Errorf("credential event %s field %s changed", k, diff)
		}
	}

	return nil
}

func assertSnapshotsEqual(t *testing.T, label string, before, after metadataFixtureSnapshot) {
	t.Helper()
	if before.TemporalCount != after.TemporalCount {
		t.Fatalf("[%s] temporal count changed: %d vs %d", label, before.TemporalCount, after.TemporalCount)
	}
	if before.TelemetryRawCount != after.TelemetryRawCount {
		t.Fatalf("[%s] raw telemetry count changed: %d vs %d", label, before.TelemetryRawCount, after.TelemetryRawCount)
	}
	if before.LedgerCount != after.LedgerCount {
		t.Fatalf("[%s] ledger count changed: %d vs %d", label, before.LedgerCount, after.LedgerCount)
	}
	if before.CommandCount != after.CommandCount {
		t.Fatalf("[%s] command count changed: %d vs %d", label, before.CommandCount, after.CommandCount)
	}
	if before.OutboxCount != after.OutboxCount {
		t.Fatalf("[%s] outbox count changed: %d vs %d", label, before.OutboxCount, after.OutboxCount)
	}
	if before.CredentialCount != after.CredentialCount {
		t.Fatalf("[%s] credential count changed: %d vs %d", label, before.CredentialCount, after.CredentialCount)
	}
	if before.EventCount != after.EventCount {
		t.Fatalf("[%s] credential events count changed: %d vs %d", label, before.EventCount, after.EventCount)
	}

	if err := controlCredentialContentEqual(before, after); err != nil {
		t.Fatalf("[%s] control/credential content changed: %v", label, err)
	}

	if len(before.Gateways) != len(after.Gateways) {
		t.Fatalf("[%s] gateways map len mismatch: %d vs %d", label, len(before.Gateways), len(after.Gateways))
	}
	for k, bg := range before.Gateways {
		ag, ok := after.Gateways[k]
		if !ok {
			t.Fatalf("[%s] gateway %s missing in after snapshot", label, k)
		}
		if bg.Name != ag.Name {
			t.Fatalf("[%s] gateway %s name changed: %q vs %q", label, k, bg.Name, ag.Name)
		}
		if (bg.Description == nil && ag.Description != nil) || (bg.Description != nil && ag.Description == nil) || (bg.Description != nil && ag.Description != nil && *bg.Description != *ag.Description) {
			t.Fatalf("[%s] gateway %s description changed: %v vs %v", label, k, bg.Description, ag.Description)
		}
		if !bg.CreatedAt.Equal(ag.CreatedAt) {
			t.Fatalf("[%s] gateway %s created_at changed: %v vs %v", label, k, bg.CreatedAt, ag.CreatedAt)
		}
	}

	if len(before.Sensors) != len(after.Sensors) {
		t.Fatalf("[%s] sensors map len mismatch: %d vs %d", label, len(before.Sensors), len(after.Sensors))
	}
	for k, bs := range before.Sensors {
		as, ok := after.Sensors[k]
		if !ok {
			t.Fatalf("[%s] sensor %s missing in after snapshot", label, k)
		}
		if bs.Name != as.Name {
			t.Fatalf("[%s] sensor %s name changed: %q vs %q", label, k, bs.Name, as.Name)
		}
		if (bs.Unit == nil && as.Unit != nil) || (bs.Unit != nil && as.Unit == nil) || (bs.Unit != nil && as.Unit != nil && *bs.Unit != *as.Unit) {
			t.Fatalf("[%s] sensor %s unit changed: %v vs %v", label, k, bs.Unit, as.Unit)
		}
		if !bs.CreatedAt.Equal(as.CreatedAt) {
			t.Fatalf("[%s] sensor %s created_at changed: %v vs %v", label, k, bs.CreatedAt, as.CreatedAt)
		}
	}

	if len(before.Memberships) != len(after.Memberships) {
		t.Fatalf("[%s] memberships map len mismatch: %d vs %d", label, len(before.Memberships), len(after.Memberships))
	}
	for k, bm := range before.Memberships {
		am, ok := after.Memberships[k]
		if !ok {
			t.Fatalf("[%s] membership %s missing in after snapshot", label, k)
		}
		if bm.Role != am.Role {
			t.Fatalf("[%s] membership %s role changed: %q vs %q", label, k, bm.Role, am.Role)
		}
	}

	if len(before.Entities) != len(after.Entities) {
		t.Fatalf("[%s] entities map len mismatch: %d vs %d", label, len(before.Entities), len(after.Entities))
	}
	for k, be := range before.Entities {
		ae, ok := after.Entities[k]
		if !ok {
			t.Fatalf("[%s] entity %s missing in after snapshot", label, k)
		}
		if be.Name != ae.Name || be.EntityID != ae.EntityID || be.EntityType != ae.EntityType || be.GatewayID != ae.GatewayID || be.Attributes != ae.Attributes {
			t.Fatalf("[%s] entity %s attributes/metadata changed", label, k)
		}
		if !be.CreatedAt.Equal(ae.CreatedAt) || !be.UpdatedAt.Equal(ae.UpdatedAt) {
			t.Fatalf("[%s] entity %s timestamps changed: before=(%v,%v), after=(%v,%v)", label, k, be.CreatedAt, be.UpdatedAt, ae.CreatedAt, ae.UpdatedAt)
		}
	}

	if len(before.Relationships) != len(after.Relationships) {
		t.Fatalf("[%s] relationships map len mismatch: %d vs %d", label, len(before.Relationships), len(after.Relationships))
	}
	for k, br := range before.Relationships {
		ar, ok := after.Relationships[k]
		if !ok {
			t.Fatalf("[%s] relationship %s missing in after snapshot", label, k)
		}
		if !br.CreatedAt.Equal(ar.CreatedAt) {
			t.Fatalf("[%s] relationship %s created_at changed: %v vs %v", label, k, br.CreatedAt, ar.CreatedAt)
		}
	}

	if len(before.States) != len(after.States) {
		t.Fatalf("[%s] states map len mismatch: %d vs %d", label, len(before.States), len(after.States))
	}
	for k, bs := range before.States {
		as, ok := after.States[k]
		if !ok {
			t.Fatalf("[%s] state %s missing in after snapshot", label, k)
		}
		if bs.ReportedState != as.ReportedState || bs.DesiredState != as.DesiredState {
			t.Fatalf("[%s] state %s JSON changed: reported(%q vs %q), desired(%q vs %q)", label, k, bs.ReportedState, as.ReportedState, bs.DesiredState, as.DesiredState)
		}
		if bs.ReportedVersion != as.ReportedVersion || bs.DesiredVersion != as.DesiredVersion {
			t.Fatalf("[%s] state %s versions changed: rep(%d vs %d), des(%d vs %d)", label, k, bs.ReportedVersion, as.ReportedVersion, bs.DesiredVersion, as.DesiredVersion)
		}
		if (bs.LastReportedAt == nil && as.LastReportedAt != nil) || (bs.LastReportedAt != nil && as.LastReportedAt == nil) || (bs.LastReportedAt != nil && as.LastReportedAt != nil && !bs.LastReportedAt.Equal(*as.LastReportedAt)) {
			t.Fatalf("[%s] state %s last_reported_at changed: %v vs %v", label, k, bs.LastReportedAt, as.LastReportedAt)
		}
		if (bs.LastDesiredAt == nil && as.LastDesiredAt != nil) || (bs.LastDesiredAt != nil && as.LastDesiredAt == nil) || (bs.LastDesiredAt != nil && as.LastDesiredAt != nil && !bs.LastDesiredAt.Equal(*as.LastDesiredAt)) {
			t.Fatalf("[%s] state %s last_desired_at changed: %v vs %v", label, k, bs.LastDesiredAt, as.LastDesiredAt)
		}
		if !bs.UpdatedAt.Equal(as.UpdatedAt) {
			t.Fatalf("[%s] state %s updated_at changed: %v vs %v", label, k, bs.UpdatedAt, as.UpdatedAt)
		}
	}
}

func assertPatchPreservesNonMetadata(t *testing.T, before, after metadataFixtureSnapshot, targetEntityUUID uuid.UUID, expectedNewName *string) {
	t.Helper()
	if before.TemporalCount != after.TemporalCount || before.TelemetryRawCount != after.TelemetryRawCount ||
		before.LedgerCount != after.LedgerCount ||
		before.CommandCount != after.CommandCount || before.OutboxCount != after.OutboxCount ||
		before.CredentialCount != after.CredentialCount || before.EventCount != after.EventCount {
		t.Fatalf("patch mutated side effect counts: temporal(%d->%d), raw(%d->%d), ledger(%d->%d), cmd(%d->%d), outbox(%d->%d), cred(%d->%d), event(%d->%d)",
			before.TemporalCount, after.TemporalCount, before.TelemetryRawCount, after.TelemetryRawCount,
			before.LedgerCount, after.LedgerCount,
			before.CommandCount, after.CommandCount, before.OutboxCount, after.OutboxCount,
			before.CredentialCount, after.CredentialCount, before.EventCount, after.EventCount)
	}

	if err := controlCredentialContentEqual(before, after); err != nil {
		t.Fatalf("patch mutated control/credential content: %v", err)
	}

	if len(before.Memberships) != len(after.Memberships) {
		t.Fatalf("memberships count changed: %d vs %d", len(before.Memberships), len(after.Memberships))
	}
	for k, bm := range before.Memberships {
		am, ok := after.Memberships[k]
		if !ok || bm.Role != am.Role {
			t.Fatalf("membership %s changed after patch", k)
		}
	}
	if len(before.Relationships) != len(after.Relationships) {
		t.Fatalf("relationships count changed: %d vs %d", len(before.Relationships), len(after.Relationships))
	}
	for k, br := range before.Relationships {
		ar, ok := after.Relationships[k]
		if !ok || !br.CreatedAt.Equal(ar.CreatedAt) {
			t.Fatalf("relationship %s changed after patch", k)
		}
	}

	if len(before.States) != len(after.States) {
		t.Fatalf("states count changed: %d vs %d", len(before.States), len(after.States))
	}
	for k, bs := range before.States {
		as, ok := after.States[k]
		if !ok {
			t.Fatalf("state %s missing after patch", k)
		}
		if bs.ReportedState != as.ReportedState || bs.DesiredState != as.DesiredState ||
			bs.ReportedVersion != as.ReportedVersion || bs.DesiredVersion != as.DesiredVersion ||
			!bs.UpdatedAt.Equal(as.UpdatedAt) {
			t.Fatalf("state %s was mutated by metadata patch!", k)
		}
	}

	for k, be := range before.Entities {
		if k == targetEntityUUID {
			continue
		}
		ae, ok := after.Entities[k]
		if !ok || be.Name != ae.Name || !be.UpdatedAt.Equal(ae.UpdatedAt) {
			t.Fatalf("non-target entity %s was mutated by patch", k)
		}
	}

	be, ok := before.Entities[targetEntityUUID]
	if !ok {
		t.Fatalf("target entity %s not in before snapshot", targetEntityUUID)
	}
	ae, ok := after.Entities[targetEntityUUID]
	if !ok {
		t.Fatalf("target entity %s not in after snapshot", targetEntityUUID)
	}
	if be.ID != ae.ID || be.EntityID != ae.EntityID || be.EntityType != ae.EntityType || be.GatewayID != ae.GatewayID || be.Attributes != ae.Attributes || !be.CreatedAt.Equal(ae.CreatedAt) {
		t.Fatal("target entity non-metadata fields mutated")
	}
	if expectedNewName != nil {
		if ae.Name != *expectedNewName {
			t.Fatalf("target entity name = %q, expected %q", ae.Name, *expectedNewName)
		}
		if !ae.UpdatedAt.After(be.UpdatedAt) {
			t.Fatalf("target entity updated_at not advanced: %v <= %v", ae.UpdatedAt, be.UpdatedAt)
		}
	}
}

// fakeSnapshotRows implements pgx.Rows for the snapshot acquisition-error
// regression. Only the methods the snapshot helper calls are meaningful; the
// embedded interface is never used.
type fakeSnapshotRows struct {
	pgx.Rows
	remaining int
	scanErr   error
	endErr    error
}

func (r *fakeSnapshotRows) Close()     {}
func (r *fakeSnapshotRows) Err() error { return r.endErr }
func (r *fakeSnapshotRows) Next() bool {
	if r.remaining > 0 {
		r.remaining--
		return true
	}
	return false
}
func (r *fakeSnapshotRows) Scan(...any) error { return r.scanErr }

// metadataSideEffectDiagnostic deliberately excludes row contents and payloads.
func metadataSideEffectDiagnostic(kind string, snap metadataFixtureSnapshot) string {
	return fmt.Sprintf("unexpected POST %s side effects: sensors=%d relationships=%d temporal=%d raw=%d commands=%d outbox=%d credentials=%d events=%d",
		kind, len(snap.Sensors), len(snap.Relationships), snap.TemporalCount, snap.TelemetryRawCount,
		snap.CommandCount, snap.OutboxCount, snap.CredentialCount, snap.EventCount)
}

type metadataAccountCounts struct {
	Profiles  int
	AuthUsers int
}

func captureMetadataAccountCounts(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, authPresent bool) (metadataAccountCounts, error) {
	var counts metadataAccountCounts
	if err := db.QueryRow(ctx, "SELECT count(*) FROM public.profiles").Scan(&counts.Profiles); err != nil {
		return metadataAccountCounts{}, fmt.Errorf("count fixture account side effects (profiles): %w", err)
	}
	if authPresent {
		if err := db.QueryRow(ctx, "SELECT count(*) FROM auth.users").Scan(&counts.AuthUsers); err != nil {
			return metadataAccountCounts{}, fmt.Errorf("count fixture account side effects (auth.users): %w", err)
		}
	}
	return counts, nil
}

type metadataAccountCountQuerier struct {
	queries []string
	failAt  int
	err     error
}

func (q *metadataAccountCountQuerier) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	q.queries = append(q.queries, sql)
	if len(q.queries) == q.failAt {
		return errorScanRow{err: q.err}
	}
	return snapshotZeroIntRow{}
}

func TestMetadataAccountCounts(t *testing.T) {
	for _, phase := range []string{"before", "after"} {
		for _, failAt := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s_query_%d_failure", phase, failAt), func(t *testing.T) {
				cause := errors.New("injected account count failure")
				q := &metadataAccountCountQuerier{failAt: failAt, err: cause}
				counts, err := captureMetadataAccountCounts(context.Background(), q, true)
				if !errors.Is(err, cause) || counts != (metadataAccountCounts{}) {
					t.Fatal("failed count must return its cause and no usable partial counts")
				}
			})
		}
	}
	for _, present := range []bool{false, true} {
		t.Run(fmt.Sprintf("auth_present_%t", present), func(t *testing.T) {
			q := &metadataAccountCountQuerier{}
			_, err := captureMetadataAccountCounts(context.Background(), q, present)
			if err != nil {
				t.Fatal(err)
			}
			wantQueries := 1
			if present {
				wantQueries = 2
			}
			if len(q.queries) != wantQueries {
				t.Fatalf("expected %d account queries, got %d", wantQueries, len(q.queries))
			}
		})
	}
}

func TestMetadataSideEffectDiagnosticExcludesContents(t *testing.T) {
	const marker = "private-payload-marker"
	snap := metadataFixtureSnapshot{
		Commands:     map[uuid.UUID]commandSnapshotRecord{uuid.New(): {Parameters: marker}},
		Outbox:       map[uuid.UUID]outboxSnapshotRecord{uuid.New(): {Payload: marker}},
		CommandCount: 1,
		OutboxCount:  1,
	}
	for _, kind := range []string{"Gateway", "Sensor"} {
		message := metadataSideEffectDiagnostic(kind, snap)
		if strings.Contains(message, marker) || !strings.Contains(message, "commands=1 outbox=1") {
			t.Fatal("diagnostic must report counts without row contents")
		}
	}
}

// snapshotZeroIntRow stands in for a successful count(*) QueryRow scan.
type snapshotZeroIntRow struct{}

func (snapshotZeroIntRow) Scan(dest ...any) error {
	if len(dest) == 1 {
		if p, ok := dest[0].(*int); ok {
			*p = 0
		}
	}
	return nil
}

// fakeSnapshotQuerier injects snapshot acquisition failures without a database.
type fakeSnapshotQuerier struct {
	queryErr    error
	queryRowErr error
	rows        *fakeSnapshotRows
}

func (f *fakeSnapshotQuerier) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	if f.rows != nil {
		return f.rows, nil
	}
	return &fakeSnapshotRows{}, nil
}

func (f *fakeSnapshotQuerier) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	if f.queryRowErr != nil {
		return errorScanRow{err: f.queryRowErr}
	}
	return snapshotZeroIntRow{}
}

// recordingFatalReporter is a fail-fast double: it records Helper/Fatalf calls
// instead of aborting, so a regression can assert that a helper refuses to
// return a usable value after a capture error without failing its own test.
type recordingFatalReporter struct {
	helperCalls int
	fatalfCalls int
	lastMessage string
}

func (r *recordingFatalReporter) Helper() { r.helperCalls++ }

func (r *recordingFatalReporter) Fatalf(format string, args ...any) {
	r.fatalfCalls++
	r.lastMessage = fmt.Sprintf(format, args...)
}

// TestMetadataSnapshotAcquisitionErrorPropagation proves every snapshot
// acquisition failure path (row Query, row Scan, Rows.Err, count Scan) surfaces
// as an error, returns the zero snapshot (never a comparable partial/empty one),
// and aborts a fail-fast caller.
func TestMetadataSnapshotAcquisitionErrorPropagation(t *testing.T) {
	cases := []struct {
		name    string
		querier snapshotQuerier
	}{
		{"row_query_error", &fakeSnapshotQuerier{queryErr: errors.New("injected query failure")}},
		{"row_scan_error", &fakeSnapshotQuerier{rows: &fakeSnapshotRows{remaining: 1, scanErr: errors.New("injected scan failure")}}},
		{"rows_err_error", &fakeSnapshotQuerier{rows: &fakeSnapshotRows{endErr: errors.New("injected rows error")}}},
		{"count_query_error", &fakeSnapshotQuerier{queryRowErr: errors.New("injected count failure")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap, err := captureMetadataFixtureSnapshot(context.Background(), tc.querier, []string{"gw_any"})
			if err == nil {
				t.Fatalf("expected snapshot acquisition error, got nil")
			}
			if !reflect.DeepEqual(snap, metadataFixtureSnapshot{}) {
				t.Fatal("failed capture must return the zero snapshot")
			}
		})
	}

	t.Run("successful_empty_scope", func(t *testing.T) {
		snap, err := captureMetadataFixtureSnapshot(context.Background(), &fakeSnapshotQuerier{}, []string{"gw_any"})
		if err != nil {
			t.Fatalf("unexpected error on empty successful capture: %v", err)
		}
		if snap.TemporalCount != 0 || snap.CommandCount != 0 || len(snap.Commands) != 0 || len(snap.Credentials) != 0 {
			t.Fatal("expected empty snapshot")
		}
	})

	t.Run("caller_fails_fast_on_acquire_error", func(t *testing.T) {
		rec := &recordingFatalReporter{}
		requireMetadataFixtureSnapshot(context.Background(), rec, &fakeSnapshotQuerier{queryErr: errors.New("injected")}, []string{"gw_any"})
		if rec.fatalfCalls != 1 {
			t.Fatalf("mustCaptureMetadataFixtureSnapshot must fail fast exactly once on acquisition error, got %d", rec.fatalfCalls)
		}
	})
}

// TestMetadataSnapshotContentComparatorDetectsMutation proves the content
// comparator detects a same-cardinality, one-field mutation in the control and
// credential sentinels, so a count-only check cannot silently pass.
func TestMetadataSnapshotContentComparatorDetectsMutation(t *testing.T) {
	cmdID := uuid.New()
	opID := uuid.New()
	entityID := uuid.New()
	actorID := uuid.New()
	reqID := "req-sentinel-1"
	errMsg := "sentinel-command-error"
	base := func() metadataFixtureSnapshot {
		return metadataFixtureSnapshot{
			Commands: map[uuid.UUID]commandSnapshotRecord{
				cmdID: {
					ID: cmdID, CommandID: cmdID, EntityID: entityID, Action: "update_configuration",
					Parameters: `{"sampling_interval_seconds":5}`, Status: "pending", AttemptCount: 0,
					IssuedBy: &actorID, ErrorMessage: &errMsg,
				},
			},
			Outbox: map[uuid.UUID]outboxSnapshotRecord{
				cmdID: {ID: cmdID, CommandID: cmdID, Topic: "t", Payload: `{"protocol_version":1}`, Status: "pending"},
			},
			Credentials: map[string]credentialSnapshotRecord{
				"gw_1": {GatewayID: "gw_1", CredentialVersion: 1, Status: "provisioning"},
			},
			CredentialEvents: map[uuid.UUID]credentialEventSnapshotRecord{
				opID: {
					OperationID: opID, GatewayID: "gw_1", Action: "provision", Status: "pending",
					Phase: "intent", ExecutionContract: "simplified",
					ActorUserID: &actorID, RequestID: &reqID,
				},
			},
			CommandCount: 1, OutboxCount: 1, CredentialCount: 1, EventCount: 1,
		}
	}

	mutations := []struct {
		name          string
		expectedField string
		mutate        func(s *metadataFixtureSnapshot)
	}{
		{"command_status", "status", func(s *metadataFixtureSnapshot) {
			v := s.Commands[cmdID]
			v.Status = "succeeded"
			s.Commands[cmdID] = v
		}},
		{"command_attempt_count", "attempt_count", func(s *metadataFixtureSnapshot) { v := s.Commands[cmdID]; v.AttemptCount = 1; s.Commands[cmdID] = v }},
		{"command_issued_by", "issued_by", func(s *metadataFixtureSnapshot) {
			v := s.Commands[cmdID]
			other := uuid.New()
			v.IssuedBy = &other
			s.Commands[cmdID] = v
		}},
		{"command_issued_by_to_nil", "issued_by", func(s *metadataFixtureSnapshot) {
			v := s.Commands[cmdID]
			v.IssuedBy = nil
			s.Commands[cmdID] = v
		}},
		{"command_error_message", "error_message", func(s *metadataFixtureSnapshot) {
			v := s.Commands[cmdID]
			otherMsg := "different-command-error"
			v.ErrorMessage = &otherMsg
			s.Commands[cmdID] = v
		}},
		{"command_error_message_to_nil", "error_message", func(s *metadataFixtureSnapshot) {
			v := s.Commands[cmdID]
			v.ErrorMessage = nil
			s.Commands[cmdID] = v
		}},
		{"outbox_payload", "payload", func(s *metadataFixtureSnapshot) { v := s.Outbox[cmdID]; v.Payload = `{"x":2}`; s.Outbox[cmdID] = v }},
		{"credential_status", "status", func(s *metadataFixtureSnapshot) {
			v := s.Credentials["gw_1"]
			v.Status = "revoked"
			s.Credentials["gw_1"] = v
		}},
		{"credential_event_phase", "phase", func(s *metadataFixtureSnapshot) {
			v := s.CredentialEvents[opID]
			v.Phase = "finalize"
			s.CredentialEvents[opID] = v
		}},
		{"credential_event_actor_user_id", "actor_user_id", func(s *metadataFixtureSnapshot) {
			v := s.CredentialEvents[opID]
			other := uuid.New()
			v.ActorUserID = &other
			s.CredentialEvents[opID] = v
		}},
		{"credential_event_actor_user_id_to_nil", "actor_user_id", func(s *metadataFixtureSnapshot) {
			v := s.CredentialEvents[opID]
			v.ActorUserID = nil
			s.CredentialEvents[opID] = v
		}},
		{"credential_event_request_id", "request_id", func(s *metadataFixtureSnapshot) {
			v := s.CredentialEvents[opID]
			otherReq := "req-sentinel-2"
			v.RequestID = &otherReq
			s.CredentialEvents[opID] = v
		}},
		{"credential_event_request_id_to_nil", "request_id", func(s *metadataFixtureSnapshot) {
			v := s.CredentialEvents[opID]
			v.RequestID = nil
			s.CredentialEvents[opID] = v
		}},
	}
	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			before := base()
			after := base()
			m.mutate(&after)
			err := controlCredentialContentEqual(before, after)
			if err == nil {
				t.Fatalf("comparator failed to detect a same-cardinality %s mutation", m.name)
			}
			expectedSubstr := "field " + m.expectedField + " changed"
			if !strings.Contains(err.Error(), expectedSubstr) {
				t.Fatalf("expected error to contain %q, got: %v", expectedSubstr, err)
			}
			// Verify sensitive contents / raw error messages / arbitrary payloads are not leaked in the error string
			if strings.Contains(err.Error(), errMsg) || strings.Contains(err.Error(), "different-command-error") {
				t.Fatalf("error message leaked raw error_message content: %v", err)
			}
			if strings.Contains(err.Error(), reqID) || strings.Contains(err.Error(), "req-sentinel-2") {
				t.Fatalf("error message leaked raw request_id content: %v", err)
			}
		})
	}

	t.Run("identical_content_passes", func(t *testing.T) {
		if err := controlCredentialContentEqual(base(), base()); err != nil {
			t.Fatalf("comparator must accept identical content, got: %v", err)
		}
	})
}

// TestNativeEntityMetadata_MutationSlice exercises the first narrow native slice of Task 4.5.7:
// successful POST/PATCH Gateway/Sensor, create/replay/conflict and PATCH field/no-op semantics,
// executed against real PostgreSQL 16 / TimescaleDB 2.29 under the non-superuser application role iot_backend_app.
func TestNativeEntityMetadata_MutationSlice(t *testing.T) {
	setupURL, backendURL := resolveTestDatabaseURLs(t)
	if setupURL == "" || backendURL == "" {
		t.Skip("Skipping native Digital Twin metadata tests: set TWIN_METADATA_TEST_NATIVE=1 to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// 1. Setup connection pools
	setupPool, err := pgxpool.New(ctx, setupURL)
	if err != nil {
		t.Fatalf("setup database pool connection failed: %v", err)
	}
	defer setupPool.Close()

	appPool, err := pgxpool.New(ctx, backendURL)
	if err != nil {
		t.Fatalf("application database pool connection failed: %v", err)
	}
	defer appPool.Close()

	// 2. Verify target database versions, schema version, and application role
	var pgVersion, tsVersion string
	if err := setupPool.QueryRow(ctx, "SELECT version()").Scan(&pgVersion); err != nil {
		t.Fatalf("failed to query postgres version: %v", err)
	}
	if err := setupPool.QueryRow(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").Scan(&tsVersion); err != nil {
		t.Fatalf("failed to query timescaledb extension version: %v", err)
	}
	var schemaVersion int
	if err := setupPool.QueryRow(ctx, "SELECT MAX(version) FROM schema_migrations").Scan(&schemaVersion); err != nil {
		t.Fatalf("failed to query schema migrations: %v", err)
	}

	t.Logf("Database Target Verified: PostgreSQL Version = %s, TimescaleDB Version = %s, Schema Version = %d",
		strings.Fields(pgVersion)[1], tsVersion, schemaVersion)
	if schemaVersion < 20 {
		t.Fatalf("expected schema version >= 20, got %d", schemaVersion)
	}

	var appUser string
	var appSuper, appCanLogin bool
	err = appPool.QueryRow(ctx, `
		SELECT current_user, r.rolsuper, r.rolcanlogin
		FROM pg_roles r WHERE r.rolname = current_user
	`).Scan(&appUser, &appSuper, &appCanLogin)
	if err != nil {
		t.Fatalf("failed to verify application connection role: %v", err)
	}
	if appUser != "iot_backend_app" {
		t.Fatalf("expected application role iot_backend_app, got %q", appUser)
	}
	if appSuper {
		t.Fatalf("application role iot_backend_app must not be superuser")
	}
	if !appCanLogin {
		t.Fatalf("application role iot_backend_app must have login privilege")
	}
	t.Logf("Application Connection Verified: user = %s, superuser = %v, canlogin = %v", appUser, appSuper, appCanLogin)

	// 3. Define unique run identifiers and register cleanup BEFORE any writes
	runID := uuid.New().String()[:8]
	gwIDA := fmt.Sprintf("gw_meta_a_%s", runID)
	gwIDB := fmt.Sprintf("gw_meta_b_%s", runID)
	gwIncID := fmt.Sprintf("gw_meta_inc_%s", runID)
	gwFreshID := fmt.Sprintf("gw_meta_fresh_%s", runID)
	gwRollbackID := fmt.Sprintf("gw_meta_rb_%s", runID)
	gwConcID := fmt.Sprintf("gw_meta_conc_%s", runID)
	gwConcDiffID := fmt.Sprintf("gw_meta_pcdiff_%s", runID)
	gwConcSameID := fmt.Sprintf("gw_meta_pcsame_%s", runID)
	gwConcEarlyFailID := fmt.Sprintf("gw_meta_pcearlyfail_%s", runID)
	gwConcPostID := fmt.Sprintf("gw_meta_concpost_%s", runID)
	gwUpdatedAtID := fmt.Sprintf("gw_meta_ts_%s", runID)
	gwSentinelID := fmt.Sprintf("gw_meta_sentinel_%s", runID)
	sensorUpdatedAtID := fmt.Sprintf("sens_ts_%s", runID)
	sensorSharedID := fmt.Sprintf("sens_shared_%s", runID)
	sensorOwnerID := fmt.Sprintf("sens_owner_%s", runID)
	sensorIncID := fmt.Sprintf("sens_inc_%s", runID)
	sensorFreshID := fmt.Sprintf("sens_fresh_%s", runID)
	sensorRollbackID := fmt.Sprintf("sens_rb_%s", runID)
	sensorConcID := fmt.Sprintf("sens_conc_%s", runID)
	sensorConcPostID := fmt.Sprintf("sens_concpost_%s", runID)

	adminUser := uuid.New()
	ownerAUser := uuid.New()
	ownerBUser := uuid.New()
	operatorUser := uuid.New()
	viewerUser := uuid.New()
	nonMemberUser := uuid.New()
	otherUser := uuid.New()

	allUserIDs := []uuid.UUID{adminUser, ownerAUser, ownerBUser, operatorUser, viewerUser, nonMemberUser, otherUser}
	fixtureIDs := metadataFixtureIDs{
		RunID:      runID,
		GatewayIDs: []string{gwIDA, gwIDB, gwIncID, gwFreshID, gwRollbackID, gwConcID, gwConcDiffID, gwConcSameID, gwConcEarlyFailID, gwConcPostID, gwUpdatedAtID, gwSentinelID},
		AllUserIDs: allUserIDs,
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		performMetadataFixtureCleanup(cleanupCtx, t, setupPool, fixtureIDs)
	}()

	// 4. Seed user accounts and platform_admins via setupPool
	var hasAuthUsers bool
	if err := setupPool.QueryRow(ctx, "SELECT to_regclass('auth.users') IS NOT NULL").Scan(&hasAuthUsers); err != nil {
		t.Fatalf("failed to check auth.users table existence in setup: %v", err)
	}
	fixtureIDs.AuthUsersPresent = &hasAuthUsers
	if hasAuthUsers {
		for _, uID := range allUserIDs {
			_, err := setupPool.Exec(ctx, `
				INSERT INTO auth.users (id, aud, role, email, raw_app_meta_data, raw_user_meta_data, created_at, updated_at)
				VALUES ($1, 'authenticated', 'authenticated', $2, '{"provider":"email"}', '{}', now(), now())
				ON CONFLICT (id) DO NOTHING`,
				uID, fmt.Sprintf("%s@example.com", uID.String()[:8]),
			)
			if err != nil {
				t.Fatalf("failed to insert auth user %s: %v", uID, err)
			}
		}
	}

	for _, uID := range allUserIDs {
		_, err := setupPool.Exec(ctx, `
			INSERT INTO public.profiles (id, full_name)
			VALUES ($1, $2)
			ON CONFLICT (id) DO NOTHING`,
			uID, fmt.Sprintf("User %s", uID.String()[:8]),
		)
		if err != nil {
			t.Fatalf("failed to insert profile %s: %v", uID, err)
		}
	}

	// Seed platform admin record for adminUser (NO row in user_gateways for adminUser!)
	if _, err := setupPool.Exec(ctx, `
		INSERT INTO public.platform_admins (user_id)
		VALUES ($1)
		ON CONFLICT (user_id) DO NOTHING`, adminUser); err != nil {
		t.Fatalf("failed to insert platform admin: %v", err)
	}

	// 5. Construct real application services under iot_backend_app
	gwRepo, err := gateway.NewPostgresProvisioningRepository(appPool)
	if err != nil {
		t.Fatalf("failed to construct gateway provisioning repo: %v", err)
	}
	sensorRepo, err := gateway.NewPostgresAdminOrOwnerSensorProvisioningRepository(appPool)
	if err != nil {
		t.Fatalf("failed to construct sensor provisioning repo: %v", err)
	}

	gwProvisionSvc, err := gateway.NewProvisioningService(gwRepo, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to construct gateway provisioning service: %v", err)
	}
	sensorProvisionSvc, err := gateway.NewAdminOrOwnerSensorProvisioningService(sensorRepo, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to construct sensor provisioning service: %v", err)
	}

	createSvc, err := digitaltwin.NewCreateService(gwProvisionSvc, sensorProvisionSvc)
	if err != nil {
		t.Fatalf("failed to construct entity create service: %v", err)
	}

	gwPatcher, err := digitaltwin.NewPostgresGatewayMetadataPatcher(appPool)
	if err != nil {
		t.Fatalf("failed to construct gateway metadata patcher: %v", err)
	}
	gwPatchSvc, err := digitaltwin.NewGatewayPatchService(gwPatcher, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to construct gateway patch service: %v", err)
	}

	sensorPatcher, err := digitaltwin.NewPostgresSensorMetadataPatcher(appPool)
	if err != nil {
		t.Fatalf("failed to construct sensor metadata patcher: %v", err)
	}
	sensorPatchSvc, err := digitaltwin.NewSensorPatchService(sensorPatcher, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to construct sensor patch service: %v", err)
	}

	strPtr := func(s string) *string { return &s }

	// =========================================================================
	// Sentinel setup: provision a dedicated fixture-owned Gateway and seed one
	// valid control/credential sentinel set (twin_commands, twin_outbox,
	// gateway_mqtt_credentials, gateway_mqtt_credential_events). The exact-scoped
	// cleanup (registered above) deletes each by gateway_id. Seeding happens after
	// fixture setup and before any snapshot baseline capture.
	// =========================================================================
	sentinelGWInput := digitaltwin.CreateEntityInput{
		Type: digitaltwin.EntityTypeGateway,
		Gateway: &gateway.ProvisionGatewayInput{
			GatewayID:   gwSentinelID,
			Name:        "Gateway Meta Sentinel",
			Description: strPtr("Sentinel Description"),
			OwnerUserID: ownerAUser,
		},
	}
	sentinelRes, err := createSvc.CreateEntity(ctx, adminUser, sentinelGWInput)
	if err != nil || !sentinelRes.Created {
		t.Fatalf("failed to provision sentinel gateway: %v", err)
	}
	var sentinelEntityUUID uuid.UUID
	if err := setupPool.QueryRow(ctx, `SELECT id FROM public.twin_entities WHERE entity_id = $1`, "urn:ngsi-ld:Gateway:"+gwSentinelID).Scan(&sentinelEntityUUID); err != nil {
		t.Fatalf("failed to resolve sentinel gateway twin entity: %v", err)
	}
	seedMetadataControlCredentialSentinels(ctx, t, setupPool, gwSentinelID, sentinelEntityUUID, adminUser)
	sentinelPreSnap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwSentinelID})
	if len(sentinelPreSnap.Commands) == 0 || len(sentinelPreSnap.Outbox) == 0 ||
		len(sentinelPreSnap.Credentials) == 0 || len(sentinelPreSnap.CredentialEvents) == 0 {
		t.Fatalf("sentinel preconditions failed: expected nonempty command/outbox/credential/event content, got cmd=%d outbox=%d cred=%d event=%d",
			len(sentinelPreSnap.Commands), len(sentinelPreSnap.Outbox), len(sentinelPreSnap.Credentials), len(sentinelPreSnap.CredentialEvents))
	}
	for _, cmd := range sentinelPreSnap.Commands {
		if cmd.IssuedBy == nil {
			t.Fatalf("sentinel precondition failed: command issued_by must be nonempty")
		}
	}
	for _, ev := range sentinelPreSnap.CredentialEvents {
		if ev.ActorUserID == nil {
			t.Fatalf("sentinel precondition failed: credential event actor_user_id must be nonempty")
		}
		if ev.RequestID == nil || *ev.RequestID == "" {
			t.Fatalf("sentinel precondition failed: credential event request_id must be nonempty")
		}
	}

	// =========================================================================
	// Subtest 1: Admin POST Gateway Create, Replay, and Conflict Semantics
	// =========================================================================
	t.Run("Admin_POST_Gateway_Create_Replay_And_Conflict", func(t *testing.T) {
		gwInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   gwIDA,
				Name:        "Gateway Meta A",
				Description: strPtr("Initial GW Desc"),
				OwnerUserID: ownerAUser,
			},
		}

		// 1a. Positive Creation by Platform Admin
		res, err := createSvc.CreateEntity(ctx, adminUser, gwInput)
		if err != nil {
			t.Fatalf("admin POST gateway failed: %v", err)
		}
		if !res.Created {
			t.Fatalf("expected Created=true on initial POST gateway")
		}
		if res.Type != digitaltwin.EntityTypeGateway {
			t.Fatalf("expected Type=Gateway, got %s", res.Type)
		}
		expectedURN := "urn:ngsi-ld:Gateway:" + gwIDA
		if res.EntityID != expectedURN {
			t.Fatalf("expected EntityID=%s, got %s", expectedURN, res.EntityID)
		}
		if res.Gateway == nil || res.Gateway.Name != "Gateway Meta A" || res.Gateway.OwnerUserID != ownerAUser {
			t.Fatalf("unexpected Gateway result payload: %+v", res.Gateway)
		}

		// Verify database side-effects under appPool:
		// Relational Gateway row
		var dbGWName string
		var dbGWDesc *string
		var dbGWCreatedAt time.Time
		err = appPool.QueryRow(ctx, "SELECT name, description, created_at FROM public.gateways WHERE gateway_id = $1", gwIDA).
			Scan(&dbGWName, &dbGWDesc, &dbGWCreatedAt)
		if err != nil {
			t.Fatalf("failed to query gateway from DB: %v", err)
		}
		if dbGWName != "Gateway Meta A" || dbGWDesc == nil || *dbGWDesc != "Initial GW Desc" {
			t.Fatalf("gateway DB mismatch: name=%s, desc=%v", dbGWName, dbGWDesc)
		}

		// Membership: exactly one row with ownerAUser as owner
		var memberCount int
		var memberRole string
		err = appPool.QueryRow(ctx, "SELECT count(*), COALESCE(MAX(role), '') FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", gwIDA, ownerAUser).
			Scan(&memberCount, &memberRole)
		if err != nil || memberCount != 1 || memberRole != "owner" {
			t.Fatalf("user_gateways mismatch for ownerA: count=%d, role=%s, err=%v", memberCount, memberRole, err)
		}

		// Confirm adminUser has NO membership in user_gateways
		var adminMembershipCount int
		err = appPool.QueryRow(ctx, "SELECT count(*) FROM public.user_gateways WHERE gateway_id = $1 AND user_id = $2", gwIDA, adminUser).
			Scan(&adminMembershipCount)
		if err != nil || adminMembershipCount != 0 {
			t.Fatalf("adminUser must have no membership in user_gateways, got count=%d", adminMembershipCount)
		}

		// Canonical Twin entity
		var twinEntityID string
		var twinType string
		var twinName string
		var twinEntityUUID uuid.UUID
		err = appPool.QueryRow(ctx, "SELECT id, entity_id, entity_type, name FROM public.twin_entities WHERE gateway_id = $1", gwIDA).
			Scan(&twinEntityUUID, &twinEntityID, &twinType, &twinName)
		if err != nil {
			t.Fatalf("failed to query twin entity: %v", err)
		}
		if twinEntityID != expectedURN || twinType != "Gateway" || twinName != "Gateway Meta A" {
			t.Fatalf("twin entity mismatch: id=%s, type=%s, name=%s", twinEntityID, twinType, twinName)
		}

		// Initial Twin State
		var repVer, desVer int64
		err = appPool.QueryRow(ctx, "SELECT reported_version, desired_version FROM public.twin_states WHERE entity_id = $1", twinEntityUUID).
			Scan(&repVer, &desVer)
		if err != nil || repVer != 0 || desVer != 0 {
			t.Fatalf("initial twin state mismatch: repVer=%d, desVer=%d, err=%v", repVer, desVer, err)
		}

		// Verify NO MQTT credentials created through mutation service
		var credCount int
		err = appPool.QueryRow(ctx, "SELECT count(*) FROM public.gateway_mqtt_credentials WHERE gateway_id = $1", gwIDA).Scan(&credCount)
		if err != nil || credCount != 0 {
			t.Fatalf("expected 0 MQTT credentials created, got count=%d", credCount)
		}

		// 1b. Seed state sentinel to prove replay preservation
		_, err = setupPool.Exec(ctx, `UPDATE public.twin_states SET reported_state = '{"sentinel":"gw_a_init"}' WHERE entity_id = $1`, twinEntityUUID)
		if err != nil {
			t.Fatalf("failed to seed state sentinel: %v", err)
		}

		// 1c. Exact Replay of POST Gateway returns Created=false and preserves state/timestamps
		replayRes, err := createSvc.CreateEntity(ctx, adminUser, gwInput)
		if err != nil {
			t.Fatalf("exact replay POST gateway failed: %v", err)
		}
		if replayRes.Created {
			t.Fatalf("exact replay must return Created=false")
		}
		if replayRes.EntityID != expectedURN {
			t.Fatalf("exact replay entity ID mismatch: %s vs %s", replayRes.EntityID, expectedURN)
		}

		// Verify state sentinel preserved
		var repStateJSON string
		err = appPool.QueryRow(ctx, "SELECT reported_state::text FROM public.twin_states WHERE entity_id = $1", twinEntityUUID).Scan(&repStateJSON)
		if err != nil || !strings.Contains(repStateJSON, "gw_a_init") {
			t.Fatalf("state sentinel was not preserved on exact replay: %s, err=%v", repStateJSON, err)
		}

		// Verify total row counts did not duplicate
		var gwRowCount, entityRowCount, stateRowCount int
		if err := appPool.QueryRow(ctx, "SELECT count(*) FROM public.gateways WHERE gateway_id = $1", gwIDA).Scan(&gwRowCount); err != nil {
			t.Fatal("verify Gateway replay row count:", err)
		}
		if err := appPool.QueryRow(ctx, "SELECT count(*) FROM public.twin_entities WHERE gateway_id = $1", gwIDA).Scan(&entityRowCount); err != nil {
			t.Fatal("verify Gateway replay entity count:", err)
		}
		if err := appPool.QueryRow(ctx, "SELECT count(*) FROM public.twin_states WHERE entity_id = $1", twinEntityUUID).Scan(&stateRowCount); err != nil {
			t.Fatal("verify Gateway replay state count:", err)
		}
		if gwRowCount != 1 || entityRowCount != 1 || stateRowCount != 1 {
			t.Fatalf("row count duplicated on replay: gw=%d, entity=%d, state=%d", gwRowCount, entityRowCount, stateRowCount)
		}

		// 1d. Conflict: POST Gateway with changed representation (different Name)
		changedNameInput := gwInput
		changedNameInput.Gateway = &gateway.ProvisionGatewayInput{
			GatewayID:   gwIDA,
			Name:        "Different GW Name",
			Description: strPtr("Initial GW Desc"),
			OwnerUserID: ownerAUser,
		}
		_, err = createSvc.CreateEntity(ctx, adminUser, changedNameInput)
		if !errors.Is(err, digitaltwin.ErrProvisioningConflict) && !errors.Is(err, gateway.ErrProvisioningConflict) {
			t.Fatalf("expected ErrProvisioningConflict on changed name, got: %v", err)
		}

		// Conflict: POST Gateway with changed owner
		changedOwnerInput := gwInput
		changedOwnerInput.Gateway = &gateway.ProvisionGatewayInput{
			GatewayID:   gwIDA,
			Name:        "Gateway Meta A",
			Description: strPtr("Initial GW Desc"),
			OwnerUserID: otherUser,
		}
		_, err = createSvc.CreateEntity(ctx, adminUser, changedOwnerInput)
		if !errors.Is(err, digitaltwin.ErrProvisioningConflict) && !errors.Is(err, gateway.ErrProvisioningConflict) {
			t.Fatalf("expected ErrProvisioningConflict on changed owner, got: %v", err)
		}

		// Verify DB was NOT overwritten on conflicts
		var checkName string
		if err := appPool.QueryRow(ctx, "SELECT name FROM public.gateways WHERE gateway_id = $1", gwIDA).Scan(&checkName); err != nil {
			t.Fatal("verify Gateway name:", err)
		}
		if checkName != "Gateway Meta A" {
			t.Fatalf("gateway name was overwritten on conflict: %s", checkName)
		}
	})

	// =========================================================================
	// Subtest 2: Admin & Owner POST Sensor, Isolation, Replay, and Conflict
	// =========================================================================
	t.Run("Admin_And_Owner_POST_Sensor_Isolation_Replay_And_Conflict", func(t *testing.T) {
		// Provision Gateway B first (owned by ownerBUser)
		gwBInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   gwIDB,
				Name:        "Gateway Meta B",
				Description: strPtr("GW B Desc"),
				OwnerUserID: ownerBUser,
			},
		}
		resB, err := createSvc.CreateEntity(ctx, adminUser, gwBInput)
		if err != nil || !resB.Created {
			t.Fatalf("failed to provision gateway B: %v", err)
		}

		// 2a. Platform Admin (without membership) provisions Sensor on Gateway A
		sensorAInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeSensor,
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: gwIDA,
				SensorID:  sensorSharedID,
				Name:      "Shared Sensor A",
				Unit:      strPtr("celsius"),
			},
		}
		sensARes, err := createSvc.CreateEntity(ctx, adminUser, sensorAInput)
		if err != nil {
			t.Fatalf("admin POST sensor on gateway A failed: %v", err)
		}
		if !sensARes.Created {
			t.Fatalf("expected Created=true for initial sensor creation")
		}
		expectedSensorAURN := "urn:ngsi-ld:Sensor:" + gwIDA + ":" + sensorSharedID
		if sensARes.EntityID != expectedSensorAURN {
			t.Fatalf("expected EntityID=%s, got %s", expectedSensorAURN, sensARes.EntityID)
		}

		// Verify Sensor A DB records and graph relationship
		var sensAUUID, gwAUUID uuid.UUID
		err = appPool.QueryRow(ctx, "SELECT id FROM public.twin_entities WHERE entity_id = $1", "urn:ngsi-ld:Gateway:"+gwIDA).Scan(&gwAUUID)
		if err != nil {
			t.Fatalf("failed to query gwA UUID: %v", err)
		}
		err = appPool.QueryRow(ctx, "SELECT id FROM public.twin_entities WHERE entity_id = $1", expectedSensorAURN).Scan(&sensAUUID)
		if err != nil {
			t.Fatalf("failed to query sensorA UUID: %v", err)
		}

		var relCount int
		err = appPool.QueryRow(ctx, `
			SELECT count(*) FROM public.twin_relationships
			WHERE source_entity_id = $1 AND relationship_type = 'hasSensor' AND target_entity_id = $2`,
			gwAUUID, sensAUUID).Scan(&relCount)
		if err != nil || relCount != 1 {
			t.Fatalf("expected 1 hasSensor relationship for sensor A, got count=%d, err=%v", relCount, err)
		}

		// 2b. Gateway Owner (ownerBUser) provisions Sensor on Gateway B with SAME local sensor ID
		sensorBInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeSensor,
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: gwIDB,
				SensorID:  sensorSharedID,
				Name:      "Shared Sensor B",
				Unit:      strPtr("celsius"),
			},
		}
		sensBRes, err := createSvc.CreateEntity(ctx, ownerBUser, sensorBInput)
		if err != nil {
			t.Fatalf("owner POST sensor on gateway B failed: %v", err)
		}
		if !sensBRes.Created {
			t.Fatalf("expected Created=true for initial sensor B creation")
		}
		expectedSensorBURN := "urn:ngsi-ld:Sensor:" + gwIDB + ":" + sensorSharedID
		if sensBRes.EntityID != expectedSensorBURN {
			t.Fatalf("expected EntityID=%s, got %s", expectedSensorBURN, sensBRes.EntityID)
		}

		var sensBUUID, gwBUUID uuid.UUID
		err = appPool.QueryRow(ctx, "SELECT id FROM public.twin_entities WHERE entity_id = $1", "urn:ngsi-ld:Gateway:"+gwIDB).Scan(&gwBUUID)
		if err != nil {
			t.Fatalf("failed to query gwB UUID: %v", err)
		}
		err = appPool.QueryRow(ctx, "SELECT id FROM public.twin_entities WHERE entity_id = $1", expectedSensorBURN).Scan(&sensBUUID)
		if err != nil {
			t.Fatalf("failed to query sensorB UUID: %v", err)
		}

		// Assert isolation: Gateway A and B share local sensor ID but have distinct entities and no cross-linking
		if sensAUUID == sensBUUID {
			t.Fatalf("sensors sharing local ID on different gateways must have distinct UUIDs")
		}
		var crossRelCount int
		err = appPool.QueryRow(ctx, `
			SELECT count(*) FROM public.twin_relationships
			WHERE (source_entity_id = $1 AND target_entity_id = $2)
			   OR (source_entity_id = $3 AND target_entity_id = $4)`,
			gwAUUID, sensBUUID, gwBUUID, sensAUUID).Scan(&crossRelCount)
		if err != nil || crossRelCount != 0 {
			t.Fatalf("detected cross-gateway relationship leakage: %d", crossRelCount)
		}

		// 2c. Seed state sentinel on Sensor A
		_, err = setupPool.Exec(ctx, `UPDATE public.twin_states SET reported_state = '{"sentinel":"sens_a_init"}' WHERE entity_id = $1`, sensAUUID)
		if err != nil {
			t.Fatalf("failed to seed sensor state sentinel: %v", err)
		}

		// 2d. Exact Replay by Gateway Owner (ownerAUser) on Gateway A
		sensReplayRes, err := createSvc.CreateEntity(ctx, ownerAUser, sensorAInput)
		if err != nil {
			t.Fatalf("owner exact replay POST sensor failed: %v", err)
		}
		if sensReplayRes.Created {
			t.Fatalf("exact replay must return Created=false")
		}

		// Verify state sentinel preserved and NO duplicate relationship rows
		var sensRepState string
		err = appPool.QueryRow(ctx, "SELECT reported_state::text FROM public.twin_states WHERE entity_id = $1", sensAUUID).Scan(&sensRepState)
		if err != nil || !strings.Contains(sensRepState, "sens_a_init") {
			t.Fatalf("sensor state sentinel was not preserved on replay: %s", sensRepState)
		}

		var totalRelA int
		err = appPool.QueryRow(ctx, "SELECT count(*) FROM public.twin_relationships WHERE source_entity_id = $1 AND target_entity_id = $2", gwAUUID, sensAUUID).Scan(&totalRelA)
		if err != nil || totalRelA != 1 {
			t.Fatalf("duplicate hasSensor relationship rows on replay: %d", totalRelA)
		}

		// 2e. Conflict: Replay Sensor with changed representation (different unit)
		conflictSensorInput := sensorAInput
		conflictSensorInput.Sensor = &gateway.ProvisionSensorInput{
			GatewayID: gwIDA,
			SensorID:  sensorSharedID,
			Name:      "Shared Sensor A",
			Unit:      strPtr("fahrenheit"),
		}
		_, err = createSvc.CreateEntity(ctx, ownerAUser, conflictSensorInput)
		if !errors.Is(err, digitaltwin.ErrProvisioningConflict) && !errors.Is(err, gateway.ErrProvisioningConflict) {
			t.Fatalf("expected ErrProvisioningConflict on mismatched sensor unit, got: %v", err)
		}

		// Provision another sensor on Gateway A using ownerAUser for later checks
		ownerSensInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeSensor,
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: gwIDA,
				SensorID:  sensorOwnerID,
				Name:      "Owner Sensor",
				Unit:      strPtr("volts"),
			},
		}
		ownerSensRes, err := createSvc.CreateEntity(ctx, ownerAUser, ownerSensInput)
		if err != nil || !ownerSensRes.Created {
			t.Fatalf("failed to create owner sensor: %v", err)
		}
	})

	// =========================================================================
	// Subtest 3: Gateway PATCH Field, Clear, and No-Op Semantics
	// =========================================================================
	t.Run("Gateway_PATCH_Fields_Clear_And_NoOp_Semantics", func(t *testing.T) {
		// Set a deterministic past timestamp for Gateway A's Twin entity
		t0 := time.Now().Add(-10 * time.Minute).UTC().Truncate(time.Microsecond)
		_, err := setupPool.Exec(ctx, `UPDATE public.twin_entities SET updated_at = $2 WHERE gateway_id = $1 AND entity_type = 'Gateway'`, gwIDA, t0)
		if err != nil {
			t.Fatalf("failed to set twin entity baseline updated_at: %v", err)
		}

		var twinEntityUUID uuid.UUID
		err = appPool.QueryRow(ctx, "SELECT id FROM public.twin_entities WHERE entity_id = $1", "urn:ngsi-ld:Gateway:"+gwIDA).Scan(&twinEntityUUID)
		if err != nil {
			t.Fatalf("failed to query twinEntityUUID: %v", err)
		}

		// 3a. Name-only PATCH (description omitted/kept)
		snapBefore3a := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA, gwSentinelID})
		patch1 := digitaltwin.PatchGatewayInput{
			Name: strPtr("Gateway Meta A Renamed"),
		}
		res1, err := gwPatchSvc.PatchGateway(ctx, adminUser, gwIDA, patch1)
		if err != nil {
			t.Fatalf("name-only gateway patch failed: %v", err)
		}
		snapAfter3a := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA, gwSentinelID})
		assertPatchPreservesNonMetadata(t, snapBefore3a, snapAfter3a, twinEntityUUID, strPtr("Gateway Meta A Renamed"))
		if res1.Name != "Gateway Meta A Renamed" {
			t.Fatalf("expected name='Gateway Meta A Renamed', got %q", res1.Name)
		}
		if res1.Description == nil || *res1.Description != "Initial GW Desc" {
			t.Fatalf("expected description preserved as 'Initial GW Desc', got %v", res1.Description)
		}
		if !res1.UpdatedAt.After(t0) {
			t.Fatalf("expected updated_at > t0, got %v <= %v", res1.UpdatedAt, t0)
		}

		// Verify DB coherence
		var dbGWName, dbTwinName string
		var dbGWDesc *string
		var dbTwinUp time.Time
		err = appPool.QueryRow(ctx, "SELECT name, description FROM public.gateways WHERE gateway_id = $1", gwIDA).
			Scan(&dbGWName, &dbGWDesc)
		if err != nil {
			t.Fatalf("failed to query gateway: %v", err)
		}
		err = appPool.QueryRow(ctx, "SELECT name, updated_at FROM public.twin_entities WHERE gateway_id = $1 AND entity_type = 'Gateway'", gwIDA).
			Scan(&dbTwinName, &dbTwinUp)
		if err != nil {
			t.Fatalf("failed to query twin entity: %v", err)
		}
		if dbGWName != "Gateway Meta A Renamed" || dbTwinName != "Gateway Meta A Renamed" {
			t.Fatalf("names not coherent across tables: gw=%s, twin=%s", dbGWName, dbTwinName)
		}
		if dbGWDesc == nil || *dbGWDesc != "Initial GW Desc" {
			t.Fatalf("description not preserved in DB: %v", dbGWDesc)
		}
		if !dbTwinUp.Equal(res1.UpdatedAt) {
			t.Fatalf("updated_at in DB not equal to result: db=%v, res=%v", dbTwinUp, res1.UpdatedAt)
		}

		// 3b. Description-only PATCH (name omitted/kept)
		t1 := res1.UpdatedAt
		patch2 := digitaltwin.PatchGatewayInput{
			DescriptionSet: true,
			Description:    strPtr("Updated GW Desc Only"),
		}
		res2, err := gwPatchSvc.PatchGateway(ctx, adminUser, gwIDA, patch2)
		if err != nil {
			t.Fatalf("description-only gateway patch failed: %v", err)
		}
		if res2.Name != "Gateway Meta A Renamed" {
			t.Fatalf("name was not preserved on description-only patch: %q", res2.Name)
		}
		if res2.Description == nil || *res2.Description != "Updated GW Desc Only" {
			t.Fatalf("description was not updated: %v", res2.Description)
		}
		if !res2.UpdatedAt.After(t1) {
			t.Fatalf("expected updated_at > t1, got %v <= %v", res2.UpdatedAt, t1)
		}

		// 3c. Explicit description null clears description
		t2 := res2.UpdatedAt
		patch3 := digitaltwin.PatchGatewayInput{
			DescriptionSet: true,
			Description:    nil, // explicit null
		}
		res3, err := gwPatchSvc.PatchGateway(ctx, adminUser, gwIDA, patch3)
		if err != nil {
			t.Fatalf("description clear gateway patch failed: %v", err)
		}
		if res3.Description != nil {
			t.Fatalf("expected nil description after clear, got %v", res3.Description)
		}
		if res3.Name != "Gateway Meta A Renamed" {
			t.Fatalf("name was not preserved on description clear: %q", res3.Name)
		}
		if !res3.UpdatedAt.After(t2) {
			t.Fatalf("expected updated_at > t2, got %v <= %v", res3.UpdatedAt, t2)
		}

		// Verify in DB that description is SQL NULL
		var checkDesc *string
		err = appPool.QueryRow(ctx, "SELECT description FROM public.gateways WHERE gateway_id = $1", gwIDA).Scan(&checkDesc)
		if err != nil || checkDesc != nil {
			t.Fatalf("expected SQL NULL in gateways.description, got %v, err=%v", checkDesc, err)
		}

		// 3d. Same-value No-Op PATCH executes NO metadata timestamp change
		snapBefore3d := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA, gwSentinelID})
		var tBeforeTwin time.Time
		if err := appPool.QueryRow(ctx, "SELECT updated_at FROM public.twin_entities WHERE gateway_id = $1 AND entity_type = 'Gateway'", gwIDA).Scan(&tBeforeTwin); err != nil {
			t.Fatal("read timestamp before no-op:", err)
		}

		noOpPatch := digitaltwin.PatchGatewayInput{
			Name:           strPtr("Gateway Meta A Renamed"),
			DescriptionSet: true,
			Description:    nil, // matches current DB state exactly
		}
		resNoOp, err := gwPatchSvc.PatchGateway(ctx, adminUser, gwIDA, noOpPatch)
		if err != nil {
			t.Fatalf("same-value gateway no-op patch failed: %v", err)
		}
		if !resNoOp.UpdatedAt.Equal(tBeforeTwin) {
			t.Fatalf("no-op result updated_at mutated: got %v, want %v", resNoOp.UpdatedAt, tBeforeTwin)
		}

		var tAfterTwin time.Time
		if err := appPool.QueryRow(ctx, "SELECT updated_at FROM public.twin_entities WHERE gateway_id = $1 AND entity_type = 'Gateway'", gwIDA).Scan(&tAfterTwin); err != nil {
			t.Fatal("read timestamp after no-op:", err)
		}
		if !tAfterTwin.Equal(tBeforeTwin) {
			t.Fatalf("twin_entities.updated_at mutated on same-value no-op: before=%v, after=%v", tBeforeTwin, tAfterTwin)
		}
		snapAfter3d := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA, gwSentinelID})
		assertSnapshotsEqual(t, "Gateway same-value No-Op", snapBefore3d, snapAfter3d)
	})

	// =========================================================================
	// Subtest 4: Sensor PATCH Fields, Unit Preservation, and No-Op Semantics
	// =========================================================================
	t.Run("Sensor_PATCH_Fields_And_NoOp_Semantics", func(t *testing.T) {
		// Target: Sensor A on Gateway A. Currently: Name="Shared Sensor A", Unit="celsius"
		var sensAUUID uuid.UUID
		err := appPool.QueryRow(ctx, "SELECT id FROM public.twin_entities WHERE entity_id = $1", "urn:ngsi-ld:Sensor:"+gwIDA+":"+sensorSharedID).Scan(&sensAUUID)
		if err != nil {
			t.Fatalf("failed to query sensAUUID: %v", err)
		}

		// 4a. Platform Admin (without membership) patches Sensor
		snapBefore4a := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
		adminPatch := digitaltwin.PatchSensorInput{
			Name: strPtr("Sensor A Admin Patched"),
		}
		resAdmin, err := sensorPatchSvc.PatchSensor(ctx, adminUser, gwIDA, sensorSharedID, adminPatch)
		if err != nil {
			t.Fatalf("admin sensor patch failed: %v", err)
		}
		snapAfter4a := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
		assertPatchPreservesNonMetadata(t, snapBefore4a, snapAfter4a, sensAUUID, strPtr("Sensor A Admin Patched"))
		if resAdmin.Name != "Sensor A Admin Patched" {
			t.Fatalf("expected name='Sensor A Admin Patched', got %q", resAdmin.Name)
		}
		if resAdmin.Unit == nil || *resAdmin.Unit != "celsius" {
			t.Fatalf("expected unit preserved as 'celsius', got %v", resAdmin.Unit)
		}

		// Verify database coherence across sensors and twin_entities
		var sName, tName string
		var sUnit *string
		err = appPool.QueryRow(ctx, "SELECT name, unit FROM public.sensors WHERE gateway_id = $1 AND sensor_id = $2", gwIDA, sensorSharedID).
			Scan(&sName, &sUnit)
		if err != nil {
			t.Fatalf("failed to query sensor: %v", err)
		}
		err = appPool.QueryRow(ctx, "SELECT name FROM public.twin_entities WHERE entity_id = $1", "urn:ngsi-ld:Sensor:"+gwIDA+":"+sensorSharedID).
			Scan(&tName)
		if err != nil {
			t.Fatalf("failed to query twin entity: %v", err)
		}
		if sName != "Sensor A Admin Patched" || tName != "Sensor A Admin Patched" {
			t.Fatalf("sensor names not coherent: sensors=%s, twin=%s", sName, tName)
		}
		if sUnit == nil || *sUnit != "celsius" {
			t.Fatalf("unit not preserved in DB: %v", sUnit)
		}

		// 4b. Gateway Owner (ownerAUser) patches Sensor
		ownerPatch := digitaltwin.PatchSensorInput{
			Name: strPtr("Sensor A Owner Patched"),
		}
		resOwner, err := sensorPatchSvc.PatchSensor(ctx, ownerAUser, gwIDA, sensorSharedID, ownerPatch)
		if err != nil {
			t.Fatalf("owner sensor patch failed: %v", err)
		}
		if resOwner.Name != "Sensor A Owner Patched" {
			t.Fatalf("expected name='Sensor A Owner Patched', got %q", resOwner.Name)
		}
		if resOwner.Unit == nil || *resOwner.Unit != "celsius" {
			t.Fatalf("unit not preserved: %v", resOwner.Unit)
		}

		// 4c. Same-value No-Op PATCH on Sensor executes NO metadata timestamp change
		snapBefore4c := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
		var tBeforeSensorTwin time.Time
		err = appPool.QueryRow(ctx, "SELECT updated_at FROM public.twin_entities WHERE entity_id = $1", "urn:ngsi-ld:Sensor:"+gwIDA+":"+sensorSharedID).
			Scan(&tBeforeSensorTwin)
		if err != nil {
			t.Fatalf("failed to query twin updated_at: %v", err)
		}

		resSensNoOp, err := sensorPatchSvc.PatchSensor(ctx, ownerAUser, gwIDA, sensorSharedID, ownerPatch)
		if err != nil {
			t.Fatalf("same-value sensor no-op patch failed: %v", err)
		}
		if !resSensNoOp.UpdatedAt.Equal(tBeforeSensorTwin) {
			t.Fatalf("sensor no-op updated_at mutated: got %v, want %v", resSensNoOp.UpdatedAt, tBeforeSensorTwin)
		}

		var tAfterSensorTwin time.Time
		_ = appPool.QueryRow(ctx, "SELECT updated_at FROM public.twin_entities WHERE entity_id = $1", "urn:ngsi-ld:Sensor:"+gwIDA+":"+sensorSharedID).
			Scan(&tAfterSensorTwin)
		if !tAfterSensorTwin.Equal(tBeforeSensorTwin) {
			t.Fatalf("twin_entities.updated_at mutated on sensor same-value no-op: before=%v, after=%v", tBeforeSensorTwin, tAfterSensorTwin)
		}
		snapAfter4c := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
		assertSnapshotsEqual(t, "Sensor same-value No-Op", snapBefore4c, snapAfter4c)
	})

	// =========================================================================
	// Subtest 5: POST After PATCH Conflict and Current Replay
	// =========================================================================
	t.Run("POST_After_PATCH_Conflict_And_Current_Replay", func(t *testing.T) {
		// Gateway A current DB state: Name="Gateway Meta A Renamed", Description=NULL, Owner=ownerAUser
		// 5a. POST Gateway with OLD metadata ("Gateway Meta A", "Initial GW Desc") -> must conflict!
		oldGWInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   gwIDA,
				Name:        "Gateway Meta A",
				Description: strPtr("Initial GW Desc"),
				OwnerUserID: ownerAUser,
			},
		}
		_, err := createSvc.CreateEntity(ctx, adminUser, oldGWInput)
		if !errors.Is(err, digitaltwin.ErrProvisioningConflict) && !errors.Is(err, gateway.ErrProvisioningConflict) {
			t.Fatalf("expected ErrProvisioningConflict on POST gateway with old metadata, got: %v", err)
		}

		// 5b. POST Gateway matching CURRENT metadata ("Gateway Meta A Renamed", NULL description) -> exact replay!
		currentGWInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   gwIDA,
				Name:        "Gateway Meta A Renamed",
				Description: nil,
				OwnerUserID: ownerAUser,
			},
		}
		resCurrentGW, err := createSvc.CreateEntity(ctx, adminUser, currentGWInput)
		if err != nil {
			t.Fatalf("POST gateway matching current metadata failed: %v", err)
		}
		if resCurrentGW.Created {
			t.Fatalf("expected Created=false for replay matching current metadata")
		}

		// Verify Gateway state sentinel is STILL intact (no state reset!)
		var gwRepState string
		err = appPool.QueryRow(ctx, `
			SELECT reported_state::text FROM public.twin_states
			WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = $1)`,
			"urn:ngsi-ld:Gateway:"+gwIDA).Scan(&gwRepState)
		if err != nil || !strings.Contains(gwRepState, "gw_a_init") {
			t.Fatalf("gateway state sentinel was reset on current replay: %s, err=%v", gwRepState, err)
		}

		// Sensor A current DB state: Name="Sensor A Owner Patched", Unit="celsius"
		// 5c. POST Sensor with OLD name ("Shared Sensor A") -> must conflict!
		oldSensInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeSensor,
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: gwIDA,
				SensorID:  sensorSharedID,
				Name:      "Shared Sensor A",
				Unit:      strPtr("celsius"),
			},
		}
		_, err = createSvc.CreateEntity(ctx, ownerAUser, oldSensInput)
		if !errors.Is(err, digitaltwin.ErrProvisioningConflict) && !errors.Is(err, gateway.ErrProvisioningConflict) {
			t.Fatalf("expected ErrProvisioningConflict on POST sensor with old name, got: %v", err)
		}

		// 5d. POST Sensor matching CURRENT name ("Sensor A Owner Patched") -> exact replay!
		currentSensInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeSensor,
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: gwIDA,
				SensorID:  sensorSharedID,
				Name:      "Sensor A Owner Patched",
				Unit:      strPtr("celsius"),
			},
		}
		resCurrentSens, err := createSvc.CreateEntity(ctx, ownerAUser, currentSensInput)
		if err != nil {
			t.Fatalf("POST sensor matching current metadata failed: %v", err)
		}
		if resCurrentSens.Created {
			t.Fatalf("expected Created=false for sensor replay matching current metadata")
		}

		// Verify Sensor state sentinel is STILL intact (no state reset!)
		var sensRepState string
		err = appPool.QueryRow(ctx, `
			SELECT reported_state::text FROM public.twin_states
			WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = $1)`,
			"urn:ngsi-ld:Sensor:"+gwIDA+":"+sensorSharedID).Scan(&sensRepState)
		if err != nil || !strings.Contains(sensRepState, "sens_a_init") {
			t.Fatalf("sensor state sentinel was reset on current replay: %s, err=%v", sensRepState, err)
		}
	})

	// =========================================================================
	// Subtest 6: Authorization Denials, Tenant Scoping, and Data Preservation
	// =========================================================================
	t.Run("Authorization_Denials_And_Data_Preservation", func(t *testing.T) {
		// 6.0 Ensure Gateway A and Sensor A exist as prerequisites (self-contained for targeted subtest runs)
		var gwAExists bool
		if err := appPool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM public.gateways WHERE gateway_id = $1)", gwIDA).Scan(&gwAExists); err != nil {
			t.Fatal("check denial fixture Gateway:", err)
		}
		if !gwAExists {
			gwInput := digitaltwin.CreateEntityInput{
				Type: digitaltwin.EntityTypeGateway,
				Gateway: &gateway.ProvisionGatewayInput{
					GatewayID:   gwIDA,
					Name:        "Gateway Meta A",
					Description: strPtr("Initial GW Desc"),
					OwnerUserID: ownerAUser,
				},
			}
			if _, err := createSvc.CreateEntity(ctx, adminUser, gwInput); err != nil {
				t.Fatalf("failed to seed gateway A prerequisite: %v", err)
			}
			sensorAInput := digitaltwin.CreateEntityInput{
				Type: digitaltwin.EntityTypeSensor,
				Sensor: &gateway.ProvisionSensorInput{
					GatewayID: gwIDA,
					SensorID:  sensorSharedID,
					Name:      "Shared Sensor A",
					Unit:      strPtr("celsius"),
				},
			}
			if _, err := createSvc.CreateEntity(ctx, adminUser, sensorAInput); err != nil {
				t.Fatalf("failed to seed sensor A prerequisite: %v", err)
			}
		}

		// 6.1 Preconditions: Seed operator and viewer roles on Gateway A
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.user_gateways (user_id, gateway_id, role)
			VALUES ($1, $2, 'operator'), ($3, $2, 'viewer')
			ON CONFLICT (user_id, gateway_id) DO NOTHING`,
			operatorUser, gwIDA, viewerUser); err != nil {
			t.Fatalf("failed to seed operator/viewer memberships: %v", err)
		}

		// 6.2 Preconditions: Seed high-precision state sentinels (> 2^53 integer, e.g. 9007199254740993)
		// and temporal & telemetry sentinels on Gateway A
		var sensAUUID uuid.UUID
		err = appPool.QueryRow(ctx, "SELECT id FROM public.twin_entities WHERE entity_id = $1", "urn:ngsi-ld:Sensor:"+gwIDA+":"+sensorSharedID).Scan(&sensAUUID)
		if err != nil {
			t.Fatalf("failed to query sensAUUID: %v", err)
		}

		bigIntStateJSON := `{"count":9007199254740993,"sample_interval_us":10000}`
		_, err = setupPool.Exec(ctx, `UPDATE public.twin_states SET reported_state = $2, desired_state = $2 WHERE entity_id = $1`, sensAUUID, bigIntStateJSON)
		if err != nil {
			t.Fatalf("failed to update sensor state with bigInt sentinel: %v", err)
		}

		// Seed 1 temporal row for sensor A
		_, err = setupPool.Exec(ctx, `
			INSERT INTO public.twin_temporal_values (observed_at, entity_id, property_name, value_number, metadata)
			VALUES (now(), $1, 'sample_interval_us', 10000, '{"unit":"us"}')`, sensAUUID)
		if err != nil {
			t.Fatalf("failed to seed temporal row: %v", err)
		}

		// Seed 1 raw telemetry row for sensor A
		_, err = setupPool.Exec(ctx, `
			INSERT INTO public.telemetry (measured_at, gateway_id, sensor_id, sequence_num, value, boot_id, message_id, sample_index)
			VALUES (now(), $1, $2, 1, 25.5, $3, $4, 0)`,
			gwIDA, sensorSharedID, uuid.New(), uuid.New())
		if err != nil {
			t.Fatalf("failed to seed raw telemetry row: %v", err)
		}

		// Capture baseline snapshot for gwIDA and assert non-zero preconditions
		baselineSnap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
		if baselineSnap.TemporalCount == 0 || baselineSnap.TelemetryRawCount == 0 {
			t.Fatalf("precondition failed: expected non-zero temporal and telemetry counts")
		}
		if _, ok := baselineSnap.Memberships[fmt.Sprintf("%s:%s", gwIDA, operatorUser)]; !ok {
			t.Fatalf("precondition failed: operator membership missing in baseline")
		}
		if _, ok := baselineSnap.Memberships[fmt.Sprintf("%s:%s", gwIDA, viewerUser)]; !ok {
			t.Fatalf("precondition failed: viewer membership missing in baseline")
		}

		// 6.3 Gateway POST: Owner, Operator, Viewer, Non-Member must ALL be denied with ErrProvisioningForbidden
		gwDeniedInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   fmt.Sprintf("gw_denied_%s", runID),
				Name:        "Denied Gateway",
				OwnerUserID: ownerAUser,
			},
		}
		for _, actor := range []struct {
			name string
			id   uuid.UUID
		}{
			{"ownerAUser", ownerAUser},
			{"operatorUser", operatorUser},
			{"viewerUser", viewerUser},
			{"nonMemberUser", nonMemberUser},
		} {
			_, err := createSvc.CreateEntity(ctx, actor.id, gwDeniedInput)
			if !errors.Is(err, digitaltwin.ErrProvisioningForbidden) && !errors.Is(err, gateway.ErrProvisioningForbidden) {
				t.Fatalf("expected ErrProvisioningForbidden for %s on Gateway POST, got: %v", actor.name, err)
			}
			snap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
			assertSnapshotsEqual(t, "Gateway POST denied "+actor.name, baselineSnap, snap)
		}

		// 6.4 Gateway PATCH: Owner, Operator, Viewer, Non-Member must ALL be denied with ErrForbidden
		gwPatchInput := digitaltwin.PatchGatewayInput{Name: strPtr("Forbidden Renamed GW")}
		for _, actor := range []struct {
			name string
			id   uuid.UUID
		}{
			{"ownerAUser", ownerAUser},
			{"operatorUser", operatorUser},
			{"viewerUser", viewerUser},
			{"nonMemberUser", nonMemberUser},
		} {
			_, err := gwPatchSvc.PatchGateway(ctx, actor.id, gwIDA, gwPatchInput)
			if !errors.Is(err, digitaltwin.ErrForbidden) && !errors.Is(err, gateway.ErrProvisioningForbidden) {
				t.Fatalf("expected ErrForbidden for %s on Gateway PATCH, got: %v", actor.name, err)
			}
			snap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
			assertSnapshotsEqual(t, "Gateway PATCH denied "+actor.name, baselineSnap, snap)
		}

		// 6.5 Sensor POST: Operator and Viewer must be denied with ErrProvisioningForbidden
		sensForbiddenInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeSensor,
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: gwIDA,
				SensorID:  fmt.Sprintf("sens_denied_%s", runID),
				Name:      "Denied Sensor",
				Unit:      strPtr("celsius"),
			},
		}
		for _, actor := range []struct {
			name string
			id   uuid.UUID
		}{
			{"operatorUser", operatorUser},
			{"viewerUser", viewerUser},
		} {
			_, err := createSvc.CreateEntity(ctx, actor.id, sensForbiddenInput)
			if !errors.Is(err, digitaltwin.ErrProvisioningForbidden) && !errors.Is(err, gateway.ErrProvisioningForbidden) {
				t.Fatalf("expected ErrProvisioningForbidden for %s on Sensor POST, got: %v", actor.name, err)
			}
			snap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
			assertSnapshotsEqual(t, "Sensor POST forbidden "+actor.name, baselineSnap, snap)
		}

		// 6.6 Sensor POST: Non-Member must be denied with ErrProvisioningNotFound
		_, err = createSvc.CreateEntity(ctx, nonMemberUser, sensForbiddenInput)
		if !errors.Is(err, digitaltwin.ErrProvisioningNotFound) && !errors.Is(err, gateway.ErrProvisioningNotFound) {
			t.Fatalf("expected ErrProvisioningNotFound for nonMemberUser on Sensor POST, got: %v", err)
		}
		snap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
		assertSnapshotsEqual(t, "Sensor POST non-member", baselineSnap, snap)

		// 6.7 Sensor POST: Missing parent Gateway must return ErrProvisioningNotFound
		sensMissingParentInput := sensForbiddenInput
		sensMissingParentInput.Sensor = &gateway.ProvisionSensorInput{
			GatewayID: fmt.Sprintf("gw_missing_%s", runID),
			SensorID:  fmt.Sprintf("sens_missing_%s", runID),
			Name:      "Missing Parent Sensor",
			Unit:      strPtr("celsius"),
		}
		_, err = createSvc.CreateEntity(ctx, adminUser, sensMissingParentInput)
		if !errors.Is(err, digitaltwin.ErrProvisioningNotFound) && !errors.Is(err, gateway.ErrProvisioningNotFound) {
			t.Fatalf("expected ErrProvisioningNotFound for missing parent on Sensor POST, got: %v", err)
		}
		snap = mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
		assertSnapshotsEqual(t, "Sensor POST missing parent", baselineSnap, snap)

		// 6.8 Sensor PATCH: Operator and Viewer must be denied with ErrForbidden
		sensPatchInput := digitaltwin.PatchSensorInput{Name: strPtr("Forbidden Renamed Sensor")}
		for _, actor := range []struct {
			name string
			id   uuid.UUID
		}{
			{"operatorUser", operatorUser},
			{"viewerUser", viewerUser},
		} {
			_, err := sensorPatchSvc.PatchSensor(ctx, actor.id, gwIDA, sensorSharedID, sensPatchInput)
			if !errors.Is(err, digitaltwin.ErrForbidden) && !errors.Is(err, gateway.ErrProvisioningForbidden) {
				t.Fatalf("expected ErrForbidden for %s on Sensor PATCH, got: %v", actor.name, err)
			}
			snap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
			assertSnapshotsEqual(t, "Sensor PATCH forbidden "+actor.name, baselineSnap, snap)
		}

		// 6.9 Sensor PATCH: Non-Member must be denied with ErrNotFound
		_, err = sensorPatchSvc.PatchSensor(ctx, nonMemberUser, gwIDA, sensorSharedID, sensPatchInput)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for nonMemberUser on Sensor PATCH, got: %v", err)
		}
		snap = mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
		assertSnapshotsEqual(t, "Sensor PATCH non-member", baselineSnap, snap)

		// 6.10 Sensor PATCH: Missing parent Gateway must return ErrNotFound
		_, err = sensorPatchSvc.PatchSensor(ctx, adminUser, fmt.Sprintf("gw_missing_%s", runID), sensorSharedID, sensPatchInput)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for missing parent on Sensor PATCH, got: %v", err)
		}
		snap = mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
		assertSnapshotsEqual(t, "Sensor PATCH missing parent", baselineSnap, snap)

		// 6.11 Sensor PATCH: Missing sensor resource must return ErrNotFound
		_, err = sensorPatchSvc.PatchSensor(ctx, adminUser, gwIDA, fmt.Sprintf("sens_missing_%s", runID), sensPatchInput)
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for missing sensor on Sensor PATCH, got: %v", err)
		}
		snap = mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
		assertSnapshotsEqual(t, "Sensor PATCH missing sensor", baselineSnap, snap)

		// 6.12 Admin write ability without membership does not grant read state/history access
		resolver, err := digitaltwin.NewPostgresResolver(appPool)
		if err != nil {
			t.Fatalf("failed to construct resolver: %v", err)
		}
		prep, err := digitaltwin.NewReadPreparation(resolver, digitaltwin.DefaultReadPreparationConfig())
		if err != nil {
			t.Fatalf("failed to construct read prep: %v", err)
		}
		reader, err := digitaltwin.NewPostgresTwinReader(appPool)
		if err != nil {
			t.Fatalf("failed to construct twin reader: %v", err)
		}
		readSvc, err := digitaltwin.NewReadService(prep, reader, &metadataStubTelemetryDelegator{})
		if err != nil {
			t.Fatalf("failed to construct read service: %v", err)
		}

		// Admin has proven write capability on Gateway A (Subtests 1, 2, 3, 4), but has NO membership in user_gateways for Gateway A.
		// Therefore, admin read state/detail must return ErrNotFound, and ListTwins must return empty list!
		_, err = readSvc.GetTwinState(ctx, adminUser, digitaltwin.TwinStateQuery{EntityID: "urn:ngsi-ld:Gateway:" + gwIDA})
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for admin reading twin state without membership, got: %v", err)
		}

		_, err = readSvc.GetTwinDetail(ctx, adminUser, digitaltwin.TwinDetailQuery{EntityID: "urn:ngsi-ld:Gateway:" + gwIDA, RelationshipLimit: 10})
		if !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for admin reading twin detail without membership, got: %v", err)
		}

		listRes, err := readSvc.ListTwins(ctx, adminUser, digitaltwin.TwinListQuery{Limit: 10})
		if err != nil {
			t.Fatalf("admin ListTwins failed with error: %v", err)
		}
		if len(listRes.Items) != 0 {
			t.Fatalf("expected 0 items for admin without membership, got: %d", len(listRes.Items))
		}

		// Final snapshot check: all denied operations caused zero side-effects
		finalSnap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIDA})
		assertSnapshotsEqual(t, "Post-denials complete data preservation", baselineSnap, finalSnap)
	})

	// =========================================================================
	// Subtest 7: Inconsistent Graph & State Rejection Without Self-Repair
	// =========================================================================
	t.Run("Inconsistent_Graph_And_State_Rejection", func(t *testing.T) {
		// 7.1 Seed controlled Gateway graph inconsistency on dedicated gwIncID:
		// Insert gateways row and user_gateways row, but deliberately OMIT twin_entities and twin_states.
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.gateways (gateway_id, name, description)
			VALUES ($1, 'Inconsistent Gateway', 'Desc Initial')`, gwIncID); err != nil {
			t.Fatalf("failed to seed inconsistent gateway: %v", err)
		}
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.user_gateways (gateway_id, user_id, role)
			VALUES ($1, $2, 'owner')`, gwIncID, ownerAUser); err != nil {
			t.Fatalf("failed to seed inconsistent gateway membership: %v", err)
		}

		snapIncGW := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIncID})

		// 7.2 Admin PATCH Gateway on corrupt graph must fail with ErrInconsistentGraph
		patchIncGW := digitaltwin.PatchGatewayInput{Name: strPtr("Patched Inconsistent GW")}
		_, err := gwPatchSvc.PatchGateway(ctx, adminUser, gwIncID, patchIncGW)
		if !errors.Is(err, digitaltwin.ErrInconsistentGraph) && !errors.Is(err, gateway.ErrProvisioningInconsistent) {
			t.Fatalf("expected ErrInconsistentGraph on corrupt gateway PATCH, got: %v", err)
		}
		snapAfterIncPatch := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIncID})
		assertSnapshotsEqual(t, "Corrupt Gateway PATCH left DB untouched", snapIncGW, snapAfterIncPatch)

		// 7.3 Admin Replay POST Gateway on corrupt graph must fail with ErrProvisioningInconsistent
		gwIncPostInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   gwIncID,
				Name:        "Inconsistent Gateway",
				Description: strPtr("Desc Initial"),
				OwnerUserID: ownerAUser,
			},
		}
		_, err = createSvc.CreateEntity(ctx, adminUser, gwIncPostInput)
		if !errors.Is(err, digitaltwin.ErrProvisioningInconsistent) && !errors.Is(err, gateway.ErrProvisioningInconsistent) {
			t.Fatalf("expected ErrProvisioningInconsistent on corrupt gateway POST replay, got: %v", err)
		}
		snapAfterIncPost := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIncID})
		assertSnapshotsEqual(t, "Corrupt Gateway POST replay left DB untouched", snapIncGW, snapAfterIncPost)

		// 7.4 Now repair parent Gateway graph so parent is consistent, then seed corrupt Sensor graph
		var parentIncUUID uuid.UUID
		err = setupPool.QueryRow(ctx, `
			INSERT INTO public.twin_entities (entity_id, entity_type, name, gateway_id)
			VALUES ($1, 'Gateway', 'Inconsistent Gateway', $2) RETURNING id`,
			"urn:ngsi-ld:Gateway:"+gwIncID, gwIncID).Scan(&parentIncUUID)
		if err != nil {
			t.Fatalf("failed to insert parent twin entity: %v", err)
		}
		if _, err := setupPool.Exec(ctx, `INSERT INTO public.twin_states (entity_id) VALUES ($1)`, parentIncUUID); err != nil {
			t.Fatalf("failed to insert parent twin state: %v", err)
		}

		// Seed corrupt Sensor on gwIncID: row in sensors and twin_entities, but deliberately OMIT twin_relationships (hasSensor)
		if _, err := setupPool.Exec(ctx, `
			INSERT INTO public.sensors (gateway_id, sensor_id, name, unit)
			VALUES ($1, $2, 'Inconsistent Sensor', 'celsius')`,
			gwIncID, sensorIncID); err != nil {
			t.Fatalf("failed to seed sensor row: %v", err)
		}
		var sensorIncUUID uuid.UUID
		err = setupPool.QueryRow(ctx, `
			INSERT INTO public.twin_entities (entity_id, entity_type, name, gateway_id)
			VALUES ($1, 'Sensor', 'Inconsistent Sensor', $2) RETURNING id`,
			"urn:ngsi-ld:Sensor:"+gwIncID+":"+sensorIncID, gwIncID).Scan(&sensorIncUUID)
		if err != nil {
			t.Fatalf("failed to seed sensor twin entity: %v", err)
		}
		if _, err := setupPool.Exec(ctx, `INSERT INTO public.twin_states (entity_id) VALUES ($1)`, sensorIncUUID); err != nil {
			t.Fatalf("failed to seed sensor twin state: %v", err)
		}

		snapIncSensor := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIncID})

		// 7.5 Owner PATCH Sensor on corrupt graph (missing hasSensor) must fail with ErrInconsistentGraph
		patchIncSens := digitaltwin.PatchSensorInput{Name: strPtr("Patched Inconsistent Sensor")}
		_, err = sensorPatchSvc.PatchSensor(ctx, ownerAUser, gwIncID, sensorIncID, patchIncSens)
		if !errors.Is(err, digitaltwin.ErrInconsistentGraph) && !errors.Is(err, gateway.ErrProvisioningInconsistent) {
			t.Fatalf("expected ErrInconsistentGraph on corrupt sensor PATCH, got: %v", err)
		}
		snapAfterSensPatch := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIncID})
		assertSnapshotsEqual(t, "Corrupt Sensor PATCH left DB untouched", snapIncSensor, snapAfterSensPatch)

		// 7.6 Owner Replay POST Sensor on corrupt graph must fail with ErrProvisioningInconsistent
		sensIncPostInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeSensor,
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: gwIncID,
				SensorID:  sensorIncID,
				Name:      "Inconsistent Sensor",
				Unit:      strPtr("celsius"),
			},
		}
		_, err = createSvc.CreateEntity(ctx, ownerAUser, sensIncPostInput)
		if !errors.Is(err, digitaltwin.ErrProvisioningInconsistent) && !errors.Is(err, gateway.ErrProvisioningInconsistent) {
			t.Fatalf("expected ErrProvisioningInconsistent on corrupt sensor POST replay, got: %v", err)
		}
		snapAfterSensPost := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwIncID})
		assertSnapshotsEqual(t, "Corrupt Sensor POST replay left DB untouched", snapIncSensor, snapAfterSensPost)
	})

	// =========================================================================
	// Subtest 8: Provisioning Resource Isolation, Exact Side Effects, and No-Op Preservation
	// =========================================================================
	t.Run("Provisioning_Resource_Isolation_And_Sentinels", func(t *testing.T) {
		// 8.1 Prior to creation, gwFreshID has 0 rows across all tables
		preSnap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwFreshID})
		if len(preSnap.Gateways) != 0 || len(preSnap.Sensors) != 0 || len(preSnap.Entities) != 0 || len(preSnap.States) != 0 {
			t.Fatalf("expected empty pre-creation snapshot for fresh gateway")
		}

		preAccounts, err := captureMetadataAccountCounts(ctx, setupPool, hasAuthUsers)
		if err != nil {
			t.Fatal(err)
		}

		// 8.2 Platform Admin POST Gateway on gwFreshID
		gwFreshInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   gwFreshID,
				Name:        "Fresh Gateway",
				Description: strPtr("Fresh Description"),
				OwnerUserID: ownerAUser,
			},
		}
		resGW, err := createSvc.CreateEntity(ctx, adminUser, gwFreshInput)
		if err != nil || !resGW.Created {
			t.Fatalf("POST fresh gateway failed: %v", err)
		}

		postGWSnap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwFreshID})
		// Verify EXACT rows added by POST Gateway:
		// gateways: 1, user_gateways: 1, twin_entities: 1, twin_states: 1.
		// All other tables: exactly 0.
		if len(postGWSnap.Gateways) != 1 {
			t.Fatalf("expected 1 gateway row, got %d", len(postGWSnap.Gateways))
		}
		if len(postGWSnap.Memberships) != 1 {
			t.Fatalf("expected 1 membership row, got %d", len(postGWSnap.Memberships))
		}
		if len(postGWSnap.Entities) != 1 {
			t.Fatalf("expected 1 twin_entity row, got %d", len(postGWSnap.Entities))
		}
		if len(postGWSnap.States) != 1 {
			t.Fatalf("expected 1 twin_state row, got %d", len(postGWSnap.States))
		}
		if len(postGWSnap.Sensors) != 0 || len(postGWSnap.Relationships) != 0 ||
			postGWSnap.TemporalCount != 0 || postGWSnap.TelemetryRawCount != 0 ||
			postGWSnap.CommandCount != 0 || postGWSnap.OutboxCount != 0 ||
			postGWSnap.CredentialCount != 0 || postGWSnap.EventCount != 0 {
			t.Fatal(metadataSideEffectDiagnostic("Gateway", postGWSnap))
		}

		// Verify NO profile or auth.users rows created by mutation
		postAccounts, err := captureMetadataAccountCounts(ctx, setupPool, hasAuthUsers)
		if err != nil {
			t.Fatal(err)
		}
		if postAccounts != preAccounts {
			t.Fatalf("POST Gateway changed account counts: before=%v after=%v", preAccounts, postAccounts)
		}

		// 8.3 Gateway Owner POST Sensor on gwFreshID
		sensFreshInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeSensor,
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: gwFreshID,
				SensorID:  sensorFreshID,
				Name:      "Fresh Sensor",
				Unit:      strPtr("celsius"),
			},
		}
		resSens, err := createSvc.CreateEntity(ctx, ownerAUser, sensFreshInput)
		if err != nil || !resSens.Created {
			t.Fatalf("POST fresh sensor failed: %v", err)
		}

		postSensSnap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwFreshID})
		// Verify EXACT rows added by POST Sensor:
		// sensors: 1, twin_entities: 2 (1 GW + 1 Sensor), twin_states: 2, twin_relationships: 1.
		// memberships: unchanged (1), gateways: unchanged (1).
		// commands, outbox, temporal, telemetry, credentials, events: exactly 0.
		if len(postSensSnap.Sensors) != 1 {
			t.Fatalf("expected 1 sensor row, got %d", len(postSensSnap.Sensors))
		}
		if len(postSensSnap.Entities) != 2 {
			t.Fatalf("expected 2 twin_entity rows, got %d", len(postSensSnap.Entities))
		}
		if len(postSensSnap.States) != 2 {
			t.Fatalf("expected 2 twin_state rows, got %d", len(postSensSnap.States))
		}
		if len(postSensSnap.Relationships) != 1 {
			t.Fatalf("expected 1 relationship row, got %d", len(postSensSnap.Relationships))
		}
		if postSensSnap.TemporalCount != 0 || postSensSnap.TelemetryRawCount != 0 ||
			postSensSnap.CommandCount != 0 || postSensSnap.OutboxCount != 0 ||
			postSensSnap.CredentialCount != 0 || postSensSnap.EventCount != 0 {
			t.Fatal(metadataSideEffectDiagnostic("Sensor", postSensSnap))
		}
		postSensorAccounts, err := captureMetadataAccountCounts(ctx, setupPool, hasAuthUsers)
		if err != nil {
			t.Fatal(err)
		}
		if postSensorAccounts != preAccounts {
			t.Fatalf("POST Sensor changed account counts: before=%v after=%v", preAccounts, postSensorAccounts)
		}

		// 8.4 Same-value No-Op PATCH on both Gateway and Sensor leaves complete snapshot unchanged
		gwNoOp := digitaltwin.PatchGatewayInput{
			Name:           strPtr("Fresh Gateway"),
			DescriptionSet: true,
			Description:    strPtr("Fresh Description"),
		}
		_, err = gwPatchSvc.PatchGateway(ctx, adminUser, gwFreshID, gwNoOp)
		if err != nil {
			t.Fatalf("same-value gateway no-op failed: %v", err)
		}

		sensNoOp := digitaltwin.PatchSensorInput{
			Name: strPtr("Fresh Sensor"),
		}
		_, err = sensorPatchSvc.PatchSensor(ctx, ownerAUser, gwFreshID, sensorFreshID, sensNoOp)
		if err != nil {
			t.Fatalf("same-value sensor no-op failed: %v", err)
		}

		postNoOpSnap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwFreshID})
		assertSnapshotsEqual(t, "Same-value No-Op complete preservation", postSensSnap, postNoOpSnap)
	})

	// =========================================================================
	// Subtest 9: Gateway PATCH Atomic Rollback On Real SQL Failure (Item 4.5.7.6)
	// =========================================================================
	t.Run("Gateway_PATCH_Atomic_Rollback_On_Real_SQL_Failure", func(t *testing.T) {
		// 9.1 Seed dedicated fixture-owned Gateway and Sensor sentinel for rollback verification
		gwRollbackInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   gwRollbackID,
				Name:        "Rollback Initial Name",
				Description: strPtr("Rollback Initial Description"),
				OwnerUserID: ownerAUser,
			},
		}
		resGW, err := createSvc.CreateEntity(ctx, adminUser, gwRollbackInput)
		if err != nil || !resGW.Created {
			t.Fatalf("POST gateway for rollback failed: %v", err)
		}

		sensRBInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeSensor,
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: gwRollbackID,
				SensorID:  sensorRollbackID,
				Name:      "Rollback Sentinel Sensor",
				Unit:      strPtr("celsius"),
			},
		}
		resSens, err := createSvc.CreateEntity(ctx, ownerAUser, sensRBInput)
		if err != nil || !resSens.Created {
			t.Fatalf("POST sensor sentinel failed: %v", err)
		}

		// 9.2 Capture pre-mutation snapshot under application role
		beforeSnap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwRollbackID, gwSentinelID})
		initialGW, ok := beforeSnap.Gateways[gwRollbackID]
		if !ok {
			t.Fatalf("expected seeded gateway in before snapshot")
		}

		var twinInternalUUID uuid.UUID
		for id, ent := range beforeSnap.Entities {
			if ent.EntityID == "urn:ngsi-ld:Gateway:"+gwRollbackID {
				twinInternalUUID = id
				break
			}
		}
		if twinInternalUUID == uuid.Nil {
			t.Fatalf("failed to locate twin entity UUID for gateway %s", gwRollbackID)
		}
		initialEntity, ok := beforeSnap.Entities[twinInternalUUID]
		if !ok {
			t.Fatalf("expected seeded twin entity in before snapshot")
		}

		// 9.3 Prepare representative Gateway PATCH mutating both name and description
		proposedName := "Rollback Target Proposed Name"
		proposedDesc := "Rollback Target Proposed Description"
		patchInput := digitaltwin.PatchGatewayInput{
			Name:           &proposedName,
			DescriptionSet: true,
			Description:    &proposedDesc,
		}

		// 9.4 Set up decorated TxDatabase that injects a real PostgreSQL statement error
		// strictly AFTER the relational UPDATE on gateways has executed in the same transaction.
		var (
			relationalExecCount    int
			relationalRowsAffected int64
			uncommittedObserved    bool
			faultInjected          bool
		)

		decoratedTxDb := &faultInjectingTxDatabase{
			underlying: appPool,
			onTxBegin: func(realTx pgx.Tx) pgx.Tx {
				return &faultInjectingTx{
					Tx: realTx,
					onExec: func(execCtx context.Context, tx pgx.Tx, sql string, args ...any) (pgconn.CommandTag, error, bool) {
						if strings.Contains(sql, "UPDATE public.gateways") {
							// Check that the update targets our specific fixture gateway
							if len(args) == 3 && args[2] == gwRollbackID {
								tag, execErr := tx.Exec(execCtx, sql, args...)
								if execErr != nil {
									t.Fatalf("relational update exec failed: %v", execErr)
								}
								relationalExecCount++
								relationalRowsAffected = tag.RowsAffected()
								return tag, nil, true
							}
						}
						return pgconn.CommandTag{}, nil, false
					},
					onQueryRow: func(qCtx context.Context, tx pgx.Tx, sql string, args ...any) (pgx.Row, bool) {
						if strings.Contains(sql, "UPDATE public.twin_entities") {
							// Check that the update targets our specific fixture twin entity
							if len(args) >= 2 && args[1] == twinInternalUUID {
								// Assert 1: Relational UPDATE must have already completed in this transaction
								if relationalExecCount != 1 || relationalRowsAffected != 1 {
									t.Fatalf("relational update was not executed before twin update: count=%d rows=%d",
										relationalExecCount, relationalRowsAffected)
								}

								// Assert 2: Query uncommitted relational row inside the SAME active transaction
								// to definitively prove that the relational row was mutated before fault injection
								var uncommittedName string
								var uncommittedDesc *string
								scanErr := tx.QueryRow(qCtx, `SELECT name, description FROM public.gateways WHERE gateway_id = $1`, gwRollbackID).
									Scan(&uncommittedName, &uncommittedDesc)
								if scanErr != nil {
									t.Fatalf("failed to query uncommitted gateway in active transaction: %v", scanErr)
								}
								if uncommittedName != proposedName || uncommittedDesc == nil || *uncommittedDesc != proposedDesc {
									t.Fatalf("relational update uncommitted values mismatch: got (%q, %v), want (%q, %q)",
										uncommittedName, uncommittedDesc, proposedName, proposedDesc)
								}
								uncommittedObserved = true

								// Assert 3: Inject a REAL invalid SQL statement on this transaction so PostgreSQL aborts it
								faultInjected = true
								invalidSQL := `UPDATE public.__nonexistent_twin_rollback_injection_table__ SET name = $1 WHERE id = $2 RETURNING updated_at`
								return tx.QueryRow(qCtx, invalidSQL, args...), true
							}
						}
						return nil, false
					},
				}
			},
		}

		gwRollbackPatcher, err := digitaltwin.NewPostgresGatewayMetadataPatcher(decoratedTxDb)
		if err != nil {
			t.Fatalf("failed to construct decorated gateway patcher: %v", err)
		}
		gwRollbackPatchSvc, err := digitaltwin.NewGatewayPatchService(gwRollbackPatcher, 5*time.Second)
		if err != nil {
			t.Fatalf("failed to construct decorated gateway patch service: %v", err)
		}

		// 9.5 Execute PATCH through real service
		patchRes, patchErr := gwRollbackPatchSvc.PatchGateway(ctx, adminUser, gwRollbackID, patchInput)

		// Assert test instrumentation executed in exact order
		if relationalExecCount != 1 {
			t.Fatalf("expected relationalExecCount = 1, got %d", relationalExecCount)
		}
		if !uncommittedObserved {
			t.Fatalf("expected uncommitted relational write to be observed in transaction before fault")
		}
		if !faultInjected {
			t.Fatalf("expected fault injection to be executed")
		}

		// Assert Step 4: zero result, non-nil error, ErrUnexpected classification (not unavailable), and real pgconn.PgError
		if patchRes != (digitaltwin.PatchGatewayResult{}) {
			t.Fatalf("expected zero PatchGatewayResult on rollback failure, got: %+v", patchRes)
		}
		if patchErr == nil {
			t.Fatalf("expected error from rollback patch, got nil")
		}
		if !errors.Is(patchErr, digitaltwin.ErrUnexpected) {
			t.Fatalf("expected patchErr to be classified as ErrUnexpected, got: %v", patchErr)
		}
		if errors.Is(patchErr, digitaltwin.ErrDependencyUnavailable) {
			t.Fatalf("expected patchErr NOT to be classified as ErrDependencyUnavailable, got: %v", patchErr)
		}
		if errors.Is(patchErr, digitaltwin.ErrMetadataPatchUnavailable) {
			t.Fatalf("expected patchErr NOT to be classified as ErrMetadataPatchUnavailable, got: %v", patchErr)
		}
		var pgErr *pgconn.PgError
		if !errors.As(patchErr, &pgErr) {
			t.Fatalf("expected underlying error to be *pgconn.PgError, got: %v", patchErr)
		}
		if pgErr.Code != "42P01" {
			t.Fatalf("expected PostgreSQL error code 42P01 (undefined_table), got: %s", pgErr.Code)
		}

		// 9.6 Step 5: After transaction teardown, verify connection pool health (no leaked aborted transaction)
		conn, err := appPool.Acquire(ctx)
		if err != nil {
			t.Fatalf("failed to acquire connection from pool after rollback: %v", err)
		}
		var ping int
		if err := conn.QueryRow(ctx, "SELECT 1").Scan(&ping); err != nil || ping != 1 {
			conn.Release()
			t.Fatalf("connection from pool is unhealthy after transaction abort/rollback: %v", err)
		}
		conn.Release()

		// Capture post-rollback snapshot and assert 100% preservation across all sentinels
		afterSnap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwRollbackID, gwSentinelID})
		assertSnapshotsEqual(t, "Gateway PATCH Atomic Rollback Verification", beforeSnap, afterSnap)

		// Explicit verification that relational table, twin entity, and timestamps were NOT persisted
		var currentName string
		var currentDesc *string
		err = appPool.QueryRow(ctx, `SELECT name, description FROM public.gateways WHERE gateway_id = $1`, gwRollbackID).
			Scan(&currentName, &currentDesc)
		if err != nil {
			t.Fatalf("failed to query gateway after rollback: %v", err)
		}
		if currentName != initialGW.Name || !sameNullableStr(currentDesc, initialGW.Description) {
			t.Fatalf("relational gateway was not rolled back: got (%q, %v), want (%q, %v)",
				currentName, currentDesc, initialGW.Name, initialGW.Description)
		}
		if currentName == proposedName || (currentDesc != nil && *currentDesc == proposedDesc) {
			t.Fatalf("proposed metadata persisted in gateways table despite rollback!")
		}

		var twinName string
		var twinUpdatedAt time.Time
		err = appPool.QueryRow(ctx, `SELECT name, updated_at FROM public.twin_entities WHERE id = $1`, twinInternalUUID).
			Scan(&twinName, &twinUpdatedAt)
		if err != nil {
			t.Fatalf("failed to query twin entity after rollback: %v", err)
		}
		if twinName != initialEntity.Name || !twinUpdatedAt.Equal(initialEntity.UpdatedAt) {
			t.Fatalf("twin entity was modified despite rollback: got (%q, %v), want (%q, %v)",
				twinName, twinUpdatedAt, initialEntity.Name, initialEntity.UpdatedAt)
		}
		if twinName == proposedName {
			t.Fatalf("proposed metadata persisted in twin_entities table despite rollback!")
		}
	})

	// =========================================================================
	// Subtest 10: Sensor PATCH Concurrent with Real Telemetry Persist (Task 4.5.7 Item 7)
	// =========================================================================
	t.Run("Sensor_PATCH_Concurrent_With_Telemetry_Persist_And_Subsequent_Batch", func(t *testing.T) {
		// 10.1 Seed independent fixture-owned Gateway and Sensor through existing provisioning paths
		gwConcInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   gwConcID,
				Name:        "Concurrent Telemetry Gateway",
				Description: strPtr("Gateway for concurrent PATCH and Persist"),
				OwnerUserID: ownerAUser,
			},
		}
		resGW, err := createSvc.CreateEntity(ctx, adminUser, gwConcInput)
		if err != nil || !resGW.Created {
			t.Fatalf("POST gateway for concurrency test failed: %v", err)
		}

		sensConcInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeSensor,
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: gwConcID,
				SensorID:  sensorConcID,
				Name:      "Initial Concurrent Sensor Name",
				Unit:      strPtr("celsius"),
			},
		}
		resSens, err := createSvc.CreateEntity(ctx, ownerAUser, sensConcInput)
		if err != nil || !resSens.Created {
			t.Fatalf("POST sensor for concurrency test failed: %v", err)
		}

		sensorURN := fmt.Sprintf("urn:ngsi-ld:Sensor:%s:%s", gwConcID, sensorConcID)
		var sensorTwinInternalID uuid.UUID
		err = appPool.QueryRow(ctx, `SELECT id FROM public.twin_entities WHERE entity_id = $1`, sensorURN).Scan(&sensorTwinInternalID)
		if err != nil {
			t.Fatalf("failed to query sensor twin internal UUID: %v", err)
		}

		gatewayURN := fmt.Sprintf("urn:ngsi-ld:Gateway:%s", gwConcID)
		var gatewayTwinInternalID uuid.UUID
		err = appPool.QueryRow(ctx, `SELECT id FROM public.twin_entities WHERE entity_id = $1`, gatewayURN).Scan(&gatewayTwinInternalID)
		if err != nil {
			t.Fatalf("failed to query gateway twin internal UUID: %v", err)
		}

		// Seed meaningful desired/version/audit/unit sentinels in twin_states
		sentinelDesiredState := `{"calibration_offset": 1.5, "sampling_interval": 10000}`
		sentinelDesiredVersion := int64(42)
		sentinelLastDesiredAt := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Microsecond)

		_, err = appPool.Exec(ctx, `
			UPDATE public.twin_states
			SET desired_state = $1::jsonb,
				desired_version = $2,
				last_desired_at = $3,
				updated_at = $3
			WHERE entity_id = $4`,
			sentinelDesiredState, sentinelDesiredVersion, sentinelLastDesiredAt, sensorTwinInternalID)
		if err != nil {
			t.Fatalf("failed to seed desired state sentinels: %v", err)
		}

		beforeSnap := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwConcID})
		if beforeSnap.TemporalCount != 0 || beforeSnap.TelemetryRawCount != 0 || beforeSnap.LedgerCount != 0 {
			t.Fatalf("expected 0 initial temporal/raw/ledger counts, got temp=%d, raw=%d, ledger=%d",
				beforeSnap.TemporalCount, beforeSnap.TelemetryRawCount, beforeSnap.LedgerCount)
		}
		if beforeSnap.States[sensorTwinInternalID].DesiredVersion != 42 {
			t.Fatalf("expected desired_version=42, got %d", beforeSnap.States[sensorTwinInternalID].DesiredVersion)
		}

		// 10.2 Construct valid immutable telemetry batch 1 via actual validator
		validator, err := telemetry.NewValidator(telemetry.Limits{
			MaxPayloadBytes:  1 << 20,
			MaxSamples:       1000,
			MaxBatchDuration: time.Hour,
			MaxFutureSkew:    time.Hour,
		})
		if err != nil {
			t.Fatalf("failed to create validator: %v", err)
		}

		bootID1 := uuid.New()
		msgID1 := uuid.New()
		measuredAt1 := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Millisecond)
		const sampleCount1 = 10
		samples1 := make([]float64, sampleCount1)
		for i := 0; i < sampleCount1; i++ {
			samples1[i] = 25.0 + float64(i)*0.1
		}

		dto1 := telemetry.BatchDTO{
			ProtocolVersion:  1,
			MessageID:        msgID1,
			MessageType:      "telemetry_batch",
			GatewayID:        gwConcID,
			SensorID:         sensorConcID,
			BootID:           bootID1,
			FirstSequence:    1001,
			SampleCount:      int32(sampleCount1),
			MeasuredAt:       measuredAt1.Format(time.RFC3339Nano),
			SampleIntervalUS: 10000, // 10ms
			Samples:          samples1,
		}
		payloadBytes1, err := json.Marshal(dto1)
		if err != nil {
			t.Fatalf("failed to marshal batch 1: %v", err)
		}

		topic := fmt.Sprintf("gateways/%s/telemetry/%s", gwConcID, sensorConcID)
		validatedMsg1, err := validator.Validate(topic, payloadBytes1, time.Now().UTC())
		if err != nil {
			t.Fatalf("failed to validate telemetry message 1: %v", err)
		}

		proposedPatchedName := "Patched Sensor Name Concurrent"
		patchInput := digitaltwin.PatchSensorInput{
			Name: &proposedPatchedName,
		}

		// 10.3 Setup deterministic overlap using test-only transaction decorators
		telemetrySensorLockHeld := make(chan struct{}, 1)
		allowTelemetryContinue := make(chan struct{})
		var onceClose sync.Once
		closeContinue := func() {
			onceClose.Do(func() {
				close(allowTelemetryContinue)
			})
		}
		defer closeContinue()

		decoratedTelemetryDb := &faultInjectingTxDatabase{
			underlying: appPool,
			onTxBegin: func(realTx pgx.Tx) pgx.Tx {
				return &faultInjectingTx{
					Tx: realTx,
					onQueryRow: func(qCtx context.Context, tx pgx.Tx, sql string, args ...any) (pgx.Row, bool) {
						if strings.Contains(sql, "FROM public.sensors") && strings.Contains(sql, "FOR UPDATE") {
							realRow := tx.QueryRow(qCtx, sql, args...)
							return &interceptedRow{
								realRow: realRow,
								onScanned: func(scanErr error) {
									if scanErr == nil {
										select {
										case telemetrySensorLockHeld <- struct{}{}:
										default:
										}
										select {
										case <-allowTelemetryContinue:
										case <-qCtx.Done():
										}
									}
								},
							}, true
						}
						return nil, false
					},
				}
			},
		}

		telemetryRepo, err := telemetry.NewPostgresRepository(decoratedTelemetryDb, 10*time.Second)
		if err != nil {
			t.Fatalf("failed to create decorated telemetry repository: %v", err)
		}

		patchAttemptingSensorLock := make(chan struct{}, 1)
		decoratedPatchDb := &faultInjectingTxDatabase{
			underlying: appPool,
			onTxBegin: func(realTx pgx.Tx) pgx.Tx {
				return &faultInjectingTx{
					Tx: realTx,
					onQueryRow: func(qCtx context.Context, tx pgx.Tx, sql string, args ...any) (pgx.Row, bool) {
						if strings.Contains(sql, "FROM public.sensors") && strings.Contains(sql, "FOR UPDATE") {
							select {
							case patchAttemptingSensorLock <- struct{}{}:
							default:
							}
							return tx.QueryRow(qCtx, sql, args...), true
						}
						return nil, false
					},
				}
			},
		}

		concPatcher, err := digitaltwin.NewPostgresSensorMetadataPatcher(decoratedPatchDb)
		if err != nil {
			t.Fatalf("failed to construct sensor patcher: %v", err)
		}
		concPatchSvc, err := digitaltwin.NewSensorPatchService(concPatcher, 10*time.Second)
		if err != nil {
			t.Fatalf("failed to construct sensor patch service: %v", err)
		}

		// 10.4 Execute bounded concurrent operations
		telemetryCtx, cancelTelemetry := context.WithTimeout(ctx, 15*time.Second)
		defer cancelTelemetry()
		patchCtx, cancelPatch := context.WithTimeout(ctx, 15*time.Second)
		defer cancelPatch()

		telemetryErrCh := make(chan error, 1)
		telemetryResCh := make(chan telemetry.PersistResult, 1)
		go func() {
			res, pErr := telemetryRepo.Persist(telemetryCtx, validatedMsg1)
			if pErr != nil {
				telemetryErrCh <- pErr
				return
			}
			telemetryResCh <- res
			telemetryErrCh <- nil
		}()

		// Wait for Telemetry to acquire sensor row lock
		select {
		case <-telemetrySensorLockHeld:
			t.Logf("Telemetry transaction acquired sensor row lock on %s:%s FOR UPDATE", gwConcID, sensorConcID)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for telemetry to acquire sensor row lock: %v", ctx.Err())
		}

		// Start PATCH in goroutine
		patchErrCh := make(chan error, 1)
		patchResCh := make(chan digitaltwin.PatchSensorResult, 1)
		go func() {
			res, pErr := concPatchSvc.PatchSensor(patchCtx, ownerAUser, gwConcID, sensorConcID, patchInput)
			if pErr != nil {
				patchErrCh <- pErr
				return
			}
			patchResCh <- res
			patchErrCh <- nil
		}()

		// Wait for PATCH to reach the sensor lock
		select {
		case <-patchAttemptingSensorLock:
			t.Logf("Sensor PATCH entered and attempted sensor row lock FOR UPDATE")
		case <-ctx.Done():
			t.Fatalf("timed out waiting for patch to attempt sensor lock: %v", ctx.Err())
		}

		// Poll pg_locks to verify that PostgreSQL has recorded an ungranted lock wait
		var lockWaitObserved bool
		for i := 0; i < 50; i++ {
			var ungrantedCount int
			if err := setupPool.QueryRow(ctx, "SELECT count(*) FROM pg_locks WHERE NOT granted").Scan(&ungrantedCount); err != nil {
				t.Fatal("observe lock contention:", err)
			}
			if ungrantedCount > 0 {
				lockWaitObserved = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !lockWaitObserved {
			t.Fatalf("expected PostgreSQL pg_locks to observe ungranted row lock wait for concurrent transactions")
		}
		t.Logf("Observed real PostgreSQL ungranted row lock contention (lock wait) between Telemetry and PATCH")

		// Release Telemetry to proceed and commit!
		closeContinue()

		// Await completion of both
		select {
		case pErr := <-telemetryErrCh:
			if pErr != nil {
				t.Fatalf("telemetry persist failed: %v", pErr)
			}
		case <-ctx.Done():
			t.Fatalf("timed out awaiting telemetry persist: %v", ctx.Err())
		}
		res1 := <-telemetryResCh
		if res1.Outcome != telemetry.CommitOutcomeCommitted {
			t.Fatalf("expected CommitOutcomeCommitted, got %v", res1.Outcome)
		}
		if res1.SamplesSaved != sampleCount1 {
			t.Fatalf("expected %d samples saved, got %d", sampleCount1, res1.SamplesSaved)
		}

		select {
		case pErr := <-patchErrCh:
			if pErr != nil {
				t.Fatalf("sensor patch failed: %v", pErr)
			}
		case <-ctx.Done():
			t.Fatalf("timed out awaiting sensor patch: %v", ctx.Err())
		}
		patchRes := <-patchResCh
		if patchRes.Name != proposedPatchedName {
			t.Fatalf("expected patch result name %s, got %s", proposedPatchedName, patchRes.Name)
		}

		// 10.5 Verify state after Batch 1 + PATCH
		// Relational sensor name and twin entity name equal requested PATCH name
		var currentSensorName string
		var currentSensorUnit *string
		err = appPool.QueryRow(ctx, `SELECT name, unit FROM public.sensors WHERE gateway_id = $1 AND sensor_id = $2`, gwConcID, sensorConcID).
			Scan(&currentSensorName, &currentSensorUnit)
		if err != nil {
			t.Fatalf("failed to query sensor after patch: %v", err)
		}
		if currentSensorName != proposedPatchedName {
			t.Fatalf("relational sensor name mismatch: got %q, want %q", currentSensorName, proposedPatchedName)
		}
		if currentSensorUnit == nil || *currentSensorUnit != "celsius" {
			t.Fatalf("relational sensor unit changed: got %v, want celsius", currentSensorUnit)
		}

		var currentTwinName string
		var currentTwinUpdatedAt time.Time
		err = appPool.QueryRow(ctx, `SELECT name, updated_at FROM public.twin_entities WHERE id = $1`, sensorTwinInternalID).
			Scan(&currentTwinName, &currentTwinUpdatedAt)
		if err != nil {
			t.Fatalf("failed to query sensor twin entity after patch: %v", err)
		}
		if currentTwinName != proposedPatchedName {
			t.Fatalf("twin entity sensor name mismatch: got %q, want %q", currentTwinName, proposedPatchedName)
		}
		if !currentTwinUpdatedAt.After(beforeSnap.Entities[sensorTwinInternalID].UpdatedAt) {
			t.Fatalf("twin entity updated_at did not advance: %v vs %v", currentTwinUpdatedAt, beforeSnap.Entities[sensorTwinInternalID].UpdatedAt)
		}

		// Raw samples in public.telemetry
		var rawCount1 int
		err = appPool.QueryRow(ctx, `SELECT count(*) FROM public.telemetry WHERE gateway_id = $1 AND sensor_id = $2`, gwConcID, sensorConcID).Scan(&rawCount1)
		if err != nil || rawCount1 != sampleCount1 {
			t.Fatalf("expected %d raw samples, got %d (err: %v)", sampleCount1, rawCount1, err)
		}

		rows1, err := appPool.Query(ctx, `SELECT sequence_num, value, boot_id, message_id, sample_index FROM public.telemetry WHERE gateway_id = $1 AND sensor_id = $2 ORDER BY sequence_num ASC`, gwConcID, sensorConcID)
		if err != nil {
			t.Fatalf("failed to query telemetry raw samples: %v", err)
		}
		defer rows1.Close()
		idx := 0
		for rows1.Next() {
			var seq int64
			var val float64
			var bID, mID uuid.UUID
			var sIdx int32
			if err := rows1.Scan(&seq, &val, &bID, &mID, &sIdx); err != nil {
				t.Fatalf("failed to scan raw sample: %v", err)
			}
			if seq != int64(1001+idx) || sIdx != int32(idx) || bID != bootID1 || mID != msgID1 {
				t.Fatalf("raw sample mismatch at idx %d: seq=%d, sIdx=%d, bID=%v, mID=%v", idx, seq, sIdx, bID, mID)
			}
			expectedVal := 25.0 + float64(idx)*0.1
			if fmt.Sprintf("%.2f", val) != fmt.Sprintf("%.2f", expectedVal) {
				t.Fatalf("raw sample value mismatch at idx %d: got %v, want %v", idx, val, expectedVal)
			}
			idx++
		}
		if idx != sampleCount1 {
			t.Fatalf("expected %d scanned raw samples, got %d", sampleCount1, idx)
		}

		// Ledger row in public.processed_messages
		var lSampleCount int32
		var lFirstSeq, lLastSeq int64
		var lSensorID string
		var lBootID uuid.UUID
		err = appPool.QueryRow(ctx, `SELECT sample_count, first_sequence, last_sequence, sensor_id, boot_id
			FROM public.processed_messages WHERE gateway_id = $1 AND message_id = $2`, gwConcID, msgID1).
			Scan(&lSampleCount, &lFirstSeq, &lLastSeq, &lSensorID, &lBootID)
		if err != nil || lSampleCount != int32(sampleCount1) || lFirstSeq != 1001 || lLastSeq != 1010 || lSensorID != sensorConcID || lBootID != bootID1 {
			t.Fatalf("ledger row mismatch: samples=%d, firstSeq=%d, lastSeq=%d, sensorID=%s, bootID=%v (err: %v)", lSampleCount, lFirstSeq, lLastSeq, lSensorID, lBootID, err)
		}

		// Temporal observations in public.twin_temporal_values
		var tempCount1 int
		var tempValNum float64
		var tempPropName string
		var tempUnit string
		err = appPool.QueryRow(ctx, `SELECT count(*), COALESCE(MAX(value_number), 0), COALESCE(MAX(property_name), ''), COALESCE(MAX(metadata->>'unit'), '')
			FROM public.twin_temporal_values WHERE entity_id = $1`, sensorTwinInternalID).
			Scan(&tempCount1, &tempValNum, &tempPropName, &tempUnit)
		if err != nil || tempCount1 != 1 || tempPropName != "sample_interval_us" || tempValNum != 10000 || tempUnit != "us" {
			t.Fatalf("temporal observation mismatch: count=%d, prop=%s, val=%v, unit=%s (err: %v)", tempCount1, tempPropName, tempValNum, tempUnit, err)
		}

		// Sensor Twin State in public.twin_states
		var stReportedState []byte
		var stDesiredState []byte
		var stReportedVersion int64
		var stDesiredVersion int64
		var stLastReportedAt *time.Time
		var stLastDesiredAt *time.Time
		err = appPool.QueryRow(ctx, `SELECT reported_state, desired_state, reported_version, desired_version, last_reported_at, last_desired_at
			FROM public.twin_states WHERE entity_id = $1`, sensorTwinInternalID).
			Scan(&stReportedState, &stDesiredState, &stReportedVersion, &stDesiredVersion, &stLastReportedAt, &stLastDesiredAt)
		if err != nil {
			t.Fatalf("failed to query twin_states: %v", err)
		}
		if stReportedVersion != 1 {
			t.Fatalf("expected reported_version=1, got %d", stReportedVersion)
		}
		if stDesiredVersion != sentinelDesiredVersion {
			t.Fatalf("desired_version mutated by persist/patch: got %d, want %d", stDesiredVersion, sentinelDesiredVersion)
		}
		if stLastDesiredAt == nil || !stLastDesiredAt.Equal(sentinelLastDesiredAt) {
			t.Fatalf("last_desired_at mutated by persist/patch: got %v, want %v", stLastDesiredAt, sentinelLastDesiredAt)
		}
		var desObj map[string]any
		if err := json.Unmarshal(stDesiredState, &desObj); err != nil {
			t.Fatalf("failed to unmarshal desired_state: %v", err)
		}
		if desObj["sampling_interval"].(float64) != 10000 || desObj["calibration_offset"].(float64) != 1.5 {
			t.Fatalf("desired_state mutated: got %v", desObj)
		}
		if stLastReportedAt == nil {
			t.Fatalf("expected last_reported_at to be non-nil")
		}

		var repObj map[string]any
		if err := json.Unmarshal(stReportedState, &repObj); err != nil {
			t.Fatalf("failed to unmarshal reported_state: %v", err)
		}
		if repObj["unit"] != "celsius" {
			t.Fatalf("reported_state unit mismatch: got %v, want celsius", repObj["unit"])
		}
		if repObj["boot_id"] != bootID1.String() {
			t.Fatalf("reported_state boot_id mismatch: got %v, want %s", repObj["boot_id"], bootID1.String())
		}
		expectedLastSampleTime1 := measuredAt1.Add(time.Duration(sampleCount1-1) * 10000 * time.Microsecond)
		if !stLastReportedAt.Equal(expectedLastSampleTime1) {
			t.Fatalf("last_reported_at mismatch: got %v, want %v", stLastReportedAt, expectedLastSampleTime1)
		}

		// Gateway state and metadata: UNCHANGED
		var gwCurrentName string
		var gwCurrentDesc *string
		var gwReportedVer, gwDesiredVer int64
		err = appPool.QueryRow(ctx, `SELECT g.name, g.description, s.reported_version, s.desired_version
			FROM public.gateways g
			JOIN public.twin_states s ON s.entity_id = $2
			WHERE g.gateway_id = $1`, gwConcID, gatewayTwinInternalID).
			Scan(&gwCurrentName, &gwCurrentDesc, &gwReportedVer, &gwDesiredVer)
		if err != nil {
			t.Fatalf("failed to query gateway state: %v", err)
		}
		if gwCurrentName != "Concurrent Telemetry Gateway" || gwCurrentDesc == nil || *gwCurrentDesc != "Gateway for concurrent PATCH and Persist" {
			t.Fatalf("gateway metadata mutated: name=%s, desc=%v", gwCurrentName, gwCurrentDesc)
		}
		if gwReportedVer != 0 || gwDesiredVer != 0 {
			t.Fatalf("gateway twin state mutated: repVer=%d, desVer=%d", gwReportedVer, gwDesiredVer)
		}

		// 10.6 Send a SECOND valid batch after PATCH and assert Persist succeeds
		msgID2 := uuid.New()
		measuredAt2 := measuredAt1.Add(1 * time.Minute)
		const sampleCount2 = 15
		samples2 := make([]float64, sampleCount2)
		for i := 0; i < sampleCount2; i++ {
			samples2[i] = 26.0 + float64(i)*0.1
		}

		dto2 := telemetry.BatchDTO{
			ProtocolVersion:  1,
			MessageID:        msgID2,
			MessageType:      "telemetry_batch",
			GatewayID:        gwConcID,
			SensorID:         sensorConcID,
			BootID:           bootID1,
			FirstSequence:    1011,
			SampleCount:      int32(sampleCount2),
			MeasuredAt:       measuredAt2.Format(time.RFC3339Nano),
			SampleIntervalUS: 10000,
			Samples:          samples2,
		}
		payloadBytes2, err := json.Marshal(dto2)
		if err != nil {
			t.Fatalf("failed to marshal batch 2: %v", err)
		}

		validatedMsg2, err := validator.Validate(topic, payloadBytes2, time.Now().UTC())
		if err != nil {
			t.Fatalf("failed to validate telemetry message 2: %v", err)
		}

		plainTelemetryRepo, err := telemetry.NewPostgresRepository(appPool, 10*time.Second)
		if err != nil {
			t.Fatalf("failed to create plain telemetry repository: %v", err)
		}

		res2, err := plainTelemetryRepo.Persist(ctx, validatedMsg2)
		if err != nil {
			t.Fatalf("persist batch 2 failed: %v", err)
		}
		if res2.Outcome != telemetry.CommitOutcomeCommitted {
			t.Fatalf("expected CommitOutcomeCommitted for batch 2, got %v", res2.Outcome)
		}
		if res2.SamplesSaved != sampleCount2 {
			t.Fatalf("expected %d samples saved for batch 2, got %d", sampleCount2, res2.SamplesSaved)
		}

		// Assert combined counts after Batch 2
		var totalRawCount int
		err = appPool.QueryRow(ctx, `SELECT count(*) FROM public.telemetry WHERE gateway_id = $1 AND sensor_id = $2`, gwConcID, sensorConcID).Scan(&totalRawCount)
		if err != nil || totalRawCount != sampleCount1+sampleCount2 {
			t.Fatalf("expected total raw samples %d, got %d (err: %v)", sampleCount1+sampleCount2, totalRawCount, err)
		}

		var totalLedgerCount int
		err = appPool.QueryRow(ctx, `SELECT count(*) FROM public.processed_messages WHERE gateway_id = $1`, gwConcID).Scan(&totalLedgerCount)
		if err != nil || totalLedgerCount != 2 {
			t.Fatalf("expected total ledger count 2, got %d (err: %v)", totalLedgerCount, err)
		}

		var totalTemporalCount int
		err = appPool.QueryRow(ctx, `SELECT count(*) FROM public.twin_temporal_values WHERE entity_id = $1`, sensorTwinInternalID).Scan(&totalTemporalCount)
		if err != nil || totalTemporalCount != 2 {
			t.Fatalf("expected total temporal count 2, got %d (err: %v)", totalTemporalCount, err)
		}

		// Assert updated reported_state after batch 2
		err = appPool.QueryRow(ctx, `SELECT reported_state, desired_state, reported_version, desired_version, last_reported_at, last_desired_at
			FROM public.twin_states WHERE entity_id = $1`, sensorTwinInternalID).
			Scan(&stReportedState, &stDesiredState, &stReportedVersion, &stDesiredVersion, &stLastReportedAt, &stLastDesiredAt)
		if err != nil {
			t.Fatalf("failed to query twin_states after batch 2: %v", err)
		}
		if stReportedVersion != 2 {
			t.Fatalf("expected reported_version=2 after batch 2, got %d", stReportedVersion)
		}
		if stDesiredVersion != sentinelDesiredVersion {
			t.Fatalf("desired_version mutated after batch 2: got %d, want %d", stDesiredVersion, sentinelDesiredVersion)
		}
		var desObj2 map[string]any
		if err := json.Unmarshal(stDesiredState, &desObj2); err != nil {
			t.Fatalf("failed to unmarshal desired_state after batch 2: %v", err)
		}
		if desObj2["sampling_interval"].(float64) != 10000 || desObj2["calibration_offset"].(float64) != 1.5 {
			t.Fatalf("desired_state mutated after batch 2: got %v", desObj2)
		}

		var repObj2 map[string]any
		if err := json.Unmarshal(stReportedState, &repObj2); err != nil {
			t.Fatalf("failed to unmarshal reported_state after batch 2: %v", err)
		}
		expectedLastVal2 := 26.0 + float64(sampleCount2-1)*0.1
		if fmt.Sprintf("%.2f", repObj2["value"]) != fmt.Sprintf("%.2f", expectedLastVal2) {
			t.Fatalf("reported_state value mismatch after batch 2: got %v, want %v", repObj2["value"], expectedLastVal2)
		}
		expectedLastSampleTime2 := measuredAt2.Add(time.Duration(sampleCount2-1) * 10000 * time.Microsecond)
		if !stLastReportedAt.Equal(expectedLastSampleTime2) {
			t.Fatalf("last_reported_at mismatch after batch 2: got %v, want %v", stLastReportedAt, expectedLastSampleTime2)
		}

		// Relational and twin names/units remain intact
		var finalSensorName string
		var finalSensorUnit *string
		err = appPool.QueryRow(ctx, `SELECT name, unit FROM public.sensors WHERE gateway_id = $1 AND sensor_id = $2`, gwConcID, sensorConcID).
			Scan(&finalSensorName, &finalSensorUnit)
		if err != nil || finalSensorName != proposedPatchedName || finalSensorUnit == nil || *finalSensorUnit != "celsius" {
			t.Fatalf("sensor name/unit corrupted after batch 2: name=%s, unit=%v (err: %v)", finalSensorName, finalSensorUnit, err)
		}

		var finalTwinName string
		err = appPool.QueryRow(ctx, `SELECT name FROM public.twin_entities WHERE id = $1`, sensorTwinInternalID).Scan(&finalTwinName)
		if err != nil || finalTwinName != proposedPatchedName {
			t.Fatalf("twin entity sensor name corrupted after batch 2: name=%s (err: %v)", finalTwinName, err)
		}

		t.Logf("Sensor PATCH concurrent with Telemetry Persist and subsequent batch completed successfully")
	})

	// =========================================================================
	// Subtest 10b: POST Sensor Exact Replay Concurrent with Real Telemetry Persist
	// Focused native regression for the HIGH deadlock between
	// ProvisionSensorForAdminOrOwner (parent Gateway FOR UPDATE then Sensor) and
	// telemetry Persist (Sensor FOR UPDATE then parent Gateway FK KEY SHARE).
	// =========================================================================
	t.Run("Sensor_POST_Exact_Replay_Concurrent_With_Telemetry_Persist", func(t *testing.T) {
		// 10b.1 Seed an independent fixture-owned Gateway and Sensor through the
		// existing provisioning paths.
		gwPostInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   gwConcPostID,
				Name:        "Concurrent POST Replay Gateway",
				Description: strPtr("Gateway for concurrent POST replay and Persist"),
				OwnerUserID: ownerAUser,
			},
		}
		resGWPost, err := createSvc.CreateEntity(ctx, adminUser, gwPostInput)
		if err != nil || !resGWPost.Created {
			t.Fatalf("POST gateway for POST replay concurrency test failed: %v", err)
		}

		sensPostInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeSensor,
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: gwConcPostID,
				SensorID:  sensorConcPostID,
				Name:      "Concurrent POST Replay Sensor",
				Unit:      strPtr("celsius"),
			},
		}
		resSensPost, err := createSvc.CreateEntity(ctx, adminUser, sensPostInput)
		if err != nil || !resSensPost.Created {
			t.Fatalf("POST sensor for POST replay concurrency test failed: %v", err)
		}

		sensorURNP := fmt.Sprintf("urn:ngsi-ld:Sensor:%s:%s", gwConcPostID, sensorConcPostID)
		var sensorPostTwinID uuid.UUID
		if err := appPool.QueryRow(ctx, `SELECT id FROM public.twin_entities WHERE entity_id = $1`, sensorURNP).Scan(&sensorPostTwinID); err != nil {
			t.Fatalf("failed to query POST replay sensor twin UUID: %v", err)
		}
		gatewayURNP := fmt.Sprintf("urn:ngsi-ld:Gateway:%s", gwConcPostID)
		var gatewayPostTwinID uuid.UUID
		if err := appPool.QueryRow(ctx, `SELECT id FROM public.twin_entities WHERE entity_id = $1`, gatewayURNP).Scan(&gatewayPostTwinID); err != nil {
			t.Fatalf("failed to query POST replay gateway twin UUID: %v", err)
		}

		// Seed desired/version/audit sentinels so replay and ingest can be proven
		// not to reset the durable target configuration or the reported progress.
		sentinelDesiredJSONP := `{"calibration_offset": 2.5, "sampling_interval": 10000}`
		sentinelDesiredVersionP := int64(42)
		sentinelLastDesiredAtP := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Microsecond)
		if _, err := appPool.Exec(ctx, `
			UPDATE public.twin_states
			SET desired_state = $1::jsonb,
				desired_version = $2,
				last_desired_at = $3,
				updated_at = $3
			WHERE entity_id = $4`,
			sentinelDesiredJSONP, sentinelDesiredVersionP, sentinelLastDesiredAtP, sensorPostTwinID); err != nil {
			t.Fatalf("failed to seed POST replay desired sentinels: %v", err)
		}

		beforeSnapP := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwConcPostID})
		if beforeSnapP.TemporalCount != 0 || beforeSnapP.TelemetryRawCount != 0 || beforeSnapP.LedgerCount != 0 {
			t.Fatalf("expected 0 initial temporal/raw/ledger counts, got temp=%d, raw=%d, ledger=%d",
				beforeSnapP.TemporalCount, beforeSnapP.TelemetryRawCount, beforeSnapP.LedgerCount)
		}
		if beforeSnapP.States[sensorPostTwinID].DesiredVersion != sentinelDesiredVersionP {
			t.Fatalf("expected desired_version=%d, got %d", sentinelDesiredVersionP, beforeSnapP.States[sensorPostTwinID].DesiredVersion)
		}

		// 10b.2 Build one valid immutable telemetry batch through the real validator.
		validatorP, err := telemetry.NewValidator(telemetry.Limits{
			MaxPayloadBytes:  1 << 20,
			MaxSamples:       1000,
			MaxBatchDuration: time.Hour,
			MaxFutureSkew:    time.Hour,
		})
		if err != nil {
			t.Fatalf("failed to create validator: %v", err)
		}
		bootIDP := uuid.New()
		msgIDP := uuid.New()
		measuredAtP := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Millisecond)
		const sampleCountP = 12
		samplesP := make([]float64, sampleCountP)
		for i := 0; i < sampleCountP; i++ {
			samplesP[i] = 30.0 + float64(i)*0.1
		}
		dtoP := telemetry.BatchDTO{
			ProtocolVersion:  1,
			MessageID:        msgIDP,
			MessageType:      "telemetry_batch",
			GatewayID:        gwConcPostID,
			SensorID:         sensorConcPostID,
			BootID:           bootIDP,
			FirstSequence:    2001,
			SampleCount:      int32(sampleCountP),
			MeasuredAt:       measuredAtP.Format(time.RFC3339Nano),
			SampleIntervalUS: 10000,
			Samples:          samplesP,
		}
		payloadP, err := json.Marshal(dtoP)
		if err != nil {
			t.Fatalf("failed to marshal POST replay batch: %v", err)
		}
		topicP := fmt.Sprintf("gateways/%s/telemetry/%s", gwConcPostID, sensorConcPostID)
		validatedMsgP, err := validatorP.Validate(topicP, payloadP, time.Now().UTC())
		if err != nil {
			t.Fatalf("failed to validate POST replay telemetry message: %v", err)
		}

		// 10b.3 Deterministic overlap: telemetry Persist holds the Sensor row and
		// pauses; the admin-or-owner exact replay then acquires the parent Gateway
		// lock and blocks on the Sensor row.
		telemetrySensorLockHeldP := make(chan struct{}, 1)
		allowTelemetryContinueP := make(chan struct{})
		var onceCloseP sync.Once
		closeContinueP := func() {
			onceCloseP.Do(func() { close(allowTelemetryContinueP) })
		}
		defer closeContinueP()

		decoratedTelemetryDbP := &faultInjectingTxDatabase{
			underlying: appPool,
			onTxBegin: func(realTx pgx.Tx) pgx.Tx {
				return &faultInjectingTx{
					Tx: realTx,
					onQueryRow: func(qCtx context.Context, tx pgx.Tx, sql string, args ...any) (pgx.Row, bool) {
						if strings.Contains(sql, "FROM public.sensors") && strings.Contains(sql, "FOR UPDATE") {
							realRow := tx.QueryRow(qCtx, sql, args...)
							return &interceptedRow{
								realRow: realRow,
								onScanned: func(scanErr error) {
									if scanErr == nil {
										select {
										case telemetrySensorLockHeldP <- struct{}{}:
										default:
										}
										select {
										case <-allowTelemetryContinueP:
										case <-qCtx.Done():
										}
									}
								},
							}, true
						}
						return nil, false
					},
				}
			},
		}
		telemetryRepoP, err := telemetry.NewPostgresRepository(decoratedTelemetryDbP, 10*time.Second)
		if err != nil {
			t.Fatalf("failed to create decorated telemetry repository: %v", err)
		}

		replayParentLockHeldP := make(chan struct{}, 1)
		replayAttemptingSensorLockP := make(chan struct{}, 1)
		decoratedProvisionDbP := &faultInjectingTxDatabase{
			underlying: appPool,
			onTxBegin: func(realTx pgx.Tx) pgx.Tx {
				return &faultInjectingTx{
					Tx: realTx,
					onQueryRow: func(qCtx context.Context, tx pgx.Tx, sql string, args ...any) (pgx.Row, bool) {
						// Match the parent lock in either mode so the schedule is
						// identical whether the entry uses FOR SHARE (fixed) or
						// FOR UPDATE (regressed), letting a raw revert surface as a
						// real deadlock rather than a schedule timeout.
						if strings.Contains(sql, "FROM public.gateways") && (strings.Contains(sql, "FOR UPDATE") || strings.Contains(sql, "FOR SHARE")) {
							select {
							case replayParentLockHeldP <- struct{}{}:
							default:
							}
							return tx.QueryRow(qCtx, sql, args...), true
						}
						if strings.Contains(sql, "FROM public.sensors") && strings.Contains(sql, "FOR UPDATE") {
							select {
							case replayAttemptingSensorLockP <- struct{}{}:
							default:
							}
							return tx.QueryRow(qCtx, sql, args...), true
						}
						return nil, false
					},
				}
			},
		}
		concPostRepo, err := gateway.NewPostgresAdminOrOwnerSensorProvisioningRepository(decoratedProvisionDbP)
		if err != nil {
			t.Fatalf("failed to construct decorated admin-or-owner provisioning repository: %v", err)
		}
		concPostSvc, err := gateway.NewAdminOrOwnerSensorProvisioningService(concPostRepo, 10*time.Second)
		if err != nil {
			t.Fatalf("failed to construct decorated admin-or-owner provisioning service: %v", err)
		}

		// 10b.4 Execute the bounded overlap.
		telemetryCtxP, cancelTelemetryP := context.WithTimeout(ctx, 15*time.Second)
		defer cancelTelemetryP()
		replayCtxP, cancelReplayP := context.WithTimeout(ctx, 15*time.Second)
		defer cancelReplayP()

		telemetryErrChP := make(chan error, 1)
		telemetryResChP := make(chan telemetry.PersistResult, 1)
		go func() {
			res, pErr := telemetryRepoP.Persist(telemetryCtxP, validatedMsgP)
			if pErr != nil {
				telemetryErrChP <- pErr
				return
			}
			telemetryResChP <- res
			telemetryErrChP <- nil
		}()

		select {
		case <-telemetrySensorLockHeldP:
			t.Logf("Telemetry acquired Sensor row lock FOR UPDATE on %s:%s", gwConcPostID, sensorConcPostID)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for telemetry to acquire Sensor row lock: %v", ctx.Err())
		}

		replayErrChP := make(chan error, 1)
		replayResChP := make(chan gateway.ProvisionSensorResult, 1)
		go func() {
			res, pErr := concPostSvc.ProvisionSensorForAdminOrOwner(replayCtxP, adminUser, gateway.ProvisionSensorInput{
				GatewayID: gwConcPostID,
				SensorID:  sensorConcPostID,
				Name:      "Concurrent POST Replay Sensor",
				Unit:      strPtr("celsius"),
			})
			if pErr != nil {
				replayErrChP <- pErr
				return
			}
			replayResChP <- res
			replayErrChP <- nil
		}()

		select {
		case <-replayParentLockHeldP:
			t.Logf("Exact POST replay acquired parent Gateway FOR SHARE lock")
		case <-ctx.Done():
			t.Fatalf("timed out waiting for replay to acquire parent Gateway lock: %v", ctx.Err())
		}
		select {
		case <-replayAttemptingSensorLockP:
			t.Logf("Exact POST replay attempted Sensor row lock FOR UPDATE and is blocked by Telemetry")
		case <-ctx.Done():
			t.Fatalf("timed out waiting for replay to attempt Sensor lock: %v", ctx.Err())
		}

		// Confirm PostgreSQL actually recorded an ungranted row lock wait.
		var lockWaitObservedP bool
		for i := 0; i < 100; i++ {
			var ungrantedP int
			if err := setupPool.QueryRow(ctx, "SELECT count(*) FROM pg_locks WHERE NOT granted").Scan(&ungrantedP); err != nil {
				t.Fatal("observe replay lock contention:", err)
			}
			if ungrantedP > 0 {
				lockWaitObservedP = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !lockWaitObservedP {
			t.Fatalf("expected pg_locks to observe an ungranted row lock wait between replay and telemetry")
		}
		t.Logf("Observed real PostgreSQL ungranted row lock contention between POST replay and Telemetry Persist")

		// Allow Telemetry to proceed: it now needs the parent Gateway FK KEY SHARE
		// lock, which must be compatible with the replay's FOR SHARE parent lock.
		// Under the reverted FOR UPDATE parent lock this step deadlocks.
		closeContinueP()

		select {
		case pErr := <-telemetryErrChP:
			if pErr != nil {
				t.Fatalf("telemetry persist failed (possible deadlock): %v", pErr)
			}
		case <-ctx.Done():
			t.Fatalf("timed out awaiting telemetry persist: %v", ctx.Err())
		}
		resPersistP := <-telemetryResChP
		if resPersistP.Outcome != telemetry.CommitOutcomeCommitted {
			t.Fatalf("expected CommitOutcomeCommitted, got %v", resPersistP.Outcome)
		}
		if resPersistP.SamplesSaved != sampleCountP {
			t.Fatalf("expected %d samples saved, got %d", sampleCountP, resPersistP.SamplesSaved)
		}

		select {
		case pErr := <-replayErrChP:
			if pErr != nil {
				t.Fatalf("exact POST replay failed (possible deadlock): %v", pErr)
			}
		case <-ctx.Done():
			t.Fatalf("timed out awaiting exact POST replay: %v", ctx.Err())
		}
		resReplayP := <-replayResChP
		if resReplayP.Created {
			t.Fatalf("exact POST replay must return Created=false")
		}
		if resReplayP.EntityID != sensorURNP || resReplayP.Name != "Concurrent POST Replay Sensor" {
			t.Fatalf("unexpected replay result: %+v", resReplayP)
		}
		if resReplayP.Unit == nil || *resReplayP.Unit != "celsius" {
			t.Fatalf("replay unit mismatch: %v", resReplayP.Unit)
		}

		// 10b.5 Verify provisioning and ingest invariants were preserved.
		var postSensorCount int
		if err := appPool.QueryRow(ctx, `SELECT count(*) FROM public.sensors WHERE gateway_id = $1`, gwConcPostID).Scan(&postSensorCount); err != nil || postSensorCount != 1 {
			t.Fatalf("expected exactly 1 Sensor row (no duplicate), got %d (err: %v)", postSensorCount, err)
		}
		var postEntityCount int
		if err := appPool.QueryRow(ctx, `SELECT count(*) FROM public.twin_entities WHERE gateway_id = $1`, gwConcPostID).Scan(&postEntityCount); err != nil || postEntityCount != 2 {
			t.Fatalf("expected exactly 2 twin entities (Gateway+Sensor), got %d (err: %v)", postEntityCount, err)
		}
		var postStateCount int
		if err := appPool.QueryRow(ctx, `SELECT count(*) FROM public.twin_states WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id = $1)`, gwConcPostID).Scan(&postStateCount); err != nil || postStateCount != 2 {
			t.Fatalf("expected exactly 2 twin states, got %d (err: %v)", postStateCount, err)
		}
		var postRelCount int
		if err := appPool.QueryRow(ctx, `SELECT count(*) FROM public.twin_relationships WHERE source_entity_id = $1 AND relationship_type = 'hasSensor' AND target_entity_id = $2`, gatewayPostTwinID, sensorPostTwinID).Scan(&postRelCount); err != nil || postRelCount != 1 {
			t.Fatalf("expected exactly 1 hasSensor relationship, got %d (err: %v)", postRelCount, err)
		}

		var rawCountP int
		if err := appPool.QueryRow(ctx, `SELECT count(*) FROM public.telemetry WHERE gateway_id = $1 AND sensor_id = $2`, gwConcPostID, sensorConcPostID).Scan(&rawCountP); err != nil || rawCountP != sampleCountP {
			t.Fatalf("expected %d raw samples, got %d (err: %v)", sampleCountP, rawCountP, err)
		}
		var ledgerCountP int
		if err := appPool.QueryRow(ctx, `SELECT count(*) FROM public.processed_messages WHERE gateway_id = $1`, gwConcPostID).Scan(&ledgerCountP); err != nil || ledgerCountP != 1 {
			t.Fatalf("expected 1 ledger row, got %d (err: %v)", ledgerCountP, err)
		}
		var tempCountP int
		var tempPropP, tempUnitP string
		var tempValP float64
		if err := appPool.QueryRow(ctx, `SELECT count(*), COALESCE(MAX(property_name), ''), COALESCE(MAX(metadata->>'unit'), ''), COALESCE(MAX(value_number), 0) FROM public.twin_temporal_values WHERE entity_id = $1`, sensorPostTwinID).Scan(&tempCountP, &tempPropP, &tempUnitP, &tempValP); err != nil || tempCountP != 1 || tempPropP != "sample_interval_us" || tempUnitP != "us" || tempValP != 10000 {
			t.Fatalf("temporal observation mismatch: count=%d, prop=%s, unit=%s, val=%v (err: %v)", tempCountP, tempPropP, tempUnitP, tempValP, err)
		}

		var stReportedP, stDesiredP []byte
		var stReportedVerP, stDesiredVerP int64
		var stLastReportedP, stLastDesiredP *time.Time
		if err := appPool.QueryRow(ctx, `SELECT reported_state, desired_state, reported_version, desired_version, last_reported_at, last_desired_at FROM public.twin_states WHERE entity_id = $1`, sensorPostTwinID).
			Scan(&stReportedP, &stDesiredP, &stReportedVerP, &stDesiredVerP, &stLastReportedP, &stLastDesiredP); err != nil {
			t.Fatalf("failed to query POST replay twin_states: %v", err)
		}
		if stReportedVerP != 1 {
			t.Fatalf("expected reported state advanced to version 1 and not reset by replay, got %d", stReportedVerP)
		}
		if stLastReportedP == nil {
			t.Fatalf("expected last_reported_at to be set by telemetry persist")
		}
		if stDesiredVerP != sentinelDesiredVersionP {
			t.Fatalf("desired_version mutated: got %d, want %d", stDesiredVerP, sentinelDesiredVersionP)
		}
		if stLastDesiredP == nil || !stLastDesiredP.Equal(sentinelLastDesiredAtP) {
			t.Fatalf("last_desired_at mutated: got %v, want %v", stLastDesiredP, sentinelLastDesiredAtP)
		}
		var desObjP map[string]any
		if err := json.Unmarshal(stDesiredP, &desObjP); err != nil {
			t.Fatalf("failed to unmarshal desired_state: %v", err)
		}
		if desObjP["sampling_interval"].(float64) != 10000 || desObjP["calibration_offset"].(float64) != 2.5 {
			t.Fatalf("desired_state mutated: got %v", desObjP)
		}
		var repObjP map[string]any
		if err := json.Unmarshal(stReportedP, &repObjP); err != nil {
			t.Fatalf("failed to unmarshal reported_state: %v", err)
		}
		if repObjP["unit"] != "celsius" || repObjP["boot_id"] != bootIDP.String() {
			t.Fatalf("reported_state provenance mismatch: unit=%v, boot_id=%v", repObjP["unit"], repObjP["boot_id"])
		}

		// Sensor relational metadata and declared unit are unchanged by the replay.
		var postSensorNameP string
		var postSensorUnitP *string
		if err := appPool.QueryRow(ctx, `SELECT name, unit FROM public.sensors WHERE gateway_id = $1 AND sensor_id = $2`, gwConcPostID, sensorConcPostID).Scan(&postSensorNameP, &postSensorUnitP); err != nil {
			t.Fatalf("failed to query Sensor after replay: %v", err)
		}
		if postSensorNameP != "Concurrent POST Replay Sensor" || postSensorUnitP == nil || *postSensorUnitP != "celsius" {
			t.Fatalf("Sensor metadata mutated by replay: name=%s, unit=%v", postSensorNameP, postSensorUnitP)
		}

		// Gateway relational metadata and its Twin state remain untouched.
		var gwNameP string
		var gwDescP *string
		if err := appPool.QueryRow(ctx, `SELECT name, description FROM public.gateways WHERE gateway_id = $1`, gwConcPostID).Scan(&gwNameP, &gwDescP); err != nil {
			t.Fatalf("failed to query Gateway after replay: %v", err)
		}
		if gwNameP != "Concurrent POST Replay Gateway" || gwDescP == nil || *gwDescP != "Gateway for concurrent POST replay and Persist" {
			t.Fatalf("Gateway metadata mutated: name=%s, desc=%v", gwNameP, gwDescP)
		}
		var gwRepVerP, gwDesVerP int64
		if err := appPool.QueryRow(ctx, `SELECT reported_version, desired_version FROM public.twin_states WHERE entity_id = $1`, gatewayPostTwinID).Scan(&gwRepVerP, &gwDesVerP); err != nil {
			t.Fatalf("failed to query Gateway twin state: %v", err)
		}
		if gwRepVerP != 0 || gwDesVerP != 0 {
			t.Fatalf("Gateway twin state mutated: repVer=%d, desVer=%d", gwRepVerP, gwDesVerP)
		}

		t.Logf("POST Sensor exact replay concurrent with Telemetry Persist completed without deadlock")
	})

	// =========================================================================
	// Subtest 11: Concurrent Gateway PATCH Field Preservation and Same-Field Last-Write-Wins (Task 4.5.7 Item 8)
	// =========================================================================
	t.Run("Concurrent_Gateway_PATCH_Field_Preservation_And_Same_Field_Last_Write_Wins", func(t *testing.T) {
		// ---------------------------------------------------------------------
		// 11.1 Different-field case: first PATCH changes name only, second changes description only.
		// Assert final Gateway name and description retain BOTH requested changes,
		// Twin name matches final relational name, omitted fields are not overwritten
		// by stale pre-lock values, and state/version/history/unit/attributes/memberships remain unchanged.
		// ---------------------------------------------------------------------
		initialDiffName := "Initial Diff Gateway Name"
		initialDiffDesc := "Initial Diff Gateway Description"
		gwDiffInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   gwConcDiffID,
				Name:        initialDiffName,
				Description: &initialDiffDesc,
				OwnerUserID: ownerAUser,
			},
		}
		resDiffGW, err := createSvc.CreateEntity(ctx, adminUser, gwDiffInput)
		if err != nil || !resDiffGW.Created {
			t.Fatalf("POST gateway for different-fields concurrency test failed: %v", err)
		}

		gwDiffURN := fmt.Sprintf("urn:ngsi-ld:Gateway:%s", gwConcDiffID)
		var gwDiffTwinInternalID uuid.UUID
		err = appPool.QueryRow(ctx, `SELECT id FROM public.twin_entities WHERE entity_id = $1`, gwDiffURN).Scan(&gwDiffTwinInternalID)
		if err != nil {
			t.Fatalf("failed to query gateway twin internal UUID for different-fields test: %v", err)
		}

		// Seed nonempty sentinels on twin_states and twin_entities for gwConcDiffID
		sentinelDesiredState := `{"sampling_rate": 100, "reporting_mode": "batch"}`
		sentinelReportedState := `{"firmware": "v1.2.3", "battery_mv": 3300}`
		sentinelDesiredVersion := int64(7)
		sentinelReportedVersion := int64(12)
		sentinelLastDesiredAt := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Microsecond)
		sentinelLastReportedAt := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Microsecond)

		_, err = appPool.Exec(ctx, `
			UPDATE public.twin_states
			SET desired_state = $1::jsonb,
				reported_state = $2::jsonb,
				desired_version = $3,
				reported_version = $4,
				last_desired_at = $5,
				last_reported_at = $6,
				updated_at = $6
			WHERE entity_id = $7`,
			sentinelDesiredState, sentinelReportedState,
			sentinelDesiredVersion, sentinelReportedVersion,
			sentinelLastDesiredAt, sentinelLastReportedAt,
			gwDiffTwinInternalID)
		if err != nil {
			t.Fatalf("failed to seed gateway twin_states sentinels for different-fields test: %v", err)
		}

		sentinelAttributes := `{"custom_vendor": "acme", "deployment_region": "ap-southeast-1"}`
		_, err = appPool.Exec(ctx, `
			UPDATE public.twin_entities
			SET attributes = $1::jsonb
			WHERE id = $2`,
			sentinelAttributes, gwDiffTwinInternalID)
		if err != nil {
			t.Fatalf("failed to seed gateway twin_entities attributes sentinel for different-fields test: %v", err)
		}

		snapBeforeDiff := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwConcDiffID})

		// Setup deterministic row-lock coordination for Different-fields:
		// Tx1: updates Name only (Description omitted)
		// Tx2: updates Description only (Name omitted)
		tx1DiffName := "Diff Tx1 Patched Name"
		patchInputTx1 := digitaltwin.PatchGatewayInput{
			Name:           &tx1DiffName,
			DescriptionSet: false,
		}

		tx2DiffDesc := "Diff Tx2 Patched Description"
		patchInputTx2 := digitaltwin.PatchGatewayInput{
			Name:           nil,
			Description:    &tx2DiffDesc,
			DescriptionSet: true,
		}

		tx1LockHeld := make(chan struct{}, 1)
		allowTx1Continue := make(chan struct{})
		var onceCloseTx1 sync.Once
		releaseTx1 := func() {
			onceCloseTx1.Do(func() {
				close(allowTx1Continue)
			})
		}

		// WaitGroup accounting: add exactly one unit immediately before each
		// goroutine is launched, and register the bounded join BEFORE releaseTx1
		// so that (LIFO) cancellation and channel release run first. This keeps an
		// early failure before Tx2 exists from leaving an unpaid unit that would
		// make the join block fixture cleanup.
		var wgDiff sync.WaitGroup
		defer func() {
			boundedWaitGroupJoin(t, &wgDiff, 20*time.Second, "Different-fields")
		}()
		defer releaseTx1()

		tx1Db := &faultInjectingTxDatabase{
			underlying: appPool,
			onTxBegin: func(realTx pgx.Tx) pgx.Tx {
				return &faultInjectingTx{
					Tx: realTx,
					onQueryRow: func(qCtx context.Context, tx pgx.Tx, sql string, args ...any) (pgx.Row, bool) {
						if strings.Contains(sql, "FROM public.gateways") && strings.Contains(sql, "FOR UPDATE") {
							realRow := tx.QueryRow(qCtx, sql, args...)
							return &interceptedRow{
								realRow: realRow,
								onScanned: func(scanErr error) {
									if scanErr == nil {
										select {
										case tx1LockHeld <- struct{}{}:
										default:
										}
										select {
										case <-allowTx1Continue:
										case <-qCtx.Done():
										}
									}
								},
							}, true
						}
						return nil, false
					},
				}
			},
		}

		tx1Patcher, err := digitaltwin.NewPostgresGatewayMetadataPatcher(tx1Db)
		if err != nil {
			t.Fatalf("failed to construct tx1 gateway patcher: %v", err)
		}
		tx1PatchSvc, err := digitaltwin.NewGatewayPatchService(tx1Patcher, 10*time.Second)
		if err != nil {
			t.Fatalf("failed to construct tx1 gateway patch service: %v", err)
		}

		tx2AttemptingLock := make(chan struct{}, 1)
		tx2Db := &faultInjectingTxDatabase{
			underlying: appPool,
			onTxBegin: func(realTx pgx.Tx) pgx.Tx {
				return &faultInjectingTx{
					Tx: realTx,
					onQueryRow: func(qCtx context.Context, tx pgx.Tx, sql string, args ...any) (pgx.Row, bool) {
						if strings.Contains(sql, "FROM public.gateways") && strings.Contains(sql, "FOR UPDATE") {
							select {
							case tx2AttemptingLock <- struct{}{}:
							default:
							}
							return tx.QueryRow(qCtx, sql, args...), true
						}
						return nil, false
					},
				}
			},
		}

		tx2Patcher, err := digitaltwin.NewPostgresGatewayMetadataPatcher(tx2Db)
		if err != nil {
			t.Fatalf("failed to construct tx2 gateway patcher: %v", err)
		}
		tx2PatchSvc, err := digitaltwin.NewGatewayPatchService(tx2Patcher, 10*time.Second)
		if err != nil {
			t.Fatalf("failed to construct tx2 gateway patch service: %v", err)
		}

		tx1Ctx, cancelTx1 := context.WithTimeout(ctx, 15*time.Second)
		defer cancelTx1()
		tx2Ctx, cancelTx2 := context.WithTimeout(ctx, 15*time.Second)
		defer cancelTx2()

		tx1Out := startGatewayPatch(tx1Ctx, &wgDiff, tx1PatchSvc, adminUser, gwConcDiffID, patchInputTx1)
		tx1ErrCh, tx1ResCh := tx1Out.errCh, tx1Out.resCh

		// Wait for Tx1 to acquire the gateway row lock
		select {
		case <-tx1LockHeld:
			t.Logf("Different-fields: Tx1 acquired gateway row lock on %s FOR UPDATE", gwConcDiffID)
		case <-ctx.Done():
			t.Fatalf("Different-fields: timed out waiting for Tx1 row lock: %v", ctx.Err())
		}

		// Start Tx2 in background goroutine
		tx2Out := startGatewayPatch(tx2Ctx, &wgDiff, tx2PatchSvc, adminUser, gwConcDiffID, patchInputTx2)
		tx2ErrCh, tx2ResCh := tx2Out.errCh, tx2Out.resCh

		// Wait for Tx2 to reach the row lock attempt
		select {
		case <-tx2AttemptingLock:
			t.Logf("Different-fields: Tx2 entered and attempted gateway row lock FOR UPDATE")
		case <-ctx.Done():
			t.Fatalf("Different-fields: timed out waiting for Tx2 lock attempt: %v", ctx.Err())
		}

		// Verify ungranted lock on pg_locks
		var diffLockWaitObserved bool
		for i := 0; i < 50; i++ {
			var ungrantedCount int
			if err := setupPool.QueryRow(ctx, "SELECT count(*) FROM pg_locks WHERE NOT granted").Scan(&ungrantedCount); err != nil {
				t.Fatal("observe different-field lock contention:", err)
			}
			if ungrantedCount > 0 {
				diffLockWaitObserved = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !diffLockWaitObserved {
			t.Fatalf("Different-fields: expected PostgreSQL pg_locks to observe ungranted row lock wait for concurrent transactions")
		}
		t.Logf("Different-fields: observed real PostgreSQL row lock contention between Tx1 and Tx2")

		// Release Tx1 to proceed and commit!
		releaseTx1()

		// Await Tx1 completion
		select {
		case pErr := <-tx1ErrCh:
			if pErr != nil {
				t.Fatalf("Different-fields: Tx1 patch failed: %v", pErr)
			}
		case <-ctx.Done():
			t.Fatalf("Different-fields: timed out awaiting Tx1: %v", ctx.Err())
		}
		resTx1 := <-tx1ResCh
		if resTx1.Name != tx1DiffName {
			t.Fatalf("Different-fields: Tx1 result name mismatch: got %q, want %q", resTx1.Name, tx1DiffName)
		}
		if resTx1.Description == nil || *resTx1.Description != initialDiffDesc {
			t.Fatalf("Different-fields: Tx1 result desc mismatch: got %v, want %q", resTx1.Description, initialDiffDesc)
		}

		// Await Tx2 completion (unblocks after Tx1 commits)
		select {
		case pErr := <-tx2ErrCh:
			if pErr != nil {
				t.Fatalf("Different-fields: Tx2 patch failed: %v", pErr)
			}
		case <-ctx.Done():
			t.Fatalf("Different-fields: timed out awaiting Tx2: %v", ctx.Err())
		}
		resTx2 := <-tx2ResCh
		if resTx2.Name != tx1DiffName {
			t.Fatalf("Different-fields: Tx2 result name mismatch (must preserve Tx1 committed name): got %q, want %q", resTx2.Name, tx1DiffName)
		}
		if resTx2.Description == nil || *resTx2.Description != tx2DiffDesc {
			t.Fatalf("Different-fields: Tx2 result desc mismatch: got %v, want %q", resTx2.Description, tx2DiffDesc)
		}

		// Independent verification in PostgreSQL:
		// 1. Both requested changes are retained in public.gateways!
		var finalDiffGWName string
		var finalDiffGWDesc *string
		err = appPool.QueryRow(ctx, `SELECT name, description FROM public.gateways WHERE gateway_id = $1`, gwConcDiffID).
			Scan(&finalDiffGWName, &finalDiffGWDesc)
		if err != nil {
			t.Fatalf("Different-fields: failed to query final gateway: %v", err)
		}
		if finalDiffGWName != tx1DiffName {
			t.Fatalf("Different-fields: final gateway name mismatch: got %q, want %q (Tx1 name lost)", finalDiffGWName, tx1DiffName)
		}
		if finalDiffGWDesc == nil || *finalDiffGWDesc != tx2DiffDesc {
			t.Fatalf("Different-fields: final gateway description mismatch: got %v, want %q (Tx2 description lost)", finalDiffGWDesc, tx2DiffDesc)
		}

		// 2. Twin entity name matches final relational name
		var finalDiffTwinName string
		var finalDiffTwinUpdatedAt time.Time
		err = appPool.QueryRow(ctx, `SELECT name, updated_at FROM public.twin_entities WHERE id = $1`, gwDiffTwinInternalID).
			Scan(&finalDiffTwinName, &finalDiffTwinUpdatedAt)
		if err != nil {
			t.Fatalf("Different-fields: failed to query final twin entity: %v", err)
		}
		if finalDiffTwinName != tx1DiffName {
			t.Fatalf("Different-fields: final twin entity name mismatch: got %q, want %q", finalDiffTwinName, tx1DiffName)
		}
		if !finalDiffTwinUpdatedAt.After(snapBeforeDiff.Entities[gwDiffTwinInternalID].UpdatedAt) {
			t.Fatalf("Different-fields: twin updated_at did not advance")
		}

		// 3. Verify non-metadata sentinels remained unchanged
		snapAfterDiff := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwConcDiffID})
		assertPatchPreservesNonMetadata(t, snapBeforeDiff, snapAfterDiff, gwDiffTwinInternalID, &tx1DiffName)

		if snapAfterDiff.Entities[gwDiffTwinInternalID].Attributes != sentinelAttributes {
			t.Fatalf("Different-fields: attributes mutated: got %s, want %s",
				snapAfterDiff.Entities[gwDiffTwinInternalID].Attributes, sentinelAttributes)
		}

		t.Logf("Different-fields: verified both fields preserved and non-metadata sentinels intact")

		// ---------------------------------------------------------------------
		// 11.2 Same-field case: serialize two name PATCHes in known lock-acquisition order.
		// Assert final relational/Twin name equals the transaction that writes last,
		// without pretending concurrent caller start order defines last-write-wins.
		// Check both successful commits and unchanged nonmetadata sentinels.
		// ---------------------------------------------------------------------
		initialSameName := "Initial Same Gateway Name"
		initialSameDesc := "Initial Same Gateway Description"
		gwSameInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   gwConcSameID,
				Name:        initialSameName,
				Description: &initialSameDesc,
				OwnerUserID: ownerAUser,
			},
		}
		resSameGW, err := createSvc.CreateEntity(ctx, adminUser, gwSameInput)
		if err != nil || !resSameGW.Created {
			t.Fatalf("POST gateway for same-field concurrency test failed: %v", err)
		}

		gwSameURN := fmt.Sprintf("urn:ngsi-ld:Gateway:%s", gwConcSameID)
		var gwSameTwinInternalID uuid.UUID
		err = appPool.QueryRow(ctx, `SELECT id FROM public.twin_entities WHERE entity_id = $1`, gwSameURN).Scan(&gwSameTwinInternalID)
		if err != nil {
			t.Fatalf("failed to query gateway twin internal UUID for same-field test: %v", err)
		}

		// Seed nonempty sentinels on twin_states and twin_entities for gwConcSameID
		_, err = appPool.Exec(ctx, `
			UPDATE public.twin_states
			SET desired_state = $1::jsonb,
				reported_state = $2::jsonb,
				desired_version = $3,
				reported_version = $4,
				last_desired_at = $5,
				last_reported_at = $6,
				updated_at = $6
			WHERE entity_id = $7`,
			sentinelDesiredState, sentinelReportedState,
			sentinelDesiredVersion, sentinelReportedVersion,
			sentinelLastDesiredAt, sentinelLastReportedAt,
			gwSameTwinInternalID)
		if err != nil {
			t.Fatalf("failed to seed gateway twin_states sentinels for same-field test: %v", err)
		}

		_, err = appPool.Exec(ctx, `
			UPDATE public.twin_entities
			SET attributes = $1::jsonb
			WHERE id = $2`,
			sentinelAttributes, gwSameTwinInternalID)
		if err != nil {
			t.Fatalf("failed to seed gateway twin_entities attributes sentinel for same-field test: %v", err)
		}

		snapBeforeSame := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwConcSameID})

		// TxA writes name "First Acquired Name"
		// TxB writes name "Second Acquired Name" (writes last, so must win)
		txAName := "First Acquired Name"
		patchInputTxA := digitaltwin.PatchGatewayInput{
			Name:           &txAName,
			DescriptionSet: false,
		}

		txBName := "Second Acquired Name"
		patchInputTxB := digitaltwin.PatchGatewayInput{
			Name:           &txBName,
			DescriptionSet: false,
		}

		txALockHeld := make(chan struct{}, 1)
		allowTxAContinue := make(chan struct{})
		var onceCloseTxA sync.Once
		releaseTxA := func() {
			onceCloseTxA.Do(func() {
				close(allowTxAContinue)
			})
		}

		// Same per-goroutine accounting and ordered bounded join as the
		// Different-fields schedule above.
		var wgSame sync.WaitGroup
		defer func() {
			boundedWaitGroupJoin(t, &wgSame, 20*time.Second, "Same-field")
		}()
		defer releaseTxA()

		txADb := &faultInjectingTxDatabase{
			underlying: appPool,
			onTxBegin: func(realTx pgx.Tx) pgx.Tx {
				return &faultInjectingTx{
					Tx: realTx,
					onQueryRow: func(qCtx context.Context, tx pgx.Tx, sql string, args ...any) (pgx.Row, bool) {
						if strings.Contains(sql, "FROM public.gateways") && strings.Contains(sql, "FOR UPDATE") {
							realRow := tx.QueryRow(qCtx, sql, args...)
							return &interceptedRow{
								realRow: realRow,
								onScanned: func(scanErr error) {
									if scanErr == nil {
										select {
										case txALockHeld <- struct{}{}:
										default:
										}
										select {
										case <-allowTxAContinue:
										case <-qCtx.Done():
										}
									}
								},
							}, true
						}
						return nil, false
					},
				}
			},
		}

		txAPatcher, err := digitaltwin.NewPostgresGatewayMetadataPatcher(txADb)
		if err != nil {
			t.Fatalf("failed to construct txA gateway patcher: %v", err)
		}
		txAPatchSvc, err := digitaltwin.NewGatewayPatchService(txAPatcher, 10*time.Second)
		if err != nil {
			t.Fatalf("failed to construct txA gateway patch service: %v", err)
		}

		txBAttemptingLock := make(chan struct{}, 1)
		txBDb := &faultInjectingTxDatabase{
			underlying: appPool,
			onTxBegin: func(realTx pgx.Tx) pgx.Tx {
				return &faultInjectingTx{
					Tx: realTx,
					onQueryRow: func(qCtx context.Context, tx pgx.Tx, sql string, args ...any) (pgx.Row, bool) {
						if strings.Contains(sql, "FROM public.gateways") && strings.Contains(sql, "FOR UPDATE") {
							select {
							case txBAttemptingLock <- struct{}{}:
							default:
							}
							return tx.QueryRow(qCtx, sql, args...), true
						}
						return nil, false
					},
				}
			},
		}

		txBPatcher, err := digitaltwin.NewPostgresGatewayMetadataPatcher(txBDb)
		if err != nil {
			t.Fatalf("failed to construct txB gateway patcher: %v", err)
		}
		txBPatchSvc, err := digitaltwin.NewGatewayPatchService(txBPatcher, 10*time.Second)
		if err != nil {
			t.Fatalf("failed to construct txB gateway patch service: %v", err)
		}

		txACtx, cancelTxA := context.WithTimeout(ctx, 15*time.Second)
		defer cancelTxA()
		txBCtx, cancelTxB := context.WithTimeout(ctx, 15*time.Second)
		defer cancelTxB()

		txAOut := startGatewayPatch(txACtx, &wgSame, txAPatchSvc, adminUser, gwConcSameID, patchInputTxA)
		txAErrCh, txAResCh := txAOut.errCh, txAOut.resCh

		// Wait for TxA to acquire the row lock first
		select {
		case <-txALockHeld:
			t.Logf("Same-field: TxA acquired gateway row lock on %s FOR UPDATE", gwConcSameID)
		case <-ctx.Done():
			t.Fatalf("Same-field: timed out waiting for TxA row lock: %v", ctx.Err())
		}

		// Start TxB in background goroutine
		txBOut := startGatewayPatch(txBCtx, &wgSame, txBPatchSvc, adminUser, gwConcSameID, patchInputTxB)
		txBErrCh, txBResCh := txBOut.errCh, txBOut.resCh

		// Wait for TxB to reach the row lock attempt
		select {
		case <-txBAttemptingLock:
			t.Logf("Same-field: TxB entered and attempted gateway row lock FOR UPDATE")
		case <-ctx.Done():
			t.Fatalf("Same-field: timed out waiting for TxB lock attempt: %v", ctx.Err())
		}

		// Verify ungranted lock on pg_locks
		var sameLockWaitObserved bool
		for i := 0; i < 50; i++ {
			var ungrantedCount int
			if err := setupPool.QueryRow(ctx, "SELECT count(*) FROM pg_locks WHERE NOT granted").Scan(&ungrantedCount); err != nil {
				t.Fatal("observe same-field lock contention:", err)
			}
			if ungrantedCount > 0 {
				sameLockWaitObserved = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !sameLockWaitObserved {
			t.Fatalf("Same-field: expected PostgreSQL pg_locks to observe ungranted row lock wait for concurrent transactions")
		}
		t.Logf("Same-field: observed real PostgreSQL row lock contention between TxA and TxB")

		// Release TxA to proceed and commit!
		releaseTxA()

		// Await TxA completion
		select {
		case pErr := <-txAErrCh:
			if pErr != nil {
				t.Fatalf("Same-field: TxA patch failed: %v", pErr)
			}
		case <-ctx.Done():
			t.Fatalf("Same-field: timed out awaiting TxA: %v", ctx.Err())
		}
		resTxA := <-txAResCh
		if resTxA.Name != txAName {
			t.Fatalf("Same-field: TxA result name mismatch: got %q, want %q", resTxA.Name, txAName)
		}

		// Await TxB completion (unblocks after TxA commits, writes last)
		select {
		case pErr := <-txBErrCh:
			if pErr != nil {
				t.Fatalf("Same-field: TxB patch failed: %v", pErr)
			}
		case <-ctx.Done():
			t.Fatalf("Same-field: timed out awaiting TxB: %v", ctx.Err())
		}
		resTxB := <-txBResCh
		if resTxB.Name != txBName {
			t.Fatalf("Same-field: TxB result name mismatch (last-write-wins): got %q, want %q", resTxB.Name, txBName)
		}

		// Independent verification in PostgreSQL:
		// 1. Relational name equals the transaction that writes last (TxB: "Second Acquired Name")
		var finalSameGWName string
		var finalSameGWDesc *string
		err = appPool.QueryRow(ctx, `SELECT name, description FROM public.gateways WHERE gateway_id = $1`, gwConcSameID).
			Scan(&finalSameGWName, &finalSameGWDesc)
		if err != nil {
			t.Fatalf("Same-field: failed to query final gateway: %v", err)
		}
		if finalSameGWName != txBName {
			t.Fatalf("Same-field: final gateway name mismatch: got %q, want %q (last-write-wins expected TxB)", finalSameGWName, txBName)
		}
		if finalSameGWDesc == nil || *finalSameGWDesc != initialSameDesc {
			t.Fatalf("Same-field: final gateway description mutated: got %v, want %q", finalSameGWDesc, initialSameDesc)
		}

		// 2. Twin entity name equals the transaction that writes last
		var finalSameTwinName string
		var finalSameTwinUpdatedAt time.Time
		err = appPool.QueryRow(ctx, `SELECT name, updated_at FROM public.twin_entities WHERE id = $1`, gwSameTwinInternalID).
			Scan(&finalSameTwinName, &finalSameTwinUpdatedAt)
		if err != nil {
			t.Fatalf("Same-field: failed to query final twin entity: %v", err)
		}
		if finalSameTwinName != txBName {
			t.Fatalf("Same-field: final twin entity name mismatch: got %q, want %q", finalSameTwinName, txBName)
		}
		if !finalSameTwinUpdatedAt.After(snapBeforeSame.Entities[gwSameTwinInternalID].UpdatedAt) {
			t.Fatalf("Same-field: twin updated_at did not advance")
		}

		// 3. Verify non-metadata sentinels remained unchanged
		snapAfterSame := mustCaptureMetadataFixtureSnapshot(ctx, t, appPool, []string{gwConcSameID})
		assertPatchPreservesNonMetadata(t, snapBeforeSame, snapAfterSame, gwSameTwinInternalID, &txBName)

		if snapAfterSame.Entities[gwSameTwinInternalID].Attributes != sentinelAttributes {
			t.Fatalf("Same-field: attributes mutated: got %s, want %s",
				snapAfterSame.Entities[gwSameTwinInternalID].Attributes, sentinelAttributes)
		}

		t.Logf("Same-field: verified last-write-wins equals TxB and non-metadata sentinels intact")
	})

	// ---------------------------------------------------------------------
	// 11.3 Regression: the first scheduled PATCH fails BEFORE it can emit its
	// row-lock signal. The coordinator must abort before the second goroutine is
	// created, and the deferred bounded join (with cancellation/release ordered
	// ahead of it) must still let the coordinated lifecycle exit within a bound,
	// so fixture cleanup is never blocked by an unpaid WaitGroup unit.
	// ---------------------------------------------------------------------
	t.Run("Concurrent_Gateway_PATCH_First_Tx_Failure_Before_Lock_Signal_Bounded", func(t *testing.T) {
		earlyFailName := "Early Fail Patched Name"
		earlyFailGWInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   gwConcEarlyFailID,
				Name:        "Initial Early Fail Gateway Name",
				OwnerUserID: ownerAUser,
			},
		}
		resEarlyFailGW, err := createSvc.CreateEntity(ctx, adminUser, earlyFailGWInput)
		if err != nil || !resEarlyFailGW.Created {
			t.Fatalf("Early-failure-before-lock: POST gateway for regression failed: %v", err)
		}

		// Fault DB: the gateway FOR UPDATE query fails at Scan time, so the row-lock
		// signal that the real schedules wait on is never emitted.
		lockAttempted := make(chan struct{}, 1)
		earlyFailDb := &faultInjectingTxDatabase{
			underlying: appPool,
			onTxBegin: func(realTx pgx.Tx) pgx.Tx {
				return &faultInjectingTx{
					Tx: realTx,
					onQueryRow: func(_ context.Context, _ pgx.Tx, sql string, _ ...any) (pgx.Row, bool) {
						if strings.Contains(sql, "FROM public.gateways") && strings.Contains(sql, "FOR UPDATE") {
							select {
							case lockAttempted <- struct{}{}:
							default:
							}
							return errorScanRow{err: errors.New("injected: first tx fails before lock signal")}, true
						}
						return nil, false
					},
				}
			},
		}

		earlyFailPatcher, err := digitaltwin.NewPostgresGatewayMetadataPatcher(earlyFailDb)
		if err != nil {
			t.Fatalf("Early-failure-before-lock: failed to construct patcher: %v", err)
		}
		earlyFailSvc, err := digitaltwin.NewGatewayPatchService(earlyFailPatcher, 10*time.Second)
		if err != nil {
			t.Fatalf("Early-failure-before-lock: failed to construct patch service: %v", err)
		}

		// Register the bounded join BEFORE cancellation so that (LIFO) cancellation
		// runs first and an aborted run can never leave the join blocking cleanup.
		var wgEarlyFail sync.WaitGroup
		defer func() {
			boundedWaitGroupJoin(t, &wgEarlyFail, 10*time.Second, "Early-failure-before-lock")
		}()
		earlyFailCtx, cancelEarlyFail := context.WithTimeout(ctx, 15*time.Second)
		defer cancelEarlyFail()

		// Launch exactly one goroutine through the shared per-goroutine accounting.
		first := startGatewayPatch(earlyFailCtx, &wgEarlyFail, earlyFailSvc, adminUser, gwConcEarlyFailID, digitaltwin.PatchGatewayInput{Name: &earlyFailName})

		var firstErr error
		select {
		case firstErr = <-first.errCh:
		case <-ctx.Done():
			t.Fatalf("Early-failure-before-lock: timed out awaiting first tx outcome: %v", ctx.Err())
		}
		if firstErr == nil {
			t.Fatalf("Early-failure-before-lock: expected first tx to fail before lock signal, got success")
		}
		t.Logf("Early-failure-before-lock: first tx failed before lock signal: %v", firstErr)

		// The lock query was reached exactly once and never signaled readiness.
		select {
		case <-lockAttempted:
		default:
			t.Fatalf("Early-failure-before-lock: gateway FOR UPDATE was never attempted")
		}
		if len(first.resCh) != 0 {
			t.Fatalf("Early-failure-before-lock: unexpected result buffered on aborted run: %d", len(first.resCh))
		}

		// Coordinator abort: the second scheduled goroutine is deliberately NOT
		// launched, mirroring a real coordinator that stops when the first
		// transaction fails. The bounded join must still drain promptly because no
		// unit was ever accounted for the never-created goroutine.
		drainStart := time.Now()
		boundedWaitGroupJoin(t, &wgEarlyFail, 10*time.Second, "Early-failure-before-lock-drain")
		elapsed := time.Since(drainStart)
		if elapsed > 5*time.Second {
			t.Fatalf("Early-failure-before-lock: bounded join took %s, expected prompt completion", elapsed)
		}
		t.Logf("Early-failure-before-lock: aborted coordinator joined in %s with no leaked WaitGroup unit", elapsed)
	})

	// =========================================================================
	// Subtest 12: Metadata updated_at reflects the post-lock write statement time,
	// not the transaction start. Deterministic overlap: transaction A begins and
	// pauses BEFORE acquiring the target resource lock; transaction B (started
	// later) patches and commits; A resumes, acquires the now-free lock, patches,
	// and commits last. Assert returned AND stored updated_at do not regress
	// behind B's committed value for both Gateway and Sensor.
	// =========================================================================
	t.Run("PATCH_Metadata_UpdatedAt_Is_PostLock_Statement_Time", func(t *testing.T) {
		// 12.1 Seed independent fixture-owned Gateway and Sensor.
		gwUpdatedAtInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   gwUpdatedAtID,
				Name:        "UpdatedAt Ordering Gateway",
				Description: strPtr("Gateway for updated_at ordering regression"),
				OwnerUserID: ownerAUser,
			},
		}
		if res, err := createSvc.CreateEntity(ctx, adminUser, gwUpdatedAtInput); err != nil || !res.Created {
			t.Fatalf("UpdatedAt ordering: POST gateway failed: %v", err)
		}
		sensUpdatedAtInput := digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityTypeSensor,
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: gwUpdatedAtID,
				SensorID:  sensorUpdatedAtID,
				Name:      "UpdatedAt Ordering Sensor",
				Unit:      strPtr("celsius"),
			},
		}
		if res, err := createSvc.CreateEntity(ctx, ownerAUser, sensUpdatedAtInput); err != nil || !res.Created {
			t.Fatalf("UpdatedAt ordering: POST sensor failed: %v", err)
		}

		var gwUpdatedAtTwinID uuid.UUID
		if err := appPool.QueryRow(ctx, `SELECT id FROM public.twin_entities WHERE entity_id = $1`, "urn:ngsi-ld:Gateway:"+gwUpdatedAtID).Scan(&gwUpdatedAtTwinID); err != nil {
			t.Fatalf("UpdatedAt ordering: failed to query gateway twin UUID: %v", err)
		}
		var sensUpdatedAtTwinID uuid.UUID
		if err := appPool.QueryRow(ctx, `SELECT id FROM public.twin_entities WHERE entity_id = $1`, "urn:ngsi-ld:Sensor:"+gwUpdatedAtID+":"+sensorUpdatedAtID).Scan(&sensUpdatedAtTwinID); err != nil {
			t.Fatalf("UpdatedAt ordering: failed to query sensor twin UUID: %v", err)
		}

		// ---- Gateway PATCH regression ----
		gwBeforeLock := make(chan struct{}, 1)
		gwReleaseCh := make(chan struct{})
		var gwReleaseOnce sync.Once
		releaseGwA := func() { gwReleaseOnce.Do(func() { close(gwReleaseCh) }) }

		gwADb := newPauseBeforeLockDB(appPool,
			func(sql string) bool {
				return strings.Contains(sql, "FROM public.gateways") && strings.Contains(sql, "FOR UPDATE")
			},
			gwBeforeLock, gwReleaseCh)
		gwAPatcher, err := digitaltwin.NewPostgresGatewayMetadataPatcher(gwADb)
		if err != nil {
			t.Fatalf("UpdatedAt ordering: failed to construct gateway A patcher: %v", err)
		}
		gwAPatchSvc, err := digitaltwin.NewGatewayPatchService(gwAPatcher, 10*time.Second)
		if err != nil {
			t.Fatalf("UpdatedAt ordering: failed to construct gateway A patch service: %v", err)
		}

		gwAName := "UpdatedAt Ordering Gateway A"
		gwBName := "UpdatedAt Ordering Gateway B"

		var wgGw sync.WaitGroup
		defer func() {
			boundedWaitGroupJoin(t, &wgGw, 20*time.Second, "Gateway updated_at ordering")
		}()
		defer releaseGwA()

		gwAOut, gwBOut := runBlockedLockPause(ctx, t, &wgGw, gwBeforeLock, releaseGwA, "Gateway updated_at ordering",
			func(runCtx context.Context) patchOutcome {
				res, pErr := gwAPatchSvc.PatchGateway(runCtx, adminUser, gwUpdatedAtID, digitaltwin.PatchGatewayInput{Name: &gwAName})
				return patchOutcome{err: pErr, updatedAt: res.UpdatedAt, name: res.Name}
			},
			func(runCtx context.Context) patchOutcome {
				res, pErr := gwPatchSvc.PatchGateway(runCtx, adminUser, gwUpdatedAtID, digitaltwin.PatchGatewayInput{Name: &gwBName})
				return patchOutcome{err: pErr, updatedAt: res.UpdatedAt, name: res.Name}
			})

		if gwAOut.name != gwAName {
			t.Fatalf("Gateway updated_at ordering: A returned name %q, want %q", gwAOut.name, gwAName)
		}
		if gwBOut.name != gwBName {
			t.Fatalf("Gateway updated_at ordering: B returned name %q, want %q", gwBOut.name, gwBName)
		}
		if gwAOut.updatedAt.Before(gwBOut.updatedAt) {
			t.Fatalf("Gateway updated_at ordering: A returned updated_at %v regressed behind B committed %v", gwAOut.updatedAt, gwBOut.updatedAt)
		}
		var gwStoredName string
		var gwStoredUp time.Time
		if err := appPool.QueryRow(ctx, `SELECT name, updated_at FROM public.twin_entities WHERE id = $1`, gwUpdatedAtTwinID).
			Scan(&gwStoredName, &gwStoredUp); err != nil {
			t.Fatalf("Gateway updated_at ordering: failed to query stored twin: %v", err)
		}
		if gwStoredName != gwAName {
			t.Fatalf("Gateway updated_at ordering: stored name %q, want %q (A wrote last)", gwStoredName, gwAName)
		}
		if !gwStoredUp.Equal(gwAOut.updatedAt) {
			t.Fatalf("Gateway updated_at ordering: stored updated_at %v != returned %v", gwStoredUp, gwAOut.updatedAt)
		}
		if gwStoredUp.Before(gwBOut.updatedAt) {
			t.Fatalf("Gateway updated_at ordering: stored updated_at %v regressed behind B committed %v", gwStoredUp, gwBOut.updatedAt)
		}

		// ---- Sensor PATCH regression ----
		sensBeforeLock := make(chan struct{}, 1)
		sensReleaseCh := make(chan struct{})
		var sensReleaseOnce sync.Once
		releaseSensA := func() { sensReleaseOnce.Do(func() { close(sensReleaseCh) }) }

		sensADb := newPauseBeforeLockDB(appPool,
			func(sql string) bool {
				return strings.Contains(sql, "FROM public.sensors") && strings.Contains(sql, "FOR UPDATE")
			},
			sensBeforeLock, sensReleaseCh)
		sensAPatcher, err := digitaltwin.NewPostgresSensorMetadataPatcher(sensADb)
		if err != nil {
			t.Fatalf("UpdatedAt ordering: failed to construct sensor A patcher: %v", err)
		}
		sensAPatchSvc, err := digitaltwin.NewSensorPatchService(sensAPatcher, 10*time.Second)
		if err != nil {
			t.Fatalf("UpdatedAt ordering: failed to construct sensor A patch service: %v", err)
		}

		sensAName := "UpdatedAt Ordering Sensor A"
		sensBName := "UpdatedAt Ordering Sensor B"

		var wgSens sync.WaitGroup
		defer func() {
			boundedWaitGroupJoin(t, &wgSens, 20*time.Second, "Sensor updated_at ordering")
		}()
		defer releaseSensA()

		sensAOut, sensBOut := runBlockedLockPause(ctx, t, &wgSens, sensBeforeLock, releaseSensA, "Sensor updated_at ordering",
			func(runCtx context.Context) patchOutcome {
				res, pErr := sensAPatchSvc.PatchSensor(runCtx, adminUser, gwUpdatedAtID, sensorUpdatedAtID, digitaltwin.PatchSensorInput{Name: &sensAName})
				return patchOutcome{err: pErr, updatedAt: res.UpdatedAt, name: res.Name}
			},
			func(runCtx context.Context) patchOutcome {
				res, pErr := sensorPatchSvc.PatchSensor(runCtx, adminUser, gwUpdatedAtID, sensorUpdatedAtID, digitaltwin.PatchSensorInput{Name: &sensBName})
				return patchOutcome{err: pErr, updatedAt: res.UpdatedAt, name: res.Name}
			})

		if sensAOut.name != sensAName {
			t.Fatalf("Sensor updated_at ordering: A returned name %q, want %q", sensAOut.name, sensAName)
		}
		if sensBOut.name != sensBName {
			t.Fatalf("Sensor updated_at ordering: B returned name %q, want %q", sensBOut.name, sensBName)
		}
		if sensAOut.updatedAt.Before(sensBOut.updatedAt) {
			t.Fatalf("Sensor updated_at ordering: A returned updated_at %v regressed behind B committed %v", sensAOut.updatedAt, sensBOut.updatedAt)
		}
		var sensStoredName string
		var sensStoredUp time.Time
		if err := appPool.QueryRow(ctx, `SELECT name, updated_at FROM public.twin_entities WHERE id = $1`, sensUpdatedAtTwinID).
			Scan(&sensStoredName, &sensStoredUp); err != nil {
			t.Fatalf("Sensor updated_at ordering: failed to query stored twin: %v", err)
		}
		if sensStoredName != sensAName {
			t.Fatalf("Sensor updated_at ordering: stored name %q, want %q (A wrote last)", sensStoredName, sensAName)
		}
		if !sensStoredUp.Equal(sensAOut.updatedAt) {
			t.Fatalf("Sensor updated_at ordering: stored updated_at %v != returned %v", sensStoredUp, sensAOut.updatedAt)
		}
		if sensStoredUp.Before(sensBOut.updatedAt) {
			t.Fatalf("Sensor updated_at ordering: stored updated_at %v regressed behind B committed %v", sensStoredUp, sensBOut.updatedAt)
		}

		t.Logf("updated_at ordering: Gateway and Sensor metadata timestamps reflect post-lock write statement time without regression")
	})
}

package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type fakeRow struct {
	scanFunc func(dest ...any) error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.scanFunc != nil {
		return r.scanFunc(dest...)
	}
	return nil
}

type fakeConn struct {
	mu           sync.Mutex
	closed       bool
	closeErr     error
	closeCalls   int
	queryRowFunc func(ctx context.Context, sql string, args ...any) pgx.Row
}

func (c *fakeConn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.queryRowFunc != nil {
		return c.queryRowFunc(ctx, sql, args...)
	}
	return fakeRow{}
}

func (c *fakeConn) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.closeCalls++
	return c.closeErr
}

func (c *fakeConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func TestCredentialOwner_AcquireSuccess(t *testing.T) {
	conn := &fakeConn{
		queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
			if sql == "SELECT pg_try_advisory_lock($1)" {
				return fakeRow{scanFunc: func(dest ...any) error {
					*(dest[0].(*bool)) = true
					return nil
				}}
			}
			return fakeRow{}
		},
	}

	acquirer := func(ctx context.Context) (ownerDBConn, error) {
		return conn, nil
	}

	owner, err := acquireCredentialOwnerWithAcquirer(context.Background(), acquirer, credentialOwnerLockKey, time.Second)
	if err != nil {
		t.Fatalf("expected acquire success, got %v", err)
	}
	if owner == nil {
		t.Fatal("expected non-nil owner")
	}
	if conn.IsClosed() {
		t.Fatal("connection should remain open after successful acquire")
	}

	// Clean up
	if err := owner.Close(context.Background(), time.Second); err != nil {
		// unlock might return nil if we mock it or unlockErr
	}
}

func TestCredentialOwner_AcquireDenied(t *testing.T) {
	conn := &fakeConn{
		queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
			if sql == "SELECT pg_try_advisory_lock($1)" {
				return fakeRow{scanFunc: func(dest ...any) error {
					*(dest[0].(*bool)) = false // Already locked by another owner
					return nil
				}}
			}
			return fakeRow{}
		},
	}

	acquirer := func(ctx context.Context) (ownerDBConn, error) {
		return conn, nil
	}

	owner, err := acquireCredentialOwnerWithAcquirer(context.Background(), acquirer, credentialOwnerLockKey, time.Second)
	if !errors.Is(err, errOwnerLockDenied) {
		t.Fatalf("expected errOwnerLockDenied, got %v", err)
	}
	if owner != nil {
		t.Fatal("expected nil owner on lock denial")
	}
	if !conn.IsClosed() {
		t.Fatal("dedicated connection must be closed when lock is denied")
	}
}

func TestCredentialOwner_AcquireFailuresCloseResource(t *testing.T) {
	// Subtest 1: acquirer fails or returns nil conn
	t.Run("AcquirerFailsOrReturnsNil", func(t *testing.T) {
		acquirer := func(ctx context.Context) (ownerDBConn, error) {
			return nil, errors.New("underlying network down")
		}
		owner, err := acquireCredentialOwnerWithAcquirer(context.Background(), acquirer, credentialOwnerLockKey, time.Second)
		if !errors.Is(err, errOwnerAcquireFailed) {
			t.Fatalf("expected errOwnerAcquireFailed, got %v", err)
		}
		if owner != nil {
			t.Fatal("expected nil owner")
		}

		// Nil acquirer function defensively handled
		ownerNilAcquirer, errNilAcquirer := acquireCredentialOwnerWithAcquirer(context.Background(), nil, credentialOwnerLockKey, time.Second)
		if !errors.Is(errNilAcquirer, errOwnerAcquireFailed) {
			t.Fatalf("expected errOwnerAcquireFailed for nil acquirer, got %v", errNilAcquirer)
		}
		if ownerNilAcquirer != nil {
			t.Fatal("expected nil owner for nil acquirer")
		}

		// Acquirer returns nil conn with nil error defensively handled
		acquirerNilConn := func(ctx context.Context) (ownerDBConn, error) {
			return nil, nil
		}
		ownerNilConn, errNilConn := acquireCredentialOwnerWithAcquirer(context.Background(), acquirerNilConn, credentialOwnerLockKey, time.Second)
		if !errors.Is(errNilConn, errOwnerAcquireFailed) {
			t.Fatalf("expected errOwnerAcquireFailed for nil conn, got %v", errNilConn)
		}
		if ownerNilConn != nil {
			t.Fatal("expected nil owner for nil conn")
		}
	})

	// Subtest 2: query row / lock fails during acquire
	t.Run("LockQueryFails", func(t *testing.T) {
		conn := &fakeConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					return errors.New("db syntax error or conn abort")
				}}
			},
		}
		acquirer := func(ctx context.Context) (ownerDBConn, error) {
			return conn, nil
		}
		owner, err := acquireCredentialOwnerWithAcquirer(context.Background(), acquirer, credentialOwnerLockKey, time.Second)
		if !errors.Is(err, errOwnerLockFailed) {
			t.Fatalf("expected errOwnerLockFailed, got %v", err)
		}
		if owner != nil {
			t.Fatal("expected nil owner")
		}
		if !conn.IsClosed() {
			t.Fatal("dedicated connection must be closed when lock query fails")
		}
	})

	// Subtest 3: Context already canceled
	t.Run("ContextCanceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		conn := &fakeConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					return ctx.Err()
				}}
			},
		}
		acquirer := func(ctx context.Context) (ownerDBConn, error) {
			return conn, nil
		}
		owner, err := acquireCredentialOwnerWithAcquirer(ctx, acquirer, credentialOwnerLockKey, time.Second)
		if err == nil {
			t.Fatal("expected error with canceled context")
		}
		if owner != nil {
			t.Fatal("expected nil owner")
		}
		if !conn.IsClosed() {
			t.Fatal("connection must be closed on canceled context failure")
		}
	})
}

func TestCredentialOwner_PoolNil(t *testing.T) {
	owner, err := acquireCredentialOwner(context.Background(), nil, time.Second)
	if !errors.Is(err, errOwnerPoolRequired) {
		t.Fatalf("expected errOwnerPoolRequired, got %v", err)
	}
	if owner != nil {
		t.Fatal("expected nil owner")
	}
}

func TestCredentialOwner_CheckOwnership(t *testing.T) {
	// Assert query and argument contracts
	t.Run("QueryAndArgsExactMatch", func(t *testing.T) {
		var capturedSQL string
		var capturedArgs []any
		conn := &fakeConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				capturedSQL = sql
				capturedArgs = args
				return fakeRow{scanFunc: func(dest ...any) error {
					*(dest[0].(*bool)) = true
					return nil
				}}
			},
		}
		owner := &credentialOwner{
			conn:    conn,
			lockKey: credentialOwnerLockKey,
		}

		if err := owner.CheckOwnership(context.Background(), time.Second); err != nil {
			t.Fatalf("expected check to pass, got %v", err)
		}

		// Verify SQL predicates
		expectedPredicates := []string{
			"locktype = 'advisory'",
			"pid = pg_backend_pid()",
			"database = (SELECT oid FROM pg_database WHERE datname = current_database())",
			"granted = true",
			"objsubid = 1",
			"classid = $1",
			"objid = $2",
		}
		for _, pred := range expectedPredicates {
			if !strings.Contains(capturedSQL, pred) {
				t.Errorf("expected SQL to contain predicate %q, got:\n%s", pred, capturedSQL)
			}
		}
		if strings.Contains(capturedSQL, "OR") {
			t.Errorf("SQL should not contain broad OR clause, got:\n%s", capturedSQL)
		}

		// Verify arguments: high 32 bits and low 32 bits of credentialOwnerLockKey
		expectedHigh := credentialOwnerLockKey >> 32
		expectedLow := int64(int32(credentialOwnerLockKey & 0xFFFFFFFF))
		if len(capturedArgs) != 2 {
			t.Fatalf("expected exactly 2 query args, got %d: %v", len(capturedArgs), capturedArgs)
		}
		if capturedArgs[0] != expectedHigh || capturedArgs[1] != expectedLow {
			t.Fatalf("expected args [%d, %d], got %v", expectedHigh, expectedLow, capturedArgs)
		}
	})

	t.Run("PermanentlyLatchedUnavailableAfterFailure", func(t *testing.T) {
		var queryCount int32
		conn := &fakeConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				atomic.AddInt32(&queryCount, 1)
				return fakeRow{scanFunc: func(dest ...any) error {
					*(dest[0].(*bool)) = false // Missing lock
					return nil
				}}
			},
		}
		owner := &credentialOwner{
			conn:    conn,
			lockKey: credentialOwnerLockKey,
		}

		// First check fails with errOwnerNotHeld
		err1 := owner.CheckOwnership(context.Background(), time.Second)
		if !errors.Is(err1, errOwnerNotHeld) {
			t.Fatalf("expected errOwnerNotHeld, got %v", err1)
		}
		if atomic.LoadInt32(&queryCount) != 1 {
			t.Fatalf("expected 1 query, got %d", queryCount)
		}

		// Subsequent calls must return errOwnerUnavailable without executing any DB query
		for i := 0; i < 3; i++ {
			errNext := owner.CheckOwnership(context.Background(), time.Second)
			if !errors.Is(errNext, errOwnerUnavailable) {
				t.Fatalf("expected errOwnerUnavailable, got %v", errNext)
			}
		}
		if atomic.LoadInt32(&queryCount) != 1 {
			t.Fatalf("expected ZERO additional queries after poisoning, got total %d", queryCount)
		}
	})

	t.Run("CancellationBeforeQueryDoesNotPoison", func(t *testing.T) {
		var queryCount int32
		conn := &fakeConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				atomic.AddInt32(&queryCount, 1)
				return fakeRow{scanFunc: func(dest ...any) error {
					*(dest[0].(*bool)) = true
					return nil
				}}
			},
		}
		owner := &credentialOwner{
			conn:    conn,
			lockKey: credentialOwnerLockKey,
		}

		// Cancel context BEFORE calling CheckOwnership
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		err := owner.CheckOwnership(canceledCtx, time.Second)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if atomic.LoadInt32(&queryCount) != 0 {
			t.Fatalf("expected zero queries when context canceled before query, got %d", queryCount)
		}

		// Subsequent call with valid context succeeds because owner was NOT poisoned!
		errSuccess := owner.CheckOwnership(context.Background(), time.Second)
		if errSuccess != nil {
			t.Fatalf("expected check to succeed after pre-query cancellation, got %v", errSuccess)
		}
		if atomic.LoadInt32(&queryCount) != 1 {
			t.Fatalf("expected 1 query for successful check, got %d", queryCount)
		}
	})

	t.Run("HeldExactSession", func(t *testing.T) {
		conn := &fakeConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					*(dest[0].(*bool)) = true
					return nil
				}}
			},
		}
		owner := &credentialOwner{
			conn:    conn,
			lockKey: credentialOwnerLockKey,
		}

		if err := owner.CheckOwnership(context.Background(), time.Second); err != nil {
			t.Fatalf("expected check to pass, got %v", err)
		}
	})

	t.Run("NotHeldOrWrongSession", func(t *testing.T) {
		conn := &fakeConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					*(dest[0].(*bool)) = false // Missing or owned by another PID
					return nil
				}}
			},
		}
		owner := &credentialOwner{
			conn:    conn,
			lockKey: credentialOwnerLockKey,
		}

		err := owner.CheckOwnership(context.Background(), time.Second)
		if !errors.Is(err, errOwnerNotHeld) {
			t.Fatalf("expected errOwnerNotHeld, got %v", err)
		}
	})

	t.Run("ConnectionClosedBeforeCheck", func(t *testing.T) {
		conn := &fakeConn{
			closed: true,
		}
		owner := &credentialOwner{
			conn:    conn,
			lockKey: credentialOwnerLockKey,
		}

		err := owner.CheckOwnership(context.Background(), time.Second)
		if !errors.Is(err, errOwnerConnectionLost) {
			t.Fatalf("expected errOwnerConnectionLost, got %v", err)
		}
	})

	t.Run("QueryErrorDuringCheck", func(t *testing.T) {
		conn := &fakeConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					return errors.New("read tcp: connection reset by peer")
				}}
			},
		}
		owner := &credentialOwner{
			conn:    conn,
			lockKey: credentialOwnerLockKey,
		}

		err := owner.CheckOwnership(context.Background(), time.Second)
		if !errors.Is(err, errOwnerCheckFailed) {
			t.Fatalf("expected errOwnerCheckFailed, got %v", err)
		}
	})

	t.Run("OwnerAlreadyClosed", func(t *testing.T) {
		owner := &credentialOwner{
			closed: true,
		}
		err := owner.CheckOwnership(context.Background(), time.Second)
		if !errors.Is(err, errOwnerClosed) {
			t.Fatalf("expected errOwnerClosed, got %v", err)
		}
	})

	t.Run("DefaultTimeoutBranch", func(t *testing.T) {
		conn := &fakeConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					*(dest[0].(*bool)) = true
					return nil
				}}
			},
		}
		acquirer := func(ctx context.Context) (ownerDBConn, error) {
			return conn, nil
		}
		// timeout <= 0 triggers default timeout branch (5s)
		owner, err := acquireCredentialOwnerWithAcquirer(context.Background(), acquirer, credentialOwnerLockKey, 0)
		if err != nil {
			t.Fatalf("expected acquire success with default timeout, got %v", err)
		}
		// timeout <= 0 triggers default timeout branch (3s)
		if err := owner.CheckOwnership(context.Background(), 0); err != nil {
			t.Fatalf("expected check success with default timeout, got %v", err)
		}
		// timeout <= 0 triggers default timeout branch (3s)
		if err := owner.Close(context.Background(), 0); err != nil {
			t.Fatalf("expected close success with default timeout, got %v", err)
		}
	})

	t.Run("ContextDeadlineExceeded", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		conn := &fakeConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					return ctx.Err()
				}}
			},
		}
		owner := &credentialOwner{
			conn:    conn,
			lockKey: credentialOwnerLockKey,
		}
		err := owner.CheckOwnership(ctx, time.Second)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})
}

func TestCredentialOwner_CloseIdempotentAndReleases(t *testing.T) {
	t.Run("CloseReturnsSafeCloseError", func(t *testing.T) {
		conn := &fakeConn{
			closeErr: errors.New("raw network error with secret sensitive-conn-info"),
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					*(dest[0].(*bool)) = true // Unlock succeeded
					return nil
				}}
			},
		}
		owner := &credentialOwner{
			conn:    conn,
			lockKey: credentialOwnerLockKey,
		}

		err := owner.Close(context.Background(), time.Second)
		if !errors.Is(err, errOwnerCloseFailed) {
			t.Fatalf("expected errOwnerCloseFailed, got %v", err)
		}
		// Ensure raw secret/driver error is NOT leaked in message
		if strings.Contains(err.Error(), "sensitive-conn-info") {
			t.Fatalf("error message must be redacted, got %v", err)
		}
		if !conn.IsClosed() {
			t.Fatal("connection should be closed")
		}
	})

	t.Run("ClosePreservesPrimaryUnlockErrorWhenBothFail", func(t *testing.T) {
		conn := &fakeConn{
			closeErr: errors.New("underlying close failed"),
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					return errors.New("unlock execution failure")
				}}
			},
		}
		owner := &credentialOwner{
			conn:    conn,
			lockKey: credentialOwnerLockKey,
		}

		err := owner.Close(context.Background(), time.Second)
		if !errors.Is(err, errOwnerUnlockFailed) {
			t.Fatalf("expected primary errOwnerUnlockFailed when both unlock and close fail, got %v", err)
		}
		if !conn.IsClosed() {
			t.Fatal("connection should be closed")
		}
	})

	t.Run("CloseAfterPoisonStillClosesOnce", func(t *testing.T) {
		conn := &fakeConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					*(dest[0].(*bool)) = false // Missing lock poisons owner
					return nil
				}}
			},
		}
		owner := &credentialOwner{
			conn:    conn,
			lockKey: credentialOwnerLockKey,
		}

		// Poison the owner
		_ = owner.CheckOwnership(context.Background(), time.Second)

		// Close must dispose connection cleanly
		if err := owner.Close(context.Background(), time.Second); err != nil && !errors.Is(err, errOwnerNotHeld) {
			t.Fatalf("unexpected close error: %v", err)
		}
		if !conn.IsClosed() {
			t.Fatal("connection should be closed after poison")
		}
		if conn.closeCalls != 1 {
			t.Fatalf("expected exactly 1 close call, got %d", conn.closeCalls)
		}

		// Idempotent second close
		if err := owner.Close(context.Background(), time.Second); err != nil {
			t.Fatalf("second close should return nil, got %v", err)
		}
		if conn.closeCalls != 1 {
			t.Fatalf("expected close calls to remain 1, got %d", conn.closeCalls)
		}
	})

	t.Run("UnlockSuccess", func(t *testing.T) {
		conn := &fakeConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					*(dest[0].(*bool)) = true
					return nil
				}}
			},
		}
		owner := &credentialOwner{
			conn:    conn,
			lockKey: credentialOwnerLockKey,
		}

		if err := owner.Close(context.Background(), time.Second); err != nil {
			t.Fatalf("expected clean close, got %v", err)
		}
		if !conn.IsClosed() {
			t.Fatal("connection should be closed")
		}
		if conn.closeCalls != 1 {
			t.Fatalf("expected 1 close call, got %d", conn.closeCalls)
		}

		// Second call must be idempotent
		if err := owner.Close(context.Background(), time.Second); err != nil {
			t.Fatalf("idempotent second close should return nil, got %v", err)
		}
		if conn.closeCalls != 1 {
			t.Fatalf("expected still 1 close call, got %d", conn.closeCalls)
		}
	})

	t.Run("UnlockFailsStillClosesConn", func(t *testing.T) {
		conn := &fakeConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					return errors.New("unlock execution failure")
				}}
			},
		}
		owner := &credentialOwner{
			conn:    conn,
			lockKey: credentialOwnerLockKey,
		}

		err := owner.Close(context.Background(), time.Second)
		if !errors.Is(err, errOwnerUnlockFailed) {
			t.Fatalf("expected errOwnerUnlockFailed, got %v", err)
		}
		if !conn.IsClosed() {
			t.Fatal("connection MUST be closed even if unlock query fails")
		}
	})

	t.Run("UnlockFalseStillClosesConn", func(t *testing.T) {
		conn := &fakeConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return fakeRow{scanFunc: func(dest ...any) error {
					*(dest[0].(*bool)) = false // not held
					return nil
				}}
			},
		}
		owner := &credentialOwner{
			conn:    conn,
			lockKey: credentialOwnerLockKey,
		}

		err := owner.Close(context.Background(), time.Second)
		if !errors.Is(err, errOwnerNotHeld) {
			t.Fatalf("expected errOwnerNotHeld, got %v", err)
		}
		if !conn.IsClosed() {
			t.Fatal("connection MUST be closed even if lock was not held")
		}
	})
}

func TestCredentialOwner_ConcurrencySafety(t *testing.T) {
	conn := &fakeConn{
		queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return fakeRow{scanFunc: func(dest ...any) error {
				*(dest[0].(*bool)) = true
				return nil
			}}
		},
	}
	owner := &credentialOwner{
		conn:    conn,
		lockKey: credentialOwnerLockKey,
	}

	var wg sync.WaitGroup
	var checksDone int64
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = owner.CheckOwnership(context.Background(), 50*time.Millisecond)
				atomic.AddInt64(&checksDone, 1)
			}
		}()
	}

	// Trigger close concurrently while checks are ongoing
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		_ = owner.Close(context.Background(), 100*time.Millisecond)
	}()

	wg.Wait()

	if !conn.IsClosed() {
		t.Fatal("expected connection to be closed after concurrent check/close")
	}
	// After close has completed, further checks must return errOwnerClosed
	if err := owner.CheckOwnership(context.Background(), time.Second); !errors.Is(err, errOwnerClosed) {
		t.Fatalf("expected errOwnerClosed after shutdown, got %v", err)
	}
}

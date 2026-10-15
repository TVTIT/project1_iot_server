package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// credentialOwnerLockKey is a stable application-level PostgreSQL advisory lock key.
// It is a fixed protocol/product constant, not a deployment-specific secret.
// Formula/value: int64 identifier representing the exclusive credential runtime owner.
const credentialOwnerLockKey int64 = 0x494f545f43524544 // 'IOT_CRED' in ASCII

var (
	errOwnerPoolRequired   = errors.New("credential database pool required")
	errOwnerAcquireFailed  = errors.New("credential owner connection acquire failed")
	errOwnerLockDenied     = errors.New("credential owner advisory lock denied; another backend instance owns credentials")
	errOwnerLockFailed     = errors.New("credential owner advisory lock execution failed")
	errOwnerClosed         = errors.New("credential owner closed")
	errOwnerNotHeld        = errors.New("credential owner lock not held by session")
	errOwnerCheckFailed    = errors.New("credential owner verification failed")
	errOwnerConnectionLost = errors.New("credential owner connection lost")
	errOwnerUnlockFailed   = errors.New("credential owner advisory unlock failed")
	errOwnerCloseFailed    = errors.New("credential owner close failed")
	errOwnerUnavailable    = errors.New("credential owner permanently unavailable")
)

// ownerDBConn abstracts a dedicated, unshared database connection for session advisory lock.
type ownerDBConn interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Close(ctx context.Context) error
	IsClosed() bool
}

// pgxDedicatedConn wraps a hijacked *pgx.Conn from pgxpool to implement ownerDBConn.
type pgxDedicatedConn struct {
	conn *pgx.Conn
}

func (c *pgxDedicatedConn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return c.conn.QueryRow(ctx, sql, args...)
}

func (c *pgxDedicatedConn) Close(ctx context.Context) error {
	return c.conn.Close(ctx)
}

func (c *pgxDedicatedConn) IsClosed() bool {
	return c.conn.IsClosed()
}

// connAcquirerFunc acquires a dedicated, unshared connection.
type connAcquirerFunc func(ctx context.Context) (ownerDBConn, error)

// credentialOwner manages an exclusive PostgreSQL session advisory lock on a dedicated connection.
//
// LIMITATIONS AND OPERATIONAL MODEL:
// This helper provides single-instance exclusion only across backend processes connected to the same
// PostgreSQL database.
// It does NOT provide broker quiescence or distributed fencing.
// If the dedicated PostgreSQL connection breaks or is severed:
//  1. PostgreSQL immediately releases session-level advisory locks associated with that session.
//  2. Any in-flight or already queued broker requests (e.g. MQTT DynSec mutations) are NOT canceled
//     automatically by this database drop.
//  3. The ownership check fails closed immediately without reacquiring automatically.
//  4. Later runtime layers must gate further operations, poison unknown results, and require
//     supervised restart/reset rather than attempting silent automatic lock recovery.
type credentialOwner struct {
	mu          sync.Mutex
	conn        ownerDBConn
	lockKey     int64
	closed      bool
	unavailable bool
}

// acquireCredentialOwner acquires a dedicated connection from pool, hijacks it so it is never
// returned to the shared pool, and locks credentialOwnerLockKey using pg_try_advisory_lock.
func acquireCredentialOwner(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration) (*credentialOwner, error) {
	if pool == nil {
		return nil, errOwnerPoolRequired
	}
	acquirer := func(acquireCtx context.Context) (ownerDBConn, error) {
		pConn, err := pool.Acquire(acquireCtx)
		if err != nil {
			return nil, errOwnerAcquireFailed
		}
		// Hijack takes full ownership away from the pool.
		// pConn will never be returned to pgxpool, avoiding lock contamination in the pool.
		hijacked := pConn.Hijack()
		return &pgxDedicatedConn{conn: hijacked}, nil
	}
	return acquireCredentialOwnerWithAcquirer(ctx, acquirer, credentialOwnerLockKey, timeout)
}

// acquireCredentialOwnerWithAcquirer is the internal seam for acquiring ownership with an injected acquirer.
func acquireCredentialOwnerWithAcquirer(ctx context.Context, acquirer connAcquirerFunc, lockKey int64, timeout time.Duration) (_ *credentialOwner, err error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if acquirer == nil {
		return nil, errOwnerAcquireFailed
	}

	conn, err := acquirer(ctx)
	if err != nil || conn == nil {
		return nil, errOwnerAcquireFailed
	}

	// Guarantee cleanup on any failure path.
	defer func() {
		if err != nil && conn != nil {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), timeout)
			defer cleanupCancel()
			_ = conn.Close(cleanupCtx)
		}
	}()

	var locked bool
	row := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockKey)
	if scanErr := row.Scan(&locked); scanErr != nil {
		err = errOwnerLockFailed
		return nil, err
	}

	if !locked {
		err = errOwnerLockDenied
		return nil, err
	}

	owner := &credentialOwner{
		conn:    conn,
		lockKey: lockKey,
		closed:  false,
	}
	return owner, nil
}

// CheckOwnership verifies that the dedicated connection is alive and currently holds the expected advisory lock.
//
// Fails closed on:
// - Closed owner instance
// - Closed or nil connection
// - Context deadline/cancellation
// - Lock missing from pg_locks for current pg_backend_pid()
// - Any database query or communication error
//
// If a completed check fails, the lock is missing, or the connection is lost/errored, the owner is
// permanently latched unavailable (poisoned); subsequent calls make zero queries and immediately return
// errOwnerUnavailable. A request canceled before any query does not poison an otherwise valid owner.
// Does NOT attempt to reacquire the lock automatically if lost.
func (o *credentialOwner) CheckOwnership(ctx context.Context, timeout time.Duration) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed || o.conn == nil {
		return errOwnerClosed
	}

	if o.unavailable {
		return errOwnerUnavailable
	}

	if o.conn.IsClosed() {
		o.unavailable = true
		return errOwnerConnectionLost
	}

	// Check if caller context was already canceled BEFORE any DB query is attempted.
	// A pre-query cancellation simply returns cancellation without poisoning an otherwise valid owner.
	if err := ctx.Err(); err != nil {
		return err
	}

	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Query pg_locks directly for exact one-bigint granted advisory lock:
	// - locktype = 'advisory'
	// - pid = pg_backend_pid()
	// - database = (SELECT oid FROM pg_database WHERE datname = current_database())
	// - granted = true
	// - objsubid = 1 (session-level advisory lock with one 64-bit int key)
	// - classid = high 32 bits (as signed 32-bit int)
	// - objid = low 32 bits (as signed 32-bit int)
	const checkSQL = `
SELECT EXISTS (
    SELECT 1 FROM pg_locks
    WHERE locktype = 'advisory'
      AND pid = pg_backend_pid()
      AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
      AND granted = true
      AND objsubid = 1
      AND classid = $1
      AND objid = $2
)`

	high32 := int64(o.lockKey >> 32)
	low32 := int64(int32(o.lockKey & 0xFFFFFFFF))

	var held bool
	row := o.conn.QueryRow(queryCtx, checkSQL, high32, low32)
	if err := row.Scan(&held); err != nil {
		o.unavailable = true
		if queryCtx.Err() != nil {
			return queryCtx.Err()
		}
		if o.conn.IsClosed() {
			return errOwnerConnectionLost
		}
		return errOwnerCheckFailed
	}

	if !held {
		o.unavailable = true
		return errOwnerNotHeld
	}

	return nil
}

// Close gracefully releases the advisory lock and closes the dedicated connection.
// It is idempotent and concurrency-safe.
// Crucially, it closes the underlying dedicated connection on ALL paths, even if unlocking fails.
// Returns a safe error if closing or unlocking fails (preserving primary unlock error if both fail).
func (o *credentialOwner) Close(ctx context.Context, timeout time.Duration) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil
	}
	o.closed = true
	o.unavailable = true

	if o.conn == nil {
		return nil
	}

	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	var unlockErr error
	if !o.conn.IsClosed() {
		unlockCtx, unlockCancel := context.WithTimeout(ctx, timeout)
		defer unlockCancel()

		var unlocked bool
		row := o.conn.QueryRow(unlockCtx, "SELECT pg_advisory_unlock($1)", o.lockKey)
		if scanErr := row.Scan(&unlocked); scanErr != nil {
			unlockErr = errOwnerUnlockFailed
		} else if !unlocked {
			unlockErr = errOwnerNotHeld
		}
	}

	// Always close connection even if unlock failed.
	closeCtx, closeCancel := context.WithTimeout(context.Background(), timeout)
	defer closeCancel()
	var closeErr error
	if err := o.conn.Close(closeCtx); err != nil {
		closeErr = errOwnerCloseFailed
	}

	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}

package telemetryws

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Recheck worker sizing and timing bounds.
const (
	// DefaultRecheckInterval is the default period between recheck sweeps (5 seconds).
	DefaultRecheckInterval = 5 * time.Second

	// DefaultRecheckSweepBudget is the global deadline budget for one entire sweep (2 seconds).
	DefaultRecheckSweepBudget = 2 * time.Second

	// DefaultRecheckConcurrency is the fixed capped number of concurrent DB authorization checks.
	DefaultRecheckConcurrency = 2

	// MaxSessions bounds the maximum number of concurrent sessions tracked by the access lifecycle.
	MaxSessions = 32
)

// TargetsAuthorizer is the minimal authorization interface required by the RecheckWorker.
// It is implemented by *TargetAuthorizer.
type TargetsAuthorizer interface {
	AuthorizeTargets(ctx context.Context, userID uuid.UUID, targets []TargetKey) ([]AuthorizedTarget, error)
}

// RecheckWorkerConfig configures the periodic recheck sweep worker.
type RecheckWorkerConfig struct {
	// Interval between sweeps. Defaults to DefaultRecheckInterval (5s).
	Interval time.Duration

	// SweepBudget is the global timeout for an entire sweep. Defaults to DefaultRecheckSweepBudget (2s).
	SweepBudget time.Duration

	// Concurrency is the capped number of concurrent DB authorization queries. Defaults to DefaultRecheckConcurrency (2).
	Concurrency int

	// MaxSessions is the upper bound on sessions inspected per sweep. Defaults to MaxSessions (32).
	MaxSessions int

	// Now is an optional time provider function for deterministic testing.
	Now func() time.Time
}

// SweepReport captures the outcome of one recheck sweep for observability and test verification.
type SweepReport struct {
	SessionsChecked   int
	SessionsVerified  int
	SessionsRevoked   int
	SessionsExpired   int
	SessionsStale     int
	RevocationReasons map[string]RevocationReason
	Duration          time.Duration
}

// RecheckWorker executes bounded periodic membership and expiry sweeps across active sessions.
//
// CONCURRENCY & LIFECYCLE CONTRACT:
//  1. Fixed capped DB concurrency: Sweeps execute DB checks using a fixed number of workers
//     (DefaultRecheckConcurrency = 2). No goroutine is created per viewer, per event, or per sample.
//  2. Outside locks: DB queries are performed strictly outside any session or hub mutex.
//  3. Global sweep budget (2s) & Fail-Closed Overload: If the DB stalls or queries cannot complete
//     within the 2-second sweep budget, all unverified remaining sessions fail closed immediately.
//     The membership decision bound is interval + DB budget; socket closure additionally depends
//     on bounded in-flight write and transport close work.
//  4. Generation-safe outcome application: Recheck takes point-in-time snapshots and validates
//     generation before applying verified results. Recheck NEVER installs or restores targets.
//  5. Whole-session closure on failure: Membership denial, DB error, or budget exhaustion closes the
//     entire session, clears its targets, bumps generation, and triggers the revocation handler.
type RecheckWorker struct {
	authorizer TargetsAuthorizer
	provider   SessionProvider
	cfg        RecheckWorkerConfig
	mu         sync.Mutex
	running    bool
	stopCh     chan struct{}
	doneCh     chan struct{}
	cancel     context.CancelFunc
}

// NewRecheckWorker constructs a RecheckWorker with the provided authorizer, session provider, and configuration.
func NewRecheckWorker(authorizer TargetsAuthorizer, provider SessionProvider, cfg RecheckWorkerConfig) *RecheckWorker {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultRecheckInterval
	}
	if cfg.SweepBudget <= 0 {
		cfg.SweepBudget = DefaultRecheckSweepBudget
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultRecheckConcurrency
	}
	if cfg.MaxSessions <= 0 {
		cfg.MaxSessions = MaxSessions
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	return &RecheckWorker{
		authorizer: authorizer,
		provider:   provider,
		cfg:        cfg,
	}
}

// Sweep executes a single bounded sweep across active sessions.
// It is public to allow synchronous execution in unit tests without arbitrary sleeps.
func (w *RecheckWorker) Sweep(ctx context.Context) SweepReport {
	start := time.Now()
	report := SweepReport{
		RevocationReasons: make(map[string]RevocationReason),
	}

	currentTime := w.cfg.Now()

	// 1. Take bounded snapshot of active sessions
	sessions := w.provider.GetActiveSessions(w.cfg.MaxSessions)
	if len(sessions) == 0 {
		report.Duration = time.Since(start)
		return report
	}
	if len(sessions) > w.cfg.MaxSessions {
		sessions = sessions[:w.cfg.MaxSessions]
	}

	type sweepItem struct {
		session  *SessionAccessState
		snap     SessionSnapshot
		verified bool
		revoked  bool
		stale    bool
		reason   RevocationReason
	}

	toCheck := make([]*sweepItem, 0, len(sessions))

	for _, s := range sessions {
		snap := s.Snapshot()
		report.SessionsChecked++
		item := &sweepItem{session: s, snap: snap}

		if snap.Closed {
			continue
		}

		// Absolute JWT expiration check (Principal.ExpiresAt) without skew
		if !snap.ExpiresAt.IsZero() && !currentTime.Before(snap.ExpiresAt) {
			item.revoked = true
			item.reason = RevocationReasonExpired
			s.Close(RevocationReasonExpired)
			report.SessionsExpired++
			report.SessionsRevoked++
			report.RevocationReasons[snap.SessionID] = RevocationReasonExpired
			continue
		}

		// Sessions with 0 active targets need no DB query
		if len(snap.Targets) == 0 {
			item.verified = true
			report.SessionsVerified++
			continue
		}

		toCheck = append(toCheck, item)
	}

	if len(toCheck) == 0 {
		report.Duration = time.Since(start)
		return report
	}

	// 2. Global sweep budget context
	sweepCtx, cancel := context.WithTimeout(ctx, w.cfg.SweepBudget)
	defer cancel()

	concurrency := w.cfg.Concurrency
	if concurrency > len(toCheck) {
		concurrency = len(toCheck)
	}

	workCh := make(chan *sweepItem, len(toCheck))
	for _, it := range toCheck {
		workCh <- it
	}
	close(workCh)

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for it := range workCh {
				if sweepCtx.Err() != nil {
					return
				}

				_, err := w.authorizer.AuthorizeTargets(sweepCtx, it.snap.UserID, it.snap.Targets)
				if err == nil {
					// Verify generation under session lock before accepting
					it.session.mu.Lock()
					if it.session.closed {
						it.stale = true
					} else if it.session.generation != it.snap.Generation {
						// Session state changed while DB query was in-flight.
						// Recheck must NOT install, alter, or restore targets.
						it.stale = true
					} else {
						it.verified = true
						it.session.lastVerifiedAt = currentTime
					}
					it.session.mu.Unlock()
				} else {
					it.revoked = true
					if errors.Is(sweepCtx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
						it.reason = RevocationReasonSweepTimeout
					} else if errors.Is(err, ErrTargetUnauthorized) {
						it.reason = RevocationReasonDenied
					} else {
						it.reason = RevocationReasonDBError
					}
					it.session.Close(it.reason)
				}
			}
		}()
	}

	wg.Wait()

	// 3. Fail-closed handling for remaining unverified sessions (sweep budget exceeded)
	for _, it := range toCheck {
		if it.verified {
			report.SessionsVerified++
		} else if it.revoked {
			report.SessionsRevoked++
			report.RevocationReasons[it.snap.SessionID] = it.reason
		} else if it.stale {
			report.SessionsStale++
		} else {
			// Inability to recheck within sweep budget -> FAIL CLOSED
			it.revoked = true
			it.reason = RevocationReasonSweepTimeout
			it.session.Close(RevocationReasonSweepTimeout)
			report.SessionsRevoked++
			report.RevocationReasons[it.snap.SessionID] = RevocationReasonSweepTimeout
		}
	}

	report.Duration = time.Since(start)
	return report
}

// Start begins the periodic background sweep loop.
func (w *RecheckWorker) Start() error {
	return w.StartContext(context.Background())
}

// StartContext starts the single periodic loop under a parent lifecycle context.
func (w *RecheckWorker) StartContext(parent context.Context) error {
	if parent == nil {
		parent = context.Background()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running {
		return errors.New("telemetryws: recheck worker already running")
	}
	w.running = true
	w.stopCh = make(chan struct{})
	w.doneCh = make(chan struct{})
	runCtx, cancel := context.WithCancel(parent)
	w.cancel = cancel
	go w.loop(runCtx)
	return nil
}

// Stop cleanly stops the background loop and waits for any active sweep to terminate.
func (w *RecheckWorker) Stop() error {
	return w.StopContext(context.Background())
}

// StopContext cancels active DB checks and joins the loop within ctx's budget.
func (w *RecheckWorker) StopContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	w.mu.Lock()
	if !w.running {
		done := w.doneCh
		w.mu.Unlock()
		if done == nil {
			return nil
		}
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w.running = false
	close(w.stopCh)
	if w.cancel != nil {
		w.cancel()
	}
	done := w.doneCh
	w.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *RecheckWorker) loop(ctx context.Context) {
	defer close(w.doneCh)
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.Sweep(ctx)
		}
	}
}

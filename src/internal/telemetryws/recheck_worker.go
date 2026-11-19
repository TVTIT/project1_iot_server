package telemetryws

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// Recheck worker sizing and timing bounds.
const (
	// DefaultRecheckInterval is the default period between recheck sweeps (5 seconds).
	DefaultRecheckInterval = 5 * time.Second

	// DefaultRecheckSweepBudget is the global deadline budget for one entire sweep (2 seconds).
	DefaultRecheckSweepBudget = 2 * time.Second

	// DefaultRecheckConcurrency is the fixed capped number of concurrent DB authorization checks.
	DefaultRecheckConcurrency = 2

	// MaxSessions is the default concurrent session limit.
	MaxSessions = 32
	// MaxSupportedSessions is the finite configuration ceiling, not a capacity claim.
	MaxSupportedSessions = 10000
)

// TargetsAuthorizer is the minimal authorization interface required by the RecheckWorker.
// It is implemented by *TargetAuthorizer.
type TargetsAuthorizer interface {
	AuthorizeTargets(ctx context.Context, userID uuid.UUID, targets []TargetKey) ([]AuthorizedTarget, error)
}

// BatchTargetsAuthorizer is the optional production path for composite-key sweep checks.
type BatchTargetsAuthorizer interface {
	AuthorizeTargetsBatch(context.Context, []gateway.SubscriptionTargetKey) ([]TargetAuthorizationOutcome, error)
}

type observedBatchTargetsAuthorizer interface {
	AuthorizeTargetsBatchObserved(context.Context, []gateway.SubscriptionTargetKey, func()) ([]TargetAuthorizationOutcome, error)
}

type recheckSweepItem struct {
	session   *SessionAccessState
	snap      SessionSnapshot
	keys      []gateway.SubscriptionTargetKey
	remaining int
	verified  bool
	revoked   bool
	stale     bool
	reason    RevocationReason
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

	// Metrics is optional; nil keeps observation disabled.
	Metrics *Metrics
}

// SweepReport captures the outcome of one recheck sweep for observability and test verification.
type SweepReport struct {
	SessionsChecked   int
	SessionsVerified  int
	SessionsRevoked   int
	SessionsExpired   int
	SessionsStale     int
	DistinctKeys      int
	Chunks            int
	Queries           int
	Timeouts          int
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
	// querySlots bounds authorization calls across all timed-out sweeps. A call
	// retains its slot until the authorizer actually returns, including after deadline.
	querySlots chan struct{}
	workerWG   sync.WaitGroup
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
		querySlots: make(chan struct{}, cfg.Concurrency),
	}
}

// Sweep executes a single bounded sweep across active sessions.
// It is public to allow synchronous execution in unit tests without arbitrary sleeps.
func (w *RecheckWorker) Sweep(ctx context.Context) SweepReport {
	start := time.Now()
	report := SweepReport{
		RevocationReasons: make(map[string]RevocationReason),
	}

	if ctx == nil {
		ctx = context.Background()
	}
	currentTime := w.cfg.Now()

	// 1. Take bounded snapshot of active sessions
	sessions := w.provider.GetActiveSessions(w.cfg.MaxSessions)
	if len(sessions) == 0 {
		report.Duration = time.Since(start)
		w.cfg.Metrics.RecheckSweep(report)
		return report
	}
	if len(sessions) > w.cfg.MaxSessions {
		sessions = sessions[:w.cfg.MaxSessions]
	}

	toCheck := make([]*recheckSweepItem, 0, len(sessions))

	for _, s := range sessions {
		snap := s.Snapshot()
		report.SessionsChecked++
		item := &recheckSweepItem{session: s, snap: snap}

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

		// Sessions with no subscriptions still received an expiry check above.
		if len(snap.Targets) == 0 {
			item.verified = markRecheckVerified(item, currentTime)
			if item.stale {
				report.SessionsStale++
			} else if item.verified {
				report.SessionsVerified++
			}
			continue
		}
		for _, target := range snap.Targets {
			item.keys = append(item.keys, gateway.SubscriptionTargetKey{UserID: snap.UserID, GatewayID: target.GatewayID, SensorID: target.SensorID})
		}
		item.remaining = len(item.keys)
		toCheck = append(toCheck, item)
	}

	if len(toCheck) == 0 {
		report.Duration = time.Since(start)
		w.cfg.Metrics.RecheckSweep(report)
		return report
	}

	// Deduplicate only within this sweep using complete principal + namespace identity.
	keyItems := make(map[gateway.SubscriptionTargetKey][]*recheckSweepItem)
	keys := make([]gateway.SubscriptionTargetKey, 0)
	for _, item := range toCheck {
		for _, key := range item.keys {
			if _, exists := keyItems[key]; !exists {
				keys = append(keys, key)
			}
			keyItems[key] = append(keyItems[key], item)
		}
	}
	report.DistinctKeys = len(keys)

	// 2. Global sweep budget context. Results are applied only by this coordinator;
	// a late/non-cooperative authorizer can never mutate session state after deadline.
	sweepCtx, cancel := context.WithTimeout(ctx, w.cfg.SweepBudget)
	defer cancel()
	chunks := make([][]gateway.SubscriptionTargetKey, 0, (len(keys)+gateway.MaxSubscriptionTargetBatchSize-1)/gateway.MaxSubscriptionTargetBatchSize)
	for begin := 0; begin < len(keys); begin += gateway.MaxSubscriptionTargetBatchSize {
		end := begin + gateway.MaxSubscriptionTargetBatchSize
		if end > len(keys) {
			end = len(keys)
		}
		chunks = append(chunks, keys[begin:end])
	}
	report.Chunks = len(chunks)
	type batchResult struct {
		outcomes []TargetAuthorizationOutcome
		err      error
		keys     []gateway.SubscriptionTargetKey
	}
	workCh := make(chan []gateway.SubscriptionTargetKey, len(chunks))
	results := make(chan batchResult, len(chunks))
	var startedQueries atomic.Int64
	for _, chunk := range chunks {
		workCh <- chunk
	}
	close(workCh)
	concurrency := w.cfg.Concurrency
	if concurrency > len(chunks) {
		concurrency = len(chunks)
	}
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		w.workerWG.Add(1)
		go func() {
			defer w.workerWG.Done()
			defer wg.Done()
			for chunk := range workCh {
				if sweepCtx.Err() != nil {
					return
				}
				select {
				case w.querySlots <- struct{}{}:
				case <-sweepCtx.Done():
					return
				}
				if sweepCtx.Err() != nil {
					<-w.querySlots
					return
				}
				result := batchResult{keys: chunk}
				countQuery := func() { startedQueries.Add(1) }
				func() {
					defer func() { <-w.querySlots }()
					switch batch := w.authorizer.(type) {
					case observedBatchTargetsAuthorizer:
						result.outcomes, result.err = batch.AuthorizeTargetsBatchObserved(sweepCtx, chunk, countQuery)
					case BatchTargetsAuthorizer:
						countQuery()
						result.outcomes, result.err = batch.AuthorizeTargetsBatch(sweepCtx, chunk)
					default:
						result.outcomes, result.err = w.authorizeLegacyChunk(sweepCtx, chunk, countQuery)
					}
				}()
				results <- result
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	completed := 0
	for completed < len(chunks) {
		select {
		case result := <-results:
			completed++
			if sweepCtx.Err() != nil {
				break
			}
			w.applyBatchResult(result.keys, result.outcomes, result.err, keyItems, currentTime, &report)
		case <-sweepCtx.Done():
			completed = len(chunks)
		}
	}
	if sweepCtx.Err() != nil {
		if report.Timeouts == 0 {
			report.Timeouts++
		}
	}
	// Workers are bounded and receive cancellation. Join promptly when cooperative;
	// do not block the sweep deadline on a broken authorizer.
	select {
	case <-done:
	case <-sweepCtx.Done():
	}
	report.Queries = int(startedQueries.Load())

	// Unverified sessions fail closed, including outstanding chunks on cancellation.
	for _, it := range toCheck {
		if it.verified {
			report.SessionsVerified++
		} else if it.revoked {
			report.SessionsRevoked++
			report.RevocationReasons[it.snap.SessionID] = it.reason
		} else if it.stale {
			report.SessionsStale++
		} else {
			if it.session.closeIfGeneration(it.snap.Generation, RevocationReasonSweepTimeout) {
				it.revoked = true
				it.reason = RevocationReasonSweepTimeout
				report.SessionsRevoked++
				report.RevocationReasons[it.snap.SessionID] = RevocationReasonSweepTimeout
			} else {
				it.stale = true
				report.SessionsStale++
			}
		}
	}

	report.Duration = time.Since(start)
	w.cfg.Metrics.RecheckSweep(report)
	return report
}

func markRecheckVerified(it *recheckSweepItem, verifiedAt time.Time) bool {
	it.session.mu.Lock()
	defer it.session.mu.Unlock()
	if it.session.closed || it.session.generation != it.snap.Generation {
		it.stale = true
		return false
	}
	it.session.lastVerifiedAt = verifiedAt
	return true
}

func (w *RecheckWorker) authorizeLegacyChunk(ctx context.Context, keys []gateway.SubscriptionTargetKey, onQueryStart func()) ([]TargetAuthorizationOutcome, error) {
	out := make([]TargetAuthorizationOutcome, 0, len(keys))
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		onQueryStart()
		allowed, err := w.authorizer.AuthorizeTargets(ctx, key.UserID, []TargetKey{{GatewayID: key.GatewayID, SensorID: key.SensorID}})
		if err != nil {
			out = append(out, TargetAuthorizationOutcome{Key: key, Err: err})
			continue
		}
		if len(allowed) != 1 {
			out = append(out, TargetAuthorizationOutcome{Key: key, Err: ErrBatchAuthorizationFailed})
			continue
		}
		out = append(out, TargetAuthorizationOutcome{Key: key, Role: allowed[0].Role, Authorized: true})
	}
	return out, nil
}

func (w *RecheckWorker) applyBatchResult(keys []gateway.SubscriptionTargetKey, outcomes []TargetAuthorizationOutcome, err error, keyItems map[gateway.SubscriptionTargetKey][]*recheckSweepItem, verifiedAt time.Time, report *SweepReport) {
	outByKey := make(map[gateway.SubscriptionTargetKey]TargetAuthorizationOutcome, len(outcomes))
	for _, outcome := range outcomes {
		outByKey[outcome.Key] = outcome
	}
	for _, key := range keys {
		for _, item := range keyItems[key] {
			if item.verified || item.revoked || item.stale {
				continue
			}
			if !markRecheckSnapshotCurrent(item) {
				continue
			}
			reason := RevocationReasonDBError
			switch {
			case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
				reason = RevocationReasonSweepTimeout
			case err == nil:
				outcome, ok := outByKey[key]
				if !ok {
					reason = RevocationReasonDBError
				} else if !outcome.Authorized {
					if errors.Is(outcome.Err, context.DeadlineExceeded) || errors.Is(outcome.Err, context.Canceled) {
						reason = RevocationReasonSweepTimeout
					} else if errors.Is(outcome.Err, ErrTargetUnauthorized) {
						reason = RevocationReasonDenied
					}
				} else {
					item.remaining--
					if item.remaining == 0 && markRecheckVerified(item, verifiedAt) {
						item.verified = true
					}
					continue
				}
			}
			if item.session.closeIfGeneration(item.snap.Generation, reason) {
				item.revoked, item.reason = true, reason
				if reason == RevocationReasonSweepTimeout && report.Timeouts == 0 {
					report.Timeouts++
				}
			} else {
				item.stale = true
			}
		}
	}
}

func markRecheckSnapshotCurrent(item *recheckSweepItem) bool {
	item.session.mu.Lock()
	defer item.session.mu.Unlock()
	if item.session.closed || item.session.generation != item.snap.Generation {
		item.stale = true
		return false
	}
	return true
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

// StopContext cancels active DB checks and joins the loop and its authorization workers within ctx's budget.
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
			return w.waitForWorkers(ctx)
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
		return w.waitForWorkers(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *RecheckWorker) waitForWorkers(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		w.workerWG.Wait()
		close(done)
	}()
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

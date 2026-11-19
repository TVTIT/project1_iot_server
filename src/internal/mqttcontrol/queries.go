package mqttcontrol

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"iot-platform/internal/digitaltwin"

	"github.com/google/uuid"
)

const (
	controlQueryQueueCapacity = 32
	controlQueryOutstanding   = 32
	controlQueryTTL           = 5 * time.Second
	controlQueryCooldown      = 30 * time.Second
	controlQueryScanInterval  = 10 * time.Second
)

// ControlQueryRepository reserves due Sensors and rechecks transient contexts.
type ControlQueryRepository interface {
	ReserveDueControlQueries(context.Context, int, time.Duration) ([]digitaltwin.ControlQueryContext, error)
	ControlQueryContextCurrent(context.Context, digitaltwin.ControlQueryContext) (bool, error)
}

// ControlQueryPublisher is the narrow transport surface for query envelopes.
type ControlQueryPublisher interface {
	PublishConfigurationQuery(context.Context, digitaltwin.ConfigurationQuery) error
	Ready() bool
}

// ControlSnapshotCandidate is transient and explicitly untrusted for mutation.
// R7 must recheck its query context in the transaction that makes a decision.
type ControlSnapshotCandidate struct {
	Query         digitaltwin.ControlQueryContext
	Snapshot      digitaltwin.ConfigurationSnapshot
	LocalDeadline time.Time
}

// ControlSnapshotHandler consumes one validated but untrusted snapshot candidate.
type ControlSnapshotHandler func(context.Context, ControlSnapshotCandidate)

// ControlQueryMetrics contains only fixed-cardinality aggregate counters.
type ControlQueryMetrics struct {
	scanned, published, offline, timeout, invalid, unavailable, queueFull, candidate atomic.Int64
}

// ControlQueryMetricsSnapshot is a point-in-time copy of query counters.
type ControlQueryMetricsSnapshot struct {
	Scanned     int64 `json:"scanned"`
	Published   int64 `json:"published"`
	Offline     int64 `json:"offline"`
	Timeout     int64 `json:"timeout"`
	Invalid     int64 `json:"invalid"`
	Unavailable int64 `json:"unavailable"`
	QueueFull   int64 `json:"queue_full"`
	Candidate   int64 `json:"candidate"`
}

// Snapshot returns aggregate query counters without identity labels.
func (m *ControlQueryMetrics) Snapshot() ControlQueryMetricsSnapshot {
	if m == nil {
		return ControlQueryMetricsSnapshot{}
	}
	return ControlQueryMetricsSnapshot{
		Scanned: m.scanned.Load(), Published: m.published.Load(), Offline: m.offline.Load(),
		Timeout: m.timeout.Load(), Invalid: m.invalid.Load(), Unavailable: m.unavailable.Load(),
		QueueFull: m.queueFull.Load(), Candidate: m.candidate.Load(),
	}
}

type pendingControlQuery struct {
	query    digitaltwin.ConfigurationQuery
	context  digitaltwin.ControlQueryContext
	deadline time.Time
}

type controlQueryJob struct {
	pending pendingControlQuery
}

// ControlQueryRuntime is one scanner plus one fixed publish worker. Correlation
// is RAM-only, globally bounded, and discarded when this runtime closes.
type ControlQueryRuntime struct {
	repo      ControlQueryRepository
	publisher ControlQueryPublisher
	handler   ControlSnapshotHandler
	metrics   *ControlQueryMetrics
	now       func() time.Time

	mu       sync.Mutex
	dispatch sync.Mutex
	pending  map[string]pendingControlQuery
	byEntity map[string]string
	jobs     chan controlQueryJob
	started  bool
	closed   bool
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	done     chan struct{}
}

// NewControlQueryRuntime creates an inert bounded query runtime.
func NewControlQueryRuntime(repo ControlQueryRepository, publisher ControlQueryPublisher, handler ControlSnapshotHandler, metrics *ControlQueryMetrics) (*ControlQueryRuntime, error) {
	if nilDependency(repo) || nilDependency(publisher) || handler == nil {
		return nil, digitaltwin.ErrInvalidInput
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &ControlQueryRuntime{
		repo: repo, publisher: publisher, handler: handler, metrics: metrics, now: time.Now,
		pending:  make(map[string]pendingControlQuery, controlQueryOutstanding),
		byEntity: make(map[string]string, controlQueryOutstanding),
		jobs:     make(chan controlQueryJob, controlQueryQueueCapacity),
		ctx:      ctx, cancel: cancel, done: make(chan struct{}),
	}, nil
}

// Start launches the fixed scanner and single query publisher worker.
func (r *ControlQueryRuntime) Start() error {
	r.mu.Lock()
	if r.started || r.closed {
		r.mu.Unlock()
		return digitaltwin.ErrInvalidInput
	}
	r.started = true
	r.wg.Add(2)
	r.mu.Unlock()
	go r.scanLoop()
	go r.publishLoop()
	go func() { r.wg.Wait(); close(r.done) }()
	return nil
}

func (r *ControlQueryRuntime) scanLoop() {
	defer r.wg.Done()
	r.scan()
	ticker := time.NewTicker(controlQueryScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.scan()
		}
	}
}

func (r *ControlQueryRuntime) scan() {
	if r.ctx.Err() != nil {
		return
	}
	r.expirePending()
	if !r.publisher.Ready() {
		r.metricsOffline()
		return
	}
	r.mu.Lock()
	available := controlQueryOutstanding - len(r.pending)
	r.mu.Unlock()
	if available <= 0 {
		return
	}
	batch, err := r.repo.ReserveDueControlQueries(r.ctx, min(available, digitaltwin.MaxControlQueryPageSize), controlQueryCooldown)
	if err != nil {
		if errors.Is(err, digitaltwin.ErrDependencyUnavailable) {
			r.metricsUnavailable()
		}
		return
	}
	if r.metrics != nil {
		r.metrics.scanned.Add(int64(len(batch)))
	}
	for _, target := range batch {
		if r.ctx.Err() != nil {
			return
		}
		now := r.now().UTC().Truncate(time.Microsecond)
		id, err := uuid.NewRandom()
		if err != nil {
			continue
		}
		query := digitaltwin.ConfigurationQuery{
			ProtocolVersion: 1, MessageType: "configuration_query", RequestID: id.String(),
			GatewayID: target.GatewayID, EntityID: target.EntityURN,
			IssuedAt: now, ExpiresAt: now.Add(controlQueryTTL),
		}
		pending := pendingControlQuery{query: query, context: target, deadline: query.ExpiresAt}
		r.mu.Lock()
		if len(r.pending) >= controlQueryOutstanding || r.byEntity[target.EntityURN] != "" {
			r.mu.Unlock()
			continue
		}
		r.pending[query.RequestID] = pending
		r.byEntity[target.EntityURN] = query.RequestID
		select {
		case r.jobs <- controlQueryJob{pending: pending}:
			r.mu.Unlock()
		default:
			delete(r.pending, query.RequestID)
			delete(r.byEntity, target.EntityURN)
			r.mu.Unlock()
			if r.metrics != nil {
				r.metrics.queueFull.Add(1)
			}
		}
	}
}

func (r *ControlQueryRuntime) publishLoop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case job := <-r.jobs:
			p := job.pending
			ctx, cancel := context.WithDeadline(r.ctx, p.deadline)
			err := r.publisher.PublishConfigurationQuery(ctx, p.query)
			cancel()
			if err != nil {
				r.remove(p.query.RequestID)
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					if r.metrics != nil {
						r.metrics.timeout.Add(1)
					}
				} else if errors.Is(err, digitaltwin.ErrDependencyUnavailable) {
					r.metricsUnavailable()
				} else if r.metrics != nil {
					r.metrics.invalid.Add(1)
				}
				continue
			}
			if r.metrics != nil {
				r.metrics.published.Add(1)
			}
		}
	}
}

// HandleSnapshot validates the transient correlation and current DB context
// before forwarding an explicitly untrusted candidate to the bounded handler.
func (r *ControlQueryRuntime) HandleSnapshot(ctx context.Context, raw RawResult) {
	if ctx == nil || ctx.Err() != nil || r.ctx.Err() != nil || raw.Retained || raw.ReceivedAt.IsZero() {
		r.metricsInvalid()
		return
	}
	snapshot, err := digitaltwin.ParseMQTTConfigurationSnapshot(raw.Topic, raw.Payload, raw.Retained)
	if err != nil {
		r.metricsInvalid()
		return
	}
	r.mu.Lock()
	pending, ok := r.pending[snapshot.RequestID]
	if ok && (snapshot.GatewayID != pending.query.GatewayID || snapshot.EntityID != pending.query.EntityID ||
		snapshot.RequestID != pending.query.RequestID) {
		r.mu.Unlock()
		r.metricsInvalid()
		return
	}
	if !ok {
		r.mu.Unlock()
		r.metricsInvalid()
		return
	}
	if !r.now().Before(pending.deadline) {
		r.removeLocked(snapshot.RequestID)
		r.mu.Unlock()
		if r.metrics != nil {
			r.metrics.timeout.Add(1)
		}
		return
	}
	r.removeLocked(snapshot.RequestID)
	r.mu.Unlock()

	boundedCtx, cancel := context.WithDeadline(ctx, pending.deadline)
	stopRuntimeCancel := context.AfterFunc(r.ctx, cancel)
	defer func() {
		stopRuntimeCancel()
		cancel()
	}()
	current, err := r.repo.ControlQueryContextCurrent(boundedCtx, pending.context)
	if err != nil {
		if !r.now().Before(pending.deadline) || errors.Is(err, context.DeadlineExceeded) {
			if r.metrics != nil {
				r.metrics.timeout.Add(1)
			}
			return
		}
		r.metricsUnavailable()
		return
	}
	if !r.now().Before(pending.deadline) {
		if r.metrics != nil {
			r.metrics.timeout.Add(1)
		}
		return
	}
	if boundedCtx.Err() != nil || r.ctx.Err() != nil {
		return
	}
	if !current {
		r.metricsInvalid()
		return
	}
	r.dispatch.Lock()
	defer r.dispatch.Unlock()
	if boundedCtx.Err() != nil || r.ctx.Err() != nil || !r.now().Before(pending.deadline) {
		if !r.now().Before(pending.deadline) && r.metrics != nil {
			r.metrics.timeout.Add(1)
		}
		return
	}
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return
	}
	r.handler(boundedCtx, ControlSnapshotCandidate{
		Query: pending.context, Snapshot: snapshot, LocalDeadline: pending.deadline,
	})
	if r.metrics != nil {
		r.metrics.candidate.Add(1)
	}
}

func (r *ControlQueryRuntime) expirePending() {
	now := r.now()
	r.mu.Lock()
	for id, pending := range r.pending {
		if !now.Before(pending.deadline) {
			r.removeLocked(id)
			if r.metrics != nil {
				r.metrics.timeout.Add(1)
			}
		}
	}
	r.mu.Unlock()
}

func (r *ControlQueryRuntime) remove(id string) {
	r.mu.Lock()
	r.removeLocked(id)
	r.mu.Unlock()
}

func (r *ControlQueryRuntime) removeLocked(id string) {
	if pending, ok := r.pending[id]; ok {
		delete(r.pending, id)
		delete(r.byEntity, pending.context.EntityURN)
	}
}

func (r *ControlQueryRuntime) metricsInvalid() {
	if r.metrics != nil {
		r.metrics.invalid.Add(1)
	}
}

func (r *ControlQueryRuntime) metricsUnavailable() {
	if r.metrics != nil {
		r.metrics.unavailable.Add(1)
	}
}

func (r *ControlQueryRuntime) metricsOffline() {
	if r.metrics != nil {
		r.metrics.offline.Add(1)
	}
}

// Close stops and joins the fixed runtime workers within the caller's budget.
func (r *ControlQueryRuntime) Close(ctx context.Context) error {
	if ctx == nil {
		return digitaltwin.ErrInvalidInput
	}
	r.Stop()
	r.mu.Lock()
	started := r.started
	r.mu.Unlock()
	if !started {
		return nil
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop cancels scanning/publication and drops all process-local correlations.
func (r *ControlQueryRuntime) Stop() {
	r.cancel()
	r.dispatch.Lock()
	defer r.dispatch.Unlock()
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		clear(r.pending)
		clear(r.byEntity)
	}
	r.mu.Unlock()
}

// Pending returns the bounded in-memory correlation count.
func (r *ControlQueryRuntime) Pending() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending)
}

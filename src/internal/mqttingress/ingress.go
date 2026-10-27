package mqttingress

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"iot-platform/internal/metrics"
	"iot-platform/internal/telemetry"
)

// AdmissionDecision reports how Admit classified one delivery. Every value
// except AdmissionAccepted means the delivery was dropped in RAM with no
// database write and no application ACK; the Gateway's own durable outbox is
// responsible for retrying.
type AdmissionDecision uint8

const (
	// AdmissionAccepted means the item was copied and enqueued for a worker.
	AdmissionAccepted AdmissionDecision = iota
	// AdmissionRejectedNotRunning means the ingress has not been started.
	AdmissionRejectedNotRunning
	// AdmissionRejectedStopping means admission was already stopped.
	AdmissionRejectedStopping
	// AdmissionRejectedRetained means a retained delivery was dropped.
	AdmissionRejectedRetained
	// AdmissionRejectedOversized means the payload exceeded the ingress limit.
	AdmissionRejectedOversized
	// AdmissionRejectedInvalidTopic means the topic shape was wrong.
	AdmissionRejectedInvalidTopic
	// AdmissionRejectedQueueFull means the bounded queue had no free slot.
	AdmissionRejectedQueueFull
	// AdmissionRejectedByteBudget means the bounded raw-payload budget was full.
	AdmissionRejectedByteBudget
	// AdmissionRejectedRateLimit means this Gateway exhausted its attempt tokens.
	AdmissionRejectedRateLimit
	// AdmissionRejectedShaperCapacity means no bounded Gateway bucket could be allocated.
	AdmissionRejectedShaperCapacity
)

// Options configures the ingress core. Every value is validated by New.
type Options struct {
	MetricsEnabled  bool
	QueueCapacity   int
	WorkerCount     int
	MaxPayloadBytes int
	// BufferedPayloadBytes bounds raw payload copies held through worker completion.
	// Zero selects the backward-compatible 16 MiB default.
	BufferedPayloadBytes int64
	// Shaper enables bounded delivery-attempt shaping per Gateway. Zero value
	// remains disabled for callers that predate shaping.
	Shaper ShaperOptions
	// Microbatch enables bounded per-worker grouping. Disabled mode exclusively
	// uses Repository.Persist.
	Microbatch MicrobatchOptions
	// RAMDedup enables the bounded process-local confirmed-commit cache.
	RAMDedup DedupCacheOptions
	// PostCommitSink optionally receives validated batch snapshots after a new
	// persistence commit is confirmed. Implementations must admit without waiting.
	PostCommitSink PostCommitSink
	// PersistTimeout bounds connection acquisition, locks, SQL and commit of
	// one Repository.Persist call.
	PersistTimeout time.Duration
	// PublishTimeout bounds one whole Publisher.Publish call.
	PublishTimeout time.Duration
	// Logger receives reason-coded, payload-free structured events. Nil uses
	// slog.Default().
	Logger *slog.Logger
}

// PostCommitSink is the optional, ingress-owned seam for bounded committed
// telemetry admission. It is independent of repository and ACK outcomes.
type PostCommitSink interface {
	TrySubmitBatch(batch telemetry.BatchDTO, confirmedObservedAt time.Time) bool
}

// DedupCacheOptions enables the bounded process-local confirmed-commit cache.
type DedupCacheOptions struct {
	Enabled    bool
	MaxEntries int64
	MaxBytes   int64
}

// MicrobatchOptions contains finite per-worker collection caps.
type MicrobatchOptions struct {
	Enabled         bool
	MaxMessages     int
	MaxRows         int
	MaxPayloadBytes int64
	MaxWait         time.Duration
}

// item is one admitted delivery. topic is an immutable string; payload is a
// private copy taken during admission so a transport callback may reuse its
// receive buffer immediately after Admit returns.
type item struct {
	topic         string
	payload       []byte
	receivedAt    time.Time
	enqueuedAt    time.Time
	reservedBytes int64
}

// validatedDelivery carries one admitted item through persistence and its ACK.
// The payload reservation follows this value until its ACK attempt is complete.
type validatedDelivery struct {
	original        item
	message         *telemetry.Message
	dequeuedAt      time.Time
	validatedAt     time.Time
	confirmedAt     time.Time
	ownsReservation bool
	processing      bool
}

// Ingress is the bounded admission queue and fixed worker pool. One admitted
// item traverses Validator.Validate -> Repository.Persist -> post-commit ACK
// exactly once.
type Ingress struct {
	validator      Validator
	repository     Repository
	publisher      Publisher
	postCommitSink PostCommitSink

	workerCount                  int
	microbatch                   MicrobatchOptions
	batchRepository              telemetry.BatchRepository
	dedupCache                   *dedupCache
	clock                        ingressClock
	maxPayloadBytes              int
	bufferedPayloadCapacityBytes int64
	persistTimeout               time.Duration
	publishTimeout               time.Duration
	logger                       *slog.Logger

	queue chan item
	// stop is closed once by StopAdmission to tell workers to drain and exit.
	stop chan struct{}

	// admitMu makes StopAdmission a barrier against Admit: a read lock is held
	// for the whole admission (including the non-blocking enqueue), and the
	// write lock is held while admission is disabled. Once StopAdmission
	// returns, no further item can be enqueued, so a drain observes the queue
	// after all admitted work. The queue channel is never closed, so no Admit
	// can ever send on a closed channel.
	admitMu  sync.RWMutex
	stopping bool

	// started is the fast-path admission flag. startMu guards started,
	// draining and the worker WaitGroup Add/Wait ordering: Drain marks draining
	// under startMu before it waits, and Start adds workers only under startMu,
	// so a worker Add can never run concurrently with Wait.
	started  atomic.Bool
	startMu  sync.Mutex
	draining bool

	runCtx    context.Context
	cancelRun context.CancelFunc

	workers      sync.WaitGroup
	rotationDone chan struct{}

	counters                                                                    counters
	shaper                                                                      *Shaper
	metricsEnabled                                                              bool
	queueWait, validation, persistence, ackPublish, batchWait, groupPersistence metrics.Histogram
}

// New builds an ingress core. It rejects a missing dependency or a non-positive
// bound rather than allocating an unbounded queue or workers.
func New(opts Options, validator Validator, repository Repository, publisher Publisher, batchRepositories ...telemetry.BatchRepository) (*Ingress, error) {
	shaper, err := NewShaper(opts.Shaper)
	if err != nil {
		return nil, err
	}
	if opts.QueueCapacity < 1 {
		return nil, errors.New("mqttingress: queue capacity must be positive")
	}
	if opts.WorkerCount < 1 {
		return nil, errors.New("mqttingress: worker count must be positive")
	}
	if opts.MaxPayloadBytes < 1 {
		return nil, errors.New("mqttingress: max payload bytes must be positive")
	}
	bufferedPayloadBytes := opts.BufferedPayloadBytes
	if bufferedPayloadBytes == 0 {
		bufferedPayloadBytes = 16 << 20
	}
	if bufferedPayloadBytes < 0 || bufferedPayloadBytes < int64(opts.MaxPayloadBytes) || bufferedPayloadBytes > 256<<20 {
		return nil, errors.New("mqttingress: buffered payload bytes must be at least max payload bytes and no more than 256 MiB")
	}
	if opts.PersistTimeout <= 0 {
		return nil, errors.New("mqttingress: persist timeout must be positive")
	}
	if opts.PublishTimeout <= 0 {
		return nil, errors.New("mqttingress: publish timeout must be positive")
	}
	if isNilInterface(validator) {
		return nil, errors.New("mqttingress: validator is required")
	}
	if isNilInterface(repository) {
		return nil, errors.New("mqttingress: repository is required")
	}
	if isNilInterface(publisher) {
		return nil, errors.New("mqttingress: publisher is required")
	}
	var batchRepository telemetry.BatchRepository
	if opts.Microbatch.Enabled {
		if len(batchRepositories) != 1 || isNilInterface(batchRepositories[0]) {
			return nil, errors.New("mqttingress: enabled microbatch requires exactly one batch repository")
		}
		if opts.Microbatch.MaxMessages < 1 || opts.Microbatch.MaxMessages > 1024 || opts.Microbatch.MaxRows < 1 || opts.Microbatch.MaxRows > 100_000 || opts.Microbatch.MaxPayloadBytes < 1 || opts.Microbatch.MaxPayloadBytes > 256<<20 || opts.Microbatch.MaxWait < time.Millisecond || opts.Microbatch.MaxWait > 5*time.Second {
			return nil, errors.New("mqttingress: invalid microbatch bounds")
		}
		if opts.Microbatch.MaxPayloadBytes > bufferedPayloadBytes/int64(opts.WorkerCount) {
			return nil, errors.New("mqttingress: microbatch payload budget exceeds per-worker buffered payload budget")
		}
		batchRepository = batchRepositories[0]
	}
	if opts.RAMDedup.Enabled && (opts.RAMDedup.MaxEntries < 1 || opts.RAMDedup.MaxBytes < 1) {
		return nil, errors.New("mqttingress: enabled RAM dedup requires positive entry and byte bounds")
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	return &Ingress{
		metricsEnabled:               opts.MetricsEnabled,
		validator:                    validator,
		repository:                   repository,
		publisher:                    publisher,
		postCommitSink:               opts.PostCommitSink,
		workerCount:                  opts.WorkerCount,
		maxPayloadBytes:              opts.MaxPayloadBytes,
		bufferedPayloadCapacityBytes: bufferedPayloadBytes,
		persistTimeout:               opts.PersistTimeout,
		publishTimeout:               opts.PublishTimeout,
		logger:                       logger,
		queue:                        make(chan item, opts.QueueCapacity),
		stop:                         make(chan struct{}),
		runCtx:                       runCtx,
		cancelRun:                    cancelRun,
		shaper:                       shaper,
		microbatch:                   opts.Microbatch,
		batchRepository:              batchRepository,
		dedupCache:                   newDedupCache(opts.RAMDedup.Enabled, opts.RAMDedup.MaxEntries, opts.RAMDedup.MaxBytes, realIngressClock{}),
		clock:                        realIngressClock{},
	}, nil
}

// Start launches the fixed worker pool. It is safe to call once; a second call
// returns an error, and a call that races Drain is refused so no worker is
// added while Drain is waiting.
func (i *Ingress) Start() error {
	i.startMu.Lock()
	defer i.startMu.Unlock()
	if i.started.Load() {
		return errors.New("mqttingress: already started")
	}
	if i.draining {
		return errors.New("mqttingress: ingress is shutting down")
	}
	i.workers.Add(i.workerCount)
	i.started.Store(true)
	if i.dedupCache.Enabled() {
		i.rotationDone = make(chan struct{})
		go func(ctx context.Context, done chan struct{}) {
			defer close(done)
			_ = i.dedupCache.RunRotation(ctx)
		}(i.runCtx, i.rotationDone)
	}
	for w := 0; w < i.workerCount; w++ {
		go i.worker()
	}
	return nil
}

// Admit is the transport callback entry point. It performs only cheap checks
// and a non-blocking enqueue: no JSON decode, no database access, no goroutine
// per message and no ACK. It copies the topic (an immutable string) and the
// payload bytes before returning, so the caller may reuse its buffer.
func (i *Ingress) Admit(topic string, payload []byte, receivedAt time.Time, retained bool) AdmissionDecision {
	if i.metricsEnabled {
		i.counters.received.Add(1)
		i.counters.receivedBytes.Add(int64(len(payload)))
	}
	if !i.started.Load() {
		i.counters.rejectedNotRunning.Add(1)
		return AdmissionRejectedNotRunning
	}

	i.admitMu.RLock()
	defer i.admitMu.RUnlock()
	if i.stopping {
		i.counters.rejectedStopping.Add(1)
		return AdmissionRejectedStopping
	}
	if retained {
		i.counters.rejectedRetained.Add(1)
		return AdmissionRejectedRetained
	}
	if len(payload) > i.maxPayloadBytes {
		i.counters.rejectedOversized.Add(1)
		return AdmissionRejectedOversized
	}
	parsedTopic, err := telemetry.ParseTopic(topic)
	if err != nil {
		i.counters.rejectedInvalidTpc.Add(1)
		return AdmissionRejectedInvalidTopic
	}
	switch i.shaper.Allow(parsedTopic.GatewayID) {
	case ShaperRateLimited:
		i.counters.shaperRateLimited.Add(1)
		return AdmissionRejectedRateLimit
	case ShaperStateCapacity:
		i.counters.shaperStateCapacity.Add(1)
		return AdmissionRejectedShaperCapacity
	case ShaperPass:
		if i.shaper.options.Enabled {
			i.counters.shaperPassed.Add(1)
		}
	}

	reservedBytes := int64(len(payload))
	if !i.reservePayloadBytes(reservedBytes) {
		i.counters.byteBudgetFull.Add(1)
		return AdmissionRejectedByteBudget
	}
	it := item{topic: topic, payload: append([]byte(nil), payload...), receivedAt: receivedAt, reservedBytes: reservedBytes}
	if i.metricsEnabled {
		it.enqueuedAt = time.Now()
	}
	select {
	case i.queue <- it:
		if i.metricsEnabled {
			i.counters.acceptedBytes.Add(int64(len(payload)))
		}
		i.counters.accepted.Add(1)
		i.recordQueueHighWater(len(i.queue))
		return AdmissionAccepted
	default:
		i.releasePayloadBytes(reservedBytes)
		// Queue full: drop the delivery from RAM. The library MQTT PUBACK (if
		// any) is only transport receipt; the Gateway retries from its durable
		// outbox. No application ACK is emitted.
		i.counters.queueFull.Add(1)
		return AdmissionRejectedQueueFull
	}
}

// reservePayloadBytes atomically reserves a bounded amount before payload copy.
func (i *Ingress) reservePayloadBytes(bytes int64) bool {
	for {
		current := i.counters.bufferedPayloadBytes.Load()
		if bytes > i.bufferedPayloadCapacityBytes-current {
			return false
		}
		if i.counters.bufferedPayloadBytes.CompareAndSwap(current, current+bytes) {
			i.recordBufferedPayloadHighWater(current + bytes)
			return true
		}
	}
}

func (i *Ingress) releasePayloadBytes(bytes int64) {
	if bytes > 0 {
		i.counters.bufferedPayloadBytes.Add(-bytes)
	}
}

func (i *Ingress) recordBufferedPayloadHighWater(observed int64) {
	for {
		current := i.counters.bufferedPayloadHighWaterBytes.Load()
		if observed <= current {
			return
		}
		if i.counters.bufferedPayloadHighWaterBytes.CompareAndSwap(current, observed) {
			return
		}
	}
}

// recordQueueHighWater updates the admission-observed queue high-water mark.
// Observation point: sampled via len(queue) immediately after a successful
// non-blocking enqueue in Admit().
// Limits: bounded within [0, QueueCapacity]; monotonically non-decreasing.
//
// Concurrent workers may consume items between the channel send and len()
// observation, so this represents an admission-observed high-water mark rather
// than an exact absolute instantaneous queue peak.
func (i *Ingress) recordQueueHighWater(observed int) {
	if observed <= 0 {
		return
	}
	qCap := cap(i.queue)
	if observed > qCap {
		observed = qCap
	}
	obs64 := int64(observed)
	for {
		current := i.counters.queueHighWater.Load()
		if obs64 <= current {
			return
		}
		if i.counters.queueHighWater.CompareAndSwap(current, obs64) {
			return
		}
	}
}

// StopAdmission disables new admissions and signals workers to begin draining.
// It is idempotent. It holds the admission write lock, so once it returns no
// further item can be enqueued and the remaining queue contents are final.
func (i *Ingress) StopAdmission() {
	i.admitMu.Lock()
	defer i.admitMu.Unlock()
	if i.stopping {
		return
	}
	i.stopping = true
	close(i.stop)
}

// Accepting reports whether the ingress is started and still admitting new
// deliveries. It is used by readiness composition: a started worker pool that
// has begun stopping is no longer accepting telemetry.
func (i *Ingress) Accepting() bool {
	if !i.started.Load() {
		return false
	}
	i.admitMu.RLock()
	defer i.admitMu.RUnlock()
	return !i.stopping
}

// Drain stops admission, lets workers finish the items already admitted within
// ctx's deadline, and waits for every worker to exit. ctx is an independent
// shutdown context (for example a fresh deadline), not the signal-canceled
// server context, so workers still get a chance to persist and ACK. If the
// deadline is reached first, Drain force-cancels the workers' processing
// context, then still waits for them to return. Drain returns nil when workers
// finished within the deadline and ctx.Err() when forced cancellation was used.
func (i *Ingress) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	// Serialize with Start so the worker WaitGroup is never Add'ed while Wait
	// runs, and refuse a Start that races this shutdown.
	i.startMu.Lock()
	i.draining = true
	started := i.started.Load()
	i.startMu.Unlock()

	if started {
		i.StopAdmission()
	}

	done := make(chan struct{})
	go func() {
		i.workers.Wait()
		close(done)
	}()

	select {
	case <-done:
		i.cancelRun()
		if i.rotationDone != nil {
			<-i.rotationDone
		}
		return nil
	case <-ctx.Done():
		i.cancelRun()
		<-done
		if i.rotationDone != nil {
			<-i.rotationDone
		}
		return ctx.Err()
	}
}

// worker takes one item at a time until admission stops and the queue is empty.
func (i *Ingress) worker() {
	defer i.workers.Done()
	if i.microbatch.Enabled {
		i.microbatchWorker()
		return
	}
	for {
		select {
		case it := <-i.queue:
			i.process(it)
		case <-i.stop:
			i.drainQueue()
			return
		}
	}
}

type ingressTimer interface {
	C() <-chan time.Time
	Stop()
}
type ingressClock interface {
	NewTimer(time.Duration) ingressTimer
	Now() time.Time
}
type realIngressClock struct{}
type realIngressTimer struct{ t *time.Timer }

// setClock keeps ingress lifecycle timestamps and cache proof expiry on the
// same clock. It is primarily useful to deterministic package tests.
func (i *Ingress) setClock(clock ingressClock) {
	i.clock = clock
	if i.dedupCache != nil {
		i.dedupCache.clock = clock
	}
}

func (realIngressClock) NewTimer(d time.Duration) ingressTimer {
	return realIngressTimer{time.NewTimer(d)}
}
func (realIngressClock) Now() time.Time        { return time.Now() }
func (t realIngressTimer) C() <-chan time.Time { return t.t.C }
func (t realIngressTimer) Stop() {
	if !t.t.Stop() {
		select {
		case <-t.t.C:
		default:
		}
	}
}

func (i *Ingress) microbatchWorker() {
	var timer ingressTimer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		// Cancellation owns every item still in the shared queue; no unconfirmed
		// item is acknowledged and each reservation is released once.
		for {
			select {
			case it := <-i.queue:
				i.releasePayloadBytes(it.reservedBytes)
			default:
				return
			}
		}
	}()
	var group []*validatedDelivery
	var rows int
	var bytes int64
	var deadline time.Time
	flush := func(ctx context.Context, reason int) {
		if len(group) == 0 {
			return
		}
		members := group
		if i.metricsEnabled {
			i.counters.groupCount.Add(1)
			i.counters.groupMembers.Add(int64(len(members)))
			i.counters.groupRows.Add(int64(rows))
			i.counters.groupPayloadBytes.Add(bytes)
			for {
				old := i.counters.groupMaxMembers.Load()
				if int64(len(members)) <= old || i.counters.groupMaxMembers.CompareAndSwap(old, int64(len(members))) {
					break
				}
			}
			switch reason {
			case 1:
				i.counters.flushMessages.Add(1)
			case 2:
				i.counters.flushRows.Add(1)
			case 3:
				i.counters.flushBytes.Add(1)
			case 4:
				i.counters.flushTimer.Add(1)
			case 5:
				i.counters.flushStop.Add(1)
			}
		}
		group = nil
		rows = 0
		bytes = 0
		deadline = time.Time{}
		if timer != nil {
			timer.Stop()
			timer = nil
		}
		i.persistGroup(ctx, members)
	}
	process := func(it item) {
		d := &validatedDelivery{original: it, ownsReservation: true}
		if i.runCtx.Err() != nil {
			i.releaseDelivery(d)
			return
		}
		i.startDelivery(d)
		i.counters.processed.Add(1)
		if !i.validateDelivery(d) {
			i.releaseDelivery(d)
			return
		}
		d.validatedAt = i.clock.Now()
		if cached, ok := i.confirmedCacheHit(d); ok {
			i.publishACK(d, cached)
			i.releaseDelivery(d)
			return
		}
		memberRows := d.message.SampleCount()
		if memberRows > i.microbatch.MaxRows || int64(len(it.payload)) > i.microbatch.MaxPayloadBytes {
			// Attribute a preceding group's flush to the standalone member's
			// exceeded cap (rows before bytes if both are exceeded).
			reason := 3
			if memberRows > i.microbatch.MaxRows {
				reason = 2
			}
			flush(i.runCtx, reason)
			if i.metricsEnabled {
				i.counters.standaloneOversized.Add(1)
			}
			i.persistStandaloneBatch(i.runCtx, d)
			return
		}
		if len(group) > 0 && (len(group) >= i.microbatch.MaxMessages || rows+memberRows > i.microbatch.MaxRows || bytes+it.reservedBytes > i.microbatch.MaxPayloadBytes) {
			// Simultaneous cap precedence: messages > rows > bytes.
			reason := 3
			if len(group) >= i.microbatch.MaxMessages {
				reason = 1
			} else if rows+memberRows > i.microbatch.MaxRows {
				reason = 2
			}
			flush(i.runCtx, reason)
		}
		if len(group) == 0 {
			deadline = i.clock.Now().Add(i.microbatch.MaxWait)
			timer = i.clock.NewTimer(i.microbatch.MaxWait)
		}
		group = append(group, d)
		rows += memberRows
		bytes += it.reservedBytes
		if len(group) >= i.microbatch.MaxMessages || rows >= i.microbatch.MaxRows || bytes >= i.microbatch.MaxPayloadBytes {
			reason := 3
			if len(group) >= i.microbatch.MaxMessages {
				reason = 1
			} else if rows >= i.microbatch.MaxRows {
				reason = 2
			}
			flush(i.runCtx, reason)
		}
	}
	for {
		var timerC <-chan time.Time
		if timer != nil {
			timerC = timer.C()
		}
		select {
		case it := <-i.queue:
			process(it)
		case <-timerC:
			flush(i.runCtx, 4)
		case <-i.stop:
			flush(i.runCtx, 5)
			for {
				select {
				case it := <-i.queue:
					process(it)
				default:
					flush(i.runCtx, 5)
					return
				}
			}
		case <-i.runCtx.Done():
			for n, d := range group {
				i.releaseDelivery(d)
				group[n] = nil
			}
			group = nil
			return
		}
		if !deadline.IsZero() && !i.clock.Now().Before(deadline) {
			flush(i.runCtx, 4)
		}
	}
}

func (i *Ingress) persistStandaloneBatch(ctx context.Context, d *validatedDelivery) {
	defer i.releaseDelivery(d)
	// Target caps never split an otherwise-valid message; use the existing
	// single-message persistence contract without revalidation or reshaping.
	confirmed, ok := i.persistDeliveryContext(ctx, d)
	if ok {
		if confirmed.result.Outcome == telemetry.CommitOutcomeCommitted {
			i.publishConfirmedProof(d, confirmed)
			i.submitCommittedBatch(d, confirmed)
		}
		i.publishACK(d, confirmed)
	}
}

func (i *Ingress) persistGroup(ctx context.Context, members []*validatedDelivery) {
	i.persistValidatedGroup(ctx, members)
}

func (i *Ingress) persistValidatedGroup(ctx context.Context, members []*validatedDelivery) {
	defer func() {
		for n, d := range members {
			i.releaseDelivery(d)
			members[n] = nil
		}
	}()
	msgs := make([]*telemetry.Message, len(members))
	for n, d := range members {
		msgs[n] = d.message
	}
	pctx, cancel := context.WithTimeout(ctx, i.persistTimeout)
	defer cancel()
	groupStart := i.metricStart()
	if i.metricsEnabled {
		for _, d := range members {
			i.batchWait.Observe(time.Since(d.validatedAt))
		}
	}
	results := i.batchRepository.PersistBatch(pctx, msgs)
	commitObserved := i.clock.Now()
	commitObservedAt := commitObserved.UTC()
	if i.metricsEnabled {
		elapsed := time.Since(groupStart)
		i.groupPersistence.Observe(elapsed)
		// Preserve the historical per-delivery Persistence series while exposing
		// the actual once-per-group SQL duration separately above.
		for range members {
			i.persistence.Observe(elapsed)
		}
	}
	if len(results) != len(members) {
		i.counters.persistFailures.Add(int64(len(members)))
		i.logReject("batch_result", &telemetry.ReasonError{Code: telemetry.ReasonCommitUnconfirmed})
		return
	}
	confirmed := make([]confirmedDelivery, len(members))
	valid := make([]bool, len(members))
	seen := make([]bool, len(members))
	for _, r := range results {
		if r.InputIndex < 0 || r.InputIndex >= len(members) || seen[r.InputIndex] {
			i.counters.persistFailures.Add(int64(len(members)))
			return
		}
		seen[r.InputIndex] = true
	}
	for _, ok := range seen {
		if !ok {
			i.counters.persistFailures.Add(int64(len(members)))
			return
		}
	}
	// Validate every success claim and its complete identity before associating
	// any result with a delivery or publishing any member ACK.
	for _, r := range results {
		if r.Err != nil {
			continue
		}
		if r.Outcome != telemetry.CommitOutcomeCommitted && r.Outcome != telemetry.CommitOutcomeDuplicateCommitted {
			i.counters.commitUnconfirmed.Add(1)
			return
		}
		d := members[r.InputIndex]
		if !identityMatches(d.message, r.Identity) || r.SamplesSaved < 0 {
			i.counters.identityMismatch.Add(1)
			return
		}
	}
	for _, r := range results {
		if r.Err != nil {
			i.counters.persistFailures.Add(1)
			i.logReject("persist", r.Err)
			continue
		}
		confirmed[r.InputIndex] = confirmedDelivery{result: telemetry.PersistResult{Identity: r.Identity, Outcome: r.Outcome, SamplesSaved: r.SamplesSaved}, confirmedAt: commitObservedAt, observedAt: commitObserved}
		valid[r.InputIndex] = true
	}
	for n, ok := range valid {
		if ok {
			r := confirmed[n].result
			if r.Outcome == telemetry.CommitOutcomeCommitted {
				i.counters.committed.Add(1)
				i.counters.samplesNew.Add(int64(r.SamplesSaved))
				i.publishConfirmedProof(members[n], confirmed[n])
			} else {
				i.counters.duplicateCommitted.Add(1)
			}
		}
	}
	// Submit every confirmed-new member before entering the ACK loop: cancellation
	// or a publish failure for one member cannot erase another committed event.
	for n, ok := range valid {
		if ok && confirmed[n].result.Outcome == telemetry.CommitOutcomeCommitted {
			i.submitCommittedBatch(members[n], confirmed[n])
		}
	}
	for n, ok := range valid {
		if ok {
			if ctx.Err() != nil {
				return
			}
			i.publishACK(members[n], confirmed[n])
		}
	}
}

// drainQueue processes the remaining admitted items after admission stopped.
// Because StopAdmission is a barrier, no new item can appear once it returns.
func (i *Ingress) drainQueue() {
	for {
		select {
		case it := <-i.queue:
			i.process(it)
		default:
			return
		}
	}
}

// process runs the single-item pipeline. Any error or unconfirmed outcome wins
// over an apparently successful result and suppresses the ACK.
func (i *Ingress) process(it item) {
	delivery := &validatedDelivery{original: it, ownsReservation: true}
	i.startDelivery(delivery)
	defer i.releaseDelivery(delivery)
	i.counters.processed.Add(1)

	if !i.validateDelivery(delivery) {
		return
	}
	if cached, ok := i.confirmedCacheHit(delivery); ok {
		i.publishACK(delivery, cached)
		return
	}
	confirmed, ok := i.persistDelivery(delivery)
	if !ok {
		return
	}
	if confirmed.result.Outcome == telemetry.CommitOutcomeCommitted {
		i.publishConfirmedProof(delivery, confirmed)
		i.submitCommittedBatch(delivery, confirmed)
	}
	i.publishACK(delivery, confirmed)
}

// startDelivery starts the per-delivery lifecycle at worker dequeue, excluding
// queue wait. Collection, persistence and ACK work remain active until release.
func (i *Ingress) startDelivery(delivery *validatedDelivery) {
	delivery.dequeuedAt = i.clock.Now()
	delivery.processing = true
	if i.metricsEnabled {
		i.counters.inFlight.Add(1)
		i.queueWait.Observe(delivery.dequeuedAt.Sub(delivery.original.enqueuedAt))
	}
}

func (i *Ingress) releaseDelivery(delivery *validatedDelivery) {
	if delivery.ownsReservation {
		delivery.original.payload = nil
		i.releasePayloadBytes(delivery.original.reservedBytes)
		delivery.ownsReservation = false
	}
	if delivery.processing {
		// Legacy aggregates are unconditional, in delivery-nanoseconds and
		// completed deliveries, not independent SQL-operation duration samples.
		i.counters.processingNanos.Add(i.clock.Now().Sub(delivery.dequeuedAt).Nanoseconds())
		i.counters.processingCount.Add(1)
		if i.metricsEnabled {
			i.counters.inFlight.Add(-1)
		}
		delivery.processing = false
	}
}

// validateDelivery records dequeue/validation observations and keeps the exact
// validator receipt timestamp from the admitted item.
func (i *Ingress) validateDelivery(delivery *validatedDelivery) bool {
	it := delivery.original
	validationStart := i.metricStart()
	message, err := i.validator.Validate(it.topic, it.payload, it.receivedAt)
	i.metricObserve(&i.validation, validationStart)
	if err != nil {
		i.counters.validationRejected.Add(1)
		i.logReject("validate", err)
		return false
	}
	delivery.message = message
	delivery.validatedAt = time.Now()
	return true
}

type confirmedDelivery struct {
	result      telemetry.PersistResult
	confirmedAt time.Time
	observedAt  time.Time
}

// persistDelivery validates the persistence result and records confirmed
// counters before any ACK work begins.
func (i *Ingress) persistDelivery(delivery *validatedDelivery) (confirmedDelivery, bool) {
	return i.persistDeliveryContext(i.runCtx, delivery)
}

func (i *Ingress) persistDeliveryContext(parent context.Context, delivery *validatedDelivery) (confirmedDelivery, bool) {
	persistCtx, cancelPersist := context.WithTimeout(parent, i.persistTimeout)
	persistStart := i.metricStart()
	result, persistErr := i.repository.Persist(persistCtx, delivery.message)
	i.metricObserve(&i.persistence, persistStart)
	cancelPersist()
	if persistErr != nil {
		i.counters.persistFailures.Add(1)
		i.logReject("persist", persistErr)
		return confirmedDelivery{}, false
	}
	if result.Outcome != telemetry.CommitOutcomeCommitted && result.Outcome != telemetry.CommitOutcomeDuplicateCommitted {
		i.counters.commitUnconfirmed.Add(1)
		i.logReject("commit", &telemetry.ReasonError{Code: telemetry.ReasonCommitUnconfirmed})
		return confirmedDelivery{}, false
	}
	if !identityMatches(delivery.message, result.Identity) {
		i.counters.identityMismatch.Add(1)
		i.logReject("identity", &telemetry.ReasonError{Code: telemetry.ReasonMessageConflict})
		return confirmedDelivery{}, false
	}

	// confirmedAt is the core's own UTC receipt of the confirmed commit, taken
	// after Persist returned. It is not the measurement time and not an exact
	// PostgreSQL commit timestamp; a retry may report a different value for the
	// same message_id/status.
	observedAt := i.clock.Now()
	delivery.confirmedAt = observedAt.UTC()

	// The database result is final: record the confirmed commit regardless of
	// whether the ACK publish below succeeds.
	if result.Outcome == telemetry.CommitOutcomeCommitted {
		i.counters.committed.Add(1)
		i.counters.samplesNew.Add(int64(result.SamplesSaved))
	} else {
		i.counters.duplicateCommitted.Add(1)
	}

	return confirmedDelivery{result: result, confirmedAt: delivery.confirmedAt, observedAt: observedAt}, true
}

func (i *Ingress) submitCommittedBatch(delivery *validatedDelivery, confirmed confirmedDelivery) {
	if i.postCommitSink == nil || delivery == nil || delivery.message == nil ||
		confirmed.result.Outcome != telemetry.CommitOutcomeCommitted ||
		!identityMatches(delivery.message, confirmed.result.Identity) {
		return
	}
	// Message.Batch returns an independent sample slice, so the sink sees the
	// exact validated batch that was passed to persistence, not later state.
	i.postCommitSink.TrySubmitBatch(delivery.message.Batch(), confirmed.observedAt)
}

func (i *Ingress) confirmedCacheHit(delivery *validatedDelivery) (confirmedDelivery, bool) {
	if !i.dedupCache.Enabled() {
		return confirmedDelivery{}, false
	}
	key, metadata, ok := buildDedupMetadata(delivery.message, i.clock.Now())
	if !ok || i.dedupCache.Lookup(key, metadata) != dedupLookupHit {
		return confirmedDelivery{}, false
	}
	identity := delivery.message.Identity()
	result := telemetry.PersistResult{Identity: identity, Outcome: telemetry.CommitOutcomeDuplicateCommitted}
	i.counters.duplicateCommitted.Add(1)
	return confirmedDelivery{result: result, confirmedAt: metadata.confirmedCommitAt}, true
}

// DedupCacheSnapshot returns fixed cache counters and O(1) generation accounting.
func (i *Ingress) DedupCacheSnapshot() DedupCacheSnapshot {
	if i == nil || i.dedupCache == nil {
		return DedupCacheSnapshot{}
	}
	return i.dedupCache.Snapshot()
}

func (i *Ingress) publishConfirmedProof(delivery *validatedDelivery, confirmed confirmedDelivery) {
	if i.dedupCache == nil || confirmed.result.Outcome != telemetry.CommitOutcomeCommitted || !identityMatches(delivery.message, confirmed.result.Identity) || confirmed.result.SamplesSaved != delivery.message.SampleCount() {
		return
	}
	key, metadata, ok := buildDedupMetadata(delivery.message, confirmed.confirmedAt)
	if !ok {
		// Record the confirmed outcome's ineligible cache proof as an invalid
		// publication skip; this never changes the already-committed result.
		key, metadata = dedupKey{}, dedupMetadata{}
	}
	i.dedupCache.Publish(key, metadata)
}

// publishACK constructs and publishes one application ACK. It deliberately
// has no in-process retry: the Gateway's durable outbox owns retries.
func (i *Ingress) publishACK(delivery *validatedDelivery, confirmed confirmedDelivery) {
	if i.runCtx.Err() != nil {
		return
	}
	message := delivery.message
	topic := message.Topic()
	batch := message.Batch()
	ackTopic, ack, buildErr := telemetry.NewCommitACK(topic.GatewayID, batch.MessageID, confirmed.result.Outcome, confirmed.confirmedAt)
	if buildErr != nil {
		i.counters.ackBuildFailures.Add(1)
		i.logReject("ack_build", buildErr)
		return
	}
	wire, marshalErr := json.Marshal(ack)
	if marshalErr != nil {
		i.counters.ackBuildFailures.Add(1)
		i.logReject("ack_marshal", &telemetry.ReasonError{Code: telemetry.ReasonAckPublishUnconfirmed})
		return
	}

	publishCtx, cancelPublish := context.WithTimeout(i.runCtx, i.publishTimeout)
	publishStart := i.metricStart()
	publishErr := i.publisher.Publish(publishCtx, Delivery{Topic: ackTopic, Payload: wire, QoS: 1, Retained: false})
	i.metricObserve(&i.ackPublish, publishStart)
	cancelPublish()
	if publishErr != nil {
		// The database result is committed and must be kept. A publish timeout
		// or disconnect proves only an unconfirmed ACK, not a failed write.
		i.counters.ackPublishFailures.Add(1)
		i.logReject("ack_publish", &telemetry.ReasonError{Code: telemetry.ReasonAckPublishUnconfirmed})
		return
	}

	i.counters.ackPublished.Add(1)
}

func (i *Ingress) metricStart() time.Time {
	if i.metricsEnabled {
		return time.Now()
	}
	return time.Time{}
}

func (i *Ingress) metricObserve(h *metrics.Histogram, start time.Time) {
	if i.metricsEnabled {
		h.Observe(time.Since(start))
	}
}

// identityMatches verifies that the persisted identity is the same resource and
// message the validated input described. A mismatch suppresses the ACK.
func identityMatches(message *telemetry.Message, identity telemetry.MessageIdentity) bool {
	return message.Identity() == identity
}

// logReject emits a payload-free, secret-free reason-coded event. It never logs
// the raw payload, driver error text, credentials or DSN.
func (i *Ingress) logReject(stage string, err error) {
	code := ""
	var reason *telemetry.ReasonError
	if errors.As(err, &reason) && reason != nil {
		code = string(reason.Code)
	}
	level := slog.LevelDebug
	if stage == "persist" {
		level = slog.LevelWarn
	}
	i.logger.Log(context.Background(), level, "mqtt ingress item not acknowledged",
		"stage", stage,
		"reason_code", code,
		"failure_class", int(telemetry.ClassifyFailure(err)),
	)
}

func isNilInterface(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

package gatewaysim

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// The scheduler is the single owner of batch generation, replay of durable
// pending rows, the bounded in-flight set and the publish/dispatch loop for one
// Gateway simulator process. It owns no durable state itself: the SQLite outbox
// owns sequence allocation, serialization and the pending rows, and the ACK
// processor owns deletes and the wake signal. The scheduler only decides when to
// enqueue a new batch, which durable rows to (re)publish, and when a retry is
// due.
//
// Scope mirrors Task 3.5.5: RAM maps/timers are bounded by GATEWAY_MAX_INFLIGHT,
// a stored row is never decoded/marshalled again, a PUBACK never deletes a row,
// a failed or timed-out attempt retains the row and retries with bounded
// exponential backoff and jitter, and shutdown keeps every unacknowledged row.

// Scheduler sentinel errors. They are stable so a caller can classify a stop
// without inspecting driver or JSON error text.
var (
	// ErrSchedulerEnqueue marks a non-capacity Enqueue failure.
	ErrSchedulerEnqueue = errors.New("gatewaysim: scheduler enqueue failed")
	// ErrSchedulerPending marks a failed bounded pending scan.
	ErrSchedulerPending = errors.New("gatewaysim: scheduler pending scan failed")
	// ErrSchedulerLookup marks a failed bounded in-flight reconciliation read.
	ErrSchedulerLookup = errors.New("gatewaysim: scheduler in-flight lookup failed")
	// ErrSchedulerCommitUnknown marks an ambiguous outbox commit. The scheduler
	// stops for a supervised restart and never regenerates the batch.
	ErrSchedulerCommitUnknown = errors.New("gatewaysim: outbox commit outcome is unknown; supervised restart required")
	// ErrSchedulerCompletion marks a finite run whose completion budget expired
	// while rows were still pending. It never reports a partial run as complete.
	ErrSchedulerCompletion = errors.New("gatewaysim: finite completion budget expired with pending items")
	// ErrSchedulerFatal marks an unexpected internal stop without a classified
	// cause (for example a transport fatal signal without an error value).
	ErrSchedulerFatal = errors.New("gatewaysim: scheduler stopped on an internal failure")
)

// Publisher is the transport boundary the scheduler publishes through.
// *Transport satisfies it: Ready/ReadyCh expose the subscribed connection,
// Publish returns only after the broker acknowledges the MQTT PUBLISH (never the
// application commit), and FatalSignal/Fatal expose a fail-closed transport.
type Publisher interface {
	Ready() bool
	ReadyCh() <-chan struct{}
	Publish(ctx context.Context, topic string, payload []byte) error
	FatalSignal() <-chan struct{}
	Fatal() error
}

// AckObserver is the ACK-processor boundary the scheduler observes. *ACKProcessor
// satisfies it: Notify is signalled only after a delete commits, and Stats
// carries the application-acknowledged counter.
type AckObserver interface {
	Notify() <-chan struct{}
	FatalSignal() <-chan struct{}
	Fatal() error
	Stats() ACKStats
}

// in-flight entry states. Exactly one publish invocation is active per entry.
const (
	// statePublishing means one Publish invocation owns the entry.
	statePublishing = iota
	// stateAwaitAck means the PUBACK arrived and the entry waits for the
	// application ACK up to AckTimeout.
	stateAwaitAck
	// stateBackoff means a failed/timed-out attempt is retained and waits for a
	// jittered exponential retry delay.
	stateBackoff
)

// maxAdmissionPages bounds the pending pagination per loop pass. Each page holds
// at most MaxInflight rows and at most MaxInflight rows can already be in
// flight, so a handful of pages always reaches the first un-admitted row.
const maxAdmissionPages = 4

// parkDuration parks the single wake timer when there is no timed work. It is
// far enough away that it never fires during a real run and is not a poll.
const parkDuration = 24 * time.Hour

// inflightEntry is one durable pending row under the scheduler's bounded
// control. The item bytes are the exact stored topic/payload and are never
// re-serialized.
type inflightEntry struct {
	item    Item
	state   int
	retries int
	nextAt  time.Time
}

// publishResult reports one completed Publish invocation back to the loop.
type publishResult struct {
	messageID uuid.UUID
	err       error
}

// SchedulerStats is a payload-free snapshot safe to log or print. It never
// contains an error string, credential or SQLite path.
type SchedulerStats struct {
	// UniqueEnqueued counts new batches committed by this run (not replays).
	UniqueEnqueued int64
	// PublishAttempts counts every Publish invocation started, including
	// retries and replays; it is never used as a unique batch count.
	PublishAttempts int64
	// AppAcknowledged counts application ACKs whose pending-row delete committed.
	AppAcknowledged int64
	// PendingRows and PendingBytes are the durable pending totals in SQLite.
	PendingRows  int64
	PendingBytes int64
	// Inflight is the current bounded in-flight entry count.
	Inflight int
	// GenerationPaused reports that new generation is paused because the outbox
	// reached its row/byte quota.
	GenerationPaused bool
	// Completed reports that a finite run acknowledged every pending row within
	// its completion budget.
	Completed bool
}

// SchedulerOptions configures one scheduler. Outbox, Publisher and Ack are
// required. The unexported fields are test seams and stay zero in production.
type SchedulerOptions struct {
	Outbox    *Outbox
	Publisher Publisher
	Ack       AckObserver
	Config    Config

	// FiniteBatch, when positive, stops generation after that many unique
	// batches were enqueued this run and then waits for every pending row to be
	// acknowledged within CompletionBudget. Zero runs until the context ends.
	FiniteBatch      int
	CompletionBudget time.Duration

	// Logger receives reason-coded, secret-free lifecycle events. Nil uses
	// slog.Default().
	Logger *slog.Logger

	// Test seams.
	clock     Clock
	randFloat func() float64
	newUUID   func() (uuid.UUID, error)
}

// schedulerConfig carries only the non-secret values the loop needs.
type schedulerConfig struct {
	gatewayID        string
	sensorID         string
	samplesPerBatch  int
	sampleIntervalUS int64
	batchSpan        time.Duration
	maxInflight      int
	ackTimeout       time.Duration
	retryMin         time.Duration
	retryMax         time.Duration
	shutdownTimeout  time.Duration
}

// Scheduler is the batch generator, bounded replay/publish loop and in-flight
// owner for one Gateway simulator. It is started once with Run and is not safe
// to restart.
type Scheduler struct {
	cfg schedulerConfig
	log *slog.Logger
	pub Publisher
	ack AckObserver
	out *Outbox

	clock     Clock
	randFloat func() float64
	newUUID   func() (uuid.UUID, error)
	bootID    uuid.UUID

	finiteTarget     int
	completionBudget time.Duration

	ctx    context.Context
	cancel context.CancelFunc

	startMu sync.Mutex
	started bool
	done    chan struct{}

	// shutdownMu guards the optional shared shutdown deadline the composition
	// registers with BeginShutdown so the publish drain shares one budget.
	shutdownMu       sync.Mutex
	shutdownDeadline time.Time

	// timer is the single wake timer; there is never one timer per row.
	timer Timer

	// inflight is the bounded RAM working set, touched only by the loop
	// goroutine. inflightCount mirrors its size for concurrent Stats reads.
	inflight      map[uuid.UUID]*inflightEntry
	inflightCount atomic.Int64

	pubResults chan publishResult

	nextGenAt          time.Time
	genDone            bool
	genReady           bool
	readyObserved      bool
	completionDeadline time.Time

	generationPaused atomic.Bool
	completed        atomic.Bool

	uniqueEnqueued  atomic.Int64
	publishAttempts atomic.Int64

	fatalMu   sync.Mutex
	fatalErr  error
	fatalOnce sync.Once
	fatalCh   chan struct{}
}

// NewScheduler validates the options, derives the non-secret loop config and
// generates the one process-wide boot UUID for new batches. Existing pending
// rows from older boots are replayed with their stored boot identity and are
// never retagged.
func NewScheduler(opts SchedulerOptions) (*Scheduler, error) {
	if opts.Outbox == nil {
		return nil, errors.New("gatewaysim: scheduler requires an outbox")
	}
	if opts.Publisher == nil {
		return nil, errors.New("gatewaysim: scheduler requires a publisher")
	}
	if opts.Ack == nil {
		return nil, errors.New("gatewaysim: scheduler requires an ACK observer")
	}
	cfg := opts.Config
	if cfg.GatewayID == "" || cfg.SensorID == "" {
		return nil, errors.New("gatewaysim: scheduler requires a gateway and sensor identity")
	}
	if cfg.SamplesPerBatch < 1 || cfg.SampleIntervalUS <= 0 || cfg.BatchSpan < 0 {
		return nil, errors.New("gatewaysim: scheduler requires a valid sample profile")
	}
	if cfg.MaxInflight < 1 || cfg.MaxInflight > cfg.OutboxMaxRows {
		return nil, errors.New("gatewaysim: scheduler max in-flight must be between 1 and the outbox row cap")
	}
	if cfg.AckTimeout <= 0 {
		return nil, errors.New("gatewaysim: scheduler requires a positive ACK timeout")
	}
	if cfg.RetryMinDelay <= 0 || cfg.RetryMaxDelay < cfg.RetryMinDelay {
		return nil, errors.New("gatewaysim: scheduler requires 0 < retry min <= retry max")
	}
	if opts.FiniteBatch < 0 {
		return nil, errors.New("gatewaysim: scheduler finite batch count must not be negative")
	}
	if opts.FiniteBatch > 0 && opts.CompletionBudget <= 0 {
		return nil, errors.New("gatewaysim: scheduler finite run requires a positive completion budget")
	}
	if opts.Ack.Notify() == nil {
		return nil, errors.New("gatewaysim: scheduler ACK observer must expose a notify channel")
	}

	clock := opts.clock
	if clock == nil {
		clock = realClock{}
	}
	randFloat := opts.randFloat
	if randFloat == nil {
		randFloat = rand.Float64
	}
	newUUID := opts.newUUID
	if newUUID == nil {
		newUUID = uuid.NewRandom
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	bootID, err := newUUID()
	if err != nil {
		return nil, fmt.Errorf("gatewaysim: boot id: %w", err)
	}
	if bootID == uuid.Nil || bootID.Variant() != uuid.RFC4122 {
		return nil, errors.New("gatewaysim: generated boot id is invalid")
	}

	return &Scheduler{
		cfg: schedulerConfig{
			gatewayID:        cfg.GatewayID,
			sensorID:         cfg.SensorID,
			samplesPerBatch:  cfg.SamplesPerBatch,
			sampleIntervalUS: cfg.SampleIntervalUS,
			batchSpan:        cfg.BatchSpan,
			maxInflight:      cfg.MaxInflight,
			ackTimeout:       cfg.AckTimeout,
			retryMin:         cfg.RetryMinDelay,
			retryMax:         cfg.RetryMaxDelay,
			shutdownTimeout:  cfg.ShutdownTimeout,
		},
		log:              log,
		pub:              opts.Publisher,
		ack:              opts.Ack,
		out:              opts.Outbox,
		clock:            clock,
		randFloat:        randFloat,
		newUUID:          newUUID,
		bootID:           bootID,
		finiteTarget:     opts.FiniteBatch,
		completionBudget: opts.CompletionBudget,
		done:             make(chan struct{}),
		inflight:         make(map[uuid.UUID]*inflightEntry),
		pubResults:       make(chan publishResult, cfg.MaxInflight),
		fatalCh:          make(chan struct{}),
	}, nil
}

// BootID returns the process-wide boot UUID used for new batches.
func (s *Scheduler) BootID() uuid.UUID { return s.bootID }

// cadence is the wall span of one batch: one sample interval per sample.
func (s *Scheduler) cadence() time.Duration {
	return time.Duration(s.cfg.sampleIntervalUS) * time.Microsecond * time.Duration(s.cfg.samplesPerBatch)
}

// Run drives the scheduler until the context ends, a finite run completes, or a
// classified fatal stop occurs. It stops generation, waits a bounded time for
// in-flight publishes to return, and never touches or closes the outbox or the
// transport: the composition stops the ACK worker and closes SQLite after Run
// returns.
func (s *Scheduler) Run(ctx context.Context) error {
	s.startMu.Lock()
	if s.started {
		s.startMu.Unlock()
		return errors.New("gatewaysim: scheduler already started")
	}
	s.started = true
	s.startMu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	defer s.cancel()

	s.timer = s.clock.NewTimer(parkDuration)
	s.nextGenAt = s.clock.Now().UTC()

	return s.loop()
}

// Stop cancels the run context so Run returns and stops generation. It is safe
// before Run and idempotent.
func (s *Scheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}

// Done returns a channel closed once Run has returned. It is created by
// NewScheduler so a composition may select on it before Run; it never closes if
// Run was never called.
func (s *Scheduler) Done() <-chan struct{} { return s.done }

// BeginShutdown records the shared shutdown deadline the composition uses for
// its whole teardown, so the scheduler's own publish drain is bounded by the
// remaining budget instead of adding a second full ShutdownTimeout on top of
// it. The earliest deadline wins; a zero deadline is ignored. It is safe to call
// before Run starts.
func (s *Scheduler) BeginShutdown(deadline time.Time) {
	if deadline.IsZero() {
		return
	}
	s.shutdownMu.Lock()
	if s.shutdownDeadline.IsZero() || deadline.Before(s.shutdownDeadline) {
		s.shutdownDeadline = deadline
	}
	s.shutdownMu.Unlock()
}

// loop is the single scheduler goroutine.
func (s *Scheduler) loop() error {
	defer close(s.done)
	for {
		// A cancelled context is a normal supervised stop, not a fatal error: a
		// bounded DB read that fails because the run context ended must not be
		// classified as an operational failure.
		if s.ctx.Err() != nil {
			s.waitPublishes()
			return nil
		}
		if err := s.Fatal(); err != nil {
			return s.fail(err)
		}
		if err := s.reconcile(); err != nil {
			if s.ctx.Err() != nil {
				s.waitPublishes()
				return nil
			}
			return s.fail(err)
		}
		if done, err := s.checkCompletion(); done {
			if err != nil {
				if s.ctx.Err() != nil {
					s.waitPublishes()
					return nil
				}
				return s.fail(err)
			}
			s.completed.Store(true)
			s.waitPublishes()
			return nil
		}
		s.armTimer()
		readyCh := s.pub.ReadyCh()
		if s.pub.Ready() != s.readyObserved {
			// Readiness changed after reconcile; re-run it before blocking.
			continue
		}
		select {
		case <-s.ctx.Done():
			s.waitPublishes()
			return nil
		case <-s.pub.FatalSignal():
			return s.fail(s.pub.Fatal())
		case <-s.ack.FatalSignal():
			return s.fail(s.ack.Fatal())
		case <-s.timer.C():
		case <-s.ack.Notify():
			s.syncInflight()
		case res := <-s.pubResults:
			s.onPublishResult(res)
		case <-readyCh:
		}
	}
}

// reconcile processes due retries, admits durable pending rows oldest-first
// into free in-flight slots and, only when no older pending row is waiting,
// generates at most one new batch when the cadence is due. Admission runs before
// generation so a restart replay always takes slots before new generation.
func (s *Scheduler) reconcile() error {
	ready := s.pub.Ready()
	// Deduce the generation-eligibility snapshot before genReady is read by
	// armTimer, then publish the readiness observation for the loop's edge check.
	defer func() {
		s.readyObserved = ready
		s.inflightCount.Store(int64(len(s.inflight)))
	}()
	if !ready {
		s.genReady = false
		return nil
	}
	now := s.clock.Now()
	if err := s.processDueRetries(now); err != nil {
		return err
	}
	more, err := s.admitPending()
	if err != nil {
		return err
	}
	free := s.cfg.maxInflight - len(s.inflight)
	s.genReady = !s.genDone && !s.generationPaused.Load() && free > 0 && !more
	if s.genReady && !now.Before(s.nextGenAt) {
		return s.generate(now)
	}
	return nil
}

// processDueRetries advances entries whose timer is due. An ACK-timed-out entry
// re-checks its row first so a late or prior-boot ACK that deleted the row is
// never ignored, then schedules a bounded backoff. A backoff-due entry is
// republished unchanged.
func (s *Scheduler) processDueRetries(now time.Time) error {
	for id, e := range s.inflight {
		if e.state == statePublishing || now.Before(e.nextAt) {
			continue
		}
		switch e.state {
		case stateAwaitAck:
			pending, err := s.rowStillPending(e.item.MessageID)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrSchedulerLookup, err)
			}
			if !pending {
				delete(s.inflight, id)
				continue
			}
			s.scheduleBackoff(e)
		case stateBackoff:
			s.startPublish(e)
		}
	}
	return nil
}

// admitPending fills free in-flight slots from the oldest durable pending rows,
// paginating past rows already in flight so a leading block of in-flight rows
// cannot starve newer pending rows. It reports whether the page cap was reached
// with slots still free, which means an older un-admitted row may remain and
// generation must wait.
func (s *Scheduler) admitPending() (bool, error) {
	free := s.cfg.maxInflight - len(s.inflight)
	if free <= 0 {
		return false, nil
	}
	pageSize := s.cfg.maxInflight
	cursor := int64(0)
	for page := 0; page < maxAdmissionPages && free > 0; page++ {
		items, err := s.out.PendingAfter(s.ctx, cursor, pageSize)
		if err != nil {
			return false, fmt.Errorf("%w: %v", ErrSchedulerPending, err)
		}
		if len(items) == 0 {
			return false, nil
		}
		for _, it := range items {
			cursor = it.ID
		}
		for _, it := range items {
			if free <= 0 {
				return false, nil
			}
			if _, ok := s.inflight[it.MessageID]; ok {
				continue
			}
			s.admit(it)
			free--
		}
		if len(items) < pageSize {
			return false, nil
		}
	}
	if free > 0 {
		return true, nil
	}
	return false, nil
}

// admit places one durable row under bounded in-flight control and starts its
// first publish immediately. The stored topic and payload are used verbatim.
func (s *Scheduler) admit(item Item) {
	e := &inflightEntry{item: item}
	s.inflight[item.MessageID] = e
	s.startPublish(e)
}

// generate commits one new batch through the outbox transaction. Capacity
// pauses generation without retaining an uncommitted batch; an ambiguous commit
// is fatal and is never regenerated.
func (s *Scheduler) generate(now time.Time) error {
	// Advance the cadence first so a slow enqueue can never create a catch-up
	// burst: at most one batch is produced per pass.
	s.nextGenAt = now.UTC().Add(s.cadence())

	msgID, err := s.newUUID()
	if err != nil {
		return fmt.Errorf("%w: message id: %v", ErrSchedulerFatal, err)
	}
	measuredAt := now.Add(-s.cfg.batchSpan).UTC().Truncate(time.Microsecond)
	item, err := s.out.Enqueue(s.ctx, EnqueueRequest{
		BootID:           s.bootID,
		MessageID:        msgID,
		MeasuredAt:       measuredAt,
		ReceivedAt:       now.UTC(),
		SampleCount:      int32(s.cfg.samplesPerBatch),
		SampleIntervalUS: s.cfg.sampleIntervalUS,
	})
	if err != nil {
		if errors.Is(err, ErrOutboxFull) {
			s.generationPaused.Store(true)
			return nil
		}
		if errors.Is(err, ErrOutboxCommitUnknown) {
			return fmt.Errorf("%w: %v", ErrSchedulerCommitUnknown, err)
		}
		return fmt.Errorf("%w: %v", ErrSchedulerEnqueue, err)
	}

	s.uniqueEnqueued.Add(1)
	s.admit(item)
	if s.finiteTarget > 0 && !s.genDone && int(s.uniqueEnqueued.Load()) >= s.finiteTarget {
		s.genDone = true
		s.completionDeadline = s.clock.Now().UTC().Add(s.completionBudget)
	}
	return nil
}

// startPublish launches exactly one bounded Publish invocation for the entry.
// It is only called when the entry does not already own a publish.
func (s *Scheduler) startPublish(e *inflightEntry) {
	e.state = statePublishing
	s.publishAttempts.Add(1)
	id := e.item.MessageID
	topic := e.item.Topic
	payload := e.item.Payload
	go func() {
		err := s.pub.Publish(s.ctx, topic, payload)
		select {
		case s.pubResults <- publishResult{messageID: id, err: err}:
		case <-s.ctx.Done():
		}
	}()
}

// onPublishResult classifies one completed publish. A PUBACK moves the entry to
// the ACK wait; a failure retains the row and schedules a backoff. A result for
// an entry already removed (for example acked) is ignored.
func (s *Scheduler) onPublishResult(res publishResult) {
	e, ok := s.inflight[res.messageID]
	if !ok || e.state != statePublishing {
		return
	}
	if res.err != nil {
		s.scheduleBackoff(e)
		return
	}
	e.state = stateAwaitAck
	e.nextAt = s.clock.Now().UTC().Add(s.cfg.ackTimeout)
}

// scheduleBackoff retains the entry and arms a jittered exponential retry.
func (s *Scheduler) scheduleBackoff(e *inflightEntry) {
	e.state = stateBackoff
	e.nextAt = s.clock.Now().UTC().Add(retryDelay(s.cfg.retryMin, s.cfg.retryMax, e.retries, s.randFloat))
	e.retries++
}

// syncInflight re-reads every in-flight row (bounded by MaxInflight) and drops
// the ones the ACK worker already deleted. It is driven by the ACK notify, so an
// early, late or prior-boot ACK that committed a delete always frees a slot even
// while a publish is still running or after the ACK timeout.
func (s *Scheduler) syncInflight() {
	for id := range s.inflight {
		pending, err := s.rowStillPending(id)
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			s.setFatal(fmt.Errorf("%w: %v", ErrSchedulerLookup, err))
			return
		}
		if !pending {
			delete(s.inflight, id)
		}
	}
	// A committed delete may have freed capacity; allow generation again.
	s.generationPaused.Store(false)
}

// rowStillPending is a bounded single-row read of the bound gateway.
func (s *Scheduler) rowStillPending(id uuid.UUID) (bool, error) {
	item, found, err := s.out.Lookup(s.ctx, id)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	return item.Status == outboxStatusPending, nil
}

// armTimer arms the single wake timer for the earliest of the next generation
// tick, the completion deadline and the earliest retry deadline. It only arms
// timed work that can actually proceed, so a paused, unready or slot-starved
// scheduler parks instead of spinning.
func (s *Scheduler) armTimer() {
	if !s.pub.Ready() {
		s.resetTimer(parkDuration)
		return
	}
	next := time.Time{}
	consider := func(t time.Time) {
		if t.IsZero() {
			return
		}
		if next.IsZero() || t.Before(next) {
			next = t
		}
	}
	if s.genReady || (!s.genDone && !s.generationPaused.Load() && s.cfg.maxInflight-len(s.inflight) > 0) {
		consider(s.nextGenAt)
	}
	if s.finiteTarget > 0 && s.genDone && !s.completionDeadline.IsZero() {
		consider(s.completionDeadline)
	}
	for _, e := range s.inflight {
		if e.state == stateAwaitAck || e.state == stateBackoff {
			consider(e.nextAt)
		}
	}
	if next.IsZero() {
		s.resetTimer(parkDuration)
		return
	}
	d := next.Sub(s.clock.Now())
	if d < 0 {
		d = 0
	}
	s.resetTimer(d)
}

// resetTimer stops and drains the wake timer then resets it to d, so a stale
// fire can never be observed as a fresh event.
func (s *Scheduler) resetTimer(d time.Duration) {
	if d < 0 {
		d = 0
	}
	if !s.timer.Stop() {
		select {
		case <-s.timer.C():
		default:
		}
	}
	s.timer.Reset(d)
}

// checkCompletion reports a finished finite run. It returns done=true only when
// either every pending row was acknowledged (success) or the completion budget
// expired with rows still pending (ErrSchedulerCompletion); a partial run is
// never reported as complete.
func (s *Scheduler) checkCompletion() (bool, error) {
	if s.finiteTarget <= 0 || !s.genDone {
		return false, nil
	}
	counts, err := s.out.Counts(s.ctx)
	if err != nil {
		return true, fmt.Errorf("%w: %v", ErrSchedulerPending, err)
	}
	if counts.PendingRows == 0 {
		return true, nil
	}
	if !s.completionDeadline.IsZero() && !s.clock.Now().Before(s.completionDeadline) {
		return true, ErrSchedulerCompletion
	}
	return false, nil
}

// waitPublishes waits a bounded time for every in-flight publish invocation to
// return after generation stopped, so shutdown does not leak a publish
// goroutine. It never waits for an application ACK: unacknowledged rows stay
// durable for the next run.
func (s *Scheduler) waitPublishes() {
	if s.publishingCount() == 0 {
		return
	}
	budget := s.publishDrainBudget()
	if budget <= 0 {
		return
	}
	timer := time.NewTimer(budget)
	defer timer.Stop()
	for s.publishingCount() > 0 {
		select {
		case res := <-s.pubResults:
			s.onPublishResult(res)
		case <-timer.C:
			return
		}
	}
}

// publishDrainBudget bounds the shutdown publish drain by both the configured
// shutdown timeout and any shared shutdown deadline registered with
// BeginShutdown, so the total teardown never adds a second full budget.
func (s *Scheduler) publishDrainBudget() time.Duration {
	budget := s.cfg.shutdownTimeout
	if budget <= 0 {
		budget = defaultShutdownTimeout
	}
	s.shutdownMu.Lock()
	deadline := s.shutdownDeadline
	s.shutdownMu.Unlock()
	if !deadline.IsZero() {
		if remaining := time.Until(deadline); remaining < budget {
			budget = remaining
		}
	}
	return budget
}

// publishingCount counts entries that currently own a publish invocation.
func (s *Scheduler) publishingCount() int {
	n := 0
	for _, e := range s.inflight {
		if e.state == statePublishing {
			n++
		}
	}
	return n
}

// Stats returns a payload-free snapshot. Reading the outbox counts is best
// effort; a transient failure leaves the durable totals at zero rather than
// exposing an error.
func (s *Scheduler) Stats() SchedulerStats {
	st := SchedulerStats{
		UniqueEnqueued:   s.uniqueEnqueued.Load(),
		PublishAttempts:  s.publishAttempts.Load(),
		AppAcknowledged:  s.ack.Stats().Acknowledged,
		Inflight:         int(s.inflightCount.Load()),
		GenerationPaused: s.generationPaused.Load(),
		Completed:        s.completed.Load(),
	}
	if counts, err := s.out.Counts(context.Background()); err == nil {
		st.PendingRows = counts.PendingRows
		st.PendingBytes = counts.PendingBytes
	}
	return st
}

// FatalSignal is closed once when the scheduler stops on an internal failure.
func (s *Scheduler) FatalSignal() <-chan struct{} { return s.fatalCh }

// Fatal returns the first internal failure, or nil while healthy.
func (s *Scheduler) Fatal() error {
	s.fatalMu.Lock()
	defer s.fatalMu.Unlock()
	return s.fatalErr
}

// setFatal records the first failure and closes the fatal signal exactly once.
func (s *Scheduler) setFatal(err error) {
	s.fatalMu.Lock()
	if s.fatalErr == nil {
		s.fatalErr = err
	}
	s.fatalMu.Unlock()
	s.fatalOnce.Do(func() { close(s.fatalCh) })
}

// fail records the fatal error and drains in-flight publishes before returning.
func (s *Scheduler) fail(err error) error {
	if err == nil {
		err = ErrSchedulerFatal
	}
	s.setFatal(err)
	s.waitPublishes()
	return err
}

// retryDelay is the pure bounded retry delay: an exponential doubling from min
// capped at max with equal jitter of ±20%, clamped so the result never exceeds
// max and is never so small it becomes a hot loop. It is a standalone function
// so it can be tested deterministically without a running scheduler.
func retryDelay(min, max time.Duration, retries int, randFloat func() float64) time.Duration {
	if min <= 0 {
		min = time.Millisecond
	}
	if max < min {
		max = min
	}
	d := min
	for i := 0; i < retries; i++ {
		if d >= max {
			break
		}
		d *= 2
		if d > max {
			d = max
		}
	}
	factor := 1 + (randFloat()*2-1)*0.2
	out := time.Duration(float64(d) * factor)
	if out > max {
		out = max
	}
	if out < 0 {
		out = 0
	}
	return out
}

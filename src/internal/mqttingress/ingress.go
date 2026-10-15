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
)

// Options configures the ingress core. Every value is validated by New.
type Options struct {
	QueueCapacity   int
	WorkerCount     int
	MaxPayloadBytes int
	// PersistTimeout bounds connection acquisition, locks, SQL and commit of
	// one Repository.Persist call.
	PersistTimeout time.Duration
	// PublishTimeout bounds one whole Publisher.Publish call.
	PublishTimeout time.Duration
	// Logger receives reason-coded, payload-free structured events. Nil uses
	// slog.Default().
	Logger *slog.Logger
}

// item is one admitted delivery. topic is an immutable string; payload is a
// private copy taken during admission so a transport callback may reuse its
// receive buffer immediately after Admit returns.
type item struct {
	topic      string
	payload    []byte
	receivedAt time.Time
}

// Ingress is the bounded admission queue and fixed worker pool. One admitted
// item traverses Validator.Validate -> Repository.Persist -> post-commit ACK
// exactly once.
type Ingress struct {
	validator  Validator
	repository Repository
	publisher  Publisher

	workerCount     int
	maxPayloadBytes int
	persistTimeout  time.Duration
	publishTimeout  time.Duration
	logger          *slog.Logger

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

	workers sync.WaitGroup

	counters counters
}

// New builds an ingress core. It rejects a missing dependency or a non-positive
// bound rather than allocating an unbounded queue or workers.
func New(opts Options, validator Validator, repository Repository, publisher Publisher) (*Ingress, error) {
	if opts.QueueCapacity < 1 {
		return nil, errors.New("mqttingress: queue capacity must be positive")
	}
	if opts.WorkerCount < 1 {
		return nil, errors.New("mqttingress: worker count must be positive")
	}
	if opts.MaxPayloadBytes < 1 {
		return nil, errors.New("mqttingress: max payload bytes must be positive")
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

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	return &Ingress{
		validator:       validator,
		repository:      repository,
		publisher:       publisher,
		workerCount:     opts.WorkerCount,
		maxPayloadBytes: opts.MaxPayloadBytes,
		persistTimeout:  opts.PersistTimeout,
		publishTimeout:  opts.PublishTimeout,
		logger:          logger,
		queue:           make(chan item, opts.QueueCapacity),
		stop:            make(chan struct{}),
		runCtx:          runCtx,
		cancelRun:       cancelRun,
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
	if _, err := telemetry.ParseTopic(topic); err != nil {
		i.counters.rejectedInvalidTpc.Add(1)
		return AdmissionRejectedInvalidTopic
	}

	it := item{topic: topic, payload: append([]byte(nil), payload...), receivedAt: receivedAt}
	select {
	case i.queue <- it:
		i.counters.accepted.Add(1)
		return AdmissionAccepted
	default:
		// Queue full: drop the delivery from RAM. The library MQTT PUBACK (if
		// any) is only transport receipt; the Gateway retries from its durable
		// outbox. No application ACK is emitted.
		i.counters.queueFull.Add(1)
		return AdmissionRejectedQueueFull
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
		return nil
	case <-ctx.Done():
		i.cancelRun()
		<-done
		return ctx.Err()
	}
}

// worker takes one item at a time until admission stops and the queue is empty.
func (i *Ingress) worker() {
	defer i.workers.Done()
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
	started := time.Now()
	defer func() {
		i.counters.processingNanos.Add(time.Since(started).Nanoseconds())
		i.counters.processingCount.Add(1)
	}()
	i.counters.processed.Add(1)

	message, err := i.validator.Validate(it.topic, it.payload, it.receivedAt)
	if err != nil {
		i.counters.validationRejected.Add(1)
		i.logReject("validate", err)
		return
	}

	persistCtx, cancelPersist := context.WithTimeout(i.runCtx, i.persistTimeout)
	result, persistErr := i.repository.Persist(persistCtx, message)
	cancelPersist()
	if persistErr != nil {
		i.counters.persistFailures.Add(1)
		i.logReject("persist", persistErr)
		return
	}
	if result.Outcome != telemetry.CommitOutcomeCommitted && result.Outcome != telemetry.CommitOutcomeDuplicateCommitted {
		i.counters.commitUnconfirmed.Add(1)
		i.logReject("commit", &telemetry.ReasonError{Code: telemetry.ReasonCommitUnconfirmed})
		return
	}
	if !identityMatches(message, result.Identity) {
		i.counters.identityMismatch.Add(1)
		i.logReject("identity", &telemetry.ReasonError{Code: telemetry.ReasonMessageConflict})
		return
	}

	// confirmedAt is the core's own UTC receipt of the confirmed commit, taken
	// after Persist returned. It is not the measurement time and not an exact
	// PostgreSQL commit timestamp; a retry may report a different value for the
	// same message_id/status.
	confirmedAt := time.Now().UTC()

	// The database result is final: record the confirmed commit regardless of
	// whether the ACK publish below succeeds.
	if result.Outcome == telemetry.CommitOutcomeCommitted {
		i.counters.committed.Add(1)
		i.counters.samplesNew.Add(int64(result.SamplesSaved))
	} else {
		i.counters.duplicateCommitted.Add(1)
	}

	topic := message.Topic()
	batch := message.Batch()
	ackTopic, ack, buildErr := telemetry.NewCommitACK(topic.GatewayID, batch.MessageID, result.Outcome, confirmedAt)
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
	publishErr := i.publisher.Publish(publishCtx, Delivery{Topic: ackTopic, Payload: wire, QoS: 1, Retained: false})
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

// identityMatches verifies that the persisted identity is the same resource and
// message the validated input described. A mismatch suppresses the ACK.
func identityMatches(message *telemetry.Message, identity telemetry.MessageIdentity) bool {
	topic := message.Topic()
	batch := message.Batch()
	return identity.GatewayID == topic.GatewayID &&
		identity.SensorID == topic.SensorID &&
		identity.MessageID == batch.MessageID
}

// logReject emits a payload-free, secret-free reason-coded event. It never logs
// the raw payload, driver error text, credentials or DSN.
func (i *Ingress) logReject(stage string, err error) {
	code := ""
	var reason *telemetry.ReasonError
	if errors.As(err, &reason) && reason != nil {
		code = string(reason.Code)
	}
	i.logger.Debug("mqtt ingress item not acknowledged",
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

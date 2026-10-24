package gatewaysim

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// --- deterministic uuid seam ----------------------------------------------

// seqUUID returns a distinct RFC4122 version-4 UUID for a counter so tests can
// predict boot and message identities.
func seqUUID(n uint64) uuid.UUID {
	var u uuid.UUID
	binary.BigEndian.PutUint64(u[0:8], n)
	u[6] = (u[6] & 0x0f) | 0x40 // version 4
	u[8] = (u[8] & 0x3f) | 0x80 // variant RFC4122
	return u
}

// seqUUIDGen returns a newUUID seam starting at base.
func seqUUIDGen(base uint64) func() (uuid.UUID, error) {
	n := base
	return func() (uuid.UUID, error) {
		n++
		return seqUUID(n), nil
	}
}

// --- fake clock ------------------------------------------------------------

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	clock  *fakeClock
	c      chan time.Time
	at     time.Time
	active bool
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{clock: c, c: make(chan time.Time, 1), at: c.now.Add(d), active: true}
	c.timers = append(c.timers, t)
	return t
}

func (t *fakeTimer) C() <-chan time.Time { return t.c }

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	was := t.active
	t.active = false
	return was
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	was := t.active
	t.active = true
	t.at = t.clock.now.Add(d)
	return was
}

// Advance moves the clock forward and fires every armed timer whose deadline was
// reached, so the scheduler observes the same transitions it would on wall time.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	now := c.now
	for _, t := range c.timers {
		if t.active && !t.at.After(now) {
			t.active = false
			select {
			case t.c <- now:
			default:
			}
		}
	}
}

// --- fake publisher --------------------------------------------------------

type pubRecord struct {
	topic   string
	payload []byte
	at      time.Time
}

type fakePublisher struct {
	clock Clock

	mu        sync.Mutex
	ready     bool
	readyCh   chan struct{}
	fatalCh   chan struct{}
	fatalErr  error
	publishFn func(topic string, payload []byte) error
	records   []pubRecord
}

func newFakePublisher(clock Clock) *fakePublisher {
	return &fakePublisher{clock: clock, readyCh: make(chan struct{}), fatalCh: make(chan struct{})}
}

func (p *fakePublisher) Ready() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ready
}

func (p *fakePublisher) ReadyCh() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.readyCh
}

func (p *fakePublisher) setReady(r bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ready == r {
		return
	}
	p.ready = r
	close(p.readyCh)
	p.readyCh = make(chan struct{})
}

func (p *fakePublisher) setPublishFn(fn func(topic string, payload []byte) error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publishFn = fn
}

func (p *fakePublisher) Publish(ctx context.Context, topic string, payload []byte) error {
	p.mu.Lock()
	fn := p.publishFn
	p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if fn != nil {
		if err := fn(topic, payload); err != nil {
			return err
		}
	}
	p.mu.Lock()
	p.records = append(p.records, pubRecord{topic: topic, payload: append([]byte(nil), payload...), at: p.clock.Now()})
	p.mu.Unlock()
	return nil
}

func (p *fakePublisher) FatalSignal() <-chan struct{} { return p.fatalCh }

func (p *fakePublisher) Fatal() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fatalErr
}

func (p *fakePublisher) snapshot() []pubRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]pubRecord, len(p.records))
	copy(out, p.records)
	return out
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.records)
}

func (p *fakePublisher) hasRecord(rec pubRecord) bool {
	for _, r := range p.snapshot() {
		if r.topic == rec.topic && bytes.Equal(r.payload, rec.payload) {
			return true
		}
	}
	return false
}

// --- harness ---------------------------------------------------------------

func baseSchedulerConfig() Config {
	return Config{
		GatewayID:        "gateway_001",
		SensorID:         "sensor_001",
		SampleRateHz:     100,
		SamplesPerBatch:  100,
		SampleIntervalUS: 10000,
		BatchSpan:        990000 * time.Microsecond,
		MaxInflight:      8,
		OutboxMaxRows:    1000,
		OutboxMaxBytes:   1 << 20,
		AckTimeout:       5 * time.Second,
		RetryMinDelay:    time.Second,
		RetryMaxDelay:    30 * time.Second,
		ShutdownTimeout:  time.Second,
	}
}

func schedOutboxConfig(path string, cfg Config) OutboxConfig {
	return OutboxConfig{
		Path:      path,
		GatewayID: cfg.GatewayID,
		SensorID:  cfg.SensorID,
		MaxRows:   cfg.OutboxMaxRows,
		MaxBytes:  cfg.OutboxMaxBytes,
		OpTimeout: 5 * time.Second,
	}
}

func pendingRows(t *testing.T, o *Outbox) int64 {
	t.Helper()
	counts, err := o.Counts(context.Background())
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	return counts.PendingRows
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func commitACKJSON(id uuid.UUID) []byte {
	return []byte(fmt.Sprintf(
		`{"protocol_version":1,"message_type":"telemetry_ack","message_id":"%s","status":"committed","committed_at":"2026-10-16T10:00:00.000000Z"}`,
		id.String()))
}

func ackMessage(p *ACKProcessor, gatewayID string, id uuid.UUID) {
	p.Admit("gateways/"+gatewayID+"/acks/"+id.String(), commitACKJSON(id), false)
}

// schedulerRun bundles one process run against an open outbox.
type schedulerRun struct {
	outbox *Outbox
	proc   *ACKProcessor
	pub    *fakePublisher
	sched  *Scheduler
	cancel context.CancelFunc
	done   chan error
}

func openRun(t *testing.T, path string, cfg Config, pub *fakePublisher, clock Clock, uuids func() (uuid.UUID, error), rf func() float64, finite int, budget time.Duration) *schedulerRun {
	t.Helper()
	outbox := openTestOutbox(t, schedOutboxConfig(path, cfg))
	proc, err := NewACKProcessor(outbox, 16)
	if err != nil {
		t.Fatalf("NewACKProcessor: %v", err)
	}
	proc.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = proc.Stop(ctx)
	})
	sched, err := NewScheduler(SchedulerOptions{
		Outbox:           outbox,
		Publisher:        pub,
		Ack:              proc,
		Config:           cfg,
		FiniteBatch:      finite,
		CompletionBudget: budget,
		clock:            clock,
		newUUID:          uuids,
		randFloat:        rf,
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	return &schedulerRun{outbox: outbox, proc: proc, pub: pub, sched: sched}
}

func (r *schedulerRun) start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.done = make(chan error, 1)
	sched := r.sched
	done := r.done
	go func() { done <- sched.Run(runCtx) }()
}

func (r *schedulerRun) waitDone(t *testing.T) error {
	t.Helper()
	select {
	case err := <-r.done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("scheduler did not finish in time")
		return nil
	}
}

func (r *schedulerRun) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("scheduler did not stop in time")
		return nil
	}
}

func enqueueRow(t *testing.T, o *Outbox, boot, msg uuid.UUID, measuredAt time.Time) Item {
	t.Helper()
	item, err := o.Enqueue(context.Background(), EnqueueRequest{
		BootID:           boot,
		MessageID:        msg,
		MeasuredAt:       measuredAt,
		ReceivedAt:       measuredAt.Add(time.Second),
		SampleCount:      100,
		SampleIntervalUS: 10000,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return item
}

// --- pure retry delay ------------------------------------------------------

func TestRetryDelayJitterAndCap(t *testing.T) {
	min := 100 * time.Millisecond
	max := 800 * time.Millisecond

	low := retryDelay(min, max, 0, func() float64 { return 0 })
	high := retryDelay(min, max, 0, func() float64 { return 1 })
	mid := retryDelay(min, max, 0, func() float64 { return 0.5 })

	if low != 80*time.Millisecond {
		t.Fatalf("low jitter = %s, want 80ms", low)
	}
	if high != 120*time.Millisecond {
		t.Fatalf("high jitter = %s, want 120ms", high)
	}
	if mid != min {
		t.Fatalf("mid jitter = %s, want %s", mid, min)
	}

	// Exponential growth then a hard cap that jitter can never exceed.
	prev := time.Duration(0)
	for retries := 0; retries < 10; retries++ {
		d := retryDelay(min, max, retries, func() float64 { return 0.5 })
		if d < prev {
			t.Fatalf("retry %d dropped from %s to %s", retries, prev, d)
		}
		prev = d
	}
	if got := retryDelay(min, max, 20, func() float64 { return 1 }); got > max {
		t.Fatalf("capped delay %s exceeds max %s", got, max)
	}
	if got := retryDelay(min, max, 3, func() float64 { return 0 }); got < min*8/10 {
		t.Fatalf("jittered delay %s below the 80%% floor", got)
	}
}

// --- normal path -----------------------------------------------------------

func TestSchedulerGeneratesPublishesAndAcknowledges(t *testing.T) {
	cfg := baseSchedulerConfig()
	cfg.MaxInflight = 4
	clock := newFakeClock()
	pub := newFakePublisher(clock)
	path := filepath.Join(t.TempDir(), "outbox.db")

	run := openRun(t, path, cfg, pub, clock, seqUUIDGen(0), func() float64 { return 0.5 }, 0, 0)
	run.start(context.Background())
	pub.setReady(true)

	waitFor(t, "first batch", func() bool { return run.sched.Stats().UniqueEnqueued == 1 })
	advanceUntil(t, clock, 100*time.Millisecond, 50, func() bool { return run.sched.Stats().UniqueEnqueued == 2 })
	waitFor(t, "pending rows", func() bool { return pendingRows(t, run.outbox) >= 2 })

	// Acknowledge every pending row and expect the outbox to drain.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		items, err := run.outbox.Pending(context.Background(), cfg.OutboxMaxRows)
		if err != nil {
			t.Fatalf("Pending: %v", err)
		}
		if len(items) == 0 {
			break
		}
		for _, it := range items {
			ackMessage(run.proc, cfg.GatewayID, it.MessageID)
		}
		time.Sleep(2 * time.Millisecond)
	}

	waitFor(t, "drain", func() bool { return pendingRows(t, run.outbox) == 0 })
	st := run.sched.Stats()
	if st.AppAcknowledged < 1 {
		t.Fatalf("appAcknowledged = %d, want >= 1", st.AppAcknowledged)
	}
	if st.PendingRows != 0 {
		t.Fatalf("pending = %d, want 0", st.PendingRows)
	}
	for _, rec := range pub.snapshot() {
		if rec.topic != "gateways/gateway_001/telemetry/sensor_001" {
			t.Fatalf("unexpected topic %q", rec.topic)
		}
	}
	if err := run.stop(t); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// --- restart: replay old boot, new boot fresh sequence ---------------------

func TestSchedulerRestartReplaysOldBootAndStartsFreshSequence(t *testing.T) {
	cfg := baseSchedulerConfig()
	cfg.MaxInflight = 4
	path := filepath.Join(t.TempDir(), "outbox.db")

	clock1 := newFakeClock()
	pub1 := newFakePublisher(clock1)
	run1 := openRun(t, path, cfg, pub1, clock1, seqUUIDGen(0), func() float64 { return 0.5 }, 0, 0)
	boot1 := run1.sched.BootID()
	run1.start(context.Background())
	pub1.setReady(true)

	waitFor(t, "first publish", func() bool { return pub1.count() >= 1 })
	waitFor(t, "one pending", func() bool { return pendingRows(t, run1.outbox) == 1 })
	rec1 := pub1.snapshot()[0]

	if err := run1.stop(t); err != nil {
		t.Fatalf("run1 stop: %v", err)
	}
	if err := run1.outbox.Close(); err != nil {
		t.Fatalf("run1 outbox close: %v", err)
	}

	clock2 := newFakeClock()
	pub2 := newFakePublisher(clock2)
	run2 := openRun(t, path, cfg, pub2, clock2, seqUUIDGen(1000), func() float64 { return 0.5 }, 0, 0)
	// The same file reopened: the unacknowledged row survives.
	if got := pendingRows(t, run2.outbox); got != 1 {
		t.Fatalf("reopened pending = %d, want 1", got)
	}
	boot2 := run2.sched.BootID()
	if boot1 == boot2 {
		t.Fatalf("boot ids must differ across processes")
	}
	run2.start(context.Background())
	pub2.setReady(true)

	waitFor(t, "replay and new batch", func() bool { return pub2.count() >= 2 })
	if !pub2.hasRecord(rec1) {
		t.Fatalf("old-boot row was not replayed byte-exact")
	}

	// Old row keeps its boot and sequence; the new boot starts at 0.
	assertStreamSequence(t, run2.outbox, boot1, 100)
	assertFirstSequence(t, run2.outbox, boot2, 0)
	assertStreamSequence(t, run2.outbox, boot2, 100)

	if err := run2.stop(t); err != nil {
		t.Fatalf("run2 stop: %v", err)
	}
}

func assertStreamSequence(t *testing.T, o *Outbox, boot uuid.UUID, want int64) {
	t.Helper()
	var next int64
	if err := o.db.QueryRow("SELECT next_sequence FROM outbox_streams WHERE boot_id = ?", boot.String()).Scan(&next); err != nil {
		t.Fatalf("read next_sequence for boot %s: %v", boot, err)
	}
	if next != want {
		t.Fatalf("boot %s next_sequence = %d, want %d", boot, next, want)
	}
}

func assertFirstSequence(t *testing.T, o *Outbox, boot uuid.UUID, want int64) {
	t.Helper()
	var first int64
	if err := o.db.QueryRow("SELECT first_sequence FROM outbox WHERE boot_id = ? ORDER BY id ASC LIMIT 1", boot.String()).Scan(&first); err != nil {
		t.Fatalf("read first_sequence for boot %s: %v", boot, err)
	}
	if first != want {
		t.Fatalf("boot %s first_sequence = %d, want %d", boot, first, want)
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// advanceUntil moves the fake clock in small steps until cond holds, giving the
// scheduler goroutine a chance to process each fired timer and arm the next one
// before time moves again.
func advanceUntil(t *testing.T, clock *fakeClock, step time.Duration, maxSteps int, cond func() bool) {
	t.Helper()
	for i := 0; i < maxSteps; i++ {
		if cond() {
			return
		}
		clock.Advance(step)
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not met after advancing time")
	}
}

// advanceToCompletion advances the clock until the run finishes.
func (r *schedulerRun) advanceToCompletion(t *testing.T, clock *fakeClock, step time.Duration, maxSteps int) error {
	t.Helper()
	for i := 0; i < maxSteps; i++ {
		select {
		case err := <-r.done:
			return err
		default:
		}
		clock.Advance(step)
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-r.done:
		return err
	default:
		t.Fatal("scheduler did not complete while advancing time")
		return nil
	}
}

// --- early ACK while a publish is still in flight --------------------------

func TestSchedulerEarlyAckWhilePublishInFlight(t *testing.T) {
	cfg := baseSchedulerConfig()
	cfg.MaxInflight = 2
	clock := newFakeClock()
	pub := newFakePublisher(clock)
	path := filepath.Join(t.TempDir(), "outbox.db")

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	pub.setPublishFn(func(_ string, _ []byte) error {
		once.Do(func() { close(started) })
		<-release
		return nil
	})

	run := openRun(t, path, cfg, pub, clock, seqUUIDGen(0), func() float64 { return 0.5 }, 0, 0)
	run.start(context.Background())
	pub.setReady(true)

	// Wait for the batch to be committed and its publish to start, then ACK it
	// before the transport returns: the delete must not be ignored.
	waitClosed(t, started, "publish start")
	waitFor(t, "pending batch", func() bool { return pendingRows(t, run.outbox) == 1 })
	items, err := run.outbox.Pending(context.Background(), cfg.OutboxMaxRows)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	ackMessage(run.proc, cfg.GatewayID, items[0].MessageID)
	waitFor(t, "row deleted", func() bool { return pendingRows(t, run.outbox) == 0 })

	close(release)
	// The late publish result for an already-acked row is ignored, not retried.
	time.Sleep(20 * time.Millisecond)
	if got := run.sched.Stats().Inflight; got != 0 {
		t.Fatalf("inflight = %d, want 0 after early ACK", got)
	}
	if got := run.sched.Stats().PendingRows; got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
	if err := run.stop(t); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// --- late ACK after the ACK timeout scheduled a retry ----------------------

func TestSchedulerLateAckAfterTimeoutAndDuplicateAck(t *testing.T) {
	cfg := baseSchedulerConfig()
	cfg.MaxInflight = 2
	cfg.AckTimeout = 200 * time.Millisecond
	cfg.RetryMinDelay = 50 * time.Millisecond
	cfg.RetryMaxDelay = 500 * time.Millisecond
	clock := newFakeClock()
	pub := newFakePublisher(clock)
	path := filepath.Join(t.TempDir(), "outbox.db")

	run := openRun(t, path, cfg, pub, clock, seqUUIDGen(0), func() float64 { return 0.5 }, 0, 0)
	run.start(context.Background())
	pub.setReady(true)

	waitFor(t, "first publish", func() bool { return run.sched.Stats().PublishAttempts == 1 })

	// ACK timeout fires and schedules a bounded backoff, then one retry.
	advanceUntil(t, clock, 10*time.Millisecond, 100, func() bool { return run.sched.Stats().PublishAttempts == 2 })

	items, err := run.outbox.Pending(context.Background(), cfg.OutboxMaxRows)
	if err != nil || len(items) != 1 {
		t.Fatalf("Pending = %v (len %d)", err, len(items))
	}
	id := items[0].MessageID

	// A late ACK still deletes the row and frees the slot.
	ackMessage(run.proc, cfg.GatewayID, id)
	waitFor(t, "late ack delete", func() bool { return pendingRows(t, run.outbox) == 0 })
	waitFor(t, "slot freed", func() bool { return run.sched.Stats().Inflight == 0 })

	// A duplicate ACK is a no-op that changes nothing.
	ackMessage(run.proc, cfg.GatewayID, id)
	time.Sleep(20 * time.Millisecond)
	if got := pendingRows(t, run.outbox); got != 0 {
		t.Fatalf("pending after duplicate ACK = %d, want 0", got)
	}
	if err := run.stop(t); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// --- bounded retry backoff, no hot loop ------------------------------------

func TestSchedulerRetryBackoffNoHotLoop(t *testing.T) {
	cfg := baseSchedulerConfig()
	cfg.MaxInflight = 1
	cfg.AckTimeout = time.Hour
	cfg.RetryMinDelay = 100 * time.Millisecond
	cfg.RetryMaxDelay = 800 * time.Millisecond
	clock := newFakeClock()
	pub := newFakePublisher(clock)
	pub.setPublishFn(func(_ string, _ []byte) error { return ErrPublishUnconfirmed })
	path := filepath.Join(t.TempDir(), "outbox.db")

	run := openRun(t, path, cfg, pub, clock, seqUUIDGen(0), func() float64 { return 0.5 }, 0, 0)
	run.start(context.Background())
	pub.setReady(true)
	waitFor(t, "first failure", func() bool { return run.sched.Stats().PublishAttempts == 1 })

	assertAttempts := func(want int64) {
		t.Helper()
		time.Sleep(15 * time.Millisecond)
		if got := run.sched.Stats().PublishAttempts; got != want {
			t.Fatalf("attempts = %d, want %d", got, want)
		}
	}

	// Retry 0 waits the full 100 ms base; a half step must not fire it.
	clock.Advance(50 * time.Millisecond)
	assertAttempts(1)
	clock.Advance(50 * time.Millisecond)
	waitFor(t, "retry 1", func() bool { return run.sched.Stats().PublishAttempts == 2 })

	// Retry 1 waits 200 ms.
	clock.Advance(150 * time.Millisecond)
	assertAttempts(2)
	clock.Advance(50 * time.Millisecond)
	waitFor(t, "retry 2", func() bool { return run.sched.Stats().PublishAttempts == 3 })

	if err := run.stop(t); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// --- in-flight bound and pagination past in-flight rows --------------------

func TestSchedulerInflightBoundAndPagination(t *testing.T) {
	cfg := baseSchedulerConfig()
	cfg.MaxInflight = 2
	cfg.AckTimeout = time.Hour
	path := filepath.Join(t.TempDir(), "outbox.db")
	clock := newFakeClock()
	pub := newFakePublisher(clock)

	outbox := openTestOutbox(t, schedOutboxConfig(path, cfg))
	oldBoot := seqUUID(900)
	base := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		enqueueRow(t, outbox, oldBoot, seqUUID(uint64(100+i)), base.Add(time.Duration(i)*time.Second))
	}
	_ = outbox.Close()

	run := openRun(t, path, cfg, pub, clock, seqUUIDGen(500), func() float64 { return 0.5 }, 0, 0)
	run.start(context.Background())
	pub.setReady(true)

	// Only MaxInflight old rows are admitted; no new generation while full.
	waitFor(t, "two admitted", func() bool { return run.sched.Stats().PublishAttempts == 2 })
	time.Sleep(20 * time.Millisecond)
	if got := run.sched.Stats().Inflight; got != 2 {
		t.Fatalf("inflight = %d, want 2", got)
	}
	if got := run.sched.Stats().UniqueEnqueued; got != 0 {
		t.Fatalf("uniqueEnqueued = %d, want 0 while old backlog fills the window", got)
	}
	if got := pendingRows(t, run.outbox); got != 5 {
		t.Fatalf("pending = %d, want 5", got)
	}

	// Acknowledge the first two admitted rows; free slots admit the next old
	// rows by paginating past the already-in-flight ids.
	items, err := run.outbox.Pending(context.Background(), cfg.OutboxMaxRows)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	for i := 0; i < 2; i++ {
		ackMessage(run.proc, cfg.GatewayID, items[i].MessageID)
	}
	waitFor(t, "next two admitted", func() bool { return run.sched.Stats().PublishAttempts == 4 })
	waitFor(t, "two pending left", func() bool { return pendingRows(t, run.outbox) == 3 })
	time.Sleep(20 * time.Millisecond)
	if got := run.sched.Stats().PublishAttempts; got != 4 {
		t.Fatalf("attempts = %d, want 4 (no starvation, no over-admission)", got)
	}
	if err := run.stop(t); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// --- capacity full including quarantined rows: pause/resume/no burst --------

func TestSchedulerCapacityFullIncludingQuarantine(t *testing.T) {
	cfg := baseSchedulerConfig()
	cfg.MaxInflight = 2
	cfg.OutboxMaxRows = 3
	path := filepath.Join(t.TempDir(), "outbox.db")
	clock := newFakeClock()
	pub := newFakePublisher(clock)

	outbox := openTestOutbox(t, schedOutboxConfig(path, cfg))
	oldBoot := seqUUID(900)
	base := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	for i := 0; i < 2; i++ {
		it := enqueueRow(t, outbox, oldBoot, seqUUID(uint64(200+i)), base.Add(time.Duration(i)*time.Second))
		if ok, err := outbox.Quarantine(context.Background(), it.MessageID, "operator isolation"); err != nil || !ok {
			t.Fatalf("Quarantine = %v, %v", ok, err)
		}
	}
	_ = outbox.Close()

	run := openRun(t, path, cfg, pub, clock, seqUUIDGen(0), func() float64 { return 0.5 }, 0, 0)
	run.start(context.Background())
	pub.setReady(true)

	// Quarantined rows count against the row cap; one new batch fits.
	waitFor(t, "one new batch", func() bool { return run.sched.Stats().UniqueEnqueued == 1 })
	waitFor(t, "one pending", func() bool { return pendingRows(t, run.outbox) == 1 })

	// The next cadence cannot commit: capacity is full, generation pauses.
	advanceUntil(t, clock, 100*time.Millisecond, 50, func() bool { return run.sched.Stats().GenerationPaused })
	if got := run.sched.Stats().UniqueEnqueued; got != 1 {
		t.Fatalf("uniqueEnqueued = %d, want 1 while paused", got)
	}

	// An ACK frees capacity; generation resumes exactly one batch per cadence.
	items, err := run.outbox.Pending(context.Background(), cfg.OutboxMaxRows)
	if err != nil || len(items) != 1 {
		t.Fatalf("Pending = %v (len %d)", err, len(items))
	}
	ackMessage(run.proc, cfg.GatewayID, items[0].MessageID)
	waitFor(t, "capacity freed", func() bool { return pendingRows(t, run.outbox) == 0 })
	time.Sleep(20 * time.Millisecond)
	if got := run.sched.Stats().UniqueEnqueued; got != 1 {
		t.Fatalf("uniqueEnqueued = %d, want 1 before the next cadence (no burst)", got)
	}

	advanceUntil(t, clock, 100*time.Millisecond, 50, func() bool { return run.sched.Stats().UniqueEnqueued == 2 })
	time.Sleep(20 * time.Millisecond)
	if got := run.sched.Stats().UniqueEnqueued; got != 2 {
		t.Fatalf("uniqueEnqueued = %d, want 2 (one batch per cadence)", got)
	}

	counts, err := run.outbox.Counts(context.Background())
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	if counts.QuarantinedRows != 2 {
		t.Fatalf("quarantined rows = %d, want 2 (never dropped)", counts.QuarantinedRows)
	}
	if err := run.stop(t); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// --- finite completion -----------------------------------------------------

func TestSchedulerFiniteCompletion(t *testing.T) {
	cfg := baseSchedulerConfig()
	cfg.MaxInflight = 4
	clock := newFakeClock()
	pub := newFakePublisher(clock)
	path := filepath.Join(t.TempDir(), "outbox.db")

	run := openRun(t, path, cfg, pub, clock, seqUUIDGen(0), func() float64 { return 0.5 }, 2, time.Hour)
	run.start(context.Background())
	pub.setReady(true)

	waitFor(t, "first batch", func() bool { return run.sched.Stats().UniqueEnqueued == 1 })
	advanceUntil(t, clock, 100*time.Millisecond, 50, func() bool { return run.sched.Stats().UniqueEnqueued == 2 })
	waitFor(t, "two pending", func() bool { return pendingRows(t, run.outbox) == 2 })

	items, err := run.outbox.Pending(context.Background(), cfg.OutboxMaxRows)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	for _, it := range items {
		ackMessage(run.proc, cfg.GatewayID, it.MessageID)
	}

	if err := run.waitDone(t); err != nil {
		t.Fatalf("finite run returned %v, want nil", err)
	}
	st := run.sched.Stats()
	if !st.Completed {
		t.Fatalf("Completed = false, want true")
	}
	if st.PendingRows != 0 {
		t.Fatalf("pending = %d, want 0", st.PendingRows)
	}
}

func TestSchedulerFiniteBudgetExpiryIsNotCompleted(t *testing.T) {
	cfg := baseSchedulerConfig()
	cfg.MaxInflight = 2
	clock := newFakeClock()
	pub := newFakePublisher(clock)
	path := filepath.Join(t.TempDir(), "outbox.db")

	run := openRun(t, path, cfg, pub, clock, seqUUIDGen(0), func() float64 { return 0.5 }, 1, 100*time.Millisecond)
	run.start(context.Background())
	pub.setReady(true)

	waitFor(t, "one batch", func() bool { return run.sched.Stats().UniqueEnqueued == 1 })
	clock.Advance(100 * time.Millisecond)

	err := run.advanceToCompletion(t, clock, 10*time.Millisecond, 200)
	if !errors.Is(err, ErrSchedulerCompletion) {
		t.Fatalf("finite run error = %v, want ErrSchedulerCompletion", err)
	}
	if run.sched.Stats().Completed {
		t.Fatalf("Completed = true, want false after budget expiry")
	}
	if got := pendingRows(t, run.outbox); got != 1 {
		t.Fatalf("pending = %d, want 1 retained for the next run", got)
	}
}

func TestSchedulerCancelledPublishesStillCompleteDrain(t *testing.T) {
	// Cancellation stops production, not completion accounting. Every invocation
	// below returns immediately with cancellation; none may leave a phantom
	// statePublishing entry for the shutdown drain to wait on.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	clock := newFakeClock()
	const count = 32
	s := &Scheduler{
		ctx: ctx, clock: clock, pub: newFakePublisher(clock),
		cfg:        schedulerConfig{shutdownTimeout: time.Second, retryMin: time.Second, retryMax: time.Second},
		randFloat:  func() float64 { return 0.5 },
		inflight:   make(map[uuid.UUID]*inflightEntry),
		pubResults: make(chan publishResult, count),
		done:       make(chan struct{}),
	}
	defer close(s.done)
	for i := 0; i < count; i++ {
		id := seqUUID(uint64(i + 1))
		e := &inflightEntry{item: Item{MessageID: id}}
		s.inflight[id] = e
		s.startPublish(e)
	}
	s.waitPublishes()
	if got := s.publishingCount(); got != 0 {
		t.Fatalf("%d completed publishes lost their results during cancellation", got)
	}
}

// TestSchedulerPublishDrainBudgetSharesDeadline proves the composition's single
// shutdown deadline bounds the scheduler's own publish drain, so teardown never
// serializes a second full ShutdownTimeout on top of the shared budget.
func TestSchedulerPublishDrainBudgetSharesDeadline(t *testing.T) {
	s := &Scheduler{cfg: schedulerConfig{shutdownTimeout: 10 * time.Second}}
	if got := s.publishDrainBudget(); got != 10*time.Second {
		t.Fatalf("no shared deadline: budget = %s, want 10s", got)
	}

	s.BeginShutdown(time.Now().Add(200 * time.Millisecond))
	got := s.publishDrainBudget()
	if got <= 0 || got > 200*time.Millisecond {
		t.Fatalf("shared deadline not honored: budget = %s", got)
	}

	// A later, longer deadline must not override the earliest one.
	s.BeginShutdown(time.Now().Add(5 * time.Second))
	if got := s.publishDrainBudget(); got > 200*time.Millisecond {
		t.Fatalf("later deadline overrode the earlier one: budget = %s", got)
	}

	// A zero deadline is ignored.
	s2 := &Scheduler{cfg: schedulerConfig{shutdownTimeout: 3 * time.Second}}
	s2.BeginShutdown(time.Time{})
	if got := s2.publishDrainBudget(); got != 3*time.Second {
		t.Fatalf("zero deadline changed the budget: %s", got)
	}
}

func TestScheduler_LatencyTracking_EarlyAckAndFiniteCompletion(t *testing.T) {
	cfg := baseSchedulerConfig()
	cfg.MaxInflight = 4
	clock := newFakeClock()
	pub := newFakePublisher(clock)
	path := filepath.Join(t.TempDir(), "outbox.db")

	tracker, err := NewLatencyTracker(10, WithTrackerClock(clock))
	if err != nil {
		t.Fatalf("NewLatencyTracker: %v", err)
	}

	outbox := openTestOutbox(t, schedOutboxConfig(path, cfg))
	proc, err := NewACKProcessor(outbox, 16, WithACKLatencyRecorder(tracker))
	if err != nil {
		t.Fatalf("NewACKProcessor: %v", err)
	}
	proc.Start()

	// Early ACK: whenever publish runs, immediately admit ACK
	pub.setPublishFn(func(_ string, payload []byte) error {
		var batch struct {
			MessageID string `json:"message_id"`
		}
		if err := json.Unmarshal(payload, &batch); err == nil && batch.MessageID != "" {
			clock.Advance(50 * time.Millisecond)
			ackMessage(proc, cfg.GatewayID, uuid.MustParse(batch.MessageID))
		}
		return nil
	})

	sched, err := NewScheduler(SchedulerOptions{
		Outbox:           outbox,
		Publisher:        pub,
		Ack:              proc,
		Config:           cfg,
		FiniteBatch:      2,
		CompletionBudget: time.Hour,
		Latency:          tracker,
		clock:            clock,
		newUUID:          seqUUIDGen(0),
		randFloat:        func() float64 { return 0.5 },
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	run := &schedulerRun{outbox: outbox, proc: proc, pub: pub, sched: sched}
	t.Cleanup(func() {
		run.cancel()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = proc.Stop(ctx)
	})

	run.start(context.Background())
	pub.setReady(true)

	waitFor(t, "first batch", func() bool { return run.sched.Stats().UniqueEnqueued >= 1 })
	advanceUntil(t, clock, 100*time.Millisecond, 50, func() bool { return run.sched.Stats().UniqueEnqueued == 2 })

	if err := run.waitDone(t); err != nil {
		t.Fatalf("finite run returned %v, want nil", err)
	}

	st := run.sched.Stats()
	if !st.Completed {
		t.Fatal("Completed = false, want true")
	}

	snap := tracker.Snapshot()
	if snap.SampleCount != 2 {
		t.Fatalf("expected 2 samples, got %d", snap.SampleCount)
	}
	if snap.FirstPublishes != 2 {
		t.Fatalf("expected 2 first publishes, got %d", snap.FirstPublishes)
	}
	if snap.RetryAttempts != 0 {
		t.Fatalf("expected 0 retry attempts, got %d", snap.RetryAttempts)
	}
	if snap.MissingOrigins != 0 {
		t.Fatalf("expected 0 missing origins, got %d", snap.MissingOrigins)
	}
	if snap.P50 <= 0 || snap.P95 <= 0 {
		t.Fatalf("expected positive latency percentiles, got p50=%v p95=%v", snap.P50, snap.P95)
	}
}

func TestScheduler_LatencyTracking_RetryPreservesFirstPublish(t *testing.T) {
	cfg := baseSchedulerConfig()
	cfg.MaxInflight = 2
	cfg.RetryMinDelay = 100 * time.Millisecond
	cfg.RetryMaxDelay = 100 * time.Millisecond
	clock := newFakeClock()
	pub := newFakePublisher(clock)
	path := filepath.Join(t.TempDir(), "outbox.db")

	tracker, err := NewLatencyTracker(10, WithTrackerClock(clock))
	if err != nil {
		t.Fatalf("NewLatencyTracker: %v", err)
	}

	outbox := openTestOutbox(t, schedOutboxConfig(path, cfg))
	proc, err := NewACKProcessor(outbox, 16, WithACKLatencyRecorder(tracker))
	if err != nil {
		t.Fatalf("NewACKProcessor: %v", err)
	}
	proc.Start()

	var attempts atomic.Int64
	pub.setPublishFn(func(_ string, payload []byte) error {
		n := attempts.Add(1)
		if n == 1 {
			// First attempt fails immediately
			return errors.New("simulated network failure")
		}
		// Second attempt (retry) advances clock by 10ms and admits ACK
		clock.Advance(10 * time.Millisecond)
		var batch struct {
			MessageID string `json:"message_id"`
		}
		if err := json.Unmarshal(payload, &batch); err == nil && batch.MessageID != "" {
			ackMessage(proc, cfg.GatewayID, uuid.MustParse(batch.MessageID))
		}
		return nil
	})

	sched, err := NewScheduler(SchedulerOptions{
		Outbox:           outbox,
		Publisher:        pub,
		Ack:              proc,
		Config:           cfg,
		FiniteBatch:      1,
		CompletionBudget: time.Hour,
		Latency:          tracker,
		clock:            clock,
		newUUID:          seqUUIDGen(0),
		randFloat:        func() float64 { return 0.5 },
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	run := &schedulerRun{outbox: outbox, proc: proc, pub: pub, sched: sched}
	t.Cleanup(func() {
		run.cancel()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = proc.Stop(ctx)
	})

	run.start(context.Background())
	pub.setReady(true)

	// Wait for first attempt to run and fail
	waitFor(t, "first attempt", func() bool { return attempts.Load() == 1 })

	// Wait briefly for scheduler loop to process failure and arm backoff timer
	time.Sleep(20 * time.Millisecond)

	// Advance clock past retry delay (100ms)
	clock.Advance(200 * time.Millisecond)

	waitFor(t, "second attempt", func() bool { return attempts.Load() == 2 })

	if err := run.waitDone(t); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	snap := tracker.Snapshot()
	if snap.SampleCount != 1 {
		t.Fatalf("expected 1 sample, got %d", snap.SampleCount)
	}
	if snap.FirstPublishes != 1 {
		t.Fatalf("expected 1 first publish, got %d", snap.FirstPublishes)
	}
	if snap.RetryAttempts != 1 {
		t.Fatalf("expected 1 retry attempt, got %d", snap.RetryAttempts)
	}
	// Latency must include the 200ms retry wait plus 10ms ACK delay = 210ms (from first publish call),
	// NOT just 10ms (from retry call)!
	if snap.P50 < 200*time.Millisecond {
		t.Fatalf("expected latency >= 200ms from first publish call, got %v", snap.P50)
	}
}

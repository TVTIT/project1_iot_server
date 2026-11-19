package mqttcontrol

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"

	"github.com/google/uuid"
)

type testRepository struct {
	mu                       sync.Mutex
	id                       uuid.UUID
	prepares, marks, expires int
	marked                   bool
}

func (r *testRepository) PendingControl(context.Context, int) ([]uuid.UUID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.marked {
		return nil, nil
	}
	return []uuid.UUID{r.id, r.id}, nil
}
func (r *testRepository) PrepareControl(context.Context, uuid.UUID) (*digitaltwin.ControlDispatch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prepares++
	return &digitaltwin.ControlDispatch{CommandID: r.id, Topic: "stable", Payload: []byte("same bytes")}, nil
}
func (r *testRepository) MarkControlPublished(context.Context, uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.marks++
	if r.marks == 1 {
		return errors.New("ambiguous mark")
	}
	r.marked = true
	return nil
}
func (r *testRepository) MarkControlReservationPublished(context.Context, uuid.UUID, int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.marks++
	if r.marks == 1 {
		return errors.New("ambiguous mark")
	}
	r.marked = true
	return nil
}
func (r *testRepository) ExpireControl(context.Context, int) error {
	r.mu.Lock()
	r.expires++
	r.mu.Unlock()
	return nil
}

type testPublisher struct {
	mu                 sync.Mutex
	calls, active, max int
	ack                chan struct{}
	ready              bool
}

type enteredPublisher struct{ entered chan struct{} }

func (*enteredPublisher) Ready() bool { return true }
func (p *enteredPublisher) Publish(ctx context.Context, _ digitaltwin.ControlDispatch) error {
	p.entered <- struct{}{}
	<-ctx.Done()
	return ctx.Err()
}

type immediatePublisher struct{}

func (immediatePublisher) Ready() bool                                                { return true }
func (immediatePublisher) Publish(context.Context, digitaltwin.ControlDispatch) error { return nil }

type nilNoOpMarkRepo struct {
	*wakeRepository
	marks chan struct{}
}

func (r *nilNoOpMarkRepo) MarkControlPublished(context.Context, uuid.UUID) error {
	r.marks <- struct{}{}
	return nil // models a conditional no-op; it deliberately changes no durable state
}

func (p *testPublisher) Ready() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.ready }
func (p *testPublisher) Publish(ctx context.Context, d digitaltwin.ControlDispatch) error {
	if d.Topic != "stable" || string(d.Payload) != "same bytes" {
		return errors.New("mutated durable bytes")
	}
	p.mu.Lock()
	p.calls++
	p.active++
	if p.active > p.max {
		p.max = p.active
	}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); p.active--; p.mu.Unlock() }()
	select {
	case <-p.ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRuntimePUBACKRetryBoundsAndStop(t *testing.T) {
	repo := &testRepository{id: uuid.New()}
	pub := &testPublisher{ack: make(chan struct{}), ready: true}
	cfg := DefaultConfig()
	cfg.Workers = 2
	cfg.ScanInterval = 10 * time.Millisecond
	cfg.ExpireInterval = 10 * time.Millisecond
	cfg.RetryBackoff = 20 * time.Millisecond
	r, err := New(cfg, repo, pub)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		r.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if e := r.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	deadline := time.Now().Add(time.Second)
	for {
		pub.mu.Lock()
		calls := pub.calls
		pub.mu.Unlock()
		if calls > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pending was not dispatched")
		}
		time.Sleep(time.Millisecond)
	}
	repo.mu.Lock()
	marks := repo.marks
	repo.mu.Unlock()
	if marks != 0 {
		t.Fatal("marked before PUBACK")
	}
	close(pub.ack)
	for {
		repo.mu.Lock()
		done := repo.marked
		repo.mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ambiguous DB mark was not retried")
		}
		time.Sleep(time.Millisecond)
	}
	pub.mu.Lock()
	max := pub.max
	calls := pub.calls
	pub.mu.Unlock()
	if max != 1 || calls != 2 {
		t.Fatalf("same-ID in-flight/retry max=%d calls=%d", max, calls)
	}
	if !r.Ready() {
		t.Fatal("running subscribed runtime not ready")
	}
	r.Stop()
	if r.Ready() {
		t.Fatal("shutdown still ready")
	}
}

func TestRuntimeFailClosedBounds(t *testing.T) {
	if _, e := New(DefaultConfig(), nil, nil); e == nil {
		t.Fatal("nil dependencies")
	}
	for _, mutate := range []func(*Config){func(c *Config) { c.Workers = 0 }, func(c *Config) { c.BatchSize = 1025 }, func(c *Config) { c.QueueCapacity = 0 }, func(c *Config) { c.QueueCapacity = 4097 }, func(c *Config) { c.MaxPayloadBytes = 8193 }, func(c *Config) { c.ScanInterval = 0 }, func(c *Config) { c.ExpireInterval = 0 }, func(c *Config) { c.PublishTimeout = 0 }, func(c *Config) { c.RetryBackoff = 0 }, func(c *Config) { c.ResultQueueCapacity = 0 }} {
		cfg := DefaultConfig()
		mutate(&cfg)
		if _, e := New(cfg, &testRepository{}, &testPublisher{}); e == nil {
			t.Errorf("invalid bounds %+v", cfg)
		}
	}
}

func TestRuntimeQueueCapacityIsIndependentOfBatchSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BatchSize = 1
	cfg.QueueCapacity = 3
	r, err := New(cfg, &testRepository{}, &testPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	if cap(r.jobs) != 3 {
		t.Fatalf("jobs capacity=%d want 3", cap(r.jobs))
	}
	if got, want := cfg.QueueCapacity+cfg.Workers, 4; got != want {
		t.Fatalf("in-flight bound=%d want %d", got, want)
	}
}

func TestRuntimeNotifyIsBoundedAndLifecycleSafe(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.WakeupEnabled = enabled
			r, err := New(cfg, &testRepository{id: uuid.New()}, &testPublisher{})
			if err != nil {
				t.Fatal(err)
			}
			r.Notify() // inert before Start
			if len(r.wakeup) != 0 {
				t.Fatal("notifier signaled before runtime start")
			}
			if err = r.Start(); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 100; i++ {
				r.Notify()
			}
			want := 0
			if enabled {
				want = 1
			}
			if len(r.wakeup) != want {
				t.Fatalf("coalesced signal count=%d want=%d", len(r.wakeup), want)
			}
			r.Stop()
			r.Notify() // must not send to a closed channel or block during shutdown
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err = r.Close(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// wakeRepository models durable pending rows. A scan returns the first batch
// not marked, making an in-flight prefix exercise refill behavior.
type wakeRepository struct {
	mu       sync.Mutex
	ids      []uuid.UUID
	marked   map[uuid.UUID]bool
	scans    chan struct{}
	prepared chan uuid.UUID
}

func (r *wakeRepository) PendingControl(_ context.Context, limit int) ([]uuid.UUID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.scans != nil {
		select {
		case r.scans <- struct{}{}:
		default:
		}
	}
	ids := make([]uuid.UUID, 0, limit)
	for _, id := range r.ids {
		if !r.marked[id] {
			ids = append(ids, id)
			if len(ids) == limit {
				break
			}
		}
	}
	return ids, nil
}
func (r *wakeRepository) PrepareControl(_ context.Context, id uuid.UUID) (*digitaltwin.ControlDispatch, error) {
	if r.prepared != nil {
		r.prepared <- id
	}
	return &digitaltwin.ControlDispatch{CommandID: id, Topic: "stable", Payload: []byte("same bytes")}, nil
}
func (r *wakeRepository) MarkControlPublished(_ context.Context, id uuid.UUID) error {
	r.mu.Lock()
	r.marked[id] = true
	r.mu.Unlock()
	return nil
}
func (r *wakeRepository) MarkControlReservationPublished(_ context.Context, id uuid.UUID, _ int) error {
	r.mu.Lock()
	r.marked[id] = true
	r.mu.Unlock()
	return nil
}
func (r *wakeRepository) ExpireControl(context.Context, int) error { return nil }

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestRuntimeWakeDispatchesAfterStartupScan(t *testing.T) {
	id := uuid.New()
	repo := &wakeRepository{ids: nil, marked: map[uuid.UUID]bool{}, scans: make(chan struct{}, 8), prepared: make(chan uuid.UUID, 8)}
	pub := &testPublisher{ack: make(chan struct{}, 8), ready: true}
	cfg := DefaultConfig()
	cfg.WakeupEnabled = true
	cfg.ScanInterval = 30 * time.Second
	r, err := New(cfg, repo, pub)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		r.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if e := r.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	waitSignal(t, repo.scans, "startup scan")
	// The startup query completed before this new durable row and its hint.
	repo.mu.Lock()
	repo.ids = append(repo.ids, id)
	newID := id
	repo.mu.Unlock()
	r.Notify()
	select {
	case got := <-repo.prepared:
		if got != newID {
			t.Fatalf("prepared %s, want new row %s", got, newID)
		}
	case <-time.After(time.Second):
		t.Fatal("wake-up did not dispatch before long periodic scan")
	}
}

func TestRuntimeWakeCoalescingAndBatchBacklogRefill(t *testing.T) {
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	repo := &wakeRepository{ids: ids, marked: map[uuid.UUID]bool{}, scans: make(chan struct{}, 32), prepared: make(chan uuid.UUID, 16)}
	ack := make(chan struct{})
	pub := &testPublisher{ack: ack, ready: true}
	cfg := DefaultConfig()
	cfg.WakeupEnabled = true
	cfg.BatchSize = 2
	cfg.ScanInterval = 30 * time.Second
	cfg.Workers = 1
	r, err := New(cfg, repo, pub)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		r.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if e := r.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	waitSignal(t, repo.scans, "startup scan")
	for i := 0; i < 10; i++ {
		r.Notify()
	}
	select {
	case <-repo.prepared:
	case <-time.After(time.Second):
		t.Fatal("first publish preparation did not start")
	}
	// Hold the first worker; repeated hints coalesce and no same ID is concurrently dispatched.
	for i := 0; i < 10; i++ {
		r.Notify()
	}
	if len(r.wakeup) > 1 {
		t.Fatal("wake channel exceeded capacity")
	}
	ack <- struct{}{}
	for range ids[1:] {
		select {
		case <-repo.prepared:
		case <-time.After(2 * time.Second):
			t.Fatal("backlog did not progress after worker release")
		}
		ack <- struct{}{}
	}
}

func TestRuntimePollingRecoversRowsWithoutHint(t *testing.T) {
	id := uuid.New()
	repo := &wakeRepository{ids: []uuid.UUID{id}, marked: map[uuid.UUID]bool{}, scans: make(chan struct{}, 8), prepared: make(chan uuid.UUID, 4)}
	pub := &testPublisher{ack: make(chan struct{}, 4), ready: true}
	cfg := DefaultConfig()
	cfg.ScanInterval = 10 * time.Millisecond
	r, err := New(cfg, repo, pub)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		r.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if e := r.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	waitSignal(t, repo.scans, "startup scan")
	select {
	case got := <-repo.prepared:
		if got != id {
			t.Fatal("wrong recovered row")
		}
	case <-time.After(time.Second):
		t.Fatal("periodic scan did not recover durable row")
	}
}

func TestRuntimeNotReadyAndQueueFullLeaveDurableRowsPending(t *testing.T) {
	id := uuid.New()
	repo := &wakeRepository{ids: []uuid.UUID{id}, marked: map[uuid.UUID]bool{}, prepared: make(chan uuid.UUID, 2)}
	pub := &testPublisher{ack: make(chan struct{}, 2), ready: true}
	cfg := DefaultConfig()
	cfg.BatchSize = 1
	cfg.QueueCapacity = 2
	m := &Metrics{}
	r, err := New(cfg, repo, pub, m)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise a full bounded queue without launching consumers.
	r.jobs <- dispatchJob{id: uuid.New()}
	r.jobs <- dispatchJob{id: uuid.New()}
	r.discover()
	if len(r.inflight) != 0 {
		t.Fatal("queue-full discovery retained reservation")
	}
	repo.mu.Lock()
	stillPending := !repo.marked[id]
	repo.mu.Unlock()
	if !stillPending {
		t.Fatal("queue-full discovery changed durable pending state")
	}
	if snapshot := m.Snapshot(); snapshot.QueueFull != 1 || snapshot.QueueDepth != 2 || snapshot.QueueCapacity != 2 || snapshot.InFlight != 0 {
		t.Fatalf("queue-full metrics = %+v", snapshot)
	}

	// An unready transport must not query/consume the pending intent; polling
	// discovers it after readiness returns, without a wake hint.
	pub.mu.Lock()
	pub.ready = false
	pub.mu.Unlock()
	cfg.ScanInterval = 10 * time.Millisecond
	r2, err := New(cfg, repo, pub)
	if err != nil {
		t.Fatal(err)
	}
	r2.discover()
	repo.mu.Lock()
	stillPending = !repo.marked[id]
	repo.mu.Unlock()
	if !stillPending {
		t.Fatal("unready transport changed durable pending state")
	}
	pub.mu.Lock()
	pub.ready = true
	pub.mu.Unlock()
	if err = r2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		r2.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if e := r2.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	select {
	case got := <-repo.prepared:
		if got != id {
			t.Fatalf("prepared %s, want %s", got, id)
		}
	case <-time.After(time.Second):
		t.Fatal("polling did not recover pending row after transport readiness")
	}
}

func TestRuntimeCloseDrainsQueuedWorkAndReservations(t *testing.T) {
	first, queued := uuid.New(), uuid.New()
	repo := &wakeRepository{ids: []uuid.UUID{first}, marked: map[uuid.UUID]bool{}, prepared: make(chan uuid.UUID, 2)}
	pub := &enteredPublisher{entered: make(chan struct{}, 1)}
	cfg := DefaultConfig()
	cfg.Workers = 1
	cfg.QueueCapacity = 2
	cfg.ScanInterval = time.Minute
	m := &Metrics{}
	r, err := New(cfg, repo, pub, m)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Start(); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, pub.entered, "publish entered")
	r.mu.Lock()
	r.inflight[queued] = true
	r.jobs <- dispatchJob{id: queued}
	r.metrics.inFlight.Store(int64(len(r.inflight)))
	r.metrics.queueDepth.Store(int64(len(r.jobs)))
	r.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(r.jobs); got != 0 {
		t.Fatalf("queued jobs retained after joined close: %d", got)
	}
	r.mu.Lock()
	reserved := len(r.inflight)
	r.mu.Unlock()
	if reserved != 0 {
		t.Fatalf("reservations retained after joined close: %d", reserved)
	}
	if s := m.Snapshot(); s.QueueDepth != 0 || s.InFlight != 0 || s.LivePublishWorkers != 0 {
		t.Fatalf("post-close gauges = %+v", s)
	}
}

func TestPublishedMarkSuccessfulCallsIncludesNilNoOpNotStateTransition(t *testing.T) {
	id := uuid.New()
	base := &wakeRepository{ids: []uuid.UUID{id}, marked: map[uuid.UUID]bool{}, prepared: make(chan uuid.UUID, 1)}
	repo := &nilNoOpMarkRepo{wakeRepository: base, marks: make(chan struct{}, 1)}
	cfg := DefaultConfig()
	cfg.ScanInterval = time.Minute
	m := &Metrics{}
	r, err := New(cfg, repo, immediatePublisher{}, m)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, repo.marks, "nil no-op mark")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	base.mu.Lock()
	changed := base.marked[id]
	base.mu.Unlock()
	if changed {
		t.Fatal("no-op repository unexpectedly changed durable state")
	}
	if got := m.Snapshot().PublishedMarkSuccessfulCalls; got != 1 {
		t.Fatalf("successful mark calls = %d, want 1 for nil/no-op", got)
	}
}

type stage6MockRepository struct {
	mu           sync.Mutex
	id           uuid.UUID
	dispatch     *digitaltwin.ControlDispatch
	prepareErr   error
	prepareCalls int
	legacyMarks  int
	fencedMarks  map[int]int
	pendingIDs   []uuid.UUID
}

func (r *stage6MockRepository) PendingControl(context.Context, int) ([]uuid.UUID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	res := r.pendingIDs
	r.pendingIDs = nil
	return res, nil
}

func (r *stage6MockRepository) PrepareControl(context.Context, uuid.UUID) (*digitaltwin.ControlDispatch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prepareCalls++
	if r.prepareErr != nil {
		return nil, r.prepareErr
	}
	return r.dispatch, nil
}

func (r *stage6MockRepository) MarkControlPublished(context.Context, uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.legacyMarks++
	return nil
}

func (r *stage6MockRepository) MarkControlReservationPublished(_ context.Context, _ uuid.UUID, attempt int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fencedMarks == nil {
		r.fencedMarks = map[int]int{}
	}
	r.fencedMarks[attempt]++
	return nil
}

func (r *stage6MockRepository) ExpireControl(context.Context, int) error { return nil }

func TestStage6RuntimeDispatchAndFencing(t *testing.T) {
	id := uuid.New()
	now := time.Now()
	dispatch := &digitaltwin.ControlDispatch{
		CommandID: id,
		Topic:     "stable",
		Payload:   []byte("same bytes"),
		Attempt:   2,
		Stage6:    true,
		ExpiresAt: now.Add(time.Minute),
		LeaseAt:   now.Add(4 * time.Second),
	}

	repo := &stage6MockRepository{
		id:         id,
		dispatch:   dispatch,
		pendingIDs: []uuid.UUID{id},
	}
	pub := &testPublisher{ack: make(chan struct{}, 8), ready: true}
	cfg := DefaultConfig()
	cfg.Workers = 1
	cfg.WakeupEnabled = true
	cfg.RetryBackoff = 5 * time.Second // Long backoff to verify Stage 6 does NOT sleep on error
	m := &Metrics{}

	r, err := New(cfg, repo, pub, m)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Not ready before prepare: consumes no grant
	pub.mu.Lock()
	pub.ready = false
	pub.mu.Unlock()

	r.jobs <- dispatchJob{id: id}
	r.mu.Lock()
	r.inflight[id] = true
	r.mu.Unlock()

	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		r.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = r.Close(ctx)
	})

	// Wait briefly: verify Prepare was NOT called while transport was not ready
	time.Sleep(50 * time.Millisecond)
	repo.mu.Lock()
	preparesWhileUnready := repo.prepareCalls
	repo.mu.Unlock()
	if preparesWhileUnready != 0 {
		t.Fatalf("prepare called while transport was unready: %d", preparesWhileUnready)
	}

	// 2. Transport becomes ready and notify triggers discovery and dispatch
	pub.mu.Lock()
	pub.ready = true
	pub.mu.Unlock()
	pub.ack <- struct{}{} // allow publish
	r.Notify()

	// Wait for publish to succeed and fenced mark to be called
	deadline := time.Now().Add(2 * time.Second)
	for {
		repo.mu.Lock()
		fencedCalls := repo.fencedMarks[2]
		legacyCalls := repo.legacyMarks
		repo.mu.Unlock()

		if fencedCalls > 0 {
			if legacyCalls != 0 {
				t.Fatalf("Stage 6 dispatch called legacy MarkControlPublished: %d", legacyCalls)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for fenced completion on attempt 2")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Verify metrics
	snap := m.Snapshot()
	if snap.Stage6Reservations != 1 {
		t.Fatalf("reservations metric = %d, want 1", snap.Stage6Reservations)
	}
	if snap.PublishCalls != 1 {
		t.Fatalf("publish_calls metric = %d, want 1", snap.PublishCalls)
	}
	if snap.Stage6RetryPublishCalls != 1 {
		t.Fatalf("retry_publishes metric = %d, want 1", snap.Stage6RetryPublishCalls)
	}
	if snap.PUBACK != 1 {
		t.Fatalf("puback metric = %d, want 1", snap.PUBACK)
	}
	if snap.GrantsSkippedBeforeNetwork != 0 {
		t.Fatalf("grant_skipped_network metric = %d, want 0", snap.GrantsSkippedBeforeNetwork)
	}

	// 3. Stage 6 worker releases slot immediately on publish failure without sleeping RetryBackoff (5s)
	failID := uuid.New()
	failDispatch := &digitaltwin.ControlDispatch{
		CommandID: failID,
		Topic:     "bad_topic", // testPublisher rejects topics other than "stable"
		Payload:   []byte("same bytes"),
		Attempt:   1,
		Stage6:    true,
		ExpiresAt: time.Now().Add(time.Minute),
		LeaseAt:   time.Now().Add(4 * time.Second),
	}
	repo.mu.Lock()
	repo.dispatch = failDispatch
	repo.mu.Unlock()

	pub.ack <- struct{}{} // allow publish attempt
	failStart := time.Now()
	enqueueStage6Job(r, failID)

	// Wait for failID to be removed from inflight
	failDeadline := time.Now().Add(2 * time.Second)
	for {
		r.mu.Lock()
		inFlight := r.inflight[failID]
		r.mu.Unlock()
		if !inFlight {
			break
		}
		if time.Now().After(failDeadline) {
			t.Fatal("timed out waiting for worker to release slot after error; worker likely slept on RetryBackoff")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if elapsed := time.Since(failStart); elapsed >= 4*time.Second {
		t.Fatalf("worker took %v to release slot, expected immediate release (< 2s, not 5s backoff)", elapsed)
	}
	if snapFail := m.Snapshot(); snapFail.PublishFailures < 1 {
		t.Fatalf("publish_failures metric = %d, want >= 1", snapFail.PublishFailures)
	}

	// 4. Authority expired before publish skips network call
	skipID := uuid.New()
	expiredDispatch := &digitaltwin.ControlDispatch{
		CommandID: skipID,
		Topic:     "stable",
		Payload:   []byte("same bytes"),
		Attempt:   1,
		Stage6:    true,
		ExpiresAt: time.Now().Add(time.Minute),
		LeaseAt:   time.Now().Add(-time.Second), // expired lease!
	}
	repo.mu.Lock()
	repo.dispatch = expiredDispatch
	repo.mu.Unlock()

	enqueueStage6Job(r, skipID)

	skipDeadline := time.Now().Add(2 * time.Second)
	for {
		r.mu.Lock()
		inFlight := r.inflight[skipID]
		r.mu.Unlock()
		if !inFlight {
			break
		}
		if time.Now().After(skipDeadline) {
			t.Fatal("timed out waiting for worker to release slot for skipped network grant")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if snapSkip := m.Snapshot(); snapSkip.GrantsSkippedBeforeNetwork != 1 {
		t.Fatalf("grant_skipped_network metric = %d, want 1", snapSkip.GrantsSkippedBeforeNetwork)
	}
}

type failingStage6Publisher struct {
	mu    sync.Mutex
	calls int
	ready bool
}

func (p *failingStage6Publisher) Ready() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.ready }
func (p *failingStage6Publisher) Publish(context.Context, digitaltwin.ControlDispatch) error {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	return errors.New("injected publish failure")
}

func enqueueStage6Job(r *Runtime, id uuid.UUID) {
	r.mu.Lock()
	r.inflight[id] = true
	r.mu.Unlock()
	r.jobs <- dispatchJob{id: id}
}

func waitStage6Released(r *Runtime, id uuid.UUID, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		held := r.inflight[id]
		r.mu.Unlock()
		if !held {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// Legacy dispatch must never be reported as a Stage 6 grant, and a managed
// retry call must be counted even when the publish itself fails.
func TestStage6MetricsDistinguishGrantsPublishesAndSkips(t *testing.T) {
	// 1. Legacy (Stage 5) dispatch: no Stage 6 grant, one publish, one PUBACK.
	legacyID := uuid.New()
	legacyRepo := &stage6MockRepository{
		id:       legacyID,
		dispatch: &digitaltwin.ControlDispatch{CommandID: legacyID, Topic: "stable", Payload: []byte("same bytes")},
	}
	legacyPub := &testPublisher{ack: make(chan struct{}, 4), ready: true}
	legacyMetrics := &Metrics{}
	legacyCfg := DefaultConfig()
	legacyCfg.Workers = 1
	legacyRuntime, err := New(legacyCfg, legacyRepo, legacyPub, legacyMetrics)
	if err != nil {
		t.Fatal(err)
	}
	if err = legacyRuntime.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		legacyRuntime.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = legacyRuntime.Close(ctx)
	})
	legacyPub.ack <- struct{}{}
	enqueueStage6Job(legacyRuntime, legacyID)
	if !waitStage6Released(legacyRuntime, legacyID, 2*time.Second) {
		t.Fatal("legacy job was not released")
	}
	legacySnap := legacyMetrics.Snapshot()
	if legacySnap.Stage6Reservations != 0 {
		t.Fatalf("legacy dispatch counted as Stage 6 reservation: %d", legacySnap.Stage6Reservations)
	}
	if legacySnap.PublishCalls != 1 || legacySnap.PUBACK != 1 {
		t.Fatalf("legacy publish accounting: calls=%d puback=%d", legacySnap.PublishCalls, legacySnap.PUBACK)
	}
	if legacySnap.Stage6RetryPublishCalls != 0 {
		t.Fatalf("legacy dispatch counted as Stage 6 retry call: %d", legacySnap.Stage6RetryPublishCalls)
	}

	// 2. Managed dispatch: the grant is confirmed, the publish call happens and
	// a failing attempt is still counted.
	stage6ID := uuid.New()
	stage6Repo := &stage6MockRepository{
		id: stage6ID,
		dispatch: &digitaltwin.ControlDispatch{
			CommandID: stage6ID,
			Topic:     "stable",
			Payload:   []byte("same bytes"),
			Attempt:   1,
			Stage6:    true,
			ExpiresAt: time.Now().Add(time.Minute),
			LeaseAt:   time.Now().Add(4 * time.Second),
		},
	}
	stage6Pub := &failingStage6Publisher{ready: true}
	stage6Metrics := &Metrics{}
	stage6Cfg := DefaultConfig()
	stage6Cfg.Workers = 1
	stage6Cfg.Stage6Enabled = true
	stage6Runtime, err := New(stage6Cfg, stage6Repo, stage6Pub, stage6Metrics)
	if err != nil {
		t.Fatal(err)
	}
	if err = stage6Runtime.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stage6Runtime.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = stage6Runtime.Close(ctx)
	})
	enqueueStage6Job(stage6Runtime, stage6ID)
	if !waitStage6Released(stage6Runtime, stage6ID, 2*time.Second) {
		t.Fatal("managed job was not released")
	}
	snap := stage6Metrics.Snapshot()
	if snap.Stage6Reservations != 1 {
		t.Fatalf("stage6_reservations = %d, want 1", snap.Stage6Reservations)
	}
	if snap.PublishCalls != 1 || snap.PublishFailures != 1 {
		t.Fatalf("publish accounting: calls=%d failures=%d", snap.PublishCalls, snap.PublishFailures)
	}
	if snap.Stage6RetryPublishCalls != 0 {
		t.Fatalf("first attempt counted as retry call: %d", snap.Stage6RetryPublishCalls)
	}

	// 3. A managed retry attempt (attempt > 1) is counted even when it fails.
	retryID := uuid.New()
	stage6Repo.mu.Lock()
	stage6Repo.dispatch = &digitaltwin.ControlDispatch{
		CommandID: retryID,
		Topic:     "stable",
		Payload:   []byte("same bytes"),
		Attempt:   2,
		Stage6:    true,
		ExpiresAt: time.Now().Add(time.Minute),
		LeaseAt:   time.Now().Add(4 * time.Second),
	}
	stage6Repo.mu.Unlock()
	enqueueStage6Job(stage6Runtime, retryID)
	if !waitStage6Released(stage6Runtime, retryID, 2*time.Second) {
		t.Fatal("managed retry job was not released")
	}
	retrySnap := stage6Metrics.Snapshot()
	if retrySnap.Stage6RetryPublishCalls != 1 {
		t.Fatalf("stage6_retry_publish_calls = %d, want 1 (failed attempt included)", retrySnap.Stage6RetryPublishCalls)
	}
	if retrySnap.Stage6Reservations != 2 {
		t.Fatalf("stage6_reservations = %d, want 2", retrySnap.Stage6Reservations)
	}
}

// A Stage 6 runtime must release the worker promptly when Prepare fails or the
// commit is ambiguous, without waiting the Stage 5 retry backoff.
func TestStage6RuntimeReleasesWorkerOnPrepareError(t *testing.T) {
	id := uuid.New()
	repo := &stage6MockRepository{id: id, prepareErr: errors.New("ambiguous commit")}
	pub := &failingStage6Publisher{ready: true}
	m := &Metrics{}
	cfg := DefaultConfig()
	cfg.Workers = 1
	cfg.Stage6Enabled = true
	cfg.RetryBackoff = 5 * time.Second
	r, err := New(cfg, repo, pub, m)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		r.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = r.Close(ctx)
	})
	start := time.Now()
	enqueueStage6Job(r, id)
	if !waitStage6Released(r, id, 2*time.Second) {
		t.Fatal("Stage 6 worker slept the Stage 5 backoff after a Prepare error")
	}
	if elapsed := time.Since(start); elapsed >= 4*time.Second {
		t.Fatalf("Stage 6 worker release took %v", elapsed)
	}
}

type stage6FailingRepository struct {
	mu           sync.Mutex
	dueID        uuid.UUID
	fail         bool
	prepareCalls int
}

func (r *stage6FailingRepository) setFail(fail bool) {
	r.mu.Lock()
	r.fail = fail
	r.mu.Unlock()
}

func (r *stage6FailingRepository) prepareCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prepareCalls
}

func (r *stage6FailingRepository) PendingControl(context.Context, int) ([]uuid.UUID, error) {
	return []uuid.UUID{r.dueID}, nil
}

func (r *stage6FailingRepository) PrepareControl(_ context.Context, id uuid.UUID) (*digitaltwin.ControlDispatch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prepareCalls++
	if r.fail {
		return nil, errors.New("injected prepare failure")
	}
	return &digitaltwin.ControlDispatch{CommandID: id, Topic: "stable", Payload: []byte("same bytes"), Attempt: 1, Stage6: true, ExpiresAt: time.Now().Add(time.Minute), LeaseAt: time.Now().Add(4 * time.Second)}, nil
}

func (r *stage6FailingRepository) MarkControlPublished(context.Context, uuid.UUID) error { return nil }

func (r *stage6FailingRepository) MarkControlReservationPublished(context.Context, uuid.UUID, int) error {
	return nil
}

func (r *stage6FailingRepository) ExpireControl(context.Context, int) error { return nil }

// A Stage 6 Prepare failure must fall back to the bounded periodic scan. A
// completion wake-up would re-run discovery on the same still-due row and
// produce an immediate loop instead of scan pacing.
func TestStage6PrepareErrorFollowsScanPacing(t *testing.T) {
	dueID := uuid.New()
	repo := &stage6FailingRepository{dueID: dueID, fail: true}
	pub := &failingStage6Publisher{ready: true}
	m := &Metrics{}
	cfg := DefaultConfig()
	cfg.Workers = 1
	cfg.Stage6Enabled = true
	cfg.WakeupEnabled = true
	cfg.ScanInterval = 200 * time.Millisecond
	cfg.RetryBackoff = 5 * time.Second
	r, err := New(cfg, repo, pub, m)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Start(); err != nil {
		t.Fatal(err)
	}

	const window = 700 * time.Millisecond
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	calls := repo.prepareCount()
	// One discovery pass per scan interval may reach PrepareControl; a wake-up
	// loop would reach it orders of magnitude more often.
	maxExpected := int(window/cfg.ScanInterval) + 3
	if calls > maxExpected {
		t.Fatalf("PrepareControl called %d times within %v (scan interval %v, bound %d): Stage 6 failure hot loop", calls, window, cfg.ScanInterval, maxExpected)
	}
	if calls < 1 {
		t.Fatal("no PrepareControl attempt observed; the failing row was never retried")
	}

	// Another valid queued job must still progress while the failing row waits
	// for the next scan.
	repo.setFail(false)
	validID := uuid.New()
	enqueueStage6Job(r, validID)
	progressDeadline := time.Now().Add(2 * time.Second)
	for {
		pub.mu.Lock()
		published := pub.calls
		pub.mu.Unlock()
		if published > 0 {
			break
		}
		if time.Now().After(progressDeadline) {
			t.Fatal("a valid queued job did not progress while one row failed")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Shutdown stays bounded.
	r.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = r.Close(ctx); err != nil {
		t.Fatalf("shutdown was not bounded: %v", err)
	}
}

type stage6NoAuthorityRepository struct {
	mu           sync.Mutex
	exhaustedID  uuid.UUID
	prepareCalls int
}

func (r *stage6NoAuthorityRepository) prepareCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prepareCalls
}

func (r *stage6NoAuthorityRepository) PendingControl(context.Context, int) ([]uuid.UUID, error) {
	return []uuid.UUID{r.exhaustedID}, nil
}

func (r *stage6NoAuthorityRepository) PrepareControl(_ context.Context, id uuid.UUID) (*digitaltwin.ControlDispatch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prepareCalls++
	if id == r.exhaustedID {
		return nil, nil
	}
	return &digitaltwin.ControlDispatch{
		CommandID: id,
		Topic:     "stable",
		Payload:   []byte("same bytes"),
		Attempt:   1,
		Stage6:    true,
		ExpiresAt: time.Now().Add(time.Minute),
		LeaseAt:   time.Now().Add(4 * time.Second),
	}, nil
}

func (r *stage6NoAuthorityRepository) MarkControlPublished(context.Context, uuid.UUID) error {
	return nil
}

func (r *stage6NoAuthorityRepository) MarkControlReservationPublished(context.Context, uuid.UUID, int) error {
	return nil
}

func (r *stage6NoAuthorityRepository) ExpireControl(context.Context, int) error { return nil }

func TestStage6NoDispatchAuthorityDoesNotWakeup(t *testing.T) {
	for _, wakeup := range []bool{true, false} {
		t.Run(fmt.Sprintf("WakeupEnabled=%v", wakeup), func(t *testing.T) {
			exhaustedID := uuid.New()
			repo := &stage6NoAuthorityRepository{exhaustedID: exhaustedID}
			pub := &failingStage6Publisher{ready: true}
			cfg := DefaultConfig()
			cfg.Workers = 1
			cfg.Stage6Enabled = true
			cfg.WakeupEnabled = wakeup
			cfg.ScanInterval = 1 * time.Second
			cfg.BatchSize = 1

			r, err := New(cfg, repo, pub, &Metrics{})
			if err != nil {
				t.Fatal(err)
			}
			if err = r.Start(); err != nil {
				t.Fatal(err)
			}

			time.Sleep(250 * time.Millisecond)

			calls := repo.prepareCount()
			t.Cleanup(func() {
				r.Stop()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = r.Close(ctx)
			})

			if calls > 3 {
				t.Fatalf("PrepareControl called %d times in 250ms with scan interval 1s (wakeup=%v): hot loop on no dispatch authority", calls, wakeup)
			}
		})
	}
}

func TestStage6NoAuthorityLaterQueuedWorkProgresses(t *testing.T) {
	exhaustedID := uuid.New()
	validID := uuid.New()
	repo := &stage6NoAuthorityRepository{exhaustedID: exhaustedID}
	pub := &testPublisher{ack: make(chan struct{}, 8), ready: true}
	pub.ack <- struct{}{}

	cfg := DefaultConfig()
	cfg.Workers = 1
	cfg.Stage6Enabled = true
	cfg.WakeupEnabled = true
	cfg.ScanInterval = 1 * time.Second
	cfg.BatchSize = 2

	r, err := New(cfg, repo, pub, &Metrics{})
	if err != nil {
		t.Fatal(err)
	}
	r.jobs <- dispatchJob{id: exhaustedID}
	r.jobs <- dispatchJob{id: validID}
	r.mu.Lock()
	r.inflight[exhaustedID] = true
	r.inflight[validID] = true
	r.mu.Unlock()

	if err = r.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = r.Close(ctx)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		pub.mu.Lock()
		published := pub.calls
		pub.mu.Unlock()
		if published > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("valid job queued behind no-authority row did not progress")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

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

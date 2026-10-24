package mqttcontrol

import (
	"context"
	"errors"
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
	for _, mutate := range []func(*Config){func(c *Config) { c.Workers = 0 }, func(c *Config) { c.BatchSize = 1025 }, func(c *Config) { c.MaxPayloadBytes = 8193 }, func(c *Config) { c.ScanInterval = 0 }, func(c *Config) { c.ExpireInterval = 0 }, func(c *Config) { c.PublishTimeout = 0 }, func(c *Config) { c.RetryBackoff = 0 }, func(c *Config) { c.ResultQueueCapacity = 0 }} {
		cfg := DefaultConfig()
		mutate(&cfg)
		if _, e := New(cfg, &testRepository{}, &testPublisher{}); e == nil {
			t.Errorf("invalid bounds %+v", cfg)
		}
	}
}

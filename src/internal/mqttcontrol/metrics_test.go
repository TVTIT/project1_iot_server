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

// retryMetricsRepository exposes each dispatcher stage to the test. PendingControl
// returns a single durable row until its second mark succeeds.
type retryMetricsRepository struct {
	id           uuid.UUID
	discoveries  chan struct{}
	dequeued     chan struct{}
	markAttempts chan int
	mu           sync.Mutex
	marked       bool
	marks        int
}

func (r *retryMetricsRepository) PendingControl(context.Context, int) ([]uuid.UUID, error) {
	r.discoveries <- struct{}{}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.marked {
		return nil, nil
	}
	return []uuid.UUID{r.id}, nil
}

func (r *retryMetricsRepository) PrepareControl(context.Context, uuid.UUID) (*digitaltwin.ControlDispatch, error) {
	r.dequeued <- struct{}{}
	return &digitaltwin.ControlDispatch{CommandID: r.id, Topic: "stable", Payload: []byte("same bytes")}, nil
}

func (r *retryMetricsRepository) MarkControlPublished(context.Context, uuid.UUID) error {
	r.mu.Lock()
	r.marks++
	mark := r.marks
	if mark == 2 {
		r.marked = true
	}
	r.mu.Unlock()
	r.markAttempts <- mark
	if mark == 1 {
		return errors.New("ambiguous mark")
	}
	return nil
}

func (*retryMetricsRepository) ExpireControl(context.Context, int) error { return nil }

type retryMetricsPublisher struct {
	mu      sync.Mutex
	calls   int
	entered chan int
	acks    []chan struct{}
}

func (*retryMetricsPublisher) Ready() bool { return true }

func (p *retryMetricsPublisher) Publish(ctx context.Context, d digitaltwin.ControlDispatch) error {
	if d.Topic != "stable" || string(d.Payload) != "same bytes" {
		return errors.New("mutated durable bytes")
	}
	p.mu.Lock()
	p.calls++
	n := p.calls
	ack := p.acks[n-1]
	p.mu.Unlock()
	p.entered <- n
	select {
	case <-ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func awaitRetryMetricEvent(t *testing.T, ch <-chan int, want int) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("event=%d, want %d", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for event %d", want)
	}
}

func TestRuntimeMetricsDoNotChangePUBACKMarkRetry(t *testing.T) {
	repo := &retryMetricsRepository{
		id: uuid.New(), discoveries: make(chan struct{}, 16),
		dequeued: make(chan struct{}, 4), markAttempts: make(chan int, 4),
	}
	pub := &retryMetricsPublisher{
		entered: make(chan int, 4),
		acks:    []chan struct{}{make(chan struct{}), make(chan struct{})},
	}
	cfg := DefaultConfig()
	cfg.ScanInterval = 5 * time.Millisecond
	cfg.RetryBackoff = 5 * time.Millisecond
	m := &Metrics{}
	r, err := New(cfg, repo, pub, m)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := r.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	// Observe initial discovery, dequeue, and publish before releasing PUBACK.
	select {
	case <-repo.discoveries:
	case <-time.After(2 * time.Second):
		t.Fatal("initial discovery did not run")
	}
	select {
	case <-repo.dequeued:
	case <-time.After(2 * time.Second):
		t.Fatal("first job was not dequeued")
	}
	awaitRetryMetricEvent(t, pub.entered, 1)
	close(pub.acks[0])
	awaitRetryMetricEvent(t, repo.markAttempts, 1)

	// The ambiguous mark leaves the row pending. The next scan must rediscover
	// and dequeue it before the second PUBACK/mark can complete.
	select {
	case <-repo.discoveries:
	case <-time.After(2 * time.Second):
		t.Fatal("retry discovery did not run")
	}
	select {
	case <-repo.dequeued:
	case <-time.After(2 * time.Second):
		t.Fatal("retry job was not dequeued")
	}
	awaitRetryMetricEvent(t, pub.entered, 2)
	close(pub.acks[1])
	awaitRetryMetricEvent(t, repo.markAttempts, 2)

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer closeCancel()
	if err := r.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	s := m.Snapshot()
	pub.mu.Lock()
	calls := pub.calls
	pub.mu.Unlock()
	if calls != 2 || s.Publish.Count != 2 || s.PUBACK != 2 || s.QueueWait.Count != 2 || s.Discovery.Count < 2 || s.Prepare.Count != 2 || s.PublishedMark.Count != 2 || s.PublishedMarkSuccessfulCalls != 1 || s.MarkErrors != 1 || s.InFlight != 0 || s.LivePublishWorkers != 0 || s.QueueDepth != 0 || s.QueueCapacity != int64(cfg.QueueCapacity) || s.Enqueued != 2 || s.Scans < 2 {
		t.Fatal(s)
	}
}

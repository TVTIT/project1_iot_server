package mqttcontrol

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRuntimeMetricsDoNotChangePUBACKMarkRetry(t *testing.T) {
	repo := &testRepository{id: uuid.New()}
	pub := &testPublisher{ack: make(chan struct{}), ready: true}
	close(pub.ack)
	cfg := DefaultConfig()
	cfg.ScanInterval = time.Millisecond
	cfg.RetryBackoff = time.Millisecond
	m := &Metrics{}
	r, err := New(cfg, repo, pub, m)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		defer closeCancel()
		if err := r.Close(closeCtx); err != nil {
			t.Error(err)
		}
	})
	for {
		repo.mu.Lock()
		done := repo.marked
		repo.mu.Unlock()
		if done {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("dispatch not completed")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	r.Stop()
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	s := m.Snapshot()
	if s.Publish.Count != 2 || s.PUBACK != 2 || s.QueueWait.Count != 2 || s.Discovery.Count == 0 {
		t.Fatal(s)
	}
}

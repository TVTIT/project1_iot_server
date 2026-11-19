package mqttingress

import (
	"context"
	"sync"
	"testing"
	"time"
)

func schedulerItem(topic string) item { return item{topic: topic, payload: []byte(topic)} }

func acquireScheduler(t *testing.T, s *sensorScheduler) *sensorLease {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	l, err := s.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestSensorSchedulerExclusiveLeaseAndGatewayIdentity(t *testing.T) {
	s := newSensorScheduler(4, 1)
	if _, depth := s.Enqueue("g1", "same", schedulerItem("a")); depth != 1 {
		t.Fatal(depth)
	}
	first := acquireScheduler(t, s)
	if first.Item.topic != "a" {
		t.Fatal(first.Item.topic)
	}
	if _, depth := s.Enqueue("g1", "same", schedulerItem("b")); depth != 1 {
		t.Fatal(depth)
	}
	if _, depth := s.Enqueue("g2", "same", schedulerItem("c")); depth != 2 {
		t.Fatal(depth)
	}
	if _, err := s.TryAcquire(); err != errSchedulerNoReadySensor {
		t.Fatalf("second lease: %v", err)
	}
	first.Release()
	second := acquireScheduler(t, s)
	if second.GatewayID != "g2" || second.SensorID != "same" || second.Item.topic != "c" {
		t.Fatalf("distinct gateway lease = %+v", second)
	}
	second.Release()
	second = acquireScheduler(t, s)
	defer second.Release()
	if second.GatewayID != "g1" || second.SensorID != "same" || second.Item.topic != "b" {
		t.Fatalf("g1 lease = %+v", second)
	}
}

func TestSensorSchedulerReadyFIFOAndPerSensorFIFO(t *testing.T) {
	s := newSensorScheduler(8, 2)
	_, _ = s.Enqueue("g", "a", schedulerItem("a1"))
	_, _ = s.Enqueue("g", "a", schedulerItem("a2"))
	_, _ = s.Enqueue("g", "b", schedulerItem("b1"))
	a := acquireScheduler(t, s)
	if a.Item.topic != "a1" {
		t.Fatal(a.Item.topic)
	}
	if got, ok := a.Pop(); !ok || got.topic != "a2" {
		t.Fatalf("pop=%v,%v", got.topic, ok)
	}
	if _, depth := s.Enqueue("g", "a", schedulerItem("a3")); depth != 2 {
		t.Fatalf("depth %d", depth)
	}
	a.Release()
	b := acquireScheduler(t, s)
	if b.SensorID != "b" || b.Item.topic != "b1" {
		t.Fatalf("first ready after release: %+v", b)
	}
	b.Release()
	a = acquireScheduler(t, s)
	defer a.Release()
	if a.SensorID != "a" || a.Item.topic != "a3" {
		t.Fatalf("requeued sensor: %+v", a)
	}
}

func TestSensorSchedulerGlobalCapacityAndReclamation(t *testing.T) {
	s := newSensorScheduler(2, 1)
	if ok, depth := s.Enqueue("g", "a", schedulerItem("1")); !ok || depth != 1 {
		t.Fatal(ok, depth)
	}
	lease := acquireScheduler(t, s)
	if ok, depth := s.Enqueue("g", "a", schedulerItem("2")); !ok || depth != 1 {
		t.Fatal(ok, depth)
	}
	if ok, depth := s.Enqueue("g", "b", schedulerItem("3")); !ok || depth != 2 {
		t.Fatal(ok, depth)
	}
	if ok, depth := s.Enqueue("g", "c", schedulerItem("4")); ok || depth != 2 {
		t.Fatal(ok, depth)
	}
	if lease.PendingDepth() != 2 || s.Capacity() != 2 {
		t.Fatal(lease.PendingDepth(), s.Capacity())
	}
	lease.Release()
	lease = acquireScheduler(t, s)
	lease.Pop()
	lease.Release()
	if ok, depth := s.Enqueue("g", "c", schedulerItem("4")); !ok || depth != 2 {
		t.Fatal(ok, depth)
	}
}

func TestSensorSchedulerWakeOnEnqueueAndRelease(t *testing.T) {
	s := newSensorScheduler(3, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if ok, _ := s.Enqueue("g", "a", schedulerItem("1")); !ok {
		t.Fatal("enqueue")
	}
	first, err := s.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan *sensorLease, 1)
	go func() { l, _ := s.Acquire(ctx); done <- l }()
	if ok, _ := s.Enqueue("g", "b", schedulerItem("2")); !ok {
		t.Fatal("enqueue b")
	}
	select {
	case l := <-done:
		if l != nil {
			t.Fatal("acquired while all leases used")
		}
	case <-time.After(20 * time.Millisecond):
	}
	first.Release()
	select {
	case l := <-done:
		if l == nil || l.SensorID != "b" {
			t.Fatalf("release wake lease=%+v", l)
		}
		l.Release()
	case <-ctx.Done():
		t.Fatal("did not wake on release")
	}
}

func TestSensorSchedulerDiscardPendingReturnsOwnershipOnce(t *testing.T) {
	s := newSensorScheduler(4, 1)
	_, _ = s.Enqueue("g", "a", schedulerItem("a1"))
	l := acquireScheduler(t, s)
	_, _ = s.Enqueue("g", "a", schedulerItem("a2"))
	_, _ = s.Enqueue("g", "b", schedulerItem("b1"))
	dropped := s.DiscardPending()
	got := make(map[string]int, len(dropped))
	for _, it := range dropped {
		got[it.topic]++
	}
	if len(dropped) != 2 || got["a2"] != 1 || got["b1"] != 1 {
		t.Fatalf("discard=%v", dropped)
	}
	for _, it := range dropped {
		if it.payload == nil || string(it.payload) != it.topic {
			t.Fatalf("payload ownership lost or duplicated for %q", it.topic)
		}
	}
	if again := s.DiscardPending(); len(again) != 0 {
		t.Fatalf("discarded twice: %d", len(again))
	}
	if depth := s.PendingDepth(); depth != 0 {
		t.Fatal(depth)
	}
	l.Release()
}

func TestSensorSchedulerConcurrentAdmissionAndLease(t *testing.T) {
	s := newSensorScheduler(32, 4)
	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for k := 0; k < 8; k++ {
				s.Enqueue("g", string(rune('a'+n)), schedulerItem("x"))
			}
		}(n)
	}
	wg.Wait()
	var workers sync.WaitGroup
	for n := 0; n < 4; n++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				l, err := s.Acquire(ctx)
				cancel()
				if err != nil {
					return
				}
				l.Pop()
				l.Release()
			}
		}()
	}
	workers.Wait()
	if got := s.PendingDepth(); got != 0 {
		t.Fatalf("pending=%d", got)
	}
}

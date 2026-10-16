package mqttingress

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

func TestDrainIdleStopsAdmission(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	ing := newIngress(t, defaultOptions(), newValidator(t), repo, pub)
	mustStart(t, ing)

	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain (idle) = %v, want nil", err)
	}
	topic, payload, receivedAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 1)
	if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionRejectedStopping {
		t.Fatalf("Admit after Drain = %v, want rejected stopping", got)
	}
	if repo.count() != 0 || len(pub.deliveries()) != 0 {
		t.Fatalf("work happened after an idle drain")
	}
}

func TestDrainProcessesQueuedItems(t *testing.T) {
	repo := &fakeRepo{enter: make(chan *telemetry.Message, 4), release: make(chan struct{})}
	pub := &fakePublisher{signal: make(chan Delivery, 4)}
	ing := newIngress(t, defaultOptions(), newValidator(t), repo, pub)
	mustStart(t, ing)

	topic, payload, measuredAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 2)
	if got := ing.Admit(topic, payload, measuredAt, false); got != AdmissionAccepted {
		t.Fatalf("Admit = %v, want accepted", got)
	}
	waitMessage(t, repo.enter)
	close(repo.release)

	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain = %v, want nil", err)
	}
	if got := len(pub.deliveries()); got != 1 {
		t.Fatalf("ACKs after drain = %d, want 1", got)
	}
}

func TestDrainForcedCancelBlockedRepository(t *testing.T) {
	// release is never closed, so Persist can only return via its context.
	repo := &fakeRepo{enter: make(chan *telemetry.Message, 4), release: make(chan struct{})}
	pub := &fakePublisher{}
	ing := newIngress(t, defaultOptions(), newValidator(t), repo, pub)
	mustStart(t, ing)

	topic, payload, receivedAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 1)
	if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionAccepted {
		t.Fatalf("Admit = %v, want accepted", got)
	}
	waitMessage(t, repo.enter)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := ing.Drain(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain (blocked repo) = %v, want context.DeadlineExceeded", err)
	}
	if len(pub.deliveries()) != 0 {
		t.Fatalf("ACK published although Persist never confirmed")
	}
	// Workers exited: admission stays closed and a second drain returns at once.
	if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionRejectedStopping {
		t.Fatalf("Admit after forced cancel = %v, want rejected stopping", got)
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("second Drain = %v, want nil", err)
	}
}

func TestDrainForcedCancelBlockedPublisher(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{onPublish: func(ctx context.Context, _ Delivery) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	ing := newIngress(t, defaultOptions(), newValidator(t), repo, pub)
	mustStart(t, ing)

	topic, payload, receivedAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 1)
	if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionAccepted {
		t.Fatalf("Admit = %v, want accepted", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := ing.Drain(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain (blocked publisher) = %v, want context.DeadlineExceeded", err)
	}
	snap := ing.Snapshot()
	if snap.Committed != 1 {
		t.Fatalf("Committed = %d, want 1 (DB result must be kept)", snap.Committed)
	}
	if snap.ACKPublishFailures != 1 || snap.ACKPublished != 0 {
		t.Fatalf("ack counters = published %d failures %d, want 0/1", snap.ACKPublished, snap.ACKPublishFailures)
	}
}

func TestCountersSnapshot(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	opts := defaultOptions()
	ing := newIngress(t, opts, newValidator(t), repo, pub)
	mustStart(t, ing)

	topic, payload, receivedAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 3)
	if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionAccepted {
		t.Fatalf("Admit = %v, want accepted", got)
	}
	if got := ing.Admit(topic, payload, receivedAt, true); got != AdmissionRejectedRetained {
		t.Fatalf("retained Admit = %v, want rejected", got)
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain = %v", err)
	}

	snap := ing.Snapshot()
	if snap.Accepted != 1 || snap.RejectedRetained != 1 {
		t.Fatalf("admission counters = %+v", snap)
	}
	if snap.Committed != 1 || snap.SamplesNew != 3 || snap.ACKPublished != 1 {
		t.Fatalf("commit counters = %+v", snap)
	}
	if snap.ProcessingCount != 1 || snap.ProcessingLatencyTotal < 0 {
		t.Fatalf("latency counters = %+v", snap)
	}
	if snap.QueueCapacity != opts.QueueCapacity {
		t.Fatalf("QueueCapacity = %d, want %d", snap.QueueCapacity, opts.QueueCapacity)
	}
	if snap.QueueDepth != 0 {
		t.Fatalf("QueueDepth after Drain = %d, want 0", snap.QueueDepth)
	}
	if snap.QueueHighWater < 0 || snap.QueueHighWater > opts.QueueCapacity {
		t.Fatalf("QueueHighWater = %d, want within [0, %d]", snap.QueueHighWater, opts.QueueCapacity)
	}
}

func TestQueueHighWaterBacklogAndDrain(t *testing.T) {
	opts := defaultOptions()
	opts.WorkerCount = 1
	opts.QueueCapacity = 3

	enter := make(chan *telemetry.Message, 16)
	release := make(chan struct{})
	repo := &fakeRepo{enter: enter, release: release}
	pub := &fakePublisher{}

	ing := newIngress(t, opts, newValidator(t), repo, pub)
	mustStart(t, ing)

	initSnap := ing.Snapshot()
	if initSnap.QueueDepth != 0 || initSnap.QueueHighWater != 0 {
		t.Fatalf("initial snapshot queue state = depth %d, high_water %d; want 0/0",
			initSnap.QueueDepth, initSnap.QueueHighWater)
	}

	// Message 0: taken by the 1 active worker, which blocks in fakeRepo.Persist on release channel.
	m0ID := uuid.New()
	t0, p0, r0 := testPayloadFor(t, "gateway_001", "sensor_001", m0ID, testBootID, 1)
	if got := ing.Admit(t0, p0, r0, false); got != AdmissionAccepted {
		t.Fatalf("Admit message 0 = %v, want accepted", got)
	}
	waitMessage(t, enter) // worker has popped item 0 and is blocked in Persist

	// At this point, the worker is occupied and queue channel is empty.
	// Now admit items 1, 2, 3 into the queue channel (capacity = 3).
	// Because the worker is blocked, items accumulate in the channel.
	for i := 1; i <= 3; i++ {
		miID := uuid.New()
		ti, pi, ri := testPayloadFor(t, "gateway_001", "sensor_001", miID, testBootID, 1)
		if got := ing.Admit(ti, pi, ri, false); got != AdmissionAccepted {
			t.Fatalf("Admit message %d = %v, want accepted", i, got)
		}
		snap := ing.Snapshot()
		if snap.QueueDepth != i {
			t.Fatalf("after admitting %d queued items, QueueDepth = %d, want %d", i, snap.QueueDepth, i)
		}
		if snap.QueueHighWater < i {
			t.Fatalf("after admitting %d queued items, QueueHighWater = %d, want >= %d", i, snap.QueueHighWater, i)
		}
		if snap.QueueHighWater > opts.QueueCapacity {
			t.Fatalf("QueueHighWater = %d exceeded capacity %d", snap.QueueHighWater, opts.QueueCapacity)
		}
	}

	snapFull := ing.Snapshot()
	if snapFull.QueueDepth != opts.QueueCapacity {
		t.Fatalf("QueueDepth at full capacity = %d, want %d", snapFull.QueueDepth, opts.QueueCapacity)
	}
	if snapFull.QueueHighWater != opts.QueueCapacity {
		t.Fatalf("QueueHighWater at full capacity = %d, want %d", snapFull.QueueHighWater, opts.QueueCapacity)
	}

	// Message 4: queue is full; admission must be rejected and high-water must not exceed capacity.
	m4ID := uuid.New()
	t4, p4, r4 := testPayloadFor(t, "gateway_001", "sensor_001", m4ID, testBootID, 1)
	if got := ing.Admit(t4, p4, r4, false); got != AdmissionRejectedQueueFull {
		t.Fatalf("Admit on full queue = %v, want queue full", got)
	}

	snapOver := ing.Snapshot()
	if snapOver.QueueFull != 1 {
		t.Fatalf("QueueFull counter = %d, want 1", snapOver.QueueFull)
	}
	if snapOver.QueueHighWater != opts.QueueCapacity {
		t.Fatalf("QueueHighWater after rejection = %d, want %d", snapOver.QueueHighWater, opts.QueueCapacity)
	}

	// Release worker and drain the backlog.
	close(release)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain = %v", err)
	}

	// Post-drain: queue is empty, but QueueHighWater remains at peak (never decreases).
	snapDrained := ing.Snapshot()
	if snapDrained.QueueDepth != 0 {
		t.Fatalf("post-drain QueueDepth = %d, want 0", snapDrained.QueueDepth)
	}
	if snapDrained.QueueHighWater != opts.QueueCapacity {
		t.Fatalf("post-drain QueueHighWater = %d, want %d (must persist after drain)",
			snapDrained.QueueHighWater, opts.QueueCapacity)
	}
	if snapDrained.Committed != 4 {
		t.Fatalf("post-drain Committed = %d, want 4 (items 0,1,2,3)", snapDrained.Committed)
	}
}

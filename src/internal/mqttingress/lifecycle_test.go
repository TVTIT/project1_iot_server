package mqttingress

import (
	"context"
	"errors"
	"testing"
	"time"

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
}

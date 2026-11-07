package mqttingress

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

func sensorLifecyclePayload(t *testing.T, gateway, sensor string) (string, []byte, time.Time) {
	t.Helper()
	return testPayloadFor(t, gateway, sensor, uuid.New(), testBootID, 1)
}

func sensorLifecycleAdmit(t *testing.T, ing *Ingress, gateway, sensor string) []byte {
	t.Helper()
	topic, payload, received := sensorLifecyclePayload(t, gateway, sensor)
	if got := ing.Admit(topic, payload, received, false); got != AdmissionAccepted {
		t.Fatalf("Admit(%s/%s) = %v", gateway, sensor, got)
	}
	return payload
}

func awaitBatchCall(t *testing.T, calls <-chan sensorBatchCall, gateway, sensor string) sensorBatchCall {
	t.Helper()
	select {
	case call := <-calls:
		if !sensorCallOnly(call, gateway, sensor) {
			t.Fatalf("PersistBatch identities = %+v; want only %s/%s", call.identities, gateway, sensor)
		}
		return call
	case <-time.After(sensorSchedulingWait):
		t.Fatalf("timed out waiting for PersistBatch %s/%s", gateway, sensor)
		return sensorBatchCall{}
	}
}

func assertSensorSchedulerReclaimed(t *testing.T, ing *Ingress) {
	t.Helper()
	s := ing.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending != 0 || s.active != 0 || len(s.queues) != 0 || len(s.ready) != 0 {
		t.Fatalf("scheduler not reclaimed: pending=%d active=%d keys=%d ready=%d", s.pending, s.active, len(s.queues), len(s.ready))
	}
}

func TestMicrobatchSensorLifecycleGlobalCapacityAndBytes(t *testing.T) {
	for _, metricsEnabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("metrics_%t", metricsEnabled), func(t *testing.T) {
			opts := microOptions()
			opts.MetricsEnabled = metricsEnabled
			opts.WorkerCount, opts.QueueCapacity, opts.Microbatch.MaxMessages = 2, 2, 1
			entered, release := make(chan string, 2), make(chan struct{})
			var once sync.Map
			pub := &fakePublisher{onPublish: func(ctx context.Context, d Delivery) error {
				if strings.Contains(d.Topic, "/acks/") {
					if _, loaded := once.LoadOrStore(d.Topic, struct{}{}); !loaded {
						entered <- d.Topic
					}
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			}}
			calls := make(chan sensorBatchCall, 8)
			batch := batchRepoFunc(func(_ context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
				ids := make([]telemetry.MessageIdentity, len(messages))
				for n, m := range messages {
					ids[n] = identityOf(m)
				}
				calls <- sensorBatchCall{identities: ids}
				return sensorTestBatch(messages)
			})
			ing := startSensorTestIngress(t, opts, newValidator(t), batch, pub)
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			var admittedBytes int64
			for _, sensor := range []string{"a", "b"} {
				admittedBytes += int64(len(sensorLifecycleAdmit(t, ing, "gateway_001", sensor)))
				awaitBatchCall(t, calls, "gateway_001", sensor)
			}
			for range 2 {
				select {
				case <-entered:
				case <-time.After(sensorSchedulingWait):
					t.Fatal("ACK did not block both Sensor leases")
				}
			}
			for _, sensor := range []string{"a", "b"} {
				admittedBytes += int64(len(sensorLifecycleAdmit(t, ing, "gateway_001", sensor)))
			}
			snap := ing.Snapshot()
			if snap.QueueDepth != 2 || snap.QueueCapacity != 2 || snap.QueueHighWater > 2 {
				t.Fatalf("aggregate queue bounds = depth %d capacity %d highwater %d", snap.QueueDepth, snap.QueueCapacity, snap.QueueHighWater)
			}
			if got := ing.Admit("gateways/gateway_001/telemetry/sensor_c", []byte("rejected"), time.Now(), false); got != AdmissionRejectedQueueFull {
				t.Fatalf("third Sensor admission = %v, want QueueFull", got)
			}
			snap = ing.Snapshot()
			if snap.QueueDepth != 2 || snap.BufferedPayloadBytes != admittedBytes {
				t.Fatalf("after rejection: depth=%d bytes=%d want depth=2 bytes=%d", snap.QueueDepth, snap.BufferedPayloadBytes, admittedBytes)
			}
			close(release)
			ctx, cancel := context.WithTimeout(context.Background(), sensorSchedulingWait)
			defer cancel()
			if err := ing.Drain(ctx); err != nil {
				t.Fatalf("Drain: %v", err)
			}
			if got := ing.Snapshot().BufferedPayloadBytes; got != 0 {
				t.Fatalf("bytes after drain=%d", got)
			}
			assertSensorSchedulerReclaimed(t, ing)
		})
	}
}

func TestMicrobatchSensorLifecycleStopAdmissionDrainsLeasedQueue(t *testing.T) {
	opts := microOptions()
	opts.WorkerCount, opts.QueueCapacity, opts.Microbatch.MaxMessages = 3, 4, 1
	entered, release := make(chan struct{}, 1), make(chan struct{})
	calls := make(chan sensorBatchCall, 4)
	batch := batchRepoFunc(func(ctx context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
		ids := make([]telemetry.MessageIdentity, len(messages))
		for n, m := range messages {
			ids[n] = identityOf(m)
		}
		calls <- sensorBatchCall{identities: ids}
		select {
		case entered <- struct{}{}:
		default:
		}
		// The first repository call is deliberately held; context cancellation
		// remains honored so test cleanup cannot strand a worker.
		select {
		case <-release:
		case <-ctx.Done():
			return nil
		}
		return sensorTestBatch(messages)
	})
	// Use one worker for the active call while extra idle workers exercise the
	// scheduler's stop wakeup; the same Sensor's second item remains leased.
	// WorkerCount is raised to three as required, but the first call is gated by
	// a validator before any other worker can consume the same-Sensor queue.
	gate := &firstValidationGate{Validator: newValidator(t), entered: make(chan struct{}), release: make(chan struct{})}
	ing := startSensorTestIngress(t, opts, gate, batch, &fakePublisher{})
	t.Cleanup(func() {
		gate.unblock()
		select {
		case <-release:
		default:
			close(release)
		}
	})
	sensorLifecycleAdmit(t, ing, "gateway_001", "a")
	waitSensorSignal(t, gate.entered, "validation barrier")
	sensorLifecycleAdmit(t, ing, "gateway_001", "a")
	gate.unblock()
	waitSensorSignal(t, entered, "blocked repository")
	// Drain after a barriered active write; an independent worker must not
	// discard the pending item merely because it has no lease to acquire.
	ing.StopAdmission()
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), sensorSchedulingWait)
	defer cancel()
	if err := ing.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if got := ing.Snapshot(); got.BufferedPayloadBytes != 0 || got.Committed != 2 || got.ACKPublished != 2 || got.InFlight != 0 {
		t.Fatalf("post-drain snapshot = %+v", got)
	}
	assertSensorSchedulerReclaimed(t, ing)
}

func TestMicrobatchSensorLifecycleForcedDrainJoinsBlockedWorkers(t *testing.T) {
	for _, phase := range []string{"repository", "ack"} {
		t.Run(phase, func(t *testing.T) {
			opts := microOptions()
			opts.WorkerCount, opts.QueueCapacity, opts.Microbatch.MaxMessages = 2, 4, 1
			entered, release := make(chan string, 4), make(chan struct{})
			batch := batchRepoFunc(func(ctx context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
				if phase == "repository" {
					select {
					case entered <- messages[0].Topic().SensorID:
					case <-ctx.Done():
						return nil
					}
					select {
					case <-release:
					case <-ctx.Done():
						return nil
					}
				}
				return sensorTestBatch(messages)
			})
			pub := &fakePublisher{onPublish: func(ctx context.Context, d Delivery) error {
				if phase == "ack" && strings.Contains(d.Topic, "/acks/") {
					select {
					case entered <- d.Topic:
					case <-ctx.Done():
						return ctx.Err()
					}
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			}}
			calls := make(chan sensorBatchCall, 4)
			wrapped := batchRepoFunc(func(ctx context.Context, ms []*telemetry.Message) []telemetry.BatchPersistResult {
				calls <- sensorBatchCall{identities: []telemetry.MessageIdentity{identityOf(ms[0])}}
				return batch.PersistBatch(ctx, ms)
			})
			ing := startSensorTestIngress(t, opts, newValidator(t), wrapped, pub)
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			for _, sensor := range []string{"a", "b"} {
				sensorLifecycleAdmit(t, ing, "gateway_001", sensor)
			}
			seen := map[string]bool{}
			for range 2 {
				select {
				case value := <-entered:
					if phase == "repository" {
						seen[value] = true
					}
				case <-time.After(sensorSchedulingWait):
					t.Fatalf("only %d Sensors reached blocked %s", len(seen), phase)
				}
			}
			if phase == "repository" && (!seen["a"] || !seen["b"]) {
				t.Fatalf("blocked repository Sensors=%v", seen)
			}
			sensorLifecycleAdmit(t, ing, "gateway_001", "queued")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			err := ing.Drain(ctx)
			cancel()
			if err == nil {
				t.Fatal("Drain succeeded despite blocked context-aware operations")
			}
			close(release)
			snap := ing.Snapshot()
			if snap.BufferedPayloadBytes != 0 || snap.QueueDepth != 0 || snap.InFlight != 0 {
				t.Fatalf("forced-drain state not reclaimed: %+v", snap)
			}
			if phase == "repository" && (snap.Committed != 0 || snap.ACKPublished != 0) {
				t.Fatalf("unconfirmed repository work was counted: committed=%d ACK=%d", snap.Committed, snap.ACKPublished)
			}
			if phase == "ack" && (snap.Committed != 2 || snap.ACKPublished != 0) {
				t.Fatalf("confirmed commits lost across canceled ACK: committed=%d ACK=%d", snap.Committed, snap.ACKPublished)
			}
			assertSensorSchedulerReclaimed(t, ing)
		})
	}
}

func TestMicrobatchSensorLifecycleCarryGetsFreshTimerAndWaitingSensorTurn(t *testing.T) {
	gate := &firstValidationGate{Validator: newValidator(t), entered: make(chan struct{}), release: make(chan struct{})}
	clock := &controlledClock{timers: make(chan *controlledTimer, 8)}
	calls := make(chan sensorBatchCall, 8)
	batch := batchRepoFunc(func(ctx context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
		ids := make([]telemetry.MessageIdentity, len(messages))
		for n, m := range messages {
			ids[n] = identityOf(m)
		}
		select {
		case calls <- sensorBatchCall{identities: ids}:
		case <-ctx.Done():
			return nil
		}
		return sensorTestBatch(messages)
	})
	opts := microOptions()
	opts.WorkerCount, opts.QueueCapacity = 1, 8
	opts.Microbatch.MaxRows = 3
	opts.Microbatch.MaxMessages = 8
	ing := startSensorTestIngress(t, opts, gate, batch, &fakePublisher{})
	ing.setClock(clock)
	t.Cleanup(gate.unblock)
	admitRows := func(sensor string, rows int) uuid.UUID {
		id := uuid.New()
		topic, payload, received := testPayloadFor(t, "gateway_001", sensor, id, testBootID, rows)
		if got := ing.Admit(topic, payload, received, false); got != AdmissionAccepted {
			t.Fatalf("Admit(%s)=%v", sensor, got)
		}
		return id
	}
	a1 := admitRows("a", 2)
	waitSensorSignal(t, gate.entered, "first validation")
	a2 := admitRows("a", 2)
	a3 := admitRows("a", 1)
	b1 := admitRows("b", 1)
	gate.unblock()
	first := awaitBatchCall(t, calls, "gateway_001", "a")
	if len(first.identities) != 1 || first.identities[0].MessageID != a1 {
		t.Fatalf("row-boundary first group=%+v", first.identities)
	}
	// A2 is the validated carry. The carry turn must start its own timer rather
	// than inherit A1's already-expired/full-group wait.
	// The first timer belongs to A1's collection window and was stopped at the
	// row boundary; the next timer belongs to A2's carry turn.
	var firstTimer, carryTimer *controlledTimer
	select {
	case firstTimer = <-clock.timers:
	case <-time.After(sensorSchedulingWait):
		t.Fatal("A1 timer was not created")
	}
	select {
	case carryTimer = <-clock.timers:
	case <-time.After(sensorSchedulingWait):
		t.Fatal("carry did not create a fresh timer")
	}
	if firstTimer == carryTimer {
		t.Fatal("carry reused its prior timer")
	}
	carryTimer.ch <- time.Now()
	second := awaitBatchCall(t, calls, "gateway_001", "a")
	if len(second.identities) != 1 || second.identities[0].MessageID != a2 {
		t.Fatalf("carry group=%+v", second.identities)
	}
	// B is also below its batch limits; fire its own wait timer so the fairness
	// assertion measures scheduling order rather than an untriggered test clock.
	var waitingSensorTimer *controlledTimer
	select {
	case waitingSensorTimer = <-clock.timers:
	case <-time.After(sensorSchedulingWait):
		t.Fatal("waiting Sensor did not start its batch timer")
	}
	waitingSensorTimer.ch <- time.Now()
	third := awaitBatchCall(t, calls, "gateway_001", "b")
	if len(third.identities) != 1 || third.identities[0].MessageID != b1 {
		t.Fatalf("waiting Sensor turn=%+v", third.identities)
	}
	ctx, cancel := context.WithTimeout(context.Background(), sensorSchedulingWait)
	defer cancel()
	if err := ing.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	last := awaitBatchCall(t, calls, "gateway_001", "a")
	if len(last.identities) != 1 || last.identities[0].MessageID != a3 {
		t.Fatalf("third A group=%+v", last.identities)
	}
	snap := ing.Snapshot()
	if snap.BufferedPayloadBytes != 0 || snap.Committed != 4 || snap.ACKPublished != 4 {
		t.Fatalf("post-drain snapshot=%+v", snap)
	}
	assertSensorSchedulerReclaimed(t, ing)
}

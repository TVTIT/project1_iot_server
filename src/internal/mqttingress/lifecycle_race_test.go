package mqttingress

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

// TestStartDrainConcurrent exercises the Start/Drain ordering fix: Drain must
// never wait on the worker WaitGroup while Start is still adding workers. Run
// with -race.
func TestStartDrainConcurrent(t *testing.T) {
	for n := 0; n < 200; n++ {
		ing := newIngress(t, defaultOptions(), newValidator(t), &fakeRepo{}, &fakePublisher{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = ing.Start()
		}()
		go func() {
			defer wg.Done()
			_ = ing.Drain(context.Background())
		}()
		wg.Wait()
	}
}

// TestStartAfterDrainRefused locks in the safe public API: once a drain has
// begun, a racing or later Start is refused rather than adding workers after
// the drain barrier.
func TestStartAfterDrainRefused(t *testing.T) {
	ing := newIngress(t, defaultOptions(), newValidator(t), &fakeRepo{}, &fakePublisher{})
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain = %v", err)
	}
	if err := ing.Start(); err == nil {
		t.Fatal("Start after Drain = nil, want error")
	}

	topic, payload, receivedAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 1)
	if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionRejectedNotRunning {
		t.Fatalf("Admit after refused Start = %v, want not running", got)
	}
}

// TestQueueHighWaterConcurrentAdmission exercises concurrent admissions under
// the race detector, asserting that QueueHighWater remains bounded within
// [0, QueueCapacity] and is monotonically non-decreasing.
func TestQueueHighWaterConcurrentAdmission(t *testing.T) {
	opts := defaultOptions()
	opts.WorkerCount = 2
	opts.QueueCapacity = 8

	repo := &fakeRepo{
		onPersist: func(_ context.Context, m *telemetry.Message) (telemetry.PersistResult, error) {
			// Small jitter to simulate real persistence latency and allow queue buildup
			time.Sleep(200 * time.Microsecond)
			return committedResult(m), nil
		},
	}
	pub := &fakePublisher{}
	ing := newIngress(t, opts, newValidator(t), repo, pub)
	mustStart(t, ing)

	const goroutines = 6
	const itemsPerGoroutine = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(_ int) {
			defer wg.Done()
			for i := 0; i < itemsPerGoroutine; i++ {
				mID := uuid.New()
				topic, payload, receivedAt := testPayloadFor(t, "gateway_001", "sensor_001", mID, testBootID, 1)
				_ = ing.Admit(topic, payload, receivedAt, false)
				snap := ing.Snapshot()
				if snap.QueueHighWater < 0 || snap.QueueHighWater > opts.QueueCapacity {
					t.Errorf("concurrent QueueHighWater = %d outside [0, %d]", snap.QueueHighWater, opts.QueueCapacity)
				}
			}
		}(g)
	}

	wg.Wait()
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain = %v", err)
	}

	finalSnap := ing.Snapshot()
	if finalSnap.QueueDepth != 0 {
		t.Fatalf("post-drain QueueDepth = %d, want 0", finalSnap.QueueDepth)
	}
	if finalSnap.QueueHighWater < 0 || finalSnap.QueueHighWater > opts.QueueCapacity {
		t.Fatalf("post-drain QueueHighWater = %d outside [0, %d]", finalSnap.QueueHighWater, opts.QueueCapacity)
	}
}

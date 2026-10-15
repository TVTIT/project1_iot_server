package mqttingress

import (
	"context"
	"sync"
	"testing"
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

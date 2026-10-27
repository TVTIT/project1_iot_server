package telemetryws

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"iot-platform/internal/telemetry"

	"github.com/google/uuid"
)

func eventBatch(n int) telemetry.BatchDTO {
	return telemetry.BatchDTO{ProtocolVersion: 1, MessageID: uuid.New(), BootID: uuid.New(), GatewayID: "gw1", SensorID: "s1", FirstSequence: 1, SampleCount: int32(n), MeasuredAt: time.Now().UTC().Format(time.RFC3339Nano), SampleIntervalUS: 1_000_000, Samples: make([]float64, n)}
}

func TestEventRuntime_EncodesOnceAndRoutesSharedBytes(t *testing.T) {
	h := NewHub(HubConfig{})
	defer func() { _ = h.Close() }()
	qs := make([]*SessionQueue, 2)
	for i := range qs {
		state, q := createTestSession(t, h, "session-"+string(rune('a'+i)))
		qs[i] = q
		gen, _ := state.PrepareSubscribe([]TargetKey{{GatewayID: "gw1"}}, time.Now())
		if _, err := h.CommitSubscribe(state.SessionID(), gen, []AuthorizedTarget{{Target: TargetKey{GatewayID: "gw1"}}}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	encoded := 0
	rt := NewEventRuntime(h, EventRuntimeConfig{QueueSlots: 2, MaxPendingBytes: 1 << 20}, func(dto TelemetryEventDTO, max int) (*CommittedBatchEvent, error) {
		mu.Lock()
		encoded++
		mu.Unlock()
		return EncodeTelemetryEvent(dto, max)
	})
	if rt == nil {
		t.Fatal("runtime")
	}
	b := eventBatch(2)
	if !rt.TrySubmitBatch(b, time.Now()) {
		t.Fatal("submit rejected")
	}
	waitFor(t, func() bool { return qs[0].QueuedSlots() == 1 && qs[1].QueuedSlots() == 1 })
	f1, _ := qs[0].Dequeue()
	f2, _ := qs[1].Dequeue()
	if f1 == nil || f2 == nil || &f1.Payload()[0] != &f2.Payload()[0] {
		t.Fatal("payload was not shared")
	}
	f1.Release()
	f2.Release()
	mu.Lock()
	defer mu.Unlock()
	if encoded != 1 {
		t.Fatalf("encoded %d times", encoded)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestEventRuntime_ReservationsCoverInFlightAndRejectWithoutBlocking(t *testing.T) {
	h := NewHub(HubConfig{})
	defer func() { _ = h.Close() }()
	entered := make(chan struct{})
	release := make(chan struct{})
	rt := NewEventRuntime(h, EventRuntimeConfig{QueueSlots: 1, MaxPendingBytes: 4096}, func(_ TelemetryEventDTO, _ int) (*CommittedBatchEvent, error) {
		close(entered)
		<-release
		return nil, errors.New("encode")
	})
	if !rt.TrySubmitBatch(eventBatch(1), time.Now()) {
		t.Fatal("first rejected")
	}
	<-entered
	if rt.TrySubmitBatch(eventBatch(1), time.Now()) {
		t.Fatal("accepted beyond in-flight slot")
	}
	close(release)
	waitFor(t, func() bool { return rt.PendingSlots() == 0 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestEventRuntime_OversizeAndNoopNil(t *testing.T) {
	if NewEventRuntime(nil, EventRuntimeConfig{}, nil) != nil {
		t.Fatal("nil hub runtime must be no-op")
	}
	h := NewHub(HubConfig{})
	defer func() { _ = h.Close() }()
	rt := NewEventRuntime(h, EventRuntimeConfig{QueueSlots: 1, MaxPendingBytes: 4096, MaxFrameBytes: 32}, nil)
	b := eventBatch(1)
	if !rt.TrySubmitBatch(b, time.Now()) {
		t.Fatal("bounded snapshot admission rejected")
	}
	waitFor(t, func() bool { return rt.PendingSlots() == 0 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition timed out")
}

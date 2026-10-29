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

func TestEventRuntime_TargetBuildFailureDoesNotCloseIndependentNamespace(t *testing.T) {
	for _, tc := range []struct {
		name     string
		maxFrame int
		encoder  EventEncoder
	}{
		{name: "encoder error", encoder: func(TelemetryEventDTO, int) (*CommittedBatchEvent, error) { return nil, errors.New("encode") }},
		{name: "encoder nil", encoder: func(TelemetryEventDTO, int) (*CommittedBatchEvent, error) { return nil, nil }},
		{name: "oversize", maxFrame: 512, encoder: func(dto TelemetryEventDTO, max int) (*CommittedBatchEvent, error) {
			return &CommittedBatchEvent{DTO: dto, Raw: make([]byte, max+1)}, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHub(HubConfig{})
			defer func() { _ = h.Close() }()
			queues := make(map[string]*SessionQueue)
			for _, item := range []struct {
				id      string
				targets []TargetKey
			}{
				{id: "A-gateway", targets: []TargetKey{{GatewayID: "gwA"}}},
				{id: "A-sensor", targets: []TargetKey{{GatewayID: "gwA", SensorID: "sensorA"}}},
				{id: "A-overlap", targets: []TargetKey{{GatewayID: "gwA"}, {GatewayID: "gwA", SensorID: "sensorA"}}},
				{id: "B", targets: []TargetKey{{GatewayID: "gwB"}}},
			} {
				state, q := createTestSession(t, h, item.id)
				queues[item.id] = q
				gen, err := state.PrepareSubscribe(item.targets, time.Now())
				if err != nil {
					t.Fatal(err)
				}
				authorized := make([]AuthorizedTarget, 0, len(item.targets))
				for _, target := range item.targets {
					authorized = append(authorized, AuthorizedTarget{Target: target})
				}
				if _, err := h.CommitSubscribe(state.SessionID(), gen, authorized, time.Now()); err != nil {
					t.Fatal(err)
				}
			}

			encoder := tc.encoder
			if encoder == nil {
				encoder = EncodeTelemetryEvent
			}
			maxFrame := tc.maxFrame
			rt := NewEventRuntime(h, EventRuntimeConfig{QueueSlots: 2, MaxPendingBytes: 1 << 20, MaxFrameBytes: maxFrame}, func(dto TelemetryEventDTO, max int) (*CommittedBatchEvent, error) {
				if dto.GatewayID == "gwA" {
					return encoder(dto, max)
				}
				return EncodeTelemetryEvent(dto, max)
			})

			bad := eventBatch(1)
			bad.GatewayID = "gwA"
			bad.SensorID = "sensorA"
			if !rt.TrySubmitBatch(bad, time.Now()) {
				t.Fatal("admitted committed batch rejected")
			}
			waitFor(t, func() bool { return rt.PendingSlots() == 0 })
			if got := rt.PendingBytes(); got != 0 {
				t.Fatalf("failed event reservation leaked %d bytes", got)
			}
			if h.SessionCount() != 1 || queues["B"].IsClosed() {
				t.Fatalf("target failure closed unrelated namespace: sessions=%d B_closed=%v", h.SessionCount(), queues["B"].IsClosed())
			}
			for _, id := range []string{"A-gateway", "A-sensor", "A-overlap"} {
				if !queues[id].IsClosed() {
					t.Errorf("target session %s remained open", id)
				}
			}

			good := eventBatch(1)
			good.GatewayID = "gwB"
			good.SensorID = "sensorB"
			if !rt.TrySubmitBatch(good, time.Now()) {
				t.Fatal("healthy namespace batch rejected")
			}
			waitFor(t, func() bool { return queues["B"].QueuedSlots() == 1 })
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := rt.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if slots, bytes := rt.PendingSlots(), rt.PendingBytes(); slots != 0 || bytes != 0 {
				t.Fatalf("shutdown leaked reservations: slots=%d bytes=%d", slots, bytes)
			}
		})
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

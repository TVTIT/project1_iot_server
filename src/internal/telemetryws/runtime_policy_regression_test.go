package telemetryws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
)

func TestSessionQueueSlotsIncludeTelemetryAndReservedControls(t *testing.T) {
	q := NewSessionQueue("slot-policy", 1, DefaultSessionQueueBytes, DefaultMaxEventBytes)
	defer func() { _ = q.Close() }()
	event := createTestEvent(t, "gw-1", "sensor-1", 8)
	if err := q.Enqueue(event); err != nil {
		t.Fatal(err)
	}
	frame, err := q.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = q.ReserveControl(1, 16); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("control admitted above total slot cap with telemetry in flight: %v", err)
	}
	if err = q.Enqueue(event); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("telemetry admitted above total slot cap with telemetry in flight: %v", err)
	}
	frame.Release()
	frame.Release()
	reservation, err := q.ReserveControl(1, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Enqueue(event); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("telemetry admitted above total slot cap with control reserved: %v", err)
	}
	reservation.Release()
	reservation.Release()
	if err = q.Enqueue(event); err != nil {
		t.Fatalf("slot not released exactly once: %v", err)
	}
	queued, err := q.Dequeue()
	if err != nil || queued == nil {
		t.Fatalf("dequeue after release: frame=%v err=%v", queued, err)
	}
	queued.Release()
}

func TestSessionQueueMixedControlTelemetrySlotsRelease(t *testing.T) {
	q := NewSessionQueue("mixed-slot-policy", 2, DefaultSessionQueueBytes, DefaultMaxEventBytes)
	defer func() { _ = q.Close() }()
	event := createTestEvent(t, "gw-1", "sensor-1", 8)
	if err := q.Enqueue(event); err != nil {
		t.Fatal(err)
	}
	control, err := q.ReserveControl(1, 16)
	if err != nil {
		t.Fatalf("one telemetry + one control should fit two slots: %v", err)
	}
	if err = q.Enqueue(event); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("third app-work item exceeded two total slots: %v", err)
	}
	control.Release()
	control.Release()
	if err = q.Enqueue(event); err != nil {
		t.Fatalf("control release did not return its slot: %v", err)
	}
	frame, err := q.Dequeue()
	if err != nil || frame == nil {
		t.Fatalf("dequeue telemetry: frame=%v err=%v", frame, err)
	}
	frame.Release()
	frame.Release()
	reservation, err := q.ReserveControl(1, 16)
	if err != nil {
		t.Fatalf("telemetry release did not return its slot: %v", err)
	}
	reservation.Release()
}

func TestMetricsSnapshotIncludesReservedControlBytes(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	defer func() {
		if err := hub.Close(); err != nil {
			t.Errorf("close hub: %v", err)
		}
	}()
	m := &Metrics{}
	hub.SetMetrics(m)
	state, err := NewSessionAccessState("control-metric", uuid.New(), time.Now().Add(time.Minute), nil)
	if err != nil {
		t.Fatal(err)
	}
	q, err := hub.RegisterSession(state)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := q.ReserveControl(37, 16)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := m.Snapshot(hub)
	if snapshot.SessionQueuedInFlightBytes != 37 {
		t.Fatalf("queued/in-flight/control bytes=%d, want 37", snapshot.SessionQueuedInFlightBytes)
	}
	if snapshot.SessionReferenceBytes != 37 || snapshot.SessionReferenceMaxBytes != DefaultSessionReferenceBytes {
		t.Fatalf("aggregate snapshot used/max=%d/%d, want 37/%d", snapshot.SessionReferenceBytes, snapshot.SessionReferenceMaxBytes, DefaultSessionReferenceBytes)
	}
	reservation.Release()
}

func TestHubSharedSessionReferenceBudgetAccountsQueuedInflightControlsAndPurge(t *testing.T) {
	const budget = 128
	hub := NewHub(HubConfig{MaxSessions: 2, MaxSessionReferenceBytes: budget, MaxQueueBytes: 1024})
	defer func() { _ = hub.Close() }()
	_, q1 := createTestSession(t, hub, "budget-a")
	_, q2 := createTestSession(t, hub, "budget-b")
	event := createTestEvent(t, "gw", "sensor", 64)
	if err := q1.Enqueue(event); err != nil {
		t.Fatal(err)
	}
	if err := q2.Enqueue(event); err != nil {
		t.Fatalf("two references should exhaust shared budget exactly: %v", err)
	}
	if used, max := hub.SessionReferenceBudget(); used != 2*event.ByteSize() || max != budget {
		t.Fatalf("shared budget used/max=%d/%d, want %d/%d", used, max, 2*event.ByteSize(), budget)
	}
	frame, err := q1.Dequeue()
	if err != nil || frame == nil {
		t.Fatalf("dequeue frame=%v err=%v", frame, err)
	}
	if _, err = q1.ReserveControl(1, 16); !errors.Is(err, ErrAggregateBudget) {
		t.Fatalf("control should see bytes retained by in-flight telemetry, err=%v", err)
	}
	if used, _ := hub.SessionReferenceBudget(); used != 2*event.ByteSize() {
		t.Fatalf("dequeue prematurely released aggregate bytes: %d", used)
	}
	frame.Release()
	frame.Release()
	if used, _ := hub.SessionReferenceBudget(); used != event.ByteSize() {
		t.Fatalf("idempotent frame release left used=%d, want %d", used, event.ByteSize())
	}
	reservation, err := q1.ReserveControl(event.ByteSize(), 16)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = q1.ReserveControl(1, 16); !errors.Is(err, ErrAggregateBudget) {
		t.Fatalf("controls should share aggregate budget, err=%v", err)
	}
	if err := q1.Close(); err != nil {
		t.Fatal(err)
	}
	if used, _ := hub.SessionReferenceBudget(); used != 2*event.ByteSize() {
		t.Fatalf("queue close released writer-held control reservation: %d", used)
	}
	reservation.Release()
	reservation.Release()
	if used, _ := hub.SessionReferenceBudget(); used != event.ByteSize() {
		t.Fatalf("control reservation release is not exactly once: %d", used)
	}
	if q2.PurgeMatching(func(*SharedEvent) bool { return true }) != 1 {
		t.Fatal("expected queued frame purged")
	}
	if used, _ := hub.SessionReferenceBudget(); used != 0 {
		t.Fatalf("purge did not release aggregate bytes: %d", used)
	}
	if err := q2.Enqueue(event); err != nil {
		t.Fatal(err)
	}
	if err := hub.Close(); err != nil {
		t.Fatal(err)
	}
	if used, _ := hub.SessionReferenceBudget(); used != 0 {
		t.Fatalf("repeated hub close changed budget: %d", used)
	}
}

func TestSharedSessionReferenceRemainingCapacityCheckDoesNotOverflow(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	if fitsQueueBudget(maxInt, maxInt-1, 0, 0, 2) {
		t.Fatal("remaining-cap check accepted bytes beyond max int capacity")
	}
	budget := newSessionReferenceBudget(maxInt)
	if !budget.reserve(maxInt-1) || budget.reserve(2) {
		t.Fatal("shared remaining-cap check overflowed")
	}
	budget.release(maxInt - 1)
	if used, _ := budget.snapshot(); used != 0 {
		t.Fatalf("budget used after release=%d, want 0", used)
	}
}

func TestInFlightFrameReleaseAfterQueueCloseReturnsSharedBudgetOnce(t *testing.T) {
	event := createTestEvent(t, "gw", "sensor", 48)
	hub := NewHub(HubConfig{MaxSessions: 1, MaxSessionReferenceBytes: event.ByteSize()})
	state, q := createTestSession(t, hub, "close-inflight")
	if err := q.Enqueue(event); err != nil {
		t.Fatal(err)
	}
	frame, err := q.Dequeue()
	if err != nil || frame == nil {
		t.Fatalf("dequeue frame=%v err=%v", frame, err)
	}
	hub.CloseSession(state.SessionID(), RevocationReasonClientClose)
	if used, _ := hub.SessionReferenceBudget(); used != event.ByteSize() {
		t.Fatalf("queue close released in-flight reservation: used=%d want=%d", used, event.ByteSize())
	}
	frame.Release()
	frame.Release()
	if used, _ := hub.SessionReferenceBudget(); used != 0 {
		t.Fatalf("release after close left shared used=%d", used)
	}
	if err := hub.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHubUnsubscribeReleasesOnlyQueuedSessionReferences(t *testing.T) {
	const bytes = 96
	hub := NewHub(HubConfig{MaxSessions: 1, MaxSessionReferenceBytes: bytes, MaxQueueBytes: bytes})
	defer func() { _ = hub.Close() }()
	state, q := createTestSession(t, hub, "unsubscribe-budget")
	target := TargetKey{GatewayID: "gw", SensorID: "sensor"}
	gen, err := state.PrepareSubscribe([]TargetKey{target}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.CommitSubscribe(state.SessionID(), gen, []AuthorizedTarget{{Target: target}}, time.Now()); err != nil {
		t.Fatal(err)
	}
	event := createTestEvent(t, target.GatewayID, target.SensorID, 32)
	if _, err = hub.FanOut(event); err != nil {
		t.Fatal(err)
	}
	frame, err := q.Dequeue()
	if err != nil || frame == nil {
		t.Fatalf("dequeue frame=%v err=%v", frame, err)
	}
	if _, err = hub.Unsubscribe(state.SessionID(), []TargetKey{target}); err != nil {
		t.Fatal(err)
	}
	if used, _ := hub.SessionReferenceBudget(); used != event.ByteSize() {
		t.Fatalf("unsubscribe released in-flight reference; used=%d want=%d", used, event.ByteSize())
	}
	frame.Release()
	if used, _ := hub.SessionReferenceBudget(); used != 0 {
		t.Fatalf("frame release left aggregate used=%d", used)
	}
}

func TestSharedSessionReferenceBudgetConcurrentReserveReleaseBounded(t *testing.T) {
	const budget = 4096
	hub := NewHub(HubConfig{MaxSessions: 4, MaxSessionReferenceBytes: budget, MaxQueueBytes: budget})
	defer func() { _ = hub.Close() }()
	var queues []*SessionQueue
	for i := 0; i < 4; i++ {
		_, q := createTestSession(t, hub, fmt.Sprintf("concurrent-%d", i))
		queues = append(queues, q)
	}
	event := createTestEvent(t, "gw", "sensor", 32)
	var wg sync.WaitGroup
	for _, q := range queues {
		q := q
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				if err := q.Enqueue(event); err != nil {
					if !errors.Is(err, ErrAggregateBudget) && !errors.Is(err, ErrQueueFull) {
						t.Errorf("enqueue: %v", err)
					}
					continue
				}
				frame, err := q.Dequeue()
				if err != nil || frame == nil {
					t.Errorf("dequeue frame=%v err=%v", frame, err)
					continue
				}
				frame.Release()
			}
		}()
	}
	wg.Wait()
	if used, max := hub.SessionReferenceBudget(); used != 0 || max != budget {
		t.Fatalf("after concurrent reserve/release used/max=%d/%d, want 0/%d", used, max, budget)
	}
}

func TestHubAggregateBudgetExhaustionIsNotSlowConsumer(t *testing.T) {
	probe := createTestEvent(t, "gw", "sensor", 16)
	hub := NewHub(HubConfig{MaxSessions: 3, MaxSessionReferenceBytes: probe.ByteSize(), MaxQueueBytes: 1024})
	defer func() { _ = hub.Close() }()
	m := &Metrics{}
	hub.SetMetrics(m)
	stateA, qA := createTestSession(t, hub, "aggregate-a")
	stateB, qB := createTestSession(t, hub, "aggregate-b")
	stateC, _ := createTestSession(t, hub, "aggregate-c")
	for _, target := range []struct {
		state  *SessionAccessState
		target TargetKey
	}{{stateA, TargetKey{GatewayID: "gw", SensorID: "sensor"}}, {stateB, TargetKey{GatewayID: "gw", SensorID: "sensor"}}, {stateC, TargetKey{GatewayID: "other", SensorID: "sensor"}}} {
		gen, err := target.state.PrepareSubscribe([]TargetKey{target.target}, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = hub.CommitSubscribe(target.state.SessionID(), gen, []AuthorizedTarget{{Target: target.target}}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := hub.FanOut(probe); err != nil {
		t.Fatal(err)
	}
	if got := hub.SessionCount(); got != 2 {
		t.Fatalf("aggregate exhaustion should close only affected session, active=%d", got)
	}
	snapshot := m.Snapshot(hub)
	if snapshot.AggregateCapacityCloses != 1 || snapshot.SlowCloses != 0 {
		t.Fatalf("aggregate and slow counters=%d/%d, want 1/0", snapshot.AggregateCapacityCloses, snapshot.SlowCloses)
	}
	if qA.IsClosed() == qB.IsClosed() {
		t.Fatal("expected exactly one of the first two sessions to close")
	}
	// A completed/purged reference frees capacity; another target can then make progress.
	qA.PurgeMatching(func(*SharedEvent) bool { return true })
	qB.PurgeMatching(func(*SharedEvent) bool { return true })
	if _, err := hub.FanOut(createTestEvent(t, "other", "sensor", 16)); err != nil {
		t.Fatal(err)
	}
	if qC, ok := hub.GetQueue(stateC.SessionID()); !ok || qC.QueuedSlots() != 1 {
		t.Fatalf("healthy peer did not receive after capacity was freed: queue=%v ok=%v", qC, ok)
	}
}

func TestApplicationControlAggregateBudgetRejectionIsDistinct(t *testing.T) {
	hub := NewHub(HubConfig{MaxSessions: 2, MaxSessionReferenceBytes: 1})
	defer func() { _ = hub.Close() }()
	m := &Metrics{}
	hub.SetMetrics(m)
	socket := newApplicationSocket(t, auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: time.Now().Add(time.Minute)}, hub, allowAllTargets{}, HandlerOptions{})
	states := hub.GetActiveSessions(1)
	if len(states) != 1 {
		t.Fatalf("active sessions=%d, want 1", len(states))
	}
	q, ok := hub.GetQueue(states[0].SessionID())
	if !ok {
		t.Fatal("socket session queue missing")
	}
	session := &appSession{acceptedAt: time.Now(), metrics: m, id: states[0].SessionID(), conn: socket.conn, state: states[0], queue: q, controls: make(chan outboundControl, 1), close: make(chan struct{})}
	socket.h.enqueueControl(session, NewErrorMessage("request", ErrorCodeInternalError, "test", ""))
	select {
	case <-session.close:
	default:
		t.Fatal("aggregate control exhaustion did not close affected session")
	}
	snapshot := m.Snapshot(hub)
	if snapshot.AggregateCapacityCloses != 1 || snapshot.SlowCloses != 0 {
		t.Fatalf("control aggregate/slow close counters=%d/%d, want 1/0", snapshot.AggregateCapacityCloses, snapshot.SlowCloses)
	}
	if len(snapshot.CloseObservations) == 0 || snapshot.CloseObservations[len(snapshot.CloseObservations)-1].Reason != "aggregate_capacity" {
		t.Fatalf("control close observation did not retain aggregate reason: %+v", snapshot.CloseObservations)
	}
}

func TestCloseReasonsCountOnlyAccessRevocations(t *testing.T) {
	for _, reason := range []RevocationReason{RevocationReasonClientClose, RevocationReasonSlowConsumer} {
		hub := NewHub(DefaultHubConfig())
		m := &Metrics{}
		hub.SetMetrics(m)
		state, err := NewSessionAccessState("close-reason", uuid.New(), time.Now().Add(time.Minute), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = hub.RegisterSession(state); err != nil {
			t.Fatal(err)
		}
		hub.CloseSession(state.SessionID(), reason)
		if got := m.Snapshot(hub).Revocations; got != 0 {
			t.Fatalf("reason %s counted as revocation: %d", reason, got)
		}
		if err := hub.Close(); err != nil {
			t.Errorf("close hub: %v", err)
		}
	}
}

func TestApplicationSocketBinaryFramesRespectRequestRate(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	defer func() { _ = hub.Close() }()
	s := newApplicationSocket(t, auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: time.Now().Add(time.Minute)}, hub, allowAllTargets{}, HandlerOptions{MaxRequestRate: 1})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.conn.Write(ctx, websocket.MessageBinary, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.conn.Read(ctx); err != nil {
		t.Fatalf("first bounded error response: %v", err)
	}
	if err := s.conn.Write(ctx, websocket.MessageBinary, []byte("x")); err != nil {
		t.Fatal(err)
	}
	_, _, err := s.conn.Read(ctx)
	if err == nil {
		t.Fatal("second application message was not rate-limited and closed")
	}
	if err := s.h.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

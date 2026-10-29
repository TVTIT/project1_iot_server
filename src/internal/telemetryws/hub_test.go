package telemetryws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Helper to create a valid SessionAccessState hooked to the Hub.
func createTestSession(t *testing.T, hub *Hub, sessionID string) (*SessionAccessState, *SessionQueue) {
	t.Helper()
	userID := uuid.New()
	expiresAt := time.Now().Add(1 * time.Hour)

	state, err := NewSessionAccessState(sessionID, userID, expiresAt, hub)
	if err != nil {
		t.Fatalf("failed to create session access state: %v", err)
	}

	queue, err := hub.RegisterSession(state)
	if err != nil {
		t.Fatalf("failed to register session in hub: %v", err)
	}

	return state, queue
}

// Helper to create a dummy SharedEvent with payload bytes.
func createTestEvent(t *testing.T, gatewayID, sensorID string, size int) *SharedEvent {
	t.Helper()
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	evt, err := NewSharedEvent(gatewayID, sensorID, payload, 0)
	if err != nil {
		t.Fatalf("failed to create shared event: %v", err)
	}
	return evt
}

// 1. Admission Cap: max 32 sessions in registry / hub.
func TestHub_AdmissionCapMax32(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	defer func() { _ = hub.Close() }()

	sessions := make([]*SessionAccessState, MaxSessions)
	for i := 0; i < MaxSessions; i++ {
		sid := fmt.Sprintf("session-%02d", i)
		state, _ := createTestSession(t, hub, sid)
		sessions[i] = state
	}

	if hub.SessionCount() != MaxSessions {
		t.Fatalf("expected %d sessions, got %d", MaxSessions, hub.SessionCount())
	}

	// 33rd session must return ErrSessionLimitExceeded
	extraState, err := NewSessionAccessState("session-overflow", uuid.New(), time.Now().Add(1*time.Hour), hub)
	if err != nil {
		t.Fatalf("unexpected error creating state: %v", err)
	}
	_, err = hub.RegisterSession(extraState)
	if !errors.Is(err, ErrSessionLimitExceeded) {
		t.Fatalf("expected ErrSessionLimitExceeded, got %v", err)
	}

	// Close one session, now admission succeeds
	hub.CloseSession("session-00", RevocationReasonClientClose)
	if hub.SessionCount() != MaxSessions-1 {
		t.Fatalf("expected %d sessions after close, got %d", MaxSessions-1, hub.SessionCount())
	}

	q, err := hub.RegisterSession(extraState)
	if err != nil {
		t.Fatalf("expected registration to succeed after close, got %v", err)
	}
	if q == nil {
		t.Fatal("expected non-nil queue")
	}
}

func TestHub_ConfiguredAdmissionCapAndRelease(t *testing.T) {
	const cap = 64
	hub := NewHub(HubConfig{MaxSessions: cap})
	defer func() { _ = hub.Close() }()
	for i := 0; i < cap; i++ {
		_, _ = createTestSession(t, hub, fmt.Sprintf("configured-%02d", i))
	}
	extra, err := NewSessionAccessState("configured-overflow", uuid.New(), time.Now().Add(time.Hour), hub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hub.RegisterSession(extra); !errors.Is(err, ErrSessionLimitExceeded) {
		t.Fatalf("cap+1 registration err=%v, want ErrSessionLimitExceeded", err)
	}
	hub.CloseSession("configured-00", RevocationReasonClientClose)
	if _, err := hub.RegisterSession(extra); err != nil {
		t.Fatalf("registration after releasing a slot: %v", err)
	}
}

func TestApplicationHandlerConfiguredSessionLimit(t *testing.T) {
	hub := NewHub(HubConfig{MaxSessions: 64})
	defer func() { _ = hub.Close() }()
	authz := &socketPolicyAuthorizer{}
	h, err := NewApplicationHandlerWithOptions(nil, ApplicationHandlerDeps{Hub: hub, Authorizer: authz, Options: HandlerOptions{MaxSessions: 64}})
	if err != nil {
		t.Fatalf("configured 64 sessions rejected: %v", err)
	}
	if got := cap(h.slots); got != 64 {
		t.Fatalf("handler admission slots=%d, want configured cap 64", got)
	}
	if _, err := NewApplicationHandlerWithOptions(nil, ApplicationHandlerDeps{Hub: hub, Authorizer: authz, Options: HandlerOptions{MaxSessions: 10001}}); err == nil {
		t.Fatal("session limit above finite ceiling accepted")
	}
}

// 2. Index Target Selection: gateway-wide vs specific sensor filter.
func TestHub_IndexTargetSelection(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	defer func() { _ = hub.Close() }()

	s1, q1 := createTestSession(t, hub, "viewer-1")
	s2, q2 := createTestSession(t, hub, "viewer-2")
	s3, q3 := createTestSession(t, hub, "viewer-3")

	now := time.Now()

	// viewer-1: subscribed to (gw-A, sensor-1)
	gen1, _ := s1.PrepareSubscribe([]TargetKey{{GatewayID: "gw-A", SensorID: "sensor-1"}}, now)
	_, err := hub.CommitSubscribe(s1.SessionID(), gen1, []AuthorizedTarget{
		{Target: TargetKey{GatewayID: "gw-A", SensorID: "sensor-1"}},
	}, now)
	if err != nil {
		t.Fatalf("CommitSubscribe viewer-1 failed: %v", err)
	}

	// viewer-2: subscribed to gateway-level (gw-A, "")
	gen2, _ := s2.PrepareSubscribe([]TargetKey{{GatewayID: "gw-A", SensorID: ""}}, now)
	_, err = hub.CommitSubscribe(s2.SessionID(), gen2, []AuthorizedTarget{
		{Target: TargetKey{GatewayID: "gw-A", SensorID: ""}},
	}, now)
	if err != nil {
		t.Fatalf("CommitSubscribe viewer-2 failed: %v", err)
	}

	// viewer-3: subscribed to (gw-B, sensor-1)
	gen3, _ := s3.PrepareSubscribe([]TargetKey{{GatewayID: "gw-B", SensorID: "sensor-1"}}, now)
	_, err = hub.CommitSubscribe(s3.SessionID(), gen3, []AuthorizedTarget{
		{Target: TargetKey{GatewayID: "gw-B", SensorID: "sensor-1"}},
	}, now)
	if err != nil {
		t.Fatalf("CommitSubscribe viewer-3 failed: %v", err)
	}

	// Event for (gw-A, sensor-1): viewer-1 and viewer-2 should receive it. viewer-3 should NOT.
	evtA1 := createTestEvent(t, "gw-A", "sensor-1", 100)
	delivered, err := hub.FanOut(evtA1)
	if err != nil {
		t.Fatalf("FanOut failed: %v", err)
	}
	if delivered != 2 {
		t.Fatalf("expected delivered=2, got %d", delivered)
	}
	if q1.QueuedSlots() != 1 {
		t.Fatalf("expected viewer-1 queue=1, got %d", q1.QueuedSlots())
	}
	if q2.QueuedSlots() != 1 {
		t.Fatalf("expected viewer-2 queue=1, got %d", q2.QueuedSlots())
	}
	if q3.QueuedSlots() != 0 {
		t.Fatalf("expected viewer-3 queue=0, got %d", q3.QueuedSlots())
	}

	// Event for (gw-A, sensor-2): ONLY viewer-2 should receive it (gateway-level).
	evtA2 := createTestEvent(t, "gw-A", "sensor-2", 100)
	delivered, err = hub.FanOut(evtA2)
	if err != nil {
		t.Fatalf("FanOut failed: %v", err)
	}
	if delivered != 1 {
		t.Fatalf("expected delivered=1, got %d", delivered)
	}
	if q1.QueuedSlots() != 1 {
		t.Fatalf("expected viewer-1 queue=1, got %d", q1.QueuedSlots())
	}
	if q2.QueuedSlots() != 2 {
		t.Fatalf("expected viewer-2 queue=2, got %d", q2.QueuedSlots())
	}
	if q3.QueuedSlots() != 0 {
		t.Fatalf("expected viewer-3 queue=0, got %d", q3.QueuedSlots())
	}
}

// 3. Same Sensor ID on two Gateways: strict namespace isolation.
func TestHub_SameSensorIDOnTwoGateways(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	defer func() { _ = hub.Close() }()

	sA, qA := createTestSession(t, hub, "viewer-gwA")
	sB, qB := createTestSession(t, hub, "viewer-gwB")

	now := time.Now()

	// viewer-gwA: (gw-1, temp-sensor)
	genA, _ := sA.PrepareSubscribe([]TargetKey{{GatewayID: "gw-1", SensorID: "temp-sensor"}}, now)
	_, err := hub.CommitSubscribe(sA.SessionID(), genA, []AuthorizedTarget{
		{Target: TargetKey{GatewayID: "gw-1", SensorID: "temp-sensor"}},
	}, now)
	if err != nil {
		t.Fatalf("CommitSubscribe failed: %v", err)
	}

	// viewer-gwB: (gw-2, temp-sensor) - same sensor ID on different gateway!
	genB, _ := sB.PrepareSubscribe([]TargetKey{{GatewayID: "gw-2", SensorID: "temp-sensor"}}, now)
	_, err = hub.CommitSubscribe(sB.SessionID(), genB, []AuthorizedTarget{
		{Target: TargetKey{GatewayID: "gw-2", SensorID: "temp-sensor"}},
	}, now)
	if err != nil {
		t.Fatalf("CommitSubscribe failed: %v", err)
	}

	// Event for gw-1: temp-sensor
	evt1 := createTestEvent(t, "gw-1", "temp-sensor", 64)
	delivered, err := hub.FanOut(evt1)
	if err != nil {
		t.Fatalf("FanOut failed: %v", err)
	}
	if delivered != 1 {
		t.Fatalf("expected delivered=1, got %d", delivered)
	}
	if qA.QueuedSlots() != 1 {
		t.Fatalf("expected viewer-gwA queue=1, got %d", qA.QueuedSlots())
	}
	if qB.QueuedSlots() != 0 {
		t.Fatalf("expected viewer-gwB queue=0, got %d", qB.QueuedSlots())
	}

	// Event for gw-2: temp-sensor
	evt2 := createTestEvent(t, "gw-2", "temp-sensor", 64)
	delivered, err = hub.FanOut(evt2)
	if err != nil {
		t.Fatalf("FanOut failed: %v", err)
	}
	if delivered != 1 {
		t.Fatalf("expected delivered=1, got %d", delivered)
	}
	if qA.QueuedSlots() != 1 {
		t.Fatalf("expected viewer-gwA queue=1, got %d", qA.QueuedSlots())
	}
	if qB.QueuedSlots() != 1 {
		t.Fatalf("expected viewer-gwB queue=1, got %d", qB.QueuedSlots())
	}
}

// 4. Overlapping Gateway and Sensor filters select each session AT MOST ONCE.
func TestHub_OverlappingFiltersAtMostOnce(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	defer func() { _ = hub.Close() }()

	s, q := createTestSession(t, hub, "viewer-overlap")
	now := time.Now()

	// Subscribe to BOTH (gw-1, "") AND (gw-1, "sensor-X")
	targets := []TargetKey{
		{GatewayID: "gw-1", SensorID: ""},
		{GatewayID: "gw-1", SensorID: "sensor-X"},
	}
	gen, err := s.PrepareSubscribe(targets, now)
	if err != nil {
		t.Fatalf("PrepareSubscribe failed: %v", err)
	}

	_, err = hub.CommitSubscribe(s.SessionID(), gen, []AuthorizedTarget{
		{Target: targets[0]},
		{Target: targets[1]},
	}, now)
	if err != nil {
		t.Fatalf("CommitSubscribe failed: %v", err)
	}

	// Event arrives matching BOTH filters
	evt := createTestEvent(t, "gw-1", "sensor-X", 128)
	delivered, err := hub.FanOut(evt)
	if err != nil {
		t.Fatalf("FanOut failed: %v", err)
	}

	if delivered != 1 {
		t.Fatalf("expected delivered=1 (selected once), got %d", delivered)
	}
	if q.QueuedSlots() != 1 {
		t.Fatalf("expected queue slot=1, got %d", q.QueuedSlots())
	}

	// Verify dequeue returns exactly 1 frame and then empty
	frame, err := q.Dequeue()
	if err != nil || frame == nil {
		t.Fatalf("expected frame, got err=%v", err)
	}
	frame.Release()

	next, err := q.Dequeue()
	if err != nil || next != nil {
		t.Fatalf("expected empty queue, got frame=%v err=%v", next, err)
	}
}

// 5. Unsubscribe versus FanOut: removed targets purge matching frames and cease receiving events.
func TestHub_UnsubscribeVersusFanout(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	defer func() { _ = hub.Close() }()

	s, q := createTestSession(t, hub, "viewer-unsub")
	now := time.Now()

	target := TargetKey{GatewayID: "gw-1", SensorID: "sensor-1"}
	gen, _ := s.PrepareSubscribe([]TargetKey{target}, now)
	_, err := hub.CommitSubscribe(s.SessionID(), gen, []AuthorizedTarget{{Target: target}}, now)
	if err != nil {
		t.Fatalf("CommitSubscribe failed: %v", err)
	}

	// Enqueue an event
	evt1 := createTestEvent(t, "gw-1", "sensor-1", 100)
	_, _ = hub.FanOut(evt1)
	if q.QueuedSlots() != 1 || q.QueuedBytes() != 100 {
		t.Fatalf("expected 1 queued slot and 100 queued bytes, got %d slots, %d bytes", q.QueuedSlots(), q.QueuedBytes())
	}

	// Unsubscribe from target
	removed, err := hub.Unsubscribe(s.SessionID(), []TargetKey{target})
	if err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}
	if len(removed) != 1 {
		t.Fatalf("expected 1 removed target, got %d", len(removed))
	}

	// Unsent queued frames matching target must be purged immediately, releasing byte counters
	if q.QueuedSlots() != 0 || q.QueuedBytes() != 0 {
		t.Fatalf("expected queue purged to 0, got %d slots, %d bytes", q.QueuedSlots(), q.QueuedBytes())
	}

	// Index must no longer have target
	if hub.SubscribedSessionsCount(target) != 0 {
		t.Fatalf("expected 0 subscribers for target, got %d", hub.SubscribedSessionsCount(target))
	}

	// Second event for the target must NOT be delivered
	evt2 := createTestEvent(t, "gw-1", "sensor-1", 100)
	delivered, err := hub.FanOut(evt2)
	if err != nil {
		t.Fatalf("FanOut failed: %v", err)
	}
	if delivered != 0 {
		t.Fatalf("expected delivered=0, got %d", delivered)
	}
}

func TestHub_FanOutRevalidatesAfterUnsubscribe(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	defer func() { _ = hub.Close() }()
	s, q := createTestSession(t, hub, "viewer-snapshot-unsub")
	now := time.Now()
	target := TargetKey{GatewayID: "gw-race", SensorID: "sensor-race"}
	gen, err := s.PrepareSubscribe([]TargetKey{target}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.CommitSubscribe(s.SessionID(), gen, []AuthorizedTarget{{Target: target}}, now); err != nil {
		t.Fatal(err)
	}

	snapshotted := make(chan struct{})
	continueFanout := make(chan struct{})
	hub.afterFanOutSnapshot = func() { close(snapshotted); <-continueFanout }
	finished := make(chan struct{})
	evt := createTestEvent(t, "gw-race", "sensor-race", 8)
	go func() { defer close(finished); _, _ = hub.FanOut(evt) }()
	<-snapshotted
	if _, err := hub.Unsubscribe(s.SessionID(), []TargetKey{target}); err != nil {
		t.Fatal(err)
	}
	close(continueFanout)
	<-finished
	if got := q.QueuedSlots(); got != 0 {
		t.Fatalf("stale FanOut queued %d frame(s) after unsubscribe returned", got)
	}
}

func TestHub_FanOutAfterUnsubscribeKeepsRemainingOverlappingAuthorization(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	defer func() { _ = hub.Close() }()
	s, q := createTestSession(t, hub, "viewer-overlap-unsub")
	now := time.Now()
	sensorTarget := TargetKey{GatewayID: "gw-overlap", SensorID: "sensor-overlap"}
	gatewayTarget := TargetKey{GatewayID: "gw-overlap"}
	gen, err := s.PrepareSubscribe([]TargetKey{sensorTarget, gatewayTarget}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.CommitSubscribe(s.SessionID(), gen, []AuthorizedTarget{{Target: sensorTarget}, {Target: gatewayTarget}}, now); err != nil {
		t.Fatal(err)
	}
	if _, err = hub.Unsubscribe(s.SessionID(), []TargetKey{sensorTarget}); err != nil {
		t.Fatal(err)
	}
	if delivered, err := hub.FanOut(createTestEvent(t, "gw-overlap", "sensor-overlap", 8)); err != nil || delivered != 1 {
		t.Fatalf("remaining gateway filter should authorize exactly one delivery: delivered=%d err=%v", delivered, err)
	}
	if q.QueuedSlots() != 1 {
		t.Fatalf("expected one authorized queued frame, got %d", q.QueuedSlots())
	}
}

func TestHub_UnsubscribePreservesQueuedEventsCoveredByRemainingOverlap(t *testing.T) {
	for _, tc := range []struct {
		name    string
		removed TargetKey
		remain  TargetKey
	}{
		{name: "remove-sensor-keep-gateway", removed: TargetKey{GatewayID: "gw-overlap", SensorID: "sensor-overlap"}, remain: TargetKey{GatewayID: "gw-overlap"}},
		{name: "remove-gateway-keep-sensor", removed: TargetKey{GatewayID: "gw-overlap"}, remain: TargetKey{GatewayID: "gw-overlap", SensorID: "sensor-overlap"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hub := NewHub(DefaultHubConfig())
			defer func() { _ = hub.Close() }()
			s, q := createTestSession(t, hub, "queued-overlap-"+tc.name)
			all := []TargetKey{{GatewayID: "gw-overlap"}, {GatewayID: "gw-overlap", SensorID: "sensor-overlap"}}
			gen, err := s.PrepareSubscribe(all, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			allowed := []AuthorizedTarget{{Target: all[0]}, {Target: all[1]}}
			if _, err = hub.CommitSubscribe(s.SessionID(), gen, allowed, time.Now()); err != nil {
				t.Fatal(err)
			}
			if n, err := hub.FanOut(createTestEvent(t, "gw-overlap", "sensor-overlap", 8)); err != nil || n != 1 {
				t.Fatalf("pre-unsubscribe fanout=%d err=%v", n, err)
			}
			if _, err := hub.Unsubscribe(s.SessionID(), []TargetKey{tc.removed}); err != nil {
				t.Fatal(err)
			}
			if !s.HasTarget(tc.remain) {
				t.Fatal("remaining filter was removed")
			}
			frame, err := q.Dequeue()
			if err != nil {
				t.Fatal(err)
			}
			if frame == nil {
				t.Fatal("queued event still authorized by remaining filter was purged")
			}
			frame.Release()
		})
	}
}

func TestHub_CommitSubscribeExpiryCallbackDoesNotDeadlock(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	expiresAt := time.Now().Add(-time.Second)
	state, err := NewSessionAccessState("expired-at-commit", uuid.New(), expiresAt, hub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hub.RegisterSession(state); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, err := hub.CommitSubscribe(state.SessionID(), 0, nil, time.Now()); result <- err }()
	select {
	case err := <-result:
		if !errors.Is(err, ErrSessionExpired) {
			t.Fatalf("expected ErrSessionExpired, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("CommitSubscribe deadlocked while expiry callback re-entered hub")
	}
	if hub.SessionCount() != 0 {
		t.Fatal("expiry callback did not remove the session")
	}
}

// 6. CommitSubscribe generation check: in-flight check invalidated by unsubscribe cannot resurrect removed filter.
func TestHub_CommitSubscribeStaleGeneration(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	defer func() { _ = hub.Close() }()

	s, _ := createTestSession(t, hub, "viewer-stale")
	now := time.Now()

	targetA := TargetKey{GatewayID: "gw-1", SensorID: "sensor-A"}
	targetB := TargetKey{GatewayID: "gw-1", SensorID: "sensor-B"}

	// Session is already subscribed to targetA
	genA, _ := s.PrepareSubscribe([]TargetKey{targetA}, now)
	_, _ = hub.CommitSubscribe(s.SessionID(), genA, []AuthorizedTarget{{Target: targetA}}, now)

	// Now start subscribing to targetB
	genB, err := s.PrepareSubscribe([]TargetKey{targetB}, now)
	if err != nil {
		t.Fatalf("PrepareSubscribe targetB failed: %v", err)
	}

	// Intervening Unsubscribe occurs (e.g. client unsubscribed from targetA)
	// This bumps generation!
	_, err = hub.Unsubscribe(s.SessionID(), []TargetKey{targetA})
	if err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}

	// Now the delayed DB query finishes and attempts CommitSubscribe with stale genB
	_, err = hub.CommitSubscribe(s.SessionID(), genB, []AuthorizedTarget{{Target: targetB}}, now)
	if !errors.Is(err, ErrStaleAuthorization) {
		t.Fatalf("expected ErrStaleAuthorization, got %v", err)
	}

	// TargetB was NOT installed
	if s.HasTarget(targetB) {
		t.Fatal("targetB should NOT be installed in state")
	}
	if hub.SubscribedSessionsCount(targetB) != 0 {
		t.Fatal("targetB should NOT be present in hub index")
	}
}

// 7. Re-subscribe is idempotent, target limits and index entries do not duplicate.
func TestHub_ReSubscribeIdempotent(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	defer func() { _ = hub.Close() }()

	s, _ := createTestSession(t, hub, "viewer-idempotent")
	now := time.Now()

	target := TargetKey{GatewayID: "gw-1", SensorID: "sensor-1"}

	// First subscribe
	gen1, _ := s.PrepareSubscribe([]TargetKey{target}, now)
	installed1, err := hub.CommitSubscribe(s.SessionID(), gen1, []AuthorizedTarget{{Target: target}}, now)
	if err != nil {
		t.Fatalf("first subscribe failed: %v", err)
	}
	if len(installed1) != 1 {
		t.Fatalf("expected 1 installed, got %d", len(installed1))
	}

	// Second subscribe for same target (recheck passes)
	gen2, err := s.PrepareSubscribe([]TargetKey{target}, now)
	if err != nil {
		t.Fatalf("second PrepareSubscribe failed: %v", err)
	}
	installed2, err := hub.CommitSubscribe(s.SessionID(), gen2, []AuthorizedTarget{{Target: target}}, now)
	if err != nil {
		t.Fatalf("second CommitSubscribe failed: %v", err)
	}
	// Already installed target is not double-installed
	if len(installed2) != 0 {
		t.Fatalf("expected 0 new targets installed, got %d", len(installed2))
	}

	if s.TargetCount() != 1 {
		t.Fatalf("expected 1 target in state, got %d", s.TargetCount())
	}
	if hub.SubscribedSessionsCount(target) != 1 {
		t.Fatalf("expected 1 session in index, got %d", hub.SubscribedSessionsCount(target))
	}
}

// 8. Queue slot and logical byte caps.
func TestQueue_SlotAndByteCaps(t *testing.T) {
	// Slot cap test: maxSlots=3, maxBytes=1000
	qSlots := NewSessionQueue("test-slots", 3, 1000, 500)
	evt1 := createTestEvent(t, "gw-1", "s-1", 50)
	evt2 := createTestEvent(t, "gw-1", "s-1", 50)
	evt3 := createTestEvent(t, "gw-1", "s-1", 50)
	evt4 := createTestEvent(t, "gw-1", "s-1", 50)

	if err := qSlots.Enqueue(evt1); err != nil {
		t.Fatalf("enqueue 1 failed: %v", err)
	}
	if err := qSlots.Enqueue(evt2); err != nil {
		t.Fatalf("enqueue 2 failed: %v", err)
	}
	if err := qSlots.Enqueue(evt3); err != nil {
		t.Fatalf("enqueue 3 failed: %v", err)
	}

	// 4th enqueue exceeds slot cap (3)
	if err := qSlots.Enqueue(evt4); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	// Byte cap test: maxSlots=10, maxBytes=200
	qBytes := NewSessionQueue("test-bytes", 10, 200, 500)
	evtB1 := createTestEvent(t, "gw-1", "s-1", 90)
	evtB2 := createTestEvent(t, "gw-1", "s-1", 90)
	evtB3 := createTestEvent(t, "gw-1", "s-1", 90)

	if err := qBytes.Enqueue(evtB1); err != nil {
		t.Fatalf("enqueue B1 failed: %v", err)
	}
	if err := qBytes.Enqueue(evtB2); err != nil {
		t.Fatalf("enqueue B2 failed: %v", err)
	}
	// 3rd enqueue: 90 + 90 + 90 = 270 > 200 byte cap!
	if err := qBytes.Enqueue(evtB3); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull on byte cap, got %v", err)
	}
}

// 9. In-Flight Byte Release and Double-Release Safety.
func TestQueue_InFlightByteRelease(t *testing.T) {
	q := NewSessionQueue("test-inflight", 10, 1000, 500)
	evt := createTestEvent(t, "gw-1", "s-1", 200)

	if err := q.Enqueue(evt); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	if q.QueuedBytes() != 200 || q.InFlightBytes() != 0 || q.TotalBytes() != 200 {
		t.Fatalf("unexpected initial bytes: queued=%d inFlight=%d total=%d",
			q.QueuedBytes(), q.InFlightBytes(), q.TotalBytes())
	}

	frame, err := q.Dequeue()
	if err != nil || frame == nil {
		t.Fatalf("dequeue failed: %v", err)
	}

	// Dequeued: bytes moved to inFlightBytes
	if q.QueuedBytes() != 0 || q.InFlightBytes() != 200 || q.TotalBytes() != 200 {
		t.Fatalf("unexpected in-flight bytes: queued=%d inFlight=%d total=%d",
			q.QueuedBytes(), q.InFlightBytes(), q.TotalBytes())
	}

	// Release frame
	frame.Release()

	if q.QueuedBytes() != 0 || q.InFlightBytes() != 0 || q.TotalBytes() != 0 {
		t.Fatalf("unexpected released bytes: queued=%d inFlight=%d total=%d",
			q.QueuedBytes(), q.InFlightBytes(), q.TotalBytes())
	}

	// Double-release must be idempotent and NOT cause negative counters
	frame.Release()
	if q.InFlightBytes() != 0 || q.TotalBytes() != 0 {
		t.Fatalf("double-release corrupted counters: inFlight=%d total=%d",
			q.InFlightBytes(), q.TotalBytes())
	}
}

// 10. Slow Viewer Isolation: full viewer queue closes that slow session and requests REST catch-up, never blocks publisher/other viewers.
func TestHub_SlowViewerIsolation(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	defer func() { _ = hub.Close() }()

	fastState, fastQ := createTestSession(t, hub, "fast-viewer")
	slowState, slowQ := createTestSession(t, hub, "slow-viewer")

	now := time.Now()
	target := TargetKey{GatewayID: "gw-1", SensorID: "sensor-1"}

	// Both subscribe to (gw-1, sensor-1)
	genFast, _ := fastState.PrepareSubscribe([]TargetKey{target}, now)
	_, _ = hub.CommitSubscribe(fastState.SessionID(), genFast, []AuthorizedTarget{{Target: target}}, now)

	genSlow, _ := slowState.PrepareSubscribe([]TargetKey{target}, now)
	_, _ = hub.CommitSubscribe(slowState.SessionID(), genSlow, []AuthorizedTarget{{Target: target}}, now)

	// Artificially fill slow viewer queue to capacity (default 64 slots)
	fillEvt := createTestEvent(t, "gw-1", "sensor-1", 10)
	for i := 0; i < DefaultSessionQueueSlots; i++ {
		if err := slowQ.Enqueue(fillEvt); err != nil {
			t.Fatalf("fill slow queue failed at %d: %v", i, err)
		}
	}
	if slowQ.QueuedSlots() != DefaultSessionQueueSlots {
		t.Fatalf("expected slow queue to have %d slots, got %d", DefaultSessionQueueSlots, slowQ.QueuedSlots())
	}

	// Now publisher fans out a new event
	fanOutEvt := createTestEvent(t, "gw-1", "sensor-1", 100)
	delivered, err := hub.FanOut(fanOutEvt)
	if err != nil {
		t.Fatalf("FanOut failed: %v", err)
	}

	// Fast viewer received the event
	if delivered != 1 {
		t.Fatalf("expected delivered=1 (fast viewer only), got %d", delivered)
	}
	if fastQ.QueuedSlots() != 1 {
		t.Fatalf("expected fast viewer queue=1, got %d", fastQ.QueuedSlots())
	}

	// Slow viewer must be closed and isolated
	if !slowQ.IsClosed() {
		t.Fatal("expected slow viewer queue to be closed")
	}
	if !slowState.IsClosed() {
		t.Fatal("expected slow viewer state to be closed")
	}

	// Slow viewer must be removed from hub
	if _, exists := hub.GetQueue("slow-viewer"); exists {
		t.Fatal("slow viewer should be removed from hub queues")
	}
	if hub.SubscribedSessionsCount(target) != 1 {
		t.Fatalf("expected 1 active subscriber in index, got %d", hub.SubscribedSessionsCount(target))
	}
}

// 11. Revocation Discard and Idempotent Cleanup.
func TestHub_RevocationDiscardAndIdempotentCleanup(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	defer func() { _ = hub.Close() }()

	state, q := createTestSession(t, hub, "revoked-session")
	now := time.Now()
	target := TargetKey{GatewayID: "gw-1", SensorID: "sensor-1"}

	gen, _ := state.PrepareSubscribe([]TargetKey{target}, now)
	_, _ = hub.CommitSubscribe(state.SessionID(), gen, []AuthorizedTarget{{Target: target}}, now)

	// Enqueue event
	evt := createTestEvent(t, "gw-1", "sensor-1", 100)
	_, _ = hub.FanOut(evt)
	if q.QueuedSlots() != 1 || q.QueuedBytes() != 100 {
		t.Fatalf("expected 1 queued frame, got %d", q.QueuedSlots())
	}

	// Revocation triggers OnRevocation
	hub.OnRevocation(state.SessionID(), RevocationReasonDenied)

	// Queue must be closed and unsent bytes cleared
	if !q.IsClosed() {
		t.Fatal("expected queue to be closed")
	}
	if q.QueuedSlots() != 0 || q.QueuedBytes() != 0 {
		t.Fatalf("expected queued bytes to be reset to 0, got slots=%d bytes=%d", q.QueuedSlots(), q.QueuedBytes())
	}

	// Index must be cleared
	if hub.IndexEntryCount() != 0 {
		t.Fatalf("expected 0 index entries, got %d", hub.IndexEntryCount())
	}
	if hub.SessionCount() != 0 {
		t.Fatalf("expected 0 sessions in hub, got %d", hub.SessionCount())
	}

	// Subsequent calls must be idempotent
	hub.OnRevocation(state.SessionID(), RevocationReasonDenied)
	hub.CloseSession(state.SessionID(), RevocationReasonClientClose)
}

// 12. SharedEvent Ownership and Mutation Safety.
func TestSharedEvent_OwnershipNoMutation(t *testing.T) {
	raw := []byte(`{"test":"immutable_json_payload"}`)
	evt := CommittedBatchEvent{
		DTO: TelemetryEventDTO{
			GatewayID: "gw-1",
			SensorID:  "sensor-1",
		},
		Raw: raw,
	}

	shared, err := NewSharedEventFromCommitted(evt, 1024)
	if err != nil {
		t.Fatalf("NewSharedEventFromCommitted failed: %v", err)
	}

	// Mutate the original raw slice
	raw[0] = 'X'
	raw[1] = 'X'
	evt.DTO.GatewayID = "mutated-gw"

	// SharedEvent payload must remain unmutated
	if string(shared.Payload()) != `{"test":"immutable_json_payload"}` {
		t.Fatalf("shared event payload was mutated! got: %s", string(shared.Payload()))
	}
	if shared.GatewayID() != "gw-1" {
		t.Fatalf("shared event gateway ID changed! got: %s", shared.GatewayID())
	}
}

// 13. Concurrent FanOut, Subscribe, Unsubscribe, and Dequeue Race Test.
func TestHub_ConcurrentFanOutAndLifecycleRace(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	defer func() { _ = hub.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const numSessions = 8
	for i := 0; i < numSessions; i++ {
		sid := fmt.Sprintf("race-session-%d", i)
		s, _ := createTestSession(t, hub, sid)
		now := time.Now()
		target := TargetKey{GatewayID: "gw-race", SensorID: fmt.Sprintf("sensor-%d", i%2)}
		gen, _ := s.PrepareSubscribe([]TargetKey{target}, now)
		_, _ = hub.CommitSubscribe(sid, gen, []AuthorizedTarget{{Target: target}}, now)
	}

	var wg sync.WaitGroup

	// Publisher goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		idx := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				sensorID := fmt.Sprintf("sensor-%d", idx%2)
				evt := createTestEvent(t, "gw-race", sensorID, 32)
				_, _ = hub.FanOut(evt)
				idx++
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	// Reader goroutines
	for i := 0; i < numSessions; i++ {
		sid := fmt.Sprintf("race-session-%d", i)
		q, ok := hub.GetQueue(sid)
		if !ok {
			continue
		}
		wg.Add(1)
		go func(queue *SessionQueue) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					frame, err := queue.Dequeue()
					if err != nil {
						return
					}
					if frame != nil {
						frame.Release()
					} else {
						time.Sleep(500 * time.Microsecond)
					}
				}
			}
		}(q)
	}

	// Concurrent subscribe/unsubscribe goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				target := TargetKey{GatewayID: "gw-race", SensorID: "sensor-0"}
				_, _ = hub.Unsubscribe("race-session-0", []TargetKey{target})
				time.Sleep(2 * time.Millisecond)

				s := hub.Registry().Get("race-session-0")
				if s != nil && !s.IsClosed() {
					now := time.Now()
					gen, err := s.PrepareSubscribe([]TargetKey{target}, now)
					if err == nil {
						_, _ = hub.CommitSubscribe("race-session-0", gen, []AuthorizedTarget{{Target: target}}, now)
					}
				}
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	wg.Wait()
}

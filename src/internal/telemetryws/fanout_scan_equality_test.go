package telemetryws

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Helper to set up a session with authorized targets
func setupSessionWithTargets(t *testing.T, hub *Hub, sessionID string, targets []TargetKey) (*SessionAccessState, *SessionQueue) {
	t.Helper()
	state, err := NewSessionAccessState(sessionID, uuid.New(), time.Now().Add(time.Hour), hub)
	if err != nil {
		t.Fatalf("failed to create session state %s: %v", sessionID, err)
	}
	queue, err := hub.RegisterSession(state)
	if err != nil {
		t.Fatalf("failed to register session %s: %v", sessionID, err)
	}
	if len(targets) > 0 {
		now := time.Now()
		gen, err := state.PrepareSubscribe(targets, now)
		if err != nil {
			t.Fatalf("PrepareSubscribe failed for %s: %v", sessionID, err)
		}
		authTargets := make([]AuthorizedTarget, len(targets))
		for i, tgt := range targets {
			authTargets[i] = AuthorizedTarget{Target: tgt}
		}
		_, err = hub.CommitSubscribe(sessionID, gen, authTargets, now)
		if err != nil {
			t.Fatalf("CommitSubscribe failed for %s: %v", sessionID, err)
		}
	}
	return state, queue
}

// TestFanoutRecipientEquality_TwoGatewaysSameSensorID verifies strict namespace isolation
// across two gateways sharing the same sensor ID produces identical recipients in indexed and scan modes.
func TestFanoutRecipientEquality_TwoGatewaysSameSensorID(t *testing.T) {
	for _, mode := range []FanoutMode{FanoutModeIndexed, FanoutModeScan} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := DefaultHubConfig()
			cfg.FanoutMode = mode
			hub := NewHub(cfg)
			defer func() { _ = hub.Close() }()

			_, qA := setupSessionWithTargets(t, hub, "viewer-gw1", []TargetKey{{GatewayID: "gw-1", SensorID: "sensor-x"}})
			_, qB := setupSessionWithTargets(t, hub, "viewer-gw2", []TargetKey{{GatewayID: "gw-2", SensorID: "sensor-x"}})

			evt1 := createTestEvent(t, "gw-1", "sensor-x", 64)
			delivered1, err := hub.FanOut(evt1)
			if err != nil {
				t.Fatalf("mode %s: FanOut evt1 failed: %v", mode, err)
			}
			if delivered1 != 1 {
				t.Fatalf("mode %s: expected delivered=1, got %d", mode, delivered1)
			}
			if qA.QueuedSlots() != 1 || qB.QueuedSlots() != 0 {
				t.Fatalf("mode %s: expected qA=1 qB=0, got qA=%d qB=%d", mode, qA.QueuedSlots(), qB.QueuedSlots())
			}

			evt2 := createTestEvent(t, "gw-2", "sensor-x", 64)
			delivered2, err := hub.FanOut(evt2)
			if err != nil {
				t.Fatalf("mode %s: FanOut evt2 failed: %v", mode, err)
			}
			if delivered2 != 1 {
				t.Fatalf("mode %s: expected delivered=1, got %d", mode, delivered2)
			}
			if qA.QueuedSlots() != 1 || qB.QueuedSlots() != 1 {
				t.Fatalf("mode %s: expected qA=1 qB=1, got qA=%d qB=%d", mode, qA.QueuedSlots(), qB.QueuedSlots())
			}
		})
	}
}

// TestFanoutRecipientEquality_FilterCombinations tests:
// Gatewaywide, Sensor-specific, Overlap (Gatewaywide + Sensor), Nonmatching, and No-subscription
// across both indexed and scan modes, ensuring recipient equality and single delivery per session.
func TestFanoutRecipientEquality_FilterCombinations(t *testing.T) {
	for _, cap := range []int{32, 64} {
		t.Run(fmt.Sprintf("cap_%d", cap), func(t *testing.T) {
			results := make(map[FanoutMode]map[string]int)

			for _, mode := range []FanoutMode{FanoutModeIndexed, FanoutModeScan} {
				cfg := HubConfig{
					MaxSessions:              cap,
					MaxQueueSlots:            DefaultSessionQueueSlots,
					MaxQueueBytes:            DefaultSessionQueueBytes,
					MaxFrameBytes:            DefaultMaxEventBytes,
					MaxSessionReferenceBytes: DefaultSessionReferenceBytes,
					FanoutMode:               mode,
				}
				hub := NewHub(cfg)

				// Session A: Gateway-wide (gw-1, all sensors)
				_, qA := setupSessionWithTargets(t, hub, "viewer-gw-wide", []TargetKey{{GatewayID: "gw-1", SensorID: ""}})

				// Session B: Sensor-specific (gw-1, temp)
				_, qB := setupSessionWithTargets(t, hub, "viewer-sensor-spec", []TargetKey{{GatewayID: "gw-1", SensorID: "temp"}})

				// Session C: Overlapping (gw-1, all sensors AND gw-1, temp)
				_, qC := setupSessionWithTargets(t, hub, "viewer-overlap", []TargetKey{
					{GatewayID: "gw-1", SensorID: ""},
					{GatewayID: "gw-1", SensorID: "temp"},
				})

				// Session D: Non-matching (gw-1, humidity AND gw-2, temp)
				_, qD := setupSessionWithTargets(t, hub, "viewer-nonmatch", []TargetKey{
					{GatewayID: "gw-1", SensorID: "humidity"},
					{GatewayID: "gw-2", SensorID: "temp"},
				})

				// Session E: No subscriptions
				_, qE := setupSessionWithTargets(t, hub, "viewer-nosub", nil)

				// Fire event for gw-1: temp
				evt := createTestEvent(t, "gw-1", "temp", 128)
				delivered, err := hub.FanOut(evt)
				if err != nil {
					t.Fatalf("mode %s: FanOut failed: %v", mode, err)
				}

				if delivered != 3 {
					t.Fatalf("mode %s: expected 3 deliveries, got %d", mode, delivered)
				}

				counts := map[string]int{
					"gw-wide":     qA.QueuedSlots(),
					"sensor-spec": qB.QueuedSlots(),
					"overlap":     qC.QueuedSlots(),
					"nonmatch":    qD.QueuedSlots(),
					"nosub":       qE.QueuedSlots(),
				}
				results[mode] = counts

				// Single delivery check: overlap session C must have exactly 1 event
				if counts["overlap"] != 1 {
					t.Fatalf("mode %s: overlapping session C received %d events, want exactly 1", mode, counts["overlap"])
				}
				if counts["nonmatch"] != 0 {
					t.Fatalf("mode %s: non-matching session D received %d events, want 0", mode, counts["nonmatch"])
				}
				if counts["nosub"] != 0 {
					t.Fatalf("mode %s: no-sub session E received %d events, want 0", mode, counts["nosub"])
				}

				_ = hub.Close()
			}

			// Assert recipient equality between indexed and scan modes
			indexedCounts := results[FanoutModeIndexed]
			scanCounts := results[FanoutModeScan]
			for k, v := range indexedCounts {
				if scanCounts[k] != v {
					t.Fatalf("cap %d mismatch for %s: indexed=%d, scan=%d", cap, k, v, scanCounts[k])
				}
			}
		})
	}
}

// TestFanoutRecipientEquality_MetricsNil verifies that FanOut executes identically
// and safely without panics when Metrics observer is nil in both modes.
func TestFanoutRecipientEquality_MetricsNil(t *testing.T) {
	for _, mode := range []FanoutMode{FanoutModeIndexed, FanoutModeScan} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := DefaultHubConfig()
			cfg.FanoutMode = mode
			hub := NewHub(cfg)
			hub.SetMetrics(nil) // explicitly ensure nil
			defer func() { _ = hub.Close() }()

			_, q := setupSessionWithTargets(t, hub, "viewer-1", []TargetKey{{GatewayID: "gw-1", SensorID: "s-1"}})

			evt := createTestEvent(t, "gw-1", "s-1", 64)
			delivered, err := hub.FanOut(evt)
			if err != nil {
				t.Fatalf("mode %s: FanOut failed with nil metrics: %v", mode, err)
			}
			if delivered != 1 || q.QueuedSlots() != 1 {
				t.Fatalf("mode %s: delivered=%d, queued=%d, want 1/1", mode, delivered, q.QueuedSlots())
			}
		})
	}
}

// TestFanoutRecipientEquality_RepresentativeUnsubscribe verifies that unsubscribing
// prevents delivery equally in both indexed and scan modes.
func TestFanoutRecipientEquality_RepresentativeUnsubscribe(t *testing.T) {
	for _, mode := range []FanoutMode{FanoutModeIndexed, FanoutModeScan} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := DefaultHubConfig()
			cfg.FanoutMode = mode
			hub := NewHub(cfg)
			defer func() { _ = hub.Close() }()

			_, q := setupSessionWithTargets(t, hub, "viewer-unsub", []TargetKey{{GatewayID: "gw-1", SensorID: "s-1"}})

			// Unsubscribe
			removed, err := hub.Unsubscribe("viewer-unsub", []TargetKey{{GatewayID: "gw-1", SensorID: "s-1"}})
			if err != nil || len(removed) != 1 {
				t.Fatalf("mode %s: unsubscribe failed: removed=%v err=%v", mode, removed, err)
			}

			// Fan-out event
			evt := createTestEvent(t, "gw-1", "s-1", 64)
			delivered, err := hub.FanOut(evt)
			if err != nil {
				t.Fatalf("mode %s: FanOut failed: %v", mode, err)
			}
			if delivered != 0 || q.QueuedSlots() != 0 {
				t.Fatalf("mode %s: expected 0 delivery after unsub, got delivered=%d queued=%d", mode, delivered, q.QueuedSlots())
			}
		})
	}
}

// TestFanoutRecipientEquality_StaleFanoutSnapshot verifies that when a session unsubscribes
// or closes during the snapshot seam, Step 2 revalidation drops delivery equally in both modes.
func TestFanoutRecipientEquality_StaleFanoutSnapshot(t *testing.T) {
	for _, mode := range []FanoutMode{FanoutModeIndexed, FanoutModeScan} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := DefaultHubConfig()
			cfg.FanoutMode = mode
			hub := NewHub(cfg)
			defer func() { _ = hub.Close() }()

			_, qA := setupSessionWithTargets(t, hub, "viewer-stay", []TargetKey{{GatewayID: "gw-1", SensorID: "s-1"}})
			_, qB := setupSessionWithTargets(t, hub, "viewer-unsub-in-seam", []TargetKey{{GatewayID: "gw-1", SensorID: "s-1"}})

			// In test seam after Step 1 snapshot, unsubscribe viewer-unsub-in-seam
			hub.afterFanOutSnapshot = func() {
				_, _ = hub.Unsubscribe("viewer-unsub-in-seam", []TargetKey{{GatewayID: "gw-1", SensorID: "s-1"}})
			}

			evt := createTestEvent(t, "gw-1", "s-1", 64)
			delivered, err := hub.FanOut(evt)
			if err != nil {
				t.Fatalf("mode %s: FanOut failed: %v", mode, err)
			}

			// Only viewer-stay should receive the event
			if delivered != 1 {
				t.Fatalf("mode %s: expected 1 delivery, got %d", mode, delivered)
			}
			if qA.QueuedSlots() != 1 {
				t.Fatalf("mode %s: viewer-stay queued=%d, want 1", mode, qA.QueuedSlots())
			}
			if qB.QueuedSlots() != 0 {
				t.Fatalf("mode %s: viewer-unsub-in-seam queued=%d, want 0", mode, qB.QueuedSlots())
			}
		})
	}
}

// TestFanoutRecipientEquality_SlowQueuePolicy verifies that a session with full queue
// is isolated and closed with RevocationReasonSlowConsumer identically in indexed and scan modes.
func TestFanoutRecipientEquality_SlowQueuePolicy(t *testing.T) {
	for _, mode := range []FanoutMode{FanoutModeIndexed, FanoutModeScan} {
		t.Run(string(mode), func(t *testing.T) {
			m := &Metrics{}
			cfg := DefaultHubConfig()
			cfg.MaxQueueSlots = 1 // only 1 slot
			cfg.FanoutMode = mode
			hub := NewHub(cfg)
			hub.SetMetrics(m)
			defer func() { _ = hub.Close() }()

			sA, qA := setupSessionWithTargets(t, hub, "viewer-slow", []TargetKey{{GatewayID: "gw-1", SensorID: "s-1"}})
			_, qB := setupSessionWithTargets(t, hub, "viewer-healthy", []TargetKey{{GatewayID: "gw-1", SensorID: "s-1"}})

			// Fill viewer-slow's queue to capacity (1 slot)
			evt1 := createTestEvent(t, "gw-1", "s-1", 64)
			if err := qA.Enqueue(evt1); err != nil {
				t.Fatalf("failed to fill queue: %v", err)
			}

			// Now send evt2 via FanOut: viewer-slow overflows, viewer-healthy receives
			evt2 := createTestEvent(t, "gw-1", "s-1", 64)
			delivered, err := hub.FanOut(evt2)
			if err != nil {
				t.Fatalf("mode %s: FanOut failed: %v", mode, err)
			}

			if delivered != 1 {
				t.Fatalf("mode %s: delivered=%d, want 1 (only healthy)", mode, delivered)
			}
			if qB.QueuedSlots() != 1 {
				t.Fatalf("mode %s: healthy queue=%d, want 1", mode, qB.QueuedSlots())
			}

			// Verify viewer-slow is closed with slow consumer reason
			if !sA.IsClosed() {
				t.Fatalf("mode %s: slow session was not closed", mode)
			}
			snap := m.Snapshot(hub)
			if snap.SlowCloses != 1 {
				t.Fatalf("mode %s: slow closes=%d, want 1", mode, snap.SlowCloses)
			}
		})
	}
}

// TestFanoutRecipientEquality_AggregateExhaustionPolicy verifies that when aggregate reference
// budget is exhausted, affected sessions are closed with RevocationReasonAggregateCapacity,
// single delivery is guaranteed for non-failing sessions, and no fairness claim is made on ordering.
func TestFanoutRecipientEquality_AggregateExhaustionPolicy(t *testing.T) {
	for _, mode := range []FanoutMode{FanoutModeIndexed, FanoutModeScan} {
		t.Run(string(mode), func(t *testing.T) {
			m := &Metrics{}
			evt := createTestEvent(t, "gw-1", "s-1", 1024)

			// Set budget to exactly 1 event's size: only 1 session can enqueue
			cfg := DefaultHubConfig()
			cfg.MaxSessionReferenceBytes = evt.ByteSize()
			cfg.FanoutMode = mode
			hub := NewHub(cfg)
			hub.SetMetrics(m)
			defer func() { _ = hub.Close() }()

			s1, q1 := setupSessionWithTargets(t, hub, "viewer-1", []TargetKey{{GatewayID: "gw-1", SensorID: "s-1"}})
			s2, q2 := setupSessionWithTargets(t, hub, "viewer-2", []TargetKey{{GatewayID: "gw-1", SensorID: "s-1"}})

			delivered, err := hub.FanOut(evt)
			if err != nil {
				t.Fatalf("mode %s: FanOut failed: %v", mode, err)
			}

			// Exactly 1 session delivered, 1 session victim
			if delivered != 1 {
				t.Fatalf("mode %s: delivered=%d, want 1", mode, delivered)
			}

			// Check that exactly one was closed with aggregate capacity reason
			// and the other received single delivery. No fairness claim on which one.
			s1Closed := s1.IsClosed()
			s2Closed := s2.IsClosed()
			if s1Closed == s2Closed {
				t.Fatalf("mode %s: expected exactly one session closed, got s1Closed=%v, s2Closed=%v", mode, s1Closed, s2Closed)
			}

			var deliveredQueue *SessionQueue
			if s1Closed {
				deliveredQueue = q2
			} else {
				deliveredQueue = q1
			}

			snap := m.Snapshot(hub)
			if snap.AggregateCapacityCloses != 1 {
				t.Fatalf("mode %s: aggregate capacity closes=%d, want 1", mode, snap.AggregateCapacityCloses)
			}
			if deliveredQueue.QueuedSlots() != 1 {
				t.Fatalf("mode %s: surviving queue slots=%d, want 1", mode, deliveredQueue.QueuedSlots())
			}
		})
	}
}

// TestFanoutSelectionPathObservation verifies:
// 1. Effective mode is exposed in MetricsSnapshot.FanoutMode and hub.FanoutMode()
// 2. In indexed mode: FanoutIndexedSelections increments, FanoutScanEvaluations is 0.
// 3. In scan mode: FanoutScanEvaluations increments by active session count, FanoutIndexedSelections is 0.
// 4. No per-identity labels, no invented timing/capacity numbers.
func TestFanoutSelectionPathObservation(t *testing.T) {
	evt := createTestEvent(t, "gw-1", "s-1", 64)

	// Indexed mode
	{
		m := &Metrics{}
		cfg := DefaultHubConfig()
		cfg.FanoutMode = FanoutModeIndexed
		hub := NewHub(cfg)
		hub.SetMetrics(m)

		for i := 0; i < 4; i++ {
			setupSessionWithTargets(t, hub, fmt.Sprintf("session-%d", i), []TargetKey{{GatewayID: "gw-1", SensorID: "s-1"}})
		}

		if hub.FanoutMode() != FanoutModeIndexed {
			t.Fatalf("hub.FanoutMode()=%q, want %q", hub.FanoutMode(), FanoutModeIndexed)
		}

		delivered, err := hub.FanOut(evt)
		if err != nil || delivered != 4 {
			t.Fatalf("indexed FanOut: delivered=%d err=%v", delivered, err)
		}

		snap := m.Snapshot(hub)
		if snap.FanoutMode != string(FanoutModeIndexed) {
			t.Fatalf("snapshot.FanoutMode=%q, want %q", snap.FanoutMode, FanoutModeIndexed)
		}
		if snap.FanoutIndexedSelections != 1 {
			t.Fatalf("snapshot.FanoutIndexedSelections=%d, want 1", snap.FanoutIndexedSelections)
		}
		if snap.FanoutScanEvaluations != 0 {
			t.Fatalf("snapshot.FanoutScanEvaluations=%d, want 0", snap.FanoutScanEvaluations)
		}
		_ = hub.Close()
	}

	// Scan mode
	{
		m := &Metrics{}
		cfg := DefaultHubConfig()
		cfg.FanoutMode = FanoutModeScan
		hub := NewHub(cfg)
		hub.SetMetrics(m)

		for i := 0; i < 4; i++ {
			setupSessionWithTargets(t, hub, fmt.Sprintf("session-%d", i), []TargetKey{{GatewayID: "gw-1", SensorID: "s-1"}})
		}

		if hub.FanoutMode() != FanoutModeScan {
			t.Fatalf("hub.FanoutMode()=%q, want %q", hub.FanoutMode(), FanoutModeScan)
		}

		delivered, err := hub.FanOut(evt)
		if err != nil || delivered != 4 {
			t.Fatalf("scan FanOut: delivered=%d err=%v", delivered, err)
		}

		snap := m.Snapshot(hub)
		if snap.FanoutMode != string(FanoutModeScan) {
			t.Fatalf("snapshot.FanoutMode=%q, want %q", snap.FanoutMode, FanoutModeScan)
		}
		if snap.FanoutIndexedSelections != 0 {
			t.Fatalf("snapshot.FanoutIndexedSelections=%d, want 0", snap.FanoutIndexedSelections)
		}
		// 4 active sessions scanned: FanoutScanEvaluations must be 4
		if snap.FanoutScanEvaluations != 4 {
			t.Fatalf("snapshot.FanoutScanEvaluations=%d, want 4", snap.FanoutScanEvaluations)
		}
		_ = hub.Close()
	}
}

// TestFanoutCloseObservationRingUnchanged verifies that close-observation ring size remains exactly 32.
func TestFanoutCloseObservationRingUnchanged(t *testing.T) {
	m := &Metrics{}
	cfg := DefaultHubConfig()
	hub := NewHub(cfg)
	hub.SetMetrics(m)
	defer func() { _ = hub.Close() }()

	// Induce 40 close observations (more than 32 ring capacity)
	for i := 0; i < 40; i++ {
		m.sessionTerminated(CloseObservation{
			Reason:    "client_close",
			Operation: "cleanup",
		})
	}

	snap := m.Snapshot(hub)
	if len(snap.CloseObservations) != 32 {
		t.Fatalf("expected close-observation ring to hold at most 32 entries, got %d", len(snap.CloseObservations))
	}
}

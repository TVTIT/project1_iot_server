package telemetryws

import (
	"context"
	"errors"
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
	if got := m.Snapshot(hub).SessionQueuedInFlightBytes; got != 37 {
		t.Fatalf("queued/in-flight/control bytes=%d, want 37", got)
	}
	reservation.Release()
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

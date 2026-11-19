package telemetryws

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
)

func closeEvidence(t *testing.T, m *Metrics, h *Hub) map[string]any {
	t.Helper()
	b, err := json.Marshal(m.Snapshot(h))
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if err = json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestTerminationPingTimeoutAndHealthyReader(t *testing.T) {
	for _, healthy := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_reader", true: "healthy_reader"}[healthy], func(t *testing.T) {
			hub := NewHub(DefaultHubConfig())
			m := &Metrics{}
			hub.SetMetrics(m)
			s := newApplicationSocket(t, auth.Principal{UserID: uuid.New(), ExpiresAt: time.Now().Add(time.Minute)}, hub, allowAllTargets{}, HandlerOptions{PingInterval: 30 * time.Millisecond, PongTimeout: 150 * time.Millisecond})
			s.send(t, "ready", ActionSubscribe, TargetKey{GatewayID: "gw-1"})
			_ = s.read(t)
			readDone := make(chan struct{})
			if healthy {
				go func() { defer close(readDone); _, _, _ = s.conn.Read(context.Background()) }()
			}
			deadline := time.Now().Add(350 * time.Millisecond)
			for time.Now().Before(deadline) && (healthy || hub.SessionCount() != 0) {
				time.Sleep(time.Millisecond)
			}
			v := closeEvidence(t, m, hub)
			counts, ok := v["close_counts"].(map[string]any)
			if !ok {
				t.Fatal("missing fixed close counters")
			}
			if healthy {
				if hub.SessionCount() != 1 || counts["ping_timeout"] != float64(0) {
					t.Fatalf("healthy peer: %v", v)
				}
				_ = s.h.Shutdown(context.Background())
				<-readDone
			} else if counts["ping_timeout"] != float64(1) {
				t.Fatalf("missing ping decision evidence: %v", v)
			}
		})
	}
}

func TestTerminationFirstCauseSurvivesCleanup(t *testing.T) {
	m := &Metrics{}
	s := &appSession{acceptedAt: time.Now(), metrics: m}
	s.recordTermination(context.Background(), nil, RevocationReasonPingTimeout, "ping", context.DeadlineExceeded)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.recordTermination(context.Background(), nil, RevocationReasonClientClose, "cleanup", nil)
		}()
	}
	wg.Wait()
	v := closeEvidence(t, m, NewHub(DefaultHubConfig()))
	counts := v["close_counts"].(map[string]any)
	if counts["ping_timeout"] != float64(1) || counts["client_close"] != float64(0) {
		t.Fatalf("first cause overwritten: %v", v)
	}
	// Metrics-off follows the same first-cause path without panic.
	s.metrics = nil
	s.recordTermination(context.Background(), nil, RevocationReasonClientClose, "cleanup", nil)
}

func TestTerminationIOClassificationAndBound(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	deadline, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	for _, tc := range []struct {
		name, op, want    string
		parent, operation context.Context
		err               error
	}{
		{"write_deadline", "write", "write_timeout", context.Background(), deadline, context.DeadlineExceeded},
		{"parent_canceled", "write", "context_canceled", parent, deadline, context.DeadlineExceeded},
		{"eof", "read", "read_eof", context.Background(), nil, io.EOF},
		{"unexpected_eof", "read", "read_eof", context.Background(), nil, io.ErrUnexpectedEOF},
		{"remote_close", "read", "remote_close", context.Background(), nil, websocket.CloseError{Code: websocket.StatusNormalClosure, Reason: "never retain peer text"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &Metrics{}
			s := &appSession{acceptedAt: time.Now(), metrics: m}
			s.recordIOError(tc.parent, tc.operation, tc.op, tc.err)
			if got := m.Snapshot(NewHub(DefaultHubConfig())).CloseCounts[tc.want]; got != 1 {
				t.Fatalf("%s count=%d", tc.want, got)
			}
		})
	}
	m := &Metrics{}
	for i := 0; i < 40; i++ {
		s := &appSession{acceptedAt: time.Now(), metrics: m}
		s.recordTermination(context.Background(), nil, RevocationReasonShutdown, "shutdown", nil)
	}
	snap := m.Snapshot(NewHub(DefaultHubConfig()))
	if len(snap.CloseObservations) != 32 || snap.CloseCounts["shutdown"] != 40 {
		t.Fatalf("unbounded or lost counts: %+v", snap)
	}
	(&appSession{acceptedAt: time.Now()}).recordIOError(context.Background(), deadline, "write", context.DeadlineExceeded)
}

func TestTerminationAccessAndShutdown(t *testing.T) {
	for _, tc := range []struct {
		reason RevocationReason
		want   string
	}{
		{RevocationReasonSlowConsumer, "queue_overflow"}, {RevocationReasonDenied, "membership_revoked"},
		{RevocationReasonDBError, "authorization_error"}, {RevocationReasonExpired, "jwt_expired"},
		{RevocationReasonShutdown, "shutdown"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			hub := NewHub(DefaultHubConfig())
			m := &Metrics{}
			hub.SetMetrics(m)
			s := newApplicationSocket(t, auth.Principal{UserID: uuid.New(), ExpiresAt: time.Now().Add(time.Minute)}, hub, allowAllTargets{}, HandlerOptions{})
			s.send(t, "ready", ActionSubscribe, TargetKey{GatewayID: "gw-1"})
			_ = s.read(t)
			if tc.reason == RevocationReasonShutdown {
				_ = s.h.Shutdown(context.Background())
			} else {
				hub.CloseAllSessions(tc.reason)
				_ = s.h.Shutdown(context.Background())
			}
			if got := m.Snapshot(hub).CloseCounts[tc.want]; got != 1 {
				t.Fatalf("reason=%s count=%d snapshot=%+v", tc.want, got, m.Snapshot(hub))
			}
		})
	}
}

func TestTerminationLoopbackRemoteCloseAndEOF(t *testing.T) {
	for _, normal := range []bool{false, true} {
		t.Run(map[bool]string{false: "abrupt_eof", true: "close_frame"}[normal], func(t *testing.T) {
			hub := NewHub(DefaultHubConfig())
			m := &Metrics{}
			hub.SetMetrics(m)
			s := newApplicationSocket(t, auth.Principal{UserID: uuid.New(), ExpiresAt: time.Now().Add(time.Minute)}, hub, allowAllTargets{}, HandlerOptions{})
			s.send(t, "ready", ActionSubscribe, TargetKey{GatewayID: "gw-1"})
			_ = s.read(t)
			if normal {
				_ = s.conn.Close(websocket.StatusNormalClosure, "private text")
			} else {
				_ = s.conn.CloseNow()
			}
			deadline := time.Now().Add(time.Second)
			for hub.SessionCount() != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			_ = s.h.Shutdown(context.Background())
			want := "read_eof"
			if normal {
				want = "remote_close"
			}
			if got := m.Snapshot(hub).CloseCounts[want]; got != 1 {
				t.Fatalf("%s count=%d snap=%+v", want, got, m.Snapshot(hub))
			}
		})
	}
}

func TestTerminationLoopbackWriteDeadlineVersusParentCancel(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "parent_cancel"}[canceled], func(t *testing.T) {
			hub := NewHub(DefaultHubConfig())
			sock := newApplicationSocket(t, auth.Principal{UserID: uuid.New(), ExpiresAt: time.Now().Add(time.Minute)}, hub, allowAllTargets{}, HandlerOptions{})
			sock.send(t, "ready", ActionSubscribe, TargetKey{GatewayID: "gw-1"})
			_ = sock.read(t)
			sock.h.mu.Lock()
			var serverConn *websocket.Conn
			for conn := range sock.h.connections {
				serverConn = conn
			}
			sock.h.mu.Unlock()
			m := &Metrics{}
			s := &appSession{conn: serverConn, metrics: m, acceptedAt: time.Now()}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if canceled {
				cancel()
			}
			// Direct socket helper stress only: bypasses the production frame cap to
			// deterministically fill the loopback TCP send window without sleeps.
			err := writeApplicationFrame(ctx, s, 30*time.Millisecond, make([]byte, 16<<20))
			if err == nil {
				t.Fatal("non-reading socket did not reach its write deadline")
			}
			want := "write_timeout"
			if canceled {
				want = "context_canceled"
			}
			if got := m.Snapshot(hub).CloseCounts[want]; got != 1 {
				t.Fatalf("%s count=%d snapshot=%+v", want, got, m.Snapshot(hub))
			}
		})
	}
}

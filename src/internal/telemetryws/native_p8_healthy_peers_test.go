package telemetryws

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"iot-platform/internal/auth"
)

func TestNativeP8HealthyPeersProbeBehavior(t *testing.T) {
	t.Run("receives two authenticated TLS peers and joins on repeated stop", func(t *testing.T) {
		var active atomic.Int32
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer token-a" && r.Header.Get("Authorization") != "Bearer token-b" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			active.Add(1)
			defer active.Add(-1)
			conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{auth.WSTelemetryProtocol}})
			if err != nil {
				return
			}
			defer func() { _ = conn.CloseNow() }()
			_, raw, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var req ClientMessage
			if json.Unmarshal(raw, &req) != nil {
				return
			}
			wantToken := "token-a"
			if req.Targets[0].GatewayID == "gw-b" {
				wantToken = "token-b"
			}
			if r.Header.Get("Authorization") != "Bearer "+wantToken {
				return
			}
			ack, _ := json.Marshal(NewAckMessage(req.RequestID, req.Action, req.Targets))
			if conn.Write(r.Context(), websocket.MessageText, ack) != nil {
				return
			}
			gw, sensor := req.Targets[0].GatewayID, req.Targets[0].SensorID
			for i := 0; i < 2; i++ {
				event, _ := json.Marshal(TelemetryEventDTO{ProtocolVersion: ProtocolVersion, MessageType: MessageTypeTelemetry, EntityID: (TargetKey{GatewayID: gw, SensorID: sensor}).EntityURN(), GatewayID: gw, SensorID: sensor, MessageID: "same-message", BootID: "boot", FirstSequence: "1", SampleCount: 1, MeasuredAt: "2026-10-28T00:00:00Z", SampleIntervalUS: 1000000, Samples: []float64{1}})
				if conn.Write(r.Context(), websocket.MessageText, event) != nil {
					return
				}
			}
			for {
				if _, _, err := conn.Read(r.Context()); err != nil {
					return
				}
			}
		}))
		defer server.Close()
		cfg, dir := healthyPeerTestConfig(t, server)
		resultCh := make(chan healthyPeersState, 1)
		go func() { resultCh <- runNativeP8HealthyPeers(cfg) }()
		waitHealthyPeersReady(t, cfg.ReadyPath)
		waitHealthyPeerEvents(t, cfg.StatePath, 4)
		if err := os.WriteFile(cfg.StopPath, nil, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cfg.StopPath, nil, 0600); err != nil {
			t.Fatal(err)
		}
		select {
		case result := <-resultCh:
			if result.Failure != "" || result.SocketCount != 2 || result.Events != 4 || result.Duplicates != 2 {
				t.Fatalf("unexpected healthy-peer result: %+v", result)
			}
		case <-time.After(4 * time.Second):
			t.Fatal("stop did not close sockets and join readers")
		}
		closeDeadline := time.Now().Add(time.Second)
		for active.Load() != 0 && time.Now().Before(closeDeadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if active.Load() != 0 {
			t.Fatalf("server retained %d peer sockets", active.Load())
		}
		if _, err := os.Stat(filepath.Join(dir, "result.json")); !os.IsNotExist(err) {
			t.Fatalf("runner unexpectedly owns orchestrator result path: %v", err)
		}
	})

	t.Run("unexpected namespace event fails", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{auth.WSTelemetryProtocol}})
			if err != nil {
				return
			}
			defer func() { _ = conn.CloseNow() }()
			_, raw, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var req ClientMessage
			if json.Unmarshal(raw, &req) != nil {
				return
			}
			ack, _ := json.Marshal(NewAckMessage(req.RequestID, req.Action, req.Targets))
			_ = conn.Write(r.Context(), websocket.MessageText, ack)
			gw, sensor := req.Targets[0].GatewayID, req.Targets[0].SensorID
			if gw == "gw-b" {
				gw, sensor = "gw-a", "sensor-a"
			}
			event, _ := json.Marshal(TelemetryEventDTO{ProtocolVersion: ProtocolVersion, MessageType: MessageTypeTelemetry, EntityID: (TargetKey{GatewayID: gw, SensorID: sensor}).EntityURN(), GatewayID: gw, SensorID: sensor, MessageID: "cross-namespace", BootID: "boot", FirstSequence: "1", SampleCount: 1, MeasuredAt: "2026-10-28T00:00:00Z", SampleIntervalUS: 1000000, Samples: []float64{1}})
			_ = conn.Write(r.Context(), websocket.MessageText, event)
			for {
				if _, _, err := conn.Read(r.Context()); err != nil {
					return
				}
			}
		}))
		defer server.Close()
		cfg, _ := healthyPeerTestConfig(t, server)
		resultCh := make(chan healthyPeersState, 1)
		go func() { resultCh <- runNativeP8HealthyPeers(cfg) }()
		waitHealthyPeersReady(t, cfg.ReadyPath)
		waitHealthyPeerEvents(t, cfg.StatePath, 1)
		if err := os.WriteFile(cfg.StopPath, nil, 0600); err != nil {
			t.Fatal(err)
		}
		select {
		case result := <-resultCh:
			if result.Failure == "" || result.Unexpected == 0 {
				t.Fatalf("cross-namespace event was not rejected: %+v", result)
			}
		case <-time.After(4 * time.Second):
			t.Fatal("probe did not stop")
		}
	})
}

func healthyPeerTestConfig(t *testing.T, server *httptest.Server) (healthyPeersConfig, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	u := "wss" + strings.TrimPrefix(server.URL, "https")
	cfg := healthyPeersConfig{URL: u, CAFile: caPath, Tokens: map[string]string{"A": "token-a", "B": "token-b"}, Peers: []healthyPeerSpec{{ID: "peer-a", Role: "A", Target: TargetKey{GatewayID: "gw-a", SensorID: "sensor-a"}}, {ID: "peer-b", Role: "B", Target: TargetKey{GatewayID: "gw-b", SensorID: "sensor-b"}}}, MaxLifetimeMS: 10000, MaxBytes: 1 << 20, MaxEvents: 100, ReadyPath: filepath.Join(dir, "ready.json"), StatePath: filepath.Join(dir, "state.json"), StopPath: filepath.Join(dir, "stop"), OutputPath: filepath.Join(dir, "result.json")}
	// The private CA verifies the TLS certificate; URL's IP is the httptest loopback literal.
	if err := writePrivateJSON(filepath.Join(dir, "config.json"), cfg); err != nil {
		t.Fatal(err)
	}
	return cfg, dir
}

func waitHealthyPeersReady(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("healthy peers were not ready")
}

func waitHealthyPeerEvents(t *testing.T, path string, events int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		var state healthyPeersState
		if err == nil && json.Unmarshal(b, &state) == nil && state.Events >= events {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("healthy-peer event count was not observed")
}

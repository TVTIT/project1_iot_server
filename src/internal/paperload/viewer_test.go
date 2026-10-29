package paperload

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"iot-platform/internal/telemetryws"
)

func TestViewerACKAndCompositeTargetMatching(t *testing.T) {
	targets := []telemetryws.TargetKey{{GatewayID: "gw-a", SensorID: "s-a"}, {GatewayID: "gw-a"}}
	b, _ := json.Marshal(struct {
		ProtocolVersion int64                   `json:"protocol_version"`
		MessageType     string                  `json:"message_type"`
		RequestID       string                  `json:"request_id"`
		Action          string                  `json:"action"`
		Targets         []telemetryws.TargetKey `json:"targets"`
	}{1, "ack", "r1", "subscribe", targets})
	if !validViewerACK(b, "r1", targets) || !viewerMatches(targets, "gw-a", "s-a") || viewerMatches(targets, "gw-b", "s-a") {
		t.Fatal("valid ack/composite filter rejected")
	}
	if validViewerACK([]byte(`{"message_type":"ack"}`), "r1", targets) {
		t.Fatal("malformed ACK accepted")
	}
}

func TestViewerTLSRunValidatesSubscribeAndCountsIdentity(t *testing.T) {
	var active, maxActive atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/ws" || r.Header.Get("Authorization") == "" {
			t.Errorf("unexpected handshake")
		}
		c, e := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"iot.telemetry.v1"}})
		if e != nil {
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "done") }()
		current := active.Add(1)
		for {
			old := maxActive.Load()
			if current <= old || maxActive.CompareAndSwap(old, current) {
				break
			}
		}
		_, req, e := c.Read(r.Context())
		if e != nil {
			return
		}
		var sub struct {
			RequestID string                  `json:"request_id"`
			Targets   []telemetryws.TargetKey `json:"targets"`
		}
		if json.Unmarshal(req, &sub) != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
		ack, _ := json.Marshal(struct {
			ProtocolVersion int64                   `json:"protocol_version"`
			MessageType     string                  `json:"message_type"`
			RequestID       string                  `json:"request_id"`
			Action          string                  `json:"action"`
			Targets         []telemetryws.TargetKey `json:"targets"`
		}{1, "ack", sub.RequestID, "subscribe", sub.Targets})
		_ = c.Write(r.Context(), websocket.MessageText, ack)
		active.Add(-1)
		event := fmt.Sprintf(`{"protocol_version":1,"message_type":"telemetry","entity_id":"urn:ngsi-ld:Sensor:gw-a:s-a","gateway_id":"gw-a","sensor_id":"s-a","message_id":"00000000-0000-4000-8000-000000000001","boot_id":"00000000-0000-4000-8000-000000000002","first_sequence":"%s","sample_count":1,"measured_at":"2026-10-28T00:00:00Z","sample_interval_us":1000000,"samples":[1]}`, strconv.FormatInt(9007199254740993, 10))
		_ = c.Write(r.Context(), websocket.MessageText, []byte(event))
		_, _, _ = c.Read(r.Context()) // remain open until the client performs its joined stop.
	}))
	defer srv.Close()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if os.WriteFile(ca, cert, 0600) != nil {
		t.Fatal("ca")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(cert) {
		t.Fatal("CA parse")
	}
	_ = pool
	token := testViewerJWT("principal-shared", time.Now().Add(time.Hour).Unix())
	dir := t.TempDir()
	private := filepath.Join(dir, "viewer.json")
	identity, _ := json.Marshal(map[string]string{"u1": token, "u2": token, "u3": token, "u4": token, "u5": token})
	if os.WriteFile(private, identity, 0600) != nil {
		t.Fatal("identity")
	}
	host := strings.TrimPrefix(srv.URL, "https://")
	m := Manifest{Experiment: "E1", Arm: "reference", Profile: "pilot", Mode: "remote", RunID: "viewer-test", Topology: Topology{Users: 5, Viewers: 5}, IdentityFiles: map[string]string{"viewer": private}, TLS: TLS{HTTPEndpoint: "https://" + host, HTTPCAFile: ca}, Limits: Limits{Workers: 2, Connections: 5, WallSeconds: 4}, Phases: Phases{MeasureSeconds: 1}, Viewer: ViewerSettings{MaxMessages: 10, MaxBytes: 10000}}
	for i := 1; i <= 5; i++ {
		m.Subscriptions = append(m.Subscriptions, Subscription{ViewerID: fmt.Sprintf("v%d", i), UserID: fmt.Sprintf("u%d", i), GatewayID: "gw-a", SensorID: "s-a"})
	}
	// Endpoint and private references are resolved against this manifest's directory.
	manifest := filepath.Join(dir, "manifest.json")
	if os.WriteFile(manifest, []byte(`{}`), 0600) != nil {
		t.Fatal("manifest")
	}
	result, err := RunViewer(context.Background(), m, manifest, filepath.Join(dir, "out"))
	if err != nil {
		t.Fatalf("run: %v (%+v)", err, result)
	}
	if result.Subscribed != 5 || result.Connected != 5 || result.Unique != 5 || result.PrincipalCount != 1 || result.SocketCount != 5 || result.ConnectionAttempts != 5 || result.EffectiveIdentityWindow != 1024 || maxActive.Load() > 2 {
		t.Fatalf("result %+v", result)
	}
	m.Viewer.Mode = "reconnect"
	m.Viewer.ReconnectCount = 1
	reconnected, err := RunViewer(context.Background(), m, manifest, filepath.Join(dir, "out-reconnect"))
	if err != nil || reconnected.ConnectionAttempts != 10 || reconnected.Reconnects != 5 || reconnected.Connected != 5 || reconnected.Subscribed != 5 || reconnected.Duplicates != 5 {
		t.Fatalf("planned reconnect failed: err=%v result=%+v", err, reconnected)
	}
	cancelCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err = RunViewer(cancelCtx, m, manifest, filepath.Join(dir, "out-cancel"))
	if err == nil {
		t.Fatal("hard stop unexpectedly completed")
	}
}

func TestViewerJWTInsufficientExpiryRejected(t *testing.T) {
	if _, err := viewerClaims(testViewerJWT("u", time.Now().Add(-time.Second).Unix())); err == nil {
		t.Fatal("expired JWT accepted")
	}
}

func TestReadViewerTokensUsesPrepareClientBundleBareMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "role_jwts.json")
	// prepare_client_bundle serializes identities['role_jwts'] directly, not a wrapper object.
	if err := os.WriteFile(path, []byte(`{"operator-label":"jwt-value"}`), 0600); err != nil {
		t.Fatal(err)
	}
	tokens, err := readViewerTokens(path)
	if err != nil || tokens["operator-label"] != "jwt-value" {
		t.Fatalf("tokens=%v err=%v", tokens, err)
	}
}

func TestReadViewerClassifiesMalformedOversizeAndUnexpectedClose(t *testing.T) {
	for _, tc := range []struct {
		name        string
		payload     []byte
		closeOnly   bool
		maxMessages int64
		slowPause   time.Duration
		want        string
	}{
		{name: "malformed", payload: []byte(`{"message_type":"unexpected"}`), maxMessages: 2, want: "malformed_event"},
		{name: "slow_pause", payload: []byte(`{"message_type":"unexpected"}`), maxMessages: 2, slowPause: 40 * time.Millisecond, want: "malformed_event"},
		{name: "wrong_namespace", payload: []byte(`{"protocol_version":1,"message_type":"telemetry","entity_id":"urn:ngsi-ld:Sensor:gw-b:s-b","gateway_id":"gw-b","sensor_id":"s-b","message_id":"00000000-0000-4000-8000-000000000001","boot_id":"00000000-0000-4000-8000-000000000002","first_sequence":"1","sample_count":1,"measured_at":"2026-10-28T00:00:00Z","sample_interval_us":1000000,"samples":[1]}`), maxMessages: 2, want: "wrong_namespace"},
		{name: "frame_limit", payload: []byte(strings.Repeat("x", viewerFrameLimit+1)), maxMessages: 2, want: "frame_limit_exceeded"},
		{name: "message_cap", payload: []byte(`{}`), maxMessages: 0, want: "limit_exceeded"},
		{name: "unexpected_close", closeOnly: true, maxMessages: 2, want: "remote_close_1000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, e := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"iot.telemetry.v1"}})
				if e != nil {
					return
				}
				defer func() { _ = c.Close(websocket.StatusNormalClosure, "test") }()
				if !tc.closeOnly {
					_ = c.Write(r.Context(), websocket.MessageText, tc.payload)
				}
			}))
			defer server.Close()
			conn, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{Subprotocols: []string{"iot.telemetry.v1"}})
			if err != nil {
				t.Fatal(err)
			}
			conn.SetReadLimit(viewerFrameLimit)
			v := &viewerSession{id: "v1", filters: []telemetryws.TargetKey{{GatewayID: "gw-a", SensorID: "s-a"}}}
			sockCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			shared := &viewerShared{result: ViewerResult{CloseReasons: map[string]int{}}, summaries: map[string]*ViewerSummary{"v1": {ID: "v1"}}, maxMessages: tc.maxMessages, maxBytes: 1 << 20, seen: map[string]map[string]struct{}{}, order: map[string][]string{}, receiving: map[string]bool{}}
			sock := &viewerSocket{viewer: v, conn: conn, ctx: sockCtx, cancel: cancel, done: make(chan struct{})}
			errs := make(chan error, 1)
			began := time.Now()
			readViewer(sock, shared, errs, tc.slowPause)
			if tc.slowPause > 0 && time.Since(began) < tc.slowPause {
				t.Fatal("slow mode did not pause reads")
			}
			if tc.closeOnly && len(errs) == 0 {
				t.Fatal("unexpected close not surfaced")
			}
			if !tc.closeOnly && len(errs) == 0 {
				t.Fatal("bad frame/cap not surfaced")
			}
			if shared.result.CloseReasons[tc.want] == 0 {
				t.Fatalf("reason=%v want %s", shared.result.CloseReasons, tc.want)
			}
		})
	}
}

func testViewerJWT(sub string, exp int64) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	payload, _ := json.Marshal(map[string]any{"sub": sub, "exp": exp})
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".signature"
}

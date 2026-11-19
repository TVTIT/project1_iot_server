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
	"sync"
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

func TestViewerPlannedStopCompletesAfterReadyAndEvents(t *testing.T) {
	var served atomic.Int32
	var active atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"iot.telemetry.v1"}})
		if err != nil {
			return
		}
		active.Add(1)
		defer func() {
			_ = c.Close(websocket.StatusNormalClosure, "done")
			active.Add(-1)
		}()
		_, req, err := c.Read(r.Context())
		if err != nil {
			return
		}
		var sub struct {
			RequestID string                  `json:"request_id"`
			Targets   []telemetryws.TargetKey `json:"targets"`
		}
		if json.Unmarshal(req, &sub) != nil {
			return
		}
		ack, _ := json.Marshal(struct {
			ProtocolVersion int64                   `json:"protocol_version"`
			MessageType     string                  `json:"message_type"`
			RequestID       string                  `json:"request_id"`
			Action          string                  `json:"action"`
			Targets         []telemetryws.TargetKey `json:"targets"`
		}{1, "ack", sub.RequestID, "subscribe", sub.Targets})
		if c.Write(r.Context(), websocket.MessageText, ack) != nil {
			return
		}
		event := fmt.Sprintf(`{"protocol_version":1,"message_type":"telemetry","entity_id":"urn:ngsi-ld:Sensor:gw-a:s-a","gateway_id":"gw-a","sensor_id":"s-a","message_id":"00000000-0000-4000-8000-00000000000%d","boot_id":"00000000-0000-4000-8000-000000000002","first_sequence":"1","sample_count":1,"measured_at":"2026-10-28T00:00:00Z","sample_interval_us":1000000,"samples":[1]}`, served.Add(1))
		_ = c.Write(r.Context(), websocket.MessageText, []byte(event))
		_, _, _ = c.Read(r.Context())
	}))
	defer srv.Close()
	dir := t.TempDir()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, cert, 0600); err != nil {
		t.Fatal(err)
	}
	identity := filepath.Join(dir, "viewer.json")
	token := testViewerJWT("principal", time.Now().Add(time.Hour).Unix())
	if err := os.WriteFile(identity, []byte(fmt.Sprintf(`{"u1":%q,"u2":%q}`, token, token)), 0600); err != nil {
		t.Fatal(err)
	}
	host := strings.TrimPrefix(srv.URL, "https://")
	m := Manifest{Experiment: "E5", Arm: "reference", Profile: "pilot", Mode: "remote", RunID: "planned-stop", Topology: Topology{Users: 2, Viewers: 2}, IdentityFiles: map[string]string{"viewer": identity}, TLS: TLS{HTTPEndpoint: "https://" + host, HTTPCAFile: ca}, Limits: Limits{Workers: 2, Connections: 2, WallSeconds: 20}, Phases: Phases{MeasureSeconds: 15}, Viewer: ViewerSettings{MaxMessages: 10, MaxBytes: 10000}}
	for i := 1; i <= 2; i++ {
		m.Subscriptions = append(m.Subscriptions, Subscription{ViewerID: fmt.Sprintf("v%d", i), UserID: fmt.Sprintf("u%d", i), GatewayID: "gw-a", SensorID: "s-a"})
	}
	manifest := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifest, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	resultCh := make(chan struct {
		r   ViewerResult
		err error
	}, 1)
	go func() {
		r, err := RunViewerWithPlannedStop(context.Background(), m, manifest, filepath.Join(dir, "out"), stop)
		resultCh <- struct {
			r   ViewerResult
			err error
		}{r, err}
	}()
	ready := filepath.Join(dir, "out", "ready.json")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil && served.Load() == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("viewers did not become ready and receive their events")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // allow the viewer readers to account for the sent events
	close(stop)
	select {
	case got := <-resultCh:
		if got.err != nil || got.r.Status != "COMPLETE" || got.r.PlannedStopReason != "stopped_by_operator" || got.r.Messages != 2 || len(got.r.IdentitySamples) != 2 || len(got.r.Viewers) != 2 || len(got.r.LifetimesMS) != 2 {
			t.Fatalf("planned stop result=%+v err=%v", got.r, got.err)
		}
		closedBy := time.Now().Add(time.Second)
		for active.Load() != 0 && time.Now().Before(closedBy) {
			time.Sleep(time.Millisecond)
		}
		if active.Load() != 0 {
			t.Fatalf("server still has %d viewer sockets after return", active.Load())
		}
		if _, err := os.Stat(ready); err == nil {
			t.Fatal("ready marker remained after termination")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("planned stop did not close and join viewers promptly")
	}
	early := make(chan struct{})
	close(early)
	failed, err := RunViewerWithPlannedStop(context.Background(), m, manifest, filepath.Join(dir, "out-early"), early)
	if err == nil || failed.Status == "COMPLETE" || failed.PrimaryError != "planned_stop_before_ready" {
		t.Fatalf("premature stop was accepted: result=%+v err=%v", failed, err)
	}
}

func TestViewerPlannedStopDoesNotMaskFailures(t *testing.T) {
	stop := make(chan struct{})
	close(stop)
	errs := make(chan error, 1)
	errs <- fmt.Errorf("transport failure")
	if stopped, ok := viewerWaitWithPlannedStop(context.Background(), time.Hour, errs, stop); stopped || ok {
		t.Fatal("planned stop masked an observed transport failure")
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if stopped, ok := viewerWaitWithPlannedStop(ctx, time.Hour, nil, stop); stopped || ok {
		t.Fatal("planned stop masked an expired wall context")
	}
}

func TestViewerStopDeliveredAtReadyPublicationUsesPlannedPath(t *testing.T) {
	stop := make(chan struct{})
	dir := t.TempDir()
	stoppedBeforeReady, err := publishViewerReady(dir, viewerReady{Status: "ready"}, stop, func() {
		close(stop) // deterministic delivery after the ready marker's publication point
	})
	if err != nil || stoppedBeforeReady {
		t.Fatalf("publication rejected a post-publication stop: stopped=%t err=%v", stoppedBeforeReady, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ready.json")); err != nil {
		t.Fatalf("ready marker was not published: %v", err)
	}
	stopped, ok := viewerWaitWithPlannedStop(context.Background(), time.Hour, nil, stop)
	if !ok || !stopped {
		t.Fatalf("post-publication stop did not take planned path: stopped=%t ok=%t", stopped, ok)
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
		payloads    [][]byte
		closeOnly   bool
		holdOpen    bool
		maxMessages int64
		maxBytes    int64
		slowPause   time.Duration
		want        string
		wantSuccess bool
	}{
		{name: "malformed", payload: []byte(`{"message_type":"unexpected"}`), maxMessages: 2, want: "malformed_event"},
		{name: "slow_pause", payload: []byte(`{"message_type":"unexpected"}`), maxMessages: 2, slowPause: 40 * time.Millisecond, want: "malformed_event"},
		{name: "wrong_namespace", payload: []byte(`{"protocol_version":1,"message_type":"telemetry","entity_id":"urn:ngsi-ld:Sensor:gw-b:s-b","gateway_id":"gw-b","sensor_id":"s-b","message_id":"00000000-0000-4000-8000-000000000001","boot_id":"00000000-0000-4000-8000-000000000002","first_sequence":"1","sample_count":1,"measured_at":"2026-10-28T00:00:00Z","sample_interval_us":1000000,"samples":[1]}`), maxMessages: 2, want: "wrong_namespace"},
		{name: "frame_limit", payload: []byte(strings.Repeat("x", viewerFrameLimit+1)), maxMessages: 2, want: "frame_limit_exceeded"},
		{name: "message_cap", payload: []byte(`{}`), maxMessages: 0, want: "limit_exceeded"},
		{name: "byte_cap", payload: []byte(`{}`), maxMessages: 2, maxBytes: 1, want: "limit_exceeded"},
		{name: "unexpected_close", closeOnly: true, maxMessages: 2, want: "remote_close_1000"},
		{name: "exact_message_cap_planned_stop", payloads: [][]byte{[]byte(`{"protocol_version":1,"message_type":"telemetry","entity_id":"urn:ngsi-ld:Sensor:gw-a:s-a","gateway_id":"gw-a","sensor_id":"s-a","message_id":"00000000-0000-4000-8000-000000000001","boot_id":"00000000-0000-4000-8000-000000000002","first_sequence":"1","sample_count":1,"measured_at":"2026-10-28T00:00:00Z","sample_interval_us":1000000,"samples":[20.01]}`)}, holdOpen: true, maxMessages: 1, wantSuccess: true},
		{name: "message_over_cap", payloads: [][]byte{[]byte(`{"protocol_version":1,"message_type":"telemetry","entity_id":"urn:ngsi-ld:Sensor:gw-a:s-a","gateway_id":"gw-a","sensor_id":"s-a","message_id":"00000000-0000-4000-8000-000000000001","boot_id":"00000000-0000-4000-8000-000000000002","first_sequence":"1","sample_count":1,"measured_at":"2026-10-28T00:00:00Z","sample_interval_us":1000000,"samples":[20.01]}`), []byte(`{"protocol_version":1,"message_type":"telemetry","entity_id":"urn:ngsi-ld:Sensor:gw-a:s-a","gateway_id":"gw-a","sensor_id":"s-a","message_id":"00000000-0000-4000-8000-000000000003","boot_id":"00000000-0000-4000-8000-000000000002","first_sequence":"2","sample_count":1,"measured_at":"2026-10-28T00:00:00Z","sample_interval_us":1000000,"samples":[20.02]}`)}, maxMessages: 1, want: "limit_exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, e := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"iot.telemetry.v1"}})
				if e != nil {
					return
				}
				defer func() { _ = c.Close(websocket.StatusNormalClosure, "test") }()
				if len(tc.payloads) > 0 {
					for _, payload := range tc.payloads {
						_ = c.Write(r.Context(), websocket.MessageText, payload)
					}
				} else if !tc.closeOnly {
					_ = c.Write(r.Context(), websocket.MessageText, tc.payload)
				}
				if tc.holdOpen {
					<-r.Context().Done()
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
			maxBytes := tc.maxBytes
			if maxBytes == 0 {
				maxBytes = 1 << 20
			}
			shared := &viewerShared{result: ViewerResult{CloseReasons: map[string]int{}}, summaries: map[string]*ViewerSummary{"v1": {ID: "v1"}}, maxMessages: tc.maxMessages, maxBytes: maxBytes, seen: map[string]map[string]struct{}{}, order: map[string][]string{}, receiving: map[string]bool{}}
			sock := &viewerSocket{viewer: v, conn: conn, ctx: sockCtx, cancel: cancel, done: make(chan struct{})}
			errs := make(chan error, 1)
			began := time.Now()
			if tc.holdOpen {
				finished := make(chan struct{})
				go func() {
					readViewer(sock, shared, errs, tc.slowPause)
					close(finished)
				}()
				deadline := time.Now().Add(time.Second)
				for {
					shared.mu.Lock()
					messages := shared.result.Messages
					shared.mu.Unlock()
					if messages == tc.maxMessages {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("viewer did not reach exact message cap")
					}
					time.Sleep(time.Millisecond)
				}
				cancel()
				select {
				case <-finished:
				case <-time.After(time.Second):
					t.Fatal("viewer did not stop after planned cancellation")
				}
			} else {
				readViewer(sock, shared, errs, tc.slowPause)
			}
			if tc.slowPause > 0 && time.Since(began) < tc.slowPause {
				t.Fatal("slow mode did not pause reads")
			}
			if tc.wantSuccess && len(errs) != 0 {
				t.Fatalf("exact-cap normal close unexpectedly failed: %v", <-errs)
			}
			if tc.closeOnly && len(errs) == 0 {
				t.Fatal("unexpected close not surfaced")
			}
			if !tc.closeOnly && !tc.wantSuccess && len(errs) == 0 {
				t.Fatal("bad frame/cap not surfaced")
			}
			if tc.want == "limit_exceeded" {
				if err := <-errs; err == nil || err.Error() != "viewer_limit_exceeded" {
					t.Fatalf("overflow error=%v, want viewer_limit_exceeded", err)
				}
			}
			if !tc.wantSuccess && shared.result.CloseReasons[tc.want] == 0 {
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

func TestViewerRuntimeRampConnectionSpacingAndCancellation(t *testing.T) {
	var mu sync.Mutex
	var connectTimes []time.Time
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/ws" || r.Header.Get("Authorization") == "" {
			t.Errorf("unexpected handshake")
		}
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"iot.telemetry.v1"}})
		if err != nil {
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "done") }()
		mu.Lock()
		connectTimes = append(connectTimes, time.Now())
		mu.Unlock()
		_, req, err := c.Read(r.Context())
		if err != nil {
			return
		}
		var sub struct {
			RequestID string                  `json:"request_id"`
			Targets   []telemetryws.TargetKey `json:"targets"`
		}
		if json.Unmarshal(req, &sub) != nil {
			return
		}
		ack, _ := json.Marshal(struct {
			ProtocolVersion int64                   `json:"protocol_version"`
			MessageType     string                  `json:"message_type"`
			RequestID       string                  `json:"request_id"`
			Action          string                  `json:"action"`
			Targets         []telemetryws.TargetKey `json:"targets"`
		}{1, "ack", sub.RequestID, "subscribe", sub.Targets})
		_ = c.Write(r.Context(), websocket.MessageText, ack)
		_, _, _ = c.Read(r.Context())
	}))
	defer srv.Close()

	dir := t.TempDir()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, cert, 0600); err != nil {
		t.Fatal(err)
	}
	identity := filepath.Join(dir, "viewer.json")
	token := testViewerJWT("principal", time.Now().Add(time.Hour).Unix())
	if err := os.WriteFile(identity, []byte(fmt.Sprintf(`{"u1":%q,"u2":%q,"u3":%q,"u4":%q}`, token, token, token, token)), 0600); err != nil {
		t.Fatal(err)
	}
	host := strings.TrimPrefix(srv.URL, "https://")
	m := Manifest{
		Experiment: "E1", Arm: "reference", Profile: "pilot", Mode: "remote", RunID: "viewer-ramp",
		Topology:      Topology{Users: 4, Viewers: 4},
		IdentityFiles: map[string]string{"viewer": identity},
		TLS:           TLS{HTTPEndpoint: "https://" + host, HTTPCAFile: ca},
		Traffic:       Traffic{RatePerSecond: 1, RampSeconds: 2},
		Limits:        Limits{Workers: 4, Connections: 4, WallSeconds: 10},
		Phases:        Phases{MeasureSeconds: 2, DrainSeconds: 1},
		Viewer:        ViewerSettings{MaxMessages: 10, MaxBytes: 10000},
	}
	for i := 1; i <= 4; i++ {
		m.Subscriptions = append(m.Subscriptions, Subscription{
			ViewerID: fmt.Sprintf("v%d", i), UserID: fmt.Sprintf("u%d", i),
			GatewayID: "gw-a", SensorID: "s-a",
		})
	}
	manifest := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifest, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var stopOnce sync.Once
	stopPlanned := func() {
		stopOnce.Do(func() {
			close(stop)
		})
	}
	defer stopPlanned()

	resultCh := make(chan struct {
		r   ViewerResult
		err error
	}, 1)
	start := time.Now()
	go func() {
		r, err := RunViewerWithPlannedStop(context.Background(), m, manifest, filepath.Join(dir, "out"), stop)
		resultCh <- struct {
			r   ViewerResult
			err error
		}{r, err}
	}()

	ready := filepath.Join(dir, "out", "ready.json")
	deadline := time.Now().Add(4 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ready marker did not appear")
		}
		time.Sleep(5 * time.Millisecond)
	}
	readyElapsed := time.Since(start)

	mu.Lock()
	times := append([]time.Time(nil), connectTimes...)
	mu.Unlock()

	if len(times) != 4 {
		t.Fatalf("expected 4 connections, got %d", len(times))
	}
	spacing := times[len(times)-1].Sub(times[0])
	if spacing < 500*time.Millisecond {
		t.Fatalf("ramp connection launch spacing not enforced: all %d connections arrived within %v across 2s ramp", len(times), spacing)
	}
	if readyElapsed < 1500*time.Millisecond {
		t.Fatalf("ready barrier published too early: elapsed %v for 2s ramp", readyElapsed)
	}

	// Terminate and join the first asynchronous run before running the cancellation subtest or server cleanup.
	stopPlanned()
	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatalf("first run failed: %v", res.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first run did not terminate after planned stop")
	}

	t.Run("EarlyCancellationDuringRamp", func(t *testing.T) {
		mu.Lock()
		connectTimes = nil
		mu.Unlock()
		cancelCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		outCancel := filepath.Join(dir, "out-cancel")
		res, err := RunViewer(cancelCtx, m, manifest, outCancel)
		if err == nil {
			t.Fatal("expected cancellation error, got nil")
		}
		if res.Status == "COMPLETE" {
			t.Fatalf("expected non-COMPLETE status on cancel, got %s", res.Status)
		}
		if _, err := os.Stat(filepath.Join(outCancel, "ready.json")); err == nil {
			t.Fatal("ready marker must not exist after cancellation during ramp")
		}
		mu.Lock()
		count := len(connectTimes)
		mu.Unlock()
		if count >= 4 {
			t.Fatalf("expected cancellation during ramp to prevent all viewers from launching, but all %d connected within timeout", count)
		}
	})
}

func TestViewerLaunchOffsetDeterministicTargetOffsets(t *testing.T) {
	// Ramp 0 behavior: all offsets are 0
	for i := 0; i < 5; i++ {
		if off := viewerLaunchOffset(i, 5, 0); off != 0 {
			t.Fatalf("ramp0: expected offset 0, got %v", off)
		}
		if off := viewerLaunchOffset(i, 5, -time.Second); off != 0 {
			t.Fatalf("negative ramp: expected offset 0, got %v", off)
		}
	}
	// Total <= 1: offset is 0
	if off := viewerLaunchOffset(0, 1, 10*time.Second); off != 0 {
		t.Fatalf("total 1: expected 0, got %v", off)
	}
	if off := viewerLaunchOffset(0, 0, 10*time.Second); off != 0 {
		t.Fatalf("total 0: expected 0, got %v", off)
	}
	// 4 viewers over 2s ramp: target offsets must be [0, 500ms, 1000ms, 1500ms]
	expected := []time.Duration{0, 500 * time.Millisecond, 1000 * time.Millisecond, 1500 * time.Millisecond}
	for i, want := range expected {
		got := viewerLaunchOffset(i, 4, 2*time.Second)
		if got != want {
			t.Fatalf("viewer %d: expected offset %v, got %v", i, want, got)
		}
	}
	// Bounds check: every offset must be strictly in [0, ramp)
	ramp := 10 * time.Second
	total := 100
	for i := 0; i < total; i++ {
		off := viewerLaunchOffset(i, total, ramp)
		if off < 0 || off >= ramp {
			t.Fatalf("viewer %d/%d: offset %v outside [0, %v)", i, total, off, ramp)
		}
		if i > 0 && off < viewerLaunchOffset(i-1, total, ramp) {
			t.Fatalf("offsets not monotonic: %v < %v", off, viewerLaunchOffset(i-1, total, ramp))
		}
	}
}

func TestViewerRuntimeRamp0Immediate(t *testing.T) {
	var connectTimes []time.Time
	var mu sync.Mutex
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"iot.telemetry.v1"}})
		if err != nil {
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "done") }()
		mu.Lock()
		connectTimes = append(connectTimes, time.Now())
		mu.Unlock()
		_, req, err := c.Read(r.Context())
		if err != nil {
			return
		}
		var sub struct {
			RequestID string                  `json:"request_id"`
			Targets   []telemetryws.TargetKey `json:"targets"`
		}
		if json.Unmarshal(req, &sub) != nil {
			return
		}
		ack, _ := json.Marshal(struct {
			ProtocolVersion int64                   `json:"protocol_version"`
			MessageType     string                  `json:"message_type"`
			RequestID       string                  `json:"request_id"`
			Action          string                  `json:"action"`
			Targets         []telemetryws.TargetKey `json:"targets"`
		}{1, "ack", sub.RequestID, "subscribe", sub.Targets})
		_ = c.Write(r.Context(), websocket.MessageText, ack)
		_, _, _ = c.Read(r.Context())
	}))
	defer srv.Close()

	dir := t.TempDir()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, cert, 0600); err != nil {
		t.Fatal(err)
	}
	identity := filepath.Join(dir, "viewer.json")
	token := testViewerJWT("principal", time.Now().Add(time.Hour).Unix())
	if err := os.WriteFile(identity, []byte(fmt.Sprintf(`{"u1":%q,"u2":%q,"u3":%q}`, token, token, token)), 0600); err != nil {
		t.Fatal(err)
	}
	host := strings.TrimPrefix(srv.URL, "https://")
	m := Manifest{
		Experiment: "E1", Arm: "reference", Profile: "pilot", Mode: "remote", RunID: "viewer-ramp0",
		Topology:      Topology{Users: 3, Viewers: 3},
		IdentityFiles: map[string]string{"viewer": identity},
		TLS:           TLS{HTTPEndpoint: "https://" + host, HTTPCAFile: ca},
		Traffic:       Traffic{RatePerSecond: 1, RampSeconds: 0},
		Limits:        Limits{Workers: 3, Connections: 3, WallSeconds: 5},
		Phases:        Phases{MeasureSeconds: 1, DrainSeconds: 1},
		Viewer:        ViewerSettings{MaxMessages: 10, MaxBytes: 10000},
	}
	for i := 1; i <= 3; i++ {
		m.Subscriptions = append(m.Subscriptions, Subscription{
			ViewerID: fmt.Sprintf("v%d", i), UserID: fmt.Sprintf("u%d", i),
			GatewayID: "gw-a", SensorID: "s-a",
		})
	}
	manifest := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifest, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var stopOnce sync.Once
	stopPlanned := func() {
		stopOnce.Do(func() { close(stop) })
	}
	defer stopPlanned()

	resultCh := make(chan struct {
		r   ViewerResult
		err error
	}, 1)
	start := time.Now()
	go func() {
		r, err := RunViewerWithPlannedStop(context.Background(), m, manifest, filepath.Join(dir, "out"), stop)
		resultCh <- struct {
			r   ViewerResult
			err error
		}{r, err}
	}()

	ready := filepath.Join(dir, "out", "ready.json")
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ready marker did not appear")
		}
		time.Sleep(2 * time.Millisecond)
	}
	readyElapsed := time.Since(start)
	if readyElapsed > 1*time.Second {
		t.Fatalf("ramp0 ready barrier took too long: %v", readyElapsed)
	}

	stopPlanned()
	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatalf("run failed: %v", res.err)
		}
		if res.r.Status != "COMPLETE" {
			t.Fatalf("expected COMPLETE, got %s", res.r.Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not terminate after planned stop")
	}

	mu.Lock()
	count := len(connectTimes)
	mu.Unlock()
	if count != 3 {
		t.Fatalf("expected 3 connections, got %d", count)
	}
}

func TestViewerRuntimeDeterministicLaunchOrdering(t *testing.T) {
	var launchedSensors []string
	var mu sync.Mutex
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"iot.telemetry.v1"}})
		if err != nil {
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "done") }()
		_, req, err := c.Read(r.Context())
		if err != nil {
			return
		}
		var sub struct {
			RequestID string                  `json:"request_id"`
			Targets   []telemetryws.TargetKey `json:"targets"`
		}
		if json.Unmarshal(req, &sub) != nil {
			return
		}
		mu.Lock()
		if len(sub.Targets) > 0 {
			launchedSensors = append(launchedSensors, sub.Targets[0].SensorID)
		}
		mu.Unlock()
		ack, _ := json.Marshal(struct {
			ProtocolVersion int64                   `json:"protocol_version"`
			MessageType     string                  `json:"message_type"`
			RequestID       string                  `json:"request_id"`
			Action          string                  `json:"action"`
			Targets         []telemetryws.TargetKey `json:"targets"`
		}{1, "ack", sub.RequestID, "subscribe", sub.Targets})
		_ = c.Write(r.Context(), websocket.MessageText, ack)
		_, _, _ = c.Read(r.Context())
	}))
	defer srv.Close()

	dir := t.TempDir()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, cert, 0600); err != nil {
		t.Fatal(err)
	}
	identity := filepath.Join(dir, "viewer.json")
	token := testViewerJWT("principal", time.Now().Add(time.Hour).Unix())
	if err := os.WriteFile(identity, []byte(fmt.Sprintf(`{"u1":%q,"u2":%q,"u3":%q}`, token, token, token)), 0600); err != nil {
		t.Fatal(err)
	}
	host := strings.TrimPrefix(srv.URL, "https://")
	m := Manifest{
		Experiment: "E1", Arm: "reference", Profile: "pilot", Mode: "remote", RunID: "viewer-ordering",
		Topology:      Topology{Users: 3, Viewers: 3},
		IdentityFiles: map[string]string{"viewer": identity},
		TLS:           TLS{HTTPEndpoint: "https://" + host, HTTPCAFile: ca},
		Traffic:       Traffic{RatePerSecond: 1, RampSeconds: 1}, // 333ms spacing
		Limits:        Limits{Workers: 3, Connections: 3, WallSeconds: 5},
		Phases:        Phases{MeasureSeconds: 1, DrainSeconds: 1},
		Viewer:        ViewerSettings{MaxMessages: 10, MaxBytes: 10000},
	}
	// Add subscriptions in scrambled order: v3, v1, v2
	m.Subscriptions = []Subscription{
		{ViewerID: "v3", UserID: "u3", GatewayID: "gw-a", SensorID: "sensor-3"},
		{ViewerID: "v1", UserID: "u1", GatewayID: "gw-a", SensorID: "sensor-1"},
		{ViewerID: "v2", UserID: "u2", GatewayID: "gw-a", SensorID: "sensor-2"},
	}
	manifest := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifest, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var stopOnce sync.Once
	stopPlanned := func() { stopOnce.Do(func() { close(stop) }) }
	defer stopPlanned()

	resultCh := make(chan struct {
		r   ViewerResult
		err error
	}, 1)
	go func() {
		r, err := RunViewerWithPlannedStop(context.Background(), m, manifest, filepath.Join(dir, "out"), stop)
		resultCh <- struct {
			r   ViewerResult
			err error
		}{r, err}
	}()

	ready := filepath.Join(dir, "out", "ready.json")
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ready marker did not appear")
		}
		time.Sleep(5 * time.Millisecond)
	}

	stopPlanned()
	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatalf("run failed: %v", res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not terminate after planned stop")
	}

	mu.Lock()
	got := append([]string(nil), launchedSensors...)
	mu.Unlock()

	want := []string{"sensor-1", "sensor-2", "sensor-3"}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("launch ordering not sorted by viewer ID: got %v, want %v", got, want)
	}
}

func TestViewerRuntimeConcurrencyBound(t *testing.T) {
	var currentActive atomic.Int32
	var maxActive atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := currentActive.Add(1)
		for {
			old := maxActive.Load()
			if cur <= old || maxActive.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"iot.telemetry.v1"}})
		currentActive.Add(-1)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "done") }()
		_, req, err := c.Read(r.Context())
		if err != nil {
			return
		}
		var sub struct {
			RequestID string                  `json:"request_id"`
			Targets   []telemetryws.TargetKey `json:"targets"`
		}
		if json.Unmarshal(req, &sub) != nil {
			return
		}
		ack, _ := json.Marshal(struct {
			ProtocolVersion int64                   `json:"protocol_version"`
			MessageType     string                  `json:"message_type"`
			RequestID       string                  `json:"request_id"`
			Action          string                  `json:"action"`
			Targets         []telemetryws.TargetKey `json:"targets"`
		}{1, "ack", sub.RequestID, "subscribe", sub.Targets})
		_ = c.Write(r.Context(), websocket.MessageText, ack)
		_, _, _ = c.Read(r.Context())
	}))
	defer srv.Close()

	dir := t.TempDir()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, cert, 0600); err != nil {
		t.Fatal(err)
	}
	identity := filepath.Join(dir, "viewer.json")
	token := testViewerJWT("principal", time.Now().Add(time.Hour).Unix())
	if err := os.WriteFile(identity, []byte(fmt.Sprintf(`{"u1":%q,"u2":%q,"u3":%q,"u4":%q}`, token, token, token, token)), 0600); err != nil {
		t.Fatal(err)
	}
	host := strings.TrimPrefix(srv.URL, "https://")
	m := Manifest{
		Experiment: "E1", Arm: "reference", Profile: "pilot", Mode: "remote", RunID: "viewer-concurrency",
		Topology:      Topology{Users: 4, Viewers: 4},
		IdentityFiles: map[string]string{"viewer": identity},
		TLS:           TLS{HTTPEndpoint: "https://" + host, HTTPCAFile: ca},
		Traffic:       Traffic{RatePerSecond: 1, RampSeconds: 0},
		Limits:        Limits{Workers: 2, Connections: 4, WallSeconds: 5}, // Workers: 2
		Phases:        Phases{MeasureSeconds: 1, DrainSeconds: 1},
		Viewer:        ViewerSettings{MaxMessages: 10, MaxBytes: 10000},
	}
	for i := 1; i <= 4; i++ {
		m.Subscriptions = append(m.Subscriptions, Subscription{
			ViewerID: fmt.Sprintf("v%d", i), UserID: fmt.Sprintf("u%d", i),
			GatewayID: "gw-a", SensorID: "s-a",
		})
	}
	manifest := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifest, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var stopOnce sync.Once
	stopPlanned := func() { stopOnce.Do(func() { close(stop) }) }
	defer stopPlanned()

	resultCh := make(chan struct {
		r   ViewerResult
		err error
	}, 1)
	go func() {
		r, err := RunViewerWithPlannedStop(context.Background(), m, manifest, filepath.Join(dir, "out"), stop)
		resultCh <- struct {
			r   ViewerResult
			err error
		}{r, err}
	}()

	ready := filepath.Join(dir, "out", "ready.json")
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ready marker did not appear")
		}
		time.Sleep(5 * time.Millisecond)
	}

	stopPlanned()
	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatalf("run failed: %v", res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not terminate after planned stop")
	}

	if max := maxActive.Load(); max > 2 {
		t.Fatalf("concurrency bound exceeded: max active %d > workers 2", max)
	}
}

func TestViewerRuntimeNoReadinessAfterCancel(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond) // slow handshake so cancel happens before setup finishes
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"iot.telemetry.v1"}})
		if err != nil {
			return
		}
		_ = c.Close(websocket.StatusNormalClosure, "done")
	}))
	defer srv.Close()

	dir := t.TempDir()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, cert, 0600); err != nil {
		t.Fatal(err)
	}
	identity := filepath.Join(dir, "viewer.json")
	token := testViewerJWT("principal", time.Now().Add(time.Hour).Unix())
	if err := os.WriteFile(identity, []byte(fmt.Sprintf(`{"u1":%q,"u2":%q}`, token, token)), 0600); err != nil {
		t.Fatal(err)
	}
	host := strings.TrimPrefix(srv.URL, "https://")
	m := Manifest{
		Experiment: "E1", Arm: "reference", Profile: "pilot", Mode: "remote", RunID: "viewer-cancel-noready",
		Topology:      Topology{Users: 2, Viewers: 2},
		IdentityFiles: map[string]string{"viewer": identity},
		TLS:           TLS{HTTPEndpoint: "https://" + host, HTTPCAFile: ca},
		Traffic:       Traffic{RatePerSecond: 1, RampSeconds: 2},
		Limits:        Limits{Workers: 2, Connections: 2, WallSeconds: 5},
		Phases:        Phases{MeasureSeconds: 1, DrainSeconds: 1},
		Viewer:        ViewerSettings{MaxMessages: 10, MaxBytes: 10000},
	}
	for i := 1; i <= 2; i++ {
		m.Subscriptions = append(m.Subscriptions, Subscription{
			ViewerID: fmt.Sprintf("v%d", i), UserID: fmt.Sprintf("u%d", i),
			GatewayID: "gw-a", SensorID: "s-a",
		})
	}
	manifest := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifest, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}

	cancelCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	outDir := filepath.Join(dir, "out")
	res, err := RunViewer(cancelCtx, m, manifest, outDir)
	if err == nil {
		t.Fatal("expected error on cancelled run, got nil")
	}
	if res.Status == "COMPLETE" {
		t.Fatalf("expected non-COMPLETE status on cancel, got %s", res.Status)
	}
	if _, err := os.Stat(filepath.Join(outDir, "ready.json")); err == nil {
		t.Fatal("ready.json must never be published when run is cancelled before ready")
	}
}

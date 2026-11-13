package telemetryws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
)

type allowAllTargets struct{}

func (allowAllTargets) AuthorizeTargets(_ context.Context, _ uuid.UUID, targets []TargetKey) ([]AuthorizedTarget, error) {
	out := make([]AuthorizedTarget, len(targets))
	for i, t := range targets {
		out[i] = AuthorizedTarget{Target: t}
	}
	return out, nil
}

type testVerifier struct{ principal auth.Principal }

func (v testVerifier) Verify(context.Context, string) (auth.Principal, error) {
	return v.principal, nil
}

func TestHandshakeOriginProtocolExpiryAndShutdown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h, err := NewHandler([]string{"https://client.example"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Shutdown(context.Background()) }()
	principal := auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: time.Now().Add(time.Minute)}
	r := gin.New()
	r.GET("/v1/ws", auth.WSAuthenticationMiddleware(testVerifier{principal}), h.ServeHTTP)
	srv := httptest.NewServer(r)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "ws"+srv.URL[4:]+"/v1/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer opaque.token")
	request.Header.Set("Sec-WebSocket-Protocol", auth.WSTelemetryProtocol)
	conn, response, err := websocket.Dial(ctx, "ws"+srv.URL[4:]+"/v1/ws", &websocket.DialOptions{HTTPHeader: request.Header, Subprotocols: []string{auth.WSTelemetryProtocol}})
	if err != nil {
		t.Fatalf("native handshake: %v", err)
	}
	if response.Header.Get("Sec-WebSocket-Protocol") != auth.WSTelemetryProtocol {
		t.Fatalf("negotiated protocol=%q", response.Header.Get("Sec-WebSocket-Protocol"))
	}
	if response.Header.Get("Sec-WebSocket-Protocol") == "bearer.opaque.token" {
		t.Fatal("credential protocol echoed")
	}
	_ = conn.CloseNow()
	// Browser-shaped protocol credential is accepted from the exact Origin and
	// the negotiated response contains only the application protocol.
	conn, response, err = websocket.Dial(ctx, "ws"+srv.URL[4:]+"/v1/ws", &websocket.DialOptions{HTTPHeader: http.Header{"Origin": {"https://client.example"}}, Subprotocols: []string{auth.WSTelemetryProtocol, "bearer.opaque.token"}})
	if err != nil {
		t.Fatalf("browser-shaped handshake: %v", err)
	}
	if response.Header.Get("Sec-WebSocket-Protocol") != auth.WSTelemetryProtocol {
		t.Fatalf("browser response protocol=%q", response.Header.Get("Sec-WebSocket-Protocol"))
	}
	_ = conn.CloseNow()

	for _, tc := range []struct {
		origin    string
		duplicate bool
	}{{"https://evil.example", false}, {"https://client.example", true}} {
		request, _ = http.NewRequestWithContext(ctx, http.MethodGet, "ws"+srv.URL[4:]+"/v1/ws", nil)
		request.Header.Set("Authorization", "Bearer opaque.token")
		request.Header.Set("Sec-WebSocket-Protocol", auth.WSTelemetryProtocol)
		request.Header.Add("Origin", tc.origin)
		if tc.duplicate {
			request.Header.Add("Origin", tc.origin)
		}
		_, response, err = websocket.Dial(ctx, "ws"+srv.URL[4:]+"/v1/ws", &websocket.DialOptions{HTTPHeader: request.Header, Subprotocols: []string{auth.WSTelemetryProtocol}})
		if err == nil {
			t.Fatal("disallowed Origin upgraded")
		}
		if response == nil || response.StatusCode != http.StatusForbidden {
			t.Fatalf("response=%v err=%v", response, err)
		}
	}
	// No Origin is explicitly allowed for native clients; expired verified claims are not.
	principal.ExpiresAt = time.Now().Add(-time.Second)
	r2 := gin.New()
	r2.GET("/v1/ws", auth.WSAuthenticationMiddleware(testVerifier{principal}), h.ServeHTTP)
	expired := httptest.NewServer(r2)
	defer expired.Close()
	request, _ = http.NewRequestWithContext(ctx, http.MethodGet, "ws"+expired.URL[4:]+"/v1/ws", nil)
	request.Header.Set("Authorization", "Bearer opaque.token")
	request.Header.Set("Sec-WebSocket-Protocol", auth.WSTelemetryProtocol)
	_, response, err = websocket.Dial(ctx, "ws"+expired.URL[4:]+"/v1/ws", &websocket.DialOptions{HTTPHeader: request.Header, Subprotocols: []string{auth.WSTelemetryProtocol}})
	if err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired response=%v err=%v", response, err)
	}
}

func TestApplicationHandlerRequiresRuntimeDependencies(t *testing.T) {
	if _, err := NewApplicationHandler(nil, nil, nil); err == nil {
		t.Fatal("application handler accepted missing runtime dependencies")
	}
}

func TestPingLoopIdlePongAndSilentPeerUseInactivityBudget(t *testing.T) {
	newPair := func(t *testing.T) (*websocket.Conn, *websocket.Conn, func()) {
		t.Helper()
		serverConn := make(chan *websocket.Conn, 1)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, err := websocket.Accept(w, r, nil)
			if err == nil {
				serverConn <- c
			}
		}))
		client, _, err := websocket.Dial(context.Background(), "ws"+srv.URL[4:], nil)
		if err != nil {
			t.Fatal(err)
		}
		server := <-serverConn
		return server, client, func() { _ = server.CloseNow(); _ = client.CloseNow(); srv.Close() }
	}

	t.Run("healthy idle peer", func(t *testing.T) {
		server, client, cleanup := newPair(t)
		defer cleanup()
		readCtx, cancelRead := context.WithCancel(context.Background())
		defer cancelRead()
		go func() {
			for {
				if _, _, err := client.Read(readCtx); err != nil {
					return
				}
			}
		}()
		s := &appSession{conn: server, close: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			(&Handler{}).pingLoop(ctx, s, HandlerOptions{PingInterval: 15 * time.Millisecond, PongTimeout: 120 * time.Millisecond})
			close(done)
		}()
		time.Sleep(85 * time.Millisecond)
		select {
		case <-s.close:
			t.Fatal("healthy idle peer was closed")
		default:
		}
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("ping loop did not stop")
		}
	})
	t.Run("silent peer", func(t *testing.T) {
		server, _, cleanup := newPair(t)
		defer cleanup()
		s := &appSession{conn: server, close: make(chan struct{})}
		started := time.Now()
		done := make(chan struct{})
		go func() {
			(&Handler{}).pingLoop(context.Background(), s, HandlerOptions{PingInterval: 20 * time.Millisecond, PongTimeout: 45 * time.Millisecond})
			close(done)
		}()
		select {
		case <-s.close:
			if time.Since(started) > 90*time.Millisecond {
				t.Fatalf("silent peer closed after inactivity budget: %s", time.Since(started))
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("silent peer was not closed within inactivity budget")
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("ping loop did not finish")
		}
	})
}

func TestApplicationSubscribeAcknowledgesInstalledFilterAndFansOut(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hub := NewHub(DefaultHubConfig())
	h, err := NewApplicationHandler(nil, hub, allowAllTargets{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Shutdown(context.Background()) }()
	r := gin.New()
	r.GET("/v1/ws", auth.WSAuthenticationMiddleware(testVerifier{auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: time.Now().Add(time.Minute)}}), h.ServeHTTP)
	srv := httptest.NewServer(r)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, "ws"+srv.URL[4:]+"/v1/ws", &websocket.DialOptions{Subprotocols: []string{auth.WSTelemetryProtocol}, HTTPHeader: http.Header{"Authorization": {"Bearer opaque.token"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.CloseNow() }()
	req := ClientMessage{ProtocolVersion: ProtocolVersion, RequestID: "r1", Action: ActionSubscribe, Targets: []TargetKey{{GatewayID: "gw-1", SensorID: "sensor-1"}}}
	data, _ := json.Marshal(req)
	if err = c.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatal(err)
	}
	_, ack, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got AckMessage
	if err = json.Unmarshal(ack, &got); err != nil || got.RequestID != "r1" || got.MessageType != MessageTypeAck {
		t.Fatalf("ack=%s err=%v", ack, err)
	}
	event, err := NewSharedEvent("gw-1", "sensor-1", []byte(`{"message_type":"telemetry"}`), DefaultMaxEventBytes)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := hub.FanOut(event); err != nil || n != 1 {
		t.Fatalf("fanout=%d err=%v", n, err)
	}
	_, frame, err := c.Read(ctx)
	if err != nil || string(frame) != string(event.Payload()) {
		t.Fatalf("frame=%s err=%v", frame, err)
	}
}

func TestWriteLoopDrainsCoalescedQueueNotification(t *testing.T) {
	gin.SetMode(gin.TestMode)
	serverConn := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		serverConn <- c
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	client, _, err := websocket.Dial(ctx, "ws"+srv.URL[4:], nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.CloseNow() }()
	conn := <-serverConn
	defer func() { _ = conn.CloseNow() }()

	q := NewSessionQueue("burst", 4, 1024, 1024)
	state, err := NewSessionAccessState("burst", uuid.New(), time.Now().Add(time.Minute), nil)
	if err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	state.targets[TargetKey{GatewayID: "gw", SensorID: "sensor"}] = struct{}{}
	state.mu.Unlock()
	for _, payload := range []string{`{"n":1}`, `{"n":2}`} {
		event, e := NewSharedEvent("gw", "sensor", []byte(payload), 1024)
		if e != nil {
			t.Fatal(e)
		}
		if e = q.Enqueue(event); e != nil {
			t.Fatal(e)
		}
	}
	s := &appSession{id: "burst", conn: conn, state: state, queue: q, close: make(chan struct{}), controls: make(chan outboundControl, 1)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&Handler{}).writeLoop(ctx, s, HandlerOptions{WriteTimeout: time.Second})
	}()
	for i, want := range []string{`{"n":1}`, `{"n":2}`} {
		_, got, readErr := client.Read(ctx)
		if readErr != nil || string(got) != want {
			t.Fatalf("frame %d=%s err=%v; coalesced queued frame was stranded", i, got, readErr)
		}
	}
	close(s.close)
	_ = conn.CloseNow()
	<-done
}

func TestControlReservationsShareSessionByteBudget(t *testing.T) {
	q := NewSessionQueue("budget", 4, 100, 100)
	reservation, err := q.ReserveControl(60, 16)
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewSharedEvent("gw", "sensor", make([]byte, 41), 100)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Enqueue(event); err != ErrQueueFull {
		t.Fatalf("enqueue over combined budget err=%v", err)
	}
	if q.TotalBytes() != 60 {
		t.Fatalf("reserved bytes=%d", q.TotalBytes())
	}
	reservation.Release()
	if err = q.Enqueue(event); err != nil {
		t.Fatalf("enqueue after control release: %v", err)
	}
	if q.TotalBytes() != event.ByteSize() {
		t.Fatalf("total after release=%d", q.TotalBytes())
	}
}

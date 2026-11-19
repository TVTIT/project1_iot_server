package telemetryws

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
)

type socketPolicyAuthorizer struct {
	mu      sync.Mutex
	allowed map[string]bool
	users   map[uuid.UUID]bool
	failFor map[uuid.UUID]bool
}

func (a *socketPolicyAuthorizer) AuthorizeTargets(_ context.Context, user uuid.UUID, targets []TargetKey) ([]AuthorizedTarget, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failFor[user] {
		return nil, errors.New("authorization database unavailable")
	}
	if allowed, scoped := a.users[user]; scoped && !allowed {
		return nil, ErrTargetUnauthorized
	}
	out := make([]AuthorizedTarget, 0, len(targets))
	for _, target := range DeduplicateTargets(targets) {
		if !a.allowed[target.String()] {
			return nil, ErrTargetUnauthorized
		}
		out = append(out, AuthorizedTarget{Target: target})
	}
	return out, nil
}

func (a *socketPolicyAuthorizer) setUser(user uuid.UUID, allowed bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.users == nil {
		a.users = make(map[uuid.UUID]bool)
	}
	a.users[user] = allowed
}

func (a *socketPolicyAuthorizer) failUser(user uuid.UUID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failFor == nil {
		a.failFor = make(map[uuid.UUID]bool)
	}
	a.failFor[user] = true
}

type applicationSocket struct {
	conn *websocket.Conn
	hub  *Hub
	h    *Handler
	srv  *httptest.Server
	user uuid.UUID
}

func newApplicationSocket(t *testing.T, principal auth.Principal, hub *Hub, authz TargetsAuthorizer, options HandlerOptions) applicationSocket {
	t.Helper()
	gin.SetMode(gin.TestMode)
	var h *Handler
	var err error
	if options == (HandlerOptions{}) {
		h, err = NewApplicationHandler(nil, hub, authz)
	} else {
		h, err = NewApplicationHandlerWithOptions(nil, ApplicationHandlerDeps{Hub: hub, Authorizer: authz, Options: options})
	}
	if err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.GET("/v1/ws", auth.WSAuthenticationMiddleware(testVerifier{principal}), h.ServeHTTP)
	srv := httptest.NewServer(r)
	t.Cleanup(func() {
		_ = h.Shutdown(context.Background())
		srv.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, "ws"+srv.URL[4:]+"/v1/ws", &websocket.DialOptions{
		Subprotocols: []string{auth.WSTelemetryProtocol},
		HTTPHeader:   http.Header{"Authorization": {"Bearer opaque.token"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	return applicationSocket{conn: c, hub: hub, h: h, srv: srv, user: principal.UserID}
}

func (s applicationSocket) send(t *testing.T, id, action string, targets ...TargetKey) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := json.Marshal(ClientMessage{ProtocolVersion: ProtocolVersion, RequestID: id, Action: action, Targets: targets})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.conn.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatal(err)
	}
}

func (s applicationSocket) read(t *testing.T) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, b, err := s.conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestApplicationSocketRejectsNonmemberAndCrossGatewaySensorID(t *testing.T) {
	adminID := uuid.New()
	admin := newApplicationSocket(t, auth.Principal{UserID: adminID, Role: "platform_admin", ExpiresAt: time.Now().Add(time.Minute)}, NewHub(DefaultHubConfig()), &socketPolicyAuthorizer{allowed: map[string]bool{"gw-1:sensor-1": true}}, HandlerOptions{})
	denied := TargetKey{GatewayID: "gw-2", SensorID: "sensor-1"}
	admin.send(t, "not-member", ActionSubscribe, denied)
	var response ErrorMessage
	if err := json.Unmarshal(admin.read(t), &response); err != nil || response.ErrorCode != ErrorCodeUnauthorized {
		t.Fatalf("denial response=%+v err=%v", response, err)
	}
	admin.hub.mu.RLock()
	installed := len(admin.hub.targetIndex[denied])
	admin.hub.mu.RUnlock()
	if installed != 0 {
		t.Fatalf("unauthorized socket installed target filter: %d", installed)
	}
}

func TestApplicationSocketExpiryMetricUsesVerifiedDeadline(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	m := &Metrics{}
	hub.SetMetrics(m)
	s := newApplicationSocket(t, auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: time.Now().Add(150 * time.Millisecond)}, hub, allowAllTargets{}, HandlerOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, err := s.conn.Read(ctx); err == nil {
		t.Fatal("socket remained open past verified token expiry")
	}
	deadline := time.Now().Add(time.Second)
	for hub.SessionCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	snapshot := m.Snapshot(hub)
	if snapshot.Expiries != 1 || snapshot.Revocations != 0 {
		t.Fatalf("expiry/revocation metrics=%d/%d", snapshot.Expiries, snapshot.Revocations)
	}
}

func TestApplicationSocketEnforcesReducedSubscriptionAndRateCaps(t *testing.T) {
	a := &socketPolicyAuthorizer{allowed: map[string]bool{"gw-1:s1": true, "gw-1:s2": true}}
	hub := NewHub(DefaultHubConfig())
	s := newApplicationSocket(t, auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: time.Now().Add(time.Minute)}, hub, a, HandlerOptions{MaxSubscriptions: 1, MaxRequestRate: 3})
	s.send(t, "one", ActionSubscribe, TargetKey{GatewayID: "gw-1", SensorID: "s1"})
	if got := s.read(t); !strings.Contains(string(got), `"message_type":"ack"`) {
		t.Fatalf("first subscribe response=%s", got)
	}
	s.send(t, "two", ActionSubscribe, TargetKey{GatewayID: "gw-1", SensorID: "s2"})
	if got := s.read(t); !strings.Contains(string(got), string(ErrorCodeTargetLimitExceeded)) {
		t.Fatalf("subscription cap response=%s", got)
	}
	// A third command in the same one-second window exceeds the configured rate.
	s.send(t, "three", ActionUnsubscribe, TargetKey{GatewayID: "gw-1", SensorID: "s1"})
	if got := s.read(t); !strings.Contains(string(got), `"message_type":"ack"`) {
		t.Fatalf("unsubscribe response=%s", got)
	}
	s.send(t, "four", ActionSubscribe, TargetKey{GatewayID: "gw-1", SensorID: "s2"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, err := s.conn.Read(ctx); err == nil {
		t.Fatal("rate cap did not close the over-limit socket")
	}
}

func TestApplicationSocketEnforcesReducedReadLimit(t *testing.T) {
	hub := NewHub(DefaultHubConfig())
	s := newApplicationSocket(t, auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: time.Now().Add(time.Minute)}, hub, allowAllTargets{}, HandlerOptions{MaxReadBytes: 64})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.conn.Write(ctx, websocket.MessageText, make([]byte, 65)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.conn.Read(ctx); err == nil {
		t.Fatal("reduced read limit did not close oversized control frame")
	}
}

func TestApplicationSocketUnsubscribeAckPrecedesAndRemovesFutureDelivery(t *testing.T) {
	target := TargetKey{GatewayID: "gw-1", SensorID: "s1"}
	hub := NewHub(DefaultHubConfig())
	s := newApplicationSocket(t, auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: time.Now().Add(time.Minute)}, hub, &socketPolicyAuthorizer{allowed: map[string]bool{target.String(): true}}, HandlerOptions{})
	s.send(t, "sub", ActionSubscribe, target)
	if got := s.read(t); !strings.Contains(string(got), `"message_type":"ack"`) {
		t.Fatalf("subscribe response=%s", got)
	}
	s.send(t, "unsub", ActionUnsubscribe, target)
	var ack AckMessage
	if err := json.Unmarshal(s.read(t), &ack); err != nil || ack.RequestID != "unsub" || ack.Action != ActionUnsubscribe {
		t.Fatalf("unsubscribe ACK=%+v err=%v", ack, err)
	}
	event, err := NewSharedEvent(target.GatewayID, target.SensorID, []byte(`{"message_type":"telemetry","marker":"after-unsubscribe"}`), DefaultMaxEventBytes)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := hub.FanOut(event); err != nil || n != 0 {
		t.Fatalf("post-ACK fanout=%d err=%v", n, err)
	}
	if n := hub.SessionCount(); n != 1 {
		t.Fatalf("unsubscribe unexpectedly closed authorized socket; active sessions=%d", n)
	}
}

func TestApplicationSocketRecheckFailureClosesRevokedViewerButKeepsPeer(t *testing.T) {
	target := TargetKey{GatewayID: "gw-1", SensorID: "s1"}
	hub := NewHub(DefaultHubConfig())
	a := &socketPolicyAuthorizer{allowed: map[string]bool{target.String(): true}}
	authorized := func() applicationSocket {
		s := newApplicationSocket(t, auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: time.Now().Add(time.Minute)}, hub, a, HandlerOptions{})
		s.send(t, "sub", ActionSubscribe, target)
		if got := s.read(t); !strings.Contains(string(got), `"message_type":"ack"`) {
			t.Fatalf("subscribe response=%s", got)
		}
		return s
	}
	viewer, peer := authorized(), authorized()
	// The actual periodic worker reauthorizes active socket state; deny only the
	// first identity and verify the independently authorized peer remains live.
	a.setUser(viewer.user, false)
	a.setUser(peer.user, true)
	a.failUser(viewer.user)
	worker := NewRecheckWorker(a, hub, RecheckWorkerConfig{Concurrency: 1, MaxSessions: 4})
	if report := worker.Sweep(context.Background()); report.SessionsRevoked != 1 {
		t.Fatalf("sweep report=%+v", report)
	}
	if hub.SessionCount() != 1 {
		t.Fatalf("revoked socket cleanup affected peer: active=%d", hub.SessionCount())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, err := viewer.conn.Read(ctx); err == nil {
		t.Fatal("DB recheck failure did not close the revoked viewer's real socket")
	}
	event, _ := NewSharedEvent(target.GatewayID, target.SensorID, []byte(`{"message_type":"telemetry"}`), DefaultMaxEventBytes)
	if n, err := hub.FanOut(event); err != nil || n != 1 {
		t.Fatalf("remaining peer fanout=%d err=%v", n, err)
	}
	if got := peer.read(t); !strings.Contains(string(got), `"message_type":"telemetry"`) {
		t.Fatalf("authorized peer frame=%s", got)
	}
}

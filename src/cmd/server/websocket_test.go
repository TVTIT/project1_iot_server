package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetryws"
)

type wsCompositionRepo struct {
	calls   atomic.Int32
	allowed atomic.Bool
}

func (r *wsCompositionRepo) AuthorizeSubscriptionTarget(context.Context, uuid.UUID, string, string) (gateway.Role, error) {
	r.calls.Add(1)
	if !r.allowed.Load() {
		return "", gateway.ErrNotFound
	}
	return gateway.RoleViewer, nil
}

type wsTestVerifier struct{ principal auth.Principal }

func (v wsTestVerifier) Verify(context.Context, string) (auth.Principal, error) {
	return v.principal, nil
}

func TestWebSocketCompositionWiresOneSharedApplicationAndRechecker(t *testing.T) {
	cfg := config.Config{WebSocketEnabled: true, AuthorizationTimeout: time.Second, WebSocket: config.WebSocketConfig{
		MaxSessions: 3, MaxSubscriptionsPerSession: 4, ReadFrameBytes: 2048, RequestRatePerSecond: 7,
		GlobalEventSlots: 4, GlobalEventBytes: 4096, SessionEventSlots: 3, SessionEventBytes: 2048,
		MaxFrameBytes: 1024, RecheckInterval: 5 * time.Second, RecheckSweepBudget: 2 * time.Second, RecheckConcurrency: 2,
		HeartbeatInterval: 20 * time.Second, PongTimeout: 45 * time.Second, WriteTimeout: 5 * time.Second,
	}}
	w, err := composeWebSocket(cfg)
	if err != nil {
		t.Fatal(err)
	}
	repo := &wsCompositionRepo{}
	repo.allowed.Store(true)
	if err := w.configure(cfg, repo); err != nil {
		t.Fatal(err)
	}
	if w.hub == nil || w.events == nil || w.handler == nil || w.worker == nil {
		t.Fatalf("incomplete enabled composition: %+v", w)
	}
	if err := w.start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := w.start(context.Background()); err == nil {
		t.Fatal("second start did not report duplicate worker")
	}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/v1/ws", auth.WSAuthenticationMiddleware(wsTestVerifier{auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: time.Now().Add(time.Minute)}}), w.handler.ServeHTTP)
	server := httptest.NewServer(router)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+server.URL[4:]+"/v1/ws", &websocket.DialOptions{Subprotocols: []string{auth.WSTelemetryProtocol}, HTTPHeader: http.Header{"Authorization": {"Bearer test"}}})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := json.Marshal(telemetryws.ClientMessage{ProtocolVersion: telemetryws.ProtocolVersion, RequestID: "sub", Action: telemetryws.ActionSubscribe, Targets: []telemetryws.TargetKey{{GatewayID: "gw-1", SensorID: "sensor-1"}}})
	if err := conn.Write(ctx, websocket.MessageText, message); err != nil {
		t.Fatal(err)
	}
	if _, _, err := conn.Read(ctx); err != nil {
		t.Fatalf("subscription ack: %v", err)
	}
	if repo.calls.Load() != 1 {
		t.Fatalf("shared authorizer calls=%d, want 1", repo.calls.Load())
	}
	repo.allowed.Store(false)
	report := w.worker.Sweep(ctx)
	if report.SessionsRevoked != 1 {
		t.Fatalf("revocation sweep=%+v", report)
	}
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("revoked websocket remained open")
	}
	_ = conn.CloseNow()
	if err := w.handler.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	server.Close()
	if err := w.close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWebSocketCompositionDisabledAndMissingDependenciesFailClosed(t *testing.T) {
	if w, err := composeWebSocket(config.Config{}); err != nil || w.hub != nil || w.events != nil || w.handler != nil || w.worker != nil {
		t.Fatalf("disabled composition=%+v err=%v", w, err)
	}
	cfg := config.Config{WebSocketEnabled: true}
	w := websocketRuntime{}
	if err := w.configure(cfg, &wsCompositionRepo{}); err == nil {
		t.Fatal("missing shared runtime accepted")
	}
	if _, err := telemetryws.NewApplicationHandlerWithOptions(nil, telemetryws.ApplicationHandlerDeps{}); err == nil {
		t.Fatal("handler accepted missing dependencies")
	}
}

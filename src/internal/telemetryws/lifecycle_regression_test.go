package telemetryws

import (
	"bufio"
	"context"
	"net"
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

type reviewPausedHijack struct {
	http.ResponseWriter
	entered chan struct{}
	release chan struct{}
}

func (w reviewPausedHijack) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := w.ResponseWriter.(http.Hijacker).Hijack()
	close(w.entered)
	<-w.release
	return conn, rw, err
}

func TestReviewShutdownMustJoinInflightUpgrade(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h, err := NewHandler(nil)
	if err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	r := gin.New()
	r.GET("/v1/ws", auth.WSAuthenticationMiddleware(testVerifier{auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: time.Now().Add(time.Minute)}}), h.ServeHTTP)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer close(done)
		r.ServeHTTP(reviewPausedHijack{w, entered, release}, req)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type dialResult struct {
		c   *websocket.Conn
		err error
	}
	dial := make(chan dialResult, 1)
	go func() {
		c, _, e := websocket.Dial(ctx, "ws"+srv.URL[4:]+"/v1/ws", &websocket.DialOptions{
			Subprotocols: []string{auth.WSTelemetryProtocol},
			HTTPHeader:   http.Header{"Authorization": {"Bearer opaque.token"}},
		})
		dial <- dialResult{c, e}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("hijack not entered")
	}

	// Verify that inflight upgrade is accounted for in Handler
	registered, inflight := h.ActiveSessions()
	if registered != 0 || inflight != 1 {
		t.Fatalf("expected registered=0 inflight=1, got registered=%d inflight=%d", registered, inflight)
	}

	shutdownDone := make(chan struct{})
	go func() {
		_ = h.Shutdown(context.Background())
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		reg, inf := h.ActiveSessions()
		t.Fatalf("Shutdown returned with an admitted hijacked upgrade still live: slots=%d registered=%d inflight=%d handler_not_joined=true", len(h.slots), reg, inf)
	case <-time.After(100 * time.Millisecond):
		// Expected: Shutdown is blocked waiting for inflight upgrade to complete
	}

	shutdownCtx, scancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	e := srv.Config.Shutdown(shutdownCtx)
	scancel()
	t.Logf("HTTP Shutdown result with hijacked upgrade outstanding: %v", e)

	close(release)
	d := <-dial
	if d.c != nil {
		_ = d.c.CloseNow()
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("handler cleanup timeout")
	}

	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("Shutdown failed to complete after releasing in-flight upgrade")
	}

	regFinal, infFinal := h.ActiveSessions()
	if len(h.slots) != 0 || regFinal != 0 || infFinal != 0 {
		t.Fatalf("cleanup leak after shutdown: slots=%d registered=%d inflight=%d", len(h.slots), regFinal, infFinal)
	}
}

func TestShutdownTimeoutReportsTruthfulError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h, err := NewHandler(nil)
	if err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	r := gin.New()
	r.GET("/v1/ws", auth.WSAuthenticationMiddleware(testVerifier{auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: time.Now().Add(time.Minute)}}), h.ServeHTTP)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer close(done)
		r.ServeHTTP(reviewPausedHijack{w, entered, release}, req)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() {
		c, _, _ := websocket.Dial(ctx, "ws"+srv.URL[4:]+"/v1/ws", &websocket.DialOptions{
			Subprotocols: []string{auth.WSTelemetryProtocol},
			HTTPHeader:   http.Header{"Authorization": {"Bearer opaque.token"}},
		})
		if c != nil {
			_ = c.CloseNow()
		}
	}()

	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("hijack not entered")
	}

	timeoutCtx, tcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer tcancel()

	err = h.Shutdown(timeoutCtx)
	if err == nil || !strings.Contains(err.Error(), "websocket shutdown unconfirmed") {
		t.Fatalf("expected unconfirmed shutdown error, got %v", err)
	}

	close(release)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("handler cleanup timeout")
	}

	// Repeated shutdown with ample budget succeeds once inflight is joined
	cleanCtx, ccancel := context.WithTimeout(context.Background(), time.Second)
	defer ccancel()
	if err := h.Shutdown(cleanCtx); err != nil {
		t.Fatalf("subsequent shutdown failed: %v", err)
	}
}

func TestShutdownSafeRepeatedAndPartialStartup(t *testing.T) {
	// Nil handler is safe
	var nilHandler *Handler
	if err := nilHandler.Shutdown(context.Background()); err != nil {
		t.Fatalf("nil shutdown failed: %v", err)
	}

	// Idle handler is safe
	h, err := NewHandler(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatalf("idle shutdown failed: %v", err)
	}

	// Repeated sequential shutdown is safe
	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatalf("repeated shutdown failed: %v", err)
	}

	// Concurrent shutdown calls are safe
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.Shutdown(context.Background()); err != nil {
				t.Errorf("concurrent shutdown failed: %v", err)
			}
		}()
	}
	wg.Wait()
}

func TestPartialFailedAcceptReleasesAccounting(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h, err := NewHandler(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Shutdown(context.Background()) }()

	r := gin.New()
	r.GET("/v1/ws", auth.WSAuthenticationMiddleware(testVerifier{auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: time.Now().Add(time.Minute)}}), h.ServeHTTP)
	srv := httptest.NewServer(r)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Dial with wrong subprotocol -> Accept negotiates unsupported protocol -> CloseNow
	_, resp, err := websocket.Dial(ctx, "ws"+srv.URL[4:]+"/v1/ws", &websocket.DialOptions{
		Subprotocols: []string{"unsupported.subprotocol"},
		HTTPHeader:   http.Header{"Authorization": {"Bearer opaque.token"}, "Sec-WebSocket-Protocol": {"unsupported.subprotocol"}},
	})
	if err == nil {
		t.Fatal("expected dial error with unsupported protocol")
	}
	if resp != nil && resp.StatusCode == http.StatusOK {
		t.Fatalf("unexpected OK response: %v", resp.StatusCode)
	}

	// Accounting must be fully clean
	reg, inf := h.ActiveSessions()
	if len(h.slots) != 0 || reg != 0 || inf != 0 {
		t.Fatalf("accounting leak after failed accept: slots=%d registered=%d inflight=%d", len(h.slots), reg, inf)
	}
}

func TestReviewShellBoundsAndActiveExpiry(t *testing.T) {
	for _, mode := range []string{"expiry", "readlimit", "cap32"} {
		t.Run(mode, func(t *testing.T) {
			h, e := NewHandler(nil)
			if e != nil {
				t.Fatal(e)
			}
			expiry := time.Now().Add(time.Minute)
			if mode == "expiry" {
				expiry = time.Now().Add(200 * time.Millisecond)
			}
			r := gin.New()
			r.GET("/v1/ws", auth.WSAuthenticationMiddleware(testVerifier{auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: expiry}}), h.ServeHTTP)
			srv := httptest.NewServer(r)
			defer srv.Close()
			defer func() { _ = h.Shutdown(context.Background()) }()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			opts := &websocket.DialOptions{
				Subprotocols: []string{auth.WSTelemetryProtocol},
				HTTPHeader:   http.Header{"Authorization": {"Bearer opaque.token"}},
			}
			if mode == "cap32" {
				var connections []*websocket.Conn
				defer func() {
					for _, c := range connections {
						_ = c.CloseNow()
					}
				}()
				for i := 0; i < maxSessions; i++ {
					c, _, e := websocket.Dial(ctx, "ws"+srv.URL[4:]+"/v1/ws", opts)
					if e != nil {
						t.Fatal(e)
					}
					connections = append(connections, c)
				}
				c, res, e := websocket.Dial(ctx, "ws"+srv.URL[4:]+"/v1/ws", opts)
				if c != nil {
					_ = c.CloseNow()
				}
				if e == nil || res == nil || res.StatusCode != http.StatusServiceUnavailable {
					t.Fatalf("33rd session res=%v err=%v", res, e)
				}
				t.Log("32 admitted; 33rd rejected 503")
				return
			}
			c, _, e := websocket.Dial(ctx, "ws"+srv.URL[4:]+"/v1/ws", opts)
			if e != nil {
				t.Fatal(e)
			}
			defer func() { _ = c.CloseNow() }()
			if mode == "readlimit" {
				_ = c.Write(ctx, websocket.MessageText, make([]byte, 1025))
			}
			_, _, e = c.Read(ctx)
			if e == nil {
				t.Fatal("shell failed to close")
			}
			if ctx.Err() != nil {
				t.Fatal("session only closed at client budget, not server policy")
			}
			t.Logf("%s server close observed: %v", mode, e)
		})
	}
}

func TestReviewP2DisabledStubBearerShape(t *testing.T) {
	v := testVerifier{auth.Principal{UserID: uuid.New(), Role: "authenticated", ExpiresAt: time.Now().Add(time.Minute)}}
	h := auth.NewInternalAuthHandler(v, nil, nil, false)
	for _, p := range []string{"/internal/auth/verify", "/internal/auth/verify/ws"} {
		req := httptest.NewRequest("GET", p, nil)
		req.Header.Set("Authorization", "Bearer opaque.token")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		t.Logf("P2 stub request headers -> %s: %d", p, rec.Code)
		if rec.Code != 204 {
			t.Errorf("P2 Bearer-only stub probe rejected by perimeter route in disabled mode: %d", rec.Code)
		}
	}
}

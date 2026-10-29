// Package telemetryws owns the bounded P7 handshake shell. It intentionally
// emits no application data until the later fan-out work is implemented.
package telemetryws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
)

// HandlerOptions are per-socket bounds. Non-positive values select documented defaults.
type HandlerOptions struct {
	MaxSessions                             int
	MaxSubscriptions                        int
	MaxReadBytes                            int64
	MaxRequestRate                          int
	PingInterval, PongTimeout, WriteTimeout time.Duration
}

func (h *Handler) observer() *Metrics {
	if h == nil || h.app == nil || h.app.Hub == nil {
		return nil
	}
	return h.app.Hub.observer()
}

// ApplicationHandlerDeps binds the application socket to the one shared indexed Hub and
// its DB-backed authorizer. RecheckWorker must be composed once by the server, using Hub
// as both SessionProvider and SessionRevocationHandler; it is not started per socket.
type ApplicationHandlerDeps struct {
	Hub        *Hub
	Authorizer TargetsAuthorizer
	Options    HandlerOptions
}

// Handler owns a bounded number of upgraded handshake-shell sessions.
type Handler struct {
	origins          map[string]struct{}
	slots            chan struct{}
	mu               sync.Mutex
	connections      map[*websocket.Conn]struct{}
	inflight         int
	wg               sync.WaitGroup
	closed           bool
	observeHandshake func(time.Duration)
	app              *ApplicationHandlerDeps
	sessionClose     map[string]func()
}

// ActiveSessions returns the count of fully registered connections and in-flight upgrades.
func (h *Handler) ActiveSessions() (registered, inflight int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.connections), h.inflight
}

// SetHandshakeObserver records the finite HTTP upgrade duration.
func (h *Handler) SetHandshakeObserver(observer func(time.Duration)) { h.observeHandshake = observer }

// NewHandler validates the exact browser Origin allowlist.
func NewHandler(origins []string) (*Handler, error) {
	h := &Handler{origins: make(map[string]struct{}), slots: make(chan struct{}, MaxSessions), connections: make(map[*websocket.Conn]struct{}), sessionClose: make(map[string]func())}
	for _, origin := range origins {
		parsed, err := url.Parse(origin)
		if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || origin == "*" {
			return nil, fmt.Errorf("invalid websocket origin allowlist")
		}
		h.origins[origin] = struct{}{}
	}
	return h, nil
}

// NewApplicationHandler constructs an application-enabled handler and fails closed when
// either shared runtime dependency is absent. NewHandler remains an explicit handshake shell.
func NewApplicationHandler(origins []string, hub *Hub, authorizer TargetsAuthorizer) (*Handler, error) {
	return NewApplicationHandlerWithOptions(origins, ApplicationHandlerDeps{Hub: hub, Authorizer: authorizer})
}

// NewApplicationHandlerWithOptions allows bounded reductions for per-session work and timers.
func NewApplicationHandlerWithOptions(origins []string, deps ApplicationHandlerDeps) (*Handler, error) {
	if deps.Hub == nil || deps.Authorizer == nil {
		return nil, fmt.Errorf("websocket application requires shared hub and target authorizer")
	}
	h, err := NewHandler(origins)
	if err != nil {
		return nil, err
	}
	if deps.Options.MaxSubscriptions <= 0 {
		deps.Options.MaxSubscriptions = MaxSubscriptionsPerSession
	}
	if deps.Options.MaxSubscriptions > MaxSubscriptionsPerSession {
		return nil, fmt.Errorf("websocket subscription limit exceeds protocol maximum")
	}
	if deps.Options.MaxSessions <= 0 {
		deps.Options.MaxSessions = MaxSessions
	}
	if deps.Options.MaxSessions > MaxSupportedSessions {
		return nil, fmt.Errorf("websocket session limit exceeds configured maximum")
	}
	if deps.Options.MaxReadBytes <= 0 {
		deps.Options.MaxReadBytes = MaxClientMessageBytes
	}
	if deps.Options.MaxReadBytes > MaxClientMessageBytes {
		return nil, fmt.Errorf("websocket read limit exceeds protocol maximum")
	}
	if deps.Options.MaxRequestRate <= 0 {
		deps.Options.MaxRequestRate = DefaultMaxClientCommandRate
	}
	if deps.Options.PingInterval <= 0 {
		deps.Options.PingInterval = 20 * time.Second
	}
	if deps.Options.PongTimeout <= 0 {
		deps.Options.PongTimeout = 45 * time.Second
	}
	if deps.Options.WriteTimeout <= 0 {
		deps.Options.WriteTimeout = 5 * time.Second
	}
	h.slots = make(chan struct{}, deps.Options.MaxSessions)
	h.app = &deps
	return h, nil
}

func (h *Handler) allowedOrigin(r *http.Request) bool {
	values := r.Header.Values("Origin")
	if len(values) == 0 {
		return true
	} // native clients do not send Origin
	if len(values) != 1 {
		return false
	}
	origin := values[0]
	parsed, err := url.Parse(origin)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || strings.ContainsAny(origin, "\r\n\x00") {
		return false
	}
	_, ok := h.origins[origin]
	return ok
}

func (h *Handler) ServeHTTP(c *gin.Context) {
	headerBytes := 0
	for name, values := range c.Request.Header {
		headerBytes += len(name)
		for _, value := range values {
			headerBytes += len(value)
		}
		if headerBytes > 24*1024 {
			c.AbortWithStatus(http.StatusRequestHeaderFieldsTooLarge)
			return
		}
	}
	principal, ok := auth.PrincipalFrom(c)
	if !ok || principal.UserID == uuid.Nil || principal.ExpiresAt.IsZero() || !time.Now().Before(principal.ExpiresAt) {
		if h.app != nil && h.app.Hub.observer() != nil {
			h.app.Hub.observer().AuthDenied()
		}
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	if !h.allowedOrigin(c.Request) {
		if h.app != nil && h.app.Hub.observer() != nil {
			h.app.Hub.observer().OriginDenied()
		}
		c.AbortWithStatus(http.StatusForbidden)
		return
	}
	select {
	case h.slots <- struct{}{}:
	default:
		c.AbortWithStatus(http.StatusServiceUnavailable)
		return
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		<-h.slots
		c.AbortWithStatus(http.StatusServiceUnavailable)
		return
	}
	h.inflight++
	h.wg.Add(1)
	h.mu.Unlock()

	var conn *websocket.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close(websocket.StatusNormalClosure, "session ended")
			h.mu.Lock()
			delete(h.connections, conn)
			h.mu.Unlock()
		}
		h.wg.Done()
		<-h.slots
	}()

	started := time.Now()
	acceptedConn, err := websocket.Accept(c.Writer, c.Request, &websocket.AcceptOptions{
		Subprotocols:       []string{auth.WSTelemetryProtocol},
		InsecureSkipVerify: true,
		CompressionMode:    websocket.CompressionDisabled,
	})
	if err != nil {
		h.mu.Lock()
		h.inflight--
		h.mu.Unlock()
		return
	}
	acceptedAt := time.Now()
	// Accept negotiates only the app protocol; credential protocols never reach it.
	if acceptedConn.Subprotocol() != auth.WSTelemetryProtocol {
		h.mu.Lock()
		h.inflight--
		h.mu.Unlock()
		_ = acceptedConn.CloseNow()
		return
	}
	h.mu.Lock()
	if h.closed {
		h.inflight--
		h.mu.Unlock()
		_ = acceptedConn.CloseNow()
		return
	}
	h.inflight--
	h.connections[acceptedConn] = struct{}{}
	conn = acceptedConn
	h.mu.Unlock()

	// Handshake metric ends at upgrade, never at session close.
	if h.observeHandshake != nil {
		h.observeHandshake(time.Since(started))
	}
	if h.app == nil {
		h.runShell(conn, principal)
		return
	}
	h.runApplication(conn, principal, acceptedAt)
}

func (h *Handler) runShell(conn *websocket.Conn, p auth.Principal) {
	ctx, cancel := context.WithDeadline(context.Background(), p.ExpiresAt)
	defer cancel()
	conn.SetReadLimit(1024)
	_, _, _ = conn.Read(ctx)
}

type outboundControl struct {
	payload     []byte
	reservation *QueueReservation
}
type appSession struct {
	ioMu            sync.Mutex
	writeContext    context.Context
	acceptedAt      time.Time
	metrics         *Metrics
	terminationOnce sync.Once
	id              string
	conn            *websocket.Conn
	state           *SessionAccessState
	queue           *SessionQueue
	controls        chan outboundControl
	close           chan struct{}
	once            sync.Once
}

func (s *appSession) stop() {
	s.once.Do(func() {
		close(s.close)
		_ = s.conn.CloseNow()
		for {
			select {
			case ctl := <-s.controls:
				if ctl.reservation != nil {
					ctl.reservation.Release()
				}
			default:
				return
			}
		}
	})
}

func (h *Handler) runApplication(conn *websocket.Conn, p auth.Principal, acceptedAt time.Time) {
	deps := h.app
	sid := uuid.NewString()
	s := &appSession{id: sid, conn: conn, controls: make(chan outboundControl, 16), close: make(chan struct{}), acceptedAt: acceptedAt, metrics: h.observer()}
	state, err := NewSessionAccessState(sid, p.UserID, p.ExpiresAt, SessionRevocationFunc(func(id string, reason RevocationReason) {
		s.recordTermination(context.Background(), nil, reason, "access", nil)
		deps.Hub.OnRevocation(id, reason)
		s.stop()
	}))
	if err != nil {
		s.recordTermination(context.Background(), nil, RevocationReasonUnknown, "state_setup", err)
		s.stop()
		return
	}
	s.state = state
	q, err := deps.Hub.RegisterSession(state)
	if err != nil {
		s.recordTermination(context.Background(), nil, RevocationReasonUnknown, "hub_register", err)
		s.stop()
		return
	}
	s.queue = q
	h.mu.Lock()
	h.sessionClose[sid] = func() {
		s.recordTermination(context.Background(), nil, RevocationReasonShutdown, "shutdown", nil)
		s.stop()
	}
	shuttingDown := h.closed
	h.mu.Unlock()
	if shuttingDown {
		s.recordTermination(context.Background(), nil, RevocationReasonShutdown, "shutdown", nil)
		s.stop()
	}
	defer func() {
		deps.Hub.UnregisterSession(sid)
		h.mu.Lock()
		delete(h.sessionClose, sid)
		h.mu.Unlock()
		s.stop()
	}()
	ctx, cancel := context.WithDeadline(context.Background(), p.ExpiresAt)
	defer cancel()
	conn.SetReadLimit(h.app.Options.MaxReadBytes)
	writerDone := make(chan struct{})
	go func() { defer close(writerDone); h.writeLoop(ctx, s, deps.Options) }()
	pingDone := make(chan struct{})
	go func() { defer close(pingDone); h.pingLoop(ctx, s, deps.Options) }()
	var rateStart time.Time
	rateCount := 0
	for {
		messageType, data, readErr := conn.Read(ctx) // always-active reader processes pong/control frames
		if readErr != nil {
			s.recordIOError(ctx, nil, "read", readErr)
			break
		}
		now := time.Now()
		if now.Sub(rateStart) >= time.Second {
			rateStart = now
			rateCount = 0
		}
		rateCount++
		if rateCount > deps.Options.MaxRequestRate {
			s.recordTermination(ctx, nil, RevocationReasonRateLimit, "read_rate", nil)
			h.enqueueControl(s, NewErrorMessage("", ErrorCodeRateLimitExceeded, "request rate exceeded", ""))
			break
		}
		if messageType != websocket.MessageText {
			h.enqueueControl(s, NewErrorMessage("", ErrorCodeInvalidMessage, "only text JSON messages are supported", ""))
			continue
		}
		msg, parseErr := ParseClientMessage(data)
		if parseErr != nil {
			var pe *ProtocolError
			if errors.As(parseErr, &pe) {
				h.enqueueControl(s, NewErrorMessage("", pe.Code, pe.Message, pe.Field))
			}
			continue
		}
		if msg.Action == ActionSubscribe {
			h.handleSubscribe(ctx, s, msg)
		} else {
			_, e := deps.Hub.Unsubscribe(sid, msg.Targets)
			if e != nil {
				h.enqueueControl(s, NewErrorMessage(msg.RequestID, ErrorCodeInternalError, "subscription update failed", ""))
				continue
			}
			h.enqueueControl(s, NewAckMessage(msg.RequestID, msg.Action, msg.Targets))
		}
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		// The read context deadline is the verified JWT expiry. Close access state
		// before deferred unregister can classify the socket as a client close.
		s.state.Close(RevocationReasonExpired)
	}
	s.stop()
	<-writerDone
	<-pingDone
}

func (h *Handler) handleSubscribe(ctx context.Context, s *appSession, msg ClientMessage) {
	newCount := 0
	for _, target := range DeduplicateTargets(msg.Targets) {
		if !s.state.HasTarget(target) {
			newCount++
		}
	}
	if s.state.TargetCount()+newCount > h.app.Options.MaxSubscriptions {
		h.enqueueControl(s, NewErrorMessage(msg.RequestID, ErrorCodeTargetLimitExceeded, "subscription limit exceeded", "targets"))
		return
	}
	gen, err := s.state.PrepareSubscribe(msg.Targets, time.Now())
	if err != nil {
		h.enqueueControl(s, NewErrorMessage(msg.RequestID, ErrorCodeTargetLimitExceeded, "subscription limit exceeded", "targets"))
		return
	}
	allowed, err := h.app.Authorizer.AuthorizeTargets(ctx, s.state.UserID(), msg.Targets)
	if err != nil {
		code := ErrorCodeUnauthorized
		text := "subscription target unauthorized"
		if !errors.Is(err, ErrTargetUnauthorized) {
			code = ErrorCodeInternalError
			text = "authorization unavailable"
		}
		h.enqueueControl(s, NewErrorMessage(msg.RequestID, code, text, ""))
		return
	}
	if _, err = h.app.Hub.CommitSubscribe(s.id, gen, allowed, time.Now()); err != nil {
		h.enqueueControl(s, NewErrorMessage(msg.RequestID, ErrorCodeInternalError, "subscription update failed", ""))
		return
	}
	h.enqueueControl(s, NewAckMessage(msg.RequestID, msg.Action, msg.Targets))
}

func (h *Handler) enqueueControl(s *appSession, msg any) {
	b, err := json.Marshal(msg)
	if err != nil || len(b) > MaxClientMessageBytes {
		s.recordTermination(context.Background(), nil, RevocationReasonUnknown, "control_encode", err)
		s.stop()
		return
	}
	reservation, reserveErr := s.queue.ReserveControl(len(b), 16)
	if reserveErr != nil {
		reason := RevocationReasonUnknown
		if errors.Is(reserveErr, ErrAggregateBudget) {
			reason = RevocationReasonAggregateCapacity
		} else if errors.Is(reserveErr, ErrQueueFull) {
			reason = RevocationReasonSlowConsumer
		}
		s.recordTermination(context.Background(), nil, reason, "control_reserve", reserveErr)
		if reason == RevocationReasonAggregateCapacity {
			if m := h.observer(); m != nil {
				m.AggregateCapacityClose()
			}
		}
		s.stop()
		return
	}
	select {
	case s.controls <- outboundControl{payload: b, reservation: reservation}:
	default:
		reservation.Release()
		s.recordTermination(context.Background(), nil, RevocationReasonSlowConsumer, "control_enqueue", nil)
		s.stop()
	}
}

func (h *Handler) writeLoop(ctx context.Context, s *appSession, opts HandlerOptions) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.close:
			return
		case ctl := <-s.controls:
			err := writeApplicationFrame(ctx, s, opts.WriteTimeout, ctl.payload)
			ctl.reservation.Release()
			if err != nil {
				s.stop()
				return
			}
			if m := h.observer(); m != nil {
				m.ControlWriteCompleted()
			}
		case <-s.queue.notifyCh:
			for {
				// Recheck control/close between frames so a burst cannot starve controls or
				// continue draining after access revocation.
				select {
				case <-s.close:
					return
				default:
				}
				select {
				case ctl := <-s.controls:
					err := writeApplicationFrame(ctx, s, opts.WriteTimeout, ctl.payload)
					ctl.reservation.Release()
					if err != nil {
						s.stop()
						return
					}
					if m := h.observer(); m != nil {
						m.ControlWriteCompleted()
					}
				default:
				}
				if s.state.IsClosed() || !time.Now().Before(s.state.ExpiresAt()) {
					return
				}
				frame, err := s.queue.Dequeue()
				if err != nil {
					return
				}
				if frame == nil {
					break
				}
				if !matchesActive(s.state, frame.Event()) {
					frame.Release()
					continue
				}
				err = writeApplicationFrame(ctx, s, opts.WriteTimeout, frame.Payload())
				if err == nil {
					if m := h.observer(); m != nil {
						m.TelemetryWriteCompleted(frame.Event().ConfirmedObservedAt())
					}
				}
				frame.Release()
				if err != nil {
					s.stop()
					return
				}
			}
		}
	}
}

func writeApplicationFrame(ctx context.Context, s *appSession, timeout time.Duration, payload []byte) error {
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	s.ioMu.Lock()
	s.writeContext = wctx
	s.ioMu.Unlock()
	err := s.conn.Write(wctx, websocket.MessageText, payload)
	if err != nil {
		s.recordIOError(ctx, wctx, "write", err)
	}
	s.ioMu.Lock()
	s.writeContext = nil
	s.ioMu.Unlock()
	return err
}

func matchesActive(s *SessionAccessState, e *SharedEvent) bool {
	return MatchesAnyTarget(s.Targets(), e.GatewayID(), e.SensorID())
}

func (h *Handler) pingLoop(ctx context.Context, s *appSession, opts HandlerOptions) {
	t := time.NewTicker(opts.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.close:
			return
		case <-t.C:
			// PongTimeout is the total read/pong inactivity budget from the prior
			// ping, not an extra wait after the next ping. Ping itself waits for a
			// pong while the active Read loop processes control frames.
			wait := opts.PongTimeout - opts.PingInterval
			if wait <= 0 {
				wait = opts.PingInterval
			}
			pctx, cancel := context.WithTimeout(ctx, wait)
			err := s.conn.Ping(pctx)
			if err != nil {
				s.recordIOError(ctx, pctx, "ping", err)
				cancel()
				s.stop()
				return
			}
			cancel()
		}
	}
}

// Shutdown stops admission and closes every upgraded socket within the context budget; safe to repeat.
func (h *Handler) Shutdown(ctx context.Context) error {
	if h == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return h.wait(ctx)
	}
	h.closed = true
	connections := make([]*websocket.Conn, 0, len(h.connections))
	for conn := range h.connections {
		connections = append(connections, conn)
	}
	h.mu.Unlock()
	h.mu.Lock()
	closers := make([]func(), 0, len(h.sessionClose))
	for _, closeSession := range h.sessionClose {
		closers = append(closers, closeSession)
	}
	h.mu.Unlock()
	for _, closeSession := range closers {
		closeSession()
	}
	for _, conn := range connections {
		_ = conn.CloseNow()
	}
	return h.wait(ctx)
}

func (h *Handler) wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		select {
		case <-done:
			return nil
		default:
			return fmt.Errorf("websocket shutdown unconfirmed: %w", ctx.Err())
		}
	}
}

package gatewaysim

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// The transport is the Gateway-side mirror of internal/mqttingress: it keeps
// one Paho connection lifetime, subscribes to exactly its own ACK filter, and
// publishes telemetry. It reuses the *patterns* of the ingress transport
// (single reconnect supervisor, epoch-gated stale callbacks, a fail-closed
// stuck teardown, a bounded publish semaphore held until the blocking Paho call
// returns) without importing or modifying that package.
//
// Scope: this file owns MQTT/TLS transport only. It never touches the SQLite
// outbox, never deletes a row on MQTT PUBACK, and never retries or reschedules
// a batch. A successful Publish only means the broker acknowledged the MQTT
// PUBLISH; the durable outbox row is removed exclusively by the ACK path after
// a committed application-level ACK.

const (
	// defaultReconnectBase is the first reconnect delay before jitter. It is a
	// starting default, not a measured capacity number.
	defaultReconnectBase = 250 * time.Millisecond
	// defaultCloseWait bounds how long a teardown waits for publish invocations
	// started on a lifetime to return after its socket is closed.
	defaultCloseWait = 2 * time.Second
	// defaultPublishStopGrace is the extra bounded wait after a publish budget
	// expires for the invocation goroutine to observe the socket close and
	// return, so the whole-call bound holds without leaking a goroutine.
	defaultPublishStopGrace = 250 * time.Millisecond
	// maxCABytes bounds the CA file the transport reads at startup.
	maxCABytes = 1 << 20

	// ackSubscribeQoS is the QoS requested for the single own-ACK subscription.
	// The granted SUBACK code must be a valid QoS no higher than this.
	ackSubscribeQoS byte = 1
	// telemetryPublishQoS is the QoS used for every telemetry publish. Telemetry
	// is always published QoS 1 and non-retained.
	telemetryPublishQoS byte = 1
)

// errLifetimeClosed is returned by the custom dial when the connection lifetime
// was already torn down while the socket was being opened.
var errLifetimeClosed = errors.New("gatewaysim: connection lifetime closed")

// Stable, payload-free transport errors. They are sentinels so the scheduler
// can classify a failure without inspecting driver text or credentials.
var (
	// ErrTransportClosed marks an operation on a stopped transport.
	ErrTransportClosed = errors.New("gatewaysim: transport is closed")
	// ErrPublishUnconfirmed marks a telemetry publish that was not confirmed.
	// It is never proof the telemetry was stored: only an application ACK is.
	ErrPublishUnconfirmed = errors.New("gatewaysim: telemetry publish was not confirmed")
	// ErrTransportStuck marks a fail-closed teardown: a tracked publish
	// invocation stayed wedged after its socket close. The transport stops
	// reconnecting and stays not ready until a supervised restart.
	ErrTransportStuck = errors.New("gatewaysim: transport teardown unconfirmed; supervised restart required")
)

// ACKAdmitter accepts one raw inbound ACK callback. *ACKProcessor satisfies it;
// Admit performs only a bounds check and a copy and returns immediately, so the
// transport never runs JSON parsing or SQL in the broker callback.
type ACKAdmitter interface {
	Admit(topic string, payload []byte, retained bool)
}

// TransportOptions configures the Paho telemetry transport and its supervisor.
// GatewayID derives the single own ACK filter; the CA is loaded and validated
// at construction so startup fails clearly. The unexported fields are test
// seams and stay zero in production.
type TransportOptions struct {
	// GatewayID is the configured Gateway identity. It must equal Username and
	// is used to derive the exact own ACK subscription filter.
	GatewayID string
	BrokerURL string
	ClientID  string
	Username  string
	Password  string
	CAFile    string

	ConnectTimeout       time.Duration
	SubscribeTimeout     time.Duration
	PublishTimeout       time.Duration
	ReconnectMaxInterval time.Duration
	// CloseWaitTimeout bounds the teardown wait for publish invocations started
	// on a lifetime to return after its socket is closed.
	CloseWaitTimeout time.Duration
	// MaxInflight bounds how many Publish invocations may be in flight at once.
	// Zero uses defaultMaxInflight. It bounds pending publish calls, not the
	// scheduler's own waiting-for-ACK window.
	MaxInflight int

	// Logger receives reason-coded, secret-free lifecycle events. Nil uses
	// slog.Default().
	Logger *slog.Logger

	// Test seams.
	roots            *x509.CertPool
	newClient        func(*mqtt.ClientOptions) mqttClient
	dial             func(context.Context, string) (net.Conn, error)
	randFloat        func() float64
	baseBackoff      time.Duration
	publishStopGrace time.Duration
}

// TransportOptionsFromConfig maps a validated simulator Config into transport
// options. It carries no secret into a log: the returned value is only passed
// to NewTransport.
func TransportOptionsFromConfig(cfg Config) TransportOptions {
	return TransportOptions{
		GatewayID:            cfg.GatewayID,
		BrokerURL:            cfg.BrokerURL,
		ClientID:             cfg.ClientID,
		Username:             cfg.Username,
		Password:             cfg.Password,
		CAFile:               cfg.CAFile,
		ConnectTimeout:       cfg.ConnectTimeout,
		SubscribeTimeout:     cfg.SubscribeTimeout,
		PublishTimeout:       cfg.PublishTimeout,
		ReconnectMaxInterval: cfg.RetryMaxDelay,
		CloseWaitTimeout:     cfg.ShutdownTimeout,
		MaxInflight:          cfg.MaxInflight,
	}
}

// lifetime is one MQTT client connection owned by the supervisor. Callbacks
// captured from this lifetime must verify they are still current before they
// change any state, so a stale callback cannot restore or invalidate a
// replacement.
type lifetime struct {
	epoch  uint64
	client mqttClient

	subscribed atomic.Bool
	failed     atomic.Bool

	mu     sync.Mutex
	conn   net.Conn
	closed bool
	// ops counts publish invocations started on this lifetime. beginOp adds
	// under mu and refuses once closed, so once closeSocket has run no new
	// invocation can start and a bounded waitOps cannot race a fresh Add.
	ops sync.WaitGroup

	// wake is closed at most once when the supervisor should tear this lifetime
	// down and reconnect. It is only a wake signal, NOT proof that Paho cleanup
	// completed, and must never be treated as teardown confirmation.
	wake     chan struct{}
	wakeOnce sync.Once
}

func newLifetime(epoch uint64) *lifetime {
	return &lifetime{epoch: epoch, wake: make(chan struct{})}
}

func (lt *lifetime) usable() bool { return lt.subscribed.Load() && !lt.failed.Load() }

func (lt *lifetime) markFailed() { lt.failed.Store(true) }

// signalWake closes the wake channel at most once. It asks the supervisor to
// tear this lifetime down and reconnect; it does not mean Paho has finished
// cleaning up.
func (lt *lifetime) signalWake() { lt.wakeOnce.Do(func() { close(lt.wake) }) }

// beginOp registers one in-flight publish invocation. It returns false when the
// lifetime is already closed or failed, so no invocation starts on a failed
// lifetime.
func (lt *lifetime) beginOp() bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	if lt.closed || lt.failed.Load() {
		return false
	}
	lt.ops.Add(1)
	return true
}

func (lt *lifetime) finishOp() { lt.ops.Done() }

// waitOps waits until every publish invocation started on this lifetime has
// returned, or until d elapses. beginOp is gated by the same lock closeSocket
// holds, so after closeSocket no new invocation can start and this cannot race
// a fresh Add.
func (lt *lifetime) waitOps(d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		lt.ops.Wait()
		close(done)
	}()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// attach stores the freshly opened socket so teardown can close it. It returns
// false when the lifetime was already closed, in which case the caller closes
// the socket immediately.
func (lt *lifetime) attach(conn net.Conn) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	if lt.closed {
		return false
	}
	lt.conn = conn
	return true
}

// closeSocket marks the lifetime closed and closes the tracked socket exactly
// once, interrupting any Paho read/write loop waiting on it and preventing any
// further publish invocation from starting.
func (lt *lifetime) closeSocket() {
	lt.mu.Lock()
	lt.closed = true
	conn := lt.conn
	lt.conn = nil
	lt.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// Transport is the Paho telemetry transport and its single connection
// supervisor. It consumes an ACKAdmitter (the bounded ACK processor) and
// publishes telemetry. Exactly one client lifetime is active at a time; Paho
// AutoReconnect/ConnectRetry are disabled so this supervisor is the only
// reconnect owner.
type Transport struct {
	opts   TransportOptions
	admit  ACKAdmitter
	url    *url.URL
	tls    *tls.Config
	log    *slog.Logger
	filter string

	mu               sync.Mutex
	current          *lifetime
	nextEpoch        uint64
	reconnectStopped bool
	closed           bool
	stateCh          chan struct{}

	// stuck marks a fail-closed teardown: a tracked publish invocation did not
	// return within the teardown budget after its socket was closed. Once set,
	// the supervisor stops reconnecting and readiness stays false until a
	// supervised restart, so a wedged Paho call cannot be replaced in a loop
	// that leaks a waiter per reconnect.
	stuck atomic.Bool

	// publishSem bounds concurrent publish invocations. It is a buffered channel
	// used only as a fixed-capacity semaphore.
	publishSem chan struct{}

	stopReconnect chan struct{}
	stopOnce      sync.Once

	superviseCtx    context.Context
	cancelSupervise context.CancelFunc
	superviseDone   chan struct{}

	startMu sync.Mutex
	started bool

	fatalMu   sync.Mutex
	fatalErr  error
	fatalOnce sync.Once
	fatalCh   chan struct{}

	counters transportCounters
}

// NewTransport validates the identity and CA, builds the TLS configuration and
// returns a transport. The CA is read and parsed here so startup fails clearly
// on an invalid or unreadable CA file; the CA path is never included in an
// error so a filesystem path cannot leak through logs.
func NewTransport(opts TransportOptions, admit ACKAdmitter) (*Transport, error) {
	if admit == nil {
		return nil, errors.New("gatewaysim: transport requires an ACK admitter")
	}
	if opts.GatewayID == "" {
		return nil, errors.New("gatewaysim: transport gateway id is required")
	}
	if opts.Username != opts.GatewayID {
		return nil, errors.New("gatewaysim: transport username must equal the gateway id")
	}
	u, err := url.Parse(opts.BrokerURL)
	if err != nil {
		return nil, errors.New("gatewaysim: broker URL must be an ssl:// URL with host and port and no userinfo, path, query or fragment")
	}
	if err := validateTLSBrokerURL(opts.BrokerURL); err != nil {
		return nil, err
	}
	if opts.ClientID == "" {
		return nil, errors.New("gatewaysim: client ID is required")
	}
	if opts.Username == "" {
		return nil, errors.New("gatewaysim: username is required")
	}
	if opts.Password == "" {
		return nil, errors.New("gatewaysim: password is required")
	}

	roots := opts.roots
	if roots == nil {
		roots, err = loadRootPool(opts.CAFile)
		if err != nil {
			return nil, err
		}
	}

	opts.ConnectTimeout = positiveOr(opts.ConnectTimeout, defaultConnectTimeout)
	opts.SubscribeTimeout = positiveOr(opts.SubscribeTimeout, defaultSubscribeTimeout)
	opts.PublishTimeout = positiveOr(opts.PublishTimeout, defaultPublishTimeout)
	opts.ReconnectMaxInterval = positiveOr(opts.ReconnectMaxInterval, defaultRetryMax)
	opts.CloseWaitTimeout = positiveOr(opts.CloseWaitTimeout, defaultCloseWait)
	opts.baseBackoff = positiveOr(opts.baseBackoff, defaultReconnectBase)
	opts.publishStopGrace = positiveOr(opts.publishStopGrace, defaultPublishStopGrace)
	if opts.MaxInflight <= 0 {
		opts.MaxInflight = defaultMaxInflight
	}

	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	if opts.randFloat == nil {
		opts.randFloat = rand.Float64
	}

	t := &Transport{
		opts:          opts,
		admit:         admit,
		url:           u,
		log:           log,
		filter:        "gateways/" + opts.GatewayID + "/acks/#",
		stateCh:       make(chan struct{}),
		publishSem:    make(chan struct{}, opts.MaxInflight),
		stopReconnect: make(chan struct{}),
		fatalCh:       make(chan struct{}),
	}
	t.tls = &tls.Config{
		RootCAs:    roots,
		ServerName: u.Hostname(),
		MinVersion: tls.VersionTLS12,
	}
	if opts.newClient == nil {
		t.opts.newClient = newPahoClient
	}
	if opts.dial == nil {
		t.opts.dial = t.dialTLS
	}
	t.superviseCtx, t.cancelSupervise = context.WithCancel(context.Background())
	return t, nil
}

// loadRootPool reads a CA file within a bounded size and returns a pool with at
// least one usable certificate. A missing, oversized, non-regular or
// certificate-less file is a clear startup failure.
func loadRootPool(path string) (*x509.CertPool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, errors.New("gatewaysim: CA file is not readable")
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxCABytes {
		return nil, errors.New("gatewaysim: CA file is not a regular file within bounds")
	}
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("gatewaysim: CA file is not readable")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, errors.New("gatewaysim: CA file contains no usable certificate")
	}
	return pool, nil
}

// dialTLS opens a TLS socket with the verified CA and hostname. It respects the
// supplied context so cancellation stops dialing and the handshake.
func (t *Transport) dialTLS(ctx context.Context, host string) (net.Conn, error) {
	dialer := tls.Dialer{NetDialer: &net.Dialer{}, Config: t.tls.Clone()}
	return dialer.DialContext(ctx, "tcp", host)
}

// Start launches the supervisor. It may be called once.
func (t *Transport) Start() error {
	t.startMu.Lock()
	defer t.startMu.Unlock()
	if t.started {
		return errors.New("gatewaysim: transport already started")
	}
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return ErrTransportClosed
	}
	t.started = true
	t.superviseDone = make(chan struct{})
	go t.supervise(t.superviseCtx)
	return nil
}

// StopReconnect stops the reconnect loop and aborts any in-flight dial, token
// wait or backoff, but deliberately leaves the current connection open so the
// scheduler can still publish within a drain window. Stop performs the final
// teardown. It is safe to call from multiple goroutines.
func (t *Transport) StopReconnect() {
	t.stopOnce.Do(func() {
		t.mu.Lock()
		t.reconnectStopped = true
		t.mu.Unlock()
		close(t.stopReconnect)
	})
	t.cancelSupervise()
}

// Stop stops reconnecting, waits for the supervisor to exit (bounded by ctx),
// then tears down the current connection within the caller's remaining budget.
// It is idempotent and concurrency safe. It returns ctx.Err() when the
// supervisor does not exit in time and otherwise the operational fatal error,
// if any; the underlying SQLite outbox is never touched.
func (t *Transport) Stop(ctx context.Context) error {
	t.StopReconnect()
	if ctx == nil {
		ctx = context.Background()
	}

	t.mu.Lock()
	alreadyClosed := t.closed
	t.closed = true
	t.mu.Unlock()

	t.startMu.Lock()
	started := t.started
	t.startMu.Unlock()

	var err error
	if started && !alreadyClosed {
		select {
		case <-t.superviseDone:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	if lt := t.takeCurrent(); lt != nil {
		// Keep the final teardown inside the caller's budget so Stop cannot
		// outlive a shared shutdown deadline waiting on a wedged Paho call.
		t.closeLifetime(lt, t.closeWaitBudget(ctx))
	}
	if err == nil {
		err = t.Fatal()
	}
	return err
}

// closeWaitBudget bounds the final publish-teardown wait by both the configured
// close wait and the caller's remaining deadline.
func (t *Transport) closeWaitBudget(ctx context.Context) time.Duration {
	budget := t.opts.CloseWaitTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < budget {
			budget = remaining
		}
	}
	if budget < 0 {
		budget = 0
	}
	return budget
}

// Ready reports whether a current connection has a positive own-ACK
// subscription and has not failed or been marked stuck.
func (t *Transport) Ready() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.readyLocked()
}

func (t *Transport) readyLocked() bool {
	if t.stuck.Load() {
		return false
	}
	lt := t.current
	return lt != nil && lt.usable()
}

// ReadyCh returns a channel that is closed when the transport readiness state
// next changes. A scheduler that wants a readiness or loss signal should read
// Ready after receiving and call ReadyCh again for the following transition.
func (t *Transport) ReadyCh() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stateCh
}

// WaitReady blocks until the transport is ready or ctx ends. It never reports
// ready without a live, subscribed connection.
func (t *Transport) WaitReady(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		t.mu.Lock()
		if t.readyLocked() {
			t.mu.Unlock()
			return nil
		}
		stuck := t.stuck.Load()
		ch := t.stateCh
		t.mu.Unlock()
		if stuck {
			return ErrTransportStuck
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Publish publishes one telemetry batch topic/payload unchanged. The whole
// call, including the blocking Paho Publish and the token wait, is bounded by
// the smaller of ctx and the configured publish timeout. The synchronous Paho
// Publish runs in a single bounded invocation goroutine; on budget expiry the
// tracked socket is closed so Paho is unblocked, and a short bounded grace
// waits for the invocation to return. A timeout or failure marks the current
// lifetime unusable and asks the supervisor to replace it, so no further
// publish is accepted on a failed lifetime.
//
// It never deletes or updates the outbox: a nil return only means the broker
// acknowledged the MQTT PUBLISH, not that the backend committed the batch.
func (t *Transport) Publish(ctx context.Context, topic string, payload []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Reject a canceled or already-expired context before any side effect, so a
	// caller that has given up cannot start a Paho call or a goroutine.
	if ctx.Err() != nil {
		t.counters.publishUnavailable.Add(1)
		return ErrPublishUnconfirmed
	}

	lt := t.currentLifetime()
	if lt == nil || !lt.usable() {
		t.counters.publishUnavailable.Add(1)
		return ErrPublishUnconfirmed
	}

	// Bound concurrent invocations with a fixed-capacity semaphore so a stalled
	// broker cannot accumulate unbounded goroutines. Waiting for a slot is
	// bounded by the caller's context. The slot is released only when the
	// invocation goroutine itself returns (not when this Publish call returns),
	// so a Paho call that stays wedged after the socket close keeps its slot and
	// the total number of stuck invocations stays bounded.
	select {
	case t.publishSem <- struct{}{}:
	case <-ctx.Done():
		t.counters.publishUnavailable.Add(1)
		return ErrPublishUnconfirmed
	}

	// The lifetime may have failed while this call waited for a slot. From here
	// the slot is owned by the invocation, so a refusal releases it explicitly.
	lt = t.currentLifetime()
	if lt == nil || !lt.usable() {
		<-t.publishSem
		t.counters.publishUnavailable.Add(1)
		return ErrPublishUnconfirmed
	}
	if !lt.beginOp() {
		<-t.publishSem
		t.counters.publishUnavailable.Add(1)
		return ErrPublishUnconfirmed
	}

	publishCtx, cancel := context.WithTimeout(ctx, t.opts.PublishTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		// The op and the semaphore slot stay held until the invocation itself
		// returns, even if this Publish call already gave up. A teardown can then
		// wait on the op, and no further publish can start a new stuck call once
		// every slot is held by a wedged invocation.
		defer func() { <-t.publishSem }()
		defer lt.finishOp()
		token := lt.client.Publish(topic, telemetryPublishQoS, false, payload)
		done <- awaitToken(publishCtx, token)
	}()

	select {
	case err := <-done:
		if err != nil {
			// A timed-out or failed publish proves only an unconfirmed publish.
			// Mark the lifetime unusable and wake the supervisor to replace it;
			// the durable outbox row is untouched.
			t.failCurrent(lt)
			t.counters.publishFailures.Add(1)
			return ErrPublishUnconfirmed
		}
		t.counters.published.Add(1)
		return nil
	case <-publishCtx.Done():
		// The whole-call budget expired while Paho was still blocked. Close the
		// socket to unblock it, then wait a bounded grace for the invocation to
		// return so it is not leaked.
		t.failCurrent(lt)
		select {
		case <-done:
		case <-time.After(t.opts.publishStopGrace):
			// Practical limitation: if Paho has still not returned after the
			// socket close, the invocation goroutine is abandoned rather than
			// blocking this call past its budget. It is bounded and cannot start
			// new work on the failed lifetime.
		}
		t.counters.publishFailures.Add(1)
		return ErrPublishUnconfirmed
	}
}

// failCurrent marks a lifetime failed, prevents further invocations by closing
// its socket, clears it as the current lifetime and wakes the supervisor to
// replace it.
func (t *Transport) failCurrent(lt *lifetime) {
	lt.markFailed()
	lt.closeSocket()
	t.clearCurrent(lt)
	lt.signalWake()
}

// supervise is the single reconnect owner. It connects, subscribes and installs
// one lifetime at a time, then waits for its loss, shutdown or StopReconnect.
func (t *Transport) supervise(ctx context.Context) {
	defer close(t.superviseDone)
	attempt := 0
	for {
		if ctx.Err() != nil || t.reconnectStoppedLocked() || t.stuck.Load() {
			return
		}
		lt, err := t.connectOnce(ctx)
		if err != nil {
			t.counters.connectFailures.Add(1)
			t.log.Debug("gatewaysim transport connect failed", "event", "connect_failed")
			if !t.waitBackoff(ctx, attempt) {
				return
			}
			attempt++
			continue
		}
		if !t.installCurrent(lt) {
			// The connection was lost between the positive SUBACK and install.
			t.counters.connectFailures.Add(1)
			t.closeLifetime(lt, 0)
			if !t.waitBackoff(ctx, attempt) {
				return
			}
			attempt++
			continue
		}
		t.counters.connected.Add(1)
		t.log.Debug("gatewaysim transport connected", "event", "connected", "epoch", lt.epoch)
		attempt = 0

		select {
		case <-ctx.Done():
			// Leave the connection open: Stop tears it down.
			return
		case <-t.stopReconnect:
			// Leave the connection open for the drain window.
			return
		case <-lt.wake:
		}

		t.clearCurrent(lt)
		clean := t.closeLifetime(lt, t.opts.CloseWaitTimeout)
		t.counters.disconnected.Add(1)

		if !clean {
			// A tracked Publish invocation stayed wedged after its socket was
			// closed. closeLifetime already recorded the fail-closed teardown;
			// do not replace the lifetime, because reconnecting would start a
			// new lifetime while the wedged invocation keeps its waiter and
			// slot, leaking one waiter per reconnect. Stay not-ready until a
			// supervised restart.
			return
		}

		if ctx.Err() != nil || t.reconnectStoppedLocked() {
			return
		}
		if !t.waitBackoff(ctx, attempt) {
			return
		}
		attempt++
	}
}

// connectOnce opens one fresh client lifetime: TLS dial, CONNECT, exact-filter
// SUBSCRIBE at QoS 1, and a positive SUBACK check. On any failure it tears the
// partial lifetime down before returning.
func (t *Transport) connectOnce(ctx context.Context) (*lifetime, error) {
	lt := newLifetime(t.allocEpoch())

	attemptCtx, cancelAttempt := context.WithTimeout(ctx, t.opts.ConnectTimeout)
	defer cancelAttempt()

	opts := mqtt.NewClientOptions().
		AddBroker(t.opts.BrokerURL).
		SetClientID(t.opts.ClientID).
		SetUsername(t.opts.Username).
		SetPassword(t.opts.Password).
		SetProtocolVersion(4).
		SetCleanSession(true).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetOrderMatters(true).
		SetResumeSubs(false).
		SetConnectTimeout(t.opts.ConnectTimeout).
		SetWriteTimeout(t.opts.PublishTimeout)

	opts.SetConnectionLostHandler(func(mqtt.Client, error) { t.onLost(lt) })
	opts.SetCustomOpenConnectionFn(func(u *url.URL, _ mqtt.ClientOptions) (net.Conn, error) {
		conn, err := t.opts.dial(attemptCtx, u.Host)
		if err != nil {
			return nil, err
		}
		if !lt.attach(conn) {
			_ = conn.Close()
			return nil, errLifetimeClosed
		}
		return conn, nil
	})

	client := t.opts.newClient(opts)
	lt.client = client

	if err := awaitToken(attemptCtx, client.Connect()); err != nil {
		t.closeLifetime(lt, 0)
		return nil, err
	}

	subCtx, cancelSub := context.WithTimeout(ctx, t.opts.SubscribeTimeout)
	defer cancelSub()
	subToken := client.Subscribe(t.filter, ackSubscribeQoS, func(topic string, payload []byte, retained bool) {
		t.onMessage(lt, topic, payload, retained)
	})
	if err := waitSubscribeToken(subCtx, subToken, t.filter); err != nil {
		t.counters.subscribeFailed.Add(1)
		t.closeLifetime(lt, 0)
		return nil, err
	}
	if lt.failed.Load() {
		// A connection loss (or explicit failure) arrived before install. Do not
		// return a lifetime that is already failed or expose a stale SUBACK.
		t.closeLifetime(lt, 0)
		return nil, errLifetimeClosed
	}

	lt.subscribed.Store(true)
	return lt, nil
}

// waitSubscribeToken waits for the SUBSCRIBE token and then requires a valid
// granted-QoS SUBACK. A completed token alone is not proof the subscription was
// granted, a non-0x80 code is not automatically valid, and a broker must not
// grant a QoS above the requested QoS.
func waitSubscribeToken(ctx context.Context, token subscribeToken, filter string) error {
	if err := awaitToken(ctx, token); err != nil {
		return err
	}
	result := token.Result()
	granted, ok := result[filter]
	if !ok {
		return fmt.Errorf("gatewaysim: subscription was not granted")
	}
	if granted == 0x80 {
		return fmt.Errorf("gatewaysim: subscription was denied by the broker")
	}
	if granted > 2 {
		return fmt.Errorf("gatewaysim: subscription returned an invalid granted QoS")
	}
	if granted > ackSubscribeQoS {
		return fmt.Errorf("gatewaysim: subscription granted a QoS higher than requested")
	}
	return nil
}

// awaitToken waits for a token, preferring an already-completed result over a
// simultaneous context expiry. It never abandons a goroutine.
func awaitToken(ctx context.Context, token waitToken) error {
	if token == nil {
		return errors.New("gatewaysim: nil token")
	}
	select {
	case <-token.Done():
		return token.Error()
	default:
	}
	select {
	case <-token.Done():
		return token.Error()
	case <-ctx.Done():
		select {
		case <-token.Done():
			return token.Error()
		default:
		}
		return ctx.Err()
	}
}

// onMessage runs in Paho's ordered router goroutine. It admits the delivery
// only when this lifetime is still current and usable; otherwise the stale
// callback is dropped. It never blocks, decodes JSON, touches SQLite or spawns
// a goroutine: Admit copies the payload into the bounded ACK queue.
func (t *Transport) onMessage(lt *lifetime, topic string, payload []byte, retained bool) {
	if !lt.usable() || t.currentLifetime() != lt {
		t.counters.staleDropped.Add(1)
		return
	}
	t.admit.Admit(topic, payload, retained)
}

// onLost is the Paho connection-lost callback. It immediately makes readiness
// false for this lifetime and wakes the supervisor. A stale loss from an
// already-replaced lifetime cannot invalidate the replacement. It only signals
// the wake; it does not claim that teardown or cleanup has completed.
func (t *Transport) onLost(lt *lifetime) {
	lt.markFailed()
	lt.signalWake()
	t.clearCurrent(lt)
	t.counters.connectionLost.Add(1)
}

// closeLifetime interrupts a lifetime: it marks it failed, closes the tracked
// socket (which unblocks Paho's read/write loops and prevents any further
// publish invocation from starting) and asks Paho to disconnect best-effort.
// When wait is positive it then waits that long for publish invocations started
// on this lifetime to return, and reports whether they all returned.
//
// Paho's Disconnect(0) teardown is asynchronous (it dispatches the packet and
// returns) and exposes no public completion signal; a connection-lost callback
// is not guaranteed for a self-initiated disconnect. This bounded wait is
// therefore only a practical bound on our own tracked operations, not proof
// that Paho finished freeing its workers or closed the socket. The tracked
// socket is closed here and Paho closes its own copy during teardown; a double
// close is harmless.
//
// A timed-out wait means a tracked Paho invocation stayed wedged after the
// socket close. That is fail-closed: the transport records the teardown as
// stuck, records a fatal error and stops reconnecting rather than replacing the
// lifetime in a loop that would leak one waiter per reconnect.
func (t *Transport) closeLifetime(lt *lifetime, wait time.Duration) bool {
	if lt == nil {
		return true
	}
	lt.markFailed()
	lt.closeSocket()
	if lt.client != nil {
		lt.client.Disconnect(0)
	}
	if wait <= 0 {
		return true
	}
	if lt.waitOps(wait) {
		return true
	}
	t.markStuck()
	return false
}

// markStuck records a fail-closed teardown exactly once: it makes readiness
// false, records the fatal error and stops the supervisor from replacing the
// wedged lifetime.
func (t *Transport) markStuck() {
	if t.stuck.Swap(true) {
		return
	}
	t.counters.teardownStuck.Add(1)
	t.mu.Lock()
	t.notifyStateLocked()
	t.mu.Unlock()
	t.log.Debug("gatewaysim transport teardown unconfirmed; fail-closed until supervised restart", "event", "teardown_stuck")
	t.setFatal(ErrTransportStuck)
}

// FatalSignal is closed once on an operational failure that requires a
// supervised restart. It is the composition's cue to stop the simulator.
func (t *Transport) FatalSignal() <-chan struct{} { return t.fatalCh }

// Fatal returns the first operational failure, or nil while healthy.
func (t *Transport) Fatal() error {
	t.fatalMu.Lock()
	defer t.fatalMu.Unlock()
	return t.fatalErr
}

// setFatal records the first operational failure and closes the fatal signal
// exactly once. It never records a raw error string as a public counter.
func (t *Transport) setFatal(err error) {
	t.fatalMu.Lock()
	if t.fatalErr == nil {
		t.fatalErr = err
	}
	t.fatalMu.Unlock()
	t.fatalOnce.Do(func() { close(t.fatalCh) })
}

// waitBackoff sleeps a jittered, bounded, exponentially growing delay. It
// returns false when the context is cancelled or reconnect was stopped.
func (t *Transport) waitBackoff(ctx context.Context, attempt int) bool {
	delay := t.backoffDelay(attempt)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.stopReconnect:
		return false
	case <-timer.C:
		return true
	}
}

// backoffDelay is an equal-jitter exponential delay capped at the configured
// maximum, so backoff is bounded and never a tight loop.
func (t *Transport) backoffDelay(attempt int) time.Duration {
	base := t.opts.baseBackoff
	maxDelay := t.opts.ReconnectMaxInterval
	if base > maxDelay {
		base = maxDelay
	}
	delay := base
	for i := 0; i < attempt; i++ {
		if delay >= maxDelay {
			break
		}
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
	factor := 0.5 + 0.5*t.opts.randFloat()
	return time.Duration(float64(delay) * factor)
}

func (t *Transport) allocEpoch() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextEpoch++
	return t.nextEpoch
}

func (t *Transport) currentLifetime() *lifetime {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.current
}

// installCurrent makes lt the current lifetime and wakes readiness waiters, but
// only when the lifetime is usable. It reports whether the lifetime was
// installed. A lifetime that lost its connection or failed between its positive
// SUBACK and install is never published as ready.
func (t *Transport) installCurrent(lt *lifetime) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !lt.usable() {
		return false
	}
	t.current = lt
	t.notifyStateLocked()
	return true
}

func (t *Transport) clearCurrent(lt *lifetime) {
	t.mu.Lock()
	if t.current == lt {
		t.current = nil
		t.notifyStateLocked()
	}
	t.mu.Unlock()
}

func (t *Transport) takeCurrent() *lifetime {
	t.mu.Lock()
	defer t.mu.Unlock()
	lt := t.current
	if lt != nil {
		t.current = nil
		t.notifyStateLocked()
	}
	return lt
}

// notifyStateLocked closes and replaces the readiness-broadcast channel so
// WaitReady and ReadyCh observe every readiness transition.
func (t *Transport) notifyStateLocked() {
	close(t.stateCh)
	t.stateCh = make(chan struct{})
}

func (t *Transport) reconnectStoppedLocked() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.reconnectStopped
}

// positiveOr returns value when strictly positive, otherwise the fallback.
func positiveOr(value, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

// transportCounters holds bounded-cardinality transport counters.
type transportCounters struct {
	connected          atomic.Int64
	connectFailures    atomic.Int64
	subscribeFailed    atomic.Int64
	disconnected       atomic.Int64
	connectionLost     atomic.Int64
	staleDropped       atomic.Int64
	published          atomic.Int64
	publishFailures    atomic.Int64
	publishUnavailable atomic.Int64
	// teardownStuck counts fail-closed teardowns where a tracked publish
	// invocation did not return within the teardown budget.
	teardownStuck atomic.Int64
}

// TransportSnapshot is a point-in-time, payload-free view of transport state.
type TransportSnapshot struct {
	Ready              bool
	Epoch              uint64
	Connected          int64
	ConnectFailures    int64
	SubscribeFailed    int64
	Disconnected       int64
	ConnectionLost     int64
	StaleDropped       int64
	Published          int64
	PublishFailures    int64
	PublishUnavailable int64
	// Stuck reports a fail-closed teardown: a tracked publish invocation stayed
	// wedged after the socket close. The transport stops reconnecting and stays
	// not ready until a supervised restart.
	Stuck         bool
	TeardownStuck int64
}

// Snapshot returns the current transport counters and readiness.
func (t *Transport) Snapshot() TransportSnapshot {
	t.mu.Lock()
	lt := t.current
	ready := t.readyLocked()
	t.mu.Unlock()

	snap := TransportSnapshot{
		Ready:              ready,
		Connected:          t.counters.connected.Load(),
		ConnectFailures:    t.counters.connectFailures.Load(),
		SubscribeFailed:    t.counters.subscribeFailed.Load(),
		Disconnected:       t.counters.disconnected.Load(),
		ConnectionLost:     t.counters.connectionLost.Load(),
		StaleDropped:       t.counters.staleDropped.Load(),
		Published:          t.counters.published.Load(),
		PublishFailures:    t.counters.publishFailures.Load(),
		PublishUnavailable: t.counters.publishUnavailable.Load(),
		Stuck:              t.stuck.Load(),
		TeardownStuck:      t.counters.teardownStuck.Load(),
	}
	if lt != nil {
		snap.Epoch = lt.epoch
	}
	return snap
}

// *ACKProcessor is the production ACK admitter for the transport.
var _ ACKAdmitter = (*ACKProcessor)(nil)

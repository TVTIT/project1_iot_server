package mqttingress

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"iot-platform/internal/telemetry"
)

// --- fake connections ------------------------------------------------------

type fakeConn struct {
	once   sync.Once
	closed chan struct{}
}

func newFakeConn() *fakeConn { return &fakeConn{closed: make(chan struct{})} }

func (c *fakeConn) Read([]byte) (int, error) { <-c.closed; return 0, io.EOF }
func (c *fakeConn) Write(b []byte) (int, error) {
	return len(b), nil
}
func (c *fakeConn) Close() error                     { c.once.Do(func() { close(c.closed) }); return nil }
func (c *fakeConn) LocalAddr() net.Addr              { return fakeAddr{} }
func (c *fakeConn) RemoteAddr() net.Addr             { return fakeAddr{} }
func (c *fakeConn) SetDeadline(time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(time.Time) error { return nil }
func (c *fakeConn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

type fakeAddr struct{}

func (fakeAddr) Network() string { return "fake" }
func (fakeAddr) String() string  { return "fake" }

// --- fake tokens -----------------------------------------------------------

type fakeToken struct {
	mu   sync.Mutex
	done chan struct{}
	err  error
}

func newFakeToken() *fakeToken { return &fakeToken{done: make(chan struct{})} }

func (t *fakeToken) Done() <-chan struct{} { return t.done }

func (t *fakeToken) Error() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

func (t *fakeToken) complete(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	select {
	case <-t.done:
		return
	default:
	}
	t.err = err
	close(t.done)
}

type fakeSubscribeToken struct {
	*fakeToken
	result map[string]byte
}

func (t *fakeSubscribeToken) Result() map[string]byte { return t.result }

func completedToken(err error) *fakeToken {
	tok := newFakeToken()
	tok.complete(err)
	return tok
}

func completedSubscribe(result map[string]byte, err error) *fakeSubscribeToken {
	return &fakeSubscribeToken{fakeToken: completedToken(err), result: result}
}

// --- fake client -----------------------------------------------------------

type fakeClient struct {
	id     int
	opts   *mqtt.ClientOptions
	record func(string)

	mu              sync.Mutex
	conn            net.Conn
	handler         mqttMessageHandler
	connects        int
	subscribes      int
	publishes       int
	disconnects     int
	lastSubTopic    string
	lastSubQoS      byte
	lastPubTopic    string
	lastPubQoS      byte
	lastPubRetained bool
	lastPubPayload  []byte

	openOnConnect bool
	connectErr    error

	connectFn    func(*fakeClient) waitToken
	subscribeFn  func(*fakeClient, string, byte) subscribeToken
	publishFn    func(*fakeClient, string, byte, bool, []byte) waitToken
	disconnectFn func(*fakeClient, uint)
}

func (c *fakeClient) Connect() waitToken {
	c.mu.Lock()
	c.connects++
	open := c.openOnConnect
	fn := c.connectFn
	c.mu.Unlock()
	if c.record != nil {
		c.record(fmt.Sprintf("connect:%d", c.id))
	}

	if open && c.opts != nil && c.opts.CustomOpenConnectionFn != nil {
		host := ""
		if len(c.opts.Servers) > 0 {
			host = c.opts.Servers[0].Host
		}
		uri, _ := url.Parse("ssl://" + host)
		conn, err := c.opts.CustomOpenConnectionFn(uri, *c.opts)
		if err != nil {
			return completedToken(err)
		}
		c.mu.Lock()
		c.conn = conn
		c.mu.Unlock()
	}

	if fn != nil {
		return fn(c)
	}
	c.mu.Lock()
	err := c.connectErr
	c.mu.Unlock()
	return completedToken(err)
}

func (c *fakeClient) Disconnect(quiesce uint) {
	c.mu.Lock()
	c.disconnects++
	fn := c.disconnectFn
	c.mu.Unlock()
	if c.record != nil {
		c.record(fmt.Sprintf("disconnect:%d", c.id))
	}
	if fn != nil {
		fn(c, quiesce)
	}
}

func (c *fakeClient) Subscribe(topic string, qos byte, handler mqttMessageHandler) subscribeToken {
	c.mu.Lock()
	c.subscribes++
	c.lastSubTopic = topic
	c.lastSubQoS = qos
	c.handler = handler
	fn := c.subscribeFn
	c.mu.Unlock()
	if fn != nil {
		return fn(c, topic, qos)
	}
	return completedSubscribe(map[string]byte{topic: qos}, nil)
}

func (c *fakeClient) Publish(topic string, qos byte, retained bool, payload []byte) waitToken {
	c.mu.Lock()
	c.publishes++
	c.lastPubTopic = topic
	c.lastPubQoS = qos
	c.lastPubRetained = retained
	c.lastPubPayload = append([]byte(nil), payload...)
	fn := c.publishFn
	c.mu.Unlock()
	if fn != nil {
		return fn(c, topic, qos, retained, payload)
	}
	return completedToken(nil)
}

func (c *fakeClient) invokeMessage(topic string, payload []byte, retained bool) bool {
	c.mu.Lock()
	handler := c.handler
	c.mu.Unlock()
	if handler == nil {
		return false
	}
	handler(topic, payload, retained)
	return true
}

func (c *fakeClient) lose(err error) {
	c.mu.Lock()
	onLost := c.opts.OnConnectionLost
	c.mu.Unlock()
	if onLost != nil {
		onLost(nil, err)
	}
}

func (c *fakeClient) counts() (connects, subscribes, publishes, disconnects int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connects, c.subscribes, c.publishes, c.disconnects
}

// --- harness ---------------------------------------------------------------

type harness struct {
	mu       sync.Mutex
	clients  []*fakeClient
	conns    []*fakeConn
	events   []string
	dialErr  error
	clientFn func(*mqtt.ClientOptions) *fakeClient
	created  chan *fakeClient
}

func newHarness() *harness {
	return &harness{created: make(chan *fakeClient, 64)}
}

func (h *harness) record(event string) {
	h.mu.Lock()
	h.events = append(h.events, event)
	h.mu.Unlock()
}

func (h *harness) eventIndex(event string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	for idx, e := range h.events {
		if e == event {
			return idx
		}
	}
	return -1
}

func (h *harness) factory(opts *mqtt.ClientOptions) mqttClient {
	h.mu.Lock()
	fn := h.clientFn
	h.mu.Unlock()
	client := fn(opts)
	client.opts = opts
	if client.record == nil {
		client.record = h.record
	}
	h.mu.Lock()
	client.id = len(h.clients)
	h.clients = append(h.clients, client)
	h.mu.Unlock()
	h.record(fmt.Sprintf("new:%d", client.id))
	h.created <- client
	return client
}

func (h *harness) dial(context.Context, string) (net.Conn, error) {
	h.mu.Lock()
	err := h.dialErr
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}
	conn := newFakeConn()
	h.mu.Lock()
	h.conns = append(h.conns, conn)
	h.mu.Unlock()
	return conn, nil
}

func (h *harness) client(i int) *fakeClient {
	h.mu.Lock()
	defer h.mu.Unlock()
	if i >= len(h.clients) {
		return nil
	}
	return h.clients[i]
}

func (h *harness) clientCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

func (h *harness) connFor(c *fakeClient) *fakeConn {
	h.mu.Lock()
	defer h.mu.Unlock()
	for idx, fc := range h.clients {
		if fc == c && idx < len(h.conns) {
			return h.conns[idx]
		}
	}
	return nil
}

// --- test transport helpers ------------------------------------------------

func testTransportOptions() TransportOptions {
	return TransportOptions{
		BrokerURL:            "ssl://broker.example:8883",
		ClientID:             "telemetry_ingress",
		Username:             "backend_service",
		Password:             "test-only-password",
		CAFile:               "/unused/ca.crt",
		roots:                x509.NewCertPool(),
		ConnectTimeout:       2 * time.Second,
		SubscribeTimeout:     2 * time.Second,
		PublishTimeout:       2 * time.Second,
		ReconnectMaxInterval: 20 * time.Millisecond,
		CloseWaitTimeout:     200 * time.Millisecond,
		baseBackoff:          time.Millisecond,
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func newTestTransport(t *testing.T, admit Admitter, h *harness, mutate func(*TransportOptions)) *Transport {
	t.Helper()
	opts := testTransportOptions()
	if mutate != nil {
		mutate(&opts)
	}
	if opts.newClient == nil {
		opts.newClient = h.factory
	}
	if opts.dial == nil {
		opts.dial = h.dial
	}
	tr, err := NewTransport(opts, admit)
	if err != nil {
		t.Fatalf("NewTransport: %v", err)
	}
	return tr
}

func waitReadyWithin(t *testing.T, tr *Transport, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

type admitCall struct {
	topic    string
	payload  []byte
	at       time.Time
	retained bool
}

type recordAdmitter struct {
	mu  sync.Mutex
	got []admitCall
}

func (a *recordAdmitter) Admit(topic string, payload []byte, at time.Time, retained bool) AdmissionDecision {
	a.mu.Lock()
	a.got = append(a.got, admitCall{topic: topic, payload: append([]byte(nil), payload...), at: at, retained: retained})
	a.mu.Unlock()
	return AdmissionAccepted
}

func (a *recordAdmitter) calls() []admitCall {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]admitCall(nil), a.got...)
}

// --- CA loading tests ------------------------------------------------------

func writeTestCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(cryptorand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.crt")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if err := pem.Encode(file, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadRootPoolAcceptsValidCA(t *testing.T) {
	path := writeTestCA(t)
	pool, err := loadRootPool(path)
	if err != nil {
		t.Fatalf("loadRootPool(valid) = %v", err)
	}
	if pool == nil {
		t.Fatal("loadRootPool returned a nil pool")
	}
}

func TestLoadRootPoolRejectsInvalidCA(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty.crt")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	garbage := filepath.Join(dir, "garbage.crt")
	if err := os.WriteFile(garbage, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{
		"missing":   dir + "/absent.crt",
		"empty":     empty,
		"garbage":   garbage,
		"directory": dir,
	} {
		if _, err := loadRootPool(path); err == nil {
			t.Fatalf("%s: loadRootPool = nil, want error", name)
		}
	}
}

func TestNewTransportRejectsInvalidCA(t *testing.T) {
	opts := testTransportOptions()
	opts.roots = nil
	opts.CAFile = filepath.Join(t.TempDir(), "absent.crt")
	if _, err := NewTransport(opts, &recordAdmitter{}); err == nil {
		t.Fatal("NewTransport accepted an unreadable CA file")
	}
}

func TestNewTransportValidatesOptions(t *testing.T) {
	valid := testTransportOptions()
	base := func() TransportOptions { return valid }
	cases := map[string]func(*TransportOptions){
		"insecure broker scheme": func(o *TransportOptions) { o.BrokerURL = "tcp://broker:1883" },
		"missing client id":      func(o *TransportOptions) { o.ClientID = "" },
		"missing username":       func(o *TransportOptions) { o.Username = "" },
		"missing password":       func(o *TransportOptions) { o.Password = "" },
	}
	for name, mutate := range cases {
		opts := base()
		mutate(&opts)
		if _, err := NewTransport(opts, &recordAdmitter{}); err == nil {
			t.Fatalf("%s: NewTransport = nil error", name)
		}
	}
	if _, err := NewTransport(valid, nil); err == nil {
		t.Fatal("NewTransport accepted a nil admitter")
	}
}

// --- connectOnce tests -----------------------------------------------------

func TestConnectOncePositiveSubscribe(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{openOnConnect: true} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)

	lt, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatalf("connectOnce = %v", err)
	}
	if !lt.usable() {
		t.Fatal("lifetime is not usable after a positive SUBACK")
	}
	client := h.client(0)
	_, subscribes, _, _ := client.counts()
	if subscribes != 1 {
		t.Fatalf("subscribe calls = %d, want 1", subscribes)
	}
	if client.lastSubTopic != DefaultTelemetryFilter || client.lastSubQoS != 1 {
		t.Fatalf("subscribe = (%q, %d), want (%q, 1)", client.lastSubTopic, client.lastSubQoS, DefaultTelemetryFilter)
	}
	conn := h.connFor(client)
	if conn == nil || conn.isClosed() {
		t.Fatal("dialed socket was not tracked open")
	}
}

func TestConnectOnceSubscribeDenied(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{
			openOnConnect: true,
			subscribeFn: func(_ *fakeClient, topic string, _ byte) subscribeToken {
				return completedSubscribe(map[string]byte{topic: 0x80}, nil)
			},
		}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)

	if _, err := tr.connectOnce(context.Background()); err == nil {
		t.Fatal("connectOnce accepted a denied SUBACK")
	}
	if got := tr.counters.subscribeFailed.Load(); got != 1 {
		t.Fatalf("subscribeFailed = %d, want 1", got)
	}
	if tr.Ready() {
		t.Fatal("denied subscribe left the transport ready")
	}
	if conn := h.connFor(h.client(0)); conn == nil || !conn.isClosed() {
		t.Fatal("denied subscribe did not close the socket")
	}
}

func TestConnectOnceRejectsUnfinishedSubscribe(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{
			subscribeFn: func(_ *fakeClient, _ string, _ byte) subscribeToken {
				return &fakeSubscribeToken{fakeToken: newFakeToken()}
			},
		}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.SubscribeTimeout = 30 * time.Millisecond
	})

	if _, err := tr.connectOnce(context.Background()); err == nil {
		t.Fatal("connectOnce accepted a subscribe token that never completed")
	}
}

func TestConnectOnceSubscribeMissingGrant(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{subscribeFn: func(*fakeClient, string, byte) subscribeToken {
			return completedSubscribe(map[string]byte{}, nil)
		}}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	if _, err := tr.connectOnce(context.Background()); err == nil {
		t.Fatal("connectOnce accepted a SUBACK without the requested filter")
	}
}

func TestConnectOnceConnectError(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{connectErr: errors.New("connack refused")} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	if _, err := tr.connectOnce(context.Background()); err == nil {
		t.Fatal("connectOnce ignored a CONNECT error")
	}
}

// --- callback / readiness tests -------------------------------------------

func TestMessageCallbackAdmits(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	admit := &recordAdmitter{}
	tr := newTestTransport(t, admit, h, nil)

	lt, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatalf("connectOnce = %v", err)
	}
	tr.installCurrent(lt)
	if !tr.Ready() {
		t.Fatal("transport not ready after install")
	}

	if !h.client(0).invokeMessage("gateways/g1/telemetry/s1", []byte("payload"), false) {
		t.Fatal("message handler was not captured")
	}
	calls := admit.calls()
	if len(calls) != 1 || calls[0].topic != "gateways/g1/telemetry/s1" || string(calls[0].payload) != "payload" {
		t.Fatalf("admit calls = %+v", calls)
	}
	// Retained is passed through to the core, which owns the rejection policy.
	h.client(0).invokeMessage("gateways/g1/telemetry/s1", []byte("x"), true)
	if calls := admit.calls(); len(calls) != 2 || !calls[1].retained {
		t.Fatalf("retained delivery was not passed through: %+v", calls)
	}
}

func TestLostClearsReadyImmediately(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)

	lt, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatalf("connectOnce = %v", err)
	}
	tr.installCurrent(lt)
	tr.onLost(lt)
	if tr.Ready() {
		t.Fatal("connection loss left the transport ready")
	}
	if got := tr.counters.connectionLost.Load(); got != 1 {
		t.Fatalf("connectionLost = %d, want 1", got)
	}
}

func TestStaleCallbacksCannotInvalidateReplacement(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	admit := &recordAdmitter{}
	tr := newTestTransport(t, admit, h, nil)

	old, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatalf("connectOnce old = %v", err)
	}
	replacement, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatalf("connectOnce replacement = %v", err)
	}
	tr.installCurrent(replacement)

	// A delayed loss and message from the old lifetime must be ignored.
	tr.onLost(old)
	tr.onMessage(old, "gateways/g1/telemetry/s1", []byte("stale"), false)

	if !tr.Ready() {
		t.Fatal("stale loss invalidated the replacement lifetime")
	}
	if calls := admit.calls(); len(calls) != 0 {
		t.Fatalf("stale callback admitted %d deliveries, want 0", len(calls))
	}
	if tr.Snapshot().Epoch != replacement.epoch {
		t.Fatalf("current epoch = %d, want %d", tr.Snapshot().Epoch, replacement.epoch)
	}
}

func TestWaitReadyTimesOutWithoutConnection(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := tr.WaitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitReady = %v, want DeadlineExceeded", err)
	}
}

// --- publish tests ---------------------------------------------------------

func TestPublishSuccess(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	lt, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatalf("connectOnce = %v", err)
	}
	tr.installCurrent(lt)

	delivery := Delivery{Topic: "gateways/g1/acks/m1", Payload: []byte("ack"), QoS: 1, Retained: false}
	if err := tr.Publish(context.Background(), delivery); err != nil {
		t.Fatalf("Publish = %v", err)
	}
	client := h.client(0)
	_, _, publishes, _ := client.counts()
	if publishes != 1 || client.lastPubTopic != delivery.Topic || client.lastPubQoS != 1 || client.lastPubRetained {
		t.Fatalf("publish = calls %d topic %q qos %d retained %v", publishes, client.lastPubTopic, client.lastPubQoS, client.lastPubRetained)
	}
	if got := tr.counters.published.Load(); got != 1 {
		t.Fatalf("published counter = %d, want 1", got)
	}
}

func TestPublishTokenErrorIsUnconfirmed(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{publishFn: func(*fakeClient, string, byte, bool, []byte) waitToken {
			return completedToken(errors.New("write failed"))
		}}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	lt, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatalf("connectOnce = %v", err)
	}
	tr.installCurrent(lt)

	err = tr.Publish(context.Background(), Delivery{Topic: "t", Payload: []byte("p"), QoS: 1})
	assertAckUnconfirmed(t, err)
	if tr.Ready() {
		t.Fatal("failed publish left the transport ready")
	}
	if got := tr.counters.publishFailures.Load(); got != 1 {
		t.Fatalf("publishFailures = %d, want 1", got)
	}
}

func TestPublishTimeoutMarksFailedAndBlocksFurther(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{publishFn: func(*fakeClient, string, byte, bool, []byte) waitToken {
			return newFakeToken() // never completes
		}}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.PublishTimeout = 40 * time.Millisecond
	})
	lt, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatalf("connectOnce = %v", err)
	}
	tr.installCurrent(lt)

	start := time.Now()
	err = tr.Publish(context.Background(), Delivery{Topic: "t", Payload: []byte("p"), QoS: 1})
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Publish took %s, want bounded by the publish timeout", elapsed)
	}
	assertAckUnconfirmed(t, err)

	// A second publish on the failed lifetime is rejected without calling Paho.
	_, _, publishesBefore, _ := h.client(0).counts()
	err = tr.Publish(context.Background(), Delivery{Topic: "t", Payload: []byte("p"), QoS: 1})
	assertAckUnconfirmed(t, err)
	_, _, publishesAfter, _ := h.client(0).counts()
	if publishesAfter != publishesBefore {
		t.Fatalf("publish attempted on a failed lifetime: %d -> %d", publishesBefore, publishesAfter)
	}
	if got := tr.counters.publishUnavailable.Load(); got != 1 {
		t.Fatalf("publishUnavailable = %d, want 1", got)
	}
}

func TestPublishOnFailedLifetimeUnconfirmed(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	lt, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatalf("connectOnce = %v", err)
	}
	lt.markFailed()
	tr.installCurrent(lt)

	err = tr.Publish(context.Background(), Delivery{Topic: "t", Payload: []byte("p"), QoS: 1})
	assertAckUnconfirmed(t, err)
	_, _, publishes, _ := h.client(0).counts()
	if publishes != 0 {
		t.Fatalf("Publish called Paho %d times on a failed lifetime", publishes)
	}
}

func assertAckUnconfirmed(t *testing.T, err error) {
	t.Helper()
	var reason *telemetry.ReasonError
	if !errors.As(err, &reason) || reason.Code != telemetry.ReasonAckPublishUnconfirmed {
		t.Fatalf("error = %v, want ack_publish_unconfirmed", err)
	}
}

// blockUntilSocketClosed simulates Paho's synchronous Publish blocking inside a
// socket write: the fake Publish CALL itself does not return until the transport
// closes the tracked socket. This exercises the whole-call bound, not only the
// token wait.
func blockUntilSocketClosed(c *fakeClient, _ string, _ byte, _ bool, _ []byte) waitToken {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if fc, ok := conn.(*fakeConn); ok {
		<-fc.closed
	}
	return completedToken(nil)
}

func primeConnectedLifetime(t *testing.T, tr *Transport) *lifetime {
	t.Helper()
	lt, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatalf("connectOnce = %v", err)
	}
	tr.installCurrent(lt)
	return lt
}

func TestPublishRejectsCanceledContextBeforeSideEffects(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	primeConnectedLifetime(t, tr)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := tr.Publish(ctx, Delivery{Topic: "t", Payload: []byte("p"), QoS: 1})
	assertAckUnconfirmed(t, err)
	if _, _, publishes, _ := h.client(0).counts(); publishes != 0 {
		t.Fatalf("Publish called Paho %d times with a canceled context", publishes)
	}
	if _, _, _, disconnects := h.client(0).counts(); disconnects != 0 {
		t.Fatalf("canceled Publish disconnected the client")
	}
	if !tr.Ready() {
		t.Fatal("a canceled publish must not fail the current lifetime")
	}
}

func TestPublishBoundsBlockingPahoCallAndRejectsNewAdmits(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{openOnConnect: true, publishFn: blockUntilSocketClosed}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.PublishTimeout = 40 * time.Millisecond
		o.publishStopGrace = 100 * time.Millisecond
	})
	lt := primeConnectedLifetime(t, tr)

	start := time.Now()
	err := tr.Publish(context.Background(), Delivery{Topic: "t", Payload: []byte("p"), QoS: 1})
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Fatalf("Publish took %s, want bounded by the publish timeout plus grace", elapsed)
	}
	assertAckUnconfirmed(t, err)

	// The blocking Paho CALL was unblocked by closing the tracked socket.
	conn := h.connFor(h.client(0))
	if conn == nil || !conn.isClosed() {
		t.Fatal("transport did not close the socket to unblock the blocking Paho call")
	}
	if tr.Ready() {
		t.Fatal("failed publish left the transport ready")
	}
	if lt.usable() {
		t.Fatal("failed publish left the lifetime usable")
	}

	// Finite pending: exactly one invocation reached Paho, and a later publish on
	// the failed lifetime is rejected without a new Paho call.
	_, _, publishesBefore, _ := h.client(0).counts()
	if publishesBefore != 1 {
		t.Fatalf("Paho publishes = %d, want 1", publishesBefore)
	}
	err = tr.Publish(context.Background(), Delivery{Topic: "t", Payload: []byte("p"), QoS: 1})
	assertAckUnconfirmed(t, err)
	if _, _, publishesAfter, _ := h.client(0).counts(); publishesAfter != publishesBefore {
		t.Fatalf("publish attempted on a failed lifetime: %d -> %d", publishesBefore, publishesAfter)
	}
	if got := tr.counters.publishUnavailable.Load(); got != 1 {
		t.Fatalf("publishUnavailable = %d, want 1", got)
	}
}

func TestPublishCancellationUnblocksPendingCall(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{openOnConnect: true, publishFn: blockUntilSocketClosed}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.PublishTimeout = 10 * time.Second // far longer than the caller context
		o.publishStopGrace = 100 * time.Millisecond
	})
	primeConnectedLifetime(t, tr)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := tr.Publish(ctx, Delivery{Topic: "t", Payload: []byte("p"), QoS: 1})
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Fatalf("Publish took %s, want bounded by the caller context", elapsed)
	}
	assertAckUnconfirmed(t, err)
	if conn := h.connFor(h.client(0)); conn == nil || !conn.isClosed() {
		t.Fatal("canceled publish did not close the socket to unblock Paho")
	}
}

func TestPublishConcurrencyIsBounded(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{openOnConnect: true, publishFn: blockUntilSocketClosed}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.PublishTimeout = 40 * time.Millisecond
		o.publishStopGrace = 100 * time.Millisecond
		o.MaxConcurrentPublishes = 1
	})
	primeConnectedLifetime(t, tr)

	const callers = 4
	var wg sync.WaitGroup
	results := make([]error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = tr.Publish(context.Background(), Delivery{Topic: "t", Payload: []byte("p"), QoS: 1})
		}(i)
	}
	wg.Wait()

	for i, err := range results {
		if err == nil {
			t.Fatalf("caller %d: Publish = nil, want unconfirmed", i)
		}
		assertAckUnconfirmed(t, err)
	}
	// Only the single in-flight invocation may reach Paho; the rest fail closed
	// because the lifetime was cleared after the timeout.
	if _, _, publishes, _ := h.client(0).counts(); publishes != 1 {
		t.Fatalf("Paho publishes = %d, want 1 (bounded concurrency)", publishes)
	}
}

func TestCloseLifetimeWaitsForInFlightPublish(t *testing.T) {
	h := newHarness()
	release := make(chan struct{})
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{publishFn: func(*fakeClient, string, byte, bool, []byte) waitToken {
			<-release
			return completedToken(nil)
		}}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.PublishTimeout = 10 * time.Second
		o.CloseWaitTimeout = 500 * time.Millisecond
	})
	lt := primeConnectedLifetime(t, tr)

	published := make(chan error, 1)
	go func() {
		published <- tr.Publish(context.Background(), Delivery{Topic: "t", Payload: []byte("p"), QoS: 1})
	}()

	// Wait until the fake Publish call is actually blocked.
	deadline := time.After(time.Second)
	for {
		if _, _, publishes, _ := h.client(0).counts(); publishes == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("fake Publish was never called")
		case <-time.After(time.Millisecond):
		}
	}

	teardownDone := make(chan struct{})
	go func() {
		tr.closeLifetime(lt, 500*time.Millisecond)
		close(teardownDone)
	}()

	// Teardown must not return while a publish invocation is still running: the
	// wake signal is only a wake, not proof of cleanup.
	select {
	case <-teardownDone:
		t.Fatal("closeLifetime returned before the in-flight publish completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	if err := <-published; err != nil {
		t.Fatalf("Publish = %v, want nil", err)
	}
	select {
	case <-teardownDone:
	case <-time.After(time.Second):
		t.Fatal("closeLifetime did not return after the publish completed")
	}
}

func TestSupervisorReconnectsAfterPublishTimeout(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{openOnConnect: true, publishFn: blockUntilSocketClosed}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.PublishTimeout = 40 * time.Millisecond
		o.publishStopGrace = 100 * time.Millisecond
	})
	if err := tr.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	defer func() { _ = tr.Close(context.Background()) }()

	first := <-h.created
	waitReadyWithin(t, tr, time.Second)

	assertAckUnconfirmed(t, tr.Publish(context.Background(), Delivery{Topic: "t", Payload: []byte("p"), QoS: 1}))

	second := <-h.created
	if second == first {
		t.Fatal("replacement reused the failed lifetime")
	}
	waitReadyWithin(t, tr, time.Second)
	if conn := h.connFor(first); conn == nil || !conn.isClosed() {
		t.Fatal("failed lifetime socket was not closed before replacement")
	}
	if snap := tr.Snapshot(); snap.Epoch != 2 || snap.Disconnected < 1 {
		t.Fatalf("snapshot after publish-timeout reconnect = %+v", snap)
	}
}

func TestConnectOnceRejectsInvalidGrantedQoS(t *testing.T) {
	cases := map[string]struct {
		granted byte
		ok      bool
	}{
		"granted qos 0":       {granted: 0x00, ok: true},
		"granted qos 1":       {granted: 0x01, ok: true},
		"denied 0x80":         {granted: 0x80, ok: false},
		"higher than request": {granted: 0x02, ok: false},
		"invalid code 0x03":   {granted: 0x03, ok: false},
		"invalid code 0xff":   {granted: 0xFF, ok: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness()
			granted := tc.granted
			h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
				return &fakeClient{subscribeFn: func(_ *fakeClient, topic string, _ byte) subscribeToken {
					return completedSubscribe(map[string]byte{topic: granted}, nil)
				}}
			}
			tr := newTestTransport(t, &recordAdmitter{}, h, nil)
			lt, err := tr.connectOnce(context.Background())
			if tc.ok {
				if err != nil {
					t.Fatalf("connectOnce = %v, want success", err)
				}
				if !lt.usable() {
					t.Fatal("positive granted QoS did not produce a usable lifetime")
				}
				return
			}
			if err == nil {
				t.Fatalf("connectOnce accepted granted code 0x%02x", granted)
			}
			if tr.Ready() {
				t.Fatal("invalid SUBACK left the transport ready")
			}
		})
	}
}

func TestConnectOnceFailsWhenLostBeforeInstall(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{subscribeFn: func(c *fakeClient, topic string, qos byte) subscribeToken {
			// A connection loss arrives after the SUBACK but before install.
			c.lose(errors.New("connection dropped during subscribe"))
			return completedSubscribe(map[string]byte{topic: qos}, nil)
		}}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)

	lt, err := tr.connectOnce(context.Background())
	if err == nil {
		t.Fatal("connectOnce returned an already-failed lifetime")
	}
	if lt != nil {
		t.Fatal("connectOnce returned a non-nil lifetime on loss")
	}
	if tr.Ready() {
		t.Fatal("lost-before-install exposed a ready transport")
	}
	if got := tr.counters.connectionLost.Load(); got != 1 {
		t.Fatalf("connectionLost = %d, want 1", got)
	}
}

func TestNewTransportRejectsDisallowedBrokerURLOptions(t *testing.T) {
	bad := []string{
		"ssl://user:pass@broker.example:8883",
		"ssl://broker.example:8883/",
		"ssl://broker.example:8883/mqtt",
		"ssl://broker.example:8883?a=1",
		"ssl://broker.example:8883#frag",
		"ssl://broker.example",
		"tcp://broker.example:1883",
	}
	for _, raw := range bad {
		opts := testTransportOptions()
		opts.BrokerURL = raw
		if _, err := NewTransport(opts, &recordAdmitter{}); err == nil {
			t.Fatalf("NewTransport accepted disallowed broker URL %q", raw)
		}
	}
}

// --- supervisor lifecycle tests -------------------------------------------

func TestSupervisorReconnectReplacesLifetime(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{openOnConnect: true} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	if err := tr.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	defer func() { _ = tr.Close(context.Background()) }()

	first := <-h.created
	waitReadyWithin(t, tr, time.Second)
	if snap := tr.Snapshot(); snap.Epoch != 1 || !snap.Ready {
		t.Fatalf("initial snapshot = %+v, want epoch 1 ready", snap)
	}

	first.lose(errors.New("broker closed the connection"))

	second := <-h.created
	if second == first {
		t.Fatal("replacement reused the old lifetime")
	}
	waitReadyWithin(t, tr, time.Second)

	// The old socket must be closed before the replacement is used.
	if conn := h.connFor(first); conn == nil || !conn.isClosed() {
		t.Fatal("old socket was not closed before replacement")
	}
	if idx := h.eventIndex("disconnect:0"); idx == -1 {
		t.Fatal("old client was not disconnected")
	} else if newIdx := h.eventIndex("new:1"); newIdx == -1 || idx > newIdx {
		t.Fatalf("disconnect of old lifetime (event %d) did not precede replacement (event %d)", idx, newIdx)
	}

	snap := tr.Snapshot()
	if snap.Epoch != 2 || snap.Connected < 2 || snap.Disconnected < 1 || snap.ConnectionLost < 1 {
		t.Fatalf("snapshot after reconnect = %+v", snap)
	}
}

func TestStopReconnectKeepsCurrentForDrain(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{openOnConnect: true} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	if err := tr.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	client := <-h.created
	waitReadyWithin(t, tr, time.Second)

	tr.StopReconnect()
	if !tr.Ready() {
		t.Fatal("StopReconnect must keep the current connection ready for draining")
	}
	if err := tr.Publish(context.Background(), Delivery{Topic: "gateways/g1/acks/m1", Payload: []byte("ack"), QoS: 1}); err != nil {
		t.Fatalf("Publish during drain = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tr.Close(ctx); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if tr.Ready() {
		t.Fatal("transport still ready after Close")
	}
	if _, _, _, disconnects := client.counts(); disconnects != 1 {
		t.Fatalf("disconnect calls after Close = %d, want 1", disconnects)
	}
	if conn := h.connFor(client); conn == nil || !conn.isClosed() {
		t.Fatal("socket not closed by Close")
	}
}

func TestCloseCancelsReconnectBackoff(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{connectErr: errors.New("broker down")} }
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.ReconnectMaxInterval = 5 * time.Second
		o.baseBackoff = time.Second
	})
	if err := tr.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	<-h.created // first attempt has started

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if err := tr.Close(ctx); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Close took %s, want it to cancel the backoff promptly", elapsed)
	}
	if got := tr.counters.connectFailures.Load(); got < 1 {
		t.Fatalf("connectFailures = %d, want >= 1", got)
	}
}

func TestSnapshotReportsReadyAndEpoch(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	if snap := tr.Snapshot(); snap.Ready || snap.Epoch != 0 {
		t.Fatalf("initial snapshot = %+v, want not ready and epoch 0", snap)
	}
	lt, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatalf("connectOnce = %v", err)
	}
	tr.installCurrent(lt)
	snap := tr.Snapshot()
	if !snap.Ready || snap.Epoch != lt.epoch {
		t.Fatalf("snapshot = %+v, want ready epoch %d", snap, lt.epoch)
	}
}

// --- fail-closed teardown and semaphore lifetime ----------------------------

// wedgedPublish returns a Publish fake whose synchronous CALL blocks until the
// returned channel is closed, ignoring the tracked socket close. It models a
// Paho invocation that stays wedged past the socket close and the teardown
// budget.
func wedgedPublish(entered chan<- struct{}, release <-chan struct{}) func(*fakeClient, string, byte, bool, []byte) waitToken {
	return func(*fakeClient, string, byte, bool, []byte) waitToken {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return completedToken(nil)
	}
}

// TestPublishSlotHeldUntilInvocationReturns verifies the semaphore slot is
// returned only when the invocation goroutine itself finishes, not when the
// bounded Publish call gives up. A wedged invocation therefore keeps its slot
// and cannot be joined by a second stuck invocation.
func TestPublishSlotHeldUntilInvocationReturns(t *testing.T) {
	h := newHarness()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{openOnConnect: true, publishFn: wedgedPublish(entered, release)}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.PublishTimeout = 30 * time.Millisecond
		o.publishStopGrace = 30 * time.Millisecond
		o.MaxConcurrentPublishes = 1
	})
	primeConnectedLifetime(t, tr)

	assertAckUnconfirmed(t, tr.Publish(context.Background(), Delivery{Topic: "t", Payload: []byte("p"), QoS: 1}))
	<-entered

	// The bounded Publish call returned, but the invocation is still wedged, so
	// the only slot must still be held.
	if got := len(tr.publishSem); got != 1 {
		t.Fatalf("publish slots in use = %d, want 1 (held until the invocation returns)", got)
	}

	close(release)
	deadline := time.After(time.Second)
	for len(tr.publishSem) != 0 {
		select {
		case <-deadline:
			t.Fatalf("publish slot was not returned after the invocation finished")
		case <-time.After(time.Millisecond):
		}
	}
}

// TestSupervisorFailClosedWhenTeardownStuck verifies that a publish invocation
// that does not return after the socket close and the teardown budget stops the
// supervisor instead of replacing the lifetime in a loop, reports the stuck
// teardown, and stays not ready until a supervised restart.
func TestSupervisorFailClosedWhenTeardownStuck(t *testing.T) {
	h := newHarness()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{openOnConnect: true, publishFn: wedgedPublish(entered, release)}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.PublishTimeout = 30 * time.Millisecond
		o.publishStopGrace = 30 * time.Millisecond
		o.CloseWaitTimeout = 60 * time.Millisecond
	})
	if err := tr.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	waitReadyWithin(t, tr, time.Second)

	assertAckUnconfirmed(t, tr.Publish(context.Background(), Delivery{Topic: "t", Payload: []byte("p"), QoS: 1}))
	<-entered

	deadline := time.After(time.Second)
	for !tr.Snapshot().Stuck {
		select {
		case <-deadline:
			t.Fatalf("transport did not fail closed; snapshot = %+v", tr.Snapshot())
		case <-time.After(time.Millisecond):
		}
	}
	if tr.Ready() {
		t.Fatal("fail-closed transport still reports ready")
	}

	// The supervisor must not replace the wedged lifetime with a new one.
	time.Sleep(150 * time.Millisecond)
	if got := h.clientCount(); got != 1 {
		t.Fatalf("supervisor created %d clients after a stuck teardown, want 1", got)
	}
	if snap := tr.Snapshot(); !snap.Stuck || snap.TeardownStuck != 1 {
		t.Fatalf("snapshot = %+v, want stuck with one reported teardown", snap)
	}

	// Close must stay finite even though the invocation is still wedged and the
	// supervisor has already exited.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- tr.Close(ctx) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close hung on a closed, stuck supervisor")
	}
	close(release)
}

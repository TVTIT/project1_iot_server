package gatewaysim

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
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

const transportTestGatewayID = "gateway_001"

// --- fake connections ------------------------------------------------------

type fakeConn struct {
	once   sync.Once
	closed chan struct{}
}

func newFakeConn() *fakeConn { return &fakeConn{closed: make(chan struct{})} }

func (c *fakeConn) Read([]byte) (int, error)         { <-c.closed; return 0, io.EOF }
func (c *fakeConn) Write(b []byte) (int, error)      { return len(b), nil }
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

// --- fake admitter ---------------------------------------------------------

type admitCall struct {
	topic    string
	payload  []byte
	retained bool
}

type recordAdmitter struct {
	mu  sync.Mutex
	got []admitCall
}

func (a *recordAdmitter) Admit(topic string, payload []byte, retained bool) {
	a.mu.Lock()
	a.got = append(a.got, admitCall{topic: topic, payload: append([]byte(nil), payload...), retained: retained})
	a.mu.Unlock()
}

func (a *recordAdmitter) calls() []admitCall {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]admitCall(nil), a.got...)
}

// --- test transport helpers ------------------------------------------------

func transportTestOptions() TransportOptions {
	return TransportOptions{
		GatewayID:            transportTestGatewayID,
		BrokerURL:            "ssl://broker.example:8883",
		ClientID:             "gateway_sim_proc",
		Username:             transportTestGatewayID,
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

func newTestTransport(t *testing.T, admit ACKAdmitter, h *harness, mutate func(*TransportOptions)) *Transport {
	t.Helper()
	opts := transportTestOptions()
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

func primeConnectedLifetime(t *testing.T, tr *Transport) *lifetime {
	t.Helper()
	lt, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatalf("connectOnce: %v", err)
	}
	tr.installCurrent(lt)
	return lt
}

func assertUnconfirmed(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrPublishUnconfirmed) {
		t.Fatalf("error = %v, want ErrPublishUnconfirmed", err)
	}
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

func TestTransportLoadRootPool(t *testing.T) {
	if pool, err := loadRootPool(writeTestCA(t)); err != nil || pool == nil {
		t.Fatalf("loadRootPool(valid) = %v/%v", pool, err)
	}
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

func TestNewTransportValidatesOptions(t *testing.T) {
	valid := transportTestOptions()
	cases := map[string]func(*TransportOptions){
		"insecure broker scheme": func(o *TransportOptions) { o.BrokerURL = "tcp://broker:1883" },
		"broker userinfo":        func(o *TransportOptions) { o.BrokerURL = "ssl://u:p@broker:8883" },
		"broker path":            func(o *TransportOptions) { o.BrokerURL = "ssl://broker:8883/mqtt" },
		"broker query":           func(o *TransportOptions) { o.BrokerURL = "ssl://broker:8883?a=1" },
		"broker fragment":        func(o *TransportOptions) { o.BrokerURL = "ssl://broker:8883#f" },
		"broker no port":         func(o *TransportOptions) { o.BrokerURL = "ssl://broker" },
		"missing gateway":        func(o *TransportOptions) { o.GatewayID = "" },
		"username mismatch":      func(o *TransportOptions) { o.Username = "other" },
		"missing client id":      func(o *TransportOptions) { o.ClientID = "" },
		"missing password":       func(o *TransportOptions) { o.Password = "" },
	}
	for name, mutate := range cases {
		opts := valid
		mutate(&opts)
		if _, err := NewTransport(opts, &recordAdmitter{}); err == nil {
			t.Fatalf("%s: NewTransport = nil error", name)
		}
	}
	if _, err := NewTransport(valid, nil); err == nil {
		t.Fatal("NewTransport accepted a nil admitter")
	}

	noCA := transportTestOptions()
	noCA.roots = nil
	noCA.CAFile = filepath.Join(t.TempDir(), "absent.crt")
	if _, err := NewTransport(noCA, &recordAdmitter{}); err == nil {
		t.Fatal("NewTransport accepted an unreadable CA file")
	}
}

// --- connectOnce tests -----------------------------------------------------

func TestTransportConnectOnceOwnACKFilter(t *testing.T) {
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
	want := "gateways/" + transportTestGatewayID + "/acks/#"
	if client.lastSubTopic != want || client.lastSubQoS != 1 {
		t.Fatalf("subscribe = (%q, %d), want (%q, 1)", client.lastSubTopic, client.lastSubQoS, want)
	}
	if conn := h.connFor(client); conn == nil || conn.isClosed() {
		t.Fatal("dialed socket was not tracked open")
	}
}

func TestTransportConnectOnceSUBACK(t *testing.T) {
	cases := map[string]struct {
		granted  byte
		hasGrant bool
		ok       bool
	}{
		"granted qos 0":       {granted: 0x00, hasGrant: true, ok: true},
		"granted qos 1":       {granted: 0x01, hasGrant: true, ok: true},
		"denied 0x80":         {granted: 0x80, hasGrant: true, ok: false},
		"higher than request": {granted: 0x02, hasGrant: true, ok: false},
		"invalid code 0x03":   {granted: 0x03, hasGrant: true, ok: false},
		"invalid code 0xff":   {granted: 0xFF, hasGrant: true, ok: false},
		"missing grant":       {hasGrant: false, ok: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness()
			tc := tc
			h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
				return &fakeClient{subscribeFn: func(_ *fakeClient, topic string, _ byte) subscribeToken {
					result := map[string]byte{}
					if tc.hasGrant {
						result[topic] = tc.granted
					}
					return completedSubscribe(result, nil)
				}}
			}
			tr := newTestTransport(t, &recordAdmitter{}, h, nil)
			lt, err := tr.connectOnce(context.Background())
			if tc.ok {
				if err != nil || !lt.usable() {
					t.Fatalf("connectOnce = %v/%v, want usable", lt, err)
				}
				return
			}
			if err == nil {
				t.Fatal("connectOnce accepted an invalid SUBACK")
			}
			if tr.Ready() {
				t.Fatal("invalid SUBACK left the transport ready")
			}
		})
	}
}

func TestTransportConnectOnceRejectsUnfinishedSubscribe(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{subscribeFn: func(*fakeClient, string, byte) subscribeToken {
			return &fakeSubscribeToken{fakeToken: newFakeToken()}
		}}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.SubscribeTimeout = 30 * time.Millisecond
	})
	if _, err := tr.connectOnce(context.Background()); err == nil {
		t.Fatal("connectOnce accepted a subscribe token that never completed")
	}
}

func TestTransportConnectOnceConnectError(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{connectErr: errors.New("connack refused")}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	if _, err := tr.connectOnce(context.Background()); err == nil {
		t.Fatal("connectOnce ignored a CONNECT error")
	}
}

func TestTransportConnectOnceCanceledDial(t *testing.T) {
	h := newHarness()
	entered := make(chan struct{}, 1)
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{openOnConnect: true} }
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.ConnectTimeout = 10 * time.Second
		o.dial = func(ctx context.Context, _ string) (net.Conn, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := tr.connectOnce(ctx); err == nil {
		t.Fatal("connectOnce accepted a dial that ended with a canceled context")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("connectOnce took %s, want it bounded by the canceled dial context", elapsed)
	}
	select {
	case <-entered:
	default:
		t.Fatal("the dial seam was never entered")
	}
}

// --- callback / readiness tests -------------------------------------------

func TestTransportMessageCallbackAdmitsToACKProcessor(t *testing.T) {
	// A real SQLite outbox and a real ACKProcessor prove the transport forwards
	// the callback to the bounded ACK path and that the ACK bytes are copied
	// before the driver buffer is reused.
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	item := enqueueTestBatch(t, o, outboxBootID, outboxMessageID, outboxBaseTime)

	processor, err := NewACKProcessor(o, 8)
	if err != nil {
		t.Fatalf("NewACKProcessor: %v", err)
	}

	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	tr := newTestTransport(t, processor, h, nil)
	primeConnectedLifetime(t, tr)

	topic, ack, err := telemetry.NewCommitACK(transportTestGatewayID, item.MessageID, telemetry.CommitOutcomeCommitted, outboxBaseTime.Add(time.Second))
	if err != nil {
		t.Fatalf("NewCommitACK: %v", err)
	}
	ackBytes, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("marshal ACK: %v", err)
	}

	// Reuse one buffer to model Paho handing us a recycled driver buffer.
	buffer := make([]byte, len(ackBytes))
	copy(buffer, ackBytes)
	if !h.client(0).invokeMessage(topic, buffer, false) {
		t.Fatal("message handler was not captured")
	}
	for i := range buffer {
		buffer[i] = 0 // overwrite the reused buffer after admission
	}

	// The worker must still see the copied bytes and delete exactly the row.
	processor.Start()
	select {
	case <-processor.Notify():
	case <-time.After(2 * time.Second):
		t.Fatal("ACK worker did not delete the pending row")
	}
	if _, ok, err := o.Lookup(context.Background(), item.MessageID); err != nil || ok {
		t.Fatalf("pending row still present after ACK: ok=%v err=%v", ok, err)
	}
	if stats := processor.Stats(); stats.Acknowledged != 1 {
		t.Fatalf("ACK stats = %+v, want one acknowledged", stats)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := processor.Stop(ctx); err != nil {
		t.Fatalf("processor.Stop: %v", err)
	}
}

func TestTransportLostClearsReadyImmediately(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	lt := primeConnectedLifetime(t, tr)
	tr.onLost(lt)
	if tr.Ready() {
		t.Fatal("connection loss left the transport ready")
	}
	if got := tr.counters.connectionLost.Load(); got != 1 {
		t.Fatalf("connectionLost = %d, want 1", got)
	}
}

func TestTransportStaleCallbacksCannotInvalidateReplacement(t *testing.T) {
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

	tr.onLost(old)
	tr.onMessage(old, "gateways/"+transportTestGatewayID+"/acks/x", []byte("stale"), false)

	if !tr.Ready() {
		t.Fatal("stale loss invalidated the replacement lifetime")
	}
	if calls := admit.calls(); len(calls) != 0 {
		t.Fatalf("stale callback admitted %d deliveries, want 0", len(calls))
	}
	if tr.Snapshot().Epoch != replacement.epoch {
		t.Fatalf("current epoch changed to %d, want %d", tr.Snapshot().Epoch, replacement.epoch)
	}
}

func TestTransportWaitReadyTimesOutWithoutConnection(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := tr.WaitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitReady = %v, want DeadlineExceeded", err)
	}
}

func TestTransportPublishGatedUntilReady(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)

	// Not ready: rejected without any Paho call.
	assertUnconfirmed(t, tr.Publish(context.Background(), "gateways/g/telemetry/s", []byte("p")))

	primeConnectedLifetime(t, tr)
	if err := tr.Publish(context.Background(), "gateways/g/telemetry/s", []byte("p")); err != nil {
		t.Fatalf("Publish after ready = %v", err)
	}
	if got := tr.Snapshot().PublishUnavailable; got != 1 {
		t.Fatalf("publishUnavailable = %d, want 1", got)
	}
}

// --- publish tests ---------------------------------------------------------

func TestTransportPublishQoS1NonRetained(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	primeConnectedLifetime(t, tr)

	topic := "gateways/" + transportTestGatewayID + "/telemetry/sensor_001"
	if err := tr.Publish(context.Background(), topic, []byte("payload")); err != nil {
		t.Fatalf("Publish = %v", err)
	}
	client := h.client(0)
	if client.lastPubTopic != topic || client.lastPubQoS != 1 || client.lastPubRetained {
		t.Fatalf("publish = (%q, %d, retained=%v), want (%q, 1, false)", client.lastPubTopic, client.lastPubQoS, client.lastPubRetained, topic)
	}
	if got := tr.Snapshot().Published; got != 1 {
		t.Fatalf("Published = %d, want 1", got)
	}
}

func TestTransportPublishTokenErrorUnconfirmed(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{publishFn: func(*fakeClient, string, byte, bool, []byte) waitToken {
			return completedToken(errors.New("write failed"))
		}}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	primeConnectedLifetime(t, tr)

	assertUnconfirmed(t, tr.Publish(context.Background(), "t", []byte("p")))
	if tr.Ready() {
		t.Fatal("failed publish left the transport ready")
	}
	if got := tr.Snapshot().PublishFailures; got != 1 {
		t.Fatalf("PublishFailures = %d, want 1", got)
	}
}

func TestTransportPublishTimeoutBlocksFurther(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{publishFn: func(*fakeClient, string, byte, bool, []byte) waitToken {
			return newFakeToken() // never completes
		}}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.PublishTimeout = 40 * time.Millisecond
	})
	primeConnectedLifetime(t, tr)

	start := time.Now()
	assertUnconfirmed(t, tr.Publish(context.Background(), "t", []byte("p")))
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Publish took %s, want bounded by the publish timeout", elapsed)
	}

	_, _, publishesBefore, _ := h.client(0).counts()
	assertUnconfirmed(t, tr.Publish(context.Background(), "t", []byte("p")))
	if _, _, publishesAfter, _ := h.client(0).counts(); publishesAfter != publishesBefore {
		t.Fatalf("publish attempted on a failed lifetime: %d -> %d", publishesBefore, publishesAfter)
	}
}

func TestTransportPublishRejectsCanceledContextBeforeSideEffects(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	primeConnectedLifetime(t, tr)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assertUnconfirmed(t, tr.Publish(ctx, "t", []byte("p")))
	if _, _, publishes, _ := h.client(0).counts(); publishes != 0 {
		t.Fatalf("Publish called Paho %d times with a canceled context", publishes)
	}
	if !tr.Ready() {
		t.Fatal("a canceled publish must not fail the current lifetime")
	}
}

// blockUntilSocketClosed models Paho's synchronous Publish blocking inside a
// socket write: the fake Publish CALL itself does not return until the
// transport closes the tracked socket. This exercises the whole-call bound, not
// only the token wait.
func blockUntilSocketClosed(c *fakeClient, _ string, _ byte, _ bool, _ []byte) waitToken {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if fc, ok := conn.(*fakeConn); ok {
		<-fc.closed
	}
	return completedToken(nil)
}

func TestTransportPublishBoundsBlockingPahoCall(t *testing.T) {
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
	assertUnconfirmed(t, tr.Publish(context.Background(), "t", []byte("p")))
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Publish took %s, want bounded by the publish timeout plus grace", elapsed)
	}
	if conn := h.connFor(h.client(0)); conn == nil || !conn.isClosed() {
		t.Fatal("transport did not close the socket to unblock the blocking Paho call")
	}
	if tr.Ready() || lt.usable() {
		t.Fatal("failed publish left the transport ready or the lifetime usable")
	}
}

func TestTransportPublishConcurrencyBounded(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{openOnConnect: true, publishFn: blockUntilSocketClosed}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.PublishTimeout = 40 * time.Millisecond
		o.publishStopGrace = 100 * time.Millisecond
		o.MaxInflight = 1
	})
	primeConnectedLifetime(t, tr)

	const callers = 4
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assertUnconfirmed(t, tr.Publish(context.Background(), "t", []byte("p")))
		}()
	}
	wg.Wait()

	if _, _, publishes, _ := h.client(0).counts(); publishes != 1 {
		t.Fatalf("Paho publishes = %d, want 1 (bounded concurrency)", publishes)
	}
}

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

func TestTransportPublishSlotHeldUntilInvocationReturns(t *testing.T) {
	h := newHarness()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{openOnConnect: true, publishFn: wedgedPublish(entered, release)}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.PublishTimeout = 30 * time.Millisecond
		o.publishStopGrace = 30 * time.Millisecond
		o.MaxInflight = 1
	})
	primeConnectedLifetime(t, tr)

	assertUnconfirmed(t, tr.Publish(context.Background(), "t", []byte("p")))
	<-entered

	if got := len(tr.publishSem); got != 1 {
		t.Fatalf("publish slots in use = %d, want 1 (held until the invocation returns)", got)
	}

	close(release)
	deadline := time.After(time.Second)
	for len(tr.publishSem) != 0 {
		select {
		case <-deadline:
			t.Fatal("publish slot was not returned after the invocation finished")
		case <-time.After(time.Millisecond):
		}
	}
}

// TestTransportPUBACKDoesNotDeleteOutboxRow proves the transport never removes
// a durable row on a successful MQTT PUBLISH: only an application ACK does.
func TestTransportPUBACKDoesNotDeleteOutboxRow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	o := openTestOutbox(t, testOutboxConfig(path))
	item := enqueueTestBatch(t, o, outboxBootID, outboxMessageID, outboxBaseTime)

	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	primeConnectedLifetime(t, tr)

	if err := tr.Publish(context.Background(), item.Topic, item.Payload); err != nil {
		t.Fatalf("Publish = %v", err)
	}
	if got := tr.Snapshot().Published; got != 1 {
		t.Fatalf("Published = %d, want 1", got)
	}
	if _, ok, err := o.Lookup(context.Background(), item.MessageID); err != nil || !ok {
		t.Fatalf("outbox row was removed by a PUBACK: ok=%v err=%v", ok, err)
	}
}

// --- supervisor lifecycle tests -------------------------------------------

func TestTransportStartOnce(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{openOnConnect: true} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	if err := tr.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	defer func() { _ = tr.Stop(context.Background()) }()
	if err := tr.Start(); err == nil {
		t.Fatal("second Start was accepted; the supervisor must be single")
	}
	waitReadyWithin(t, tr, time.Second)
}

func TestTransportSupervisorReconnectReplacesLifetime(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{openOnConnect: true} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	if err := tr.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	defer func() { _ = tr.Stop(context.Background()) }()

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

	if conn := h.connFor(first); conn == nil || !conn.isClosed() {
		t.Fatal("old socket was not closed before replacement")
	}
	if snap := tr.Snapshot(); snap.Epoch != 2 || snap.Connected < 2 || snap.Disconnected < 1 || snap.ConnectionLost < 1 {
		t.Fatalf("snapshot after reconnect = %+v", snap)
	}
}

func TestTransportStopReconnectKeepsCurrentForDrain(t *testing.T) {
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
	if err := tr.Publish(context.Background(), "gateways/g/telemetry/s", []byte("p")); err != nil {
		t.Fatalf("Publish during drain = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tr.Stop(ctx); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	if tr.Ready() {
		t.Fatal("transport still ready after Stop")
	}
	if _, _, _, disconnects := client.counts(); disconnects != 1 {
		t.Fatalf("disconnect calls after Stop = %d, want 1", disconnects)
	}
	if conn := h.connFor(client); conn == nil || !conn.isClosed() {
		t.Fatal("socket not closed by Stop")
	}
}

func TestTransportStopCancelsReconnectBackoff(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{connectErr: errors.New("broker down")}
	}
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.ReconnectMaxInterval = 5 * time.Second
		o.baseBackoff = time.Second
	})
	if err := tr.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	<-h.created

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if err := tr.Stop(ctx); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Stop took %s, want it to cancel the backoff promptly", elapsed)
	}
	if got := tr.Snapshot().ConnectFailures; got < 1 {
		t.Fatalf("ConnectFailures = %d, want >= 1", got)
	}
}

func TestTransportStopHonorsDeadline(t *testing.T) {
	h := newHarness()
	release := make(chan struct{})
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{openOnConnect: true} }
	tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) {
		o.ConnectTimeout = 10 * time.Second
		o.dial = func(context.Context, string) (net.Conn, error) {
			<-release // deliberately ignores context cancellation
			return newFakeConn(), nil
		}
	})
	if err := tr.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	<-h.created

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := tr.Stop(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want DeadlineExceeded honoring the caller deadline", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Stop took %s, want it bounded by the caller deadline", elapsed)
	}

	close(release)
	_ = tr.Stop(context.Background())
}

func TestTransportSupervisorFailClosedWhenTeardownStuck(t *testing.T) {
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

	assertUnconfirmed(t, tr.Publish(context.Background(), "t", []byte("p")))
	<-entered

	select {
	case <-tr.FatalSignal():
	case <-time.After(time.Second):
		t.Fatalf("transport did not fail closed; snapshot = %+v", tr.Snapshot())
	}
	if tr.Ready() {
		t.Fatal("fail-closed transport still reports ready")
	}
	if !errors.Is(tr.Fatal(), ErrTransportStuck) {
		t.Fatalf("Fatal = %v, want ErrTransportStuck", tr.Fatal())
	}

	// The supervisor must not replace the wedged lifetime with a new one.
	if got := h.clientCount(); got != 1 {
		t.Fatalf("supervisor created %d clients after a stuck teardown, want 1", got)
	}
	if snap := tr.Snapshot(); !snap.Stuck || snap.TeardownStuck != 1 {
		t.Fatalf("snapshot = %+v, want stuck with one reported teardown", snap)
	}

	// Stop must stay finite even though the invocation is still wedged.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- tr.Stop(ctx) }()
	select {
	case err := <-closed:
		if err != nil && !errors.Is(err, ErrTransportStuck) {
			t.Fatalf("Stop = %v, want nil or ErrTransportStuck", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop hung on a closed, stuck supervisor")
	}
	close(release)
}

func TestTransportSnapshotReportsReadyAndEpoch(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	tr := newTestTransport(t, &recordAdmitter{}, h, nil)
	if snap := tr.Snapshot(); snap.Ready || snap.Epoch != 0 {
		t.Fatalf("initial snapshot = %+v, want not ready and epoch 0", snap)
	}
	lt := primeConnectedLifetime(t, tr)
	snap := tr.Snapshot()
	if !snap.Ready || snap.Epoch != lt.epoch {
		t.Fatalf("snapshot = %+v, want ready epoch %d", snap, lt.epoch)
	}
}

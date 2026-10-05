package mqttcredential

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

const dynSecControlTopic = "$CONTROL/dynamic-security/v1"
const dynSecResponseTopic = dynSecControlTopic + "/response"

var errDynSecClientAbsent = errors.New("dynsec_client_absent")
var errDynSecRoleAbsent = errors.New("dynsec_role_absent")

// DynSecError is intentionally diagnostic-free. A timeout/disconnect/plugin
// error does not prove that a mutation was unchanged. Never wrap driver errors.
type DynSecError struct{ Code string }

func (e *DynSecError) Error() string { return "dynsec_" + e.Code }
func dynSecError(code string) error  { return &DynSecError{Code: code} }

// DynSecConfig is injected by a future composition root, not request input.
// Validators are server-owned allowlists (including per-target role ownership),
// not authorization. PostgreSQL admin admission is still required by the caller.
type DynSecConfig struct {
	BrokerURL          string
	CAPEM              []byte
	ManagerUsername    string
	ManagerPassword    string
	ProtectedUsernames []string
	Timeout            time.Duration
	MaxPayloadBytes    int
	MaxInflight        int
	QueueSize          int
	ValidateTarget     func(string) bool
	ValidateRole       func(string, string) bool
	// Optional private IPC raw transport. TLS wrapping and CA/SAN verification
	// remain owned here; this cannot provide an insecure Paho connection.
	ManagementDial func(context.Context) (net.Conn, error)
}

func (DynSecConfig) String() string { return "mqttcredential.DynSecConfig{redacted}" }

// GoString returns a redacted string representation of DynSecConfig.
func (c DynSecConfig) GoString() string { return c.String() }

// DynSecClientMetadata is an explicit safe projection. No native password,
// hash, salt, raw JSON or JSON-library types leave this adapter.
type DynSecClientMetadata struct {
	Username string   `json:"username"`
	Disabled bool     `json:"disabled"`
	Roles    []string `json:"roles"`
	Groups   []string `json:"groups,omitempty"`
	ClientID string   `json:"client_id,omitempty"`
}

// dynSecTransport must bound all operations by ctx; Subscribe must complete
// SUBACK before returning. Its callback transfers only bounded copied data.
// Publish receipt is transport-only and NEVER a logical command success.
type dynSecTransport interface {
	Subscribe(context.Context, func(string, []byte, bool), func()) error
	Publish(context.Context, []byte) error
	Close()
}
type dynSecPacket struct{ payload []byte }
type dynSecResult struct {
	data json.RawMessage
	err  error
}
type dynSecPending struct {
	command string
	result  chan dynSecResult
}

// DynSecClient owns one bounded response worker and bounded in-flight requests.
// Disconnect/overflow invalidates this lifetime; construct a fresh client after
// reconnect. No automatic command replay, arbitrary commands or gate controls.
type DynSecClient struct {
	cfg        DynSecConfig
	transport  dynSecTransport
	mu         sync.Mutex
	pending    map[string]dynSecPending
	stopped    bool
	queue      chan dynSecPacket
	done       chan struct{}
	workerDone chan struct{}
	closeOnce  sync.Once
}

func (*DynSecClient) String() string { return "mqttcredential.DynSecClient{redacted}" }

// GoString returns a redacted string representation of DynSecClient.
func (c *DynSecClient) GoString() string { return c.String() }

func validDynSecConfig(c DynSecConfig) bool {
	return c.ManagerUsername != "" && len(c.ProtectedUsernames) > 0 && c.Timeout > 0 && c.Timeout <= time.Minute && c.MaxPayloadBytes > 0 && c.MaxPayloadBytes <= 1<<20 && c.MaxInflight > 0 && c.MaxInflight <= 1024 && c.QueueSize > 0 && c.QueueSize <= 4096 && c.ValidateTarget != nil && c.ValidateRole != nil
}

// NewDynSecClient connects only with TLS >=1.2, CA and URL hostname/SAN checks.
// No insecure TLS switch is offered. Management and backend credentials remain
// distinct. This constructor does not implement a broker lifecycle controller.
func NewDynSecClient(ctx context.Context, cfg DynSecConfig) (*DynSecClient, error) {
	if !validDynSecConfig(cfg) || cfg.ManagerPassword == "" {
		return nil, dynSecError("configuration")
	}
	u, e := url.Parse(cfg.BrokerURL)
	if e != nil || u.Scheme != "ssl" || u.Hostname() == "" || u.Port() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, dynSecError("configuration")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cfg.CAPEM) {
		return nil, dynSecError("configuration")
	}
	id, e := uuid.NewRandom()
	if e != nil {
		return nil, dynSecError("unavailable")
	}
	t := &dynSecPahoTransport{url: u, tls: &tls.Config{RootCAs: roots, ServerName: u.Hostname(), MinVersion: tls.VersionTLS12}, cfg: cfg, clientID: "dynsec-" + id.String()}
	return newDynSecClient(ctx, cfg, t)
}

func newDynSecClient(ctx context.Context, cfg DynSecConfig, t dynSecTransport) (*DynSecClient, error) {
	if !validDynSecConfig(cfg) || dynSecNil(t) {
		return nil, dynSecError("configuration")
	}
	// Own config slices; callers cannot mutate principal guards after admission.
	cfg.ProtectedUsernames = append([]string(nil), cfg.ProtectedUsernames...)
	cfg.ManagerPassword = ""
	cfg.CAPEM = nil
	c := &DynSecClient{cfg: cfg, transport: t, pending: make(map[string]dynSecPending), queue: make(chan dynSecPacket, cfg.QueueSize), done: make(chan struct{}), workerDone: make(chan struct{})}
	go c.responses()
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	if e := t.Subscribe(ctx, c.receive, func() { c.invalidate("disconnected") }); e != nil {
		c.Close()
		return nil, dynSecError("unavailable")
	}
	c.mu.Lock()
	stopped := c.stopped
	c.mu.Unlock()
	if stopped {
		c.Close()
		return nil, dynSecError("disconnected")
	}
	return c, nil
}

// Interfaces containing nil concrete capabilities must fail configuration before
// starting a worker or invoking methods (including the package-private seam).
func dynSecNil(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return r.IsNil()
	default:
		return false
	}
}
func (c *DynSecClient) receive(topic string, b []byte, retained bool) {
	if topic != dynSecResponseTopic || retained {
		return
	}
	if len(b) > c.cfg.MaxPayloadBytes {
		c.invalidate("response_invalid")
		return
	}
	select {
	case <-c.done:
		return
	default:
	}
	select {
	case c.queue <- dynSecPacket{append([]byte(nil), b...)}:
	default:
		c.invalidate("response_overflow")
	}
}
func (c *DynSecClient) invalidate(code string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	c.stopped = true
	close(c.done)
	for key, p := range c.pending {
		p.result <- dynSecResult{err: dynSecError(code)}
		delete(c.pending, key)
	}
}

// Close terminates the dynamic security client and background worker.
func (c *DynSecClient) Close() {
	c.closeOnce.Do(func() { c.invalidate("closed"); c.transport.Close(); <-c.workerDone })
}

func (c *DynSecClient) responses() {
	defer close(c.workerDone)
	for {
		select {
		case <-c.done:
			return
		case packet := <-c.queue:
			if strictDynSecJSON(packet.payload) != nil {
				c.invalidate("response_invalid")
				continue
			}
			var envelope struct {
				Responses []struct {
					Command string          `json:"command"`
					Nonce   string          `json:"correlationData"`
					Error   json.RawMessage `json:"error"`
					Data    json.RawMessage `json:"data"`
				} `json:"responses"`
			}
			if json.Unmarshal(packet.payload, &envelope) != nil || envelope.Responses == nil || len(envelope.Responses) > c.cfg.MaxInflight {
				c.invalidate("response_invalid")
				continue
			}
			for _, r := range envelope.Responses {
				c.mu.Lock()
				p, ok := c.pending[r.Nonce]
				if ok && p.command == r.Command {
					result := dynSecResult{data: r.Data}
					if len(r.Error) > 0 {
						var message string
						if bytes.Equal(bytes.TrimSpace(r.Error), []byte("null")) || json.Unmarshal(r.Error, &message) != nil || message != "" {
							result = dynSecResult{err: dynSecError("command_rejected")}
							if p.command == "getClient" && message == "Client not found" {
								result.err = errDynSecClientAbsent
							}
							if p.command == "getRole" && message == "Role not found" {
								result.err = errDynSecRoleAbsent
							}
						}
					}
					delete(c.pending, r.Nonce)
					p.result <- result
				}
				c.mu.Unlock()
			}
		}
	}
}

func (c *DynSecClient) target(username string) bool {
	if username == "" || username == c.cfg.ManagerUsername || username == BackendUsername {
		return false
	}
	for _, s := range c.cfg.ProtectedUsernames {
		if username == s {
			return false
		}
	}
	return c.cfg.ValidateTarget(username)
}
func (c *DynSecClient) request(ctx context.Context, command, username, password, role string) (json.RawMessage, error) {
	if !c.target(username) {
		return nil, dynSecError("target_denied")
	}
	switch command {
	case "createClient", "disableClient", "enableClient", "getClient":
	case "setClientPassword":
		if !validPassword(password) {
			return nil, dynSecError("input")
		}
	case "addClientRole":
		if role == "" || !c.cfg.ValidateRole(username, role) {
			return nil, dynSecError("role_denied")
		}
	default:
		return nil, dynSecError("input")
	}
	fields := map[string]any{"username": username}
	if password != "" {
		fields["password"] = password
	}
	if role != "" {
		fields["rolename"] = role
	}
	return c.adapterRequest(ctx, command, fields)
}

// Package-private framing seam: adapter owns role schemas, never HTTP callers.
func (c *DynSecClient) adapterRequest(ctx context.Context, command string, fields map[string]any) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	if ctx.Err() != nil {
		return nil, dynSecError("cancelled")
	}
	nonce, e := uuid.NewRandom()
	if e != nil {
		return nil, dynSecError("unavailable")
	}
	// Private request-local DTO prevents routine fmt from disclosing passwords.
	fields["command"], fields["correlationData"] = command, nonce.String()
	payload, e := json.Marshal(map[string]any{"commands": []any{fields}})
	if e != nil || len(payload) > c.cfg.MaxPayloadBytes {
		return nil, dynSecError("payload_limit")
	}
	defer clear(payload)
	result := make(chan dynSecResult, 1)
	key := nonce.String()
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil, dynSecError("disconnected")
	}
	if len(c.pending) >= c.cfg.MaxInflight {
		c.mu.Unlock()
		return nil, dynSecError("busy")
	}
	c.pending[key] = dynSecPending{command, result}
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, key); c.mu.Unlock() }()
	if c.transport.Publish(ctx, payload) != nil {
		// Do not admit more work behind a stalled Paho writer/pending token.
		c.invalidate("publication_uncertain")
		return nil, dynSecError("publication_uncertain")
	}
	select {
	case r := <-result:
		return r.data, r.err
	case <-ctx.Done():
		return nil, dynSecError("response_uncertain")
	}
}

// CreateClient deliberately omits password/roles/disabled: pinned 2.0.18
// ignores createClient.disabled. Caller must disable, set password/role then
// enable while the independently owned maintenance gate remains closed.
func (c *DynSecClient) CreateClient(ctx context.Context, u string) error {
	_, e := c.request(ctx, "createClient", u, "", "")
	return e
}

// DisableClient sends a disableClient command to dynamic security for the target username.
func (c *DynSecClient) DisableClient(ctx context.Context, u string) error {
	_, e := c.request(ctx, "disableClient", u, "", "")
	return e
}

// EnableClient sends an enableClient command to dynamic security for the target username.
func (c *DynSecClient) EnableClient(ctx context.Context, u string) error {
	_, e := c.request(ctx, "enableClient", u, "", "")
	return e
}

// SetClientPassword updates the dynamic security password for the target username.
func (c *DynSecClient) SetClientPassword(ctx context.Context, u, password string) error {
	_, e := c.request(ctx, "setClientPassword", u, password, "")
	return e
}

// AddClientRole assigns an existing dynamic security role to the target username.
func (c *DynSecClient) AddClientRole(ctx context.Context, u, role string) error {
	_, e := c.request(ctx, "addClientRole", u, "", role)
	return e
}

// GetClient retrieves projected client metadata for the target username.
func (c *DynSecClient) GetClient(ctx context.Context, u string) (DynSecClientMetadata, error) {
	b, e := c.request(ctx, "getClient", u, "", "")
	if e != nil {
		return DynSecClientMetadata{}, e
	}
	var data struct {
		Client json.RawMessage `json:"client"`
	}
	if json.Unmarshal(b, &data) != nil {
		return DynSecClientMetadata{}, dynSecError("response_invalid")
	}
	return projectDynSecClient(data.Client, u)
}

// Token waiting is bounded without a goroutine per token/message.
func dynSecWait(ctx context.Context, t mqtt.Token) error {
	select {
	case <-ctx.Done():
		return dynSecError("unavailable")
	case <-t.Done():
		if t.Error() != nil {
			return dynSecError("unavailable")
		}
		return nil
	}
}

type dynSecPahoTransport struct {
	cfg      DynSecConfig
	url      *url.URL
	tls      *tls.Config
	clientID string
	client   mqtt.Client
	mu       sync.Mutex
	conn     net.Conn
	closed   bool
}

func (t *dynSecPahoTransport) Subscribe(ctx context.Context, receive func(string, []byte, bool), lost func()) error {
	opts := mqtt.NewClientOptions().AddBroker(t.url.String()).SetClientID(t.clientID).SetUsername(t.cfg.ManagerUsername).SetPassword(t.cfg.ManagerPassword).SetProtocolVersion(4).SetAutoReconnect(false).SetConnectRetry(false).SetCleanSession(true).SetConnectTimeout(t.cfg.Timeout).SetWriteTimeout(t.cfg.Timeout).SetOrderMatters(true)
	opts.SetConnectionLostHandler(func(mqtt.Client, error) { lost() })
	opts.SetCustomOpenConnectionFn(func(_ *url.URL, _ mqtt.ClientOptions) (net.Conn, error) {
		var conn net.Conn
		var e error
		if t.cfg.ManagementDial == nil {
			dialer := tls.Dialer{NetDialer: &net.Dialer{}, Config: t.tls.Clone()}
			conn, e = dialer.DialContext(ctx, "tcp", t.url.Host)
		} else {
			var raw net.Conn
			raw, e = t.cfg.ManagementDial(ctx)
			if e == nil && raw != nil {
				secure := tls.Client(raw, t.tls.Clone())
				e = secure.HandshakeContext(ctx)
				if e != nil {
					_ = raw.Close()
				} else {
					conn = secure
				}
			} else if e == nil {
				e = dynSecError("unavailable")
			}
		}
		if e != nil {
			return nil, dynSecError("unavailable")
		}
		t.mu.Lock()
		if t.closed || ctx.Err() != nil {
			t.mu.Unlock()
			_ = conn.Close()
			return nil, dynSecError("unavailable")
		}
		t.conn = conn
		t.mu.Unlock()
		return conn, nil
	})
	t.client = mqtt.NewClient(opts)
	if e := dynSecWait(ctx, t.client.Connect()); e != nil {
		return e
	}
	token := t.client.Subscribe(dynSecResponseTopic, 0, func(_ mqtt.Client, m mqtt.Message) { receive(m.Topic(), m.Payload(), m.Retained()) })
	if e := dynSecWait(ctx, token); e != nil {
		return e
	}
	if sub, ok := token.(*mqtt.SubscribeToken); !ok || sub.Result()[dynSecResponseTopic] == 0x80 {
		return dynSecError("subscription_denied")
	}
	return nil
}
func (t *dynSecPahoTransport) Publish(ctx context.Context, b []byte) error {
	// Paho owns the immutable copy until its writer completes, even on cancellation.
	return dynSecWait(ctx, t.client.Publish(dynSecControlTopic, 1, false, append([]byte(nil), b...)))
}
func (t *dynSecPahoTransport) Close() {
	t.mu.Lock()
	t.closed = true
	if t.conn != nil {
		_ = t.conn.Close()
	}
	t.mu.Unlock()
	if t.client != nil {
		t.client.Disconnect(0)
	}
	t.cfg.ManagerPassword = ""
}

// strictDynSecJSON rejects duplicate keys (including ignored hash fields),
// trailing values and deeply nested inputs rather than accepting last-key wins.
func strictDynSecJSON(b []byte) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 32 {
			return dynSecError("json_invalid")
		}
		token, e := d.Token()
		if e != nil {
			return dynSecError("json_invalid")
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{':
				keys := map[string]bool{}
				for d.More() {
					k, e := d.Token()
					s, ok := k.(string)
					if e != nil || !ok || keys[s] {
						return dynSecError("json_invalid")
					}
					keys[s] = true
					if e = walk(depth + 1); e != nil {
						return e
					}
				}
			case '[':
				for d.More() {
					if e = walk(depth + 1); e != nil {
						return e
					}
				}
			default:
				return dynSecError("json_invalid")
			}
			if _, e = d.Token(); e != nil {
				return dynSecError("json_invalid")
			}
		}
		return nil
	}
	if e := walk(0); e != nil {
		return e
	}
	if _, e := d.Token(); e != io.EOF {
		return dynSecError("json_invalid")
	}
	return nil
}

func projectDynSecClient(b []byte, target string) (DynSecClientMetadata, error) {
	bad := func() (DynSecClientMetadata, error) { return DynSecClientMetadata{}, dynSecError("metadata_invalid") }
	var fields map[string]json.RawMessage
	if json.Unmarshal(b, &fields) != nil || fields == nil {
		return bad()
	}
	var u string
	if json.Unmarshal(fields["username"], &u) != nil || u == "" || u != target {
		return bad()
	}
	result := DynSecClientMetadata{Username: u, Roles: []string{}}
	if raw, ok := fields["clientid"]; ok {
		if string(raw) == "null" || json.Unmarshal(raw, &result.ClientID) != nil {
			return bad()
		}
	}
	if raw, ok := fields["groups"]; ok {
		var groups []struct {
			Name string `json:"groupname"`
		}
		if string(raw) == "null" || json.Unmarshal(raw, &groups) != nil {
			return bad()
		}
		for _, g := range groups {
			if g.Name == "" {
				return bad()
			}
			result.Groups = append(result.Groups, g.Name)
		}
	}
	if raw, ok := fields["disabled"]; ok {
		if string(raw) == "null" || json.Unmarshal(raw, &result.Disabled) != nil {
			return bad()
		}
	}
	// Missing disabled is native 2.0.18 enabled default, NOT a read-failure default.
	if raw, ok := fields["roles"]; ok {
		var roles []map[string]json.RawMessage
		if string(raw) == "null" || json.Unmarshal(raw, &roles) != nil {
			return bad()
		}
		seen := map[string]bool{}
		for _, r := range roles {
			var name string
			if json.Unmarshal(r["rolename"], &name) != nil || name == "" || strings.ContainsAny(name, "\x00\r\n") || seen[name] {
				return bad()
			}
			seen[name] = true
			result.Roles = append(result.Roles, name)
		}
	}
	return result, nil
}

package mqttcredential

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type dynsecFake struct {
	mu         sync.Mutex
	receive    func(string, []byte, bool)
	lost       func()
	publish    func([]byte)
	subscribed bool
	fail       bool
}

func (f *dynsecFake) Subscribe(_ context.Context, receive func(string, []byte, bool), lost func()) error {
	f.receive, f.lost, f.subscribed = receive, lost, true
	return nil
}
func (f *dynsecFake) Publish(_ context.Context, payload []byte) error {
	if !f.subscribed {
		panic("publish before subscribe")
	}
	if f.fail {
		return errors.New("SECRET transport")
	}
	f.publish(payload)
	return nil
}
func (f *dynsecFake) Close() {}
func dynsecTestConfig() DynSecConfig {
	return DynSecConfig{ManagerUsername: "manager", ProtectedUsernames: []string{"backend"}, Timeout: 50 * time.Millisecond, MaxPayloadBytes: 4096, MaxInflight: 4, QueueSize: 8, ValidateTarget: func(s string) bool { return s == "A" || s == "B" }, ValidateRole: func(s, r string) bool { return r == "gateway_"+s }}
}

func TestDynSecInternalTypedNilTransport(t *testing.T) {
	var transport *dynsecFake
	if c, e := newDynSecClient(context.Background(), dynsecTestConfig(), transport); c != nil || e == nil {
		t.Fatal("typed nil transport accepted")
	}
}
func dynsecReply(raw []byte, command, nonce string, fields map[string]any) []byte {
	var req struct {
		Commands []map[string]any `json:"commands"`
	}
	_ = json.Unmarshal(raw, &req)
	if command == "" {
		command = req.Commands[0]["command"].(string)
	}
	if nonce == "" {
		nonce = req.Commands[0]["correlationData"].(string)
	}
	r := map[string]any{"command": command, "correlationData": nonce}
	for k, v := range fields {
		r[k] = v
	}
	b, _ := json.Marshal(map[string]any{"responses": []any{r}})
	return b
}
func TestDynSecCorrelationAndSafeProjection(t *testing.T) {
	f := &dynsecFake{}
	c, e := newDynSecClient(context.Background(), dynsecTestConfig(), f)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if strings.Contains(fmt.Sprintf("%#v", c), "SECRET") || c.String() == "" {
		t.Fatal("client formatter")
	}
	f.publish = func(b []byte) {
		f.receive(dynSecResponseTopic, dynsecReply(b, "disableClient", "", nil), false)
		f.receive(dynSecResponseTopic, dynsecReply(b, "", "stale", nil), false)
		f.receive(dynSecResponseTopic, dynsecReply(b, "", "", map[string]any{"data": map[string]any{"client": map[string]any{"username": "A", "password": "SECRET", "roles": []any{map[string]any{"rolename": "gateway_A"}}}}}), false)
	}
	got, e := c.GetClient(context.Background(), "A")
	if e != nil || got.Username != "A" || got.Disabled || len(got.Roles) != 1 {
		t.Fatalf("projection: %+v %v", got, e)
	}
	b, _ := json.Marshal(got)
	if strings.Contains(string(b), "SECRET") {
		t.Fatal("secret projection")
	}
}
func TestDynSecUncertaintyAndProtection(t *testing.T) {
	for _, mode := range []string{"timeout", "error", "malformed", "disconnect", "retained", "oversize", "transport"} {
		t.Run(mode, func(t *testing.T) {
			f := &dynsecFake{}
			cfg := dynsecTestConfig()
			c, e := newDynSecClient(context.Background(), cfg, f)
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			f.publish = func(b []byte) {
				switch mode {
				case "error":
					f.receive(dynSecResponseTopic, dynsecReply(b, "", "", map[string]any{"error": "SECRET password"}), false)
				case "malformed":
					f.receive(dynSecResponseTopic, []byte("{"), false)
				case "disconnect":
					f.lost()
				case "retained":
					f.receive(dynSecResponseTopic, dynsecReply(b, "", "", nil), true)
				case "oversize":
					f.receive(dynSecResponseTopic, make([]byte, 4097), false)
				}
			}
			f.fail = mode == "transport"
			e = c.DisableClient(context.Background(), "A")
			if e == nil || strings.Contains(e.Error(), "SECRET") {
				t.Fatalf("safe uncertain error: %v", e)
			}
			c.mu.Lock()
			n := len(c.pending)
			c.mu.Unlock()
			if n != 0 {
				t.Fatal("pending leak")
			}
			if c.EnableClient(context.Background(), "manager") == nil || c.EnableClient(context.Background(), "backend") == nil || c.AddClientRole(context.Background(), "A", "management") == nil {
				t.Fatal("protected principal/role")
			}
		})
	}
}
func TestDynSecAllCommands(t *testing.T) {
	f := &dynsecFake{}
	c, e := newDynSecClient(context.Background(), dynsecTestConfig(), f)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	f.publish = func(b []byte) { f.receive(dynSecResponseTopic, dynsecReply(b, "", "", nil), false) }
	for _, fn := range []func() error{func() error { return c.CreateClient(context.Background(), "A") }, func() error { return c.DisableClient(context.Background(), "A") }, func() error { return c.SetClientPassword(context.Background(), "A", strings.Repeat("x", 43)) }, func() error { return c.AddClientRole(context.Background(), "A", "gateway_A") }, func() error { return c.EnableClient(context.Background(), "A") }} {
		if e := fn(); e != nil {
			t.Fatal(e)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if c.DisableClient(ctx, "A") == nil {
		t.Fatal("cancelled")
	}
}

func TestDynSecConcurrencyBoundsAndFreshLifetime(t *testing.T) {
	f := &dynsecFake{}
	cfg := dynsecTestConfig()
	cfg.MaxInflight = 1
	cfg.Timeout = time.Second
	c, e := newDynSecClient(context.Background(), cfg, f)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	sent := make(chan []byte, 1)
	f.publish = func(b []byte) { sent <- append([]byte(nil), b...) }
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- c.DisableClient(ctx, "A") }()
	request := <-sent
	if c.EnableClient(context.Background(), "B") == nil {
		t.Fatal("inflight bound")
	}
	f.receive("unrelated", request, false)
	cancel()
	if <-finished == nil {
		t.Fatal("cancellation success")
	}
	// Stale response from removed request cannot complete a new request.
	f.publish = func(b []byte) {
		f.receive(dynSecResponseTopic, dynsecReply(request, "", "", nil), false)
		f.receive(dynSecResponseTopic, dynsecReply(b, "", "", nil), false)
	}
	if e = c.DisableClient(context.Background(), "A"); e != nil {
		t.Fatal(e)
	}
	f.lost()
	if c.EnableClient(context.Background(), "A") == nil {
		t.Fatal("reuse disconnected lifetime")
	}
	fresh := &dynsecFake{}
	next, e := newDynSecClient(context.Background(), cfg, fresh)
	if e != nil {
		t.Fatal(e)
	}
	defer next.Close()
	fresh.publish = func(b []byte) { fresh.receive(dynSecResponseTopic, dynsecReply(b, "", "", nil), false) }
	if e = next.DisableClient(context.Background(), "A"); e != nil {
		t.Fatal(e)
	}
}

func TestDynSecValidationAndStrictJSON(t *testing.T) {
	cfg := dynsecTestConfig()
	cfg.ManagerPassword = "TOPSECRET"
	cfg.CAPEM = []byte("TOPSECRET")
	for _, format := range []string{"%v", "%+v", "%#v"} {
		if strings.Contains(fmt.Sprintf(format, cfg), "TOPSECRET") {
			t.Fatal("config formatter disclosure")
		}
	}
	for _, raw := range []string{"", `{} {}`, `{"a":1,"a":2}`, `[`, strings.Repeat("[", 34) + strings.Repeat("]", 34)} {
		if strictDynSecJSON([]byte(raw)) == nil {
			t.Fatal("invalid JSON accepted")
		}
	}
	for _, raw := range []string{`null`, `{}`, `{"username":"B"}`, `{"username":"A","roles":null}`, `{"username":"A","roles":[{"rolename":"x"},{"rolename":"x"}]}`} {
		if _, e := projectDynSecClient([]byte(raw), "A"); e == nil {
			t.Fatal("invalid projection")
		}
	}
	for _, bad := range []DynSecConfig{DynSecConfig{}, cfg} {
		if _, e := NewDynSecClient(context.Background(), bad); e == nil {
			t.Fatal("invalid endpoint")
		}
	}
	f := &dynsecFake{}
	c, e := newDynSecClient(context.Background(), dynsecTestConfig(), f)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	f.publish = func(b []byte) {
		f.receive(dynSecResponseTopic, dynsecReply(b, "", "", map[string]any{"error": true}), false)
	}
	if c.SetClientPassword(context.Background(), "A", "") == nil {
		t.Fatal("invalid password")
	}
	if _, e = c.request(context.Background(), "deleteClient", "A", "", ""); e == nil {
		t.Fatal("raw command")
	}
	if c.DisableClient(context.Background(), "A") == nil {
		t.Fatal("malformed command error")
	}
}

func TestDynSecInvalidEnvelopesAndLimits(t *testing.T) {
	for _, raw := range []string{`{}`, `{"responses":null}`, `{"responses":1}`, `{"responses":[{}, {}, {}, {}, {}]}`} {
		f := &dynsecFake{}
		c, e := newDynSecClient(context.Background(), dynsecTestConfig(), f)
		if e != nil {
			t.Fatal(e)
		}
		f.publish = func([]byte) { f.receive(dynSecResponseTopic, []byte(raw), false) }
		if c.DisableClient(context.Background(), "A") == nil {
			t.Fatal("invalid envelope success")
		}
		c.Close()
	}
	cfg := dynsecTestConfig()
	cfg.MaxPayloadBytes = 16
	f := &dynsecFake{}
	c, e := newDynSecClient(context.Background(), cfg, f)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if c.DisableClient(context.Background(), "A") == nil {
		t.Fatal("request payload bound")
	}
	if _, e = newDynSecClient(context.Background(), DynSecConfig{}, f); e == nil {
		t.Fatal("invalid config")
	}
	for _, raw := range []string{`null`, `{}`, `{"client":null}`, `{"client":{"username":"B"}}`} {
		ff := &dynsecFake{}
		cc, e := newDynSecClient(context.Background(), dynsecTestConfig(), ff)
		if e != nil {
			t.Fatal(e)
		}
		ff.publish = func(b []byte) {
			reply := dynsecReply(b, "", "", map[string]any{"data": json.RawMessage(raw)})
			ff.receive(dynSecResponseTopic, reply, false)
		}
		if _, e = cc.GetClient(context.Background(), "A"); e == nil {
			t.Fatal("bad metadata success")
		}
		cc.Close()
	}
}

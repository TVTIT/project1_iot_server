//nolint:errcheck,misspell // Cleanup only; Mosquitto runtime paths are product names, not spelling errors.
package mqttcredential

// Executed by the isolated harness as UID1883. Fixture secrets enter only via
// stdin, never flags, environment or diagnostics. No shipped debug API.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

type integrationFixture struct{ Phase, A, B, Old, New, BPassword, Backend string }

func TestProductionRuntimeFaultIntegration(t *testing.T) {
	if os.Getenv("MQTT_RUNTIME_INTEGRATION") != "1" {
		t.Skip("explicit disposable-container selector required")
	}
	if os.Getuid() != 1883 {
		t.Fatal("requires UID1883")
	}
	var f integrationFixture
	if json.NewDecoder(os.Stdin).Decode(&f) != nil {
		t.Fatal("invalid stdin fixture")
	}
	if f.Phase == "fault-enospc" {
		// The fixture's PRIVATE /tmp is a size=1m tmpfs, never host disk.
		var stat syscall.Statfs_t
		if syscall.Statfs("/tmp", &stat) != nil || stat.Type != 0x01021994 || uint64(stat.Bsize)*stat.Blocks > 2<<20 {
			t.Fatal("requires private limited tmpfs")
		}
		dir, e := os.MkdirTemp("/tmp", "auth-fault-")
		if e != nil {
			t.Fatal(e)
		}
		defer os.RemoveAll(dir)
		old, e := os.ReadFile("/mosquitto/auth/passwd")
		if e != nil {
			t.Fatal(e)
		}
		for _, name := range []string{"passwd", "passwd.last-good"} {
			if e := os.WriteFile(filepath.Join(dir, name), old, 0600); e != nil {
				t.Fatal(e)
			}
		}
		// Write until ENOSPC only after proving the bounded private filesystem.
		fill := filepath.Join(dir, "filler")
		if e := os.WriteFile(fill, make([]byte, 2<<20), 0600); !errors.Is(e, syscall.ENOSPC) {
			t.Fatal("did not reach bounded ENOSPC")
		}
		r, e := NewRuntime(RuntimeConfig{AuthDir: dir, Reload: func(context.Context) error { t.Error("ENOSPC reached reload"); return nil }, RecoveryProbe: func(context.Context, string, string) error { t.Error("ENOSPC reached recovery"); return nil }})
		if e != nil {
			t.Fatal(e)
		}
		if e := r.UpsertGatewayCredential(context.Background(), "disk-full", f.B, f.New, successfulProbe); !errors.Is(e, ErrPersistenceFailure) {
			t.Fatal(e)
		}
		for _, name := range []string{"passwd", "passwd.last-good"} {
			b, e := os.ReadFile(filepath.Join(dir, name))
			if e != nil || !bytes.Equal(b, old) {
				t.Fatal("ENOSPC changed authoritative snapshot")
			}
		}
		if e := os.Remove(fill); e != nil {
			t.Fatal(e)
		}
		if e := r.CheckRuntime(context.Background()); e != nil {
			t.Fatal(e)
		}
		t.Log("production adapter phase completed: " + f.Phase)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p, e := NewProbe("ssl://broker:8883", "/mosquitto/config/ca.crt", time.Second)
	if e != nil {
		t.Fatal(e)
	}
	reload, e := NewReloadClient(ReloadClientConfig{SocketPath: "/mosquitto/control/reload.sock", Timeout: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	eventually := func(c context.Context, check ProbeFunc) error {
		for {
			if check(c) == nil {
				return nil
			}
			select {
			case <-c.Done():
				return ErrVerificationFailed
			case <-time.After(25 * time.Millisecond):
			}
		}
	}
	r, e := NewRuntime(RuntimeConfig{AuthDir: "/mosquitto/auth", Reload: reload.Reload, RecoveryTimeout: 3 * time.Second, ProbeTimeout: 2 * time.Second, RecoveryProbe: func(c context.Context, op, id string) error {
		if op != "fault" || id != f.B {
			return ErrVerificationFailed
		}
		return eventually(c, func(c context.Context) error { return p.Login(c, id, f.BPassword) })
	}})
	if e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(r.path("passwd"))
	if e != nil {
		t.Fatal(e)
	}
	last, e := os.ReadFile(r.path("passwd.last-good"))
	if e != nil {
		t.Fatal(e)
	}
	if e = r.CheckRuntime(ctx); e != nil {
		t.Fatal(e)
	}
	if f.Phase == "fault-lost-ack" {
		var calls int
		r.config.Reload = func(c context.Context) error {
			calls++
			if e := reload.Reload(c); e != nil {
				return e
			}
			if calls == 1 {
				return ErrReloadUnavailable
			}
			return nil
		}
	}
	verify := func(c context.Context) error {
		return eventually(c, func(c context.Context) error { return p.Login(c, f.B, f.New) })
	}
	if f.Phase == "fault-probe" {
		verify = func(context.Context) error { return ErrVerificationFailed }
	}
	if f.Phase != "fault-probe" && f.Phase != "fault-lost-ack" && f.Phase != "fault-killed-reloader" && f.Phase != "fault-offline" {
		t.Fatal("unknown fault phase")
	}
	e = r.UpsertGatewayCredential(ctx, "fault", f.B, f.New, verify)
	if e == nil {
		t.Fatal("fault returned success")
	}
	after, readErr := os.ReadFile(r.path("passwd"))
	if readErr != nil || !bytes.Equal(before, after) {
		t.Fatal("previous snapshot not preserved")
	}
	afterLast, readErr := os.ReadFile(r.path("passwd.last-good"))
	if readErr != nil || !bytes.Equal(last, afterLast) {
		t.Fatal("last-good changed")
	}
	fresh, _ := NewRuntime(r.config)
	if f.Phase == "fault-killed-reloader" || f.Phase == "fault-offline" {
		if !errors.Is(e, ErrRecoveryRequired) || !errors.Is(fresh.CheckRuntime(ctx), ErrRecoveryRequired) {
			t.Fatal("unknown recovery was not durable fail-closed")
		}
		if !errors.Is(fresh.RemoveGatewayCredential(ctx, "blocked", f.B, verify), ErrRecoveryRequired) {
			t.Fatal("mutation accepted after uncertain recovery")
		}
		if f.Phase == "fault-offline" && p.Rejected(ctx, f.B, f.BPassword) == nil {
			t.Fatal("network failure misclassified as auth rejection")
		}
	} else {
		if errors.Is(e, ErrRecoveryRequired) {
			t.Fatal(e)
		}
		if e = fresh.CheckRuntime(ctx); e != nil {
			t.Fatal(e)
		}
		if e = p.Login(ctx, f.B, f.BPassword); e != nil {
			t.Fatal(e)
		}
		if e = p.Rejected(ctx, f.B, f.New); e != nil {
			t.Fatal(e)
		}
	}
	t.Log("production adapter phase completed: " + f.Phase)
}

func TestProductionRuntimeIntegration(t *testing.T) {
	if os.Getenv("MQTT_RUNTIME_INTEGRATION") != "1" {
		t.Skip("explicit disposable-container selector required")
	}
	if os.Getuid() != 1883 {
		t.Fatal("requires UID1883")
	}
	var f integrationFixture
	if json.NewDecoder(os.Stdin).Decode(&f) != nil {
		t.Fatal("invalid stdin fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	must := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	p, e := NewProbe("ssl://broker:8883", "/mosquitto/config/ca.crt", 2*time.Second)
	must(e)
	reload, e := NewReloadClient(ReloadClientConfig{SocketPath: "/mosquitto/control/reload.sock", Timeout: 2 * time.Second})
	must(e)
	previous := func(c context.Context, op, id string) error {
		switch op {
		case "create-a":
			return p.Rejected(c, id, f.Old)
		case "create-b":
			return p.Rejected(c, id, f.BPassword)
		case "rotate-a":
			return p.Login(c, id, f.Old)
		case "revoke-a":
			return p.Login(c, id, f.New)
		}
		return ErrVerificationFailed
	}
	r, e := NewRuntime(RuntimeConfig{AuthDir: "/mosquitto/auth", Reload: reload.Reload, RecoveryProbe: previous, ProbeTimeout: 6 * time.Second})
	must(e)
	must(r.CheckRuntime(ctx))
	must(reload.Check(ctx))
	must(p.Login(ctx, "backend_service", f.Backend))
	before, e := os.ReadFile("/mosquitto/auth/passwd")
	must(e)
	backendLine := func(b []byte) []byte {
		for _, l := range bytes.Split(b, []byte("\n")) {
			if bytes.HasPrefix(l, []byte("backend_service:")) {
				return l
			}
		}
		return nil
	}
	protected := append([]byte(nil), backendLine(before)...)
	if len(protected) == 0 {
		t.Fatal("missing protected principal")
	}
	// Signal ACK is not reload proof: each bounded retry opens fresh Paho TLS.
	eventually := func(check ProbeFunc) ProbeFunc {
		return func(c context.Context) error {
			for {
				if check(c) == nil {
					return nil
				}
				select {
				case <-c.Done():
					return ErrVerificationFailed
				case <-time.After(25 * time.Millisecond):
				}
			}
		}
	}
	switch f.Phase {
	case "mutate":
		must(r.UpsertGatewayCredential(ctx, "create-a", f.A, f.Old, eventually(func(c context.Context) error { return p.Login(c, f.A, f.Old) })))
		must(r.UpsertGatewayCredential(ctx, "create-b", f.B, f.BPassword, eventually(func(c context.Context) error { return p.Login(c, f.B, f.BPassword) })))
		withB, e := os.ReadFile("/mosquitto/auth/passwd")
		must(e)
		bLine := func(contents []byte) []byte {
			for _, line := range bytes.Split(contents, []byte("\n")) {
				if bytes.HasPrefix(line, []byte(f.B+":")) {
					return line
				}
			}
			return nil
		}
		unchangedB := append([]byte(nil), bLine(withB)...)
		if len(unchangedB) == 0 {
			t.Fatal("Gateway B missing")
		}
		must(p.Rejected(ctx, f.A, "wrong-password-test"))
		integrationACL(t, p, f, f.Old)
		session := integrationClient(t, p, f.A, f.Old)
		must(r.UpsertGatewayCredential(ctx, "rotate-a", f.A, f.New, eventually(func(c context.Context) error {
			if e := p.Login(c, f.A, f.New); e != nil {
				return e
			}
			return p.Rejected(c, f.A, f.Old)
		})))
		t.Logf("rotation_existing_session_connected=%t (observation only)", session.IsConnectionOpen())
		session.Disconnect(0)
		must(p.Login(ctx, f.B, f.BPassword))
		must(p.Login(ctx, "backend_service", f.Backend))
		integrationACL(t, p, f, f.New)
		session = integrationClient(t, p, f.A, f.New)
		must(r.RemoveGatewayCredential(ctx, "revoke-a", f.A, eventually(func(c context.Context) error { return p.Rejected(c, f.A, f.New) })))
		t.Logf("revocation_existing_session_connected=%t (observation only)", session.IsConnectionOpen())
		session.Disconnect(0)
		final, e := os.ReadFile("/mosquitto/auth/passwd")
		must(e)
		if !bytes.Equal(unchangedB, bLine(final)) {
			t.Fatal("unrelated Gateway B hash changed")
		}
	case "persist":
		must(p.Rejected(ctx, f.A, f.Old))
		must(p.Rejected(ctx, f.A, f.New))
	default:
		t.Fatal("unknown integration phase")
	}
	must(p.Login(ctx, f.B, f.BPassword))
	must(p.Login(ctx, "backend_service", f.Backend))
	must(r.CheckRuntime(ctx))
	after, e := os.ReadFile("/mosquitto/auth/passwd")
	must(e)
	if !bytes.Equal(protected, backendLine(after)) {
		t.Fatal("protected backend hash mutated")
	}
	if e := r.RemoveGatewayCredential(ctx, "protected", "backend_service", func(context.Context) error { return nil }); !errors.Is(e, ErrInvalidInput) {
		t.Fatal("protected mutation accepted")
	}
	for _, ca := range []string{"/mosquitto/config/wrong-ca.crt", "/mosquitto/config/ca.crt"} {
		bad, e := NewProbe(p.broker, ca, time.Second)
		must(e)
		if ca == "/mosquitto/config/ca.crt" {
			bad.tls.ServerName = "wrong.invalid"
		}
		if bad.Rejected(ctx, "backend_service", f.Backend) == nil || bad.Login(ctx, "backend_service", f.Backend) == nil {
			t.Fatal("TLS failure accepted as auth evidence")
		}
	}
	o := mqtt.NewClientOptions().AddBroker(p.broker).SetTLSConfig(p.tls.Clone()).SetClientID(fmt.Sprintf("anonymous-%d", time.Now().UnixNano())).SetConnectTimeout(time.Second).SetAutoReconnect(false)
	c := mqtt.NewClient(o)
	token := c.Connect()
	if !token.WaitTimeout(2 * time.Second) {
		t.Fatal("anonymous connect unbounded")
	}
	c.Disconnect(0)
	if !errors.Is(token.Error(), packets.ConnErrors[packets.ErrRefusedNotAuthorised]) {
		t.Fatal("anonymous not explicitly rejected")
	}
	t.Log("production adapter phase completed: " + f.Phase)
}

func integrationClient(t *testing.T, p *Probe, user, password string) mqtt.Client {
	t.Helper()
	o := mqtt.NewClientOptions().AddBroker(p.broker).SetTLSConfig(p.tls.Clone()).SetUsername(user).SetPassword(password).SetClientID(fmt.Sprintf("acl-%d", time.Now().UnixNano())).SetCleanSession(true).SetAutoReconnect(false).SetConnectRetry(false).SetConnectTimeout(2 * time.Second).SetWriteTimeout(2 * time.Second)
	c := mqtt.NewClient(o)
	integrationToken(t, c.Connect())
	t.Cleanup(func() { c.Disconnect(0) })
	return c
}
func integrationToken(t *testing.T, token mqtt.Token) {
	t.Helper()
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		t.Fatal("bounded MQTT operation failed")
	}
}

func integrationACL(t *testing.T, p *Probe, f integrationFixture, password string) {
	t.Helper()
	// Fresh clean sessions, nonretained unique payloads; prove DELIVERY, not ACKs.
	// A positive observer barrier precedes each negative publication. The 500ms
	// local absence window is at least four measured barrier round trips; fail
	// closed rather than assert absence on an overloaded/slower test network.
	check := func(subUser, subPass, topic, pubUser, pubPass, barrier string, allowed bool) {
		t.Helper()
		sub := integrationClient(t, p, subUser, subPass)
		defer sub.Disconnect(0)
		messages := make(chan string, 16)
		receive := func(_ mqtt.Client, m mqtt.Message) {
			select {
			case messages <- string(m.Payload()):
			default:
			}
		}
		integrationToken(t, sub.Subscribe(topic, 1, receive))
		if !allowed {
			integrationToken(t, sub.Subscribe(barrier, 1, receive))
			backend := integrationClient(t, p, "backend_service", f.Backend)
			defer backend.Disconnect(0)
			start := time.Now()
			integrationToken(t, backend.Publish(barrier, 1, false, "barrier"))
			select {
			case got := <-messages:
				if got != "barrier" {
					t.Fatal("invalid observer barrier")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("observer barrier absent")
			}
			if time.Since(start) >= 125*time.Millisecond {
				t.Fatal("observer too slow for negative window")
			}
		}
		pub := integrationClient(t, p, pubUser, pubPass)
		defer pub.Disconnect(0)
		marker := fmt.Sprintf("marker-%d", time.Now().UnixNano())
		integrationToken(t, pub.Publish(topic, 1, false, marker))
		budget := 500 * time.Millisecond
		if allowed {
			budget = 2 * time.Second
		}
		timer := time.NewTimer(budget)
		defer timer.Stop()
		select {
		case got := <-messages:
			if !allowed || got != marker {
				t.Fatal("unauthorized or unexpected delivery")
			}
		case <-timer.C:
			if allowed {
				t.Fatal("positive ACL delivery absent")
			}
		}
	}
	prefix := func(id, kind string) string { return "gateways/" + id + "/" + kind + "/integration" }
	check("backend_service", f.Backend, prefix(f.A, "telemetry"), f.A, password, "", true)
	check(f.A, password, prefix(f.A, "commands"), "backend_service", f.Backend, "", true)
	check(f.A, password, prefix(f.A, "acks"), "backend_service", f.Backend, "", true)
	// Backend cannot publish telemetry for its own barrier; use Gateway B as the
	// authorized observer of attempted cross-Gateway publications instead.
	check(f.B, f.BPassword, prefix(f.B, "commands"), f.A, password, prefix(f.B, "acks"), false)
	check(f.A, password, prefix(f.B, "commands"), "backend_service", f.Backend, prefix(f.A, "acks"), false)
	check(f.A, password, prefix(f.B, "acks"), "backend_service", f.Backend, prefix(f.A, "commands"), false)
	check(f.A, password, prefix(f.B, "telemetry"), f.B, f.BPassword, prefix(f.A, "acks"), false)
	check(f.A, password, prefix(f.A, "commands"), f.A, password, prefix(f.A, "acks"), false)
	// Cross-Gateway telemetry publication observed by backend; its positive
	// barrier is sent by B (the backend is intentionally not allowed to publish).
	sub := integrationClient(t, p, "backend_service", f.Backend)
	defer sub.Disconnect(0)
	ch := make(chan string, 8)
	integrationToken(t, sub.Subscribe(prefix(f.B, "telemetry"), 1, func(_ mqtt.Client, m mqtt.Message) {
		select {
		case ch <- string(m.Payload()):
		default:
		}
	}))
	b := integrationClient(t, p, f.B, f.BPassword)
	defer b.Disconnect(0)
	start := time.Now()
	integrationToken(t, b.Publish(prefix(f.B, "telemetry"), 1, false, "barrier"))
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("telemetry barrier absent")
	}
	if time.Since(start) >= 125*time.Millisecond {
		t.Fatal("telemetry observer too slow for negative window")
	}
	a := integrationClient(t, p, f.A, password)
	defer a.Disconnect(0)
	integrationToken(t, a.Publish(prefix(f.B, "telemetry"), 1, false, "forbidden"))
	select {
	case <-ch:
		t.Fatal("cross Gateway telemetry delivered")
	case <-time.After(500 * time.Millisecond):
	}
}

package digitaltwin_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"sync"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/mqttcontrol"
	"iot-platform/internal/mqttcredential"
	"iot-platform/internal/mqttingress"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

func controlPhase(t *testing.T, phase string) {
	t.Helper()
	if err := os.WriteFile("/tmp/task54-phase", []byte(phase), 0600); err != nil {
		t.Fatal(err)
	}
}
func awaitControl(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("native control condition timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type nativeControlDelivery struct {
	topic    string
	payload  []byte
	qos      byte
	retained bool
}

type blockedNativePublisher struct {
	transport        *mqttcontrol.Transport
	entered, release chan struct{}
	once             sync.Once
}

func (p *blockedNativePublisher) Ready() bool { return p.transport.Ready() }
func (p *blockedNativePublisher) Publish(ctx context.Context, d digitaltwin.ControlDispatch) error {
	p.once.Do(func() { close(p.entered) })
	select {
	case <-p.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return p.transport.Publish(ctx, d)
}

func controlGateway(t *testing.T, ca []byte) (mqtt.Client, <-chan nativeControlDelivery) {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca)
	packets := make(chan nativeControlDelivery, 16)
	options := mqtt.NewClientOptions().AddBroker("ssl://broker:8883").SetClientID("stage54-gateway-" + uuid.NewString()).SetUsername("stage54_gateway").SetPassword(os.Getenv("CONTROL_MQTT_GATEWAY_PASSWORD")).SetTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}).SetAutoReconnect(false).SetConnectTimeout(time.Second).SetWriteTimeout(time.Second)
	c := mqtt.NewClient(options)
	token := c.Connect()
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		t.Fatal("native Gateway CONNECT failed")
	}
	token = c.Subscribe("gateways/stage54_gateway/commands/#", 1, func(_ mqtt.Client, m mqtt.Message) {
		select {
		case packets <- nativeControlDelivery{m.Topic(), append([]byte(nil), m.Payload()...), m.Qos(), m.Retained()}:
		default:
		}
	})
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		c.Disconnect(0)
		t.Fatal("native Gateway SUBACK failed")
	}
	if sub, ok := token.(*mqtt.SubscribeToken); !ok || sub.Result()["gateways/stage54_gateway/commands/#"] != 1 {
		c.Disconnect(0)
		t.Fatal("native Gateway required QoS 1")
	}
	t.Cleanup(func() { c.Disconnect(0) })
	return c, packets
}

func TestNativeMQTTControlDurableLifecycle(t *testing.T) {
	if os.Getenv("CONTROL_MQTT_ISOLATED") != "1" {
		t.Skip("requires owned isolated DB+Mosquitto fixture")
	}
	f := newControlFixtureGateway(t, "stage54_gateway")
	r, err := f.repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.1", "0", "broker-offline"))
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.MustParse(*r.CommandID)
	ca, err := os.ReadFile("/tmp/task54-ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	management := mqttcredential.DynSecConfig{BrokerURL: "ssl://broker:8883", CAPEM: ca, ManagerUsername: "bootstrap_manager", ManagerPassword: os.Getenv("CONTROL_MQTT_MANAGER_PASSWORD"), ProtectedUsernames: []string{"bootstrap_backend"}, Timeout: time.Second, MaxPayloadBytes: 65536, MaxInflight: 1, QueueSize: 8, ValidateTarget: func(string) bool { return false }, ValidateRole: func(string, string) bool { return false }}
	options := mqttingress.TransportOptions{BrokerURL: "ssl://broker:8883", ClientID: "stage54-data", Username: "bootstrap_backend", Password: os.Getenv("CONTROL_MQTT_BACKEND_PASSWORD"), CAFile: "/tmp/task54-ca.crt", ConnectTimeout: time.Second, SubscribeTimeout: time.Second, PublishTimeout: time.Second, ReconnectMaxInterval: 100 * time.Millisecond}
	options.VerifyConnection = func(ctx context.Context) error {
		c, e := mqttcredential.NewDynSecClient(ctx, management)
		if e != nil {
			return e
		}
		defer c.Close()
		return c.VerifyBackendACL(ctx, "bootstrap_backend", "stage54-data")
	}
	cfg := mqttcontrol.DefaultConfig()
	cfg.ScanInterval = 20 * time.Millisecond
	cfg.ExpireInterval = 20 * time.Millisecond
	cfg.RetryBackoff = 50 * time.Millisecond
	cfg.PublishTimeout = time.Second
	tr, err := mqttcontrol.NewTransport(options, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = tr.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		tr.StopIntake()
		if e := tr.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	time.Sleep(150 * time.Millisecond)
	if tr.Ready() {
		t.Fatal("offline broker ready")
	}
	controlPhase(t, "intent_committed_offline")
	awaitControl(t, tr.Ready)
	_, packets := controlGateway(t, ca)
	// Targeted real mark failure: every MQTT send/PUBACK is real, no fake broker.
	f.exec(t, `CREATE FUNCTION stage54_puback_mark_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.command_id='`+id.String()+`'::uuid AND NEW.status='published' THEN RAISE EXCEPTION 'isolated mark failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER stage54_puback_mark_failure BEFORE UPDATE ON twin_commands FOR EACH ROW EXECUTE FUNCTION stage54_puback_mark_failure()`)
	blocked := &blockedNativePublisher{transport: tr, entered: make(chan struct{}), release: make(chan struct{})}
	workers, err := mqttcontrol.New(cfg, f.repo, blocked)
	if err != nil {
		t.Fatal(err)
	}
	if err = workers.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("publisher not entered")
	}
	locked, err := f.admin.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = locked.Exec(f.ctx, `SELECT sensor_id FROM sensors WHERE gateway_id=$1 AND sensor_id='s1' FOR UPDATE NOWAIT`, f.gw)
	if err == nil {
		_, err = locked.Exec(f.ctx, `SELECT entity_id FROM twin_states WHERE entity_id=$1 FOR UPDATE NOWAIT`, f.entity)
	}
	_ = locked.Rollback(f.ctx)
	if err != nil {
		t.Fatal("SQL transaction retained across blocked network invocation", err)
	}
	close(blocked.release)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if e := workers.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	receive := func() nativeControlDelivery {
		select {
		case p := <-packets:
			return p
		case <-time.After(5 * time.Second):
			t.Fatal("native command delivery missing")
			return nativeControlDelivery{}
		}
	}
	first, second := receive(), receive()
	if first.topic != "gateways/stage54_gateway/commands/"+id.String() || first.topic != second.topic || !bytes.Equal(first.payload, second.payload) || first.qos != 1 || second.qos != 1 || first.retained || second.retained {
		t.Fatal("native stable bytes/ID/QoS/retain mismatch")
	}
	var status string
	if err = f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id).Scan(&status); err != nil || status != "pending" {
		t.Fatalf("mark failure lost pending %s %v", status, err)
	}
	// The blocked-invocation assertion above tests lock release at the exact
	// network boundary, not during a legitimate concurrent DB mark/expiry.
	f.exec(t, `DROP TRIGGER stage54_puback_mark_failure ON twin_commands; DROP FUNCTION stage54_puback_mark_failure()`)
	awaitControl(t, func() bool {
		return f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id).Scan(&status) == nil && status == "published"
	})
	workers.Stop()
	ctx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	if err = workers.Close(ctx); err != nil {
		t.Fatal(err)
	}
	stopCancel()
	// Resubscribing after the publisher stopped must not replay retained commands.
	observer, fresh := controlGateway(t, ca)
	select {
	case <-fresh:
		t.Fatal("command was retained")
	case <-time.After(150 * time.Millisecond):
	}
	observer.Disconnect(0)
	// Measured application payload boundary through the real broker. Whitespace
	// changes no validated fields; this is a transport probe, not an outbox rewrite.
	dispatch, err := f.repo.PrepareControl(f.ctx, id)
	if err != nil || dispatch != nil {
		t.Fatal("published command remained dispatchable", err)
	}
	padded := append(append([]byte(nil), first.payload...), bytes.Repeat([]byte(" "), digitaltwin.MaxControlPayloadBytes-len(first.payload))...)
	probe := digitaltwin.ControlDispatch{CommandID: id, Topic: first.topic, Payload: padded}
	if err = tr.Publish(f.ctx, probe); err != nil {
		t.Fatal("8 KiB native payload", err)
	}
	for {
		packet := receive()
		if len(packet.payload) == digitaltwin.MaxControlPayloadBytes {
			if !bytes.Equal(packet.payload, padded) || packet.qos != 1 {
				t.Fatal("native cap bytes/QoS")
			}
			break
		}
	}
	probe.Payload = append(padded, ' ')
	if tr.Publish(f.ctx, probe) == nil {
		t.Fatal("8 KiB+1 payload accepted")
	}
	// Simulate a result after real PUBACK but before mark; never call result ingress.
	_, _ = f.sensor(t, "early")
	early, err := f.repo.Admit(f.ctx, f.owner, "urn:ngsi-ld:Sensor:stage54_gateway:early", controlRequest(t, "1", "0", "early"))
	if err != nil {
		t.Fatal(err)
	}
	earlyID := uuid.MustParse(*early.CommandID)
	d, err := f.repo.PrepareControl(f.ctx, earlyID)
	if err != nil || d == nil {
		t.Fatal("early prepare", err)
	}
	if err = tr.Publish(f.ctx, *d); err != nil {
		t.Fatal(err)
	}
	f.exec(t, `UPDATE twin_commands SET status='succeeded',completed_at=clock_timestamp() WHERE command_id=$1`, earlyID)
	if err = f.repo.MarkControlPublished(f.ctx, earlyID); err != nil {
		t.Fatal(err)
	}
	if err = f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, earlyID).Scan(&status); err != nil || status != "succeeded" {
		t.Fatal("early result regressed", status, err)
	}
	// Parent mutates only the owned fixture, restarts it with an extra grant.
	controlPhase(t, "restart_with_extra_acl")
	awaitControl(t, func() bool { return !tr.Ready() })
	awaitControl(t, func() bool { b, _ := os.ReadFile("/tmp/task54-action"); return string(b) == "extra_acl_live" })
	time.Sleep(350 * time.Millisecond)
	if tr.Ready() {
		t.Fatal("reconnect bypassed ACL mismatch")
	}
	controlPhase(t, "extra_acl_rejected")
	awaitControl(t, tr.Ready)
	if snap := tr.Snapshot(); snap.Connected < 2 || snap.ConnectionLost < 1 {
		t.Fatalf("native reconnect evidence %+v", snap)
	}
	controlPhase(t, "reconnected_verified")
	// Missing command permission must also fail on the next fresh connection.
	awaitControl(t, func() bool { return !tr.Ready() })
	awaitControl(t, func() bool { b, _ := os.ReadFile("/tmp/task54-action"); return string(b) == "missing_acl_live" })
	time.Sleep(350 * time.Millisecond)
	if tr.Ready() {
		t.Fatal("missing ACL ready")
	}
	controlPhase(t, "missing_acl_rejected")
	awaitControl(t, tr.Ready)
	deadline := time.Now().Add(time.Second)
	shutdown, cancelShutdown := context.WithDeadline(context.Background(), deadline)
	defer cancelShutdown()
	tr.StopIntake()
	workers.Stop()
	if err = tr.Close(shutdown); err != nil {
		t.Fatal(err)
	}
	if err = workers.Close(shutdown); err != nil {
		t.Fatal(err)
	}
	if tr.Ready() || workers.Ready() || time.Now().After(deadline) {
		t.Fatal("shutdown readiness/budget")
	}
	controlPhase(t, "complete")
}

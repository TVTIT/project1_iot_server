package digitaltwin_test

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/mqttcontrol"
	"iot-platform/internal/mqttcredential"
	"iot-platform/internal/mqttingress"

	"github.com/google/uuid"
)

// stage6MQTTPolicy keeps the durable schedule finite and short so the native
// rehearsal observes the real retry path without a long sleep.
func stage6MQTTPolicy() digitaltwin.RetryPolicy {
	return digitaltwin.RetryPolicy{
		BaseDelay:      100 * time.Millisecond,
		CapDelay:       200 * time.Millisecond,
		MaxAttempts:    5,
		PublishTimeout: 200 * time.Millisecond,
		SafetyMargin:   100 * time.Millisecond,
	}
}

func stage6MQTTOptions(ca []byte) mqttingress.TransportOptions {
	options := mqttingress.TransportOptions{BrokerURL: "ssl://broker:8883", ClientID: "stage56-data", Username: "bootstrap_backend", Password: os.Getenv("CONTROL_MQTT_BACKEND_PASSWORD"), CAFile: "/tmp/task54-ca.crt", ConnectTimeout: time.Second, SubscribeTimeout: time.Second, PublishTimeout: time.Second, ReconnectMaxInterval: 100 * time.Millisecond}
	management := mqttcredential.DynSecConfig{BrokerURL: "ssl://broker:8883", CAPEM: ca, ManagerUsername: "bootstrap_manager", ManagerPassword: os.Getenv("CONTROL_MQTT_MANAGER_PASSWORD"), ProtectedUsernames: []string{"bootstrap_backend"}, Timeout: time.Second, MaxPayloadBytes: 65536, MaxInflight: 1, QueueSize: 8, ValidateTarget: func(string) bool { return false }, ValidateRole: func(string, string) bool { return false }}
	options.VerifyConnection = func(ctx context.Context) error {
		c, e := mqttcredential.NewDynSecClient(ctx, management)
		if e != nil {
			return e
		}
		defer c.Close()
		return c.VerifyBackendACL(ctx, "bootstrap_backend", "stage56-data")
	}
	return options
}

// TestNativeMQTTStage6RetryResilience proves the Stage 6 durable retry path
// against a real broker: a transport that is not ready consumes no grant, a
// PUBACK followed by a failed durable mark is retried with byte-identical
// payload, the persisted schedule survives a runtime reconstruction and the
// periodic scan (without wake-up) finds the due work.
func TestNativeMQTTStage6RetryResilience(t *testing.T) {
	if os.Getenv("CONTROL_MQTT_ISOLATED") != "1" {
		t.Skip("requires owned isolated DB+Mosquitto fixture")
	}
	f := newControlFixtureGatewayReuse(t, "stage54_gateway")
	ca, err := os.ReadFile("/tmp/task54-ca.crt")
	if err != nil {
		t.Fatal(err)
	}

	controlCfg := digitaltwin.DefaultControlConfig()
	controlCfg.Stage6Enabled = true
	controlCfg.RetryPolicy = stage6MQTTPolicy()
	repo, err := digitaltwin.NewPostgresControlRepository(f.app, controlCfg)
	if err != nil {
		t.Fatal(err)
	}

	dispatchCfg := mqttcontrol.DefaultConfig()
	dispatchCfg.Stage6Enabled = true
	dispatchCfg.RetryPolicy = stage6MQTTPolicy()
	dispatchCfg.ScanInterval = 20 * time.Millisecond
	dispatchCfg.ExpireInterval = 50 * time.Millisecond
	dispatchCfg.PublishTimeout = 200 * time.Millisecond
	// Wake-up stays disabled: the periodic scan is the only discovery path.
	dispatchCfg.WakeupEnabled = false

	transport, err := mqttcontrol.NewTransport(stage6MQTTOptions(ca), dispatchCfg)
	if err != nil {
		t.Fatal(err)
	}

	r, err := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.5", "0", "s6-retry"))
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.MustParse(*r.CommandID)

	// A durable mark failure after the real PUBACK: the broker saw the command,
	// the database could not record it, so the outcome stays unknown.
	f.exec(t, `CREATE FUNCTION stage56_puback_mark_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.command_id='`+id.String()+`'::uuid AND NEW.status='published' THEN RAISE EXCEPTION 'isolated stage 6 mark failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER stage56_puback_mark_failure BEFORE UPDATE ON twin_commands FOR EACH ROW EXECUTE FUNCTION stage56_puback_mark_failure()`)

	workers, err := mqttcontrol.New(dispatchCfg, repo, transport)
	if err != nil {
		t.Fatal(err)
	}
	if err = workers.Start(); err != nil {
		t.Fatal(err)
	}
	stopWorkers := func() {
		workers.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if e := workers.Close(ctx); e != nil {
			t.Error(e)
		}
	}
	t.Cleanup(stopWorkers)

	// 1. The transport is not ready: no attempt may be consumed at all.
	time.Sleep(300 * time.Millisecond)
	var attempts int
	if err = f.admin.QueryRow(f.ctx, `SELECT attempt_count FROM twin_commands WHERE command_id=$1`, id).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 {
		t.Fatalf("unready transport consumed an attempt: %d", attempts)
	}

	// 2. With the transport ready, the periodic scan dispatches the command.
	// The Gateway subscriber (SUBACK confirmed) exists before the transport can
	// become ready, so the first delivery is never lost to a clean session.
	_, packets := controlGateway(t, ca)
	if err = transport.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		transport.StopIntake()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if e := transport.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	awaitControl(t, transport.Ready)
	receive := func() nativeControlDelivery {
		select {
		case p := <-packets:
			return p
		case <-time.After(15 * time.Second):
			t.Fatal("native Stage 6 command delivery missing")
			return nativeControlDelivery{}
		}
	}
	first := receive()
	if first.topic != "gateways/stage54_gateway/commands/"+id.String() || first.qos != 1 || first.retained {
		t.Fatalf("native Stage 6 delivery envelope mismatch: topic=%s qos=%d retained=%v", first.topic, first.qos, first.retained)
	}
	firstAt := time.Now()
	parsedFirst, err := digitaltwin.ParseMQTTCommand(first.topic, first.payload, false)
	if err != nil {
		t.Fatal(err)
	}

	// 3. The failed durable mark keeps the command active and unknown.
	awaitControl(t, func() bool {
		var status string
		if f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id).Scan(&status) != nil {
			return false
		}
		return status == "pending"
	})
	if err = f.admin.QueryRow(f.ctx, `SELECT attempt_count FROM twin_commands WHERE command_id=$1`, id).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("attempt count after the first grant = %d, want 1", attempts)
	}

	// 4. Reconstruct the runtime and the repository against the same database.
	// The persisted schedule must survive and the new periodic scan must find
	// the due work without any wake-up signal.
	stopWorkers()
	for {
		select {
		case <-packets:
			continue
		default:
		}
		break
	}
	restartedRepo, err := digitaltwin.NewPostgresControlRepository(f.app, controlCfg)
	if err != nil {
		t.Fatal(err)
	}
	workers, err = mqttcontrol.New(dispatchCfg, restartedRepo, transport)
	if err != nil {
		t.Fatal(err)
	}
	if err = workers.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stopWorkers)

	second := receive()
	if second.topic != first.topic || !bytes.Equal(second.payload, first.payload) || second.qos != 1 || second.retained {
		t.Fatal("Stage 6 retry changed the durable topic, bytes, QoS or retain flag")
	}
	if gap := time.Since(firstAt); gap < 50*time.Millisecond {
		t.Fatalf("Stage 6 retry fired before the persisted schedule: %v", gap)
	}
	parsedSecond, err := digitaltwin.ParseMQTTCommand(second.topic, second.payload, false)
	if err != nil {
		t.Fatal(err)
	}
	if parsedFirst.DesiredVersion != parsedSecond.DesiredVersion || !parsedFirst.IssuedAt.Equal(parsedSecond.IssuedAt) || !parsedFirst.ExpiresAt.Equal(parsedSecond.ExpiresAt) {
		t.Fatal("Stage 6 retry changed the command version, issued time or expiry")
	}

	// 5. With the durable mark working again, the retry commits the published
	// state without inventing a success that never happened.
	time.Sleep(200 * time.Millisecond)
	var status string
	if err = f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id).Scan(&status); err != nil || status != "pending" {
		t.Fatalf("status before the recovery = %s err=%v, want pending", status, err)
	}
	f.exec(t, `DROP TRIGGER stage56_puback_mark_failure ON twin_commands; DROP FUNCTION stage56_puback_mark_failure()`)
	awaitControl(t, func() bool {
		if f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id).Scan(&status) != nil {
			return false
		}
		return status == "published"
	})
	third := receive()
	if third.topic != first.topic || !bytes.Equal(third.payload, first.payload) {
		t.Fatal("Stage 6 retry after recovery changed the durable bytes")
	}
	if err = f.admin.QueryRow(f.ctx, `SELECT attempt_count FROM twin_commands WHERE command_id=$1`, id).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts < 3 {
		t.Fatalf("attempt count after the recovery retry = %d, want at least 3", attempts)
	}
	var stopReason *string
	if err = f.admin.QueryRow(f.ctx, `SELECT retry_stop_reason FROM twin_commands WHERE command_id=$1`, id).Scan(&stopReason); err != nil {
		t.Fatal(err)
	}
	if stopReason != nil {
		t.Fatalf("retry stop reason set while retries were still running: %q", *stopReason)
	}
	// The durable payload is unchanged across every attempt.
	var storedTopic string
	var storedPayload []byte
	if err = f.admin.QueryRow(f.ctx, `SELECT topic,payload_bytes FROM twin_outbox WHERE command_id=$1`, id).Scan(&storedTopic, &storedPayload); err != nil {
		t.Fatal(err)
	}
	if storedTopic != first.topic || !bytes.Equal(storedPayload, first.payload) {
		t.Fatal("the durable outbox payload changed across attempts")
	}
}

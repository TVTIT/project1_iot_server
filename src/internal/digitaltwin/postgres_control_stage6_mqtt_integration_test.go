package digitaltwin_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gatewaysim"
	"iot-platform/internal/metrics"
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

type nativeReceiptSampler struct {
	mu          sync.Mutex
	active      int64
	sets        int
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func (s *nativeReceiptSampler) releaseApply() {
	s.releaseOnce.Do(func() { close(s.release) })
}

func (s *nativeReceiptSampler) SetSampleInterval(ctx context.Context, interval int64) error {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	s.active = interval
	s.sets++
	s.mu.Unlock()
	return nil
}

func (s *nativeReceiptSampler) ActiveSampleInterval() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

type nativeReceiptACK struct{}

func (nativeReceiptACK) Admit(string, []byte, bool) {}

type gatewayResponsePublisher interface {
	Ready() bool
	ReadyCh() <-chan struct{}
	Publish(context.Context, string, []byte) error
	FatalSignal() <-chan struct{}
	Fatal() error
}

// dropOneGatewayResultPublisher loses one command's result during its original
// TTL while leaving receipt, snapshot, and every other command result intact.
type dropOneGatewayResultPublisher struct {
	upstream  gatewayResponsePublisher
	commandID string
	mu        sync.Mutex
	dropped   bool
	allowLate bool
	payload   []byte
}

func (p *dropOneGatewayResultPublisher) Ready() bool              { return p.upstream.Ready() }
func (p *dropOneGatewayResultPublisher) ReadyCh() <-chan struct{} { return p.upstream.ReadyCh() }
func (p *dropOneGatewayResultPublisher) FatalSignal() <-chan struct{} {
	return p.upstream.FatalSignal()
}
func (p *dropOneGatewayResultPublisher) Fatal() error { return p.upstream.Fatal() }

func (p *dropOneGatewayResultPublisher) Publish(ctx context.Context, topic string, payload []byte) error {
	var envelope struct {
		MessageType string `json:"message_type"`
	}
	if topic == "gateways/stage54_gateway/responses/"+p.commandID &&
		json.Unmarshal(payload, &envelope) == nil && envelope.MessageType == "command_result" {
		p.mu.Lock()
		if !p.allowLate {
			if !p.dropped {
				p.dropped = true
				p.payload = append([]byte(nil), payload...)
			}
			p.mu.Unlock()
			// Drop only this command's result during its immutable TTL window.
			return nil
		}
		p.mu.Unlock()
	}
	return p.upstream.Publish(ctx, topic, payload)
}

func (p *dropOneGatewayResultPublisher) droppedResult() ([]byte, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.payload...), p.dropped
}

func (p *dropOneGatewayResultPublisher) allowLateReplay() {
	p.mu.Lock()
	p.allowLate = true
	p.mu.Unlock()
}

func awaitNativeResponse(t *testing.T, responses <-chan mqttcontrol.RawResult, commandID, wantType string, timeout time.Duration) mqttcontrol.RawResult {
	t.Helper()
	topic := "gateways/stage54_gateway/responses/" + commandID
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case raw, ok := <-responses:
			if !ok {
				t.Fatal("native response observation queue closed")
			}
			if raw.Topic != topic {
				continue
			}
			if raw.Retained {
				t.Fatal("native response was unexpectedly retained")
			}
			switch wantType {
			case "command_receipt":
				receipt, err := digitaltwin.ParseMQTTReceipt(raw.Topic, raw.Payload, false)
				if err == nil && receipt.CommandID == commandID {
					return raw
				}
			case "command_result":
				result, err := digitaltwin.ParseMQTTResult(raw.Topic, raw.Payload, false)
				if err == nil && result.CommandID == commandID {
					return raw
				}
			default:
				t.Fatalf("unsupported expected response type %q", wantType)
			}
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("native %s response for target command not observed within %s", wantType, timeout)
	return mqttcontrol.RawResult{}
}

func captureAfterReconciliation(
	repo *digitaltwin.PostgresControlRepository,
	candidates chan<- mqttcontrol.ControlSnapshotCandidate,
	metrics *mqttcontrol.ReconciliationMetrics,
) mqttcontrol.ControlSnapshotHandler {
	reconcile := mqttcontrol.NewReconciliationSnapshotHandler(repo, metrics)
	return func(ctx context.Context, candidate mqttcontrol.ControlSnapshotCandidate) {
		reconcile(ctx, candidate)
		candidates <- candidate
	}
}

// TestNativeMQTTStage6GatewayQueryAndReceiptRoundTrip composes the actual Stage 6
// backend server consumer with the real Gateway Controller/ControlRuntime,
// Gateway SQLite store, TLS MQTT transport and owned PostgreSQL fixture.
func TestNativeMQTTStage6GatewayQueryAndReceiptRoundTrip(t *testing.T) {
	if os.Getenv("CONTROL_MQTT_ISOLATED") != "1" {
		t.Skip("requires owned isolated DB+Mosquitto fixture and Stage 6 server")
	}
	sensorID := "r53_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	f := newControlFixtureGatewayReuseSensor(t, "stage54_gateway", sensorID)
	controlCfg := digitaltwin.DefaultControlConfig()
	controlCfg.Stage6Enabled = true
	controlCfg.RetryPolicy = stage6MQTTPolicy()
	repo, err := digitaltwin.NewPostgresControlRepository(f.app, controlCfg)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := os.ReadFile("/tmp/task54-ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	gatewayDB := filepath.Join(t.TempDir(), "stage6-gateway.db")
	env := map[string]string{
		"GATEWAY_ID": "stage54_gateway", "SENSOR_ID": sensorID,
		"GATEWAY_MQTT_BROKER_URL": "ssl://broker:8883",
		"GATEWAY_MQTT_CLIENT_ID":  "stage6-receipt-gateway-" + uuid.NewString(),
		"GATEWAY_MQTT_USERNAME":   "stage54_gateway",
		"GATEWAY_MQTT_PASSWORD":   os.Getenv("CONTROL_MQTT_GATEWAY_PASSWORD"),
		"GATEWAY_MQTT_CA_FILE":    "/tmp/task54-ca.crt",
		"GATEWAY_OUTBOX_PATH":     gatewayDB,
		"GATEWAY_CONTROL_ENABLED": "true",
		"GATEWAY_STAGE6_ENABLED":  "true",
	}
	gatewayCfg, err := gatewaysim.Load(func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	})
	if err != nil {
		t.Fatal("load isolated Gateway configuration:", err)
	}
	out, err := gatewaysim.OpenOutbox(gatewaysim.OutboxConfig{
		Path: gatewayCfg.OutboxPath, GatewayID: gatewayCfg.GatewayID, SensorID: gatewayCfg.SensorID,
		MaxRows: gatewayCfg.OutboxMaxRows, MaxBytes: gatewayCfg.OutboxMaxBytes, OpTimeout: gatewayCfg.SQLiteTimeout,
	})
	if err != nil {
		t.Fatal("open isolated Gateway SQLite store:", err)
	}
	t.Cleanup(func() {
		if err := out.Close(); err != nil {
			t.Error(err)
		}
	})
	inbox := gatewaysim.NewCommandInbox(gatewayCfg.GatewayID, nativeReceiptACK{})
	sampler := &nativeReceiptSampler{active: 10000, entered: make(chan struct{}), release: make(chan struct{})}
	controller, err := gatewaysim.NewController(out, sampler, true)
	if err != nil {
		t.Fatal(err)
	}
	gatewayTransport, err := gatewaysim.NewTransport(gatewaysim.TransportOptionsFromConfig(gatewayCfg), inbox)
	if err != nil {
		t.Fatal(err)
	}
	controlRuntime, err := gatewaysim.NewControlRuntime(controller, inbox, gatewayTransport, gatewayCfg.PublishTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		sampler.releaseApply()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := controlRuntime.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := gatewayTransport.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := controlRuntime.Start(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := gatewayTransport.Start(); err != nil {
		t.Fatal(err)
	}
	awaitControl(t, gatewayTransport.Ready)

	duplicateCfg := mqttcontrol.DefaultConfig()
	duplicateCfg.Stage6Enabled = true
	duplicateCfg.RetryPolicy = stage6MQTTPolicy()
	duplicateOptions := stage6MQTTOptions(ca)
	duplicateOptions.ClientID = "stage6-receipt-duplicate-" + uuid.NewString()
	duplicateTransport, err := mqttcontrol.NewTransport(duplicateOptions, duplicateCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		duplicateTransport.StopIntake()
		if err := duplicateTransport.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := duplicateTransport.Start(); err != nil {
		t.Fatal(err)
	}
	awaitControl(t, duplicateTransport.Ready)

	admission, err := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.5", "0", "native-r53-receipt"))
	if err != nil {
		t.Fatal("admit Stage 6 command:", err)
	}
	id := uuid.MustParse(*admission.CommandID)
	var topic string
	var commandBytes []byte
	if err := f.admin.QueryRow(f.ctx, `SELECT topic,payload_bytes FROM twin_outbox WHERE command_id=$1`, id).Scan(&topic, &commandBytes); err != nil {
		t.Fatal("read admitted immutable command bytes:", err)
	}
	var configurationBefore sql.NullString
	if err := f.admin.QueryRow(f.ctx, `SELECT reported_state->'configuration'->>'sample_interval_us' FROM twin_states WHERE entity_id=$1`, f.entity).Scan(&configurationBefore); err != nil {
		t.Fatal(err)
	}

	select {
	case <-sampler.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("Gateway sampler was not held at apply barrier")
	}
	localEvidence, err := out.ControlReceipts(f.ctx)
	if err != nil || len(localEvidence) != 1 || len(localEvidence[0].ReceiptBytes) == 0 || len(localEvidence[0].Result) != 0 {
		t.Fatalf("Gateway receipt not durable before held apply: count=%d err=%v", len(localEvidence), err)
	}
	receiptBytes := append([]byte(nil), localEvidence[0].ReceiptBytes...)
	firstReceipt := awaitNativeResponse(t, duplicateTransport.Results(), id.String(), "command_receipt", 5*time.Second)
	if !bytes.Equal(firstReceipt.Payload, receiptBytes) {
		t.Fatal("first native receipt did not match Gateway's durable SQLite bytes")
	}
	awaitControl(t, func() bool {
		var status string
		return f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id).Scan(&status) == nil &&
			status == string(digitaltwin.CommandAcknowledged)
	})
	var heldReported sql.NullString
	if err := f.admin.QueryRow(f.ctx, `SELECT reported_state->'configuration'->>'sample_interval_us' FROM twin_states WHERE entity_id=$1`, f.entity).Scan(&heldReported); err != nil {
		t.Fatal(err)
	}
	if heldReported != configurationBefore {
		t.Fatalf("receipt changed reported configuration before execution result: before=%v held=%v", configurationBefore, heldReported)
	}
	if err := repo.MarkControlPublished(f.ctx, id); err != nil {
		t.Fatal("late broker PUBACK mark:", err)
	}
	var statusAfterMark string
	if err := f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id).Scan(&statusAfterMark); err != nil || statusAfterMark != string(digitaltwin.CommandAcknowledged) {
		t.Fatalf("PUBACK mark regressed acknowledged: status=%s err=%v", statusAfterMark, err)
	}

	sampler.releaseApply()
	firstResult := awaitNativeResponse(t, duplicateTransport.Results(), id.String(), "command_result", 5*time.Second)
	awaitControl(t, func() bool {
		var status string
		return f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id).Scan(&status) == nil &&
			status == string(digitaltwin.CommandSucceeded)
	})
	var reported sql.NullString
	if err := f.admin.QueryRow(f.ctx, `SELECT reported_state->'configuration'->>'sample_interval_us' FROM twin_states WHERE entity_id=$1`, f.entity).Scan(&reported); err != nil {
		t.Fatal(err)
	}
	if !reported.Valid || reported.String != "2000000" {
		t.Fatalf("successful result did not commit reported configuration: %v", reported)
	}
	localEvidence, err = out.ControlReceipts(f.ctx)
	if err != nil || len(localEvidence) != 1 || !bytes.Equal(localEvidence[0].ReceiptBytes, receiptBytes) ||
		!bytes.Equal(localEvidence[0].Result, firstResult.Payload) {
		t.Fatalf("Gateway did not retain exact receipt/result bytes: count=%d err=%v", len(localEvidence), err)
	}

	// Republish the exact original command through an authorized backend data
	// transport. The Gateway's completed duplicate path must only resend its
	// stored receipt/result and must not call the sampler again.
	dispatch := digitaltwin.ControlDispatch{CommandID: id, Topic: topic, Payload: commandBytes}
	if err := duplicateTransport.Publish(f.ctx, dispatch); err != nil {
		t.Fatal("republish exact original command:", err)
	}
	duplicateReceipt := awaitNativeResponse(t, duplicateTransport.Results(), id.String(), "command_receipt", 900*time.Millisecond)
	duplicateResult := awaitNativeResponse(t, duplicateTransport.Results(), id.String(), "command_result", 900*time.Millisecond)
	if !bytes.Equal(duplicateReceipt.Payload, receiptBytes) || !bytes.Equal(duplicateResult.Payload, firstResult.Payload) {
		t.Fatal("Gateway duplicate did not resend the canonical stored receipt/result bytes")
	}
	sampler.mu.Lock()
	sets := sampler.sets
	sampler.mu.Unlock()
	if sets != 1 {
		t.Fatalf("ordinary duplicate reapplied sampler %d times", sets)
	}
	var finalStatus string
	if err := f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id).Scan(&finalStatus); err != nil || finalStatus != string(digitaltwin.CommandSucceeded) {
		t.Fatalf("duplicate response changed terminal status: %s/%v", finalStatus, err)
	}

	// Run the real bounded snapshot consumer and query correlator against the
	// same TLS transport, Gateway runtime and PostgreSQL repository.
	queryMetrics := &mqttcontrol.ControlQueryMetrics{}
	candidates := make(chan mqttcontrol.ControlSnapshotCandidate, 1)
	queryRuntime, err := mqttcontrol.NewControlQueryRuntime(repo, duplicateTransport,
		captureAfterReconciliation(repo, candidates, nil), queryMetrics)
	if err != nil {
		t.Fatal("create native query runtime:", err)
	}
	var queryRuntimeMu sync.RWMutex
	consumer, err := mqttcontrol.NewResultConsumer(1, time.Second, repo, duplicateTransport.Results(), nil)
	if err != nil {
		t.Fatal("create native bounded response consumer:", err)
	}
	if err := consumer.ConfigureSnapshots(true, func(ctx context.Context, raw mqttcontrol.RawResult) {
		queryRuntimeMu.RLock()
		runtime := queryRuntime
		queryRuntimeMu.RUnlock()
		runtime.HandleSnapshot(ctx, raw)
	}); err != nil {
		t.Fatal("configure strict snapshot consumer:", err)
	}
	if err := consumer.Start(); err != nil {
		t.Fatal("start strict snapshot consumer:", err)
	}
	t.Cleanup(func() {
		consumer.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := consumer.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := queryRuntime.Start(); err != nil {
		t.Fatal("start native periodic query runtime:", err)
	}
	t.Cleanup(func() {
		queryRuntime.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := queryRuntime.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	var beforeVersion string
	var beforeDesired, beforeReported string
	var beforeSchedule sql.NullTime
	var beforeDecision sql.NullString
	var beforeReconciledAt sql.NullTime
	if err := f.admin.QueryRow(f.ctx, `SELECT desired_version::text,desired_state::text,reported_state::text,next_query_at,
		last_reconcile_decision,last_reconciled_at FROM twin_states WHERE entity_id=$1`, f.entity).
		Scan(&beforeVersion, &beforeDesired, &beforeReported, &beforeSchedule, &beforeDecision, &beforeReconciledAt); err != nil {
		t.Fatal("capture pre-query durable state:", err)
	}
	var commandCount int
	if err := f.admin.QueryRow(f.ctx, `SELECT count(*) FROM twin_commands WHERE entity_id=$1`, f.entity).Scan(&commandCount); err != nil {
		t.Fatal(err)
	}
	beforeControl := f.counts(t)
	controlRows := func() string {
		var rows string
		if err := f.admin.QueryRow(f.ctx, `SELECT jsonb_build_array(
			COALESCE((SELECT jsonb_agg(to_jsonb(c) ORDER BY c.command_id) FROM twin_commands c WHERE c.entity_id=$1),'[]'::jsonb),
			COALESCE((SELECT jsonb_agg(to_jsonb(o) ORDER BY o.command_id) FROM twin_outbox o
				JOIN twin_commands c USING(command_id) WHERE c.entity_id=$1),'[]'::jsonb),
			COALESCE((SELECT jsonb_agg(to_jsonb(r) ORDER BY r.actor_id,r.entity_id,r.idempotency_key)
				FROM twin_command_requests r WHERE r.entity_id=$1),'[]'::jsonb)
		)::text`, f.entity).Scan(&rows); err != nil {
			t.Fatal("capture command/outbox/receipt rows:", err)
		}
		return rows
	}
	beforeControlRows := controlRows()
	var telemetryBefore int64
	if err := f.admin.QueryRow(f.ctx, `SELECT count(*) FROM telemetry WHERE gateway_id=$1 AND sensor_id=$2`, f.gw, sensorID).Scan(&telemetryBefore); err != nil {
		t.Fatal("capture pre-query telemetry:", err)
	}
	evidenceBefore, err := out.ControlReceipts(f.ctx)
	if err != nil {
		t.Fatal("capture pre-query Gateway receipts:", err)
	}
	var firstCandidate mqttcontrol.ControlSnapshotCandidate
	select {
	case firstCandidate = <-candidates:
		if firstCandidate.Query.EntityID != f.entity || firstCandidate.Query.EntityURN != f.urn ||
			firstCandidate.Snapshot.GatewayID != "stage54_gateway" || firstCandidate.Snapshot.EntityID != f.urn ||
			firstCandidate.Snapshot.ConfigurationState != "configured" || firstCandidate.Snapshot.DurableSampleIntervalUS == nil ||
			*firstCandidate.Snapshot.DurableSampleIntervalUS != 2_000_000 || firstCandidate.Snapshot.ActualSampleIntervalUS == nil ||
			*firstCandidate.Snapshot.ActualSampleIntervalUS != 2_000_000 || firstCandidate.LocalDeadline.IsZero() {
			t.Fatalf("native query produced incorrect Gateway readback candidate: %+v", firstCandidate)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("native configuration query produced no correlated snapshot; metrics=%+v", queryMetrics.Snapshot())
	}
	var afterVersion string
	var afterDesired, afterReported string
	var afterSchedule sql.NullTime
	var afterDecision sql.NullString
	var afterReconciledAt sql.NullTime
	if err := f.admin.QueryRow(f.ctx, `SELECT desired_version::text,desired_state::text,reported_state::text,next_query_at,
		last_reconcile_decision,last_reconciled_at FROM twin_states WHERE entity_id=$1`, f.entity).
		Scan(&afterVersion, &afterDesired, &afterReported, &afterSchedule, &afterDecision, &afterReconciledAt); err != nil {
		t.Fatal("capture post-query durable state:", err)
	}
	var commandsAfter int
	if err := f.admin.QueryRow(f.ctx, `SELECT count(*) FROM twin_commands WHERE entity_id=$1`, f.entity).Scan(&commandsAfter); err != nil {
		t.Fatal(err)
	}
	var telemetryAfter int64
	if err := f.admin.QueryRow(f.ctx, `SELECT count(*) FROM telemetry WHERE gateway_id=$1 AND sensor_id=$2`, f.gw, sensorID).Scan(&telemetryAfter); err != nil {
		t.Fatal("capture post-query telemetry:", err)
	}
	evidenceAfter, err := out.ControlReceipts(f.ctx)
	if err != nil {
		t.Fatal("capture post-query Gateway receipts:", err)
	}
	if beforeVersion != afterVersion || beforeDesired != afterDesired || beforeReported != afterReported ||
		commandCount != commandsAfter || beforeControl != f.counts(t) || beforeControlRows != controlRows() ||
		telemetryBefore != telemetryAfter ||
		!bytes.Equal(evidenceBefore[0].ReceiptBytes, evidenceAfter[0].ReceiptBytes) ||
		!bytes.Equal(evidenceBefore[0].Result, evidenceAfter[0].Result) ||
		!afterDecision.Valid || afterDecision.String != string(digitaltwin.ReconciliationConverged) ||
		!afterReconciledAt.Valid ||
		!afterSchedule.Valid || (beforeSchedule.Valid && !afterSchedule.Time.After(beforeSchedule.Time)) {
		t.Fatalf("query/snapshot changed more than schedule/reconciliation metadata: version %s/%s desired=%t reported=%t commands=%d/%d schedule=%v/%v decision=%v/%v",
			beforeVersion, afterVersion, beforeDesired == afterDesired, beforeReported == afterReported,
			commandCount, commandsAfter, beforeSchedule, afterSchedule, beforeDecision, afterDecision)
	}
	if queryMetrics.Snapshot().Candidate != 1 {
		t.Fatalf("expected one correlated native snapshot, metrics=%+v", queryMetrics.Snapshot())
	}

	// Take the Gateway transport offline, reconstruct the backend runtime, and
	// let one real periodic query expire without a response. Reconnect the
	// Gateway and prove a later periodic scan succeeds without presence events.
	queryRuntime.Stop()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := queryRuntime.Close(closeCtx); err != nil {
		closeCancel()
		t.Fatal("close first backend query runtime:", err)
	}
	closeCancel()
	gatewayCtx, gatewayCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := controlRuntime.Close(gatewayCtx); err != nil {
		gatewayCancel()
		t.Fatal("stop Gateway control runtime for offline interval:", err)
	}
	if err := gatewayTransport.Stop(gatewayCtx); err != nil {
		gatewayCancel()
		t.Fatal("disconnect Gateway transport for offline interval:", err)
	}
	gatewayCancel()
	if _, err := f.admin.Exec(f.ctx, `UPDATE twin_states SET next_query_at=clock_timestamp() WHERE entity_id=$1`, f.entity); err != nil {
		t.Fatal("make durable query schedule due for offline rehearsal:", err)
	}
	reconstructedRuntime, err := mqttcontrol.NewControlQueryRuntime(repo, duplicateTransport,
		captureAfterReconciliation(repo, candidates, nil), queryMetrics)
	if err != nil {
		t.Fatal("reconstruct backend query runtime:", err)
	}
	queryRuntimeMu.Lock()
	queryRuntime = reconstructedRuntime
	queryRuntimeMu.Unlock()
	if err := queryRuntime.Start(); err != nil {
		t.Fatal("start backend query runtime while Gateway is offline:", err)
	}
	expiryPoll := time.NewTicker(100 * time.Millisecond)
	expiryDeadline := time.NewTimer(18 * time.Second)
	defer expiryPoll.Stop()
	defer expiryDeadline.Stop()
	for queryMetrics.Snapshot().Timeout == 0 {
		select {
		case <-expiryPoll.C:
		case <-expiryDeadline.C:
			t.Fatalf("offline query did not expire in bounded runtime cleanup: metrics=%+v", queryMetrics.Snapshot())
		}
	}
	queryRuntime.Stop()
	closeCtx, closeCancel = context.WithTimeout(context.Background(), 3*time.Second)
	if err := queryRuntime.Close(closeCtx); err != nil {
		closeCancel()
		t.Fatal("close offline backend query runtime:", err)
	}
	closeCancel()

	inbox = gatewaysim.NewCommandInbox(gatewayCfg.GatewayID, nativeReceiptACK{})
	controller, err = gatewaysim.NewController(out, sampler, true)
	if err != nil {
		t.Fatal("reconstruct Gateway controller after offline interval:", err)
	}
	gatewayTransport, err = gatewaysim.NewTransport(gatewaysim.TransportOptionsFromConfig(gatewayCfg), inbox)
	if err != nil {
		t.Fatal("reconstruct Gateway TLS transport:", err)
	}
	controlRuntime, err = gatewaysim.NewControlRuntime(controller, inbox, gatewayTransport, gatewayCfg.PublishTimeout)
	if err != nil {
		t.Fatal("reconstruct Gateway control runtime:", err)
	}
	if err := controlRuntime.Start(f.ctx); err != nil {
		t.Fatal("start reconstructed Gateway control runtime:", err)
	}
	if err := gatewayTransport.Start(); err != nil {
		t.Fatal("reconnect Gateway TLS transport:", err)
	}
	awaitControl(t, gatewayTransport.Ready)
	if _, err := f.admin.Exec(f.ctx, `UPDATE twin_states SET next_query_at=clock_timestamp() WHERE entity_id=$1`, f.entity); err != nil {
		t.Fatal("make due Sensor discoverable after Gateway reconnect:", err)
	}

	// This prior-lifetime response is a valid old Gateway snapshot envelope but
	// has no correlation in the reconstructed backend runtime.
	stale := firstCandidate.Snapshot
	stalePayload, err := digitaltwin.MarshalMQTTConfigurationSnapshot(stale)
	if err != nil {
		t.Fatal("serialize prior-lifetime snapshot:", err)
	}
	reconnectedRuntime, err := mqttcontrol.NewControlQueryRuntime(repo, duplicateTransport,
		captureAfterReconciliation(repo, candidates, nil), queryMetrics)
	if err != nil {
		t.Fatal("reconstruct backend query runtime after Gateway reconnect:", err)
	}
	queryRuntimeMu.Lock()
	queryRuntime = reconnectedRuntime
	queryRuntimeMu.Unlock()
	invalidBefore := queryMetrics.Snapshot().Invalid
	queryRuntime.HandleSnapshot(context.Background(), mqttcontrol.RawResult{
		Topic: "gateways/stage54_gateway/responses/" + stale.RequestID, Payload: stalePayload,
		ReceivedAt: time.Now(),
	})
	if queryMetrics.Snapshot().Invalid != invalidBefore+1 {
		t.Fatal("prior-lifetime request ID was not rejected after backend reconstruction")
	}
	if err := queryRuntime.Start(); err != nil {
		t.Fatal("start periodic query scan after Gateway reconnect:", err)
	}
	select {
	case candidate := <-candidates:
		if candidate.Snapshot.RequestID == firstCandidate.Snapshot.RequestID {
			t.Fatal("reconstructed backend reused a prior-lifetime query request ID")
		}
		if candidate.Query.EntityURN != f.urn || candidate.Snapshot.ActualSampleIntervalUS == nil ||
			*candidate.Snapshot.ActualSampleIntervalUS != 2_000_000 {
			t.Fatalf("fresh periodic query after runtime reconstruction was invalid: %+v", candidate)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("fresh periodic query after runtime reconstruction missing; metrics=%+v", queryMetrics.Snapshot())
	}

	// R7.3: let a real new user command reach/apply at the Gateway, but drop
	// command_result publishes through its TTL. The durable SQLite result and
	// immutable command remain available for late-result replay.
	queryRuntime.Stop()
	queryCloseCtx, queryCloseCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := queryRuntime.Close(queryCloseCtx); err != nil {
		queryCloseCancel()
		t.Fatal("stop pre-convergence query runtime:", err)
	}
	queryCloseCancel()
	gatewayCloseCtx, gatewayCloseCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := controlRuntime.Close(gatewayCloseCtx); err != nil {
		gatewayCloseCancel()
		t.Fatal("stop Gateway runtime before scoped result fault:", err)
	}
	if err := gatewayTransport.Stop(gatewayCloseCtx); err != nil {
		gatewayCloseCancel()
		t.Fatal("disconnect Gateway before scoped result fault:", err)
	}
	gatewayCloseCancel()

	shortControlCfg := digitaltwin.DefaultControlConfig()
	shortControlCfg.Stage6Enabled = true
	shortControlCfg.TTL = 5 * time.Second
	shortControlCfg.RetryPolicy = stage6MQTTPolicy()
	shortRepo, err := digitaltwin.NewPostgresControlRepository(f.app, shortControlCfg)
	if err != nil {
		t.Fatal("create short-TTL native Stage 6 repository:", err)
	}
	reconciliationMetrics := &mqttcontrol.ReconciliationMetrics{}
	reconciliationTimeline, err := metrics.NewControlTimeline(16, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	shortRepo.SetControlTimeline(reconciliationTimeline)
	reconciliationAdmission, err := shortRepo.Admit(f.ctx, f.owner, f.urn,
		controlRequest(t, "1", "1", "native-r73-lost-result-root"))
	if err != nil || reconciliationAdmission.StatusCode != 202 || reconciliationAdmission.CommandID == nil {
		t.Fatalf("admit real lost-result root command: receipt=%+v err=%v", reconciliationAdmission, err)
	}
	timelineBeforeSuccessor := reconciliationTimeline.Snapshot()
	rootID := *reconciliationAdmission.CommandID
	rootUUID := uuid.MustParse(rootID)
	var rootTopic string
	var rootCommandBytes []byte
	var rootIssuedAt, rootExpiresAt time.Time
	if err := f.admin.QueryRow(f.ctx, `SELECT o.topic,o.payload_bytes,c.issued_at,c.expires_at
		FROM twin_outbox o JOIN twin_commands c USING(command_id) WHERE c.command_id=$1`, rootID).
		Scan(&rootTopic, &rootCommandBytes, &rootIssuedAt, &rootExpiresAt); err != nil {
		t.Fatal("capture original immutable root command:", err)
	}
	originalCommand, err := digitaltwin.ParseMQTTCommand(rootTopic, rootCommandBytes, false)
	if err != nil || originalCommand.DesiredVersion != 2 || originalCommand.Parameters.SampleIntervalUS != 1_000_000 {
		t.Fatalf("root command bytes do not match new user intent: command=%+v err=%v", originalCommand, err)
	}

	inbox = gatewaysim.NewCommandInbox(gatewayCfg.GatewayID, nativeReceiptACK{})
	controller, err = gatewaysim.NewController(out, sampler, true)
	if err != nil {
		t.Fatal("reconstruct Gateway controller for lost-result rehearsal:", err)
	}
	gatewayCfg.ClientID = "stage6-reconcile-gateway-" + uuid.NewString()
	gatewayTransport, err = gatewaysim.NewTransport(gatewaysim.TransportOptionsFromConfig(gatewayCfg), inbox)
	if err != nil {
		t.Fatal("reconnect Gateway with scoped result fault:", err)
	}
	faultPublisher := &dropOneGatewayResultPublisher{
		upstream: gatewayTransport, commandID: rootID,
	}
	controlRuntime, err = gatewaysim.NewControlRuntime(controller, inbox, faultPublisher, gatewayCfg.PublishTimeout)
	if err != nil {
		t.Fatal("create Gateway runtime with scoped result fault:", err)
	}
	if err := controlRuntime.Start(f.ctx); err != nil {
		t.Fatal("start Gateway runtime with scoped result fault:", err)
	}
	if err := gatewayTransport.Start(); err != nil {
		t.Fatal("start Gateway TLS transport with scoped result fault:", err)
	}
	awaitControl(t, gatewayTransport.Ready)
	sampler.mu.Lock()
	applyCountBeforeLostResult := sampler.sets
	sampler.mu.Unlock()

	dispatchCfg := mqttcontrol.DefaultConfig()
	dispatchCfg.Stage6Enabled = true
	dispatchCfg.WakeupEnabled = true
	dispatchCfg.ScanInterval = 50 * time.Millisecond
	dispatchCfg.ExpireInterval = 50 * time.Millisecond
	dispatchCfg.RetryBackoff = 50 * time.Millisecond
	dispatchCfg.PublishTimeout = time.Second
	dispatchCfg.RetryPolicy = stage6MQTTPolicy()
	dispatcher, err := mqttcontrol.New(dispatchCfg, shortRepo, duplicateTransport, nil)
	if err != nil {
		t.Fatal("create normal bounded dispatch/expiry runtime:", err)
	}
	shortRepo.SetAdmissionNotifier(dispatcher.Notify)
	if err := dispatcher.Start(); err != nil {
		t.Fatal("start normal bounded dispatch/expiry runtime:", err)
	}
	t.Cleanup(func() {
		dispatcher.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := dispatcher.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	// Require both real Gateway apply evidence and the scoped dropped publish;
	// receipt processing is allowed and may move the backend root to acknowledged.
	var rootGatewayResult []byte
	awaitControl(t, func() bool {
		receipts, readErr := out.ControlReceipts(f.ctx)
		if readErr != nil {
			return false
		}
		for _, receipt := range receipts {
			if receipt.Command.CommandID == rootID && len(receipt.Result) > 0 {
				rootGatewayResult = append([]byte(nil), receipt.Result...)
				return true
			}
		}
		return false
	})
	droppedBytes, dropped := faultPublisher.droppedResult()
	if !dropped || !bytes.Equal(droppedBytes, rootGatewayResult) {
		t.Fatalf("original command_result was not the only dropped response: dropped=%t match=%t",
			dropped, bytes.Equal(droppedBytes, rootGatewayResult))
	}
	sampler.mu.Lock()
	applyCountAfterLostResult := sampler.sets
	sampler.mu.Unlock()
	if applyCountAfterLostResult != applyCountBeforeLostResult+1 {
		t.Fatalf("original command or broker retry applied more than once: before=%d after=%d",
			applyCountBeforeLostResult, applyCountAfterLostResult)
	}
	awaitControl(t, func() bool {
		var status string
		return f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, rootID).Scan(&status) == nil &&
			status == string(digitaltwin.CommandAcknowledged)
	})
	awaitControl(t, func() bool {
		var status string
		return f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, rootID).Scan(&status) == nil &&
			status == string(digitaltwin.CommandTimeout)
	})
	var statusAfterExpiry string
	var issuedAfterExpiry, expiresAfterExpiry time.Time
	var bytesAfterExpiry []byte
	if err := f.admin.QueryRow(f.ctx, `SELECT c.status,c.issued_at,c.expires_at,o.payload_bytes
		FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE c.command_id=$1`, rootID).
		Scan(&statusAfterExpiry, &issuedAfterExpiry, &expiresAfterExpiry, &bytesAfterExpiry); err != nil {
		t.Fatal("read expired original root:", err)
	}
	if statusAfterExpiry != string(digitaltwin.CommandTimeout) ||
		!issuedAfterExpiry.Equal(rootIssuedAt) || !expiresAfterExpiry.Equal(rootExpiresAt) ||
		!bytes.Equal(bytesAfterExpiry, rootCommandBytes) {
		t.Fatalf("original timeout intent was mutated: status=%s issued=%v/%v expires=%v/%v bytes_equal=%t",
			statusAfterExpiry, issuedAfterExpiry, rootIssuedAt, expiresAfterExpiry, rootExpiresAt,
			bytes.Equal(bytesAfterExpiry, rootCommandBytes))
	}

	// Start the production query runtime with the production synchronous
	// reconciliation handler; only the durable schedule is advanced for this
	// isolated test, never desired/reported/command state.
	queryMetrics = &mqttcontrol.ControlQueryMetrics{}
	reconciliationHandler := captureAfterReconciliation(shortRepo, candidates, reconciliationMetrics)
	reconstructedRuntime, err = mqttcontrol.NewControlQueryRuntime(shortRepo, duplicateTransport, reconciliationHandler, queryMetrics)
	if err != nil {
		t.Fatal("create composed reconciliation query runtime:", err)
	}
	queryRuntimeMu.Lock()
	queryRuntime = reconstructedRuntime
	queryRuntimeMu.Unlock()
	if _, err := f.admin.Exec(f.ctx, `UPDATE twin_states SET next_query_at=clock_timestamp() WHERE entity_id=$1`, f.entity); err != nil {
		t.Fatal("advance only the isolated query schedule:", err)
	}
	if err := queryRuntime.Start(); err != nil {
		t.Fatal("start composed reconciliation query runtime:", err)
	}
	var lostResultSnapshot mqttcontrol.ControlSnapshotCandidate
	select {
	case lostResultSnapshot = <-candidates:
		if lostResultSnapshot.Query.DesiredVersion != 2 ||
			lostResultSnapshot.Snapshot.WatermarkVersion != 2 ||
			lostResultSnapshot.Snapshot.ActualSampleIntervalUS == nil ||
			*lostResultSnapshot.Snapshot.ActualSampleIntervalUS != 1_000_000 {
			t.Fatalf("post-timeout correlated Gateway readback differs from applied intent: %+v", lostResultSnapshot)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("post-timeout query did not produce a reconciled candidate: query=%+v reconciliation=%+v",
			queryMetrics.Snapshot(), reconciliationMetrics.Snapshot())
	}
	var childID string
	var childVersion int64
	var childOrigin, childStatus string
	var childParent, childRoot, childActor, rootActor uuid.UUID
	var childBudget int
	awaitControl(t, func() bool {
		return f.admin.QueryRow(f.ctx, `SELECT c.command_id::text,c.desired_version,c.origin,c.status,
			c.parent_command_id,c.root_command_id,r.auto_successors_used,c.original_actor_id,r.original_actor_id
			FROM twin_commands c JOIN twin_commands r ON r.command_id=c.root_command_id
			WHERE c.parent_command_id=$1`, rootUUID).
			Scan(&childID, &childVersion, &childOrigin, &childStatus, &childParent, &childRoot,
				&childBudget, &childActor, &rootActor) == nil
	})
	if childID == rootID || childVersion != 3 || childOrigin != "reconciliation" ||
		childParent != rootUUID || childRoot != rootUUID || childBudget != 1 ||
		childActor != f.owner || rootActor != f.owner {
		t.Fatalf("runtime did not create the single budgeted successor: id=%s version=%d origin=%s parent=%s root=%s actor=%s/%s budget=%d",
			childID, childVersion, childOrigin, childParent, childRoot, childActor, rootActor, childBudget)
	}
	var childTopic string
	var childPayload []byte
	var childExpiry time.Time
	if err := f.admin.QueryRow(f.ctx, `SELECT o.topic,o.payload_bytes,c.expires_at
		FROM twin_outbox o JOIN twin_commands c USING(command_id) WHERE c.command_id=$1`, childID).
		Scan(&childTopic, &childPayload, &childExpiry); err != nil {
		t.Fatal("read successor outbox:", err)
	}
	childCommand, err := digitaltwin.ParseMQTTCommand(childTopic, childPayload, false)
	if err != nil || childCommand.CommandID == rootID ||
		childCommand.DesiredVersion != 3 || childCommand.Parameters.SampleIntervalUS != originalCommand.Parameters.SampleIntervalUS ||
		!childExpiry.After(rootExpiresAt) || !childExpiry.After(time.Now()) {
		t.Fatalf("successor did not preserve desired interval with a fresh bounded command: command=%+v expires=%v err=%v",
			childCommand, childExpiry, err)
	}
	awaitControl(t, func() bool {
		var status string
		return f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, childID).Scan(&status) == nil &&
			status == string(digitaltwin.CommandSucceeded)
	})
	sampler.mu.Lock()
	applyCountAfterSuccessor := sampler.sets
	sampler.mu.Unlock()
	if applyCountAfterSuccessor != applyCountAfterLostResult+1 {
		t.Fatalf("successor replay applied more than once: after_root=%d after_successor=%d",
			applyCountAfterLostResult, applyCountAfterSuccessor)
	}
	var confirmedVersion int64
	var confirmedInterval string
	if err := f.admin.QueryRow(f.ctx, `SELECT (reported_state->'configuration'->>'desired_version')::bigint,
		reported_state->'configuration'->>'sample_interval_us' FROM twin_states WHERE entity_id=$1`, f.entity).
		Scan(&confirmedVersion, &confirmedInterval); err != nil ||
		confirmedVersion != 3 || confirmedInterval != "1000000" {
		t.Fatalf("successor result did not confirm exact reported configuration: version=%d interval=%s err=%v",
			confirmedVersion, confirmedInterval, err)
	}
	timelineAfterSuccessor := reconciliationTimeline.Snapshot()
	if timelineAfterSuccessor.MissingOrigin != timelineBeforeSuccessor.MissingOrigin+1 ||
		timelineAfterSuccessor.Entries != timelineBeforeSuccessor.Entries ||
		reconciliationMetrics.Snapshot().SuccessorCreated != 1 {
		t.Fatalf("system successor fabricated HTTP timing or missed outcome metric: timeline=%+v outcomes=%+v",
			timelineAfterSuccessor, reconciliationMetrics.Snapshot())
	}

	// Rebuild only the volatile query runtime, force the already-due schedule,
	// and confirm the persisted root budget prevents a second automatic child.
	queryRuntime.Stop()
	queryCloseCtx, queryCloseCancel = context.WithTimeout(context.Background(), 3*time.Second)
	if err := queryRuntime.Close(queryCloseCtx); err != nil {
		queryCloseCancel()
		t.Fatal("close first reconciliation query runtime:", err)
	}
	queryCloseCancel()
	reconstructedRuntime, err = mqttcontrol.NewControlQueryRuntime(shortRepo, duplicateTransport,
		captureAfterReconciliation(shortRepo, candidates, reconciliationMetrics), queryMetrics)
	if err != nil {
		t.Fatal("reconstruct bounded reconciliation query runtime:", err)
	}
	queryRuntimeMu.Lock()
	queryRuntime = reconstructedRuntime
	queryRuntimeMu.Unlock()
	if _, err := f.admin.Exec(f.ctx, `UPDATE twin_states SET next_query_at=clock_timestamp() WHERE entity_id=$1`, f.entity); err != nil {
		t.Fatal("advance only isolated query schedule after reconstruction:", err)
	}
	if err := queryRuntime.Start(); err != nil {
		t.Fatal("start reconstructed reconciliation query runtime:", err)
	}
	select {
	case repeatCandidate := <-candidates:
		if repeatCandidate.Snapshot.WatermarkVersion != 3 {
			t.Fatalf("reconstructed query did not read completed successor watermark: %+v", repeatCandidate)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("reconstructed query failed to revisit due Sensor: query=%+v", queryMetrics.Snapshot())
	}
	awaitControl(t, func() bool { return reconciliationMetrics.Snapshot().BlockedBudget >= 1 })
	var budgetCommandCount int
	if err := f.admin.QueryRow(f.ctx, `SELECT count(*) FROM twin_commands WHERE entity_id=$1`, f.entity).Scan(&budgetCommandCount); err != nil {
		t.Fatal(err)
	}
	if budgetCommandCount != 3 {
		t.Fatalf("reconstructed reconciliation exceeded one-successor budget: commands=%d", budgetCommandCount)
	}

	// Release the Gateway's exact durable original result after timeout through
	// its authenticated TLS publisher. It must be recorded only as late evidence;
	// it cannot revive the root or regress the successor's confirmed config.
	faultPublisher.allowLateReplay()
	if err := faultPublisher.Publish(f.ctx, "gateways/stage54_gateway/responses/"+rootID, rootGatewayResult); err != nil {
		t.Fatal("publish stored late Gateway result through its authenticated transport:", err)
	}
	awaitControl(t, func() bool {
		var late []byte
		return f.admin.QueryRow(f.ctx, `SELECT late_result_bytes FROM twin_commands WHERE command_id=$1`, rootID).Scan(&late) == nil &&
			bytes.Equal(late, rootGatewayResult)
	})
	var finalRootStatus string
	var finalReportedVersion int64
	var finalReportedInterval string
	if err := f.admin.QueryRow(f.ctx, `SELECT c.status,
		(st.reported_state->'configuration'->>'desired_version')::bigint,
		st.reported_state->'configuration'->>'sample_interval_us'
		FROM twin_commands c JOIN twin_states st ON st.entity_id=c.entity_id WHERE c.command_id=$1`, rootID).
		Scan(&finalRootStatus, &finalReportedVersion, &finalReportedInterval); err != nil ||
		finalRootStatus != string(digitaltwin.CommandTimeout) || finalReportedVersion != 3 ||
		finalReportedInterval != "1000000" {
		t.Fatalf("late original result changed timeout/history or reported successor config: root=%s reported=%d/%s err=%v",
			finalRootStatus, finalReportedVersion, finalReportedInterval, err)
	}
	sampler.mu.Lock()
	finalApplyCount := sampler.sets
	sampler.mu.Unlock()
	if finalApplyCount != applyCountAfterSuccessor {
		t.Fatalf("late original result replay reapplied configuration: before_replay=%d after_replay=%d",
			applyCountAfterSuccessor, finalApplyCount)
	}
	t.Logf("R7.3 convergence PASS: original=%s timeout; child=%s succeeded; late result retained; root budget=%d; metrics=%+v",
		rootID, childID, childBudget, reconciliationMetrics.Snapshot())
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

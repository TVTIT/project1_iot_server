package digitaltwin_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/telemetry"
)

func resultIntent(t *testing.T, f *controlFixture, hz, version string) digitaltwin.Command {
	t.Helper()
	r, err := f.repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, hz, version, uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}
	var topic string
	var payload []byte
	if err := f.admin.QueryRow(f.ctx, "SELECT topic,payload_bytes FROM twin_outbox WHERE command_id=$1", *r.CommandID).Scan(&topic, &payload); err != nil {
		t.Fatal(err)
	}
	c, err := digitaltwin.ParseMQTTCommand(topic, payload, false)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func resultEnvelope(c digitaltwin.Command) digitaltwin.CommandResult {
	return digitaltwin.CommandResult{ProtocolVersion: 1, MessageType: "command_result", CommandID: c.CommandID, GatewayID: c.GatewayID, EntityID: c.EntityID, DesiredVersion: c.DesiredVersion, Status: digitaltwin.CommandSucceeded, ReportedState: &c.Parameters, CompletedAt: time.Now().UTC().Truncate(time.Microsecond)}
}

func acceptResult(t *testing.T, f *controlFixture, result digitaltwin.CommandResult) (digitaltwin.ControlResultOutcome, error) {
	t.Helper()
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return f.repo.AcceptControlResult(f.ctx, "gateways/"+result.GatewayID+"/responses/"+result.CommandID, payload, time.Now())
}

func TestNativeControlResultAtomicEarlyDuplicateAndConfiguration(t *testing.T) {
	f := newControlFixture(t)
	f.exec(t, `UPDATE twin_states SET reported_state='{"value":42,"sequence_num":7,"sample_interval_us":10000,"preserve":true}',reported_version=8,last_reported_at='2026-01-01Z' WHERE entity_id=$1`, f.entity)
	c := resultIntent(t, f, "0.1", "0")
	r := resultEnvelope(c)
	outcome, err := acceptResult(t, f, r)
	if err != nil || outcome != digitaltwin.ControlResultAccepted {
		t.Fatalf("early result outcome=%s err=%v", outcome, err)
	}
	before := f.counts(t)
	for i := 0; i < 2; i++ {
		outcome, err = acceptResult(t, f, r)
		if err != nil || outcome != digitaltwin.ControlResultDuplicate {
			t.Fatalf("duplicate %s/%v", outcome, err)
		}
	}
	if before != f.counts(t) {
		t.Fatal("duplicate changed state/version")
	}
	if err := f.repo.MarkControlPublished(f.ctx, uuid.MustParse(c.CommandID)); err != nil {
		t.Fatal(err)
	}
	view, err := f.repo.GetCommand(f.ctx, f.viewer, f.urn, c.CommandID)
	if err != nil || view.Status != digitaltwin.CommandSucceeded || view.Result == nil || view.Result.ReportedState.SampleIntervalUS != 10000000 {
		t.Fatalf("safe query result=%+v err=%v", view, err)
	}
	var preserved bool
	if err := f.admin.QueryRow(f.ctx, `SELECT reported_version=9 AND desired_version=1 AND desired_state->>'sample_interval_us'='10000000' AND reported_state->>'value'='42' AND reported_state->>'sequence_num'='7' AND reported_state->>'sample_interval_us'='10000' AND reported_state->>'preserve'='true' AND reported_state->'configuration'->>'sample_interval_us'='10000000' AND reported_state->'configuration'->>'desired_version'='1' AND last_reported_at='2026-01-01Z' FROM twin_states WHERE entity_id=$1`, f.entity).Scan(&preserved); err != nil || !preserved {
		t.Fatalf("configuration/measurement isolation=%v/%v", preserved, err)
	}
	var bytesStored, hashOK, stopped bool
	if err := f.admin.QueryRow(f.ctx, `SELECT result_bytes IS NOT NULL,result_hash=encode(sha256(result_bytes),'hex'),o.status<>'pending' FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE command_id=$1`, c.CommandID).Scan(&bytesStored, &hashOK, &stopped); err != nil || !bytesStored || !hashOK || !stopped {
		t.Fatalf("atomic evidence/outbox=%v/%v/%v err=%v", bytesStored, hashOK, stopped, err)
	}
	conflict := r
	conflict.Status = digitaltwin.CommandFailed
	conflict.ReportedState = nil
	conflict.ErrorCode = "APPLY_FAILED"
	if _, err := acceptResult(t, f, conflict); !errors.Is(err, digitaltwin.ErrControlResultConflict) {
		t.Fatalf("terminal conflict %v", err)
	}
	if before != f.counts(t) {
		t.Fatal("terminal conflict changed state")
	}
}

func TestNativeControlResultMismatchRollbackAndFailed(t *testing.T) {
	f := newControlFixture(t)
	c := resultIntent(t, f, "60", "0")
	before := f.counts(t)
	for _, mutate := range []func(*digitaltwin.CommandResult){
		func(r *digitaltwin.CommandResult) { r.CommandID = uuid.NewString() },
		func(r *digitaltwin.CommandResult) {
			r.GatewayID = "another_gateway"
			r.EntityID = digitaltwin.SensorURNPrefix + "another_gateway:s1"
		},
		func(r *digitaltwin.CommandResult) { r.EntityID = digitaltwin.SensorURNPrefix + f.gw + ":s2" },
		func(r *digitaltwin.CommandResult) { r.DesiredVersion++ },
		func(r *digitaltwin.CommandResult) {
			r.ReportedState = &digitaltwin.SamplingParameters{SampleIntervalUS: 20000}
		},
		func(r *digitaltwin.CommandResult) { r.CompletedAt = c.IssuedAt.Add(-time.Second) },
		func(r *digitaltwin.CommandResult) { r.CompletedAt = c.ExpiresAt.Add(time.Second) },
	} {
		r := resultEnvelope(c)
		mutate(&r)
		if _, err := acceptResult(t, f, r); err == nil {
			t.Fatal("mismatch accepted")
		}
		if f.counts(t) != before {
			t.Fatal("mismatch modified state")
		}
	}
	// Force the state write to fail after a command write. The whole result tx
	// must roll back, including immutable result evidence and outbox stopping.
	f.exec(t, `UPDATE twin_states SET reported_version=9223372036854775807 WHERE entity_id=$1`, f.entity)
	r := resultEnvelope(c)
	if _, err := acceptResult(t, f, r); err == nil {
		t.Fatal("version overflow did not roll back")
	}
	var untouched bool
	if err := f.admin.QueryRow(f.ctx, `SELECT c.status='pending' AND c.result_bytes IS NULL AND o.status='pending' FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE command_id=$1`, c.CommandID).Scan(&untouched); err != nil || !untouched {
		t.Fatalf("partial success survived rollback: %v/%v", untouched, err)
	}
	f.exec(t, `UPDATE twin_states SET reported_version=0 WHERE entity_id=$1`, f.entity)
	r.Status = digitaltwin.CommandFailed
	r.ReportedState = nil
	r.ErrorCode = "APPLY_FAILED"
	r.ErrorMessage = "untrusted diagnostic must not be public"
	if outcome, err := acceptResult(t, f, r); err != nil || outcome != digitaltwin.ControlResultAccepted {
		t.Fatalf("failed result: %s/%v", outcome, err)
	}
	view, err := f.repo.GetCommand(f.ctx, f.owner, f.urn, c.CommandID)
	if err != nil || view.Status != digitaltwin.CommandFailed || view.ErrorMessage != digitaltwin.SafeCommandErrorMessage("APPLY_FAILED") {
		t.Fatalf("unsafe failed DTO: %+v/%v", view, err)
	}
	var reported []byte
	if err := f.admin.QueryRow(f.ctx, "SELECT reported_state FROM twin_states WHERE entity_id=$1", f.entity).Scan(&reported); err != nil || bytes.Contains(reported, []byte("configuration")) {
		t.Fatalf("failed result confirmed config: %s/%v", reported, err)
	}
}

func TestNativeControlResultLateBeforeExpiryScanAndOldVersion(t *testing.T) {
	f := newControlFixture(t)
	cfg := digitaltwin.DefaultControlConfig()
	cfg.TTL = time.Second
	var err error
	f.repo, err = digitaltwin.NewPostgresControlRepository(f.app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c := resultIntent(t, f, "0.1", "0")
	r := resultEnvelope(c)
	time.Sleep(time.Until(c.ExpiresAt) + 20*time.Millisecond)
	if outcome, err := acceptResult(t, f, r); err != nil || outcome != digitaltwin.ControlResultLate {
		t.Fatalf("late before scan: %s/%v", outcome, err)
	}
	before := f.counts(t)
	if outcome, err := acceptResult(t, f, r); err != nil || outcome != digitaltwin.ControlResultDuplicate {
		t.Fatalf("duplicate late: %s/%v", outcome, err)
	}
	if before != f.counts(t) {
		t.Fatal("late result confirmed config")
	}
	newer := resultIntent(t, f, "60", "1")
	if outcome, err := acceptResult(t, f, resultEnvelope(newer)); err != nil || outcome != digitaltwin.ControlResultAccepted {
		t.Fatalf("new result: %s/%v", outcome, err)
	}
	newState := f.counts(t)
	if _, err := acceptResult(t, f, r); err != nil {
		t.Fatal(err)
	}
	if newState != f.counts(t) {
		t.Fatal("old late result overwrote newer configuration")
	}
	var terminal, evidence bool
	if err := f.admin.QueryRow(f.ctx, `SELECT status='timeout',result_bytes IS NULL AND late_result_bytes IS NOT NULL FROM twin_commands WHERE command_id=$1`, c.CommandID).Scan(&terminal, &evidence); err != nil || !terminal || !evidence {
		t.Fatalf("late evidence: %v/%v/%v", terminal, evidence, err)
	}
}

func TestNativeControlResultOldTelemetryPreservesConfigAndPrecision(t *testing.T) {
	f := newControlFixture(t)
	f.exec(t, `UPDATE twin_states SET desired_version=9007199254740992 WHERE entity_id=$1`, f.entity)
	c := resultIntent(t, f, "0.00000000011102230246251565404236316680908203125", "9007199254740992")
	if c.Parameters.SampleIntervalUS != 1<<53 || c.DesiredVersion != 9007199254740993 {
		t.Fatal("fixture lost numeric precision")
	}
	if _, err := acceptResult(t, f, resultEnvelope(c)); err != nil {
		t.Fatal(err)
	}
	var configurationBefore string
	if err := f.admin.QueryRow(f.ctx, "SELECT (reported_state->'configuration')::text FROM twin_states WHERE entity_id=$1", f.entity).Scan(&configurationBefore); err != nil {
		t.Fatal(err)
	}
	measurement := c.IssuedAt.Add(-time.Second)
	dto := telemetry.BatchDTO{ProtocolVersion: 1, MessageType: "telemetry_batch", MessageID: uuid.New(), GatewayID: f.gw, SensorID: "s1", BootID: uuid.New(), FirstSequence: 0, SampleCount: 1, MeasuredAt: measurement.Format(time.RFC3339Nano), SampleIntervalUS: 10000, Samples: []float64{42}}
	data, err := json.Marshal(dto)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := telemetry.NewValidator(telemetry.Limits{MaxPayloadBytes: 65536, MaxSamples: 1000, MaxBatchDuration: time.Minute, MaxFutureSkew: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	message, err := validator.Validate("gateways/"+f.gw+"/telemetry/s1", data, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	repo, err := telemetry.NewPostgresRepository(f.app, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if committed, err := repo.Persist(f.ctx, message); err != nil || committed.Outcome != telemetry.CommitOutcomeCommitted {
		t.Fatalf("late telemetry: %+v/%v", committed, err)
	}
	var after string
	var reportedVersion int64
	if err := f.admin.QueryRow(f.ctx, "SELECT (reported_state->'configuration')::text,reported_version FROM twin_states WHERE entity_id=$1", f.entity).Scan(&after, &reportedVersion); err != nil || after != configurationBefore || reportedVersion != 2 {
		t.Fatalf("telemetry overwrote config: %s/%d/%v", after, reportedVersion, err)
	}
	var raw, temporal int
	if err := f.admin.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM telemetry WHERE gateway_id=$1),(SELECT count(*) FROM twin_temporal_values WHERE entity_id=$2 AND property_name='sample_interval_us')`, f.gw, f.entity).Scan(&raw, &temporal); err != nil || raw != 1 || temporal != 1 {
		t.Fatalf("measurement history %d/%d/%v", raw, temporal, err)
	}
	view, err := f.repo.GetCommand(f.ctx, f.owner, f.urn, c.CommandID)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(view)
	if err != nil || !bytes.Contains(encoded, []byte(`"desired_version":"9007199254740993"`)) || !bytes.Contains(encoded, []byte(`"sample_interval_us":9007199254740992`)) {
		t.Fatalf("safe DTO precision: %s/%v", encoded, err)
	}
}

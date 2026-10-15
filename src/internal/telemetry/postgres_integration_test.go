package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestPostgresTelemetryIntegration is run only by the isolated Stage 3 fixture.
// The fixture runs a statically-linked test binary inside the TimescaleDB
// container, with setup and application credentials supplied in its env.
func TestPostgresTelemetryIntegration(t *testing.T) {
	if os.Getenv("TELEMETRY_TEST_ISOLATED") != "1" {
		t.Skip("isolated database fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	setup, err := pgxpool.New(ctx, os.Getenv("TELEMETRY_TEST_SETUP_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close()
	app, err := pgxpool.New(ctx, os.Getenv("TELEMETRY_TEST_BACKEND_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	var role string
	if err := app.QueryRow(ctx, "SELECT current_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	if role != "iot_backend_app" {
		t.Fatalf("application connection role = %q", role)
	}
	if os.Getenv("TELEMETRY_TEST_LATE_CHUNK") == "1" {
		runCompressedChunkCase(ctx, t, setup, app)
		return
	}
	if os.Getenv("TELEMETRY_TEST_LEGACY") == "1" {
		runLegacyMarkerCase(ctx, t, setup, app)
		return
	}

	// Setup-only seed. The application connection below exercises Persist.
	_, err = setup.Exec(ctx, `INSERT INTO gateways(gateway_id,name) VALUES ('fixture_gw2','integration gateway');
INSERT INTO sensors(gateway_id,sensor_id,name,unit) VALUES ('fixture_gw2','fixture_sensor','integration sensor','degC');
INSERT INTO twin_entities(entity_id,entity_type,gateway_id) VALUES ('urn:ngsi-ld:Gateway:fixture_gw2','Gateway','fixture_gw2'),('urn:ngsi-ld:Sensor:fixture_gw2:fixture_sensor','Sensor','fixture_gw2');
INSERT INTO twin_relationships(source_entity_id,relationship_type,target_entity_id) SELECT g.id,'hasSensor',s.id FROM twin_entities g,twin_entities s WHERE g.entity_id='urn:ngsi-ld:Gateway:fixture_gw2' AND s.entity_id='urn:ngsi-ld:Sensor:fixture_gw2:fixture_sensor';
INSERT INTO twin_states(entity_id) SELECT id FROM twin_entities WHERE gateway_id='fixture_gw2';
INSERT INTO sensors(gateway_id,sensor_id,name,unit) VALUES ('fixture_gw','fixture_sensor2','second sensor','degC');
INSERT INTO twin_entities(entity_id,entity_type,gateway_id) VALUES ('urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor2','Sensor','fixture_gw');
INSERT INTO twin_relationships(source_entity_id,relationship_type,target_entity_id) SELECT g.id,'hasSensor',s.id FROM twin_entities g,twin_entities s WHERE g.entity_id='urn:ngsi-ld:Gateway:fixture_gw' AND s.entity_id='urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor2';
INSERT INTO twin_states(entity_id) SELECT id FROM twin_entities WHERE entity_id='urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor2';
UPDATE twin_states SET reported_state='{"temperature":21.5}',desired_state='{"sampling_seconds":30}',reported_version=7,desired_version=4 WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id='urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor');`)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := NewValidator(Limits{MaxPayloadBytes: 1 << 20, MaxSamples: 1000, MaxBatchDuration: time.Hour, MaxFutureSkew: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	repo, err := NewPostgresRepository(app, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	const sensorEntity = "urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor"
	const gatewayEntity = "urn:ngsi-ld:Gateway:fixture_gw"
	// Sensor desired state, its version, and its audit timestamps/actor must
	// survive a reported-state advance untouched, and the Gateway Twin must stay
	// untouched. Capture both before any persist result can influence them.
	sensorDesiredBefore := snapshotRows(ctx, t, setup, `SELECT row(s.desired_state,s.desired_version,s.last_desired_at,s.last_desired_by)::text FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id='`+sensorEntity+`'`)
	gatewayStateBefore := snapshotRows(ctx, t, setup, `SELECT row(s.reported_state,s.desired_state,s.reported_version,s.desired_version,s.last_reported_at,s.last_desired_at)::text FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id='`+gatewayEntity+`'`)
	var twinTemporalBefore int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_temporal_values").Scan(&twinTemporalBefore); err != nil {
		t.Fatal(err)
	}
	sensorVersion := func() int64 {
		t.Helper()
		var version int64
		if err := setup.QueryRow(ctx, `SELECT s.reported_version FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=$1`, sensorEntity).Scan(&version); err != nil {
			t.Fatal(err)
		}
		return version
	}
	sensorTemporalCount := func() int {
		t.Helper()
		var count int
		if err := setup.QueryRow(ctx, `SELECT count(*) FROM twin_temporal_values WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, sensorEntity).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	makePayload := func(id, gw, sensor string, seq int64, boot string, values []float64) []byte {
		b, _ := json.Marshal(BatchDTO{ProtocolVersion: 1, MessageID: mustUUID(id), MessageType: "telemetry_batch", GatewayID: gw, SensorID: sensor, BootID: mustUUID(boot), FirstSequence: seq, SampleCount: int32(len(values)), MeasuredAt: base.Format(time.RFC3339Nano), SampleIntervalUS: 10000, Samples: values})
		return b
	}
	values := make([]float64, 100)
	for i := range values {
		values[i] = float64(i) + 0.25
	}
	const boot = "10000000-0000-4000-8000-000000000001"
	const id = "20000000-0000-4000-8000-000000000001"
	payload := makePayload(id, "fixture_gw", "fixture_sensor", 12501, boot, values)
	msg, err := validator.Validate("gateways/fixture_gw/telemetry/fixture_sensor", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	result, err := repo.Persist(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != CommitOutcomeCommitted || result.SamplesSaved != 100 || result.Identity.GatewayID != "fixture_gw" || result.Identity.SensorID != "fixture_sensor" {
		t.Fatalf("unexpected persist result: %+v", result)
	}
	var currentRole string
	if err := app.QueryRow(ctx, "SELECT current_user").Scan(&currentRole); err != nil || currentRole != "iot_backend_app" {
		t.Fatalf("role after persist %q (%v)", currentRole, err)
	}
	var rows int
	if err := setup.QueryRow(ctx, `SELECT count(*) FROM telemetry WHERE gateway_id='fixture_gw' AND message_id=$1`, mustUUID(id)).Scan(&rows); err != nil || rows != 100 {
		t.Fatalf("raw count=%d err=%v", rows, err)
	}
	var ledger int
	if err := setup.QueryRow(ctx, `SELECT count(*) FROM processed_messages WHERE gateway_id='fixture_gw' AND message_id=$1 AND payload_hash=$2 AND source_topic=$3 AND first_sequence=12501 AND last_sequence=12600 AND sample_count=100`, mustUUID(id), msg.PayloadHash(), msg.Topic().SourceTopic).Scan(&ledger); err != nil || ledger != 1 {
		t.Fatalf("ledger rows=%d err=%v", ledger, err)
	}
	var firstSeq, lastSeq int64
	var firstTime, lastTime time.Time
	var firstValue, lastValue float64
	var firstIndex, lastIndex int
	if err := setup.QueryRow(ctx, `SELECT sequence_num,measured_at,value,sample_index FROM telemetry WHERE gateway_id='fixture_gw' AND message_id=$1 ORDER BY sample_index LIMIT 1`, mustUUID(id)).Scan(&firstSeq, &firstTime, &firstValue, &firstIndex); err != nil {
		t.Fatal(err)
	}
	if err := setup.QueryRow(ctx, `SELECT sequence_num,measured_at,value,sample_index FROM telemetry WHERE gateway_id='fixture_gw' AND message_id=$1 ORDER BY sample_index DESC LIMIT 1`, mustUUID(id)).Scan(&lastSeq, &lastTime, &lastValue, &lastIndex); err != nil {
		t.Fatal(err)
	}
	if firstSeq != 12501 || lastSeq != 12600 || firstIndex != 0 || lastIndex != 99 || firstValue != .25 || lastValue != 99.25 || !firstTime.Equal(base) || !lastTime.Equal(base.Add(990*time.Millisecond)) {
		t.Fatalf("sample boundaries invalid: %d/%d %d/%d %v/%v %v/%v", firstSeq, lastSeq, firstIndex, lastIndex, firstTime, lastTime, firstValue, lastValue)
	}
	sampleRows, err := setup.Query(ctx, `SELECT sample_index,sequence_num,measured_at,value,boot_id,message_id,gateway_id,sensor_id FROM telemetry WHERE gateway_id='fixture_gw' AND message_id=$1 ORDER BY sample_index`, mustUUID(id))
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for sampleRows.Next() {
		var index int32
		var sequence int64
		var measured time.Time
		var value float64
		var gotBoot, gotMessage uuid.UUID
		var gatewayID, sensorID string
		if err := sampleRows.Scan(&index, &sequence, &measured, &value, &gotBoot, &gotMessage, &gatewayID, &sensorID); err != nil {
			sampleRows.Close()
			t.Fatal(err)
		}
		if index != int32(checked) || sequence != 12501+int64(checked) || !measured.Equal(base.Add(time.Duration(checked)*10*time.Millisecond)) || value != values[checked] || gotBoot != mustUUID(boot) || gotMessage != mustUUID(id) || gatewayID != "fixture_gw" || sensorID != "fixture_sensor" {
			sampleRows.Close()
			t.Fatalf("sample %d mismatch: index=%d seq=%d time=%s value=%v IDs=%s/%s resource=%s/%s", checked, index, sequence, measured, value, gotBoot, gotMessage, gatewayID, sensorID)
		}
		checked++
	}
	if err := sampleRows.Err(); err != nil {
		t.Fatal(err)
	}
	sampleRows.Close()
	if checked != 100 {
		t.Fatalf("validated row count=%d", checked)
	}
	// Task 3.3 normal path: one transaction commits the ledger, 100 raw samples,
	// exactly one Sensor reported-state advance, and exactly one
	// sample_interval_us temporal observation. The Gateway Twin and the Sensor
	// desired state, its version, and its audit fields must not change.
	if after := snapshotRows(ctx, t, setup, `SELECT row(s.desired_state,s.desired_version,s.last_desired_at,s.last_desired_by)::text FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id='`+sensorEntity+`'`); after != sensorDesiredBefore {
		t.Fatalf("Sensor desired state changed: %s -> %s", sensorDesiredBefore, after)
	}
	if after := snapshotRows(ctx, t, setup, `SELECT row(s.reported_state,s.desired_state,s.reported_version,s.desired_version,s.last_reported_at,s.last_desired_at)::text FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id='`+gatewayEntity+`'`); after != gatewayStateBefore {
		t.Fatalf("Gateway Twin changed: %s -> %s", gatewayStateBefore, after)
	}
	var reportedVersion int64
	var lastReportedAt *time.Time
	var reportedValue, reportedBoot, reportedSequence, reportedInterval, reportedUnit, reportedObserved, reportedTemperature string
	if err := setup.QueryRow(ctx, `SELECT s.reported_version, s.last_reported_at,
		s.reported_state->>'value', s.reported_state->>'boot_id', s.reported_state->>'sequence_num',
		s.reported_state->>'sample_interval_us', s.reported_state->>'unit', s.reported_state->>'observed_at',
		s.reported_state->>'temperature'
		FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=$1`, sensorEntity).Scan(&reportedVersion, &lastReportedAt, &reportedValue, &reportedBoot, &reportedSequence, &reportedInterval, &reportedUnit, &reportedObserved, &reportedTemperature); err != nil {
		t.Fatal(err)
	}
	if reportedVersion != 8 {
		t.Fatalf("Sensor reported_version after one new batch = %d; want 8", reportedVersion)
	}
	if lastReportedAt == nil || !lastReportedAt.UTC().Equal(lastTime) {
		t.Fatalf("Sensor last_reported_at = %v; want %v", lastReportedAt, lastTime)
	}
	if reportedValue != fmt.Sprintf("%g", lastValue) || reportedBoot != boot || reportedSequence != fmt.Sprintf("%d", lastSeq) ||
		reportedInterval != "10000" || reportedUnit != "degC" || reportedTemperature != "21.5" {
		t.Fatalf("Sensor reported_state merge value=%q boot=%q seq=%q interval=%q unit=%q temperature=%q", reportedValue, reportedBoot, reportedSequence, reportedInterval, reportedUnit, reportedTemperature)
	}
	if observed, err := time.Parse(time.RFC3339Nano, reportedObserved); err != nil || !observed.UTC().Equal(lastTime) {
		t.Fatalf("Sensor reported observed_at = %q (%v); want %v", reportedObserved, err, lastTime)
	}
	var temporalProperty string
	var temporalNumber *float64
	var temporalText *string
	var temporalBoolean *bool
	var temporalMetadata []byte
	var temporalMessage *uuid.UUID
	var temporalObserved time.Time
	if err := setup.QueryRow(ctx, `SELECT property_name, value_number, value_text, value_boolean, metadata, message_id, observed_at FROM twin_temporal_values WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, sensorEntity).Scan(&temporalProperty, &temporalNumber, &temporalText, &temporalBoolean, &temporalMetadata, &temporalMessage, &temporalObserved); err != nil {
		t.Fatal(err)
	}
	if got := sensorTemporalCount(); got != 1 {
		t.Fatalf("Sensor sample_interval_us temporal rows = %d; want exactly 1", got)
	}
	if temporalProperty != "sample_interval_us" || temporalNumber == nil || *temporalNumber != 10000 || temporalText != nil || temporalBoolean != nil {
		t.Fatalf("temporal typed value property=%q number=%v text=%v boolean=%v", temporalProperty, temporalNumber, temporalText, temporalBoolean)
	}
	if temporalMessage == nil || *temporalMessage != mustUUID(id) || !temporalObserved.UTC().Equal(lastTime) {
		t.Fatalf("temporal provenance message=%v observed=%v; want %s %v", temporalMessage, temporalObserved, id, lastTime)
	}
	var temporalMeta map[string]any
	if err := json.Unmarshal(temporalMetadata, &temporalMeta); err != nil {
		t.Fatal(err)
	}
	if temporalMeta["unit"] != "us" || temporalMeta["boot_id"] != boot || temporalMeta["source"] != "telemetry_batch" {
		t.Fatalf("temporal metadata = %s", temporalMetadata)
	}
	var totalTemporal int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_temporal_values").Scan(&totalTemporal); err != nil {
		t.Fatal(err)
	}
	if totalTemporal != twinTemporalBefore+1 {
		t.Fatalf("total temporal rows after first batch = %d; want %d", totalTemporal, twinTemporalBefore+1)
	}
	ledgerSnapshot := snapshotRows(ctx, t, setup, `SELECT row(gateway_id,message_id,payload_hash,source_topic,sensor_id,boot_id,first_sequence,last_sequence,sample_count)::text FROM processed_messages ORDER BY gateway_id,message_id`)
	rawSnapshot := snapshotRows(ctx, t, setup, `SELECT row(measured_at,gateway_id,sensor_id,sequence_num,value,boot_id,message_id,sample_index)::text FROM telemetry ORDER BY measured_at,gateway_id,message_id,sample_index`)
	expectedLedger, expectedRaw := 1, 100
	assertUnchanged := func(t *testing.T) {
		t.Helper()
		var l, r int
		if e := setup.QueryRow(ctx, "SELECT count(*) FROM processed_messages").Scan(&l); e != nil {
			t.Fatal(e)
		}
		if e := setup.QueryRow(ctx, "SELECT count(*) FROM telemetry").Scan(&r); e != nil {
			t.Fatal(e)
		}
		if l != expectedLedger || r != expectedRaw {
			t.Fatalf("unexpected ledger/raw totals %d/%d", l, r)
		}
		if got := snapshotRows(ctx, t, setup, `SELECT row(gateway_id,message_id,payload_hash,source_topic,sensor_id,boot_id,first_sequence,last_sequence,sample_count)::text FROM processed_messages ORDER BY gateway_id,message_id`); got != ledgerSnapshot {
			t.Fatal("ledger contents changed after retry/rejection")
		}
		if got := snapshotRows(ctx, t, setup, `SELECT row(measured_at,gateway_id,sensor_id,sequence_num,value,boot_id,message_id,sample_index)::text FROM telemetry ORDER BY measured_at,gateway_id,message_id,sample_index`); got != rawSnapshot {
			t.Fatal("raw contents changed after retry/rejection")
		}
	}
	var reason *ReasonError
	dup, err := validator.Validate(msg.Topic().SourceTopic, payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	dupResult, err := repo.Persist(ctx, dup)
	if err != nil || dupResult.Outcome != CommitOutcomeDuplicateCommitted {
		t.Fatalf("duplicate outcome=%+v err=%v", dupResult, err)
	}
	assertUnchanged(t)
	// A committed retry confirms only the prior ledger row: no second
	// reported-state advance and no second temporal observation.
	if got := sensorVersion(); got != 8 {
		t.Fatalf("duplicate advanced Sensor reported_version to %d; want 8", got)
	}
	if got := sensorTemporalCount(); got != 1 {
		t.Fatalf("duplicate appended Sensor temporal rows = %d; want 1", got)
	}
	// Same gateway/message ID presented on a different, already provisioned Sensor topic is a conflict.
	otherTopic, err := validator.Validate("gateways/fixture_gw/telemetry/fixture_sensor2", makePayload(id, "fixture_gw", "fixture_sensor2", 12501, boot, values), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Persist(ctx, otherTopic)
	reason = nil
	if !errors.As(err, &reason) || reason.Code != ReasonMessageConflict {
		t.Fatalf("topic conflict: %v", err)
	}
	assertUnchanged(t)
	unknownSensor, err := validator.Validate("gateways/fixture_gw/telemetry/missing_sensor", makePayload("20000000-0000-4000-8000-000000000003", "fixture_gw", "missing_sensor", 1, boot, []float64{1}), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Persist(ctx, unknownSensor)
	reason = nil
	if !errors.As(err, &reason) || reason.Code != ReasonUnknownSensor {
		t.Fatalf("unknown Sensor: %v", err)
	}
	assertUnchanged(t)
	changed := append([]byte(" "), payload...)
	conflict, err := validator.Validate(msg.Topic().SourceTopic, changed, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Persist(ctx, conflict)
	if !errors.As(err, &reason) || reason.Code != ReasonMessageConflict {
		t.Fatalf("changed payload conflict: %v", err)
	}
	assertUnchanged(t)
	unknown, err := validator.Validate("gateways/missing/telemetry/missing", makePayload("20000000-0000-4000-8000-000000000002", "missing", "missing", 1, boot, []float64{1}), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Persist(ctx, unknown)
	reason = nil
	if !errors.As(err, &reason) || reason.Code != ReasonUnknownGateway {
		t.Fatalf("unknown gateway: %v", err)
	}
	assertUnchanged(t)
	// Inclusive endpoint overlap is rejected; immediately adjacent and gapped ranges are accepted.
	makeOne := func(messageID string, sequence int64) (*Message, error) {
		p := makePayload(messageID, "fixture_gw", "fixture_sensor", sequence, boot, []float64{9.5})
		return validator.Validate("gateways/fixture_gw/telemetry/fixture_sensor", p, time.Now().UTC())
	}
	overlap, err := makeOne("20000000-0000-4000-8000-000000000005", 12600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Persist(ctx, overlap)
	reason = nil
	if !errors.As(err, &reason) || reason.Code != ReasonSequenceOverlap {
		t.Fatalf("inclusive overlap: %v", err)
	}
	assertUnchanged(t)
	// Every ledger/raw rejection above returned before the Twin step, so the
	// Sensor reported state and its temporal history are unchanged.
	if got := sensorVersion(); got != 8 {
		t.Fatalf("rejected writes advanced Sensor reported_version to %d; want 8", got)
	}
	if got := sensorTemporalCount(); got != 1 {
		t.Fatalf("rejected writes appended Sensor temporal rows = %d; want 1", got)
	}
	for _, candidate := range []struct {
		id       string
		sequence int64
	}{
		{"20000000-0000-4000-8000-000000000006", 12601},
		{"20000000-0000-4000-8000-000000000007", 12603},
	} {
		message, err := makeOne(candidate.id, candidate.sequence)
		if err != nil {
			t.Fatal(err)
		}
		written, err := repo.Persist(ctx, message)
		if err != nil || written.Outcome != CommitOutcomeCommitted || written.SamplesSaved != 1 {
			t.Fatalf("nonoverlap sequence %d: %+v %v", candidate.sequence, written, err)
		}
	}
	differentBoot, err := validator.Validate("gateways/fixture_gw/telemetry/fixture_sensor", makePayload("20000000-0000-4000-8000-000000000015", "fixture_gw", "fixture_sensor", 12501, "10000000-0000-4000-8000-000000000002", []float64{4.5}), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if written, e := repo.Persist(ctx, differentBoot); e != nil || written.Outcome != CommitOutcomeCommitted {
		t.Fatalf("different boot reused sequence/time: %+v %v", written, e)
	}
	// The three older-time new batches (adjacent 12601, gapped 12603, and the
	// different-boot batch) each append exactly one temporal observation but do
	// not regress the newer current state or its version.
	if got := sensorVersion(); got != 8 {
		t.Fatalf("older-time new batches advanced Sensor reported_version to %d; want 8", got)
	}
	if got := sensorTemporalCount(); got != 4 {
		t.Fatalf("older-time new batches Sensor temporal rows = %d; want 4", got)
	}
	var finalLedger, finalRaw int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM processed_messages").Scan(&finalLedger); err != nil {
		t.Fatal(err)
	}
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM telemetry").Scan(&finalRaw); err != nil {
		t.Fatal(err)
	}
	if finalLedger != 4 || finalRaw != 103 {
		t.Fatalf("nonoverlap totals ledger/raw=%d/%d", finalLedger, finalRaw)
	}
	expectedLedger, expectedRaw = finalLedger, finalRaw
	ledgerSnapshot = snapshotRows(ctx, t, setup, `SELECT row(gateway_id,message_id,payload_hash,source_topic,sensor_id,boot_id,first_sequence,last_sequence,sample_count)::text FROM processed_messages ORDER BY gateway_id,message_id`)
	rawSnapshot = snapshotRows(ctx, t, setup, `SELECT row(measured_at,gateway_id,sensor_id,sequence_num,value,boot_id,message_id,sample_index)::text FROM telemetry ORDER BY measured_at,gateway_id,message_id,sample_index`)
	// Incomplete graph edges and Gateway/Sensor states fail closed without writes.
	incomplete := func(topic, gateway, sensor, mid string) {
		t.Helper()
		m, e := validator.Validate(topic, makePayload(mid, gateway, sensor, 30000, boot, []float64{1}), time.Now().UTC())
		if e != nil {
			t.Fatal(e)
		}
		_, e = repo.Persist(ctx, m)
		var r *ReasonError
		if !errors.As(e, &r) || r.Code != ReasonResourceGraphIncomplete {
			t.Fatalf("incomplete graph %s: %v", topic, e)
		}
	}
	_, err = setup.Exec(ctx, `DELETE FROM twin_relationships r USING twin_entities g,twin_entities s WHERE r.source_entity_id=g.id AND r.target_entity_id=s.id AND g.entity_id='urn:ngsi-ld:Gateway:fixture_gw' AND s.entity_id='urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor2'`)
	if err != nil {
		t.Fatal(err)
	}
	incomplete("gateways/fixture_gw/telemetry/fixture_sensor2", "fixture_gw", "fixture_sensor2", "20000000-0000-4000-8000-000000000008")
	_, err = setup.Exec(ctx, `INSERT INTO twin_relationships(source_entity_id,relationship_type,target_entity_id) SELECT g.id,'hasSensor',s.id FROM twin_entities g,twin_entities s WHERE g.entity_id='urn:ngsi-ld:Gateway:fixture_gw' AND s.entity_id='urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor2'`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = setup.Exec(ctx, `DELETE FROM twin_states WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id='urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor2')`)
	if err != nil {
		t.Fatal(err)
	}
	incomplete("gateways/fixture_gw/telemetry/fixture_sensor2", "fixture_gw", "fixture_sensor2", "20000000-0000-4000-8000-000000000009")
	_, err = setup.Exec(ctx, `INSERT INTO twin_states(entity_id) SELECT id FROM twin_entities WHERE entity_id='urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor2'`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = setup.Exec(ctx, `UPDATE twin_entities SET gateway_id='fixture_gw' WHERE entity_id='urn:ngsi-ld:Gateway:fixture_gw2'`)
	if err != nil {
		t.Fatal(err)
	}
	incomplete("gateways/fixture_gw2/telemetry/fixture_sensor", "fixture_gw2", "fixture_sensor", "20000000-0000-4000-8000-000000000017")
	_, err = setup.Exec(ctx, `UPDATE twin_entities SET gateway_id='fixture_gw2' WHERE entity_id='urn:ngsi-ld:Gateway:fixture_gw2'`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = setup.Exec(ctx, `DELETE FROM twin_states WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id='urn:ngsi-ld:Gateway:fixture_gw2')`)
	if err != nil {
		t.Fatal(err)
	}
	incomplete("gateways/fixture_gw2/telemetry/fixture_sensor", "fixture_gw2", "fixture_sensor", "20000000-0000-4000-8000-000000000010")
	_, err = setup.Exec(ctx, `INSERT INTO twin_states(entity_id) SELECT id FROM twin_entities WHERE entity_id='urn:ngsi-ld:Gateway:fixture_gw2'`)
	if err != nil {
		t.Fatal(err)
	}
	assertUnchanged(t)
	// A real COPY failure on sample 50 must roll back both the new ledger row and all copied samples.
	_, err = setup.Exec(ctx, `CREATE FUNCTION public.stage3_fail_sample_50() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.sample_index=50 THEN RAISE EXCEPTION 'fixture injected sample failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER stage3_fail_sample_50 BEFORE INSERT ON telemetry FOR EACH ROW EXECUTE FUNCTION public.stage3_fail_sample_50()`)
	if err != nil {
		t.Fatal(err)
	}
	failing, err := validator.Validate("gateways/fixture_gw/telemetry/fixture_sensor", makePayload("20000000-0000-4000-8000-000000000011", "fixture_gw", "fixture_sensor", 40000, boot, values), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	_, persistErr := repo.Persist(ctx, failing)
	_, dropErr := setup.Exec(ctx, `DROP TRIGGER stage3_fail_sample_50 ON telemetry; DROP FUNCTION stage3_fail_sample_50()`)
	if dropErr != nil {
		t.Fatal(dropErr)
	}
	if persistErr == nil {
		t.Fatal("fixture COPY failure unexpectedly succeeded")
	}
	assertUnchanged(t)
	// Two physical pool connections race the same ID/hash/topic behind one start barrier.
	connA, err := app.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connA.Release()
	connB, err := app.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connB.Release()
	repoA, err := NewPostgresRepository(connA.Conn(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	repoB, err := NewPostgresRepository(connB.Conn(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	concurrentPayload := makePayload("20000000-0000-4000-8000-000000000012", "fixture_gw", "fixture_sensor", 50001, boot, values)
	concurrentMessage, err := validator.Validate("gateways/fixture_gw/telemetry/fixture_sensor", concurrentPayload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	type persistAnswer struct {
		result PersistResult
		err    error
	}
	raceVersionBefore, raceTemporalBefore := sensorVersion(), sensorTemporalCount()
	start := make(chan struct{})
	answers := make(chan persistAnswer, 2)
	for _, concurrentRepo := range []Repository{repoA, repoB} {
		go func(r Repository) {
			<-start
			result, e := r.Persist(ctx, concurrentMessage)
			answers <- persistAnswer{result, e}
		}(concurrentRepo)
	}
	close(start)
	newCount, duplicateCount := 0, 0
	for range 2 {
		answer := <-answers
		if answer.err != nil {
			t.Fatal(answer.err)
		}
		switch answer.result.Outcome {
		case CommitOutcomeCommitted:
			newCount++
		case CommitOutcomeDuplicateCommitted:
			duplicateCount++
		default:
			t.Fatalf("same-ID race outcome: %+v", answer.result)
		}
	}
	if newCount != 1 || duplicateCount != 1 {
		t.Fatalf("same-ID concurrency outcomes committed=%d duplicate=%d", newCount, duplicateCount)
	}
	if got := snapshotRows(ctx, t, setup, `SELECT row(gateway_id,message_id,payload_hash,source_topic,sensor_id,boot_id,first_sequence,last_sequence,sample_count)::text FROM processed_messages ORDER BY gateway_id,message_id`); len(got) == 0 {
		t.Fatal("same-ID race lost ledger")
	}
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM telemetry WHERE gateway_id='fixture_gw' AND message_id=$1", mustUUID("20000000-0000-4000-8000-000000000012")).Scan(&finalRaw); err != nil || finalRaw != 100 {
		t.Fatalf("same-ID race samples=%d err=%v", finalRaw, err)
	}
	var concurrentLedger int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM processed_messages WHERE gateway_id='fixture_gw' AND message_id=$1", mustUUID("20000000-0000-4000-8000-000000000012")).Scan(&concurrentLedger); err != nil || concurrentLedger != 1 {
		t.Fatalf("same-ID race ledger=%d err=%v", concurrentLedger, err)
	}
	// Exactly one exact delivery advanced the Twin once and appended one
	// temporal observation; the duplicate delivery wrote neither.
	if got := sensorVersion(); got != raceVersionBefore+1 {
		t.Fatalf("same-ID race Sensor reported_version=%d; want one advance from %d", got, raceVersionBefore)
	}
	if got := sensorTemporalCount(); got != raceTemporalBefore+1 {
		t.Fatalf("same-ID race Sensor temporal rows=%d; want one append from %d", got, raceTemporalBefore)
	}
	// Two distinct overlapping IDs start together. Exactly one batch may commit.
	makeRaceMessage := func(mid string, first int64) (*Message, error) {
		p := makePayload(mid, "fixture_gw", "fixture_sensor", first, boot, values[:5])
		return validator.Validate("gateways/fixture_gw/telemetry/fixture_sensor", p, time.Now().UTC())
	}
	raceA, err := makeRaceMessage("20000000-0000-4000-8000-000000000013", 60000)
	if err != nil {
		t.Fatal(err)
	}
	raceB, err := makeRaceMessage("20000000-0000-4000-8000-000000000014", 60003)
	if err != nil {
		t.Fatal(err)
	}
	start = make(chan struct{})
	answers = make(chan persistAnswer, 2)
	for i, candidate := range []*Message{raceA, raceB} {
		concurrentRepo := repoA
		if i == 1 {
			concurrentRepo = repoB
		}
		go func(r Repository, m *Message) {
			<-start
			result, e := r.Persist(ctx, m)
			answers <- persistAnswer{result, e}
		}(concurrentRepo, candidate)
	}
	close(start)
	committed, overlapCount := 0, 0
	for range 2 {
		answer := <-answers
		if answer.err == nil && answer.result.Outcome == CommitOutcomeCommitted {
			committed++
		} else {
			var r *ReasonError
			if errors.As(answer.err, &r) && r.Code == ReasonSequenceOverlap {
				overlapCount++
			} else {
				t.Fatalf("overlap race outcome=%+v err=%v", answer.result, answer.err)
			}
		}
	}
	if committed != 1 || overlapCount != 1 {
		t.Fatalf("range race outcomes committed=%d overlap=%d", committed, overlapCount)
	}
	var raceLedger, raceRaw int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM processed_messages").Scan(&raceLedger); err != nil {
		t.Fatal(err)
	}
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM telemetry").Scan(&raceRaw); err != nil {
		t.Fatal(err)
	}
	if raceLedger != 6 || raceRaw != 208 {
		t.Fatalf("concurrency totals ledger/raw=%d/%d", raceLedger, raceRaw)
	}
	var overlapLedger, overlapSamples int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM processed_messages WHERE message_id IN ($1,$2)", mustUUID("20000000-0000-4000-8000-000000000013"), mustUUID("20000000-0000-4000-8000-000000000014")).Scan(&overlapLedger); err != nil || overlapLedger != 1 {
		t.Fatalf("range race ledger winner count=%d err=%v", overlapLedger, err)
	}
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM telemetry WHERE message_id IN ($1,$2)", mustUUID("20000000-0000-4000-8000-000000000013"), mustUUID("20000000-0000-4000-8000-000000000014")).Scan(&overlapSamples); err != nil || overlapSamples != 5 {
		t.Fatalf("range race winning raw count=%d err=%v", overlapSamples, err)
	}
	if got := sensorVersion(); got != 9 {
		t.Fatalf("range race advanced Sensor reported_version to %d; want 9", got)
	}
	if got := sensorTemporalCount(); got != 6 {
		t.Fatalf("range race Sensor temporal rows = %d; want 6", got)
	}
	expectedLedger, expectedRaw = raceLedger, raceRaw
	ledgerSnapshot = snapshotRows(ctx, t, setup, `SELECT row(gateway_id,message_id,payload_hash,source_topic,sensor_id,boot_id,first_sequence,last_sequence,sample_count)::text FROM processed_messages ORDER BY gateway_id,message_id`)
	rawSnapshot = snapshotRows(ctx, t, setup, `SELECT row(measured_at,gateway_id,sensor_id,sequence_num,value,boot_id,message_id,sample_index)::text FROM telemetry ORDER BY measured_at,gateway_id,message_id,sample_index`)
	// Task 3.3 boundary: persistence advances only the resolved Sensor Twin and
	// appends one temporal observation per new batch. No new Twin state rows are
	// created, the Gateway Twin is untouched, and the Sensor desired state and
	// audit fields are preserved.
	var statesAfter int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_states").Scan(&statesAfter); err != nil {
		t.Fatal(err)
	}
	if statesAfter != 5 {
		t.Fatalf("Twin state row count = %d; want 5 (no new rows)", statesAfter)
	}
	if got := sensorVersion(); got != 9 {
		t.Fatalf("final Sensor reported_version = %d; want 9", got)
	}
	if got := sensorTemporalCount(); got != 6 {
		t.Fatalf("final Sensor temporal rows = %d; want 6", got)
	}
	var finalValue, finalSequence, finalInterval, finalObserved string
	var finalLastReported *time.Time
	if err := setup.QueryRow(ctx, `SELECT s.reported_state->>'value', s.reported_state->>'sequence_num', s.reported_state->>'sample_interval_us', s.reported_state->>'observed_at', s.last_reported_at FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=$1`, sensorEntity).Scan(&finalValue, &finalSequence, &finalInterval, &finalObserved, &finalLastReported); err != nil {
		t.Fatal(err)
	}
	if finalValue != fmt.Sprintf("%g", lastValue) || finalSequence != "50100" || finalInterval != "10000" {
		t.Fatalf("final Sensor current value=%q seq=%q interval=%q", finalValue, finalSequence, finalInterval)
	}
	if finalLastReported == nil || !finalLastReported.UTC().Equal(lastTime) {
		t.Fatalf("final Sensor last_reported_at = %v; want %v", finalLastReported, lastTime)
	}
	if observed, err := time.Parse(time.RFC3339Nano, finalObserved); err != nil || !observed.UTC().Equal(lastTime) {
		t.Fatalf("final Sensor observed_at = %q (%v); want %v", finalObserved, err, lastTime)
	}
	if after := snapshotRows(ctx, t, setup, `SELECT row(s.desired_state,s.desired_version,s.last_desired_at,s.last_desired_by)::text FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id='`+sensorEntity+`'`); after != sensorDesiredBefore {
		t.Fatalf("Sensor desired state changed: %s -> %s", sensorDesiredBefore, after)
	}
	if after := snapshotRows(ctx, t, setup, `SELECT row(s.reported_state,s.desired_state,s.reported_version,s.desired_version,s.last_reported_at,s.last_desired_at)::text FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id='`+gatewayEntity+`'`); after != gatewayStateBefore {
		t.Fatalf("Gateway Twin changed: %s -> %s", gatewayStateBefore, after)
	}
	// The same Sensor ID exists under two Gateways; resolution follows the topic's Gateway.
	isolated, err := validator.Validate("gateways/fixture_gw2/telemetry/fixture_sensor", makePayload("20000000-0000-4000-8000-000000000016", "fixture_gw2", "fixture_sensor", 1, boot, []float64{8.75}), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	isolatedResult, err := repo.Persist(ctx, isolated)
	if err != nil || isolatedResult.Outcome != CommitOutcomeCommitted || isolatedResult.Identity.GatewayID != "fixture_gw2" {
		t.Fatalf("same Sensor ID Gateway isolation: %+v %v", isolatedResult, err)
	}
	var isolatedRows int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM telemetry WHERE gateway_id='fixture_gw2' AND sensor_id='fixture_sensor' AND message_id=$1", mustUUID("20000000-0000-4000-8000-000000000016")).Scan(&isolatedRows); err != nil || isolatedRows != 1 {
		t.Fatalf("isolated Gateway rows=%d err=%v", isolatedRows, err)
	}
	// The same Sensor ID under another Gateway advances only its own Twin: one
	// temporal observation and one version for the fixture_gw2 Sensor, and no
	// change to the fixture_gw Sensor.
	var isolatedTemporal int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_temporal_values WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id='urn:ngsi-ld:Sensor:fixture_gw2:fixture_sensor')").Scan(&isolatedTemporal); err != nil || isolatedTemporal != 1 {
		t.Fatalf("isolated Gateway sensor temporal=%d err=%v", isolatedTemporal, err)
	}
	var isolatedVersion int64
	if err := setup.QueryRow(ctx, "SELECT s.reported_version FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id='urn:ngsi-ld:Sensor:fixture_gw2:fixture_sensor'").Scan(&isolatedVersion); err != nil || isolatedVersion != 1 {
		t.Fatalf("isolated Gateway sensor version=%d err=%v", isolatedVersion, err)
	}
	if got := sensorVersion(); got != 9 {
		t.Fatalf("isolated write changed fixture_gw Sensor version to %d; want 9", got)
	}

	// Task 3.3 ordering is decided at the database, under the Sensor/Twin lock.
	// Every case below builds a Validator.Validate message, uses a distinct
	// boot/range so the ledger overlap gate cannot mask the ordering branch, and
	// seeds the Sensor current state to drive each newer/older/equal branch
	// deterministically. Each case asserts the exact reported_version delta,
	// current JSON/version/timestamps, and committed raw/ledger/temporal counts.
	type batchSpec struct {
		id         string
		sequence   int64
		bootID     string
		measuredAt time.Time
		intervalUS int64
		values     []float64
	}
	sensorTopic := "gateways/fixture_gw/telemetry/fixture_sensor"
	makeOrderPayload := func(s batchSpec) []byte {
		b, _ := json.Marshal(BatchDTO{ProtocolVersion: 1, MessageID: mustUUID(s.id), MessageType: "telemetry_batch", GatewayID: "fixture_gw", SensorID: "fixture_sensor", BootID: mustUUID(s.bootID), FirstSequence: s.sequence, SampleCount: int32(len(s.values)), MeasuredAt: s.measuredAt.UTC().Format(time.RFC3339Nano), SampleIntervalUS: s.intervalUS, Samples: s.values})
		return b
	}
	validateOrder := func(s batchSpec) *Message {
		t.Helper()
		message, err := validator.Validate(sensorTopic, makeOrderPayload(s), time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		return message
	}
	persistOrder := func(s batchSpec) PersistResult {
		t.Helper()
		result, err := repo.Persist(ctx, validateOrder(s))
		if err != nil || result.Outcome != CommitOutcomeCommitted || result.SamplesSaved != len(s.values) {
			t.Fatalf("order case %s result=%+v err=%v", s.id, result, err)
		}
		return result
	}
	counts := func() (int, int, int) {
		t.Helper()
		var ledger, raw int
		if err := setup.QueryRow(ctx, "SELECT count(*) FROM processed_messages").Scan(&ledger); err != nil {
			t.Fatal(err)
		}
		if err := setup.QueryRow(ctx, "SELECT count(*) FROM telemetry").Scan(&raw); err != nil {
			t.Fatal(err)
		}
		return ledger, raw, sensorTemporalCount()
	}
	seedSensorState := func(stateJSON string, version int64, lastReported time.Time) {
		t.Helper()
		if _, err := setup.Exec(ctx, `UPDATE twin_states SET reported_state=$1::jsonb, reported_version=$2, last_reported_at=$3 WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$4)`, stateJSON, version, lastReported, sensorEntity); err != nil {
			t.Fatal(err)
		}
	}
	sensorCurrent := func() string {
		t.Helper()
		return snapshotRows(ctx, t, setup, `SELECT row(s.reported_state,s.reported_version,s.last_reported_at)::text FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id='`+sensorEntity+`'`)
	}
	sensorDesired := func() string {
		t.Helper()
		return snapshotRows(ctx, t, setup, `SELECT row(s.desired_state,s.desired_version,s.last_desired_at,s.last_desired_by)::text FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id='`+sensorEntity+`'`)
	}
	assertAdvanced := func(label, wantBoot string, wantObserved time.Time, wantSequence int64, wantValue float64, wantVersion int64) {
		t.Helper()
		var version int64
		var lastReported *time.Time
		var boot, sequence, observed, value string
		if err := setup.QueryRow(ctx, `SELECT s.reported_version, s.last_reported_at, s.reported_state->>'boot_id', s.reported_state->>'sequence_num', s.reported_state->>'observed_at', s.reported_state->>'value' FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=$1`, sensorEntity).Scan(&version, &lastReported, &boot, &sequence, &observed, &value); err != nil {
			t.Fatal(err)
		}
		if version != wantVersion || lastReported == nil || !lastReported.UTC().Equal(wantObserved.UTC()) ||
			boot != wantBoot || sequence != fmt.Sprintf("%d", wantSequence) || value != fmt.Sprintf("%g", wantValue) {
			t.Fatalf("%s advanced state mismatch: version=%d last=%v boot=%q seq=%q value=%q", label, version, lastReported, boot, sequence, value)
		}
		if parsed, err := time.Parse(time.RFC3339Nano, observed); err != nil || !parsed.UTC().Equal(wantObserved.UTC()) {
			t.Fatalf("%s observed_at=%q (%v); want %v", label, observed, err, wantObserved)
		}
	}
	assertHeld := func(label, wantCurrent string, wantVersion int64) {
		t.Helper()
		if got := sensorCurrent(); got != wantCurrent {
			t.Fatalf("%s changed the Sensor current state:\n%s", label, got)
		}
		var version int64
		if err := setup.QueryRow(ctx, `SELECT s.reported_version FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=$1`, sensorEntity).Scan(&version); err != nil {
			t.Fatal(err)
		}
		if version != wantVersion {
			t.Fatalf("%s reported_version=%d; want %d", label, version, wantVersion)
		}
	}

	const (
		orderBootBase   = "60000000-0000-4000-8000-000000000001"
		orderBootNewer  = "60000000-0000-4000-8000-000000000002"
		orderBootOlder  = "60000000-0000-4000-8000-000000000003"
		orderBootEqual  = "60000000-0000-4000-8000-000000000004"
		orderBootLegacy = "60000000-0000-4000-8000-000000000005"
		orderBootFifo   = "60000000-0000-4000-8000-000000000013"
	)
	orderVersion := int64(50)
	orderBase := base.Add(2 * time.Second)
	baselineState := fmt.Sprintf(`{"temperature":21.5,"unit":"degC","value":1.5,"boot_id":%q,"sequence_num":1000,"observed_at":%q,"sample_interval_us":10000}`, orderBootBase, orderBase.Format(time.RFC3339Nano))

	// (a) a newer measurement time advances even when the boot ID differs.
	seedSensorState(baselineState, orderVersion, orderBase)
	l0, r0, t0 := counts()
	persistOrder(batchSpec{id: "60000000-0000-4000-8000-0000000000a1", sequence: 4001, bootID: orderBootNewer, measuredAt: orderBase.Add(time.Second), intervalUS: 10000, values: []float64{7.5}})
	assertAdvanced("newer/different boot", orderBootNewer, orderBase.Add(time.Second), 4001, 7.5, orderVersion+1)
	if l1, r1, t1 := counts(); l1 != l0+1 || r1 != r0+1 || t1 != t0+1 {
		t.Fatalf("newer/different boot counts ledger/raw/temporal = %d/%d/%d; want %d/%d/%d", l1, r1, t1, l0+1, r0+1, t0+1)
	}

	// (b) an older valid batch appends raw/ledger/temporal but holds the state.
	seedSensorState(baselineState, orderVersion, orderBase)
	beforeCurrent := sensorCurrent()
	l0, r0, t0 = counts()
	persistOrder(batchSpec{id: "60000000-0000-4000-8000-0000000000a2", sequence: 4002, bootID: orderBootOlder, measuredAt: orderBase.Add(-time.Second), intervalUS: 10000, values: []float64{8.5}})
	assertHeld("older/new batch", beforeCurrent, orderVersion)
	if l1, r1, t1 := counts(); l1 != l0+1 || r1 != r0+1 || t1 != t0+1 {
		t.Fatalf("older/new batch counts ledger/raw/temporal = %d/%d/%d; want %d/%d/%d", l1, r1, t1, l0+1, r0+1, t0+1)
	}

	// (c) an equal timestamp with a different boot ID holds the state.
	seedSensorState(baselineState, orderVersion, orderBase)
	beforeCurrent = sensorCurrent()
	l0, r0, t0 = counts()
	persistOrder(batchSpec{id: "60000000-0000-4000-8000-0000000000a3", sequence: 4003, bootID: orderBootEqual, measuredAt: orderBase, intervalUS: 10000, values: []float64{9.5}})
	assertHeld("equal time/different boot", beforeCurrent, orderVersion)
	if l1, r1, t1 := counts(); l1 != l0+1 || r1 != r0+1 || t1 != t0+1 {
		t.Fatalf("equal time/different boot counts ledger/raw/temporal = %d/%d/%d; want %d/%d/%d", l1, r1, t1, l0+1, r0+1, t0+1)
	}

	// (d) equal timestamp, same boot, strictly greater sequence advances.
	seedSensorState(baselineState, orderVersion, orderBase)
	l0, r0, t0 = counts()
	persistOrder(batchSpec{id: "60000000-0000-4000-8000-0000000000a4", sequence: 5001, bootID: orderBootBase, measuredAt: orderBase, intervalUS: 10000, values: []float64{10.5}})
	assertAdvanced("equal time/same boot greater sequence", orderBootBase, orderBase, 5001, 10.5, orderVersion+1)
	if l1, r1, t1 := counts(); l1 != l0+1 || r1 != r0+1 || t1 != t0+1 {
		t.Fatalf("greater sequence counts ledger/raw/temporal = %d/%d/%d; want %d/%d/%d", l1, r1, t1, l0+1, r0+1, t0+1)
	}

	// (e) equal timestamp, same boot, equal sequence holds.
	seedSensorState(baselineState, orderVersion, orderBase)
	beforeCurrent = sensorCurrent()
	l0, r0, t0 = counts()
	persistOrder(batchSpec{id: "60000000-0000-4000-8000-0000000000a5", sequence: 1000, bootID: orderBootBase, measuredAt: orderBase, intervalUS: 10000, values: []float64{11.5}})
	assertHeld("equal time/same boot equal sequence", beforeCurrent, orderVersion)
	if l1, r1, t1 := counts(); l1 != l0+1 || r1 != r0+1 || t1 != t0+1 {
		t.Fatalf("equal sequence counts ledger/raw/temporal = %d/%d/%d; want %d/%d/%d", l1, r1, t1, l0+1, r0+1, t0+1)
	}

	// (f) equal timestamp, same boot, lower sequence holds.
	seedSensorState(baselineState, orderVersion, orderBase)
	beforeCurrent = sensorCurrent()
	l0, r0, t0 = counts()
	persistOrder(batchSpec{id: "60000000-0000-4000-8000-0000000000a6", sequence: 999, bootID: orderBootBase, measuredAt: orderBase, intervalUS: 10000, values: []float64{12.5}})
	assertHeld("equal time/same boot lower sequence", beforeCurrent, orderVersion)
	if l1, r1, t1 := counts(); l1 != l0+1 || r1 != r0+1 || t1 != t0+1 {
		t.Fatalf("lower sequence counts ledger/raw/temporal = %d/%d/%d; want %d/%d/%d", l1, r1, t1, l0+1, r0+1, t0+1)
	}

	// Legacy states with missing or malformed boot/sequence at an equal timestamp
	// hold the current state. A valid batch still commits raw/ledger/temporal, so
	// a malformed legacy JSON value must never fail an otherwise valid write.
	for _, legacy := range []struct {
		name  string
		id    string
		state string
		seq   int64
	}{
		{"missing boot and sequence", "60000000-0000-4000-8000-0000000000b1", `{"temperature":42,"style":"legacy"}`, 6001},
		{"missing sequence with matching boot", "60000000-0000-4000-8000-0000000000b2", fmt.Sprintf(`{"boot_id":%q,"temperature":43}`, orderBootLegacy), 6002},
		{"malformed boot", "60000000-0000-4000-8000-0000000000b3", `{"boot_id":"not-a-uuid","sequence_num":5,"temperature":44}`, 6003},
		{"malformed sequence", "60000000-0000-4000-8000-0000000000b4", fmt.Sprintf(`{"boot_id":%q,"sequence_num":"7","temperature":45}`, orderBootLegacy), 6004},
	} {
		seedSensorState(legacy.state, orderVersion, orderBase)
		beforeCurrent = sensorCurrent()
		l0, r0, t0 = counts()
		persistOrder(batchSpec{id: legacy.id, sequence: legacy.seq, bootID: orderBootLegacy, measuredAt: orderBase, intervalUS: 10000, values: []float64{13.5}})
		assertHeld("legacy "+legacy.name, beforeCurrent, orderVersion)
		if l1, r1, t1 := counts(); l1 != l0+1 || r1 != r0+1 || t1 != t0+1 {
			t.Fatalf("legacy %s counts ledger/raw/temporal = %d/%d/%d; want %d/%d/%d", legacy.name, l1, r1, t1, l0+1, r0+1, t0+1)
		}
	}

	// Deterministic two-connection Sensor ordering. A setup transaction holds the
	// Sensor row lock so both workers queue on one lock; the first worker is
	// started and then observed waiting (wait_event_type='Lock', bounded timeout)
	// before the second starts, so the PostgreSQL lock queue grants them in that
	// order. No sleep or stress loop proves the order.
	seedSensorState(baselineState, orderVersion, orderBase)
	raceDesired := sensorDesired()
	var pidA, pidB int32
	if err := connA.Conn().QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pidA); err != nil {
		t.Fatal(err)
	}
	if err := connB.Conn().QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pidB); err != nil {
		t.Fatal(err)
	}
	waitForLockWait := func(pid int32, label string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			var waiting bool
			if err := setup.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')`, pid).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out observing %s Sensor-lock wait (pid %d)", label, pid)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	runLockOrderedRace := func(first, second *Message, label string) {
		t.Helper()
		blockTx, err := setup.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var locked int
		if err := blockTx.QueryRow(ctx, `SELECT 1 FROM sensors WHERE gateway_id='fixture_gw' AND sensor_id='fixture_sensor' FOR UPDATE`).Scan(&locked); err != nil {
			t.Fatal(err)
		}
		firstDone := make(chan persistAnswer, 1)
		secondDone := make(chan persistAnswer, 1)
		go func() {
			result, e := repoA.Persist(ctx, first)
			firstDone <- persistAnswer{result, e}
		}()
		waitForLockWait(pidA, label+" first")
		go func() {
			result, e := repoB.Persist(ctx, second)
			secondDone <- persistAnswer{result, e}
		}()
		waitForLockWait(pidB, label+" second")
		if err := blockTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		firstAnswer := <-firstDone
		secondAnswer := <-secondDone
		if firstAnswer.err != nil || firstAnswer.result.Outcome != CommitOutcomeCommitted {
			t.Fatalf("%s first worker: result=%+v err=%v", label, firstAnswer.result, firstAnswer.err)
		}
		if secondAnswer.err != nil || secondAnswer.result.Outcome != CommitOutcomeCommitted {
			t.Fatalf("%s second worker: result=%+v err=%v", label, secondAnswer.result, secondAnswer.err)
		}
	}

	// (1) The newer measurement is queued on the Sensor lock first, is granted
	// first, and advances; the older measurement queued second appends
	// raw/ledger/temporal but must hold the already-newer current state.
	newerSpec := batchSpec{id: "60000000-0000-4000-8000-0000000000c1", sequence: 7001, bootID: "60000000-0000-4000-8000-000000000011", measuredAt: orderBase.Add(10 * time.Second), intervalUS: 10000, values: []float64{21.5}}
	olderSpec := batchSpec{id: "60000000-0000-4000-8000-0000000000c2", sequence: 7001, bootID: "60000000-0000-4000-8000-000000000012", measuredAt: orderBase.Add(-10 * time.Second), intervalUS: 10000, values: []float64{22.5}}
	l0, r0, t0 = counts()
	runLockOrderedRace(validateOrder(newerSpec), validateOrder(olderSpec), "newer/older")
	if l1, r1, t1 := counts(); l1 != l0+2 || r1 != r0+2 || t1 != t0+2 {
		t.Fatalf("newer/older ordering counts ledger/raw/temporal = %d/%d/%d; want %d/%d/%d", l1, r1, t1, l0+2, r0+2, t0+2)
	}
	assertAdvanced("newer/older ordering", newerSpec.bootID, newerSpec.measuredAt, newerSpec.sequence, newerSpec.values[0], orderVersion+1)
	if got := sensorDesired(); got != raceDesired {
		t.Fatalf("newer/older ordering changed the Sensor desired state:\n%s", got)
	}

	// (2) Decisive queue-order check: equal measurement time, same boot, and
	// strictly increasing sequence. The batch queued first (lower sequence) must
	// be granted the lock first and advance; the batch queued second then sees
	// that state and must also advance. A queue inversion would apply only the
	// second batch and leave version at +1, failing the +2 assertion, so the
	// observed lock-wait order is genuinely proven.
	seedSensorState(fmt.Sprintf(`{"temperature":21.5,"unit":"degC","value":1.5,"boot_id":%q,"sequence_num":9000,"observed_at":%q,"sample_interval_us":10000}`, orderBootFifo, orderBase.Format(time.RFC3339Nano)), orderVersion, orderBase)
	l0, r0, t0 = counts()
	runLockOrderedRace(
		validateOrder(batchSpec{id: "60000000-0000-4000-8000-0000000000c3", sequence: 9501, bootID: orderBootFifo, measuredAt: orderBase, intervalUS: 10000, values: []float64{51.5}}),
		validateOrder(batchSpec{id: "60000000-0000-4000-8000-0000000000c4", sequence: 9502, bootID: orderBootFifo, measuredAt: orderBase, intervalUS: 10000, values: []float64{52.5}}),
		"fifo/sequence",
	)
	if l1, r1, t1 := counts(); l1 != l0+2 || r1 != r0+2 || t1 != t0+2 {
		t.Fatalf("fifo/sequence ordering counts ledger/raw/temporal = %d/%d/%d; want %d/%d/%d", l1, r1, t1, l0+2, r0+2, t0+2)
	}
	assertAdvanced("fifo/sequence ordering", orderBootFifo, orderBase, 9502, 52.5, orderVersion+2)

	// Real SQL failures injected after the raw CopyFrom. A twin_states UPDATE
	// failure and a temporal INSERT failure must each roll back ledger, raw,
	// temporal, current and desired state while leaving earlier committed data
	// intact. The triggers are setup-only fixtures removed by cleanup.
	dropFixtureTrigger := func(ddl string) {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCleanup()
		if _, err := setup.Exec(cleanupCtx, ddl); err != nil {
			t.Logf("fixture trigger cleanup failed: %v", err)
		}
	}
	const twinFailDDL = `DROP TRIGGER IF EXISTS stage3_fail_twin_state ON twin_states; DROP FUNCTION IF EXISTS public.stage3_fail_twin_state()`
	seedSensorState(baselineState, orderVersion, orderBase)
	twinBefore := sensorCurrent()
	desiredBefore := sensorDesired()
	l0, r0, t0 = counts()
	if _, err := setup.Exec(ctx, `CREATE FUNCTION public.stage3_fail_twin_state() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture injected twin_states failure'; END $$; CREATE TRIGGER stage3_fail_twin_state BEFORE UPDATE ON twin_states FOR EACH ROW EXECUTE FUNCTION public.stage3_fail_twin_state()`); err != nil {
		t.Fatal(err)
	}
	defer dropFixtureTrigger(twinFailDDL)
	if result, err := repo.Persist(ctx, validateOrder(batchSpec{id: "60000000-0000-4000-8000-0000000000d1", sequence: 8001, bootID: orderBootNewer, measuredAt: orderBase.Add(3 * time.Second), intervalUS: 10000, values: []float64{31.5}})); err == nil || result != (PersistResult{}) {
		t.Fatalf("twin_states UPDATE failure: result=%+v err=%v; want rollback", result, err)
	}
	dropFixtureTrigger(twinFailDDL)
	if got := sensorCurrent(); got != twinBefore {
		t.Fatalf("twin_states failure changed the Sensor current state:\n%s", got)
	}
	if got := sensorDesired(); got != desiredBefore {
		t.Fatalf("twin_states failure changed the Sensor desired state:\n%s", got)
	}
	if l1, r1, t1 := counts(); l1 != l0 || r1 != r0 || t1 != t0 {
		t.Fatalf("twin_states failure leaked ledger/raw/temporal = %d/%d/%d; want %d/%d/%d", l1, r1, t1, l0, r0, t0)
	}

	const temporalFailDDL = `DROP TRIGGER IF EXISTS stage3_fail_temporal ON twin_temporal_values; DROP FUNCTION IF EXISTS public.stage3_fail_temporal()`
	seedSensorState(baselineState, orderVersion, orderBase)
	temporalBefore := sensorCurrent()
	desiredBefore = sensorDesired()
	l0, r0, t0 = counts()
	if _, err := setup.Exec(ctx, `CREATE FUNCTION public.stage3_fail_temporal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture injected temporal failure'; END $$; CREATE TRIGGER stage3_fail_temporal BEFORE INSERT ON twin_temporal_values FOR EACH ROW EXECUTE FUNCTION public.stage3_fail_temporal()`); err != nil {
		t.Fatal(err)
	}
	defer dropFixtureTrigger(temporalFailDDL)
	if result, err := repo.Persist(ctx, validateOrder(batchSpec{id: "60000000-0000-4000-8000-0000000000d2", sequence: 8002, bootID: orderBootNewer, measuredAt: orderBase.Add(4 * time.Second), intervalUS: 10000, values: []float64{32.5}})); err == nil || result != (PersistResult{}) {
		t.Fatalf("temporal INSERT failure: result=%+v err=%v; want rollback", result, err)
	}
	dropFixtureTrigger(temporalFailDDL)
	if got := sensorCurrent(); got != temporalBefore {
		t.Fatalf("temporal failure did not roll back the Sensor current state:\n%s", got)
	}
	if got := sensorDesired(); got != desiredBefore {
		t.Fatalf("temporal failure changed the Sensor desired state:\n%s", got)
	}
	if l1, r1, t1 := counts(); l1 != l0 || r1 != r0 || t1 != t0 {
		t.Fatalf("temporal failure leaked ledger/raw/temporal = %d/%d/%d; want %d/%d/%d", l1, r1, t1, l0, r0, t0)
	}

	// The maximum exactly representable interval (2^53) round-trips through the
	// temporal DOUBLE PRECISION column and the reported-state JSON exactly.
	seedSensorState(baselineState, orderVersion, orderBase)
	l0, r0, t0 = counts()
	const bigInterval int64 = 1 << 53
	bigID := "60000000-0000-4000-8000-0000000000e1"
	persistOrder(batchSpec{id: bigID, sequence: 9001, bootID: orderBootNewer, measuredAt: orderBase.Add(5 * time.Second), intervalUS: bigInterval, values: []float64{41.5}})
	var storedInterval float64
	if err := setup.QueryRow(ctx, `SELECT value_number FROM twin_temporal_values WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1) AND message_id=$2`, sensorEntity, mustUUID(bigID)).Scan(&storedInterval); err != nil {
		t.Fatal(err)
	}
	if storedInterval != float64(bigInterval) {
		t.Fatalf("interval round-trip value_number = %v; want %d", storedInterval, bigInterval)
	}
	var bigReportedInterval string
	if err := setup.QueryRow(ctx, `SELECT s.reported_state->>'sample_interval_us' FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=$1`, sensorEntity).Scan(&bigReportedInterval); err != nil {
		t.Fatal(err)
	}
	if bigReportedInterval != fmt.Sprintf("%d", bigInterval) {
		t.Fatalf("reported sample_interval_us = %q; want %d", bigReportedInterval, bigInterval)
	}
	assertAdvanced("interval round-trip", orderBootNewer, orderBase.Add(5*time.Second), 9001, 41.5, orderVersion+1)
	if l1, r1, t1 := counts(); l1 != l0+1 || r1 != r0+1 || t1 != t0+1 {
		t.Fatalf("interval round-trip counts ledger/raw/temporal = %d/%d/%d; want %d/%d/%d", l1, r1, t1, l0+1, r0+1, t0+1)
	}

	// No ordering, rollback, or interval case above may alter the Sensor desired
	// state, its version, or its audit fields.
	if got := sensorDesired(); got != sensorDesiredBefore {
		t.Fatalf("Sensor desired state changed across ordering/rollback cases:\n%s", got)
	}
	fmt.Println("INTEGRATION CASES PASS: 100 validated rows, retries/conflicts, graph rejection, overlap/adjacent/gap, COPY rollback, same-ID and range concurrency, application role, atomic Sensor reported-state advance + one sample_interval_us temporal per new batch, desired/Gateway preservation, DB ordering (newer/older/equal boot+sequence, legacy provenance), deterministic two-connection Sensor-lock ordering, twin_states/temporal SQL rollback, 2^53 interval round-trip")
}

func mustUUID(s string) uuid.UUID { return uuid.MustParse(s) }

func snapshotRows(ctx context.Context, t *testing.T, pool *pgxpool.Pool, query string) string {
	t.Helper()
	rows, err := pool.Query(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			t.Fatal(err)
		}
		result += row + "\n"
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func runCompressedChunkCase(ctx context.Context, t *testing.T, setup, app *pgxpool.Pool) {
	t.Helper()
	var legacyMin time.Time
	if err := setup.QueryRow(ctx, `SELECT min(legacy.measured_at) FROM stage3_legacy_telemetry_snapshot legacy JOIN timescaledb_information.chunks chunk ON chunk.hypertable_name='telemetry' AND legacy.measured_at >= chunk.range_start AND legacy.measured_at < chunk.range_end AND chunk.is_compressed WHERE legacy.gateway_id='fixture_gw'`).Scan(&legacyMin); err != nil {
		t.Fatal(err)
	}
	measuredAt := legacyMin.Add(time.Minute)
	values := []float64{77.25, 78.5}
	batch := BatchDTO{ProtocolVersion: 1, MessageID: mustUUID("30000000-0000-4000-8000-000000000001"), MessageType: "telemetry_batch", GatewayID: "fixture_gw", SensorID: "fixture_sensor", BootID: mustUUID("40000000-0000-4000-8000-000000000001"), FirstSequence: 60001, SampleCount: 2, MeasuredAt: measuredAt.Format(time.RFC3339Nano), SampleIntervalUS: 10000, Samples: values}
	payload, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := NewValidator(Limits{MaxPayloadBytes: 1 << 20, MaxSamples: 100, MaxBatchDuration: time.Hour, MaxFutureSkew: 0})
	if err != nil {
		t.Fatal(err)
	}
	message, err := validator.Validate("gateways/fixture_gw/telemetry/fixture_sensor", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	repo, err := NewPostgresRepository(app, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := repo.Persist(ctx, message)
	if err != nil {
		t.Fatalf("late write into compressed legacy chunk failed: %v", err)
	}
	if result.Outcome != CommitOutcomeCommitted || result.SamplesSaved != 2 {
		t.Fatalf("late write result: %+v", result)
	}
	var count int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM telemetry WHERE gateway_id='fixture_gw' AND message_id=$1", batch.MessageID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("late samples=%d err=%v", count, err)
	}
	t.Logf("late-data CopyFrom accepted in compressed chunk; legacy_min=%s", legacyMin.UTC().Format(time.RFC3339Nano))
	fmt.Println("INTEGRATION CASE PASS: late-data insert into legacy compressed Timescale chunk")
}

func runLegacyMarkerCase(ctx context.Context, t *testing.T, setup, app *pgxpool.Pool) {
	t.Helper()
	var markerBefore string
	var rawBefore int
	legacyID := mustUUID("00000000-0000-4000-8000-000000000001")
	if err := setup.QueryRow(ctx, "SELECT row(gateway_id,message_id,payload_hash,processed_at)::text FROM processed_messages WHERE gateway_id='fixture_gw' AND message_id=$1", legacyID).Scan(&markerBefore); err != nil {
		t.Fatal(err)
	}
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM telemetry WHERE gateway_id='fixture_gw'").Scan(&rawBefore); err != nil {
		t.Fatal(err)
	}
	batch := BatchDTO{ProtocolVersion: 1, MessageID: legacyID, MessageType: "telemetry_batch", GatewayID: "fixture_gw", SensorID: "fixture_sensor", BootID: mustUUID("50000000-0000-4000-8000-000000000001"), FirstSequence: 90001, SampleCount: 1, MeasuredAt: time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond).Format(time.RFC3339Nano), SampleIntervalUS: 10000, Samples: []float64{3.14}}
	payload, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := NewValidator(Limits{MaxPayloadBytes: 1 << 20, MaxSamples: 10, MaxBatchDuration: time.Hour, MaxFutureSkew: 0})
	if err != nil {
		t.Fatal(err)
	}
	message, err := validator.Validate("gateways/fixture_gw/telemetry/fixture_sensor", payload, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	repo, err := NewPostgresRepository(app, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Persist(ctx, message)
	var reason *ReasonError
	if !errors.As(err, &reason) || reason.Code != ReasonLegacyProvenanceUnknown {
		t.Fatalf("legacy marker replay error: %v", err)
	}
	var markerAfter string
	var rawAfter int
	if err := setup.QueryRow(ctx, "SELECT row(gateway_id,message_id,payload_hash,processed_at)::text FROM processed_messages WHERE gateway_id='fixture_gw' AND message_id=$1", legacyID).Scan(&markerAfter); err != nil {
		t.Fatal(err)
	}
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM telemetry WHERE gateway_id='fixture_gw'").Scan(&rawAfter); err != nil {
		t.Fatal(err)
	}
	if markerAfter != markerBefore || rawAfter != rawBefore {
		t.Fatalf("legacy retry changed data: marker %s -> %s raw %d -> %d", markerBefore, markerAfter, rawBefore, rawAfter)
	}
	fmt.Println("INTEGRATION CASE PASS: legacy marker replay returns legacy_provenance_unknown unchanged")
}

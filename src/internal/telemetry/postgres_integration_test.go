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
	twinStateBefore := snapshotRows(ctx, t, setup, `SELECT row(s.reported_state,s.desired_state,s.reported_version,s.desired_version,s.last_reported_at,s.last_desired_at,s.updated_at)::text FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id='urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor'`)
	var twinTemporalBefore int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_temporal_values").Scan(&twinTemporalBefore); err != nil {
		t.Fatal(err)
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
	expectedLedger, expectedRaw = raceLedger, raceRaw
	ledgerSnapshot = snapshotRows(ctx, t, setup, `SELECT row(gateway_id,message_id,payload_hash,source_topic,sensor_id,boot_id,first_sequence,last_sequence,sample_count)::text FROM processed_messages ORDER BY gateway_id,message_id`)
	rawSnapshot = snapshotRows(ctx, t, setup, `SELECT row(measured_at,gateway_id,sensor_id,sequence_num,value,boot_id,message_id,sample_index)::text FROM telemetry ORDER BY measured_at,gateway_id,message_id,sample_index`)
	// Twin boundary: persistence does not mutate current state or temporal history.
	var statesBefore, temporalBefore int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_states").Scan(&statesBefore); err != nil {
		t.Fatal(err)
	}
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_temporal_values").Scan(&temporalBefore); err != nil {
		t.Fatal(err)
	}
	if statesBefore != 5 || temporalBefore != 0 {
		t.Fatalf("unexpected fixture Twin baseline states=%d temporal=%d", statesBefore, temporalBefore)
	}
	var statesAfter, temporalAfter int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_states").Scan(&statesAfter); err != nil {
		t.Fatal(err)
	}
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_temporal_values").Scan(&temporalAfter); err != nil {
		t.Fatal(err)
	}
	if statesAfter != statesBefore || temporalAfter != twinTemporalBefore {
		t.Fatalf("Twin changed: states %d->%d temporal %d->%d", statesBefore, statesAfter, temporalBefore, temporalAfter)
	}
	if current := snapshotRows(ctx, t, setup, `SELECT row(s.reported_state,s.desired_state,s.reported_version,s.desired_version,s.last_reported_at,s.last_desired_at,s.updated_at)::text FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id='urn:ngsi-ld:Sensor:fixture_gw:fixture_sensor'`); current != twinStateBefore {
		t.Fatal("reported/desired state or versions changed during Task 3.2 persistence")
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
	fmt.Println("INTEGRATION CASES PASS: 100 validated rows, retries/conflicts, graph rejection, overlap/adjacent/gap, COPY rollback, same-ID and range concurrency, application role, Twin boundary")
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

package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var reportedStateTestEntityID = uuid.MustParse("aaaaaaaa-0000-4000-8000-000000000001")

// reportedStateTestCandidateTime is the measurement time of the last sample
// produced by reportedStateTestMessage: the base measurement time plus one
// 10 ms sample interval.
func reportedStateTestCandidateTime() time.Time {
	return time.Date(2026, 8, 21, 10, 15, 0, 20_000_000, time.UTC)
}

func reportedStateTestResource(unit *string) resolvedTelemetryResource {
	return resolvedTelemetryResource{
		GatewayID: "gateway_001",
		SensorID:  "sensor_001",
		EntityID:  reportedStateTestEntityID,
		Unit:      unit,
	}
}

func reportedStateTestMessage(t *testing.T, measuredAt string, firstSequence int64, boot string, values []float64) *Message {
	t.Helper()
	batch := BatchDTO{
		ProtocolVersion:  1,
		MessageID:        mustUUID("20000000-0000-4000-8000-0000000000aa"),
		MessageType:      "telemetry_batch",
		GatewayID:        "gateway_001",
		SensorID:         "sensor_001",
		BootID:           mustUUID(boot),
		FirstSequence:    firstSequence,
		SampleCount:      int32(len(values)),
		MeasuredAt:       measuredAt,
		SampleIntervalUS: 10000,
		Samples:          values,
	}
	payload, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := NewValidator(messageTestLimits())
	if err != nil {
		t.Fatal(err)
	}
	message, err := validator.Validate("gateways/gateway_001/telemetry/sensor_001", payload, messageTestReceivedAt())
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func TestNewReportedStateCandidateAllowlistAndMerge(t *testing.T) {
	unit := "degC"
	message := reportedStateTestMessage(t, "2026-08-21T10:15:00Z", 12501, decodeTestBootID, []float64{25.1, 25.2, 25.3})

	for _, test := range []struct {
		name string
		unit *string
	}{
		{name: "with provisioned unit", unit: &unit},
		{name: "without provisioned unit", unit: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate, err := newReportedStateCandidate(reportedStateTestResource(test.unit), message)
			if err != nil {
				t.Fatal(err)
			}
			if candidate.value != 25.3 || !candidate.observedAt.Equal(reportedStateTestCandidateTime()) ||
				candidate.bootID != mustUUID(decodeTestBootID) || candidate.sequenceNum != 12503 ||
				candidate.sampleIntervalUS != 10000 {
				t.Fatalf("candidate = %#v", candidate)
			}
			want := map[string]any{
				"value":              25.3,
				"observed_at":        "2026-08-21T10:15:00.02Z",
				"boot_id":            decodeTestBootID,
				"sequence_num":       int64(12503),
				"sample_interval_us": int64(10000),
			}
			if test.unit != nil {
				want["unit"] = *test.unit
			}
			if got := candidate.merge(); !reflect.DeepEqual(got, want) {
				t.Fatalf("merge() = %#v; want allowlisted keys only %#v", got, want)
			}
		})
	}
}

func TestNewReportedStateCandidateRejectsInvalidInput(t *testing.T) {
	resource := reportedStateTestResource(nil)
	if _, err := newReportedStateCandidate(resource, nil); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("nil message error = %v; want invalid_payload", err)
	}
	if _, err := newReportedStateCandidate(resource, &Message{}); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("no sample error = %v; want invalid_payload", err)
	}
	manual := &Message{batch: BatchDTO{SampleIntervalUS: 0}, samples: []SampleCandidate{{Value: 1}}}
	if _, err := newReportedStateCandidate(resource, manual); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("non-positive interval error = %v; want invalid_payload", err)
	}
}

func TestApplySensorReportedStateAdvancesUnderLock(t *testing.T) {
	unit := "degC"
	message := reportedStateTestMessage(t, "2026-08-21T10:15:00Z", 12501, decodeTestBootID, []float64{25.1, 25.2, 25.3})
	earlier := reportedStateTestCandidateTime().Add(-time.Second)
	tx := &telemetryFakeTx{
		sensorState:        []byte(fmt.Sprintf(`{"temperature":21.5,"configuration":{"sample_interval_us":25000},"boot_id":%q,"sequence_num":9000}`, decodeTestBootID)),
		sensorVersion:      7,
		sensorLastReported: &earlier,
		execRows:           1,
	}
	advanced, err := applySensorReportedState(context.Background(), tx, reportedStateTestResource(&unit), message)
	if err != nil {
		t.Fatal(err)
	}
	if !advanced {
		t.Fatal("applySensorReportedState() advanced = false; want true")
	}
	if tx.execCalls != 1 || tx.execQuery != sensorReportedStateAdvanceSQL {
		t.Fatalf("exec calls=%d query=%q", tx.execCalls, tx.execQuery)
	}
	if len(tx.execArgs) != 4 {
		t.Fatalf("exec args = %#v; want entity, merge, observed_at, version", tx.execArgs)
	}
	if tx.execArgs[0] != reportedStateTestEntityID {
		t.Fatalf("entity arg = %#v", tx.execArgs[0])
	}
	merged, ok := tx.execArgs[1].(map[string]any)
	if !ok || merged["value"] != 25.3 || merged["boot_id"] != decodeTestBootID ||
		merged["sequence_num"] != int64(12503) || merged["sample_interval_us"] != int64(10000) ||
		merged["unit"] != unit || merged["observed_at"] != "2026-08-21T10:15:00.02Z" {
		t.Fatalf("merge arg = %#v", tx.execArgs[1])
	}
	if observed, ok := tx.execArgs[2].(time.Time); !ok || !observed.Equal(reportedStateTestCandidateTime()) {
		t.Fatalf("observed arg = %#v", tx.execArgs[2])
	}
	if tx.execArgs[3] != int64(7) {
		t.Fatalf("version arg = %#v; want locked version 7", tx.execArgs[3])
	}
	if _, overwritesConfiguration := merged["configuration"]; overwritesConfiguration || !strings.Contains(sensorReportedStateAdvanceSQL, "reported_state = reported_state ||") {
		t.Fatalf("telemetry merge must preserve existing reported.configuration: merge=%#v SQL=%q", merged, sensorReportedStateAdvanceSQL)
	}
}

func TestApplySensorReportedStateOrdering(t *testing.T) {
	message := reportedStateTestMessage(t, "2026-08-21T10:15:00Z", 12501, decodeTestBootID, []float64{25.1, 25.2, 25.3})
	candidateTime := reportedStateTestCandidateTime()
	before := candidateTime.Add(-time.Second)
	after := candidateTime.Add(time.Second)
	const otherBoot = "8f2c45f7-0a76-4e28-8a37-7793d7d85b04"

	state := func(body string) []byte { return []byte(body) }

	for _, test := range []struct {
		name           string
		state          []byte
		lastReportedAt *time.Time
		want           bool
	}{
		{name: "null last_reported_at advances", state: state(`{}`), lastReportedAt: nil, want: true},
		{name: "newer timestamp advances", state: state(`{}`), lastReportedAt: &before, want: true},
		{name: "older timestamp holds", state: state(fmt.Sprintf(`{"boot_id":%q,"sequence_num":1}`, decodeTestBootID)), lastReportedAt: &after, want: false},
		{name: "equal timestamp same boot larger sequence advances", state: state(fmt.Sprintf(`{"boot_id":%q,"sequence_num":12502}`, decodeTestBootID)), lastReportedAt: &candidateTime, want: true},
		{name: "equal timestamp same boot equal sequence holds", state: state(fmt.Sprintf(`{"boot_id":%q,"sequence_num":12503}`, decodeTestBootID)), lastReportedAt: &candidateTime, want: false},
		{name: "equal timestamp same boot smaller sequence holds", state: state(fmt.Sprintf(`{"boot_id":%q,"sequence_num":12504}`, decodeTestBootID)), lastReportedAt: &candidateTime, want: false},
		{name: "equal timestamp different boot holds", state: state(fmt.Sprintf(`{"boot_id":%q,"sequence_num":0}`, otherBoot)), lastReportedAt: &candidateTime, want: false},
		{name: "equal timestamp missing boot holds", state: state(`{"sequence_num":1}`), lastReportedAt: &candidateTime, want: false},
		{name: "equal timestamp malformed boot holds", state: state(`{"boot_id":"not-a-uuid","sequence_num":1}`), lastReportedAt: &candidateTime, want: false},
		{name: "equal timestamp missing sequence holds", state: state(fmt.Sprintf(`{"boot_id":%q}`, decodeTestBootID)), lastReportedAt: &candidateTime, want: false},
		{name: "equal timestamp string sequence holds", state: state(fmt.Sprintf(`{"boot_id":%q,"sequence_num":"12400"}`, decodeTestBootID)), lastReportedAt: &candidateTime, want: false},
		{name: "equal timestamp negative sequence holds", state: state(fmt.Sprintf(`{"boot_id":%q,"sequence_num":-1}`, decodeTestBootID)), lastReportedAt: &candidateTime, want: false},
		{name: "equal timestamp non-object state holds", state: state(`[1,2,3]`), lastReportedAt: &candidateTime, want: false},
		{name: "equal timestamp malformed json holds", state: state(`{not json`), lastReportedAt: &candidateTime, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx := &telemetryFakeTx{
				sensorState:        test.state,
				sensorVersion:      7,
				sensorLastReported: test.lastReportedAt,
				execRows:           1,
			}
			advanced, err := applySensorReportedState(context.Background(), tx, reportedStateTestResource(nil), message)
			if err != nil {
				t.Fatal(err)
			}
			if advanced != test.want {
				t.Fatalf("advanced = %v; want %v", advanced, test.want)
			}
			wantExec := 0
			if test.want {
				wantExec = 1
			}
			if tx.execCalls != wantExec {
				t.Fatalf("exec calls = %d; want %d", tx.execCalls, wantExec)
			}
		})
	}
}

func TestApplySensorReportedStateFailures(t *testing.T) {
	message := reportedStateTestMessage(t, "2026-08-21T10:15:00Z", 12501, decodeTestBootID, []float64{25.1, 25.2, 25.3})
	resource := reportedStateTestResource(nil)
	before := reportedStateTestCandidateTime().Add(-time.Second)

	t.Run("missing twin state row errors", func(t *testing.T) {
		tx := &telemetryFakeTx{sensorReadErr: pgx.ErrNoRows}
		advanced, err := applySensorReportedState(context.Background(), tx, resource, message)
		if advanced || reasonCode(err) != ReasonResourceGraphIncomplete {
			t.Fatalf("advanced=%v err=%v; want graph-incomplete error", advanced, err)
		}
	})
	t.Run("negative version errors", func(t *testing.T) {
		tx := &telemetryFakeTx{sensorState: []byte(`{}`), sensorVersion: -1}
		advanced, err := applySensorReportedState(context.Background(), tx, resource, message)
		if advanced || reasonCode(err) != ReasonResourceGraphIncomplete {
			t.Fatalf("advanced=%v err=%v; want graph-incomplete error", advanced, err)
		}
	})
	t.Run("read error is dependency", func(t *testing.T) {
		tx := &telemetryFakeTx{sensorReadErr: errors.New("read failed")}
		advanced, err := applySensorReportedState(context.Background(), tx, resource, message)
		if advanced || reasonCode(err) != ReasonDependencyUnavailable || !strings.Contains(err.Error(), "read failed") {
			t.Fatalf("advanced=%v err=%v; want dependency error with cause", advanced, err)
		}
	})
	t.Run("update error is dependency", func(t *testing.T) {
		tx := &telemetryFakeTx{sensorState: []byte(`{}`), sensorVersion: 7, sensorLastReported: &before, execRows: 1, execErr: errors.New("update failed")}
		advanced, err := applySensorReportedState(context.Background(), tx, resource, message)
		if advanced || reasonCode(err) != ReasonDependencyUnavailable || !strings.Contains(err.Error(), "update failed") {
			t.Fatalf("advanced=%v err=%v; want dependency error with cause", advanced, err)
		}
	})
	t.Run("unexpected row count errors", func(t *testing.T) {
		tx := &telemetryFakeTx{sensorState: []byte(`{}`), sensorVersion: 7, sensorLastReported: &before, execRows: 0}
		advanced, err := applySensorReportedState(context.Background(), tx, resource, message)
		if advanced || reasonCode(err) != ReasonResourceGraphIncomplete {
			t.Fatalf("advanced=%v err=%v; want graph-incomplete error", advanced, err)
		}
	})
}

func TestApplySensorReportedStateRejectsInvalidInput(t *testing.T) {
	message := reportedStateTestMessage(t, "2026-08-21T10:15:00Z", 12501, decodeTestBootID, []float64{1})
	resource := reportedStateTestResource(nil)

	if advanced, err := applySensorReportedState(context.Background(), nil, resource, message); advanced || reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("nil tx advanced=%v err=%v; want invalid_payload", advanced, err)
	}
	zeroEntity := resource
	zeroEntity.EntityID = uuid.Nil
	tx := &telemetryFakeTx{}
	if advanced, err := applySensorReportedState(context.Background(), tx, zeroEntity, message); advanced || reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("zero entity advanced=%v err=%v; want invalid_payload", advanced, err)
	}
	if advanced, err := applySensorReportedState(context.Background(), tx, resource, nil); advanced || reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("nil message advanced=%v err=%v; want invalid_payload", advanced, err)
	}
	if tx.queryCalls != 0 || tx.execCalls != 0 {
		t.Fatalf("invalid input reached the database: query=%d exec=%d", tx.queryCalls, tx.execCalls)
	}
}

// TestSensorProvenanceAdvancesBoundaries pins the pure ordering core shared by
// the single-message path and the group projection, including the legacy branches
// that hold the current state instead of failing a valid batch.
func TestSensorProvenanceAdvancesBoundaries(t *testing.T) {
	message := reportedStateTestMessage(t, "2026-08-21T10:15:00Z", 12501, decodeTestBootID, []float64{25.1, 25.2, 25.3})
	candidate, err := newReportedStateCandidate(reportedStateTestResource(nil), message)
	if err != nil {
		t.Fatal(err)
	}
	candidateTime := reportedStateTestCandidateTime()
	newer := candidateTime.Add(time.Second)
	older := candidateTime.Add(-time.Second)
	const otherBoot = "8f2c45f7-0a76-4e28-8a37-7793d7d85b04"

	for _, test := range []struct {
		name           string
		lastReportedAt *time.Time
		storedBoot     string
		storedSequence int64
		hasProvenance  bool
		want           bool
	}{
		{name: "missing last report advances", lastReportedAt: nil, want: true},
		{name: "newer measurement advances", lastReportedAt: &older, hasProvenance: true, storedBoot: decodeTestBootID, storedSequence: 1, want: true},
		{name: "older measurement holds", lastReportedAt: &newer, hasProvenance: true, storedBoot: decodeTestBootID, storedSequence: 1, want: false},
		{name: "equal time same boot smaller sequence advances", lastReportedAt: &candidateTime, hasProvenance: true, storedBoot: decodeTestBootID, storedSequence: 12502, want: true},
		{name: "equal time same boot equal sequence holds", lastReportedAt: &candidateTime, hasProvenance: true, storedBoot: decodeTestBootID, storedSequence: 12503, want: false},
		{name: "equal time different boot holds", lastReportedAt: &candidateTime, hasProvenance: true, storedBoot: otherBoot, storedSequence: 0, want: false},
		{name: "equal time malformed boot holds", lastReportedAt: &candidateTime, hasProvenance: true, storedBoot: "not-a-uuid", storedSequence: 0, want: false},
		{name: "equal time missing provenance holds", lastReportedAt: &candidateTime, hasProvenance: false, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := sensorProvenanceAdvances(test.lastReportedAt, test.storedBoot, test.storedSequence, test.hasProvenance, candidate); got != test.want {
				t.Fatalf("sensorProvenanceAdvances() = %v; want %v", got, test.want)
			}
		})
	}
}

// TestReadSensorReportedStatesBatchBuildsProjectionSeeds pins the read batch used
// by the group path: one statement per entity, ordered, with provenance parsed
// from the reported state so the projection can replay the sequential ordering.
func TestReadSensorReportedStatesBatchBuildsProjectionSeeds(t *testing.T) {
	state := []byte(fmt.Sprintf(`{"boot_id":%q,"sequence_num":9000}`, decodeTestBootID))
	lastReported := reportedStateTestCandidateTime().Add(-time.Second)
	tx := &telemetryFakeTx{sensorState: state, sensorVersion: 7, sensorLastReported: &lastReported}
	entities := []uuid.UUID{reportedStateTestEntityID, uuid.MustParse("aaaaaaaa-0000-4000-8000-000000000002")}
	snapshots, err := readSensorReportedStatesBatch(context.Background(), tx, entities)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != len(entities) || len(tx.batches) != 1 || len(tx.batches[0]) != len(entities) {
		t.Fatalf("snapshots=%d batches=%#v; want one read batch with one statement per entity", len(snapshots), tx.batches)
	}
	for _, entity := range entities {
		snapshot, ok := snapshots[entity]
		if !ok {
			t.Fatalf("snapshot missing for %s", entity)
		}
		if snapshot.entityID != entity || snapshot.version != 7 || !snapshot.hasProvenance ||
			snapshot.bootID != decodeTestBootID || snapshot.sequence != 9000 ||
			snapshot.lastReportedAt == nil || !snapshot.lastReportedAt.Equal(lastReported) {
			t.Fatalf("snapshot for %s = %#v", entity, snapshot)
		}
	}
	if tx.openBatches != 0 {
		t.Fatalf("openBatches=%d; the read batch must be closed", tx.openBatches)
	}
	// The merge is `reported_state || $2::jsonb`, which stays an object only when
	// the stored value already is one, so only a JSON object keeps readable
	// provenance after an advance.
	for _, test := range []struct {
		name  string
		state []byte
		want  bool
	}{
		{name: "object", state: []byte(fmt.Sprintf(`{"boot_id":%q,"sequence_num":9000}`, decodeTestBootID)), want: true},
		{name: "empty object", state: []byte(`{}`), want: true},
		{name: "empty bytes", state: []byte(``), want: false},
		{name: "null", state: []byte(`null`), want: false},
		{name: "array", state: []byte(`[]`), want: false},
		{name: "scalar", state: []byte(`7`), want: false},
		{name: "string", state: []byte(`"legacy"`), want: false},
	} {
		t.Run("reported_state shape "+test.name, func(t *testing.T) {
			shapeTx := &telemetryFakeTx{sensorState: test.state, sensorVersion: 3}
			shapeSnapshots, err := readSensorReportedStatesBatch(context.Background(), shapeTx, []uuid.UUID{reportedStateTestEntityID})
			if err != nil {
				t.Fatal(err)
			}
			if got := shapeSnapshots[reportedStateTestEntityID].objectShaped; got != test.want {
				t.Fatalf("objectShaped(%s) = %v; want %v", test.state, got, test.want)
			}
		})
	}
	if _, err := readSensorReportedStatesBatch(context.Background(), tx, []uuid.UUID{uuid.Nil}); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("nil entity error = %v; want invalid_payload", err)
	}
}

// TestPlanReportedStateAdvancesProjectsSameSensorGroup pins the projection: three
// messages for one Sensor emit three plans whose versions follow the seeded
// snapshot, and a message that does not advance emits none.
func TestPlanReportedStateAdvancesProjectsSameSensorGroup(t *testing.T) {
	resources := map[telemetryResourceKey]resolvedTelemetryResource{
		{"gateway_001", "sensor_001"}: reportedStateTestResource(nil),
	}
	seed := reportedStateTestCandidateTime().Add(-time.Minute)
	snapshots := map[uuid.UUID]reportedStateSnapshot{
		reportedStateTestEntityID: {
			entityID:       reportedStateTestEntityID,
			version:        41,
			lastReportedAt: &seed,
			bootID:         decodeTestBootID,
			sequence:       12500,
			hasProvenance:  true,
			objectShaped:   true,
		},
	}
	messages := []*Message{
		reportedStateTestMessage(t, "2026-08-21T10:15:00Z", 12501, decodeTestBootID, []float64{1, 2, 3}),
		reportedStateTestMessage(t, "2026-08-21T10:15:00Z", 12504, decodeTestBootID, []float64{4, 5, 6}),
		reportedStateTestMessage(t, "2026-08-21T10:15:00Z", 12507, decodeTestBootID, []float64{7, 8, 9}),
	}
	plans, err := planReportedStateAdvances(messages, []int{0, 1, 2}, resources, snapshots)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 3 {
		t.Fatalf("plans=%d; want one per advancing message", len(plans))
	}
	for index, plan := range plans {
		if plan.entityID != reportedStateTestEntityID || plan.version != int64(41+index) {
			t.Fatalf("plan[%d] = %#v; want entity and version %d", index, plan, 41+index)
		}
	}
	held, err := planReportedStateAdvances(messages, []int{0}, resources, snapshots)
	if err != nil {
		t.Fatal(err)
	}
	if len(held) != 1 {
		t.Fatalf("held plans=%d; want the single advance", len(held))
	}
	// An older measurement does not advance, so the projection is untouched and
	// no plan is emitted for it.
	olderMessage := reportedStateTestMessage(t, "2026-08-21T10:14:00Z", 12501, decodeTestBootID, []float64{1})
	holdPlans, err := planReportedStateAdvances([]*Message{olderMessage}, []int{0}, resources, snapshots)
	if err != nil {
		t.Fatal(err)
	}
	if len(holdPlans) != 0 {
		t.Fatalf("holding plans=%#v; want none when the candidate does not advance", holdPlans)
	}
}

// TestPlanReportedStateAdvancesHoldsSecondEqualTimestampOnNonObjectState pins the
// projection's JSON shape handling. A legacy array reported_state merges into
// another non-object, so its provenance stays unreadable and an equal-timestamp
// follow-up message must hold, exactly like the sequential path that re-reads the
// merged row.
func TestPlanReportedStateAdvancesHoldsSecondEqualTimestampOnNonObjectState(t *testing.T) {
	old := reportedStateTestCandidateTime().Add(-time.Minute)
	tx := &telemetryFakeTx{sensorState: []byte(`[]`), sensorVersion: 7, sensorLastReported: &old}
	snapshots, err := readSensorReportedStatesBatch(context.Background(), tx, []uuid.UUID{reportedStateTestEntityID})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ok := snapshots[reportedStateTestEntityID]
	if !ok {
		t.Fatalf("snapshots=%#v; want the read entity", snapshots)
	}
	if snapshot.objectShaped || snapshot.hasProvenance || snapshot.version != 7 {
		t.Fatalf("snapshot = %#v; want a non-object legacy state without readable provenance", snapshot)
	}
	resources := map[telemetryResourceKey]resolvedTelemetryResource{
		{"gateway_001", "sensor_001"}: reportedStateTestResource(nil),
	}
	// Both messages end on the same measurement time; only their final sequences
	// differ, so the second message can only win through stored provenance.
	first := reportedStateTestMessage(t, "2026-08-21T10:15:00Z", 12501, decodeTestBootID, []float64{1, 2, 3})
	second := reportedStateTestMessage(t, "2026-08-21T10:15:00.010Z", 12601, decodeTestBootID, []float64{4, 5})
	firstLast, _ := first.LastSample()
	secondLast, _ := second.LastSample()
	if !firstLast.MeasuredAt.Equal(secondLast.MeasuredAt) {
		t.Fatalf("test setup requires equal final sample times: %s vs %s", firstLast.MeasuredAt, secondLast.MeasuredAt)
	}
	plans, err := planReportedStateAdvances([]*Message{first, second}, []int{0, 1}, resources, snapshots)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 1 {
		t.Fatalf("plans=%d (%#v); want only the first message to advance, because the merge cannot restore provenance from a non-object state", len(plans), plans)
	}
	if plans[0].version != 7 || plans[0].entityID != reportedStateTestEntityID {
		t.Fatalf("plan = %#v; want the snapshot version 7 for the read entity", plans[0])
	}
}

func TestSensorReportedStateSQLKeepsAllowlistAndLock(t *testing.T) {
	if !strings.Contains(sensorReportedStateReadSQL, "FOR UPDATE") {
		t.Fatalf("read SQL must lock the row: %s", sensorReportedStateReadSQL)
	}
	if !strings.Contains(sensorReportedStateAdvanceSQL, "reported_state || $2::jsonb") {
		t.Fatalf("advance SQL must merge keys: %s", sensorReportedStateAdvanceSQL)
	}
	if !strings.Contains(sensorReportedStateAdvanceSQL, "reported_version = reported_version + 1") {
		t.Fatalf("advance SQL must increment exactly once: %s", sensorReportedStateAdvanceSQL)
	}
	if strings.Contains(strings.ToLower(sensorReportedStateAdvanceSQL), "desired") ||
		strings.Contains(strings.ToLower(sensorReportedStateReadSQL), "desired") {
		t.Fatal("reported-state SQL must never touch desired state")
	}
}

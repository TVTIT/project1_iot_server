package telemetry

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func temporalTestMessageID() uuid.UUID {
	return mustUUID("20000000-0000-4000-8000-0000000000aa")
}

func TestAppendSensorSampleIntervalTemporalInsertsOneRow(t *testing.T) {
	message := reportedStateTestMessage(t, "2026-08-21T10:15:00Z", 12501, decodeTestBootID, []float64{1, 2, 3})
	tx := &telemetryFakeTx{execRows: 1}

	if err := appendSensorSampleIntervalTemporal(context.Background(), tx, reportedStateTestResource(nil), message); err != nil {
		t.Fatal(err)
	}
	if tx.execCalls != 1 || tx.execQuery != sensorSampleIntervalTemporalInsertSQL {
		t.Fatalf("exec calls=%d query=%q", tx.execCalls, tx.execQuery)
	}
	if len(tx.execArgs) != 5 {
		t.Fatalf("exec args = %#v; want observed_at, entity_id, value_number, metadata, message_id", tx.execArgs)
	}
	if observed, ok := tx.execArgs[0].(time.Time); !ok || !observed.Equal(reportedStateTestCandidateTime()) {
		t.Fatalf("observed_at arg = %#v; want the final sample's UTC time", tx.execArgs[0])
	}
	if tx.execArgs[1] != reportedStateTestEntityID {
		t.Fatalf("entity arg = %#v; want the resolved Sensor entity", tx.execArgs[1])
	}
	if interval, ok := tx.execArgs[2].(float64); !ok || interval != 10000 {
		t.Fatalf("value_number arg = %#v; want 10000", tx.execArgs[2])
	}
	metadata, ok := tx.execArgs[3].(map[string]any)
	if !ok || metadata["unit"] != "us" || metadata["boot_id"] != decodeTestBootID || metadata["source"] != "telemetry_batch" {
		t.Fatalf("metadata arg = %#v; want unit/boot_id/source only", tx.execArgs[3])
	}
	if tx.execArgs[4] != temporalTestMessageID() {
		t.Fatalf("message_id arg = %#v; want the batch message_id", tx.execArgs[4])
	}
}

func TestAppendSensorSampleIntervalTemporalRejectsInvalidInput(t *testing.T) {
	message := reportedStateTestMessage(t, "2026-08-21T10:15:00Z", 12501, decodeTestBootID, []float64{1})
	resource := reportedStateTestResource(nil)
	tx := &telemetryFakeTx{}

	if err := appendSensorSampleIntervalTemporal(context.Background(), nil, resource, message); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("nil tx error = %v; want invalid_payload", err)
	}
	zeroEntity := resource
	zeroEntity.EntityID = uuid.Nil
	if err := appendSensorSampleIntervalTemporal(context.Background(), tx, zeroEntity, message); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("zero entity error = %v; want invalid_payload", err)
	}
	if err := appendSensorSampleIntervalTemporal(context.Background(), tx, resource, nil); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("nil message error = %v; want invalid_payload", err)
	}
	if err := appendSensorSampleIntervalTemporal(context.Background(), tx, resource, &Message{}); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("no sample error = %v; want invalid_payload", err)
	}
	sample := SampleCandidate{Value: 1, MeasuredAt: reportedStateTestCandidateTime(), BootID: mustUUID(decodeTestBootID), MessageID: temporalTestMessageID()}
	nonPositive := &Message{batch: BatchDTO{SampleIntervalUS: 0}, samples: []SampleCandidate{sample}}
	if err := appendSensorSampleIntervalTemporal(context.Background(), tx, resource, nonPositive); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("non-positive interval error = %v; want invalid_payload", err)
	}
	tooWide := &Message{batch: BatchDTO{SampleIntervalUS: maxExactFloat64Integer + 1}, samples: []SampleCandidate{sample}}
	if err := appendSensorSampleIntervalTemporal(context.Background(), tx, resource, tooWide); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("unrepresentable interval error = %v; want invalid_payload", err)
	}
	if tx.execCalls != 0 || tx.queryCalls != 0 {
		t.Fatalf("invalid input reached the database: exec=%d query=%d", tx.execCalls, tx.queryCalls)
	}
}

func TestAppendSensorSampleIntervalTemporalFailures(t *testing.T) {
	message := reportedStateTestMessage(t, "2026-08-21T10:15:00Z", 12501, decodeTestBootID, []float64{1, 2, 3})
	resource := reportedStateTestResource(nil)

	t.Run("insert error is dependency", func(t *testing.T) {
		tx := &telemetryFakeTx{execErr: errors.New("insert failed")}
		err := appendSensorSampleIntervalTemporal(context.Background(), tx, resource, message)
		if reasonCode(err) != ReasonDependencyUnavailable || !strings.Contains(err.Error(), "insert failed") {
			t.Fatalf("err = %v; want dependency error with cause", err)
		}
	})
	t.Run("unexpected row count errors", func(t *testing.T) {
		tx := &telemetryFakeTx{execRows: 0}
		err := appendSensorSampleIntervalTemporal(context.Background(), tx, resource, message)
		if reasonCode(err) != ReasonDependencyUnavailable {
			t.Fatalf("err = %v; want dependency error", err)
		}
	})
}

func TestSensorSampleIntervalTemporalSQLKeepsSingleTypedValue(t *testing.T) {
	sql := sensorSampleIntervalTemporalInsertSQL
	if !strings.Contains(sql, "'sample_interval_us'") {
		t.Fatalf("temporal SQL must write the sample_interval_us property: %s", sql)
	}
	if !strings.Contains(sql, "value_text, value_boolean") || !strings.Contains(sql, "NULL, NULL") {
		t.Fatalf("temporal SQL must leave text/boolean typed values NULL: %s", sql)
	}
	if !strings.Contains(sql, "metadata") || !strings.Contains(sql, "message_id") {
		t.Fatalf("temporal SQL must persist metadata and message_id: %s", sql)
	}
}

func TestPersistNewMessageAppliesReportedStateThenTemporalBeforeCommit(t *testing.T) {
	tx := &telemetryFakeTx{message: telemetryRepositoryTestMessage(t), copyCount: 3, execRows: 1}
	repository, _ := newTelemetryFakeRepository(t, tx)
	result, err := repository.Persist(context.Background(), tx.message)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != CommitOutcomeCommitted {
		t.Fatalf("Persist() = %#v; want committed", result)
	}
	if len(tx.execs) != 2 || tx.execs[0].query != sensorReportedStateAdvanceSQL || tx.execs[1].query != sensorSampleIntervalTemporalInsertSQL {
		t.Fatalf("exec order = %#v; want reported-state then temporal before commit", tx.execs)
	}
	if tx.commitCalls != 1 || tx.rollbackCalls != 1 {
		t.Fatalf("commit=%d rollback=%d", tx.commitCalls, tx.rollbackCalls)
	}
}

func TestPersistDuplicateBypassesReportedStateAndTemporal(t *testing.T) {
	tx := &telemetryFakeTx{message: telemetryRepositoryTestMessage(t), duplicate: true}
	repository, _ := newTelemetryFakeRepository(t, tx)
	result, err := repository.Persist(context.Background(), tx.message)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != CommitOutcomeDuplicateCommitted {
		t.Fatalf("Persist() = %#v; want duplicate committed", result)
	}
	if tx.execCalls != 0 || len(tx.execs) != 0 {
		t.Fatalf("duplicate reached Twin/temporal: execs=%#v", tx.execs)
	}
	if tx.commitCalls != 1 || tx.rollbackCalls != 1 {
		t.Fatalf("commit=%d rollback=%d; want the duplicate ledger commit only", tx.commitCalls, tx.rollbackCalls)
	}
}

func TestPersistReportedStateFailureRollsBackWithoutCommitOrTemporal(t *testing.T) {
	tx := &telemetryFakeTx{message: telemetryRepositoryTestMessage(t), copyCount: 3, sensorExec: &telemetryFakeExecConfig{rows: 0}}
	repository, _ := newTelemetryFakeRepository(t, tx)
	result, err := repository.Persist(context.Background(), tx.message)
	if reasonCode(err) != ReasonResourceGraphIncomplete || result != (PersistResult{}) {
		t.Fatalf("Persist() = %#v, %v; want graph-incomplete and zero result", result, err)
	}
	if len(tx.execs) != 1 || tx.execs[0].query != sensorReportedStateAdvanceSQL {
		t.Fatalf("execs = %#v; want only the failed reported-state update", tx.execs)
	}
	if tx.commitCalls != 0 || tx.rollbackCalls != 1 || !tx.rollbackValid {
		t.Fatalf("commit=%d rollback=%d bounded=%v; want bounded rollback without commit", tx.commitCalls, tx.rollbackCalls, tx.rollbackValid)
	}
}

func TestPersistTemporalFailureRollsBackWithoutCommit(t *testing.T) {
	tx := &telemetryFakeTx{
		message:      telemetryRepositoryTestMessage(t),
		copyCount:    3,
		sensorExec:   &telemetryFakeExecConfig{rows: 1},
		temporalExec: &telemetryFakeExecConfig{err: errors.New("temporal insert failed")},
	}
	repository, _ := newTelemetryFakeRepository(t, tx)
	result, err := repository.Persist(context.Background(), tx.message)
	if reasonCode(err) != ReasonDependencyUnavailable || result != (PersistResult{}) || !strings.Contains(err.Error(), "temporal insert failed") {
		t.Fatalf("Persist() = %#v, %v; want dependency error with cause and zero result", result, err)
	}
	if len(tx.execs) != 2 || tx.execs[0].query != sensorReportedStateAdvanceSQL || tx.execs[1].query != sensorSampleIntervalTemporalInsertSQL {
		t.Fatalf("execs = %#v; want reported-state then temporal attempts before rollback", tx.execs)
	}
	if tx.commitCalls != 0 || tx.rollbackCalls != 1 || !tx.rollbackValid {
		t.Fatalf("commit=%d rollback=%d bounded=%v", tx.commitCalls, tx.rollbackCalls, tx.rollbackValid)
	}
}

func TestPersistCommitAmbiguityAfterTwinWritesIsUnconfirmed(t *testing.T) {
	tx := &telemetryFakeTx{
		message:      telemetryRepositoryTestMessage(t),
		copyCount:    3,
		sensorExec:   &telemetryFakeExecConfig{rows: 1},
		temporalExec: &telemetryFakeExecConfig{rows: 1},
		commitErr:    errors.New("commit response lost"),
	}
	repository, _ := newTelemetryFakeRepository(t, tx)
	result, err := repository.Persist(context.Background(), tx.message)
	if reasonCode(err) != ReasonCommitUnconfirmed || result != (PersistResult{}) || !strings.Contains(err.Error(), "commit response lost") {
		t.Fatalf("Persist() = %#v, %v; want commit-unconfirmed and zero result", result, err)
	}
	if len(tx.execs) != 2 || tx.commitCalls != 1 || tx.rollbackCalls != 1 || !tx.rollbackValid {
		t.Fatalf("execs=%d commit=%d rollback=%d bounded=%v", len(tx.execs), tx.commitCalls, tx.rollbackCalls, tx.rollbackValid)
	}
}

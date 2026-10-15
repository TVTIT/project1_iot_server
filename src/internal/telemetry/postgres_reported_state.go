package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// sensorReportedStateReadSQL reads and locks the resolved Sensor Twin's current
// state for the caller's transaction. resolveTelemetryResource already holds the
// Sensor row lock; this adds the state row lock so the ordering decision and the
// merge update observe one consistent snapshot under the existing Sensor lock.
const sensorReportedStateReadSQL = `SELECT reported_state, reported_version, last_reported_at
	FROM public.twin_states
	WHERE entity_id = $1
	FOR UPDATE`

// sensorReportedStateAdvanceSQL merges the server-allowlisted telemetry keys
// into the Sensor reported state. It is guarded by the reported_version read
// under the same row lock, so a mismatch means the locked snapshot changed and
// the caller must fail closed. It never touches desired_state, its version, or
// its timestamps. The condition lives in the DB transaction under the lock: the
// merge only runs when the caller has already established that the candidate
// advances, and the version guard prevents an unguarded overwrite.
const sensorReportedStateAdvanceSQL = `UPDATE public.twin_states
	SET reported_state = reported_state || $2::jsonb,
		reported_version = reported_version + 1,
		last_reported_at = $3,
		updated_at = now()
	WHERE entity_id = $1 AND reported_version = $4`

// reportedStateCandidate is the server-allowlisted Sensor reported-state update
// derived from one validated telemetry batch. Every field is authored by the
// server: the batch's final sample supplies value/boot/sequence/measurement
// time, the validated batch supplies the interval, and the resolved Sensor
// supplies the optional unit. No raw payload field is copied verbatim.
type reportedStateCandidate struct {
	value            float64
	observedAt       time.Time
	bootID           uuid.UUID
	sequenceNum      int64
	sampleIntervalUS int64
	unit             *string
}

// newReportedStateCandidate builds the allowlisted update from a validated
// message's final sample and its provisioned Sensor unit. It fails closed when
// the message has no final sample or the interval is not positive.
func newReportedStateCandidate(resource resolvedTelemetryResource, message *Message) (reportedStateCandidate, error) {
	if message == nil {
		return reportedStateCandidate{}, &ReasonError{Code: ReasonInvalidPayload, Field: "message"}
	}
	last, ok := message.LastSample()
	if !ok {
		return reportedStateCandidate{}, &ReasonError{Code: ReasonInvalidPayload, Field: "message_samples"}
	}
	interval := message.batch.SampleIntervalUS
	if interval <= 0 {
		return reportedStateCandidate{}, &ReasonError{Code: ReasonInvalidPayload, Field: "sample_interval_us"}
	}
	candidate := reportedStateCandidate{
		value:            last.Value,
		observedAt:       last.MeasuredAt.UTC(),
		bootID:           last.BootID,
		sequenceNum:      last.SequenceNum,
		sampleIntervalUS: interval,
	}
	if resource.Unit != nil {
		unit := *resource.Unit
		candidate.unit = &unit
	}
	return candidate, nil
}

// merge maps the candidate to the exact server-allowlisted reported_state keys.
// The unit key is present only when the provisioned Sensor declares one, so a
// Sensor without a unit never gains a null or payload-supplied value.
func (candidate reportedStateCandidate) merge() map[string]any {
	merge := map[string]any{
		"value":              candidate.value,
		"observed_at":        candidate.observedAt.Format(time.RFC3339Nano),
		"boot_id":            candidate.bootID.String(),
		"sequence_num":       candidate.sequenceNum,
		"sample_interval_us": candidate.sampleIntervalUS,
	}
	if candidate.unit != nil {
		merge["unit"] = *candidate.unit
	}
	return merge
}

// applySensorReportedState conditionally advances the resolved Sensor Twin's
// reported state for one new telemetry batch inside the caller-owned
// transaction. It uses the caller's pgx.Tx for the lock, read, and update; it
// never begins, commits, or rolls back, so the caller keeps transaction and
// rollback ownership.
//
// The ordering condition is evaluated under the Sensor row lock: a candidate
// advances only when last_reported_at is NULL, when its measurement time is
// newer, or when the time is equal and the stored boot ID is a valid UUID that
// matches the candidate together with a valid stored sequence smaller than the
// candidate. Malformed or missing legacy boot/sequence values hold the current
// state instead of failing a valid batch, and UUID ordering is never used to
// guess which boot is newer. A missing Twin state row is an error, never a
// no-progress result. The returned bool reports whether the state advanced.
func applySensorReportedState(ctx context.Context, tx pgx.Tx, resource resolvedTelemetryResource, message *Message) (bool, error) {
	if tx == nil {
		return false, &ReasonError{Code: ReasonInvalidPayload, Field: "tx"}
	}
	if resource.EntityID == uuid.Nil {
		return false, &ReasonError{Code: ReasonInvalidPayload, Field: "entity_id"}
	}
	candidate, err := newReportedStateCandidate(resource, message)
	if err != nil {
		return false, err
	}

	var reportedState []byte
	var reportedVersion int64
	var lastReportedAt *time.Time
	err = tx.QueryRow(ctx, sensorReportedStateReadSQL, resource.EntityID).
		Scan(&reportedState, &reportedVersion, &lastReportedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, &ReasonError{Code: ReasonResourceGraphIncomplete, Field: "twin_state"}
	}
	if err != nil {
		return false, dependencyError("lock telemetry Sensor state", err)
	}
	if reportedVersion < 0 {
		return false, &ReasonError{Code: ReasonResourceGraphIncomplete, Field: "reported_version"}
	}

	if !sensorStateAdvances(reportedState, lastReportedAt, candidate) {
		return false, nil
	}

	tag, err := tx.Exec(ctx, sensorReportedStateAdvanceSQL,
		resource.EntityID, candidate.merge(), candidate.observedAt, reportedVersion)
	if err != nil {
		return false, dependencyError("advance telemetry Sensor reported state", err)
	}
	if tag.RowsAffected() != 1 {
		return false, &ReasonError{Code: ReasonResourceGraphIncomplete, Field: "twin_state"}
	}
	return true, nil
}

// sensorStateAdvances reports whether the candidate may replace the current
// Sensor reported state using only server-authored values. Legacy states with a
// missing or malformed boot ID or sequence number at an equal timestamp hold the
// current state because no reliable ordering can be established.
func sensorStateAdvances(stored []byte, lastReportedAt *time.Time, candidate reportedStateCandidate) bool {
	if lastReportedAt == nil {
		return true
	}
	if candidate.observedAt.After(*lastReportedAt) {
		return true
	}
	if !candidate.observedAt.Equal(*lastReportedAt) {
		return false
	}
	storedBoot, storedSequence, ok := storedSensorProvenance(stored)
	if !ok {
		return false
	}
	storedBootID, err := uuid.Parse(storedBoot)
	if err != nil || storedBootID != candidate.bootID {
		return false
	}
	return storedSequence < candidate.sequenceNum
}

// storedSensorProvenance extracts a valid stored boot ID string and sequence
// number from the current reported state. Malformed JSON, a non-string boot ID,
// or a sequence that is not a non-negative integer is reported as unavailable so
// the caller holds the current state instead of failing the transaction.
func storedSensorProvenance(stored []byte) (string, int64, bool) {
	if len(stored) == 0 {
		return "", 0, false
	}
	decoder := json.NewDecoder(bytes.NewReader(stored))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil || object == nil {
		return "", 0, false
	}
	boot, ok := object["boot_id"].(string)
	if !ok || boot == "" {
		return "", 0, false
	}
	number, ok := object["sequence_num"].(json.Number)
	if !ok {
		return "", 0, false
	}
	sequence, err := number.Int64()
	if err != nil || sequence < 0 {
		return "", 0, false
	}
	return boot, sequence, true
}

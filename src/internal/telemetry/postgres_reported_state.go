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

// reportedStateSnapshot is the locked Sensor reported state read for one batch
// group. Only the fields the ordering decision needs are kept: the raw
// reported_state JSON is parsed once into provenance, and objectShaped records
// whether the stored value is a JSON object, because the advance SQL merge
// (`reported_state || $2::jsonb`) only stays an object when the stored value
// already is one.
type reportedStateSnapshot struct {
	entityID       uuid.UUID
	version        int64
	lastReportedAt *time.Time
	bootID         string
	sequence       int64
	hasProvenance  bool
	objectShaped   bool
}

// reportedStatePlan is one queued Sensor reported-state advance. version is the
// reported_version the batched UPDATE must match, so a group that advances one
// Sensor several times queues version, version+1, ... in message order.
type reportedStatePlan struct {
	entityID  uuid.UUID
	version   int64
	candidate reportedStateCandidate
}

// readSensorReportedStatesBatch locks and reads the reported state of every
// entity in one SendBatch round trip. entities must be unique and in
// first-appearance order. It keeps the caller's transaction ownership and the
// same fail-closed results as the single-message read: a missing Twin state row
// is a graph error, never a no-progress result.
func readSensorReportedStatesBatch(ctx context.Context, tx pgx.Tx, entities []uuid.UUID) (map[uuid.UUID]reportedStateSnapshot, error) {
	if tx == nil {
		return nil, &ReasonError{Code: ReasonInvalidPayload, Field: "tx"}
	}
	for _, entity := range entities {
		if entity == uuid.Nil {
			return nil, &ReasonError{Code: ReasonInvalidPayload, Field: "entity_id"}
		}
	}
	batch := &pgx.Batch{}
	for _, entity := range entities {
		batch.Queue(sensorReportedStateReadSQL, entity)
	}
	br := tx.SendBatch(ctx, batch)
	snapshots := make(map[uuid.UUID]reportedStateSnapshot, len(entities))
	for _, entity := range entities {
		var reportedState []byte
		var reportedVersion int64
		var lastReportedAt *time.Time
		err := br.QueryRow().Scan(&reportedState, &reportedVersion, &lastReportedAt)
		if err != nil {
			_ = br.Close()
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, &ReasonError{Code: ReasonResourceGraphIncomplete, Field: "twin_state"}
			}
			return nil, dependencyError("lock telemetry Sensor state", err)
		}
		if reportedVersion < 0 {
			_ = br.Close()
			return nil, &ReasonError{Code: ReasonResourceGraphIncomplete, Field: "reported_version"}
		}
		storedBoot, storedSequence, ok := storedSensorProvenance(reportedState)
		snapshots[entity] = reportedStateSnapshot{
			entityID:       entity,
			version:        reportedVersion,
			lastReportedAt: lastReportedAt,
			bootID:         storedBoot,
			sequence:       storedSequence,
			hasProvenance:  ok,
			objectShaped:   reportedStateObjectShaped(reportedState),
		}
	}
	if err := br.Close(); err != nil {
		return nil, dependencyError("close telemetry Sensor reported-state read batch", err)
	}
	return snapshots, nil
}

// planReportedStateAdvances decides, in Go, which new members advance their
// Sensor reported state. A group may carry several messages for the same Sensor,
// so reading the state once and writing it several times would queue the same
// reported_version repeatedly and fail the version guard on the second UPDATE.
// The projection therefore replays the sequential loop: each emitted plan
// advances the projection the way the SQL merge does, so the queued versions are
// version, version+1, ... for that entity in message order.
//
// The projection must mirror the merge, not the candidate. The advance SQL is
// `reported_state || $2::jsonb`, which only stays a JSON object when the stored
// value is already an object; a legacy array or scalar stays non-object and its
// boot/sequence provenance stays unreadable forever. The projection therefore
// carries objectShaped across advances and derives hasProvenance from it, so an
// equal-timestamp follow-up message holds on a non-object state exactly like the
// sequential path, which re-reads the merged row every time.
func planReportedStateAdvances(messages []*Message, newMembers []int, resources map[telemetryResourceKey]resolvedTelemetryResource, snapshots map[uuid.UUID]reportedStateSnapshot) ([]reportedStatePlan, error) {
	if len(newMembers) == 0 {
		return nil, nil
	}
	projections := make(map[uuid.UUID]reportedStateSnapshot, len(snapshots))
	for entity, snapshot := range snapshots {
		projections[entity] = snapshot
	}
	plans := make([]reportedStatePlan, 0, len(newMembers))
	for _, index := range newMembers {
		message := messages[index]
		key := telemetryResourceKey{message.Topic().GatewayID, message.Topic().SensorID}
		resource, ok := resources[key]
		if !ok {
			return nil, &ReasonError{Code: ReasonResourceGraphIncomplete, Field: "twin_graph"}
		}
		if resource.EntityID == uuid.Nil {
			return nil, &ReasonError{Code: ReasonInvalidPayload, Field: "entity_id"}
		}
		candidate, err := newReportedStateCandidate(resource, message)
		if err != nil {
			return nil, err
		}
		projection, ok := projections[resource.EntityID]
		if !ok {
			return nil, &ReasonError{Code: ReasonResourceGraphIncomplete, Field: "twin_state"}
		}
		if !sensorProvenanceAdvances(projection.lastReportedAt, projection.bootID, projection.sequence, projection.hasProvenance, candidate) {
			continue
		}
		plans = append(plans, reportedStatePlan{entityID: resource.EntityID, version: projection.version, candidate: candidate})
		observedAt := candidate.observedAt
		projection.version++
		projection.lastReportedAt = &observedAt
		projection.bootID = candidate.bootID.String()
		projection.sequence = candidate.sequenceNum
		// The merge cannot make a non-object state readable, so provenance stays
		// unavailable for a non-object state after the advance too.
		projection.hasProvenance = projection.objectShaped
		projections[resource.EntityID] = projection
	}
	return plans, nil
}

// advanceSensorReportedStatesBatch applies every planned reported-state advance
// in plan order with one SendBatch round trip. Each statement still carries its
// own version guard, so a mismatch means the locked snapshot changed and the
// caller fails closed instead of overwriting it.
func advanceSensorReportedStatesBatch(ctx context.Context, tx pgx.Tx, plans []reportedStatePlan) error {
	if tx == nil {
		return &ReasonError{Code: ReasonInvalidPayload, Field: "tx"}
	}
	if len(plans) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, plan := range plans {
		batch.Queue(sensorReportedStateAdvanceSQL,
			plan.entityID, plan.candidate.merge(), plan.candidate.observedAt, plan.version)
	}
	br := tx.SendBatch(ctx, batch)
	for range plans {
		tag, err := br.Exec()
		if err != nil {
			_ = br.Close()
			return dependencyError("advance telemetry Sensor reported state", err)
		}
		if tag.RowsAffected() != 1 {
			_ = br.Close()
			return &ReasonError{Code: ReasonResourceGraphIncomplete, Field: "twin_state"}
		}
	}
	if err := br.Close(); err != nil {
		return dependencyError("close telemetry Sensor reported-state advance batch", err)
	}
	return nil
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
// Sensor reported state using only server-authored values. It parses the stored
// provenance and delegates to the shared ordering core.
func sensorStateAdvances(stored []byte, lastReportedAt *time.Time, candidate reportedStateCandidate) bool {
	storedBoot, storedSequence, ok := storedSensorProvenance(stored)
	return sensorProvenanceAdvances(lastReportedAt, storedBoot, storedSequence, ok, candidate)
}

// sensorProvenanceAdvances is the ordering core shared by the single-message
// path and the group projection. Legacy states with a missing or malformed boot
// ID or sequence number at an equal timestamp hold the current state because no
// reliable ordering can be established; UUID ordering is never used to guess
// which boot is newer.
func sensorProvenanceAdvances(lastReportedAt *time.Time, storedBoot string, storedSequence int64, hasProvenance bool, candidate reportedStateCandidate) bool {
	if lastReportedAt == nil {
		return true
	}
	if candidate.observedAt.After(*lastReportedAt) {
		return true
	}
	if !candidate.observedAt.Equal(*lastReportedAt) {
		return false
	}
	if !hasProvenance {
		return false
	}
	storedBootID, err := uuid.Parse(storedBoot)
	if err != nil || storedBootID != candidate.bootID {
		return false
	}
	return storedSequence < candidate.sequenceNum
}

// reportedStateObjectShaped reports whether the stored reported_state is a JSON
// object. Empty bytes, null, arrays and scalars are not. The advance SQL merges
// with `reported_state || $2::jsonb`, which leaves a non-object value
// non-object, so a merged non-object state never gains readable boot/sequence
// provenance and the projection must not assume it does.
func reportedStateObjectShaped(reportedState []byte) bool {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(reportedState, &object); err != nil || object == nil {
		return false
	}
	return true
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

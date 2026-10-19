package telemetry

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// sensorSampleIntervalTemporalInsertSQL appends exactly one temporal
// observation for the Sensor Twin of a new telemetry batch. It stores the
// batch's sample interval as the single typed value (value_number) and leaves
// value_text and value_boolean NULL so the num_nonnulls typed-value constraint
// is satisfied by the interval alone. Raw sample values are not duplicated
// here; they stay in the telemetry hypertable.
const sensorSampleIntervalTemporalInsertSQL = `INSERT INTO public.twin_temporal_values (
	observed_at, entity_id, property_name, value_number, value_text, value_boolean, metadata, message_id
) VALUES ($1, $2, 'sample_interval_us', $3, NULL, NULL, $4, $5)`

// appendSensorSampleIntervalTemporal records one sample_interval_us temporal
// observation for the resolved Sensor Twin inside the caller-owned transaction.
// It uses the caller's pgx.Tx and never begins, commits, or rolls back, so the
// caller keeps transaction and rollback ownership.
//
// The observation is server-authored: observed_at is the final sample's
// measurement time in UTC, entity_id is the resolved Sensor entity, message_id
// is the batch identifier, and value_number is the validated batch interval.
// metadata carries unit='us', the batch boot_id, and source='telemetry_batch'.
// The interval must be positive and exactly representable as a DOUBLE PRECISION
// integer; a wider-but-valid value is rejected before writing so the stored
// value is never silently rounded. Exactly one row must be inserted.
func appendSensorSampleIntervalTemporal(ctx context.Context, tx pgx.Tx, resource resolvedTelemetryResource, message *Message) error {
	if tx == nil {
		return &ReasonError{Code: ReasonInvalidPayload, Field: "tx"}
	}
	if resource.EntityID == uuid.Nil {
		return &ReasonError{Code: ReasonInvalidPayload, Field: "entity_id"}
	}
	if message == nil {
		return &ReasonError{Code: ReasonInvalidPayload, Field: "message"}
	}
	last, ok := message.LastSample()
	if !ok {
		return &ReasonError{Code: ReasonInvalidPayload, Field: "message_samples"}
	}
	interval := message.batch.SampleIntervalUS
	if interval <= 0 || interval > maxExactFloat64Integer {
		return &ReasonError{Code: ReasonInvalidPayload, Field: "sample_interval_us"}
	}

	metadata := map[string]any{
		"unit":    "us",
		"boot_id": last.BootID.String(),
		"source":  "telemetry_batch",
	}
	tag, err := tx.Exec(ctx, sensorSampleIntervalTemporalInsertSQL,
		last.MeasuredAt.UTC(), resource.EntityID, float64(interval), metadata, last.MessageID)
	if err != nil {
		return dependencyError("append telemetry Sensor temporal value", err)
	}
	if tag.RowsAffected() != 1 {
		return &ReasonError{Code: ReasonDependencyUnavailable, Field: "twin_temporal_values"}
	}
	return nil
}

package digitaltwin

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"iot-platform/internal/telemetry"
)

// maxExactFloat64Integer is 2^53, the largest integer magnitude representable
// exactly in a float64. Stored sample_interval_us above this threshold is invalid.
const maxExactFloat64Integer = int64(1) << 53

// isInvalidSampleInterval returns true if val is not a valid sample_interval_us observation value.
// The writer/protocol contract specifies positive integer microseconds representable
// exactly in float64 without precision loss: [1, 2^53]. Fractional values, non-positive
// values, values > 2^53, NaN, and Inf are invalid.
func isInvalidSampleInterval(val float64) bool {
	return math.IsNaN(val) || math.IsInf(val, 0) || val <= 0 || val > float64(maxExactFloat64Integer) || val != math.Trunc(val)
}

// rawTemporalHistoryQuery queries public.twin_temporal_values for an authorized Sensor entity.
// It joins public.sensors, public.twin_entities, public.twin_relationships, public.gateways,
// and public.user_gateways to ensure that the actor holds membership and the canonical
// same-Gateway provisioning graph (Gateway hasSensor Sensor) is satisfied.
//
// A single-statement snapshot strategy executes both whole-window summary aggregation
// (window_summary CTE) and bounded observations fetch (bounded_observations CTE) in one query.
// This guarantees that window-level classification (type consistency, unit uniformity, interval invariants)
// and returned observations observe the exact same MVCC database snapshot without multi-statement transaction overhead.
//
// If the sensor is unauthorized or nonexistent, authorized_sensor produces 0 rows and the query returns 0 rows (ErrNotFound).
// If the sensor is authorized but has 0 observations in [from, to), LEFT JOIN bounded_observations produces 1 sentinel row
// with b.observed_at IS NULL and s.total_count = 0.
const rawTemporalHistoryQuery = `WITH authorized_sensor AS (
	SELECT e.id AS entity_uuid, e.entity_id, e.gateway_id, sens.sensor_id
	FROM public.twin_entities AS e
	JOIN public.sensors AS sens
	  ON sens.gateway_id = e.gateway_id AND sens.sensor_id = $1
	 AND e.entity_id = 'urn:ngsi-ld:Sensor:' || sens.gateway_id || ':' || sens.sensor_id
	JOIN public.twin_entities AS parent_e
	  ON parent_e.entity_id = 'urn:ngsi-ld:Gateway:' || e.gateway_id
	 AND parent_e.gateway_id = e.gateway_id
	 AND parent_e.entity_type = 'Gateway'
	JOIN public.twin_relationships AS r
	  ON r.source_entity_id = parent_e.id AND r.target_entity_id = e.id AND r.relationship_type = 'hasSensor'
	JOIN public.gateways AS g
	  ON g.gateway_id = e.gateway_id
	JOIN public.user_gateways AS ug
	  ON ug.gateway_id = g.gateway_id AND ug.user_id = $2
	WHERE e.id = $3
	  AND e.entity_id = $4
	  AND e.entity_type = 'Sensor'
	  AND e.gateway_id = $5
),
window_summary AS (
	SELECT
		COUNT(*)::bigint AS total_count,
		COUNT(v.value_number)::bigint AS number_count,
		COUNT(v.value_text)::bigint AS text_count,
		COUNT(v.value_boolean)::bigint AS boolean_count,
		COUNT(*) FILTER (
			WHERE (CASE WHEN v.value_number IS NOT NULL THEN 1 ELSE 0 END +
			       CASE WHEN v.value_text IS NOT NULL THEN 1 ELSE 0 END +
			       CASE WHEN v.value_boolean IS NOT NULL THEN 1 ELSE 0 END) != 1
		)::bigint AS invalid_type_count,
		COUNT(*) FILTER (
			WHERE v.value_number IS NOT NULL AND (
				v.value_number <= 0 OR
				v.value_number > 9007199254740992 OR
				v.value_number <> trunc(v.value_number)
			)
		)::bigint AS invalid_interval_count,
		COUNT(*) FILTER (
			WHERE NOT (v.metadata ? 'unit') OR jsonb_typeof(v.metadata->'unit') = 'null'
		)::bigint AS unknown_unit_count,
		COUNT(*) FILTER (
			WHERE (v.metadata ? 'unit') AND jsonb_typeof(v.metadata->'unit') NOT IN ('null', 'string')
		)::bigint AS malformed_unit_count,
		COUNT(DISTINCT (v.metadata->>'unit')) FILTER (
			WHERE jsonb_typeof(v.metadata->'unit') = 'string'
		)::bigint AS distinct_unit_count,
		MIN(v.metadata->>'unit') FILTER (
			WHERE jsonb_typeof(v.metadata->'unit') = 'string'
		) AS min_unit
	FROM authorized_sensor src
	JOIN public.twin_temporal_values v
	  ON v.entity_id = src.entity_uuid
	 AND v.property_name = $6
	 AND v.observed_at >= $7
	 AND v.observed_at < $8
),
bounded_observations AS (
	SELECT
		v.observed_at,
		v.value_number,
		v.value_text,
		v.value_boolean,
		v.metadata,
		v.message_id
	FROM authorized_sensor src
	JOIN public.twin_temporal_values v
	  ON v.entity_id = src.entity_uuid
	 AND v.property_name = $6
	 AND v.observed_at >= $7
	 AND v.observed_at < $8
	ORDER BY v.observed_at ASC, v.message_id ASC NULLS LAST
	LIMIT $9
)
SELECT
	src.entity_uuid,
	src.entity_id,
	b.observed_at,
	b.value_number,
	b.value_text,
	b.value_boolean,
	b.metadata,
	b.message_id,
	s.total_count,
	s.number_count,
	s.text_count,
	s.boolean_count,
	s.invalid_type_count,
	s.invalid_interval_count,
	s.unknown_unit_count,
	s.malformed_unit_count,
	s.distinct_unit_count,
	s.min_unit
FROM authorized_sensor src
CROSS JOIN window_summary s
LEFT JOIN bounded_observations b ON true
ORDER BY b.observed_at ASC NULLS LAST, b.message_id ASC NULLS LAST`

// bucketTemporalHistoryQuery queries public.twin_temporal_values for an authorized Sensor entity,
// aggregating numeric values into time_bucket buckets aligned to UTC epoch (1970-01-01 00:00:00+00).
//
// In the same single MVCC snapshot, it computes whole-window summary statistics (window_summary CTE)
// and grouped bucket aggregates (bucket_aggregates CTE).
// If the sensor is unauthorized or nonexistent, authorized_sensor produces 0 rows (ErrNotFound).
// If the sensor is authorized but has 0 observations in [from, to), LEFT JOIN produces 1 sentinel row
// with b.bucket_start IS NULL and s.total_count = 0.
const bucketTemporalHistoryQuery = `WITH authorized_sensor AS (
	SELECT e.id AS entity_uuid, e.entity_id, e.gateway_id, sens.sensor_id
	FROM public.twin_entities AS e
	JOIN public.sensors AS sens
	  ON sens.gateway_id = e.gateway_id AND sens.sensor_id = $1
	 AND e.entity_id = 'urn:ngsi-ld:Sensor:' || sens.gateway_id || ':' || sens.sensor_id
	JOIN public.twin_entities AS parent_e
	  ON parent_e.entity_id = 'urn:ngsi-ld:Gateway:' || e.gateway_id
	 AND parent_e.gateway_id = e.gateway_id
	 AND parent_e.entity_type = 'Gateway'
	JOIN public.twin_relationships AS r
	  ON r.source_entity_id = parent_e.id AND r.target_entity_id = e.id AND r.relationship_type = 'hasSensor'
	JOIN public.gateways AS g
	  ON g.gateway_id = e.gateway_id
	JOIN public.user_gateways AS ug
	  ON ug.gateway_id = g.gateway_id AND ug.user_id = $2
	WHERE e.id = $3
	  AND e.entity_id = $4
	  AND e.entity_type = 'Sensor'
	  AND e.gateway_id = $5
),
window_summary AS (
	SELECT
		COUNT(*)::bigint AS total_count,
		COUNT(v.value_number)::bigint AS number_count,
		COUNT(v.value_text)::bigint AS text_count,
		COUNT(v.value_boolean)::bigint AS boolean_count,
		COUNT(*) FILTER (
			WHERE (CASE WHEN v.value_number IS NOT NULL THEN 1 ELSE 0 END +
			       CASE WHEN v.value_text IS NOT NULL THEN 1 ELSE 0 END +
			       CASE WHEN v.value_boolean IS NOT NULL THEN 1 ELSE 0 END) != 1
		)::bigint AS invalid_type_count,
		COUNT(*) FILTER (
			WHERE v.value_number IS NOT NULL AND (
				v.value_number <= 0 OR
				v.value_number > 9007199254740992 OR
				v.value_number <> trunc(v.value_number)
			)
		)::bigint AS invalid_interval_count,
		COUNT(*) FILTER (
			WHERE NOT (v.metadata ? 'unit') OR jsonb_typeof(v.metadata->'unit') = 'null'
		)::bigint AS unknown_unit_count,
		COUNT(*) FILTER (
			WHERE (v.metadata ? 'unit') AND jsonb_typeof(v.metadata->'unit') NOT IN ('null', 'string')
		)::bigint AS malformed_unit_count,
		COUNT(DISTINCT (v.metadata->>'unit')) FILTER (
			WHERE jsonb_typeof(v.metadata->'unit') = 'string'
		)::bigint AS distinct_unit_count,
		MIN(v.metadata->>'unit') FILTER (
			WHERE jsonb_typeof(v.metadata->'unit') = 'string'
		) AS min_unit
	FROM authorized_sensor src
	JOIN public.twin_temporal_values v
	  ON v.entity_id = src.entity_uuid
	 AND v.property_name = $6
	 AND v.observed_at >= $7
	 AND v.observed_at < $8
),
bucket_aggregates AS (
	SELECT
		time_bucket($9::interval, v.observed_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS bucket_start,
		AVG(v.value_number)::double precision AS avg_value,
		MIN(v.value_number)::double precision AS min_value,
		MAX(v.value_number)::double precision AS max_value,
		COUNT(*)::bigint AS sample_count
	FROM authorized_sensor src
	JOIN public.twin_temporal_values v
	  ON v.entity_id = src.entity_uuid
	 AND v.property_name = $6
	 AND v.observed_at >= $7
	 AND v.observed_at < $8
	GROUP BY bucket_start
	ORDER BY bucket_start ASC
	LIMIT $10
)
SELECT
	src.entity_uuid,
	src.entity_id,
	b.bucket_start,
	b.avg_value,
	b.min_value,
	b.max_value,
	b.sample_count,
	s.total_count,
	s.number_count,
	s.text_count,
	s.boolean_count,
	s.invalid_type_count,
	s.invalid_interval_count,
	s.unknown_unit_count,
	s.malformed_unit_count,
	s.distinct_unit_count,
	s.min_unit
FROM authorized_sensor src
CROSS JOIN window_summary s
LEFT JOIN bucket_aggregates b ON true
ORDER BY b.bucket_start ASC NULLS LAST`

// ReadTemporalHistory reads temporal history observations for Digital Twin entities.
// It verifies actor and Sensor entity identity, validates property selector (public SelectorSampleIntervalUS),
// and delegates to readTemporalHistoryInternal.
func (r *PostgresTwinReader) ReadTemporalHistory(
	ctx context.Context,
	actor uuid.UUID,
	entity ResolvedEntity,
	q TwinHistoryQuery,
) (*TwinHistoryResult, error) {
	if actor == uuid.Nil {
		return nil, ErrNilActor
	}
	if ctx.Err() != nil {
		return nil, classifyReaderError(ctx.Err())
	}
	if entity.Type != EntityTypeSensor {
		return nil, ErrInvalidProperty
	}
	if q.Property != SelectorSampleIntervalUS {
		return nil, ErrInvalidProperty
	}
	return r.readTemporalHistoryInternal(ctx, actor, entity, string(q.Property), q)
}

// readTemporalHistoryInternal executes temporal history reading for a specific property name.
// It serves as the private typed read execution seam used by ReadTemporalHistory (for SelectorSampleIntervalUS)
// and by package-internal unit and regression test fixtures (for private typed text/bool validation).
func (r *PostgresTwinReader) readTemporalHistoryInternal(
	ctx context.Context,
	actor uuid.UUID,
	entity ResolvedEntity,
	propertyName string,
	q TwinHistoryQuery,
) (*TwinHistoryResult, error) {
	if actor == uuid.Nil {
		return nil, ErrNilActor
	}
	if ctx.Err() != nil {
		return nil, classifyReaderError(ctx.Err())
	}
	if entity.Type != EntityTypeSensor {
		return nil, ErrInvalidProperty
	}
	if q.MaxPoints <= 0 {
		return nil, ErrInvalidInput
	}

	fromUTC := q.Range.From.UTC()
	toUTC := q.Range.To.UTC()
	if !fromUTC.Before(toUTC) {
		return nil, ErrInvalidInput
	}

	res := telemetry.Resolution(q.Resolution)
	if !res.Valid() {
		return nil, fmt.Errorf("%w: resolution %q is not supported", ErrInvalidResolution, q.Resolution)
	}

	effectiveRawWindow := q.EffectiveRawWindow
	if effectiveRawWindow <= 0 || effectiveRawWindow > ContractMaxRawWindow {
		effectiveRawWindow = ContractMaxRawWindow
	}

	diffMicros := toUTC.Sub(fromUTC).Microseconds()

	switch res {
	case telemetry.ResolutionRaw:
		if diffMicros > effectiveRawWindow.Microseconds() {
			return nil, ErrRawLimitExceeded
		}
		points, summary, err := r.readTemporalDataInternal(
			ctx,
			actor,
			entity,
			propertyName,
			fromUTC,
			toUTC,
			q.MaxPoints,
		)
		if err != nil {
			return nil, err
		}

		classification, err := classifyTemporalWindow(summary, propertyName)
		if err != nil {
			return nil, err
		}

		if len(points) > q.MaxPoints || summary.TotalCount > int64(q.MaxPoints) {
			return nil, ErrRawLimitExceeded
		}

		if propertyName == string(SelectorSampleIntervalUS) {
			for _, pt := range points {
				if pt.ValueType != "number" || pt.ValueNumber == nil {
					return nil, fmt.Errorf("%w: sample_interval_us observation has non-numeric type %q", ErrUnexpected, pt.ValueType)
				}
				val := *pt.ValueNumber
				if isInvalidSampleInterval(val) {
					return nil, fmt.Errorf("%w: sample_interval_us observation has invalid interval value %v", ErrUnexpected, val)
				}
			}
		}

		return &TwinHistoryResult{
			EntityID:       entity.PublicURN,
			Property:       HistorySelector(propertyName),
			Unit:           classification.Unit,
			From:           fromUTC,
			To:             toUTC,
			Mode:           "raw",
			Resolution:     "raw",
			PointCount:     len(points),
			TemporalPoints: points,
		}, nil

	case telemetry.ResolutionAuto:
		if diffMicros <= effectiveRawWindow.Microseconds() {
			// Short window (<= effectiveRawWindow): attempt bounded raw probe.
			points, summary, err := r.readTemporalDataInternal(
				ctx,
				actor,
				entity,
				propertyName,
				fromUTC,
				toUTC,
				q.MaxPoints,
			)
			if err != nil {
				return nil, err
			}

			classification, err := classifyTemporalWindow(summary, propertyName)
			if err != nil {
				return nil, err
			}

			// If points fit within maxPoints: return bounded raw.
			if len(points) <= q.MaxPoints && summary.TotalCount <= int64(q.MaxPoints) {
				if propertyName == string(SelectorSampleIntervalUS) {
					for _, pt := range points {
						if pt.ValueType != "number" || pt.ValueNumber == nil {
							return nil, fmt.Errorf("%w: sample_interval_us observation has non-numeric type %q", ErrUnexpected, pt.ValueType)
						}
						val := *pt.ValueNumber
						if isInvalidSampleInterval(val) {
							return nil, fmt.Errorf("%w: sample_interval_us observation has invalid interval value %v", ErrUnexpected, val)
						}
					}
				}
				return &TwinHistoryResult{
					EntityID:       entity.PublicURN,
					Property:       HistorySelector(propertyName),
					Unit:           classification.Unit,
					From:           fromUTC,
					To:             toUTC,
					Mode:           "raw",
					Resolution:     "raw",
					PointCount:     len(points),
					TemporalPoints: points,
				}, nil
			}

			// Raw overflow!
			// If non-numeric (text/bool), cannot aggregate -> ErrRawLimitExceeded.
			if classification.ValueType != "number" {
				return nil, ErrRawLimitExceeded
			}

			// Numeric overflow: discard ALL probe observations and execute independently validated buckets.
			chosenRes, err := telemetry.SelectAutoBucketResolution(fromUTC, toUTC, q.MaxPoints)
			if err != nil {
				return nil, ErrResolutionTooFine
			}
			dur, ok := telemetry.ResolutionDuration(chosenRes)
			if !ok {
				return nil, ErrResolutionTooFine
			}

			buckets, bucketSummary, err := r.readTemporalBucketInternal(
				ctx,
				actor,
				entity,
				propertyName,
				fromUTC,
				toUTC,
				dur,
				q.MaxPoints,
			)
			if err != nil {
				return nil, err
			}

			bucketClassification, err := classifyTemporalWindow(bucketSummary, propertyName)
			if err != nil {
				return nil, err
			}
			if bucketClassification.TotalCount > 0 && bucketClassification.ValueType != "number" {
				return nil, ErrRawLimitExceeded
			}
			if len(buckets) > q.MaxPoints {
				return nil, ErrResolutionTooFine
			}

			return &TwinHistoryResult{
				EntityID:        entity.PublicURN,
				Property:        HistorySelector(propertyName),
				Unit:            bucketClassification.Unit,
				From:            fromUTC,
				To:              toUTC,
				Mode:            "bucket",
				Resolution:      string(chosenRes),
				PointCount:      len(buckets),
				TemporalBuckets: buckets,
			}, nil
		}

		// Long window (> effectiveRawWindow): select bucket resolution directly.
		chosenRes, err := telemetry.SelectAutoBucketResolution(fromUTC, toUTC, q.MaxPoints)
		if err != nil {
			return nil, ErrResolutionTooFine
		}
		dur, ok := telemetry.ResolutionDuration(chosenRes)
		if !ok {
			return nil, ErrResolutionTooFine
		}

		buckets, bucketSummary, err := r.readTemporalBucketInternal(
			ctx,
			actor,
			entity,
			propertyName,
			fromUTC,
			toUTC,
			dur,
			q.MaxPoints,
		)
		if err != nil {
			return nil, err
		}

		bucketClassification, err := classifyTemporalWindow(bucketSummary, propertyName)
		if err != nil {
			return nil, err
		}
		if bucketClassification.TotalCount > 0 && bucketClassification.ValueType != "number" {
			// Non-numeric in long auto window cannot be aggregated
			return nil, ErrRawLimitExceeded
		}
		if len(buckets) > q.MaxPoints {
			return nil, ErrResolutionTooFine
		}

		return &TwinHistoryResult{
			EntityID:        entity.PublicURN,
			Property:        HistorySelector(propertyName),
			Unit:            bucketClassification.Unit,
			From:            fromUTC,
			To:              toUTC,
			Mode:            "bucket",
			Resolution:      string(chosenRes),
			PointCount:      len(buckets),
			TemporalBuckets: buckets,
		}, nil

	default:
		// Explicit bucket resolution.
		dur, ok := telemetry.ResolutionDuration(res)
		if !ok {
			return nil, fmt.Errorf("%w: resolution %q is not supported", ErrInvalidResolution, q.Resolution)
		}

		intersectingCount, err := telemetry.IntersectingBucketCount(fromUTC, toUTC, dur)
		if err != nil {
			return nil, ErrInvalidInput
		}
		if intersectingCount > int64(q.MaxPoints) {
			return nil, ErrResolutionTooFine
		}

		buckets, bucketSummary, err := r.readTemporalBucketInternal(
			ctx,
			actor,
			entity,
			propertyName,
			fromUTC,
			toUTC,
			dur,
			q.MaxPoints,
		)
		if err != nil {
			return nil, err
		}

		bucketClassification, err := classifyTemporalWindow(bucketSummary, propertyName)
		if err != nil {
			return nil, err
		}
		if bucketClassification.TotalCount > 0 && bucketClassification.ValueType != "number" {
			return nil, ErrInvalidResolution
		}
		if len(buckets) > q.MaxPoints {
			return nil, ErrResolutionTooFine
		}

		return &TwinHistoryResult{
			EntityID:        entity.PublicURN,
			Property:        HistorySelector(propertyName),
			Unit:            bucketClassification.Unit,
			From:            fromUTC,
			To:              toUTC,
			Mode:            "bucket",
			Resolution:      string(res),
			PointCount:      len(buckets),
			TemporalBuckets: buckets,
		}, nil
	}
}

// TemporalWindowSummary captures SQL-aggregated summary statistics across the
// entire requested time window [from, to) for a given property.
type TemporalWindowSummary struct {
	TotalCount           int64
	NumberCount          int64
	TextCount            int64
	BooleanCount         int64
	InvalidTypeCount     int64
	InvalidIntervalCount int64
	UnknownUnitCount     int64
	MalformedUnitCount   int64
	DistinctUnitCount    int64
	MinUnit              *string
}

// TemporalClassification contains the normalized type and unit classification
// for a temporal window.
type TemporalClassification struct {
	ValueType     string  // "number", "text", "boolean", or "" if empty
	Unit          *string // nil if all unknown or empty; homogeneous unit if exactly one known unit with no unknown
	IsHomogeneous bool
	TotalCount    int64
}

// classifyTemporalWindow validates whole-window type consistency, property invariants,
// and unit uniformity. It returns ErrInconsistentUnit for numeric series with multiple
// known units or known/unknown mixtures, and ErrUnexpected on malformed units or invariant breaches.
func classifyTemporalWindow(summary *TemporalWindowSummary, propertyName string) (*TemporalClassification, error) {
	if summary == nil || summary.TotalCount == 0 {
		return &TemporalClassification{
			ValueType:     "",
			Unit:          nil,
			IsHomogeneous: true,
			TotalCount:    0,
		}, nil
	}

	if summary.InvalidTypeCount > 0 {
		return nil, fmt.Errorf("%w: invalid temporal observation: expected exactly 1 non-null value column", ErrUnexpected)
	}

	if summary.MalformedUnitCount > 0 {
		return nil, fmt.Errorf("%w: temporal metadata has malformed non-string unit", ErrUnexpected)
	}

	var valType string
	if summary.NumberCount == summary.TotalCount {
		valType = "number"
	} else if summary.TextCount == summary.TotalCount {
		valType = "text"
	} else if summary.BooleanCount == summary.TotalCount {
		valType = "boolean"
	} else {
		return nil, fmt.Errorf("%w: mixed stored types in temporal window (numbers: %d, text: %d, boolean: %d)", ErrUnexpected, summary.NumberCount, summary.TextCount, summary.BooleanCount)
	}

	if propertyName == string(SelectorSampleIntervalUS) {
		if valType != "number" {
			return nil, fmt.Errorf("%w: sample_interval_us observation has non-numeric type %q", ErrUnexpected, valType)
		}
		if summary.InvalidIntervalCount > 0 {
			return nil, fmt.Errorf("%w: sample_interval_us observation has invalid interval value (must be integer 0 < val <= 2^53)", ErrUnexpected)
		}
	}

	var (
		unit          *string
		isHomogeneous bool
	)

	if summary.UnknownUnitCount == summary.TotalCount {
		// All unknown yields nil unit, never a fabricated default.
		unit = nil
		isHomogeneous = true
	} else if summary.UnknownUnitCount == 0 && summary.DistinctUnitCount == 1 {
		// Exactly one known unit with no unknown is homogeneous.
		unit = summary.MinUnit
		isHomogeneous = true
	} else if valType == "number" {
		// For numeric series, multiple known units or known/unknown mix produces ErrInconsistentUnit.
		return nil, ErrInconsistentUnit
	} else {
		// For non-numeric series (text/boolean), unit is nil without error.
		unit = nil
		isHomogeneous = false
	}

	return &TemporalClassification{
		ValueType:     valType,
		Unit:          unit,
		IsHomogeneous: isHomogeneous,
		TotalCount:    summary.TotalCount,
	}, nil
}

// readTemporalObservationsInternal is an unexported helper used by package-internal unit/regression tests.
// It executes the parameterized query with the server-owned propertyName and decodes typed observations.
func (r *PostgresTwinReader) readTemporalObservationsInternal(
	ctx context.Context,
	actor uuid.UUID,
	entity ResolvedEntity,
	propertyName string,
	from time.Time,
	to time.Time,
	maxPoints int,
) ([]TemporalSamplePoint, error) {
	points, _, err := r.readTemporalDataInternal(ctx, actor, entity, propertyName, from, to, maxPoints)
	return points, err
}

// readTemporalDataInternal executes the parameterized query with the server-owned propertyName
// and decodes both bounded typed observations and whole-window summary statistics.
func (r *PostgresTwinReader) readTemporalDataInternal(
	ctx context.Context,
	actor uuid.UUID,
	entity ResolvedEntity,
	propertyName string,
	from time.Time,
	to time.Time,
	maxPoints int,
) ([]TemporalSamplePoint, *TemporalWindowSummary, error) {
	if actor == uuid.Nil {
		return nil, nil, ErrNilActor
	}
	if ctx.Err() != nil {
		return nil, nil, classifyReaderError(ctx.Err())
	}
	if entity.Type != EntityTypeSensor {
		return nil, nil, ErrInvalidProperty
	}
	if maxPoints <= 0 {
		return nil, nil, ErrInvalidInput
	}

	limit := maxPoints + 1
	rows, err := r.database.Query(
		ctx,
		rawTemporalHistoryQuery,
		entity.SensorID,
		actor,
		entity.InternalID,
		entity.PublicURN,
		entity.GatewayID,
		propertyName,
		from.UTC(),
		to.UTC(),
		limit,
	)
	if err != nil {
		return nil, nil, classifyReaderError(err)
	}

	return scanTemporalRows(ctx, rows)
}

// scanTemporalRows iterates over pgx.Rows, scanning bounded observations and whole-window summary.
// It skips the outer-join NULL sentinel row for authorized sensors with 0 observations,
// enforces that exactly one value column is non-null, preserves metadata as json.RawMessage,
// and ensures rows are closed and terminal errors checked before returning.
func scanTemporalRows(ctx context.Context, rows pgx.Rows) ([]TemporalSamplePoint, *TemporalWindowSummary, error) {
	defer rows.Close()

	var (
		rowCount int
		points   = make([]TemporalSamplePoint, 0)
		summary  *TemporalWindowSummary
	)

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, nil, classifyReaderError(err)
		}

		var (
			srcUUID      uuid.UUID
			srcEntityID  string
			observedAt   *time.Time
			valueNumber  *float64
			valueText    *string
			valueBoolean *bool
			metadataRaw  []byte
			messageID    *uuid.UUID

			totalCount           int64
			numberCount          int64
			textCount            int64
			booleanCount         int64
			invalidTypeCount     int64
			invalidIntervalCount int64
			unknownUnitCount     int64
			malformedUnitCount   int64
			distinctUnitCount    int64
			minUnit              *string
		)

		dest := []any{
			&srcUUID,
			&srcEntityID,
			&observedAt,
			&valueNumber,
			&valueText,
			&valueBoolean,
			&metadataRaw,
			&messageID,
			&totalCount,
			&numberCount,
			&textCount,
			&booleanCount,
			&invalidTypeCount,
			&invalidIntervalCount,
			&unknownUnitCount,
			&malformedUnitCount,
			&distinctUnitCount,
			&minUnit,
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, nil, classifyReaderError(err)
		}

		rowCount++

		if summary == nil {
			summary = &TemporalWindowSummary{
				TotalCount:           totalCount,
				NumberCount:          numberCount,
				TextCount:            textCount,
				BooleanCount:         booleanCount,
				InvalidTypeCount:     invalidTypeCount,
				InvalidIntervalCount: invalidIntervalCount,
				UnknownUnitCount:     unknownUnitCount,
				MalformedUnitCount:   malformedUnitCount,
				DistinctUnitCount:    distinctUnitCount,
				MinUnit:              minUnit,
			}
		}

		if observedAt == nil {
			// Outer-join sentinel row indicates authorized Sensor with 0 observations.
			continue
		}

		// Count non-null value columns to enforce exactly one stored value type.
		nonNullCount := 0
		var valType string
		if valueNumber != nil {
			nonNullCount++
			valType = "number"
		}
		if valueText != nil {
			nonNullCount++
			valType = "text"
		}
		if valueBoolean != nil {
			nonNullCount++
			valType = "boolean"
		}

		if nonNullCount != 1 {
			return nil, nil, fmt.Errorf("%w: invalid temporal observation: expected exactly 1 non-null value column, got %d", ErrUnexpected, nonNullCount)
		}

		// Preserve metadata verbatim as json.RawMessage (defensive copy to isolate buffer reuse).
		var metadata json.RawMessage
		if len(metadataRaw) > 0 {
			metadata = make([]byte, len(metadataRaw))
			copy(metadata, metadataRaw)
		} else {
			metadata = json.RawMessage("{}")
		}

		points = append(points, TemporalSamplePoint{
			ObservedAt:   observedAt.UTC(),
			ValueType:    valType,
			ValueNumber:  valueNumber,
			ValueText:    valueText,
			ValueBoolean: valueBoolean,
			Metadata:     metadata,
			MessageID:    messageID,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, nil, classifyReaderError(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, classifyReaderError(err)
	}

	if rowCount == 0 {
		return nil, nil, ErrNotFound
	}

	// In unit test mocks that only populated observation columns without setting dest[8..17],
	// derive summary from points to keep legacy mocks functional.
	if summary != nil && summary.TotalCount == 0 && len(points) > 0 {
		populateSummaryFromPoints(summary, points)
	}

	return points, summary, nil
}

// populateSummaryFromPoints derives summary statistics from points slice for test mocks.
func populateSummaryFromPoints(s *TemporalWindowSummary, points []TemporalSamplePoint) {
	s.TotalCount = int64(len(points))
	units := make(map[string]struct{})
	for _, p := range points {
		switch p.ValueType {
		case "number":
			s.NumberCount++
			if p.ValueNumber != nil {
				val := *p.ValueNumber
				if isInvalidSampleInterval(val) {
					s.InvalidIntervalCount++
				}
			} else {
				s.InvalidTypeCount++
			}
		case "text":
			s.TextCount++
		case "boolean":
			s.BooleanCount++
		default:
			s.InvalidTypeCount++
		}

		u, malformed := inspectMetadataUnit(p.Metadata)
		if malformed {
			s.MalformedUnitCount++
		} else if u == nil {
			s.UnknownUnitCount++
		} else {
			units[*u] = struct{}{}
			if s.MinUnit == nil || *u < *s.MinUnit {
				unitCopy := *u
				s.MinUnit = &unitCopy
			}
		}
	}
	s.DistinctUnitCount = int64(len(units))
}

// inspectMetadataUnit inspects the "unit" key in metadata JSONB.
// Returns unit string pointer if string; (nil, false) if missing or JSON null;
// (nil, true) if malformed JSON or non-string unit (number, bool, array, object).
func inspectMetadataUnit(metadata json.RawMessage) (unit *string, isMalformed bool) {
	if len(metadata) == 0 {
		return nil, false
	}
	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &rawMap); err != nil {
		return nil, true
	}
	rawVal, ok := rawMap["unit"]
	if !ok {
		return nil, false // missing
	}
	trimmed := strings.TrimSpace(string(rawVal))
	if trimmed == "null" {
		return nil, false // JSON null
	}
	var s string
	if err := json.Unmarshal(rawVal, &s); err != nil {
		return nil, true // non-string (e.g. number, bool, array, object)
	}
	return &s, false
}

// readTemporalBucketInternal executes the parameterized bucket query for a specific property name
// and bucket width, scanning aggregated bucket points and whole-window summary statistics.
func (r *PostgresTwinReader) readTemporalBucketInternal(
	ctx context.Context,
	actor uuid.UUID,
	entity ResolvedEntity,
	propertyName string,
	from time.Time,
	to time.Time,
	bucketWidth time.Duration,
	maxPoints int,
) ([]TemporalBucketPoint, *TemporalWindowSummary, error) {
	if actor == uuid.Nil {
		return nil, nil, ErrNilActor
	}
	if ctx.Err() != nil {
		return nil, nil, classifyReaderError(ctx.Err())
	}
	if entity.Type != EntityTypeSensor {
		return nil, nil, ErrInvalidProperty
	}
	if maxPoints <= 0 {
		return nil, nil, ErrInvalidInput
	}

	interval := pgtype.Interval{
		Microseconds: bucketWidth.Microseconds(),
		Valid:        true,
	}

	limit := maxPoints + 1
	rows, err := r.database.Query(
		ctx,
		bucketTemporalHistoryQuery,
		entity.SensorID,
		actor,
		entity.InternalID,
		entity.PublicURN,
		entity.GatewayID,
		propertyName,
		from.UTC(),
		to.UTC(),
		interval,
		limit,
	)
	if err != nil {
		return nil, nil, classifyReaderError(err)
	}

	return scanTemporalBucketRows(ctx, rows)
}

// scanTemporalBucketRows iterates over pgx.Rows, scanning aggregated bucket points and whole-window summary.
// It skips the outer-join NULL sentinel row for authorized sensors with 0 observations,
// validates completeness of aggregate values, and ensures rows are closed and terminal errors checked.
func scanTemporalBucketRows(ctx context.Context, rows pgx.Rows) ([]TemporalBucketPoint, *TemporalWindowSummary, error) {
	defer rows.Close()

	var (
		rowCount int
		buckets  = make([]TemporalBucketPoint, 0)
		summary  *TemporalWindowSummary
	)

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, nil, classifyReaderError(err)
		}

		var (
			srcUUID     uuid.UUID
			srcEntityID string
			bucketStart *time.Time
			avgValue    *float64
			minValue    *float64
			maxValue    *float64
			sampleCount *int64

			totalCount           int64
			numberCount          int64
			textCount            int64
			booleanCount         int64
			invalidTypeCount     int64
			invalidIntervalCount int64
			unknownUnitCount     int64
			malformedUnitCount   int64
			distinctUnitCount    int64
			minUnit              *string
		)

		dest := []any{
			&srcUUID,
			&srcEntityID,
			&bucketStart,
			&avgValue,
			&minValue,
			&maxValue,
			&sampleCount,
			&totalCount,
			&numberCount,
			&textCount,
			&booleanCount,
			&invalidTypeCount,
			&invalidIntervalCount,
			&unknownUnitCount,
			&malformedUnitCount,
			&distinctUnitCount,
			&minUnit,
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, nil, classifyReaderError(err)
		}

		rowCount++

		if summary == nil {
			summary = &TemporalWindowSummary{
				TotalCount:           totalCount,
				NumberCount:          numberCount,
				TextCount:            textCount,
				BooleanCount:         booleanCount,
				InvalidTypeCount:     invalidTypeCount,
				InvalidIntervalCount: invalidIntervalCount,
				UnknownUnitCount:     unknownUnitCount,
				MalformedUnitCount:   malformedUnitCount,
				DistinctUnitCount:    distinctUnitCount,
				MinUnit:              minUnit,
			}
		}

		if bucketStart == nil {
			// Outer-join sentinel row indicates authorized Sensor with 0 observations.
			continue
		}

		if avgValue == nil || minValue == nil || maxValue == nil || sampleCount == nil {
			if summary != nil && summary.NumberCount == summary.TotalCount {
				return nil, nil, fmt.Errorf("%w: incomplete aggregate values in bucket row", ErrUnexpected)
			}
			continue
		}

		buckets = append(buckets, TemporalBucketPoint{
			BucketStart: bucketStart.UTC(),
			Avg:         *avgValue,
			Min:         *minValue,
			Max:         *maxValue,
			Count:       *sampleCount,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, nil, classifyReaderError(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, classifyReaderError(err)
	}

	if rowCount == 0 {
		return nil, nil, ErrNotFound
	}

	// In unit test mocks that only populated bucket columns without setting dest[7..16],
	// derive summary from buckets to keep mocks simple.
	if summary != nil && summary.TotalCount == 0 && len(buckets) > 0 {
		populateSummaryFromBuckets(summary, buckets)
	}

	return buckets, summary, nil
}

// populateSummaryFromBuckets derives summary statistics from bucket points for test mocks.
func populateSummaryFromBuckets(s *TemporalWindowSummary, buckets []TemporalBucketPoint) {
	for _, b := range buckets {
		s.TotalCount += b.Count
		s.NumberCount += b.Count
	}
}

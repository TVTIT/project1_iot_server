package telemetry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"iot-platform/internal/gateway"
)

// HistoryDatabase is the small query interface required by the history repository.
// pgxpool.Pool satisfies it directly without creating a new pool or altering writer transactions.
type HistoryDatabase interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// PostgresHistoryRepository executes authorized read queries for telemetry history data.
type PostgresHistoryRepository struct {
	database HistoryDatabase
	cfg      HistoryPreparationConfig
}

var _ HistoryReader = (*PostgresHistoryRepository)(nil)

// NewPostgresHistoryRepository creates a PostgreSQL history repository for telemetry data.
// It verifies that database is non-nil (rejecting typed nils) and that cfg satisfies
// history preparation and bounds constraints so callers cannot bypass configured limits.
func NewPostgresHistoryRepository(database HistoryDatabase, cfg HistoryPreparationConfig) (*PostgresHistoryRepository, error) {
	if isNilDependency(database) {
		return nil, fmt.Errorf("database is required")
	}
	if err := ValidateHistoryPreparationConfig(cfg); err != nil {
		return nil, err
	}
	return &PostgresHistoryRepository{
		database: database,
		cfg:      cfg,
	}, nil
}

const rawTelemetryHistoryQuery = `WITH authorized_sensor AS (
	SELECT s.gateway_id, s.sensor_id, s.unit
	FROM public.sensors s
	JOIN public.user_gateways ug
	  ON ug.gateway_id = s.gateway_id AND ug.user_id = $1
	WHERE s.gateway_id = $2 AND s.sensor_id = $3
)
SELECT
	src.gateway_id,
	src.sensor_id,
	src.unit,
	t.measured_at,
	t.value,
	t.sequence_num,
	t.boot_id,
	t.message_id,
	t.sample_index
FROM authorized_sensor src
LEFT JOIN public.telemetry t
  ON t.gateway_id = src.gateway_id
 AND t.sensor_id = src.sensor_id
 AND t.measured_at >= $4
 AND t.measured_at < $5
ORDER BY t.measured_at ASC, t.sample_id ASC
LIMIT $6`

const bucketTelemetryHistoryQuery = `WITH authorized_sensor AS (
	SELECT s.gateway_id, s.sensor_id, s.unit
	FROM public.sensors s
	JOIN public.user_gateways ug
	  ON ug.gateway_id = s.gateway_id AND ug.user_id = $1
	WHERE s.gateway_id = $2 AND s.sensor_id = $3
),
clipped_telemetry AS (
	SELECT
		t.gateway_id,
		t.sensor_id,
		time_bucket($4::interval, t.measured_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS bucket_start,
		AVG(t.value) AS avg_value,
		MIN(t.value) AS min_value,
		MAX(t.value) AS max_value,
		COUNT(*) AS sample_count
	FROM public.telemetry t
	JOIN authorized_sensor a
	  ON a.gateway_id = t.gateway_id AND a.sensor_id = t.sensor_id
	WHERE t.measured_at >= $5
	  AND t.measured_at < $6
	GROUP BY t.gateway_id, t.sensor_id, bucket_start
)
SELECT
	src.gateway_id,
	src.sensor_id,
	src.unit,
	b.bucket_start,
	b.avg_value,
	b.min_value,
	b.max_value,
	b.sample_count
FROM authorized_sensor src
LEFT JOIN clipped_telemetry b
  ON b.gateway_id = src.gateway_id AND b.sensor_id = src.sensor_id
ORDER BY b.bucket_start ASC
LIMIT $7`

// ReadTelemetryHistory executes an authorized history read query for raw or bucket resolutions.
// It scopes access via user_gateways actor membership in SQL, distinguishes zero source
// rows (not found) from nullable sentinel (authorized empty), preserves 64-bit sequences and
// nullable provenance for raw mode, aggregates float64 min/max/avg and int64 counts for bucket mode,
// and detects overflow without silent truncation.
func (r *PostgresHistoryRepository) ReadTelemetryHistory(
	ctx context.Context,
	actor uuid.UUID,
	q TelemetryHistoryQuery,
) (*TelemetryHistoryResult, error) {
	if ctx == nil {
		return nil, ErrHistoryInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(ErrHistoryDependency, err)
	}

	if actor == uuid.Nil {
		return nil, ErrHistoryNilActor
	}

	if err := gateway.ValidateGatewayID(q.GatewayID); err != nil {
		return nil, ErrHistoryInvalidInput
	}
	if err := gateway.ValidateSensorID(q.SensorID); err != nil {
		return nil, ErrHistoryInvalidInput
	}

	if q.MaxPoints < r.cfg.MinPoints || q.MaxPoints > r.cfg.MaxPoints {
		return nil, ErrHistoryInvalidInput
	}

	if q.Resolution == ResolutionAuto {
		return nil, fmt.Errorf("%w: resolution %q is not supported by bucket reader (use HistoryService for auto)", ErrHistoryInvalidResolution, q.Resolution)
	}

	if err := ValidateHistoryTimeRange(q.Range, q.Resolution, r.cfg.MaxRange, r.cfg.RawWindow, q.MaxPoints); err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, errors.Join(ErrHistoryDependency, err)
	}

	if q.Resolution == ResolutionRaw {
		return r.readRawTelemetryHistory(ctx, actor, q)
	}

	dur, ok := ResolutionDuration(q.Resolution)
	if !ok {
		return nil, fmt.Errorf("%w: resolution %q is not supported by bucket reader", ErrHistoryInvalidResolution, q.Resolution)
	}

	return r.readBucketTelemetryHistory(ctx, actor, q, dur)
}

func (r *PostgresHistoryRepository) readRawTelemetryHistory(
	ctx context.Context,
	actor uuid.UUID,
	q TelemetryHistoryQuery,
) (*TelemetryHistoryResult, error) {
	limit := q.MaxPoints + 1
	rows, err := r.database.Query(
		ctx,
		rawTelemetryHistoryQuery,
		actor,
		q.GatewayID,
		q.SensorID,
		q.Range.From.UTC(),
		q.Range.To.UTC(),
		limit,
	)
	if err != nil {
		return nil, classifyHistoryError(err)
	}
	defer rows.Close()

	if err := ctx.Err(); err != nil {
		return nil, errors.Join(ErrHistoryDependency, err)
	}

	var (
		rowCount     int
		resolvedGW   string
		resolvedSens string
		currentUnit  *string
		points       = make([]RawSamplePoint, 0)
	)

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(ErrHistoryDependency, err)
		}

		var (
			gwID        string
			sID         string
			unit        *string
			measuredAt  *time.Time
			value       *float64
			sequenceNum *int64
			bootID      *uuid.UUID
			messageID   *uuid.UUID
			sampleIndex *int32
		)

		if err := rows.Scan(
			&gwID,
			&sID,
			&unit,
			&measuredAt,
			&value,
			&sequenceNum,
			&bootID,
			&messageID,
			&sampleIndex,
		); err != nil {
			return nil, classifyHistoryError(err)
		}

		if rowCount == 0 {
			resolvedGW = gwID
			resolvedSens = sID
			currentUnit = unit
		}
		rowCount++

		if measuredAt == nil {
			// Nullable sentinel row indicates authorized sensor with zero telemetry samples.
			continue
		}

		if value == nil || sequenceNum == nil {
			return nil, errors.Join(ErrHistoryUnexpected, errors.New("incomplete sample values in telemetry row"))
		}

		points = append(points, RawSamplePoint{
			MeasuredAt:  *measuredAt,
			Value:       *value,
			SequenceNum: *sequenceNum,
			BootID:      bootID,
			MessageID:   messageID,
			SampleIndex: sampleIndex,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, classifyHistoryError(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(ErrHistoryDependency, err)
	}

	if rowCount == 0 {
		return nil, ErrHistoryNotFound
	}

	if len(points) > q.MaxPoints {
		return nil, ErrHistoryRawLimitExceeded
	}

	return &TelemetryHistoryResult{
		GatewayID:  resolvedGW,
		SensorID:   resolvedSens,
		Unit:       currentUnit,
		From:       q.Range.From.UTC(),
		To:         q.Range.To.UTC(),
		Mode:       string(ResolutionRaw),
		Resolution: string(ResolutionRaw),
		PointCount: len(points),
		RawPoints:  points,
	}, nil
}

func (r *PostgresHistoryRepository) readBucketTelemetryHistory(
	ctx context.Context,
	actor uuid.UUID,
	q TelemetryHistoryQuery,
	bucketWidth time.Duration,
) (*TelemetryHistoryResult, error) {
	interval := pgtype.Interval{
		Microseconds: bucketWidth.Microseconds(),
		Valid:        true,
	}

	limit := q.MaxPoints + 1
	rows, err := r.database.Query(
		ctx,
		bucketTelemetryHistoryQuery,
		actor,
		q.GatewayID,
		q.SensorID,
		interval,
		q.Range.From.UTC(),
		q.Range.To.UTC(),
		limit,
	)
	if err != nil {
		return nil, classifyHistoryError(err)
	}
	defer rows.Close()

	if err := ctx.Err(); err != nil {
		return nil, errors.Join(ErrHistoryDependency, err)
	}

	var (
		rowCount     int
		resolvedGW   string
		resolvedSens string
		currentUnit  *string
		points       = make([]BucketSamplePoint, 0)
	)

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(ErrHistoryDependency, err)
		}

		var (
			gwID        string
			sID         string
			unit        *string
			bucketStart *time.Time
			avgValue    *float64
			minValue    *float64
			maxValue    *float64
			sampleCount *int64
		)

		if err := rows.Scan(
			&gwID,
			&sID,
			&unit,
			&bucketStart,
			&avgValue,
			&minValue,
			&maxValue,
			&sampleCount,
		); err != nil {
			return nil, classifyHistoryError(err)
		}

		if rowCount == 0 {
			resolvedGW = gwID
			resolvedSens = sID
			currentUnit = unit
		}
		rowCount++

		if bucketStart == nil {
			// Nullable sentinel row indicates authorized sensor with zero telemetry samples.
			continue
		}

		if avgValue == nil || minValue == nil || maxValue == nil || sampleCount == nil {
			return nil, errors.Join(ErrHistoryUnexpected, errors.New("incomplete aggregate values in bucket row"))
		}

		points = append(points, BucketSamplePoint{
			BucketStart: *bucketStart,
			Avg:         *avgValue,
			Min:         *minValue,
			Max:         *maxValue,
			Count:       *sampleCount,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, classifyHistoryError(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(ErrHistoryDependency, err)
	}

	if rowCount == 0 {
		return nil, ErrHistoryNotFound
	}

	if len(points) > q.MaxPoints {
		return nil, ErrHistoryResolutionTooFine
	}

	return &TelemetryHistoryResult{
		GatewayID:    resolvedGW,
		SensorID:     resolvedSens,
		Unit:         currentUnit,
		From:         q.Range.From.UTC(),
		To:           q.Range.To.UTC(),
		Mode:         "bucket",
		Resolution:   string(q.Resolution),
		PointCount:   len(points),
		BucketPoints: points,
	}, nil
}

func classifyHistoryError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(ErrHistoryDependency, err)
	}
	classified := gateway.ClassifyPostgresAvailabilityError(err, ErrHistoryDependency)
	if errors.Is(classified, ErrHistoryDependency) {
		return classified
	}
	return errors.Join(ErrHistoryUnexpected, err)
}

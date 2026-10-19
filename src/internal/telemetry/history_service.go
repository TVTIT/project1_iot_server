package telemetry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// AllowedBucketResolutions defines the project allowlist of bucket resolutions in strictly ascending order of duration.
var AllowedBucketResolutions = []Resolution{
	Resolution100ms,
	Resolution1s,
	Resolution10s,
	Resolution1m,
	Resolution5m,
	Resolution15m,
	Resolution1h,
	Resolution6h,
	Resolution1d,
}

// SelectAutoBucketResolution selects the smallest allowed bucket resolution from the project allowlist
// such that IntersectingBucketCount(from, to, width) <= maxPoints.
// It uses mathematical floor division relative to Unix epoch (including partial edges and pre-epoch ranges).
// If no allowed resolution satisfies the maxPoints constraint, it returns ErrHistoryResolutionTooFine.
func SelectAutoBucketResolution(from, to time.Time, maxPoints int) (Resolution, error) {
	if maxPoints <= 0 {
		return "", ErrHistoryInvalidInput
	}
	if !from.Before(to) {
		return "", ErrHistoryInvalidInput
	}

	for _, res := range AllowedBucketResolutions {
		dur, ok := ResolutionDuration(res)
		if !ok {
			continue
		}
		count, err := IntersectingBucketCount(from, to, dur)
		if err != nil {
			return "", ErrHistoryInvalidInput
		}
		if count <= int64(maxPoints) {
			return res, nil
		}
	}

	return "", ErrHistoryResolutionTooFine
}

// TelemetryHistoryService orchestrates telemetry history reads, auto-resolution selection,
// single-deadline scoping, and data queries.
//
//nolint:revive // Telemetry prefix disambiguates from Digital Twin history service types.
type TelemetryHistoryService struct {
	prep   *HistoryPreparation
	reader HistoryReader
	cfg    HistoryPreparationConfig
}

var _ HistoryService = (*TelemetryHistoryService)(nil)

// NewTelemetryHistoryService creates a new HistoryService implementation orchestrating
// validation, membership resolution, auto-resolution selection, and history reading.
// It verifies that resolver and reader are non-nil (rejecting typed nils) and that cfg is valid.
func NewTelemetryHistoryService(
	resolver SensorResolver,
	reader HistoryReader,
	cfg HistoryPreparationConfig,
) (*TelemetryHistoryService, error) {
	if isNilDependency(resolver) {
		return nil, fmt.Errorf("resolver is required")
	}
	if isNilDependency(reader) {
		return nil, fmt.Errorf("reader is required")
	}
	if err := ValidateHistoryPreparationConfig(cfg); err != nil {
		return nil, err
	}
	prep, err := NewHistoryPreparation(resolver, cfg)
	if err != nil {
		return nil, err
	}
	return &TelemetryHistoryService{
		prep:   prep,
		reader: reader,
		cfg:    cfg,
	}, nil
}

// NewHistoryService is an alias for NewTelemetryHistoryService matching the HistoryService interface convention.
func NewHistoryService(
	resolver SensorResolver,
	reader HistoryReader,
	cfg HistoryPreparationConfig,
) (*TelemetryHistoryService, error) {
	return NewTelemetryHistoryService(resolver, reader, cfg)
}

// NewTelemetryHistoryServiceFromPreparation creates a HistoryService reusing an existing HistoryPreparation
// orchestrator and HistoryReader, ensuring preparation and service use the exact same configuration.
func NewTelemetryHistoryServiceFromPreparation(
	prep *HistoryPreparation,
	reader HistoryReader,
) (*TelemetryHistoryService, error) {
	if isNilDependency(prep) {
		return nil, fmt.Errorf("preparation is required")
	}
	if isNilDependency(reader) {
		return nil, fmt.Errorf("reader is required")
	}
	return &TelemetryHistoryService{
		prep:   prep,
		reader: reader,
		cfg:    prep.cfg,
	}, nil
}

// GetTelemetryHistory executes an authorized telemetry history read request.
// It validates the actor and query constraints, resolves sensor membership, selects
// effective resolution (probing raw for short auto windows or selecting bucket for long windows),
// and executes the data query under a single unified timeout budget.
func (s *TelemetryHistoryService) GetTelemetryHistory(
	ctx context.Context,
	actor uuid.UUID,
	q TelemetryHistoryQuery,
) (*TelemetryHistoryResult, error) {
	if isNilDependency(s.reader) {
		return nil, ErrHistoryDependency
	}
	if isNilDependency(s.prep) {
		return nil, ErrHistoryDependency
	}
	if actor == uuid.Nil {
		return nil, ErrHistoryNilActor
	}

	var capturedResult *TelemetryHistoryResult
	err := s.prep.PrepareTelemetryHistory(ctx, actor, q, func(scopedCtx context.Context, _ ResolvedSensor, prepQuery TelemetryHistoryQuery) error {
		res, execErr := s.executeEffectiveQuery(scopedCtx, actor, prepQuery)
		if execErr != nil {
			return execErr
		}
		if scopedCtx.Err() != nil {
			return fmt.Errorf("%w: %w", ErrHistoryDependency, scopedCtx.Err())
		}
		capturedResult = res
		return nil
	})
	if err != nil {
		return nil, err
	}
	if capturedResult == nil {
		return nil, ErrHistoryUnexpected
	}
	return capturedResult, nil
}

// ExecuteEffectiveHistoryQuery executes the effective history data query using an existing
// scoped context with caller-managed deadline/timeout.
//
// Reusable Entry for Digital Twin History Delegation:
// This method does NOT initiate a new timeout/budget. Callers (such as future Digital Twin
// history service delegation) MUST supply a context with an active deadline and MUST
// have validated the actor and query bounds.
// In the telemetry service path, this is called directly from within HistoryPreparation's
// scoped callback, sharing the single unified query timeout.
func (s *TelemetryHistoryService) ExecuteEffectiveHistoryQuery(
	ctx context.Context,
	actor uuid.UUID,
	q TelemetryHistoryQuery,
) (*TelemetryHistoryResult, error) {
	if ctx == nil {
		return nil, ErrHistoryInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrHistoryDependency, err)
	}
	if isNilDependency(s.reader) {
		return nil, ErrHistoryDependency
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
	if q.MaxPoints < s.cfg.MinPoints || q.MaxPoints > s.cfg.MaxPoints {
		return nil, ErrHistoryInvalidInput
	}
	if err := ValidateHistoryTimeRange(q.Range, q.Resolution, s.cfg.MaxRange, s.cfg.RawWindow, q.MaxPoints); err != nil {
		return nil, err
	}

	res, err := s.executeEffectiveQuery(ctx, actor, q)
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("%w: %w", ErrHistoryDependency, ctx.Err())
	}
	return res, nil
}

func (s *TelemetryHistoryService) executeEffectiveQuery(
	ctx context.Context,
	actor uuid.UUID,
	q TelemetryHistoryQuery,
) (*TelemetryHistoryResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrHistoryDependency, err)
	}

	switch q.Resolution {
	case ResolutionRaw:
		return s.reader.ReadTelemetryHistory(ctx, actor, q)

	case ResolutionAuto:
		diffMicros := ToMicrosecondsSinceEpoch(q.Range.To.UTC()) - ToMicrosecondsSinceEpoch(q.Range.From.UTC())
		isShort := diffMicros <= s.cfg.RawWindow.Microseconds()

		if isShort {
			// Short auto window: probe raw first
			rawQuery := q
			rawQuery.Resolution = ResolutionRaw
			rawResult, err := s.reader.ReadTelemetryHistory(ctx, actor, rawQuery)
			if err == nil {
				return rawResult, nil
			}
			// Only ErrHistoryRawLimitExceeded triggers discard + bucket fallback
			if !errors.Is(err, ErrHistoryRawLimitExceeded) {
				return nil, err
			}
			// Context check between probe and fallback: cancel prevents next query
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, fmt.Errorf("%w: %w", ErrHistoryDependency, ctxErr)
			}
		}

		// Direct bucket selection for long auto window or fallback on raw limit exceeded
		bucketRes, err := SelectAutoBucketResolution(q.Range.From, q.Range.To, q.MaxPoints)
		if err != nil {
			return nil, err
		}

		bucketQuery := q
		bucketQuery.Resolution = bucketRes
		return s.reader.ReadTelemetryHistory(ctx, actor, bucketQuery)

	default:
		// Explicit bucket resolution (e.g. "100ms", "1s", "10s", etc.)
		return s.reader.ReadTelemetryHistory(ctx, actor, q)
	}
}

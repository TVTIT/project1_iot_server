package digitaltwin

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

// readService coordinates Digital Twin read operations by delegating bounds validation,
// actor verification, and single-budget context scoping to ReadPreparation, followed by
// data reading from TwinReader or delegation to TelemetryHistoryDelegator.
type readService struct {
	preparation      *ReadPreparation
	reader           TwinReader
	telemetryHistory TelemetryHistoryDelegator
}

var _ ReadService = (*readService)(nil)

// NewReadService creates a new ReadService implementation.
// It verifies that preparation, reader, and telemetryHistory dependencies are non-nil (rejecting typed nils).
func NewReadService(preparation *ReadPreparation, reader TwinReader, telemetryHistory TelemetryHistoryDelegator) (ReadService, error) {
	if isNilDependency(preparation) {
		return nil, fmt.Errorf("preparation is required")
	}
	if isNilDependency(reader) {
		return nil, fmt.Errorf("reader is required")
	}
	if isNilDependency(telemetryHistory) {
		return nil, fmt.Errorf("telemetryHistory is required")
	}
	return &readService{
		preparation:      preparation,
		reader:           reader,
		telemetryHistory: telemetryHistory,
	}, nil
}

// ListTwins executes membership-scoped entity listing within a unified deadline budget.
// If preparation returns an error (such as validation failure, nil actor, or context deadline expiry),
// no partial result is exposed.
func (s *readService) ListTwins(ctx context.Context, actor uuid.UUID, q TwinListQuery) (*TwinListResult, error) {
	if isNilDependency(s) || isNilDependency(s.preparation) || isNilDependency(s.reader) {
		return nil, ErrDependencyUnavailable
	}

	var result *TwinListResult
	err := s.preparation.PrepareList(ctx, actor, q, func(scopedCtx context.Context, validatedQ TwinListQuery) error {
		res, err := s.reader.ListEntities(scopedCtx, actor, validatedQ)
		if err != nil {
			return err
		}
		if res == nil {
			return fmt.Errorf("%w: reader returned nil result without error", ErrUnexpected)
		}
		result = res
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetTwinDetail executes entity resolution, membership recheck, and graph detail reading within a unified deadline budget.
// If preparation or reader fails, no partial result is exposed.
func (s *readService) GetTwinDetail(ctx context.Context, actor uuid.UUID, q TwinDetailQuery) (*TwinDetailResult, error) {
	if isNilDependency(s) || isNilDependency(s.preparation) || isNilDependency(s.reader) {
		return nil, ErrDependencyUnavailable
	}

	var result *TwinDetailResult
	err := s.preparation.PrepareDetail(ctx, actor, q, func(scopedCtx context.Context, entity ResolvedEntity, validatedQ TwinDetailQuery) error {
		res, err := s.reader.GetDetail(scopedCtx, actor, entity, validatedQ)
		if err != nil {
			return err
		}
		if res == nil {
			return fmt.Errorf("%w: reader returned nil result without error", ErrUnexpected)
		}
		result = res
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetTwinState executes entity resolution, membership recheck, and current reported/desired state reading within a unified deadline budget.
// If preparation or reader fails, no partial result is exposed.
func (s *readService) GetTwinState(ctx context.Context, actor uuid.UUID, q TwinStateQuery) (*TwinStateResult, error) {
	if isNilDependency(s) || isNilDependency(s.preparation) || isNilDependency(s.reader) {
		return nil, ErrDependencyUnavailable
	}

	var result *TwinStateResult
	err := s.preparation.PrepareState(ctx, actor, q, func(scopedCtx context.Context, entity ResolvedEntity, _ TwinStateQuery) error {
		res, err := s.reader.GetState(scopedCtx, actor, entity)
		if err != nil {
			return err
		}
		if res == nil {
			return fmt.Errorf("%w: reader returned nil result without error", ErrUnexpected)
		}
		result = res
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetTwinHistory executes sensor entity resolution, membership recheck, and temporal history reading within a unified deadline budget.
// If Property is SelectorSampleIntervalUS, it calls TwinReader.ReadTemporalHistory.
// If Property is SelectorValue, it delegates to the telemetry history engine via TelemetryHistoryDelegator.
// If preparation, reader, or delegator fails, no partial result is exposed.
func (s *readService) GetTwinHistory(ctx context.Context, actor uuid.UUID, q TwinHistoryQuery) (*TwinHistoryResult, error) {
	if isNilDependency(s) || isNilDependency(s.preparation) || isNilDependency(s.reader) {
		return nil, ErrDependencyUnavailable
	}

	var result *TwinHistoryResult
	err := s.preparation.PrepareHistory(ctx, actor, q, func(scopedCtx context.Context, entity ResolvedEntity, validatedQ TwinHistoryQuery) error {
		switch validatedQ.Property {
		case SelectorSampleIntervalUS:
			res, err := s.reader.ReadTemporalHistory(scopedCtx, actor, entity, validatedQ)
			if err != nil {
				return err
			}
			if res == nil {
				return fmt.Errorf("%w: reader returned nil result without error", ErrUnexpected)
			}
			result = res
			return nil
		case SelectorValue:
			res, err := s.delegateTelemetryHistory(scopedCtx, actor, entity, validatedQ)
			if err != nil {
				return err
			}
			if res == nil {
				return fmt.Errorf("%w: telemetry delegator returned nil result without error", ErrUnexpected)
			}
			result = res
			return nil
		default:
			return ErrInvalidProperty
		}
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// delegateTelemetryHistory delegates property=value history queries to the telemetry history engine.
// It constructs a TelemetryHistoryQuery using only the resolved sensor entity and validated query parameters,
// calls ExecuteEffectiveHistoryQuery within the existing scoped context and actor, wraps the result in TwinHistoryResult,
// and maps telemetry errors to the Digital Twin domain error family while preserving original causes for errors.Is.
func (s *readService) delegateTelemetryHistory(ctx context.Context, actor uuid.UUID, entity ResolvedEntity, q TwinHistoryQuery) (*TwinHistoryResult, error) {
	if isNilDependency(s.telemetryHistory) {
		return nil, ErrDependencyUnavailable
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
	}

	telemQ := telemetry.TelemetryHistoryQuery{
		GatewayID:  entity.GatewayID,
		SensorID:   entity.SensorID,
		Range:      q.Range,
		Resolution: q.Resolution,
		MaxPoints:  q.MaxPoints,
	}

	telemRes, err := s.telemetryHistory.ExecuteEffectiveHistoryQuery(ctx, actor, telemQ)
	if ctx.Err() != nil {
		if err != nil && !errors.Is(err, ctx.Err()) {
			return nil, fmt.Errorf("%w: %w: %w", ErrDependencyUnavailable, ctx.Err(), err)
		}
		return nil, fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
	}
	if err != nil {
		return nil, mapTelemetryDelegationError(err)
	}
	if telemRes == nil {
		return nil, fmt.Errorf("%w: telemetry delegator returned nil result without error", ErrUnexpected)
	}

	return &TwinHistoryResult{
		EntityID:        entity.PublicURN,
		Property:        SelectorValue,
		Unit:            telemRes.Unit,
		From:            telemRes.From,
		To:              telemRes.To,
		Mode:            telemRes.Mode,
		Resolution:      telemRes.Resolution,
		PointCount:      telemRes.PointCount,
		TelemetryResult: telemRes,
	}, nil
}

// mapTelemetryDelegationError maps errors returned by the telemetry history engine to the Digital Twin
// error family, while preserving original causes for errors.Is.
func mapTelemetryDelegationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrRangeTooLarge) ||
		errors.Is(err, ErrInvalidResolution) ||
		errors.Is(err, ErrRawLimitExceeded) ||
		errors.Is(err, ErrResolutionTooFine) {
		return err
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, telemetry.ErrHistoryNotFound) {
		if errors.Is(err, ErrNotFound) {
			return err
		}
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	}
	if errors.Is(err, ErrDependencyUnavailable) ||
		errors.Is(err, telemetry.ErrHistoryDependency) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		if errors.Is(err, ErrDependencyUnavailable) {
			return err
		}
		return fmt.Errorf("%w: %w", ErrDependencyUnavailable, err)
	}
	if errors.Is(err, ErrInvalidInput) || errors.Is(err, telemetry.ErrHistoryInvalidInput) {
		if errors.Is(err, ErrInvalidInput) {
			return err
		}
		return fmt.Errorf("%w: %w", ErrInvalidInput, err)
	}
	if errors.Is(err, ErrNilActor) || errors.Is(err, telemetry.ErrHistoryNilActor) {
		if errors.Is(err, ErrNilActor) {
			return err
		}
		return fmt.Errorf("%w: %w", ErrNilActor, err)
	}
	if errors.Is(err, ErrUnexpected) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrUnexpected, err)
}

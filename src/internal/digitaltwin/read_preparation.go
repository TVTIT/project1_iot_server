package digitaltwin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

// ReadPreparationConfig configures timeouts and bounds for Digital Twin read operations.
type ReadPreparationConfig struct {
	QueryTimeout         time.Duration
	ListMaxLimit         int
	RelationshipMaxLimit int
	HistoryMaxRange      time.Duration
	HistoryRawWindow     time.Duration
	HistoryMinPoints     int
	HistoryMaxPoints     int
}

// DefaultReadPreparationConfig returns contract defaults for Digital Twin read operations.
func DefaultReadPreparationConfig() ReadPreparationConfig {
	return ReadPreparationConfig{
		QueryTimeout:         ContractDefaultQueryTimeout,
		ListMaxLimit:         ContractMaxLimit,
		RelationshipMaxLimit: ContractMaxLimit,
		HistoryMaxRange:      ContractMaxHistoryRange,
		HistoryRawWindow:     ContractMaxRawWindow,
		HistoryMinPoints:     ContractMinPointsLowerLimit,
		HistoryMaxPoints:     ContractMaxPointsUpperLimit,
	}
}

// NewReadPreparationConfig creates a validated ReadPreparationConfig.
func NewReadPreparationConfig(
	timeout time.Duration,
	listMaxLimit, relMaxLimit int,
	historyMaxRange, historyRawWindow time.Duration,
	historyMinPoints, historyMaxPoints int,
) (ReadPreparationConfig, error) {
	cfg := ReadPreparationConfig{
		QueryTimeout:         timeout,
		ListMaxLimit:         listMaxLimit,
		RelationshipMaxLimit: relMaxLimit,
		HistoryMaxRange:      historyMaxRange,
		HistoryRawWindow:     historyRawWindow,
		HistoryMinPoints:     historyMinPoints,
		HistoryMaxPoints:     historyMaxPoints,
	}
	if err := ValidateReadPreparationConfig(cfg); err != nil {
		return ReadPreparationConfig{}, err
	}
	return cfg, nil
}

// ValidateReadPreparationConfig validates Digital Twin read preparation configuration bounds.
func ValidateReadPreparationConfig(cfg ReadPreparationConfig) error {
	if cfg.QueryTimeout <= 0 {
		return fmt.Errorf("READ_QUERY_TIMEOUT must be a positive duration")
	}
	if cfg.QueryTimeout > ContractMaxQueryTimeout {
		return fmt.Errorf("READ_QUERY_TIMEOUT cannot exceed contract hard cap of %s", ContractMaxQueryTimeout)
	}
	if cfg.ListMaxLimit <= 0 || cfg.ListMaxLimit > ContractMaxLimit {
		return fmt.Errorf("TWIN_LIST_MAX_LIMIT cannot exceed contract hard cap of %d", ContractMaxLimit)
	}
	if cfg.RelationshipMaxLimit <= 0 || cfg.RelationshipMaxLimit > ContractMaxLimit {
		return fmt.Errorf("TWIN_RELATIONSHIP_MAX_LIMIT cannot exceed contract hard cap of %d", ContractMaxLimit)
	}
	if cfg.HistoryMaxRange <= 0 {
		return fmt.Errorf("HISTORY_MAX_RANGE must be a positive duration")
	}
	if cfg.HistoryMaxRange > ContractMaxHistoryRange {
		return fmt.Errorf("HISTORY_MAX_RANGE cannot exceed contract hard cap of %s", ContractMaxHistoryRange)
	}
	if cfg.HistoryRawWindow <= 0 {
		return fmt.Errorf("HISTORY_RAW_WINDOW must be a positive duration")
	}
	if cfg.HistoryRawWindow > ContractMaxRawWindow {
		return fmt.Errorf("HISTORY_RAW_WINDOW cannot exceed contract hard cap of %s", ContractMaxRawWindow)
	}
	if cfg.HistoryRawWindow > cfg.HistoryMaxRange {
		return fmt.Errorf("HISTORY_RAW_WINDOW cannot exceed HISTORY_MAX_RANGE")
	}
	if cfg.HistoryMinPoints < ContractMinPointsLowerLimit {
		return fmt.Errorf("HISTORY_MIN_POINTS cannot be less than contract lower limit of %d", ContractMinPointsLowerLimit)
	}
	if cfg.HistoryMinPoints > ContractMaxPointsUpperLimit {
		return fmt.Errorf("HISTORY_MIN_POINTS cannot exceed contract hard cap of %d", ContractMaxPointsUpperLimit)
	}
	if cfg.HistoryMaxPoints > ContractMaxPointsUpperLimit {
		return fmt.Errorf("HISTORY_MAX_POINTS cannot exceed contract hard cap of %d", ContractMaxPointsUpperLimit)
	}
	if cfg.HistoryMinPoints > cfg.HistoryMaxPoints {
		return fmt.Errorf("HISTORY_MIN_POINTS must be less than or equal to HISTORY_MAX_POINTS")
	}
	return nil
}

// ListCallback is invoked with the scoped context and validated query for List operations.
type ListCallback func(ctx context.Context, q TwinListQuery) error

// DetailCallback is invoked with the scoped context, resolved entity, and validated query for Detail operations.
type DetailCallback func(ctx context.Context, entity ResolvedEntity, q TwinDetailQuery) error

// StateCallback is invoked with the scoped context, resolved entity, and validated query for State operations.
type StateCallback func(ctx context.Context, entity ResolvedEntity, q TwinStateQuery) error

// HistoryCallback is invoked with the scoped context, resolved entity, and validated query for History operations.
type HistoryCallback func(ctx context.Context, entity ResolvedEntity, q TwinHistoryQuery) error

// ReadPreparation orchestrates actor and query validation, membership resolution,
// and single-budget context scoping for Digital Twin read operations.
//
// SQL Membership Recheck Documentation:
// The preparation layer verifies actor membership by resolving the target entity
// (Gateway or Sensor) through EntityResolver, which performs SQL JOINs against
// public.user_gateways (WHERE ug.user_id = $actor). For List queries, no resource precheck
// is performed because list queries represent membership-scoped sets.
// When subsequent data read queries execute in Tasks 4.2–4.4 inside the scoped callbacks:
// 1. Detail / State queries MUST re-verify or parameterize by actor membership in SQL queries.
// 2. List queries MUST include a JOIN public.user_gateways ug ON ug.gateway_id = e.gateway_id AND ug.user_id = $actor.
// 3. Temporal History queries in TimescaleDB MUST join public.user_gateways with ug.user_id = $actor.
// This guarantees defense-in-depth authorization without capability bypass or relying solely on prechecks.
type ReadPreparation struct {
	resolver EntityResolver
	cfg      ReadPreparationConfig
}

// NewReadPreparation creates a new ReadPreparation orchestrator.
// It verifies that resolver is non-nil (rejecting typed nils) and that cfg is valid.
func NewReadPreparation(resolver EntityResolver, cfg ReadPreparationConfig) (*ReadPreparation, error) {
	if isNilDependency(resolver) {
		return nil, fmt.Errorf("resolver is required")
	}
	if err := ValidateReadPreparationConfig(cfg); err != nil {
		return nil, err
	}
	return &ReadPreparation{
		resolver: resolver,
		cfg:      cfg,
	}, nil
}

// PrepareList orchestrates validation and context scoping for GET /v1/digital-twins.
// It validates actor and bounds before execution. It does NOT perform a single-resource precheck
// because List operations are membership-scoped sets executed directly by the query reader.
func (p *ReadPreparation) PrepareList(
	parentCtx context.Context,
	actor uuid.UUID,
	q TwinListQuery,
	fn ListCallback,
) error {
	if isNilDependency(p.resolver) {
		return ErrDependencyUnavailable
	}
	if actor == uuid.Nil {
		return ErrNilActor
	}
	if fn == nil {
		return ErrInvalidInput
	}
	if err := ValidateTwinListQuery(q, p.cfg.ListMaxLimit); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(parentCtx, p.cfg.QueryTimeout)
	defer cancel()

	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
	}

	err := fn(ctx, q)
	if ctx.Err() != nil {
		if err != nil && !errors.Is(err, ctx.Err()) {
			return fmt.Errorf("%w: %w: %w", ErrDependencyUnavailable, ctx.Err(), err)
		}
		return fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
	}
	return err
}

// PrepareDetail orchestrates validation, membership resolution, and context scoping for GET /v1/digital-twins/{entity_id}.
func (p *ReadPreparation) PrepareDetail(
	parentCtx context.Context,
	actor uuid.UUID,
	q TwinDetailQuery,
	fn DetailCallback,
) error {
	if isNilDependency(p.resolver) {
		return ErrDependencyUnavailable
	}
	if actor == uuid.Nil {
		return ErrNilActor
	}
	if fn == nil {
		return ErrInvalidInput
	}
	if err := ValidateTwinDetailQuery(q, p.cfg.RelationshipMaxLimit); err != nil {
		return err
	}

	parsed, err := ParseEntityURN(q.EntityID)
	if err != nil {
		return ErrInvalidInput
	}

	ctx, cancel := context.WithTimeout(parentCtx, p.cfg.QueryTimeout)
	defer cancel()

	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
	}

	entity, err := p.resolver.ResolveEntity(ctx, actor, parsed)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
		}
		return err
	}
	if entity == nil {
		return ErrNotFound
	}

	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
	}

	err = fn(ctx, *entity, q)
	if ctx.Err() != nil {
		if err != nil && !errors.Is(err, ctx.Err()) {
			return fmt.Errorf("%w: %w: %w", ErrDependencyUnavailable, ctx.Err(), err)
		}
		return fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
	}
	return err
}

// PrepareState orchestrates validation, membership resolution, and context scoping for GET /v1/digital-twins/{entity_id}/state.
func (p *ReadPreparation) PrepareState(
	parentCtx context.Context,
	actor uuid.UUID,
	q TwinStateQuery,
	fn StateCallback,
) error {
	if isNilDependency(p.resolver) {
		return ErrDependencyUnavailable
	}
	if actor == uuid.Nil {
		return ErrNilActor
	}
	if fn == nil {
		return ErrInvalidInput
	}
	if err := ValidateTwinStateQuery(q); err != nil {
		return err
	}

	parsed, err := ParseEntityURN(q.EntityID)
	if err != nil {
		return ErrInvalidInput
	}

	ctx, cancel := context.WithTimeout(parentCtx, p.cfg.QueryTimeout)
	defer cancel()

	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
	}

	entity, err := p.resolver.ResolveEntity(ctx, actor, parsed)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
		}
		return err
	}
	if entity == nil {
		return ErrNotFound
	}

	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
	}

	err = fn(ctx, *entity, q)
	if ctx.Err() != nil {
		if err != nil && !errors.Is(err, ctx.Err()) {
			return fmt.Errorf("%w: %w: %w", ErrDependencyUnavailable, ctx.Err(), err)
		}
		return fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
	}
	return err
}

// PrepareHistory orchestrates validation, membership resolution, and context scoping for GET /v1/digital-twins/{entity_id}/history.
func (p *ReadPreparation) PrepareHistory(
	parentCtx context.Context,
	actor uuid.UUID,
	q TwinHistoryQuery,
	fn HistoryCallback,
) error {
	if isNilDependency(p.resolver) {
		return ErrDependencyUnavailable
	}
	if actor == uuid.Nil {
		return ErrNilActor
	}
	if fn == nil {
		return ErrInvalidInput
	}
	if err := ValidateTwinHistoryQuery(q); err != nil {
		return err
	}

	parsed, err := ParseEntityURN(q.EntityID)
	if err != nil {
		return ErrInvalidInput
	}
	if parsed.Type != EntityTypeSensor {
		return ErrInvalidProperty
	}
	if q.MaxPoints < p.cfg.HistoryMinPoints || q.MaxPoints > p.cfg.HistoryMaxPoints {
		return ErrInvalidInput
	}
	if err := telemetry.ValidateHistoryTimeRange(q.Range, q.Resolution, p.cfg.HistoryMaxRange, p.cfg.HistoryRawWindow, q.MaxPoints); err != nil {
		return mapTelemetryHistoryError(err)
	}

	ctx, cancel := context.WithTimeout(parentCtx, p.cfg.QueryTimeout)
	defer cancel()

	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
	}

	entity, err := p.resolver.ResolveEntity(ctx, actor, parsed)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
		}
		return err
	}
	if entity == nil {
		return ErrNotFound
	}

	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
	}

	err = fn(ctx, *entity, q)
	if ctx.Err() != nil {
		if err != nil && !errors.Is(err, ctx.Err()) {
			return fmt.Errorf("%w: %w: %w", ErrDependencyUnavailable, ctx.Err(), err)
		}
		return fmt.Errorf("%w: %w", ErrDependencyUnavailable, ctx.Err())
	}
	return err
}

func mapTelemetryHistoryError(err error) error {
	if errors.Is(err, telemetry.ErrHistoryInvalidInput) {
		return ErrInvalidInput
	}
	return err
}

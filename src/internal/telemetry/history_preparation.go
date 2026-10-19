package telemetry

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// HistoryBounds constants for telemetry history preparation.
const (
	DefaultHistoryQueryTimeout = 5 * time.Second
	MaxHistoryQueryTimeout     = 5 * time.Second
	DefaultHistoryMaxRange     = 744 * time.Hour
	MaxHistoryMaxRange         = 744 * time.Hour
	DefaultHistoryRawWindow    = 10 * time.Second
	MaxHistoryRawWindow        = 10 * time.Second
	DefaultHistoryMinPoints    = 100
	MinHistoryPointsLowerLimit = 100
	DefaultHistoryMaxPoints    = 2000
	MaxHistoryPointsUpperLimit = 2000
)

// HistoryPreparationConfig configures the telemetry history preparation orchestrator.
type HistoryPreparationConfig struct {
	QueryTimeout time.Duration
	MaxRange     time.Duration
	RawWindow    time.Duration
	MinPoints    int
	MaxPoints    int
}

// DefaultHistoryPreparationConfig returns standard contract defaults for telemetry history queries.
func DefaultHistoryPreparationConfig() HistoryPreparationConfig {
	return HistoryPreparationConfig{
		QueryTimeout: DefaultHistoryQueryTimeout,
		MaxRange:     DefaultHistoryMaxRange,
		RawWindow:    DefaultHistoryRawWindow,
		MinPoints:    DefaultHistoryMinPoints,
		MaxPoints:    DefaultHistoryMaxPoints,
	}
}

// NewHistoryPreparationConfig creates a validated HistoryPreparationConfig.
func NewHistoryPreparationConfig(timeout, maxRange, rawWindow time.Duration, minPoints, maxPoints int) (HistoryPreparationConfig, error) {
	cfg := HistoryPreparationConfig{
		QueryTimeout: timeout,
		MaxRange:     maxRange,
		RawWindow:    rawWindow,
		MinPoints:    minPoints,
		MaxPoints:    maxPoints,
	}
	if err := ValidateHistoryPreparationConfig(cfg); err != nil {
		return HistoryPreparationConfig{}, err
	}
	return cfg, nil
}

// ValidateHistoryPreparationConfig validates history preparation configuration.
func ValidateHistoryPreparationConfig(cfg HistoryPreparationConfig) error {
	if cfg.QueryTimeout <= 0 {
		return fmt.Errorf("HISTORY_QUERY_TIMEOUT must be a positive duration")
	}
	if cfg.QueryTimeout > MaxHistoryQueryTimeout {
		return fmt.Errorf("HISTORY_QUERY_TIMEOUT cannot exceed contract hard cap of %s", MaxHistoryQueryTimeout)
	}
	if cfg.MaxRange <= 0 {
		return fmt.Errorf("HISTORY_MAX_RANGE must be a positive duration")
	}
	if cfg.MaxRange > MaxHistoryMaxRange {
		return fmt.Errorf("HISTORY_MAX_RANGE cannot exceed contract hard cap of %s", MaxHistoryMaxRange)
	}
	if cfg.RawWindow <= 0 {
		return fmt.Errorf("HISTORY_RAW_WINDOW must be a positive duration")
	}
	if cfg.RawWindow > MaxHistoryRawWindow {
		return fmt.Errorf("HISTORY_RAW_WINDOW cannot exceed contract hard cap of %s", MaxHistoryRawWindow)
	}
	if cfg.RawWindow > cfg.MaxRange {
		return fmt.Errorf("HISTORY_RAW_WINDOW cannot exceed HISTORY_MAX_RANGE")
	}
	if cfg.MinPoints < MinHistoryPointsLowerLimit {
		return fmt.Errorf("HISTORY_MIN_POINTS cannot be less than contract lower limit of %d", MinHistoryPointsLowerLimit)
	}
	if cfg.MinPoints > MaxHistoryPointsUpperLimit {
		return fmt.Errorf("HISTORY_MIN_POINTS cannot exceed contract hard cap of %d", MaxHistoryPointsUpperLimit)
	}
	if cfg.MaxPoints > MaxHistoryPointsUpperLimit {
		return fmt.Errorf("HISTORY_MAX_POINTS cannot exceed contract hard cap of %d", MaxHistoryPointsUpperLimit)
	}
	if cfg.MinPoints > cfg.MaxPoints {
		return fmt.Errorf("HISTORY_MIN_POINTS must be less than or equal to HISTORY_MAX_POINTS")
	}
	return nil
}

// HistoryCallback is invoked with the scoped context, resolved sensor identity, and validated query.
// The caller executes the actual data query inside this callback.
type HistoryCallback func(ctx context.Context, sensor ResolvedSensor, q TelemetryHistoryQuery) error

// HistoryPreparation orchestrates actor/query validation, membership resolution,
// and deadline management for telemetry history read queries.
//
// SQL Membership Recheck Documentation:
// The preparation layer resolves sensor authorization under actor membership via SensorResolver
// (which executes a SQL query joining public.user_gateways ON ug.gateway_id = s.gateway_id AND ug.user_id = $actor).
// When subsequent data read queries execute in Tasks 4.2–4.4 inside the scoped callback, the data
// query repository MUST ALSO enforce actor membership scoping in SQL queries (re-verifying user_gateways
// membership predicate) so that authorization is defense-in-depth and does not rely solely on precheck.
type HistoryPreparation struct {
	resolver SensorResolver
	cfg      HistoryPreparationConfig
}

// NewHistoryPreparation creates a new HistoryPreparation orchestrator.
// It verifies that resolver is non-nil (rejecting typed nils) and that cfg is valid.
func NewHistoryPreparation(resolver SensorResolver, cfg HistoryPreparationConfig) (*HistoryPreparation, error) {
	if isNilDependency(resolver) {
		return nil, fmt.Errorf("resolver is required")
	}
	if err := ValidateHistoryPreparationConfig(cfg); err != nil {
		return nil, err
	}
	return &HistoryPreparation{
		resolver: resolver,
		cfg:      cfg,
	}, nil
}

// PrepareTelemetryHistory orchestrates validation, single-budget context scoping,
// sensor resolution, and callback execution for GET /v1/telemetry/history.
// It validates that actor is non-nil and query constraints are satisfied before calling the resolver.
// It enforces a single deadline covering both resolution and callback data query execution.
func (p *HistoryPreparation) PrepareTelemetryHistory(
	parentCtx context.Context,
	actor uuid.UUID,
	q TelemetryHistoryQuery,
	fn HistoryCallback,
) error {
	if isNilDependency(p.resolver) {
		return ErrHistoryDependency
	}
	if actor == uuid.Nil {
		return ErrHistoryNilActor
	}
	if fn == nil {
		return ErrHistoryInvalidInput
	}
	if err := gateway.ValidateGatewayID(q.GatewayID); err != nil {
		return ErrHistoryInvalidInput
	}
	if err := gateway.ValidateSensorID(q.SensorID); err != nil {
		return ErrHistoryInvalidInput
	}
	if q.MaxPoints < p.cfg.MinPoints || q.MaxPoints > p.cfg.MaxPoints {
		return ErrHistoryInvalidInput
	}
	if err := ValidateHistoryTimeRange(q.Range, q.Resolution, p.cfg.MaxRange, p.cfg.RawWindow, q.MaxPoints); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(parentCtx, p.cfg.QueryTimeout)
	defer cancel()

	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrHistoryDependency, ctx.Err())
	}

	sensor, err := p.resolver.ResolveSensor(ctx, actor, q.GatewayID, q.SensorID)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %w", ErrHistoryDependency, ctx.Err())
		}
		return err
	}
	if sensor == nil {
		return ErrHistoryNotFound
	}

	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrHistoryDependency, ctx.Err())
	}

	err = fn(ctx, *sensor, q)
	if ctx.Err() != nil {
		if err != nil && !errors.Is(err, ctx.Err()) {
			return fmt.Errorf("%w: %w: %w", ErrHistoryDependency, ctx.Err(), err)
		}
		return fmt.Errorf("%w: %w", ErrHistoryDependency, ctx.Err())
	}
	return err
}

func isNilDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

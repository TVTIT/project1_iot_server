package digitaltwin

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetry"
)

// Database is the database interface required by resource resolvers.
type Database interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// PostgresResolver implements resource resolution scoped by user_gateways membership.
type PostgresResolver struct {
	database Database
}

var (
	_ EntityResolver           = (*PostgresResolver)(nil)
	_ GatewayResolver          = (*PostgresResolver)(nil)
	_ telemetry.SensorResolver = (*PostgresResolver)(nil)
	_ ResourceResolver         = (*PostgresResolver)(nil)
)

// NewPostgresResolver creates a PostgreSQL resource resolver adapter.
// It verifies that database is non-nil, including typed nil values.
func NewPostgresResolver(database Database) (*PostgresResolver, error) {
	if isNilDependency(database) {
		return nil, fmt.Errorf("database is required")
	}
	return &PostgresResolver{database: database}, nil
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

const resolveGatewayEntityQuery = `SELECT e.id, e.entity_id, e.entity_type, e.gateway_id
FROM public.twin_entities AS e
JOIN public.gateways AS g ON g.gateway_id = e.gateway_id
JOIN public.user_gateways AS ug ON ug.gateway_id = g.gateway_id AND ug.user_id = $1
WHERE e.entity_id = $2
  AND e.entity_type = 'Gateway'
  AND e.gateway_id = $3`

const resolveSensorEntityQuery = `SELECT e.id, e.entity_id, e.entity_type, e.gateway_id, s.sensor_id
FROM public.twin_entities AS e
JOIN public.sensors AS s
  ON s.gateway_id = e.gateway_id AND s.sensor_id = $1
JOIN public.twin_entities AS parent_e
  ON parent_e.entity_id = $2
  AND parent_e.entity_type = 'Gateway'
  AND parent_e.gateway_id = e.gateway_id
JOIN public.twin_relationships AS rel
  ON rel.source_entity_id = parent_e.id
  AND rel.relationship_type = 'hasSensor'
  AND rel.target_entity_id = e.id
JOIN public.gateways AS g
  ON g.gateway_id = e.gateway_id
JOIN public.user_gateways AS ug
  ON ug.gateway_id = g.gateway_id AND ug.user_id = $3
WHERE e.entity_id = $4
  AND e.entity_type = 'Sensor'
  AND e.gateway_id = $5`

const resolveGatewayQuery = `SELECT e.id, g.gateway_id
FROM public.gateways AS g
JOIN public.twin_entities AS e
  ON e.gateway_id = g.gateway_id
  AND e.entity_type = 'Gateway'
  AND e.entity_id = $2
JOIN public.user_gateways AS ug
  ON ug.gateway_id = g.gateway_id AND ug.user_id = $1
WHERE g.gateway_id = $3`

const resolveSensorQuery = `SELECT s.gateway_id, s.sensor_id, s.unit
FROM public.sensors AS s
JOIN public.user_gateways AS ug
  ON ug.gateway_id = s.gateway_id AND ug.user_id = $1
WHERE s.gateway_id = $2 AND s.sensor_id = $3`

// ResolveGateway resolves a Gateway by gateway_id ensuring the actor holds membership.
func (r *PostgresResolver) ResolveGateway(ctx context.Context, actor uuid.UUID, gatewayID string) (*ResolvedGateway, error) {
	if actor == uuid.Nil {
		return nil, ErrNilActor
	}
	if err := gateway.ValidateGatewayID(gatewayID); err != nil {
		return nil, ErrInvalidInput
	}

	gatewayURN := GatewayURNPrefix + gatewayID
	var id uuid.UUID
	var resolvedGW string
	err := r.database.QueryRow(ctx, resolveGatewayQuery, actor, gatewayURN, gatewayID).Scan(&id, &resolvedGW)
	if err != nil {
		return nil, classifyResolverError(err, ErrNotFound, ErrDependencyUnavailable, ErrUnexpected)
	}

	return &ResolvedGateway{
		InternalID: id,
		GatewayID:  resolvedGW,
	}, nil
}

// ResolveEntity resolves an entity by parsed URN identity ensuring the actor holds membership.
// For Sensor entities, it validates the full provisioning graph (resource, parent Gateway, hasSensor relationship).
func (r *PostgresResolver) ResolveEntity(ctx context.Context, actor uuid.UUID, parsed ParsedIdentity) (*ResolvedEntity, error) {
	if actor == uuid.Nil {
		return nil, ErrNilActor
	}
	if !parsed.Type.Valid() {
		return nil, ErrInvalidInput
	}
	if err := gateway.ValidateGatewayID(parsed.GatewayID); err != nil {
		return nil, ErrInvalidInput
	}

	switch parsed.Type {
	case EntityTypeGateway:
		if parsed.SensorID != "" {
			return nil, ErrInvalidInput
		}
		if parsed.URN != GatewayURNPrefix+parsed.GatewayID {
			return nil, ErrInvalidInput
		}

		var id uuid.UUID
		var entityID, entityType, gatewayID string
		err := r.database.QueryRow(ctx, resolveGatewayEntityQuery, actor, parsed.URN, parsed.GatewayID).
			Scan(&id, &entityID, &entityType, &gatewayID)
		if err != nil {
			return nil, classifyResolverError(err, ErrNotFound, ErrDependencyUnavailable, ErrUnexpected)
		}

		return &ResolvedEntity{
			InternalID: id,
			PublicURN:  entityID,
			Type:       EntityType(entityType),
			GatewayID:  gatewayID,
			SensorID:   "",
		}, nil

	case EntityTypeSensor:
		if err := gateway.ValidateSensorID(parsed.SensorID); err != nil {
			return nil, ErrInvalidInput
		}
		if parsed.URN != SensorURNPrefix+parsed.GatewayID+":"+parsed.SensorID {
			return nil, ErrInvalidInput
		}

		parentURN := GatewayURNPrefix + parsed.GatewayID
		var id uuid.UUID
		var entityID, entityType, gatewayID, sensorID string
		err := r.database.QueryRow(ctx, resolveSensorEntityQuery, parsed.SensorID, parentURN, actor, parsed.URN, parsed.GatewayID).
			Scan(&id, &entityID, &entityType, &gatewayID, &sensorID)
		if err != nil {
			return nil, classifyResolverError(err, ErrNotFound, ErrDependencyUnavailable, ErrUnexpected)
		}

		return &ResolvedEntity{
			InternalID: id,
			PublicURN:  entityID,
			Type:       EntityType(entityType),
			GatewayID:  gatewayID,
			SensorID:   sensorID,
		}, nil

	default:
		return nil, ErrInvalidInput
	}
}

// ResolveSensor resolves a Sensor for telemetry reading, ensuring the actor holds membership.
func (r *PostgresResolver) ResolveSensor(ctx context.Context, actor uuid.UUID, gatewayID, sensorID string) (*telemetry.ResolvedSensor, error) {
	if actor == uuid.Nil {
		return nil, telemetry.ErrHistoryNilActor
	}
	if err := gateway.ValidateGatewayID(gatewayID); err != nil {
		return nil, telemetry.ErrHistoryInvalidInput
	}
	if err := gateway.ValidateSensorID(sensorID); err != nil {
		return nil, telemetry.ErrHistoryInvalidInput
	}

	var resolvedGW, resolvedSensor string
	var unit *string
	err := r.database.QueryRow(ctx, resolveSensorQuery, actor, gatewayID, sensorID).
		Scan(&resolvedGW, &resolvedSensor, &unit)
	if err != nil {
		return nil, classifyResolverError(err, telemetry.ErrHistoryNotFound, telemetry.ErrHistoryDependency, telemetry.ErrHistoryUnexpected)
	}

	return &telemetry.ResolvedSensor{
		GatewayID: resolvedGW,
		SensorID:  resolvedSensor,
		Unit:      unit,
	}, nil
}

func classifyResolverError(err error, notFound, dependency, unexpected error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return notFound
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(dependency, err)
	}
	classified := gateway.ClassifyPostgresAvailabilityError(err, dependency)
	if errors.Is(classified, dependency) {
		return classified
	}
	return errors.Join(unexpected, err)
}

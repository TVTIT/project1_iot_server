package telemetry

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"iot-platform/internal/digitaltwin/mapper"
)

// resolvedTelemetryResource is the provisioned Sensor graph that later
// transaction steps may safely use. Unit is nullable because the schema allows
// Sensors without a declared unit.
type resolvedTelemetryResource struct {
	GatewayID string
	SensorID  string
	EntityID  uuid.UUID
	Unit      *string
}

type telemetryResourceKey struct{ gatewayID, sensorID string }

// prepareTelemetryResources acquires parent and child locks in separate stable
// phases. Call only after the complete batch is ready to persist.
func prepareTelemetryResources(ctx context.Context, tx pgx.Tx, messages []*Message) (map[telemetryResourceKey]resolvedTelemetryResource, error) {
	return prepareTelemetryResourcesWithMetrics(ctx, tx, messages, nil)
}

func prepareTelemetryResourcesWithMetrics(ctx context.Context, tx pgx.Tx, messages []*Message, metrics *PersistenceMetrics) (map[telemetryResourceKey]resolvedTelemetryResource, error) {
	gateways := make(map[string]struct{})
	sensorSet := make(map[telemetryResourceKey]struct{})
	for _, message := range messages {
		if message == nil || message.Topic().GatewayID == "" || message.Topic().SensorID == "" {
			return nil, &ReasonError{Code: ReasonInvalidPayload, Field: "message"}
		}
		topic := message.Topic()
		gateways[topic.GatewayID] = struct{}{}
		sensorSet[telemetryResourceKey{topic.GatewayID, topic.SensorID}] = struct{}{}
	}
	gatewayIDs := make([]string, 0, len(gateways))
	for id := range gateways {
		gatewayIDs = append(gatewayIDs, id)
	}
	sort.Strings(gatewayIDs)
	for _, id := range gatewayIDs {
		started := time.Now()
		err := lockTelemetryGatewayShare(ctx, tx, id)
		if metrics != nil {
			observePersistenceDuration(&metrics.durations.gatewayLock, started)
		}
		if err != nil {
			return nil, err
		}
	}
	keys := make([]telemetryResourceKey, 0, len(sensorSet))
	for key := range sensorSet {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].gatewayID != keys[j].gatewayID {
			return keys[i].gatewayID < keys[j].gatewayID
		}
		return keys[i].sensorID < keys[j].sensorID
	})
	units := make(map[telemetryResourceKey]*string, len(keys))
	for _, key := range keys {
		started := time.Now()
		unit, err := lockTelemetrySensor(ctx, tx, key.gatewayID, key.sensorID)
		if metrics != nil {
			observePersistenceDuration(&metrics.durations.sensorLock, started)
		}
		if err != nil {
			return nil, err
		}
		units[key] = unit
	}
	resources := make(map[telemetryResourceKey]resolvedTelemetryResource, len(keys))
	for _, key := range keys {
		started := time.Now()
		resource, err := verifyTelemetryGraph(ctx, tx, key.gatewayID, key.sensorID, units[key])
		if metrics != nil {
			observePersistenceDuration(&metrics.durations.graphLookup, started)
		}
		if err != nil {
			return nil, err
		}
		resources[key] = resource
	}
	for _, key := range keys {
		started := time.Now()
		err := lockTelemetrySensorEntity(ctx, tx, resources[key])
		if metrics != nil {
			observePersistenceDuration(&metrics.durations.entityLock, started)
		}
		if err != nil {
			return nil, err
		}
	}
	for _, key := range keys {
		started := time.Now()
		err := lockTelemetrySensorState(ctx, tx, resources[key])
		if metrics != nil {
			observePersistenceDuration(&metrics.durations.stateLock, started)
		}
		if err != nil {
			return nil, err
		}
	}
	return resources, nil
}

func resolveTelemetryResourceWithMetrics(ctx context.Context, tx pgx.Tx, message *Message, metrics *PersistenceMetrics) (resolvedTelemetryResource, error) {
	if message == nil {
		return resolvedTelemetryResource{}, &ReasonError{Code: ReasonInvalidPayload, Field: "message"}
	}
	topic := message.Topic()
	if topic.GatewayID == "" || topic.SensorID == "" {
		return resolvedTelemetryResource{}, &ReasonError{Code: ReasonInvalidPayload, Field: "message"}
	}
	started := time.Now()
	err := lockTelemetryGatewayShare(ctx, tx, topic.GatewayID)
	if metrics != nil {
		observePersistenceDuration(&metrics.durations.gatewayLock, started)
	}
	if err != nil {
		return resolvedTelemetryResource{}, err
	}
	started = time.Now()
	unit, err := lockTelemetrySensor(ctx, tx, topic.GatewayID, topic.SensorID)
	if metrics != nil {
		observePersistenceDuration(&metrics.durations.sensorLock, started)
	}
	if err != nil {
		return resolvedTelemetryResource{}, err
	}
	started = time.Now()
	resource, err := verifyTelemetryGraph(ctx, tx, topic.GatewayID, topic.SensorID, unit)
	if metrics != nil {
		observePersistenceDuration(&metrics.durations.graphLookup, started)
	}
	if err != nil {
		return resolvedTelemetryResource{}, err
	}
	started = time.Now()
	err = lockTelemetrySensorEntity(ctx, tx, resource)
	if metrics != nil {
		observePersistenceDuration(&metrics.durations.entityLock, started)
	}
	if err != nil {
		return resolvedTelemetryResource{}, err
	}
	started = time.Now()
	err = lockTelemetrySensorState(ctx, tx, resource)
	if metrics != nil {
		observePersistenceDuration(&metrics.durations.stateLock, started)
	}
	if err != nil {
		return resolvedTelemetryResource{}, err
	}
	return resource, nil
}

func lockTelemetryGatewayShare(ctx context.Context, tx pgx.Tx, gatewayID string) error {
	var locked string
	err := tx.QueryRow(ctx, `SELECT gateway_id FROM public.gateways WHERE gateway_id = $1 FOR SHARE`, gatewayID).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return &ReasonError{Code: ReasonUnknownGateway, Field: "gateway_id"}
	}
	if err != nil {
		return dependencyError("lock telemetry Gateway", err)
	}
	return nil
}

func lockTelemetrySensor(ctx context.Context, tx pgx.Tx, gatewayID, sensorID string) (*string, error) {
	var unit *string
	err := tx.QueryRow(ctx, `SELECT unit FROM public.sensors WHERE gateway_id = $1 AND sensor_id = $2 FOR UPDATE`, gatewayID, sensorID).Scan(&unit)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &ReasonError{Code: ReasonUnknownSensor, Field: "sensor_id"}
	}
	if err != nil {
		return nil, dependencyError("lock telemetry Sensor", err)
	}
	return unit, nil
}

func verifyTelemetryGraph(ctx context.Context, tx pgx.Tx, gatewayID, sensorID string, unit *string) (resolvedTelemetryResource, error) {
	var gatewayEntityID, sensorEntityID uuid.UUID
	var sensorStatePresent bool
	err := tx.QueryRow(ctx, `SELECT gateway_entity.id, sensor_entity.id,
		(sensor_state.entity_id IS NOT NULL)
		FROM public.twin_entities AS gateway_entity
	JOIN public.twin_entities AS sensor_entity
		ON sensor_entity.entity_id = $2 AND sensor_entity.entity_type = 'Sensor' AND sensor_entity.gateway_id = $3
	LEFT JOIN public.twin_states AS gateway_state ON gateway_state.entity_id = gateway_entity.id
	LEFT JOIN public.twin_states AS sensor_state ON sensor_state.entity_id = sensor_entity.id
	WHERE gateway_entity.entity_id = $1 AND gateway_entity.entity_type = 'Gateway'
		AND gateway_entity.gateway_id = $3 AND gateway_state.entity_id IS NOT NULL
		AND EXISTS (SELECT 1 FROM public.twin_relationships AS relationship
			WHERE relationship.source_entity_id = gateway_entity.id AND relationship.target_entity_id = sensor_entity.id
			AND relationship.relationship_type = 'hasSensor')`, mapper.GatewayEntityID(gatewayID), mapper.SensorEntityID(gatewayID, sensorID), gatewayID).
		Scan(&gatewayEntityID, &sensorEntityID, &sensorStatePresent)
	if errors.Is(err, pgx.ErrNoRows) {
		return resolvedTelemetryResource{}, &ReasonError{Code: ReasonResourceGraphIncomplete, Field: "twin_graph"}
	}
	if err != nil {
		return resolvedTelemetryResource{}, dependencyError("verify telemetry Twin graph", err)
	}
	if gatewayEntityID == uuid.Nil || sensorEntityID == uuid.Nil || !sensorStatePresent {
		return resolvedTelemetryResource{}, &ReasonError{Code: ReasonResourceGraphIncomplete, Field: "twin_graph"}
	}
	return resolvedTelemetryResource{GatewayID: gatewayID, SensorID: sensorID, EntityID: sensorEntityID, Unit: unit}, nil
}

func lockTelemetrySensorEntity(ctx context.Context, tx pgx.Tx, resource resolvedTelemetryResource) error {
	var id uuid.UUID
	err := tx.QueryRow(ctx, `SELECT id FROM public.twin_entities WHERE entity_id = $1 AND entity_type = 'Sensor' AND gateway_id = $2 FOR UPDATE`, mapper.SensorEntityID(resource.GatewayID, resource.SensorID), resource.GatewayID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return &ReasonError{Code: ReasonResourceGraphIncomplete, Field: "twin_graph"}
	}
	if err != nil {
		return dependencyError("lock telemetry Sensor entity", err)
	}
	return nil
}

func lockTelemetrySensorState(ctx context.Context, tx pgx.Tx, resource resolvedTelemetryResource) error {
	var id uuid.UUID
	err := tx.QueryRow(ctx, `SELECT entity_id FROM public.twin_states WHERE entity_id = $1 FOR UPDATE`, resource.EntityID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return &ReasonError{Code: ReasonResourceGraphIncomplete, Field: "twin_state"}
	}
	if err != nil {
		return dependencyError("lock telemetry Sensor state", err)
	}
	return nil
}

func dependencyError(operation string, err error) error {
	return fmt.Errorf("%s: %w: %w", operation, &ReasonError{Code: ReasonDependencyUnavailable}, err)
}

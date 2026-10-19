package telemetry

import (
	"context"
	"errors"
	"fmt"

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

// resolveTelemetryResource confirms that a validated message targets an
// existing Gateway and locks only its Sensor row for the rest of the caller's
// transaction. It verifies the provisioned Twin graph without creating or
// changing any entity, state, relationship, or temporal value.
func resolveTelemetryResource(ctx context.Context, tx pgx.Tx, message *Message) (resolvedTelemetryResource, error) {
	if message == nil {
		return resolvedTelemetryResource{}, &ReasonError{Code: ReasonInvalidPayload, Field: "message"}
	}
	topic := message.Topic()
	if topic.GatewayID == "" || topic.SensorID == "" {
		return resolvedTelemetryResource{}, &ReasonError{Code: ReasonInvalidPayload, Field: "message"}
	}

	var gatewayExists bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM public.gateways WHERE gateway_id = $1
	)`, topic.GatewayID).Scan(&gatewayExists)
	if err != nil {
		return resolvedTelemetryResource{}, dependencyError("check telemetry Gateway", err)
	}
	if !gatewayExists {
		return resolvedTelemetryResource{}, &ReasonError{Code: ReasonUnknownGateway, Field: "gateway_id"}
	}

	var unit *string
	err = tx.QueryRow(ctx, `SELECT unit
		FROM public.sensors
		WHERE gateway_id = $1 AND sensor_id = $2
		FOR UPDATE`, topic.GatewayID, topic.SensorID).Scan(&unit)
	if errors.Is(err, pgx.ErrNoRows) {
		return resolvedTelemetryResource{}, &ReasonError{Code: ReasonUnknownSensor, Field: "sensor_id"}
	}
	if err != nil {
		return resolvedTelemetryResource{}, dependencyError("lock telemetry Sensor", err)
	}

	var gatewayEntityID uuid.UUID
	var sensorEntityID uuid.UUID
	var sensorStatePresent bool
	err = tx.QueryRow(ctx, `SELECT gateway_entity.id, sensor_entity.id,
		(sensor_state.entity_id IS NOT NULL)
		FROM public.twin_entities AS gateway_entity
	JOIN public.twin_entities AS sensor_entity
		ON sensor_entity.entity_id = $2
		AND sensor_entity.entity_type = 'Sensor'
		AND sensor_entity.gateway_id = $3
	LEFT JOIN public.twin_states AS gateway_state
		ON gateway_state.entity_id = gateway_entity.id
	LEFT JOIN public.twin_states AS sensor_state
		ON sensor_state.entity_id = sensor_entity.id
	WHERE gateway_entity.entity_id = $1
		AND gateway_entity.entity_type = 'Gateway'
		AND gateway_entity.gateway_id = $3
		AND gateway_state.entity_id IS NOT NULL
		AND EXISTS (
			SELECT 1 FROM public.twin_relationships AS relationship
			WHERE relationship.source_entity_id = gateway_entity.id
				AND relationship.target_entity_id = sensor_entity.id
				AND relationship.relationship_type = 'hasSensor'
		)`, mapper.GatewayEntityID(topic.GatewayID), mapper.SensorEntityID(topic.GatewayID, topic.SensorID), topic.GatewayID).
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

	return resolvedTelemetryResource{
		GatewayID: topic.GatewayID,
		SensorID:  topic.SensorID,
		EntityID:  sensorEntityID,
		Unit:      unit,
	}, nil
}

func dependencyError(operation string, err error) error {
	return fmt.Errorf("%s: %w: %w", operation, &ReasonError{Code: ReasonDependencyUnavailable}, err)
}

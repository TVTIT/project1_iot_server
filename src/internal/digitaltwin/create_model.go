package digitaltwin

import (
	"errors"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// CreateEntityInput represents the validated domain input for entity creation, tagged by entity type.
// Exactly one of Gateway or Sensor is non-nil, strictly matching Type.
type CreateEntityInput struct {
	Type    EntityType
	Gateway *gateway.ProvisionGatewayInput
	Sensor  *gateway.ProvisionSensorInput
}

// CreateEntityResult carries the creation or replay outcome for either Gateway or Sensor.
// Exactly one of Gateway or Sensor is non-nil, matching Type.
type CreateEntityResult struct {
	Created  bool
	Type     EntityType
	EntityID string
	Gateway  *gateway.ProvisionGatewayResult
	Sensor   *gateway.ProvisionSensorResult
}

// ValidateCreateEntityInput validates the combined entity creation input against Type.
// Actor is passed separately and must not be uuid.Nil. Target owner_user_id does not grant actor authority.
func ValidateCreateEntityInput(actor uuid.UUID, input CreateEntityInput) error {
	if actor == uuid.Nil {
		return ErrNilActor
	}
	switch input.Type {
	case EntityTypeGateway:
		if input.Gateway == nil || input.Sensor != nil {
			return ErrInvalidInput
		}
		if err := gateway.ValidateProvisionGatewayInput(actor, *input.Gateway); err != nil {
			return errors.Join(ErrInvalidInput, err)
		}
		return nil

	case EntityTypeSensor:
		if input.Sensor == nil || input.Gateway != nil {
			return ErrInvalidInput
		}
		if err := gateway.ValidateProvisionSensorInput(actor, *input.Sensor); err != nil {
			return errors.Join(ErrInvalidInput, err)
		}
		return nil

	default:
		return ErrInvalidInput
	}
}

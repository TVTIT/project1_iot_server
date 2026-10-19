package digitaltwin

import (
	"time"

	"iot-platform/internal/gateway"
)

// PatchGatewayInput holds the presence-aware fields for updating Gateway metadata.
// Name is nil when omitted from the request. When non-nil, it must be valid and non-blank after trim.
// DescriptionSet indicates whether "description" was explicitly present in the request.
// If DescriptionSet is true and Description is nil, it represents an explicit clear (SQL NULL).
// If DescriptionSet is true and Description is non-nil, it represents an update to *Description.
// If DescriptionSet is false, Description must be nil (omitted/unchanged).
type PatchGatewayInput struct {
	Name           *string
	DescriptionSet bool
	Description    *string
}

// PatchSensorInput holds the presence-aware fields for updating Sensor metadata.
// For Sensor, only Name is permitted and it is required.
type PatchSensorInput struct {
	Name *string
}

// PatchEntityInput represents a combined presence-aware PATCH input tagged by entity type.
// Target must be a valid ParsedIdentity.
// Exactly one of Gateway or Sensor is non-nil, strictly matching Target.Type.
type PatchEntityInput struct {
	Target  ParsedIdentity
	Gateway *PatchGatewayInput
	Sensor  *PatchSensorInput
}

// ValidatePatchGatewayInput validates the presence-aware input for Gateway PATCH.
// It requires at least one field to be supplied (rejecting empty patches).
// When Name is supplied, it enforces non-blank, length <= 256 bytes, valid UTF-8, and no NUL.
// When Description is supplied, it enforces length <= 4096 bytes, valid UTF-8, and no NUL.
// Same-value requests are valid domain inputs; no-op evaluation occurs against database state later.
func ValidatePatchGatewayInput(input PatchGatewayInput) error {
	if input.Name == nil && !input.DescriptionSet {
		return ErrInvalidInput
	}
	if !input.DescriptionSet && input.Description != nil {
		return ErrInvalidInput
	}
	if input.Name != nil {
		if !gateway.ValidProvisioningName(*input.Name) {
			return ErrInvalidInput
		}
	}
	if input.DescriptionSet && input.Description != nil {
		if !gateway.ValidMetadata(*input.Description, 4096) {
			return ErrInvalidInput
		}
	}
	return nil
}

// ValidatePatchSensorInput validates the presence-aware input for Sensor PATCH.
// For Sensor, Name is required, non-blank, length <= 256 bytes, valid UTF-8, and no NUL.
func ValidatePatchSensorInput(input PatchSensorInput) error {
	if input.Name == nil {
		return ErrInvalidInput
	}
	if !gateway.ValidProvisioningName(*input.Name) {
		return ErrInvalidInput
	}
	return nil
}

// ValidatePatchEntityInput validates the combined entity patch input against Target.Type.
func ValidatePatchEntityInput(input PatchEntityInput) error {
	switch input.Target.Type {
	case EntityTypeGateway:
		if input.Gateway == nil || input.Sensor != nil {
			return ErrInvalidInput
		}
		if err := gateway.ValidateGatewayID(input.Target.GatewayID); err != nil {
			return ErrInvalidInput
		}
		if input.Target.SensorID != "" {
			return ErrInvalidInput
		}
		return ValidatePatchGatewayInput(*input.Gateway)

	case EntityTypeSensor:
		if input.Sensor == nil || input.Gateway != nil {
			return ErrInvalidInput
		}
		if err := gateway.ValidateGatewayID(input.Target.GatewayID); err != nil {
			return ErrInvalidInput
		}
		if err := gateway.ValidateSensorID(input.Target.SensorID); err != nil {
			return ErrInvalidInput
		}
		return ValidatePatchSensorInput(*input.Sensor)

	default:
		return ErrInvalidInput
	}
}

// PatchGatewayResult is the plain metadata domain view returned after a confirmed Gateway PATCH.
// It exposes no internal UUIDs, state, relationships, or context.
type PatchGatewayResult struct {
	ID          string     `json:"id"`
	Type        EntityType `json:"type"`
	GatewayID   string     `json:"gateway_id"`
	Name        string     `json:"name"`
	Description *string    `json:"description"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// PatchSensorResult is the plain metadata domain view returned after a confirmed Sensor PATCH.
// It exposes no internal UUIDs, state, relationships, or context.
type PatchSensorResult struct {
	ID        string     `json:"id"`
	Type      EntityType `json:"type"`
	GatewayID string     `json:"gateway_id"`
	SensorID  string     `json:"sensor_id"`
	Name      string     `json:"name"`
	Unit      *string    `json:"unit"`
	UpdatedAt time.Time  `json:"updated_at"`
}

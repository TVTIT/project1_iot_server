package gateway

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Provisioning error categories support errors.Is, following the read API's
// sentinel convention. They contain no database details or caller metadata.
// Context cancellation/deadline errors remain context errors, not these sentinels.
var (
	ErrInvalidProvisioningInput = errors.New("invalid provisioning input")
	ErrProvisioningNotFound     = errors.New("provisioning resource not found")
	ErrProvisioningConflict     = errors.New("provisioning representation conflict")
	ErrProvisioningForbidden    = errors.New("provisioning forbidden")
	ErrProvisioningInconsistent = errors.New("inconsistent provisioning graph")
)

// ProvisionGatewayInput contains path identity and admin-selected metadata.
// Actor identity is passed separately, never taken from client body metadata.
type ProvisionGatewayInput struct {
	GatewayID   string
	Name        string
	Description *string
	OwnerUserID uuid.UUID
}

// ProvisionSensorInput does not accept client-selected Twin identity or state.
type ProvisionSensorInput struct {
	GatewayID string
	SensorID  string
	Name      string
	Unit      *string
}

// ProvisionGatewayResult is returned only after commit. Created distinguishes
// creation from a matching no-op retry. CreatedAt preserves legacy database NULL.
type ProvisionGatewayResult struct {
	Created     bool
	GatewayID   string
	Name        string
	Description *string
	OwnerUserID uuid.UUID
	EntityID    string
	CreatedAt   *time.Time
}

// ProvisionSensorResult exposes no internal Twin UUID, state or credentials.
type ProvisionSensorResult struct {
	Created   bool
	GatewayID string
	SensorID  string
	Name      string
	Unit      *string
	EntityID  string
	CreatedAt *time.Time
}

// ValidateProvisionGatewayInput checks shape only; the repository checks owner
// existence and actor authorization in its transaction. Values are not mutated.
func ValidateProvisionGatewayInput(actorUserID uuid.UUID, input ProvisionGatewayInput) error {
	if actorUserID == uuid.Nil || input.OwnerUserID == uuid.Nil {
		return ErrInvalidProvisioningInput
	}
	if err := ValidateGatewayID(input.GatewayID); err != nil {
		return err
	}
	if !validProvisioningName(input.Name) || !validNullableMetadata(input.Description, 4096) {
		return ErrInvalidProvisioningInput
	}
	return nil
}

// ValidateProvisionSensorInput preserves NULL versus empty unit and raw names.
func ValidateProvisionSensorInput(actorUserID uuid.UUID, input ProvisionSensorInput) error {
	if actorUserID == uuid.Nil {
		return ErrInvalidProvisioningInput
	}
	if err := ValidateGatewayID(input.GatewayID); err != nil {
		return err
	}
	if err := ValidateSensorID(input.SensorID); err != nil {
		return err
	}
	if !validProvisioningName(input.Name) || !validNullableMetadata(input.Unit, 64) {
		return ErrInvalidProvisioningInput
	}
	return nil
}

func validProvisioningName(value string) bool {
	return validMetadata(value, 256) && strings.TrimSpace(value) != ""
}

func validNullableMetadata(value *string, maxBytes int) bool {
	return value == nil || validMetadata(*value, maxBytes)
}

func validMetadata(value string, maxBytes int) bool {
	return len(value) <= maxBytes && utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}

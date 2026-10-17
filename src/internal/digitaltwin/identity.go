// Package digitaltwin provides core types and read contracts for Digital Twin entities.
package digitaltwin

import (
	"fmt"
	"strings"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// URN prefix constants for public Digital Twin entities.
const (
	GatewayURNPrefix = "urn:ngsi-ld:Gateway:"
	SensorURNPrefix  = "urn:ngsi-ld:Sensor:"
)

// EntityType identifies supported Digital Twin entity types.
type EntityType string

// Supported Digital Twin entity types.
const (
	EntityTypeGateway EntityType = "Gateway"
	EntityTypeSensor  EntityType = "Sensor"
)

// Valid reports whether the entity type is supported.
func (t EntityType) Valid() bool {
	switch t {
	case EntityTypeGateway, EntityTypeSensor:
		return true
	default:
		return false
	}
}

// ParsedIdentity carries the parsed components of a public NGSI-LD URN.
// UUIDs are internal only and never stored in ParsedIdentity.
type ParsedIdentity struct {
	Type      EntityType
	GatewayID string
	SensorID  string
	URN       string
}

// String returns a diagnostic label without leaking sensitive contents.
func (p ParsedIdentity) String() string {
	if p.Type == EntityTypeSensor {
		return fmt.Sprintf("%s(%s:%s)", p.Type, p.GatewayID, p.SensorID)
	}
	return fmt.Sprintf("%s(%s)", p.Type, p.GatewayID)
}

// ResolvedEntity is an authorized internal representation of an entity,
// including its internal backend UUID (from twin_entities.id) and parent Gateway scope.
type ResolvedEntity struct {
	InternalID uuid.UUID
	PublicURN  string
	Type       EntityType
	GatewayID  string
	SensorID   string
}

// ParseEntityURN parses and strictly validates a public NGSI-LD URN.
// It enforces decoded byte cap of 160 bytes, exact case, component counts,
// identifier character constraints, and reserved gateway checks.
// It assumes the string has been decoded once by the HTTP layer and performs no second unescape.
func ParseEntityURN(raw string) (ParsedIdentity, error) {
	if raw == "" || len(raw) > MaxDecodedURNLength {
		return ParsedIdentity{}, ErrInvalidInput
	}

	if strings.HasPrefix(raw, GatewayURNPrefix) {
		remainder := strings.TrimPrefix(raw, GatewayURNPrefix)
		if remainder == "" || strings.ContainsRune(remainder, ':') {
			return ParsedIdentity{}, ErrInvalidInput
		}
		if err := gateway.ValidateGatewayID(remainder); err != nil {
			return ParsedIdentity{}, ErrInvalidInput
		}
		return ParsedIdentity{
			Type:      EntityTypeGateway,
			GatewayID: remainder,
			SensorID:  "",
			URN:       raw,
		}, nil
	}

	if strings.HasPrefix(raw, SensorURNPrefix) {
		remainder := strings.TrimPrefix(raw, SensorURNPrefix)
		parts := strings.Split(remainder, ":")
		if len(parts) != 2 {
			return ParsedIdentity{}, ErrInvalidInput
		}
		gatewayID := parts[0]
		sensorID := parts[1]
		if gatewayID == "" || sensorID == "" {
			return ParsedIdentity{}, ErrInvalidInput
		}
		if err := gateway.ValidateGatewayID(gatewayID); err != nil {
			return ParsedIdentity{}, ErrInvalidInput
		}
		if err := gateway.ValidateSensorID(sensorID); err != nil {
			return ParsedIdentity{}, ErrInvalidInput
		}
		return ParsedIdentity{
			Type:      EntityTypeSensor,
			GatewayID: gatewayID,
			SensorID:  sensorID,
			URN:       raw,
		}, nil
	}

	return ParsedIdentity{}, ErrInvalidInput
}

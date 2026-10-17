package httpserver

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
)

// PostEntityKind represents the exact entity type supported by POST /v1/digital-twins.
type PostEntityKind string

// Supported entity types for POST /v1/digital-twins.
const (
	PostEntityKindGateway PostEntityKind = "Gateway"
	PostEntityKindSensor  PostEntityKind = "Sensor"
)

// PostEntityRequest contains the validated provisioning input for either Gateway or Sensor.
// Exactly one of Gateway or Sensor is non-nil, strictly determined by EntityType.
type PostEntityRequest struct {
	EntityType PostEntityKind
	Gateway    *gateway.ProvisionGatewayInput
	Sensor     *gateway.ProvisionSensorInput
}

// ToDomain maps a parsed and tagged PostEntityRequest into the domain CreateEntityInput.
func (r PostEntityRequest) ToDomain() digitaltwin.CreateEntityInput {
	switch r.EntityType {
	case PostEntityKindGateway:
		return digitaltwin.CreateEntityInput{
			Type:    digitaltwin.EntityTypeGateway,
			Gateway: r.Gateway,
		}
	case PostEntityKindSensor:
		return digitaltwin.CreateEntityInput{
			Type:   digitaltwin.EntityTypeSensor,
			Sensor: r.Sensor,
		}
	default:
		return digitaltwin.CreateEntityInput{
			Type: digitaltwin.EntityType(r.EntityType),
		}
	}
}

// parsePostEntityRequest parses and validates POST /v1/digital-twins requests.
// It returns the tagged PostEntityRequest and HTTP status (0 on success, or 400/413/415).
func parsePostEntityRequest(r *http.Request, limit int64) (PostEntityRequest, int) {
	var req PostEntityRequest
	entries, status := readStrictJSONObject(r, limit)
	if status != 0 {
		return req, status
	}

	rawType, ok := entries["entity_type"]
	if !ok || bytes.Equal(rawType, []byte("null")) {
		return req, http.StatusBadRequest
	}
	var entityType string
	if err := json.Unmarshal(rawType, &entityType); err != nil {
		return req, http.StatusBadRequest
	}

	switch PostEntityKind(entityType) {
	case PostEntityKindGateway:
		gwInput, st := parsePostGatewayFields(entries)
		if st != 0 {
			return req, st
		}
		req.EntityType = PostEntityKindGateway
		req.Gateway = &gwInput
		return req, 0

	case PostEntityKindSensor:
		sensorInput, st := parsePostSensorFields(entries)
		if st != 0 {
			return req, st
		}
		req.EntityType = PostEntityKindSensor
		req.Sensor = &sensorInput
		return req, 0

	default:
		return req, http.StatusBadRequest
	}
}

func parsePostGatewayFields(entries map[string]json.RawMessage) (gateway.ProvisionGatewayInput, int) {
	for k := range entries {
		switch k {
		case "entity_type", "gateway_id", "name", "description", "owner_user_id":
			// allowed
		default:
			return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
		}
	}

	if _, ok := entries["gateway_id"]; !ok {
		return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
	}
	if _, ok := entries["name"]; !ok {
		return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
	}
	if _, ok := entries["owner_user_id"]; !ok {
		return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
	}

	rawGWID := entries["gateway_id"]
	if bytes.Equal(rawGWID, []byte("null")) {
		return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
	}
	var gatewayID string
	if err := json.Unmarshal(rawGWID, &gatewayID); err != nil {
		return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
	}
	if err := gateway.ValidateGatewayID(gatewayID); err != nil {
		return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
	}

	rawName := entries["name"]
	if bytes.Equal(rawName, []byte("null")) {
		return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
	}
	var name string
	if err := json.Unmarshal(rawName, &name); err != nil {
		return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
	}
	if !gateway.ValidProvisioningName(name) {
		return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
	}

	rawOwner := entries["owner_user_id"]
	if bytes.Equal(rawOwner, []byte("null")) {
		return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
	}
	var ownerStr string
	if err := json.Unmarshal(rawOwner, &ownerStr); err != nil {
		return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
	}
	ownerUUID, err := uuid.Parse(ownerStr)
	if err != nil || ownerUUID == uuid.Nil {
		return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
	}

	var description *string
	if rawDesc, ok := entries["description"]; ok {
		if !bytes.Equal(rawDesc, []byte("null")) {
			var desc string
			if err := json.Unmarshal(rawDesc, &desc); err != nil {
				return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
			}
			if !gateway.ValidNullableMetadata(&desc, 4096) {
				return gateway.ProvisionGatewayInput{}, http.StatusBadRequest
			}
			description = &desc
		}
	}

	return gateway.ProvisionGatewayInput{
		GatewayID:   gatewayID,
		Name:        name,
		Description: description,
		OwnerUserID: ownerUUID,
	}, 0
}

func parsePostSensorFields(entries map[string]json.RawMessage) (gateway.ProvisionSensorInput, int) {
	for k := range entries {
		switch k {
		case "entity_type", "gateway_id", "sensor_id", "name", "unit":
			// allowed
		default:
			return gateway.ProvisionSensorInput{}, http.StatusBadRequest
		}
	}

	if _, ok := entries["gateway_id"]; !ok {
		return gateway.ProvisionSensorInput{}, http.StatusBadRequest
	}
	if _, ok := entries["sensor_id"]; !ok {
		return gateway.ProvisionSensorInput{}, http.StatusBadRequest
	}
	if _, ok := entries["name"]; !ok {
		return gateway.ProvisionSensorInput{}, http.StatusBadRequest
	}

	rawGWID := entries["gateway_id"]
	if bytes.Equal(rawGWID, []byte("null")) {
		return gateway.ProvisionSensorInput{}, http.StatusBadRequest
	}
	var gatewayID string
	if err := json.Unmarshal(rawGWID, &gatewayID); err != nil {
		return gateway.ProvisionSensorInput{}, http.StatusBadRequest
	}
	if err := gateway.ValidateGatewayID(gatewayID); err != nil {
		return gateway.ProvisionSensorInput{}, http.StatusBadRequest
	}

	rawSensorID := entries["sensor_id"]
	if bytes.Equal(rawSensorID, []byte("null")) {
		return gateway.ProvisionSensorInput{}, http.StatusBadRequest
	}
	var sensorID string
	if err := json.Unmarshal(rawSensorID, &sensorID); err != nil {
		return gateway.ProvisionSensorInput{}, http.StatusBadRequest
	}
	if err := gateway.ValidateSensorID(sensorID); err != nil {
		return gateway.ProvisionSensorInput{}, http.StatusBadRequest
	}

	rawName := entries["name"]
	if bytes.Equal(rawName, []byte("null")) {
		return gateway.ProvisionSensorInput{}, http.StatusBadRequest
	}
	var name string
	if err := json.Unmarshal(rawName, &name); err != nil {
		return gateway.ProvisionSensorInput{}, http.StatusBadRequest
	}
	if !gateway.ValidProvisioningName(name) {
		return gateway.ProvisionSensorInput{}, http.StatusBadRequest
	}

	var unit *string
	if rawUnit, ok := entries["unit"]; ok {
		if !bytes.Equal(rawUnit, []byte("null")) {
			var unitStr string
			if err := json.Unmarshal(rawUnit, &unitStr); err != nil {
				return gateway.ProvisionSensorInput{}, http.StatusBadRequest
			}
			if !gateway.ValidNullableMetadata(&unitStr, 64) {
				return gateway.ProvisionSensorInput{}, http.StatusBadRequest
			}
			unit = &unitStr
		}
	}

	return gateway.ProvisionSensorInput{
		GatewayID: gatewayID,
		SensorID:  sensorID,
		Name:      name,
		Unit:      unit,
	}, 0
}

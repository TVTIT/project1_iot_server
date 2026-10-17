package httpserver

import (
	"bytes"
	"encoding/json"
	"net/http"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
)

// PatchEntityRequest carries the parsed, validated presence-aware PATCH request
// for either a Gateway or Sensor entity.
// Target contains the strictly parsed NGSI-LD identity derived from the path parameter.
// Exactly one of Gateway or Sensor is non-nil, strictly determined by Target.Type.
type PatchEntityRequest struct {
	Target  digitaltwin.ParsedIdentity
	Gateway *digitaltwin.PatchGatewayInput
	Sensor  *digitaltwin.PatchSensorInput
}

// parsePatchEntityRequest parses and strictly validates a PATCH /v1/digital-twins/{entity_id} request.
//
// Contract expectation for Task 4.6 router/handler wiring:
// decodedEntityID MUST be the public entity identifier string already decoded once by the HTTP layer
// (e.g. via Gin's c.Param("entity_id")). This function strictly enforces URN format via
// digitaltwin.ParseEntityURN and NEVER performs a second URL-decoding step.
// Body identity and type overrides are strictly rejected.
//
// It returns the validated PatchEntityRequest and an HTTP status code:
//   - 0 on successful parsing and validation
//   - 400 (Bad Request) on invalid URN, non-Gateway/Sensor entity type, query params present,
//     JSON framing/syntax/duplicate errors, missing/unknown/injected keys, empty patches,
//     null name, blank name, or oversized/NUL-containing strings
//   - 413 (Payload Too Large) if body exceeds limit
//   - 415 (Unsupported Media Type) if Content-Type is not application/json with UTF-8 charset.
func parsePatchEntityRequest(r *http.Request, decodedEntityID string, limit int64) (PatchEntityRequest, int) {
	parsed, err := digitaltwin.ParseEntityURN(decodedEntityID)
	if err != nil {
		return PatchEntityRequest{}, http.StatusBadRequest
	}
	if parsed.Type != digitaltwin.EntityTypeGateway && parsed.Type != digitaltwin.EntityTypeSensor {
		return PatchEntityRequest{}, http.StatusBadRequest
	}

	entries, status := readStrictJSONObject(r, limit)
	if status != 0 {
		return PatchEntityRequest{}, status
	}

	switch parsed.Type {
	case digitaltwin.EntityTypeGateway:
		gwInput, st := parsePatchGatewayFields(entries)
		if st != 0 {
			return PatchEntityRequest{}, st
		}
		return PatchEntityRequest{
			Target:  parsed,
			Gateway: &gwInput,
		}, 0

	case digitaltwin.EntityTypeSensor:
		sensorInput, st := parsePatchSensorFields(entries)
		if st != 0 {
			return PatchEntityRequest{}, st
		}
		return PatchEntityRequest{
			Target: parsed,
			Sensor: &sensorInput,
		}, 0

	default:
		return PatchEntityRequest{}, http.StatusBadRequest
	}
}

func parsePatchGatewayFields(entries map[string]json.RawMessage) (digitaltwin.PatchGatewayInput, int) {
	if len(entries) == 0 {
		return digitaltwin.PatchGatewayInput{}, http.StatusBadRequest
	}
	for k := range entries {
		switch k {
		case "name", "description":
			// allowed
		default:
			return digitaltwin.PatchGatewayInput{}, http.StatusBadRequest
		}
	}

	var input digitaltwin.PatchGatewayInput

	if rawName, ok := entries["name"]; ok {
		if bytes.Equal(rawName, []byte("null")) {
			return digitaltwin.PatchGatewayInput{}, http.StatusBadRequest
		}
		var name string
		if err := json.Unmarshal(rawName, &name); err != nil {
			return digitaltwin.PatchGatewayInput{}, http.StatusBadRequest
		}
		if !gateway.ValidProvisioningName(name) {
			return digitaltwin.PatchGatewayInput{}, http.StatusBadRequest
		}
		input.Name = &name
	}

	if rawDesc, ok := entries["description"]; ok {
		input.DescriptionSet = true
		if bytes.Equal(rawDesc, []byte("null")) {
			input.Description = nil
		} else {
			var desc string
			if err := json.Unmarshal(rawDesc, &desc); err != nil {
				return digitaltwin.PatchGatewayInput{}, http.StatusBadRequest
			}
			if !gateway.ValidMetadata(desc, 4096) {
				return digitaltwin.PatchGatewayInput{}, http.StatusBadRequest
			}
			input.Description = &desc
		}
	}

	if err := digitaltwin.ValidatePatchGatewayInput(input); err != nil {
		return digitaltwin.PatchGatewayInput{}, http.StatusBadRequest
	}

	return input, 0
}

func parsePatchSensorFields(entries map[string]json.RawMessage) (digitaltwin.PatchSensorInput, int) {
	if len(entries) == 0 {
		return digitaltwin.PatchSensorInput{}, http.StatusBadRequest
	}
	for k := range entries {
		switch k {
		case "name":
			// allowed
		default:
			return digitaltwin.PatchSensorInput{}, http.StatusBadRequest
		}
	}

	rawName, ok := entries["name"]
	if !ok || bytes.Equal(rawName, []byte("null")) {
		return digitaltwin.PatchSensorInput{}, http.StatusBadRequest
	}

	var name string
	if err := json.Unmarshal(rawName, &name); err != nil {
		return digitaltwin.PatchSensorInput{}, http.StatusBadRequest
	}
	if !gateway.ValidProvisioningName(name) {
		return digitaltwin.PatchSensorInput{}, http.StatusBadRequest
	}

	input := digitaltwin.PatchSensorInput{
		Name: &name,
	}

	if err := digitaltwin.ValidatePatchSensorInput(input); err != nil {
		return digitaltwin.PatchSensorInput{}, http.StatusBadRequest
	}

	return input, 0
}

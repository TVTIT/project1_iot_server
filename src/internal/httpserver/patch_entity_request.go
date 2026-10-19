package httpserver

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/digitaltwin/mapper"
	"iot-platform/internal/gateway"
)

// NGSILDNullSentinel is the NGSI-LD sentinel URI used to clear optional fields (e.g. Gateway description).
const NGSILDNullSentinel = "urn:ngsi-ld:null"

// PatchEntityRequest carries the parsed, validated presence-aware PATCH request
// for either a Gateway or Sensor entity.
// Target contains the strictly parsed NGSI-LD identity derived from the path parameter.
// Exactly one of Gateway or Sensor is non-nil, strictly determined by Target.Type.
type PatchEntityRequest struct {
	Target  digitaltwin.ParsedIdentity
	Gateway *digitaltwin.PatchGatewayInput
	Sensor  *digitaltwin.PatchSensorInput
}

// parsePatchEntityRequest parses and strictly validates a PATCH /v1/digital-twins/{entity_id}/attrs request.
//
// Contract expectation:
// decodedEntityID MUST be the public entity identifier string already decoded once by the HTTP layer
// (e.g. via Gin's c.Param("entity_id")). This function strictly enforces URN format via
// digitaltwin.ParseEntityURN and NEVER performs a second URL-decoding step.
// Body identity and type overrides are strictly rejected.
//
// It returns the validated PatchEntityRequest and an HTTP status code:
//   - 0 on successful parsing and validation
//   - 400 (Bad Request) on invalid URN, non-Gateway/Sensor entity type, query params present,
//     JSON framing/syntax/duplicate errors, missing/unknown/injected keys, empty patches,
//     null name/value, blank name, or oversized/NUL-containing strings
//   - 413 (Payload Too Large) if body exceeds limit
//   - 415 (Unsupported Media Type) if Content-Type is not application/json or application/ld+json with UTF-8 charset.
func parsePatchEntityRequest(r *http.Request, decodedEntityID string, limit int64) (PatchEntityRequest, int) {
	parsed, err := digitaltwin.ParseEntityURN(decodedEntityID)
	if err != nil {
		return PatchEntityRequest{}, http.StatusBadRequest
	}
	if parsed.Type != digitaltwin.EntityTypeGateway && parsed.Type != digitaltwin.EntityTypeSensor {
		return PatchEntityRequest{}, http.StatusBadRequest
	}

	entries, media, status := readStrictAttrsJSONObject(r, limit)
	if status != 0 {
		return PatchEntityRequest{}, status
	}

	if media == "application/ld+json" {
		rawCtx, ok := entries["@context"]
		if !ok {
			return PatchEntityRequest{}, http.StatusBadRequest
		}
		if st := validateAttrsContext(rawCtx); st != 0 {
			return PatchEntityRequest{}, st
		}
	} else {
		// application/json uses fixed vocabulary (no @context)
		if _, ok := entries["@context"]; ok {
			return PatchEntityRequest{}, http.StatusBadRequest
		}
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

func readStrictAttrsJSONObject(r *http.Request, limit int64) (map[string]json.RawMessage, string, int) {
	if r.URL.RawQuery != "" || r.URL.ForceQuery {
		return nil, "", http.StatusBadRequest
	}
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || (media != "application/json" && media != "application/ld+json") {
		return nil, "", http.StatusUnsupportedMediaType
	}
	for key, value := range params {
		if key != "charset" || !strings.EqualFold(value, "utf-8") {
			return nil, "", http.StatusUnsupportedMediaType
		}
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if int64(len(data)) > limit {
		return nil, "", http.StatusRequestEntityTooLarge
	}
	if err != nil || !validJSONUnicode(data) {
		return nil, "", http.StatusBadRequest
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return nil, "", http.StatusBadRequest
	}
	entries := make(map[string]json.RawMessage)
	seen := make(map[string]bool)
	for dec.More() {
		token, err = dec.Token()
		if err != nil {
			return nil, "", http.StatusBadRequest
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return nil, "", http.StatusBadRequest
		}
		seen[key] = true
		var raw json.RawMessage
		if dec.Decode(&raw) != nil {
			return nil, "", http.StatusBadRequest
		}
		entries[key] = raw
	}
	if token, err = dec.Token(); err != nil || token != json.Delim('}') {
		return nil, "", http.StatusBadRequest
	}
	if _, err = dec.Token(); err != io.EOF {
		return nil, "", http.StatusBadRequest
	}
	return entries, media, 0
}

func allowedAttrsCustomTerms() map[string]string {
	terms := mapper.ProjectCustomTerms()
	return map[string]string{
		"Gateway":            terms.Gateway,
		"Sensor":             terms.Sensor,
		"gateway_id":         terms.GatewayID,
		"measurement":        terms.Measurement,
		"sample_interval_us": terms.SampleIntervalUs,
		"hasSensor":          terms.HasSensor,
		"isHostedBy":         terms.IsHostedBy,
		"measurementUnit":    terms.MeasurementUnit,
	}
}

func validateAttrsContext(raw json.RawMessage) int {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return http.StatusBadRequest
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return http.StatusBadRequest
	}

	switch v := tok.(type) {
	case string:
		if v != mapper.CoreContextV19 {
			return http.StatusBadRequest
		}
		if _, err := dec.Token(); err != io.EOF {
			return http.StatusBadRequest
		}
		return 0

	case json.Delim:
		if v != '[' {
			return http.StatusBadRequest
		}

		firstTok, err := dec.Token()
		if err != nil {
			return http.StatusBadRequest
		}

		switch fv := firstTok.(type) {
		case string:
			if fv != mapper.CoreContextV19 {
				return http.StatusBadRequest
			}
			closeArr, err := dec.Token()
			if err != nil || closeArr != json.Delim(']') {
				return http.StatusBadRequest
			}
			if _, err := dec.Token(); err != io.EOF {
				return http.StatusBadRequest
			}
			return 0

		case json.Delim:
			if fv != '{' {
				return http.StatusBadRequest
			}

			allowedTerms := allowedAttrsCustomTerms()
			seenKeys := make(map[string]bool)

			for dec.More() {
				kTok, err := dec.Token()
				if err != nil {
					return http.StatusBadRequest
				}
				key, ok := kTok.(string)
				if !ok || seenKeys[key] {
					return http.StatusBadRequest
				}
				seenKeys[key] = true

				expectedIRI, ok := allowedTerms[key]
				if !ok {
					return http.StatusBadRequest
				}

				vTok, err := dec.Token()
				if err != nil {
					return http.StatusBadRequest
				}
				valStr, ok := vTok.(string)
				if !ok || valStr != expectedIRI {
					return http.StatusBadRequest
				}
			}

			closeObj, err := dec.Token()
			if err != nil || closeObj != json.Delim('}') {
				return http.StatusBadRequest
			}
			if len(seenKeys) == 0 {
				return http.StatusBadRequest
			}

			if !dec.More() {
				return http.StatusBadRequest
			}
			secTok, err := dec.Token()
			if err != nil {
				return http.StatusBadRequest
			}
			secStr, ok := secTok.(string)
			if !ok || secStr != mapper.CoreContextV19 {
				return http.StatusBadRequest
			}

			closeArr, err := dec.Token()
			if err != nil || closeArr != json.Delim(']') {
				return http.StatusBadRequest
			}
			if _, err := dec.Token(); err != io.EOF {
				return http.StatusBadRequest
			}
			return 0

		default:
			return http.StatusBadRequest
		}

	default:
		return http.StatusBadRequest
	}
}

type propertyFragment struct {
	Type  string
	Value string
}

func parsePropertyFragment(raw json.RawMessage) (propertyFragment, int) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return propertyFragment{}, http.StatusBadRequest
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return propertyFragment{}, http.StatusBadRequest
	}
	seen := make(map[string]bool)
	var prop propertyFragment
	hasType := false
	hasValue := false

	for dec.More() {
		token, err = dec.Token()
		if err != nil {
			return propertyFragment{}, http.StatusBadRequest
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return propertyFragment{}, http.StatusBadRequest
		}
		seen[key] = true
		switch key {
		case "type":
			var tVal any
			if err := dec.Decode(&tVal); err != nil {
				return propertyFragment{}, http.StatusBadRequest
			}
			tStr, ok := tVal.(string)
			if !ok || tStr != "Property" {
				return propertyFragment{}, http.StatusBadRequest
			}
			prop.Type = tStr
			hasType = true

		case "value":
			var vVal any
			if err := dec.Decode(&vVal); err != nil {
				return propertyFragment{}, http.StatusBadRequest
			}
			vStr, ok := vVal.(string)
			if !ok {
				return propertyFragment{}, http.StatusBadRequest
			}
			prop.Value = vStr
			hasValue = true

		default:
			return propertyFragment{}, http.StatusBadRequest
		}
	}
	if token, err = dec.Token(); err != nil || token != json.Delim('}') {
		return propertyFragment{}, http.StatusBadRequest
	}
	if _, err = dec.Token(); err != io.EOF {
		return propertyFragment{}, http.StatusBadRequest
	}
	if !hasType || !hasValue {
		return propertyFragment{}, http.StatusBadRequest
	}
	return prop, 0
}

func parsePatchGatewayFields(entries map[string]json.RawMessage) (digitaltwin.PatchGatewayInput, int) {
	var input digitaltwin.PatchGatewayInput
	attrCount := 0

	for k, raw := range entries {
		switch k {
		case "@context":
			// Validated earlier

		case "name":
			attrCount++
			frag, st := parsePropertyFragment(raw)
			if st != 0 {
				return digitaltwin.PatchGatewayInput{}, st
			}
			if frag.Value == NGSILDNullSentinel {
				return digitaltwin.PatchGatewayInput{}, http.StatusBadRequest
			}
			if !gateway.ValidProvisioningName(frag.Value) {
				return digitaltwin.PatchGatewayInput{}, http.StatusBadRequest
			}
			input.Name = &frag.Value

		case "description":
			attrCount++
			frag, st := parsePropertyFragment(raw)
			if st != 0 {
				return digitaltwin.PatchGatewayInput{}, st
			}
			input.DescriptionSet = true
			if frag.Value == NGSILDNullSentinel {
				input.Description = nil
			} else {
				if !gateway.ValidMetadata(frag.Value, 4096) || len(frag.Value) > 512 {
					return digitaltwin.PatchGatewayInput{}, http.StatusBadRequest
				}
				input.Description = &frag.Value
			}

		default:
			return digitaltwin.PatchGatewayInput{}, http.StatusBadRequest
		}
	}

	if attrCount == 0 {
		return digitaltwin.PatchGatewayInput{}, http.StatusBadRequest
	}
	if err := digitaltwin.ValidatePatchGatewayInput(input); err != nil {
		return digitaltwin.PatchGatewayInput{}, http.StatusBadRequest
	}
	return input, 0
}

func parsePatchSensorFields(entries map[string]json.RawMessage) (digitaltwin.PatchSensorInput, int) {
	var input digitaltwin.PatchSensorInput
	attrCount := 0

	for k, raw := range entries {
		switch k {
		case "@context":
			// Validated earlier

		case "name":
			attrCount++
			frag, st := parsePropertyFragment(raw)
			if st != 0 {
				return digitaltwin.PatchSensorInput{}, st
			}
			if frag.Value == NGSILDNullSentinel {
				return digitaltwin.PatchSensorInput{}, http.StatusBadRequest
			}
			if !gateway.ValidProvisioningName(frag.Value) {
				return digitaltwin.PatchSensorInput{}, http.StatusBadRequest
			}
			input.Name = &frag.Value

		default:
			return digitaltwin.PatchSensorInput{}, http.StatusBadRequest
		}
	}

	if attrCount == 0 || input.Name == nil {
		return digitaltwin.PatchSensorInput{}, http.StatusBadRequest
	}
	if err := digitaltwin.ValidatePatchSensorInput(input); err != nil {
		return digitaltwin.PatchSensorInput{}, http.StatusBadRequest
	}
	return input, 0
}

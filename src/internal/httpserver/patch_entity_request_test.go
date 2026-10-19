package httpserver

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"encoding/json"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/digitaltwin/mapper"
)

func TestParsePatchEntityRequest_Gateway_Success(t *testing.T) {
	gwURN := "urn:ngsi-ld:Gateway:gateway_001"

	t.Run("name only with untrimmed string preserved", func(t *testing.T) {
		body := `{"name": {"type": "Property", "value": "  AM5728 Gateway 001  "}}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN+"/attrs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		parsed, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Target.Type != digitaltwin.EntityTypeGateway || parsed.Target.GatewayID != "gateway_001" {
			t.Fatalf("unexpected target: %+v", parsed.Target)
		}
		if parsed.Gateway == nil || parsed.Sensor != nil {
			t.Fatalf("expected Gateway set and Sensor nil, got gateway=%v sensor=%v", parsed.Gateway, parsed.Sensor)
		}
		if parsed.Gateway.Name == nil || *parsed.Gateway.Name != "  AM5728 Gateway 001  " {
			t.Fatalf("expected untrimmed name preserved, got %v", parsed.Gateway.Name)
		}
		if parsed.Gateway.DescriptionSet || parsed.Gateway.Description != nil {
			t.Fatalf("expected description omitted, got set=%v desc=%v", parsed.Gateway.DescriptionSet, parsed.Gateway.Description)
		}
	})

	t.Run("description only string", func(t *testing.T) {
		body := `{"description": {"type": "Property", "value": "Bàn giao lab IoT"}}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN+"/attrs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		parsed, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Gateway.Name != nil {
			t.Fatalf("expected name omitted, got %v", parsed.Gateway.Name)
		}
		if !parsed.Gateway.DescriptionSet || parsed.Gateway.Description == nil || *parsed.Gateway.Description != "Bàn giao lab IoT" {
			t.Fatalf("expected description set with value, got set=%v desc=%v", parsed.Gateway.DescriptionSet, parsed.Gateway.Description)
		}
	})

	t.Run("clear description with sentinel urn:ngsi-ld:null", func(t *testing.T) {
		body := `{"description": {"type": "Property", "value": "urn:ngsi-ld:null"}}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN+"/attrs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		parsed, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Gateway.Name != nil {
			t.Fatalf("expected name omitted, got %v", parsed.Gateway.Name)
		}
		if !parsed.Gateway.DescriptionSet || parsed.Gateway.Description != nil {
			t.Fatalf("expected DescriptionSet=true and Description=nil, got set=%v desc=%v", parsed.Gateway.DescriptionSet, parsed.Gateway.Description)
		}
	})

	t.Run("both name and description string", func(t *testing.T) {
		body := `{"name": {"type": "Property", "value": "GW 001 Updated"}, "description": {"type": "Property", "value": "New description"}}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN+"/attrs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		parsed, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Gateway.Name == nil || *parsed.Gateway.Name != "GW 001 Updated" {
			t.Fatalf("expected updated name, got %v", parsed.Gateway.Name)
		}
		if !parsed.Gateway.DescriptionSet || parsed.Gateway.Description == nil || *parsed.Gateway.Description != "New description" {
			t.Fatalf("expected updated description, got set=%v desc=%v", parsed.Gateway.DescriptionSet, parsed.Gateway.Description)
		}
	})

	t.Run("name string and description clear sentinel", func(t *testing.T) {
		body := `{"name": {"type": "Property", "value": "GW 001 Clear Desc"}, "description": {"type": "Property", "value": "urn:ngsi-ld:null"}}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN+"/attrs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		parsed, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Gateway.Name == nil || *parsed.Gateway.Name != "GW 001 Clear Desc" {
			t.Fatalf("expected name set, got %v", parsed.Gateway.Name)
		}
		if !parsed.Gateway.DescriptionSet || parsed.Gateway.Description != nil {
			t.Fatalf("expected description clear null, got set=%v desc=%v", parsed.Gateway.DescriptionSet, parsed.Gateway.Description)
		}
	})

	t.Run("application/ld+json with locked core context URI string", func(t *testing.T) {
		body := `{"name": {"type": "Property", "value": "AM5728 LD"}, "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN+"/attrs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/ld+json")

		parsed, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Gateway.Name == nil || *parsed.Gateway.Name != "AM5728 LD" {
			t.Fatalf("expected parsed name, got %v", parsed.Gateway.Name)
		}
	})

	t.Run("application/ld+json with locked core context URI in array", func(t *testing.T) {
		body := `{"name": {"type": "Property", "value": "AM5728 LD Array"}, "@context": ["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN+"/attrs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/ld+json")

		parsed, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Gateway.Name == nil || *parsed.Gateway.Name != "AM5728 LD Array" {
			t.Fatalf("expected parsed name, got %v", parsed.Gateway.Name)
		}
	})

	t.Run("application/ld+json with project context array", func(t *testing.T) {
		body := `{"name": {"type": "Property", "value": "AM5728 Project Context"}, "@context": [{"Gateway": "https://example.org/ngsi-ld/iot/v1#Gateway"}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN+"/attrs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/ld+json")

		parsed, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Gateway.Name == nil || *parsed.Gateway.Name != "AM5728 Project Context" {
			t.Fatalf("expected parsed name, got %v", parsed.Gateway.Name)
		}
	})

	t.Run("application/ld+json with canonical emitted project context array", func(t *testing.T) {
		canonicalCtx, err := json.Marshal(mapper.ProjectContextArray())
		if err != nil {
			t.Fatalf("failed to marshal ProjectContextArray: %v", err)
		}
		body := `{"name": {"type": "Property", "value": "AM5728 Full Canonical Context"}, "@context": ` + string(canonicalCtx) + `}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN+"/attrs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/ld+json")

		parsed, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Gateway.Name == nil || *parsed.Gateway.Name != "AM5728 Full Canonical Context" {
			t.Fatalf("expected parsed name, got %v", parsed.Gateway.Name)
		}
	})
}

func TestParsePatchEntityRequest_Sensor_Success(t *testing.T) {
	sensorURN := "urn:ngsi-ld:Sensor:gateway_001:sensor_001"

	t.Run("name only with untrimmed string preserved", func(t *testing.T) {
		body := `{"name": {"type": "Property", "value": "  Temperature Sensor 01  "}}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+sensorURN+"/attrs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		parsed, status := parsePatchEntityRequest(req, sensorURN, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Target.Type != digitaltwin.EntityTypeSensor || parsed.Target.GatewayID != "gateway_001" || parsed.Target.SensorID != "sensor_001" {
			t.Fatalf("unexpected target: %+v", parsed.Target)
		}
		if parsed.Sensor == nil || parsed.Gateway != nil {
			t.Fatalf("expected Sensor set and Gateway nil, got sensor=%v gateway=%v", parsed.Sensor, parsed.Gateway)
		}
		if parsed.Sensor.Name == nil || *parsed.Sensor.Name != "  Temperature Sensor 01  " {
			t.Fatalf("expected untrimmed name preserved, got %v", parsed.Sensor.Name)
		}
	})

	t.Run("application/ld+json with locked context", func(t *testing.T) {
		body := `{"name": {"type": "Property", "value": "Sensor LD"}, "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+sensorURN+"/attrs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/ld+json; charset=utf-8")

		parsed, status := parsePatchEntityRequest(req, sensorURN, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Sensor.Name == nil || *parsed.Sensor.Name != "Sensor LD" {
			t.Fatalf("expected parsed sensor name, got %v", parsed.Sensor.Name)
		}
	})
}

func TestParsePatchEntityRequest_Rejections(t *testing.T) {
	gwURN := "urn:ngsi-ld:Gateway:gateway_001"
	sensorURN := "urn:ngsi-ld:Sensor:gateway_001:sensor_001"

	cases := []struct {
		name        string
		entityID    string
		contentType string
		body        string
		wantStatus  int
	}{
		// Empty patch
		{"Gateway empty object", gwURN, "application/json", `{}`, http.StatusBadRequest},
		{"Sensor empty object", sensorURN, "application/json", `{}`, http.StatusBadRequest},

		// Flat body compatibility rejected (must be Property fragment)
		{"Gateway flat name string", gwURN, "application/json", `{"name": "plain"}`, http.StatusBadRequest},
		{"Gateway flat description string", gwURN, "application/json", `{"description": "plain"}`, http.StatusBadRequest},
		{"Sensor flat name string", sensorURN, "application/json", `{"name": "plain"}`, http.StatusBadRequest},

		// Root JSON null literal
		{"Gateway name null literal", gwURN, "application/json", `{"name": null}`, http.StatusBadRequest},
		{"Gateway description null literal", gwURN, "application/json", `{"description": null}`, http.StatusBadRequest},
		{"Sensor name null literal", sensorURN, "application/json", `{"name": null}`, http.StatusBadRequest},

		// Property value null
		{"Gateway name value null", gwURN, "application/json", `{"name": {"type": "Property", "value": null}}`, http.StatusBadRequest},
		{"Gateway description value null", gwURN, "application/json", `{"description": {"type": "Property", "value": null}}`, http.StatusBadRequest},
		{"Sensor name value null", sensorURN, "application/json", `{"name": {"type": "Property", "value": null}}`, http.StatusBadRequest},

		// Name deletion sentinel forbidden
		{"Gateway name sentinel forbidden", gwURN, "application/json", `{"name": {"type": "Property", "value": "urn:ngsi-ld:null"}}`, http.StatusBadRequest},
		{"Sensor name sentinel forbidden", sensorURN, "application/json", `{"name": {"type": "Property", "value": "urn:ngsi-ld:null"}}`, http.StatusBadRequest},

		// Blank or whitespace-only name
		{"Gateway empty name", gwURN, "application/json", `{"name": {"type": "Property", "value": ""}}`, http.StatusBadRequest},
		{"Gateway spaces-only name", gwURN, "application/json", `{"name": {"type": "Property", "value": "   "}}`, http.StatusBadRequest},
		{"Sensor empty name", sensorURN, "application/json", `{"name": {"type": "Property", "value": ""}}`, http.StatusBadRequest},
		{"Sensor spaces-only name", sensorURN, "application/json", `{"name": {"type": "Property", "value": " \t\n "}}`, http.StatusBadRequest},

		// Wrong Property type
		{"Gateway wrong property type", gwURN, "application/json", `{"name": {"type": "Relationship", "value": "gw"}}`, http.StatusBadRequest},
		{"Gateway missing property type", gwURN, "application/json", `{"name": {"value": "gw"}}`, http.StatusBadRequest},
		{"Gateway invalid property type string", gwURN, "application/json", `{"name": {"type": "GeoProperty", "value": "gw"}}`, http.StatusBadRequest},
		{"Sensor wrong property type", sensorURN, "application/json", `{"name": {"type": "Relationship", "value": "s1"}}`, http.StatusBadRequest},

		// Missing value
		{"Gateway missing value", gwURN, "application/json", `{"name": {"type": "Property"}}`, http.StatusBadRequest},

		// Extra fields in Property object
		{"Gateway extra property field", gwURN, "application/json", `{"name": {"type": "Property", "value": "gw", "extra": "field"}}`, http.StatusBadRequest},
		{"Sensor extra property field", sensorURN, "application/json", `{"name": {"type": "Property", "value": "s1", "unitCode": "CEL"}}`, http.StatusBadRequest},

		// Duplicate keys
		{"Gateway duplicate keys in property", gwURN, "application/json", `{"name": {"type": "Property", "type": "Property", "value": "gw"}}`, http.StatusBadRequest},
		{"Gateway duplicate root keys", gwURN, "application/json", `{"name": {"type": "Property", "value": "a"}, "name": {"type": "Property", "value": "b"}}`, http.StatusBadRequest},
		{"Sensor duplicate root keys", sensorURN, "application/json", `{"name": {"type": "Property", "value": "a"}, "name": {"type": "Property", "value": "b"}}`, http.StatusBadRequest},

		// Wrong value types
		{"Gateway name number", gwURN, "application/json", `{"name": {"type": "Property", "value": 123}}`, http.StatusBadRequest},
		{"Gateway name boolean", gwURN, "application/json", `{"name": {"type": "Property", "value": true}}`, http.StatusBadRequest},
		{"Gateway name array", gwURN, "application/json", `{"name": {"type": "Property", "value": ["abc"]}}`, http.StatusBadRequest},
		{"Gateway name object", gwURN, "application/json", `{"name": {"type": "Property", "value": {"val":"abc"}}}`, http.StatusBadRequest},
		{"Gateway description number", gwURN, "application/json", `{"description": {"type": "Property", "value": 123}}`, http.StatusBadRequest},
		{"Gateway description boolean", gwURN, "application/json", `{"description": {"type": "Property", "value": false}}`, http.StatusBadRequest},
		{"Sensor name number", sensorURN, "application/json", `{"name": {"type": "Property", "value": 456}}`, http.StatusBadRequest},

		// Oversized fields
		{"Gateway oversized name > 256 bytes", gwURN, "application/json", `{"name": {"type": "Property", "value": "` + strings.Repeat("a", 257) + `"}}`, http.StatusBadRequest},
		{"Gateway oversized description > 4096 bytes", gwURN, "application/json", `{"description": {"type": "Property", "value": "` + strings.Repeat("b", 4097) + `"}}`, http.StatusBadRequest},
		{"Sensor oversized name > 256 bytes", sensorURN, "application/json", `{"name": {"type": "Property", "value": "` + strings.Repeat("a", 257) + `"}}`, http.StatusBadRequest},

		// NUL byte and invalid Unicode
		{"Gateway name with escaped NUL", gwURN, "application/json", `{"name": {"type": "Property", "value": "gw\u000001"}}`, http.StatusBadRequest},
		{"Gateway description with escaped NUL", gwURN, "application/json", `{"description": {"type": "Property", "value": "desc\u0000bad"}}`, http.StatusBadRequest},
		{"Sensor name with escaped NUL", sensorURN, "application/json", `{"name": {"type": "Property", "value": "sensor\u000001"}}`, http.StatusBadRequest},
		{"Gateway name with lone surrogate", gwURN, "application/json", `{"name": {"type": "Property", "value": "gw\uD800bad"}}`, http.StatusBadRequest},

		// Injected / unsupported fields on Gateway
		{"Gateway unit injection", gwURN, "application/json", `{"unit": {"type": "Property", "value": "degC"}}`, http.StatusBadRequest},
		{"Gateway id injection", gwURN, "application/json", `{"id": "urn:ngsi-ld:Gateway:gw1"}`, http.StatusBadRequest},
		{"Gateway entity_id injection", gwURN, "application/json", `{"entity_id": "urn:ngsi-ld:Gateway:gw1"}`, http.StatusBadRequest},
		{"Gateway entity_type injection", gwURN, "application/json", `{"entity_type": "Gateway"}`, http.StatusBadRequest},
		{"Gateway gateway_id injection", gwURN, "application/json", `{"gateway_id": "gateway_001"}`, http.StatusBadRequest},
		{"Gateway sensor_id injection", gwURN, "application/json", `{"sensor_id": "sensor_001"}`, http.StatusBadRequest},
		{"Gateway owner_user_id injection", gwURN, "application/json", `{"owner_user_id": "3f2a1b4c-0000-4000-8000-000000000001"}`, http.StatusBadRequest},
		{"Gateway attributes injection", gwURN, "application/json", `{"attributes": {}}`, http.StatusBadRequest},
		{"Gateway hasSensor injection", gwURN, "application/json", `{"hasSensor": []}`, http.StatusBadRequest},
		{"Gateway reported_state injection", gwURN, "application/json", `{"reported_state": {}}`, http.StatusBadRequest},
		{"Gateway desired_state injection", gwURN, "application/json", `{"desired_state": {}}`, http.StatusBadRequest},
		{"Gateway reported_version injection", gwURN, "application/json", `{"reported_version": 1}`, http.StatusBadRequest},
		{"Gateway desired_version injection", gwURN, "application/json", `{"desired_version": 1}`, http.StatusBadRequest},
		{"Gateway sample_interval_us injection", gwURN, "application/json", `{"sample_interval_us": 10000}`, http.StatusBadRequest},
		{"Gateway role injection", gwURN, "application/json", `{"role": "owner"}`, http.StatusBadRequest},
		{"Gateway credentials injection", gwURN, "application/json", `{"credentials": {}}`, http.StatusBadRequest},
		{"Gateway null injected field", gwURN, "application/json", `{"unit": null}`, http.StatusBadRequest},

		// Injected / unsupported fields on Sensor
		{"Sensor description injection", sensorURN, "application/json", `{"description": {"type": "Property", "value": "not allowed on sensor"}}`, http.StatusBadRequest},
		{"Sensor unit injection", sensorURN, "application/json", `{"unit": {"type": "Property", "value": "degC"}}`, http.StatusBadRequest},
		{"Sensor id injection", sensorURN, "application/json", `{"id": "urn:ngsi-ld:Sensor:gw1:s1"}`, http.StatusBadRequest},
		{"Sensor entity_id injection", sensorURN, "application/json", `{"entity_id": "urn:ngsi-ld:Sensor:gw1:s1"}`, http.StatusBadRequest},
		{"Sensor entity_type injection", sensorURN, "application/json", `{"entity_type": "Sensor"}`, http.StatusBadRequest},
		{"Sensor gateway_id injection", sensorURN, "application/json", `{"gateway_id": "gateway_001"}`, http.StatusBadRequest},
		{"Sensor sensor_id injection", sensorURN, "application/json", `{"sensor_id": "sensor_001"}`, http.StatusBadRequest},
		{"Sensor reported_state injection", sensorURN, "application/json", `{"reported_state": {}}`, http.StatusBadRequest},
		{"Sensor desired_state injection", sensorURN, "application/json", `{"desired_state": {}}`, http.StatusBadRequest},
		{"Sensor role injection", sensorURN, "application/json", `{"role": "owner"}`, http.StatusBadRequest},

		// Context violations
		{"application/json with context", gwURN, "application/json", `{"name": {"type": "Property", "value": "gw1"}, "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"}`, http.StatusBadRequest},
		{"application/ld+json missing context", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}}`, http.StatusBadRequest},
		{"application/ld+json foreign context URL", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": "https://evil.org/context.jsonld"}`, http.StatusBadRequest},
		{"application/ld+json foreign context array", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": ["https://evil.org/context.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json core term override", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": [{"name": "https://evil.org/name"}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json @import directive in context object", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": [{"@import": "https://attacker.invalid/context.jsonld"}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json @import directive after core context", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": ["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld", {"@import": "https://attacker.invalid/context.jsonld"}]}`, http.StatusBadRequest},
		{"application/ld+json unknown term in context", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": [{"unknownTerm": "https://example.org/ngsi-ld/iot/v1#unknown"}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json changed custom term IRI", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": [{"measurement": "https://attacker.invalid/a"}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json core term override type", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": [{"type": "https://evil.org/type"}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json core term override Property", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": [{"Property": "https://evil.org/Property"}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json core term override value", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": [{"value": "https://evil.org/value"}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json core term override description", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": [{"description": "https://evil.org/desc"}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json duplicate keys in context object same value", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": [{"measurement": "https://example.org/ngsi-ld/iot/v1#measurement", "measurement": "https://example.org/ngsi-ld/iot/v1#measurement"}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json duplicate keys in context object different value", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": [{"measurement": "https://attacker.invalid/a", "measurement": "https://attacker.invalid/b"}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json duplicate core context in array", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": ["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld", "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json multiple context objects in array", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": [{"Gateway": "https://example.org/ngsi-ld/iot/v1#Gateway"}, {"Sensor": "https://example.org/ngsi-ld/iot/v1#Sensor"}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json inverted context order", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": ["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld", {"Gateway": "https://example.org/ngsi-ld/iot/v1#Gateway"}]}`, http.StatusBadRequest},
		{"application/ld+json ProjectNamespace as hosted URL string", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": ["https://example.org/ngsi-ld/iot/v1#", "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json empty context array", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": []}`, http.StatusBadRequest},
		{"application/ld+json empty context object in array", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": [{}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json bare context object missing core context", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": {"Gateway": "https://example.org/ngsi-ld/iot/v1#Gateway"}}`, http.StatusBadRequest},
		{"application/ld+json non-string term value in context object", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": [{"Gateway": 123}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},
		{"application/ld+json number element in context array", gwURN, "application/ld+json", `{"name": {"type": "Property", "value": "gw1"}, "@context": [123, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"]}`, http.StatusBadRequest},

		// Invalid entity URN path parameters
		{"empty entity_id", "", "application/json", `{"name": {"type": "Property", "value": "Valid Name"}}`, http.StatusBadRequest},
		{"plain identifier without URN prefix", "gateway_001", "application/json", `{"name": {"type": "Property", "value": "Valid Name"}}`, http.StatusBadRequest},
		{"unknown entity type URN", "urn:ngsi-ld:Device:dev_01", "application/json", `{"name": {"type": "Property", "value": "Valid Name"}}`, http.StatusBadRequest},
		{"double URL-encoded entity_id", "urn%3Angsi-ld%3AGateway%3Agateway_001", "application/json", `{"name": {"type": "Property", "value": "Valid Name"}}`, http.StatusBadRequest},
		{"colon in gateway remainder", "urn:ngsi-ld:Gateway:gw:extra", "application/json", `{"name": {"type": "Property", "value": "Valid Name"}}`, http.StatusBadRequest},
		{"sensor URN with missing sensor_id", "urn:ngsi-ld:Sensor:gw1", "application/json", `{"name": {"type": "Property", "value": "Valid Name"}}`, http.StatusBadRequest},
		{"sensor URN with extra colon part", "urn:ngsi-ld:Sensor:gw1:s1:extra", "application/json", `{"name": {"type": "Property", "value": "Valid Name"}}`, http.StatusBadRequest},

		// Malformed JSON / framing
		{"trailing JSON document", gwURN, "application/json", `{"name": {"type": "Property", "value": "gw1"}} {"name": {"type": "Property", "value": "gw2"}}`, http.StatusBadRequest},
		{"top-level array", gwURN, "application/json", `["name", "gw1"]`, http.StatusBadRequest},
		{"top-level string", gwURN, "application/json", `"name"`, http.StatusBadRequest},
		{"malformed JSON syntax", gwURN, "application/json", `{"name": "gw1"`, http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("PATCH", "/v1/digital-twins/test/attrs", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.contentType)

			_, status := parsePatchEntityRequest(req, tc.entityID, 16384)
			if status != tc.wantStatus {
				t.Fatalf("expected status %d, got %d", tc.wantStatus, status)
			}
		})
	}
}

func TestParsePatchEntityRequest_FramingAndMedia(t *testing.T) {
	gwURN := "urn:ngsi-ld:Gateway:gateway_001"
	validBody := `{"name": {"type": "Property", "value": "AM5728 Gateway"}}`

	t.Run("query string rejected with 400", func(t *testing.T) {
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN+"?foo=bar", strings.NewReader(validBody))
		req.Header.Set("Content-Type", "application/json")

		_, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", status)
		}
	})

	t.Run("ForceQuery rejected with 400", func(t *testing.T) {
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN, strings.NewReader(validBody))
		req.Header.Set("Content-Type", "application/json")
		req.URL.ForceQuery = true

		_, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", status)
		}
	})

	t.Run("missing Content-Type rejected with 415", func(t *testing.T) {
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN, strings.NewReader(validBody))

		_, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != http.StatusUnsupportedMediaType {
			t.Fatalf("expected status 415, got %d", status)
		}
	})

	t.Run("wrong Content-Type rejected with 415", func(t *testing.T) {
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN, strings.NewReader(validBody))
		req.Header.Set("Content-Type", "text/plain")

		_, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != http.StatusUnsupportedMediaType {
			t.Fatalf("expected status 415, got %d", status)
		}
	})

	t.Run("wrong charset rejected with 415", func(t *testing.T) {
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN, strings.NewReader(validBody))
		req.Header.Set("Content-Type", "application/json; charset=iso-8859-1")

		_, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != http.StatusUnsupportedMediaType {
			t.Fatalf("expected status 415, got %d", status)
		}
	})

	t.Run("valid charset utf-8 accepted", func(t *testing.T) {
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN, strings.NewReader(validBody))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")

		_, status := parsePatchEntityRequest(req, gwURN, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
	})

	t.Run("body exceeds limit rejected with 413", func(t *testing.T) {
		oversized := `{"name": "` + strings.Repeat("a", 100) + `"}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN, bytes.NewReader([]byte(oversized)))
		req.Header.Set("Content-Type", "application/json")

		_, status := parsePatchEntityRequest(req, gwURN, 50)
		if status != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected status 413, got %d", status)
		}
	})
}

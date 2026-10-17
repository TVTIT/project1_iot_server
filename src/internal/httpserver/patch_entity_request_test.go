package httpserver

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"iot-platform/internal/digitaltwin"
)

func TestParsePatchEntityRequest_Gateway_Success(t *testing.T) {
	gwURN := "urn:ngsi-ld:Gateway:gateway_001"

	t.Run("name only with untrimmed string preserved", func(t *testing.T) {
		body := `{"name": "  AM5728 Gateway 001  "}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN, strings.NewReader(body))
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
		body := `{"description": "Bàn giao lab IoT"}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN, strings.NewReader(body))
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

	t.Run("clear description with explicit null", func(t *testing.T) {
		body := `{"description": null}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN, strings.NewReader(body))
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
		body := `{"name": "GW 001 Updated", "description": "New description"}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN, strings.NewReader(body))
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

	t.Run("name string and description clear null", func(t *testing.T) {
		body := `{"name": "GW 001 Clear Desc", "description": null}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+gwURN, strings.NewReader(body))
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
}

func TestParsePatchEntityRequest_Sensor_Success(t *testing.T) {
	sensorURN := "urn:ngsi-ld:Sensor:gateway_001:sensor_001"

	t.Run("name only with untrimmed string preserved", func(t *testing.T) {
		body := `{"name": "  Temperature Sensor 01  "}`
		req := httptest.NewRequest("PATCH", "/v1/digital-twins/"+sensorURN, strings.NewReader(body))
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
}

func TestParsePatchEntityRequest_Rejections(t *testing.T) {
	gwURN := "urn:ngsi-ld:Gateway:gateway_001"
	sensorURN := "urn:ngsi-ld:Sensor:gateway_001:sensor_001"

	cases := []struct {
		name       string
		entityID   string
		body       string
		wantStatus int
	}{
		// Empty patch
		{"Gateway empty object", gwURN, `{}`, http.StatusBadRequest},
		{"Sensor empty object", sensorURN, `{}`, http.StatusBadRequest},

		// Name null
		{"Gateway name null", gwURN, `{"name": null}`, http.StatusBadRequest},
		{"Sensor name null", sensorURN, `{"name": null}`, http.StatusBadRequest},

		// Blank or whitespace-only name
		{"Gateway empty name", gwURN, `{"name": ""}`, http.StatusBadRequest},
		{"Gateway spaces-only name", gwURN, `{"name": "   "}`, http.StatusBadRequest},
		{"Sensor empty name", sensorURN, `{"name": ""}`, http.StatusBadRequest},
		{"Sensor spaces-only name", sensorURN, `{"name": " \t\n "}`, http.StatusBadRequest},

		// Wrong types
		{"Gateway name number", gwURN, `{"name": 123}`, http.StatusBadRequest},
		{"Gateway name boolean", gwURN, `{"name": true}`, http.StatusBadRequest},
		{"Gateway name array", gwURN, `{"name": ["abc"]}`, http.StatusBadRequest},
		{"Gateway name object", gwURN, `{"name": {"val":"abc"}}`, http.StatusBadRequest},
		{"Gateway description number", gwURN, `{"description": 123}`, http.StatusBadRequest},
		{"Gateway description boolean", gwURN, `{"description": false}`, http.StatusBadRequest},
		{"Gateway description array", gwURN, `{"description": ["desc"]}`, http.StatusBadRequest},
		{"Gateway description object", gwURN, `{"description": {"desc":"abc"}}`, http.StatusBadRequest},
		{"Sensor name number", sensorURN, `{"name": 456}`, http.StatusBadRequest},

		// Oversized fields
		{"Gateway oversized name > 256 bytes", gwURN, `{"name": "` + strings.Repeat("a", 257) + `"}`, http.StatusBadRequest},
		{"Gateway oversized description > 4096 bytes", gwURN, `{"description": "` + strings.Repeat("b", 4097) + `"}`, http.StatusBadRequest},
		{"Sensor oversized name > 256 bytes", sensorURN, `{"name": "` + strings.Repeat("a", 257) + `"}`, http.StatusBadRequest},

		// NUL byte and invalid Unicode
		{"Gateway name with escaped NUL", gwURN, `{"name": "gw\u000001"}`, http.StatusBadRequest},
		{"Gateway description with escaped NUL", gwURN, `{"description": "desc\u0000bad"}`, http.StatusBadRequest},
		{"Sensor name with escaped NUL", sensorURN, `{"name": "sensor\u000001"}`, http.StatusBadRequest},
		{"Gateway name with lone surrogate", gwURN, `{"name": "gw\uD800bad"}`, http.StatusBadRequest},

		// Injected / unsupported fields on Gateway
		{"Gateway unit injection", gwURN, `{"unit": "degC"}`, http.StatusBadRequest},
		{"Gateway id injection", gwURN, `{"id": "urn:ngsi-ld:Gateway:gw1"}`, http.StatusBadRequest},
		{"Gateway entity_id injection", gwURN, `{"entity_id": "urn:ngsi-ld:Gateway:gw1"}`, http.StatusBadRequest},
		{"Gateway entity_type injection", gwURN, `{"entity_type": "Gateway"}`, http.StatusBadRequest},
		{"Gateway gateway_id injection", gwURN, `{"gateway_id": "gateway_001"}`, http.StatusBadRequest},
		{"Gateway sensor_id injection", gwURN, `{"sensor_id": "sensor_001"}`, http.StatusBadRequest},
		{"Gateway owner_user_id injection", gwURN, `{"owner_user_id": "3f2a1b4c-0000-4000-8000-000000000001"}`, http.StatusBadRequest},
		{"Gateway attributes injection", gwURN, `{"attributes": {}}`, http.StatusBadRequest},
		{"Gateway hasSensor injection", gwURN, `{"hasSensor": []}`, http.StatusBadRequest},
		{"Gateway reported_state injection", gwURN, `{"reported_state": {}}`, http.StatusBadRequest},
		{"Gateway desired_state injection", gwURN, `{"desired_state": {}}`, http.StatusBadRequest},
		{"Gateway reported_version injection", gwURN, `{"reported_version": 1}`, http.StatusBadRequest},
		{"Gateway desired_version injection", gwURN, `{"desired_version": 1}`, http.StatusBadRequest},
		{"Gateway sample_interval_us injection", gwURN, `{"sample_interval_us": 10000}`, http.StatusBadRequest},
		{"Gateway role injection", gwURN, `{"role": "owner"}`, http.StatusBadRequest},
		{"Gateway credentials injection", gwURN, `{"credentials": {}}`, http.StatusBadRequest},
		{"Gateway null injected field", gwURN, `{"unit": null}`, http.StatusBadRequest},

		// Injected / unsupported fields on Sensor
		{"Sensor description injection", sensorURN, `{"description": "not allowed on sensor"}`, http.StatusBadRequest},
		{"Sensor description null injection", sensorURN, `{"description": null}`, http.StatusBadRequest},
		{"Sensor unit injection", sensorURN, `{"unit": "degC"}`, http.StatusBadRequest},
		{"Sensor id injection", sensorURN, `{"id": "urn:ngsi-ld:Sensor:gw1:s1"}`, http.StatusBadRequest},
		{"Sensor entity_id injection", sensorURN, `{"entity_id": "urn:ngsi-ld:Sensor:gw1:s1"}`, http.StatusBadRequest},
		{"Sensor entity_type injection", sensorURN, `{"entity_type": "Sensor"}`, http.StatusBadRequest},
		{"Sensor gateway_id injection", sensorURN, `{"gateway_id": "gateway_001"}`, http.StatusBadRequest},
		{"Sensor sensor_id injection", sensorURN, `{"sensor_id": "sensor_001"}`, http.StatusBadRequest},
		{"Sensor reported_state injection", sensorURN, `{"reported_state": {}}`, http.StatusBadRequest},
		{"Sensor desired_state injection", sensorURN, `{"desired_state": {}}`, http.StatusBadRequest},
		{"Sensor role injection", sensorURN, `{"role": "owner"}`, http.StatusBadRequest},

		// Invalid entity URN path parameters
		{"empty entity_id", "", `{"name": "Valid Name"}`, http.StatusBadRequest},
		{"plain identifier without URN prefix", "gateway_001", `{"name": "Valid Name"}`, http.StatusBadRequest},
		{"unknown entity type URN", "urn:ngsi-ld:Device:dev_01", `{"name": "Valid Name"}`, http.StatusBadRequest},
		{"double URL-encoded entity_id", "urn%3Angsi-ld%3AGateway%3Agateway_001", `{"name": "Valid Name"}`, http.StatusBadRequest},
		{"colon in gateway remainder", "urn:ngsi-ld:Gateway:gw:extra", `{"name": "Valid Name"}`, http.StatusBadRequest},
		{"sensor URN with missing sensor_id", "urn:ngsi-ld:Sensor:gw1", `{"name": "Valid Name"}`, http.StatusBadRequest},
		{"sensor URN with extra colon part", "urn:ngsi-ld:Sensor:gw1:s1:extra", `{"name": "Valid Name"}`, http.StatusBadRequest},

		// Malformed JSON / framing
		{"duplicate keys in body", gwURN, `{"name": "gw1", "name": "gw2"}`, http.StatusBadRequest},
		{"trailing JSON document", gwURN, `{"name": "gw1"} {"name": "gw2"}`, http.StatusBadRequest},
		{"top-level array", gwURN, `["name", "gw1"]`, http.StatusBadRequest},
		{"top-level string", gwURN, `"name"`, http.StatusBadRequest},
		{"malformed JSON syntax", gwURN, `{"name": "gw1"`, http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("PATCH", "/v1/digital-twins/test", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")

			_, status := parsePatchEntityRequest(req, tc.entityID, 16384)
			if status != tc.wantStatus {
				t.Fatalf("expected status %d, got %d", tc.wantStatus, status)
			}
		})
	}
}

func TestParsePatchEntityRequest_FramingAndMedia(t *testing.T) {
	gwURN := "urn:ngsi-ld:Gateway:gateway_001"
	validBody := `{"name": "AM5728 Gateway"}`

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

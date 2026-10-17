package httpserver

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
)

func TestParsePostEntityRequest_Gateway_Success(t *testing.T) {
	ownerID := uuid.MustParse("3f2a1b4c-0000-4000-8000-000000000001")

	t.Run("with description string and untrimmed name preserved", func(t *testing.T) {
		body := `{
			"entity_type": "Gateway",
			"gateway_id": "gateway_001",
			"name": "  AM5728 Gateway 001  ",
			"description": "Bàn giao lab",
			"owner_user_id": "3f2a1b4c-0000-4000-8000-000000000001"
		}`
		req := httptest.NewRequest("POST", "/v1/digital-twins", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		parsed, status := parsePostEntityRequest(req, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.EntityType != PostEntityKindGateway {
			t.Fatalf("expected entity type Gateway, got %q", parsed.EntityType)
		}
		if parsed.Gateway == nil || parsed.Sensor != nil {
			t.Fatalf("expected Gateway non-nil and Sensor nil, got gateway=%v sensor=%v", parsed.Gateway, parsed.Sensor)
		}
		if parsed.Gateway.GatewayID != "gateway_001" {
			t.Fatalf("expected gateway_id gateway_001, got %q", parsed.Gateway.GatewayID)
		}
		if parsed.Gateway.Name != "  AM5728 Gateway 001  " {
			t.Fatalf("expected untrimmed name preserved, got %q", parsed.Gateway.Name)
		}
		if parsed.Gateway.Description == nil || *parsed.Gateway.Description != "Bàn giao lab" {
			t.Fatalf("expected description 'Bàn giao lab', got %v", parsed.Gateway.Description)
		}
		if parsed.Gateway.OwnerUserID != ownerID {
			t.Fatalf("expected owner_user_id %v, got %v", ownerID, parsed.Gateway.OwnerUserID)
		}
	})

	t.Run("with explicit null description", func(t *testing.T) {
		body := `{
			"entity_type": "Gateway",
			"gateway_id": "gateway_002",
			"name": "GW 2",
			"description": null,
			"owner_user_id": "3f2a1b4c-0000-4000-8000-000000000001"
		}`
		req := httptest.NewRequest("POST", "/v1/digital-twins", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		parsed, status := parsePostEntityRequest(req, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Gateway.Description != nil {
			t.Fatalf("expected nil description for explicit null, got %v", parsed.Gateway.Description)
		}
	})

	t.Run("with omitted description", func(t *testing.T) {
		body := `{
			"entity_type": "Gateway",
			"gateway_id": "gateway_003",
			"name": "GW 3",
			"owner_user_id": "3f2a1b4c-0000-4000-8000-000000000001"
		}`
		req := httptest.NewRequest("POST", "/v1/digital-twins", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		parsed, status := parsePostEntityRequest(req, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Gateway.Description != nil {
			t.Fatalf("expected nil description for omitted description, got %v", parsed.Gateway.Description)
		}
	})
}

func TestParsePostEntityRequest_Sensor_Success(t *testing.T) {
	t.Run("with unit string and untrimmed name preserved", func(t *testing.T) {
		body := `{
			"entity_type": "Sensor",
			"gateway_id": "gateway_001",
			"sensor_id": "sensor_001",
			"name": "  Temperature Sensor  ",
			"unit": "degC"
		}`
		req := httptest.NewRequest("POST", "/v1/digital-twins", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		parsed, status := parsePostEntityRequest(req, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.EntityType != PostEntityKindSensor {
			t.Fatalf("expected entity type Sensor, got %q", parsed.EntityType)
		}
		if parsed.Sensor == nil || parsed.Gateway != nil {
			t.Fatalf("expected Sensor non-nil and Gateway nil, got sensor=%v gateway=%v", parsed.Sensor, parsed.Gateway)
		}
		if parsed.Sensor.GatewayID != "gateway_001" {
			t.Fatalf("expected gateway_id gateway_001, got %q", parsed.Sensor.GatewayID)
		}
		if parsed.Sensor.SensorID != "sensor_001" {
			t.Fatalf("expected sensor_id sensor_001, got %q", parsed.Sensor.SensorID)
		}
		if parsed.Sensor.Name != "  Temperature Sensor  " {
			t.Fatalf("expected untrimmed name preserved, got %q", parsed.Sensor.Name)
		}
		if parsed.Sensor.Unit == nil || *parsed.Sensor.Unit != "degC" {
			t.Fatalf("expected unit 'degC', got %v", parsed.Sensor.Unit)
		}
	})

	t.Run("with explicit null unit", func(t *testing.T) {
		body := `{
			"entity_type": "Sensor",
			"gateway_id": "gateway_001",
			"sensor_id": "sensor_002",
			"name": "Sensor 2",
			"unit": null
		}`
		req := httptest.NewRequest("POST", "/v1/digital-twins", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		parsed, status := parsePostEntityRequest(req, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Sensor.Unit != nil {
			t.Fatalf("expected nil unit for explicit null, got %v", parsed.Sensor.Unit)
		}
	})

	t.Run("with omitted unit", func(t *testing.T) {
		body := `{
			"entity_type": "Sensor",
			"gateway_id": "gateway_001",
			"sensor_id": "sensor_003",
			"name": "Sensor 3"
		}`
		req := httptest.NewRequest("POST", "/v1/digital-twins", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		parsed, status := parsePostEntityRequest(req, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
		if parsed.Sensor.Unit != nil {
			t.Fatalf("expected nil unit for omitted unit, got %v", parsed.Sensor.Unit)
		}
	})
}

func TestParsePostEntityRequest_EntityType_Validation(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"missing entity_type", `{"gateway_id":"gw1","name":"GW1","owner_user_id":"3f2a1b4c-0000-4000-8000-000000000001"}`},
		{"null entity_type", `{"entity_type":null,"gateway_id":"gw1","name":"GW1","owner_user_id":"3f2a1b4c-0000-4000-8000-000000000001"}`},
		{"lowercase gateway", `{"entity_type":"gateway","gateway_id":"gw1","name":"GW1","owner_user_id":"3f2a1b4c-0000-4000-8000-000000000001"}`},
		{"lowercase sensor", `{"entity_type":"sensor","gateway_id":"gw1","sensor_id":"s1","name":"S1"}`},
		{"unsupported Device", `{"entity_type":"Device","gateway_id":"gw1","name":"Dev"}`},
		{"empty string entity_type", `{"entity_type":"","gateway_id":"gw1","name":"GW1","owner_user_id":"3f2a1b4c-0000-4000-8000-000000000001"}`},
		{"number entity_type", `{"entity_type":1,"gateway_id":"gw1","name":"GW1","owner_user_id":"3f2a1b4c-0000-4000-8000-000000000001"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/digital-twins", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			_, status := parsePostEntityRequest(req, 16384)
			if status != 400 {
				t.Fatalf("expected status 400, got %d", status)
			}
		})
	}
}

func TestParsePostEntityRequest_Gateway_Negatives(t *testing.T) {
	valid := `{"entity_type":"Gateway","gateway_id":"gw1","name":"GW1","owner_user_id":"3f2a1b4c-0000-4000-8000-000000000001","description":"desc"}`
	for _, tc := range []struct {
		name string
		body string
	}{
		{"missing gateway_id", strings.Replace(valid, `"gateway_id":"gw1",`, "", 1)},
		{"missing name", strings.Replace(valid, `"name":"GW1",`, "", 1)},
		{"missing owner_user_id", strings.Replace(valid, `,"owner_user_id":"3f2a1b4c-0000-4000-8000-000000000001"`, "", 1)},
		{"null gateway_id", strings.Replace(valid, `"gw1"`, `null`, 1)},
		{"null name", strings.Replace(valid, `"GW1"`, `null`, 1)},
		{"null owner_user_id", strings.Replace(valid, `"3f2a1b4c-0000-4000-8000-000000000001"`, `null`, 1)},
		{"reserved gateway_id backend_service", strings.Replace(valid, `"gw1"`, `"backend_service"`, 1)},
		{"invalid gateway_id special chars", strings.Replace(valid, `"gw1"`, `"gw@01"`, 1)},
		{"invalid gateway_id empty string", strings.Replace(valid, `"gw1"`, `""`, 1)},
		{"invalid gateway_id too long", strings.Replace(valid, `"gw1"`, `"`+strings.Repeat("a", 65)+`"`, 1)},
		{"invalid name empty string", strings.Replace(valid, `"GW1"`, `""`, 1)},
		{"invalid name blank after trim", strings.Replace(valid, `"GW1"`, `"   "`, 1)},
		{"invalid name too long", strings.Replace(valid, `"GW1"`, `"`+strings.Repeat("n", 257)+`"`, 1)},
		{"invalid name contains NUL", strings.Replace(valid, `"GW1"`, `"a\u0000b"`, 1)},
		{"invalid owner_user_id non-uuid", strings.Replace(valid, `"3f2a1b4c-0000-4000-8000-000000000001"`, `"not-a-uuid"`, 1)},
		{"invalid owner_user_id nil uuid", strings.Replace(valid, `"3f2a1b4c-0000-4000-8000-000000000001"`, `"00000000-0000-0000-0000-000000000000"`, 1)},
		{"invalid description too long", strings.Replace(valid, `"desc"`, `"`+strings.Repeat("d", 4097)+`"`, 1)},
		{"invalid description contains NUL", strings.Replace(valid, `"desc"`, `"d\u0000e"`, 1)},
		{"invalid description wrong type number", strings.Replace(valid, `"desc"`, `123`, 1)},
		{"invalid description wrong type bool", strings.Replace(valid, `"desc"`, `true`, 1)},
		// Branch conflicting fields on Gateway:
		{"conflict sensor_id present", valid[:len(valid)-1] + `,"sensor_id":"s1"}`},
		{"conflict sensor_id null", valid[:len(valid)-1] + `,"sensor_id":null}`},
		{"conflict unit present", valid[:len(valid)-1] + `,"unit":"degC"}`},
		{"conflict unit null", valid[:len(valid)-1] + `,"unit":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/digital-twins", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			_, status := parsePostEntityRequest(req, 16384)
			if status != 400 {
				t.Fatalf("expected status 400, got %d", status)
			}
		})
	}
}

func TestParsePostEntityRequest_Sensor_Negatives(t *testing.T) {
	valid := `{"entity_type":"Sensor","gateway_id":"gw1","sensor_id":"sensor_001","name":"Temp","unit":"degC"}`
	for _, tc := range []struct {
		name string
		body string
	}{
		{"missing gateway_id", strings.Replace(valid, `"gateway_id":"gw1",`, "", 1)},
		{"missing sensor_id", strings.Replace(valid, `"sensor_id":"sensor_001",`, "", 1)},
		{"missing name", strings.Replace(valid, `"name":"Temp",`, "", 1)},
		{"null gateway_id", strings.Replace(valid, `"gw1"`, `null`, 1)},
		{"null sensor_id", strings.Replace(valid, `"sensor_001"`, `null`, 1)},
		{"null name", strings.Replace(valid, `"Temp"`, `null`, 1)},
		{"reserved gateway_id backend_service", strings.Replace(valid, `"gw1"`, `"backend_service"`, 1)},
		{"invalid sensor_id special chars", strings.Replace(valid, `"sensor_001"`, `"sensor@01"`, 1)},
		{"invalid sensor_id empty string", strings.Replace(valid, `"sensor_001"`, `""`, 1)},
		{"invalid sensor_id too long", strings.Replace(valid, `"sensor_001"`, `"`+strings.Repeat("s", 65)+`"`, 1)},
		{"invalid name empty string", strings.Replace(valid, `"Temp"`, `""`, 1)},
		{"invalid name blank after trim", strings.Replace(valid, `"Temp"`, `"   "`, 1)},
		{"invalid name too long", strings.Replace(valid, `"Temp"`, `"`+strings.Repeat("t", 257)+`"`, 1)},
		{"invalid name contains NUL", strings.Replace(valid, `"Temp"`, `"t\u0000p"`, 1)},
		{"invalid unit too long", strings.Replace(valid, `"degC"`, `"`+strings.Repeat("u", 65)+`"`, 1)},
		{"invalid unit contains NUL", strings.Replace(valid, `"degC"`, `"u\u0000c"`, 1)},
		{"invalid unit wrong type number", strings.Replace(valid, `"degC"`, `123`, 1)},
		{"invalid unit wrong type bool", strings.Replace(valid, `"degC"`, `false`, 1)},
		// Branch conflicting fields on Sensor:
		{"conflict owner_user_id present", valid[:len(valid)-1] + `,"owner_user_id":"3f2a1b4c-0000-4000-8000-000000000001"}`},
		{"conflict owner_user_id null", valid[:len(valid)-1] + `,"owner_user_id":null}`},
		{"conflict description present", valid[:len(valid)-1] + `,"description":"desc"}`},
		{"conflict description null", valid[:len(valid)-1] + `,"description":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/digital-twins", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			_, status := parsePostEntityRequest(req, 16384)
			if status != 400 {
				t.Fatalf("expected status 400, got %d", status)
			}
		})
	}
}

func TestParsePostEntityRequest_Injection_Rejection(t *testing.T) {
	baseGateway := `{"entity_type":"Gateway","gateway_id":"gw1","name":"GW1","owner_user_id":"3f2a1b4c-0000-4000-8000-000000000001"}`
	baseSensor := `{"entity_type":"Sensor","gateway_id":"gw1","sensor_id":"s1","name":"S1"}`

	for _, tc := range []struct {
		name string
		body string
	}{
		{"role injection on Gateway", baseGateway[:len(baseGateway)-1] + `,"role":"admin"}`},
		{"actor injection on Gateway", baseGateway[:len(baseGateway)-1] + `,"actor":"3f2a1b4c-0000-4000-8000-000000000001"}`},
		{"user_id injection on Gateway", baseGateway[:len(baseGateway)-1] + `,"user_id":"3f2a1b4c-0000-4000-8000-000000000001"}`},
		{"id injection on Gateway", baseGateway[:len(baseGateway)-1] + `,"id":"urn:ngsi-ld:Gateway:gw1"}`},
		{"entity_id injection on Gateway", baseGateway[:len(baseGateway)-1] + `,"entity_id":"urn:ngsi-ld:Gateway:gw1"}`},
		{"attributes injection on Gateway", baseGateway[:len(baseGateway)-1] + `,"attributes":{}}`},
		{"relationships injection on Gateway", baseGateway[:len(baseGateway)-1] + `,"relationships":{}}`},
		{"hasSensor injection on Gateway", baseGateway[:len(baseGateway)-1] + `,"hasSensor":[]}`},
		{"reported_state injection on Gateway", baseGateway[:len(baseGateway)-1] + `,"reported_state":{}}`},
		{"desired_state injection on Gateway", baseGateway[:len(baseGateway)-1] + `,"desired_state":{}}`},
		{"sample_interval_us injection on Gateway", baseGateway[:len(baseGateway)-1] + `,"sample_interval_us":10000}`},
		{"password injection on Gateway", baseGateway[:len(baseGateway)-1] + `,"password":"secret"}`},
		{"credentials injection on Gateway", baseGateway[:len(baseGateway)-1] + `,"credentials":{}}`},
		{"twin_entities injection on Gateway", baseGateway[:len(baseGateway)-1] + `,"twin_entities":{}}`},

		{"role injection on Sensor", baseSensor[:len(baseSensor)-1] + `,"role":"admin"}`},
		{"actor injection on Sensor", baseSensor[:len(baseSensor)-1] + `,"actor":"3f2a1b4c-0000-4000-8000-000000000001"}`},
		{"id injection on Sensor", baseSensor[:len(baseSensor)-1] + `,"id":"urn:ngsi-ld:Sensor:gw1:s1"}`},
		{"reported_state injection on Sensor", baseSensor[:len(baseSensor)-1] + `,"reported_state":{}}`},
		{"desired_state injection on Sensor", baseSensor[:len(baseSensor)-1] + `,"desired_state":{}}`},
		{"sample_interval_us injection on Sensor", baseSensor[:len(baseSensor)-1] + `,"sample_interval_us":10000}`},
		{"credentials injection on Sensor", baseSensor[:len(baseSensor)-1] + `,"credentials":{}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/digital-twins", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			_, status := parsePostEntityRequest(req, 16384)
			if status != 400 {
				t.Fatalf("expected status 400, got %d", status)
			}
		})
	}
}

func TestParsePostEntityRequest_Transport_And_Framing(t *testing.T) {
	good := `{"entity_type":"Gateway","gateway_id":"gw1","name":"GW1","owner_user_id":"3f2a1b4c-0000-4000-8000-000000000001"}`

	for _, tc := range []struct {
		name, body, media, query string
		limit                    int64
		status                   int
	}{
		{"valid exact limit", good, "application/json", "", int64(len(good)), 0},
		{"valid utf8 charset", good, "application/json; charset=UTF-8", "", 16384, 0},
		{"valid surrogate pair emoji", strings.Replace(good, `"GW1"`, `"GW \ud83d\ude00"`, 1), "application/json", "", 16384, 0},

		{"over limit", good, "application/json", "", int64(len(good) - 1), 413},
		{"query param present", good, "application/json", "?foo=bar", 16384, 400},
		{"empty query present", good, "application/json", "?", 16384, 400},
		{"media text/json", good, "text/json", "", 16384, 415},
		{"media text/plain", good, "text/plain", "", 16384, 415},
		{"charset latin1", good, "application/json; charset=latin1", "", 16384, 415},
		{"extra param", good, "application/json; version=1", "", 16384, 415},

		{"duplicate key", strings.Replace(good, `"gw1"`, `"gw1","gateway_id":"gw2"`, 1), "application/json", "", 16384, 400},
		{"duplicate escaped key", strings.Replace(good, `"name":"GW1"`, `"name":"GW1","na\u006de":"GW2"`, 1), "application/json", "", 16384, 400},
		{"trailing document", good + ` {}`, "application/json", "", 16384, 400},
		{"array root", `[]`, "application/json", "", 16384, 400},
		{"string root", `"hello"`, "application/json", "", 16384, 400},
		{"number root", `123`, "application/json", "", 16384, 400},
		{"null root", `null`, "application/json", "", 16384, 400},
		{"high surrogate", strings.Replace(good, `"GW1"`, `"\ud800"`, 1), "application/json", "", 16384, 400},
		{"low surrogate", strings.Replace(good, `"GW1"`, `"\udc00"`, 1), "application/json", "", 16384, 400},
		{"escaped NUL", strings.Replace(good, `"GW1"`, `"\u0000"`, 1), "application/json", "", 16384, 400},
		{"raw NUL byte", strings.Replace(good, `"GW1"`, "\"\x00\"", 1), "application/json", "", 16384, 400},
		{"invalid UTF-8 byte", strings.Replace(good, `"GW1"`, "\""+string([]byte{255})+"\"", 1), "application/json", "", 16384, 400},
		{"case sensitive key rejection", strings.Replace(good, `"name"`, `"Name"`, 1), "application/json", "", 16384, 400},
		{"case sensitive entity_type rejection", strings.Replace(good, `"entity_type"`, `"Entity_Type"`, 1), "application/json", "", 16384, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/digital-twins"+tc.query, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.media)
			_, status := parsePostEntityRequest(req, tc.limit)
			if status != tc.status {
				t.Fatalf("status=%d want=%d", status, tc.status)
			}
		})
	}
}

func TestPostEntityRequest_ToDomain(t *testing.T) {
	ownerID := uuid.New()
	desc := "test desc"
	unit := "celsius"

	t.Run("gateway mapping", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/v1/digital-twins", strings.NewReader(`{
			"entity_type": "Gateway",
			"gateway_id": "gw1",
			"name": "GW 1",
			"description": "test desc",
			"owner_user_id": "`+ownerID.String()+`"
		}`))
		req.Header.Set("Content-Type", "application/json")
		parsed, status := parsePostEntityRequest(req, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}

		domain := parsed.ToDomain()
		if domain.Type != digitaltwin.EntityTypeGateway {
			t.Fatalf("expected EntityTypeGateway, got %v", domain.Type)
		}
		if domain.Gateway == nil || domain.Sensor != nil {
			t.Fatal("expected non-nil Gateway and nil Sensor")
		}
		if domain.Gateway.GatewayID != "gw1" || domain.Gateway.Name != "GW 1" || domain.Gateway.OwnerUserID != ownerID {
			t.Fatalf("unexpected gateway fields: %+v", domain.Gateway)
		}
		if domain.Gateway.Description == nil || *domain.Gateway.Description != desc {
			t.Fatalf("unexpected description: %v", domain.Gateway.Description)
		}
	})

	t.Run("sensor mapping", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/v1/digital-twins", strings.NewReader(`{
			"entity_type": "Sensor",
			"gateway_id": "gw1",
			"sensor_id": "s1",
			"name": "Sensor 1",
			"unit": "celsius"
		}`))
		req.Header.Set("Content-Type", "application/json")
		parsed, status := parsePostEntityRequest(req, 16384)
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}

		domain := parsed.ToDomain()
		if domain.Type != digitaltwin.EntityTypeSensor {
			t.Fatalf("expected EntityTypeSensor, got %v", domain.Type)
		}
		if domain.Sensor == nil || domain.Gateway != nil {
			t.Fatal("expected non-nil Sensor and nil Gateway")
		}
		if domain.Sensor.GatewayID != "gw1" || domain.Sensor.SensorID != "s1" || domain.Sensor.Name != "Sensor 1" {
			t.Fatalf("unexpected sensor fields: %+v", domain.Sensor)
		}
		if domain.Sensor.Unit == nil || *domain.Sensor.Unit != unit {
			t.Fatalf("unexpected unit: %v", domain.Sensor.Unit)
		}
	})
}

package httpserver

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"
)

func ptr[T any](v T) *T {
	return &v
}

func TestTwinDetailResponseDTO_Gateway(t *testing.T) {
	updatedAt := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	relCreatedAt := time.Date(2026, 10, 15, 8, 30, 0, 0, time.UTC)

	res := &digitaltwin.TwinDetailResult{
		ID:          "urn:ngsi-ld:Gateway:gateway_001",
		Type:        digitaltwin.EntityTypeGateway,
		Name:        "AM5728 Gateway 001",
		Description: nil, // null description
		Unit:        nil,
		GatewayID:   "gateway_001",
		UpdatedAt:   updatedAt,
		Relationships: []digitaltwin.RelationshipItem{
			{
				TargetURN: "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
				CreatedAt: relCreatedAt,
			},
		},
		RelationshipPage: &digitaltwin.RelationshipPage{
			RelationshipType: "hasSensor",
			Limit:            1,
			Count:            1,
			Partial:          true,
			NextCursor:       ptr("v1.eyJzIjoidGVzdCJ9"),
		},
	}

	dto, err := NewTwinDetailResponseDTO(res)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("failed to marshal DTO: %v", err)
	}
	jsonStr := string(b)

	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	// Verify @context
	if _, ok := m["@context"]; !ok {
		t.Error("missing @context in Gateway detail")
	}

	// Verify id, type, gateway_id
	if m["id"] != "urn:ngsi-ld:Gateway:gateway_001" {
		t.Errorf("id = %v, want urn:ngsi-ld:Gateway:gateway_001", m["id"])
	}
	if m["type"] != "Gateway" {
		t.Errorf("type = %v, want Gateway", m["type"])
	}
	if m["gateway_id"] != "gateway_001" {
		t.Errorf("gateway_id = %v, want gateway_001", m["gateway_id"])
	}

	// Verify name Property
	nameMap, ok := m["name"].(map[string]any)
	if !ok {
		t.Fatalf("name is not object: %v", m["name"])
	}
	if nameMap["type"] != "Property" || nameMap["value"] != "AM5728 Gateway 001" {
		t.Errorf("unexpected name property: %v", nameMap)
	}
	if nameMap["modifiedAt"] != "2026-10-15T09:00:00.000000Z" {
		t.Errorf("name modifiedAt = %v, want 2026-10-15T09:00:00.000000Z", nameMap["modifiedAt"])
	}

	// Verify description Property with null value
	descMap, ok := m["description"].(map[string]any)
	if !ok {
		t.Fatalf("description is not object: %v", m["description"])
	}
	if descMap["type"] != "Property" || descMap["value"] != nil {
		t.Errorf("unexpected description property: %v", descMap)
	}
	if descMap["modifiedAt"] != "2026-10-15T09:00:00.000000Z" {
		t.Errorf("description modifiedAt = %v", descMap["modifiedAt"])
	}

	// Verify unit is omitted
	if _, ok := m["unit"]; ok {
		t.Errorf("Gateway detail must not contain unit: %s", jsonStr)
	}

	// Verify hasSensor
	hasSensorArr, ok := m["hasSensor"].([]any)
	if !ok || len(hasSensorArr) != 1 {
		t.Fatalf("hasSensor = %v, want array with 1 item", m["hasSensor"])
	}
	relItem := hasSensorArr[0].(map[string]any)
	if relItem["type"] != "Relationship" || relItem["object"] != "urn:ngsi-ld:Sensor:gateway_001:sensor_001" {
		t.Errorf("unexpected relationship item: %v", relItem)
	}
	if relItem["createdAt"] != "2026-10-15T08:30:00.000000Z" {
		t.Errorf("relationship createdAt = %v", relItem["createdAt"])
	}

	// Verify relationship_page
	relPage, ok := m["relationship_page"].(map[string]any)
	if !ok {
		t.Fatalf("relationship_page is not object: %v", m["relationship_page"])
	}
	if relPage["relationship_type"] != "hasSensor" || relPage["limit"] != float64(1) || relPage["count"] != float64(1) || relPage["partial"] != true {
		t.Errorf("unexpected relationship_page: %v", relPage)
	}

	// Test Gateway with zero sensors -> hasSensor must be [] (not omitted, not null)
	resEmpty := &digitaltwin.TwinDetailResult{
		ID:            "urn:ngsi-ld:Gateway:gateway_001",
		Type:          digitaltwin.EntityTypeGateway,
		Name:          "AM5728 Gateway 001",
		GatewayID:     "gateway_001",
		UpdatedAt:     updatedAt,
		Relationships: nil,
	}
	dtoEmpty, err := NewTwinDetailResponseDTO(resEmpty)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bEmpty, err := json.Marshal(dtoEmpty)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}
	if !strings.Contains(string(bEmpty), `"hasSensor":[]`) {
		t.Errorf("gateway with 0 sensors must serialize hasSensor as [], got: %s", string(bEmpty))
	}
}

func TestTwinDetailResponseDTO_Sensor(t *testing.T) {
	updatedAt := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)

	res := &digitaltwin.TwinDetailResult{
		ID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Type:      digitaltwin.EntityTypeSensor,
		Name:      "Temperature",
		Unit:      ptr("degC"),
		GatewayID: "gateway_001",
		UpdatedAt: updatedAt,
	}

	dto, err := NewTwinDetailResponseDTO(res)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("failed to marshal DTO: %v", err)
	}
	jsonStr := string(b)

	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	// Verify @context
	if _, ok := m["@context"]; !ok {
		t.Error("missing @context in Sensor detail")
	}

	// Verify id, type, gateway_id
	if m["id"] != "urn:ngsi-ld:Sensor:gateway_001:sensor_001" {
		t.Errorf("id = %v", m["id"])
	}
	if m["type"] != "Sensor" {
		t.Errorf("type = %v", m["type"])
	}
	if m["gateway_id"] != "gateway_001" {
		t.Errorf("gateway_id = %v", m["gateway_id"])
	}

	// Verify name Property
	nameMap := m["name"].(map[string]any)
	if nameMap["value"] != "Temperature" {
		t.Errorf("name value = %v, want Temperature", nameMap["value"])
	}

	// Verify unit Property
	unitMap, ok := m["unit"].(map[string]any)
	if !ok {
		t.Fatalf("unit is not object: %v", m["unit"])
	}
	if unitMap["type"] != "Property" || unitMap["value"] != "degC" {
		t.Errorf("unexpected unit property: %v", unitMap)
	}
	if unitMap["modifiedAt"] != "2026-10-15T09:00:00.000000Z" {
		t.Errorf("unit modifiedAt = %v", unitMap["modifiedAt"])
	}

	// Assert description, hasSensor, relationship_page are omitted from JSON
	if _, ok := m["description"]; ok {
		t.Errorf("Sensor detail must omit description: %s", jsonStr)
	}
	if _, ok := m["hasSensor"]; ok {
		t.Errorf("Sensor detail must omit hasSensor: %s", jsonStr)
	}
	if _, ok := m["relationship_page"]; ok {
		t.Errorf("Sensor detail must omit relationship_page: %s", jsonStr)
	}

	// Test Sensor with null unit
	resNullUnit := &digitaltwin.TwinDetailResult{
		ID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Type:      digitaltwin.EntityTypeSensor,
		Name:      "RawVoltage",
		Unit:      nil,
		GatewayID: "gateway_001",
		UpdatedAt: updatedAt,
	}
	dtoNullUnit, err := NewTwinDetailResponseDTO(resNullUnit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bNullUnit, _ := json.Marshal(dtoNullUnit)
	var mNull map[string]any
	_ = json.Unmarshal(bNullUnit, &mNull)
	unitNullMap := mNull["unit"].(map[string]any)
	if unitNullMap["value"] != nil {
		t.Errorf("unit value should be null, got %v", unitNullMap["value"])
	}
}

func TestTwinListResponseDTO(t *testing.T) {
	updatedAt := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)

	// List with 2 items and next_cursor
	res := &digitaltwin.TwinListResult{
		Items: []digitaltwin.TwinSummary{
			{
				ID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
				Type:      digitaltwin.EntityTypeSensor,
				Name:      ptr("Temperature"),
				GatewayID: "gateway_001",
				UpdatedAt: updatedAt,
			},
			{
				ID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_002",
				Type:      digitaltwin.EntityTypeSensor,
				Name:      ptr("Humidity"),
				GatewayID: "gateway_001",
				UpdatedAt: updatedAt,
			},
		},
		NextCursor: ptr("v1.eyJrIjoidGVzdCJ9"),
	}

	dto, err := NewTwinListResponseDTO(res)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	itemsArr, ok := m["items"].([]any)
	if !ok || len(itemsArr) != 2 {
		t.Fatalf("items = %v, want 2 items", m["items"])
	}
	if m["next_cursor"] != "v1.eyJrIjoidGVzdCJ9" {
		t.Errorf("next_cursor = %v", m["next_cursor"])
	}

	// Empty items array [] (not null) with nil cursor
	resEmpty := &digitaltwin.TwinListResult{
		Items:      nil,
		NextCursor: nil,
	}
	dtoEmpty, err := NewTwinListResponseDTO(resEmpty)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bEmpty, err := json.Marshal(dtoEmpty)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}
	jsonEmpty := string(bEmpty)
	if !strings.Contains(jsonEmpty, `"items":[]`) {
		t.Errorf("expected \"items\":[], got %s", jsonEmpty)
	}
	if !strings.Contains(jsonEmpty, `"next_cursor":null`) {
		t.Errorf("expected \"next_cursor\":null, got %s", jsonEmpty)
	}
}

func TestTwinStateResponseDTO(t *testing.T) {
	updatedAt := time.Date(2026, 10, 15, 9, 30, 0, 0, time.UTC)
	lastReported := time.Date(2026, 8, 21, 10, 15, 1, 250000000, time.UTC)

	// Note: 9007199254740993 is 2^53 + 1 (cannot be represented precisely as IEEE-754 float64).
	res := &digitaltwin.TwinStateResult{
		ID:              "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Type:            digitaltwin.EntityTypeSensor,
		GatewayID:       "gateway_001",
		ReportedState:   json.RawMessage(`{"sequence_num":9007199254740993,"value":25.5,"active":true}`),
		DesiredState:    json.RawMessage(`{"threshold":10000000000000001}`),
		ReportedVersion: 9007199254740995,
		DesiredVersion:  0,
		LastReportedAt:  &lastReported,
		LastDesiredAt:   nil,
		UpdatedAt:       updatedAt,
	}

	dto, err := NewTwinStateResponseDTO(res)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}
	jsonStr := string(b)

	// Verbatim preservation of big integer in reported_state
	if !strings.Contains(jsonStr, `"sequence_num":9007199254740993`) {
		t.Errorf("reported_state big integer not preserved verbatim: %s", jsonStr)
	}
	// Verbatim preservation of big integer in desired_state
	if !strings.Contains(jsonStr, `"threshold":10000000000000001`) {
		t.Errorf("desired_state big integer not preserved verbatim: %s", jsonStr)
	}

	// Reported and desired versions formatted as decimal strings
	if !strings.Contains(jsonStr, `"reported_version":"9007199254740995"`) {
		t.Errorf("reported_version not formatted as decimal string: %s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"desired_version":"0"`) {
		t.Errorf("desired_version not formatted as decimal string: %s", jsonStr)
	}

	// Nullable last timestamps
	if !strings.Contains(jsonStr, `"last_reported_at":"2026-08-21T10:15:01.250000Z"`) {
		t.Errorf("last_reported_at formatting mismatch: %s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"last_desired_at":null`) {
		t.Errorf("last_desired_at must be null: %s", jsonStr)
	}

	// Updated at
	if !strings.Contains(jsonStr, `"updated_at":"2026-10-15T09:30:00.000000Z"`) {
		t.Errorf("updated_at formatting mismatch: %s", jsonStr)
	}

	// Assert last_desired_by and uncontracted audit fields are absent
	if strings.Contains(jsonStr, "last_desired_by") {
		t.Errorf("last_desired_by must be absent from state DTO: %s", jsonStr)
	}
}

func TestTimestamps_NonUTCToUTC(t *testing.T) {
	loc := time.FixedZone("UTC+7", 7*3600)
	localTime := time.Date(2026, 10, 15, 16, 0, 0, 123456000, loc)

	res := &digitaltwin.TwinDetailResult{
		ID:        "urn:ngsi-ld:Gateway:gateway_001",
		Type:      digitaltwin.EntityTypeGateway,
		Name:      "Gateway 1",
		GatewayID: "gateway_001",
		UpdatedAt: localTime,
	}

	dto, err := NewTwinDetailResponseDTO(res)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if dto.Name.ModifiedAt != "2026-10-15T09:00:00.123456Z" {
		t.Errorf("got %q, want 2026-10-15T09:00:00.123456Z", dto.Name.ModifiedAt)
	}
}

func TestValidationErrors(t *testing.T) {
	validTime := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	yearZero := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)
	negativeYear := time.Date(-10, 1, 1, 0, 0, 0, 0, time.UTC)

	// 1. Nil result
	if _, err := NewTwinListResponseDTO(nil); err == nil {
		t.Error("expected error for nil TwinListResult")
	}
	if _, err := NewTwinDetailResponseDTO(nil); err == nil {
		t.Error("expected error for nil TwinDetailResult")
	}
	if _, err := NewTwinStateResponseDTO(nil); err == nil {
		t.Error("expected error for nil TwinStateResult")
	}

	// 2. Invalid URN
	if _, err := NewTwinDetailResponseDTO(&digitaltwin.TwinDetailResult{
		ID:        "not-a-urn",
		Type:      digitaltwin.EntityTypeGateway,
		Name:      "GW",
		GatewayID: "gateway_001",
		UpdatedAt: validTime,
	}); err == nil {
		t.Error("expected error for invalid URN")
	}

	// 3. Year 0000 or negative year
	if _, err := NewTwinDetailResponseDTO(&digitaltwin.TwinDetailResult{
		ID:        "urn:ngsi-ld:Gateway:gateway_001",
		Type:      digitaltwin.EntityTypeGateway,
		Name:      "GW",
		GatewayID: "gateway_001",
		UpdatedAt: yearZero,
	}); err == nil {
		t.Error("expected error for year zero in detail")
	}

	if _, err := NewTwinStateResponseDTO(&digitaltwin.TwinStateResult{
		ID:        "urn:ngsi-ld:Gateway:gateway_001",
		Type:      digitaltwin.EntityTypeGateway,
		GatewayID: "gateway_001",
		UpdatedAt: negativeYear,
	}); err == nil {
		t.Error("expected error for negative year in state")
	}

	if _, err := NewTwinListResponseDTO(&digitaltwin.TwinListResult{
		Items: []digitaltwin.TwinSummary{
			{
				ID:        "urn:ngsi-ld:Gateway:gateway_001",
				Type:      digitaltwin.EntityTypeGateway,
				GatewayID: "gateway_001",
				UpdatedAt: yearZero,
			},
		},
	}); err == nil {
		t.Error("expected error for year zero in list item")
	}

	// 4. Relationship page count mismatch
	if _, err := NewTwinDetailResponseDTO(&digitaltwin.TwinDetailResult{
		ID:        "urn:ngsi-ld:Gateway:gateway_001",
		Type:      digitaltwin.EntityTypeGateway,
		Name:      "GW",
		GatewayID: "gateway_001",
		UpdatedAt: validTime,
		Relationships: []digitaltwin.RelationshipItem{
			{
				TargetURN: "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
				CreatedAt: validTime,
			},
		},
		RelationshipPage: &digitaltwin.RelationshipPage{
			RelationshipType: "hasSensor",
			Limit:            10,
			Count:            5, // Mismatch: 5 vs 1
			Partial:          false,
		},
	}); err == nil {
		t.Error("expected error for relationship count mismatch")
	}

	// 5. Gateway with unit
	if _, err := NewTwinDetailResponseDTO(&digitaltwin.TwinDetailResult{
		ID:        "urn:ngsi-ld:Gateway:gateway_001",
		Type:      digitaltwin.EntityTypeGateway,
		Name:      "GW",
		Unit:      ptr("degC"),
		GatewayID: "gateway_001",
		UpdatedAt: validTime,
	}); err == nil {
		t.Error("expected error for Gateway with unit")
	}

	// 6. Sensor with description
	if _, err := NewTwinDetailResponseDTO(&digitaltwin.TwinDetailResult{
		ID:          "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Type:        digitaltwin.EntityTypeSensor,
		Name:        "Sensor 1",
		Description: ptr("A sensor"),
		GatewayID:   "gateway_001",
		UpdatedAt:   validTime,
	}); err == nil {
		t.Error("expected error for Sensor with description")
	}

	// 7. Sensor with outgoing relationships
	if _, err := NewTwinDetailResponseDTO(&digitaltwin.TwinDetailResult{
		ID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Type:      digitaltwin.EntityTypeSensor,
		Name:      "Sensor 1",
		GatewayID: "gateway_001",
		UpdatedAt: validTime,
		Relationships: []digitaltwin.RelationshipItem{
			{TargetURN: "urn:ngsi-ld:Sensor:gateway_001:sensor_002", CreatedAt: validTime},
		},
	}); err == nil {
		t.Error("expected error for Sensor with outgoing relationships")
	}

	// 8. Negative versions in state
	if _, err := NewTwinStateResponseDTO(&digitaltwin.TwinStateResult{
		ID:              "urn:ngsi-ld:Gateway:gateway_001",
		Type:            digitaltwin.EntityTypeGateway,
		GatewayID:       "gateway_001",
		ReportedVersion: -1,
		DesiredVersion:  0,
		UpdatedAt:       validTime,
	}); err == nil {
		t.Error("expected error for negative reported_version")
	}
}

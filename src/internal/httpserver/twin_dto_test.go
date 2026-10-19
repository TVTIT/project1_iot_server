package httpserver

import (
	"encoding/json"
	"strconv"
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
		Description: nil, // null description must be omitted
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

	// Verify @context: core context must be last
	ctxArr, ok := m["@context"].([]any)
	if !ok || len(ctxArr) != 2 {
		t.Fatalf("missing or invalid @context in Gateway detail: %v", m["@context"])
	}
	if ctxArr[1] != "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld" {
		t.Errorf("core context must be last element: %v", ctxArr[1])
	}

	// Verify id, type
	if m["id"] != "urn:ngsi-ld:Gateway:gateway_001" {
		t.Errorf("id = %v, want urn:ngsi-ld:Gateway:gateway_001", m["id"])
	}
	if m["type"] != "Gateway" {
		t.Errorf("type = %v, want Gateway", m["type"])
	}

	// Verify name Property
	nameMap, ok := m["name"].(map[string]any)
	if !ok {
		t.Fatalf("name is not object: %v", m["name"])
	}
	if nameMap["type"] != "Property" || nameMap["value"] != "AM5728 Gateway 001" {
		t.Errorf("unexpected name property: %v", nameMap)
	}

	// Verify optional null omission: description is nil -> must be omitted
	if _, ok := m["description"]; ok {
		t.Errorf("description must be omitted when nil: %s", jsonStr)
	}

	// Verify non-nil description is emitted
	resWithDesc := &digitaltwin.TwinDetailResult{
		ID:          "urn:ngsi-ld:Gateway:gateway_001",
		Type:        digitaltwin.EntityTypeGateway,
		Name:        "AM5728 Gateway 001",
		Description: ptr("Main station gateway"),
		GatewayID:   "gateway_001",
		UpdatedAt:   updatedAt,
	}
	dtoWithDesc, err := NewTwinDetailResponseDTO(resWithDesc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bWithDesc, _ := json.Marshal(dtoWithDesc)
	var mWithDesc map[string]any
	_ = json.Unmarshal(bWithDesc, &mWithDesc)
	descMap, ok := mWithDesc["description"].(map[string]any)
	if !ok || descMap["value"] != "Main station gateway" {
		t.Errorf("expected description 'Main station gateway', got: %v", mWithDesc["description"])
	}

	// Verify unit is omitted
	if _, ok := m["unit"]; ok {
		t.Errorf("Gateway detail must not contain unit: %s", jsonStr)
	}

	// Verify hasSensor: 1 sensor -> object is array of strings, objectType is Sensor
	hasSensorMap, ok := m["hasSensor"].(map[string]any)
	if !ok {
		t.Fatalf("hasSensor must be an object, got: %T (%v)", m["hasSensor"], m["hasSensor"])
	}
	if hasSensorMap["type"] != "Relationship" {
		t.Errorf("hasSensor.type = %v, want Relationship", hasSensorMap["type"])
	}
	if hasSensorMap["objectType"] != "Sensor" {
		t.Errorf("hasSensor.objectType = %v, want Sensor", hasSensorMap["objectType"])
	}
	objects, ok := hasSensorMap["object"].([]any)
	if !ok || len(objects) != 1 || objects[0] != "urn:ngsi-ld:Sensor:gateway_001:sensor_001" {
		t.Errorf("hasSensor.object must be array with 1 item, got: %v", hasSensorMap["object"])
	}

	// Multiple sensors: hasSensor.object array of 2 URIs
	resMultiple := &digitaltwin.TwinDetailResult{
		ID:        "urn:ngsi-ld:Gateway:gateway_001",
		Type:      digitaltwin.EntityTypeGateway,
		Name:      "AM5728 Gateway 001",
		GatewayID: "gateway_001",
		UpdatedAt: updatedAt,
		Relationships: []digitaltwin.RelationshipItem{
			{TargetURN: "urn:ngsi-ld:Sensor:gateway_001:sensor_001"},
			{TargetURN: "urn:ngsi-ld:Sensor:gateway_001:sensor_002"},
		},
	}
	dtoMulti, err := NewTwinDetailResponseDTO(resMultiple)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bMulti, _ := json.Marshal(dtoMulti)
	var mMulti map[string]any
	_ = json.Unmarshal(bMulti, &mMulti)
	multiHasSensor := mMulti["hasSensor"].(map[string]any)
	multiObjects := multiHasSensor["object"].([]any)
	if len(multiObjects) != 2 || multiObjects[0] != "urn:ngsi-ld:Sensor:gateway_001:sensor_001" || multiObjects[1] != "urn:ngsi-ld:Sensor:gateway_001:sensor_002" {
		t.Errorf("unexpected multiple hasSensor objects: %v", multiObjects)
	}

	// Verify relationship_page metadata is NOT in body
	if _, ok := m["relationship_page"]; ok {
		t.Errorf("relationship_page metadata must be omitted from body: %s", jsonStr)
	}

	// Test Gateway with zero sensors -> hasSensor must be omitted completely
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
	var mEmpty map[string]any
	_ = json.Unmarshal(bEmpty, &mEmpty)
	if _, ok := mEmpty["hasSensor"]; ok {
		t.Errorf("gateway with 0 sensors must omit hasSensor completely, got: %s", string(bEmpty))
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

	stateRes := &digitaltwin.TwinStateResult{
		ID:              "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Type:            digitaltwin.EntityTypeSensor,
		GatewayID:       "gateway_001",
		ReportedState:   json.RawMessage(`{"value":25.4,"observed_at":"2026-10-18T00:00:01.000000Z","sample_interval_us":10000.0}`),
		DesiredState:    json.RawMessage(`{"value":999.9,"sample_interval_us":99999}`),
		ReportedVersion: 2,
		DesiredVersion:  1,
		UpdatedAt:       updatedAt,
	}

	dto, err := NewTwinDetailResponseDTO(res, stateRes)
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

	// Verify @context: core context must be last
	ctxArr, ok := m["@context"].([]any)
	if !ok || len(ctxArr) != 2 {
		t.Fatalf("missing or invalid @context in Sensor detail: %v", m["@context"])
	}
	if ctxArr[1] != "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld" {
		t.Errorf("core context must be last element: %v", ctxArr[1])
	}

	// Verify id, type
	if m["id"] != "urn:ngsi-ld:Sensor:gateway_001:sensor_001" {
		t.Errorf("id = %v", m["id"])
	}
	if m["type"] != "Sensor" {
		t.Errorf("type = %v", m["type"])
	}

	// Verify name Property
	nameMap := m["name"].(map[string]any)
	if nameMap["value"] != "Temperature" {
		t.Errorf("name value = %v, want Temperature", nameMap["value"])
	}

	// Verify isHostedBy relationship pointing to parent Gateway URN
	hostedMap, ok := m["isHostedBy"].(map[string]any)
	if !ok {
		t.Fatalf("missing or invalid isHostedBy: %v", m["isHostedBy"])
	}
	if hostedMap["type"] != "Relationship" {
		t.Errorf("isHostedBy.type = %v, want Relationship", hostedMap["type"])
	}
	if hostedMap["object"] != "urn:ngsi-ld:Gateway:gateway_001" {
		t.Errorf("isHostedBy.object = %v, want urn:ngsi-ld:Gateway:gateway_001", hostedMap["object"])
	}

	// Root unitCode must NOT be emitted (unitCode is Property metadata, not entity attribute)
	if _, ok := m["unitCode"]; ok {
		t.Errorf("root unitCode must not be emitted: %s", jsonStr)
	}
	if _, ok := m["measurementUnit"]; ok {
		t.Errorf("measurementUnit must be omitted for observed sensor with known unit: %s", jsonStr)
	}
	if _, ok := m["unit"]; ok {
		t.Errorf("old 'unit' property must not be present: %s", jsonStr)
	}

	// Verify latest observations from reported_state
	measMap, ok := m["measurement"].(map[string]any)
	if !ok {
		t.Fatalf("missing measurement property: %v", m["measurement"])
	}
	if measMap["type"] != "Property" || measMap["value"] != 25.4 {
		t.Errorf("unexpected measurement property: %v", measMap)
	}
	if measMap["observedAt"] != "2026-10-18T00:00:01.000000Z" {
		t.Errorf("measurement observedAt = %v, want 2026-10-18T00:00:01.000000Z", measMap["observedAt"])
	}
	if measMap["unitCode"] != "CEL" {
		t.Errorf("measurement unitCode = %v, want CEL", measMap["unitCode"])
	}

	intervalMap, ok := m["sample_interval_us"].(map[string]any)
	if !ok {
		t.Fatalf("missing sample_interval_us property: %v", m["sample_interval_us"])
	}
	if intervalMap["type"] != "Property" || intervalMap["value"] != float64(10000) {
		t.Errorf("unexpected sample_interval_us property: %v", intervalMap)
	}
	if intervalMap["observedAt"] != "2026-10-18T00:00:01.000000Z" {
		t.Errorf("sample_interval_us observedAt = %v", intervalMap["observedAt"])
	}

	// Assert desired state values are NEVER emitted as reported
	if measMap["value"] == 999.9 {
		t.Error("desired_state value was emitted as reported measurement!")
	}
	if intervalMap["value"] == float64(99999) {
		t.Error("desired_state sample_interval_us was emitted as reported interval!")
	}

	// Assert description, hasSensor, relationship_page are omitted
	if _, ok := m["description"]; ok {
		t.Errorf("Sensor detail must omit description: %s", jsonStr)
	}
	if _, ok := m["hasSensor"]; ok {
		t.Errorf("Sensor detail must omit hasSensor: %s", jsonStr)
	}
	if _, ok := m["relationship_page"]; ok {
		t.Errorf("Sensor detail must omit relationship_page: %s", jsonStr)
	}

	// Test Sensor with unknown unit -> measurementUnit property emitted, unitCode omitted
	resUnknownUnit := &digitaltwin.TwinDetailResult{
		ID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Type:      digitaltwin.EntityTypeSensor,
		Name:      "RawVoltage",
		Unit:      ptr("custom_unknown_unit"),
		GatewayID: "gateway_001",
		UpdatedAt: updatedAt,
	}
	dtoUnknownUnit, err := NewTwinDetailResponseDTO(resUnknownUnit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bUnknown, _ := json.Marshal(dtoUnknownUnit)
	var mUnknown map[string]any
	_ = json.Unmarshal(bUnknown, &mUnknown)
	measUnitMap, ok := mUnknown["measurementUnit"].(map[string]any)
	if !ok || measUnitMap["value"] != "custom_unknown_unit" {
		t.Errorf("expected measurementUnit 'custom_unknown_unit', got %v", mUnknown["measurementUnit"])
	}
	if _, ok := mUnknown["unitCode"]; ok {
		t.Errorf("unitCode must be omitted for unknown unit: %s", string(bUnknown))
	}

	// Test unobserved Sensor with known unit -> measurementUnit contains original stored unit 'degC', no root unitCode
	resUnobservedKnown := &digitaltwin.TwinDetailResult{
		ID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Type:      digitaltwin.EntityTypeSensor,
		Name:      "Temperature",
		Unit:      ptr("degC"),
		GatewayID: "gateway_001",
		UpdatedAt: updatedAt,
	}
	dtoUnobservedKnown, err := NewTwinDetailResponseDTO(resUnobservedKnown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bUnobservedKnown, _ := json.Marshal(dtoUnobservedKnown)
	var mUnobservedKnown map[string]any
	_ = json.Unmarshal(bUnobservedKnown, &mUnobservedKnown)
	if _, ok := mUnobservedKnown["unitCode"]; ok {
		t.Errorf("unobserved sensor must not have root unitCode: %s", string(bUnobservedKnown))
	}
	unobsMeasUnit, ok := mUnobservedKnown["measurementUnit"].(map[string]any)
	if !ok || unobsMeasUnit["value"] != "degC" {
		t.Errorf("unobserved sensor must expose original unit 'degC' under measurementUnit, got: %v", mUnobservedKnown["measurementUnit"])
	}
	if _, ok := mUnobservedKnown["measurement"]; ok {
		t.Errorf("unobserved sensor must not have measurement: %s", string(bUnobservedKnown))
	}

	// Test Sensor with null unit -> both unitCode and measurementUnit omitted
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
	if _, ok := mNull["unitCode"]; ok {
		t.Errorf("unitCode must be omitted when unit is nil: %s", string(bNullUnit))
	}
	if _, ok := mNull["measurementUnit"]; ok {
		t.Errorf("measurementUnit must be omitted when unit is nil: %s", string(bNullUnit))
	}

	// Test Sensor with large integer > 2^53 in reported_state value
	stateBigInt := &digitaltwin.TwinStateResult{
		ID:              "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Type:            digitaltwin.EntityTypeSensor,
		GatewayID:       "gateway_001",
		ReportedState:   json.RawMessage(`{"value":9007199254740993,"observed_at":"2026-10-18T00:00:01.000000Z","sample_interval_us":10000}`),
		ReportedVersion: 1,
		UpdatedAt:       updatedAt,
	}
	dtoBigInt, err := NewTwinDetailResponseDTO(res, stateBigInt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bBigInt, _ := json.Marshal(dtoBigInt)
	if !strings.Contains(string(bBigInt), `"value":9007199254740993`) {
		t.Errorf("large integer > 2^53 was rounded or distorted: %s", string(bBigInt))
	}

	// Test non-numeric observations: string/bool/object must not be emitted as numeric measurement
	stateInvalidObs := &digitaltwin.TwinStateResult{
		ID:              "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Type:            digitaltwin.EntityTypeSensor,
		GatewayID:       "gateway_001",
		ReportedState:   json.RawMessage(`{"value":"not-a-number","observed_at":"2026-10-18T00:00:01.000000Z","sample_interval_us":"bad"}`),
		ReportedVersion: 1,
		UpdatedAt:       updatedAt,
	}
	dtoInvalidObs, err := NewTwinDetailResponseDTO(res, stateInvalidObs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bInvalidObs, _ := json.Marshal(dtoInvalidObs)
	var mInvalidObs map[string]any
	_ = json.Unmarshal(bInvalidObs, &mInvalidObs)
	if _, ok := mInvalidObs["measurement"]; ok {
		t.Errorf("string value must not be emitted as measurement: %s", string(bInvalidObs))
	}
	if _, ok := mInvalidObs["sample_interval_us"]; ok {
		t.Errorf("string sample_interval_us must not be emitted: %s", string(bInvalidObs))
	}

	// Test negative sample_interval_us is omitted
	stateNegInterval := &digitaltwin.TwinStateResult{
		ID:              "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Type:            digitaltwin.EntityTypeSensor,
		GatewayID:       "gateway_001",
		ReportedState:   json.RawMessage(`{"value":25.4,"observed_at":"2026-10-18T00:00:01.000000Z","sample_interval_us":-100}`),
		ReportedVersion: 1,
		UpdatedAt:       updatedAt,
	}
	dtoNegInterval, err := NewTwinDetailResponseDTO(res, stateNegInterval)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bNegInterval, _ := json.Marshal(dtoNegInterval)
	var mNegInterval map[string]any
	_ = json.Unmarshal(bNegInterval, &mNegInterval)
	if _, ok := mNegInterval["sample_interval_us"]; ok {
		t.Errorf("negative sample_interval_us must be omitted: %s", string(bNegInterval))
	}

	// Test Sensor without observations in state -> measurement and sample_interval_us omitted
	stateEmpty := &digitaltwin.TwinStateResult{
		ID:              "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Type:            digitaltwin.EntityTypeSensor,
		GatewayID:       "gateway_001",
		ReportedState:   json.RawMessage(`{}`),
		ReportedVersion: 1,
		UpdatedAt:       updatedAt,
	}
	dtoNoObs, err := NewTwinDetailResponseDTO(res, stateEmpty)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bNoObs, _ := json.Marshal(dtoNoObs)
	var mNoObs map[string]any
	_ = json.Unmarshal(bNoObs, &mNoObs)
	if _, ok := mNoObs["measurement"]; ok {
		t.Errorf("measurement must be omitted when not observed: %s", string(bNoObs))
	}
	if _, ok := mNoObs["sample_interval_us"]; ok {
		t.Errorf("sample_interval_us must be omitted when not observed: %s", string(bNoObs))
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
				Name:      nil, // nil name should be omitted
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
	rawJSON := string(b)

	// Body must be a root array, not an object with items/next_cursor
	if strings.Contains(rawJSON, `"next_cursor"`) {
		t.Errorf("list body must not contain next_cursor: %s", rawJSON)
	}
	if strings.HasPrefix(rawJSON, "{") {
		t.Fatalf("list body must be a root array, got object: %s", rawJSON)
	}

	var itemsArr []map[string]any
	if err := json.Unmarshal(b, &itemsArr); err != nil {
		t.Fatalf("failed to unmarshal root array: %v (raw: %s)", err, rawJSON)
	}
	if len(itemsArr) != 2 {
		t.Fatalf("items = %v, want 2 items", len(itemsArr))
	}

	item0 := itemsArr[0]
	if item0["id"] != "urn:ngsi-ld:Sensor:gateway_001:sensor_001" {
		t.Errorf("item 0 id = %v", item0["id"])
	}
	if item0["type"] != "Sensor" {
		t.Errorf("item 0 type = %v", item0["type"])
	}
	if item0["modifiedAt"] != "2026-10-15T09:00:00.000000Z" {
		t.Errorf("item 0 modifiedAt = %v", item0["modifiedAt"])
	}

	// Verify name Property
	nameProp, ok := item0["name"].(map[string]any)
	if !ok {
		t.Fatalf("item 0 name is not Property object: %v", item0["name"])
	}
	if nameProp["type"] != "Property" || nameProp["value"] != "Temperature" {
		t.Errorf("unexpected name property: %v", nameProp)
	}

	// Verify gateway_id Property
	gwProp, ok := item0["gateway_id"].(map[string]any)
	if !ok {
		t.Fatalf("item 0 gateway_id is not Property object: %v", item0["gateway_id"])
	}
	if gwProp["type"] != "Property" || gwProp["value"] != "gateway_001" {
		t.Errorf("unexpected gateway_id property: %v", gwProp)
	}

	// Verify @context array
	ctxArr, ok := item0["@context"].([]any)
	if !ok || len(ctxArr) != 2 {
		t.Fatalf("item 0 @context must be array of 2 elements, got: %v", item0["@context"])
	}
	customTerms, ok := ctxArr[0].(map[string]any)
	if !ok {
		t.Fatalf("first element of @context must be custom terms map, got: %v", ctxArr[0])
	}
	if customTerms["Gateway"] != "https://example.org/ngsi-ld/iot/v1#Gateway" {
		t.Errorf("customTerms Gateway = %v", customTerms["Gateway"])
	}
	if customTerms["Sensor"] != "https://example.org/ngsi-ld/iot/v1#Sensor" {
		t.Errorf("customTerms Sensor = %v", customTerms["Sensor"])
	}
	if customTerms["gateway_id"] != "https://example.org/ngsi-ld/iot/v1#gatewayId" {
		t.Errorf("customTerms gateway_id = %v", customTerms["gateway_id"])
	}
	if ctxArr[1] != "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld" {
		t.Errorf("second element of @context must be core context URI, got: %v", ctxArr[1])
	}

	// Item 1 has nil name -> name property must be omitted
	item1 := itemsArr[1]
	if _, ok := item1["name"]; ok {
		t.Errorf("item 1 name must be omitted when nil: %v", item1["name"])
	}

	// Empty items array -> root [] (not null)
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
	if jsonEmpty != "[]" {
		t.Errorf("expected \"[]\", got %s", jsonEmpty)
	}
}

func TestTwinSimplifiedListResponseDTO(t *testing.T) {
	name := "Sensor 1"
	res := &digitaltwin.TwinListResult{Items: []digitaltwin.TwinSummary{
		{
			ID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
			Type:      digitaltwin.EntityTypeSensor,
			Name:      &name,
			GatewayID: "gateway_001",
			UpdatedAt: time.Date(2026, 10, 15, 16, 0, 0, 123000000, time.FixedZone("ICT", 7*60*60)),
		},
		{
			ID:        "urn:ngsi-ld:Gateway:gateway_001",
			Type:      digitaltwin.EntityTypeGateway,
			Name:      nil,
			GatewayID: "gateway_001",
			UpdatedAt: time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC),
		},
	}}

	dto, err := NewTwinSimplifiedListResponseDTO(res)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var items []map[string]any
	if err := json.Unmarshal(b, &items); err != nil {
		t.Fatalf("failed to unmarshal root array: %v (raw: %s)", err, string(b))
	}
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}

	item0 := items[0]
	if item0["id"] != "urn:ngsi-ld:Sensor:gateway_001:sensor_001" {
		t.Errorf("item 0 id = %v", item0["id"])
	}
	if item0["type"] != "Sensor" {
		t.Errorf("item 0 type = %v", item0["type"])
	}
	if item0["gateway_id"] != "gateway_001" {
		t.Errorf("item 0 gateway_id = %v, want flat string", item0["gateway_id"])
	}
	if item0["name"] != "Sensor 1" {
		t.Errorf("item 0 name = %v, want flat string", item0["name"])
	}
	if item0["modifiedAt"] != "2026-10-15T09:00:00.123000Z" {
		t.Errorf("item 0 modifiedAt = %v, want UTC microsecond", item0["modifiedAt"])
	}
	if _, ok := item0["@context"]; ok {
		t.Errorf("simplified item must not carry @context: %v", item0["@context"])
	}

	if _, ok := items[1]["name"]; ok {
		t.Errorf("nil name must be omitted, got: %v", items[1]["name"])
	}
	if len(items[1]) != 4 {
		t.Errorf("item 1 keys = %v, want 4 keys (id, type, gateway_id, modifiedAt)", items[1])
	}

	// Empty items -> root [] (not null)
	bEmpty, err := json.Marshal(mustSimplifiedDTO(t, &digitaltwin.TwinListResult{Items: nil}))
	if err != nil {
		t.Fatalf("failed to marshal empty: %v", err)
	}
	if string(bEmpty) != "[]" {
		t.Errorf("expected \"[]\", got %s", string(bEmpty))
	}

	// Validation is shared with the normalized projection.
	invalid := []struct {
		name string
		res  *digitaltwin.TwinListResult
	}{
		{name: "nil_result", res: nil},
		{
			name: "invalid_urn",
			res: &digitaltwin.TwinListResult{Items: []digitaltwin.TwinSummary{{
				ID: "not-a-urn", Type: digitaltwin.EntityTypeSensor, GatewayID: "gateway_001",
			}}},
		},
		{
			name: "type_mismatch",
			res: &digitaltwin.TwinListResult{Items: []digitaltwin.TwinSummary{{
				ID: "urn:ngsi-ld:Sensor:gateway_001:sensor_001", Type: digitaltwin.EntityTypeGateway, GatewayID: "gateway_001",
			}}},
		},
		{
			name: "gateway_mismatch",
			res: &digitaltwin.TwinListResult{Items: []digitaltwin.TwinSummary{{
				ID: "urn:ngsi-ld:Sensor:gateway_001:sensor_001", Type: digitaltwin.EntityTypeSensor, GatewayID: "gateway_002",
			}}},
		},
		{
			name: "invalid_timestamp",
			res: &digitaltwin.TwinListResult{Items: []digitaltwin.TwinSummary{{
				ID: "urn:ngsi-ld:Sensor:gateway_001:sensor_001", Type: digitaltwin.EntityTypeSensor, GatewayID: "gateway_001",
				UpdatedAt: time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC),
			}}},
		},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewTwinSimplifiedListResponseDTO(tc.res); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func mustSimplifiedDTO(t *testing.T, res *digitaltwin.TwinListResult) []TwinSummaryFlatDTO {
	t.Helper()
	dto, err := NewTwinSimplifiedListResponseDTO(res)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dto == nil {
		t.Fatal("simplified DTO must be non-nil so it marshals as [] not null")
	}
	return dto
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

	// Nullable last timestamps: last_reported_at present, last_desired_at nil -> omitted
	if !strings.Contains(jsonStr, `"last_reported_at":"2026-08-21T10:15:01.250000Z"`) {
		t.Errorf("last_reported_at formatting mismatch: %s", jsonStr)
	}
	if strings.Contains(jsonStr, "last_desired_at") {
		t.Errorf("last_desired_at must be omitted when nil, got: %s", jsonStr)
	}

	// Updated at
	if !strings.Contains(jsonStr, `"updated_at":"2026-10-15T09:30:00.000000Z"`) {
		t.Errorf("updated_at formatting mismatch: %s", jsonStr)
	}

	// State response must NOT contain @context
	if strings.Contains(jsonStr, "@context") {
		t.Errorf("state response must not contain @context: %s", jsonStr)
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

func TestTwinDetailResponseDTO_Sensor_UnitCodeTable(t *testing.T) {
	updatedAt := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	obsAt := "2026-10-18T00:00:01.000000Z"

	tests := []struct {
		name                string
		sensorUnit          *string
		hasObservation      bool
		obsValue            float64
		wantMeasurement     bool
		wantMeasurementVal  float64
		wantUnitCode        *string
		wantMeasurementUnit *string
	}{
		{
			name:                "rad with genuine observation maps to C81 and omits measurementUnit",
			sensorUnit:          ptr("rad"),
			hasObservation:      true,
			obsValue:            1.570796,
			wantMeasurement:     true,
			wantMeasurementVal:  1.570796,
			wantUnitCode:        ptr("C81"),
			wantMeasurementUnit: nil,
		},
		{
			name:                "degC with genuine observation maps to CEL and omits measurementUnit",
			sensorUnit:          ptr("degC"),
			hasObservation:      true,
			obsValue:            25.4,
			wantMeasurement:     true,
			wantMeasurementVal:  25.4,
			wantUnitCode:        ptr("CEL"),
			wantMeasurementUnit: nil,
		},
		{
			name:                "unknown unit with genuine observation omits unitCode and emits measurementUnit",
			sensorUnit:          ptr("custom_unknown_unit"),
			hasObservation:      true,
			obsValue:            42.0,
			wantMeasurement:     true,
			wantMeasurementVal:  42.0,
			wantUnitCode:        nil,
			wantMeasurementUnit: ptr("custom_unknown_unit"),
		},
		{
			name:                "nil unit with genuine observation omits both unitCode and measurementUnit",
			sensorUnit:          nil,
			hasObservation:      true,
			obsValue:            100.0,
			wantMeasurement:     true,
			wantMeasurementVal:  100.0,
			wantUnitCode:        nil,
			wantMeasurementUnit: nil,
		},
		{
			name:                "unobserved sensor with rad exposes measurementUnit rad and omits measurement",
			sensorUnit:          ptr("rad"),
			hasObservation:      false,
			wantMeasurement:     false,
			wantUnitCode:        nil,
			wantMeasurementUnit: ptr("rad"),
		},
		{
			name:                "unobserved sensor with unknown unit exposes measurementUnit and omits measurement",
			sensorUnit:          ptr("custom_unknown_unit"),
			hasObservation:      false,
			wantMeasurement:     false,
			wantUnitCode:        nil,
			wantMeasurementUnit: ptr("custom_unknown_unit"),
		},
		{
			name:                "unobserved sensor with nil unit omits both measurement and measurementUnit",
			sensorUnit:          nil,
			hasObservation:      false,
			wantMeasurement:     false,
			wantUnitCode:        nil,
			wantMeasurementUnit: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := &digitaltwin.TwinDetailResult{
				ID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
				Type:      digitaltwin.EntityTypeSensor,
				Name:      "TestSensor",
				Unit:      tt.sensorUnit,
				GatewayID: "gateway_001",
				UpdatedAt: updatedAt,
			}

			var stateRes []*digitaltwin.TwinStateResult
			if tt.hasObservation {
				stateRes = []*digitaltwin.TwinStateResult{
					{
						ID:              "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
						Type:            digitaltwin.EntityTypeSensor,
						GatewayID:       "gateway_001",
						ReportedState:   json.RawMessage(`{"value":` + strconv.FormatFloat(tt.obsValue, 'f', -1, 64) + `,"observed_at":"` + obsAt + `","sample_interval_us":10000.0}`),
						ReportedVersion: 1,
						UpdatedAt:       updatedAt,
					},
				}
			}

			dto, err := NewTwinDetailResponseDTO(res, stateRes...)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			b, err := json.Marshal(dto)
			if err != nil {
				t.Fatalf("failed to marshal DTO: %v", err)
			}

			var m map[string]any
			if err := json.Unmarshal(b, &m); err != nil {
				t.Fatalf("failed to unmarshal JSON: %v", err)
			}

			// Root unitCode must NEVER be present as entity attribute
			if _, ok := m["unitCode"]; ok {
				t.Errorf("root unitCode must not be emitted: %s", string(b))
			}

			// Measurement property assertions
			if tt.wantMeasurement {
				measMap, ok := m["measurement"].(map[string]any)
				if !ok {
					t.Fatalf("missing measurement property in: %s", string(b))
				}
				if measMap["type"] != "Property" {
					t.Errorf("measurement.type = %v, want Property", measMap["type"])
				}
				if measMap["observedAt"] != obsAt {
					t.Errorf("measurement.observedAt = %v, want %s", measMap["observedAt"], obsAt)
				}
				if measMap["value"] != tt.wantMeasurementVal {
					t.Errorf("measurement.value = %v, want %v", measMap["value"], tt.wantMeasurementVal)
				}

				if tt.wantUnitCode != nil {
					if measMap["unitCode"] != *tt.wantUnitCode {
						t.Errorf("measurement.unitCode = %v, want %v", measMap["unitCode"], *tt.wantUnitCode)
					}
				} else {
					if _, ok := measMap["unitCode"]; ok {
						t.Errorf("measurement.unitCode should be omitted, got %v", measMap["unitCode"])
					}
				}
			} else {
				if _, ok := m["measurement"]; ok {
					t.Errorf("measurement property should be omitted for unobserved sensor: %s", string(b))
				}
			}

			// MeasurementUnit property assertions
			if tt.wantMeasurementUnit != nil {
				measUnitMap, ok := m["measurementUnit"].(map[string]any)
				if !ok {
					t.Fatalf("missing measurementUnit property in: %s", string(b))
				}
				if measUnitMap["type"] != "Property" {
					t.Errorf("measurementUnit.type = %v, want Property", measUnitMap["type"])
				}
				if measUnitMap["value"] != *tt.wantMeasurementUnit {
					t.Errorf("measurementUnit.value = %v, want %v", measUnitMap["value"], *tt.wantMeasurementUnit)
				}
			} else {
				if _, ok := m["measurementUnit"]; ok {
					t.Errorf("measurementUnit should be omitted in: %s", string(b))
				}
			}
		})
	}
}

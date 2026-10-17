package mapper

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestProjectInlineContext(t *testing.T) {
	ctx := ProjectInlineContext()
	if ctx.Version != 1.1 {
		t.Fatalf("@version = %v, want 1.1", ctx.Version)
	}
	if ctx.ID != "@id" || ctx.Type != "@type" {
		t.Fatalf("id/type = %q/%q, want @id/@type", ctx.ID, ctx.Type)
	}
	if ctx.Property != "https://uri.etsi.org/ngsi-ld/Property" {
		t.Fatalf("Property = %q", ctx.Property)
	}
	if ctx.Relationship != "https://uri.etsi.org/ngsi-ld/Relationship" {
		t.Fatalf("Relationship = %q", ctx.Relationship)
	}
	if ctx.Value != "https://uri.etsi.org/ngsi-ld/hasValue" {
		t.Fatalf("value = %q", ctx.Value)
	}
	if ctx.ObservedAt != "https://uri.etsi.org/ngsi-ld/observedAt" {
		t.Fatalf("observedAt = %q", ctx.ObservedAt)
	}
	if ctx.CreatedAt != "https://uri.etsi.org/ngsi-ld/createdAt" {
		t.Fatalf("createdAt = %q", ctx.CreatedAt)
	}
	if ctx.ModifiedAt != "https://uri.etsi.org/ngsi-ld/modifiedAt" {
		t.Fatalf("modifiedAt = %q", ctx.ModifiedAt)
	}
	if ctx.Name != "https://example.org/ngsi-ld/iot/v1#name" {
		t.Fatalf("name = %q", ctx.Name)
	}
	if ctx.Description != "https://example.org/ngsi-ld/iot/v1#description" {
		t.Fatalf("description = %q", ctx.Description)
	}
	if ctx.Unit != "https://example.org/ngsi-ld/iot/v1#unit" {
		t.Fatalf("unit = %q", ctx.Unit)
	}
	if ctx.Measurement != "https://example.org/ngsi-ld/iot/v1#measurement" {
		t.Fatalf("measurement = %q", ctx.Measurement)
	}
	if ctx.SampleIntervalUs != "https://example.org/ngsi-ld/iot/v1#sampleIntervalUs" {
		t.Fatalf("sampleIntervalUs = %q", ctx.SampleIntervalUs)
	}
	if ctx.HasSensor != "https://example.org/ngsi-ld/iot/v1#hasSensor" {
		t.Fatalf("hasSensor = %q", ctx.HasSensor)
	}
	if ctx.GatewayID != "https://example.org/ngsi-ld/iot/v1#gatewayId" {
		t.Fatalf("gateway_id = %q", ctx.GatewayID)
	}

	data, err := json.Marshal(ctx)
	if err != nil {
		t.Fatalf("failed to marshal context: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("failed to unmarshal context JSON: %v", err)
	}

	expectedKeys := []string{
		"@version", "id", "type", "Property", "Relationship",
		"value", "observedAt", "createdAt", "modifiedAt",
		"name", "description", "unit", "measurement",
		"sampleIntervalUs", "hasSensor", "gateway_id",
	}
	for _, k := range expectedKeys {
		if _, ok := m[k]; !ok {
			t.Errorf("missing key %q in marshaled context", k)
		}
	}
}

func TestFormatRFC3339Micro_Valid(t *testing.T) {
	utcTime := time.Date(2026, 10, 15, 9, 30, 45, 123456000, time.UTC)
	s, err := FormatRFC3339Micro(utcTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := "2026-10-15T09:30:45.123456Z"; s != want {
		t.Fatalf("got %q, want %q", s, want)
	}

	// Non-UTC timezone converted to UTC
	loc := time.FixedZone("UTC+7", 7*3600)
	localTime := time.Date(2026, 10, 15, 16, 30, 45, 123456000, loc)
	s2, err := FormatRFC3339Micro(localTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := "2026-10-15T09:30:45.123456Z"; s2 != want {
		t.Fatalf("got %q, want %q", s2, want)
	}

	// Exact zero fractional digits
	zeroMicro := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	s3, err := FormatRFC3339Micro(zeroMicro)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := "2026-08-21T10:00:00.000000Z"; s3 != want {
		t.Fatalf("got %q, want %q", s3, want)
	}
}

func TestFormatRFC3339Micro_RejectInvalidYears(t *testing.T) {
	testCases := []struct {
		name string
		t    time.Time
	}{
		{"year zero", time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"negative year", time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"large negative year", time.Date(-2000, 5, 10, 0, 0, 0, 0, time.UTC)},
		{"year 10000", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"year 99999", time.Date(99999, 12, 31, 23, 59, 59, 0, time.UTC)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := FormatRFC3339Micro(tc.t)
			if err == nil {
				t.Fatalf("expected error for %s, got string %q", tc.name, s)
			}
			if err != ErrInvalidTimestampYear {
				t.Fatalf("got err %v, want %v", err, ErrInvalidTimestampYear)
			}
		})
	}
}

func TestPropertyDTO_JSON(t *testing.T) {
	// Standard property: observedAt omitted
	p1 := NewProperty("AM5728 Gateway 001", "2026-10-15T09:00:00.000000Z")
	b1, err := json.Marshal(p1)
	if err != nil {
		t.Fatalf("failed to marshal property: %v", err)
	}
	if strings.Contains(string(b1), "observedAt") {
		t.Fatalf("standard property must not contain observedAt, got %s", string(b1))
	}

	// Null value preserved
	p2 := NewProperty(nil, "2026-10-15T09:00:00.000000Z")
	b2, err := json.Marshal(p2)
	if err != nil {
		t.Fatalf("failed to marshal null property: %v", err)
	}
	if !strings.Contains(string(b2), `"value":null`) {
		t.Fatalf("null property must preserve null value, got %s", string(b2))
	}

	// Observation property: observedAt included
	p3 := NewObservationProperty(25.5, "2026-10-15T09:00:00.000000Z", "2026-10-15T08:59:59.900000Z")
	b3, err := json.Marshal(p3)
	if err != nil {
		t.Fatalf("failed to marshal observation property: %v", err)
	}
	if !strings.Contains(string(b3), `"observedAt":"2026-10-15T08:59:59.900000Z"`) {
		t.Fatalf("observation property must contain observedAt, got %s", string(b3))
	}
}

func TestRelationshipDTO_JSON(t *testing.T) {
	r := NewRelationship("urn:ngsi-ld:Sensor:gateway_001:sensor_001", "2026-10-15T08:30:00.000000Z")
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("failed to marshal relationship: %v", err)
	}

	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if m["type"] != "Relationship" {
		t.Errorf("type = %q, want Relationship", m["type"])
	}
	if m["object"] != "urn:ngsi-ld:Sensor:gateway_001:sensor_001" {
		t.Errorf("object = %q", m["object"])
	}
	if m["createdAt"] != "2026-10-15T08:30:00.000000Z" {
		t.Errorf("createdAt = %q", m["createdAt"])
	}
}

func TestRelationshipPageDTO_JSON(t *testing.T) {
	cursor := "v1.testcursor"
	page := RelationshipPageDTO{
		RelationshipType: "hasSensor",
		Limit:            10,
		Count:            2,
		Partial:          true,
		NextCursor:       &cursor,
	}

	b, err := json.Marshal(page)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}
	if !strings.Contains(string(b), `"next_cursor":"v1.testcursor"`) {
		t.Fatalf("expected next_cursor, got %s", string(b))
	}

	// Terminal page with null cursor
	pageTerm := RelationshipPageDTO{
		RelationshipType: "hasSensor",
		Limit:            10,
		Count:            2,
		Partial:          false,
		NextCursor:       nil,
	}
	bTerm, err := json.Marshal(pageTerm)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}
	if !strings.Contains(string(bTerm), `"next_cursor":null`) {
		t.Fatalf("expected next_cursor:null, got %s", string(bTerm))
	}
}

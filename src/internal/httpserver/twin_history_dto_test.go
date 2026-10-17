package httpserver_test

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/telemetry"
)

func TestNewTwinHistoryResponseDTO_DelegatedValueRaw(t *testing.T) {
	t.Parallel()

	bootID := uuid.MustParse("7f2c45f7-0a76-4e28-8a37-7793d7d85b04")
	msgID := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
	zeroIndex := int32(0)
	unit := "degC"

	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := time.Date(2026, 8, 21, 10, 15, 2, 0, time.UTC)
	t1 := time.Date(2026, 8, 21, 10, 15, 0, 500000000, time.UTC)
	t2 := time.Date(2026, 8, 21, 10, 15, 1, 250000000, time.UTC)

	telemRes := &telemetry.TelemetryHistoryResult{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		Unit:       &unit,
		From:       from,
		To:         to,
		Mode:       "raw",
		Resolution: "raw",
		PointCount: 2,
		RawPoints: []telemetry.RawSamplePoint{
			{
				MeasuredAt:  t1,
				Value:       25.1,
				SequenceNum: 12501,
				BootID:      &bootID,
				MessageID:   &msgID,
				SampleIndex: &zeroIndex,
			},
			{
				MeasuredAt:  t2,
				Value:       25.2,
				SequenceNum: 9007199254740993, // > 2^53
				BootID:      nil,
				MessageID:   nil,
				SampleIndex: nil,
			},
		},
	}

	result := &digitaltwin.TwinHistoryResult{
		EntityID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Property:        digitaltwin.SelectorValue,
		Unit:            &unit,
		From:            from,
		To:              to,
		Mode:            "raw",
		Resolution:      "raw",
		PointCount:      2,
		TelemetryResult: telemRes,
	}

	dto, err := httpserver.NewTwinHistoryResponseDTO(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("failed to marshal JSON: %v", err)
	}

	var rawMap map[string]any
	if err := json.Unmarshal(data, &rawMap); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	if rawMap["entity_id"] != "urn:ngsi-ld:Sensor:gateway_001:sensor_001" {
		t.Errorf("expected entity_id urn:ngsi-ld:Sensor:gateway_001:sensor_001, got %v", rawMap["entity_id"])
	}
	if rawMap["property"] != "value" {
		t.Errorf("expected property value, got %v", rawMap["property"])
	}
	if rawMap["unit"] != "degC" {
		t.Errorf("expected unit degC, got %v", rawMap["unit"])
	}
	if rawMap["from"] != "2026-08-21T10:15:00.000000Z" {
		t.Errorf("expected from 2026-08-21T10:15:00.000000Z, got %v", rawMap["from"])
	}
	if rawMap["to"] != "2026-08-21T10:15:02.000000Z" {
		t.Errorf("expected to 2026-08-21T10:15:02.000000Z, got %v", rawMap["to"])
	}
	if rawMap["mode"] != "raw" {
		t.Errorf("expected mode raw, got %v", rawMap["mode"])
	}
	if rawMap["resolution"] != "raw" {
		t.Errorf("expected resolution raw, got %v", rawMap["resolution"])
	}
	if rawMap["point_count"] != float64(2) {
		t.Errorf("expected point_count 2, got %v", rawMap["point_count"])
	}

	// Verify internal fields are omitted
	for _, badField := range []string{"gateway_id", "sensor_id", "telemetry_result", "temporal_points", "temporal_buckets", "raw_points", "bucket_points", "@context"} {
		if _, ok := rawMap[badField]; ok {
			t.Errorf("internal/forbidden field %q leaked in wire JSON", badField)
		}
	}

	pts, ok := rawMap["points"].([]any)
	if !ok || len(pts) != 2 {
		t.Fatalf("expected points array of len 2, got %v", rawMap["points"])
	}

	p0 := pts[0].(map[string]any)
	if p0["measured_at"] != "2026-08-21T10:15:00.500000Z" || p0["value"] != 25.1 || p0["sequence_num"] != "12501" ||
		p0["boot_id"] != "7f2c45f7-0a76-4e28-8a37-7793d7d85b04" || p0["message_id"] != "0195e18c-9fc1-7a42-9064-69ea49e63bf2" ||
		p0["sample_index"] != float64(0) {
		t.Errorf("p0 mismatch: %v", p0)
	}

	p1 := pts[1].(map[string]any)
	if p1["measured_at"] != "2026-08-21T10:15:01.250000Z" || p1["value"] != 25.2 || p1["sequence_num"] != "9007199254740993" ||
		p1["boot_id"] != nil || p1["message_id"] != nil || p1["sample_index"] != nil {
		t.Errorf("p1 legacy NULL / BIGINT mismatch: %v", p1)
	}

	// Verify typed getter
	rawPts, ok := dto.RawPoints()
	if !ok || len(rawPts) != 2 {
		t.Fatalf("expected typed RawPoints() len 2, got (%v, %v)", rawPts, ok)
	}
}

func TestNewTwinHistoryResponseDTO_DelegatedValueBucket(t *testing.T) {
	t.Parallel()

	unit := "degC"
	from := time.Date(2026, 8, 21, 10, 15, 0, 250000000, time.UTC)
	to := time.Date(2026, 8, 21, 10, 15, 3, 750000000, time.UTC)
	b0 := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	b1 := time.Date(2026, 8, 21, 10, 15, 1, 0, time.UTC)
	b2 := time.Date(2026, 8, 21, 10, 15, 3, 0, time.UTC)

	telemRes := &telemetry.TelemetryHistoryResult{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		Unit:       &unit,
		From:       from,
		To:         to,
		Mode:       "bucket",
		Resolution: "1s",
		PointCount: 3,
		BucketPoints: []telemetry.BucketSamplePoint{
			{BucketStart: b0, Avg: 25.15, Min: 25.1, Max: 25.2, Count: 100},
			{BucketStart: b1, Avg: 25.30, Min: 25.2, Max: 25.4, Count: 9007199254740993},
			{BucketStart: b2, Avg: 25.05, Min: 25.0, Max: 25.1, Count: 40},
		},
	}

	result := &digitaltwin.TwinHistoryResult{
		EntityID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Property:        digitaltwin.SelectorValue,
		Unit:            &unit,
		From:            from,
		To:              to,
		Mode:            "bucket",
		Resolution:      "1s",
		PointCount:      3,
		TelemetryResult: telemRes,
	}

	dto, err := httpserver.NewTwinHistoryResponseDTO(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var rawMap map[string]any
	if err := json.Unmarshal(data, &rawMap); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if rawMap["entity_id"] != "urn:ngsi-ld:Sensor:gateway_001:sensor_001" || rawMap["property"] != "value" ||
		rawMap["mode"] != "bucket" || rawMap["resolution"] != "1s" || rawMap["point_count"] != float64(3) {
		t.Errorf("envelope mismatch: %v", rawMap)
	}

	pts := rawMap["points"].([]any)
	p1 := pts[1].(map[string]any)
	if p1["count"] != "9007199254740993" {
		t.Errorf("expected count '9007199254740993', got %v", p1["count"])
	}

	bPts, ok := dto.BucketPoints()
	if !ok || len(bPts) != 3 {
		t.Fatalf("expected typed BucketPoints() len 3, got (%v, %v)", bPts, ok)
	}
}

func TestNewTwinHistoryResponseDTO_TemporalNumericRaw(t *testing.T) {
	t.Parallel()

	unit := "us"
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := time.Date(2026, 8, 21, 10, 15, 2, 0, time.UTC)
	obsTime := time.Date(2026, 8, 21, 10, 15, 1, 900000000, time.UTC)
	msgID := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")

	valNum := float64(10000)
	metaRaw := []byte(`{"boot_id":"7f2c45f7-0a76-4e28-8a37-7793d7d85b04","large_num":9007199254740993,"source":"telemetry_batch","unit":"us"}`)

	result := &digitaltwin.TwinHistoryResult{
		EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Property:   digitaltwin.SelectorSampleIntervalUS,
		Unit:       &unit,
		From:       from,
		To:         to,
		Mode:       "raw",
		Resolution: "raw",
		PointCount: 1,
		TemporalPoints: []digitaltwin.TemporalSamplePoint{
			{
				ObservedAt:   obsTime,
				ValueType:    "number",
				ValueNumber:  &valNum,
				ValueText:    nil,
				ValueBoolean: nil,
				Metadata:     metaRaw,
				MessageID:    &msgID,
			},
		},
	}

	dto, err := httpserver.NewTwinHistoryResponseDTO(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var rawMap map[string]any
	if err := json.Unmarshal(data, &rawMap); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if rawMap["entity_id"] != "urn:ngsi-ld:Sensor:gateway_001:sensor_001" {
		t.Errorf("entity_id mismatch: %v", rawMap["entity_id"])
	}
	if rawMap["property"] != "sample_interval_us" {
		t.Errorf("property mismatch: %v", rawMap["property"])
	}
	if rawMap["unit"] != "us" {
		t.Errorf("unit mismatch: %v", rawMap["unit"])
	}
	if rawMap["point_count"] != float64(1) {
		t.Errorf("point_count mismatch: %v", rawMap["point_count"])
	}

	pts := rawMap["points"].([]any)
	p0 := pts[0].(map[string]any)
	if p0["observed_at"] != "2026-08-21T10:15:01.900000Z" {
		t.Errorf("observed_at mismatch: %v", p0["observed_at"])
	}
	if p0["value_type"] != "number" {
		t.Errorf("value_type mismatch: %v", p0["value_type"])
	}
	if p0["value"] != float64(10000) {
		t.Errorf("value mismatch: %v", p0["value"])
	}
	if p0["message_id"] != "0195e18c-9fc1-7a42-9064-69ea49e63bf2" {
		t.Errorf("message_id mismatch: %v", p0["message_id"])
	}

	// Verify exact integer preservation in metadata without float64 precision loss
	jsonStr := string(data)
	if !strings.Contains(jsonStr, `"large_num":9007199254740993`) && !strings.Contains(jsonStr, `"large_num": 9007199254740993`) {
		t.Errorf("expected large integer 9007199254740993 preserved in metadata JSON, got %s", jsonStr)
	}

	tPts, ok := dto.TemporalRawPoints()
	if !ok || len(tPts) != 1 {
		t.Fatalf("expected typed TemporalRawPoints() len 1, got (%v, %v)", tPts, ok)
	}
}

func TestNewTwinHistoryResponseDTO_TemporalNumericBucket(t *testing.T) {
	t.Parallel()

	unit := "us"
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := time.Date(2026, 8, 21, 10, 15, 2, 0, time.UTC)
	b0 := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	b1 := time.Date(2026, 8, 21, 10, 15, 1, 0, time.UTC)

	result := &digitaltwin.TwinHistoryResult{
		EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Property:   digitaltwin.SelectorSampleIntervalUS,
		Unit:       &unit,
		From:       from,
		To:         to,
		Mode:       "bucket",
		Resolution: "1s",
		PointCount: 2,
		TemporalBuckets: []digitaltwin.TemporalBucketPoint{
			{BucketStart: b0, Avg: 10000, Min: 10000, Max: 10000, Count: 100},
			{BucketStart: b1, Avg: 20000, Min: 20000, Max: 20000, Count: 9007199254740993},
		},
	}

	dto, err := httpserver.NewTwinHistoryResponseDTO(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var rawMap map[string]any
	if err := json.Unmarshal(data, &rawMap); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if rawMap["mode"] != "bucket" || rawMap["resolution"] != "1s" || rawMap["point_count"] != float64(2) {
		t.Errorf("envelope mismatch: %v", rawMap)
	}

	pts := rawMap["points"].([]any)
	p0 := pts[0].(map[string]any)
	if p0["bucket_start"] != "2026-08-21T10:15:00.000000Z" || p0["avg"] != float64(10000) || p0["count"] != "100" {
		t.Errorf("p0 mismatch: %v", p0)
	}

	p1 := pts[1].(map[string]any)
	if p1["count"] != "9007199254740993" {
		t.Errorf("p1 count mismatch: %v", p1["count"])
	}
}

func TestNewTwinHistoryResponseDTO_TemporalTypedRaw_TextAndBooleanAndNullMetadata(t *testing.T) {
	t.Parallel()

	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := time.Date(2026, 8, 21, 10, 15, 2, 0, time.UTC)
	t0 := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	t1 := time.Date(2026, 8, 21, 10, 15, 0, 500000000, time.UTC)
	t2 := time.Date(2026, 8, 21, 10, 15, 1, 0, time.UTC)

	textVal := "auto"
	emptyTextVal := ""
	boolVal := false
	numVal := 12.5
	msgID := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")

	result := &digitaltwin.TwinHistoryResult{
		EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Property:   digitaltwin.HistorySelector("custom_reserved_prop"),
		Unit:       nil,
		From:       from,
		To:         to,
		Mode:       "raw",
		Resolution: "raw",
		PointCount: 4,
		TemporalPoints: []digitaltwin.TemporalSamplePoint{
			{
				ObservedAt: t0,
				ValueType:  "text",
				ValueText:  &textVal,
				Metadata:   []byte(`{}`),
				MessageID:  nil,
			},
			{
				ObservedAt:   t1,
				ValueType:    "boolean",
				ValueBoolean: &boolVal, // false preserved
				Metadata:     []byte(`{"unit":null}`),
				MessageID:    nil,
			},
			{
				ObservedAt: t1,
				ValueType:  "text",
				ValueText:  &emptyTextVal, // empty string preserved
				Metadata:   nil,           // nil normalized to {}
				MessageID:  nil,
			},
			{
				ObservedAt:  t2,
				ValueType:   "number",
				ValueNumber: &numVal,
				Metadata:    []byte(`{"unit":"degC"}`),
				MessageID:   &msgID,
			},
		},
	}

	dto, err := httpserver.NewTwinHistoryResponseDTO(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var rawMap map[string]any
	if err := json.Unmarshal(data, &rawMap); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if rawMap["unit"] != nil {
		t.Errorf("expected unit null, got %v", rawMap["unit"])
	}
	if rawMap["point_count"] != float64(4) {
		t.Errorf("expected point_count 4, got %v", rawMap["point_count"])
	}

	pts := rawMap["points"].([]any)
	p0 := pts[0].(map[string]any)
	if p0["value_type"] != "text" || p0["value"] != "auto" || p0["message_id"] != nil {
		t.Errorf("p0 mismatch: %v", p0)
	}

	p1 := pts[1].(map[string]any)
	if p1["value_type"] != "boolean" || p1["value"] != false || p1["message_id"] != nil {
		t.Errorf("p1 mismatch: %v", p1)
	}
	// Verify false is boolean false, not null or 0
	if _, ok := p1["value"].(bool); !ok {
		t.Errorf("p1 value must be boolean type, got %T", p1["value"])
	}

	p2 := pts[2].(map[string]any)
	if p2["value_type"] != "text" || p2["value"] != "" {
		t.Errorf("p2 empty string mismatch: %v", p2)
	}
	// Verify nil metadata normalized to {}
	meta2 := p2["metadata"].(map[string]any)
	if len(meta2) != 0 {
		t.Errorf("p2 metadata expected empty object {}, got %v", meta2)
	}

	p3 := pts[3].(map[string]any)
	if p3["value_type"] != "number" || p3["value"] != 12.5 || p3["message_id"] != "0195e18c-9fc1-7a42-9064-69ea49e63bf2" {
		t.Errorf("p3 mismatch: %v", p3)
	}
}

func TestNewTwinHistoryResponseDTO_AuthorizedEmpty(t *testing.T) {
	t.Parallel()

	from := time.Date(2026, 8, 21, 10, 20, 0, 0, time.UTC)
	to := time.Date(2026, 8, 21, 10, 20, 5, 0, time.UTC)

	// Temporal raw authorized empty with null unit
	resRaw := &digitaltwin.TwinHistoryResult{
		EntityID:       "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Property:       digitaltwin.SelectorSampleIntervalUS,
		Unit:           nil,
		From:           from,
		To:             to,
		Mode:           "raw",
		Resolution:     "raw",
		PointCount:     0,
		TemporalPoints: []digitaltwin.TemporalSamplePoint{},
	}

	dtoRaw, err := httpserver.NewTwinHistoryResponseDTO(resRaw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dataRaw, err := json.Marshal(dtoRaw)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	jsonStr := string(dataRaw)
	if !strings.Contains(jsonStr, `"unit":null`) && !strings.Contains(jsonStr, `"unit": null`) {
		t.Errorf("expected 'unit': null in JSON, got %s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"points":[]`) && !strings.Contains(jsonStr, `"points": []`) {
		t.Errorf("expected 'points': [] in JSON, got %s", jsonStr)
	}

	// Temporal bucket authorized empty with unit
	unit := "us"
	resBucket := &digitaltwin.TwinHistoryResult{
		EntityID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Property:        digitaltwin.SelectorSampleIntervalUS,
		Unit:            &unit,
		From:            from,
		To:              to,
		Mode:            "bucket",
		Resolution:      "1m",
		PointCount:      0,
		TemporalBuckets: []digitaltwin.TemporalBucketPoint{},
	}

	dtoBucket, err := httpserver.NewTwinHistoryResponseDTO(resBucket)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dataBucket, err := json.Marshal(dtoBucket)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	jsonBucketStr := string(dataBucket)
	if !strings.Contains(jsonBucketStr, `"unit":"us"`) && !strings.Contains(jsonBucketStr, `"unit": "us"`) {
		t.Errorf("expected 'unit': 'us' in JSON, got %s", jsonBucketStr)
	}
	if !strings.Contains(jsonBucketStr, `"points":[]`) && !strings.Contains(jsonBucketStr, `"points": []`) {
		t.Errorf("expected 'points': [] in JSON, got %s", jsonBucketStr)
	}
}

func TestNewTwinHistoryResponseDTO_TimestampNormalization(t *testing.T) {
	t.Parallel()

	// Non-UTC timezone (UTC+7)
	loc := time.FixedZone("UTC+7", 7*3600)
	fromLocal := time.Date(2026, 8, 21, 17, 15, 0, 0, loc) // 10:15:00Z
	toLocal := time.Date(2026, 8, 21, 17, 15, 2, 0, loc)   // 10:15:02Z
	obsLocal := time.Date(2026, 8, 21, 17, 15, 1, 500000000, loc)

	val := float64(5000)
	res := &digitaltwin.TwinHistoryResult{
		EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Property:   digitaltwin.SelectorSampleIntervalUS,
		From:       fromLocal,
		To:         toLocal,
		Mode:       "raw",
		Resolution: "raw",
		PointCount: 1,
		TemporalPoints: []digitaltwin.TemporalSamplePoint{
			{
				ObservedAt:  obsLocal,
				ValueType:   "number",
				ValueNumber: &val,
				Metadata:    []byte(`{}`),
			},
		},
	}

	dto, err := httpserver.NewTwinHistoryResponseDTO(res)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if dto.From != "2026-08-21T10:15:00.000000Z" {
		t.Errorf("expected from normalized to UTC, got %v", dto.From)
	}
	if dto.To != "2026-08-21T10:15:02.000000Z" {
		t.Errorf("expected to normalized to UTC, got %v", dto.To)
	}
	pts, ok := dto.TemporalRawPoints()
	if !ok || pts[0].ObservedAt != "2026-08-21T10:15:01.500000Z" {
		t.Errorf("expected observed_at normalized to UTC, got %v", pts[0].ObservedAt)
	}
}

func TestNewTwinHistoryResponseDTO_ValidationAndConsistencyErrors(t *testing.T) {
	t.Parallel()

	validFrom := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	validTo := time.Date(2026, 8, 21, 10, 15, 2, 0, time.UTC)
	valNum := 10.0

	tests := []struct {
		name    string
		mutate  func() *digitaltwin.TwinHistoryResult
		wantErr string
	}{
		{
			name: "nil result",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return nil
			},
			wantErr: "must not be nil",
		},
		{
			name: "invalid entity_id URN",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID: "invalid-urn",
					Property: digitaltwin.SelectorSampleIntervalUS,
					From:     validFrom,
					To:       validTo,
					Mode:     "raw",
				}
			},
			wantErr: "invalid entity id",
		},
		{
			name: "gateway entity URN rejected for history",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID: "urn:ngsi-ld:Gateway:gateway_001",
					Property: digitaltwin.SelectorSampleIntervalUS,
					From:     validFrom,
					To:       validTo,
					Mode:     "raw",
				}
			},
			wantErr: "Sensor",
		},
		{
			name: "empty property",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID: "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property: "",
					From:     validFrom,
					To:       validTo,
					Mode:     "raw",
				}
			},
			wantErr: "property",
		},
		{
			name: "from not before to",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID: "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property: digitaltwin.SelectorSampleIntervalUS,
					From:     validTo,
					To:       validFrom,
					Mode:     "raw",
				}
			},
			wantErr: "before",
		},
		{
			name: "auto reported as effective resolution",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property:   digitaltwin.SelectorSampleIntervalUS,
					From:       validFrom,
					To:         validTo,
					Mode:       "raw",
					Resolution: "auto",
				}
			},
			wantErr: "auto",
		},
		{
			name: "negative point_count",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property:   digitaltwin.SelectorSampleIntervalUS,
					From:       validFrom,
					To:         validTo,
					Mode:       "raw",
					Resolution: "raw",
					PointCount: -1,
				}
			},
			wantErr: "negative",
		},
		{
			name: "delegated value with nil TelemetryResult",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property:        digitaltwin.SelectorValue,
					From:            validFrom,
					To:              validTo,
					Mode:            "raw",
					Resolution:      "raw",
					TelemetryResult: nil,
				}
			},
			wantErr: "delegated",
		},
		{
			name: "delegated value with conflicting entity URN vs TelemetryResult",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property:   digitaltwin.SelectorValue,
					From:       validFrom,
					To:         validTo,
					Mode:       "raw",
					Resolution: "raw",
					TelemetryResult: &telemetry.TelemetryHistoryResult{
						GatewayID:  "other_gateway",
						SensorID:   "sensor_001",
						From:       validFrom,
						To:         validTo,
						Mode:       "raw",
						Resolution: "raw",
					},
				}
			},
			wantErr: "match",
		},
		{
			name: "delegated value with active temporal points branch",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property:   digitaltwin.SelectorValue,
					From:       validFrom,
					To:         validTo,
					Mode:       "raw",
					Resolution: "raw",
					TelemetryResult: &telemetry.TelemetryHistoryResult{
						GatewayID:  "gateway_001",
						SensorID:   "sensor_001",
						From:       validFrom,
						To:         validTo,
						Mode:       "raw",
						Resolution: "raw",
					},
					TemporalPoints: []digitaltwin.TemporalSamplePoint{
						{ObservedAt: validFrom, ValueType: "number", ValueNumber: &valNum},
					},
				}
			},
			wantErr: "branch",
		},
		{
			name: "temporal raw point_count mismatch",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID:       "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property:       digitaltwin.SelectorSampleIntervalUS,
					From:           validFrom,
					To:             validTo,
					Mode:           "raw",
					Resolution:     "raw",
					PointCount:     2,
					TemporalPoints: []digitaltwin.TemporalSamplePoint{},
				}
			},
			wantErr: "point_count",
		},
		{
			name: "temporal raw with bucket points present",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property:        digitaltwin.SelectorSampleIntervalUS,
					From:            validFrom,
					To:              validTo,
					Mode:            "raw",
					Resolution:      "raw",
					PointCount:      0,
					TemporalBuckets: []digitaltwin.TemporalBucketPoint{{BucketStart: validFrom, Avg: 1, Min: 1, Max: 1, Count: 1}},
				}
			},
			wantErr: "bucket points",
		},
		{
			name: "temporal bucket with raw points present",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID:       "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property:       digitaltwin.SelectorSampleIntervalUS,
					From:           validFrom,
					To:             validTo,
					Mode:           "bucket",
					Resolution:     "1s",
					PointCount:     0,
					TemporalPoints: []digitaltwin.TemporalSamplePoint{{ObservedAt: validFrom, ValueType: "number", ValueNumber: &valNum}},
				}
			},
			wantErr: "raw points",
		},
		{
			name: "temporal point missing value for number type",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property:   digitaltwin.SelectorSampleIntervalUS,
					From:       validFrom,
					To:         validTo,
					Mode:       "raw",
					Resolution: "raw",
					PointCount: 1,
					TemporalPoints: []digitaltwin.TemporalSamplePoint{
						{ObservedAt: validFrom, ValueType: "number", ValueNumber: nil},
					},
				}
			},
			wantErr: "ValueNumber",
		},
		{
			name: "temporal point with multiple non-null value columns",
			mutate: func() *digitaltwin.TwinHistoryResult {
				strVal := "abc"
				return &digitaltwin.TwinHistoryResult{
					EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property:   digitaltwin.SelectorSampleIntervalUS,
					From:       validFrom,
					To:         validTo,
					Mode:       "raw",
					Resolution: "raw",
					PointCount: 1,
					TemporalPoints: []digitaltwin.TemporalSamplePoint{
						{ObservedAt: validFrom, ValueType: "number", ValueNumber: &valNum, ValueText: &strVal},
					},
				}
			},
			wantErr: "exactly one",
		},
		{
			name: "temporal point with NaN value",
			mutate: func() *digitaltwin.TwinHistoryResult {
				nan := math.NaN()
				return &digitaltwin.TwinHistoryResult{
					EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property:   digitaltwin.SelectorSampleIntervalUS,
					From:       validFrom,
					To:         validTo,
					Mode:       "raw",
					Resolution: "raw",
					PointCount: 1,
					TemporalPoints: []digitaltwin.TemporalSamplePoint{
						{ObservedAt: validFrom, ValueType: "number", ValueNumber: &nan},
					},
				}
			},
			wantErr: "finite",
		},
		{
			name: "temporal point metadata is array not object",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property:   digitaltwin.SelectorSampleIntervalUS,
					From:       validFrom,
					To:         validTo,
					Mode:       "raw",
					Resolution: "raw",
					PointCount: 1,
					TemporalPoints: []digitaltwin.TemporalSamplePoint{
						{ObservedAt: validFrom, ValueType: "number", ValueNumber: &valNum, Metadata: []byte(`[1,2,3]`)},
					},
				}
			},
			wantErr: "JSON object",
		},
		{
			name: "temporal bucket with negative count",
			mutate: func() *digitaltwin.TwinHistoryResult {
				return &digitaltwin.TwinHistoryResult{
					EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
					Property:   digitaltwin.SelectorSampleIntervalUS,
					From:       validFrom,
					To:         validTo,
					Mode:       "bucket",
					Resolution: "1s",
					PointCount: 1,
					TemporalBuckets: []digitaltwin.TemporalBucketPoint{
						{BucketStart: validFrom, Avg: 1, Min: 1, Max: 1, Count: -5},
					},
				}
			},
			wantErr: "negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			res := tt.mutate()
			_, err := httpserver.NewTwinHistoryResponseDTO(res)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

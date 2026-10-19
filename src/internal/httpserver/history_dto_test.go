package httpserver_test

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/httpserver"
	"iot-platform/internal/telemetry"
)

func TestNewTelemetryHistoryResponseDTO_RawNormalAndLegacy(t *testing.T) {
	t.Parallel()

	bootID := uuid.MustParse("7f2c45f7-0a76-4e28-8a37-7793d7d85b04")
	msgID := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
	zeroIndex := int32(0)
	unit := "degC"

	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := time.Date(2026, 8, 21, 10, 15, 2, 0, time.UTC)
	t1 := time.Date(2026, 8, 21, 10, 15, 0, 500000000, time.UTC)
	t2 := time.Date(2026, 8, 21, 10, 15, 1, 250000000, time.UTC)

	result := &telemetry.TelemetryHistoryResult{
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

	dto, err := httpserver.NewTelemetryHistoryResponseDTO(result)
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

	// Verify top-level fields
	if rawMap["gateway_id"] != "gateway_001" {
		t.Errorf("expected gateway_id gateway_001, got %v", rawMap["gateway_id"])
	}
	if rawMap["sensor_id"] != "sensor_001" {
		t.Errorf("expected sensor_id sensor_001, got %v", rawMap["sensor_id"])
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

	// Internal fields must not leak
	if _, ok := rawMap["raw_points"]; ok {
		t.Error("raw_points leaked in JSON")
	}
	if _, ok := rawMap["bucket_points"]; ok {
		t.Error("bucket_points leaked in JSON")
	}

	points, ok := rawMap["points"].([]any)
	if !ok || len(points) != 2 {
		t.Fatalf("expected points array of len 2, got %v", rawMap["points"])
	}

	// Point 0 checks
	p0 := points[0].(map[string]any)
	if p0["measured_at"] != "2026-08-21T10:15:00.500000Z" {
		t.Errorf("p0 measured_at mismatch: %v", p0["measured_at"])
	}
	if p0["value"] != 25.1 {
		t.Errorf("p0 value mismatch: %v", p0["value"])
	}
	if p0["sequence_num"] != "12501" {
		t.Errorf("p0 sequence_num must be string '12501', got %v", p0["sequence_num"])
	}
	if p0["boot_id"] != "7f2c45f7-0a76-4e28-8a37-7793d7d85b04" {
		t.Errorf("p0 boot_id mismatch: %v", p0["boot_id"])
	}
	if p0["message_id"] != "0195e18c-9fc1-7a42-9064-69ea49e63bf2" {
		t.Errorf("p0 message_id mismatch: %v", p0["message_id"])
	}
	if p0["sample_index"] != float64(0) {
		t.Errorf("p0 sample_index must be numeric 0, got %v", p0["sample_index"])
	}

	// Point 1 checks (legacy NULLs and BIGINT > 2^53)
	p1 := points[1].(map[string]any)
	if p1["measured_at"] != "2026-08-21T10:15:01.250000Z" {
		t.Errorf("p1 measured_at mismatch: %v", p1["measured_at"])
	}
	if p1["value"] != 25.2 {
		t.Errorf("p1 value mismatch: %v", p1["value"])
	}
	if p1["sequence_num"] != "9007199254740993" {
		t.Errorf("p1 sequence_num must be decimal string '9007199254740993', got %v", p1["sequence_num"])
	}
	if p1["boot_id"] != nil {
		t.Errorf("p1 boot_id must be null, got %v", p1["boot_id"])
	}
	if p1["message_id"] != nil {
		t.Errorf("p1 message_id must be null, got %v", p1["message_id"])
	}
	if p1["sample_index"] != nil {
		t.Errorf("p1 sample_index must be null, got %v", p1["sample_index"])
	}
}

func TestNewTelemetryHistoryResponseDTO_BucketNormal(t *testing.T) {
	t.Parallel()

	unit := "degC"
	from := time.Date(2026, 8, 21, 10, 15, 0, 250000000, time.UTC)
	to := time.Date(2026, 8, 21, 10, 15, 3, 750000000, time.UTC)

	b0 := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	b1 := time.Date(2026, 8, 21, 10, 15, 1, 0, time.UTC)
	b2 := time.Date(2026, 8, 21, 10, 15, 3, 0, time.UTC)

	result := &telemetry.TelemetryHistoryResult{
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
			{BucketStart: b1, Avg: 25.30, Min: 25.2, Max: 25.4, Count: 9007199254740993}, // > 2^53 count
			{BucketStart: b2, Avg: 25.05, Min: 25.0, Max: 25.1, Count: 40},
		},
	}

	dto, err := httpserver.NewTelemetryHistoryResponseDTO(result)
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

	if rawMap["mode"] != "bucket" {
		t.Errorf("expected mode bucket, got %v", rawMap["mode"])
	}
	if rawMap["resolution"] != "1s" {
		t.Errorf("expected resolution 1s, got %v", rawMap["resolution"])
	}
	if rawMap["point_count"] != float64(3) {
		t.Errorf("expected point_count 3, got %v", rawMap["point_count"])
	}

	points, ok := rawMap["points"].([]any)
	if !ok || len(points) != 3 {
		t.Fatalf("expected points len 3, got %v", rawMap["points"])
	}

	p0 := points[0].(map[string]any)
	if p0["bucket_start"] != "2026-08-21T10:15:00.000000Z" {
		t.Errorf("p0 bucket_start mismatch: %v", p0["bucket_start"])
	}
	if p0["avg"] != 25.15 || p0["min"] != 25.1 || p0["max"] != 25.2 {
		t.Errorf("p0 avg/min/max mismatch: %v", p0)
	}
	if p0["count"] != "100" {
		t.Errorf("p0 count must be decimal string '100', got %v", p0["count"])
	}

	p1 := points[1].(map[string]any)
	if p1["count"] != "9007199254740993" {
		t.Errorf("p1 count must be decimal string '9007199254740993', got %v", p1["count"])
	}
}

func TestNewTelemetryHistoryResponseDTO_AuthorizedEmpty(t *testing.T) {
	t.Parallel()

	from := time.Date(2026, 8, 21, 10, 20, 0, 0, time.UTC)
	to := time.Date(2026, 8, 21, 10, 20, 5, 0, time.UTC)

	// Raw authorized empty with null unit
	resRaw := &telemetry.TelemetryHistoryResult{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		Unit:       nil,
		From:       from,
		To:         to,
		Mode:       "raw",
		Resolution: "raw",
		PointCount: 0,
		RawPoints:  []telemetry.RawSamplePoint{},
	}

	dtoRaw, err := httpserver.NewTelemetryHistoryResponseDTO(resRaw)
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

	// Bucket authorized empty with unit
	unit := "degC"
	resBucket := &telemetry.TelemetryHistoryResult{
		GatewayID:    "gateway_001",
		SensorID:     "sensor_001",
		Unit:         &unit,
		From:         from,
		To:           to,
		Mode:         "bucket",
		Resolution:   "1m",
		PointCount:   0,
		BucketPoints: []telemetry.BucketSamplePoint{},
	}

	dtoBucket, err := httpserver.NewTelemetryHistoryResponseDTO(resBucket)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dataBucket, err := json.Marshal(dtoBucket)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	jsonBucketStr := string(dataBucket)
	if !strings.Contains(jsonBucketStr, `"points":[]`) && !strings.Contains(jsonBucketStr, `"points": []`) {
		t.Errorf("expected 'points': [] in JSON, got %s", jsonBucketStr)
	}
}

func TestNewTelemetryHistoryResponseDTO_TimezoneNormalization(t *testing.T) {
	t.Parallel()

	loc := time.FixedZone("UTC+7", 7*3600)
	from := time.Date(2026, 8, 21, 17, 15, 0, 0, loc) // 10:15:00 UTC
	to := time.Date(2026, 8, 21, 17, 15, 2, 0, loc)   // 10:15:02 UTC
	t1 := time.Date(2026, 8, 21, 17, 15, 0, 500000000, loc)

	result := &telemetry.TelemetryHistoryResult{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		From:       from,
		To:         to,
		Mode:       "raw",
		Resolution: "raw",
		PointCount: 1,
		RawPoints: []telemetry.RawSamplePoint{
			{
				MeasuredAt:  t1,
				Value:       10.0,
				SequenceNum: 1,
			},
		},
	}

	dto, err := httpserver.NewTelemetryHistoryResponseDTO(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if dto.From != "2026-08-21T10:15:00.000000Z" {
		t.Errorf("expected normalized from 2026-08-21T10:15:00.000000Z, got %s", dto.From)
	}
	if dto.To != "2026-08-21T10:15:02.000000Z" {
		t.Errorf("expected normalized to 2026-08-21T10:15:02.000000Z, got %s", dto.To)
	}

	rawPts, ok := dto.RawPoints()
	if !ok || len(rawPts) != 1 {
		t.Fatalf("expected 1 raw point")
	}
	if rawPts[0].MeasuredAt != "2026-08-21T10:15:00.500000Z" {
		t.Errorf("expected normalized measured_at 2026-08-21T10:15:00.500000Z, got %s", rawPts[0].MeasuredAt)
	}
}

func TestNewTelemetryHistoryResponseDTO_UUIDZeroVsNil(t *testing.T) {
	t.Parallel()

	zeroUUID := uuid.Nil
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := time.Date(2026, 8, 21, 10, 15, 2, 0, time.UTC)

	result := &telemetry.TelemetryHistoryResult{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		From:       from,
		To:         to,
		Mode:       "raw",
		Resolution: "raw",
		PointCount: 1,
		RawPoints: []telemetry.RawSamplePoint{
			{
				MeasuredAt:  from,
				Value:       1.0,
				SequenceNum: 100,
				BootID:      &zeroUUID,
				MessageID:   nil,
			},
		},
	}

	dto, err := httpserver.NewTelemetryHistoryResponseDTO(result)
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

	p := rawMap["points"].([]any)[0].(map[string]any)
	if p["boot_id"] != "00000000-0000-0000-0000-000000000000" {
		t.Errorf("expected string zero UUID, got %v", p["boot_id"])
	}
	if p["message_id"] != nil {
		t.Errorf("expected null message_id, got %v", p["message_id"])
	}
}

func TestNewTelemetryHistoryResponseDTO_ValidationErrors(t *testing.T) {
	t.Parallel()

	validFrom := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	validTo := time.Date(2026, 8, 21, 10, 5, 0, 0, time.UTC)

	cases := []struct {
		name   string
		result *telemetry.TelemetryHistoryResult
	}{
		{
			name:   "nil result",
			result: nil,
		},
		{
			name: "empty gateway_id",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         validTo,
				Mode:       "raw",
				Resolution: "raw",
			},
		},
		{
			name: "invalid gateway_id",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "bad gateway ID",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         validTo,
				Mode:       "raw",
				Resolution: "raw",
			},
		},
		{
			name: "invalid sensor_id",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "bad sensor ID",
				From:       validFrom,
				To:         validTo,
				Mode:       "raw",
				Resolution: "raw",
			},
		},
		{
			name: "from year underflow",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       time.Date(0, 12, 31, 23, 59, 59, 0, time.UTC),
				To:         validTo,
				Mode:       "raw",
				Resolution: "raw",
			},
		},
		{
			name: "to year overflow",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC),
				Mode:       "raw",
				Resolution: "raw",
			},
		},
		{
			name: "raw point year underflow",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         validTo,
				Mode:       "raw",
				Resolution: "raw",
				PointCount: 1,
				RawPoints: []telemetry.RawSamplePoint{
					{MeasuredAt: time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), Value: 1, SequenceNum: 1},
				},
			},
		},
		{
			name: "bucket point year underflow",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         validTo,
				Mode:       "bucket",
				Resolution: "1s",
				PointCount: 1,
				BucketPoints: []telemetry.BucketSamplePoint{
					{BucketStart: time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), Avg: 1, Min: 1, Max: 1, Count: 1},
				},
			},
		},
		{
			name: "from not before to",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       validTo,
				To:         validFrom,
				Mode:       "raw",
				Resolution: "raw",
			},
		},
		{
			name: "unsupported mode auto",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         validTo,
				Mode:       "auto",
				Resolution: "1s",
			},
		},
		{
			name: "unsupported resolution auto",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         validTo,
				Mode:       "bucket",
				Resolution: "auto",
			},
		},
		{
			name: "raw mode with non-raw resolution",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         validTo,
				Mode:       "raw",
				Resolution: "1s",
			},
		},
		{
			name: "bucket mode with raw resolution",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         validTo,
				Mode:       "bucket",
				Resolution: "raw",
			},
		},
		{
			name: "bucket mode with unknown resolution",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         validTo,
				Mode:       "bucket",
				Resolution: "unknown_res",
			},
		},
		{
			name: "raw mode with leaked bucket points",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         validTo,
				Mode:       "raw",
				Resolution: "raw",
				PointCount: 0,
				RawPoints:  []telemetry.RawSamplePoint{},
				BucketPoints: []telemetry.BucketSamplePoint{
					{BucketStart: validFrom, Avg: 1, Min: 1, Max: 1, Count: 1},
				},
			},
		},
		{
			name: "raw mode count mismatch",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         validTo,
				Mode:       "raw",
				Resolution: "raw",
				PointCount: 1,
				RawPoints:  []telemetry.RawSamplePoint{},
			},
		},
		{
			name: "bucket mode with leaked raw points",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:    "gateway_001",
				SensorID:     "sensor_001",
				From:         validFrom,
				To:           validTo,
				Mode:         "bucket",
				Resolution:   "1s",
				PointCount:   0,
				BucketPoints: []telemetry.BucketSamplePoint{},
				RawPoints: []telemetry.RawSamplePoint{
					{MeasuredAt: validFrom, Value: 1, SequenceNum: 1},
				},
			},
		},
		{
			name: "bucket mode count mismatch",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:    "gateway_001",
				SensorID:     "sensor_001",
				From:         validFrom,
				To:           validTo,
				Mode:         "bucket",
				Resolution:   "1s",
				PointCount:   5,
				BucketPoints: []telemetry.BucketSamplePoint{},
			},
		},
		{
			name: "raw point with NaN value",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         validTo,
				Mode:       "raw",
				Resolution: "raw",
				PointCount: 1,
				RawPoints: []telemetry.RawSamplePoint{
					{MeasuredAt: validFrom, Value: math.NaN(), SequenceNum: 1},
				},
			},
		},
		{
			name: "raw point with Inf value",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         validTo,
				Mode:       "raw",
				Resolution: "raw",
				PointCount: 1,
				RawPoints: []telemetry.RawSamplePoint{
					{MeasuredAt: validFrom, Value: math.Inf(1), SequenceNum: 1},
				},
			},
		},
		{
			name: "bucket point with negative count",
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				From:       validFrom,
				To:         validTo,
				Mode:       "bucket",
				Resolution: "1s",
				PointCount: 1,
				BucketPoints: []telemetry.BucketSamplePoint{
					{BucketStart: validFrom, Avg: 1, Min: 1, Max: 1, Count: -1},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := httpserver.NewTelemetryHistoryResponseDTO(tc.result)
			if err == nil {
				t.Fatalf("expected error for case %q, got nil", tc.name)
			}
		})
	}
}

func TestNewTelemetryHistoryResponseDTO_MicrosecondsAndBIGINT(t *testing.T) {
	t.Parallel()

	from := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	to := time.Date(2026, 8, 21, 10, 1, 0, 0, time.UTC)

	// Test trailing zero preservation in fractional seconds
	t000000 := time.Date(2026, 8, 21, 10, 0, 1, 0, time.UTC)
	t100000 := time.Date(2026, 8, 21, 10, 0, 2, 100_000_000, time.UTC)
	t120000 := time.Date(2026, 8, 21, 10, 0, 3, 120_000_000, time.UTC)
	t123450 := time.Date(2026, 8, 21, 10, 0, 4, 123_450_000, time.UTC)
	t123456 := time.Date(2026, 8, 21, 10, 0, 5, 123_456_000, time.UTC)

	idx0 := int32(0)
	idx999 := int32(999)

	resRaw := &telemetry.TelemetryHistoryResult{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		From:       from,
		To:         to,
		Mode:       "raw",
		Resolution: "raw",
		PointCount: 5,
		RawPoints: []telemetry.RawSamplePoint{
			{MeasuredAt: t000000, Value: 1.0, SequenceNum: 0, SampleIndex: &idx0},
			{MeasuredAt: t100000, Value: 2.0, SequenceNum: -1, SampleIndex: &idx999},
			{MeasuredAt: t120000, Value: 3.0, SequenceNum: 9007199254740993}, // 2^53 + 1
			{MeasuredAt: t123450, Value: 4.0, SequenceNum: math.MaxInt64},
			{MeasuredAt: t123456, Value: 5.0, SequenceNum: math.MinInt64},
		},
	}

	dtoRaw, err := httpserver.NewTelemetryHistoryResponseDTO(resRaw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pts, ok := dtoRaw.RawPoints()
	if !ok || len(pts) != 5 {
		t.Fatalf("expected 5 raw points")
	}
	if pts[0].MeasuredAt != "2026-08-21T10:00:01.000000Z" {
		t.Errorf("expected 000000 trailing zeros, got %s", pts[0].MeasuredAt)
	}
	if pts[1].MeasuredAt != "2026-08-21T10:00:02.100000Z" {
		t.Errorf("expected 100000 trailing zeros, got %s", pts[1].MeasuredAt)
	}
	if pts[2].MeasuredAt != "2026-08-21T10:00:03.120000Z" {
		t.Errorf("expected 120000 trailing zeros, got %s", pts[2].MeasuredAt)
	}
	if pts[3].MeasuredAt != "2026-08-21T10:00:04.123450Z" {
		t.Errorf("expected 123450 trailing zeros, got %s", pts[3].MeasuredAt)
	}
	if pts[4].MeasuredAt != "2026-08-21T10:00:05.123456Z" {
		t.Errorf("expected 123456, got %s", pts[4].MeasuredAt)
	}

	if pts[0].SequenceNum != "0" {
		t.Errorf("expected sequence_num '0', got %s", pts[0].SequenceNum)
	}
	if pts[1].SequenceNum != "-1" {
		t.Errorf("expected sequence_num '-1', got %s", pts[1].SequenceNum)
	}
	if pts[2].SequenceNum != "9007199254740993" {
		t.Errorf("expected sequence_num '9007199254740993', got %s", pts[2].SequenceNum)
	}
	if pts[3].SequenceNum != "9223372036854775807" {
		t.Errorf("expected sequence_num MaxInt64, got %s", pts[3].SequenceNum)
	}
	if pts[4].SequenceNum != "-9223372036854775808" {
		t.Errorf("expected sequence_num MinInt64, got %s", pts[4].SequenceNum)
	}

	// Bucket BIGINT count testing
	resBucket := &telemetry.TelemetryHistoryResult{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		From:       from,
		To:         to,
		Mode:       "bucket",
		Resolution: "10s",
		PointCount: 3,
		BucketPoints: []telemetry.BucketSamplePoint{
			{BucketStart: from, Avg: 1, Min: 1, Max: 1, Count: 0},
			{BucketStart: from.Add(10 * time.Second), Avg: 2, Min: 2, Max: 2, Count: 9007199254740993},
			{BucketStart: from.Add(20 * time.Second), Avg: 3, Min: 3, Max: 3, Count: math.MaxInt64},
		},
	}

	dtoBucket, err := httpserver.NewTelemetryHistoryResponseDTO(resBucket)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	bPts, ok := dtoBucket.BucketPoints()
	if !ok || len(bPts) != 3 {
		t.Fatalf("expected 3 bucket points")
	}
	if bPts[0].Count != "0" {
		t.Errorf("expected count '0', got %s", bPts[0].Count)
	}
	if bPts[1].Count != "9007199254740993" {
		t.Errorf("expected count '9007199254740993', got %s", bPts[1].Count)
	}
	if bPts[2].Count != "9223372036854775807" {
		t.Errorf("expected count MaxInt64, got %s", bPts[2].Count)
	}

	// Verify cross-getter behavior
	if _, ok := dtoRaw.BucketPoints(); ok {
		t.Errorf("expected BucketPoints to return false on raw DTO")
	}
	if _, ok := dtoBucket.RawPoints(); ok {
		t.Errorf("expected RawPoints to return false on bucket DTO")
	}
	if _, ok := (*httpserver.TelemetryHistoryResponseDTO)(nil).RawPoints(); ok {
		t.Errorf("expected nil DTO to return false on RawPoints")
	}
	if _, ok := (*httpserver.TelemetryHistoryResponseDTO)(nil).BucketPoints(); ok {
		t.Errorf("expected nil DTO to return false on BucketPoints")
	}
}

func TestNewTelemetryHistoryResponseDTO_Year0001ValidInstants(t *testing.T) {
	t.Parallel()

	from := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(1, 1, 1, 0, 0, 1, 0, time.UTC)

	// 1. Authorized-empty raw result from 0001-01-01T00:00:00Z to 0001-01-01T00:00:01Z
	resEmpty := &telemetry.TelemetryHistoryResult{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		From:       from,
		To:         to,
		Mode:       "raw",
		Resolution: "raw",
		PointCount: 0,
		RawPoints:  []telemetry.RawSamplePoint{},
	}

	dtoEmpty, err := httpserver.NewTelemetryHistoryResponseDTO(resEmpty)
	if err != nil {
		t.Fatalf("unexpected error for year 0001 empty raw result: %v", err)
	}

	dataEmpty, err := json.Marshal(dtoEmpty)
	if err != nil {
		t.Fatalf("failed to marshal empty raw JSON: %v", err)
	}

	jsonEmpty := string(dataEmpty)
	if !strings.Contains(jsonEmpty, `"from":"0001-01-01T00:00:00.000000Z"`) {
		t.Errorf("expected exact UTC six-fractional-digit from, got %s", jsonEmpty)
	}
	if !strings.Contains(jsonEmpty, `"to":"0001-01-01T00:00:01.000000Z"`) {
		t.Errorf("expected exact UTC six-fractional-digit to, got %s", jsonEmpty)
	}

	// 2. Raw result with point at 0001-01-01T00:00:00Z
	resRawWithPt := &telemetry.TelemetryHistoryResult{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		From:       from,
		To:         to,
		Mode:       "raw",
		Resolution: "raw",
		PointCount: 1,
		RawPoints: []telemetry.RawSamplePoint{
			{
				MeasuredAt:  from,
				Value:       42.5,
				SequenceNum: 100,
			},
		},
	}

	dtoRawWithPt, err := httpserver.NewTelemetryHistoryResponseDTO(resRawWithPt)
	if err != nil {
		t.Fatalf("unexpected error for year 0001 raw point: %v", err)
	}

	dataRawWithPt, err := json.Marshal(dtoRawWithPt)
	if err != nil {
		t.Fatalf("failed to marshal raw point JSON: %v", err)
	}
	jsonRawWithPt := string(dataRawWithPt)
	if !strings.Contains(jsonRawWithPt, `"measured_at":"0001-01-01T00:00:00.000000Z"`) {
		t.Errorf("expected exact UTC six-fractional-digit measured_at, got %s", jsonRawWithPt)
	}

	// 3. Bucket result with valid bucket_start at 0001-01-01T00:00:00Z
	resBucket := &telemetry.TelemetryHistoryResult{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		From:       from,
		To:         to,
		Mode:       "bucket",
		Resolution: "1s",
		PointCount: 1,
		BucketPoints: []telemetry.BucketSamplePoint{
			{
				BucketStart: from,
				Avg:         20.0,
				Min:         19.5,
				Max:         20.5,
				Count:       50,
			},
		},
	}

	dtoBucket, err := httpserver.NewTelemetryHistoryResponseDTO(resBucket)
	if err != nil {
		t.Fatalf("unexpected error for year 0001 bucket point: %v", err)
	}

	dataBucket, err := json.Marshal(dtoBucket)
	if err != nil {
		t.Fatalf("failed to marshal bucket JSON: %v", err)
	}

	jsonBucket := string(dataBucket)
	if !strings.Contains(jsonBucket, `"from":"0001-01-01T00:00:00.000000Z"`) {
		t.Errorf("expected exact UTC six-fractional-digit from, got %s", jsonBucket)
	}
	if !strings.Contains(jsonBucket, `"to":"0001-01-01T00:00:01.000000Z"`) {
		t.Errorf("expected exact UTC six-fractional-digit to, got %s", jsonBucket)
	}
	if !strings.Contains(jsonBucket, `"bucket_start":"0001-01-01T00:00:00.000000Z"`) {
		t.Errorf("expected exact UTC six-fractional-digit bucket_start, got %s", jsonBucket)
	}
}

package httpserver_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"iot-platform/internal/config"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/telemetry"
)

func validTestConfig() config.HistoryConfig {
	return config.DefaultHistoryConfig()
}

func TestNewHistoryQueryParser_Constructors(t *testing.T) {
	t.Run("nil config fails", func(t *testing.T) {
		parser, err := httpserver.NewHistoryQueryParser(nil)
		if err == nil {
			t.Fatal("expected error for nil config, got nil")
		}
		if parser != nil {
			t.Fatalf("expected nil parser, got %+v", parser)
		}
	})

	t.Run("invalid config with zero timeout fails", func(t *testing.T) {
		cfg := validTestConfig()
		cfg.QueryTimeout = 0
		parser, err := httpserver.NewHistoryQueryParser(&cfg)
		if err == nil {
			t.Fatal("expected error for zero QueryTimeout, got nil")
		}
		if parser != nil {
			t.Fatalf("expected nil parser, got %+v", parser)
		}
	})

	t.Run("invalid config with negative MaxRange fails", func(t *testing.T) {
		cfg := validTestConfig()
		cfg.MaxRange = -time.Hour
		parser, err := httpserver.NewHistoryQueryParser(&cfg)
		if err == nil {
			t.Fatal("expected error for negative MaxRange, got nil")
		}
		if parser != nil {
			t.Fatalf("expected nil parser, got %+v", parser)
		}
	})

	t.Run("valid config succeeds", func(t *testing.T) {
		cfg := validTestConfig()
		parser, err := httpserver.NewHistoryQueryParser(&cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if parser == nil {
			t.Fatal("expected non-nil parser")
		}
	})
}

func TestParseTelemetryHistoryQuery_TableDriven(t *testing.T) {
	cfg := validTestConfig()
	parser, err := httpserver.NewHistoryQueryParser(&cfg)
	if err != nil {
		t.Fatalf("failed to create parser: %v", err)
	}

	validFrom := "2026-08-21T10:15:00Z"
	validTo := "2026-08-21T10:15:05Z"

	tests := []struct {
		name          string
		rawQuery      string
		wantErrCode   string
		wantSentinel  error
		validateQuery func(t *testing.T, q *telemetry.TelemetryHistoryQuery)
	}{
		{
			name:     "valid normal query with defaults",
			rawQuery: "gateway_id=gw_01&sensor_id=sen-01&from=" + validFrom + "&to=" + validTo,
			validateQuery: func(t *testing.T, q *telemetry.TelemetryHistoryQuery) {
				if q.GatewayID != "gw_01" {
					t.Errorf("GatewayID = %q, want gw_01", q.GatewayID)
				}
				if q.SensorID != "sen-01" {
					t.Errorf("SensorID = %q, want sen-01", q.SensorID)
				}
				if q.Resolution != telemetry.ResolutionAuto {
					t.Errorf("Resolution = %q, want auto", q.Resolution)
				}
				if q.MaxPoints != 1000 {
					t.Errorf("MaxPoints = %d, want 1000", q.MaxPoints)
				}
				expectedFrom := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
				expectedTo := time.Date(2026, 8, 21, 10, 15, 5, 0, time.UTC)
				if !q.Range.From.Equal(expectedFrom) {
					t.Errorf("From = %v, want %v", q.Range.From, expectedFrom)
				}
				if !q.Range.To.Equal(expectedTo) {
					t.Errorf("To = %v, want %v", q.Range.To, expectedTo)
				}
			},
		},
		{
			name:     "valid with explicit resolution and max_points",
			rawQuery: "gateway_id=GW1&sensor_id=SEN1&from=" + validFrom + "&to=" + validTo + "&resolution=1s&max_points=500",
			validateQuery: func(t *testing.T, q *telemetry.TelemetryHistoryQuery) {
				if q.GatewayID != "GW1" {
					t.Errorf("GatewayID = %q, want GW1", q.GatewayID)
				}
				if q.SensorID != "SEN1" {
					t.Errorf("SensorID = %q, want SEN1", q.SensorID)
				}
				if q.Resolution != telemetry.Resolution1s {
					t.Errorf("Resolution = %q, want 1s", q.Resolution)
				}
				if q.MaxPoints != 500 {
					t.Errorf("MaxPoints = %d, want 500", q.MaxPoints)
				}
			},
		},
		{
			name:     "valid explicit raw within 10s",
			rawQuery: "gateway_id=gw1&sensor_id=sen1&from=2026-08-21T10:15:00Z&to=2026-08-21T10:15:10Z&resolution=raw",
			validateQuery: func(t *testing.T, q *telemetry.TelemetryHistoryQuery) {
				if q.Resolution != telemetry.ResolutionRaw {
					t.Errorf("Resolution = %q, want raw", q.Resolution)
				}
			},
		},
		{
			name:     "valid time with timezone offset +07:00 normalized to UTC",
			rawQuery: "gateway_id=gw1&sensor_id=sen1&from=2026-08-21T17:15:00%2B07:00&to=2026-08-21T17:15:05%2B07:00",
			validateQuery: func(t *testing.T, q *telemetry.TelemetryHistoryQuery) {
				expectedFrom := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
				expectedTo := time.Date(2026, 8, 21, 10, 15, 5, 0, time.UTC)
				if !q.Range.From.Equal(expectedFrom) {
					t.Errorf("From = %v, want %v", q.Range.From, expectedFrom)
				}
				if !q.Range.To.Equal(expectedTo) {
					t.Errorf("To = %v, want %v", q.Range.To, expectedTo)
				}
			},
		},
		{
			name:     "valid time with fractional digits 1 to 6",
			rawQuery: "gateway_id=gw1&sensor_id=sen1&from=2026-08-21T10:15:00.123456Z&to=2026-08-21T10:15:05.1Z",
			validateQuery: func(t *testing.T, q *telemetry.TelemetryHistoryQuery) {
				expectedFrom := time.Date(2026, 8, 21, 10, 15, 0, 123456000, time.UTC)
				expectedTo := time.Date(2026, 8, 21, 10, 15, 5, 100000000, time.UTC)
				if !q.Range.From.Equal(expectedFrom) {
					t.Errorf("From = %v, want %v", q.Range.From, expectedFrom)
				}
				if !q.Range.To.Equal(expectedTo) {
					t.Errorf("To = %v, want %v", q.Range.To, expectedTo)
				}
			},
		},
		{
			name:     "valid lowercase z offset",
			rawQuery: "gateway_id=gw1&sensor_id=sen1&from=2026-08-21T10:15:00z&to=2026-08-21T10:15:05z",
			validateQuery: func(t *testing.T, q *telemetry.TelemetryHistoryQuery) {
				if q.Range.From.Location() != time.UTC {
					t.Errorf("expected UTC location")
				}
			},
		},
		{
			name:        "reject raw query exceeding 1024 bytes before url.ParseQuery",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validTo + "&padding=" + strings.Repeat("a", 1025),
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject malformed percent encoding",
			rawQuery:    "gateway_id=gw%ZZ&sensor_id=sen1&from=" + validFrom + "&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject semicolon in query",
			rawQuery:    "gateway_id=gw1;sensor_id=sen1&from=" + validFrom + "&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject unknown query parameter",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validTo + "&unknown_param=1",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject user_id identity query parameter",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validTo + "&user_id=123",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject role identity query parameter",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validTo + "&role=owner",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject repeated parameter",
			rawQuery:    "gateway_id=gw1&gateway_id=gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject missing gateway_id",
			rawQuery:    "sensor_id=sen1&from=" + validFrom + "&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject missing sensor_id",
			rawQuery:    "gateway_id=gw1&from=" + validFrom + "&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject missing from",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject missing to",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=" + validFrom,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject empty gateway_id",
			rawQuery:    "gateway_id=&sensor_id=sen1&from=" + validFrom + "&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject empty optional resolution parameter",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validTo + "&resolution=",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject empty optional max_points parameter",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validTo + "&max_points=",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject reserved gateway_id backend_service",
			rawQuery:    "gateway_id=backend_service&sensor_id=sen1&from=" + validFrom + "&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject gateway_id with whitespace (no trim)",
			rawQuery:    "gateway_id=%20gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject sensor_id with invalid characters",
			rawQuery:    "gateway_id=gw1&sensor_id=sen%201&from=" + validFrom + "&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject max_points with non-digit ASCII",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validTo + "&max_points=500a",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject max_points with sign +",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validTo + "&max_points=+500",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject max_points below min 100",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validTo + "&max_points=99",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject max_points above max 2000",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validTo + "&max_points=2001",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject max_points integer overflow",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validTo + "&max_points=9999999999999999999999999999999",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject timestamp with 7 fractional digits",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=2026-08-21T10:15:00.1234567Z&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject timestamp with 7 fractional digits with trailing zero (.1234560Z)",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=2026-08-21T10:15:00.1234560Z&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject timestamp missing timezone offset",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=2026-08-21T10:15:00&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject timestamp with invalid timezone offset hour 24 (+24:00)",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=2026-08-21T10:15:00+24:00&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject timestamp with invalid timezone offset hour 24 (-24:00)",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=2026-08-21T10:15:00-24:00&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject timestamp with invalid timezone offset minute 60 (+05:60)",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=2026-08-21T10:15:00+05:60&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject timestamp with invalid timezone offset minute 60 (+00:60)",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=2026-08-21T10:15:00+00:60&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject timestamp with year 0000",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=0000-08-21T10:15:00Z&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject timestamp with UTC conversion shifting year to 10000",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=9999-12-31T23:30:00-01:00&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject timestamp with UTC conversion shifting year to 0000",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=0001-01-01T00:30:00+01:00&to=" + validTo,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject from equals to",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validFrom,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject from after to",
			rawQuery:    "gateway_id=gw1&sensor_id=sen1&from=" + validTo + "&to=" + validFrom,
			wantErrCode: "invalid_request",
		},
		{
			name:         "reject range exceeding 744h",
			rawQuery:     "gateway_id=gw1&sensor_id=sen1&from=2026-08-01T00:00:00Z&to=2026-09-02T00:00:01Z",
			wantErrCode:  "range_too_large",
			wantSentinel: telemetry.ErrHistoryRangeTooLarge,
		},
		{
			name:         "reject unknown resolution",
			rawQuery:     "gateway_id=gw1&sensor_id=sen1&from=" + validFrom + "&to=" + validTo + "&resolution=2s",
			wantErrCode:  "invalid_resolution",
			wantSentinel: telemetry.ErrHistoryInvalidResolution,
		},
		{
			name:         "reject raw resolution when window exceeds 10s",
			rawQuery:     "gateway_id=gw1&sensor_id=sen1&from=2026-08-21T10:15:00Z&to=2026-08-21T10:15:11Z&resolution=raw",
			wantErrCode:  "raw_limit_exceeded",
			wantSentinel: telemetry.ErrHistoryRawLimitExceeded,
		},
		{
			name:         "reject explicit bucket when resolution too fine",
			rawQuery:     "gateway_id=gw1&sensor_id=sen1&from=2026-08-21T10:00:00Z&to=2026-08-21T10:20:00Z&resolution=100ms&max_points=1000",
			wantErrCode:  "resolution_too_fine",
			wantSentinel: telemetry.ErrHistoryResolutionTooFine,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseTelemetryHistoryQuery(tt.rawQuery)
			if tt.wantErrCode != "" {
				if err == nil {
					t.Fatalf("expected error code %q, got nil error", tt.wantErrCode)
				}
				var qe *httpserver.QueryError
				if !errors.As(err, &qe) {
					t.Fatalf("expected *httpserver.QueryError, got %T: %v", err, err)
				}
				if qe.Code != tt.wantErrCode {
					t.Errorf("QueryError.Code = %q, want %q", qe.Code, tt.wantErrCode)
				}
				if tt.wantSentinel != nil && !errors.Is(err, tt.wantSentinel) {
					t.Errorf("errors.Is(%v, %v) = false", err, tt.wantSentinel)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if tt.validateQuery != nil {
					tt.validateQuery(t, q)
				}
			}
		})
	}
}

func TestParseTwinHistoryQuery_TableDriven(t *testing.T) {
	cfg := validTestConfig()
	parser, err := httpserver.NewHistoryQueryParser(&cfg)
	if err != nil {
		t.Fatalf("failed to create parser: %v", err)
	}

	sensorURN := "urn:ngsi-ld:Sensor:gw_01:sen_01"
	gatewayURN := "urn:ngsi-ld:Gateway:gw_01"
	validFrom := "2026-08-21T10:15:00Z"
	validTo := "2026-08-21T10:15:05Z"

	tests := []struct {
		name          string
		rawQuery      string
		entityID      string
		wantErrCode   string
		wantSentinel  error
		validateQuery func(t *testing.T, q *digitaltwin.TwinHistoryQuery)
	}{
		{
			name:     "valid Sensor twin history with property=value",
			rawQuery: "property=value&from=" + validFrom + "&to=" + validTo,
			entityID: sensorURN,
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery) {
				if q.EntityID != sensorURN {
					t.Errorf("EntityID = %q, want %q", q.EntityID, sensorURN)
				}
				if q.Property != digitaltwin.SelectorValue {
					t.Errorf("Property = %q, want %q", q.Property, digitaltwin.SelectorValue)
				}
				if q.Resolution != telemetry.ResolutionAuto {
					t.Errorf("Resolution = %q, want auto", q.Resolution)
				}
			},
		},
		{
			name:     "valid Sensor twin history with property=sample_interval_us",
			rawQuery: "property=sample_interval_us&from=" + validFrom + "&to=" + validTo + "&resolution=1s&max_points=500",
			entityID: sensorURN,
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery) {
				if q.Property != digitaltwin.SelectorSampleIntervalUS {
					t.Errorf("Property = %q, want %q", q.Property, digitaltwin.SelectorSampleIntervalUS)
				}
				if q.Resolution != telemetry.Resolution1s {
					t.Errorf("Resolution = %q, want 1s", q.Resolution)
				}
				if q.MaxPoints != 500 {
					t.Errorf("MaxPoints = %d, want 500", q.MaxPoints)
				}
			},
		},
		{
			name:        "reject missing property",
			rawQuery:    "from=" + validFrom + "&to=" + validTo,
			entityID:    sensorURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject empty property",
			rawQuery:    "property=&from=" + validFrom + "&to=" + validTo,
			entityID:    sensorURN,
			wantErrCode: "invalid_request",
		},
		{
			name:         "reject property=measurement (mapper term, not API selector)",
			rawQuery:     "property=measurement&from=" + validFrom + "&to=" + validTo,
			entityID:     sensorURN,
			wantErrCode:  "invalid_property",
			wantSentinel: digitaltwin.ErrInvalidProperty,
		},
		{
			name:         "reject property=sampleIntervalUs (mapper term, not API selector)",
			rawQuery:     "property=sampleIntervalUs&from=" + validFrom + "&to=" + validTo,
			entityID:     sensorURN,
			wantErrCode:  "invalid_property",
			wantSentinel: digitaltwin.ErrInvalidProperty,
		},
		{
			name:         "reject unsupported inference property anomaly_score",
			rawQuery:     "property=anomaly_score&from=" + validFrom + "&to=" + validTo,
			entityID:     sensorURN,
			wantErrCode:  "invalid_property",
			wantSentinel: digitaltwin.ErrInvalidProperty,
		},
		{
			name:        "reject gateway_id in twin history query",
			rawQuery:    "property=value&gateway_id=gw1&from=" + validFrom + "&to=" + validTo,
			entityID:    sensorURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject sensor_id in twin history query",
			rawQuery:    "property=value&sensor_id=sen1&from=" + validFrom + "&to=" + validTo,
			entityID:    sensorURN,
			wantErrCode: "invalid_request",
		},
		{
			name:         "reject Gateway entity for property selectors",
			rawQuery:     "property=value&from=" + validFrom + "&to=" + validTo,
			entityID:     gatewayURN,
			wantErrCode:  "invalid_property",
			wantSentinel: digitaltwin.ErrInvalidProperty,
		},
		{
			name:        "reject empty entityID",
			rawQuery:    "property=value&from=" + validFrom + "&to=" + validTo,
			entityID:    "",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject oversized entityID",
			rawQuery:    "property=value&from=" + validFrom + "&to=" + validTo,
			entityID:    strings.Repeat("a", digitaltwin.MaxDecodedURNLength+1),
			wantErrCode: "invalid_request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseTwinHistoryQuery(tt.rawQuery, tt.entityID)
			if tt.wantErrCode != "" {
				if err == nil {
					t.Fatalf("expected error code %q, got nil error", tt.wantErrCode)
				}
				var qe *httpserver.QueryError
				if !errors.As(err, &qe) {
					t.Fatalf("expected *httpserver.QueryError, got %T: %v", err, err)
				}
				if qe.Code != tt.wantErrCode {
					t.Errorf("QueryError.Code = %q, want %q", qe.Code, tt.wantErrCode)
				}
				if tt.wantSentinel != nil && !errors.Is(err, tt.wantSentinel) {
					t.Errorf("errors.Is(%v, %v) = false", err, tt.wantSentinel)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if tt.validateQuery != nil {
					tt.validateQuery(t, q)
				}
			}
		})
	}
}

func TestParseTwinStateQuery_TableDriven(t *testing.T) {
	entityURN := "urn:ngsi-ld:Gateway:gw_01"

	tests := []struct {
		name        string
		rawQuery    string
		entityID    string
		wantErrCode string
	}{
		{
			name:     "valid empty query",
			rawQuery: "",
			entityID: entityURN,
		},
		{
			name:        "reject non-empty query",
			rawQuery:    "foo=bar",
			entityID:    entityURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject question mark only query",
			rawQuery:    "?",
			entityID:    entityURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject empty entityID",
			rawQuery:    "",
			entityID:    "",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject oversized entityID",
			rawQuery:    "",
			entityID:    strings.Repeat("u", digitaltwin.MaxDecodedURNLength+1),
			wantErrCode: "invalid_request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := httpserver.ParseTwinStateQuery(tt.rawQuery, tt.entityID)
			if tt.wantErrCode != "" {
				if err == nil {
					t.Fatalf("expected error code %q, got nil error", tt.wantErrCode)
				}
				var qe *httpserver.QueryError
				if !errors.As(err, &qe) {
					t.Fatalf("expected *httpserver.QueryError, got %T: %v", err, err)
				}
				if qe.Code != tt.wantErrCode {
					t.Errorf("QueryError.Code = %q, want %q", qe.Code, tt.wantErrCode)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if q.EntityID != entityURN {
					t.Errorf("EntityID = %q, want %q", q.EntityID, entityURN)
				}
			}
		})
	}
}

func TestParseTemporalHistoryQuery_TableDriven(t *testing.T) {
	cfg := validTestConfig()
	parser, err := httpserver.NewHistoryQueryParser(&cfg)
	if err != nil {
		t.Fatalf("failed to create parser: %v", err)
	}

	validSensorURN := "urn:ngsi-ld:Sensor:gw_01:s_01"
	validTimeAt := "2026-10-18T00:00:00Z"
	validEndTimeAt := "2026-10-18T00:00:05Z"
	baseQuery := "attrs=measurement&timerel=between&timeAt=" + validTimeAt + "&endTimeAt=" + validEndTimeAt

	tests := []struct {
		name          string
		entityID      string
		rawQuery      string
		wantErrCode   string
		wantSentinel  error
		validateQuery func(t *testing.T, q *digitaltwin.TwinHistoryQuery, pres *httpserver.TemporalPresentationOptions)
	}{
		{
			name:     "valid raw query with defaults (measurement)",
			entityID: validSensorURN,
			rawQuery: baseQuery,
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery, pres *httpserver.TemporalPresentationOptions) {
				if q.EntityID != validSensorURN {
					t.Errorf("EntityID = %q, want %q", q.EntityID, validSensorURN)
				}
				if q.Property != digitaltwin.SelectorValue {
					t.Errorf("Property = %q, want %q", q.Property, digitaltwin.SelectorValue)
				}
				if q.Resolution != telemetry.ResolutionRaw {
					t.Errorf("Resolution = %q, want %q (never auto!)", q.Resolution, telemetry.ResolutionRaw)
				}
				if q.MaxPoints != cfg.DefaultPoints {
					t.Errorf("MaxPoints = %d, want %d", q.MaxPoints, cfg.DefaultPoints)
				}
				if pres.PublicAttr != "measurement" {
					t.Errorf("pres.PublicAttr = %q, want measurement", pres.PublicAttr)
				}
				if pres.Format != "temporalValues" {
					t.Errorf("pres.Format = %q, want temporalValues", pres.Format)
				}
				if len(pres.AggrMethods) != 0 {
					t.Errorf("pres.AggrMethods = %v, want empty in raw mode", pres.AggrMethods)
				}
			},
		},
		{
			name:     "valid raw query with alias value",
			entityID: validSensorURN,
			rawQuery: "attrs=value&timerel=between&timeAt=" + validTimeAt + "&endTimeAt=" + validEndTimeAt,
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery, pres *httpserver.TemporalPresentationOptions) {
				if q.Property != digitaltwin.SelectorValue {
					t.Errorf("Property = %q, want %q", q.Property, digitaltwin.SelectorValue)
				}
				if pres.PublicAttr != "measurement" {
					t.Errorf("pres.PublicAttr = %q, want measurement (normalized from alias value)", pres.PublicAttr)
				}
			},
		},
		{
			name:     "valid raw query with sample_interval_us",
			entityID: validSensorURN,
			rawQuery: "attrs=sample_interval_us&timerel=between&timeAt=" + validTimeAt + "&endTimeAt=" + validEndTimeAt,
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery, pres *httpserver.TemporalPresentationOptions) {
				if q.Property != digitaltwin.SelectorSampleIntervalUS {
					t.Errorf("Property = %q, want %q", q.Property, digitaltwin.SelectorSampleIntervalUS)
				}
				if pres.PublicAttr != "sample_interval_us" {
					t.Errorf("pres.PublicAttr = %q, want sample_interval_us", pres.PublicAttr)
				}
			},
		},
		{
			name:     "valid raw query with timeproperty=observedAt",
			entityID: validSensorURN,
			rawQuery: baseQuery + "&timeproperty=observedAt",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery, _ *httpserver.TemporalPresentationOptions) {
				if q.Resolution != telemetry.ResolutionRaw {
					t.Errorf("Resolution = %q, want raw", q.Resolution)
				}
			},
		},
		{
			name:     "valid raw query with format=temporalValues explicitly",
			entityID: validSensorURN,
			rawQuery: baseQuery + "&format=temporalValues",
			validateQuery: func(t *testing.T, _ *digitaltwin.TwinHistoryQuery, pres *httpserver.TemporalPresentationOptions) {
				if pres.Format != "temporalValues" {
					t.Errorf("pres.Format = %q, want temporalValues", pres.Format)
				}
			},
		},
		{
			name:     "valid raw query with fractional UTC Z timestamps (3 and 6 digits)",
			entityID: validSensorURN,
			rawQuery: "attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00.123Z&endTimeAt=2026-10-18T00:00:05.123456Z",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery, _ *httpserver.TemporalPresentationOptions) {
				expectedFrom := time.Date(2026, 10, 18, 0, 0, 0, 123000000, time.UTC)
				expectedTo := time.Date(2026, 10, 18, 0, 0, 5, 123456000, time.UTC)
				if !q.Range.From.Equal(expectedFrom) {
					t.Errorf("From = %v, want %v", q.Range.From, expectedFrom)
				}
				if !q.Range.To.Equal(expectedTo) {
					t.Errorf("To = %v, want %v", q.Range.To, expectedTo)
				}
			},
		},
		{
			name:     "valid bucket query with canonical duration PT1S",
			entityID: validSensorURN,
			rawQuery: baseQuery + "&aggrPeriodDuration=PT1S",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery, pres *httpserver.TemporalPresentationOptions) {
				if q.Resolution != telemetry.Resolution1s {
					t.Errorf("Resolution = %q, want %q", q.Resolution, telemetry.Resolution1s)
				}
				if pres.Format != "aggregatedValues" {
					t.Errorf("pres.Format = %q, want aggregatedValues", pres.Format)
				}
				// Default methods: avg, min, max, totalCount
				if len(pres.AggrMethods) != 4 || pres.AggrMethods[0] != "avg" || pres.AggrMethods[3] != "totalCount" {
					t.Errorf("pres.AggrMethods = %v, want [avg, min, max, totalCount]", pres.AggrMethods)
				}
			},
		},
		{
			name:     "valid bucket query with custom aggrMethods subset",
			entityID: validSensorURN,
			rawQuery: baseQuery + "&aggrPeriodDuration=PT1S&aggrMethods=avg,totalCount&format=aggregatedValues",
			validateQuery: func(t *testing.T, _ *digitaltwin.TwinHistoryQuery, pres *httpserver.TemporalPresentationOptions) {
				if len(pres.AggrMethods) != 2 || pres.AggrMethods[0] != "avg" || pres.AggrMethods[1] != "totalCount" {
					t.Errorf("pres.AggrMethods = %v, want [avg, totalCount]", pres.AggrMethods)
				}
			},
		},
		// Canonical ISO 8601 Durations checks
		{
			name:     "canonical duration PT0.1S -> 100ms",
			entityID: validSensorURN,
			rawQuery: baseQuery + "&aggrPeriodDuration=PT0.1S",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery, _ *httpserver.TemporalPresentationOptions) {
				if q.Resolution != telemetry.Resolution100ms {
					t.Errorf("Resolution = %q, want %q", q.Resolution, telemetry.Resolution100ms)
				}
			},
		},
		{
			name:     "canonical duration PT10S -> 10s",
			entityID: validSensorURN,
			rawQuery: baseQuery + "&aggrPeriodDuration=PT10S",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery, _ *httpserver.TemporalPresentationOptions) {
				if q.Resolution != telemetry.Resolution10s {
					t.Errorf("Resolution = %q, want %q", q.Resolution, telemetry.Resolution10s)
				}
			},
		},
		{
			name:     "canonical duration PT1M -> 1m",
			entityID: validSensorURN,
			rawQuery: "attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T00:05:00Z&aggrPeriodDuration=PT1M",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery, _ *httpserver.TemporalPresentationOptions) {
				if q.Resolution != telemetry.Resolution1m {
					t.Errorf("Resolution = %q, want %q", q.Resolution, telemetry.Resolution1m)
				}
			},
		},
		{
			name:     "canonical duration PT5M -> 5m",
			entityID: validSensorURN,
			rawQuery: "attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T00:15:00Z&aggrPeriodDuration=PT5M",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery, _ *httpserver.TemporalPresentationOptions) {
				if q.Resolution != telemetry.Resolution5m {
					t.Errorf("Resolution = %q, want %q", q.Resolution, telemetry.Resolution5m)
				}
			},
		},
		{
			name:     "canonical duration PT15M -> 15m",
			entityID: validSensorURN,
			rawQuery: "attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T01:00:00Z&aggrPeriodDuration=PT15M",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery, _ *httpserver.TemporalPresentationOptions) {
				if q.Resolution != telemetry.Resolution15m {
					t.Errorf("Resolution = %q, want %q", q.Resolution, telemetry.Resolution15m)
				}
			},
		},
		{
			name:     "canonical duration PT1H -> 1h",
			entityID: validSensorURN,
			rawQuery: "attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T06:00:00Z&aggrPeriodDuration=PT1H",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery, _ *httpserver.TemporalPresentationOptions) {
				if q.Resolution != telemetry.Resolution1h {
					t.Errorf("Resolution = %q, want %q", q.Resolution, telemetry.Resolution1h)
				}
			},
		},
		{
			name:     "canonical duration PT6H -> 6h",
			entityID: validSensorURN,
			rawQuery: "attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-19T00:00:00Z&aggrPeriodDuration=PT6H",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery, _ *httpserver.TemporalPresentationOptions) {
				if q.Resolution != telemetry.Resolution6h {
					t.Errorf("Resolution = %q, want %q", q.Resolution, telemetry.Resolution6h)
				}
			},
		},
		{
			name:     "canonical duration P1D -> 1d",
			entityID: validSensorURN,
			rawQuery: "attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-25T00:00:00Z&aggrPeriodDuration=P1D",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinHistoryQuery, _ *httpserver.TemporalPresentationOptions) {
				if q.Resolution != telemetry.Resolution1d {
					t.Errorf("Resolution = %q, want %q", q.Resolution, telemetry.Resolution1d)
				}
			},
		},
		// Entity ID validations
		{
			name:         "reject Gateway entity ID with invalid_property",
			entityID:     "urn:ngsi-ld:Gateway:gw_01",
			rawQuery:     baseQuery,
			wantErrCode:  "invalid_property",
			wantSentinel: digitaltwin.ErrInvalidProperty,
		},
		{
			name:         "reject invalid URN with invalid_request",
			entityID:     "invalid:urn",
			rawQuery:     baseQuery,
			wantErrCode:  "invalid_request",
			wantSentinel: digitaltwin.ErrInvalidInput,
		},
		{
			name:         "reject double encoded URN with invalid_request",
			entityID:     "urn%253Angsi-ld%253ASensor%253Agw_01%253As_01",
			rawQuery:     baseQuery,
			wantErrCode:  "invalid_request",
			wantSentinel: digitaltwin.ErrInvalidInput,
		},
		// Attrs validations
		{
			name:         "reject missing attrs",
			entityID:     validSensorURN,
			rawQuery:     "timerel=between&timeAt=" + validTimeAt + "&endTimeAt=" + validEndTimeAt,
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject empty attrs",
			entityID:     validSensorURN,
			rawQuery:     "attrs=&timerel=between&timeAt=" + validTimeAt + "&endTimeAt=" + validEndTimeAt,
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject multiple attrs tokens",
			entityID:     validSensorURN,
			rawQuery:     "attrs=measurement,sample_interval_us&timerel=between&timeAt=" + validTimeAt + "&endTimeAt=" + validEndTimeAt,
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject unknown attrs token",
			entityID:     validSensorURN,
			rawQuery:     "attrs=humidity&timerel=between&timeAt=" + validTimeAt + "&endTimeAt=" + validEndTimeAt,
			wantErrCode:  "invalid_property",
			wantSentinel: digitaltwin.ErrInvalidProperty,
		},
		// Timerel validations
		{
			name:         "reject missing timerel",
			entityID:     validSensorURN,
			rawQuery:     "attrs=measurement&timeAt=" + validTimeAt + "&endTimeAt=" + validEndTimeAt,
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject timerel=before",
			entityID:     validSensorURN,
			rawQuery:     "attrs=measurement&timerel=before&timeAt=" + validTimeAt + "&endTimeAt=" + validEndTimeAt,
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject timerel=after",
			entityID:     validSensorURN,
			rawQuery:     "attrs=measurement&timerel=after&timeAt=" + validTimeAt + "&endTimeAt=" + validEndTimeAt,
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		// Timestamp UTC Z validations
		{
			name:         "reject missing timeAt",
			entityID:     validSensorURN,
			rawQuery:     "attrs=measurement&timerel=between&endTimeAt=" + validEndTimeAt,
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject missing endTimeAt",
			entityID:     validSensorURN,
			rawQuery:     "attrs=measurement&timerel=between&timeAt=" + validTimeAt,
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject non-Z timezone offset +00:00",
			entityID:     validSensorURN,
			rawQuery:     "attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00+00:00&endTimeAt=" + validEndTimeAt,
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject non-Z timezone offset +07:00",
			entityID:     validSensorURN,
			rawQuery:     "attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00+07:00&endTimeAt=" + validEndTimeAt,
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject 7 fractional digits in timeAt",
			entityID:     validSensorURN,
			rawQuery:     "attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00.1234567Z&endTimeAt=" + validEndTimeAt,
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject timeAt >= endTimeAt",
			entityID:     validSensorURN,
			rawQuery:     "attrs=measurement&timerel=between&timeAt=" + validEndTimeAt + "&endTimeAt=" + validTimeAt,
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		// Timeproperty validations
		{
			name:         "reject timeproperty=modifiedAt",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&timeproperty=modifiedAt",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject timeproperty=createdAt",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&timeproperty=createdAt",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		// Duration validations
		{
			name:         "reject invalid duration PT0S",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&aggrPeriodDuration=PT0S",
			wantErrCode:  "invalid_resolution",
			wantSentinel: telemetry.ErrHistoryInvalidResolution,
		},
		{
			name:         "reject legacy resolution string 1m",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&aggrPeriodDuration=1m",
			wantErrCode:  "invalid_resolution",
			wantSentinel: telemetry.ErrHistoryInvalidResolution,
		},
		{
			name:         "reject unsupported duration PT2M",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&aggrPeriodDuration=PT2M",
			wantErrCode:  "invalid_resolution",
			wantSentinel: telemetry.ErrHistoryInvalidResolution,
		},
		{
			name:         "reject calendar month duration P1M",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&aggrPeriodDuration=P1M",
			wantErrCode:  "invalid_resolution",
			wantSentinel: telemetry.ErrHistoryInvalidResolution,
		},
		{
			name:         "reject lowercase duration pt1m",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&aggrPeriodDuration=pt1m",
			wantErrCode:  "invalid_resolution",
			wantSentinel: telemetry.ErrHistoryInvalidResolution,
		},
		// AggrMethods validations
		{
			name:         "reject aggrMethods in raw mode",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&aggrMethods=avg",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject duplicate aggrMethods avg,avg",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&aggrPeriodDuration=PT1S&aggrMethods=avg,avg",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject duplicate aggrMethods avg,min,avg",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&aggrPeriodDuration=PT1S&aggrMethods=avg,min,avg",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject unsupported aggrMethod sum",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&aggrPeriodDuration=PT1S&aggrMethods=sum",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject unsupported aggrMethod count (must be totalCount)",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&aggrPeriodDuration=PT1S&aggrMethods=count",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		// Format validations
		{
			name:         "reject format=aggregatedValues in raw mode",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&format=aggregatedValues",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject format=temporalValues in bucket mode",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&aggrPeriodDuration=PT1S&format=temporalValues",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject unsupported format=keyValues",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&format=keyValues",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		// Replaced and unknown query parameters
		{
			name:         "reject replaced parameter from",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&from=" + validTimeAt,
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject replaced parameter to",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&to=" + validEndTimeAt,
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject replaced parameter resolution",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&resolution=1s",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject replaced parameter property",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&property=value",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject replaced parameter gateway_id",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&gateway_id=gw_01",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject duplicate query parameter attrs=measurement&attrs=value",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&attrs=value",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:         "reject unknown parameter foo",
			entityID:     validSensorURN,
			rawQuery:     baseQuery + "&foo=bar",
			wantErrCode:  "invalid_request",
			wantSentinel: telemetry.ErrHistoryInvalidInput,
		},
		// Range validations
		{
			name:         "reject raw window exceeding RawWindow (e.g. 15s > 10s)",
			entityID:     validSensorURN,
			rawQuery:     "attrs=measurement&timerel=between&timeAt=2026-10-18T00:00:00Z&endTimeAt=2026-10-18T00:00:15Z",
			wantErrCode:  "raw_limit_exceeded",
			wantSentinel: telemetry.ErrHistoryRawLimitExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, pres, err := parser.ParseTemporalHistoryQuery(tt.rawQuery, tt.entityID)
			if tt.wantErrCode != "" {
				if err == nil {
					t.Fatalf("expected error code %q, got nil error", tt.wantErrCode)
				}
				var qe *httpserver.QueryError
				if !errors.As(err, &qe) {
					t.Fatalf("expected *httpserver.QueryError, got %T: %v", err, err)
				}
				if qe.Code != tt.wantErrCode {
					t.Errorf("QueryError.Code = %q, want %q (err: %v)", qe.Code, tt.wantErrCode, err)
				}
				if tt.wantSentinel != nil && !errors.Is(err, tt.wantSentinel) {
					t.Errorf("errors.Is(err, %v) = false, got %v", tt.wantSentinel, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if q == nil || pres == nil {
					t.Fatal("expected non-nil query and presentation options")
				}
				if tt.validateQuery != nil {
					tt.validateQuery(t, q, pres)
				}
			}
		})
	}
}

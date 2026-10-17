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

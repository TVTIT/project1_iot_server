package httpserver_test

import (
	"errors"
	"strings"
	"testing"

	"iot-platform/internal/config"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/httpserver"
)

func validTwinConfig() config.TwinConfig {
	return config.DefaultTwinConfig()
}

func TestNewTwinQueryParser(t *testing.T) {
	t.Run("nil config fails", func(t *testing.T) {
		_, err := httpserver.NewTwinQueryParser(nil)
		if err == nil {
			t.Fatal("expected error with nil config, got nil")
		}
	})

	t.Run("invalid config fails", func(t *testing.T) {
		cfg := validTwinConfig()
		cfg.ListDefaultLimit = 300 // > ListMaxLimit
		_, err := httpserver.NewTwinQueryParser(&cfg)
		if err == nil {
			t.Fatal("expected error with invalid config, got nil")
		}
	})

	t.Run("valid config succeeds", func(t *testing.T) {
		cfg := validTwinConfig()
		parser, err := httpserver.NewTwinQueryParser(&cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if parser == nil {
			t.Fatal("expected non-nil parser")
		}
	})
}

func TestParseTwinListQuery_TableDriven(t *testing.T) {
	cfg := validTwinConfig()
	parser, err := httpserver.NewTwinQueryParser(&cfg)
	if err != nil {
		t.Fatalf("failed to create parser: %v", err)
	}

	gw01 := "gw_01"
	sensorType := digitaltwin.EntityTypeSensor
	gatewayType := digitaltwin.EntityTypeGateway

	validSensorCursor, err := digitaltwin.EncodeListCursor("urn:ngsi-ld:Sensor:gw_01:s1", &gw01, &sensorType)
	if err != nil {
		t.Fatalf("failed to encode cursor: %v", err)
	}

	validGatewayCursor, err := digitaltwin.EncodeListCursor("urn:ngsi-ld:Gateway:gw_01", &gw01, &gatewayType)
	if err != nil {
		t.Fatalf("failed to encode cursor: %v", err)
	}

	noFilterCursor, err := digitaltwin.EncodeListCursor("urn:ngsi-ld:Sensor:gw_01:s1", nil, nil)
	if err != nil {
		t.Fatalf("failed to encode cursor: %v", err)
	}

	tests := []struct {
		name          string
		rawQuery      string
		wantErrCode   string
		wantSentinel  error
		validateQuery func(t *testing.T, q *digitaltwin.TwinListQuery)
	}{
		{
			name:     "valid default list query",
			rawQuery: "",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinListQuery) {
				if q.Limit != 50 {
					t.Errorf("Limit = %d, want 50", q.Limit)
				}
				if q.GatewayID != nil {
					t.Errorf("GatewayID = %v, want nil", *q.GatewayID)
				}
				if q.EntityType != nil {
					t.Errorf("EntityType = %v, want nil", *q.EntityType)
				}
				if q.After != nil {
					t.Errorf("After = %v, want nil", *q.After)
				}
			},
		},
		{
			name:     "valid override limit",
			rawQuery: "limit=100",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinListQuery) {
				if q.Limit != 100 {
					t.Errorf("Limit = %d, want 100", q.Limit)
				}
			},
		},
		{
			name:     "valid override max limit",
			rawQuery: "limit=200",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinListQuery) {
				if q.Limit != 200 {
					t.Errorf("Limit = %d, want 200", q.Limit)
				}
			},
		},
		{
			name:     "valid filter gateway_id",
			rawQuery: "gateway_id=gw_01",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinListQuery) {
				if q.GatewayID == nil || *q.GatewayID != "gw_01" {
					t.Errorf("GatewayID = %v, want gw_01", q.GatewayID)
				}
			},
		},
		{
			name:     "valid filter type Sensor",
			rawQuery: "type=Sensor",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinListQuery) {
				if q.EntityType == nil || *q.EntityType != digitaltwin.EntityTypeSensor {
					t.Errorf("EntityType = %v, want Sensor", q.EntityType)
				}
			},
		},
		{
			name:     "valid filter type Gateway",
			rawQuery: "type=Gateway",
			validateQuery: func(t *testing.T, q *digitaltwin.TwinListQuery) {
				if q.EntityType == nil || *q.EntityType != digitaltwin.EntityTypeGateway {
					t.Errorf("EntityType = %v, want Gateway", q.EntityType)
				}
			},
		},
		{
			name:     "valid list query with all parameters and matching cursor",
			rawQuery: "gateway_id=gw_01&type=Sensor&limit=75&cursor=" + validSensorCursor,
			validateQuery: func(t *testing.T, q *digitaltwin.TwinListQuery) {
				if q.Limit != 75 {
					t.Errorf("Limit = %d, want 75", q.Limit)
				}
				if q.GatewayID == nil || *q.GatewayID != "gw_01" {
					t.Errorf("GatewayID = %v, want gw_01", q.GatewayID)
				}
				if q.EntityType == nil || *q.EntityType != digitaltwin.EntityTypeSensor {
					t.Errorf("EntityType = %v, want Sensor", q.EntityType)
				}
				if q.After == nil || *q.After != validSensorCursor {
					t.Errorf("After = %v, want %q", q.After, validSensorCursor)
				}
			},
		},
		{
			name:     "valid list query with cursor and no filters",
			rawQuery: "cursor=" + noFilterCursor,
			validateQuery: func(t *testing.T, q *digitaltwin.TwinListQuery) {
				if q.After == nil || *q.After != noFilterCursor {
					t.Errorf("After = %v, want %q", q.After, noFilterCursor)
				}
			},
		},
		{
			name:        "reject replaced parameter entity_type",
			rawQuery:    "entity_type=Sensor",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject replaced parameter after",
			rawQuery:    "after=" + validSensorCursor,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject repeated type",
			rawQuery:    "type=Gateway&type=Sensor",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject repeated cursor",
			rawQuery:    "cursor=" + validSensorCursor + "&cursor=" + validSensorCursor,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject unknown parameter user_id",
			rawQuery:    "user_id=123",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject unknown parameter role",
			rawQuery:    "role=owner",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject repeated limit",
			rawQuery:    "limit=50&limit=50",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject empty gateway_id parameter",
			rawQuery:    "gateway_id=",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject empty type parameter",
			rawQuery:    "type=",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject empty limit parameter",
			rawQuery:    "limit=",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject empty cursor parameter",
			rawQuery:    "cursor=",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject limit 0",
			rawQuery:    "limit=0",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject negative limit",
			rawQuery:    "limit=-5",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject limit exceeding max 200",
			rawQuery:    "limit=201",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject non-digit limit",
			rawQuery:    "limit=fifty",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject decimal limit",
			rawQuery:    "limit=50.5",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject invalid type Device",
			rawQuery:    "type=Device",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject lowercase type gateway",
			rawQuery:    "type=gateway",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject reserved backend_service gateway_id",
			rawQuery:    "gateway_id=backend_service",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject invalid gateway_id characters",
			rawQuery:    "gateway_id=gw@01",
			wantErrCode: "invalid_request",
		},
		{
			name:         "reject invalid cursor string",
			rawQuery:     "cursor=not-a-cursor",
			wantErrCode:  "invalid_cursor",
			wantSentinel: digitaltwin.ErrInvalidCursor,
		},
		{
			name:         "reject cursor with filter mismatch (cursor has Sensor, filter has Gateway)",
			rawQuery:     "type=Gateway&cursor=" + validSensorCursor,
			wantErrCode:  "invalid_cursor",
			wantSentinel: digitaltwin.ErrInvalidCursor,
		},
		{
			name:         "reject cursor with gateway mismatch",
			rawQuery:     "gateway_id=gw_02&cursor=" + validGatewayCursor,
			wantErrCode:  "invalid_cursor",
			wantSentinel: digitaltwin.ErrInvalidCursor,
		},
		{
			name:        "reject raw query exceeding 1024 bytes",
			rawQuery:    "gateway_id=" + strings.Repeat("a", 1020),
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject semicolons in raw query",
			rawQuery:    "limit=50;gateway_id=gw_01",
			wantErrCode: "invalid_request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, _, err := parser.ParseTwinListQuery(tt.rawQuery)
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
					t.Errorf("got error %v, want sentinel %v", err, tt.wantSentinel)
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

// TestParseTwinListQuery_Presentation_TableDriven locks the format=simplified
// presentation extension on GET /v1/digital-twins: it must accept format=simplified
// combined with type/gateway_id/limit/cursor, keep presentation outside the domain
// TwinListQuery, keep the existing query bounds and filter-bound cursor codec, and
// fail closed (400 invalid_request / 400 invalid_cursor) for unknown, empty, repeated,
// or unsupported format values and invalid filters/cursors.
func TestParseTwinListQuery_Presentation_TableDriven(t *testing.T) {
	cfg := validTwinConfig()
	parser, err := httpserver.NewTwinQueryParser(&cfg)
	if err != nil {
		t.Fatalf("failed to create parser: %v", err)
	}

	gw01 := "gw_01"
	sensorType := digitaltwin.EntityTypeSensor
	gatewayType := digitaltwin.EntityTypeGateway

	validSensorCursor, err := digitaltwin.EncodeListCursor("urn:ngsi-ld:Sensor:gw_01:s1", &gw01, &sensorType)
	if err != nil {
		t.Fatalf("failed to encode cursor: %v", err)
	}
	validGatewayCursor, err := digitaltwin.EncodeListCursor("urn:ngsi-ld:Gateway:gw_01", &gw01, &gatewayType)
	if err != nil {
		t.Fatalf("failed to encode cursor: %v", err)
	}

	tests := []struct {
		name         string
		rawQuery     string
		wantErrCode  string
		wantSentinel error
		validate     func(t *testing.T, q *digitaltwin.TwinListQuery, pres *httpserver.TwinListPresentationOptions)
	}{
		{
			name:     "format simplified alone selects simplified presentation with default domain query",
			rawQuery: "format=simplified",
			validate: func(t *testing.T, q *digitaltwin.TwinListQuery, pres *httpserver.TwinListPresentationOptions) {
				if pres == nil || pres.Format != "simplified" {
					t.Fatalf("presentation = %+v, want Format=simplified", pres)
				}
				if q.Limit != 50 {
					t.Errorf("Limit = %d, want 50", q.Limit)
				}
				if q.GatewayID != nil || q.EntityType != nil || q.After != nil {
					t.Errorf("domain query must carry no filters/cursor, got %+v", q)
				}
			},
		},
		{
			name:     "format simplified combined with type filter",
			rawQuery: "format=simplified&type=Sensor",
			validate: func(t *testing.T, q *digitaltwin.TwinListQuery, pres *httpserver.TwinListPresentationOptions) {
				if pres == nil || pres.Format != "simplified" {
					t.Fatalf("presentation = %+v, want Format=simplified", pres)
				}
				if q.EntityType == nil || *q.EntityType != digitaltwin.EntityTypeSensor {
					t.Errorf("EntityType = %v, want Sensor", q.EntityType)
				}
			},
		},
		{
			name:     "format simplified combined with all query parameters and matching cursor",
			rawQuery: "format=simplified&gateway_id=gw_01&type=Sensor&limit=75&cursor=" + validSensorCursor,
			validate: func(t *testing.T, q *digitaltwin.TwinListQuery, pres *httpserver.TwinListPresentationOptions) {
				if pres == nil || pres.Format != "simplified" {
					t.Fatalf("presentation = %+v, want Format=simplified", pres)
				}
				if q.Limit != 75 {
					t.Errorf("Limit = %d, want 75", q.Limit)
				}
				if q.GatewayID == nil || *q.GatewayID != "gw_01" {
					t.Errorf("GatewayID = %v, want gw_01", q.GatewayID)
				}
				if q.EntityType == nil || *q.EntityType != digitaltwin.EntityTypeSensor {
					t.Errorf("EntityType = %v, want Sensor", q.EntityType)
				}
				if q.After == nil || *q.After != validSensorCursor {
					t.Errorf("After = %v, want %q", q.After, validSensorCursor)
				}
			},
		},
		{
			name:     "format simplified combined with gateway type and filter-bound cursor",
			rawQuery: "format=simplified&type=Gateway&gateway_id=gw_01&cursor=" + validGatewayCursor,
			validate: func(t *testing.T, q *digitaltwin.TwinListQuery, pres *httpserver.TwinListPresentationOptions) {
				if pres == nil || pres.Format != "simplified" {
					t.Fatalf("presentation = %+v, want Format=simplified", pres)
				}
				if q.EntityType == nil || *q.EntityType != digitaltwin.EntityTypeGateway {
					t.Errorf("EntityType = %v, want Gateway", q.EntityType)
				}
				if q.After == nil || *q.After != validGatewayCursor {
					t.Errorf("After = %v, want %q", q.After, validGatewayCursor)
				}
			},
		},
		{
			name:     "absent format keeps normalized default presentation",
			rawQuery: "type=Sensor&limit=10",
			validate: func(t *testing.T, q *digitaltwin.TwinListQuery, pres *httpserver.TwinListPresentationOptions) {
				if pres == nil {
					t.Fatal("presentation must not be nil")
				}
				if pres.Format != "" {
					t.Errorf("Format = %q, want empty (normalized default)", pres.Format)
				}
				if q.Limit != 10 {
					t.Errorf("Limit = %d, want 10", q.Limit)
				}
			},
		},
		{
			name:        "reject empty format value",
			rawQuery:    "format=",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject repeated format",
			rawQuery:    "format=simplified&format=simplified",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject repeated format with different values",
			rawQuery:    "format=simplified&format=normalized",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject unsupported format normalized",
			rawQuery:    "format=normalized",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject unsupported format flat",
			rawQuery:    "format=flat",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject case-sensitive format Simplified",
			rawQuery:    "format=Simplified",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject unknown key combined with format",
			rawQuery:    "format=simplified&entity_type=Sensor",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject replaced parameter after combined with format",
			rawQuery:    "format=simplified&after=" + validSensorCursor,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject unknown parameter foo combined with format",
			rawQuery:    "format=simplified&foo=bar",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject invalid limit combined with format",
			rawQuery:    "format=simplified&limit=201",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject zero limit combined with format",
			rawQuery:    "format=simplified&limit=0",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject invalid type combined with format",
			rawQuery:    "format=simplified&type=Device",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject invalid gateway_id combined with format",
			rawQuery:    "format=simplified&gateway_id=gw@01",
			wantErrCode: "invalid_request",
		},
		{
			name:         "reject invalid cursor combined with format",
			rawQuery:     "format=simplified&type=Sensor&cursor=not-a-cursor",
			wantErrCode:  "invalid_cursor",
			wantSentinel: digitaltwin.ErrInvalidCursor,
		},
		{
			name:         "reject cursor filter mismatch combined with format",
			rawQuery:     "format=simplified&type=Gateway&cursor=" + validSensorCursor,
			wantErrCode:  "invalid_cursor",
			wantSentinel: digitaltwin.ErrInvalidCursor,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, pres, err := parser.ParseTwinListQuery(tt.rawQuery)
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
					t.Errorf("got error %v, want sentinel %v", err, tt.wantSentinel)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.validate != nil {
				tt.validate(t, q, pres)
			}
		})
	}
}

func TestParseTwinDetailQuery_TableDriven(t *testing.T) {
	cfg := validTwinConfig()
	parser, err := httpserver.NewTwinQueryParser(&cfg)
	if err != nil {
		t.Fatalf("failed to create parser: %v", err)
	}

	srcURN := "urn:ngsi-ld:Gateway:gw_01"
	targetURN := "urn:ngsi-ld:Sensor:gw_01:s1"
	validRelCursor, err := digitaltwin.EncodeRelationshipCursor(srcURN, "hasSensor", targetURN)
	if err != nil {
		t.Fatalf("failed to encode cursor: %v", err)
	}

	tests := []struct {
		name          string
		rawQuery      string
		entityID      string
		wantErrCode   string
		wantSentinel  error
		validateQuery func(t *testing.T, q *digitaltwin.TwinDetailQuery)
	}{
		{
			name:     "valid default detail query",
			rawQuery: "",
			entityID: srcURN,
			validateQuery: func(t *testing.T, q *digitaltwin.TwinDetailQuery) {
				if q.EntityID != srcURN {
					t.Errorf("EntityID = %q, want %q", q.EntityID, srcURN)
				}
				if q.RelationshipLimit != 50 {
					t.Errorf("RelationshipLimit = %d, want 50", q.RelationshipLimit)
				}
				if q.RelationshipAfter != nil {
					t.Errorf("RelationshipAfter = %v, want nil", *q.RelationshipAfter)
				}
			},
		},
		{
			name:     "valid override relationship_limit",
			rawQuery: "relationship_limit=100",
			entityID: srcURN,
			validateQuery: func(t *testing.T, q *digitaltwin.TwinDetailQuery) {
				if q.RelationshipLimit != 100 {
					t.Errorf("RelationshipLimit = %d, want 100", q.RelationshipLimit)
				}
			},
		},
		{
			name:     "valid override relationship_limit to max 200",
			rawQuery: "relationship_limit=200",
			entityID: srcURN,
			validateQuery: func(t *testing.T, q *digitaltwin.TwinDetailQuery) {
				if q.RelationshipLimit != 200 {
					t.Errorf("RelationshipLimit = %d, want 200", q.RelationshipLimit)
				}
			},
		},
		{
			name:     "valid relationship query with cursor",
			rawQuery: "relationship_limit=25&relationship_after=" + validRelCursor,
			entityID: srcURN,
			validateQuery: func(t *testing.T, q *digitaltwin.TwinDetailQuery) {
				if q.RelationshipLimit != 25 {
					t.Errorf("RelationshipLimit = %d, want 25", q.RelationshipLimit)
				}
				if q.RelationshipAfter == nil || *q.RelationshipAfter != validRelCursor {
					t.Errorf("RelationshipAfter = %v, want %q", q.RelationshipAfter, validRelCursor)
				}
			},
		},
		{
			name:     "valid detail format simplified",
			rawQuery: "format=simplified",
			entityID: srcURN,
			validateQuery: func(t *testing.T, q *digitaltwin.TwinDetailQuery) {
				if q.EntityID != srcURN {
					t.Errorf("EntityID = %q, want %q", q.EntityID, srcURN)
				}
			},
		},
		{
			name:        "reject unsupported detail format",
			rawQuery:    "format=normalized",
			entityID:    srcURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject detail format empty",
			rawQuery:    "format=",
			entityID:    srcURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject format simplified mixed with relationship_limit",
			rawQuery:    "format=simplified&relationship_limit=25",
			entityID:    srcURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject relationship_limit mixed with format simplified",
			rawQuery:    "relationship_limit=25&format=simplified",
			entityID:    srcURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject unknown query parameter on detail",
			rawQuery:    "unknown_param=123",
			entityID:    srcURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject invalid entityID",
			rawQuery:    "",
			entityID:    "not-a-valid-urn",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject empty entityID",
			rawQuery:    "",
			entityID:    "",
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject unknown parameter foo",
			rawQuery:    "foo=bar",
			entityID:    srcURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject repeated relationship_limit",
			rawQuery:    "relationship_limit=50&relationship_limit=50",
			entityID:    srcURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject empty relationship_limit",
			rawQuery:    "relationship_limit=",
			entityID:    srcURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject empty relationship_after",
			rawQuery:    "relationship_after=",
			entityID:    srcURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject relationship_limit 0",
			rawQuery:    "relationship_limit=0",
			entityID:    srcURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject negative relationship_limit",
			rawQuery:    "relationship_limit=-10",
			entityID:    srcURN,
			wantErrCode: "invalid_request",
		},
		{
			name:        "reject relationship_limit exceeding 200",
			rawQuery:    "relationship_limit=201",
			entityID:    srcURN,
			wantErrCode: "invalid_request",
		},
		{
			name:         "reject invalid cursor in relationship_after",
			rawQuery:     "relationship_after=invalid-cursor",
			entityID:     srcURN,
			wantErrCode:  "invalid_cursor",
			wantSentinel: digitaltwin.ErrInvalidCursor,
		},
		{
			name:         "reject cursor source mismatch with path entityID",
			rawQuery:     "relationship_after=" + validRelCursor,
			entityID:     "urn:ngsi-ld:Gateway:gw_other",
			wantErrCode:  "invalid_cursor",
			wantSentinel: digitaltwin.ErrInvalidCursor,
		},
		{
			name:        "reject raw query exceeding 1024 bytes",
			rawQuery:    "relationship_after=" + strings.Repeat("x", 1020),
			entityID:    srcURN,
			wantErrCode: "invalid_request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseTwinDetailQuery(tt.rawQuery, tt.entityID)
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
					t.Errorf("got error %v, want sentinel %v", err, tt.wantSentinel)
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

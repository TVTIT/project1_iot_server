package digitaltwin_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"iot-platform/internal/digitaltwin"
)

func TestParseEntityURN(t *testing.T) {
	longest64 := strings.Repeat("a", 64)
	longestGatewayURN := "urn:ngsi-ld:Gateway:" + longest64
	longestSensorURN := "urn:ngsi-ld:Sensor:" + longest64 + ":" + longest64

	tests := []struct {
		name          string
		input         string
		wantErr       bool
		wantType      digitaltwin.EntityType
		wantGatewayID string
		wantSensorID  string
		wantURN       string
	}{
		{
			name:          "valid Gateway URN",
			input:         "urn:ngsi-ld:Gateway:gw_01",
			wantErr:       false,
			wantType:      digitaltwin.EntityTypeGateway,
			wantGatewayID: "gw_01",
			wantSensorID:  "",
			wantURN:       "urn:ngsi-ld:Gateway:gw_01",
		},
		{
			name:          "valid Sensor URN",
			input:         "urn:ngsi-ld:Sensor:gw_01:sensor_01",
			wantErr:       false,
			wantType:      digitaltwin.EntityTypeSensor,
			wantGatewayID: "gw_01",
			wantSensorID:  "sensor_01",
			wantURN:       "urn:ngsi-ld:Sensor:gw_01:sensor_01",
		},
		{
			name:          "two Gateways with same sensor ID",
			input:         "urn:ngsi-ld:Sensor:gw_02:sensor_01",
			wantErr:       false,
			wantType:      digitaltwin.EntityTypeSensor,
			wantGatewayID: "gw_02",
			wantSensorID:  "sensor_01",
			wantURN:       "urn:ngsi-ld:Sensor:gw_02:sensor_01",
		},
		{
			name:          "longest valid Gateway URN (64 chars ID)",
			input:         longestGatewayURN,
			wantErr:       false,
			wantType:      digitaltwin.EntityTypeGateway,
			wantGatewayID: longest64,
			wantSensorID:  "",
			wantURN:       longestGatewayURN,
		},
		{
			name:          "longest valid Sensor URN (two 64 chars IDs)",
			input:         longestSensorURN,
			wantErr:       false,
			wantType:      digitaltwin.EntityTypeSensor,
			wantGatewayID: longest64,
			wantSensorID:  longest64,
			wantURN:       longestSensorURN,
		},
		{
			name:    "reject empty input",
			input:   "",
			wantErr: true,
		},
		{
			name:    "reject oversized URN (> 160 bytes)",
			input:   "urn:ngsi-ld:Sensor:" + strings.Repeat("x", 80) + ":" + strings.Repeat("y", 80), // > 160
			wantErr: true,
		},
		{
			name:    "reject lowercase gateway in prefix",
			input:   "urn:ngsi-ld:gateway:gw_01",
			wantErr: true,
		},
		{
			name:    "reject uppercase SENSOR in prefix",
			input:   "urn:ngsi-ld:SENSOR:gw_01:sensor_01",
			wantErr: true,
		},
		{
			name:    "reject non-matching prefix",
			input:   "urn:ngsi-ld:Device:dev_01",
			wantErr: true,
		},
		{
			name:    "reject missing prefix",
			input:   "Gateway:gw_01",
			wantErr: true,
		},
		{
			name:    "reject UUID input",
			input:   "0195e18c-9fc1-7a42-9064-69ea49e63bf2",
			wantErr: true,
		},
		{
			name:    "reject reserved backend_service Gateway",
			input:   "urn:ngsi-ld:Gateway:backend_service",
			wantErr: true,
		},
		{
			name:    "reject reserved backend_service in Sensor Gateway part",
			input:   "urn:ngsi-ld:Sensor:backend_service:sensor_01",
			wantErr: true,
		},
		{
			name:    "reject extra colon in Gateway URN",
			input:   "urn:ngsi-ld:Gateway:gw_01:extra",
			wantErr: true,
		},
		{
			name:    "reject extra colon in Sensor URN",
			input:   "urn:ngsi-ld:Sensor:gw_01:sensor_01:extra",
			wantErr: true,
		},
		{
			name:    "reject missing component in Gateway URN",
			input:   "urn:ngsi-ld:Gateway:",
			wantErr: true,
		},
		{
			name:    "reject missing sensor component in Sensor URN",
			input:   "urn:ngsi-ld:Sensor:gw_01:",
			wantErr: true,
		},
		{
			name:    "reject missing gateway component in Sensor URN",
			input:   "urn:ngsi-ld:Sensor::sensor_01",
			wantErr: true,
		},
		{
			name:    "reject invalid characters in gateway ID",
			input:   "urn:ngsi-ld:Gateway:gw@01",
			wantErr: true,
		},
		{
			name:    "reject invalid characters in sensor ID",
			input:   "urn:ngsi-ld:Sensor:gw_01:sensor 01",
			wantErr: true,
		},
		{
			name:    "no second unescape - percent-encoded string rejected as invalid ID",
			input:   "urn:ngsi-ld:Gateway:gw%3A01",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := digitaltwin.ParseEntityURN(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseEntityURN(%q) succeeded, want error", tt.input)
				}
				if !errors.Is(err, digitaltwin.ErrInvalidInput) {
					t.Errorf("ParseEntityURN(%q) err = %v, want ErrInvalidInput", tt.input, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseEntityURN(%q) unexpected error: %v", tt.input, err)
			}
			if parsed.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", parsed.Type, tt.wantType)
			}
			if parsed.GatewayID != tt.wantGatewayID {
				t.Errorf("GatewayID = %q, want %q", parsed.GatewayID, tt.wantGatewayID)
			}
			if parsed.SensorID != tt.wantSensorID {
				t.Errorf("SensorID = %q, want %q", parsed.SensorID, tt.wantSensorID)
			}
			if parsed.URN != tt.wantURN {
				t.Errorf("URN = %q, want %q", parsed.URN, tt.wantURN)
			}
		})
	}
}

// TestEncodedPathHelper_DecodeOnce simulates net/http path parameter decoding once,
// verifying that single URL-encoded path parses, while double-encoded path fails.
func TestEncodedPathHelper_DecodeOnce(t *testing.T) {
	// In Stage 4 contract 7.2, client URL-encodes {entity_id} in path (e.g. %3A for colons):
	// /v1/digital-twins/urn%3Angsi-ld%3AGateway%3Agw_01
	singleEncoded := "urn%3Angsi-ld%3AGateway%3Agw_01"
	// Double encoded: % is escaped as %25: urn%253Angsi-ld%253AGateway%253Agw_01
	doubleEncoded := "urn%253Angsi-ld%253AGateway%253Agw_01"

	// Helper simulating net/http unescape once:
	decodeOnce := func(pathParam string) (string, error) {
		req, err := http.NewRequest(http.MethodGet, "/v1/digital-twins/"+pathParam, nil)
		if err != nil {
			return "", err
		}
		// req.URL.Path is decoded once by net/http
		return strings.TrimPrefix(req.URL.Path, "/v1/digital-twins/"), nil
	}

	// 1. Single-encoded: decodes to "urn:ngsi-ld:Gateway:gw_01" -> ParseEntityURN succeeds
	decodedOnce, err := decodeOnce(singleEncoded)
	if err != nil {
		t.Fatalf("failed to decode single-encoded path: %v", err)
	}
	parsed, err := digitaltwin.ParseEntityURN(decodedOnce)
	if err != nil {
		t.Fatalf("ParseEntityURN on single-decoded path failed: %v", err)
	}
	if parsed.Type != digitaltwin.EntityTypeGateway || parsed.GatewayID != "gw_01" {
		t.Errorf("unexpected parsed result: %+v", parsed)
	}

	// 2. Double-encoded: decodes once to "urn%3Angsi-ld%3AGateway%3Agw_01" -> ParseEntityURN MUST FAIL
	decodedDouble, err := decodeOnce(doubleEncoded)
	if err != nil {
		t.Fatalf("failed to decode double-encoded path: %v", err)
	}
	_, err = digitaltwin.ParseEntityURN(decodedDouble)
	if err == nil {
		t.Fatalf("ParseEntityURN on double-encoded path succeeded, expected failure without second unescape")
	}
	if !errors.Is(err, digitaltwin.ErrInvalidInput) {
		t.Errorf("expected ErrInvalidInput, got %v", err)
	}
}

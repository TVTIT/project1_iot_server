package digitaltwin_test

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"

	"iot-platform/internal/digitaltwin"
)

func TestListCursor_EncodeDecode_RoundTrip(t *testing.T) {
	gwID := "gw_01"
	sensorType := digitaltwin.EntityTypeSensor
	gatewayType := digitaltwin.EntityTypeGateway
	sensorURN := "urn:ngsi-ld:Sensor:gw_01:sensor_01"
	gatewayURN := "urn:ngsi-ld:Gateway:gw_01"

	t.Run("no filters (g=null, t=null)", func(t *testing.T) {
		encoded, err := digitaltwin.EncodeListCursor(sensorURN, nil, nil)
		if err != nil {
			t.Fatalf("EncodeListCursor failed: %v", err)
		}
		if !strings.HasPrefix(encoded, "v1.") {
			t.Errorf("encoded cursor = %q, want v1. prefix", encoded)
		}

		decoded, err := digitaltwin.ParseListCursor(encoded, nil, nil)
		if err != nil {
			t.Fatalf("ParseListCursor failed: %v", err)
		}
		if decoded.EntityID != sensorURN {
			t.Errorf("EntityID = %q, want %q", decoded.EntityID, sensorURN)
		}
		if decoded.GatewayID != nil {
			t.Errorf("GatewayID = %v, want nil", *decoded.GatewayID)
		}
		if decoded.EntityType != nil {
			t.Errorf("EntityType = %v, want nil", *decoded.EntityType)
		}
	})

	t.Run("with gateway filter", func(t *testing.T) {
		encoded, err := digitaltwin.EncodeListCursor(sensorURN, &gwID, nil)
		if err != nil {
			t.Fatalf("EncodeListCursor failed: %v", err)
		}
		decoded, err := digitaltwin.ParseListCursor(encoded, &gwID, nil)
		if err != nil {
			t.Fatalf("ParseListCursor failed: %v", err)
		}
		if decoded.EntityID != sensorURN {
			t.Errorf("EntityID = %q, want %q", decoded.EntityID, sensorURN)
		}
		if decoded.GatewayID == nil || *decoded.GatewayID != gwID {
			t.Errorf("GatewayID = %v, want %q", decoded.GatewayID, gwID)
		}
		if decoded.EntityType != nil {
			t.Errorf("EntityType = %v, want nil", *decoded.EntityType)
		}
	})

	t.Run("with entity_type filter Sensor", func(t *testing.T) {
		encoded, err := digitaltwin.EncodeListCursor(sensorURN, nil, &sensorType)
		if err != nil {
			t.Fatalf("EncodeListCursor failed: %v", err)
		}
		decoded, err := digitaltwin.ParseListCursor(encoded, nil, &sensorType)
		if err != nil {
			t.Fatalf("ParseListCursor failed: %v", err)
		}
		if decoded.EntityType == nil || *decoded.EntityType != sensorType {
			t.Errorf("EntityType = %v, want %v", decoded.EntityType, sensorType)
		}
	})

	t.Run("with entity_type filter Gateway", func(t *testing.T) {
		encoded, err := digitaltwin.EncodeListCursor(gatewayURN, nil, &gatewayType)
		if err != nil {
			t.Fatalf("EncodeListCursor failed: %v", err)
		}
		decoded, err := digitaltwin.ParseListCursor(encoded, nil, &gatewayType)
		if err != nil {
			t.Fatalf("ParseListCursor failed: %v", err)
		}
		if decoded.EntityType == nil || *decoded.EntityType != gatewayType {
			t.Errorf("EntityType = %v, want %v", decoded.EntityType, gatewayType)
		}
	})

	t.Run("with both filters", func(t *testing.T) {
		encoded, err := digitaltwin.EncodeListCursor(sensorURN, &gwID, &sensorType)
		if err != nil {
			t.Fatalf("EncodeListCursor failed: %v", err)
		}
		decoded, err := digitaltwin.ParseListCursor(encoded, &gwID, &sensorType)
		if err != nil {
			t.Fatalf("ParseListCursor failed: %v", err)
		}
		if decoded.GatewayID == nil || *decoded.GatewayID != gwID {
			t.Errorf("GatewayID = %v, want %q", decoded.GatewayID, gwID)
		}
		if decoded.EntityType == nil || *decoded.EntityType != sensorType {
			t.Errorf("EntityType = %v, want %v", decoded.EntityType, sensorType)
		}
	})

	t.Run("longest 64-char IDs roundtrip and bounds check", func(t *testing.T) {
		longGW := strings.Repeat("g", 64)
		longSen := strings.Repeat("s", 64)
		longURN := fmt.Sprintf("urn:ngsi-ld:Sensor:%s:%s", longGW, longSen)

		encoded, err := digitaltwin.EncodeListCursor(longURN, &longGW, &sensorType)
		if err != nil {
			t.Fatalf("EncodeListCursor failed with longest IDs: %v", err)
		}

		if len(encoded) > digitaltwin.MaxEncodedCursorLength {
			t.Errorf("encoded length = %d, exceeds max %d", len(encoded), digitaltwin.MaxEncodedCursorLength)
		}

		decoded, err := digitaltwin.ParseListCursor(encoded, &longGW, &sensorType)
		if err != nil {
			t.Fatalf("ParseListCursor failed with longest IDs: %v", err)
		}
		if decoded.EntityID != longURN {
			t.Errorf("EntityID = %q, want %q", decoded.EntityID, longURN)
		}
	})
}

func TestListCursor_ValidationErrors(t *testing.T) {
	gwID := "gw_01"
	sensorType := digitaltwin.EntityTypeSensor
	sensorURN := "urn:ngsi-ld:Sensor:gw_01:sensor_01"

	encodeJSON := func(jsonStr string) string {
		return "v1." + base64.RawURLEncoding.EncodeToString([]byte(jsonStr))
	}

	tests := []struct {
		name       string
		cursor     string
		filterGW   *string
		filterType *digitaltwin.EntityType
	}{
		{
			name:   "reject missing version prefix",
			cursor: base64.RawURLEncoding.EncodeToString([]byte(`{"k":"` + sensorURN + `","g":null,"t":null}`)),
		},
		{
			name:   "reject wrong version prefix v2.",
			cursor: "v2." + base64.RawURLEncoding.EncodeToString([]byte(`{"k":"`+sensorURN+`","g":null,"t":null}`)),
		},
		{
			name:   "reject base64 with padding =",
			cursor: "v1." + base64.RawURLEncoding.EncodeToString([]byte(`{"k":"`+sensorURN+`","g":null,"t":null}`)) + "=",
		},
		{
			name:   "reject invalid base64 characters",
			cursor: "v1.!!invalid-base64!!",
		},
		{
			name:   "reject invalid UTF-8 bytes in decoded JSON",
			cursor: "v1." + base64.RawURLEncoding.EncodeToString([]byte{0xff, 0xfe, 0xfd}),
		},
		{
			name:   "reject oversized encoded cursor (> 768 bytes)",
			cursor: "v1." + strings.Repeat("A", digitaltwin.MaxEncodedCursorLength),
		},
		{
			name:   "reject oversized decoded JSON (> 512 bytes)",
			cursor: "v1." + base64.RawURLEncoding.EncodeToString([]byte(`{"k":"`+strings.Repeat("a", 500)+`","g":null,"t":null}`)),
		},
		{
			name:   "reject duplicate key k",
			cursor: encodeJSON(`{"k":"` + sensorURN + `","k":"` + sensorURN + `","g":null,"t":null}`),
		},
		{
			name:   "reject duplicate key g",
			cursor: encodeJSON(`{"k":"` + sensorURN + `","g":null,"g":null,"t":null}`),
		},
		{
			name:   "reject unknown key",
			cursor: encodeJSON(`{"k":"` + sensorURN + `","g":null,"t":null,"extra":"val"}`),
		},
		{
			name:   "reject missing key t",
			cursor: encodeJSON(`{"k":"` + sensorURN + `","g":null}`),
		},
		{
			name:   "reject wrong type for k (number)",
			cursor: encodeJSON(`{"k":12345,"g":null,"t":null}`),
		},
		{
			name:   "reject wrong type for k (null)",
			cursor: encodeJSON(`{"k":null,"g":null,"t":null}`),
		},
		{
			name:   "reject wrong type for g (number)",
			cursor: encodeJSON(`{"k":"` + sensorURN + `","g":123,"t":null}`),
		},
		{
			name:   "reject wrong type for t (number)",
			cursor: encodeJSON(`{"k":"` + sensorURN + `","g":null,"t":123}`),
		},
		{
			name:   "reject extra JSON document",
			cursor: encodeJSON(`{"k":"` + sensorURN + `","g":null,"t":null} {"extra":"doc"}`),
		},
		{
			name:   "reject extra trailing token",
			cursor: encodeJSON(`{"k":"` + sensorURN + `","g":null,"t":null} 123`),
		},
		{
			name:   "reject raw NUL byte",
			cursor: "v1." + base64.RawURLEncoding.EncodeToString([]byte("{\"k\":\""+sensorURN+"\",\x00\"g\":null,\"t\":null}")),
		},
		{
			name:   "reject escaped NUL byte in string",
			cursor: encodeJSON(`{"k":"urn:ngsi-ld:Sensor:gw_01:\u0000sen","g":null,"t":null}`),
		},
		{
			name:   "reject surrogate escape in string",
			cursor: encodeJSON(`{"k":"urn:ngsi-ld:Sensor:gw_01:\uD800sen","g":null,"t":null}`),
		},
		{
			name:   "reject invalid public URN in k",
			cursor: encodeJSON(`{"k":"not-a-urn","g":null,"t":null}`),
		},
		{
			name:   "reject UUID in k",
			cursor: encodeJSON(`{"k":"0195e18c-9fc1-7a42-9064-69ea49e63bf2","g":null,"t":null}`),
		},
		{
			name:       "reject filter mismatch: cursor has g=gw_01 but filter is absent",
			cursor:     encodeJSON(`{"k":"` + sensorURN + `","g":"gw_01","t":null}`),
			filterGW:   nil,
			filterType: nil,
		},
		{
			name:       "reject filter mismatch: cursor has g=null but filter is gw_01",
			cursor:     encodeJSON(`{"k":"` + sensorURN + `","g":null,"t":null}`),
			filterGW:   &gwID,
			filterType: nil,
		},
		{
			name:       "reject filter mismatch: cursor has g=gw_01 but filter is gw_02",
			cursor:     encodeJSON(`{"k":"` + sensorURN + `","g":"gw_01","t":null}`),
			filterGW:   func() *string { s := "gw_02"; return &s }(),
			filterType: nil,
		},
		{
			name:       "reject filter mismatch: cursor has t=Sensor but filter is absent",
			cursor:     encodeJSON(`{"k":"` + sensorURN + `","g":null,"t":"Sensor"}`),
			filterGW:   nil,
			filterType: nil,
		},
		{
			name:       "reject filter mismatch: cursor has t=null but filter is Sensor",
			cursor:     encodeJSON(`{"k":"` + sensorURN + `","g":null,"t":null}`),
			filterGW:   nil,
			filterType: &sensorType,
		},
		{
			name:       "reject filter mismatch: cursor has t=Gateway but filter is Sensor",
			cursor:     encodeJSON(`{"k":"urn:ngsi-ld:Gateway:gw_01","g":null,"t":"Gateway"}`),
			filterGW:   nil,
			filterType: &sensorType,
		},
		{
			name:       "reject k URN type mismatch with filter",
			cursor:     encodeJSON(`{"k":"urn:ngsi-ld:Gateway:gw_01","g":null,"t":"Sensor"}`),
			filterGW:   nil,
			filterType: &sensorType,
		},
		{
			name:       "reject k URN gateway mismatch with filter",
			cursor:     encodeJSON(`{"k":"urn:ngsi-ld:Sensor:gw_02:sensor_01","g":"gw_01","t":null}`),
			filterGW:   &gwID,
			filterType: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := digitaltwin.ParseListCursor(tt.cursor, tt.filterGW, tt.filterType)
			if err == nil {
				t.Fatalf("ParseListCursor(%q) succeeded, want ErrInvalidCursor", tt.cursor)
			}
			if !errors.Is(err, digitaltwin.ErrInvalidCursor) {
				t.Errorf("ParseListCursor(%q) err = %v, want ErrInvalidCursor", tt.cursor, err)
			}
		})
	}
}

func TestRelationshipCursor_EncodeDecode_RoundTrip(t *testing.T) {
	srcURN := "urn:ngsi-ld:Gateway:gw_01"
	targetURN := "urn:ngsi-ld:Sensor:gw_01:sensor_01"

	t.Run("valid relationship cursor roundtrip", func(t *testing.T) {
		encoded, err := digitaltwin.EncodeRelationshipCursor(srcURN, "hasSensor", targetURN)
		if err != nil {
			t.Fatalf("EncodeRelationshipCursor failed: %v", err)
		}
		if !strings.HasPrefix(encoded, "v1.") {
			t.Errorf("encoded = %q, want v1. prefix", encoded)
		}

		decoded, err := digitaltwin.ParseRelationshipCursor(encoded, srcURN)
		if err != nil {
			t.Fatalf("ParseRelationshipCursor failed: %v", err)
		}
		if decoded.SourceURN != srcURN {
			t.Errorf("SourceURN = %q, want %q", decoded.SourceURN, srcURN)
		}
		if decoded.RelType != "hasSensor" {
			t.Errorf("RelType = %q, want hasSensor", decoded.RelType)
		}
		if decoded.TargetURN != targetURN {
			t.Errorf("TargetURN = %q, want %q", decoded.TargetURN, targetURN)
		}
	})

	t.Run("longest 64-char IDs roundtrip and bounds check", func(t *testing.T) {
		longGW := strings.Repeat("g", 64)
		longSen := strings.Repeat("s", 64)
		longSrc := "urn:ngsi-ld:Gateway:" + longGW
		longTgt := fmt.Sprintf("urn:ngsi-ld:Sensor:%s:%s", longGW, longSen)

		encoded, err := digitaltwin.EncodeRelationshipCursor(longSrc, "hasSensor", longTgt)
		if err != nil {
			t.Fatalf("EncodeRelationshipCursor with longest IDs failed: %v", err)
		}
		if len(encoded) > digitaltwin.MaxEncodedCursorLength {
			t.Errorf("encoded length %d exceeds max %d", len(encoded), digitaltwin.MaxEncodedCursorLength)
		}

		decoded, err := digitaltwin.ParseRelationshipCursor(encoded, longSrc)
		if err != nil {
			t.Fatalf("ParseRelationshipCursor with longest IDs failed: %v", err)
		}
		if decoded.SourceURN != longSrc || decoded.TargetURN != longTgt {
			t.Errorf("unexpected decoded result: %+v", decoded)
		}
	})
}

func TestRelationshipCursor_ValidationErrors(t *testing.T) {
	srcURN := "urn:ngsi-ld:Gateway:gw_01"
	targetURN := "urn:ngsi-ld:Sensor:gw_01:sensor_01"

	encodeJSON := func(jsonStr string) string {
		return "v1." + base64.RawURLEncoding.EncodeToString([]byte(jsonStr))
	}

	tests := []struct {
		name       string
		cursor     string
		requestSrc string
	}{
		{
			name:       "reject wrong version prefix",
			cursor:     "v2." + base64.RawURLEncoding.EncodeToString([]byte(`{"s":"`+srcURN+`","r":"hasSensor","k":"`+targetURN+`"}`)),
			requestSrc: srcURN,
		},
		{
			name:       "reject base64 padding =",
			cursor:     "v1." + base64.RawURLEncoding.EncodeToString([]byte(`{"s":"`+srcURN+`","r":"hasSensor","k":"`+targetURN+`"}`)) + "=",
			requestSrc: srcURN,
		},
		{
			name:       "reject source mismatch (cursor s != requested source)",
			cursor:     encodeJSON(`{"s":"urn:ngsi-ld:Gateway:gw_02","r":"hasSensor","k":"urn:ngsi-ld:Sensor:gw_02:sensor_01"}`),
			requestSrc: srcURN,
		},
		{
			name:       "reject rel type not hasSensor",
			cursor:     encodeJSON(`{"s":"` + srcURN + `","r":"connectedTo","k":"` + targetURN + `"}`),
			requestSrc: srcURN,
		},
		{
			name:       "reject target not in same Gateway",
			cursor:     encodeJSON(`{"s":"` + srcURN + `","r":"hasSensor","k":"urn:ngsi-ld:Sensor:gw_02:sensor_01"}`),
			requestSrc: srcURN,
		},
		{
			name:       "reject target is Gateway URN instead of Sensor",
			cursor:     encodeJSON(`{"s":"` + srcURN + `","r":"hasSensor","k":"urn:ngsi-ld:Gateway:gw_01"}`),
			requestSrc: srcURN,
		},
		{
			name:       "reject source is Sensor URN instead of Gateway",
			cursor:     encodeJSON(`{"s":"urn:ngsi-ld:Sensor:gw_01:s1","r":"hasSensor","k":"` + targetURN + `"}`),
			requestSrc: "urn:ngsi-ld:Sensor:gw_01:s1",
		},
		{
			name:       "reject UUID target in k",
			cursor:     encodeJSON(`{"s":"` + srcURN + `","r":"hasSensor","k":"0195e18c-9fc1-7a42-9064-69ea49e63bf2"}`),
			requestSrc: srcURN,
		},
		{
			name:       "reject duplicate key s",
			cursor:     encodeJSON(`{"s":"` + srcURN + `","s":"` + srcURN + `","r":"hasSensor","k":"` + targetURN + `"}`),
			requestSrc: srcURN,
		},
		{
			name:       "reject missing key r",
			cursor:     encodeJSON(`{"s":"` + srcURN + `","k":"` + targetURN + `"}`),
			requestSrc: srcURN,
		},
		{
			name:       "reject unknown key x",
			cursor:     encodeJSON(`{"s":"` + srcURN + `","r":"hasSensor","k":"` + targetURN + `","x":"extra"}`),
			requestSrc: srcURN,
		},
		{
			name:       "reject null value for s",
			cursor:     encodeJSON(`{"s":null,"r":"hasSensor","k":"` + targetURN + `"}`),
			requestSrc: srcURN,
		},
		{
			name:       "reject null value for r",
			cursor:     encodeJSON(`{"s":"` + srcURN + `","r":null,"k":"` + targetURN + `"}`),
			requestSrc: srcURN,
		},
		{
			name:       "reject null value for k",
			cursor:     encodeJSON(`{"s":"` + srcURN + `","r":"hasSensor","k":null}`),
			requestSrc: srcURN,
		},
		{
			name:       "reject extra document",
			cursor:     encodeJSON(`{"s":"` + srcURN + `","r":"hasSensor","k":"` + targetURN + `"} {"second":"doc"}`),
			requestSrc: srcURN,
		},
		{
			name:       "reject raw NUL byte",
			cursor:     "v1." + base64.RawURLEncoding.EncodeToString([]byte("{\"s\":\""+srcURN+"\",\x00\"r\":\"hasSensor\",\"k\":\""+targetURN+"\"}")),
			requestSrc: srcURN,
		},
		{
			name:       "reject surrogate escape",
			cursor:     encodeJSON(`{"s":"` + srcURN + `","r":"hasSensor","k":"urn:ngsi-ld:Sensor:gw_01:\uD800sen"}`),
			requestSrc: srcURN,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := digitaltwin.ParseRelationshipCursor(tt.cursor, tt.requestSrc)
			if err == nil {
				t.Fatalf("ParseRelationshipCursor(%q) succeeded, want ErrInvalidCursor", tt.cursor)
			}
			if !errors.Is(err, digitaltwin.ErrInvalidCursor) {
				t.Errorf("ParseRelationshipCursor(%q) err = %v, want ErrInvalidCursor", tt.cursor, err)
			}
		})
	}
}

func TestCursor_NamespaceMismatch(t *testing.T) {
	srcURN := "urn:ngsi-ld:Gateway:gw_01"
	targetURN := "urn:ngsi-ld:Sensor:gw_01:sensor_01"

	listCursor, err := digitaltwin.EncodeListCursor(targetURN, nil, nil)
	if err != nil {
		t.Fatalf("failed to encode list cursor: %v", err)
	}

	relCursor, err := digitaltwin.EncodeRelationshipCursor(srcURN, "hasSensor", targetURN)
	if err != nil {
		t.Fatalf("failed to encode relationship cursor: %v", err)
	}

	// Passing list cursor to ParseRelationshipCursor must fail
	_, err = digitaltwin.ParseRelationshipCursor(listCursor, srcURN)
	if err == nil {
		t.Errorf("ParseRelationshipCursor accepted list cursor, want ErrInvalidCursor")
	}
	if !errors.Is(err, digitaltwin.ErrInvalidCursor) {
		t.Errorf("err = %v, want ErrInvalidCursor", err)
	}

	// Passing relationship cursor to ParseListCursor must fail
	_, err = digitaltwin.ParseListCursor(relCursor, nil, nil)
	if err == nil {
		t.Errorf("ParseListCursor accepted relationship cursor, want ErrInvalidCursor")
	}
	if !errors.Is(err, digitaltwin.ErrInvalidCursor) {
		t.Errorf("err = %v, want ErrInvalidCursor", err)
	}
}

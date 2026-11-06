package telemetryws

import (
	"strings"
	"testing"
)

// E1 manifests reserve 8192 bytes per event: twice the 4096-byte bound for a
// 100-sample synthetic batch at supported identifier/provenance boundaries.
func TestE1SyntheticHundredSampleFrameFitsDocumentedBound(t *testing.T) {
	const frameBound = 4096
	samples := make([]float64, 100)
	for i := range samples {
		// The Gateway emits 20 + position/100 on a 2000-sample triangular
		// wave. Alternate near both turning points to exercise decimal JSON
		// encodings (20.01 and 29.99), not the shorter integer endpoints.
		position := int64(1)
		if i%2 == 0 {
			position = 999
		}
		samples[i] = 20 + float64(position)/100
	}
	dto := TelemetryEventDTO{
		ProtocolVersion: ProtocolVersion, MessageType: MessageTypeTelemetry,
		EntityID:  "",
		GatewayID: "",
		SensorID:  "s", MessageID: "00000000-0000-4000-8000-000000000001",
		BootID:        "00000000-0000-4000-8000-000000000002",
		FirstSequence: "9223372036854775807", SampleCount: 100,
		MeasuredAt:       "9999-12-31T23:59:59.999999999Z",
		SampleIntervalUS: 9007199254740992, Samples: samples,
	}
	// Use maximum supported ASCII ID lengths, and make entity_id consistent.
	dto.GatewayID = "g" + strings.Repeat("a", 63)
	dto.SensorID = "s" + strings.Repeat("a", 63)
	dto.EntityID = "urn:ngsi-ld:Sensor:" + dto.GatewayID + ":" + dto.SensorID
	event, err := EncodeTelemetryEvent(dto, frameBound)
	if err != nil {
		t.Fatal(err)
	}
	if len(event.Raw) > frameBound {
		t.Fatalf("encoded E1 event is %d bytes; bound is %d", len(event.Raw), frameBound)
	}
	t.Logf("100-sample synthetic boundary frame: %d/%d bytes", len(event.Raw), frameBound)
}

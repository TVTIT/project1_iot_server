package telemetry

import "testing"

func TestLedgerMetadataRequiresValidatedSampleRange(t *testing.T) {
	if _, err := ledgerMetadata(nil); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("nil message error = %v, want %s", err, ReasonInvalidPayload)
	}

	validator, err := NewValidator(messageTestLimits())
	if err != nil {
		t.Fatal(err)
	}
	payload := messageTestBatchPayload(t, "2026-08-21T10:15:00Z", 12501, []float64{1, 2, 3})
	message, err := validator.Validate("gateways/gateway_001/telemetry/sensor_001", payload, messageTestReceivedAt())
	if err != nil {
		t.Fatal(err)
	}
	got, err := ledgerMetadata(message)
	if err != nil {
		t.Fatalf("ledgerMetadata(valid message): %v", err)
	}
	if got.topic.SourceTopic != "gateways/gateway_001/telemetry/sensor_001" ||
		got.payloadHash != message.PayloadHash() || got.firstSample.SequenceNum != 12501 ||
		got.lastSample.SequenceNum != 12503 || got.sampleCount != 3 {
		t.Fatalf("ledger metadata = %#v, want exact topic/hash and checked range", got)
	}

	// Corrupt only the private checked candidate snapshot. The guard must reject
	// inconsistent range metadata instead of writing an internally contradictory ledger row.
	malformed := *message
	malformed.samples = append([]SampleCandidate(nil), message.samples...)
	malformed.samples[len(malformed.samples)-1].SequenceNum++
	if _, err := ledgerMetadata(&malformed); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("inconsistent sample range error = %v, want %s", err, ReasonInvalidPayload)
	}
}

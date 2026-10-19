package telemetry

import (
	"errors"
	"fmt"
	"testing"
)

func TestClassifyFailure(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		class FailureClass
	}{
		{"nil", nil, FailureNone},
		{"invalid topic", &ReasonError{Code: ReasonInvalidTopic}, FailurePermanentInputConflict},
		{"unsupported protocol wrapped", fmt.Errorf("decode: %w", &ReasonError{Code: ReasonUnsupportedProtocol}), FailurePermanentInputConflict},
		{"payload too large", &ReasonError{Code: ReasonPayloadTooLarge}, FailurePermanentInputConflict},
		{"invalid payload", &ReasonError{Code: ReasonInvalidPayload}, FailurePermanentInputConflict},
		{"invalid timestamp", &ReasonError{Code: ReasonInvalidTimestamp}, FailurePermanentInputConflict},
		{"invalid sequence", &ReasonError{Code: ReasonInvalidSequenceRange}, FailurePermanentInputConflict},
		{"message conflict", &ReasonError{Code: ReasonMessageConflict}, FailurePermanentInputConflict},
		{"sequence overlap", &ReasonError{Code: ReasonSequenceOverlap}, FailurePermanentInputConflict},
		{"timestamp future", &ReasonError{Code: ReasonTimestampInFuture}, FailureCheckClock},
		{"queue full", &ReasonError{Code: ReasonQueueFull}, FailureRetryWithBackoff},
		{"dependency", &ReasonError{Code: ReasonDependencyUnavailable}, FailureRetryWithBackoff},
		{"unknown gateway", &ReasonError{Code: ReasonUnknownGateway}, FailureNeedsIntervention},
		{"unknown sensor", &ReasonError{Code: ReasonUnknownSensor}, FailureNeedsIntervention},
		{"incomplete graph", &ReasonError{Code: ReasonResourceGraphIncomplete}, FailureNeedsIntervention},
		{"legacy provenance", &ReasonError{Code: ReasonLegacyProvenanceUnknown}, FailureNeedsIntervention},
		{"commit uncertain", &ReasonError{Code: ReasonCommitUnconfirmed}, FailureCommitUnconfirmed},
		{"ack transfer uncertain", &ReasonError{Code: ReasonAckPublishUnconfirmed}, FailureAckPublishUnconfirmed},
		{"unknown reason", &ReasonError{Code: "attacker controlled"}, FailureUnknown},
		{"ordinary error", errors.New("failure"), FailureUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ClassifyFailure(test.err); got != test.class {
				t.Fatalf("ClassifyFailure() = %v, want %v", got, test.class)
			}
		})
	}
}

func TestAddedReasonCodesAreSafe(t *testing.T) {
	for _, code := range []ReasonCode{ReasonUnknownGateway, ReasonUnknownSensor, ReasonResourceGraphIncomplete, ReasonLegacyProvenanceUnknown} {
		err := &ReasonError{Code: code, Field: "sensitive-value"}
		if got := err.Error(); got != "telemetry error: "+string(code) {
			t.Fatalf("Error() = %q, want safe stable code", got)
		}
	}
}

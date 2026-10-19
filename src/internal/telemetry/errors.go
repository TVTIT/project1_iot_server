package telemetry

import "errors"

// ReasonCode identifies a stable telemetry parsing or validation failure.
type ReasonCode string

// Telemetry reason codes are safe to expose in application diagnostics.
const (
	ReasonInvalidTopic            ReasonCode = "invalid_topic"
	ReasonUnsupportedProtocol     ReasonCode = "unsupported_protocol"
	ReasonPayloadTooLarge         ReasonCode = "payload_too_large"
	ReasonInvalidPayload          ReasonCode = "invalid_payload"
	ReasonInvalidTimestamp        ReasonCode = "invalid_timestamp"
	ReasonTimestampInFuture       ReasonCode = "timestamp_in_future"
	ReasonInvalidSequenceRange    ReasonCode = "invalid_sequence_range"
	ReasonUnknownGateway          ReasonCode = "unknown_gateway"
	ReasonUnknownSensor           ReasonCode = "unknown_sensor"
	ReasonResourceGraphIncomplete ReasonCode = "resource_graph_incomplete"
	ReasonLegacyProvenanceUnknown ReasonCode = "legacy_provenance_unknown"
	ReasonMessageConflict         ReasonCode = "message_conflict"
	ReasonSequenceOverlap         ReasonCode = "sequence_overlap"
	ReasonQueueFull               ReasonCode = "queue_full"
	ReasonDependencyUnavailable   ReasonCode = "dependency_unavailable"
	ReasonCommitUnconfirmed       ReasonCode = "commit_unconfirmed"
	ReasonAckPublishUnconfirmed   ReasonCode = "ack_publish_unconfirmed"
)

// ReasonError reports a stable reason code and optional field name.
// Its Error string deliberately excludes field values and payload contents.
type ReasonError struct {
	Code  ReasonCode
	Field string
}

// Error returns a payload-safe description of the reason code.
func (e *ReasonError) Error() string {
	if e == nil {
		return "telemetry error"
	}
	if !knownReasonCode(e.Code) {
		return "telemetry error"
	}
	return "telemetry error: " + string(e.Code)
}

func knownReasonCode(code ReasonCode) bool {
	switch code {
	case ReasonInvalidTopic,
		ReasonUnsupportedProtocol,
		ReasonPayloadTooLarge,
		ReasonInvalidPayload,
		ReasonInvalidTimestamp,
		ReasonTimestampInFuture,
		ReasonInvalidSequenceRange,
		ReasonUnknownGateway,
		ReasonUnknownSensor,
		ReasonResourceGraphIncomplete,
		ReasonLegacyProvenanceUnknown,
		ReasonMessageConflict,
		ReasonSequenceOverlap,
		ReasonQueueFull,
		ReasonDependencyUnavailable,
		ReasonCommitUnconfirmed,
		ReasonAckPublishUnconfirmed:
		return true
	default:
		return false
	}
}

// FailureClass describes the handling boundary for a telemetry failure. It is
// a classification only; it does not perform retries, persistence, or ACKs.
type FailureClass uint8

const (
	// FailureNone indicates that no failure was supplied.
	FailureNone FailureClass = iota
	// FailurePermanentInputConflict indicates invalid or conflicting input.
	FailurePermanentInputConflict
	// FailureRetryWithBackoff indicates a retryable dependency or capacity issue.
	FailureRetryWithBackoff
	// FailureCheckClock indicates that the sender or server clock may be wrong.
	FailureCheckClock
	// FailureNeedsIntervention indicates missing or incomplete resource data.
	FailureNeedsIntervention
	// FailureCommitUnconfirmed indicates that the database outcome is uncertain.
	FailureCommitUnconfirmed
	// FailureAckPublishUnconfirmed indicates that publishing the ACK is uncertain.
	FailureAckPublishUnconfirmed
	// FailureUnknown indicates that the error has no known telemetry classification.
	FailureUnknown
)

// ClassifyFailure classifies known reason errors, including errors wrapped by
// callers. Unknown errors fail closed as FailureUnknown.
func ClassifyFailure(err error) FailureClass {
	if err == nil {
		return FailureNone
	}
	var reason *ReasonError
	if !errors.As(err, &reason) || reason == nil {
		return FailureUnknown
	}
	switch reason.Code {
	case ReasonInvalidTopic, ReasonUnsupportedProtocol, ReasonPayloadTooLarge,
		ReasonInvalidPayload, ReasonInvalidTimestamp, ReasonInvalidSequenceRange,
		ReasonMessageConflict, ReasonSequenceOverlap:
		return FailurePermanentInputConflict
	case ReasonQueueFull, ReasonDependencyUnavailable:
		return FailureRetryWithBackoff
	case ReasonTimestampInFuture:
		return FailureCheckClock
	case ReasonUnknownGateway, ReasonUnknownSensor, ReasonResourceGraphIncomplete,
		ReasonLegacyProvenanceUnknown:
		return FailureNeedsIntervention
	case ReasonCommitUnconfirmed:
		return FailureCommitUnconfirmed
	case ReasonAckPublishUnconfirmed:
		return FailureAckPublishUnconfirmed
	default:
		return FailureUnknown
	}
}

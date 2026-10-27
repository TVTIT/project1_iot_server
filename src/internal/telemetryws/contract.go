// Package telemetryws defines the WebSocket client subscription, server acknowledgment,
// error, and committed-batch telemetry event contracts.
package telemetryws

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
)

// ProtocolVersion is the required version for client and server WebSocket messages.
const ProtocolVersion int64 = 1

// Action types supported in client messages.
const (
	ActionSubscribe   = "subscribe"
	ActionUnsubscribe = "unsubscribe"
)

// Message types emitted by server and client.
const (
	MessageTypeAck         = "ack"
	MessageTypeError       = "error"
	MessageTypeTelemetry   = "telemetry"
	MessageTypeGap         = "gap"
	MessageTypeSubscribe   = "subscribe"
	MessageTypeUnsubscribe = "unsubscribe"
)

// Bounded protocol sizing and rate-limiting limits.
const (
	// MaxClientMessageBytes bounds the incoming text frame size for control messages.
	MaxClientMessageBytes = 4096 // 4 KiB

	// MaxTargetsPerMessage bounds the number of subscription targets per subscribe/unsubscribe request.
	MaxTargetsPerMessage = 16

	// MaxSubscriptionsPerSession bounds the active subscription targets per WebSocket session.
	MaxSubscriptionsPerSession = 16

	// MaxRequestIDLength bounds the client request correlation identifier.
	MaxRequestIDLength = 64

	// DefaultMaxClientCommandRate bounds incoming control frames per second per session.
	DefaultMaxClientCommandRate = 10

	// MaxEnvelopeOverheadBytes is a representative envelope allowance, not a
	// mathematical bound: JSON float canonicalization can expand compact input values.
	MaxEnvelopeOverheadBytes = 1024

	// DefaultMaxEventBytes is the default WebSocket frame cap. A legal MQTT batch
	// whose canonical JSON representation exceeds it is treated as a stream gap and
	// causes fail-closed session resynchronization; this is not full legal-input coverage.
	DefaultMaxEventBytes = 65536 + MaxEnvelopeOverheadBytes // 66,560 bytes

	// MaxSupportedEventBytes accommodates the maximum supported MQTT payload limit (1 MiB)
	// plus envelope overhead.
	MaxSupportedEventBytes = (1 << 20) + 4096 // 1,052,672 bytes
)

// ErrorCode identifies the error category for server error responses.
type ErrorCode string

// Error codes emitted in ErrorMessage responses.
const (
	ErrorCodeInvalidMessage      ErrorCode = "INVALID_MESSAGE"
	ErrorCodeUnsupportedProtocol ErrorCode = "UNSUPPORTED_PROTOCOL"
	ErrorCodeMessageTooLarge     ErrorCode = "MESSAGE_TOO_LARGE"
	ErrorCodeInvalidAction       ErrorCode = "INVALID_ACTION"
	ErrorCodeInvalidTarget       ErrorCode = "INVALID_TARGET"
	ErrorCodeTargetLimitExceeded ErrorCode = "TARGET_LIMIT_EXCEEDED"
	ErrorCodeRateLimitExceeded   ErrorCode = "RATE_LIMIT_EXCEEDED"
	ErrorCodeUnauthorized        ErrorCode = "UNAUTHORIZED"
	ErrorCodeNotFound            ErrorCode = "NOT_FOUND"
	ErrorCodeInternalError       ErrorCode = "INTERNAL_ERROR"
)

// ProtocolError represents a typed contract or parsing violation.
type ProtocolError struct {
	Code    ErrorCode
	Field   string
	Message string
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return ""
	}
	if e.Field != "" {
		return fmt.Sprintf("%s: %s (field: %s)", e.Code, e.Message, e.Field)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// TargetKey is the canonical target identifier for a subscription filter.
// SensorID is optional; when empty, the filter matches all sensors on GatewayID.
type TargetKey struct {
	GatewayID string `json:"gateway_id"`
	SensorID  string `json:"sensor_id,omitempty"`
}

// IsSensor reports whether the target references a specific composite Sensor.
func (k TargetKey) IsSensor() bool {
	return k.SensorID != ""
}

// EntityURN returns the public NGSI-LD URN representation.
func (k TargetKey) EntityURN() string {
	if k.SensorID != "" {
		return fmt.Sprintf("urn:ngsi-ld:Sensor:%s:%s", k.GatewayID, k.SensorID)
	}
	return fmt.Sprintf("urn:ngsi-ld:Gateway:%s", k.GatewayID)
}

// String returns a compact representation.
func (k TargetKey) String() string {
	if k.SensorID != "" {
		return fmt.Sprintf("%s:%s", k.GatewayID, k.SensorID)
	}
	return k.GatewayID
}

// Matches evaluates whether this target filter matches a telemetry event from (gatewayID, sensorID).
func (k TargetKey) Matches(gatewayID, sensorID string) bool {
	if k.GatewayID != gatewayID {
		return false
	}
	if k.SensorID == "" {
		return true // gateway-level subscription matches all sensors
	}
	return k.SensorID == sensorID
}

// MatchesAnyTarget checks if any target matches (gatewayID, sensorID).
// Matching returns immediately, ensuring overlapping Gateway and Sensor filters
// evaluate to at most one match for a session.
func MatchesAnyTarget(targets []TargetKey, gatewayID, sensorID string) bool {
	for _, t := range targets {
		if t.Matches(gatewayID, sensorID) {
			return true
		}
	}
	return false
}

// DeduplicateTargets filters duplicate target keys preserving order.
func DeduplicateTargets(targets []TargetKey) []TargetKey {
	if len(targets) <= 1 {
		return targets
	}
	seen := make(map[string]struct{}, len(targets))
	result := make([]TargetKey, 0, len(targets))
	for _, t := range targets {
		key := t.String()
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			result = append(result, t)
		}
	}
	return result
}

// ClientMessage is the validated representation of a client subscribe or unsubscribe frame.
type ClientMessage struct {
	ProtocolVersion int64       `json:"protocol_version"`
	RequestID       string      `json:"request_id"`
	Action          string      `json:"action"`
	Targets         []TargetKey `json:"targets"`
}

type rawTarget struct {
	GatewayID string `json:"gateway_id"`
	SensorID  string `json:"sensor_id"`
	EntityID  string `json:"entity_id"`
}

type rawClientMessage struct {
	ProtocolVersion *int64      `json:"protocol_version"`
	RequestID       *string     `json:"request_id"`
	Action          *string     `json:"action"`
	MessageType     *string     `json:"message_type"`
	Targets         []rawTarget `json:"targets"`
}

// ParseClientMessage parses and validates one client control message.
// It enforces payload size bounds, JSON structure, field allowlists, and ID syntax.
// Parsing distinguishes syntax failure from authorization denial and never leaks database details.
func ParseClientMessage(data []byte) (ClientMessage, error) {
	if len(data) > MaxClientMessageBytes {
		return ClientMessage{}, &ProtocolError{
			Code:    ErrorCodeMessageTooLarge,
			Field:   "payload",
			Message: "client message exceeds maximum allowed byte length",
		}
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var raw rawClientMessage
	if err := decoder.Decode(&raw); err != nil {
		var fieldErr string
		if strings.HasPrefix(err.Error(), "json: unknown field ") {
			fieldErr = strings.Trim(strings.TrimPrefix(err.Error(), "json: unknown field "), `"`)
		}
		return ClientMessage{}, &ProtocolError{
			Code:    ErrorCodeInvalidMessage,
			Field:   fieldErr,
			Message: "malformed json message or unknown fields",
		}
	}

	// Verify no trailing garbage tokens after valid JSON object
	if decoder.More() {
		return ClientMessage{}, &ProtocolError{
			Code:    ErrorCodeInvalidMessage,
			Message: "unexpected trailing tokens after JSON object",
		}
	}

	// Validate protocol_version
	if raw.ProtocolVersion == nil || *raw.ProtocolVersion != ProtocolVersion {
		return ClientMessage{}, &ProtocolError{
			Code:    ErrorCodeUnsupportedProtocol,
			Field:   "protocol_version",
			Message: "protocol_version must be 1",
		}
	}

	// Validate request_id
	if raw.RequestID == nil || strings.TrimSpace(*raw.RequestID) == "" {
		return ClientMessage{}, &ProtocolError{
			Code:    ErrorCodeInvalidMessage,
			Field:   "request_id",
			Message: "request_id is required",
		}
	}
	reqID := *raw.RequestID
	if len(reqID) > MaxRequestIDLength || strings.ContainsAny(reqID, "\r\n\x00") {
		return ClientMessage{}, &ProtocolError{
			Code:    ErrorCodeInvalidMessage,
			Field:   "request_id",
			Message: "request_id exceeds length or contains invalid characters",
		}
	}

	// Determine and validate action
	action := ""
	if raw.Action != nil {
		action = *raw.Action
	}
	if action == "" && raw.MessageType != nil {
		action = *raw.MessageType
	}
	if raw.Action != nil && raw.MessageType != nil && *raw.Action != *raw.MessageType {
		return ClientMessage{}, &ProtocolError{
			Code:    ErrorCodeInvalidAction,
			Field:   "action",
			Message: "action and message_type mismatch",
		}
	}
	if action != ActionSubscribe && action != ActionUnsubscribe {
		return ClientMessage{}, &ProtocolError{
			Code:    ErrorCodeInvalidAction,
			Field:   "action",
			Message: "action must be subscribe or unsubscribe",
		}
	}

	// Validate targets list
	if len(raw.Targets) == 0 {
		return ClientMessage{}, &ProtocolError{
			Code:    ErrorCodeInvalidTarget,
			Field:   "targets",
			Message: "targets list must not be empty",
		}
	}
	if len(raw.Targets) > MaxTargetsPerMessage {
		return ClientMessage{}, &ProtocolError{
			Code:    ErrorCodeTargetLimitExceeded,
			Field:   "targets",
			Message: "targets list exceeds maximum count",
		}
	}

	targets := make([]TargetKey, len(raw.Targets))
	for i, t := range raw.Targets {
		var gwID, senID string

		if t.EntityID != "" {
			parsed, err := digitaltwin.ParseEntityURN(t.EntityID)
			if err != nil {
				return ClientMessage{}, &ProtocolError{
					Code:    ErrorCodeInvalidTarget,
					Field:   "entity_id",
					Message: "invalid entity URN format",
				}
			}
			gwID = parsed.GatewayID
			senID = parsed.SensorID

			// If explicit gateway_id/sensor_id were also provided, they must agree
			if t.GatewayID != "" && t.GatewayID != gwID {
				return ClientMessage{}, &ProtocolError{
					Code:    ErrorCodeInvalidTarget,
					Field:   "entity_id",
					Message: "entity_id and gateway_id mismatch",
				}
			}
			if t.SensorID != "" && t.SensorID != senID {
				return ClientMessage{}, &ProtocolError{
					Code:    ErrorCodeInvalidTarget,
					Field:   "entity_id",
					Message: "entity_id and sensor_id mismatch",
				}
			}
		} else {
			if t.GatewayID == "" {
				return ClientMessage{}, &ProtocolError{
					Code:    ErrorCodeInvalidTarget,
					Field:   "gateway_id",
					Message: "missing gateway_id in target",
				}
			}
			if err := gateway.ValidateGatewayID(t.GatewayID); err != nil {
				return ClientMessage{}, &ProtocolError{
					Code:    ErrorCodeInvalidTarget,
					Field:   "gateway_id",
					Message: "invalid gateway identifier",
				}
			}
			gwID = t.GatewayID

			if t.SensorID != "" {
				if err := gateway.ValidateSensorID(t.SensorID); err != nil {
					return ClientMessage{}, &ProtocolError{
						Code:    ErrorCodeInvalidTarget,
						Field:   "sensor_id",
						Message: "invalid sensor identifier",
					}
				}
				senID = t.SensorID
			}
		}

		targets[i] = TargetKey{
			GatewayID: gwID,
			SensorID:  senID,
		}
	}

	return ClientMessage{
		ProtocolVersion: ProtocolVersion,
		RequestID:       reqID,
		Action:          action,
		Targets:         targets,
	}, nil
}

// AckMessage represents a server acknowledgment of a client subscribe/unsubscribe request.
type AckMessage struct {
	ProtocolVersion int64       `json:"protocol_version"`
	MessageType     string      `json:"message_type"`
	RequestID       string      `json:"request_id"`
	Action          string      `json:"action"`
	Targets         []TargetKey `json:"targets"`
}

// NewAckMessage constructs an AckMessage.
func NewAckMessage(requestID, action string, targets []TargetKey) AckMessage {
	return AckMessage{
		ProtocolVersion: ProtocolVersion,
		MessageType:     MessageTypeAck,
		RequestID:       requestID,
		Action:          action,
		Targets:         targets,
	}
}

// ErrorMessage represents a server error response.
type ErrorMessage struct {
	ProtocolVersion int64     `json:"protocol_version"`
	MessageType     string    `json:"message_type"`
	RequestID       string    `json:"request_id,omitempty"`
	ErrorCode       ErrorCode `json:"error_code"`
	ErrorMessage    string    `json:"error_message"`
	Field           string    `json:"field,omitempty"`
}

// NewErrorMessage constructs an ErrorMessage.
func NewErrorMessage(requestID string, code ErrorCode, message string, field string) ErrorMessage {
	return ErrorMessage{
		ProtocolVersion: ProtocolVersion,
		MessageType:     MessageTypeError,
		RequestID:       requestID,
		ErrorCode:       code,
		ErrorMessage:    message,
		Field:           field,
	}
}

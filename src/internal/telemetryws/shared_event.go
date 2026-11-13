package telemetryws

import (
	"errors"
	"fmt"
	"time"
)

// Common event errors.
var (
	// ErrEmptyPayload indicates an attempt to wrap or enqueue an empty payload.
	ErrEmptyPayload = errors.New("telemetryws: empty event payload")

	// ErrInvalidTargetIdentifier indicates missing gateway_id or sensor_id.
	ErrInvalidTargetIdentifier = errors.New("telemetryws: invalid gateway_id or sensor_id")
)

// SharedEvent represents an immutable, already-encoded telemetry event
// that can be safely referenced across multiple viewer sessions without copying.
//
// OWNERSHIP & MUTATION SAFETY CONTRACT:
// The underlying JSON byte slice is copied once upon construction and unexported.
// External mutations to the original CommittedBatchEvent.Raw or DTO cannot alter
// the buffered payload or corrupt in-flight byte accounting.
// Telemetry message bodies, raw MQTT payloads, and DTO sample arrays are NOT retained.
type SharedEvent struct {
	gatewayID         string
	sensorID          string
	payload           []byte
	confirmedObserved time.Time
}

func (e *SharedEvent) setConfirmedObservedAt(t time.Time) {
	if e != nil {
		e.confirmedObserved = t
	}
}

// ConfirmedObservedAt returns the same-process confirmed-commit observation time.
func (e *SharedEvent) ConfirmedObservedAt() time.Time {
	if e == nil {
		return time.Time{}
	}
	return e.confirmedObserved
}

// NewSharedEvent constructs a new SharedEvent with byte ownership protection.
// The provided payload slice is copied immediately into unexported storage.
func NewSharedEvent(gatewayID, sensorID string, rawPayload []byte, maxFrameBytes int) (*SharedEvent, error) {
	if gatewayID == "" || sensorID == "" {
		return nil, ErrInvalidTargetIdentifier
	}
	if len(rawPayload) == 0 {
		return nil, ErrEmptyPayload
	}
	if maxFrameBytes > 0 && len(rawPayload) > maxFrameBytes {
		return nil, &EventTooLargeError{Size: len(rawPayload), MaxBytes: maxFrameBytes}
	}

	payloadCopy := make([]byte, len(rawPayload))
	copy(payloadCopy, rawPayload)

	return &SharedEvent{
		gatewayID: gatewayID,
		sensorID:  sensorID,
		payload:   payloadCopy,
	}, nil
}

// NewSharedEventFromCommitted derives a SharedEvent from a validated CommittedBatchEvent.
// Only gateway_id, sensor_id, and an owned copy of Raw bytes are retained.
func NewSharedEventFromCommitted(evt CommittedBatchEvent, maxFrameBytes int) (*SharedEvent, error) {
	gwID := evt.DTO.GatewayID
	sID := evt.DTO.SensorID
	if gwID == "" || sID == "" {
		return nil, fmt.Errorf("%w: gateway=%q sensor=%q", ErrInvalidTargetIdentifier, gwID, sID)
	}
	return NewSharedEvent(gwID, sID, evt.Raw, maxFrameBytes)
}

// GatewayID returns the originating gateway identifier.
func (e *SharedEvent) GatewayID() string {
	return e.gatewayID
}

// SensorID returns the originating sensor identifier.
func (e *SharedEvent) SensorID() string {
	return e.sensorID
}

// TargetKey returns the sensor target key for this event.
func (e *SharedEvent) TargetKey() TargetKey {
	return TargetKey{
		GatewayID: e.gatewayID,
		SensorID:  e.sensorID,
	}
}

// GatewayTargetKey returns the gateway-level target key for this event.
func (e *SharedEvent) GatewayTargetKey() TargetKey {
	return TargetKey{
		GatewayID: e.gatewayID,
		SensorID:  "",
	}
}

// Payload returns the immutable byte slice of the encoded event.
func (e *SharedEvent) Payload() []byte {
	return e.payload
}

// ByteSize returns the length in bytes of the encoded payload.
func (e *SharedEvent) ByteSize() int {
	return len(e.payload)
}

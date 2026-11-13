// Package telemetryws defines the committed-batch telemetry event representation
// and immutable serialization for WebSocket fan-out.
package telemetryws

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetry"
)

// maxExactFloat64Integer matches the PostgreSQL DOUBLE PRECISION exact integer limit (2^53).
const maxExactFloat64Integer = int64(1) << 53

// TelemetryEventDTO represents ONE validated newly committed telemetry batch.
// Its fields are derived directly from the persistence batch DTO.
// Provenance integers (such as first_sequence) are encoded as JSON strings
// to prevent client safe-integer precision loss in browser / Flutter Web environments.
type TelemetryEventDTO struct {
	ProtocolVersion  int64     `json:"protocol_version"`
	MessageType      string    `json:"message_type"`
	EntityID         string    `json:"entity_id"`
	GatewayID        string    `json:"gateway_id"`
	SensorID         string    `json:"sensor_id"`
	MessageID        string    `json:"message_id"`
	BootID           string    `json:"boot_id"`
	FirstSequence    string    `json:"first_sequence"` // JSON string for integer provenance precision
	SampleCount      int32     `json:"sample_count"`
	MeasuredAt       string    `json:"measured_at"`
	SampleIntervalUS int64     `json:"sample_interval_us"`
	Samples          []float64 `json:"samples"`
}

// CommittedBatchEvent pairs the event DTO with its pre-encoded immutable JSON payload.
type CommittedBatchEvent struct {
	DTO TelemetryEventDTO
	Raw []byte
}

// EventTooLargeError indicates that an event exceeded the maximum allowed WebSocket envelope bytes.
type EventTooLargeError struct {
	Size     int
	MaxBytes int
}

func (e *EventTooLargeError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("telemetry event size %d bytes exceeds maximum %d bytes", e.Size, e.MaxBytes)
}

// NewTelemetryEvent derives an immutable TelemetryEventDTO from one validated newly committed telemetry batch.
// It never downsamples, conflates multiple sensors into one event, claims current Twin state advancement,
// infers configuration, or introduces command-result fields.
func NewTelemetryEvent(batch telemetry.BatchDTO) (TelemetryEventDTO, error) {
	if batch.ProtocolVersion != 1 {
		return TelemetryEventDTO{}, errors.New("unsupported telemetry protocol version")
	}
	if batch.MessageID == uuid.Nil {
		return TelemetryEventDTO{}, errors.New("nil message_id")
	}
	if batch.BootID == uuid.Nil {
		return TelemetryEventDTO{}, errors.New("nil boot_id")
	}
	if err := gateway.ValidateGatewayID(batch.GatewayID); err != nil {
		return TelemetryEventDTO{}, fmt.Errorf("invalid gateway_id: %w", err)
	}
	if err := gateway.ValidateSensorID(batch.SensorID); err != nil {
		return TelemetryEventDTO{}, fmt.Errorf("invalid sensor_id: %w", err)
	}
	if batch.FirstSequence < 0 {
		return TelemetryEventDTO{}, errors.New("negative first_sequence")
	}
	if batch.SampleCount <= 0 {
		return TelemetryEventDTO{}, errors.New("sample_count must be positive")
	}
	if len(batch.Samples) != int(batch.SampleCount) {
		return TelemetryEventDTO{}, errors.New("sample_count does not match len(samples)")
	}
	if batch.SampleIntervalUS <= 0 || batch.SampleIntervalUS > maxExactFloat64Integer {
		return TelemetryEventDTO{}, errors.New("invalid sample_interval_us")
	}
	if _, err := time.Parse(time.RFC3339Nano, batch.MeasuredAt); err != nil {
		if _, err = time.Parse(time.RFC3339, batch.MeasuredAt); err != nil {
			return TelemetryEventDTO{}, fmt.Errorf("invalid measured_at timestamp: %w", err)
		}
	}

	entityID := fmt.Sprintf("urn:ngsi-ld:Sensor:%s:%s", batch.GatewayID, batch.SensorID)
	samplesClone := append([]float64(nil), batch.Samples...)

	return TelemetryEventDTO{
		ProtocolVersion:  ProtocolVersion,
		MessageType:      MessageTypeTelemetry,
		EntityID:         entityID,
		GatewayID:        batch.GatewayID,
		SensorID:         batch.SensorID,
		MessageID:        batch.MessageID.String(),
		BootID:           batch.BootID.String(),
		FirstSequence:    strconv.FormatInt(batch.FirstSequence, 10),
		SampleCount:      batch.SampleCount,
		MeasuredAt:       batch.MeasuredAt,
		SampleIntervalUS: batch.SampleIntervalUS,
		Samples:          samplesClone,
	}, nil
}

// EncodeTelemetryEvent serializes a TelemetryEventDTO into an immutable CommittedBatchEvent.
// If maxBytes > 0 and the resulting JSON exceeds maxBytes, EventTooLargeError is returned.
func EncodeTelemetryEvent(dto TelemetryEventDTO, maxBytes int) (*CommittedBatchEvent, error) {
	raw, err := json.Marshal(dto)
	if err != nil {
		return nil, fmt.Errorf("marshal telemetry event: %w", err)
	}
	if maxBytes > 0 && len(raw) > maxBytes {
		return nil, &EventTooLargeError{Size: len(raw), MaxBytes: maxBytes}
	}
	return &CommittedBatchEvent{
		DTO: dto,
		Raw: raw,
	}, nil
}

// OversizeGapNotice is the signal emitted when a committed batch exceeds the WebSocket frame size.
// It instructs the client to catch up on missed raw samples via the REST temporal measurement endpoint.
type OversizeGapNotice struct {
	ProtocolVersion int64  `json:"protocol_version"`
	MessageType     string `json:"message_type"`
	GatewayID       string `json:"gateway_id"`
	SensorID        string `json:"sensor_id"`
	EntityID        string `json:"entity_id"`
	Reason          string `json:"reason"`
	MessageID       string `json:"message_id"`
	FirstSequence   string `json:"first_sequence"`
	SampleCount     int32  `json:"sample_count"`
	ResyncEndpoint  string `json:"resync_endpoint"`
}

// NewOversizeGapNotice constructs an OversizeGapNotice for an oversized event.
func NewOversizeGapNotice(dto TelemetryEventDTO) OversizeGapNotice {
	return OversizeGapNotice{
		ProtocolVersion: ProtocolVersion,
		MessageType:     MessageTypeGap,
		GatewayID:       dto.GatewayID,
		SensorID:        dto.SensorID,
		EntityID:        dto.EntityID,
		Reason:          "OVERSIZED_BATCH",
		MessageID:       dto.MessageID,
		FirstSequence:   dto.FirstSequence,
		SampleCount:     dto.SampleCount,
		ResyncEndpoint:  fmt.Sprintf("/v1/digital-twins/%s/temporal?attrs=measurement", dto.EntityID),
	}
}

// Package telemetry defines the MQTT telemetry input types and topic helpers.
package telemetry

import (
	"time"

	"github.com/google/uuid"
)

// BatchDTO is the unvalidated representation of one telemetry_batch payload.
// Validation and topic matching are performed by later ingestion steps.
type BatchDTO struct {
	ProtocolVersion  int64     `json:"protocol_version"`
	MessageID        uuid.UUID `json:"message_id"`
	MessageType      string    `json:"message_type"`
	GatewayID        string    `json:"gateway_id"`
	SensorID         string    `json:"sensor_id"`
	BootID           uuid.UUID `json:"boot_id"`
	FirstSequence    int64     `json:"first_sequence"`
	SampleCount      int32     `json:"sample_count"`
	MeasuredAt       string    `json:"measured_at"`
	SampleIntervalUS int64     `json:"sample_interval_us"`
	Samples          []float64 `json:"samples"`
}

// SampleCandidate is one expanded sample carrying its source identifiers.
// Its fields are data only and do not imply that persistence has succeeded.
type SampleCandidate struct {
	GatewayID   string
	SensorID    string
	BootID      uuid.UUID
	MessageID   uuid.UUID
	SampleIndex int32
	SequenceNum int64
	MeasuredAt  time.Time
	Value       float64
}

// Limits contains caller-supplied bounds for telemetry payload validation.
type Limits struct {
	MaxPayloadBytes  int
	MaxSamples       int
	MaxBatchDuration time.Duration
	MaxFutureSkew    time.Duration
}

// Validate reports whether every configured telemetry bound is usable.
func (limits Limits) Validate() error {
	if limits.MaxPayloadBytes <= 0 {
		return &ReasonError{Code: ReasonInvalidPayload, Field: "max_payload_bytes"}
	}
	if limits.MaxSamples <= 0 || uint64(limits.MaxSamples) > uint64(^uint32(0)>>1) {
		return &ReasonError{Code: ReasonInvalidPayload, Field: "max_samples"}
	}
	if limits.MaxBatchDuration <= 0 {
		return &ReasonError{Code: ReasonInvalidPayload, Field: "max_batch_duration"}
	}
	if limits.MaxFutureSkew < 0 {
		return &ReasonError{Code: ReasonInvalidPayload, Field: "max_future_skew"}
	}
	return nil
}

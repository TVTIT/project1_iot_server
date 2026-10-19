package gatewaysim

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetry"
)

// ServerLimits returns the backend telemetry ingress bounds the simulator must
// fit inside. They mirror the Task 3.4 defaults (64 KiB payload, 1000 samples,
// 60 s batch span, 2 min future skew) and are never widened by the simulator.
func ServerLimits() telemetry.Limits {
	return telemetry.Limits{
		MaxPayloadBytes:  65536,
		MaxSamples:       1000,
		MaxBatchDuration: 60 * time.Second,
		MaxFutureSkew:    2 * time.Minute,
	}
}

// Fixed probe identifiers used only to check the configured sample profile at
// load time. They never reach a real outbox.
var (
	probeBootID    = uuid.MustParse("00000000-0000-4000-8000-000000000001")
	probeMessageID = uuid.MustParse("00000000-0000-4000-8000-000000000002")
)

// validateSampleProfile checks that the configured sample rate and batch size
// produce a batch inside the backend telemetry limits. It reuses the real batch
// builder and telemetry validator instead of duplicating their rules, and
// returns the derived microsecond interval and batch span.
func validateSampleProfile(gatewayID, sensorID string, rateHz, samplesPerBatch int) (int64, time.Duration, error) {
	if rateHz <= 0 || 1_000_000%rateHz != 0 {
		return 0, 0, fmt.Errorf("GATEWAY_SAMPLE_RATE_HZ must be a positive integer that divides 1000000")
	}
	intervalUS := int64(1_000_000 / rateHz)

	// A fixed reference time keeps the probe deterministic; the probe only
	// needs a not-in-the-future measured_at to exercise the validator.
	reference := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := BuildSyntheticBatch(BatchRequest{
		GatewayID:        gatewayID,
		SensorID:         sensorID,
		BootID:           probeBootID,
		MessageID:        probeMessageID,
		FirstSequence:    0,
		MeasuredAt:       reference,
		SampleCount:      int32(samplesPerBatch),
		SampleIntervalUS: intervalUS,
	}, ServerLimits(), reference); err != nil {
		return 0, 0, fmt.Errorf("GATEWAY_SAMPLE_RATE_HZ/GATEWAY_SAMPLES_PER_BATCH profile is not accepted by the telemetry validator: %w", err)
	}

	span := time.Duration(int64(samplesPerBatch-1)) * time.Duration(intervalUS) * time.Microsecond
	return intervalUS, span, nil
}

// BatchRequest carries the caller-supplied identities and time for one
// synthetic batch. The builder never generates message or boot UUIDs and never
// allocates sequence ranges: those belong to the process composition and the
// durable outbox transaction, so a retry or resend reuses them unchanged.
type BatchRequest struct {
	GatewayID        string
	SensorID         string
	BootID           uuid.UUID
	MessageID        uuid.UUID
	FirstSequence    int64
	MeasuredAt       time.Time
	SampleCount      int32
	SampleIntervalUS int64
}

// BuiltBatch is one serialized synthetic telemetry batch. Payload is the exact
// immutable byte slice to store in the outbox and publish unchanged on every
// resend; the builder never regenerates it.
type BuiltBatch struct {
	Topic           string
	Payload         []byte
	Batch           telemetry.BatchDTO
	FirstMeasuredAt time.Time
	LastMeasuredAt  time.Time
}

// BuildSyntheticBatch builds and serializes exactly one telemetry_batch from the
// caller-supplied identifiers and time, then validates it with the existing
// telemetry validator. receivedAt is supplied by the caller so no wall-clock is
// read here; the scheduler must only call it once the observation interval is
// due, and the validator rejects a batch whose last sample is in the future.
func BuildSyntheticBatch(req BatchRequest, limits telemetry.Limits, receivedAt time.Time) (BuiltBatch, error) {
	if err := gateway.ValidateGatewayID(req.GatewayID); err != nil {
		return BuiltBatch{}, fmt.Errorf("gatewaysim: invalid gateway identifier")
	}
	if err := gateway.ValidateSensorID(req.SensorID); err != nil {
		return BuiltBatch{}, fmt.Errorf("gatewaysim: invalid sensor identifier")
	}
	if req.BootID == uuid.Nil || req.BootID.Variant() != uuid.RFC4122 {
		return BuiltBatch{}, fmt.Errorf("gatewaysim: invalid boot id")
	}
	if req.MessageID == uuid.Nil || req.MessageID.Variant() != uuid.RFC4122 ||
		(req.MessageID.Version() != 4 && req.MessageID.Version() != 7) {
		return BuiltBatch{}, fmt.Errorf("gatewaysim: invalid message id")
	}
	if req.SampleCount <= 0 {
		return BuiltBatch{}, fmt.Errorf("gatewaysim: sample count must be positive")
	}
	if req.SampleIntervalUS <= 0 {
		return BuiltBatch{}, fmt.Errorf("gatewaysim: sample interval must be positive")
	}
	if req.MeasuredAt.IsZero() {
		return BuiltBatch{}, fmt.Errorf("gatewaysim: measured_at must be set")
	}

	samples := make([]float64, req.SampleCount)
	for i := range samples {
		samples[i] = syntheticValue(req.FirstSequence + int64(i))
	}

	batch := telemetry.BatchDTO{
		ProtocolVersion:  1,
		MessageID:        req.MessageID,
		MessageType:      "telemetry_batch",
		GatewayID:        req.GatewayID,
		SensorID:         req.SensorID,
		BootID:           req.BootID,
		FirstSequence:    req.FirstSequence,
		SampleCount:      req.SampleCount,
		MeasuredAt:       formatMeasuredAt(req.MeasuredAt),
		SampleIntervalUS: req.SampleIntervalUS,
		Samples:          samples,
	}

	// Serialize exactly once; the returned bytes are what the outbox stores and
	// the publisher resends unchanged.
	payload, err := json.Marshal(batch)
	if err != nil {
		return BuiltBatch{}, fmt.Errorf("gatewaysim: marshal batch: %w", err)
	}

	topic := fmt.Sprintf("gateways/%s/telemetry/%s", req.GatewayID, req.SensorID)

	validator, err := telemetry.NewValidator(limits)
	if err != nil {
		return BuiltBatch{}, err
	}
	message, err := validator.Validate(topic, payload, receivedAt)
	if err != nil {
		return BuiltBatch{}, err
	}
	first, ok := message.FirstSample()
	if !ok {
		return BuiltBatch{}, fmt.Errorf("gatewaysim: validated batch has no samples")
	}
	last, _ := message.LastSample()

	return BuiltBatch{
		Topic:           topic,
		Payload:         payload,
		Batch:           message.Batch(),
		FirstMeasuredAt: first.MeasuredAt,
		LastMeasuredAt:  last.MeasuredAt,
	}, nil
}

// formatMeasuredAt renders a UTC microsecond-precision RFC3339 timestamp that
// the telemetry parser accepts. Sub-microsecond precision is truncated so the
// serialized bytes are stable for the same input instant.
func formatMeasuredAt(t time.Time) string {
	return t.UTC().Truncate(time.Microsecond).Format("2006-01-02T15:04:05.000000Z07:00")
}

// syntheticValue returns a bounded deterministic simulated reading for a global
// sample sequence number. The value follows a triangular wave between 20.00 and
// 30.00 and is simulated data, not a real sensor measurement. It depends only on
// the sequence number so a resend of the same range yields identical bytes.
func syntheticValue(sequence int64) float64 {
	const period = 2000
	position := sequence % period
	if position < 0 {
		position += period
	}
	if position > period/2 {
		position = period - position
	}
	return 20 + float64(position)/100
}

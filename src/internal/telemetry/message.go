package telemetry

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Validator validates one telemetry message using fixed deployment limits.
// Its limits are copied at construction and cannot be changed by callers.
type Validator struct {
	limits Limits
}

// NewValidator creates a validator when all supplied limits are valid.
func NewValidator(limits Limits) (*Validator, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	return &Validator{limits: limits}, nil
}

// Validate parses, checks, snapshots, hashes, and expands one telemetry batch.
// The returned Message owns its private payload and sample slices. Callers
// must not mutate topic or payload concurrently with this call.
func (v *Validator) Validate(topic string, payload []byte, receivedAt time.Time) (*Message, error) {
	if v == nil {
		return nil, invalidPayloadError("limits")
	}
	if err := v.limits.Validate(); err != nil {
		return nil, err
	}
	if len(payload) > v.limits.MaxPayloadBytes {
		return nil, &ReasonError{Code: ReasonPayloadTooLarge, Field: "payload"}
	}

	rawPayload := append([]byte(nil), payload...)
	parsedTopic, err := ParseTopic(topic)
	if err != nil {
		return nil, err
	}
	batch, err := decodeBatch(rawPayload, parsedTopic, v.limits)
	if err != nil {
		return nil, err
	}
	checkedRange, err := validateRange(batch, v.limits, receivedAt)
	if err != nil {
		return nil, err
	}

	samples := make([]SampleCandidate, len(batch.Samples))
	for i, value := range batch.Samples {
		index := int32(i)
		samples[i] = SampleCandidate{
			GatewayID:   batch.GatewayID,
			SensorID:    batch.SensorID,
			BootID:      batch.BootID,
			MessageID:   batch.MessageID,
			SampleIndex: index,
			SequenceNum: batch.FirstSequence + int64(index),
			MeasuredAt:  checkedRange.FirstMeasuredAt.Add(time.Duration(index) * checkedRange.Interval),
			Value:       value,
		}
	}

	hash := sha256.Sum256(rawPayload)
	return &Message{
		topic:       parsedTopic,
		batch:       batch,
		rawPayload:  rawPayload,
		payloadHash: hex.EncodeToString(hash[:]),
		receivedAt:  receivedAt,
		samples:     samples,
	}, nil
}

// Message is a fully validated telemetry batch with its expanded samples.
// Its internal slices are private and all slice accessors return copies.
type Message struct {
	topic       ParsedTopic
	batch       BatchDTO
	rawPayload  []byte
	payloadHash string
	receivedAt  time.Time
	samples     []SampleCandidate
}

// Topic returns the exact parsed topic and its Gateway and Sensor identifiers.
func (m *Message) Topic() ParsedTopic {
	if m == nil {
		return ParsedTopic{}
	}
	return m.topic
}

// Batch returns the decoded batch with an independent samples slice.
func (m *Message) Batch() BatchDTO {
	if m == nil {
		return BatchDTO{}
	}
	batch := m.batch
	batch.Samples = append([]float64(nil), m.batch.Samples...)
	return batch
}

// RawPayload returns a copy of the exact bytes received for this message.
func (m *Message) RawPayload() []byte {
	if m == nil {
		return nil
	}
	return append([]byte(nil), m.rawPayload...)
}

// PayloadHash returns the lowercase SHA-256 hash of the original payload.
func (m *Message) PayloadHash() string {
	if m == nil {
		return ""
	}
	return m.payloadHash
}

// ReceivedAt returns the receipt time supplied to Validate.
func (m *Message) ReceivedAt() time.Time {
	if m == nil {
		return time.Time{}
	}
	return m.receivedAt
}

// Samples returns an independent copy of the expanded sample candidates.
func (m *Message) Samples() []SampleCandidate {
	if m == nil {
		return nil
	}
	return append([]SampleCandidate(nil), m.samples...)
}

// FirstSample returns the first checked candidate, if the message has one.
func (m *Message) FirstSample() (SampleCandidate, bool) {
	if m == nil || len(m.samples) == 0 {
		return SampleCandidate{}, false
	}
	return m.samples[0], true
}

// LastSample returns the last checked candidate, if the message has one.
func (m *Message) LastSample() (SampleCandidate, bool) {
	if m == nil || len(m.samples) == 0 {
		return SampleCandidate{}, false
	}
	return m.samples[len(m.samples)-1], true
}

package telemetry

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func messageTestLimits() Limits {
	return Limits{
		MaxPayloadBytes:  65536,
		MaxSamples:       1000,
		MaxBatchDuration: 60 * time.Second,
		MaxFutureSkew:    2 * time.Minute,
	}
}

func messageTestReceivedAt() time.Time {
	return time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
}

func TestValidatorExpandsEverySampleAndNormalizesTime(t *testing.T) {
	values := make([]float64, 100)
	for i := range values {
		values[i] = float64(i) - 25.5
	}
	payload := messageTestBatchPayload(t, "2026-08-21T12:15:00.000000+02:00", 12501, values)
	validator, err := NewValidator(messageTestLimits())
	if err != nil {
		t.Fatal(err)
	}

	message, err := validator.Validate("gateways/gateway_001/telemetry/sensor_001", payload, messageTestReceivedAt())
	if err != nil {
		t.Fatal(err)
	}
	samples := message.Samples()
	if len(samples) != 100 {
		t.Fatalf("got %d samples, want 100", len(samples))
	}
	t0 := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	messageID := message.Batch().MessageID
	bootID := message.Batch().BootID
	for i, sample := range samples {
		wantTime := t0.Add(time.Duration(i) * 10 * time.Millisecond)
		if sample.GatewayID != "gateway_001" || sample.SensorID != "sensor_001" || sample.MessageID != messageID || sample.BootID != bootID ||
			sample.SampleIndex != int32(i) || sample.SequenceNum != int64(12501+i) || !sample.MeasuredAt.Equal(wantTime) || sample.Value != values[i] {
			t.Errorf("sample[%d] = %#v; want identity, index %d, sequence %d, time %s, value %v", i, sample, i, 12501+i, wantTime, values[i])
		}
	}
	last, ok := message.LastSample()
	if !ok || last.SequenceNum != 12600 || !last.MeasuredAt.Equal(t0.Add(990*time.Millisecond)) {
		t.Fatalf("LastSample() = %#v, %v; want sequence 12600 at t0+990ms", last, ok)
	}
	first, ok := message.FirstSample()
	if !ok || first.SequenceNum != 12501 || !first.MeasuredAt.Equal(t0) {
		t.Fatalf("FirstSample() = %#v, %v; want sequence 12501 at t0", first, ok)
	}
	if got := message.Topic(); got.SourceTopic != "gateways/gateway_001/telemetry/sensor_001" || got.GatewayID != "gateway_001" || got.SensorID != "sensor_001" {
		t.Fatalf("Topic() = %#v", got)
	}
	if !message.ReceivedAt().Equal(messageTestReceivedAt()) {
		t.Fatalf("ReceivedAt() = %s", message.ReceivedAt())
	}
}

func TestValidatorHashIsOriginalPayloadSHA256(t *testing.T) {
	original := decodeTestPayload()
	validator, err := NewValidator(decodeTestLimitsWithRange())
	if err != nil {
		t.Fatal(err)
	}
	receivedAt := time.Date(2026, 8, 21, 10, 15, 1, 0, time.UTC)
	message, err := validator.Validate(decodeTestTopic().SourceTopic, original, receivedAt)
	if err != nil {
		t.Fatal(err)
	}
	const wantHash = "3c1e36f8b46a933642a3c8392329bb5c5b23845a62b88d27fe48fdb68e28cfaa"
	if message.PayloadHash() != wantHash {
		t.Fatalf("PayloadHash() = %s, want %s", message.PayloadHash(), wantHash)
	}

	spaced := append([]byte(" \n"), original...)
	spaced = append(spaced, []byte("\t\n")...)
	spacedMessage, err := validator.Validate(decodeTestTopic().SourceTopic, spaced, receivedAt)
	if err != nil {
		t.Fatal(err)
	}
	if spacedMessage.PayloadHash() == message.PayloadHash() {
		t.Fatal("payload hash ignored valid surrounding whitespace")
	}
	if got := hex.EncodeToString(sha256Bytes(spaced)); spacedMessage.PayloadHash() != got {
		t.Fatalf("whitespace payload hash = %s, want SHA-256 %s", spacedMessage.PayloadHash(), got)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(original, &fields); err != nil {
		t.Fatal(err)
	}
	reordered, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	reorderedMessage, err := validator.Validate(decodeTestTopic().SourceTopic, reordered, receivedAt)
	if err != nil {
		t.Fatal(err)
	}
	if reorderedMessage.PayloadHash() == message.PayloadHash() {
		t.Fatal("payload hash ignored a valid change in JSON key order")
	}
}

func TestMessageAccessorsReturnCopies(t *testing.T) {
	payload := decodeTestPayload()
	wantPayload := append([]byte(nil), payload...)
	validator, err := NewValidator(decodeTestLimitsWithRange())
	if err != nil {
		t.Fatal(err)
	}
	message, err := validator.Validate(decodeTestTopic().SourceTopic, payload, time.Date(2026, 8, 21, 10, 15, 1, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	wantHash := message.PayloadHash()
	wantBatch := message.Batch()
	wantSamples := message.Samples()

	payload[0] = 'x'
	raw := message.RawPayload()
	raw[0] = 'y'
	batch := message.Batch()
	batch.Samples[0] = 88
	samples := message.Samples()
	samples[0].Value = 99

	if !reflect.DeepEqual(message.RawPayload(), wantPayload) {
		t.Fatal("raw payload changed after caller or accessor buffer mutation")
	}
	if message.PayloadHash() != wantHash {
		t.Fatal("stored payload hash changed after caller or accessor buffer mutation")
	}
	if !reflect.DeepEqual(message.Batch(), wantBatch) {
		t.Fatal("stored batch changed after Batch() samples were mutated")
	}
	if !reflect.DeepEqual(message.Samples(), wantSamples) {
		t.Fatal("stored candidates changed after Samples() values were mutated")
	}
}

func TestValidatorReturnsNoMessageForInvalidInputs(t *testing.T) {
	limits := messageTestLimits()
	validator, err := NewValidator(limits)
	if err != nil {
		t.Fatal(err)
	}
	valid := decodeTestPayload()
	tooLargeLimits := limits
	tooLargeLimits.MaxPayloadBytes = len(valid) - 1
	tooLargeValidator, err := NewValidator(tooLargeLimits)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		v       *Validator
		topic   string
		payload []byte
		want    ReasonCode
	}{
		{name: "oversized payload", v: tooLargeValidator, topic: decodeTestTopic().SourceTopic, payload: valid, want: ReasonPayloadTooLarge},
		{name: "invalid topic", v: validator, topic: "gateways/+/telemetry/sensor_001", payload: valid, want: ReasonInvalidTopic},
		{name: "invalid payload", v: validator, topic: decodeTestTopic().SourceTopic, payload: []byte(`{}`), want: ReasonInvalidPayload},
		{name: "invalid range", v: validator, topic: decodeTestTopic().SourceTopic, payload: replaceField(valid, "first_sequence", "-1"), want: ReasonInvalidSequenceRange},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, err := test.v.Validate(test.topic, test.payload, messageTestReceivedAt())
			if message != nil {
				t.Fatalf("Validate() returned message %#v on error", message)
			}
			assertReasonCode(t, err, test.want)
		})
	}
}

func TestValidatorIntegratedPayloadAndRangeBounds(t *testing.T) {
	baseLimits := messageTestLimits()
	receivedAt := messageTestReceivedAt()
	basePayload := decodeTestPayload()
	twoSamples := messageTestBatchPayload(t, "2026-08-21T10:15:00Z", 20, []float64{1, 2})

	tests := []struct {
		name       string
		limits     Limits
		payload    []byte
		wantReason ReasonCode
	}{
		{name: "payload exactly at byte bound", limits: withPayloadLimit(baseLimits, len(basePayload)), payload: basePayload},
		{name: "payload one byte above bound", limits: withPayloadLimit(baseLimits, len(basePayload)), payload: append(append([]byte(nil), basePayload...), ' '), wantReason: ReasonPayloadTooLarge},
		{name: "sample count exactly at bound", limits: withSampleLimit(baseLimits, 2), payload: twoSamples},
		{name: "sample count above bound", limits: withSampleLimit(baseLimits, 1), payload: twoSamples, wantReason: ReasonInvalidPayload},
		{name: "last sample exceeds future skew", limits: withSkew(baseLimits, time.Second), payload: messageTestBatchPayload(t, "2026-08-21T10:15:00.995000Z", 20, []float64{1, 2}), wantReason: ReasonTimestampInFuture},
		{name: "timestamp precision above microseconds", limits: baseLimits, payload: replaceField(basePayload, "measured_at", `"2026-08-21T10:15:00.0000000Z"`), wantReason: ReasonInvalidTimestamp},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validator, err := NewValidator(test.limits)
			if err != nil {
				t.Fatal(err)
			}
			message, err := validator.Validate(decodeTestTopic().SourceTopic, test.payload, receivedAt)
			if test.wantReason == "" {
				if err != nil || message == nil {
					t.Fatalf("Validate() = %v, %v; want a validated message", message, err)
				}
				return
			}
			if message != nil {
				t.Fatalf("Validate() returned message %#v on error", message)
			}
			assertReasonCode(t, err, test.wantReason)
		})
	}
}

func withPayloadLimit(limits Limits, maxBytes int) Limits {
	limits.MaxPayloadBytes = maxBytes
	return limits
}

func withSampleLimit(limits Limits, maxSamples int) Limits {
	limits.MaxSamples = maxSamples
	return limits
}

func withSkew(limits Limits, maxFutureSkew time.Duration) Limits {
	limits.MaxFutureSkew = maxFutureSkew
	return limits
}

func TestNewValidatorAndZeroValidatorFailClosed(t *testing.T) {
	if validator, err := NewValidator(Limits{}); validator != nil || err == nil {
		t.Fatalf("NewValidator(zero limits) = %v, %v; want nil validator and error", validator, err)
	}
	var zero Validator
	if message, err := zero.Validate(decodeTestTopic().SourceTopic, decodeTestPayload(), messageTestReceivedAt()); message != nil || err == nil {
		t.Fatalf("zero Validator.Validate() = %v, %v; want nil message and error", message, err)
	}
	var nilValidator *Validator
	if message, err := nilValidator.Validate(decodeTestTopic().SourceTopic, decodeTestPayload(), messageTestReceivedAt()); message != nil || err == nil {
		t.Fatalf("nil Validator.Validate() = %v, %v; want nil message and error", message, err)
	}

	limits := messageTestLimits()
	validator, err := NewValidator(limits)
	if err != nil {
		t.Fatal(err)
	}
	limits.MaxSamples = 0
	if message, err := validator.Validate(decodeTestTopic().SourceTopic, decodeTestPayload(), messageTestReceivedAt()); message == nil || err != nil {
		t.Fatalf("validator no longer accepts input after original limits changed; error = %v", err)
	}
}

func decodeTestLimitsWithRange() Limits {
	limits := decodeTestLimits()
	limits.MaxBatchDuration = time.Second
	limits.MaxFutureSkew = time.Second
	return limits
}

func messageTestBatchPayload(t *testing.T, measuredAt string, firstSequence int64, values []float64) []byte {
	t.Helper()
	var samples strings.Builder
	for i, value := range values {
		if i > 0 {
			samples.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&samples, "%g", value)
	}
	return []byte(fmt.Sprintf(`{"protocol_version":1,"message_id":%q,"message_type":"telemetry_batch","gateway_id":"gateway_001","sensor_id":"sensor_001","boot_id":%q,"first_sequence":%d,"sample_count":%d,"measured_at":%q,"sample_interval_us":10000,"samples":[%s]}`, decodeTestMessageID, decodeTestBootID, firstSequence, len(values), measuredAt, samples.String()))
}

func sha256Bytes(payload []byte) []byte {
	hash := sha256.Sum256(payload)
	return hash[:]
}

func assertReasonCode(t *testing.T, err error, want ReasonCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want reason %s", want)
	}
	var reason *ReasonError
	if !errors.As(err, &reason) || reason.Code != want {
		t.Fatalf("error = %v, want reason %s", err, want)
	}
}

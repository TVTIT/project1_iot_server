package gatewaysim

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

var (
	testBootID    = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	testMessageID = uuid.MustParse("22222222-2222-4222-8222-222222222222")
)

// defaultRequest returns a valid 100 Hz / 100-sample request and a receipt time
// one second after the first sample, matching the scheduler's due-time call.
func defaultRequest() (BatchRequest, time.Time) {
	measuredAt := time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)
	return BatchRequest{
		GatewayID:        "gateway_001",
		SensorID:         "sensor_001",
		BootID:           testBootID,
		MessageID:        testMessageID,
		FirstSequence:    12501,
		MeasuredAt:       measuredAt,
		SampleCount:      100,
		SampleIntervalUS: 10000,
	}, measuredAt.Add(time.Second)
}

func TestBuildSyntheticBatchDefaultProfile(t *testing.T) {
	req, receivedAt := defaultRequest()
	built, err := BuildSyntheticBatch(req, ServerLimits(), receivedAt)
	if err != nil {
		t.Fatalf("BuildSyntheticBatch: %v", err)
	}

	if built.Topic != "gateways/gateway_001/telemetry/sensor_001" {
		t.Fatalf("topic = %q", built.Topic)
	}

	b := built.Batch
	if b.ProtocolVersion != 1 || b.MessageType != "telemetry_batch" {
		t.Fatalf("protocol/message_type = %d/%q", b.ProtocolVersion, b.MessageType)
	}
	if b.MessageID != testMessageID || b.BootID != testBootID {
		t.Fatalf("ids = %s/%s", b.MessageID, b.BootID)
	}
	if b.GatewayID != "gateway_001" || b.SensorID != "sensor_001" {
		t.Fatalf("identity = %q/%q", b.GatewayID, b.SensorID)
	}
	if b.FirstSequence != 12501 || b.SampleCount != 100 || b.SampleIntervalUS != 10000 {
		t.Fatalf("range = %d/%d/%d", b.FirstSequence, b.SampleCount, b.SampleIntervalUS)
	}
	if len(b.Samples) != 100 {
		t.Fatalf("len(samples) = %d", len(b.Samples))
	}
	for i, v := range b.Samples {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			t.Fatalf("sample %d not finite: %v", i, v)
		}
	}
	if b.MeasuredAt != "2026-10-16T10:00:00.000000Z" {
		t.Fatalf("measured_at = %q", b.MeasuredAt)
	}

	wantLast := req.MeasuredAt.Add(99 * 10000 * time.Microsecond)
	if !built.FirstMeasuredAt.Equal(req.MeasuredAt) {
		t.Fatalf("first measured = %s", built.FirstMeasuredAt)
	}
	if !built.LastMeasuredAt.Equal(wantLast) {
		t.Fatalf("last measured = %s, want %s", built.LastMeasuredAt, wantLast)
	}

	validator, err := telemetry.NewValidator(ServerLimits())
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	msg, err := validator.Validate(built.Topic, built.Payload, receivedAt)
	if err != nil {
		t.Fatalf("re-validate payload: %v", err)
	}
	if got := msg.Batch().SampleCount; got != 100 {
		t.Fatalf("validated sample_count = %d", got)
	}
}

func TestBuildSyntheticBatchDeterministicAndImmutableBytes(t *testing.T) {
	req, receivedAt := defaultRequest()
	first, err := BuildSyntheticBatch(req, ServerLimits(), receivedAt)
	if err != nil {
		t.Fatalf("first build: %v", err)
	}
	second, err := BuildSyntheticBatch(req, ServerLimits(), receivedAt)
	if err != nil {
		t.Fatalf("second build: %v", err)
	}
	if !bytes.Equal(first.Payload, second.Payload) {
		t.Fatalf("payloads differ between identical builds")
	}

	original := append([]byte(nil), first.Payload...)
	first.Payload[0] ^= 0xFF
	third, err := BuildSyntheticBatch(req, ServerLimits(), receivedAt)
	if err != nil {
		t.Fatalf("third build: %v", err)
	}
	if !bytes.Equal(third.Payload, original) {
		t.Fatalf("mutating a returned payload changed a later build")
	}
}

func TestBuildSyntheticBatchRejectsInvalidIdentifiers(t *testing.T) {
	cases := map[string]func(*BatchRequest){
		"gateway": func(r *BatchRequest) { r.GatewayID = "bad id" },
		"sensor":  func(r *BatchRequest) { r.SensorID = "" },
	}
	for name, mutate := range cases {
		req, receivedAt := defaultRequest()
		mutate(&req)
		if _, err := BuildSyntheticBatch(req, ServerLimits(), receivedAt); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestBuildSyntheticBatchRejectsInvalidUUIDs(t *testing.T) {
	cases := map[string]func(*BatchRequest){
		"nil boot":            func(r *BatchRequest) { r.BootID = uuid.Nil },
		"nil message":         func(r *BatchRequest) { r.MessageID = uuid.Nil },
		"message version one": func(r *BatchRequest) { r.MessageID = uuid.MustParse("22222222-2222-1222-8222-222222222222") },
	}
	for name, mutate := range cases {
		req, receivedAt := defaultRequest()
		mutate(&req)
		if _, err := BuildSyntheticBatch(req, ServerLimits(), receivedAt); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestBuildSyntheticBatchRejectsSequenceOverflow(t *testing.T) {
	req, receivedAt := defaultRequest()
	req.FirstSequence = math.MaxInt64
	req.SampleCount = 2
	if _, err := BuildSyntheticBatch(req, ServerLimits(), receivedAt); err == nil {
		t.Fatal("expected sequence overflow error")
	}
}

func TestBuildSyntheticBatchRejectsFutureAndZeroTimestamp(t *testing.T) {
	req, receivedAt := defaultRequest()
	req.MeasuredAt = receivedAt.Add(10 * time.Minute)
	if _, err := BuildSyntheticBatch(req, ServerLimits(), receivedAt); err == nil {
		t.Error("expected future timestamp error")
	}

	req, receivedAt = defaultRequest()
	req.MeasuredAt = time.Time{}
	if _, err := BuildSyntheticBatch(req, ServerLimits(), receivedAt); err == nil {
		t.Error("expected zero measured_at error")
	}
}

func TestBuildSyntheticBatchRejectsSampleBounds(t *testing.T) {
	req, receivedAt := defaultRequest()
	req.SampleCount = 0
	if _, err := BuildSyntheticBatch(req, ServerLimits(), receivedAt); err == nil {
		t.Error("expected error for zero sample count")
	}

	req, receivedAt = defaultRequest()
	req.SampleCount = 1001
	if _, err := BuildSyntheticBatch(req, ServerLimits(), receivedAt); err == nil {
		t.Error("expected error for sample count above the validator limit")
	}
}

func TestBuildSyntheticBatchRejectsPayloadBound(t *testing.T) {
	req, receivedAt := defaultRequest()
	limits := ServerLimits()
	limits.MaxPayloadBytes = 16
	if _, err := BuildSyntheticBatch(req, limits, receivedAt); err == nil {
		t.Fatal("expected payload bound error")
	}
}

func TestSyntheticValueDeterministicAndFinite(t *testing.T) {
	const period = 2000
	for seq := int64(-3); seq < 4000; seq++ {
		value := syntheticValue(seq)
		if math.IsNaN(value) || math.IsInf(value, 0) {
			t.Fatalf("syntheticValue(%d) not finite", seq)
		}
		if value < 20 || value > 30 {
			t.Fatalf("syntheticValue(%d) = %v out of range", seq, value)
		}
	}
	if syntheticValue(5) != syntheticValue(5+period) {
		t.Fatal("syntheticValue is not periodic")
	}
}

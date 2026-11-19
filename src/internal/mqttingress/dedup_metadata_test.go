package mqttingress

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

func metadataTestMessage(t *testing.T) *telemetry.Message {
	t.Helper()
	validator, err := telemetry.NewValidator(telemetry.Limits{
		MaxPayloadBytes: 4096, MaxSamples: 10, MaxBatchDuration: time.Hour, MaxFutureSkew: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	measuredAt := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Minute).Format(time.RFC3339Nano)
	payload, err := json.Marshal(telemetry.BatchDTO{
		ProtocolVersion: 1, MessageID: uuid.MustParse("20000000-0000-4000-8000-000000000001"), MessageType: "telemetry_batch",
		GatewayID: "gateway_001", SensorID: "sensor_001", BootID: uuid.MustParse("30000000-0000-4000-8000-000000000001"),
		FirstSequence: 40, SampleCount: 2, MeasuredAt: measuredAt, SampleIntervalUS: 1000, Samples: []float64{1, 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	message, err := validator.Validate("gateways/gateway_001/telemetry/sensor_001", payload, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func TestDedupMetadataBuildEqualityAndMismatch(t *testing.T) {
	message := metadataTestMessage(t)
	commitAt := time.Date(2026, 10, 26, 9, 0, 0, 123456789, time.FixedZone("test", 3600))
	key, proof, ok := buildDedupMetadata(message, commitAt)
	if !ok {
		t.Fatal("valid message was not buildable")
	}
	key2, proof2, ok := buildDedupMetadata(message, commitAt)
	if !ok || key != key2 || !proof.equal(proof2) {
		t.Fatal("same message did not produce equal metadata")
	}
	if proof.originalTimestamp.Location() != time.UTC || proof.originalTimestamp.Nanosecond()%1000 != 0 {
		t.Fatalf("original timestamp = %s, want UTC microsecond precision", proof.originalTimestamp)
	}
	if proof.confirmedCommitAt.Location() != time.UTC || !proof.expiresAt.Equal(proof.confirmedCommitAt.Add(DedupTTL)) {
		t.Fatalf("commit/expiry timestamps = %s / %s, want UTC and fixed TTL", proof.confirmedCommitAt, proof.expiresAt)
	}
	mutations := []func(*dedupMetadata){
		func(m *dedupMetadata) { m.messageID += "x" },
		func(m *dedupMetadata) { m.payloadHash += "x" },
		func(m *dedupMetadata) { m.sourceTopic += "/x" },
		func(m *dedupMetadata) { m.lastSequence++ },
		func(m *dedupMetadata) { m.sampleCount++ },
		func(m *dedupMetadata) { m.originalTimestamp = m.originalTimestamp.Add(time.Microsecond) },
	}
	for i, mutate := range mutations {
		changed := proof
		mutate(&changed)
		if proof.equal(changed) {
			t.Fatalf("metadata mismatch %d was equal", i)
		}
	}
	if got, want := proof.logicalBytes(key), int64(len(key.gatewayID)+len(key.sensorID)+len(key.bootID)+len(proof.messageID)+len(proof.payloadHash)+len(proof.sourceTopic))+DedupMetadataFixedOverheadBytes; got != want {
		t.Fatalf("logical byte estimate = %d, want strings + fixed overhead %d", got, want)
	}
}

func TestDedupMetadataRejectsNilOrInvalidMessage(t *testing.T) {
	if _, _, ok := buildDedupMetadata(nil, time.Now()); ok {
		t.Fatal("nil message accepted")
	}
	empty := &telemetry.Message{}
	if _, _, ok := buildDedupMetadata(empty, time.Now()); ok {
		t.Fatal("message without checked samples accepted")
	}
}

func TestDedupMetadataContainsOnlyScalarProofFields(t *testing.T) {
	typ := reflect.TypeOf(dedupMetadata{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Type.Kind() == reflect.Slice || field.Type.Kind() == reflect.Pointer || field.Type == reflect.TypeOf(telemetry.Message{}) {
			t.Fatalf("metadata field %s retains a reference/payload-bearing type: %s", field.Name, field.Type)
		}
	}
}

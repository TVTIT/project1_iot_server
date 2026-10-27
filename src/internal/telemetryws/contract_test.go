package telemetryws

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

func TestParseClientMessage_ValidSubscribe(t *testing.T) {
	t.Run("gateway and sensor targets", func(t *testing.T) {
		raw := []byte(`{
			"protocol_version": 1,
			"request_id": "req-001",
			"action": "subscribe",
			"targets": [
				{"gateway_id": "gateway_001", "sensor_id": "sensor_001"},
				{"gateway_id": "gateway_002"}
			]
		}`)
		msg, err := ParseClientMessage(raw)
		if err != nil {
			t.Fatalf("unexpected parse error: %v", err)
		}
		if msg.ProtocolVersion != 1 {
			t.Errorf("protocol_version = %d, want 1", msg.ProtocolVersion)
		}
		if msg.RequestID != "req-001" {
			t.Errorf("request_id = %q, want %q", msg.RequestID, "req-001")
		}
		if msg.Action != ActionSubscribe {
			t.Errorf("action = %q, want %q", msg.Action, ActionSubscribe)
		}
		if len(msg.Targets) != 2 {
			t.Fatalf("len(targets) = %d, want 2", len(msg.Targets))
		}
		if msg.Targets[0].GatewayID != "gateway_001" || msg.Targets[0].SensorID != "sensor_001" {
			t.Errorf("targets[0] = %+v, want gateway_001:sensor_001", msg.Targets[0])
		}
		if msg.Targets[1].GatewayID != "gateway_002" || msg.Targets[1].SensorID != "" {
			t.Errorf("targets[1] = %+v, want gateway_002 (all sensors)", msg.Targets[1])
		}
	})

	t.Run("entity_id URN targets", func(t *testing.T) {
		raw := []byte(`{
			"protocol_version": 1,
			"request_id": "req-002",
			"action": "subscribe",
			"targets": [
				{"entity_id": "urn:ngsi-ld:Sensor:gateway_001:sensor_001"},
				{"entity_id": "urn:ngsi-ld:Gateway:gateway_002"}
			]
		}`)
		msg, err := ParseClientMessage(raw)
		if err != nil {
			t.Fatalf("unexpected parse error: %v", err)
		}
		if len(msg.Targets) != 2 {
			t.Fatalf("len(targets) = %d, want 2", len(msg.Targets))
		}
		if msg.Targets[0].GatewayID != "gateway_001" || msg.Targets[0].SensorID != "sensor_001" {
			t.Errorf("targets[0] = %+v, want gateway_001:sensor_001", msg.Targets[0])
		}
		if msg.Targets[1].GatewayID != "gateway_002" || msg.Targets[1].SensorID != "" {
			t.Errorf("targets[1] = %+v, want gateway_002", msg.Targets[1])
		}
	})

	t.Run("consistent entity_id and gateway_id/sensor_id", func(t *testing.T) {
		raw := []byte(`{
			"protocol_version": 1,
			"request_id": "req-003",
			"action": "subscribe",
			"targets": [
				{"gateway_id": "gw1", "sensor_id": "s1", "entity_id": "urn:ngsi-ld:Sensor:gw1:s1"}
			]
		}`)
		msg, err := ParseClientMessage(raw)
		if err != nil {
			t.Fatalf("unexpected parse error: %v", err)
		}
		if msg.Targets[0].GatewayID != "gw1" || msg.Targets[0].SensorID != "s1" {
			t.Errorf("targets[0] = %+v", msg.Targets[0])
		}
	})
}

func TestParseClientMessage_ValidUnsubscribe(t *testing.T) {
	raw := []byte(`{
		"protocol_version": 1,
		"request_id": "req-unsub-1",
		"action": "unsubscribe",
		"targets": [
			{"gateway_id": "gateway_001", "sensor_id": "sensor_001"}
		]
	}`)
	msg, err := ParseClientMessage(raw)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if msg.Action != ActionUnsubscribe {
		t.Errorf("action = %q, want %q", msg.Action, ActionUnsubscribe)
	}
}

func TestParseClientMessage_SyntaxAndValidationErrors(t *testing.T) {
	cases := []struct {
		name         string
		payload      string
		wantCode     ErrorCode
		wantField    string
		syntaxTarget bool
	}{
		{
			name:         "malformed json syntax",
			payload:      `{"protocol_version": 1, "request_id": `,
			wantCode:     ErrorCodeInvalidMessage,
			syntaxTarget: true,
		},
		{
			name:         "trailing tokens after json",
			payload:      `{"protocol_version": 1, "request_id": "r1", "action": "subscribe", "targets": [{"gateway_id": "gw1"}]} extra`,
			wantCode:     ErrorCodeInvalidMessage,
			syntaxTarget: true,
		},
		{
			name:         "unknown field in root",
			payload:      `{"protocol_version": 1, "request_id": "r1", "action": "subscribe", "targets": [{"gateway_id": "gw1"}], "unknown_field": 123}`,
			wantCode:     ErrorCodeInvalidMessage,
			wantField:    "unknown_field",
			syntaxTarget: true,
		},
		{
			name:         "unknown field in target object",
			payload:      `{"protocol_version": 1, "request_id": "r1", "action": "subscribe", "targets": [{"gateway_id": "gw1", "extra": "evil"}]}`,
			wantCode:     ErrorCodeInvalidMessage,
			wantField:    "extra",
			syntaxTarget: true,
		},
		{
			name:         "unsupported protocol version 2",
			payload:      `{"protocol_version": 2, "request_id": "r1", "action": "subscribe", "targets": [{"gateway_id": "gw1"}]}`,
			wantCode:     ErrorCodeUnsupportedProtocol,
			wantField:    "protocol_version",
			syntaxTarget: true,
		},
		{
			name:         "unsupported protocol version 0",
			payload:      `{"protocol_version": 0, "request_id": "r1", "action": "subscribe", "targets": [{"gateway_id": "gw1"}]}`,
			wantCode:     ErrorCodeUnsupportedProtocol,
			wantField:    "protocol_version",
			syntaxTarget: true,
		},
		{
			name:         "missing request_id",
			payload:      `{"protocol_version": 1, "action": "subscribe", "targets": [{"gateway_id": "gw1"}]}`,
			wantCode:     ErrorCodeInvalidMessage,
			wantField:    "request_id",
			syntaxTarget: true,
		},
		{
			name:         "empty request_id",
			payload:      `{"protocol_version": 1, "request_id": "   ", "action": "subscribe", "targets": [{"gateway_id": "gw1"}]}`,
			wantCode:     ErrorCodeInvalidMessage,
			wantField:    "request_id",
			syntaxTarget: true,
		},
		{
			name:         "request_id exceeds max length",
			payload:      fmt.Sprintf(`{"protocol_version": 1, "request_id": %q, "action": "subscribe", "targets": [{"gateway_id": "gw1"}]}`, strings.Repeat("x", MaxRequestIDLength+1)),
			wantCode:     ErrorCodeInvalidMessage,
			wantField:    "request_id",
			syntaxTarget: true,
		},
		{
			name:         "invalid action publish",
			payload:      `{"protocol_version": 1, "request_id": "r1", "action": "publish", "targets": [{"gateway_id": "gw1"}]}`,
			wantCode:     ErrorCodeInvalidAction,
			wantField:    "action",
			syntaxTarget: true,
		},
		{
			name:         "empty targets array",
			payload:      `{"protocol_version": 1, "request_id": "r1", "action": "subscribe", "targets": []}`,
			wantCode:     ErrorCodeInvalidTarget,
			wantField:    "targets",
			syntaxTarget: true,
		},
		{
			name:         "nil/missing targets",
			payload:      `{"protocol_version": 1, "request_id": "r1", "action": "subscribe"}`,
			wantCode:     ErrorCodeInvalidTarget,
			wantField:    "targets",
			syntaxTarget: true,
		},
		{
			name:         "target list exceeds max count",
			payload:      makeExcessiveTargetsJSON(MaxTargetsPerMessage + 1),
			wantCode:     ErrorCodeTargetLimitExceeded,
			wantField:    "targets",
			syntaxTarget: true,
		},
		{
			name:         "mqtt wildcard + in gateway_id",
			payload:      `{"protocol_version": 1, "request_id": "r1", "action": "subscribe", "targets": [{"gateway_id": "+"}]}`,
			wantCode:     ErrorCodeInvalidTarget,
			wantField:    "gateway_id",
			syntaxTarget: true,
		},
		{
			name:         "mqtt wildcard # in sensor_id",
			payload:      `{"protocol_version": 1, "request_id": "r1", "action": "subscribe", "targets": [{"gateway_id": "gw1", "sensor_id": "#"}]}`,
			wantCode:     ErrorCodeInvalidTarget,
			wantField:    "sensor_id",
			syntaxTarget: true,
		},
		{
			name:         "arbitrary mqtt topic in target",
			payload:      `{"protocol_version": 1, "request_id": "r1", "action": "subscribe", "targets": [{"gateway_id": "gateways/gw1/telemetry/s1"}]}`,
			wantCode:     ErrorCodeInvalidTarget,
			wantField:    "gateway_id",
			syntaxTarget: true,
		},
		{
			name:         "reserved gateway backend_service",
			payload:      `{"protocol_version": 1, "request_id": "r1", "action": "subscribe", "targets": [{"gateway_id": "backend_service"}]}`,
			wantCode:     ErrorCodeInvalidTarget,
			wantField:    "gateway_id",
			syntaxTarget: true,
		},
		{
			name:         "inconsistent entity_id and gateway_id",
			payload:      `{"protocol_version": 1, "request_id": "r1", "action": "subscribe", "targets": [{"gateway_id": "gw1", "sensor_id": "s1", "entity_id": "urn:ngsi-ld:Sensor:gw2:s1"}]}`,
			wantCode:     ErrorCodeInvalidTarget,
			wantField:    "entity_id",
			syntaxTarget: true,
		},
		{
			name:         "inconsistent entity_id and sensor_id",
			payload:      `{"protocol_version": 1, "request_id": "r1", "action": "subscribe", "targets": [{"gateway_id": "gw1", "sensor_id": "s1", "entity_id": "urn:ngsi-ld:Sensor:gw1:s2"}]}`,
			wantCode:     ErrorCodeInvalidTarget,
			wantField:    "entity_id",
			syntaxTarget: true,
		},
		{
			name:         "missing gateway_id and entity_id",
			payload:      `{"protocol_version": 1, "request_id": "r1", "action": "subscribe", "targets": [{"sensor_id": "s1"}]}`,
			wantCode:     ErrorCodeInvalidTarget,
			wantField:    "gateway_id",
			syntaxTarget: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseClientMessage([]byte(tc.payload))
			if err == nil {
				t.Fatalf("expected error for case %q, got nil", tc.name)
			}
			var protoErr *ProtocolError
			if !errors.As(err, &protoErr) {
				t.Fatalf("expected *ProtocolError, got %T: %v", err, err)
			}
			if protoErr.Code != tc.wantCode {
				t.Errorf("error code = %q, want %q (err: %v)", protoErr.Code, tc.wantCode, err)
			}
			if tc.wantField != "" && protoErr.Field != tc.wantField {
				t.Errorf("error field = %q, want %q", protoErr.Field, tc.wantField)
			}
			// Security requirement: syntax error must NOT leak namespace existence
			errMsg := protoErr.Error()
			if strings.Contains(errMsg, "twin_entities") || strings.Contains(errMsg, "user_gateways") || strings.Contains(errMsg, "postgres") {
				t.Errorf("error message leaks internal DB details: %s", errMsg)
			}
		})
	}
}

func TestParseClientMessage_MessageSizeLimit(t *testing.T) {
	oversized := fmt.Sprintf(`{"protocol_version":1,"request_id":"r1","action":"subscribe","targets":[{"gateway_id":"gw1"}],"padding":%q}`, strings.Repeat("A", MaxClientMessageBytes+1))
	_, err := ParseClientMessage([]byte(oversized))
	if err == nil {
		t.Fatal("expected error for oversized message, got nil")
	}
	var protoErr *ProtocolError
	if !errors.As(err, &protoErr) {
		t.Fatalf("expected *ProtocolError, got %T: %v", err, err)
	}
	if protoErr.Code != ErrorCodeMessageTooLarge {
		t.Errorf("code = %q, want %q", protoErr.Code, ErrorCodeMessageTooLarge)
	}
}

func TestTargetKey_Semantics(t *testing.T) {
	t.Run("matching", func(t *testing.T) {
		gwTarget := TargetKey{GatewayID: "gw1"}
		sensorTarget := TargetKey{GatewayID: "gw1", SensorID: "s1"}

		if !gwTarget.Matches("gw1", "s1") {
			t.Error("gateway target should match sensor s1 on gw1")
		}
		if !gwTarget.Matches("gw1", "s2") {
			t.Error("gateway target should match sensor s2 on gw1")
		}
		if gwTarget.Matches("gw2", "s1") {
			t.Error("gateway target gw1 should not match sensor on gw2")
		}

		if !sensorTarget.Matches("gw1", "s1") {
			t.Error("sensor target should match exact sensor (gw1, s1)")
		}
		if sensorTarget.Matches("gw1", "s2") {
			t.Error("sensor target (gw1, s1) should not match sensor s2 on gw1")
		}
		if sensorTarget.Matches("gw2", "s1") {
			t.Error("sensor target (gw1, s1) should not match sensor on gw2")
		}
	})

	t.Run("overlapping filter deduplication", func(t *testing.T) {
		targets := []TargetKey{
			{GatewayID: "gw1"},
			{GatewayID: "gw1", SensorID: "s1"},
		}
		// A session subscribed to both gw1 and (gw1, s1) must match exactly once
		matched := MatchesAnyTarget(targets, "gw1", "s1")
		if !matched {
			t.Error("expected matches to be true")
		}
	})

	t.Run("idempotent deduplicate targets", func(t *testing.T) {
		targets := []TargetKey{
			{GatewayID: "gw1", SensorID: "s1"},
			{GatewayID: "gw1", SensorID: "s1"},
			{GatewayID: "gw1"},
			{GatewayID: "gw1"},
		}
		deduped := DeduplicateTargets(targets)
		if len(deduped) != 2 {
			t.Fatalf("len(deduped) = %d, want 2", len(deduped))
		}
	})

	t.Run("entity URN generation", func(t *testing.T) {
		sen := TargetKey{GatewayID: "gw1", SensorID: "s1"}
		if got := sen.EntityURN(); got != "urn:ngsi-ld:Sensor:gw1:s1" {
			t.Errorf("sen.EntityURN() = %q, want %q", got, "urn:ngsi-ld:Sensor:gw1:s1")
		}
		gw := TargetKey{GatewayID: "gw1"}
		if got := gw.EntityURN(); got != "urn:ngsi-ld:Gateway:gw1" {
			t.Errorf("gw.EntityURN() = %q, want %q", got, "urn:ngsi-ld:Gateway:gw1")
		}
	})
}

func TestServerMessages_Formatting(t *testing.T) {
	t.Run("ack message", func(t *testing.T) {
		ack := NewAckMessage("req-42", ActionSubscribe, []TargetKey{
			{GatewayID: "gw1", SensorID: "s1"},
		})
		bytes, err := json.Marshal(ack)
		if err != nil {
			t.Fatalf("marshal ack: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(bytes, &m); err != nil {
			t.Fatalf("unmarshal ack: %v", err)
		}
		if m["protocol_version"] != float64(1) {
			t.Errorf("protocol_version = %v", m["protocol_version"])
		}
		if m["message_type"] != "ack" {
			t.Errorf("message_type = %v", m["message_type"])
		}
		if m["request_id"] != "req-42" {
			t.Errorf("request_id = %v", m["request_id"])
		}
		if m["action"] != "subscribe" {
			t.Errorf("action = %v", m["action"])
		}
	})

	t.Run("error message", func(t *testing.T) {
		errMsg := NewErrorMessage("req-43", ErrorCodeInvalidTarget, "invalid sensor id", "sensor_id")
		bytes, err := json.Marshal(errMsg)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(bytes, &m); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}
		if m["protocol_version"] != float64(1) {
			t.Errorf("protocol_version = %v", m["protocol_version"])
		}
		if m["message_type"] != "error" {
			t.Errorf("message_type = %v", m["message_type"])
		}
		if m["request_id"] != "req-43" {
			t.Errorf("request_id = %v", m["request_id"])
		}
		if m["error_code"] != "INVALID_TARGET" {
			t.Errorf("error_code = %v", m["error_code"])
		}
		if m["field"] != "sensor_id" {
			t.Errorf("field = %v", m["field"])
		}
	})
}

func TestTelemetryEvent_DerivationAndPrecision(t *testing.T) {
	msgID := uuid.New()
	bootID := uuid.New()
	const largeSeq = int64(9007199254740993) // > Number.MAX_SAFE_INTEGER (9007199254740991)

	batch := telemetry.BatchDTO{
		ProtocolVersion:  1,
		MessageID:        msgID,
		MessageType:      "telemetry_batch",
		GatewayID:        "gateway_001",
		SensorID:         "sensor_001",
		BootID:           bootID,
		FirstSequence:    largeSeq,
		SampleCount:      3,
		MeasuredAt:       "2026-10-27T10:00:00.000000Z",
		SampleIntervalUS: 10000,
		Samples:          []float64{25.5, 25.6, 25.7},
	}

	dto, err := NewTelemetryEvent(batch)
	if err != nil {
		t.Fatalf("NewTelemetryEvent failed: %v", err)
	}

	if dto.ProtocolVersion != 1 {
		t.Errorf("ProtocolVersion = %d, want 1", dto.ProtocolVersion)
	}
	if dto.MessageType != MessageTypeTelemetry {
		t.Errorf("MessageType = %q, want %q", dto.MessageType, MessageTypeTelemetry)
	}
	if dto.EntityID != "urn:ngsi-ld:Sensor:gateway_001:sensor_001" {
		t.Errorf("EntityID = %q, want urn:ngsi-ld:Sensor:gateway_001:sensor_001", dto.EntityID)
	}
	if dto.GatewayID != "gateway_001" || dto.SensorID != "sensor_001" {
		t.Errorf("gateway/sensor mismatch: %s / %s", dto.GatewayID, dto.SensorID)
	}
	if dto.MessageID != msgID.String() {
		t.Errorf("MessageID = %q, want %q", dto.MessageID, msgID.String())
	}
	if dto.BootID != bootID.String() {
		t.Errorf("BootID = %q, want %q", dto.BootID, bootID.String())
	}
	// Safe integer preservation: first_sequence MUST be a string
	if dto.FirstSequence != "9007199254740993" {
		t.Errorf("FirstSequence = %q, want %q", dto.FirstSequence, "9007199254740993")
	}
	if dto.SampleCount != 3 {
		t.Errorf("SampleCount = %d, want 3", dto.SampleCount)
	}
	if dto.MeasuredAt != "2026-10-27T10:00:00.000000Z" {
		t.Errorf("MeasuredAt = %q", dto.MeasuredAt)
	}
	if dto.SampleIntervalUS != 10000 {
		t.Errorf("SampleIntervalUS = %d", dto.SampleIntervalUS)
	}
	if len(dto.Samples) != 3 {
		t.Fatalf("len(Samples) = %d, want 3", len(dto.Samples))
	}

	// Verify deep copy of samples: modifying batch.Samples must NOT mutate dto.Samples
	batch.Samples[0] = 999.9
	if dto.Samples[0] == 999.9 {
		t.Error("dto.Samples was not deeply cloned from batch.Samples")
	}

	// Encode to JSON and verify field types in raw JSON
	event, err := EncodeTelemetryEvent(dto, DefaultMaxEventBytes)
	if err != nil {
		t.Fatalf("EncodeTelemetryEvent: %v", err)
	}

	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(event.Raw, &rawMap); err != nil {
		t.Fatalf("unmarshal raw event: %v", err)
	}

	// Verify first_sequence is quoted string in JSON text
	firstSeqRaw := string(rawMap["first_sequence"])
	if firstSeqRaw != `"9007199254740993"` {
		t.Errorf("first_sequence raw json = %s, want quoted string %s", firstSeqRaw, `"9007199254740993"`)
	}

	// Verify no unexpected twin state advancement or command result fields
	for _, forbidden := range []string{"reported_version", "desired_version", "status", "command_id", "configuration"} {
		if _, ok := rawMap[forbidden]; ok {
			t.Errorf("event unexpectedly contains forbidden field %q", forbidden)
		}
	}
}

func TestTelemetryEvent_RejectInvalidBatch(t *testing.T) {
	validBatch := telemetry.BatchDTO{
		ProtocolVersion:  1,
		MessageID:        uuid.New(),
		MessageType:      "telemetry_batch",
		GatewayID:        "gw1",
		SensorID:         "s1",
		BootID:           uuid.New(),
		FirstSequence:    10,
		SampleCount:      2,
		MeasuredAt:       "2026-10-27T10:00:00.000000Z",
		SampleIntervalUS: 10000,
		Samples:          []float64{1.0, 2.0},
	}

	t.Run("mismatched sample_count and len(samples)", func(t *testing.T) {
		b := validBatch
		b.SampleCount = 5
		_, err := NewTelemetryEvent(b)
		if err == nil {
			t.Error("expected error for mismatched sample_count")
		}
	})

	t.Run("nil message_id", func(t *testing.T) {
		b := validBatch
		b.MessageID = uuid.Nil
		_, err := NewTelemetryEvent(b)
		if err == nil {
			t.Error("expected error for nil message_id")
		}
	})

	t.Run("nil boot_id", func(t *testing.T) {
		b := validBatch
		b.BootID = uuid.Nil
		_, err := NewTelemetryEvent(b)
		if err == nil {
			t.Error("expected error for nil boot_id")
		}
	})

	t.Run("invalid gateway_id", func(t *testing.T) {
		b := validBatch
		b.GatewayID = "backend_service" // reserved
		_, err := NewTelemetryEvent(b)
		if err == nil {
			t.Error("expected error for reserved gateway_id")
		}
	})

	t.Run("invalid sensor_id", func(t *testing.T) {
		b := validBatch
		b.SensorID = "invalid/sensor/id"
		_, err := NewTelemetryEvent(b)
		if err == nil {
			t.Error("expected error for invalid sensor_id")
		}
	})

	t.Run("negative first_sequence", func(t *testing.T) {
		b := validBatch
		b.FirstSequence = -1
		_, err := NewTelemetryEvent(b)
		if err == nil {
			t.Error("expected error for negative first_sequence")
		}
	})

	t.Run("invalid timestamp", func(t *testing.T) {
		b := validBatch
		b.MeasuredAt = "invalid-time"
		_, err := NewTelemetryEvent(b)
		if err == nil {
			t.Error("expected error for invalid timestamp")
		}
	})
}

func TestTelemetryEvent_RepresentativeSizingAndOversizePolicy(t *testing.T) {
	// Representative 1,000-sample sizing check only. Compact source decimals may
	// canonicalize to longer JSON floats, so this does not prove all legal payloads fit.
	const targetSamples = 1000
	samples := make([]float64, targetSamples)
	for i := range samples {
		samples[i] = 25.123456789
	}

	maxGW := strings.Repeat("G", 64)
	maxSen := strings.Repeat("S", 64)

	batch := telemetry.BatchDTO{
		ProtocolVersion:  1,
		MessageID:        uuid.New(),
		MessageType:      "telemetry_batch",
		GatewayID:        maxGW,
		SensorID:         maxSen,
		BootID:           uuid.New(),
		FirstSequence:    math.MaxInt64,
		SampleCount:      targetSamples,
		MeasuredAt:       "9999-12-31T23:59:59.999999Z",
		SampleIntervalUS: int64(1) << 53,
		Samples:          samples,
	}

	dto, err := NewTelemetryEvent(batch)
	if err != nil {
		t.Fatalf("NewTelemetryEvent: %v", err)
	}

	event, err := EncodeTelemetryEvent(dto, DefaultMaxEventBytes)
	if err != nil {
		t.Fatalf("EncodeTelemetryEvent failed at legal boundary: %v", err)
	}

	// Verify envelope overhead:
	// Calculate size of MQTT batch representation vs WS event representation
	mqttRepr, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal mqtt batch: %v", err)
	}
	overhead := len(event.Raw) - len(mqttRepr)
	t.Logf("Legal boundary sizing: MQTT bytes=%d, WS event bytes=%d, Overhead=%d bytes", len(mqttRepr), len(event.Raw), overhead)

	// This representative input remains under the illustrative envelope allowance.
	if overhead > MaxEnvelopeOverheadBytes {
		t.Errorf("envelope overhead %d exceeds MaxEnvelopeOverheadBytes %d", overhead, MaxEnvelopeOverheadBytes)
	}

	// Test Oversize Gap / Resync policy:
	// When maxBytes is deliberately smaller than the event, EncodeTelemetryEvent returns EventTooLargeError
	// but the original batch and DTO remain intact.
	_, err = EncodeTelemetryEvent(dto, 100)
	if err == nil {
		t.Fatal("expected EventTooLargeError when maxBytes exceeded")
	}
	var sizeErr *EventTooLargeError
	if !errors.As(err, &sizeErr) {
		t.Fatalf("expected *EventTooLargeError, got %T: %v", err, err)
	}
	if sizeErr.Size != len(event.Raw) {
		t.Errorf("sizeErr.Size = %d, want %d", sizeErr.Size, len(event.Raw))
	}
	if sizeErr.MaxBytes != 100 {
		t.Errorf("sizeErr.MaxBytes = %d, want 100", sizeErr.MaxBytes)
	}

	// Verify OversizeGapNotice construction
	notice := NewOversizeGapNotice(dto)
	if notice.MessageType != "gap" {
		t.Errorf("notice.MessageType = %q, want %q", notice.MessageType, "gap")
	}
	if notice.Reason != "OVERSIZED_BATCH" {
		t.Errorf("notice.Reason = %q, want %q", notice.Reason, "OVERSIZED_BATCH")
	}
	expectedURI := "/v1/digital-twins/urn:ngsi-ld:Sensor:" + maxGW + ":" + maxSen + "/temporal?attrs=measurement"
	if notice.ResyncEndpoint != expectedURI {
		t.Errorf("notice.ResyncEndpoint = %q, want %q", notice.ResyncEndpoint, expectedURI)
	}
}

func makeExcessiveTargetsJSON(count int) string {
	targets := make([]string, count)
	for i := 0; i < count; i++ {
		targets[i] = fmt.Sprintf(`{"gateway_id": "gw_%03d"}`, i)
	}
	return fmt.Sprintf(`{"protocol_version": 1, "request_id": "r1", "action": "subscribe", "targets": [%s]}`, strings.Join(targets, ","))
}

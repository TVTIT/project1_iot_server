package telemetry

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

const (
	decodeTestMessageID = "0195e18c-9fc1-7a42-9064-69ea49e63bf2"
	decodeTestBootID    = "7f2c45f7-0a76-4e28-8a37-7793d7d85b04"
)

func decodeTestTopic() ParsedTopic {
	return ParsedTopic{SourceTopic: "gateways/gateway_001/telemetry/sensor_001", GatewayID: "gateway_001", SensorID: "sensor_001"}
}

func decodeTestLimits() Limits { return Limits{MaxPayloadBytes: 65536, MaxSamples: 3} }

func decodeTestPayload() []byte {
	return []byte(fmt.Sprintf(`{"protocol_version":1,"message_id":%q,"message_type":"telemetry_batch","gateway_id":"gateway_001","sensor_id":"sensor_001","boot_id":%q,"first_sequence":12,"sample_count":2,"measured_at":"2026-08-21T10:15:00Z","sample_interval_us":10000,"samples":[0,-1.25]}`, decodeTestMessageID, decodeTestBootID))
}

func replaceField(payload []byte, field, replacement string) []byte {
	needle := fmt.Sprintf(`"%s":`, field)
	text := string(payload)
	parts := strings.SplitN(text, needle, 2)
	if len(parts) != 2 {
		panic("test field missing")
	}
	start := len(parts[0]) + len(needle)
	depth, inString, escaped := 0, false, false
	end := len(text)
	for i := start; i < len(text); i++ {
		c := text[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == '"' {
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '[', '{':
			depth++
		case ']', '}':
			if depth == 0 {
				end = i
				i = len(text)
				continue
			}
			depth--
		case ',':
			if depth == 0 {
				end = i
				i = len(text)
			}
		}
	}
	return []byte(text[:start] + replacement + text[end:])
}

func requireReason(t *testing.T, payload []byte, topic ParsedTopic, limits Limits, want ReasonCode) {
	t.Helper()
	batch, err := decodeBatch(payload, topic, limits)
	if err == nil || batch.ProtocolVersion != 0 || batch.MessageID != uuid.Nil || batch.MessageType != "" || batch.GatewayID != "" || batch.SensorID != "" || batch.BootID != uuid.Nil || batch.FirstSequence != 0 || batch.SampleCount != 0 || batch.MeasuredAt != "" || batch.SampleIntervalUS != 0 || batch.Samples != nil {
		t.Fatalf("decodeBatch() = %#v, %v; want zero batch and %s", batch, err, want)
	}
	var reason *ReasonError
	if !errors.As(err, &reason) || reason.Code != want {
		t.Fatalf("decodeBatch() error = %v, want reason %s", err, want)
	}
}

func TestDecodeBatchValidAndWhitespace(t *testing.T) {
	payload := append([]byte(" \n\t"), decodeTestPayload()...)
	payload = append(payload, []byte(" \r\n")...)
	batch, err := decodeBatch(payload, decodeTestTopic(), decodeTestLimits())
	if err != nil {
		t.Fatal(err)
	}
	if batch.ProtocolVersion != 1 || batch.MessageID.String() != decodeTestMessageID || batch.MessageType != "telemetry_batch" || batch.GatewayID != "gateway_001" || batch.SensorID != "sensor_001" || batch.BootID.String() != decodeTestBootID || batch.FirstSequence != 12 || batch.SampleCount != 2 || batch.MeasuredAt != "2026-08-21T10:15:00Z" || batch.SampleIntervalUS != 10000 {
		t.Fatalf("decoded fields differ from input: %#v", batch)
	}
	if len(batch.Samples) != 2 || batch.Samples[0] != 0 || batch.Samples[1] != -1.25 {
		t.Fatalf("samples = %v, want [0 -1.25]", batch.Samples)
	}
}

func TestDecodeBatchRequiresEveryFieldAndRejectsNull(t *testing.T) {
	fields := []string{"protocol_version", "message_id", "message_type", "gateway_id", "sensor_id", "boot_id", "first_sequence", "sample_count", "measured_at", "sample_interval_us", "samples"}
	for _, field := range fields {
		t.Run("missing_"+field, func(t *testing.T) {
			payload := string(decodeTestPayload())
			needle := fmt.Sprintf(`"%s":`, field)
			start := strings.Index(payload, needle)
			end := strings.Index(payload[start:], ",")
			if end < 0 {
				end = strings.Index(payload[start:], "}")
			}
			payload = payload[:start] + payload[start+end+1:]
			requireReason(t, []byte(payload), decodeTestTopic(), decodeTestLimits(), ReasonInvalidPayload)
		})
		t.Run("null_"+field, func(t *testing.T) {
			requireReason(t, replaceField(decodeTestPayload(), field, "null"), decodeTestTopic(), decodeTestLimits(), ReasonInvalidPayload)
		})
	}
	for _, sample := range []string{`[null]`, `[0,null]`} {
		t.Run("null_sample_"+strings.ReplaceAll(sample, ",", "_"), func(t *testing.T) {
			requireReason(t, replaceField(decodeTestPayload(), "samples", sample), decodeTestTopic(), decodeTestLimits(), ReasonInvalidPayload)
		})
	}
}

func TestDecodeBatchRejectsUnknownAndTrailingDocuments(t *testing.T) {
	payload := append(decodeTestPayload()[:len(decodeTestPayload())-1], []byte(`,"untrusted-secret-field":"secret"}`)...)
	_, err := decodeBatch(payload, decodeTestTopic(), decodeTestLimits())
	if err == nil || strings.Contains(err.Error(), "untrusted-secret-field") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("unknown-field error leaked input or was accepted: %v", err)
	}
	requireReason(t, append(decodeTestPayload(), []byte(` {}`)...), decodeTestTopic(), decodeTestLimits(), ReasonInvalidPayload)
	requireReason(t, []byte(`[]`), decodeTestTopic(), decodeTestLimits(), ReasonInvalidPayload)
	requireReason(t, []byte(`{} garbage`), decodeTestTopic(), decodeTestLimits(), ReasonInvalidPayload)
}

func TestDecodeBatchIntegerLexicalForms(t *testing.T) {
	for _, field := range []string{"protocol_version", "first_sequence", "sample_count", "sample_interval_us"} {
		for _, value := range []string{`"1"`, "1.0", "1e0"} {
			t.Run(field+"_"+strings.ReplaceAll(value, ".", "dot"), func(t *testing.T) {
				requireReason(t, replaceField(decodeTestPayload(), field, value), decodeTestTopic(), decodeTestLimits(), ReasonInvalidPayload)
			})
		}
	}
	requireReason(t, replaceField(decodeTestPayload(), "first_sequence", "9223372036854775808"), decodeTestTopic(), decodeTestLimits(), ReasonInvalidPayload)
	requireReason(t, replaceField(decodeTestPayload(), "sample_count", "2147483648"), decodeTestTopic(), decodeTestLimits(), ReasonInvalidPayload)
}

func TestDecodeBatchProtocolAndUUIDRules(t *testing.T) {
	if _, err := decodeBatch(replaceField(decodeTestPayload(), "protocol_version", "2"), decodeTestTopic(), decodeTestLimits()); reasonCode(err) != ReasonUnsupportedProtocol {
		t.Fatalf("wrong version error = %v, want unsupported_protocol", err)
	}
	if _, err := decodeBatch(replaceField(decodeTestPayload(), "message_type", `"other"`), decodeTestTopic(), decodeTestLimits()); reasonCode(err) != ReasonUnsupportedProtocol {
		t.Fatalf("wrong type value error = %v, want unsupported_protocol", err)
	}
	for _, id := range []string{"00000000-0000-0000-0000-000000000000", "0195e18c-9fc1-37a2-9064-69ea49e63bf2", "0195e18c-9fc1-7a42-0064-69ea49e63bf2"} {
		requireReason(t, replaceField(decodeTestPayload(), "message_id", fmt.Sprintf("%q", id)), decodeTestTopic(), decodeTestLimits(), ReasonInvalidPayload)
	}
	for _, id := range []string{"550e8400-e29b-41d4-a716-446655440000", decodeTestMessageID} {
		if _, err := decodeBatch(replaceField(decodeTestPayload(), "message_id", fmt.Sprintf("%q", id)), decodeTestTopic(), decodeTestLimits()); err != nil {
			t.Fatalf("valid message UUID %s rejected: %v", id, err)
		}
	}
	wrappedUUID := "x" + decodeTestMessageID + "x"
	if len(wrappedUUID) != 38 {
		t.Fatalf("malformed UUID fixture length = %d, want 38", len(wrappedUUID))
	}
	for _, field := range []string{"message_id", "boot_id"} {
		t.Run("wrapped_uuid_"+field, func(t *testing.T) {
			requireReason(t, replaceField(decodeTestPayload(), field, fmt.Sprintf("%q", wrappedUUID)), decodeTestTopic(), decodeTestLimits(), ReasonInvalidPayload)
		})
	}
	bootV1 := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	if _, err := decodeBatch(replaceField(decodeTestPayload(), "boot_id", fmt.Sprintf("%q", bootV1)), decodeTestTopic(), decodeTestLimits()); err != nil {
		t.Fatalf("boot UUIDv1 rejected: %v", err)
	}
	if _, err := decodeBatch(replaceField(decodeTestPayload(), "boot_id", `"00000000-0000-0000-0000-000000000000"`), decodeTestTopic(), decodeTestLimits()); reasonCode(err) != ReasonInvalidPayload {
		t.Fatalf("nil boot UUID error = %v", err)
	}
}

func TestDecodeBatchTopicMismatch(t *testing.T) {
	topic := decodeTestTopic()
	for _, field := range []string{"gateway_id", "sensor_id"} {
		requireReason(t, replaceField(decodeTestPayload(), field, `"different_id"`), topic, decodeTestLimits(), ReasonInvalidTopic)
	}
}

func TestDecodeBatchSampleBoundsAndNumbers(t *testing.T) {
	limits := decodeTestLimits()
	limits.MaxSamples = 2
	requireReason(t, replaceField(decodeTestPayload(), "samples", `[1,2,3]`), decodeTestTopic(), limits, ReasonInvalidPayload)
	requireReason(t, replaceField(decodeTestPayload(), "sample_count", "1"), decodeTestTopic(), limits, ReasonInvalidPayload)
	for _, value := range []string{`"1"`, "true", "{}", "[]", "1e309", "NaN", "Infinity", "-Infinity"} {
		requireReason(t, replaceField(decodeTestPayload(), "samples", "["+value+"]"), decodeTestTopic(), limits, ReasonInvalidPayload)
	}
	if _, err := decodeBatch(replaceField(decodeTestPayload(), "samples", `[-0.5,0]`), decodeTestTopic(), limits); err != nil {
		t.Fatalf("finite negative and zero samples rejected: %v", err)
	}
}

func TestDecodeBatchPayloadByteLimit(t *testing.T) {
	payload := decodeTestPayload()
	limits := decodeTestLimits()
	limits.MaxPayloadBytes = len(payload)
	if _, err := decodeBatch(payload, decodeTestTopic(), limits); err != nil {
		t.Fatalf("payload at exact byte limit rejected: %v", err)
	}
	limits.MaxPayloadBytes++
	if _, err := decodeBatch(append(payload, ' '), decodeTestTopic(), limits); err != nil {
		t.Fatalf("payload at exact byte limit with trailing whitespace rejected: %v", err)
	}
	limits.MaxPayloadBytes = len(payload)
	if _, err := decodeBatch(append(payload, ' '), decodeTestTopic(), limits); reasonCode(err) != ReasonPayloadTooLarge {
		t.Fatalf("one byte over limit error = %v, want payload_too_large", err)
	}
}

func reasonCode(err error) ReasonCode {
	var reason *ReasonError
	if errors.As(err, &reason) {
		return reason.Code
	}
	return ""
}

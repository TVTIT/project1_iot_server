package telemetry

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"strconv"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

var batchFieldNames = map[string]struct{}{
	"protocol_version": {}, "message_id": {}, "message_type": {}, "gateway_id": {},
	"sensor_id": {}, "boot_id": {}, "first_sequence": {}, "sample_count": {},
	"measured_at": {}, "sample_interval_us": {}, "samples": {},
}

// decodeBatch decodes one telemetry_batch JSON object and validates its fields
// and topic identity. Timestamp, range arithmetic, hashing, and expansion are
// deliberately left to later validation steps.
func decodeBatch(payload []byte, topic ParsedTopic, limits Limits) (BatchDTO, error) {
	if len(payload) > limits.MaxPayloadBytes {
		return BatchDTO{}, &ReasonError{Code: ReasonPayloadTooLarge, Field: "payload"}
	}
	if limits.MaxPayloadBytes <= 0 || limits.MaxSamples <= 0 {
		return BatchDTO{}, invalidPayloadError("limits")
	}

	fields, err := decodeBatchObject(payload)
	if err != nil {
		return BatchDTO{}, invalidPayloadError("object")
	}

	var batch BatchDTO
	if batch.ProtocolVersion, err = decodeInt64(fields["protocol_version"]); err != nil {
		return BatchDTO{}, invalidPayloadError("protocol_version")
	}
	if batch.ProtocolVersion != 1 {
		return BatchDTO{}, &ReasonError{Code: ReasonUnsupportedProtocol, Field: "protocol_version"}
	}
	if batch.MessageID, err = decodeUUID(fields["message_id"]); err != nil || batch.MessageID == uuid.Nil || (batch.MessageID.Version() != 4 && batch.MessageID.Version() != 7) || batch.MessageID.Variant() != uuid.RFC4122 {
		return BatchDTO{}, invalidPayloadError("message_id")
	}
	if batch.MessageType, err = decodeString(fields["message_type"]); err != nil {
		return BatchDTO{}, invalidPayloadError("message_type")
	}
	if batch.MessageType != "telemetry_batch" {
		return BatchDTO{}, &ReasonError{Code: ReasonUnsupportedProtocol, Field: "message_type"}
	}
	if batch.GatewayID, err = decodeString(fields["gateway_id"]); err != nil || gateway.ValidateGatewayID(batch.GatewayID) != nil {
		return BatchDTO{}, invalidPayloadError("gateway_id")
	}
	if batch.SensorID, err = decodeString(fields["sensor_id"]); err != nil || gateway.ValidateSensorID(batch.SensorID) != nil {
		return BatchDTO{}, invalidPayloadError("sensor_id")
	}
	if batch.BootID, err = decodeUUID(fields["boot_id"]); err != nil || batch.BootID == uuid.Nil || batch.BootID.Variant() != uuid.RFC4122 {
		return BatchDTO{}, invalidPayloadError("boot_id")
	}
	if batch.FirstSequence, err = decodeInt64(fields["first_sequence"]); err != nil {
		return BatchDTO{}, invalidPayloadError("first_sequence")
	}
	if batch.SampleCount, err = decodeInt32(fields["sample_count"]); err != nil {
		return BatchDTO{}, invalidPayloadError("sample_count")
	}
	if batch.MeasuredAt, err = decodeString(fields["measured_at"]); err != nil {
		return BatchDTO{}, invalidPayloadError("measured_at")
	}
	if batch.SampleIntervalUS, err = decodeInt64(fields["sample_interval_us"]); err != nil {
		return BatchDTO{}, invalidPayloadError("sample_interval_us")
	}
	if batch.SampleCount <= 0 || int64(batch.SampleCount) > int64(limits.MaxSamples) {
		return BatchDTO{}, invalidPayloadError("sample_count")
	}
	batch.Samples, err = decodeSamples(fields["samples"], limits.MaxSamples)
	if err != nil {
		return BatchDTO{}, invalidPayloadError("samples")
	}
	if len(batch.Samples) != int(batch.SampleCount) {
		return BatchDTO{}, invalidPayloadError("sample_count")
	}
	if batch.GatewayID != topic.GatewayID || batch.SensorID != topic.SensorID {
		return BatchDTO{}, &ReasonError{Code: ReasonInvalidTopic, Field: "topic"}
	}
	return batch, nil
}

func decodeBatchObject(payload []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	opening, ok := token.(json.Delim)
	if !ok || opening != '{' {
		return nil, io.ErrUnexpectedEOF
	}
	fields := make(map[string]json.RawMessage, len(batchFieldNames))
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := token.(string)
		if !ok {
			return nil, io.ErrUnexpectedEOF
		}
		if _, known := batchFieldNames[name]; !known {
			return nil, io.ErrUnexpectedEOF
		}
		if _, duplicate := fields[name]; duplicate {
			return nil, io.ErrUnexpectedEOF
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[name] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if len(fields) != len(batchFieldNames) {
		return nil, io.ErrUnexpectedEOF
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return fields, nil
}

func decodeInt64(raw json.RawMessage) (int64, error) {
	if isNull(raw) {
		return 0, io.ErrUnexpectedEOF
	}
	return strconv.ParseInt(string(raw), 10, 64)
}

func decodeInt32(raw json.RawMessage) (int32, error) {
	if isNull(raw) {
		return 0, io.ErrUnexpectedEOF
	}
	value, err := strconv.ParseInt(string(raw), 10, 32)
	return int32(value), err
}

func decodeString(raw json.RawMessage) (string, error) {
	if isNull(raw) {
		return "", io.ErrUnexpectedEOF
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	return value, nil
}

func decodeUUID(raw json.RawMessage) (uuid.UUID, error) {
	value, err := decodeString(raw)
	if err != nil {
		return uuid.Nil, err
	}
	if err := uuid.Validate(value); err != nil {
		return uuid.Nil, err
	}
	return uuid.Parse(value)
}

func decodeSamples(raw json.RawMessage, maximum int) ([]float64, error) {
	if isNull(raw) {
		return nil, io.ErrUnexpectedEOF
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	opening, ok := token.(json.Delim)
	if !ok || opening != '[' {
		return nil, io.ErrUnexpectedEOF
	}
	capacity := maximum
	if capacity > 16 {
		capacity = 16
	}
	samples := make([]float64, 0, capacity)
	for decoder.More() {
		if len(samples) == maximum {
			return nil, io.ErrUnexpectedEOF
		}
		var item json.RawMessage
		if err := decoder.Decode(&item); err != nil || isNull(item) {
			return nil, io.ErrUnexpectedEOF
		}
		var value float64
		if err := json.Unmarshal(item, &value); err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, io.ErrUnexpectedEOF
		}
		samples = append(samples, value)
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, io.ErrUnexpectedEOF
	}
	return samples, nil
}

func isNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func invalidPayloadError(field string) error {
	return &ReasonError{Code: ReasonInvalidPayload, Field: field}
}

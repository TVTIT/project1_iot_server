package digitaltwin

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// MaxReadbackPayloadBytes bounds configuration query and snapshot envelopes.
const MaxReadbackPayloadBytes = 1024

// ConfigurationQuery is a bounded backend request for one Sensor readback.
type ConfigurationQuery struct {
	ProtocolVersion int       `json:"protocol_version"`
	MessageType     string    `json:"message_type"`
	RequestID       string    `json:"request_id"`
	GatewayID       string    `json:"gateway_id"`
	EntityID        string    `json:"entity_id"`
	IssuedAt        time.Time `json:"issued_at"`
	ExpiresAt       time.Time `json:"expires_at"`
}

// ConfigurationSnapshot is a transient observation, not command evidence.
type ConfigurationSnapshot struct {
	ProtocolVersion         int       `json:"protocol_version"`
	MessageType             string    `json:"message_type"`
	RequestID               string    `json:"request_id"`
	GatewayID               string    `json:"gateway_id"`
	EntityID                string    `json:"entity_id"`
	WatermarkVersion        Version   `json:"watermark_version"`
	ConfigurationState      string    `json:"configuration_state"`
	DurableSampleIntervalUS *int64    `json:"durable_sample_interval_us"`
	ActualSampleIntervalUS  *int64    `json:"actual_sample_interval_us"`
	ReadbackAt              time.Time `json:"readback_at"`
}

func readbackIdentity(topic string, fields map[string]json.RawMessage, kind string) (requestID, gatewayID, entityID string, err error) {
	requestID, err = controlString(fields["request_id"])
	if err != nil {
		return
	}
	id, parseErr := uuid.Parse(requestID)
	if parseErr != nil || id == uuid.Nil || id.String() != requestID ||
		(id.Version() != 4 && id.Version() != 7) {
		err = ErrInvalidInput
		return
	}
	gatewayID, err = controlString(fields["gateway_id"])
	if err != nil {
		return
	}
	entityID, err = controlString(fields["entity_id"])
	if err != nil {
		return
	}
	urn, parseErr := ParseEntityURN(entityID)
	if parseErr != nil || urn.Type != EntityTypeSensor || urn.GatewayID != gatewayID ||
		topic != "gateways/"+gatewayID+"/"+kind+"/"+requestID {
		err = ErrInvalidInput
	}
	return
}

// ParseMQTTConfigurationQuery validates strict query bytes and their downlink topic.
// Current-time expiry is deliberately checked by the Gateway runtime, not by parsing.
func ParseMQTTConfigurationQuery(topic string, data []byte, retained bool) (ConfigurationQuery, error) {
	if len(data) > MaxReadbackPayloadBytes {
		return ConfigurationQuery{}, ErrControlPayloadTooLarge
	}
	if retained {
		return ConfigurationQuery{}, ErrInvalidInput
	}
	fields := []string{"protocol_version", "message_type", "request_id", "gateway_id", "entity_id", "issued_at", "expires_at"}
	m, err := controlObject(data, fields...)
	if err != nil || len(m) != len(fields) || !bytes.Equal(bytes.TrimSpace(m["protocol_version"]), []byte("1")) {
		return ConfigurationQuery{}, ErrInvalidInput
	}
	q := ConfigurationQuery{ProtocolVersion: 1}
	q.RequestID, q.GatewayID, q.EntityID, err = readbackIdentity(topic, m, "commands")
	if err != nil {
		return ConfigurationQuery{}, err
	}
	q.MessageType, err = controlString(m["message_type"])
	if err != nil || q.MessageType != "configuration_query" {
		return ConfigurationQuery{}, ErrInvalidInput
	}
	q.IssuedAt, err = controlTime(m["issued_at"])
	if err != nil {
		return ConfigurationQuery{}, err
	}
	q.ExpiresAt, err = controlTime(m["expires_at"])
	if err != nil || !q.ExpiresAt.After(q.IssuedAt) {
		return ConfigurationQuery{}, ErrInvalidInput
	}
	return q, nil
}

// ValidAt checks a parsed query against this Gateway's local UTC clock.
func (q ConfigurationQuery) ValidAt(now time.Time) bool {
	return !now.Before(q.IssuedAt) && now.Before(q.ExpiresAt)
}

// MarshalMQTTConfigurationQuery validates and serializes canonical query bytes.
func MarshalMQTTConfigurationQuery(q ConfigurationQuery) ([]byte, error) {
	if q.ProtocolVersion != 1 || q.MessageType != "configuration_query" ||
		q.IssuedAt.IsZero() || q.ExpiresAt.IsZero() {
		return nil, ErrInvalidInput
	}
	data, err := json.Marshal(q)
	if err != nil || len(data) > MaxReadbackPayloadBytes {
		if len(data) > MaxReadbackPayloadBytes {
			return nil, ErrControlPayloadTooLarge
		}
		return nil, ErrInvalidInput
	}
	if _, err := ParseMQTTConfigurationQuery("gateways/"+q.GatewayID+"/commands/"+q.RequestID, data, false); err != nil {
		return nil, err
	}
	return data, nil
}

func readbackInterval(raw json.RawMessage) (*int64, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	token := string(bytes.TrimSpace(raw))
	if !decimalDigits(token) || len(token) > 16 {
		return nil, ErrInvalidInput
	}
	value, err := strconv.ParseInt(token, 10, 64)
	if err != nil || value < MinSampleIntervalUS || value > MaxSampleIntervalUS {
		return nil, ErrInvalidInput
	}
	return &value, nil
}

// ParseMQTTConfigurationSnapshot validates strict snapshot bytes and response topic.
func ParseMQTTConfigurationSnapshot(topic string, data []byte, retained bool) (ConfigurationSnapshot, error) {
	if len(data) > MaxReadbackPayloadBytes {
		return ConfigurationSnapshot{}, ErrControlPayloadTooLarge
	}
	if retained {
		return ConfigurationSnapshot{}, ErrInvalidInput
	}
	fields := []string{"protocol_version", "message_type", "request_id", "gateway_id", "entity_id", "watermark_version", "configuration_state", "durable_sample_interval_us", "actual_sample_interval_us", "readback_at"}
	m, err := controlObject(data, fields...)
	if err != nil || len(m) != len(fields) || !bytes.Equal(bytes.TrimSpace(m["protocol_version"]), []byte("1")) {
		return ConfigurationSnapshot{}, ErrInvalidInput
	}
	s := ConfigurationSnapshot{ProtocolVersion: 1}
	s.RequestID, s.GatewayID, s.EntityID, err = readbackIdentity(topic, m, "responses")
	if err != nil {
		return ConfigurationSnapshot{}, err
	}
	s.MessageType, err = controlString(m["message_type"])
	if err != nil || s.MessageType != "configuration_snapshot" {
		return ConfigurationSnapshot{}, ErrInvalidInput
	}
	if err = s.WatermarkVersion.UnmarshalJSON(m["watermark_version"]); err != nil {
		return ConfigurationSnapshot{}, err
	}
	s.ConfigurationState, err = controlString(m["configuration_state"])
	if err != nil {
		return ConfigurationSnapshot{}, err
	}
	s.DurableSampleIntervalUS, err = readbackInterval(m["durable_sample_interval_us"])
	if err != nil {
		return ConfigurationSnapshot{}, err
	}
	s.ActualSampleIntervalUS, err = readbackInterval(m["actual_sample_interval_us"])
	if err != nil {
		return ConfigurationSnapshot{}, err
	}
	s.ReadbackAt, err = controlTime(m["readback_at"])
	if err != nil {
		return ConfigurationSnapshot{}, err
	}
	switch s.ConfigurationState {
	case "configured":
		if s.DurableSampleIntervalUS == nil {
			return ConfigurationSnapshot{}, ErrInvalidInput
		}
	case "unconfigured", "unavailable":
		if s.WatermarkVersion != 0 || s.DurableSampleIntervalUS != nil {
			return ConfigurationSnapshot{}, ErrInvalidInput
		}
	default:
		return ConfigurationSnapshot{}, ErrInvalidInput
	}
	return s, nil
}

// MarshalMQTTConfigurationSnapshot validates and serializes canonical snapshot bytes.
func MarshalMQTTConfigurationSnapshot(s ConfigurationSnapshot) ([]byte, error) {
	if s.ProtocolVersion != 1 || s.MessageType != "configuration_snapshot" || s.ReadbackAt.IsZero() {
		return nil, ErrInvalidInput
	}
	data, err := json.Marshal(s)
	if err != nil {
		return nil, ErrInvalidInput
	}
	if len(data) > MaxReadbackPayloadBytes {
		return nil, ErrControlPayloadTooLarge
	}
	if _, err := ParseMQTTConfigurationSnapshot("gateways/"+s.GatewayID+"/responses/"+s.RequestID, data, false); err != nil {
		return nil, err
	}
	return data, nil
}

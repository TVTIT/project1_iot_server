package telemetry

import (
	"strings"

	"iot-platform/internal/gateway"
)

// ParsedTopic contains the exact topic and identifiers from a telemetry topic.
type ParsedTopic struct {
	SourceTopic string
	GatewayID   string
	SensorID    string
}

// ParseTopic parses a concrete v1 telemetry topic with exactly four segments.
// It does not normalize or decode any part of the topic.
func ParseTopic(topic string) (ParsedTopic, error) {
	segments := strings.Split(topic, "/")
	if len(segments) != 4 || segments[0] != "gateways" || segments[2] != "telemetry" {
		return ParsedTopic{}, invalidTopicError()
	}

	gatewayID := segments[1]
	sensorID := segments[3]
	if gateway.ValidateGatewayID(gatewayID) != nil || gateway.ValidateSensorID(sensorID) != nil {
		return ParsedTopic{}, invalidTopicError()
	}

	return ParsedTopic{
		SourceTopic: topic,
		GatewayID:   gatewayID,
		SensorID:    sensorID,
	}, nil
}

func invalidTopicError() error {
	return &ReasonError{Code: ReasonInvalidTopic, Field: "topic"}
}

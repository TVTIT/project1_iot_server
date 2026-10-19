package telemetry

import (
	"errors"
	"strings"
	"testing"
)

func TestParseTopic(t *testing.T) {
	longID := "a" + strings.Repeat("b", 63)
	tests := []struct {
		name       string
		topic      string
		want       ParsedTopic
		wantReason ReasonCode
	}{
		{
			name:  "valid",
			topic: "gateways/gateway_001/telemetry/sensor_001",
			want:  ParsedTopic{SourceTopic: "gateways/gateway_001/telemetry/sensor_001", GatewayID: "gateway_001", SensorID: "sensor_001"},
		},
		{
			name:  "maximum length identifiers",
			topic: "gateways/" + longID + "/telemetry/" + longID,
			want:  ParsedTopic{SourceTopic: "gateways/" + longID + "/telemetry/" + longID, GatewayID: longID, SensorID: longID},
		},
		{
			name:  "reserved gateway string is allowed for sensor",
			topic: "gateways/gateway_001/telemetry/backend_service",
			want:  ParsedTopic{SourceTopic: "gateways/gateway_001/telemetry/backend_service", GatewayID: "gateway_001", SensorID: "backend_service"},
		},
		{name: "gateway reserved identifier", topic: "gateways/backend_service/telemetry/sensor_001", wantReason: ReasonInvalidTopic},
		{name: "missing segment", topic: "gateways/gateway_001/telemetry", wantReason: ReasonInvalidTopic},
		{name: "extra segment", topic: "gateways/gateway_001/telemetry/sensor_001/extra", wantReason: ReasonInvalidTopic},
		{name: "empty gateway", topic: "gateways//telemetry/sensor_001", wantReason: ReasonInvalidTopic},
		{name: "empty sensor", topic: "gateways/gateway_001/telemetry/", wantReason: ReasonInvalidTopic},
		{name: "empty fixed segment", topic: "gateways/gateway_001//sensor_001", wantReason: ReasonInvalidTopic},
		{name: "gateway wildcard", topic: "gateways/+/telemetry/sensor_001", wantReason: ReasonInvalidTopic},
		{name: "sensor wildcard", topic: "gateways/gateway_001/telemetry/#", wantReason: ReasonInvalidTopic},
		{name: "invalid identifier character", topic: "gateways/gateway.001/telemetry/sensor_001", wantReason: ReasonInvalidTopic},
		{name: "identifier too long", topic: "gateways/" + longID + "x/telemetry/sensor_001", wantReason: ReasonInvalidTopic},
		{name: "wrong prefix case", topic: "Gateways/gateway_001/telemetry/sensor_001", wantReason: ReasonInvalidTopic},
		{name: "case is preserved", topic: "gateways/Gateway_A/telemetry/Sensor_B", want: ParsedTopic{SourceTopic: "gateways/Gateway_A/telemetry/Sensor_B", GatewayID: "Gateway_A", SensorID: "Sensor_B"}},
		{name: "no trimming", topic: " gateways/gateway_001/telemetry/sensor_001", wantReason: ReasonInvalidTopic},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseTopic(test.topic)
			if test.wantReason == "" {
				if err != nil {
					t.Fatalf("ParseTopic() error = %v, want nil", err)
				}
				if got != test.want {
					t.Fatalf("ParseTopic() = %#v, want %#v", got, test.want)
				}
				return
			}
			var reasonErr *ReasonError
			if !errors.As(err, &reasonErr) {
				t.Fatalf("ParseTopic() error = %v, want *ReasonError", err)
			}
			if reasonErr.Code != test.wantReason {
				t.Fatalf("reason code = %q, want %q", reasonErr.Code, test.wantReason)
			}
			if got != (ParsedTopic{}) {
				t.Fatalf("ParseTopic() = %#v, want zero result on error", got)
			}
		})
	}
}

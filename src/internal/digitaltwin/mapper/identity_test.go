package mapper

import (
	"strings"
	"testing"
)

func TestGatewayEntityID(t *testing.T) {
	for _, id := range []string{"a", "A_0-9", strings.Repeat("a", 64)} {
		if got, want := GatewayEntityID(id), "urn:ngsi-ld:Gateway:"+id; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func TestSensorEntityID(t *testing.T) {
	for _, id := range []string{"a", "A_0-9", "backend_service", strings.Repeat("a", 64)} {
		if got, want := SensorEntityID("Gateway_A", id), "urn:ngsi-ld:Sensor:Gateway_A:"+id; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
		if SensorEntityID("Gateway_A", id) == SensorEntityID("Gateway_B", id) {
			t.Fatal("sensor identity must include its parent Gateway")
		}
	}
}

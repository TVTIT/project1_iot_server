package mqttcontrol

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/mqttingress"

	"github.com/google/uuid"
)

type captureWire struct {
	delivery mqttingress.Delivery
	calls    int
}

func (w *captureWire) Publish(_ context.Context, d mqttingress.Delivery) error {
	w.delivery = d
	w.calls++
	return nil
}
func (*captureWire) Ready() bool { return true }
func TestControlTransportExactTopicsPayloadQoSAndQueue(t *testing.T) {
	id := uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)
	c := digitaltwin.Command{ProtocolVersion: 1, MessageType: "command", CommandID: id.String(), GatewayID: "fixture_gw", EntityID: "urn:ngsi-ld:Sensor:fixture_gw:s1", Action: digitaltwin.ActionUpdateConfiguration, Parameters: digitaltwin.SamplingParameters{SampleIntervalUS: 10000}, DesiredVersion: 1, IssuedAt: now, ExpiresAt: now.Add(time.Minute)}
	payload, _ := json.Marshal(c)
	topic := "gateways/fixture_gw/commands/" + id.String()
	d := digitaltwin.ControlDispatch{CommandID: id, Topic: topic, Payload: payload}
	w := &captureWire{}
	tr := &Transport{wire: w, queue: make(chan RawResult, 1), maxPayload: 8192}
	if err := tr.Publish(context.Background(), d); err != nil {
		t.Fatal("valid command rejected", err)
	}
	if w.calls != 1 || w.delivery.QoS != 1 || w.delivery.Retained || string(w.delivery.Payload) != string(payload) {
		t.Fatal("wire contract")
	}
	for _, bad := range []string{"#", "gateways/+/commands/#", "gateways/foreign/commands/" + id.String(), "gateways/fixture_gw/responses/" + id.String(), "$CONTROL/dynamic-security/v1", topic + "/child"} {
		d.Topic = bad
		if tr.Publish(context.Background(), d) == nil {
			t.Errorf("allowed %s", bad)
		}
	}
	d.Topic = topic
	d.Payload = []byte(strings.Repeat("x", 8193))
	if tr.Publish(context.Background(), d) == nil {
		t.Fatal("oversize publish")
	}
	d.Payload = payload
	d.CommandID = uuid.New()
	if tr.Publish(context.Background(), d) == nil {
		t.Fatal("ID mismatch")
	}
	c.ExpiresAt = now.Add(-time.Second)
	c.IssuedAt = now.Add(-time.Minute)
	d.CommandID = id
	d.Payload, _ = json.Marshal(c)
	if tr.Publish(context.Background(), d) == nil {
		t.Fatal("expired dispatch")
	}
	response := "gateways/fixture_gw/responses/" + id.String()
	input := []byte("raw")
	for _, bad := range []string{"#", topic, response + "/child", "gateways/+/responses/" + id.String()} {
		tr.Admit(bad, input, now, false)
	}
	tr.Admit(response, input, now, true)
	tr.Admit(response, make([]byte, 8193), now, false)
	if len(tr.queue) != 0 {
		t.Fatal("invalid result admitted")
	}
	tr.Admit(response, input, now, false)
	input[0] = 'X'
	tr.Admit(response, []byte("overflow"), now, false)
	if len(tr.queue) != 1 {
		t.Fatal("bounded result groundwork missing")
	}
	got := <-tr.queue
	if string(got.Payload) != "raw" {
		t.Fatal("Paho buffer retained")
	}
}

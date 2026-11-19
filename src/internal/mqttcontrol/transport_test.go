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

func TestPublishConfigurationQueryUsesNarrowBoundedEnvelope(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	query := digitaltwin.ConfigurationQuery{
		ProtocolVersion: 1, MessageType: "configuration_query", RequestID: uuid.NewString(),
		GatewayID: "fixture_gw", EntityID: "urn:ngsi-ld:Sensor:fixture_gw:s1",
		IssuedAt: now, ExpiresAt: now.Add(5 * time.Second),
	}
	wire := &captureWire{}
	transport := &Transport{wire: wire, stage6Enabled: true}
	if err := transport.PublishConfigurationQuery(context.Background(), query); err != nil {
		t.Fatal(err)
	}
	if wire.calls != 1 || wire.delivery.Topic != "gateways/fixture_gw/commands/"+query.RequestID ||
		wire.delivery.QoS != 1 || wire.delivery.Retained {
		t.Fatalf("query delivery=%+v calls=%d", wire.delivery, wire.calls)
	}
	if _, err := digitaltwin.ParseMQTTConfigurationQuery(wire.delivery.Topic, wire.delivery.Payload, wire.delivery.Retained); err != nil {
		t.Fatalf("published query was not canonical: %v", err)
	}

	disabled := &Transport{wire: &captureWire{}, stage6Enabled: false}
	if err := disabled.PublishConfigurationQuery(context.Background(), query); err == nil {
		t.Fatal("Stage 6-off transport published query")
	}
	oversizedTTL := query
	oversizedTTL.ExpiresAt = oversizedTTL.IssuedAt.Add(6 * time.Second)
	if err := transport.PublishConfigurationQuery(context.Background(), oversizedTTL); err == nil {
		t.Fatal("query exceeded the fixed TTL")
	}
	invalidIdentity := query
	invalidIdentity.EntityID = "urn:ngsi-ld:Sensor:other_gateway:s1"
	if err := transport.PublishConfigurationQuery(context.Background(), invalidIdentity); err == nil {
		t.Fatal("query with mismatched gateway identity published")
	}
}

func TestTransportOversizedAdmissionDoesNotClassifyPayload(t *testing.T) {
	resultMetrics, receiptMetrics := &ResultMetrics{}, &ReceiptMetrics{}
	admissionMetrics := &ResponseAdmissionMetrics{}
	tr := &Transport{queue: make(chan RawResult, 1), maxPayload: 64}
	tr.SetResultMetrics(resultMetrics)
	tr.SetReceiptMetrics(receiptMetrics)
	tr.SetResponseAdmissionMetrics(admissionMetrics)

	payload := append([]byte(`{"message_type":"command_receipt"}`), []byte(strings.Repeat(" ", 128))...)
	tr.Admit("gateways/fixture_gw/responses/"+uuid.NewString(), payload, time.Now(), false)
	tr.Admit("gateways/fixture_gw/responses/"+uuid.NewString(), []byte(`{"message_type":"command_receipt"}`), time.Now(), true)
	tr.stopped.Store(true)
	tr.Admit("gateways/fixture_gw/responses/"+uuid.NewString(), []byte(`{"message_type":"command_result"}`), time.Now(), false)

	if got := admissionMetrics.Snapshot().Rejected; got != 3 {
		t.Fatalf("general admission reject count=%d, want 3", got)
	}
	if got := receiptMetrics.Snapshot().Rejected; got != 0 {
		t.Fatalf("rejected input was classified as a receipt: rejected=%d", got)
	}
	if got := resultMetrics.Snapshot().Rejected; got != 0 {
		t.Fatalf("rejected input was classified as a result: rejected=%d", got)
	}
}

func TestTransportQueueFullUsesOnlyBoundedMetricsHint(t *testing.T) {
	resultMetrics, receiptMetrics := &ResultMetrics{}, &ReceiptMetrics{}
	admissionMetrics := &ResponseAdmissionMetrics{}
	tr := &Transport{queue: make(chan RawResult), maxPayload: 64}
	tr.SetResultMetrics(resultMetrics)
	tr.SetReceiptMetrics(receiptMetrics)
	tr.SetResponseAdmissionMetrics(admissionMetrics)
	payload := []byte(`{"message_type":"command_receipt"}`)
	tr.Admit("gateways/fixture_gw/responses/"+uuid.NewString(), payload, time.Now(), false)
	if got := admissionMetrics.Snapshot().QueueFull; got != 1 {
		t.Fatalf("general queue-full count=%d, want 1", got)
	}
	if got := receiptMetrics.Snapshot().QueueFull; got != 1 {
		t.Fatalf("bounded receipt queue-full hint=%d, want 1", got)
	}
	if got := resultMetrics.Snapshot().QueueFull; got != 0 {
		t.Fatalf("receipt queue-full was counted as a result: %d", got)
	}
}

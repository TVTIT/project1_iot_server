package mqttmedia

import (
	"context"
	"testing"
	"time"

	"iot-platform/internal/mediaprotocol"
	"iot-platform/internal/mqttingress"
)

func TestAdmitIsBoundedCopiedAndDirectionChecked(t *testing.T) {
	tr := &Transport{queue: make(chan Delivery, 1), maxPayload: mediaprotocol.MaxEnvelopeBytes}
	topic := mediaprotocol.UploadRequestTopic("gw-1")
	payload := []byte(`{"version":1}`)
	if got := tr.Admit(topic, payload, time.Now(), false); got != mqttingress.AdmissionAccepted {
		t.Fatalf("Admit = %v, want accepted", got)
	}
	payload[0] = 'X'
	got := <-tr.Messages()
	if got.Payload[0] != '{' {
		t.Fatalf("payload wasn't copied: %q", got.Payload)
	}
	for _, tc := range []struct {
		topic    string
		data     []byte
		retained bool
	}{
		{topic, []byte("x"), true},
		{topic, make([]byte, mediaprotocol.MaxEnvelopeBytes+1), false},
		{"gateways/gw-1/media/down/upload_grant", []byte("x"), false},
		{"gateways/gw-1/media/up/unknown", []byte("x"), false},
	} {
		if got := tr.Admit(tc.topic, tc.data, time.Now(), tc.retained); got != mqttingress.AdmissionRejectedNotRunning {
			t.Errorf("Admit(%q) = %v, want rejected", tc.topic, got)
		}
	}
	if got := tr.Admit(topic, []byte("x"), time.Now(), false); got != mqttingress.AdmissionAccepted {
		t.Fatal("could not fill queue")
	}
	if got := tr.Admit(topic, []byte("x"), time.Now(), false); got != mqttingress.AdmissionRejectedNotRunning {
		t.Fatalf("full queue Admit = %v, want rejected", got)
	}
}

func TestNewTransportRequiresVerifierAndExactFilter(t *testing.T) {
	if _, err := NewTransport(mqttingress.TransportOptions{}); err == nil {
		t.Fatal("transport accepted missing ACL verifier")
	}
	if _, err := NewTransport(mqttingress.TransportOptions{
		VerifyConnection: func(context.Context) error { return nil },
		Filter:           "gateways/+/telemetry/#",
	}); err == nil {
		t.Fatal("transport accepted an alternate filter")
	}
}

func TestPublishGrantValidatesCorrelationAndDeadline(t *testing.T) {
	wire := &recordWire{ready: true}
	tr := &Transport{wire: wire}
	grant := mediaprotocol.UploadGrant{Version: 1, GatewayID: "gw-1", UploadID: "up_1", Status: mediaprotocol.GrantNeedUpload, MediaID: "m_1", UploadURL: "https://storage.example/object?token=secret", ExpiresAt: time.Now().UTC().Add(time.Minute).Format(time.RFC3339)}
	if err := tr.Publish(context.Background(), grant); err != nil {
		t.Fatal(err)
	}
	if wire.delivery.Topic != mediaprotocol.UploadGrantTopic("gw-1") || wire.delivery.QoS != 1 || wire.delivery.Retained || len(wire.delivery.Payload) == 0 {
		t.Fatalf("unexpected delivery: %+v", wire.delivery)
	}
	grant.Status = "not-a-status"
	if err := tr.PublishGrant(context.Background(), grant); err == nil {
		t.Fatal("accepted invalid grant")
	}
	grant.Status = mediaprotocol.GrantNeedUpload
	grant.ExpiresAt = time.Now().UTC().Add(-time.Second).Format(time.RFC3339)
	if err := tr.PublishGrant(context.Background(), grant); err == nil {
		t.Fatal("published an expired grant")
	}
}

type recordWire struct {
	ready    bool
	delivery mqttingress.Delivery
}

func (w *recordWire) Ready() bool { return w.ready }
func (w *recordWire) Publish(_ context.Context, d mqttingress.Delivery) error {
	w.delivery = d
	return nil
}

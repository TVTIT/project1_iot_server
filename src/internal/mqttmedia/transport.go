// Package mqttmedia contains the bounded MQTT transport adapter for Stage 8 media.
// It only stages raw envelopes; callers decode and perform application work off-callback.
package mqttmedia

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"iot-platform/internal/gateway"
	"iot-platform/internal/mediaprotocol"
	"iot-platform/internal/mqttingress"
)

const (
	UploadFilter  = "gateways/+/media/up/#"
	queueCapacity = 128
	maxTopicBytes = 256
)

var ErrInvalidMessage = errors.New("mqtt media: invalid message")

type Delivery struct {
	Topic      string
	Payload    []byte
	ReceivedAt time.Time
	Retained   bool
}

// Transport shares the ingress reconnect/TLS/PUBACK lifetime supervisor.
type Transport struct {
	*mqttingress.Transport
	wire interface {
		Ready() bool
		Publish(context.Context, mqttingress.Delivery) error
	}
	queue      chan Delivery
	maxPayload int
	stopped    atomic.Bool
}

// NewTransport requires a read-only ACL verifier on every connection lifetime.
func NewTransport(opts mqttingress.TransportOptions) (*Transport, error) {
	if opts.VerifyConnection == nil || (opts.Filter != "" && opts.Filter != UploadFilter) {
		return nil, ErrInvalidMessage
	}
	opts.Filter = UploadFilter
	opts.RequireQoS1 = true
	t := &Transport{queue: make(chan Delivery, queueCapacity), maxPayload: mediaprotocol.MaxEnvelopeBytes}
	base, err := mqttingress.NewTransport(opts, t)
	if err != nil {
		return nil, err
	}
	t.Transport, t.wire = base, base
	return t, nil
}

// Admit checks only bounded topic/byte properties and copies for worker decoding.
func (t *Transport) Admit(topic string, payload []byte, at time.Time, retained bool) mqttingress.AdmissionDecision {
	if t.stopped.Load() || retained || len(topic) > maxTopicBytes || len(payload) == 0 || len(payload) > t.maxPayload {
		return mqttingress.AdmissionRejectedNotRunning
	}
	parts := strings.Split(topic, "/")
	if len(parts) != 5 || parts[0] != "gateways" || parts[2] != "media" || parts[3] != "up" ||
		(parts[4] != "upload_request" && parts[4] != "upload_complete") || gateway.ValidateGatewayID(parts[1]) != nil {
		return mqttingress.AdmissionRejectedNotRunning
	}
	d := Delivery{Topic: topic, Payload: append([]byte(nil), payload...), ReceivedAt: at, Retained: retained}
	select {
	case t.queue <- d:
		return mqttingress.AdmissionAccepted
	default:
		return mqttingress.AdmissionRejectedNotRunning
	}
}

func (t *Transport) Messages() <-chan Delivery { return t.queue }

// Publish accepts only typed media downlink messages.
func (t *Transport) Publish(ctx context.Context, message any) error {
	switch m := message.(type) {
	case mediaprotocol.UploadGrant:
		return t.PublishGrant(ctx, m)
	case mediaprotocol.UploadResult:
		return t.PublishResult(ctx, m)
	default:
		return ErrInvalidMessage
	}
}

func (t *Transport) StopIntake() {
	t.stopped.Store(true)
	t.StopReconnect()
}

func (t *Transport) PublishGrant(ctx context.Context, grant mediaprotocol.UploadGrant) error {
	topic := mediaprotocol.UploadGrantTopic(grant.GatewayID)
	payload, err := mediaprotocol.Encode(topic, grant)
	if err != nil || ctx == nil || ctx.Err() != nil || t.stopped.Load() || t.wire == nil || !t.wire.Ready() {
		return ErrInvalidMessage
	}
	if grant.Status == mediaprotocol.GrantNeedUpload {
		expiry, err := time.Parse(time.RFC3339, grant.ExpiresAt)
		if err != nil || !expiry.After(time.Now()) {
			return ErrInvalidMessage
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, expiry)
		defer cancel()
	}
	return t.wire.Publish(ctx, mqttingress.Delivery{Topic: topic, Payload: payload, QoS: mediaprotocol.WireQoS, Retained: false})
}

func (t *Transport) PublishResult(ctx context.Context, result mediaprotocol.UploadResult) error {
	topic := mediaprotocol.UploadResultTopic(result.GatewayID)
	payload, err := mediaprotocol.Encode(topic, result)
	if err != nil || ctx == nil || ctx.Err() != nil || t.stopped.Load() || t.wire == nil || !t.wire.Ready() {
		return ErrInvalidMessage
	}
	return t.wire.Publish(ctx, mqttingress.Delivery{Topic: topic, Payload: payload, QoS: mediaprotocol.WireQoS, Retained: false})
}

var _ mqttingress.Admitter = (*Transport)(nil)

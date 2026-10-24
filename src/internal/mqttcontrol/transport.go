package mqttcontrol

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
	"iot-platform/internal/mqttingress"
)

// ResponsesFilter is the only backend control subscription.
const ResponsesFilter = "gateways/+/responses/#"

// RawResult is bounded transport groundwork only, not an accepted execution result.
type RawResult struct {
	Topic      string
	Payload    []byte
	ReceivedAt time.Time
}
type dataWire interface {
	Publish(context.Context, mqttingress.Delivery) error
	Ready() bool
}

// Transport reuses the ingress TLS/reconnect/PUBACK lifetime supervisor.
type Transport struct {
	*mqttingress.Transport
	wire       dataWire
	queue      chan RawResult
	maxPayload int
	stopped    atomic.Bool
}

// NewTransport requires a read-only ACL verifier independent of credential endpoints.
func NewTransport(opts mqttingress.TransportOptions, cfg Config) (*Transport, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if opts.VerifyConnection == nil || (opts.Filter != "" && opts.Filter != ResponsesFilter) {
		return nil, digitaltwin.ErrInvalidInput
	}
	opts.Filter = ResponsesFilter
	opts.RequireQoS1 = true
	opts.MaxConcurrentPublishes = cfg.Workers
	t := &Transport{queue: make(chan RawResult, cfg.ResultQueueCapacity), maxPayload: cfg.MaxPayloadBytes}
	base, err := mqttingress.NewTransport(opts, t)
	if err != nil {
		return nil, err
	}
	t.Transport = base
	t.wire = base
	return t, nil
}

// Publish only sends validated, unexpired command bytes at QoS 1, retain=false.
func (t *Transport) Publish(ctx context.Context, d digitaltwin.ControlDispatch) error {
	if ctx == nil || ctx.Err() != nil || t.stopped.Load() || !t.wire.Ready() {
		return digitaltwin.ErrDependencyUnavailable
	}
	command, err := digitaltwin.ParseMQTTCommand(d.Topic, d.Payload, false)
	if err != nil {
		return err
	}
	if command.CommandID != d.CommandID.String() || !command.ExpiresAt.After(time.Now()) {
		return digitaltwin.ErrInvalidInput
	}
	// Expiry also bounds the network wait; bytes, ID and timestamps never change.
	ctx, cancel := context.WithDeadline(ctx, command.ExpiresAt)
	defer cancel()
	return t.wire.Publish(ctx, mqttingress.Delivery{Topic: d.Topic, Payload: d.Payload, QoS: 1, Retained: false})
}

// Admit bounds/copies only. No result parsing, workers or database writes occur.
func (t *Transport) Admit(topic string, payload []byte, at time.Time, retained bool) mqttingress.AdmissionDecision {
	if t.stopped.Load() || retained || len(topic) > 256 || len(payload) == 0 || len(payload) > t.maxPayload {
		return mqttingress.AdmissionRejectedNotRunning
	}
	parts := strings.Split(topic, "/")
	if len(parts) != 4 || parts[0] != "gateways" || parts[2] != "responses" || gateway.ValidateGatewayID(parts[1]) != nil {
		return mqttingress.AdmissionRejectedNotRunning
	}
	id, err := uuid.Parse(parts[3])
	if err != nil || id.String() != parts[3] {
		return mqttingress.AdmissionRejectedNotRunning
	}
	select {
	case t.queue <- RawResult{topic, append([]byte(nil), payload...), at}:
		return mqttingress.AdmissionAccepted
	default:
		return mqttingress.AdmissionRejectedNotRunning
	}
}

// StopIntake rejects raw results and new commands before teardown.
func (t *Transport) StopIntake() { t.stopped.Store(true); t.StopReconnect() }

// Results exposes bounded raw deliveries for the future result-ingress task.
func (t *Transport) Results() <-chan RawResult { return t.queue }

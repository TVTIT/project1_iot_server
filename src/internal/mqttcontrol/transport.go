package mqttcontrol

import (
	"context"
	"encoding/json"
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
	QueuedAt   time.Time
	Retained   bool
}

// ResponseAdmissionMetrics aggregates drops that cannot safely be attributed to
// a command-result or receipt envelope before strict consumer parsing.
type ResponseAdmissionMetrics struct {
	rejected, queueFull atomic.Int64
}

// ResponseAdmissionMetricsSnapshot contains only bounded aggregate counters.
type ResponseAdmissionMetricsSnapshot struct {
	Rejected  int64 `json:"rejected"`
	QueueFull int64 `json:"queue_full"`
}

// Snapshot returns aggregate raw-response admission drops.
func (m *ResponseAdmissionMetrics) Snapshot() ResponseAdmissionMetricsSnapshot {
	return ResponseAdmissionMetricsSnapshot{Rejected: m.rejected.Load(), QueueFull: m.queueFull.Load()}
}

type dataWire interface {
	Publish(context.Context, mqttingress.Delivery) error
	Ready() bool
}

// Transport reuses the ingress TLS/reconnect/PUBACK lifetime supervisor.
type Transport struct {
	*mqttingress.Transport
	wire             dataWire
	queue            chan RawResult
	maxPayload       int
	stage6Enabled    bool
	stopped          atomic.Bool
	resultMetrics    *ResultMetrics
	receiptMetrics   *ReceiptMetrics
	admissionMetrics *ResponseAdmissionMetrics
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
	t := &Transport{queue: make(chan RawResult, cfg.ResultQueueCapacity), maxPayload: cfg.MaxPayloadBytes, stage6Enabled: cfg.Stage6Enabled}
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

// PublishConfigurationQuery publishes only a strictly validated, short-lived
// read-only query on the existing own commands topic and ACL.
func (t *Transport) PublishConfigurationQuery(ctx context.Context, query digitaltwin.ConfigurationQuery) error {
	if ctx == nil || ctx.Err() != nil || t.stopped.Load() || !t.stage6Enabled || !t.wire.Ready() {
		return digitaltwin.ErrDependencyUnavailable
	}
	now := time.Now().UTC()
	if !query.ValidAt(now) || query.ExpiresAt.Sub(query.IssuedAt) > 5*time.Second {
		return digitaltwin.ErrInvalidInput
	}
	payload, err := digitaltwin.MarshalMQTTConfigurationQuery(query)
	if err != nil {
		return err
	}
	topic := "gateways/" + query.GatewayID + "/commands/" + query.RequestID
	parsed, err := digitaltwin.ParseMQTTConfigurationQuery(topic, payload, false)
	if err != nil || parsed != query {
		return digitaltwin.ErrInvalidInput
	}
	publishCtx, cancel := context.WithDeadline(ctx, query.ExpiresAt)
	defer cancel()
	return t.wire.Publish(publishCtx, mqttingress.Delivery{Topic: topic, Payload: payload, QoS: 1, Retained: false})
}

// Admit bounds/copies raw responses only. Full response parsing and DB writes
// are deferred to the bounded consumer; only bounded queue-full hints affect
// per-envelope queue counters.
func (t *Transport) Admit(topic string, payload []byte, at time.Time, retained bool) mqttingress.AdmissionDecision {
	if t.stopped.Load() || retained || len(topic) > 256 || len(payload) == 0 || len(payload) > t.maxPayload {
		t.recordAdmissionReject()
		return mqttingress.AdmissionRejectedNotRunning
	}
	parts := strings.Split(topic, "/")
	if len(parts) != 4 || parts[0] != "gateways" || parts[2] != "responses" || gateway.ValidateGatewayID(parts[1]) != nil {
		t.recordAdmissionReject()
		return mqttingress.AdmissionRejectedNotRunning
	}
	id, err := uuid.Parse(parts[3])
	if err != nil || id.String() != parts[3] {
		t.recordAdmissionReject()
		return mqttingress.AdmissionRejectedNotRunning
	}
	select {
	case t.queue <- RawResult{Topic: topic, Payload: append([]byte(nil), payload...), ReceivedAt: at, QueuedAt: time.Now(), Retained: retained}:
		return mqttingress.AdmissionAccepted
	default:
		if t.admissionMetrics != nil {
			t.admissionMetrics.queueFull.Add(1)
		}
		// The validated admission path has already bounded payload to maxPayload.
		// This hint is diagnostic only; it never authorizes processing.
		messageType := responseMessageTypeHint(payload, t.maxPayload)
		if messageType == "command_receipt" {
			if t.receiptMetrics != nil {
				t.receiptMetrics.queueFull.Add(1)
			}
		} else if messageType == "command_result" && t.resultMetrics != nil {
			t.resultMetrics.queueFull.Add(1)
		}
		return mqttingress.AdmissionRejectedNotRunning
	}
}

// StopIntake rejects raw results and new commands before teardown.
func (t *Transport) StopIntake() { t.stopped.Store(true); t.StopReconnect() }

// Results exposes bounded raw deliveries for the future result-ingress task.
func (t *Transport) Results() <-chan RawResult { return t.queue }

// SetResultMetrics installs an optional observer before the transport starts.
func (t *Transport) SetResultMetrics(m *ResultMetrics) { t.resultMetrics = m }

// SetReceiptMetrics installs the receipt-only observer before transport start.
func (t *Transport) SetReceiptMetrics(m *ReceiptMetrics) { t.receiptMetrics = m }

// SetResponseAdmissionMetrics installs an aggregate observer before transport start.
func (t *Transport) SetResponseAdmissionMetrics(m *ResponseAdmissionMetrics) {
	t.admissionMetrics = m
}

func (t *Transport) recordAdmissionReject() {
	if t.admissionMetrics != nil {
		t.admissionMetrics.rejected.Add(1)
	}
}

// responseMessageTypeHint is a bounded, metrics-only discriminator for queue
// saturation. It does not gate or validate message processing.
func responseMessageTypeHint(payload []byte, maxBytes int) string {
	if maxBytes < 1 || len(payload) > maxBytes {
		return ""
	}
	var envelope struct {
		MessageType string `json:"message_type"`
	}
	if json.Unmarshal(payload, &envelope) != nil {
		return ""
	}
	return envelope.MessageType
}

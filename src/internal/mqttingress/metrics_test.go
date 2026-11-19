package mqttingress

import (
	"context"
	"errors"
	"testing"
	"time"

	"iot-platform/internal/telemetry"
)

func TestMetricsPipelineOutcomesAndDisabledParity(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, outcome := range []string{"new", "duplicate", "failure", "unconfirmed", "invalid"} {
			t.Run(outcome+map[bool]string{true: "/enabled", false: "/disabled"}[enabled], func(t *testing.T) {
				opts := defaultOptions()
				opts.MetricsEnabled = enabled
				repo := &fakeRepo{onPersist: func(_ context.Context, m *telemetry.Message) (telemetry.PersistResult, error) {
					switch outcome {
					case "duplicate":
						return duplicateResult(m), nil
					case "failure":
						return telemetry.PersistResult{}, errors.New("private driver error")
					case "unconfirmed":
						return telemetry.PersistResult{}, nil
					default:
						return committedResult(m), nil
					}
				}}
				pub := &fakePublisher{}
				i := newIngress(t, opts, newValidator(t), repo, pub)
				mustStart(t, i)
				topic, payload, at := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 3)
				if outcome == "invalid" {
					payload = []byte("invalid")
				}
				i.Admit(topic, payload, at, true)
				i.Admit(topic, payload, at, false)
				if err := i.Drain(context.Background()); err != nil {
					t.Fatal(err)
				}
				s := i.Snapshot()
				if s.Accepted != 1 || s.RejectedRetained != 1 || s.InFlight != 0 {
					t.Fatal(s)
				}
				wantACK := int64(0)
				if outcome == "new" || outcome == "duplicate" {
					wantACK = 1
				}
				if s.ACKPublished != wantACK || int64(pub.count()) != wantACK {
					t.Fatal(s)
				}
				if enabled {
					if s.Received != 2 || s.ReceivedBytes != int64(2*len(payload)) || s.AcceptedBytes != int64(len(payload)) || s.Durations.QueueWait.Count != 1 || s.Durations.Validation.Count != 1 {
						t.Fatal(s)
					}
					wantPersist := uint64(1)
					if outcome == "invalid" {
						wantPersist = 0
					}
					if s.Durations.Persistence.Count != wantPersist || s.Durations.ACKPublish.Count != uint64(wantACK) {
						t.Fatal(s)
					}
				} else if s.Received != 0 || s.Durations.Validation.Count != 0 {
					t.Fatal(s)
				}
				if outcome == "new" && (s.Committed != 1 || s.SamplesNew != 3) {
					t.Fatal(s)
				}
				if outcome == "duplicate" && (s.DuplicateCommitted != 1 || s.SamplesNew != 0) {
					t.Fatal(s)
				}
			})
		}
	}
}

func TestMetricsQueueWaitDoesNotUseSensorTime(t *testing.T) {
	opts := defaultOptions()
	opts.MetricsEnabled = true
	i := newIngress(t, opts, newValidator(t), &fakeRepo{}, &fakePublisher{})
	topic, payload, _ := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 1)
	i.process(item{topic: topic, payload: payload, receivedAt: time.Now().Add(-time.Hour), enqueuedAt: time.Now()})
	s := i.Snapshot()
	if s.Durations.QueueWait.SumSeconds >= 1 || s.InFlight != 0 {
		t.Fatal(s)
	}
}

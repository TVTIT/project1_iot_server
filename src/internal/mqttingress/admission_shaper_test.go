package mqttingress

import (
	"context"
	"errors"
	"testing"
	"time"
)

func shapedOptions() Options {
	opts := defaultOptions()
	opts.Shaper = ShaperOptions{Enabled: true, RatePerSecond: 1, Burst: 1, MaxGateways: 2, IdleRetention: time.Minute}
	return opts
}

func withShaperClock(t *testing.T, ing *Ingress, now *time.Time) {
	t.Helper()
	shaper, err := newShaper(ing.shaper.options, func() time.Time { return *now })
	if err != nil {
		t.Fatal(err)
	}
	ing.shaper = shaper
}

func TestAdmissionShaperCheapChecksDoNotAllocateOrDebit(t *testing.T) {
	opts := shapedOptions()
	validator := &fakeValidator{}
	ing := newIngress(t, opts, validator, &fakeRepo{}, &fakePublisher{})
	mustStart(t, ing)
	now := time.Now()
	withShaperClock(t, ing, &now)
	for _, tc := range []struct {
		topic    string
		payload  []byte
		retained bool
		want     AdmissionDecision
	}{
		{"gateways/gateway_001/telemetry/sensor_001", []byte("x"), true, AdmissionRejectedRetained},
		{"gateways/gateway_001/telemetry/sensor_001", make([]byte, opts.MaxPayloadBytes+1), false, AdmissionRejectedOversized},
		{"bad/topic", []byte("x"), false, AdmissionRejectedInvalidTopic},
	} {
		if got := ing.Admit(tc.topic, tc.payload, now, tc.retained); got != tc.want {
			t.Fatalf("Admit(%q) = %v, want %v", tc.topic, got, tc.want)
		}
	}
	if entries, _ := ing.shaper.EntriesAndCap(); entries != 0 {
		t.Fatalf("cheap checks allocated %d shaper entries", entries)
	}
	if got := ing.Admit("gateways/gateway_001/telemetry/sensor_001", []byte("{}"), now, false); got != AdmissionAccepted {
		t.Fatalf("first valid admission = %v, want accepted (cheap checks must not debit)", got)
	}
	if got, _ := ing.shaper.EntriesAndCap(); got != 1 {
		t.Fatalf("entry count after valid admission = %d, want 1", got)
	}
	_ = ing.Drain(context.Background())
}

func TestAdmissionShaperSharesGatewayBucketAndSeparatesGateways(t *testing.T) {
	opts := shapedOptions()
	ing := newIngress(t, opts, &fakeValidator{}, &fakeRepo{}, &fakePublisher{})
	mustStart(t, ing)
	now := time.Now()
	withShaperClock(t, ing, &now)
	admit := func(gateway, sensor string) AdmissionDecision {
		return ing.Admit("gateways/"+gateway+"/telemetry/"+sensor, []byte("x"), now, false)
	}
	if got := admit("gateway_001", "sensor_001"); got != AdmissionAccepted {
		t.Fatalf("first = %v", got)
	}
	if got := admit("gateway_001", "sensor_002"); got != AdmissionRejectedRateLimit {
		t.Fatalf("same Gateway second Sensor = %v, want rate limit", got)
	}
	if got := admit("gateway_002", "sensor_001"); got != AdmissionAccepted {
		t.Fatalf("independent Gateway = %v, want accepted", got)
	}
	_ = ing.Drain(context.Background())
}

func TestAdmissionShaperRateAndCapacityRejectBeforeWorker(t *testing.T) {
	for _, tc := range []struct {
		name   string
		second string
		want   AdmissionDecision
	}{
		{"rate", "gateway_001", AdmissionRejectedRateLimit},
		{"state capacity", "gateway_002", AdmissionRejectedShaperCapacity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := shapedOptions()
			opts.MetricsEnabled = false
			if tc.name == "state capacity" {
				opts.Shaper.MaxGateways = 1
			}
			validator := &gatedByteValidator{entered: make(chan []byte, 2), release: make(chan struct{}), err: errors.New("hold")}
			repo, pub := &fakeRepo{}, &fakePublisher{}
			ing := newIngress(t, opts, validator, repo, pub)
			mustStart(t, ing)
			now := time.Now()
			withShaperClock(t, ing, &now)
			if got := ing.Admit("gateways/gateway_001/telemetry/sensor_001", []byte("x"), now, false); got != AdmissionAccepted {
				t.Fatalf("seed = %v", got)
			}
			<-validator.entered
			got := ing.Admit("gateways/"+tc.second+"/telemetry/sensor_001", []byte("x"), now, false)
			if got != tc.want {
				t.Fatalf("rejection = %v, want %v", got, tc.want)
			}
			select {
			case <-validator.entered:
				t.Fatal("rejected attempt reached validation")
			default:
			}
			if repo.count() != 0 || pub.count() != 0 {
				t.Fatalf("rejected attempt reached persistence/ACK: repo=%d publisher=%d", repo.count(), pub.count())
			}
			s := ing.Snapshot()
			if tc.want == AdmissionRejectedRateLimit && s.ShaperRateLimited != 1 {
				t.Fatalf("rate counter = %d", s.ShaperRateLimited)
			}
			if tc.want == AdmissionRejectedShaperCapacity && s.ShaperStateCapacity != 1 {
				t.Fatalf("capacity counter = %d", s.ShaperStateCapacity)
			}
			if s.ShaperPassed != 1 || !s.ShaperEnabled {
				t.Fatalf("shaper snapshot = %+v", s)
			}
			close(validator.release)
			_ = ing.Drain(context.Background())
		})
	}
}

func TestAdmissionShaperDoesNotRefundOnByteOrQueueRefusal(t *testing.T) {
	for _, failAt := range []string{"byte", "queue"} {
		t.Run(failAt, func(t *testing.T) {
			opts := shapedOptions()
			opts.QueueCapacity = 1
			opts.MaxPayloadBytes = 4
			opts.BufferedPayloadBytes = 4
			opts.Shaper.MaxGateways = 3
			if failAt == "queue" {
				opts.BufferedPayloadBytes = 12
			}
			validator := &gatedByteValidator{entered: make(chan []byte, 1), release: make(chan struct{}), err: errors.New("hold")}
			ing := newIngress(t, opts, validator, &fakeRepo{}, &fakePublisher{})
			mustStart(t, ing)
			now := time.Now()
			withShaperClock(t, ing, &now)
			if got := ing.Admit("gateways/gateway_001/telemetry/sensor_001", []byte("1234"), now, false); got != AdmissionAccepted {
				t.Fatalf("initial = %v", got)
			}
			<-validator.entered
			want := AdmissionRejectedByteBudget
			if failAt == "queue" {
				// Occupy the sole queue slot while the worker is held in validation.
				if got := ing.Admit("gateways/gateway_002/telemetry/sensor_001", []byte("q"), now, false); got != AdmissionAccepted {
					t.Fatalf("queue filler = %v", got)
				}
				if got := ing.Admit("gateways/gateway_003/telemetry/sensor_001", []byte("r"), now, false); got != AdmissionRejectedQueueFull {
					t.Fatalf("queue refusal = %v", got)
				}
				if got := ing.Admit("gateways/gateway_003/telemetry/sensor_002", []byte("s"), now, false); got != AdmissionRejectedRateLimit {
					t.Fatalf("retry after queue refusal = %v, want rate limit", got)
				}
				close(validator.release)
				_ = ing.Drain(context.Background())
				return
			}
			if got := ing.Admit("gateways/gateway_002/telemetry/sensor_001", []byte("x"), now, false); got != want {
				t.Fatalf("refusal = %v, want %v", got, want)
			}
			if got := ing.Admit("gateways/gateway_002/telemetry/sensor_002", []byte("x"), now, false); got != AdmissionRejectedRateLimit {
				t.Fatalf("retry after refusal = %v, want rate limit", got)
			}
			close(validator.release)
			_ = ing.Drain(context.Background())
		})
	}
}

func TestAdmissionShaperOffAndOnShareByteBudgetAndMetricsOffEnforces(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		opts := defaultOptions()
		opts.MetricsEnabled = false
		opts.MaxPayloadBytes = 4
		opts.BufferedPayloadBytes = 4
		opts.Shaper = ShaperOptions{Enabled: enabled, RatePerSecond: 1, Burst: 1, MaxGateways: 2, IdleRetention: time.Minute}
		validator := &gatedByteValidator{entered: make(chan []byte, 1), release: make(chan struct{}), err: errors.New("stop")}
		ing := newIngress(t, opts, validator, &fakeRepo{}, &fakePublisher{})
		mustStart(t, ing)
		if got := ing.Admit("gateways/gateway_001/telemetry/sensor_001", []byte("1234"), time.Now(), false); got != AdmissionAccepted {
			t.Fatalf("enabled=%v initial = %v", enabled, got)
		}
		<-validator.entered
		if got := ing.Admit("gateways/gateway_002/telemetry/sensor_002", []byte("x"), time.Now(), false); got != AdmissionRejectedByteBudget {
			t.Fatalf("enabled=%v byte refusal = %v", enabled, got)
		}
		s := ing.Snapshot()
		if s.BufferedPayloadBytes != 4 || s.ByteBudgetFull != 1 || s.ShaperEnabled != enabled || s.ShaperPassed != 2*boolInt64(enabled) {
			t.Fatalf("enabled=%v snapshot=%+v", enabled, s)
		}
		close(validator.release)
		_ = ing.Drain(context.Background())
	}
}

func boolInt64(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func TestAdmissionShaperOptionsRejectInvalidEnabledConfiguration(t *testing.T) {
	opts := defaultOptions()
	opts.Shaper = ShaperOptions{Enabled: true}
	if _, err := New(opts, &fakeValidator{}, &fakeRepo{}, &fakePublisher{}); err == nil {
		t.Fatal("New accepted invalid enabled shaper options")
	}
	if _, err := New(defaultOptions(), &fakeValidator{}, &fakeRepo{}, &fakePublisher{}); err != nil {
		t.Fatalf("zero disabled shaper rejected: %v", err)
	}
}

func TestTransportCallbackUsesIngressShaperDecisionWithoutACK(t *testing.T) {
	opts := shapedOptions()
	validator, repo, pub := &fakeValidator{}, &fakeRepo{}, &fakePublisher{}
	ing := newIngress(t, opts, validator, repo, pub)
	mustStart(t, ing)
	now := time.Now()
	withShaperClock(t, ing, &now)
	tr := &Transport{admit: ing, opts: TransportOptions{now: func() time.Time { return now }}}
	lt := newLifetime(1)
	lt.subscribed.Store(true)
	tr.current = lt
	tr.onMessage(lt, "gateways/gateway_001/telemetry/sensor_001", []byte("{}"), false)
	tr.onMessage(lt, "gateways/gateway_001/telemetry/sensor_002", []byte("{}"), false)
	if got := ing.Snapshot(); got.Accepted != 1 || got.ShaperRateLimited != 1 {
		t.Fatalf("callback decisions not reflected in ingress: %+v", got)
	}
	if pub.count() != 0 {
		t.Fatalf("callback rejection emitted application ACK: publishes=%d", pub.count())
	}
	_ = ing.Drain(context.Background())
}

var _ Validator = (*fakeValidator)(nil)
var _ Repository = (*fakeRepo)(nil)
var _ Publisher = (*fakePublisher)(nil)

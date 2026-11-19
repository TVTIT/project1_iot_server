package mqttingress

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"iot-platform/internal/telemetry"
)

type gatedByteValidator struct {
	entered chan []byte
	release chan struct{}
	err     error
}

func (v *gatedByteValidator) Validate(_ string, payload []byte, _ time.Time) (*telemetry.Message, error) {
	v.entered <- payload
	<-v.release
	return nil, v.err
}

func TestBufferedPayloadBudgetReservesThroughProcessingAndCopiesPayload(t *testing.T) {
	validator := &gatedByteValidator{entered: make(chan []byte, 1), release: make(chan struct{}), err: errors.New("stop")}
	repo, pub := &fakeRepo{}, &fakePublisher{}
	opts := defaultOptions()
	opts.MaxPayloadBytes = 4
	opts.BufferedPayloadBytes = 4
	ing := newIngress(t, opts, validator, repo, pub)
	mustStart(t, ing)

	payload := []byte("four")
	if got := ing.Admit("gateways/gateway_001/telemetry/sensor_001", payload, time.Now(), false); got != AdmissionAccepted {
		t.Fatalf("first admission = %v, want accepted", got)
	}
	workerPayload := <-validator.entered
	payload[0] = 'X'
	if string(workerPayload) != "four" {
		t.Fatalf("worker payload = %q, want independent copy", workerPayload)
	}
	if got := ing.Snapshot().BufferedPayloadBytes; got != 4 {
		t.Fatalf("buffered bytes while validation blocked = %d, want 4", got)
	}
	if got := ing.Admit("gateways/gateway_001/telemetry/sensor_002", []byte("x"), time.Now(), false); got != AdmissionRejectedByteBudget {
		t.Fatalf("second admission = %v, want byte-budget rejection", got)
	}
	close(validator.release)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if got := ing.Snapshot().BufferedPayloadBytes; got != 0 {
		t.Fatalf("buffered bytes after drain = %d, want 0", got)
	}
}

func TestBufferedPayloadBudgetReleasesAfterQueueFull(t *testing.T) {
	validator := &gatedByteValidator{entered: make(chan []byte, 1), release: make(chan struct{}), err: errors.New("stop")}
	opts := defaultOptions()
	opts.QueueCapacity = 1
	opts.WorkerCount = 1
	opts.MaxPayloadBytes = 4
	opts.BufferedPayloadBytes = 12
	ing := newIngress(t, opts, validator, &fakeRepo{}, &fakePublisher{})
	mustStart(t, ing)
	topic := "gateways/gateway_001/telemetry/sensor_001"
	if got := ing.Admit(topic, []byte("1234"), time.Now(), false); got != AdmissionAccepted {
		t.Fatalf("active admission = %v", got)
	}
	<-validator.entered
	if got := ing.Admit(topic, []byte("5678"), time.Now(), false); got != AdmissionAccepted {
		t.Fatalf("queued admission = %v", got)
	}
	if got := ing.Admit(topic, []byte("9"), time.Now(), false); got != AdmissionRejectedQueueFull {
		t.Fatalf("full queue admission = %v, want queue-full", got)
	}
	if got := ing.Snapshot().BufferedPayloadBytes; got != 8 {
		t.Fatalf("buffered bytes after queue-full = %d, want 8", got)
	}
	close(validator.release)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if got := ing.Snapshot().BufferedPayloadBytes; got != 0 {
		t.Fatalf("buffered bytes after drain = %d, want 0", got)
	}
}

func TestBufferedPayloadBudgetValidationAndMetricsOff(t *testing.T) {
	defaultOpts := defaultOptions()
	defaultIngress := newIngress(t, defaultOpts, &fakeValidator{}, &fakeRepo{}, &fakePublisher{})
	if got, want := defaultIngress.Snapshot().BufferedPayloadCapacityBytes, int64(16<<20); got != want {
		t.Fatalf("default buffered payload capacity = %d, want %d", got, want)
	}
	for _, tc := range []struct {
		name string
		cap  int64
		want string
	}{
		{name: "negative", cap: -1, want: "buffered payload bytes"},
		{name: "below max payload", cap: 3, want: "buffered payload bytes"},
		{name: "above supported maximum", cap: 256<<20 + 1, want: "buffered payload bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := defaultOptions()
			opts.MaxPayloadBytes = 4
			opts.BufferedPayloadBytes = tc.cap
			_, err := New(opts, &fakeValidator{}, &fakeRepo{}, &fakePublisher{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("New error = %v, want containing %q", err, tc.want)
			}
		})
	}

	validator := &gatedByteValidator{entered: make(chan []byte, 1), release: make(chan struct{}), err: errors.New("stop")}
	opts := defaultOptions()
	opts.MetricsEnabled = false
	opts.MaxPayloadBytes = 4
	opts.BufferedPayloadBytes = 4
	ing := newIngress(t, opts, validator, &fakeRepo{}, &fakePublisher{})
	mustStart(t, ing)
	topic := "gateways/gateway_001/telemetry/sensor_001"
	if got := ing.Admit(topic, []byte("1234"), time.Now(), false); got != AdmissionAccepted {
		t.Fatalf("admission = %v", got)
	}
	<-validator.entered
	if got := ing.Admit(topic, []byte("x"), time.Now(), false); got != AdmissionRejectedByteBudget {
		t.Fatalf("metrics-off admission = %v, want byte-budget rejection", got)
	}
	snapshot := ing.Snapshot()
	if snapshot.BufferedPayloadBytes != 4 || snapshot.BufferedPayloadCapacityBytes != 4 || snapshot.BufferedPayloadHighWaterBytes != 4 || snapshot.ByteBudgetFull != 1 {
		t.Fatalf("metrics-off budget snapshot = %+v", snapshot)
	}
	close(validator.release)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
}

func TestBufferedPayloadBudgetConcurrentAdmissionStaysBounded(t *testing.T) {
	validator := &gatedByteValidator{entered: make(chan []byte, 32), release: make(chan struct{}), err: errors.New("stop")}
	opts := defaultOptions()
	opts.QueueCapacity = 32
	opts.WorkerCount = 1
	opts.MaxPayloadBytes = 4
	opts.BufferedPayloadBytes = 16
	ing := newIngress(t, opts, validator, &fakeRepo{}, &fakePublisher{})
	mustStart(t, ing)
	topic := "gateways/gateway_001/telemetry/sensor_001"
	var wg sync.WaitGroup
	for n := 0; n < 32; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ing.Admit(topic, []byte("1234"), time.Now(), false)
		}()
	}
	wg.Wait()
	snapshot := ing.Snapshot()
	if snapshot.BufferedPayloadBytes > snapshot.BufferedPayloadCapacityBytes || snapshot.BufferedPayloadHighWaterBytes > snapshot.BufferedPayloadCapacityBytes {
		t.Fatalf("byte budget exceeded: %+v", snapshot)
	}
	close(validator.release)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if got := ing.Snapshot().BufferedPayloadBytes; got != 0 {
		t.Fatalf("buffered bytes after drain = %d, want 0", got)
	}
}

func TestBufferedPayloadBudgetIsHeldUntilACKReturns(t *testing.T) {
	topic, payload, receivedAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 1)
	validator := newValidator(t)
	ackEntered := make(chan struct{})
	ackRelease := make(chan struct{})
	pub := &fakePublisher{onPublish: func(ctx context.Context, _ Delivery) error {
		close(ackEntered)
		select {
		case <-ackRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	opts := defaultOptions()
	opts.BufferedPayloadBytes = int64(opts.MaxPayloadBytes)
	ing := newIngress(t, opts, validator, &fakeRepo{}, pub)
	mustStart(t, ing)
	if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionAccepted {
		t.Fatalf("admission = %v, want accepted", got)
	}
	select {
	case <-ackEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not reach ACK publish")
	}
	if got := ing.Snapshot().BufferedPayloadBytes; got != int64(len(payload)) {
		t.Fatalf("buffered bytes during ACK = %d, want %d", got, len(payload))
	}
	close(ackRelease)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if got := ing.Snapshot().BufferedPayloadBytes; got != 0 {
		t.Fatalf("buffered bytes after ACK/drain = %d, want 0", got)
	}
}

func TestBufferedPayloadBudgetReturnsAfterForcedDrain(t *testing.T) {
	topic, payload, receivedAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 1)
	opts := defaultOptions()
	opts.BufferedPayloadBytes = int64(opts.MaxPayloadBytes)
	repo := &fakeRepo{enter: make(chan *telemetry.Message, 1), release: make(chan struct{})}
	ing := newIngress(t, opts, newValidator(t), repo, &fakePublisher{})
	mustStart(t, ing)
	if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionAccepted {
		t.Fatalf("admission = %v, want accepted", got)
	}
	waitMessage(t, repo.enter)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ing.Drain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain error = %v, want context.Canceled", err)
	}
	if got := ing.Snapshot().BufferedPayloadBytes; got != 0 {
		t.Fatalf("buffered bytes after forced drain = %d, want 0", got)
	}
}

func TestBufferedPayloadBudgetReturnsForWorkerOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		validation error
		persist    func(*fakeRepo)
		publishErr error
	}{
		{name: "validation rejection", validation: errors.New("invalid")},
		{name: "persist failure", persist: func(r *fakeRepo) {
			r.onPersist = func(context.Context, *telemetry.Message) (telemetry.PersistResult, error) {
				return telemetry.PersistResult{}, errors.New("database")
			}
		}},
		{name: "unconfirmed commit", persist: func(r *fakeRepo) {
			r.onPersist = func(_ context.Context, m *telemetry.Message) (telemetry.PersistResult, error) {
				result := committedResult(m)
				result.Outcome = telemetry.CommitOutcomeUnconfirmed
				return result, nil
			}
		}},
		{name: "identity mismatch", persist: func(r *fakeRepo) {
			r.onPersist = func(_ context.Context, m *telemetry.Message) (telemetry.PersistResult, error) {
				result := committedResult(m)
				result.Identity.SensorID = "sensor_other"
				return result, nil
			}
		}},
		{name: "ACK failure", publishErr: errors.New("broker unavailable")},
		{name: "normal completion"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic, payload, receivedAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 1)
			var validator Validator = newValidator(t)
			if tc.validation != nil {
				validator = rejectAllValidator{err: tc.validation}
			}
			repo, pub := &fakeRepo{}, &fakePublisher{}
			if tc.persist != nil {
				tc.persist(repo)
			}
			if tc.publishErr != nil {
				pub.onPublish = func(context.Context, Delivery) error { return tc.publishErr }
			}
			opts := defaultOptions()
			opts.BufferedPayloadBytes = int64(opts.MaxPayloadBytes)
			ing := newIngress(t, opts, validator, repo, pub)
			mustStart(t, ing)
			if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionAccepted {
				t.Fatalf("admission = %v", got)
			}
			if err := ing.Drain(context.Background()); err != nil {
				t.Fatalf("Drain: %v", err)
			}
			if got := ing.Snapshot().BufferedPayloadBytes; got != 0 {
				t.Fatalf("buffered bytes after %s = %d, want zero", tc.name, got)
			}
		})
	}
}

func TestDeadlineCrossingAfterPopStillTransfersReservation(t *testing.T) {
	// The clock crosses the absolute deadline during the destructive Pop. Once
	// Pop succeeds, the returned item is owned and must be validated/released.
	start := time.Unix(100, 0)
	clock := &deadlineCrossingClock{before: start, after: start.Add(time.Second)}
	opts := defaultOptions()
	opts.BufferedPayloadBytes = int64(opts.MaxPayloadBytes)
	ing, err := New(opts, newValidator(t), &fakeRepo{}, &fakePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	ing.setClock(clock)
	payload := []byte("payload")
	if !ing.reservePayloadBytes(int64(len(payload))) {
		t.Fatal("reservation failed")
	}
	queued := item{payload: payload, reservedBytes: int64(len(payload))}
	scheduler := newSensorScheduler(2, 1)
	if ok, _ := scheduler.Enqueue("gateway_001", "sensor_001", item{}); !ok {
		t.Fatal("initial enqueue failed")
	}
	lease, err := scheduler.TryAcquire()
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := scheduler.Enqueue("gateway_001", "sensor_001", queued); !ok {
		t.Fatal("queued enqueue failed")
	}
	got, ok, reason, _ := takeSensorGroupItem(ing, lease, start.Add(time.Millisecond))
	if !ok || reason != 0 || got.reservedBytes != int64(len(payload)) {
		t.Fatalf("take = (%v, %v, %d), want acquired item despite deadline crossing", got.reservedBytes, ok, reason)
	}
	if !clock.Now().After(start.Add(time.Millisecond)) {
		t.Fatal("controlled clock did not cross the deadline after Pop")
	}
	ing.releasePayloadBytes(got.reservedBytes)
	lease.Release()
	if got := ing.Snapshot().BufferedPayloadBytes; got != 0 {
		t.Fatalf("buffered bytes = %d, want 0", got)
	}
}

type deadlineCrossingClock struct {
	before, after time.Time
	calls         int
}

func (c *deadlineCrossingClock) Now() time.Time {
	c.calls++
	if c.calls == 1 {
		return c.before
	}
	return c.after
}
func (*deadlineCrossingClock) NewTimer(time.Duration) ingressTimer {
	return &controlledTimer{ch: make(chan time.Time, 1)}
}

type rejectAllValidator struct{ err error }

func (v rejectAllValidator) Validate(string, []byte, time.Time) (*telemetry.Message, error) {
	return nil, v.err
}

package mqttingress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

var (
	testMessageID = uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
	testBootID    = uuid.MustParse("7f2c45f7-0a76-4e28-8a37-7793d7d85b04")
)

// --- fakes -----------------------------------------------------------------

type fakeValidator struct {
	mu            sync.Mutex
	calls         int
	gotTopic      string
	gotPayload    []byte
	gotReceivedAt time.Time
	err           error
}

func (v *fakeValidator) Validate(topic string, payload []byte, receivedAt time.Time) (*telemetry.Message, error) {
	v.mu.Lock()
	v.calls++
	v.gotTopic = topic
	v.gotPayload = append([]byte(nil), payload...)
	v.gotReceivedAt = receivedAt
	err := v.err
	v.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return nil, &telemetry.ReasonError{Code: telemetry.ReasonInvalidPayload, Field: "fake"}
}

type fakeRepo struct {
	mu            sync.Mutex
	calls         int
	concurrent    int
	maxConcurrent int
	enter         chan *telemetry.Message
	release       chan struct{}
	onPersist     func(context.Context, *telemetry.Message) (telemetry.PersistResult, error)
}

func (r *fakeRepo) Persist(ctx context.Context, m *telemetry.Message) (telemetry.PersistResult, error) {
	r.mu.Lock()
	r.calls++
	r.concurrent++
	if r.concurrent > r.maxConcurrent {
		r.maxConcurrent = r.concurrent
	}
	enter := r.enter
	release := r.release
	handler := r.onPersist
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.concurrent--
		r.mu.Unlock()
	}()

	if enter != nil {
		enter <- m
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return telemetry.PersistResult{}, ctx.Err()
		}
	}
	if handler != nil {
		return handler(ctx, m)
	}
	return committedResult(m), nil
}

func (r *fakeRepo) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *fakeRepo) maxParallel() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.maxConcurrent
}

type fakePublisher struct {
	mu        sync.Mutex
	calls     int
	got       []Delivery
	signal    chan Delivery
	onPublish func(context.Context, Delivery) error
}

func (p *fakePublisher) Publish(ctx context.Context, d Delivery) error {
	p.mu.Lock()
	p.calls++
	p.got = append(p.got, d)
	sig := p.signal
	handler := p.onPublish
	p.mu.Unlock()

	if handler != nil {
		if err := handler(ctx, d); err != nil {
			return err
		}
	}
	if sig != nil {
		sig <- d
	}
	return nil
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *fakePublisher) deliveries() []Delivery {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Delivery(nil), p.got...)
}

// --- helpers ---------------------------------------------------------------

func committedResult(m *telemetry.Message) telemetry.PersistResult {
	return telemetry.PersistResult{
		Identity:     identityOf(m),
		Outcome:      telemetry.CommitOutcomeCommitted,
		SamplesSaved: len(m.Samples()),
	}
}

func duplicateResult(m *telemetry.Message) telemetry.PersistResult {
	return telemetry.PersistResult{Identity: identityOf(m), Outcome: telemetry.CommitOutcomeDuplicateCommitted}
}

func identityOf(m *telemetry.Message) telemetry.MessageIdentity {
	return telemetry.MessageIdentity{
		GatewayID: m.Topic().GatewayID,
		SensorID:  m.Topic().SensorID,
		BootID:    m.Batch().BootID,
		MessageID: m.Batch().MessageID,
	}
}

func newValidator(t *testing.T) *telemetry.Validator {
	t.Helper()
	v, err := telemetry.NewValidator(telemetry.Limits{
		MaxPayloadBytes:  65536,
		MaxSamples:       1000,
		MaxBatchDuration: time.Minute,
		MaxFutureSkew:    2 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func testPayloadFor(t *testing.T, gatewayID, sensorID string, messageID, bootID uuid.UUID, samples int) (string, []byte, time.Time) {
	t.Helper()
	measuredAt := time.Now().UTC().Truncate(time.Microsecond)
	values := make([]string, samples)
	for idx := range values {
		values[idx] = "1"
	}
	payload := []byte(fmt.Sprintf(
		`{"protocol_version":1,"message_id":%q,"message_type":"telemetry_batch","gateway_id":%q,"sensor_id":%q,"boot_id":%q,"first_sequence":100,"sample_count":%d,"measured_at":%q,"sample_interval_us":10000,"samples":[%s]}`,
		messageID.String(), gatewayID, sensorID, bootID.String(), samples,
		measuredAt.Format("2006-01-02T15:04:05.000000Z"), strings.Join(values, ","),
	))
	topic := "gateways/" + gatewayID + "/telemetry/" + sensorID
	return topic, payload, measuredAt
}

func defaultOptions() Options {
	return Options{
		QueueCapacity:   16,
		WorkerCount:     1,
		MaxPayloadBytes: 65536,
		PersistTimeout:  2 * time.Second,
		PublishTimeout:  2 * time.Second,
	}
}

func newIngress(t *testing.T, opts Options, validator Validator, repo Repository, pub Publisher) *Ingress {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	ing, err := New(opts, validator, repo, pub)
	if err != nil {
		t.Fatal(err)
	}
	return ing
}

func mustStart(t *testing.T, ing *Ingress) {
	t.Helper()
	if err := ing.Start(); err != nil {
		t.Fatal(err)
	}
}

func waitMessage(t *testing.T, ch <-chan *telemetry.Message) *telemetry.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Persist entry")
		return nil
	}
}

func waitDelivery(t *testing.T, ch <-chan Delivery) Delivery {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ACK delivery")
		return Delivery{}
	}
}

// decodeACK checks the exact five-field shape and returns the parsed UTC time.
func decodeACK(t *testing.T, d Delivery) time.Time {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(d.Payload, &fields); err != nil {
		t.Fatalf("ACK payload is not a JSON object: %v", err)
	}
	for _, key := range []string{"protocol_version", "message_type", "message_id", "status", "committed_at"} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("ACK missing field %q: %s", key, d.Payload)
		}
	}
	if len(fields) != 5 {
		t.Fatalf("ACK has %d fields, want exactly 5: %s", len(fields), d.Payload)
	}
	var version int
	if err := json.Unmarshal(fields["protocol_version"], &version); err != nil || version != 1 {
		t.Fatalf("protocol_version = %s, want 1", fields["protocol_version"])
	}
	var messageType, messageID, status string
	for raw, dst := range map[string]*string{
		"message_type": &messageType, "message_id": &messageID, "status": &status,
	} {
		if err := json.Unmarshal(fields[raw], dst); err != nil {
			t.Fatalf("field %s is not a string: %v", raw, err)
		}
	}
	if messageType != "telemetry_ack" || messageID != testMessageID.String() || status != "committed" {
		t.Fatalf("ACK fields = type=%q id=%q status=%q", messageType, messageID, status)
	}
	var rawTime string
	if err := json.Unmarshal(fields["committed_at"], &rawTime); err != nil {
		t.Fatalf("committed_at is not a string: %v", err)
	}
	parsed, err := time.Parse(time.RFC3339Nano, rawTime)
	if err != nil {
		t.Fatalf("committed_at is not RFC3339: %v", err)
	}
	if parsed.Location() != time.UTC {
		t.Fatalf("committed_at location = %v, want UTC", parsed.Location())
	}
	return parsed
}

func assertDeliveryShape(t *testing.T, d Delivery, measuredAt time.Time) time.Time {
	t.Helper()
	wantTopic := "gateways/gateway_001/acks/" + testMessageID.String()
	if d.Topic != wantTopic {
		t.Fatalf("ACK topic = %q, want %q", d.Topic, wantTopic)
	}
	if d.QoS != 1 || d.Retained {
		t.Fatalf("ACK QoS=%d retained=%v, want QoS 1 non-retained", d.QoS, d.Retained)
	}
	committedAt := decodeACK(t, d)
	if !committedAt.After(measuredAt) {
		t.Fatalf("committed_at %v is not after measurement time %v (must not be measurement time)", committedAt, measuredAt)
	}
	return committedAt
}

// --- pipeline tests --------------------------------------------------------

func TestACKOnlyAfterConfirmedCommit(t *testing.T) {
	repo := &fakeRepo{enter: make(chan *telemetry.Message, 4), release: make(chan struct{})}
	pub := &fakePublisher{signal: make(chan Delivery, 4)}
	ing := newIngress(t, defaultOptions(), newValidator(t), repo, pub)
	mustStart(t, ing)

	topic, payload, measuredAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 3)
	if got := ing.Admit(topic, payload, measuredAt, false); got != AdmissionAccepted {
		t.Fatalf("Admit = %v, want accepted", got)
	}

	// Persist has entered and is blocked on release; no ACK may exist yet.
	waitMessage(t, repo.enter)
	if pub.count() != 0 {
		t.Fatalf("publisher called %d times before the commit was confirmed", pub.count())
	}

	close(repo.release)
	d := waitDelivery(t, pub.signal)
	assertDeliveryShape(t, d, measuredAt)

	snap := ing.Snapshot()
	if snap.Committed != 1 || snap.SamplesNew != 3 || snap.ACKPublished != 1 {
		t.Fatalf("snapshot after commit = %+v", snap)
	}
	if snap.PersistFailures != 0 || snap.ValidationRejected != 0 {
		t.Fatalf("unexpected failures in snapshot: %+v", snap)
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain = %v", err)
	}
	if pub.count() != 1 {
		t.Fatalf("ack published %d times, want exactly 1", pub.count())
	}
}

func TestDuplicateCommittedReACKs(t *testing.T) {
	repo := &fakeRepo{onPersist: func(_ context.Context, m *telemetry.Message) (telemetry.PersistResult, error) {
		return duplicateResult(m), nil
	}}
	pub := &fakePublisher{signal: make(chan Delivery, 4)}
	ing := newIngress(t, defaultOptions(), newValidator(t), repo, pub)
	mustStart(t, ing)

	topic, payload, measuredAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 3)
	if got := ing.Admit(topic, payload, measuredAt, false); got != AdmissionAccepted {
		t.Fatalf("Admit = %v, want accepted", got)
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain = %v", err)
	}
	d := waitDelivery(t, pub.signal)
	assertDeliveryShape(t, d, measuredAt)

	snap := ing.Snapshot()
	if snap.DuplicateCommitted != 1 || snap.Committed != 0 || snap.SamplesNew != 0 {
		t.Fatalf("duplicate snapshot = %+v", snap)
	}
}

func TestNoACKOnErrorUnconfirmedOrMismatch(t *testing.T) {
	tests := []struct {
		name       string
		onPersist  func(context.Context, *telemetry.Message) (telemetry.PersistResult, error)
		assertSnap func(*testing.T, Snapshot)
	}{
		{
			name: "persist error overrides an apparently committed result",
			onPersist: func(_ context.Context, m *telemetry.Message) (telemetry.PersistResult, error) {
				return committedResult(m), errors.New("adapter reported success but returned an error")
			},
			assertSnap: func(t *testing.T, s Snapshot) {
				if s.PersistFailures != 1 || s.Committed != 0 || s.ACKPublished != 0 {
					t.Fatalf("snapshot = %+v", s)
				}
			},
		},
		{
			name: "zero outcome is never success",
			onPersist: func(_ context.Context, _ *telemetry.Message) (telemetry.PersistResult, error) {
				return telemetry.PersistResult{}, nil
			},
			assertSnap: func(t *testing.T, s Snapshot) {
				if s.CommitUnconfirmed != 1 || s.ACKPublished != 0 {
					t.Fatalf("snapshot = %+v", s)
				}
			},
		},
		{
			name: "mismatched identity suppresses the ACK",
			onPersist: func(_ context.Context, m *telemetry.Message) (telemetry.PersistResult, error) {
				r := committedResult(m)
				r.Identity.GatewayID = "gateway_002"
				return r, nil
			},
			assertSnap: func(t *testing.T, s Snapshot) {
				if s.IdentityMismatch != 1 || s.ACKPublished != 0 {
					t.Fatalf("snapshot = %+v", s)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeRepo{onPersist: tt.onPersist}
			pub := &fakePublisher{}
			ing := newIngress(t, defaultOptions(), newValidator(t), repo, pub)
			mustStart(t, ing)

			topic, payload, receivedAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 2)
			if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionAccepted {
				t.Fatalf("Admit = %v, want accepted", got)
			}
			if err := ing.Drain(context.Background()); err != nil {
				t.Fatalf("Drain = %v", err)
			}
			if got := len(pub.deliveries()); got != 0 {
				t.Fatalf("published %d ACKs, want 0", got)
			}
			tt.assertSnap(t, ing.Snapshot())
		})
	}
}

func TestFixedWorkerBound(t *testing.T) {
	repo := &fakeRepo{enter: make(chan *telemetry.Message, 8), release: make(chan struct{})}
	pub := &fakePublisher{}
	opts := defaultOptions()
	opts.WorkerCount = 2
	opts.QueueCapacity = 8
	ing := newIngress(t, opts, newValidator(t), repo, pub)
	mustStart(t, ing)

	topic, payload, receivedAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 1)
	for n := 0; n < 6; n++ {
		if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionAccepted {
			t.Fatalf("Admit #%d = %v, want accepted", n, got)
		}
	}
	// Both workers are now blocked inside Persist.
	waitMessage(t, repo.enter)
	waitMessage(t, repo.enter)
	if got := repo.maxParallel(); got != 2 {
		t.Fatalf("max concurrent Persist = %d, want exactly 2", got)
	}

	close(repo.release)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain = %v", err)
	}
	if got := repo.count(); got != 6 {
		t.Fatalf("Persist calls = %d, want 6", got)
	}
}

func TestAdmissionCopiesPayloadAndTopic(t *testing.T) {
	validator := &fakeValidator{}
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	ing := newIngress(t, defaultOptions(), validator, repo, pub)
	mustStart(t, ing)

	topic, payload, receivedAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 3)
	original := append([]byte(nil), payload...)
	if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionAccepted {
		t.Fatalf("Admit = %v, want accepted", got)
	}
	// The caller may reuse its buffer immediately after Admit returns.
	for idx := range payload {
		payload[idx] = 'X'
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain = %v", err)
	}

	if validator.gotTopic != topic {
		t.Fatalf("worker saw topic %q, want %q", validator.gotTopic, topic)
	}
	if string(validator.gotPayload) != string(original) {
		t.Fatalf("worker payload was not a private copy: got %q, want original", validator.gotPayload)
	}
	if !validator.gotReceivedAt.Equal(receivedAt) {
		t.Fatalf("worker receivedAt = %v, want %v", validator.gotReceivedAt, receivedAt)
	}
}

func TestAdmissionRejectionsDoNotPersist(t *testing.T) {
	repo := &fakeRepo{enter: make(chan *telemetry.Message, 4), release: make(chan struct{})}
	pub := &fakePublisher{}
	topic, payload, receivedAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 1)

	opts := defaultOptions()
	opts.QueueCapacity = 1
	opts.WorkerCount = 1
	opts.MaxPayloadBytes = len(payload)
	ing := newIngress(t, opts, newValidator(t), repo, pub)
	mustStart(t, ing)

	if got := ing.Admit(topic, payload, receivedAt, true); got != AdmissionRejectedRetained {
		t.Fatalf("retained Admit = %v, want rejected", got)
	}
	oversized := append(append([]byte(nil), payload...), ' ')
	if got := ing.Admit(topic, oversized, receivedAt, false); got != AdmissionRejectedOversized {
		t.Fatalf("oversized Admit = %v, want rejected", got)
	}
	if got := ing.Admit("gateways/gateway_001/telemetry", payload, receivedAt, false); got != AdmissionRejectedInvalidTopic {
		t.Fatalf("bad-topic Admit = %v, want rejected", got)
	}

	// Occupy the single worker, then fill and overflow the one-slot queue.
	if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionAccepted {
		t.Fatalf("first Admit = %v, want accepted", got)
	}
	waitMessage(t, repo.enter)
	if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionAccepted {
		t.Fatalf("second Admit = %v, want accepted", got)
	}
	if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionRejectedQueueFull {
		t.Fatalf("third Admit = %v, want queue full", got)
	}

	close(repo.release)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatalf("Drain = %v", err)
	}

	if got := repo.count(); got != 2 {
		t.Fatalf("Persist calls = %d, want only the 2 accepted items", got)
	}
	if got := len(pub.deliveries()); got != 2 {
		t.Fatalf("ACKs = %d, want 2", got)
	}
	snap := ing.Snapshot()
	if snap.RejectedRetained != 1 || snap.RejectedOversized != 1 || snap.RejectedInvalidTopic != 1 || snap.QueueFull != 1 {
		t.Fatalf("rejection snapshot = %+v", snap)
	}
}

func TestAdmitBeforeStartRejected(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	ing := newIngress(t, defaultOptions(), newValidator(t), repo, pub)

	topic, payload, receivedAt := testPayloadFor(t, "gateway_001", "sensor_001", testMessageID, testBootID, 1)
	if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionRejectedNotRunning {
		t.Fatalf("Admit before Start = %v, want not running", got)
	}
	if repo.count() != 0 || len(pub.deliveries()) != 0 {
		t.Fatalf("persist/publish happened before Start")
	}
	if got := ing.Snapshot().RejectedNotRunning; got != 1 {
		t.Fatalf("RejectedNotRunning = %d, want 1", got)
	}
}

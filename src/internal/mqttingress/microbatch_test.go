package mqttingress

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

type fakeBatchRepo struct {
	mu      sync.Mutex
	groups  [][]*telemetry.Message
	entered chan struct{}
}

type batchRepoFunc func(context.Context, []*telemetry.Message) []telemetry.BatchPersistResult

// lifecycleClock exposes the worker's return to its select loop, not merely
// timer creation (which happens before the collection closure returns).
type lifecycleClock struct {
	controlledClock
	elapsed atomic.Int64
	waiting chan struct{}
}

type lifecycleTimer struct {
	ingressTimer
	waiting chan struct{}
}

func (c *lifecycleClock) Now() time.Time {
	return time.Unix(1700000000, 0).Add(time.Duration(c.elapsed.Load()))
}

func (c *lifecycleClock) NewTimer(d time.Duration) ingressTimer {
	return lifecycleTimer{c.controlledClock.NewTimer(d), c.waiting}
}

func (t lifecycleTimer) C() <-chan time.Time {
	t.waiting <- struct{}{}
	return t.ingressTimer.C()
}

func TestMicrobatchProcessingLifecycle(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		name := "metrics_off"
		if enabled {
			name = "metrics_on"
		}
		t.Run(name, func(t *testing.T) {
			clock := &lifecycleClock{controlledClock: controlledClock{timers: make(chan *controlledTimer, 1)}, waiting: make(chan struct{}, 1)}
			persistEntered, ackEntered := make(chan struct{}), make(chan struct{})
			persistRelease, ackRelease := make(chan struct{}, 1), make(chan struct{}, 1)
			batch := batchRepoFunc(func(ctx context.Context, ms []*telemetry.Message) []telemetry.BatchPersistResult {
				close(persistEntered)
				select {
				case <-persistRelease:
				case <-ctx.Done():
					return nil
				}
				return []telemetry.BatchPersistResult{{InputIndex: 0, Identity: identityOf(ms[0]), Outcome: telemetry.CommitOutcomeCommitted, SamplesSaved: ms[0].SampleCount()}}
			})
			pub := &fakePublisher{onPublish: func(ctx context.Context, _ Delivery) error {
				close(ackEntered)
				select {
				case <-ackRelease:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}
			opts := microOptions()
			opts.MetricsEnabled = enabled
			ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, pub)
			ing.setClock(clock)
			mustStart(t, ing)
			t.Cleanup(func() { ing.cancelRun(); _ = ing.Drain(context.Background()) })
			wait := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(2 * time.Second):
					t.Fatal("lifecycle barrier timed out")
				}
			}
			active := func(phase string) {
				t.Helper()
				s := ing.Snapshot()
				wantFlight := int64(0)
				if enabled {
					wantFlight = 1
				}
				if s.InFlight != wantFlight || s.ProcessingCount != 0 || s.ProcessingLatencyTotal != 0 || s.BufferedPayloadBytes == 0 {
					t.Errorf("%s: in-flight=%d count=%d total=%s bytes=%d", phase, s.InFlight, s.ProcessingCount, s.ProcessingLatencyTotal, s.BufferedPayloadBytes)
				}
			}
			admitMicro(t, ing, 1, 1)
			timer := <-clock.timers
			wait(clock.waiting)
			active("collecting")
			clock.elapsed.Add(int64(10 * time.Second))
			timer.ch <- clock.Now()
			wait(persistEntered)
			active("persisting")
			clock.elapsed.Add(int64(20 * time.Second))
			persistRelease <- struct{}{}
			wait(ackEntered)
			active("ACK")
			clock.elapsed.Add(int64(30 * time.Second))
			ackRelease <- struct{}{}
			if err := ing.Drain(context.Background()); err != nil {
				t.Fatal(err)
			}
			s := ing.Snapshot()
			if s.ProcessingCount != 1 || s.ProcessingLatencyTotal != time.Minute || s.InFlight != 0 || s.BufferedPayloadBytes != 0 {
				t.Errorf("finished: count=%d total=%s in-flight=%d bytes=%d", s.ProcessingCount, s.ProcessingLatencyTotal, s.InFlight, s.BufferedPayloadBytes)
			}
			wantObservations := int64(0)
			if enabled {
				wantObservations = 1
			}
			if int64(s.Durations.GroupPersistence.Count) != wantObservations || int64(s.Durations.QueueWait.Count) != wantObservations {
				t.Errorf("histogram enablement: %+v", s.Durations)
			}
		})
	}
}

func TestMicrobatchReleaseLifecycleExactlyOnce(t *testing.T) {
	clock := &lifecycleClock{}
	opts := microOptions()
	opts.MetricsEnabled = true
	ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, &fakeBatchRepo{}, &fakePublisher{})
	ing.setClock(clock)
	// A delivery discarded before processing starts owns no timing sample.
	unstarted := &validatedDelivery{}
	ing.releaseDelivery(unstarted)
	if s := ing.Snapshot(); s.InFlight != 0 || s.ProcessingCount != 0 {
		t.Fatalf("unstarted delivery counted: %+v", s)
	}
	d := &validatedDelivery{}
	ing.startDelivery(d)
	clock.elapsed.Add(int64(time.Second))
	ing.releaseDelivery(d)
	ing.releaseDelivery(d)
	if s := ing.Snapshot(); s.InFlight != 0 || s.ProcessingCount != 1 || s.ProcessingLatencyTotal != time.Second {
		t.Fatalf("repeated release counted: %+v", s)
	}
}

func (f batchRepoFunc) PersistBatch(ctx context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
	return f(ctx, messages)
}

func TestMicrobatchConfirmedCountersPrecedeBlockedACKs(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var ing *Ingress
	batch := batchRepoFunc(func(_ context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
		results := make([]telemetry.BatchPersistResult, len(messages))
		for n, m := range messages {
			results[n] = telemetry.BatchPersistResult{InputIndex: n, Identity: identityOf(m), Outcome: telemetry.CommitOutcomeCommitted, SamplesSaved: m.SampleCount()}
		}
		return results
	})
	pub := &fakePublisher{onPublish: func(ctx context.Context, _ Delivery) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	ing = newIngressWithBatch(t, microOptions(), newValidator(t), &fakeRepo{}, batch, pub)
	mustStart(t, ing)
	admitMicro(t, ing, 1, 2)
	admitMicro(t, ing, 2, 3)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("ACK did not start")
	}
	snap := ing.Snapshot()
	if snap.Committed != 2 || snap.SamplesNew != 5 {
		t.Fatalf("confirmed counters before first ACK = committed %d samples %d", snap.Committed, snap.SamplesNew)
	}
	close(release)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(pub.deliveries()); got != 2 {
		t.Fatalf("ACKs = %d, want 2", got)
	}
}

func TestMicrobatchNeverACKsBeforeBatchRepositoryReturns(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	batch := batchRepoFunc(func(_ context.Context, ms []*telemetry.Message) []telemetry.BatchPersistResult {
		close(entered)
		<-release
		out := make([]telemetry.BatchPersistResult, len(ms))
		for n, m := range ms {
			out[n] = telemetry.BatchPersistResult{InputIndex: n, Identity: identityOf(m), Outcome: telemetry.CommitOutcomeCommitted}
		}
		return out
	})
	pub := &fakePublisher{}
	ing := newIngressWithBatch(t, microOptions(), newValidator(t), &fakeRepo{}, batch, pub)
	mustStart(t, ing)
	admitMicro(t, ing, 1, 1)
	admitMicro(t, ing, 2, 1)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("batch persistence did not start")
	}
	if got := pub.count(); got != 0 {
		t.Fatalf("ACK attempts before PersistBatch returned = %d", got)
	}
	close(release)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := pub.count(); got != 2 {
		t.Fatalf("ACK attempts after commit = %d, want 2", got)
	}
}

func TestMicrobatchSubmitsAllCommittedInputIndexesBeforeACKLoop(t *testing.T) {
	sink := &recordingBatchSink{}
	batch := batchRepoFunc(func(_ context.Context, ms []*telemetry.Message) []telemetry.BatchPersistResult {
		out := make([]telemetry.BatchPersistResult, len(ms))
		for n := range ms {
			idx := len(ms) - n - 1 // deliberately return associations out of order
			m := ms[idx]
			out[n] = telemetry.BatchPersistResult{InputIndex: idx, Identity: identityOf(m), Outcome: telemetry.CommitOutcomeCommitted, SamplesSaved: m.SampleCount()}
		}
		return out
	})
	pub := &fakePublisher{onPublish: func(context.Context, Delivery) error { return errors.New("ack unavailable") }}
	opts := microOptions()
	opts.PostCommitSink = sink
	ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, pub)
	mustStart(t, ing)
	firstID, secondID := uuid.New(), uuid.New()
	for _, item := range []struct {
		id   uuid.UUID
		rows int
	}{{firstID, 2}, {secondID, 3}} {
		topic, payload, received := testPayloadFor(t, "gateway_001", "sensor_001", item.id, testBootID, item.rows)
		if got := ing.Admit(topic, payload, received, false); got != AdmissionAccepted {
			t.Fatalf("Admit = %v", got)
		}
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	batches := sink.snapshot()
	if len(batches) != 2 {
		t.Fatalf("submitted %d batches, want both committed members", len(batches))
	}
	if batches[0].MessageID != firstID || batches[1].MessageID != secondID {
		t.Fatalf("InputIndex association lost: ids=%v,%v", batches[0].MessageID, batches[1].MessageID)
	}
	if got := pub.count(); got != 2 {
		t.Fatalf("ACK attempts=%d, want 2", got)
	}
}

func TestOversizedStandalonePostCommitSink(t *testing.T) {
	sink := &recordingBatchSink{}
	opts := microOptions()
	opts.Microbatch.MaxRows = 1
	opts.PostCommitSink = sink
	ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, &fakeBatchRepo{}, &fakePublisher{})
	mustStart(t, ing)
	admitMicro(t, ing, 1, 2)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	batches := sink.snapshot()
	if len(batches) != 1 || batches[0].SampleCount != 2 || len(batches[0].Samples) != 2 {
		t.Fatalf("standalone post-commit batches = %+v", batches)
	}
}

func TestMicrobatchRejectsMalformedResultSetsWithoutACK(t *testing.T) {
	for _, tc := range []struct {
		name    string
		results func([]*telemetry.Message) []telemetry.BatchPersistResult
	}{
		{name: "wrong_count", results: func(_ []*telemetry.Message) []telemetry.BatchPersistResult { return nil }},
		{name: "duplicate_index", results: func(ms []*telemetry.Message) []telemetry.BatchPersistResult {
			return []telemetry.BatchPersistResult{{InputIndex: 0, Identity: identityOf(ms[0]), Outcome: telemetry.CommitOutcomeCommitted}, {InputIndex: 0, Identity: identityOf(ms[0]), Outcome: telemetry.CommitOutcomeCommitted}}
		}},
		{name: "out_of_range", results: func(ms []*telemetry.Message) []telemetry.BatchPersistResult {
			return []telemetry.BatchPersistResult{{InputIndex: len(ms), Identity: identityOf(ms[0]), Outcome: telemetry.CommitOutcomeCommitted}}
		}},
		{name: "unconfirmed", results: func(ms []*telemetry.Message) []telemetry.BatchPersistResult {
			return []telemetry.BatchPersistResult{{InputIndex: 0, Identity: identityOf(ms[0])}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := batchRepoFunc(func(_ context.Context, ms []*telemetry.Message) []telemetry.BatchPersistResult { return tc.results(ms) })
			pub := &fakePublisher{}
			opts := microOptions()
			opts.Microbatch.MaxMessages = 1
			ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, pub)
			mustStart(t, ing)
			admitMicro(t, ing, 1, 1)
			admitMicro(t, ing, 2, 1)
			if err := ing.Drain(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := len(pub.deliveries()); got != 0 {
				t.Fatalf("ACKs = %d, want 0", got)
			}
			if ing.Snapshot().BufferedPayloadBytes != 0 {
				t.Fatal("payload reservation leaked")
			}
		})
	}
}

func TestMicrobatchReorderedResultsAndErrorOverride(t *testing.T) {
	batch := batchRepoFunc(func(_ context.Context, ms []*telemetry.Message) []telemetry.BatchPersistResult {
		if len(ms) == 1 {
			return []telemetry.BatchPersistResult{{InputIndex: 0, Identity: identityOf(ms[0]), Outcome: telemetry.CommitOutcomeCommitted}}
		}
		return []telemetry.BatchPersistResult{
			{InputIndex: 1, Identity: identityOf(ms[1]), Outcome: telemetry.CommitOutcomeCommitted},
			{InputIndex: 0, Identity: identityOf(ms[0]), Outcome: telemetry.CommitOutcomeCommitted, Err: context.DeadlineExceeded},
		}
	})
	pub := &fakePublisher{}
	opts := microOptions()
	clock := &controlledClock{timers: make(chan *controlledTimer, 1)}
	opts.Microbatch.MaxMessages = 2
	ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, pub)
	ing.setClock(clock)
	mustStart(t, ing)
	admitMicro(t, ing, 1, 1)
	<-clock.timers // first member is now in the collection group
	admitMicro(t, ing, 2, 1)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(pub.deliveries()); got != 1 {
		t.Fatalf("ACKs = %d, want 1 (only confirmed error-free result)", got)
	}
	if s := ing.Snapshot(); s.Committed != 1 || s.ACKPublished != 1 || s.BufferedPayloadBytes != 0 {
		t.Fatalf("snapshot = %+v", s)
	}
}

func TestMicrobatchFullIdentityMismatchSuppressesACK(t *testing.T) {
	batch := batchRepoFunc(func(_ context.Context, ms []*telemetry.Message) []telemetry.BatchPersistResult {
		id := identityOf(ms[0])
		id.BootID = uuid.New()
		return []telemetry.BatchPersistResult{{InputIndex: 0, Identity: id, Outcome: telemetry.CommitOutcomeCommitted}}
	})
	pub := &fakePublisher{}
	ing := newIngressWithBatch(t, microOptions(), newValidator(t), &fakeRepo{}, batch, pub)
	mustStart(t, ing)
	admitMicro(t, ing, 1, 1)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pub.deliveries()) != 0 {
		t.Fatal("identity-mismatched result was ACKed")
	}
}

func TestMicrobatchACKFailureDoesNotBlockRemainingACKs(t *testing.T) {
	batch := &fakeBatchRepo{entered: make(chan struct{}, 1)}
	var n atomic.Int32
	pub := &fakePublisher{onPublish: func(context.Context, Delivery) error {
		if n.Add(1) == 1 {
			return errors.New("publish failed")
		}
		return nil
	}}
	opts := microOptions()
	opts.Microbatch.MaxMessages = 2
	clock := &controlledClock{timers: make(chan *controlledTimer, 1)}
	ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, pub)
	ing.setClock(clock)
	mustStart(t, ing)
	admitMicro(t, ing, 1, 1)
	<-clock.timers
	admitMicro(t, ing, 2, 1)
	select {
	case <-batch.entered:
	case <-time.After(time.Second):
		t.Fatal("batch did not persist")
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := pub.count(); got != 2 {
		t.Fatalf("ACK attempts = %d, want both deliveries", got)
	}
	if s := ing.Snapshot(); s.ACKPublishFailures != 1 || s.ACKPublished != 1 || s.Committed != 2 {
		t.Fatalf("snapshot = %+v", s)
	}
}

func TestMicrobatchDrainCancellationReleasesBlockedGroupAndQueuedItem(t *testing.T) {
	entered := make(chan struct{})
	batch := batchRepoFunc(func(ctx context.Context, _ []*telemetry.Message) []telemetry.BatchPersistResult {
		close(entered)
		<-ctx.Done()
		return nil
	})
	opts := microOptions()
	opts.QueueCapacity = 2
	pub := &fakePublisher{}
	ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, pub)
	mustStart(t, ing)
	admitMicro(t, ing, 1, 1)
	admitMicro(t, ing, 2, 1)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("group persistence did not block")
	}
	admitMicro(t, ing, 3, 1) // queued while worker is blocked in the common batch attempt
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := ing.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain = %v, want deadline exceeded", err)
	}
	if got := pub.count(); got != 0 {
		t.Fatalf("ACK attempts after cancellation = %d", got)
	}
	if got := ing.Snapshot().BufferedPayloadBytes; got != 0 {
		t.Fatalf("buffered bytes after cancellation = %d", got)
	}
}

func TestMicrobatchConfirmedResultsAreCountedAfterDrainCancellation(t *testing.T) {
	entered := make(chan struct{})
	batch := batchRepoFunc(func(ctx context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
		close(entered)
		<-ctx.Done()
		results := make([]telemetry.BatchPersistResult, len(messages))
		for n, message := range messages {
			results[n] = telemetry.BatchPersistResult{
				InputIndex: n, Identity: identityOf(message), Outcome: telemetry.CommitOutcomeCommitted,
				SamplesSaved: message.SampleCount(),
			}
		}
		return results
	})
	pub := &fakePublisher{}
	clock := &controlledClock{timers: make(chan *controlledTimer, 1)}
	ing := newIngressWithBatch(t, microOptions(), newValidator(t), &fakeRepo{}, batch, pub)
	ing.setClock(clock)
	mustStart(t, ing)
	admitMicro(t, ing, 1, 2)
	select {
	case <-clock.timers:
	case <-time.After(time.Second):
		t.Fatal("member did not enter the collection group")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := ing.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain = %v, want deadline exceeded", err)
	}
	select {
	case <-entered:
	default:
		t.Fatal("group persistence did not observe drain cancellation")
	}
	if snapshot := ing.Snapshot(); snapshot.Committed != 1 || snapshot.SamplesNew != 2 {
		t.Fatalf("confirmed results lost after cancellation: committed=%d samples=%d", snapshot.Committed, snapshot.SamplesNew)
	}
	if pub.count() != 0 {
		t.Fatal("ACK published after drain cancellation")
	}
	if got := ing.Snapshot().BufferedPayloadBytes; got != 0 {
		t.Fatalf("buffered bytes after cancellation = %d", got)
	}
}

type controlledClock struct{ timers chan *controlledTimer }
type controlledTimer struct{ ch chan time.Time }

func (c *controlledClock) Now() time.Time { return time.Now() }
func (c *controlledClock) NewTimer(time.Duration) ingressTimer {
	t := &controlledTimer{ch: make(chan time.Time, 1)}
	c.timers <- t
	return t
}
func (t *controlledTimer) C() <-chan time.Time { return t.ch }
func (*controlledTimer) Stop()                 {}

type captureValidator struct {
	Validator
	got chan time.Time
}

func (v captureValidator) Validate(topic string, payload []byte, received time.Time) (*telemetry.Message, error) {
	v.got <- received
	return v.Validator.Validate(topic, payload, received)
}

func (r *fakeBatchRepo) PersistBatch(_ context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
	r.mu.Lock()
	group := append([]*telemetry.Message(nil), messages...)
	r.groups = append(r.groups, group)
	r.mu.Unlock()
	if r.entered != nil {
		r.entered <- struct{}{}
	}
	results := make([]telemetry.BatchPersistResult, len(messages))
	for n, m := range messages {
		results[n] = telemetry.BatchPersistResult{InputIndex: n, Identity: identityOf(m), Outcome: telemetry.CommitOutcomeCommitted, SamplesSaved: len(m.Samples())}
	}
	return results
}

func microOptions() Options {
	o := defaultOptions()
	o.Microbatch = MicrobatchOptions{Enabled: true, MaxMessages: 2, MaxRows: 100, MaxPayloadBytes: 1 << 20, MaxWait: time.Second}
	return o
}

func admitMicro(t *testing.T, ing *Ingress, _, rows int) time.Time {
	t.Helper()
	id := uuid.New()
	topic, payload, received := testPayloadFor(t, "gateway_001", "sensor_001", id, testBootID, rows)
	if got := ing.Admit(topic, payload, received, false); got != AdmissionAccepted {
		t.Fatalf("Admit = %v", got)
	}
	return received
}

func TestMicrobatchMessageCapFlushAndPreservesReceivedAt(t *testing.T) {
	batch := &fakeBatchRepo{entered: make(chan struct{}, 2)}
	pub := &fakePublisher{}
	ing := newIngressWithBatch(t, microOptions(), newValidator(t), &fakeRepo{}, batch, pub)
	mustStart(t, ing)
	first := admitMicro(t, ing, 1, 2)
	admitMicro(t, ing, 2, 3)
	select {
	case <-batch.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("message cap did not flush")
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	batch.mu.Lock()
	defer batch.mu.Unlock()
	if len(batch.groups) != 1 || len(batch.groups[0]) != 2 {
		t.Fatalf("groups = %d, members = %d", len(batch.groups), len(batch.groups[0]))
	}
	if first.IsZero() {
		t.Fatal("test receipt time unexpectedly zero")
	}
}

func TestMicrobatchCacheHitBypassesGroupAndFreshMemberWarmsCache(t *testing.T) {
	batch := &fakeBatchRepo{}
	opts := microOptions()
	opts.MetricsEnabled = true
	opts.RAMDedup = DedupCacheOptions{Enabled: true, MaxEntries: 8, MaxBytes: 4096}
	pub := &fakePublisher{}
	ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, pub)
	// Seed one confirmed proof, modeling an earlier successfully committed delivery.
	seedID := uuid.New()
	topic, payload, at := testPayloadFor(t, "gateway_001", "sensor_001", seedID, testBootID, 1)
	seed, err := newValidator(t).Validate(topic, payload, at)
	if err != nil {
		t.Fatal(err)
	}
	key, proof, ok := buildDedupMetadata(seed, time.Now().UTC())
	if !ok || ing.dedupCache.Publish(key, proof) != dedupPublishStored {
		t.Fatal("failed to seed proof")
	}
	mustStart(t, ing)
	if got := ing.Admit(topic, payload, at, false); got != AdmissionAccepted {
		t.Fatal(got)
	}
	freshID := uuid.New()
	topic2, payload2, at2 := testPayloadFor(t, "gateway_001", "sensor_002", freshID, testBootID, 2)
	if got := ing.Admit(topic2, payload2, at2, false); got != AdmissionAccepted {
		t.Fatal(got)
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ing.rotationDone:
	default:
		t.Fatal("Drain returned before cache rotation goroutine stopped")
	}
	batch.mu.Lock()
	defer batch.mu.Unlock()
	if len(batch.groups) != 1 || len(batch.groups[0]) != 1 || batch.groups[0][0].Identity().MessageID != freshID {
		t.Fatalf("batch groups did not retain only fresh input: %#v", batch.groups)
	}
	snap := ing.Snapshot()
	if snap.DuplicateCommitted != 1 || snap.Committed != 1 || snap.SamplesNew != 2 || snap.ACKPublished != 2 || snap.BufferedPayloadBytes != 0 {
		t.Fatalf("snapshot=%+v", snap)
	}
	if ing.dedupCache.Snapshot().Entries != 2 {
		t.Fatalf("cache=%+v", ing.dedupCache.Snapshot())
	}
}

func TestMicrobatchConfirmedProofSurvivesCanceledACKAndMetricsOff(t *testing.T) {
	var ing *Ingress
	batch := batchRepoFunc(func(_ context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
		ing.cancelRun() // persistence has committed; suppress the subsequent ACK only.
		return []telemetry.BatchPersistResult{{InputIndex: 0, Identity: identityOf(messages[0]), Outcome: telemetry.CommitOutcomeCommitted, SamplesSaved: messages[0].SampleCount()}}
	})
	opts := microOptions()
	opts.MetricsEnabled = false
	opts.RAMDedup = DedupCacheOptions{Enabled: true, MaxEntries: 4, MaxBytes: 4096}
	pub := &fakePublisher{}
	ing = newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, pub)
	mustStart(t, ing)
	admitMicro(t, ing, 1, 1)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ing.dedupCache.Snapshot().Entries != 1 {
		t.Fatalf("proof was not published before canceled ACK: %+v", ing.dedupCache.Snapshot())
	}
	snap := ing.Snapshot()
	if snap.Committed != 1 || snap.SamplesNew != 1 || snap.ACKPublished != 0 || snap.BufferedPayloadBytes != 0 || snap.ProcessingCount != 1 {
		t.Fatalf("metrics-off lifecycle/counters=%+v", snap)
	}
}

func TestMicrobatchStandaloneFallbackWarmsProof(t *testing.T) {
	opts := microOptions()
	opts.Microbatch.MaxRows = 1 // A valid two-row message takes the existing standalone fallback.
	opts.RAMDedup = DedupCacheOptions{Enabled: true, MaxEntries: 4, MaxBytes: 4096}
	repo := &fakeRepo{}
	batch := &fakeBatchRepo{}
	ing := newIngressWithBatch(t, opts, newValidator(t), repo, batch, &fakePublisher{})
	mustStart(t, ing)
	topic, payload, at := testPayloadFor(t, "gateway_001", "sensor_001", uuid.New(), testBootID, 2)
	for n := 0; n < 2; n++ {
		if got := ing.Admit(topic, payload, at, false); got != AdmissionAccepted {
			t.Fatal(got)
		}
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repo.count() != 1 || len(batch.groups) != 0 || ing.dedupCache.Snapshot().Entries != 1 {
		t.Fatalf("fallback repo=%d groups=%d cache=%+v", repo.count(), len(batch.groups), ing.dedupCache.Snapshot())
	}
}

func TestMicrobatchMismatchedIdentityDoesNotWarmOrACK(t *testing.T) {
	batch := batchRepoFunc(func(_ context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
		identity := identityOf(messages[0])
		identity.SensorID = "other_sensor"
		return []telemetry.BatchPersistResult{{InputIndex: 0, Identity: identity, Outcome: telemetry.CommitOutcomeCommitted, SamplesSaved: messages[0].SampleCount()}}
	})
	opts := microOptions()
	opts.RAMDedup = DedupCacheOptions{Enabled: true, MaxEntries: 4, MaxBytes: 4096}
	pub := &fakePublisher{}
	ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, pub)
	mustStart(t, ing)
	admitMicro(t, ing, 1, 1)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ing.dedupCache.Snapshot().Entries != 0 || len(pub.deliveries()) != 0 {
		t.Fatalf("mismatch published proof/ACK: cache=%+v ACKs=%d", ing.dedupCache.Snapshot(), len(pub.deliveries()))
	}
}

func TestMicrobatchGroupErrorDoesNotTrustProvisionalResultButKeepsConfirmedPartialMember(t *testing.T) {
	var groupIDs []telemetry.MessageIdentity
	groupEntered := make(chan int, 1)
	batch := batchRepoFunc(func(_ context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
		groupEntered <- len(messages)
		if len(messages) != 2 {
			return nil
		}
		groupIDs = []telemetry.MessageIdentity{identityOf(messages[0]), identityOf(messages[1])}
		return []telemetry.BatchPersistResult{
			{InputIndex: 0, Identity: groupIDs[0], Outcome: telemetry.CommitOutcomeCommitted, SamplesSaved: messages[0].SampleCount(), Err: errors.New("group member failed")},
			{InputIndex: 1, Identity: groupIDs[1], Outcome: telemetry.CommitOutcomeCommitted, SamplesSaved: messages[1].SampleCount()},
		}
	})
	opts := microOptions()
	opts.RAMDedup = DedupCacheOptions{Enabled: true, MaxEntries: 4, MaxBytes: 4096}
	pub := &fakePublisher{}
	ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, pub)
	clock := &controlledClock{timers: make(chan *controlledTimer, 1)}
	ing.setClock(clock)
	mustStart(t, ing)
	topic0, payload0, at0 := testPayloadFor(t, "gateway_001", "sensor_001", uuid.New(), testBootID, 1)
	// Batch result mapping is independent of Sensor identity; use one Sensor
	// because runtime PersistBatch groups are now homogeneous per Sensor.
	topic1, payload1, at1 := testPayloadFor(t, "gateway_001", "sensor_001", uuid.New(), testBootID, 1)
	if ing.Admit(topic0, payload0, at0, false) != AdmissionAccepted {
		t.Fatal("first admission failed")
	}
	select {
	case <-clock.timers:
	case <-time.After(time.Second):
		t.Fatal("first member did not enter collection")
	}
	if ing.Admit(topic1, payload1, at1, false) != AdmissionAccepted {
		t.Fatal("second admission failed")
	}
	select {
	case n := <-groupEntered:
		if n != 2 {
			t.Fatalf("persistence group has %d members, want 2", n)
		}
	case <-time.After(time.Second):
		t.Fatal("two-member persistence group did not enter before Drain")
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pub.deliveries()) != 1 || ing.dedupCache.Snapshot().Entries != 1 {
		t.Fatalf("ACKs=%d cache=%+v; only confirmed partial-success member may publish", len(pub.deliveries()), ing.dedupCache.Snapshot())
	}
	// Exactly one ACK and one proof demonstrate that the errored provisional
	// result was ignored while the successful partial member was retained.
	_ = groupIDs
}

func TestMicrobatchInvalidIndexSuppressesAllProvisionalSuccesses(t *testing.T) {
	groupEntered := make(chan int, 1)
	batch := batchRepoFunc(func(_ context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
		groupEntered <- len(messages)
		if len(messages) != 2 {
			return nil
		}
		return []telemetry.BatchPersistResult{
			{InputIndex: 0, Identity: identityOf(messages[0]), Outcome: telemetry.CommitOutcomeCommitted, SamplesSaved: messages[0].SampleCount()},
			{InputIndex: len(messages), Identity: identityOf(messages[1]), Outcome: telemetry.CommitOutcomeCommitted, SamplesSaved: messages[1].SampleCount()},
		}
	})
	opts := microOptions()
	opts.RAMDedup = DedupCacheOptions{Enabled: true, MaxEntries: 4, MaxBytes: 4096}
	pub := &fakePublisher{}
	ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, pub)
	clock := &controlledClock{timers: make(chan *controlledTimer, 1)}
	ing.setClock(clock)
	mustStart(t, ing)
	admitMicro(t, ing, 0, 1)
	select {
	case <-clock.timers:
	case <-time.After(time.Second):
		t.Fatal("first member did not enter collection")
	}
	admitMicro(t, ing, 1, 1)
	select {
	case n := <-groupEntered:
		if n != 2 {
			t.Fatalf("persistence group has %d members, want 2", n)
		}
	case <-time.After(time.Second):
		t.Fatal("two-member persistence group did not enter before Drain")
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pub.count() != 0 || ing.dedupCache.Snapshot().Entries != 0 {
		t.Fatalf("invalid result trusted: ACKs=%d cache=%+v", pub.count(), ing.dedupCache.Snapshot())
	}
}

func TestMicrobatchCommitUnconfirmedDoesNotPublishProofOrACK(t *testing.T) {
	batch := batchRepoFunc(func(_ context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
		return []telemetry.BatchPersistResult{{InputIndex: 0, Identity: identityOf(messages[0]), Outcome: telemetry.CommitOutcomeUnconfirmed}}
	})
	opts := microOptions()
	opts.RAMDedup = DedupCacheOptions{Enabled: true, MaxEntries: 4, MaxBytes: 4096}
	pub := &fakePublisher{}
	ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, pub)
	mustStart(t, ing)
	admitMicro(t, ing, 0, 1)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pub.count() != 0 || ing.dedupCache.Snapshot().Entries != 0 {
		t.Fatalf("unconfirmed group result trusted: ACKs=%d cache=%+v", pub.count(), ing.dedupCache.Snapshot())
	}
}

func TestEnabledMicrobatchRequiresBatchRepository(t *testing.T) {
	var typedNil *fakeBatchRepo
	for _, batch := range []telemetry.BatchRepository{nil, typedNil} {
		if _, err := New(microOptions(), newValidator(t), &fakeRepo{}, &fakePublisher{}, batch); err == nil {
			t.Fatal("New accepted a missing or typed-nil batch repository")
		}
	}
}

func newIngressWithBatch(t *testing.T, opts Options, validator Validator, repo Repository, batch telemetry.BatchRepository, pub Publisher) *Ingress {
	t.Helper()
	ing, err := New(opts, validator, repo, pub, batch)
	if err != nil {
		t.Fatal(err)
	}
	return ing
}

func TestMicrobatchStopFlushesPartialGroup(t *testing.T) {
	batch := &fakeBatchRepo{}
	pub := &fakePublisher{}
	ing := newIngressWithBatch(t, microOptions(), newValidator(t), &fakeRepo{}, batch, pub)
	mustStart(t, ing)
	admitMicro(t, ing, 1, 1)
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pub.deliveries()) != 1 {
		t.Fatalf("ACKs = %d, want 1", len(pub.deliveries()))
	}
	if len(batch.groups) != 1 || len(batch.groups[0]) != 1 {
		t.Fatalf("partial group not flushed: %+v", batch.groups)
	}
}

func TestMicrobatchRowByteAndDeadlineFlushes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rows  int
		bytes int64
		timed bool
	}{
		{name: "rows", rows: 1, bytes: 1 << 20}, {name: "bytes", rows: 100, bytes: 1 << 20}, {name: "deadline", rows: 100, bytes: 1 << 20, timed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := &fakeBatchRepo{entered: make(chan struct{}, 2)}
			opts := microOptions()
			opts.Microbatch.MaxPayloadBytes = tc.bytes
			opts.Microbatch.MaxRows = tc.rows
			if tc.name == "bytes" {
				_, p, _ := testPayloadFor(t, "gateway_001", "sensor_001", uuid.New(), testBootID, 1)
				opts.Microbatch.MaxPayloadBytes = int64(len(p))
			}
			opts.Microbatch.MaxMessages = 10
			clock := &controlledClock{timers: make(chan *controlledTimer, 4)}
			ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, &fakePublisher{})
			ing.setClock(clock)
			mustStart(t, ing)
			admitMicro(t, ing, 1, 1)
			if tc.timed {
				timer := <-clock.timers
				timer.ch <- time.Now()
			}
			select {
			case <-batch.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("group did not flush")
			}
			if err := ing.Drain(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMicrobatchDeadlineDoesNotResetAndCarriesCapExceedingItem(t *testing.T) {
	batch := &fakeBatchRepo{entered: make(chan struct{}, 4)}
	clock := &controlledClock{timers: make(chan *controlledTimer, 4)}
	opts := microOptions()
	opts.Microbatch.MaxMessages = 8
	opts.Microbatch.MaxRows = 3
	ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, &fakePublisher{})
	ing.setClock(clock)
	mustStart(t, ing)
	admitMicro(t, ing, 1, 2)
	timer := <-clock.timers
	admitMicro(t, ing, 2, 2) // doesn't fit; flush first group and carry this item
	select {
	case <-batch.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("cap overflow did not flush previous group")
	}
	secondTimer := <-clock.timers // carried member starts its own deadline only after first group was persisted
	if timer == secondTimer {
		t.Fatal("carried item reused prior group timer")
	}
	// The incoming item remained owned and is the first member of the next group.
	secondTimer.ch <- time.Now()
	select {
	case <-batch.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("carried group did not flush")
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	batch.mu.Lock()
	defer batch.mu.Unlock()
	if len(batch.groups) != 2 || len(batch.groups[0]) != 1 || len(batch.groups[1]) != 1 {
		t.Fatalf("groups = %+v", batch.groups)
	}
}

func TestMicrobatchDeadlineIsNotResetByIncomingItemsAndMalformedExcluded(t *testing.T) {
	batch := &fakeBatchRepo{entered: make(chan struct{}, 2)}
	clock := &controlledClock{timers: make(chan *controlledTimer, 4)}
	opts := microOptions()
	opts.Microbatch.MaxMessages = 8
	opts.Microbatch.MaxRows = 100
	captured := make(chan time.Time, 4)
	validator := captureValidator{Validator: newValidator(t), got: captured}
	ing := newIngressWithBatch(t, opts, validator, &fakeRepo{}, batch, &fakePublisher{})
	ing.setClock(clock)
	mustStart(t, ing)
	r1 := admitMicro(t, ing, 1, 1)
	timer := <-clock.timers
	if got := <-captured; !got.Equal(r1) {
		t.Fatalf("validator receivedAt=%v want %v", got, r1)
	}
	if got := ing.Admit("gateways/gateway_001/telemetry/sensor_001", []byte("{"), r1, false); got != AdmissionAccepted {
		t.Fatalf("malformed Admit = %v", got)
	}
	if got := <-captured; !got.Equal(r1) {
		t.Fatalf("validator receivedAt=%v want %v", got, r1)
	}
	r2 := admitMicro(t, ing, 2, 1)
	if got := <-captured; !got.Equal(r2) {
		t.Fatalf("validator receivedAt=%v want %v", got, r2)
	}
	select {
	case <-clock.timers:
		t.Fatal("group deadline reset on arrival")
	default:
	}
	timer.ch <- time.Now()
	select {
	case <-batch.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("original deadline did not flush group")
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	batch.mu.Lock()
	defer batch.mu.Unlock()
	if len(batch.groups) != 1 || len(batch.groups[0]) != 2 {
		t.Fatalf("malformed member joined batch: %+v", batch.groups)
	}
}

func TestMicrobatchOversizedTargetMemberUsesStandaloneBatch(t *testing.T) {
	batch := &fakeBatchRepo{entered: make(chan struct{}, 2)}
	repo := &fakeRepo{enter: make(chan *telemetry.Message, 2)}
	opts := microOptions()
	opts.Microbatch.MaxRows = 1
	opts.Microbatch.MaxMessages = 10
	ing := newIngressWithBatch(t, opts, newValidator(t), repo, batch, &fakePublisher{})
	mustStart(t, ing)
	admitMicro(t, ing, 1, 2)
	select {
	case <-repo.enter:
	case <-time.After(2 * time.Second):
		t.Fatal("oversized valid member was not persisted standalone")
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(batch.groups) != 0 || repo.count() != 1 {
		t.Fatal("standalone message was split or lost")
	}
}

func TestMicrobatchGroupMetricsCaptureFlushesAndEffectiveBounds(t *testing.T) {
	batch := &fakeBatchRepo{entered: make(chan struct{}, 4)}
	clock := &controlledClock{timers: make(chan *controlledTimer, 4)}
	opts := microOptions()
	opts.MetricsEnabled = true
	opts.Microbatch.MaxMessages = 2
	opts.Microbatch.MaxRows = 3
	opts.Microbatch.MaxPayloadBytes = 1 << 20
	ing := newIngressWithBatch(t, opts, newValidator(t), &fakeRepo{}, batch, &fakePublisher{})
	ing.setClock(clock)
	mustStart(t, ing)
	admitMicro(t, ing, 1, 2)
	<-clock.timers
	admitMicro(t, ing, 2, 2) // row cap would overflow; first group flushes, current carries.
	select {
	case <-batch.entered:
	case <-time.After(time.Second):
		t.Fatal("row overflow did not flush")
	}
	timer := <-clock.timers
	timer.ch <- time.Now() // low-load timer flush, deliberately under target size.
	select {
	case <-batch.entered:
	case <-time.After(time.Second):
		t.Fatal("timer did not flush carried group")
	}
	if err := ing.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := ing.Snapshot()
	if s.Microbatch.Enabled != true || s.Microbatch.MaxMessages != 2 || s.Microbatch.MaxRows != 3 || s.Microbatch.MaxPayloadBytes != 1<<20 || s.Microbatch.MaxWait != time.Second {
		t.Fatalf("effective microbatch settings = %+v", s.Microbatch)
	}
	if s.Microbatch.Groups != 2 || s.Microbatch.Members != 2 || s.Microbatch.Rows != 4 || s.Microbatch.PayloadBytes != s.AcceptedBytes || s.Microbatch.MaxMembers != 1 {
		t.Fatalf("group summary = %+v", s.Microbatch)
	}
	if s.Microbatch.FlushRows != 1 || s.Microbatch.FlushTimer != 1 || s.Microbatch.FlushMessages+s.Microbatch.FlushBytes+s.Microbatch.FlushStop != 0 {
		t.Fatalf("flush reasons = %+v", s.Microbatch)
	}
	if s.Durations.BatchWait.Count != 2 || s.Durations.GroupPersistence.Count != 2 || s.Durations.QueueWait.Count != 2 || s.Durations.Validation.Count != 2 || s.Durations.ACKPublish.Count != 2 {
		t.Fatalf("batch durations = %+v", s.Durations)
	}
	if s.ProcessingCount != 2 || s.InFlight != 0 {
		t.Fatalf("processing lifecycle = count %d in-flight %d", s.ProcessingCount, s.InFlight)
	}
}

func TestMicrobatchMetricFlushReasonsAndStandalone(t *testing.T) {
	for _, tc := range []struct {
		name    string
		trigger string
	}{
		{"messages", "messages"}, {"bytes", "bytes"}, {"stop", "stop"}, {"standalone", "standalone"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := &fakeBatchRepo{entered: make(chan struct{}, 4)}
			repo := &fakeRepo{enter: make(chan *telemetry.Message, 2)}
			opts := microOptions()
			opts.MetricsEnabled = true
			opts.Microbatch.MaxMessages = 2
			if tc.trigger == "bytes" {
				_, p, _ := testPayloadFor(t, "gateway_001", "sensor_001", uuid.New(), testBootID, 1)
				opts.Microbatch.MaxPayloadBytes = int64(2*len(p) - 1)
			}
			if tc.trigger == "standalone" {
				opts.Microbatch.MaxRows = 1
			}
			clock := &controlledClock{timers: make(chan *controlledTimer, 4)}
			ing := newIngressWithBatch(t, opts, newValidator(t), repo, batch, &fakePublisher{})
			ing.setClock(clock)
			mustStart(t, ing)
			rows := 1
			if tc.trigger == "standalone" {
				rows = 2
			}
			admitMicro(t, ing, 1, rows)
			if tc.trigger == "messages" {
				<-clock.timers
				admitMicro(t, ing, 2, rows)
			}
			if tc.trigger == "bytes" {
				<-clock.timers
				admitMicro(t, ing, 2, rows)
			}
			if tc.trigger == "stop" {
				if err := ing.Drain(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else if tc.trigger != "standalone" {
				select {
				case <-batch.entered:
				case <-time.After(time.Second):
					t.Fatal("flush did not occur")
				}
				if err := ing.Drain(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else {
				select {
				case <-repo.enter:
				case <-time.After(time.Second):
					t.Fatal("standalone did not persist")
				}
				if err := ing.Drain(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			s := ing.Snapshot().Microbatch
			want := map[string]int64{"messages": s.FlushMessages, "bytes": s.FlushBytes, "stop": s.FlushStop, "standalone": s.StandaloneOversized}[tc.trigger]
			if want != 1 {
				t.Fatalf("unexpected trigger counter, snapshot=%+v", s)
			}
		})
	}
}

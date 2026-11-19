package paperload

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gatewaysim"
)

type fakeLoadTransport struct {
	mu        sync.Mutex
	readyErr  error
	publishes [][]byte
	stopped   bool
}

type blockingLoadTransport struct {
	entered chan struct{}
	stopped chan struct{}
	once    sync.Once
	active  atomic.Int64
}

func newBlockingLoadTransport() *blockingLoadTransport {
	return &blockingLoadTransport{entered: make(chan struct{}), stopped: make(chan struct{})}
}
func (f *blockingLoadTransport) Start() error                    { return nil }
func (f *blockingLoadTransport) WaitReady(context.Context) error { return nil }
func (f *blockingLoadTransport) Publish(ctx context.Context, _ string, _ []byte) error {
	f.active.Add(1)
	defer f.active.Add(-1)
	f.once.Do(func() { close(f.entered) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.stopped:
		return errors.New("stopped")
	}
}
func (f *blockingLoadTransport) StopReconnect() {}
func (f *blockingLoadTransport) Stop(context.Context) error {
	select {
	case <-f.stopped:
	default:
		close(f.stopped)
	}
	return nil
}

func (f *fakeLoadTransport) Start() error                    { return nil }
func (f *fakeLoadTransport) WaitReady(context.Context) error { return f.readyErr }
func (f *fakeLoadTransport) Publish(_ context.Context, _ string, p []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishes = append(f.publishes, append([]byte(nil), p...))
	return nil
}
func (f *fakeLoadTransport) StopReconnect()             { f.stopped = true }
func (f *fakeLoadTransport) Stop(context.Context) error { f.stopped = true; return nil }

func telemetryManifest(samples int) Manifest {
	m := validManifest()
	m.Experiment = "E2"
	m.Roles = []string{"telemetry"}
	m.Topology.Users = 0
	m.Topology.Viewers = 0
	m.Subscriptions = nil
	m.SamplesPerMessage = samples
	m.IdentityFiles = map[string]string{"telemetry": "identity.json"}
	m.GatewayIdentityFiles = map[string]string{"gw-a": "private/gw-a.json", "gw-b": "private/gw-b.json"}
	m.Traffic.BurstMessages = 0
	m.Limits.PendingMessages = 10
	m.Limits.PendingBytes = 1 << 20
	return m
}

func TestScheduledOpportunitiesIntegratesRampAndDoesNotInventCatchup(t *testing.T) {
	if got := ScheduledOpportunities(2*time.Second, 10, 4*time.Second); got != 5 {
		t.Fatalf("ramp opportunities=%d want 5", got)
	}
	if got := ScheduledOpportunities(6*time.Second, 10, 4*time.Second); got != 40 {
		t.Fatalf("steady-after-ramp opportunities=%d want 40", got)
	}
	first, skipped := BoundedOpportunityRange(10, 100000, 64)
	if first != 99936 || skipped != 99926 {
		t.Fatalf("bounded catchup first=%d skipped=%d", first, skipped)
	}
}

func TestPhaseObserverOrderBoundAndFailureIsPartial(t *testing.T) {
	m := telemetryManifest(1)
	m.Phases.WarmupSeconds, m.Phases.MeasureSeconds, m.Phases.DrainSeconds = 0, 0, 1
	m.Traffic.BurstMessages = 1
	transport := &fakeLoadTransport{}
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": transport, "gw-b": &fakeLoadTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"warmup_begin", "warmup_end", "measure_begin", "measure_end", "burst_begin", "burst_end", "generation_stopped", "drain_begin", "drain_end", "workers_joined"}
	var got []string
	e.phaseObserver = func(event PhaseEvent) error { got = append(got, event.Event); return nil }
	_, _ = e.Run(context.Background())
	if len(got) != len(want) {
		t.Fatalf("events=%v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events=%v", got)
		}
	}

	e2, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": &fakeLoadTransport{}, "gw-b": &fakeLoadTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	e2.phaseObserver = func(PhaseEvent) error { return errors.New("observer unavailable") }
	status, runErr := e2.Run(context.Background())
	if runErr != nil || status != "PARTIAL" {
		t.Fatalf("observer failure status=%s err=%v", status, runErr)
	}
}

func TestFiniteNoACKDrainKeepsPendingAndReturnsPartial(t *testing.T) {
	m := telemetryManifest(1)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	m.Phases = Phases{MeasureSeconds: 1, DrainSeconds: 1, CooldownSeconds: 1}
	m.Limits.WallSeconds = 4
	m.Traffic = Traffic{RatePerSecond: 1, BurstMessages: 1}
	m.Limits.Retries = 0
	f := &fakeLoadTransport{}
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": f})
	if err != nil {
		t.Fatal(err)
	}
	status, err := e.Run(context.Background())
	if err != nil || status != "PARTIAL" {
		t.Fatalf("status=%s err=%v", status, err)
	}
	c := e.Counters()
	if c.PendingMessages != 2 || c.PendingBytes == 0 || c.ApplicationACKs != 0 {
		t.Fatalf("pending erased or ACK fabricated: %+v", c)
	}
}

func retryTestEngine(t *testing.T, retries int) (*TelemetryEngine, *fakeLoadTransport) {
	t.Helper()
	m := telemetryManifest(1)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	m.Limits.Retries = retries
	m.RetryPolicy = &RetryPolicy{Mode: "bounded_backoff_v1", InitialIntervalMS: 100, MaxIntervalMS: 400, JitterPercent: 10, RatePerSecond: 2, Burst: 1}
	f := &fakeLoadTransport{}
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": f})
	if err != nil {
		t.Fatal(err)
	}
	return e, f
}

func TestBoundedRetryDelayDeterministicExponentialCapAndPositiveJitter(t *testing.T) {
	p := RetryPolicy{InitialIntervalMS: 100, MaxIntervalMS: 400, JitterPercent: 100}
	id := uuid.New()
	for n, want := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 400 * time.Millisecond} {
		a, b := retryDelay(p, id, n+1), retryDelay(p, id, n+1)
		if a != b || a <= 0 || a > 400*time.Millisecond {
			t.Fatalf("attempt %d delay=%s second=%s", n+1, a, b)
		}
		if n == 3 && want != 400*time.Millisecond {
			t.Fatal("bad cap test")
		}
	}
}

func TestRetryTokenEnvelopeBoundsActualDispatches(t *testing.T) {
	e, _ := retryTestEngine(t, 4)
	e.m.RetryPolicy.Burst = 2
	e.retryTokens = 2
	start := time.Now()
	first := e.takeRetryToken(start)
	second := e.takeRetryToken(start)
	if !first || !second {
		t.Fatal("configured retry burst was not available")
	}
	if e.takeRetryToken(start) {
		t.Fatal("retry dispatch exceeded burst without elapsed time")
	}
	if !e.takeRetryToken(start.Add(500 * time.Millisecond)) {
		t.Fatal("one token per half-second was not replenished")
	}
}

func TestPolicyRetryTokenWaitDoesNotCountAndRetryIsByteIdentical(t *testing.T) {
	e, f := retryTestEngine(t, 2)
	if err := e.generate(time.Now(), "measure"); err != nil {
		t.Fatal(err)
	}
	id := <-e.jobs
	if !e.processJob(context.Background(), id) {
		t.Fatal("initial publish")
	}
	original := append([]byte(nil), f.publishes[0]...)
	e.mu.Lock()
	e.pending[id].nextRetry = time.Now().Add(-time.Second)
	e.retryScanAt = time.Time{}
	e.retryTokens = 1
	e.mu.Unlock()
	e.retryDue(time.Now().Add(time.Second))
	queued := <-e.retryJobs
	e.retryQueueOccupancy.Add(-1)
	e.retryTokens = 0
	if e.processJob(context.Background(), queued) {
		t.Fatal("retry unexpectedly dispatched without token")
	}
	if c := e.Counters(); c.Retries != 0 || c.PublishAttempts != 1 || c.RetryBudgetDelayed != 1 {
		t.Fatalf("token wait accounting: %+v", c)
	}
	e.mu.Lock()
	e.pending[id].nextRetry = time.Now().Add(-time.Second)
	e.retryScanAt = time.Time{}
	e.retryTokens = 1
	e.mu.Unlock()
	e.retryDue(time.Now().Add(time.Second))
	queued = <-e.retryJobs
	e.retryQueueOccupancy.Add(-1)
	if !e.processJob(context.Background(), queued) {
		t.Fatal("retry not dispatched")
	}
	if string(original) != string(f.publishes[1]) {
		t.Fatal("retry bytes changed")
	}
}

func TestPolicyACKBeforeRetryDequeueAndExhaustionRetainEvidence(t *testing.T) {
	e, f := retryTestEngine(t, 1)
	if err := e.generate(time.Now(), "measure"); err != nil {
		t.Fatal(err)
	}
	id := <-e.jobs
	e.processJob(context.Background(), id)
	e.mu.Lock()
	e.pending[id].nextRetry = time.Now().Add(-time.Second)
	e.retryScanAt = time.Time{}
	e.mu.Unlock()
	e.retryDue(time.Now().Add(time.Second))
	e.handleACK(ackInput{gatewayID: "gw-a", topic: "gateways/gw-a/acks/" + id.String(), payload: commitACKPayload(id), at: time.Now()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.worker(ctx); close(done) }()
	deadline := time.Now().Add(time.Second)
	for e.retryQueueOccupancy.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if e.retryQueueOccupancy.Load() != 0 || len(f.publishes) != 1 {
		t.Fatal("ACKed retry was published")
	}

	e2, f2 := retryTestEngine(t, 0)
	if err := e2.generate(time.Now(), "measure"); err != nil {
		t.Fatal(err)
	}
	id2 := <-e2.jobs
	e2.processJob(context.Background(), id2)
	e2.mu.Lock()
	e2.pending[id2].nextRetry = time.Now().Add(-time.Second)
	e2.retryScanAt = time.Time{}
	e2.mu.Unlock()
	e2.retryDue(time.Now().Add(time.Second))
	if e2.Counters().RetryExhaustedPending != 1 || e2.Counters().PendingMessages != 1 || len(f2.publishes) != 1 {
		t.Fatalf("exhaustion discarded pending: %+v", e2.Counters())
	}
}

func TestPolicyCancellationDoesNotEraseQueuedPending(t *testing.T) {
	e, f := retryTestEngine(t, 2)
	if err := e.generate(time.Now(), "measure"); err != nil {
		t.Fatal(err)
	}
	id := <-e.jobs
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e.processJob(ctx, id) {
		t.Fatal("canceled publish was dispatched")
	}
	if c := e.Counters(); c.PendingMessages != 1 || c.PublishAttempts != 0 || len(f.publishes) != 0 {
		t.Fatalf("cancellation lost pending evidence or counted a publish: %+v", c)
	}
}

func TestPolicyRetryQueueBoundedWithLargePendingAndFirstPublishProgress(t *testing.T) {
	e, _ := retryTestEngine(t, 2)
	e.m.Limits.PendingMessages = 128
	e.jobs = make(chan uuid.UUID, 128)
	e.retryJobs = make(chan uuid.UUID, 128)
	for i := 0; i < 100; i++ {
		if err := e.generate(time.Now(), "measure"); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 100; i++ {
		id := <-e.jobs
		e.mu.Lock()
		p := e.pending[id]
		p.attempts = 1
		p.nextRetry = time.Now().Add(-time.Second)
		e.mu.Unlock()
	}
	e.retryScanAt = time.Time{}
	e.retryDue(time.Now())
	if len(e.retryJobs) > 128 || e.Counters().RetryQueueOccupancy > 128 {
		t.Fatalf("retry queue unbounded: %+v", e.Counters())
	}
	if err := e.generate(time.Now(), "measure"); err != nil {
		t.Fatal(err)
	}
	if len(e.jobs) != 1 {
		t.Fatal("retry backlog prevented first publish queue progress")
	}
}

func TestPolicyRetryOccupancyDecrementsOnceWhenTokenDenied(t *testing.T) {
	e, _ := retryTestEngine(t, 2)
	if err := e.generate(time.Now(), "measure"); err != nil {
		t.Fatal(err)
	}
	id := <-e.jobs
	e.processJob(context.Background(), id)
	e.mu.Lock()
	e.pending[id].nextRetry = time.Now().Add(-time.Second)
	e.retryScanAt = time.Time{}
	e.retryTokens = 0
	e.mu.Unlock()
	e.retryDue(time.Now())
	if got := e.retryQueueOccupancy.Load(); got != 1 {
		t.Fatalf("queued occupancy=%d, want 1", got)
	}
	queued := <-e.retryJobs
	e.retryQueueOccupancy.Add(-1) // worker's dequeue accounting
	e.processJob(context.Background(), queued)
	if got := e.retryQueueOccupancy.Load(); got != 0 {
		t.Fatalf("occupancy after denied dispatch=%d, want 0", got)
	}
	if got := e.retryQueueHighWater.Load(); got > int64(cap(e.retryJobs)) {
		t.Fatalf("high-water exceeded capacity: %d", got)
	}
}

func TestPolicyExhaustedPendingMetricIsCurrentAndACKRemovesIt(t *testing.T) {
	e, _ := retryTestEngine(t, 0)
	if err := e.generate(time.Now(), "measure"); err != nil {
		t.Fatal(err)
	}
	id := <-e.jobs
	e.processJob(context.Background(), id)
	e.mu.Lock()
	e.pending[id].nextRetry = time.Time{}
	e.mu.Unlock()
	for i := 0; i < 4; i++ {
		e.retryDue(time.Now().Add(time.Duration(i+1) * time.Second))
	}
	if got := e.Counters().RetryExhaustedPending; got != 1 {
		t.Fatalf("exhausted metric inflated: %d", got)
	}
	e.handleACK(ackInput{gatewayID: "gw-a", topic: "gateways/gw-a/acks/" + id.String(), payload: commitACKPayload(id), at: time.Now()})
	if got := e.Counters().RetryExhaustedPending; got != 0 {
		t.Fatalf("exhausted metric after ACK=%d, want 0", got)
	}
}

func TestPolicyRetryGetsFairOpportunityDuringFirstPublishBacklog(t *testing.T) {
	e, f := retryTestEngine(t, 2)
	const firstCount = 12
	e.m.Limits.PendingMessages = firstCount + 1
	e.m.Limits.PendingBytes = 1 << 20
	e.jobs = make(chan uuid.UUID, firstCount)
	e.retryJobs = make(chan uuid.UUID, 1)
	e.retryTokens = 1
	var retryID uuid.UUID
	for i := 0; i < firstCount; i++ {
		if err := e.generate(time.Now(), "measure"); err != nil {
			t.Fatal(err)
		}
	}
	first := make([]uuid.UUID, 0, firstCount)
	for i := 0; i < firstCount; i++ {
		first = append(first, <-e.jobs)
	}
	retryID = uuid.New()
	e.pending[retryID] = e.pending[first[0]]
	e.pending[retryID].attempts = 1
	e.pending[retryID].queued = true
	e.pending[retryID].retryJob = true
	delete(e.pending, first[0])
	for _, id := range first[1:] {
		e.jobs <- id
	}
	retryPayload := append([]byte(nil), e.pending[retryID].batch.Payload...)
	e.retryJobs <- retryID
	e.retryQueueOccupancy.Store(1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.worker(ctx); close(done) }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		n := len(f.publishes)
		f.mu.Unlock()
		if n >= 9 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.publishes) < 9 || string(f.publishes[8]) != string(retryPayload) {
		t.Fatalf("retry starved by first-publish backlog")
	}
}

func TestLegacyManifestDoesNotAllocateRetryQueue(t *testing.T) {
	m := telemetryManifest(1)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": &fakeLoadTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	if e.retryJobs != nil {
		t.Fatal("legacy manifest allocated policy retry queue")
	}
}

func TestGenerationFailureCannotCompleteWithZeroOffered(t *testing.T) {
	m := telemetryManifest(100)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	m.Traffic = Traffic{RatePerSecond: 1}
	m.Phases = Phases{MeasureSeconds: 1, DrainSeconds: 1}
	m.Limits.WallSeconds = 4
	m.Limits.PendingBytes = 1 // valid stream, but no generated batch can fit.
	transport := &fakeLoadTransport{}
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": transport})
	if err != nil {
		t.Fatal(err)
	}
	status, err := e.Run(context.Background())
	if status != "FAILED" || err == nil || err.Error() != "pending_byte_cap" {
		t.Fatalf("generation failure status=%s err=%v", status, err)
	}
	if got := e.Counters(); got.Generated != 0 || got.OfferedUnique != 0 {
		t.Fatalf("failed payload was counted as offered: %+v", got)
	}
}

func TestDrainDeadlineCancelsAndJoinsWorkersWithQueuedPending(t *testing.T) {
	m := telemetryManifest(1)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "a"}, {GatewayID: "gw-a", SensorID: "b"}, {GatewayID: "gw-a", SensorID: "c"}}
	m.Limits.Workers = 1
	m.Limits.PendingMessages = 8
	m.Phases = Phases{MeasureSeconds: 1, DrainSeconds: 1}
	m.Traffic = Traffic{RatePerSecond: 1, BurstMessages: 5}
	f := newBlockingLoadTransport()
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": f})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := e.generate(time.Now(), "measure"); err != nil {
			t.Fatal(err)
		}
	}
	e.stopGeneration.Store(true)
	done := make(chan struct{})
	var status string
	go func() { status, _ = e.Run(context.Background()); close(done) }()
	select {
	case <-f.entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not enter publish")
	}
	select {
	case <-done:
	case <-time.After(1800 * time.Millisecond):
		t.Fatal("Run did not return after bounded drain")
	}
	if f.active.Load() != 0 {
		t.Fatalf("worker still publishing after Run returned: %d", f.active.Load())
	}
	if status != "PARTIAL" || e.Counters().PendingMessages == 0 {
		t.Fatalf("queued pending erased or not partial: status=%s counters=%+v", status, e.Counters())
	}
}

func TestACKMustMatchPendingGatewayAndPublishedState(t *testing.T) {
	m := telemetryManifest(1)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}, {GatewayID: "gw-b", SensorID: "s-b"}}
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": &fakeLoadTransport{}, "gw-b": &fakeLoadTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.generate(time.Now(), "measure"); err != nil {
		t.Fatal(err)
	}
	var id uuid.UUID
	for candidate, pending := range e.pending {
		if pending.batch.Batch.GatewayID == "gw-a" {
			id = candidate
			break
		}
	}
	e.mu.Lock()
	p := e.pending[id]
	p.attempts = 1
	p.firstPublishedAt = time.Now()
	e.mu.Unlock()
	ack := commitACKPayload(id)
	e.handleACK(ackInput{gatewayID: "gw-b", topic: "gateways/gw-b/acks/" + id.String(), payload: ack, at: time.Now()})
	e.mu.Lock()
	remaining := e.pending[id] != nil
	e.mu.Unlock()
	if !remaining || e.Counters().AckLatencyCount != 0 {
		t.Fatalf("foreign gateway ACK consumed pending: %+v", e.Counters())
	}
	e.mu.Lock()
	p = e.pending[id]
	p.attempts = 0
	p.firstPublishedAt = time.Time{}
	e.mu.Unlock()
	e.handleACK(ackInput{gatewayID: "gw-a", topic: "gateways/gw-a/acks/" + id.String(), payload: ack, at: time.Now()})
	e.mu.Lock()
	remaining = e.pending[id] != nil
	e.mu.Unlock()
	if !remaining || e.Counters().AckLatencyCount != 0 {
		t.Fatalf("unpublished ACK consumed pending: %+v", e.Counters())
	}
}

func TestRetryReservationIsSingleAndCountsAtActualPublish(t *testing.T) {
	m := telemetryManifest(1)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	f := &fakeLoadTransport{}
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": f})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.generate(time.Now(), "measure"); err != nil {
		t.Fatal(err)
	}
	id := <-e.jobs
	e.mu.Lock()
	p := e.pending[id]
	p.queued = false
	p.inFlight = true
	p.attempts = 1
	p.firstPublishedAt = time.Now()
	p.nextRetry = time.Now().Add(-time.Second)
	e.mu.Unlock()
	e.counts.attempts.Add(1)
	e.counts.offered.Add(1)
	e.retryDue(time.Now())
	e.retryDue(time.Now())
	if len(e.jobs) != 0 || e.Counters().Retries != 0 {
		t.Fatalf("retry queued while publish in flight: %+v", e.Counters())
	}
	e.mu.Lock()
	p = e.pending[id]
	p.inFlight = false
	p.nextRetry = time.Now().Add(-time.Second)
	e.mu.Unlock()
	e.retryDue(time.Now())
	e.retryDue(time.Now())
	if len(e.jobs) != 1 || e.Counters().Retries != 0 {
		t.Fatalf("retry not reserved exactly once or counted too early: %+v", e.Counters())
	}
	if !e.processJob(context.Background(), <-e.jobs) {
		t.Fatal("retry worker did not process job")
	}
	c := e.Counters()
	if c.PublishAttempts != 2 || c.Retries != 1 || c.BrokerPUBACKs != 1 || c.OfferedUnique != 1 {
		t.Fatalf("actual retry accounting: %+v", c)
	}
}

func TestOfferedUniqueCountsOnlyAtPublishInvocation(t *testing.T) {
	m := telemetryManifest(1)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	f := newBlockingLoadTransport()
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": f})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.generate(time.Now(), "measure"); err != nil {
		t.Fatal(err)
	}
	if e.Counters().OfferedUnique != 0 {
		t.Fatalf("enqueued work counted offered: %+v", e.Counters())
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.processJob(ctx, <-e.jobs); close(done) }()
	<-f.entered
	if c := e.Counters(); c.OfferedUnique != 1 || c.PublishAttempts != 1 {
		t.Fatalf("publish invocation not counted: %+v", c)
	}
	cancel()
	<-done
}

func TestSyntheticHorizonIsCheckedForMaximumOfficialInputs(t *testing.T) {
	m := telemetryManifest(100)
	m.Profile = "official"
	m.Traffic = Traffic{RatePerSecond: MaxRatePerSecond, RampSeconds: 0, BurstMessages: 1000000}
	m.Phases = Phases{WarmupSeconds: 3600, MeasureSeconds: 3600}
	m.Topology = Topology{Gateways: 2, SensorsPerGateway: 2}
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}, {GatewayID: "gw-b", SensorID: "s-b"}}
	m.IdentityFiles = map[string]string{"telemetry": "identity.json"}
	m.DurabilityMode = "synthetic-load"
	m.Phases.DrainSeconds, m.Phases.CooldownSeconds = 0, 0
	m.Limits.WallSeconds = 7200
	_, horizon, err := syntheticStreamBounds(m)
	if err != nil {
		t.Fatalf("maximum official horizon arithmetic failed: %v", err)
	}
	if horizon <= 0 || int64(horizon) >= int64(^uint64(0)>>1) {
		t.Fatalf("invalid maximum official horizon: %s", horizon)
	}
	if _, err := Validate(m, true); err != nil {
		t.Fatalf("maximum official manifest rejected: %v", err)
	}
}

func commitACKPayload(id uuid.UUID) []byte {
	b, _ := json.Marshal(map[string]any{"protocol_version": 1, "message_type": "telemetry_ack", "message_id": id.String(), "status": "committed", "committed_at": time.Now().UTC().Format(time.RFC3339Nano)})
	return b
}

func TestSyntheticBatchProfileAndPendingACKSemantics(t *testing.T) {
	f := &fakeLoadTransport{}
	m := telemetryManifest(100)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "same"}}
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": f})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err = e.generate(now, "measure"); err != nil {
		t.Fatal(err)
	}
	var id uuid.UUID
	for key := range e.pending {
		id = key
	}
	e.mu.Lock()
	p := e.pending[id]
	original := append([]byte(nil), p.batch.Payload...)
	first, last := p.batch.FirstMeasuredAt, p.batch.LastMeasuredAt
	e.mu.Unlock()
	if !last.Before(now) || last.Sub(first) > 60*time.Second {
		t.Fatalf("invalid backdated span first=%s last=%s now=%s", first, last, now)
	}
	if !e.processJob(context.Background(), <-e.jobs) {
		t.Fatal("initial publication was not invoked")
	}
	initial := e.Counters()
	if initial.BrokerPUBACKs != 1 || initial.ApplicationACKs != 0 || initial.PendingMessages != 1 ||
		initial.GeneratedPayloadBytes != int64(len(original)) || initial.OfferedUniquePayloadBytes != int64(len(original)) ||
		initial.AttemptedPayloadBytes != int64(len(original)) {
		t.Fatalf("initial payload byte accounting/PUBACK semantics: %+v", initial)
	}
	e.retryDue(time.Now().Add(time.Duration(m.RetryIntervalMS) * time.Millisecond))
	queued := <-e.jobs
	if queued != id {
		t.Fatalf("retry id=%s want %s", queued, id)
	}
	e.mu.Lock()
	retry := append([]byte(nil), e.pending[id].batch.Payload...)
	e.mu.Unlock()
	if string(original) != string(retry) {
		t.Fatal("retry payload changed")
	}
	if !e.processJob(context.Background(), queued) {
		t.Fatal("retry publication was not invoked")
	}
	ackBytes, _ := json.Marshal(map[string]any{"protocol_version": 1, "message_type": "telemetry_ack", "message_id": id.String(), "status": "committed", "committed_at": time.Now().UTC().Format(time.RFC3339Nano)})
	e.handleACK(ackInput{gatewayID: "gw-a", topic: "gateways/gw-a/acks/" + id.String(), payload: ackBytes, at: time.Now()})
	if c := e.Counters(); c.ApplicationACKs != 1 || c.MeasureApplicationACKs != 1 ||
		c.WarmupApplicationACKs != 0 || c.BurstApplicationACKs != 0 || c.PendingMessages != 0 || c.PendingBytes != 0 {
		t.Fatalf("application ACK not matched: %+v", c)
	}
	if c := e.Counters(); c.GeneratedPayloadBytes != int64(len(original)) ||
		c.OfferedUniquePayloadBytes != int64(len(original)) || c.AttemptedPayloadBytes != int64(2*len(original)) {
		t.Fatalf("retry payload bytes must count exact same payload per attempt: %+v", c)
	}
}

func TestApplicationACKRetainsGeneratedCohortAcrossDrainBoundary(t *testing.T) {
	f := &fakeLoadTransport{}
	m := telemetryManifest(1)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": f})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.generate(time.Now(), "warmup"); err != nil {
		t.Fatal(err)
	}
	var id uuid.UUID
	for key := range e.pending {
		id = key
	}
	if !e.processJob(context.Background(), <-e.jobs) {
		t.Fatal("publish was not invoked")
	}
	// ACK arrives after the measured cutoff: it must remain charged to the original warmup cohort.
	e.handleACK(ackInput{gatewayID: "gw-a", topic: "gateways/gw-a/acks/" + id.String(),
		payload: commitACKPayload(id), at: time.Now()})
	c := e.Counters()
	if c.WarmupApplicationACKs != 1 || c.MeasureApplicationACKs != 0 || c.ApplicationACKs != 1 {
		t.Fatalf("ACK cohort crossed phase cutoff: %+v", c)
	}
}

func TestSyntheticSingleSampleAndCompositeSequenceRanges(t *testing.T) {
	m := telemetryManifest(1)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "same"}, {GatewayID: "gw-b", SensorID: "same"}}
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": &fakeLoadTransport{}, "gw-b": &fakeLoadTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err = e.generate(now, "measure"); err != nil {
		t.Fatal(err)
	}
	if err = e.generate(now, "measure"); err != nil {
		t.Fatal(err)
	}
	if len(e.pending) != 2 || e.nextSequence["gw-a\x00same"] != 1 || e.nextSequence["gw-b\x00same"] != 1 {
		t.Fatalf("composite sequence allocation: %#v", e.nextSequence)
	}
	for _, p := range e.pending {
		if !p.batch.LastMeasuredAt.Before(now) {
			t.Fatal("sample timestamp is not in the past")
		}
	}
}

func TestPendingCapsAndInvalidRetainedForeignACKs(t *testing.T) {
	m := telemetryManifest(1)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	m.Limits.PendingMessages = 1
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": &fakeLoadTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err = e.generate(now, "measure"); err != nil {
		t.Fatal(err)
	}
	if err = e.generate(now, "measure"); err == nil {
		t.Fatal("pending message cap not enforced")
	}
	var id uuid.UUID
	for k := range e.pending {
		id = k
	}
	bad := []byte(`{"protocol_version":1}`)
	e.handleACK(ackInput{gatewayID: "gw-a", topic: "gateways/gw-a/acks/" + id.String(), payload: bad, at: now})
	e.handleACK(ackInput{gatewayID: "gw-a", topic: "gateways/other/acks/" + id.String(), payload: bad, at: now})
	e.handleACK(ackInput{gatewayID: "gw-a", topic: "gateways/gw-a/acks/" + id.String(), payload: bad, retained: true, at: now})
	if c := e.Counters(); c.InvalidACKs != 2 || c.RetainedACKs != 1 || c.PendingMessages != 1 {
		t.Fatalf("invalid ACK changed pending: %+v", c)
	}
}

func TestInvalidACKDoesNotConsumeValidACKIgnoreBudget(t *testing.T) {
	m := telemetryManifest(1)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	m.E4.Mode = "unique"
	f := &fakeLoadTransport{}
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": f})
	if err != nil {
		t.Fatal(err)
	}
	e.ignoreACKBudget = 1
	if err = e.generate(time.Now(), "measure"); err != nil {
		t.Fatal(err)
	}
	id := <-e.jobs
	e.processJob(context.Background(), id)
	topic := "gateways/gw-a/acks/" + id.String()
	e.handleACK(ackInput{gatewayID: "gw-a", topic: topic, payload: []byte(`{"protocol_version":1}`), at: time.Now()})
	if e.Counters().IgnoredValidACKs != 0 {
		t.Fatal("invalid ACK consumed ignore budget")
	}
	e.handleACK(ackInput{gatewayID: "gw-a", topic: topic, payload: commitACKPayload(id), at: time.Now()})
	if e.Counters().IgnoredValidACKs != 1 || e.Counters().PendingMessages != 1 {
		t.Fatalf("first valid ACK was not ignored: %+v", e.Counters())
	}
	e.retryDue(time.Now().Add(time.Second))
	if !e.processJob(context.Background(), <-e.jobs) {
		t.Fatal("retry not published")
	}
	e.handleACK(ackInput{gatewayID: "gw-a", topic: topic, payload: commitACKPayload(id), at: time.Now()})
	c := e.Counters()
	if c.Retries != 1 || c.AttemptedPayloadBytes != int64(2*len(f.publishes[0])) || c.ApplicationACKs != 1 {
		t.Fatalf("valid ACK ignore did not cause exact retry: %+v", c)
	}
}

func TestReplayUsesOriginalBatchWithoutUniqueAccounting(t *testing.T) {
	m := telemetryManifest(1)
	m.Experiment = "E4"
	m.E4.Mode = "replay"
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	transport := &fakeLoadTransport{}
	seedManifest := m
	seedManifest.E4.Mode = "unique"
	seed, err := NewTelemetryEngine(seedManifest, map[string]TelemetryTransport{"gw-a": &fakeLoadTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	if err = seed.generate(time.Now(), "measure"); err != nil {
		t.Fatal(err)
	}
	var original gatewaysim.BuiltBatch
	for _, p := range seed.pending {
		original = p.batch
	}
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": transport})
	if err != nil {
		t.Fatal(err)
	}
	e.replay = []E4ReplayBatch{{Batch: original}}
	e.counts.replayScheduled.Add(1) // mirrors the scheduled opportunity in Run.
	if err = e.generate(time.Now(), "measure"); err != nil {
		t.Fatal(err)
	}
	id := <-e.jobs
	if !e.processJob(context.Background(), id) {
		t.Fatal("replay not offered")
	}
	e.handleACK(ackInput{gatewayID: "gw-a", topic: "gateways/gw-a/acks/" + id.String(), payload: commitACKPayload(id), at: time.Now()})
	c := e.Counters()
	if c.Generated != 0 || c.OfferedUnique != 0 || c.ApplicationACKs != 0 || c.ReplayScheduled != 1 || c.ReplayOffered != 1 || c.ReplayApplicationACKs != 1 {
		t.Fatalf("replay contaminated unique counters: %+v", c)
	}
	if string(transport.publishes[0]) != string(original.Payload) {
		t.Fatal("replay payload changed")
	}
}

func TestUniqueJournalFailurePreventsEnqueueAndPublish(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	journal, err := CreateE4Journal(filepath.Join(dir, "failed.jsonl"), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	m := telemetryManifest(1)
	m.Experiment, m.E4.Mode = "E4", "unique"
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	transport := &fakeLoadTransport{}
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": transport})
	if err != nil {
		t.Fatal(err)
	}
	e.journal = journal
	if err = e.generate(time.Now(), "measure"); err == nil {
		t.Fatal("journal byte cap was ignored")
	}
	if len(e.jobs) != 0 || len(transport.publishes) != 0 || e.Counters().Generated != 0 {
		t.Fatalf("failed journal allowed publication: jobs=%d counters=%+v", len(e.jobs), e.Counters())
	}
}

func TestE4UniqueOpenLoopStopsAtParameterizedJournalCount(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	journal, err := CreateE4Journal(filepath.Join(dir, "unique.jsonl"), 8, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	m := telemetryManifest(100)
	m.Experiment, m.E4.Mode = "E4", "unique"
	m.E4.MaxMessages, m.E4.MaxBytes = 8, 1<<20
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	engine, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": &fakeLoadTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	engine.journal = journal
	if !engine.runOpenLoop(context.Background(), 4*time.Second, 0, false) {
		t.Fatal("bounded E4 unique phase did not complete")
	}
	counters := engine.Counters()
	if counters.Generated != 8 || counters.Scheduled != 8 || counters.Deferred != 0 {
		t.Fatalf("E4 unique phase exceeded its parameterized schedule: %+v", counters)
	}
}

func TestGeneratedAndOfferedUniqueAreDifferentAtQueueCap(t *testing.T) {
	m := telemetryManifest(1)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": &fakeLoadTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	e.jobs = make(chan uuid.UUID, 1)
	e.jobs <- uuid.New() // occupy the one-slot queue without creating a real offer.
	if err = e.generate(time.Now(), "measure"); err == nil {
		t.Fatal("full bounded publish queue accepted work")
	}
	c := e.Counters()
	if c.Generated != 1 || c.OfferedUnique != 0 || c.PendingMessages != 0 {
		t.Fatalf("generated/offered accounting: %+v", c)
	}
}

func TestReadyFailureIsPartialFreeAndCredentialsArePrivate(t *testing.T) {
	m := telemetryManifest(1)
	m.Targets = []Target{{GatewayID: "gw-a", SensorID: "s-a"}}
	for _, failure := range []string{"tls_verification_failed", "suback_denied"} {
		f := &fakeLoadTransport{readyErr: errors.New(failure)}
		e, err := NewTelemetryEngine(m, map[string]TelemetryTransport{"gw-a": f})
		if err != nil {
			t.Fatal(err)
		}
		status, err := e.Run(context.Background())
		if status != "FAILED" || err == nil || !f.stopped {
			t.Fatalf("failed readiness status=%s err=%v stopped=%v", status, err, f.stopped)
		}
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "identity.json")
	if err := os.WriteFile(path, []byte(`{"username":"gw-a","password":"private"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readCredential(path, "gw-a"); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readCredential(path, "gw-a"); err == nil {
		t.Fatal("world-readable credential accepted")
	}
}

func TestBuilderRejectsUnsupportedSyntheticRateSpan(t *testing.T) {
	_, err := gatewaysim.BuildSyntheticBatch(gatewaysim.BatchRequest{GatewayID: "gw-a", SensorID: "s-a", BootID: uuid.New(), MessageID: uuid.New(), MeasuredAt: time.Now().Add(-time.Second), SampleCount: 100, SampleIntervalUS: 1_000_000}, gatewaysim.ServerLimits(), time.Now())
	if err == nil {
		t.Fatal("validator accepted span over telemetry limit")
	}
}

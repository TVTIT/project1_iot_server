package mqttcontrol

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"

	"github.com/google/uuid"
)

type reconciliationTestRepository struct {
	calls    int
	ctx      context.Context
	query    digitaltwin.ControlQueryContext
	snapshot digitaltwin.ConfigurationSnapshot
	deadline time.Time
	outcome  digitaltwin.ReconciliationOutcome
	err      error
}

func (r *reconciliationTestRepository) ReconcileSnapshot(ctx context.Context, query digitaltwin.ControlQueryContext, snapshot digitaltwin.ConfigurationSnapshot, deadline time.Time) (digitaltwin.ReconciliationOutcome, error) {
	r.calls++
	r.ctx, r.query, r.snapshot, r.deadline = ctx, query, snapshot, deadline
	return r.outcome, r.err
}

func TestReconciliationSnapshotHandlerPassesTrustedCorrelationAndCountsCommittedOutcome(t *testing.T) {
	repo := &reconciliationTestRepository{outcome: digitaltwin.ReconciliationOutcome{
		Decision: digitaltwin.ReconciliationSuccessorCreated, CommandID: uuid.NewString(), DesiredVersion: 2,
	}}
	metrics := &ReconciliationMetrics{}
	handler := NewReconciliationSnapshotHandler(repo, metrics)
	target := testQueryContext(4)
	deadline := time.Now().Add(time.Second)
	snapshot := digitaltwin.ConfigurationSnapshot{
		ProtocolVersion: 1, MessageType: "configuration_snapshot", RequestID: uuid.NewString(),
		GatewayID: target.GatewayID, EntityID: target.EntityURN,
		ConfigurationState: "unconfigured", WatermarkVersion: 0,
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	handler(ctx, ControlSnapshotCandidate{Query: target, Snapshot: snapshot, LocalDeadline: deadline})
	if repo.calls != 1 || repo.query != target || repo.snapshot.RequestID != snapshot.RequestID ||
		!repo.deadline.Equal(deadline) {
		t.Fatalf("reconcile input not forwarded: repo=%+v", repo)
	}
	if got, ok := repo.ctx.Deadline(); !ok || !got.Equal(deadline) {
		t.Fatalf("handler did not preserve bounded context deadline: %v/%t want %v", got, ok, deadline)
	}
	got := metrics.Snapshot()
	if got.Matched != 1 || got.SuccessorCreated != 1 || got.Deadline != 0 || got.CommitUncertain != 0 {
		t.Fatalf("confirmed outcome metrics=%+v", got)
	}
}

func TestReconciliationSnapshotHandlerDoesNotCountFailedOrExpiredAsSuccess(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantMetric string
	}{
		{name: "stale context", err: digitaltwin.ErrControlQueryContextChanged, wantMetric: "stale"},
		{name: "expired", err: digitaltwin.ErrControlQueryExpired, wantMetric: "deadline"},
		{name: "database", err: digitaltwin.ErrDependencyUnavailable, wantMetric: "dependency"},
		{name: "ambiguous commit", err: errors.Join(digitaltwin.ErrControlCommitUncertain, digitaltwin.ErrDependencyUnavailable), wantMetric: "commit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &reconciliationTestRepository{err: tc.err}
			metrics := &ReconciliationMetrics{}
			handler := NewReconciliationSnapshotHandler(repo, metrics)
			handler(context.Background(), ControlSnapshotCandidate{
				Query: testQueryContext(5), Snapshot: digitaltwin.ConfigurationSnapshot{},
				LocalDeadline: time.Now().Add(time.Second),
			})
			got := metrics.Snapshot()
			if got.Matched != 0 || got.SuccessorCreated != 0 {
				t.Fatalf("failed reconciliation recorded success: %+v", got)
			}
			switch tc.wantMetric {
			case "stale":
				if got.StaleContext != 1 {
					t.Fatalf("stale metric=%+v", got)
				}
			case "deadline":
				if got.Deadline != 1 {
					t.Fatalf("deadline metric=%+v", got)
				}
			case "dependency":
				if got.DependencyError != 1 {
					t.Fatalf("dependency metric=%+v", got)
				}
			case "commit":
				if got.CommitUncertain != 1 {
					t.Fatalf("commit metric=%+v", got)
				}
			}
		})
	}

	repo := &reconciliationTestRepository{}
	metrics := (*ReconciliationMetrics)(nil)
	handler := NewReconciliationSnapshotHandler(repo, metrics)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	handler(ctx, ControlSnapshotCandidate{LocalDeadline: time.Now().Add(time.Second)})
	if repo.calls != 0 {
		t.Fatal("cancelled query called reconciliation repository")
	}
}

type queryTestRepository struct {
	mu            sync.Mutex
	targets       []digitaltwin.ControlQueryContext
	current       bool
	calls         int
	checkStarted  chan struct{}
	checkRelease  chan struct{}
	checkDeadline chan time.Time
}

func (r *queryTestRepository) ReserveDueControlQueries(_ context.Context, limit int, _ time.Duration) ([]digitaltwin.ControlQueryContext, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if limit > len(r.targets) {
		limit = len(r.targets)
	}
	out := append([]digitaltwin.ControlQueryContext(nil), r.targets[:limit]...)
	r.targets = r.targets[limit:]
	return out, nil
}

func (r *queryTestRepository) ControlQueryContextCurrent(ctx context.Context, _ digitaltwin.ControlQueryContext) (bool, error) {
	r.mu.Lock()
	current, started, release, deadlineCh := r.current, r.checkStarted, r.checkRelease, r.checkDeadline
	r.mu.Unlock()
	if deadlineCh != nil {
		if deadline, ok := ctx.Deadline(); ok {
			deadlineCh <- deadline
		}
	}
	if started != nil {
		close(started)
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return current, nil
}

type queryTestPublisher struct {
	mu        sync.Mutex
	ready     bool
	err       error
	published []digitaltwin.ConfigurationQuery
	onPublish func(digitaltwin.ConfigurationQuery)
}

func (p *queryTestPublisher) Ready() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ready
}

func (p *queryTestPublisher) PublishConfigurationQuery(_ context.Context, q digitaltwin.ConfigurationQuery) error {
	p.mu.Lock()
	p.published = append(p.published, q)
	fn := p.onPublish
	err := p.err
	p.mu.Unlock()
	if err != nil {
		return err
	}
	if fn != nil {
		fn(q)
	}
	return nil
}

func testQueryContext(n int) digitaltwin.ControlQueryContext {
	gatewayID := "fixture_gw"
	sensorID := "sensor_" + string(rune('a'+n))
	return digitaltwin.ControlQueryContext{
		EntityID: uuid.New(), EntityURN: "urn:ngsi-ld:Sensor:" + gatewayID + ":" + sensorID,
		GatewayID: gatewayID, SensorID: sensorID, DesiredVersion: digitaltwin.Version(n),
	}
}

func snapshotForQuery(t *testing.T, q digitaltwin.ConfigurationQuery) RawResult {
	t.Helper()
	actual := int64(1000000)
	snapshot := digitaltwin.ConfigurationSnapshot{
		ProtocolVersion: 1, MessageType: "configuration_snapshot", RequestID: q.RequestID,
		GatewayID: q.GatewayID, EntityID: q.EntityID, WatermarkVersion: 0,
		ConfigurationState: "unconfigured", ActualSampleIntervalUS: &actual,
		ReadbackAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	payload, err := digitaltwin.MarshalMQTTConfigurationSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return RawResult{
		Topic:   "gateways/" + q.GatewayID + "/responses/" + q.RequestID,
		Payload: payload, ReceivedAt: time.Now(),
	}
}

func waitForQuery(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(what)
}

func TestControlQueryRuntimeRegistersCorrelationBeforeImmediateSnapshot(t *testing.T) {
	target := testQueryContext(1)
	repo := &queryTestRepository{targets: []digitaltwin.ControlQueryContext{target}, current: true}
	publisher := &queryTestPublisher{ready: true}
	metrics := &ControlQueryMetrics{}
	candidates := make(chan ControlSnapshotCandidate, 1)
	runtime, err := NewControlQueryRuntime(repo, publisher, func(_ context.Context, candidate ControlSnapshotCandidate) {
		candidates <- candidate
	}, metrics)
	if err != nil {
		t.Fatal(err)
	}
	publisher.onPublish = func(q digitaltwin.ConfigurationQuery) {
		runtime.HandleSnapshot(context.Background(), snapshotForQuery(t, q))
	}
	if err := runtime.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	select {
	case candidate := <-candidates:
		if candidate.Query.EntityURN != target.EntityURN || candidate.Snapshot.ConfigurationState != "unconfigured" ||
			candidate.LocalDeadline.IsZero() {
			t.Fatalf("candidate=%+v", candidate)
		}
	case <-time.After(time.Second):
		t.Fatal("immediate snapshot was not correlated")
	}
	if runtime.Pending() != 0 || metrics.Snapshot().Candidate != 1 {
		t.Fatalf("correlation not consumed: pending=%d metrics=%+v", runtime.Pending(), metrics.Snapshot())
	}
}

func TestControlQueryRuntimeRejectsWrongReplayExpiredAndChangedContext(t *testing.T) {
	target := testQueryContext(2)
	repo := &queryTestRepository{current: false}
	publisher := &queryTestPublisher{ready: true}
	var accepted int
	runtime, err := NewControlQueryRuntime(repo, publisher, func(context.Context, ControlSnapshotCandidate) { accepted++ }, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	query := digitaltwin.ConfigurationQuery{
		ProtocolVersion: 1, MessageType: "configuration_query", RequestID: uuid.NewString(),
		GatewayID: target.GatewayID, EntityID: target.EntityURN,
		IssuedAt: now, ExpiresAt: now.Add(controlQueryTTL),
	}
	pending := pendingControlQuery{query: query, context: target, deadline: now.Add(controlQueryTTL)}
	runtime.pending[query.RequestID] = pending
	runtime.byEntity[target.EntityURN] = query.RequestID

	wrongQuery := query
	wrongQuery.EntityID = "urn:ngsi-ld:Sensor:" + query.GatewayID + ":other_sensor"
	wrong := snapshotForQuery(t, wrongQuery)
	runtime.HandleSnapshot(context.Background(), wrong)
	if runtime.Pending() != 1 {
		t.Fatal("malformed snapshot consumed valid correlation")
	}

	runtime.HandleSnapshot(context.Background(), snapshotForQuery(t, query))
	if runtime.Pending() != 0 || accepted != 0 {
		t.Fatalf("changed DB context forwarded: pending=%d accepted=%d", runtime.Pending(), accepted)
	}
	repo.current = true
	runtime.HandleSnapshot(context.Background(), snapshotForQuery(t, query))
	if accepted != 0 {
		t.Fatal("replayed snapshot reused consumed request ID")
	}

	query.RequestID = uuid.NewString()
	pending.query = query
	pending.deadline = now.Add(-time.Second)
	runtime.pending[query.RequestID] = pending
	runtime.byEntity[target.EntityURN] = query.RequestID
	runtime.now = func() time.Time { return now }
	runtime.HandleSnapshot(context.Background(), snapshotForQuery(t, query))
	if runtime.Pending() != 0 || accepted != 0 {
		t.Fatal("expired local correlation accepted")
	}
}

func TestControlQueryRuntimeDoesNotForwardSnapshotAfterRecheckCrossesLocalDeadline(t *testing.T) {
	target := testQueryContext(6)
	now := time.Now().UTC().Truncate(time.Microsecond)
	query := digitaltwin.ConfigurationQuery{
		ProtocolVersion: 1, MessageType: "configuration_query", RequestID: uuid.NewString(),
		GatewayID: target.GatewayID, EntityID: target.EntityURN,
		IssuedAt: now, ExpiresAt: now.Add(controlQueryTTL),
	}
	deadline := query.ExpiresAt
	repo := &queryTestRepository{
		current: true, checkStarted: make(chan struct{}), checkRelease: make(chan struct{}),
		checkDeadline: make(chan time.Time, 1),
	}
	publisher := &queryTestPublisher{ready: true}
	candidates := make(chan ControlSnapshotCandidate, 1)
	runtime, err := NewControlQueryRuntime(repo, publisher, func(_ context.Context, candidate ControlSnapshotCandidate) {
		candidates <- candidate
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var clockMu sync.Mutex
	fakeNow := now.Add(time.Second)
	runtime.now = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return fakeNow
	}
	runtime.pending[query.RequestID] = pendingControlQuery{query: query, context: target, deadline: deadline}
	runtime.byEntity[target.EntityURN] = query.RequestID

	done := make(chan struct{})
	go func() {
		runtime.HandleSnapshot(context.Background(), snapshotForQuery(t, query))
		close(done)
	}()
	select {
	case <-repo.checkStarted:
	case <-time.After(time.Second):
		t.Fatal("database context recheck did not start")
	}
	var gotDeadline time.Time
	select {
	case gotDeadline = <-repo.checkDeadline:
	default:
	}
	clockMu.Lock()
	fakeNow = deadline.Add(time.Microsecond)
	clockMu.Unlock()
	close(repo.checkRelease)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("snapshot handler did not return")
	}
	select {
	case candidate := <-candidates:
		t.Fatalf("snapshot after local expiry was forwarded: %+v", candidate)
	default:
	}
	if !gotDeadline.Equal(deadline) {
		t.Fatalf("DB context deadline=%v want local query deadline=%v", gotDeadline, deadline)
	}
	if runtime.Pending() != 0 {
		t.Fatalf("expired query correlation retained: %d", runtime.Pending())
	}
}

func TestControlQueryRuntimeStopCancelsSynchronousSnapshotHandler(t *testing.T) {
	target := testQueryContext(8)
	now := time.Now().UTC().Truncate(time.Microsecond)
	query := digitaltwin.ConfigurationQuery{
		ProtocolVersion: 1, MessageType: "configuration_query", RequestID: uuid.NewString(),
		GatewayID: target.GatewayID, EntityID: target.EntityURN,
		IssuedAt: now, ExpiresAt: now.Add(controlQueryTTL),
	}
	repo := &queryTestRepository{current: true}
	publisher := &queryTestPublisher{ready: true}
	entered := make(chan struct{})
	handlerDone := make(chan struct{})
	runtime, err := NewControlQueryRuntime(repo, publisher, func(ctx context.Context, _ ControlSnapshotCandidate) {
		close(entered)
		<-ctx.Done()
		close(handlerDone)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	runtime.pending[query.RequestID] = pendingControlQuery{
		query: query, context: target, deadline: query.ExpiresAt,
	}
	runtime.byEntity[target.EntityURN] = query.RequestID
	callDone := make(chan struct{})
	go func() {
		runtime.HandleSnapshot(context.Background(), snapshotForQuery(t, query))
		close(callDone)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("snapshot handler did not start")
	}
	stopDone := make(chan struct{})
	go func() {
		runtime.Stop()
		close(stopDone)
	}()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("runtime stop did not cancel synchronous handler context")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("runtime stop remained blocked after bounded handler cancellation")
	}
	select {
	case <-callDone:
	case <-time.After(time.Second):
		t.Fatal("snapshot call did not return after stop")
	}
}

func TestControlQueryRuntimeExpiresCorrelationEvenWhenTransportOffline(t *testing.T) {
	target := testQueryContext(7)
	now := time.Now().UTC().Truncate(time.Microsecond)
	query := digitaltwin.ConfigurationQuery{
		ProtocolVersion: 1, MessageType: "configuration_query", RequestID: uuid.NewString(),
		GatewayID: target.GatewayID, EntityID: target.EntityURN,
		IssuedAt: now.Add(-controlQueryTTL), ExpiresAt: now,
	}
	repo := &queryTestRepository{}
	publisher := &queryTestPublisher{ready: false}
	metrics := &ControlQueryMetrics{}
	runtime, err := NewControlQueryRuntime(repo, publisher, func(context.Context, ControlSnapshotCandidate) {}, metrics)
	if err != nil {
		t.Fatal(err)
	}
	runtime.now = func() time.Time { return now.Add(time.Microsecond) }
	runtime.pending[query.RequestID] = pendingControlQuery{query: query, context: target, deadline: query.ExpiresAt}
	runtime.byEntity[target.EntityURN] = query.RequestID

	runtime.scan()
	if runtime.Pending() != 0 || repo.calls != 0 || metrics.Snapshot().Timeout != 1 || metrics.Snapshot().Offline != 1 {
		t.Fatalf("offline scan retained expired correlation or reserved work: pending=%d calls=%d metrics=%+v",
			runtime.Pending(), repo.calls, metrics.Snapshot())
	}
}

func TestControlQueryRuntimeStopCancelsInFlightContextRecheck(t *testing.T) {
	target := testQueryContext(8)
	now := time.Now().UTC().Truncate(time.Microsecond)
	query := digitaltwin.ConfigurationQuery{
		ProtocolVersion: 1, MessageType: "configuration_query", RequestID: uuid.NewString(),
		GatewayID: target.GatewayID, EntityID: target.EntityURN,
		IssuedAt: now, ExpiresAt: now.Add(controlQueryTTL),
	}
	repo := &queryTestRepository{
		current: true, checkStarted: make(chan struct{}), checkRelease: make(chan struct{}),
	}
	runtime, err := NewControlQueryRuntime(repo, &queryTestPublisher{ready: true},
		func(context.Context, ControlSnapshotCandidate) { t.Error("handler invoked after runtime shutdown") }, nil)
	if err != nil {
		t.Fatal(err)
	}
	runtime.pending[query.RequestID] = pendingControlQuery{query: query, context: target, deadline: query.ExpiresAt}
	runtime.byEntity[target.EntityURN] = query.RequestID
	done := make(chan struct{})
	go func() {
		runtime.HandleSnapshot(context.Background(), snapshotForQuery(t, query))
		close(done)
	}()
	select {
	case <-repo.checkStarted:
	case <-time.After(time.Second):
		t.Fatal("database recheck did not start")
	}
	runtime.Stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel in-flight snapshot recheck")
	}
	if runtime.Pending() != 0 {
		t.Fatalf("Stop retained correlations: %d", runtime.Pending())
	}
}

func TestControlQueryRuntimeBoundsGlobalAndPerSensorOutstandingAndClose(t *testing.T) {
	targets := make([]digitaltwin.ControlQueryContext, 0, 40)
	for i := range 40 {
		targets = append(targets, testQueryContext(i))
	}
	repo := &queryTestRepository{targets: targets, current: true}
	publisher := &queryTestPublisher{ready: true}
	runtime, err := NewControlQueryRuntime(repo, publisher, func(context.Context, ControlSnapshotCandidate) {}, nil)
	if err != nil {
		t.Fatal(err)
	}
	runtime.scan()
	// The same Sensor cannot acquire a second in-flight request.
	repo.targets = []digitaltwin.ControlQueryContext{targets[0]}
	runtime.scan()
	if got := runtime.Pending(); got != 16 {
		t.Fatalf("duplicate Sensor acquired another request: %d", got)
	}
	repo.targets = append([]digitaltwin.ControlQueryContext(nil), targets[16:]...)
	runtime.scan()
	if got := runtime.Pending(); got != controlQueryOutstanding {
		t.Fatalf("global outstanding cap=%d want=%d", got, controlQueryOutstanding)
	}
	// A repeated due row does not allocate another request for the same Sensor.
	runtime.scan()
	if got := runtime.Pending(); got != controlQueryOutstanding {
		t.Fatalf("duplicate Sensor exceeded outstanding cap: %d", got)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runtime.Pending() != 0 {
		t.Fatal("close retained old-process correlations")
	}
}

func TestControlQueryRuntimeSkipsOfflineGatewayWithoutDurableReservation(t *testing.T) {
	repo := &queryTestRepository{targets: []digitaltwin.ControlQueryContext{testQueryContext(1)}}
	publisher := &queryTestPublisher{ready: false}
	metrics := &ControlQueryMetrics{}
	runtime, err := NewControlQueryRuntime(repo, publisher, func(context.Context, ControlSnapshotCandidate) {}, metrics)
	if err != nil {
		t.Fatal(err)
	}
	runtime.scan()
	if repo.calls != 0 || runtime.Pending() != 0 || metrics.Snapshot().Offline != 1 {
		t.Fatalf("offline scan reserved work: calls=%d pending=%d metrics=%+v", repo.calls, runtime.Pending(), metrics.Snapshot())
	}
	publisher.mu.Lock()
	publisher.ready = true
	publisher.mu.Unlock()
	runtime.scan() // Same periodic scan path retries once the existing transport is ready.
	if repo.calls != 1 || runtime.Pending() != 1 {
		t.Fatalf("online Gateway was not rediscovered: calls=%d pending=%d", repo.calls, runtime.Pending())
	}
	_ = runtime.Close(context.Background())
}

func TestControlQueryRuntimeDropsQueueFullAndFailedPublishCorrelation(t *testing.T) {
	target := testQueryContext(5)
	repo := &queryTestRepository{targets: []digitaltwin.ControlQueryContext{target}, current: true}
	publisher := &queryTestPublisher{ready: true}
	metrics := &ControlQueryMetrics{}
	runtime, err := NewControlQueryRuntime(repo, publisher, func(context.Context, ControlSnapshotCandidate) {}, metrics)
	if err != nil {
		t.Fatal(err)
	}
	for range controlQueryQueueCapacity {
		runtime.jobs <- controlQueryJob{}
	}
	runtime.scan()
	if runtime.Pending() != 0 || metrics.Snapshot().QueueFull != 1 {
		t.Fatalf("queue-full reservation leaked correlation: pending=%d metrics=%+v", runtime.Pending(), metrics.Snapshot())
	}

	publisher2 := &queryTestPublisher{ready: true, err: digitaltwin.ErrDependencyUnavailable}
	repo2 := &queryTestRepository{targets: []digitaltwin.ControlQueryContext{target}, current: true}
	runtime2, err := NewControlQueryRuntime(repo2, publisher2, func(context.Context, ControlSnapshotCandidate) {}, metrics)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime2.Start(); err != nil {
		t.Fatal(err)
	}
	waitForQuery(t, "failed publish did not clean correlation", func() bool {
		return metrics.Snapshot().Unavailable > 0 && runtime2.Pending() == 0
	})
	if err := runtime2.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(publisher2.err, digitaltwin.ErrDependencyUnavailable) {
		t.Fatal("publisher error fixture changed")
	}
}

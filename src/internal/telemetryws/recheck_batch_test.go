package telemetryws

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

type recordingBatchAuthorizer struct {
	mu        sync.Mutex
	calls     [][]gateway.SubscriptionTargetKey
	active    int
	maxActive int
	delay     time.Duration
}

func (a *recordingBatchAuthorizer) AuthorizeTargets(context.Context, uuid.UUID, []TargetKey) ([]AuthorizedTarget, error) {
	return nil, ErrBatchAuthorizationFailed
}

func (a *recordingBatchAuthorizer) AuthorizeTargetsBatch(ctx context.Context, keys []gateway.SubscriptionTargetKey) ([]TargetAuthorizationOutcome, error) {
	a.mu.Lock()
	a.calls = append(a.calls, append([]gateway.SubscriptionTargetKey(nil), keys...))
	a.active++
	if a.active > a.maxActive {
		a.maxActive = a.active
	}
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.active--
		a.mu.Unlock()
	}()
	if a.delay > 0 {
		select {
		case <-time.After(a.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	out := make([]TargetAuthorizationOutcome, len(keys))
	for i, key := range keys {
		out[i] = TargetAuthorizationOutcome{Key: key, Role: gateway.RoleViewer, Authorized: true}
	}
	return out, nil
}

func batchTestSession(t *testing.T, registry *SessionRegistry, id string, user uuid.UUID, targets []TargetKey, now time.Time) *SessionAccessState {
	t.Helper()
	s, err := NewSessionAccessState(id, user, now.Add(time.Hour), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 0 {
		gen, err := s.PrepareSubscribe(targets, now)
		if err != nil {
			t.Fatal(err)
		}
		authorized := make([]AuthorizedTarget, len(targets))
		for i, target := range targets {
			authorized[i] = AuthorizedTarget{Target: target, Role: gateway.RoleViewer}
		}
		if _, err := s.CommitSubscribe(gen, authorized, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := registry.Register(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRecheckWorkerBatchCoversAllAndDeduplicatesCompositeKeys(t *testing.T) {
	now := time.Now()
	reg := NewSessionRegistry(64)
	auth := &recordingBatchAuthorizer{}
	sharedUser, otherUser := uuid.New(), uuid.New()
	for i := 0; i < 64; i++ {
		user := sharedUser
		target := TargetKey{GatewayID: "gw-a", SensorID: "same"}
		switch i {
		case 63:
			user = otherUser
		case 62:
			target.GatewayID = "gw-b"
		}
		batchTestSession(t, reg, uuid.NewString(), user, []TargetKey{target}, now)
	}
	w := NewRecheckWorker(auth, reg, RecheckWorkerConfig{MaxSessions: 64, Concurrency: 2})
	r := w.Sweep(context.Background())
	if r.SessionsChecked != 64 || r.SessionsVerified != 64 {
		t.Fatalf("all sessions must be checked and verified: %+v", r)
	}
	if r.DistinctKeys != 3 || r.Queries != 1 || r.Chunks != 1 {
		t.Fatalf("expected three composite keys in one chunk/query: %+v", r)
	}
	if len(auth.calls) != 1 || len(auth.calls[0]) != 3 {
		t.Fatalf("expected one batch containing three distinct composite keys, calls=%v", auth.calls)
	}
	second := w.Sweep(context.Background())
	if second.Queries != 1 || len(auth.calls) != 2 {
		t.Fatalf("new sweep reused a prior permission decision: report=%+v calls=%d", second, len(auth.calls))
	}
}

func TestRecheckWorkerBatchChunksAtRepositoryLimitWithBoundedWorkers(t *testing.T) {
	now := time.Now()
	reg := NewSessionRegistry(300)
	auth := &recordingBatchAuthorizer{delay: 5 * time.Millisecond}
	for i := 0; i < 257; i++ {
		batchTestSession(t, reg, uuid.NewString(), uuid.New(), []TargetKey{{GatewayID: "gw", SensorID: uuid.NewString()}}, now)
	}
	w := NewRecheckWorker(auth, reg, RecheckWorkerConfig{MaxSessions: 300, Concurrency: 2})
	r := w.Sweep(context.Background())
	if r.SessionsChecked != 257 || r.DistinctKeys != 257 || r.Queries != 2 || r.Chunks != 2 {
		t.Fatalf("expected 257 sessions/keys in two bounded chunks: %+v", r)
	}
	if auth.maxActive > 2 || auth.maxActive < 1 {
		t.Fatalf("batch query concurrency exceeded configured workers: %d", auth.maxActive)
	}
	for _, call := range auth.calls {
		if len(call) > gateway.MaxSubscriptionTargetBatchSize {
			t.Fatalf("oversized authorization chunk: %d", len(call))
		}
	}
}

type blockedBatchAuthorizer struct {
	entered   chan struct{}
	returnNow chan struct{}
}

func (a blockedBatchAuthorizer) AuthorizeTargets(context.Context, uuid.UUID, []TargetKey) ([]AuthorizedTarget, error) {
	return nil, ErrBatchAuthorizationFailed
}

func (a blockedBatchAuthorizer) AuthorizeTargetsBatch(context.Context, []gateway.SubscriptionTargetKey) ([]TargetAuthorizationOutcome, error) {
	close(a.entered)
	<-a.returnNow // deliberately ignores cancellation to exercise late-result handling
	return []TargetAuthorizationOutcome{{Err: ErrTargetUnauthorized}}, nil
}

func TestRecheckBatchLateDenialDoesNotApplyToChangedGeneration(t *testing.T) {
	now := time.Now()
	reg := NewSessionRegistry(2)
	user := uuid.New()
	targetA, targetB := TargetKey{GatewayID: "gw", SensorID: "old"}, TargetKey{GatewayID: "gw", SensorID: "new"}
	s := batchTestSession(t, reg, "race-session", user, []TargetKey{targetA}, now)
	auth := blockedBatchAuthorizer{entered: make(chan struct{}), returnNow: make(chan struct{})}
	w := NewRecheckWorker(auth, reg, RecheckWorkerConfig{MaxSessions: 2, SweepBudget: 50 * time.Millisecond})
	done := make(chan SweepReport, 1)
	go func() { done <- w.Sweep(context.Background()) }()
	<-auth.entered
	_, _ = s.Unsubscribe([]TargetKey{targetA})
	gen, _ := s.PrepareSubscribe([]TargetKey{targetB}, now)
	_, _ = s.CommitSubscribe(gen, []AuthorizedTarget{{Target: targetB, Role: gateway.RoleViewer}}, now)
	select {
	case report := <-done:
		if report.SessionsRevoked != 0 || report.SessionsStale != 1 {
			t.Fatalf("changed generation should remain open/stale: %+v", report)
		}
	case <-time.After(time.Second):
		t.Fatal("sweep blocked on authorizer past its deadline")
	}
	close(auth.returnNow)
	if s.IsClosed() || !s.HasTarget(targetB) || s.HasTarget(targetA) {
		t.Fatal("late denial restored/revoked the wrong subscription generation")
	}
}

type neverCooperativeBatchAuthorizer struct {
	started   chan struct{}
	release   chan struct{}
	finished  chan struct{}
	mu        sync.Mutex
	active    int
	maxActive int
	calls     int
}

func (a *neverCooperativeBatchAuthorizer) AuthorizeTargets(context.Context, uuid.UUID, []TargetKey) ([]AuthorizedTarget, error) {
	return nil, ErrBatchAuthorizationFailed
}

func (a *neverCooperativeBatchAuthorizer) AuthorizeTargetsBatch(context.Context, []gateway.SubscriptionTargetKey) ([]TargetAuthorizationOutcome, error) {
	a.mu.Lock()
	a.active++
	a.calls++
	if a.active > a.maxActive {
		a.maxActive = a.active
	}
	if a.calls == 1 {
		close(a.started)
	}
	a.mu.Unlock()
	<-a.release
	a.mu.Lock()
	a.active--
	a.mu.Unlock()
	close(a.finished)
	return []TargetAuthorizationOutcome{{Err: ErrTargetUnauthorized}}, nil
}

func TestRecheckWorkerTimeoutsDoNotAccumulateCallsAcrossSweeps(t *testing.T) {
	now := time.Now()
	reg := NewSessionRegistry(8)
	auth := &neverCooperativeBatchAuthorizer{started: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	w := NewRecheckWorker(auth, reg, RecheckWorkerConfig{MaxSessions: 8, Concurrency: 1, SweepBudget: 25 * time.Millisecond})
	reports := make([]SweepReport, 0, 3)
	for i := 0; i < 3; i++ {
		batchTestSession(t, reg, uuid.NewString(), uuid.New(), []TargetKey{{GatewayID: "gw", SensorID: uuid.NewString()}}, now)
		report := w.Sweep(context.Background())
		if report.SessionsRevoked != 1 || report.Timeouts != 1 {
			t.Fatalf("sweep %d did not fail closed: %+v", i, report)
		}
		reports = append(reports, report)
		if i == 0 {
			<-auth.started
		}
	}
	auth.mu.Lock()
	calls, maxActive := auth.calls, auth.maxActive
	auth.mu.Unlock()
	if calls != 1 || maxActive > 1 {
		t.Fatalf("authorization calls accumulated across sweeps: calls=%d max_active=%d", calls, maxActive)
	}
	if reports[0].Queries != 1 || reports[1].Queries != 0 || reports[2].Queries != 0 {
		t.Fatalf("timeout-started call accounting incorrect: reports=%+v", reports)
	}
	close(auth.release)
	select {
	case <-auth.finished:
	case <-time.After(time.Second):
		t.Fatal("blocked authorization worker did not finish after release")
	}
}

type cooperativeBatchAuthorizer struct {
	started  chan struct{}
	finished chan struct{}
}

func (a *cooperativeBatchAuthorizer) AuthorizeTargets(context.Context, uuid.UUID, []TargetKey) ([]AuthorizedTarget, error) {
	return nil, ErrBatchAuthorizationFailed
}

func (a *cooperativeBatchAuthorizer) AuthorizeTargetsBatch(ctx context.Context, _ []gateway.SubscriptionTargetKey) ([]TargetAuthorizationOutcome, error) {
	close(a.started)
	<-ctx.Done()
	close(a.finished)
	return nil, ctx.Err()
}

func TestRecheckWorkerStopJoinsCooperativeAuthorizationWorker(t *testing.T) {
	now := time.Now()
	reg := NewSessionRegistry(2)
	batchTestSession(t, reg, "stop-join", uuid.New(), []TargetKey{{GatewayID: "gw", SensorID: "sensor"}}, now)
	auth := &cooperativeBatchAuthorizer{started: make(chan struct{}), finished: make(chan struct{})}
	w := NewRecheckWorker(auth, reg, RecheckWorkerConfig{MaxSessions: 2, Concurrency: 1, Interval: 5 * time.Millisecond, SweepBudget: time.Second})
	if err := w.StartContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-auth.started:
	case <-time.After(time.Second):
		t.Fatal("periodic sweep did not start authorization")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.StopContext(stopCtx); err != nil {
		t.Fatalf("StopContext failed to join cooperative worker: %v", err)
	}
	select {
	case <-auth.finished:
	default:
		t.Fatal("StopContext returned before cooperative authorization worker exited")
	}
}

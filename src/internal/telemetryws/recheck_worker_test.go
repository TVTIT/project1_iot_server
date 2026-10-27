package telemetryws

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

type mockAuthorizerRepo struct {
	mu         sync.Mutex
	roles      map[string]gateway.Role
	errs       map[string]error
	delay      time.Duration
	queryCount int32
}

func newMockAuthorizerRepo() *mockAuthorizerRepo {
	return &mockAuthorizerRepo{
		roles: make(map[string]gateway.Role),
		errs:  make(map[string]error),
	}
}

func (m *mockAuthorizerRepo) key(userID uuid.UUID, gatewayID, sensorID string) string {
	return userID.String() + ":" + gatewayID + ":" + sensorID
}

func (m *mockAuthorizerRepo) setRole(userID uuid.UUID, gatewayID, sensorID string, role gateway.Role) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.roles[m.key(userID, gatewayID, sensorID)] = role
}

func (m *mockAuthorizerRepo) setErr(userID uuid.UUID, gatewayID, sensorID string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errs[m.key(userID, gatewayID, sensorID)] = err
}

func (m *mockAuthorizerRepo) AuthorizeSubscriptionTarget(ctx context.Context, userID uuid.UUID, gatewayID string, sensorID string) (gateway.Role, error) {
	atomic.AddInt32(&m.queryCount, 1)

	if m.delay > 0 {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(m.delay):
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	k := m.key(userID, gatewayID, sensorID)
	if err, ok := m.errs[k]; ok && err != nil {
		return "", err
	}
	if role, ok := m.roles[k]; ok {
		return role, nil
	}
	return "", gateway.ErrNotFound
}

func TestRecheckWorker_MemberRoles_And_Denial(t *testing.T) {
	repo := newMockAuthorizerRepo()
	authorizer, err := NewTargetAuthorizer(repo, 2*time.Second)
	if err != nil {
		t.Fatalf("NewTargetAuthorizer failed: %v", err)
	}

	reg := NewSessionRegistry(MaxSessions)
	baseTime := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	expiry := baseTime.Add(10 * time.Minute)

	userViewer := uuid.New()
	userOperator := uuid.New()
	userOwner := uuid.New()
	userRevoked := uuid.New()

	targetGW := TargetKey{GatewayID: "gw-1"}
	targetSensor := TargetKey{GatewayID: "gw-1", SensorID: "sensor-1"}

	repo.setRole(userViewer, "gw-1", "", gateway.RoleViewer)
	repo.setRole(userOperator, "gw-1", "sensor-1", gateway.RoleOperator)
	repo.setRole(userOwner, "gw-1", "", gateway.RoleOwner)
	// userRevoked initially had role, but now revoked (no entry in repo -> ErrNotFound)

	var revokedSessions sync.Map
	revocationHandler := SessionRevocationFunc(func(sessionID string, reason RevocationReason) {
		revokedSessions.Store(sessionID, reason)
	})

	// Setup session 1 (Viewer)
	s1, _ := NewSessionAccessState("sess-viewer", userViewer, expiry, revocationHandler)
	gen1, _ := s1.PrepareSubscribe([]TargetKey{targetGW}, baseTime)
	_, _ = s1.CommitSubscribe(gen1, []AuthorizedTarget{{Target: targetGW, Role: gateway.RoleViewer}}, baseTime)
	_ = reg.Register(s1)

	// Setup session 2 (Operator)
	s2, _ := NewSessionAccessState("sess-operator", userOperator, expiry, revocationHandler)
	gen2, _ := s2.PrepareSubscribe([]TargetKey{targetSensor}, baseTime)
	_, _ = s2.CommitSubscribe(gen2, []AuthorizedTarget{{Target: targetSensor, Role: gateway.RoleOperator}}, baseTime)
	_ = reg.Register(s2)

	// Setup session 3 (Owner)
	s3, _ := NewSessionAccessState("sess-owner", userOwner, expiry, revocationHandler)
	gen3, _ := s3.PrepareSubscribe([]TargetKey{targetGW}, baseTime)
	_, _ = s3.CommitSubscribe(gen3, []AuthorizedTarget{{Target: targetGW, Role: gateway.RoleOwner}}, baseTime)
	_ = reg.Register(s3)

	// Setup session 4 (Revoked membership)
	s4, _ := NewSessionAccessState("sess-revoked", userRevoked, expiry, revocationHandler)
	gen4, _ := s4.PrepareSubscribe([]TargetKey{targetGW}, baseTime)
	_, _ = s4.CommitSubscribe(gen4, []AuthorizedTarget{{Target: targetGW, Role: gateway.RoleViewer}}, baseTime)
	_ = reg.Register(s4)

	worker := NewRecheckWorker(authorizer, reg, RecheckWorkerConfig{
		Interval:    5 * time.Second,
		SweepBudget: 2 * time.Second,
		Concurrency: 2,
		MaxSessions: MaxSessions,
		Now:         func() time.Time { return baseTime },
	})

	report := worker.Sweep(context.Background())

	if report.SessionsChecked != 4 {
		t.Fatalf("expected 4 sessions checked, got %d", report.SessionsChecked)
	}
	if report.SessionsVerified != 3 {
		t.Fatalf("expected 3 sessions verified (viewer, operator, owner), got %d", report.SessionsVerified)
	}
	if report.SessionsRevoked != 1 {
		t.Fatalf("expected 1 session revoked, got %d", report.SessionsRevoked)
	}

	// Verify s4 was closed
	if !s4.IsClosed() {
		t.Fatalf("expected s4 (revoked) to be closed")
	}
	if s4.TargetCount() != 0 {
		t.Fatalf("expected s4 targets to be cleared, got %d", s4.TargetCount())
	}
	val, ok := revokedSessions.Load("sess-revoked")
	if !ok || val.(RevocationReason) != RevocationReasonDenied {
		t.Fatalf("expected sess-revoked to have reason %v, got %v", RevocationReasonDenied, val)
	}

	// Active sessions should remain open
	if s1.IsClosed() || s2.IsClosed() || s3.IsClosed() {
		t.Errorf("authorized sessions s1, s2, s3 must remain open")
	}
}

func TestRecheckWorker_DatabaseError_FailsClosed(t *testing.T) {
	repo := newMockAuthorizerRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)

	reg := NewSessionRegistry(MaxSessions)
	baseTime := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	expiry := baseTime.Add(10 * time.Minute)
	user := uuid.New()
	target := TargetKey{GatewayID: "gw-db-err"}

	// Simulate DB connection failure
	repo.setErr(user, "gw-db-err", "", errors.New("connection reset by peer"))

	var revokedReason RevocationReason
	revocationHandler := SessionRevocationFunc(func(_ string, reason RevocationReason) {
		revokedReason = reason
	})

	s, _ := NewSessionAccessState("sess-db-err", user, expiry, revocationHandler)
	gen, _ := s.PrepareSubscribe([]TargetKey{target}, baseTime)
	_, _ = s.CommitSubscribe(gen, []AuthorizedTarget{{Target: target, Role: gateway.RoleViewer}}, baseTime)
	_ = reg.Register(s)

	worker := NewRecheckWorker(authorizer, reg, RecheckWorkerConfig{
		Interval:    5 * time.Second,
		SweepBudget: 2 * time.Second,
		Concurrency: 2,
		MaxSessions: MaxSessions,
		Now:         func() time.Time { return baseTime },
	})

	report := worker.Sweep(context.Background())

	if report.SessionsRevoked != 1 {
		t.Fatalf("expected 1 session revoked on DB error, got %d", report.SessionsRevoked)
	}
	if !s.IsClosed() {
		t.Fatalf("session should be closed on DB error")
	}
	if revokedReason != RevocationReasonDBError {
		t.Fatalf("expected RevocationReasonDBError, got %v", revokedReason)
	}
}

func TestRecheckWorker_SweepBudget_Timeout_FailClosed(t *testing.T) {
	repo := newMockAuthorizerRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)

	reg := NewSessionRegistry(MaxSessions)
	baseTime := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	expiry := baseTime.Add(10 * time.Minute)

	// Set repo delay to 200ms, but sweep budget is 50ms with 4 sessions and concurrency 1
	repo.delay = 200 * time.Millisecond

	var revokedCount int32
	revocationHandler := SessionRevocationFunc(func(_ string, _ RevocationReason) {
		atomic.AddInt32(&revokedCount, 1)
	})

	// Create 4 sessions
	sessions := make([]*SessionAccessState, 4)
	for i := 0; i < 4; i++ {
		u := uuid.New()
		target := TargetKey{GatewayID: "gw-slow"}
		repo.setRole(u, "gw-slow", "", gateway.RoleViewer)

		s, _ := NewSessionAccessState(uuid.New().String(), u, expiry, revocationHandler)
		gen, _ := s.PrepareSubscribe([]TargetKey{target}, baseTime)
		_, _ = s.CommitSubscribe(gen, []AuthorizedTarget{{Target: target, Role: gateway.RoleViewer}}, baseTime)
		_ = reg.Register(s)
		sessions[i] = s
	}

	worker := NewRecheckWorker(authorizer, reg, RecheckWorkerConfig{
		Interval:    5 * time.Second,
		SweepBudget: 50 * time.Millisecond, // very short sweep budget to force timeout!
		Concurrency: 1,                     // sequential execution
		MaxSessions: MaxSessions,
		Now:         func() time.Time { return baseTime },
	})

	report := worker.Sweep(context.Background())

	// Because of 50ms sweep budget and 200ms DB delay, the sweep budget expires during check.
	// All unverified sessions MUST fail closed!
	if report.SessionsRevoked != 4 {
		t.Fatalf("expected all 4 sessions to be revoked upon sweep timeout fail-closed, got %d", report.SessionsRevoked)
	}

	for i, s := range sessions {
		if !s.IsClosed() {
			t.Errorf("session %d should be closed due to sweep timeout fail-closed", i)
		}
	}
	if atomic.LoadInt32(&revokedCount) != 4 {
		t.Fatalf("expected 4 revocation callbacks, got %d", atomic.LoadInt32(&revokedCount))
	}
}

func TestRecheckWorker_CallerCancellation(t *testing.T) {
	repo := newMockAuthorizerRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)

	reg := NewSessionRegistry(MaxSessions)
	baseTime := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	expiry := baseTime.Add(10 * time.Minute)
	user := uuid.New()
	target := TargetKey{GatewayID: "gw-cancel"}
	repo.setRole(user, "gw-cancel", "", gateway.RoleViewer)
	repo.delay = 100 * time.Millisecond

	s, _ := NewSessionAccessState("sess-cancel", user, expiry, nil)
	gen, _ := s.PrepareSubscribe([]TargetKey{target}, baseTime)
	_, _ = s.CommitSubscribe(gen, []AuthorizedTarget{{Target: target, Role: gateway.RoleViewer}}, baseTime)
	_ = reg.Register(s)

	worker := NewRecheckWorker(authorizer, reg, RecheckWorkerConfig{
		Interval:    5 * time.Second,
		SweepBudget: 2 * time.Second,
		Concurrency: 1,
		MaxSessions: MaxSessions,
		Now:         func() time.Time { return baseTime },
	})

	// Cancel context before or immediately upon sweep
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled immediately

	report := worker.Sweep(ctx)
	if report.SessionsRevoked != 1 {
		t.Fatalf("expected session to be revoked fail-closed upon context cancellation, got %d", report.SessionsRevoked)
	}
	if !s.IsClosed() {
		t.Fatalf("session should be closed upon context cancellation")
	}
}

func TestRecheckWorker_StaleOutcome_GenerationChanged(t *testing.T) {
	repo := newMockAuthorizerRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)

	reg := NewSessionRegistry(MaxSessions)
	baseTime := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	expiry := baseTime.Add(10 * time.Minute)
	user := uuid.New()
	targetA := TargetKey{GatewayID: "gw-1", SensorID: "sensor-a"}
	targetB := TargetKey{GatewayID: "gw-1", SensorID: "sensor-b"}

	repo.setRole(user, "gw-1", "sensor-a", gateway.RoleViewer)
	repo.setRole(user, "gw-1", "sensor-b", gateway.RoleViewer)
	repo.delay = 20 * time.Millisecond

	s, _ := NewSessionAccessState("sess-stale", user, expiry, nil)
	genA, _ := s.PrepareSubscribe([]TargetKey{targetA}, baseTime)
	_, _ = s.CommitSubscribe(genA, []AuthorizedTarget{{Target: targetA, Role: gateway.RoleViewer}}, baseTime)
	_ = reg.Register(s)

	worker := NewRecheckWorker(authorizer, reg, RecheckWorkerConfig{
		Interval:    5 * time.Second,
		SweepBudget: 2 * time.Second,
		Concurrency: 1,
		MaxSessions: MaxSessions,
		Now:         func() time.Time { return baseTime },
	})

	// Run sweep concurrently while user modifies subscription (changes generation)
	var sweepReport SweepReport
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		sweepReport = worker.Sweep(context.Background())
	}()

	go func() {
		defer wg.Done()
		// Small sleep to ensure sweep captured snapshot of targetA
		time.Sleep(5 * time.Millisecond)
		// Change generation: unsubscribe targetA and subscribe targetB
		_, _ = s.Unsubscribe([]TargetKey{targetA})
		genB, _ := s.PrepareSubscribe([]TargetKey{targetB}, baseTime)
		_, _ = s.CommitSubscribe(genB, []AuthorizedTarget{{Target: targetB, Role: gateway.RoleViewer}}, baseTime)
	}()

	wg.Wait()

	if sweepReport.SessionsChecked != 1 {
		t.Fatalf("expected 1 session checked in sweep, got %d", sweepReport.SessionsChecked)
	}

	// Session must remain open with targetB!
	if s.IsClosed() {
		t.Fatalf("session should NOT be closed when generation changed during valid recheck")
	}
	if !s.HasTarget(targetB) {
		t.Fatalf("session should have targetB")
	}
	if s.HasTarget(targetA) {
		t.Fatalf("recheck must NOT restore or resurrect targetA")
	}
}

func TestRecheckWorker_AbsoluteJWTExpiry(t *testing.T) {
	repo := newMockAuthorizerRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)

	reg := NewSessionRegistry(MaxSessions)
	baseTime := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	expiry := baseTime.Add(1 * time.Minute)

	user := uuid.New()
	target := TargetKey{GatewayID: "gw-1"}
	repo.setRole(user, "gw-1", "", gateway.RoleViewer)

	var revokedReason RevocationReason
	revocationHandler := SessionRevocationFunc(func(_ string, reason RevocationReason) {
		revokedReason = reason
	})

	s, _ := NewSessionAccessState("sess-jwt-exp", user, expiry, revocationHandler)
	gen, _ := s.PrepareSubscribe([]TargetKey{target}, baseTime)
	_, _ = s.CommitSubscribe(gen, []AuthorizedTarget{{Target: target, Role: gateway.RoleViewer}}, baseTime)
	_ = reg.Register(s)

	// Simulate sweep occurring AFTER expiry
	sweepTime := expiry.Add(1 * time.Second)

	worker := NewRecheckWorker(authorizer, reg, RecheckWorkerConfig{
		Interval:    5 * time.Second,
		SweepBudget: 2 * time.Second,
		Concurrency: 1,
		MaxSessions: MaxSessions,
		Now:         func() time.Time { return sweepTime },
	})

	report := worker.Sweep(context.Background())

	if report.SessionsExpired != 1 {
		t.Fatalf("expected 1 session expired, got %d", report.SessionsExpired)
	}
	if !s.IsClosed() {
		t.Fatalf("session should be closed upon JWT expiry")
	}
	if revokedReason != RevocationReasonExpired {
		t.Fatalf("expected RevocationReasonExpired, got %v", revokedReason)
	}
	// Verify authorizer DB query was NOT even called because token was already expired
	if atomic.LoadInt32(&repo.queryCount) != 0 {
		t.Fatalf("DB query should not be invoked for expired session, got %d queries", repo.queryCount)
	}
}

func TestRecheckWorker_LifecycleStartStop(t *testing.T) {
	repo := newMockAuthorizerRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)
	reg := NewSessionRegistry(MaxSessions)

	worker := NewRecheckWorker(authorizer, reg, RecheckWorkerConfig{
		Interval:    10 * time.Millisecond,
		SweepBudget: 2 * time.Second,
		Concurrency: 1,
		MaxSessions: MaxSessions,
	})

	if err := worker.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := worker.Start(); err == nil {
		t.Fatalf("expected error on duplicate Start")
	}

	time.Sleep(30 * time.Millisecond)

	if err := worker.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if err := worker.Stop(); err != nil {
		t.Fatalf("repeated Stop should be safe: %v", err)
	}
}

func TestRecheckWorker_PartialTargetDenialClosesSession(t *testing.T) {
	repo := newMockAuthorizerRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)

	reg := NewSessionRegistry(MaxSessions)
	baseTime := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	expiry := baseTime.Add(10 * time.Minute)
	user := uuid.New()

	targetGood := TargetKey{GatewayID: "gw-1", SensorID: "sensor-good"}
	targetRevoked := TargetKey{GatewayID: "gw-1", SensorID: "sensor-revoked"}

	// Good target has role, but revoked target has no role (ErrNotFound)
	repo.setRole(user, "gw-1", "sensor-good", gateway.RoleViewer)

	var revokedReason RevocationReason
	revocationHandler := SessionRevocationFunc(func(_ string, reason RevocationReason) {
		revokedReason = reason
	})

	s, _ := NewSessionAccessState("sess-partial-denial", user, expiry, revocationHandler)
	gen, _ := s.PrepareSubscribe([]TargetKey{targetGood, targetRevoked}, baseTime)
	// Seed both targets
	_, _ = s.CommitSubscribe(gen, []AuthorizedTarget{
		{Target: targetGood, Role: gateway.RoleViewer},
		{Target: targetRevoked, Role: gateway.RoleViewer},
	}, baseTime)
	_ = reg.Register(s)

	worker := NewRecheckWorker(authorizer, reg, RecheckWorkerConfig{
		Interval:    5 * time.Second,
		SweepBudget: 2 * time.Second,
		Concurrency: 1,
		MaxSessions: MaxSessions,
		Now:         func() time.Time { return baseTime },
	})

	report := worker.Sweep(context.Background())

	if report.SessionsRevoked != 1 {
		t.Fatalf("expected session to be revoked on partial target denial, got %d", report.SessionsRevoked)
	}
	if !s.IsClosed() {
		t.Fatalf("session should be closed when any target is denied")
	}
	if s.TargetCount() != 0 {
		t.Fatalf("expected all targets cleared, got %d", s.TargetCount())
	}
	if revokedReason != RevocationReasonDenied {
		t.Fatalf("expected RevocationReasonDenied, got %v", revokedReason)
	}
}

func TestRecheckWorker_IdleSessionZeroTargets(t *testing.T) {
	repo := newMockAuthorizerRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)

	reg := NewSessionRegistry(MaxSessions)
	baseTime := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	expiry := baseTime.Add(10 * time.Minute)
	user := uuid.New()

	// Session with zero targets
	s, _ := NewSessionAccessState("sess-idle", user, expiry, nil)
	_ = reg.Register(s)

	worker := NewRecheckWorker(authorizer, reg, RecheckWorkerConfig{
		Interval:    5 * time.Second,
		SweepBudget: 2 * time.Second,
		Concurrency: 1,
		MaxSessions: MaxSessions,
		Now:         func() time.Time { return baseTime },
	})

	report := worker.Sweep(context.Background())

	if report.SessionsChecked != 1 {
		t.Fatalf("expected 1 session checked, got %d", report.SessionsChecked)
	}
	if report.SessionsVerified != 1 {
		t.Fatalf("expected 1 session verified (idle), got %d", report.SessionsVerified)
	}
	if report.SessionsRevoked != 0 {
		t.Fatalf("idle session should not be revoked")
	}
	if atomic.LoadInt32(&repo.queryCount) != 0 {
		t.Fatalf("idle session must not issue DB queries, got %d", repo.queryCount)
	}
}

func TestRecheckWorker_ProviderOverloadCapsAtMaxSessions(t *testing.T) {
	repo := newMockAuthorizerRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)

	baseTime := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	expiry := baseTime.Add(10 * time.Minute)

	// Create custom provider returning 50 sessions (exceeding MaxSessions=32)
	sessions50 := make([]*SessionAccessState, 50)
	for i := 0; i < 50; i++ {
		s, _ := NewSessionAccessState(uuid.New().String(), uuid.New(), expiry, nil)
		sessions50[i] = s
	}

	mockProvider := &mockSessionProvider{sessions: sessions50}

	worker := NewRecheckWorker(authorizer, mockProvider, RecheckWorkerConfig{
		Interval:    5 * time.Second,
		SweepBudget: 2 * time.Second,
		Concurrency: 2,
		MaxSessions: 32,
		Now:         func() time.Time { return baseTime },
	})

	report := worker.Sweep(context.Background())

	if report.SessionsChecked != 32 {
		t.Fatalf("expected sweep to cap at MaxSessions 32, checked %d", report.SessionsChecked)
	}
}

type mockSessionProvider struct {
	sessions []*SessionAccessState
}

func (m *mockSessionProvider) GetActiveSessions(_ int) []*SessionAccessState {
	// Provider deliberately returns all sessions to test worker capping
	return m.sessions
}

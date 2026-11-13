package telemetryws

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

func TestSessionAccessState_Constructor(t *testing.T) {
	validUser := uuid.New()
	validExpiry := time.Now().Add(10 * time.Minute)

	t.Run("valid parameters", func(t *testing.T) {
		s, err := NewSessionAccessState("sess-1", validUser, validExpiry, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s.SessionID() != "sess-1" {
			t.Errorf("expected session ID sess-1, got %s", s.SessionID())
		}
		if s.UserID() != validUser {
			t.Errorf("expected user ID %v, got %v", validUser, s.UserID())
		}
		if !s.ExpiresAt().Equal(validExpiry) {
			t.Errorf("expected expiry %v, got %v", validExpiry, s.ExpiresAt())
		}
		if s.Generation() != 1 {
			t.Errorf("expected initial generation 1, got %d", s.Generation())
		}
		if s.IsClosed() {
			t.Errorf("expected new session to not be closed")
		}
		if s.TargetCount() != 0 {
			t.Errorf("expected initial target count 0, got %d", s.TargetCount())
		}
	})

	t.Run("empty session ID rejected", func(t *testing.T) {
		_, err := NewSessionAccessState("", validUser, validExpiry, nil)
		if err == nil {
			t.Fatalf("expected error on empty session ID")
		}
	})

	t.Run("nil user ID rejected", func(t *testing.T) {
		_, err := NewSessionAccessState("sess-1", uuid.Nil, validExpiry, nil)
		if !errors.Is(err, ErrInvalidUserID) {
			t.Fatalf("expected ErrInvalidUserID, got %v", err)
		}
	})

	t.Run("zero expiry rejected", func(t *testing.T) {
		_, err := NewSessionAccessState("sess-1", validUser, time.Time{}, nil)
		if err == nil {
			t.Fatalf("expected error on zero expiry")
		}
	})
}

func TestSessionAccessState_TargetBound_Max16(t *testing.T) {
	userID := uuid.New()
	expiry := time.Now().Add(10 * time.Minute)
	now := time.Now()

	s, err := NewSessionAccessState("sess-bound", userID, expiry, nil)
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}

	// Prepare 16 targets
	targets16 := make([]TargetKey, 16)
	authorized16 := make([]AuthorizedTarget, 16)
	for i := 0; i < 16; i++ {
		targets16[i] = TargetKey{
			GatewayID: "gw-1",
			SensorID:  uuid.New().String()[:8],
		}
		authorized16[i] = AuthorizedTarget{
			Target: targets16[i],
			Role:   gateway.RoleViewer,
		}
	}

	gen, err := s.PrepareSubscribe(targets16, now)
	if err != nil {
		t.Fatalf("PrepareSubscribe 16 targets failed: %v", err)
	}
	installed, err := s.CommitSubscribe(gen, authorized16, now)
	if err != nil {
		t.Fatalf("CommitSubscribe 16 targets failed: %v", err)
	}
	if len(installed) != 16 {
		t.Fatalf("expected 16 installed targets, got %d", len(installed))
	}
	if s.TargetCount() != 16 {
		t.Fatalf("expected TargetCount 16, got %d", s.TargetCount())
	}

	// 17th target should be rejected
	target17 := []TargetKey{{GatewayID: "gw-1", SensorID: "sensor-17"}}
	_, err = s.PrepareSubscribe(target17, now)
	if !errors.Is(err, ErrTargetLimitExceeded) {
		t.Fatalf("expected ErrTargetLimitExceeded, got %v", err)
	}

	// Subscribing to an already subscribed target does not increase count
	existingTarget := []TargetKey{targets16[0]}
	genExisting, err := s.PrepareSubscribe(existingTarget, now)
	if err != nil {
		t.Fatalf("PrepareSubscribe for existing target should succeed: %v", err)
	}
	installedExisting, err := s.CommitSubscribe(genExisting, []AuthorizedTarget{{Target: targets16[0], Role: gateway.RoleViewer}}, now)
	if err != nil {
		t.Fatalf("CommitSubscribe for existing target failed: %v", err)
	}
	if len(installedExisting) != 0 {
		t.Errorf("expected 0 new targets installed for existing target, got %d", len(installedExisting))
	}
	if s.TargetCount() != 16 {
		t.Errorf("target count should remain 16, got %d", s.TargetCount())
	}
}

func TestSessionAccessState_AbsoluteJWTExpiry(t *testing.T) {
	userID := uuid.New()
	baseTime := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	expiresAt := baseTime.Add(60 * time.Second) // exact exp

	var revokedReason RevocationReason
	revocationHandler := SessionRevocationFunc(func(_ string, reason RevocationReason) {
		revokedReason = reason
	})

	s, err := NewSessionAccessState("sess-expiry", userID, expiresAt, revocationHandler)
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}

	// 1s before expiry -> not expired
	beforeExp := expiresAt.Add(-1 * time.Second)
	if s.IsExpired(beforeExp) {
		t.Errorf("expected not expired before expiry time")
	}

	// At exact expiry -> expired (no skew added!)
	if !s.IsExpired(expiresAt) {
		t.Errorf("expected expired at exact expiry time without skew")
	}

	// 1s after expiry -> expired
	afterExp := expiresAt.Add(1 * time.Second)
	if !s.IsExpired(afterExp) {
		t.Errorf("expected expired after expiry time")
	}

	// PrepareSubscribe after expiry must fail and close the session
	target := []TargetKey{{GatewayID: "gw-1", SensorID: "s-1"}}
	_, err = s.PrepareSubscribe(target, afterExp)
	if !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("expected ErrSessionExpired, got %v", err)
	}
	if !s.IsClosed() {
		t.Errorf("expected session to be closed upon expiry")
	}
	if revokedReason != RevocationReasonExpired {
		t.Errorf("expected RevocationReasonExpired, got %v", revokedReason)
	}

	// CommitSubscribe after expiry must also fail
	s2, _ := NewSessionAccessState("sess-exp-2", userID, expiresAt, nil)
	gen2, err := s2.PrepareSubscribe(target, beforeExp)
	if err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	_, err = s2.CommitSubscribe(gen2, []AuthorizedTarget{{Target: target[0], Role: gateway.RoleViewer}}, afterExp)
	if !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("expected ErrSessionExpired on commit after expiry, got %v", err)
	}
	if !s2.IsClosed() {
		t.Errorf("expected s2 to be closed on commit after expiry")
	}
}

func TestSessionAccessState_GenerationSafety_Unsubscribe(t *testing.T) {
	userID := uuid.New()
	expiry := time.Now().Add(10 * time.Minute)
	now := time.Now()

	s, err := NewSessionAccessState("sess-gen-unsub", userID, expiry, nil)
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}

	targetA := TargetKey{GatewayID: "gw-1", SensorID: "sensor-a"}
	targetB := TargetKey{GatewayID: "gw-1", SensorID: "sensor-b"}

	// 1. Subscribe to target A
	genA, err := s.PrepareSubscribe([]TargetKey{targetA}, now)
	if err != nil {
		t.Fatalf("prepare A failed: %v", err)
	}
	_, err = s.CommitSubscribe(genA, []AuthorizedTarget{{Target: targetA, Role: gateway.RoleViewer}}, now)
	if err != nil {
		t.Fatalf("commit A failed: %v", err)
	}
	if !s.HasTarget(targetA) {
		t.Fatalf("target A should be active")
	}

	// 2. Start subscribing to target B: capture generation
	genB, err := s.PrepareSubscribe([]TargetKey{targetB}, now)
	if err != nil {
		t.Fatalf("prepare B failed: %v", err)
	}

	// 3. User unsubscribes from target A while DB query for target B is in flight
	removed, err := s.Unsubscribe([]TargetKey{targetA})
	if err != nil {
		t.Fatalf("unsubscribe A failed: %v", err)
	}
	if len(removed) != 1 || removed[0] != targetA {
		t.Fatalf("expected target A removed, got %v", removed)
	}
	if s.HasTarget(targetA) {
		t.Fatalf("target A should no longer be active")
	}

	// 4. Now attempt to commit target B using the stale generation genB
	_, err = s.CommitSubscribe(genB, []AuthorizedTarget{{Target: targetB, Role: gateway.RoleViewer}}, now)
	if !errors.Is(err, ErrStaleAuthorization) {
		t.Fatalf("expected ErrStaleAuthorization due to generation bump on unsubscribe, got %v", err)
	}

	// Verify target B was NOT installed and target A was NOT resurrected
	if s.HasTarget(targetB) {
		t.Errorf("target B must NOT have been installed with stale generation")
	}
	if s.HasTarget(targetA) {
		t.Errorf("target A must NOT have been resurrected")
	}
	if s.TargetCount() != 0 {
		t.Errorf("expected 0 active targets, got %d", s.TargetCount())
	}
}

func TestSessionAccessState_GenerationSafety_Close(t *testing.T) {
	userID := uuid.New()
	expiry := time.Now().Add(10 * time.Minute)
	now := time.Now()

	s, err := NewSessionAccessState("sess-gen-close", userID, expiry, nil)
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}

	target := TargetKey{GatewayID: "gw-1", SensorID: "sensor-c"}

	// Capture generation for subscription
	gen, err := s.PrepareSubscribe([]TargetKey{target}, now)
	if err != nil {
		t.Fatalf("prepare failed: %v", err)
	}

	// Close session while DB query is in flight
	s.Close(RevocationReasonClientClose)
	if !s.IsClosed() {
		t.Fatalf("session should be closed")
	}

	// Attempt to commit authorization after close
	_, err = s.CommitSubscribe(gen, []AuthorizedTarget{{Target: target, Role: gateway.RoleViewer}}, now)
	if !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("expected ErrSessionClosed, got %v", err)
	}

	// Verify target is not installed and session remains closed
	if s.HasTarget(target) {
		t.Errorf("target must not be installed in closed session")
	}
	if !s.IsClosed() {
		t.Errorf("session must remain closed")
	}
}

func TestSessionAccessState_ResubscribeMustRecheck(t *testing.T) {
	userID := uuid.New()
	expiry := time.Now().Add(10 * time.Minute)
	now := time.Now()

	s, err := NewSessionAccessState("sess-resub", userID, expiry, nil)
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}

	target := TargetKey{GatewayID: "gw-1", SensorID: "sensor-x"}

	// Initial subscribe
	gen1, err := s.PrepareSubscribe([]TargetKey{target}, now)
	if err != nil {
		t.Fatalf("prepare 1 failed: %v", err)
	}
	_, err = s.CommitSubscribe(gen1, []AuthorizedTarget{{Target: target, Role: gateway.RoleViewer}}, now)
	if err != nil {
		t.Fatalf("commit 1 failed: %v", err)
	}

	// Unsubscribe
	_, err = s.Unsubscribe([]TargetKey{target})
	if err != nil {
		t.Fatalf("unsub failed: %v", err)
	}
	if s.HasTarget(target) {
		t.Fatalf("target should not be present after unsubscribe")
	}

	// Re-subscribing with stale gen1 must fail
	_, err = s.CommitSubscribe(gen1, []AuthorizedTarget{{Target: target, Role: gateway.RoleViewer}}, now)
	if !errors.Is(err, ErrStaleAuthorization) {
		t.Fatalf("expected ErrStaleAuthorization when reusing old gen, got %v", err)
	}

	// Re-subscribing requires a NEW PrepareSubscribe -> fresh generation
	gen2, err := s.PrepareSubscribe([]TargetKey{target}, now)
	if err != nil {
		t.Fatalf("prepare 2 failed: %v", err)
	}
	if gen2 <= gen1 {
		t.Fatalf("expected gen2 (%d) > gen1 (%d)", gen2, gen1)
	}
}

func TestSessionAccessState_CallbackInvokedOnce(t *testing.T) {
	userID := uuid.New()
	expiry := time.Now().Add(10 * time.Minute)

	var callCount int32
	var lastReason RevocationReason
	handler := SessionRevocationFunc(func(_ string, reason RevocationReason) {
		atomic.AddInt32(&callCount, 1)
		lastReason = reason
	})

	s, err := NewSessionAccessState("sess-callback-once", userID, expiry, handler)
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}

	// Multiple calls to Close concurrently
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			reason := RevocationReasonClientClose
			if idx%2 == 0 {
				reason = RevocationReasonDenied
			}
			s.Close(reason)
		}(i)
	}
	wg.Wait()

	if atomic.LoadInt32(&callCount) != 1 {
		t.Fatalf("expected callback to be called exactly once, called %d times", callCount)
	}
	if lastReason == "" {
		t.Fatalf("expected a valid reason to be recorded")
	}
}

func TestSessionAccessState_ConcurrentAccess(t *testing.T) {
	userID := uuid.New()
	expiry := time.Now().Add(10 * time.Minute)
	now := time.Now()

	s, err := NewSessionAccessState("sess-concur", userID, expiry, nil)
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}

	target := TargetKey{GatewayID: "gw-concur", SensorID: "sensor-c"}
	gen, err := s.PrepareSubscribe([]TargetKey{target}, now)
	if err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	_, err = s.CommitSubscribe(gen, []AuthorizedTarget{{Target: target, Role: gateway.RoleViewer}}, now)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(_ int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = s.Snapshot()
				_ = s.Targets()
				_ = s.HasTarget(target)
				_ = s.IsExpired(now)
				_ = s.TargetCount()
				_ = s.Generation()
				_ = s.IsClosed()
			}
		}(i)
	}
	wg.Wait()
}

func TestSessionRegistry_Bounds(t *testing.T) {

	reg := NewSessionRegistry(MaxSessions)
	userID := uuid.New()
	expiry := time.Now().Add(10 * time.Minute)

	// Register 32 sessions
	for i := 0; i < MaxSessions; i++ {
		s, err := NewSessionAccessState(uuid.New().String(), userID, expiry, nil)
		if err != nil {
			t.Fatalf("session create failed: %v", err)
		}
		if err := reg.Register(s); err != nil {
			t.Fatalf("register failed at %d: %v", i, err)
		}
	}
	if reg.Count() != MaxSessions {
		t.Fatalf("expected %d sessions, got %d", MaxSessions, reg.Count())
	}

	// 33rd registration must fail with ErrSessionLimitExceeded
	s33, err := NewSessionAccessState("sess-33", userID, expiry, nil)
	if err != nil {
		t.Fatalf("session create failed: %v", err)
	}
	if err := reg.Register(s33); !errors.Is(err, ErrSessionLimitExceeded) {
		t.Fatalf("expected ErrSessionLimitExceeded, got %v", err)
	}

	// Unregister one session allows another
	active := reg.GetActiveSessions(1)
	if len(active) == 0 {
		t.Fatalf("expected at least 1 active session")
	}
	reg.Unregister(active[0].SessionID())
	if reg.Count() != MaxSessions-1 {
		t.Fatalf("expected %d sessions after unregister, got %d", MaxSessions-1, reg.Count())
	}

	if err := reg.Register(s33); err != nil {
		t.Fatalf("register after unregister failed: %v", err)
	}
	if reg.Count() != MaxSessions {
		t.Fatalf("expected %d sessions, got %d", MaxSessions, reg.Count())
	}
}

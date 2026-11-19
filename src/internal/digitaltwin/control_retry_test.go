package digitaltwin_test

import (
	"math"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"
)

func TestRetryPolicyValidation(t *testing.T) {
	p := digitaltwin.DefaultRetryPolicy()
	if err := p.Validate(); err != nil {
		t.Fatalf("default policy invalid: %v", err)
	}

	invalid := []digitaltwin.RetryPolicy{
		{BaseDelay: 0, CapDelay: 8 * time.Second, MaxAttempts: 5, PublishTimeout: 3 * time.Second, SafetyMargin: time.Second},
		{BaseDelay: 10 * time.Second, CapDelay: 5 * time.Second, MaxAttempts: 5, PublishTimeout: 3 * time.Second, SafetyMargin: time.Second},
		{BaseDelay: time.Second, CapDelay: 8 * time.Second, MaxAttempts: 0, PublishTimeout: 3 * time.Second, SafetyMargin: time.Second},
		{BaseDelay: time.Second, CapDelay: 8 * time.Second, MaxAttempts: 5, PublishTimeout: 0, SafetyMargin: time.Second},
		{BaseDelay: time.Second, CapDelay: 8 * time.Second, MaxAttempts: 5, PublishTimeout: 3 * time.Second, SafetyMargin: -time.Second},
	}
	for i, tc := range invalid {
		if err := tc.Validate(); err == nil {
			t.Errorf("case %d: expected validation error for %+v", i, tc)
		}
	}
}

func TestRetryPolicyBackoffComputation(t *testing.T) {
	p := digitaltwin.DefaultRetryPolicy() // Base: 1s, Cap: 8s, MaxAttempts: 5

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: time.Second},
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 3, want: 4 * time.Second},
		{attempt: 4, want: 8 * time.Second},
		{attempt: 5, want: 8 * time.Second},
		{attempt: 6, want: 8 * time.Second},
		{attempt: 10, want: 8 * time.Second},
		{attempt: 100, want: 8 * time.Second},
		{attempt: math.MaxInt, want: 8 * time.Second},
	}
	for _, tc := range cases {
		got := p.ComputeBackoff(tc.attempt)
		if got != tc.want {
			t.Errorf("attempt %d: got %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestJitterDelayBounds(t *testing.T) {
	backoff := 8 * time.Second

	// factor 0.0 => exactly 50%
	minDelay := digitaltwin.JitterDelay(backoff, 0.0)
	if minDelay != 4*time.Second {
		t.Errorf("factor 0.0: got %v, want 4s", minDelay)
	}

	// factor 1.0 => exactly 100%
	maxDelay := digitaltwin.JitterDelay(backoff, 1.0)
	if maxDelay != 8*time.Second {
		t.Errorf("factor 1.0: got %v, want 8s", maxDelay)
	}

	// factor 0.5 => 75% (6s)
	midDelay := digitaltwin.JitterDelay(backoff, 0.5)
	if midDelay != 6*time.Second {
		t.Errorf("factor 0.5: got %v, want 6s", midDelay)
	}

	// clamping
	if got := digitaltwin.JitterDelay(backoff, -0.5); got != 4*time.Second {
		t.Errorf("negative factor clamp: got %v, want 4s", got)
	}
	if got := digitaltwin.JitterDelay(backoff, 1.5); got != 8*time.Second {
		t.Errorf("excess factor clamp: got %v, want 8s", got)
	}

	// zero/negative backoff
	if got := digitaltwin.JitterDelay(0, 0.5); got != 0 {
		t.Errorf("zero backoff: got %v, want 0", got)
	}
}

// A non-finite jitter factor must never produce an unbounded or negative delay.
func TestJitterDelayNonFiniteFactors(t *testing.T) {
	backoff := 8 * time.Second
	for name, factor := range map[string]float64{"nan": math.NaN(), "+inf": math.Inf(1), "-inf": math.Inf(-1)} {
		got := digitaltwin.JitterDelay(backoff, factor)
		if got != 4*time.Second {
			t.Errorf("%s factor: got %v, want the 50%% floor 4s", name, got)
		}
	}
}

func TestNextAttemptSchedule(t *testing.T) {
	p := digitaltwin.DefaultRetryPolicy() // Base: 1s, Cap: 8s, Max: 5, Timeout: 3s, Safety: 1s (Lease: 4s)
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	expiresAt := now.Add(60 * time.Second)

	// Attempt 1 with minimum jitter (factor 0.0 -> delay 500ms):
	// Lease is 4s, which is greater than delay 500ms. Lease wins!
	next1, exhausted1 := p.NextAttemptSchedule(now, 1, 0.0, expiresAt)
	if exhausted1 {
		t.Fatal("attempt 1 must not be exhausted")
	}
	if want := now.Add(4 * time.Second); !next1.Equal(want) {
		t.Errorf("attempt 1: got %v, want %v", next1, want)
	}

	// Attempt 4 with maximum jitter (factor 1.0 -> delay 8s):
	// Lease is 4s. Delay 8s wins!
	next4, exhausted4 := p.NextAttemptSchedule(now, 4, 1.0, expiresAt)
	if exhausted4 {
		t.Fatal("attempt 4 must not be exhausted")
	}
	if want := now.Add(8 * time.Second); !next4.Equal(want) {
		t.Errorf("attempt 4: got %v, want %v", next4, want)
	}

	// Attempt 5 (max attempt = 5):
	// Reached max attempts: exhausted is true, next attempt is capped at expiresAt.
	next5, exhausted5 := p.NextAttemptSchedule(now, 5, 0.5, expiresAt)
	if !exhausted5 {
		t.Fatal("attempt 5 must be exhausted")
	}
	if !next5.Equal(expiresAt) {
		t.Errorf("attempt 5: got %v, want %v", next5, expiresAt)
	}

	// Tight expiry: remaining window (2s) is less than lease (4s)
	tightExpires := now.Add(2 * time.Second)
	nextTight, exhaustedTight := p.NextAttemptSchedule(now, 1, 0.0, tightExpires)
	if !exhaustedTight {
		t.Fatal("tight expiry must be exhausted")
	}
	if !nextTight.Equal(tightExpires) {
		t.Errorf("tight expiry: got %v, want %v", nextTight, tightExpires)
	}
}

func TestNextAttemptScheduleStopReasonSeparation(t *testing.T) {
	p := digitaltwin.DefaultRetryPolicy() // MaxAttempts: 5, Lease: 4s
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	expiresAt := now.Add(60 * time.Second)

	// Attempt 5 reached: reason must be "retry_exhausted"
	_, exhausted5, reason5 := p.NextAttemptScheduleWithReason(now, 5, 0.5, expiresAt)
	if !exhausted5 || reason5 != "retry_exhausted" {
		t.Fatalf("attempt 5: exhausted=%v, reason=%q, want retry_exhausted", exhausted5, reason5)
	}

	// Attempt 1 with tight expiry (< lease 4s): exhausted is true (no fit), but reason must be EMPTY (waits for TTL expiry)
	tightExpires := now.Add(2 * time.Second)
	_, exhaustedTight, reasonTight := p.NextAttemptScheduleWithReason(now, 1, 0.0, tightExpires)
	if !exhaustedTight || reasonTight != "" {
		t.Fatalf("tight expiry: exhausted=%v, reason=%q, want empty (normal TTL wait)", exhaustedTight, reasonTight)
	}
}

func TestJitterDelayScalingNoOverflow(t *testing.T) {
	maxDuration := time.Duration(math.MaxInt64)
	got := digitaltwin.JitterDelay(maxDuration, 1.0)
	if got <= 0 {
		t.Fatalf("expected positive duration without overflow, got %v", got)
	}
}

func TestControlDispatchContextTimeout(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	pubTimeout := 3 * time.Second

	// Legacy dispatch without timing metadata returns configured timeout
	legacy := digitaltwin.ControlDispatch{CommandID: [16]byte{1}}
	to, ok := legacy.ContextTimeout(pubTimeout, now)
	if !ok || to != pubTimeout {
		t.Fatalf("legacy timeout: ok=%v, to=%v", ok, to)
	}

	// Managed dispatch where lease remaining is 1.5s (< pubTimeout 3s)
	managed := digitaltwin.ControlDispatch{
		CommandID: [16]byte{2},
		Attempt:   1,
		Stage6:    true,
		ExpiresAt: now.Add(30 * time.Second),
		LeaseAt:   now.Add(1500 * time.Millisecond),
	}
	to, ok = managed.ContextTimeout(pubTimeout, now)
	if !ok || to != 1500*time.Millisecond {
		t.Fatalf("lease bound timeout: ok=%v, to=%v, want 1.5s", ok, to)
	}

	// Managed dispatch where expiry remaining is 500ms (< pubTimeout 3s)
	managedExpiry := digitaltwin.ControlDispatch{
		CommandID: [16]byte{3},
		Attempt:   1,
		Stage6:    true,
		ExpiresAt: now.Add(500 * time.Millisecond),
		LeaseAt:   now.Add(4 * time.Second),
	}
	to, ok = managedExpiry.ContextTimeout(pubTimeout, now)
	if !ok || to != 500*time.Millisecond {
		t.Fatalf("expiry bound timeout: ok=%v, to=%v, want 500ms", ok, to)
	}

	// Managed dispatch without a reserved attempt fails closed: no publish authority.
	missingAttempt := digitaltwin.ControlDispatch{
		CommandID: [16]byte{5},
		Stage6:    true,
		ExpiresAt: now.Add(30 * time.Second),
		LeaseAt:   now.Add(4 * time.Second),
	}
	if _, ok = missingAttempt.ContextTimeout(pubTimeout, now); ok {
		t.Fatal("managed dispatch without attempt must fail closed")
	}

	// Managed dispatch without expiry or lease fails closed.
	missingExpiry := digitaltwin.ControlDispatch{
		CommandID: [16]byte{6},
		Attempt:   1,
		Stage6:    true,
		LeaseAt:   now.Add(4 * time.Second),
	}
	if _, ok = missingExpiry.ContextTimeout(pubTimeout, now); ok {
		t.Fatal("managed dispatch without expiry must fail closed")
	}
	missingLease := digitaltwin.ControlDispatch{
		CommandID: [16]byte{7},
		Attempt:   1,
		Stage6:    true,
		ExpiresAt: now.Add(30 * time.Second),
	}
	if _, ok = missingLease.ContextTimeout(pubTimeout, now); ok {
		t.Fatal("managed dispatch without lease must fail closed")
	}

	// Authority already expired at start: returns ok = false
	expired := digitaltwin.ControlDispatch{
		CommandID: [16]byte{4},
		Stage6:    true,
		ExpiresAt: now.Add(10 * time.Second),
		LeaseAt:   now.Add(-100 * time.Millisecond),
	}
	_, ok = expired.ContextTimeout(pubTimeout, now)
	if ok {
		t.Fatal("expired authority must return ok = false")
	}
}

package metrics

import (
	"context"
	"testing"
	"time"
)

func TestControlTimelineRacesOrderAndExactlyOneMeasurement(t *testing.T) {
	base := time.Now()
	m, err := NewControlTimeline(8, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithControlHTTPEntry(context.Background(), base)
	// Result and PUBACK can be recorded before admission returns to HTTP.
	m.ResultCommit("early", "succeeded", base.Add(25*time.Millisecond))
	m.PUBACK("early", base.Add(20*time.Millisecond))
	m.Register(ctx, "early")
	m.Register(ctx, "normal")
	m.PUBACK("normal", base.Add(30*time.Millisecond))
	m.ResultCommit("normal", "failed", base.Add(40*time.Millisecond))
	for i := 0; i < 3; i++ {
		m.PUBACK("early", base.Add(time.Second))
		m.ResultCommit("early", "succeeded", base.Add(time.Second))
		m.Register(ctx, "early")
	}
	s := m.Snapshot()
	if s.Dispatch.Count != 2 || s.Succeeded.Count != 1 || s.Failed.Count != 1 {
		t.Fatalf("counts=%+v", s)
	}
	if s.Dispatch.SumSeconds < .0499 || s.Dispatch.SumSeconds > .0501 || s.Succeeded.SumSeconds != .025 || s.Failed.SumSeconds != .040 {
		t.Fatalf("wrong monotonic boundaries: %+v", s)
	}
}

func TestControlTimelineBoundExpiryAndMissingNeverZeroSamples(t *testing.T) {
	base := time.Now()
	m, _ := NewControlTimeline(1, time.Second)
	m.now = func() time.Time { return base }
	m.Register(WithControlHTTPEntry(context.Background(), base), "waiting")
	m.Register(WithControlHTTPEntry(context.Background(), base), "overflow")
	m.PUBACK("no-capacity", base.Add(time.Millisecond))
	s := m.Snapshot()
	if s.Entries != 1 || s.Overflow != 2 || s.Dispatch.Count != 0 || s.Succeeded.Count != 0 {
		t.Fatalf("unbounded/fake durations: %+v", s)
	}
	m.now = func() time.Time { return base.Add(2 * time.Second) }
	s = m.Snapshot()
	if s.Entries != 0 || s.MissingDispatch != 2 || s.MissingResult != 1 {
		t.Fatalf("missing measurements not counted: %+v", s)
	}
	// A restarted process has no HTTP origin. Observe pending unavailability,
	// then missing on expiry, rather than subtracting unrelated device clocks.
	m.ResultCommit("prior-process", "succeeded", base.Add(2*time.Second))
	if s = m.Snapshot(); s.PendingOrigins != 1 || s.Succeeded.Count != 0 {
		t.Fatalf("restart invented duration: %+v", s)
	}
	m.now = func() time.Time { return base.Add(4 * time.Second) }
	if s = m.Snapshot(); s.MissingResult != 2 || s.Succeeded.Count != 0 {
		t.Fatalf("orphan expired: %+v", s)
	}
}

func TestControlTimelineCommitToFirstDiscoveryAndEnqueue(t *testing.T) {
	base := time.Now()
	m, _ := NewControlTimeline(8, time.Minute)
	ctx := WithControlHTTPEntry(context.Background(), base)
	m.Discovered("racing", base.Add(20*time.Millisecond))
	m.Enqueued("racing", base.Add(25*time.Millisecond))
	m.RegisterCommitted(ctx, "racing", base.Add(10*time.Millisecond))
	m.PUBACK("racing", base.Add(30*time.Millisecond))
	m.ResultCommit("racing", "succeeded", base.Add(40*time.Millisecond))
	m.Discovered("racing", base.Add(50*time.Millisecond))
	m.Enqueued("racing", base.Add(60*time.Millisecond))
	s := m.Snapshot()
	if s.CommitToDiscovery.Count != 1 || s.CommitToEnqueue.Count != 1 || s.CommitToDiscovery.SumSeconds != .010 || s.CommitToEnqueue.SumSeconds != .015 || s.Dispatch.Count != 1 || s.Dispatch.SumSeconds != .030 || s.Succeeded.Count != 1 || s.Succeeded.SumSeconds != .040 {
		t.Fatalf("first event timing incorrect: %+v", s)
	}
}

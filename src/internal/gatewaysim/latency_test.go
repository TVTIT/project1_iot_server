package gatewaysim

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestLatencyTracker_NearestRankQuantiles(t *testing.T) {
	// 100 durations from 1ms to 100ms
	fc := newFakeClock()
	tracker, err := NewLatencyTracker(100, WithTrackerClock(fc))
	if err != nil {
		t.Fatalf("NewLatencyTracker: %v", err)
	}

	start := fc.Now()
	ids := make([]uuid.UUID, 100)
	for i := 0; i < 100; i++ {
		ids[i] = seqUUID(uint64(i + 1))
		tracker.RecordFirstPublish(ids[i])
	}

	for i := 0; i < 100; i++ {
		// ACK at start + (i+1)*time.Millisecond
		fc.mu.Lock()
		fc.now = start.Add(time.Duration(i+1) * time.Millisecond)
		fc.mu.Unlock()
		tracker.RecordAckDeleted(ids[i])
	}

	snap := tracker.Snapshot()
	if !snap.HasPercentiles {
		t.Fatal("expected HasPercentiles=true")
	}
	if snap.SampleCount != 100 {
		t.Fatalf("expected 100 samples, got %d", snap.SampleCount)
	}
	if snap.Min != 1*time.Millisecond {
		t.Fatalf("expected min 1ms, got %v", snap.Min)
	}
	if snap.Max != 100*time.Millisecond {
		t.Fatalf("expected max 100ms, got %v", snap.Max)
	}
	// Nearest-rank on 100 items:
	// p50: rank = ceil(50*100/100) = 50 -> index 49 -> 50ms
	if snap.P50 != 50*time.Millisecond {
		t.Fatalf("expected p50 50ms, got %v", snap.P50)
	}
	// p95: rank = ceil(95*100/100) = 95 -> index 94 -> 95ms
	if snap.P95 != 95*time.Millisecond {
		t.Fatalf("expected p95 95ms, got %v", snap.P95)
	}

	summary := FormatMeasurementSummary(snap)
	if !strings.Contains(summary, "samples=100") ||
		!strings.Contains(summary, "p50_ms=50.00") ||
		!strings.Contains(summary, "p95_ms=95.00") ||
		!strings.Contains(summary, "min_ms=1.00") ||
		!strings.Contains(summary, "max_ms=100.00") {
		t.Fatalf("unexpected summary format: %s", summary)
	}
}

func TestLatencyTracker_SmallSampleQuantiles(t *testing.T) {
	fc := newFakeClock()
	// Single sample
	t1, err := NewLatencyTracker(10, WithTrackerClock(fc))
	if err != nil {
		t.Fatalf("NewLatencyTracker: %v", err)
	}
	id1 := seqUUID(1)
	t1.RecordFirstPublish(id1)
	fc.mu.Lock()
	fc.now = fc.now.Add(42 * time.Millisecond)
	fc.mu.Unlock()
	t1.RecordAckDeleted(id1)

	s1 := t1.Snapshot()
	if s1.SampleCount != 1 || s1.P50 != 42*time.Millisecond || s1.P95 != 42*time.Millisecond {
		t.Fatalf("unexpected 1-sample quantiles: count=%d p50=%v p95=%v", s1.SampleCount, s1.P50, s1.P95)
	}

	// Two samples: 10ms and 20ms
	// p50: ceil(50*2/100) = 1 -> index 0 -> 10ms
	// p95: ceil(95*2/100) = 2 -> index 1 -> 20ms
	t2, err := NewLatencyTracker(10, WithTrackerClock(fc))
	if err != nil {
		t.Fatalf("NewLatencyTracker: %v", err)
	}
	idA, idB := seqUUID(10), seqUUID(11)
	t2.RecordFirstPublish(idA)
	t2.RecordFirstPublish(idB)
	fc.mu.Lock()
	fc.now = fc.now.Add(10 * time.Millisecond)
	fc.mu.Unlock()
	t2.RecordAckDeleted(idA)
	fc.mu.Lock()
	fc.now = fc.now.Add(10 * time.Millisecond)
	fc.mu.Unlock()
	t2.RecordAckDeleted(idB)

	s2 := t2.Snapshot()
	if s2.SampleCount != 2 || s2.P50 != 10*time.Millisecond || s2.P95 != 20*time.Millisecond {
		t.Fatalf("unexpected 2-sample quantiles: count=%d p50=%v p95=%v", s2.SampleCount, s2.P50, s2.P95)
	}
}

func TestLatencyTracker_ZeroSamplesAbsence(t *testing.T) {
	fc := newFakeClock()
	tracker, err := NewLatencyTracker(10, WithTrackerClock(fc))
	if err != nil {
		t.Fatalf("NewLatencyTracker: %v", err)
	}

	snap := tracker.Snapshot()
	if snap.HasPercentiles {
		t.Fatal("expected HasPercentiles=false when 0 samples")
	}
	if snap.SampleCount != 0 {
		t.Fatalf("expected 0 samples, got %d", snap.SampleCount)
	}

	summary := FormatMeasurementSummary(snap)
	if !strings.Contains(summary, "samples=0") ||
		!strings.Contains(summary, "p50_ms=none") ||
		!strings.Contains(summary, "p95_ms=none") ||
		!strings.Contains(summary, "min_ms=none") ||
		!strings.Contains(summary, "max_ms=none") {
		t.Fatalf("expected absence representation ('none') for zero samples, got: %s", summary)
	}
}

func TestLatencyTracker_CapExhaustionAndOverflow(t *testing.T) {
	fc := newFakeClock()
	// Capacity is 2
	tracker, err := NewLatencyTracker(2, WithTrackerClock(fc))
	if err != nil {
		t.Fatalf("NewLatencyTracker: %v", err)
	}

	id1, id2, id3 := seqUUID(1), seqUUID(2), seqUUID(3)
	tracker.RecordFirstPublish(id1)
	tracker.RecordFirstPublish(id2)
	// Third publish exceeds capacity
	tracker.RecordFirstPublish(id3)

	snap1 := tracker.Snapshot()
	if snap1.FirstPublishes != 2 {
		t.Fatalf("expected 2 first publishes, got %d", snap1.FirstPublishes)
	}
	if snap1.OverflowSamples != 1 {
		t.Fatalf("expected 1 overflow sample, got %d", snap1.OverflowSamples)
	}

	// ACK all three
	fc.mu.Lock()
	fc.now = fc.now.Add(10 * time.Millisecond)
	fc.mu.Unlock()
	tracker.RecordAckDeleted(id1)
	tracker.RecordAckDeleted(id2)
	tracker.RecordAckDeleted(id3) // id3 was never registered due to overflow -> missing origin!

	snap2 := tracker.Snapshot()
	if snap2.SampleCount != 2 {
		t.Fatalf("expected 2 samples, got %d", snap2.SampleCount)
	}
	if snap2.MissingOrigins != 1 {
		t.Fatalf("expected 1 missing origin for overflowed id3, got %d", snap2.MissingOrigins)
	}
}

func TestLatencyTracker_MissingOrigins(t *testing.T) {
	fc := newFakeClock()
	tracker, err := NewLatencyTracker(10, WithTrackerClock(fc))
	if err != nil {
		t.Fatalf("NewLatencyTracker: %v", err)
	}

	// ACK for unknown message that was never published in this process
	tracker.RecordAckDeleted(seqUUID(999))

	snap := tracker.Snapshot()
	if snap.MissingOrigins != 1 {
		t.Fatalf("expected 1 missing origin, got %d", snap.MissingOrigins)
	}
	if snap.SampleCount != 0 {
		t.Fatalf("expected 0 duration samples, got %d", snap.SampleCount)
	}
}

func TestLatencyTracker_RetryPreservesFirstCall(t *testing.T) {
	fc := newFakeClock()
	tracker, err := NewLatencyTracker(10, WithTrackerClock(fc))
	if err != nil {
		t.Fatalf("NewLatencyTracker: %v", err)
	}

	id := seqUUID(1)
	// First publish at t=0
	tracker.RecordFirstPublish(id)

	// Advance clock by 100ms
	fc.mu.Lock()
	fc.now = fc.now.Add(100 * time.Millisecond)
	fc.mu.Unlock()

	// Second publish (retry) at t=100ms
	tracker.RecordFirstPublish(id)

	// Advance clock by 50ms more (t=150ms total)
	fc.mu.Lock()
	fc.now = fc.now.Add(50 * time.Millisecond)
	fc.mu.Unlock()

	// ACK completes at t=150ms
	tracker.RecordAckDeleted(id)

	snap := tracker.Snapshot()
	if snap.FirstPublishes != 1 {
		t.Fatalf("expected 1 unique first publish, got %d", snap.FirstPublishes)
	}
	if snap.RetryAttempts != 1 {
		t.Fatalf("expected 1 retry attempt, got %d", snap.RetryAttempts)
	}
	if snap.SampleCount != 1 {
		t.Fatalf("expected 1 sample, got %d", snap.SampleCount)
	}
	// Latency MUST be 150ms (from first publish), NOT 50ms (from retry)
	if snap.P50 != 150*time.Millisecond {
		t.Fatalf("expected latency 150ms from first publish call, got %v", snap.P50)
	}
}

func TestLatencyTracker_DuplicateAckAddsNoSample(t *testing.T) {
	fc := newFakeClock()
	tracker, err := NewLatencyTracker(10, WithTrackerClock(fc))
	if err != nil {
		t.Fatalf("NewLatencyTracker: %v", err)
	}

	id := seqUUID(1)
	tracker.RecordFirstPublish(id)
	fc.mu.Lock()
	fc.now = fc.now.Add(25 * time.Millisecond)
	fc.mu.Unlock()
	tracker.RecordAckDeleted(id)

	// Duplicate ACK delete attempt
	fc.mu.Lock()
	fc.now = fc.now.Add(25 * time.Millisecond)
	fc.mu.Unlock()
	tracker.RecordAckDeleted(id)

	snap := tracker.Snapshot()
	if snap.SampleCount != 1 {
		t.Fatalf("expected exactly 1 sample, got %d", snap.SampleCount)
	}
	if snap.MissingOrigins != 1 {
		t.Fatalf("expected 1 missing origin for duplicate ack, got %d", snap.MissingOrigins)
	}
	if snap.P50 != 25*time.Millisecond {
		t.Fatalf("expected 25ms, got %v", snap.P50)
	}
}

func TestLatencyTracker_DisabledModeBoundedBehavior(t *testing.T) {
	tracker := NewDisabledLatencyTracker()
	if tracker.Snapshot().Enabled {
		t.Fatal("expected disabled tracker to report Enabled=false")
	}

	// Operations should be no-ops and not panic
	id := seqUUID(1)
	tracker.RecordRunStart()
	tracker.RecordGenerationStart()
	tracker.RecordFirstPublish(id)
	tracker.RecordAckDeleted(id)
	tracker.RecordGenerationEnd()
	tracker.RecordDrainComplete()
	tracker.RecordRunEnd()

	snap := tracker.Snapshot()
	if snap.SampleCount != 0 || snap.FirstPublishes != 0 || snap.RetryAttempts != 0 {
		t.Fatalf("expected 0 counts on disabled tracker, got %+v", snap)
	}
	if tracker.firstPublish != nil || tracker.completed != nil || tracker.durations != nil {
		t.Fatal("disabled tracker must not allocate maps or duration slices")
	}
}

func TestLatencyTracker_WindowDurations(t *testing.T) {
	fc := newFakeClock()
	tracker, err := NewLatencyTracker(10, WithTrackerClock(fc))
	if err != nil {
		t.Fatalf("NewLatencyTracker: %v", err)
	}

	tracker.RecordRunStart()

	fc.mu.Lock()
	fc.now = fc.now.Add(100 * time.Millisecond)
	fc.mu.Unlock()
	tracker.RecordGenerationStart()

	id := seqUUID(1)
	fc.mu.Lock()
	fc.now = fc.now.Add(50 * time.Millisecond)
	fc.mu.Unlock()
	tracker.RecordFirstPublish(id)

	fc.mu.Lock()
	fc.now = fc.now.Add(200 * time.Millisecond)
	fc.mu.Unlock()
	tracker.RecordGenerationEnd()

	fc.mu.Lock()
	fc.now = fc.now.Add(150 * time.Millisecond)
	fc.mu.Unlock()
	tracker.RecordAckDeleted(id)
	tracker.RecordDrainComplete()

	fc.mu.Lock()
	fc.now = fc.now.Add(50 * time.Millisecond)
	fc.mu.Unlock()
	tracker.RecordRunEnd()

	snap := tracker.Snapshot()
	// Total duration: 100 + 50 + 200 + 150 + 50 = 550ms
	if snap.TotalDuration != 550*time.Millisecond {
		t.Fatalf("expected TotalDuration 550ms, got %v", snap.TotalDuration)
	}
	// Generation duration: from genStart to genEnd = 50 + 200 = 250ms
	if snap.GenerationDuration != 250*time.Millisecond {
		t.Fatalf("expected GenerationDuration 250ms, got %v", snap.GenerationDuration)
	}
	// Publish-to-drain duration: from first publish to drain complete = 200 + 150 = 350ms
	if snap.PublishDrainDuration != 350*time.Millisecond {
		t.Fatalf("expected PublishDrainDuration 350ms, got %v", snap.PublishDrainDuration)
	}
}

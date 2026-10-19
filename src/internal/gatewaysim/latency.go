package gatewaysim

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

// MaxMeasurementCapacity is the explicit maximum capacity for bounded latency
// tracking in one finite simulator run.
const MaxMeasurementCapacity = 100_000

// LatencyRecorder is the narrow interface Scheduler and ACKProcessor call to
// record lifecycle points for bounded latency and window measurement.
type LatencyRecorder interface {
	RecordFirstPublish(id uuid.UUID)
	RecordAckDeleted(id uuid.UUID)
	RecordRunStart()
	RecordRunEnd()
	RecordGenerationStart()
	RecordGenerationEnd()
	RecordDrainComplete()
}

// LatencyTrackerOption configures an optional LatencyTracker property.
type LatencyTrackerOption func(*LatencyTracker)

// WithTrackerClock injects a custom monotonic clock into the tracker.
func WithTrackerClock(c Clock) LatencyTrackerOption {
	return func(t *LatencyTracker) {
		if c != nil {
			t.clock = c
		}
	}
}

// LatencyTracker is the concurrency-safe, bounded latency and window tracker.
// In disabled mode, it retains no state and performs zero allocations.
type LatencyTracker struct {
	mu sync.Mutex

	enabled  bool
	capacity int
	clock    Clock

	// in-flight first publish timestamps by message ID
	firstPublish map[uuid.UUID]time.Time
	// completed set to prevent retry/late publishes from re-registering an ACKed message
	completed map[uuid.UUID]struct{}

	// collected duration samples
	durations []time.Duration

	// counters
	firstPublishes  int64
	retryAttempts   int64
	missingOrigins  int64
	overflowSamples int64

	// monotonic window markers
	runStart      time.Time
	runEnd        time.Time
	genStart      time.Time
	genEnd        time.Time
	firstPubTime  time.Time
	lastAckTime   time.Time
	drainComplete time.Time
}

// NewLatencyTracker creates an enabled, bounded latency tracker with the given
// capacity. Capacity must be between 1 and MaxMeasurementCapacity.
func NewLatencyTracker(capacity int, opts ...LatencyTrackerOption) (*LatencyTracker, error) {
	if capacity <= 0 || capacity > MaxMeasurementCapacity {
		return nil, fmt.Errorf("gatewaysim: measurement capacity must be between 1 and %d", MaxMeasurementCapacity)
	}
	prealloc := capacity
	if prealloc > 1024 {
		prealloc = 1024
	}
	t := &LatencyTracker{
		enabled:      true,
		capacity:     capacity,
		clock:        realClock{},
		firstPublish: make(map[uuid.UUID]time.Time, prealloc),
		completed:    make(map[uuid.UUID]struct{}, prealloc),
		durations:    make([]time.Duration, 0, prealloc),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t, nil
}

// NewDisabledLatencyTracker returns a disabled tracker that performs no-ops.
func NewDisabledLatencyTracker() *LatencyTracker {
	return &LatencyTracker{enabled: false}
}

// RecordFirstPublish registers the first publish call for a message. Registration
// must happen immediately before Publisher.Publish. If the message was already
// published or completed, first registration wins and retryAttempts is incremented.
func (t *LatencyTracker) RecordFirstPublish(id uuid.UUID) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.enabled {
		return
	}

	now := t.clock.Now()
	if t.firstPubTime.IsZero() {
		t.firstPubTime = now
	}

	if _, exists := t.firstPublish[id]; exists {
		t.retryAttempts++
		return
	}
	if _, done := t.completed[id]; done {
		t.retryAttempts++
		return
	}

	if len(t.firstPublish)+len(t.completed) >= t.capacity {
		t.overflowSamples++
		return
	}

	t.firstPublish[id] = now
	t.firstPublishes++
}

// RecordAckDeleted records latency after a valid application ACK successfully
// commits deletion of the pending SQLite row.
func (t *LatencyTracker) RecordAckDeleted(id uuid.UUID) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.enabled {
		return
	}

	now := t.clock.Now()
	t.lastAckTime = now

	start, ok := t.firstPublish[id]
	if !ok {
		t.missingOrigins++
		return
	}

	delete(t.firstPublish, id)
	t.completed[id] = struct{}{}

	if len(t.durations) >= t.capacity {
		t.overflowSamples++
		return
	}

	d := now.Sub(start)
	if d < 0 {
		d = 0
	}
	t.durations = append(t.durations, d)
}

// RecordRunStart records process run start.
func (t *LatencyTracker) RecordRunStart() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.enabled {
		return
	}
	if t.runStart.IsZero() {
		t.runStart = t.clock.Now()
	}
}

// RecordRunEnd records process run end.
func (t *LatencyTracker) RecordRunEnd() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.enabled {
		return
	}
	t.runEnd = t.clock.Now()
}

// RecordGenerationStart marks when the first batch is enqueued.
func (t *LatencyTracker) RecordGenerationStart() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.enabled {
		return
	}
	if t.genStart.IsZero() {
		t.genStart = t.clock.Now()
	}
}

// RecordGenerationEnd marks when finite batch generation completes.
func (t *LatencyTracker) RecordGenerationEnd() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.enabled {
		return
	}
	if t.genEnd.IsZero() {
		t.genEnd = t.clock.Now()
	}
}

// RecordDrainComplete marks when all pending outbox rows are acknowledged.
func (t *LatencyTracker) RecordDrainComplete() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.enabled {
		return
	}
	if t.drainComplete.IsZero() {
		t.drainComplete = t.clock.Now()
	}
}

// LatencySnapshot captures a safe, payload-free snapshot of measurement results.
type LatencySnapshot struct {
	Enabled         bool
	Capacity        int
	SampleCount     int
	HasPercentiles  bool
	P50             time.Duration
	P95             time.Duration
	Min             time.Duration
	Max             time.Duration
	FirstPublishes  int64
	RetryAttempts   int64
	MissingOrigins  int64
	OverflowSamples int64

	GenerationDuration   time.Duration
	PublishDrainDuration time.Duration
	TotalDuration        time.Duration
}

// nearestRankIndex returns the 0-based index for percentile p (1-100)
// using the nearest-rank method: rank = ceil(p/100 * n).
func nearestRankIndex(p, n int) int {
	if n <= 1 {
		return 0
	}
	k := (p*n + 99) / 100
	if k < 1 {
		k = 1
	}
	if k > n {
		k = n
	}
	return k - 1
}

// Snapshot computes percentiles and window durations from collected samples.
func (t *LatencyTracker) Snapshot() LatencySnapshot {
	if t == nil {
		return LatencySnapshot{Enabled: false}
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.enabled {
		return LatencySnapshot{Enabled: false}
	}

	snap := LatencySnapshot{
		Enabled:         true,
		Capacity:        t.capacity,
		SampleCount:     len(t.durations),
		FirstPublishes:  t.firstPublishes,
		RetryAttempts:   t.retryAttempts,
		MissingOrigins:  t.missingOrigins,
		OverflowSamples: t.overflowSamples,
	}

	if len(t.durations) > 0 {
		sorted := append([]time.Duration(nil), t.durations...)
		slices.Sort(sorted)
		n := len(sorted)
		snap.HasPercentiles = true
		snap.Min = sorted[0]
		snap.Max = sorted[n-1]
		snap.P50 = sorted[nearestRankIndex(50, n)]
		snap.P95 = sorted[nearestRankIndex(95, n)]
	}

	// Calculate window durations
	now := t.clock.Now()
	end := t.runEnd
	if end.IsZero() {
		end = now
	}

	if !t.runStart.IsZero() && !end.Before(t.runStart) {
		snap.TotalDuration = end.Sub(t.runStart)
	}

	if !t.genStart.IsZero() {
		genEnd := t.genEnd
		if genEnd.IsZero() {
			genEnd = end
		}
		if !genEnd.Before(t.genStart) {
			snap.GenerationDuration = genEnd.Sub(t.genStart)
		}
	}

	if !t.firstPubTime.IsZero() {
		drainEnd := t.drainComplete
		if drainEnd.IsZero() {
			drainEnd = t.lastAckTime
		}
		if drainEnd.IsZero() {
			drainEnd = end
		}
		if !drainEnd.Before(t.firstPubTime) {
			snap.PublishDrainDuration = drainEnd.Sub(t.firstPubTime)
		}
	}

	return snap
}

// FormatMeasurementSummary formats a safe structured measurement line.
func FormatMeasurementSummary(snap LatencySnapshot) string {
	p50Str := "none"
	p95Str := "none"
	minStr := "none"
	maxStr := "none"
	if snap.HasPercentiles {
		p50Str = fmt.Sprintf("%.2f", float64(snap.P50.Nanoseconds())/1e6)
		p95Str = fmt.Sprintf("%.2f", float64(snap.P95.Nanoseconds())/1e6)
		minStr = fmt.Sprintf("%.2f", float64(snap.Min.Nanoseconds())/1e6)
		maxStr = fmt.Sprintf("%.2f", float64(snap.Max.Nanoseconds())/1e6)
	}
	return fmt.Sprintf("measurement summary: samples=%d p50_ms=%s p95_ms=%s min_ms=%s max_ms=%s first_publishes=%d retry_attempts=%d missing_origins=%d overflow_samples=%d gen_duration_ms=%.2f publish_drain_duration_ms=%.2f total_duration_ms=%.2f",
		snap.SampleCount, p50Str, p95Str, minStr, maxStr,
		snap.FirstPublishes, snap.RetryAttempts, snap.MissingOrigins, snap.OverflowSamples,
		float64(snap.GenerationDuration.Nanoseconds())/1e6,
		float64(snap.PublishDrainDuration.Nanoseconds())/1e6,
		float64(snap.TotalDuration.Nanoseconds())/1e6)
}

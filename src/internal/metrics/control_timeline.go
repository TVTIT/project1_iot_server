package metrics

import (
	"context"
	"errors"
	"sync"
	"time"
)

type controlEntryKey struct{}

// WithControlHTTPEntry carries the Go HTTP entry's monotonic timestamp internally.
func WithControlHTTPEntry(ctx context.Context, at time.Time) context.Context {
	return context.WithValue(ctx, controlEntryKey{}, at)
}

type commandTiming struct {
	start, puback, commit, seen time.Time
	outcome                     string
	dispatchDone, resultDone    bool
}

// ControlTimeline is a bounded, process-local correlation table, never a registry
// of metric labels. It tolerates a PUBACK/result racing post-commit registration.
type ControlTimeline struct {
	mu                                                      sync.Mutex
	capacity                                                int
	ttl                                                     time.Duration
	entries                                                 map[string]*commandTiming
	now                                                     func() time.Time
	dispatch, succeeded, failed                             Histogram
	missingOrigin, missingDispatch, missingResult, overflow int64
}

// ControlTimelineSnapshot exposes counts/histograms only, with no command IDs.
type ControlTimelineSnapshot struct {
	Dispatch        HistogramSnapshot `json:"d_dispatch"`
	Succeeded       HistogramSnapshot `json:"d_e2e_succeeded"`
	Failed          HistogramSnapshot `json:"d_e2e_failed"`
	MissingOrigin   int64             `json:"missing_origin"`
	MissingDispatch int64             `json:"missing_dispatch"`
	MissingResult   int64             `json:"missing_result"`
	Overflow        int64             `json:"overflow"`
	PendingOrigins  int               `json:"pending_origins"`
	Entries         int               `json:"entries"`
	Capacity        int               `json:"capacity"`
	TTLSeconds      float64           `json:"ttl_seconds"`
}

// NewControlTimeline requires finite bounds independent of traffic identifiers.
func NewControlTimeline(capacity int, ttl time.Duration) (*ControlTimeline, error) {
	if capacity < 1 || capacity > 10000 || ttl < time.Second || ttl > time.Hour {
		return nil, errors.New("invalid control measurement bounds")
	}
	return &ControlTimeline{capacity: capacity, ttl: ttl, entries: map[string]*commandTiming{}, now: time.Now}, nil
}

// Register is called only for a new, committed intent, never replay/no-op/reject.
func (m *ControlTimeline) Register(ctx context.Context, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expire()
	if ctx == nil {
		m.missingOrigin++
		return
	}
	start, ok := ctx.Value(controlEntryKey{}).(time.Time)
	if !ok || start.IsZero() || m.now().Sub(start) >= m.ttl {
		m.missingOrigin++
		return
	}
	e := m.entry(id)
	if e == nil {
		m.missingOrigin++
		return
	}
	if e.start.IsZero() {
		e.start = start
	}
	m.observe(e)
}

// PUBACK records the first successful broker acknowledgment independently of DB mark.
func (m *ControlTimeline) PUBACK(id string, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expire()
	e := m.entry(id)
	if e == nil {
		m.missingDispatch++
		return
	}
	if e.puback.IsZero() {
		e.puback = at
	}
	m.observe(e)
}

// ResultCommit records a confirmed result transaction, not Gateway wall time.
func (m *ControlTimeline) ResultCommit(id, outcome string, at time.Time) {
	if outcome != "succeeded" && outcome != "failed" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expire()
	e := m.entry(id)
	if e == nil {
		m.missingResult++
		return
	}
	if e.commit.IsZero() {
		e.commit = at
		e.outcome = outcome
	}
	m.observe(e)
}

// Snapshot lazily expires bounded entries and exposes missing measurements explicitly.
func (m *ControlTimeline) Snapshot() ControlTimelineSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expire()
	pending := 0
	for _, e := range m.entries {
		if e.start.IsZero() {
			pending++
		}
	}
	return ControlTimelineSnapshot{Dispatch: m.dispatch.Snapshot(), Succeeded: m.succeeded.Snapshot(), Failed: m.failed.Snapshot(), MissingOrigin: m.missingOrigin, MissingDispatch: m.missingDispatch, MissingResult: m.missingResult, Overflow: m.overflow, PendingOrigins: pending, Entries: len(m.entries), Capacity: m.capacity, TTLSeconds: m.ttl.Seconds()}
}

func (m *ControlTimeline) entry(id string) *commandTiming {
	if id == "" || len(id) > 128 {
		return nil
	}
	if e := m.entries[id]; e != nil {
		return e
	}
	if len(m.entries) >= m.capacity {
		m.overflow++
		return nil
	}
	e := &commandTiming{seen: m.now()}
	m.entries[id] = e
	return e
}

func (m *ControlTimeline) expire() {
	now := m.now()
	for id, e := range m.entries {
		if now.Sub(e.seen) < m.ttl {
			continue
		}
		if !e.dispatchDone && (!e.start.IsZero() || !e.puback.IsZero()) {
			m.missingDispatch++
		}
		if !e.resultDone && (!e.start.IsZero() || !e.commit.IsZero()) {
			m.missingResult++
		}
		if e.start.IsZero() {
			m.missingOrigin++
		}
		delete(m.entries, id)
	}
}

func (m *ControlTimeline) observe(e *commandTiming) {
	if e.start.IsZero() {
		return
	}
	if !e.dispatchDone && !e.puback.IsZero() {
		e.dispatchDone = true
		if e.puback.Before(e.start) {
			m.missingDispatch++
		} else {
			m.dispatch.Observe(e.puback.Sub(e.start))
		}
	}
	if !e.resultDone && !e.commit.IsZero() {
		e.resultDone = true
		if e.commit.Before(e.start) {
			m.missingResult++
		} else if e.outcome == "succeeded" {
			m.succeeded.Observe(e.commit.Sub(e.start))
		} else {
			m.failed.Observe(e.commit.Sub(e.start))
		}
	}
}

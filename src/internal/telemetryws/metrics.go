package telemetryws

import (
	"sync"
	"sync/atomic"
	"time"

	"iot-platform/internal/metrics"
)

// Metrics is a fixed-cardinality WS observer. It never stores identities or samples.
type Metrics struct {
	closeMu                                                                                sync.Mutex
	closeCounts                                                                            [len(closeReasonKeys)]uint64
	closeObservations                                                                      []CloseObservation // last 32, identity-free; never a session registry
	submissions, newEvents, buildFailures, deliveries, writes, controls, drops, slowCloses uint64
	authDenials, originDenials, expiries, revocations, originMissing                       uint64
	submit, dispatch, write                                                                metrics.Histogram
}

// MetricsSnapshot is the fixed, identity-free WS metrics schema.
type MetricsSnapshot struct {
	CloseCounts                map[string]uint64         `json:"close_counts"`
	CloseObservations          []CloseObservation        `json:"close_observations"`
	Availability               string                    `json:"availability"`
	Sessions                   int                       `json:"sessions"`
	Subscriptions              int                       `json:"subscriptions"`
	GlobalQueuedWork           int                       `json:"global_queued_work"`
	GlobalQueuedBytes          int                       `json:"global_queued_bytes"`
	SessionQueuedInFlightBytes int                       `json:"session_queued_in_flight_bytes"`
	Submissions                uint64                    `json:"submissions"`
	NewEvents                  uint64                    `json:"new_events"`
	BuildFailures              uint64                    `json:"build_failures"`
	DeliveriesEnqueued         uint64                    `json:"deliveries_enqueued"`
	TelemetryWritesCompleted   uint64                    `json:"telemetry_writes_completed"`
	ControlWritesCompleted     uint64                    `json:"control_writes_completed"`
	Drops                      uint64                    `json:"drops"`
	SlowCloses                 uint64                    `json:"slow_closes"`
	AuthDenials                uint64                    `json:"auth_denials"`
	OriginDenials              uint64                    `json:"origin_denials"`
	Expiries                   uint64                    `json:"expiries"`
	Revocations                uint64                    `json:"revocations"`
	OriginMissing              uint64                    `json:"origin_missing"`
	ConfirmedToSubmit          metrics.HistogramSnapshot `json:"confirmed_to_submit_seconds"`
	ConfirmedToDispatch        metrics.HistogramSnapshot `json:"confirmed_to_dispatch_seconds"`
	ConfirmedToWrite           metrics.HistogramSnapshot `json:"confirmed_to_write_seconds"`
}

func inc(v *uint64, n uint64) { atomic.AddUint64(v, n) }
func elapsed(h *metrics.Histogram, t time.Time) {
	if t.IsZero() {
		return
	}
	d := time.Since(t)
	if d >= 0 {
		h.Observe(d)
	}
}

// Submitted counts a WS event submission and records its process-local origin time.
func (m *Metrics) Submitted(t time.Time) {
	if m == nil {
		return
	}
	inc(&m.submissions, 1)
	if t.IsZero() {
		inc(&m.originMissing, 1)
	} else {
		elapsed(&m.submit, t)
	}
}

// NewEvent counts a successfully built new committed-event frame.
func (m *Metrics) NewEvent(t time.Time) {
	if m == nil {
		return
	}
	inc(&m.newEvents, 1)
	if !t.IsZero() {
		elapsed(&m.dispatch, t)
	}
}

// BuildFailure counts an event encoding or validation failure.
func (m *Metrics) BuildFailure() {
	if m != nil {
		inc(&m.buildFailures, 1)
	}
}

// DeliveryEnqueued adds viewer-queue enqueues, not unique committed events.
func (m *Metrics) DeliveryEnqueued(n int) {
	if m != nil && n > 0 {
		inc(&m.deliveries, uint64(n))
	}
}

// TelemetryWriteCompleted counts successful socket telemetry writes, not receipt.
func (m *Metrics) TelemetryWriteCompleted(t time.Time) {
	if m == nil {
		return
	}
	inc(&m.writes, 1)
	if t.IsZero() {
		inc(&m.originMissing, 1)
	} else {
		elapsed(&m.write, t)
	}
}

// ControlWriteCompleted counts successful application control writes.
func (m *Metrics) ControlWriteCompleted() {
	if m != nil {
		inc(&m.controls, 1)
	}
}

// Drop counts admission or delivery loss handled by bounded WS policy.
func (m *Metrics) Drop() {
	if m != nil {
		inc(&m.drops, 1)
	}
}

// SlowClose counts sessions isolated for queue overflow.
func (m *Metrics) SlowClose() {
	if m != nil {
		inc(&m.slowCloses, 1)
	}
}

// AuthDenied counts WS handshake identity denials only.
func (m *Metrics) AuthDenied() {
	if m != nil {
		inc(&m.authDenials, 1)
	}
}

// OriginDenied counts WS handshake Origin denials only.
func (m *Metrics) OriginDenied() {
	if m != nil {
		inc(&m.originDenials, 1)
	}
}

// Expired counts sessions closed for token expiry.
func (m *Metrics) Expired() {
	if m != nil {
		inc(&m.expiries, 1)
	}
}

// Revoked counts non-expiry session revocations.
func (m *Metrics) Revoked() {
	if m != nil {
		inc(&m.revocations, 1)
	}
}

// Snapshot takes bounded in-memory locks only. Nil source means unavailable, not zero.
func (m *Metrics) Snapshot(h *Hub, runtimes ...*EventRuntime) MetricsSnapshot {
	s := MetricsSnapshot{Availability: "unavailable"}
	if m == nil || h == nil {
		return s
	}
	s.Availability = "available"
	m.closeMu.Lock()
	s.CloseCounts = make(map[string]uint64, len(closeReasonKeys))
	for i, key := range closeReasonKeys {
		s.CloseCounts[key] = m.closeCounts[i]
	}
	s.CloseObservations = append([]CloseObservation(nil), m.closeObservations...)
	m.closeMu.Unlock()
	s.Sessions = h.SessionCount()
	s.Subscriptions = h.SubscriptionCount()
	s.SessionQueuedInFlightBytes = h.TotalQueuedBytes() + h.TotalInFlightBytes() + h.TotalControlBytes()
	if len(runtimes) > 0 && runtimes[0] != nil {
		s.GlobalQueuedWork = runtimes[0].PendingWork()
		s.GlobalQueuedBytes = runtimes[0].PendingBytes()
	}
	s.Submissions = atomic.LoadUint64(&m.submissions)
	s.NewEvents = atomic.LoadUint64(&m.newEvents)
	s.BuildFailures = atomic.LoadUint64(&m.buildFailures)
	s.DeliveriesEnqueued = atomic.LoadUint64(&m.deliveries)
	s.TelemetryWritesCompleted = atomic.LoadUint64(&m.writes)
	s.ControlWritesCompleted = atomic.LoadUint64(&m.controls)
	s.Drops = atomic.LoadUint64(&m.drops)
	s.SlowCloses = atomic.LoadUint64(&m.slowCloses)
	s.AuthDenials = atomic.LoadUint64(&m.authDenials)
	s.OriginDenials = atomic.LoadUint64(&m.originDenials)
	s.Expiries = atomic.LoadUint64(&m.expiries)
	s.Revocations = atomic.LoadUint64(&m.revocations)
	s.OriginMissing = atomic.LoadUint64(&m.originMissing)
	s.ConfirmedToSubmit = m.submit.Snapshot()
	s.ConfirmedToDispatch = m.dispatch.Snapshot()
	s.ConfirmedToWrite = m.write.Snapshot()
	return s
}

func (m *Metrics) sessionTerminated(obs CloseObservation) {
	if m == nil {
		return
	}
	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	for i, key := range closeReasonKeys {
		if obs.Reason == key {
			m.closeCounts[i]++
			break
		}
	}
	if len(m.closeObservations) == 32 {
		copy(m.closeObservations, m.closeObservations[1:])
		m.closeObservations = m.closeObservations[:31]
	}
	m.closeObservations = append(m.closeObservations, obs)
}

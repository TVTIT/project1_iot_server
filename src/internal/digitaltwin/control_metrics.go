package digitaltwin

import (
	"sync/atomic"
	"time"

	"iot-platform/internal/metrics"
)

// ControlMetrics observes repository admission, not execution or dispatch E2E.
type ControlMetrics struct {
	accepted, replay, noop, rejected atomic.Int64
	admission                        metrics.Histogram
}

// ControlMetricsSnapshot separates new intent from exact receipt replay/no-op.
type ControlMetricsSnapshot struct {
	Accepted  int64                     `json:"accepted"`
	Replay    int64                     `json:"replay"`
	Noop      int64                     `json:"noop"`
	Rejected  int64                     `json:"rejected"`
	Admission metrics.HistogramSnapshot `json:"admission"`
}

func (m *ControlMetrics) observe(outcome string, duration time.Duration) {
	m.admission.Observe(duration)
	switch outcome {
	case "accepted":
		m.accepted.Add(1)
	case "replay":
		m.replay.Add(1)
	case "noop":
		m.noop.Add(1)
	default:
		m.rejected.Add(1)
	}
}

// Snapshot copies fixed counters; rejected includes dependency/commit failures.
func (m *ControlMetrics) Snapshot() ControlMetricsSnapshot {
	return ControlMetricsSnapshot{m.accepted.Load(), m.replay.Load(), m.noop.Load(), m.rejected.Load(), m.admission.Snapshot()}
}

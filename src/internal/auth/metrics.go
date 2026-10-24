package auth

import (
	"sync/atomic"

	"iot-platform/internal/metrics"
)

// Metrics retains only fixed authentication outcomes and verify durations.
type Metrics struct {
	allow, deny atomic.Int64
	verify      metrics.Histogram
}

// MetricsSnapshot never contains token, subject or verifier errors.
type MetricsSnapshot struct {
	Allow  int64                     `json:"allow"`
	Deny   int64                     `json:"deny"`
	Verify metrics.HistogramSnapshot `json:"verify"`
}

// Snapshot reads fixed-cardinality process counters.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{Allow: m.allow.Load(), Deny: m.deny.Load(), Verify: m.verify.Snapshot()}
}

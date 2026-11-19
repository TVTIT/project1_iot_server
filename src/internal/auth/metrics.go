package auth

import (
	"sync/atomic"

	"iot-platform/internal/metrics"
)

// Metrics retains only fixed authentication outcomes and verify durations.
type Metrics struct {
	allow, deny, websocketDeny atomic.Int64
	verify                     metrics.Histogram
}

// MetricsSnapshot never contains token, subject or verifier errors.
type MetricsSnapshot struct {
	Allow         int64                     `json:"allow"`
	Deny          int64                     `json:"deny"`
	WebSocketDeny int64                     `json:"websocket_deny"`
	Verify        metrics.HistogramSnapshot `json:"verify"`
}

// Snapshot reads fixed-cardinality process counters.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{Allow: m.allow.Load(), Deny: m.deny.Load(), WebSocketDeny: m.websocketDeny.Load(), Verify: m.verify.Snapshot()}
}

// WebSocketDenied counts WS middleware denials separately from REST auth.
func (m *Metrics) WebSocketDenied() {
	if m != nil {
		m.websocketDeny.Add(1)
	}
}

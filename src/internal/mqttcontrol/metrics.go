package mqttcontrol

import (
	"sync/atomic"

	"iot-platform/internal/metrics"
)

// Metrics contains only local dispatcher stage measurements, no command IDs.
type Metrics struct {
	discovery, queueWait, publish metrics.Histogram
	puback, publishFailures       atomic.Int64
}

// MetricsSnapshot distinguishes PUBACK from execution and durable marking.
type MetricsSnapshot struct {
	Discovery       metrics.HistogramSnapshot `json:"discovery"`
	QueueWait       metrics.HistogramSnapshot `json:"queue_wait"`
	Publish         metrics.HistogramSnapshot `json:"publish"`
	PUBACK          int64                     `json:"puback"`
	PublishFailures int64                     `json:"publish_failures"`
}

// Snapshot copies bounded state.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{m.discovery.Snapshot(), m.queueWait.Snapshot(), m.publish.Snapshot(), m.puback.Load(), m.publishFailures.Load()}
}

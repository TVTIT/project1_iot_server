package mqttcontrol

import (
	"sync/atomic"

	"iot-platform/internal/metrics"
)

// Metrics contains only local dispatcher stage measurements, no command IDs.
type Metrics struct {
	discovery, queueWait, prepare, publish, mark                     metrics.Histogram
	notifications, coalesced, scans, scanErrors, enqueued, queueFull atomic.Int64
	puback, publishFailures, markSuccessCalls, markErrors            atomic.Int64
	queueDepth, queueCapacity, liveWorkers, inFlight                 atomic.Int64
}

// MetricsSnapshot distinguishes PUBACK from execution and durable marking.
type MetricsSnapshot struct {
	Discovery                    metrics.HistogramSnapshot `json:"discovery"`
	QueueWait                    metrics.HistogramSnapshot `json:"queue_wait"`
	Prepare                      metrics.HistogramSnapshot `json:"prepare_db_recheck"`
	Publish                      metrics.HistogramSnapshot `json:"publish"`
	PublishedMark                metrics.HistogramSnapshot `json:"published_mark"`
	Notifications                int64                     `json:"notification_signals_accepted"`
	Coalesced                    int64                     `json:"notification_signals_coalesced"`
	Scans                        int64                     `json:"scans"`
	ScanErrors                   int64                     `json:"scan_errors"`
	Enqueued                     int64                     `json:"enqueued"`
	QueueFull                    int64                     `json:"queue_full"`
	QueueDepth                   int64                     `json:"queue_depth"`
	QueueCapacity                int64                     `json:"queue_capacity"`
	LivePublishWorkers           int64                     `json:"live_publish_workers"`
	InFlight                     int64                     `json:"in_flight"`
	PUBACK                       int64                     `json:"puback"`
	PublishFailures              int64                     `json:"publish_failures"`
	PublishedMarkSuccessfulCalls int64                     `json:"published_mark_successful_calls"`
	MarkErrors                   int64                     `json:"published_mark_errors"`
}

// Snapshot copies bounded state.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{Discovery: m.discovery.Snapshot(), QueueWait: m.queueWait.Snapshot(), Prepare: m.prepare.Snapshot(), Publish: m.publish.Snapshot(), PublishedMark: m.mark.Snapshot(), Notifications: m.notifications.Load(), Coalesced: m.coalesced.Load(), Scans: m.scans.Load(), ScanErrors: m.scanErrors.Load(), Enqueued: m.enqueued.Load(), QueueFull: m.queueFull.Load(), QueueDepth: m.queueDepth.Load(), QueueCapacity: m.queueCapacity.Load(), LivePublishWorkers: m.liveWorkers.Load(), InFlight: m.inFlight.Load(), PUBACK: m.puback.Load(), PublishFailures: m.publishFailures.Load(), PublishedMarkSuccessfulCalls: m.markSuccessCalls.Load(), MarkErrors: m.markErrors.Load()}
}

package mqttingress

import (
	"sync/atomic"
	"time"

	"iot-platform/internal/metrics"
)

// counters holds bounded-cardinality ingress counters. Every counter is a
// fixed field (no maps keyed by sender-supplied values), so cardinality cannot
// grow with input.
type counters struct {
	received, receivedBytes, acceptedBytes, inFlight     atomic.Int64
	bufferedPayloadBytes, bufferedPayloadHighWaterBytes  atomic.Int64
	accepted                                             atomic.Int64
	rejectedNotRunning                                   atomic.Int64
	rejectedStopping                                     atomic.Int64
	rejectedRetained                                     atomic.Int64
	rejectedOversized                                    atomic.Int64
	rejectedInvalidTpc                                   atomic.Int64
	queueFull                                            atomic.Int64
	byteBudgetFull                                       atomic.Int64
	shaperPassed, shaperRateLimited, shaperStateCapacity atomic.Int64
	queueHighWater                                       atomic.Int64

	processed          atomic.Int64
	validationRejected atomic.Int64
	persistFailures    atomic.Int64
	commitUnconfirmed  atomic.Int64
	identityMismatch   atomic.Int64
	ackBuildFailures   atomic.Int64

	committed          atomic.Int64
	duplicateCommitted atomic.Int64
	samplesNew         atomic.Int64
	ackPublished       atomic.Int64
	ackPublishFailures atomic.Int64

	processingNanos atomic.Int64
	processingCount atomic.Int64

	groupCount, groupMembers, groupRows, groupPayloadBytes      atomic.Int64
	groupMaxMembers                                             atomic.Int64
	flushMessages, flushRows, flushBytes, flushTimer, flushStop atomic.Int64
	standaloneOversized                                         atomic.Int64
}

// Snapshot is a point-in-time, bounded view of ingress counters and queue state.
// It contains no payload, credential or driver error data.
type Snapshot struct {
	Received, ReceivedBytes, AcceptedBytes, InFlight int64
	Durations                                        DurationSnapshot
	Microbatch                                       MicrobatchSnapshot `json:"microbatch"`
	RAMDedup                                         DedupCacheSnapshot `json:"ram_dedup"`
	// Admission.
	Accepted                      int64
	RejectedNotRunning            int64
	RejectedStopping              int64
	RejectedRetained              int64
	RejectedOversized             int64
	RejectedInvalidTopic          int64
	QueueFull                     int64
	BufferedPayloadBytes          int64
	BufferedPayloadCapacityBytes  int64
	BufferedPayloadHighWaterBytes int64
	ByteBudgetFull                int64
	ShaperPassed                  int64
	ShaperRateLimited             int64
	ShaperStateCapacity           int64
	ShaperEnabled                 bool
	ShaperEntries                 int
	ShaperCapacity                int

	// Worker outcomes.
	Processed          int64
	ValidationRejected int64
	PersistFailures    int64
	CommitUnconfirmed  int64
	IdentityMismatch   int64
	ACKBuildFailures   int64

	// Confirmed database commits (independent of ACK delivery).
	Committed          int64
	DuplicateCommitted int64
	SamplesNew         int64
	// Post-commit ACK publication results.
	ACKPublished       int64
	ACKPublishFailures int64

	// Processing latency sums completed delivery lifetimes from worker dequeue
	// through release (including batch wait, persistence and ACK), not SQL time.
	// These legacy aggregates remain populated even when MetricsEnabled is false.
	ProcessingLatencyTotal time.Duration
	ProcessingCount        int64

	// Queue gauge: approximate number of items waiting (excluding items a
	// worker has already taken) and the fixed capacity.
	QueueDepth    int
	QueueCapacity int

	// QueueHighWater is the highest admission-observed queue depth, sampled via
	// len(queue) immediately following each successful non-blocking enqueue in
	// Admit().
	// Observation point: sampled in Admit() immediately after case i.queue <- it succeeds.
	// Limits: bounded within [0, QueueCapacity]; monotonically non-decreasing.
	// Note: Concurrent workers may consume items from the channel between the
	// channel send and the len() observation, so this represents an
	// admission-observed high-water mark rather than an absolute instantaneous
	// queue peak.
	QueueHighWater int
}

// Snapshot returns the current counters and queue state.
func (i *Ingress) Snapshot() Snapshot {
	shaperEntries, shaperCapacity := i.shaper.EntriesAndCap()
	return Snapshot{
		Received: i.counters.received.Load(), ReceivedBytes: i.counters.receivedBytes.Load(),
		AcceptedBytes: i.counters.acceptedBytes.Load(), InFlight: i.counters.inFlight.Load(),
		RAMDedup:                      i.dedupCache.Snapshot(),
		Durations:                     DurationSnapshot{QueueWait: i.queueWait.Snapshot(), Validation: i.validation.Snapshot(), Persistence: i.persistence.Snapshot(), ACKPublish: i.ackPublish.Snapshot(), BatchWait: i.batchWait.Snapshot(), GroupPersistence: i.groupPersistence.Snapshot()},
		Microbatch:                    MicrobatchSnapshot{Enabled: i.microbatch.Enabled, MaxMessages: i.microbatch.MaxMessages, MaxRows: i.microbatch.MaxRows, MaxPayloadBytes: i.microbatch.MaxPayloadBytes, MaxWait: i.microbatch.MaxWait, Groups: i.counters.groupCount.Load(), Members: i.counters.groupMembers.Load(), Rows: i.counters.groupRows.Load(), PayloadBytes: i.counters.groupPayloadBytes.Load(), MaxMembers: i.counters.groupMaxMembers.Load(), FlushMessages: i.counters.flushMessages.Load(), FlushRows: i.counters.flushRows.Load(), FlushBytes: i.counters.flushBytes.Load(), FlushTimer: i.counters.flushTimer.Load(), FlushStop: i.counters.flushStop.Load(), StandaloneOversized: i.counters.standaloneOversized.Load()},
		Accepted:                      i.counters.accepted.Load(),
		RejectedNotRunning:            i.counters.rejectedNotRunning.Load(),
		RejectedStopping:              i.counters.rejectedStopping.Load(),
		RejectedRetained:              i.counters.rejectedRetained.Load(),
		RejectedOversized:             i.counters.rejectedOversized.Load(),
		RejectedInvalidTopic:          i.counters.rejectedInvalidTpc.Load(),
		QueueFull:                     i.counters.queueFull.Load(),
		BufferedPayloadBytes:          i.counters.bufferedPayloadBytes.Load(),
		BufferedPayloadCapacityBytes:  i.bufferedPayloadCapacityBytes,
		BufferedPayloadHighWaterBytes: i.counters.bufferedPayloadHighWaterBytes.Load(),
		ByteBudgetFull:                i.counters.byteBudgetFull.Load(),
		ShaperPassed:                  i.counters.shaperPassed.Load(),
		ShaperRateLimited:             i.counters.shaperRateLimited.Load(),
		ShaperStateCapacity:           i.counters.shaperStateCapacity.Load(),
		ShaperEnabled:                 i.shaper.options.Enabled,
		ShaperEntries:                 shaperEntries,
		ShaperCapacity:                shaperCapacity,

		Processed:          i.counters.processed.Load(),
		ValidationRejected: i.counters.validationRejected.Load(),
		PersistFailures:    i.counters.persistFailures.Load(),
		CommitUnconfirmed:  i.counters.commitUnconfirmed.Load(),
		IdentityMismatch:   i.counters.identityMismatch.Load(),
		ACKBuildFailures:   i.counters.ackBuildFailures.Load(),

		Committed:          i.counters.committed.Load(),
		DuplicateCommitted: i.counters.duplicateCommitted.Load(),
		SamplesNew:         i.counters.samplesNew.Load(),
		ACKPublished:       i.counters.ackPublished.Load(),
		ACKPublishFailures: i.counters.ackPublishFailures.Load(),

		ProcessingLatencyTotal: time.Duration(i.counters.processingNanos.Load()),
		ProcessingCount:        i.counters.processingCount.Load(),

		QueueDepth:     len(i.queue),
		QueueCapacity:  cap(i.queue),
		QueueHighWater: int(i.counters.queueHighWater.Load()),
	}
}

// DurationSnapshot records only boundaries actually executed, not SQL-only time.
// Persistence is retained as per-delivery observations. GroupPersistence is a
// distinct per-group series and therefore observes one duration per SQL batch.
type DurationSnapshot struct {
	QueueWait        metrics.HistogramSnapshot `json:"queue_wait"`
	Validation       metrics.HistogramSnapshot `json:"validation"`
	Persistence      metrics.HistogramSnapshot `json:"persistence"`
	ACKPublish       metrics.HistogramSnapshot `json:"ack_publish"`
	BatchWait        metrics.HistogramSnapshot `json:"batch_wait"`
	GroupPersistence metrics.HistogramSnapshot `json:"group_persistence"`
}

// MicrobatchSnapshot reports effective group bounds and fixed-cardinality counters.
type MicrobatchSnapshot struct {
	Enabled             bool          `json:"enabled"`
	MaxMessages         int           `json:"max_messages"`
	MaxRows             int           `json:"max_rows"`
	MaxPayloadBytes     int64         `json:"max_payload_bytes"`
	MaxWait             time.Duration `json:"max_wait_ns"`
	Groups              int64         `json:"groups"`
	Members             int64         `json:"members"`
	Rows                int64         `json:"rows"`
	PayloadBytes        int64         `json:"payload_bytes"`
	MaxMembers          int64         `json:"max_members"`
	FlushMessages       int64         `json:"flush_messages"`
	FlushRows           int64         `json:"flush_rows"`
	FlushBytes          int64         `json:"flush_bytes"`
	FlushTimer          int64         `json:"flush_timer"`
	FlushStop           int64         `json:"flush_stop"`
	StandaloneOversized int64         `json:"standalone_oversized_members"`
}

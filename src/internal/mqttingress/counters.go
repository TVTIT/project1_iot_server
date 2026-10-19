package mqttingress

import (
	"sync/atomic"
	"time"
)

// counters holds bounded-cardinality ingress counters. Every counter is a
// fixed field (no maps keyed by sender-supplied values), so cardinality cannot
// grow with input.
type counters struct {
	accepted           atomic.Int64
	rejectedNotRunning atomic.Int64
	rejectedStopping   atomic.Int64
	rejectedRetained   atomic.Int64
	rejectedOversized  atomic.Int64
	rejectedInvalidTpc atomic.Int64
	queueFull          atomic.Int64
	queueHighWater     atomic.Int64

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
}

// Snapshot is a point-in-time, bounded view of ingress counters and queue state.
// It contains no payload, credential or driver error data.
type Snapshot struct {
	// Admission.
	Accepted             int64
	RejectedNotRunning   int64
	RejectedStopping     int64
	RejectedRetained     int64
	RejectedOversized    int64
	RejectedInvalidTopic int64
	QueueFull            int64

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

	// Processing latency aggregate over processed items.
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
	return Snapshot{
		Accepted:             i.counters.accepted.Load(),
		RejectedNotRunning:   i.counters.rejectedNotRunning.Load(),
		RejectedStopping:     i.counters.rejectedStopping.Load(),
		RejectedRetained:     i.counters.rejectedRetained.Load(),
		RejectedOversized:    i.counters.rejectedOversized.Load(),
		RejectedInvalidTopic: i.counters.rejectedInvalidTpc.Load(),
		QueueFull:            i.counters.queueFull.Load(),

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

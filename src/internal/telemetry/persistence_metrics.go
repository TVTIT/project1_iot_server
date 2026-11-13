package telemetry

import (
	"sync/atomic"
	"time"

	"iot-platform/internal/metrics"
)

// PersistenceDurationsSnapshot reports client-observed call durations in seconds.
// Counts are executed calls, including calls that return an error.
type PersistenceDurationsSnapshot struct {
	Begin         metrics.HistogramSnapshot `json:"begin"`
	GatewayLock   metrics.HistogramSnapshot `json:"gateway_lock"`
	SensorLock    metrics.HistogramSnapshot `json:"sensor_lock"`
	GraphLookup   metrics.HistogramSnapshot `json:"graph_lookup"`
	EntityLock    metrics.HistogramSnapshot `json:"entity_lock"`
	StateLock     metrics.HistogramSnapshot `json:"state_lock"`
	Ledger        metrics.HistogramSnapshot `json:"ledger"`
	Copy          metrics.HistogramSnapshot `json:"copy"`
	ReportedState metrics.HistogramSnapshot `json:"reported_state"`
	Temporal      metrics.HistogramSnapshot `json:"temporal"`
	Commit        metrics.HistogramSnapshot `json:"commit"`
}

// PersistenceMetrics is a bounded collector for actual telemetry repository operations.
// Its zero value is usable; repositories may omit it without affecting persistence.
type PersistenceMetrics struct {
	singleAttempts     atomic.Uint64
	groupAttempts      atomic.Uint64
	groupMembers       atomic.Uint64
	groupRows          atomic.Uint64
	groupPayloadBytes  atomic.Uint64
	fallbackAttempts   atomic.Uint64
	beginAttempts      atomic.Uint64
	beginSucceeded     atomic.Uint64
	confirmedCommits   atomic.Uint64
	rollbackAttempts   atomic.Uint64
	confirmedRollbacks atomic.Uint64
	commitUnconfirmed  atomic.Uint64
	copyCalls          atomic.Uint64
	copyReturnedRows   atomic.Uint64
	confirmedNewRows   atomic.Uint64
	durations          persistenceDurations
}

type persistenceDurations struct {
	begin, gatewayLock, sensorLock, graphLookup metrics.Histogram
	entityLock, stateLock, ledger, copy         metrics.Histogram
	reportedState, temporal, commit             metrics.Histogram
}

func observePersistenceDuration(histogram *metrics.Histogram, started time.Time) {
	histogram.Observe(time.Since(started))
}

// PersistenceMetricsSnapshot reports operation counts; rows from failed/rolled-back
// transactions remain in CopyReturnedRows but are excluded from ConfirmedNewRows.
type PersistenceMetricsSnapshot struct {
	SingleAttempts     uint64                       `json:"single_attempts"`
	GroupAttempts      uint64                       `json:"group_attempts"`
	GroupMembers       uint64                       `json:"group_members"`
	GroupRows          uint64                       `json:"group_rows"`
	GroupPayloadBytes  uint64                       `json:"group_payload_bytes"`
	FallbackAttempts   uint64                       `json:"fallback_attempts"`
	BeginAttempts      uint64                       `json:"begin_attempts"`
	BeginSucceeded     uint64                       `json:"begin_succeeded"`
	ConfirmedCommits   uint64                       `json:"confirmed_commits"`
	RollbackAttempts   uint64                       `json:"rollback_attempts"`
	ConfirmedRollbacks uint64                       `json:"confirmed_rollbacks"`
	CommitUnconfirmed  uint64                       `json:"commit_unconfirmed"`
	CopyCalls          uint64                       `json:"copy_calls"`
	CopyReturnedRows   uint64                       `json:"copy_returned_rows"`
	ConfirmedNewRows   uint64                       `json:"confirmed_new_rows"`
	Durations          PersistenceDurationsSnapshot `json:"durations"`
}

// Snapshot returns a bounded counter snapshot. Concurrent updates may span snapshots.
func (m *PersistenceMetrics) Snapshot() PersistenceMetricsSnapshot {
	if m == nil {
		return PersistenceMetricsSnapshot{}
	}
	return PersistenceMetricsSnapshot{
		SingleAttempts: m.singleAttempts.Load(), GroupAttempts: m.groupAttempts.Load(),
		GroupMembers: m.groupMembers.Load(), GroupRows: m.groupRows.Load(), GroupPayloadBytes: m.groupPayloadBytes.Load(),
		FallbackAttempts: m.fallbackAttempts.Load(), BeginAttempts: m.beginAttempts.Load(), BeginSucceeded: m.beginSucceeded.Load(),
		ConfirmedCommits: m.confirmedCommits.Load(), RollbackAttempts: m.rollbackAttempts.Load(), ConfirmedRollbacks: m.confirmedRollbacks.Load(),
		CommitUnconfirmed: m.commitUnconfirmed.Load(), CopyCalls: m.copyCalls.Load(), CopyReturnedRows: m.copyReturnedRows.Load(), ConfirmedNewRows: m.confirmedNewRows.Load(),
		Durations: PersistenceDurationsSnapshot{
			Begin: m.durations.begin.Snapshot(), GatewayLock: m.durations.gatewayLock.Snapshot(), SensorLock: m.durations.sensorLock.Snapshot(),
			GraphLookup: m.durations.graphLookup.Snapshot(), EntityLock: m.durations.entityLock.Snapshot(), StateLock: m.durations.stateLock.Snapshot(),
			Ledger: m.durations.ledger.Snapshot(), Copy: m.durations.copy.Snapshot(), ReportedState: m.durations.reportedState.Snapshot(),
			Temporal: m.durations.temporal.Snapshot(), Commit: m.durations.commit.Snapshot(),
		},
	}
}

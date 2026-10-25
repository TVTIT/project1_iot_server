package telemetry

import "sync/atomic"

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
}

// PersistenceMetricsSnapshot reports operation counts; rows from failed/rolled-back
// transactions remain in CopyReturnedRows but are excluded from ConfirmedNewRows.
type PersistenceMetricsSnapshot struct {
	SingleAttempts     uint64 `json:"single_attempts"`
	GroupAttempts      uint64 `json:"group_attempts"`
	GroupMembers       uint64 `json:"group_members"`
	GroupRows          uint64 `json:"group_rows"`
	GroupPayloadBytes  uint64 `json:"group_payload_bytes"`
	FallbackAttempts   uint64 `json:"fallback_attempts"`
	BeginAttempts      uint64 `json:"begin_attempts"`
	BeginSucceeded     uint64 `json:"begin_succeeded"`
	ConfirmedCommits   uint64 `json:"confirmed_commits"`
	RollbackAttempts   uint64 `json:"rollback_attempts"`
	ConfirmedRollbacks uint64 `json:"confirmed_rollbacks"`
	CommitUnconfirmed  uint64 `json:"commit_unconfirmed"`
	CopyCalls          uint64 `json:"copy_calls"`
	CopyReturnedRows   uint64 `json:"copy_returned_rows"`
	ConfirmedNewRows   uint64 `json:"confirmed_new_rows"`
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
	}
}

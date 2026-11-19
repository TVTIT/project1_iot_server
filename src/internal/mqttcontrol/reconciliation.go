package mqttcontrol

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"iot-platform/internal/digitaltwin"
)

// ReconciliationSnapshotRepository atomically rechecks a candidate and may
// commit one finite decision and one budgeted successor.
type ReconciliationSnapshotRepository interface {
	ReconcileSnapshot(context.Context, digitaltwin.ControlQueryContext, digitaltwin.ConfigurationSnapshot, time.Time) (digitaltwin.ReconciliationOutcome, error)
}

// ReconciliationMetrics contains fixed-cardinality counters only.
type ReconciliationMetrics struct {
	matched, converged, mismatch, successorCreated atomic.Int64
	activeBusy, blockedCancelled, blockedBudget    atomic.Int64
	manual, unavailable, precondition              atomic.Int64
	staleContext, deadline, cancelled              atomic.Int64
	dependencyError, commitUncertain, conflict     atomic.Int64
	internalError                                  atomic.Int64
}

// ReconciliationMetricsSnapshot contains no command, Sensor, Gateway, or user identifiers.
type ReconciliationMetricsSnapshot struct {
	Matched          int64 `json:"matched"`
	Converged        int64 `json:"converged"`
	Mismatch         int64 `json:"mismatch"`
	SuccessorCreated int64 `json:"successor_created"`
	ActiveBusy       int64 `json:"active_busy"`
	BlockedCancelled int64 `json:"blocked_cancelled"`
	BlockedBudget    int64 `json:"blocked_budget"`
	Manual           int64 `json:"manual"`
	Unavailable      int64 `json:"unavailable"`
	Precondition     int64 `json:"precondition"`
	StaleContext     int64 `json:"stale_context"`
	Deadline         int64 `json:"deadline"`
	Cancelled        int64 `json:"cancelled"`
	DependencyError  int64 `json:"dependency_error"`
	CommitUncertain  int64 `json:"commit_uncertain"`
	Conflict         int64 `json:"conflict"`
	InternalError    int64 `json:"internal_error"`
}

// Snapshot returns fixed-cardinality reconciliation outcome counters.
func (m *ReconciliationMetrics) Snapshot() ReconciliationMetricsSnapshot {
	if m == nil {
		return ReconciliationMetricsSnapshot{}
	}
	return ReconciliationMetricsSnapshot{
		Matched: m.matched.Load(), Converged: m.converged.Load(),
		Mismatch: m.mismatch.Load(), SuccessorCreated: m.successorCreated.Load(),
		ActiveBusy: m.activeBusy.Load(), BlockedCancelled: m.blockedCancelled.Load(),
		BlockedBudget: m.blockedBudget.Load(), Manual: m.manual.Load(),
		Unavailable: m.unavailable.Load(), Precondition: m.precondition.Load(),
		StaleContext: m.staleContext.Load(), Deadline: m.deadline.Load(),
		Cancelled: m.cancelled.Load(), DependencyError: m.dependencyError.Load(),
		CommitUncertain: m.commitUncertain.Load(), Conflict: m.conflict.Load(),
		InternalError: m.internalError.Load(),
	}
}

// NewReconciliationSnapshotHandler synchronously consumes one correlated
// candidate on the existing bounded ResultConsumer worker. The repository's
// transaction remains the authoritative context/lineage/snapshot check.
func NewReconciliationSnapshotHandler(
	repo ReconciliationSnapshotRepository,
	metrics *ReconciliationMetrics,
) ControlSnapshotHandler {
	return func(ctx context.Context, candidate ControlSnapshotCandidate) {
		if ctx == nil {
			if metrics != nil {
				metrics.cancelled.Add(1)
			}
			return
		}
		if ctx.Err() != nil {
			if metrics != nil {
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					metrics.deadline.Add(1)
				} else {
					metrics.cancelled.Add(1)
				}
			}
			return
		}
		if candidate.LocalDeadline.IsZero() || !time.Now().Before(candidate.LocalDeadline) {
			if metrics != nil {
				metrics.deadline.Add(1)
			}
			return
		}
		if nilDependency(repo) {
			if metrics != nil {
				metrics.dependencyError.Add(1)
			}
			return
		}
		bounded, cancel := context.WithDeadline(ctx, candidate.LocalDeadline)
		defer cancel()
		outcome, err := repo.ReconcileSnapshot(
			bounded, candidate.Query, candidate.Snapshot, candidate.LocalDeadline,
		)
		if metrics == nil {
			return
		}
		if err != nil {
			metrics.observeError(err)
			return
		}
		metrics.observeOutcome(outcome.Decision)
	}
}

func (m *ReconciliationMetrics) observeError(err error) {
	switch {
	case errors.Is(err, digitaltwin.ErrControlCommitUncertain):
		m.commitUncertain.Add(1)
	case errors.Is(err, digitaltwin.ErrControlQueryContextChanged):
		m.staleContext.Add(1)
	case errors.Is(err, digitaltwin.ErrControlQueryExpired),
		errors.Is(err, context.DeadlineExceeded):
		m.deadline.Add(1)
	case errors.Is(err, context.Canceled):
		m.cancelled.Add(1)
	case errors.Is(err, digitaltwin.ErrControlCapacity):
		m.conflict.Add(1)
	case errors.Is(err, digitaltwin.ErrDependencyUnavailable):
		m.dependencyError.Add(1)
	case errors.Is(err, digitaltwin.ErrInvalidInput),
		errors.Is(err, digitaltwin.ErrNotFound),
		errors.Is(err, digitaltwin.ErrInconsistentGraph):
		m.conflict.Add(1)
	default:
		m.internalError.Add(1)
	}
}

func (m *ReconciliationMetrics) observeOutcome(decision digitaltwin.ReconciliationDecision) {
	m.matched.Add(1)
	switch decision {
	case digitaltwin.ReconciliationConverged:
		m.converged.Add(1)
	case digitaltwin.ReconciliationSuccessorCreated:
		m.mismatch.Add(1)
		m.successorCreated.Add(1)
	case digitaltwin.ReconciliationActiveBusy:
		m.activeBusy.Add(1)
	case digitaltwin.ReconciliationBlockedCancelled:
		m.blockedCancelled.Add(1)
	case digitaltwin.ReconciliationBlockedBudget:
		m.blockedBudget.Add(1)
	case digitaltwin.ReconciliationManualUnconfirmed:
		m.manual.Add(1)
	case digitaltwin.ReconciliationCorruptUnavailable:
		m.unavailable.Add(1)
	case digitaltwin.ReconciliationPreconditionUnmet:
		m.precondition.Add(1)
	default:
		m.conflict.Add(1)
	}
}

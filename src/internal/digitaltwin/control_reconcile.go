package digitaltwin

import "errors"

const maxAutomaticReconcileSuccessors = 1

var (
	// ErrControlQueryContextChanged marks a transient snapshot whose database context is stale.
	ErrControlQueryContextChanged = errors.New("control query context changed")
	// ErrControlQueryExpired marks a candidate whose backend-local deadline has elapsed.
	ErrControlQueryExpired = errors.New("control query expired")
)

// ReconciliationDecision is a finite persisted decision value from migration 27.
type ReconciliationDecision string

const (
	// ReconciliationConverged records exact agreement across server and Gateway evidence.
	ReconciliationConverged ReconciliationDecision = "converged"
	// ReconciliationSuccessorCreated records atomic creation of the one allowed child command.
	ReconciliationSuccessorCreated ReconciliationDecision = "successor_created"
	// ReconciliationActiveBusy records an existing unexpired active command.
	ReconciliationActiveBusy ReconciliationDecision = "active_busy"
	// ReconciliationBlockedCancelled records a durable cancellation block.
	ReconciliationBlockedCancelled ReconciliationDecision = "blocked_cancelled"
	// ReconciliationBlockedBudget records an exhausted root successor budget.
	ReconciliationBlockedBudget ReconciliationDecision = "blocked_budget"
	// ReconciliationManualUnconfirmed records Gateway evidence requiring operator review.
	ReconciliationManualUnconfirmed ReconciliationDecision = "manual_unconfirmed"
	// ReconciliationCorruptUnavailable records unavailable or unsupported Gateway state.
	ReconciliationCorruptUnavailable ReconciliationDecision = "corrupt_unavailable"
	// ReconciliationPreconditionUnmet records absence of committed user intent.
	ReconciliationPreconditionUnmet ReconciliationDecision = "precondition_unmet"
)

// ReconciliationInput contains only values read or validated for one locked Sensor.
// It is not an authorization grant; callers must evaluate it after transaction locks.
type ReconciliationInput struct {
	DesiredVersion                Version
	DesiredIntervalUS             int64
	ConfirmedConfigurationVersion Version
	ReportedIntervalUS            *int64
	HasCommittedUserIntent        bool
	ActiveCommand                 bool
	ManualUnconfirmed             bool
	Cancelled                     bool
	AutoSuccessorsUsed            int
	Snapshot                      ConfigurationSnapshot
}

// ReconciliationOutcome reports a committed decision. CommandID is empty unless
// Decision is successor_created.
type ReconciliationOutcome struct {
	Decision       ReconciliationDecision
	CommandID      string
	DesiredVersion Version
}

func decideControlReconciliation(input ReconciliationInput) (ReconciliationDecision, error) {
	if input.DesiredVersion < 0 || input.DesiredIntervalUS < MinSampleIntervalUS ||
		input.DesiredIntervalUS > MaxSampleIntervalUS || input.ConfirmedConfigurationVersion < 0 ||
		input.AutoSuccessorsUsed < 0 || input.AutoSuccessorsUsed > maxAutomaticReconcileSuccessors {
		return "", ErrInvalidInput
	}
	if input.ReportedIntervalUS != nil && (*input.ReportedIntervalUS < MinSampleIntervalUS ||
		*input.ReportedIntervalUS > MaxSampleIntervalUS) {
		return "", ErrInvalidInput
	}
	snapshot := input.Snapshot
	if snapshot.WatermarkVersion < 0 {
		return "", ErrInvalidInput
	}
	for _, interval := range []*int64{snapshot.DurableSampleIntervalUS, snapshot.ActualSampleIntervalUS} {
		if interval != nil && (*interval < MinSampleIntervalUS || *interval > MaxSampleIntervalUS) {
			return "", ErrInvalidInput
		}
	}
	if snapshot.ConfigurationState != "configured" &&
		snapshot.ConfigurationState != "unconfigured" &&
		snapshot.ConfigurationState != "unavailable" {
		return "", ErrInvalidInput
	}
	if snapshot.ConfigurationState == "configured" &&
		(snapshot.DurableSampleIntervalUS == nil || snapshot.ActualSampleIntervalUS == nil) {
		input.ManualUnconfirmed = true
	}
	switch {
	case input.ActiveCommand:
		return ReconciliationActiveBusy, nil
	case !input.HasCommittedUserIntent:
		return ReconciliationPreconditionUnmet, nil
	case input.ManualUnconfirmed || snapshot.WatermarkVersion > input.DesiredVersion:
		return ReconciliationManualUnconfirmed, nil
	case snapshot.ConfigurationState == "unavailable":
		return ReconciliationCorruptUnavailable, nil
	case input.Cancelled:
		return ReconciliationBlockedCancelled, nil
	case input.AutoSuccessorsUsed >= maxAutomaticReconcileSuccessors:
		return ReconciliationBlockedBudget, nil
	}
	if snapshot.ConfigurationState == "unconfigured" && snapshot.ActualSampleIntervalUS == nil {
		return ReconciliationManualUnconfirmed, nil
	}

	if input.ReportedIntervalUS != nil &&
		*input.ReportedIntervalUS == input.DesiredIntervalUS &&
		input.ConfirmedConfigurationVersion == input.DesiredVersion &&
		snapshot.ConfigurationState == "configured" &&
		snapshot.WatermarkVersion == input.DesiredVersion &&
		snapshot.DurableSampleIntervalUS != nil &&
		*snapshot.DurableSampleIntervalUS == input.DesiredIntervalUS &&
		snapshot.ActualSampleIntervalUS != nil &&
		*snapshot.ActualSampleIntervalUS == input.DesiredIntervalUS {
		return ReconciliationConverged, nil
	}
	return ReconciliationSuccessorCreated, nil
}

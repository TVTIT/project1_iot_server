package mqttcredential

import (
	"context"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// RecoveryStatus defines the status of a credential recovery operation.
type RecoveryStatus string

// RecoveryStatus constants define valid recovery record statuses.
const (
	RecoveryPending  RecoveryStatus = "pending"
	RecoveryDisabled RecoveryStatus = "disabled"
	RecoveryStartup                 = "startup"
	// OperationResolvedByRecovery is a read/replay disposition, never a replacement historical event status.
	OperationResolvedByRecovery OperationStatus = "resolved_by_recovery"
)

// RecoveryRecord tracks state for a startup recovery attempt. Trusted startup system attribution: no human actor and no platform-admin
// authority. This separate capability must never be exposed by HTTP handlers.
// Epoch identifies the freshly loaded broker where cleanup must be observed.
type RecoveryRecord struct {
	RecoveryID, OperationID uuid.UUID
	GatewayID               string
	AttemptedVersion        int64
	Origin                  string
	Status                  RecoveryStatus
	BrokerEpoch             string
	Evidence                RecoveryDisableEvidence
	CreatedAt, UpdatedAt    time.Time
	CompletedAt             *time.Time
}

// RecoveryDisableEvidence confirms client disable across RAM and snapshot. Disable evidence is neither positive-password proof nor an fsync receipt.
type RecoveryDisableEvidence struct {
	RAMDisabled      bool
	SnapshotObserved bool
	BrokerEpoch      string
}

// Validate checks that disable evidence matches the expected broker epoch.
func (e RecoveryDisableEvidence) Validate(epoch string) error {
	if !e.RAMDisabled || !e.SnapshotObserved || !validEpoch(epoch) || e.BrokerEpoch != epoch {
		return &DomainError{Code: CodeInvalidRequest}
	}
	return nil
}

// Validate validates internal consistency of a recovery record.
func (r RecoveryRecord) Validate() error {
	if r.RecoveryID == uuid.Nil || r.OperationID == uuid.Nil || gateway.ValidateGatewayID(r.GatewayID) != nil || r.AttemptedVersion <= 0 || r.Origin != RecoveryStartup || !validEpoch(r.BrokerEpoch) {
		return &DomainError{Code: CodeInvalidRequest}
	}
	if r.Status == RecoveryPending && r.CompletedAt == nil && r.Evidence == (RecoveryDisableEvidence{}) {
		return nil
	}
	if r.Status == RecoveryDisabled && r.CompletedAt != nil && !r.CompletedAt.IsZero() && r.Evidence.Validate(r.BrokerEpoch) == nil {
		return nil
	}
	return &DomainError{Code: CodeInvalidRequest}
}

// RecoveryRequest captures parameters for initiating or verifying a recovery attempt.
type RecoveryRequest struct {
	RecoveryID  uuid.UUID // server-generated before intent; retry uses same UUID
	Guard       MaintenanceGuard
	BrokerEpoch string // fresh verified lifetime, not the previous checkpoint epoch
}

// RecoveryRepository provides database storage for startup recovery tracking. Pending failures remain pending and keep the fence. New broker lifetimes bind
// via CAS before another disable attempt; never reuse evidence from the old epoch.
// BeginRecovery returning pending is not execution authority: startup must recheck
// the current guard and trusted controller lifetime under its single-worker gate.
// CommitOutcomeUnknown.OperationID carries the recovery UUID for these methods;
// resolve using ResolveRecovery, not ResolveCommitAmbiguity/ResolveMaintenance.
type RecoveryRepository interface {
	BeginRecovery(context.Context, RecoveryRequest) (RecoveryRecord, error)
	BindRecoveryEpoch(context.Context, uuid.UUID, string, string) (RecoveryRecord, error)
	CompleteRecovery(context.Context, RecoveryRequest, RecoveryDisableEvidence) (Metadata, error)
	ResolveRecovery(context.Context, uuid.UUID) (RecoveryRecord, error)
	ListPendingRecovery(context.Context, uuid.UUID, int) ([]RecoveryRecord, error)
}

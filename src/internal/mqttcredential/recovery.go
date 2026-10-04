package mqttcredential

import (
	"context"
	"time"

	"github.com/google/uuid"
	"iot-platform/internal/gateway"
)

type RecoveryStatus string

const (
	RecoveryPending  RecoveryStatus = "pending"
	RecoveryDisabled RecoveryStatus = "disabled"
	RecoveryStartup                 = "startup"
	// A read/replay disposition, never a replacement historical event status.
	OperationResolvedByRecovery OperationStatus = "resolved_by_recovery"
)

// Trusted startup system attribution: no human actor and no platform-admin
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

// Disable evidence is neither positive-password proof nor an fsync receipt.
type RecoveryDisableEvidence struct {
	RAMDisabled      bool
	SnapshotObserved bool
	BrokerEpoch      string
}

func (e RecoveryDisableEvidence) Validate(epoch string) error {
	if !e.RAMDisabled || !e.SnapshotObserved || !validEpoch(epoch) || e.BrokerEpoch != epoch {
		return &DomainError{Code: CodeInvalidRequest}
	}
	return nil
}

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

type RecoveryRequest struct {
	RecoveryID  uuid.UUID // server-generated before intent; retry uses same UUID
	Guard       MaintenanceGuard
	BrokerEpoch string // fresh verified lifetime, not the previous checkpoint epoch
}

// Pending failures remain pending and keep the fence. New broker lifetimes bind
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

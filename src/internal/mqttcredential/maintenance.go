package mqttcredential

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type MaintenanceStatus string

const (
	MaintenanceInProgress     MaintenanceStatus = "in_progress"
	MaintenanceCompleted      MaintenanceStatus = "completed"
	MaintenanceRecoveryNeeded MaintenanceStatus = "recovery_needed"
)

// MaintenanceCheckpoint records business progress, NOT broker authentication,
// device_updated, password delivery, or automatic gate closure on backend death.
// Completed rows are immutable. Startup handles only unresolved rows; subsequent
// controller lifetimes must reconcile current durable decisions independently.
type MaintenanceCheckpoint struct {
	OperationID          uuid.UUID
	GatewayID            string
	Status               MaintenanceStatus
	BrokerEpoch          *string
	ErrorCode            *ErrorCode
	CreatedAt, UpdatedAt time.Time
	CompletedAt          *time.Time
	RecoveryID           *uuid.UUID
}

// Epoch is a nonsecret broker lifetime identity from the controller challenge, never
// created_at. CAS rebinding invalidates previous lifetime ACKs. The service must
// serialize global runtime execution; per-Gateway queued intents are permitted.
type MaintenanceGuard struct {
	OperationGuard
	ExpectedStatus MaintenanceStatus
	ExpectedEpoch  *string
}

// Separate capability preserves the existing Repository and its test doubles.
// CompleteMaintenance is called only AFTER reconcile + epoch-correlated OPEN
// ACK; secret return requires its confirmed commit. Unknown commit or absent
// lookup never authorizes runtime replay or secret return.
type MaintenanceRepository interface {
	ResolveMaintenance(context.Context, uuid.UUID) (MaintenanceCheckpoint, error)
	ListPendingMaintenance(context.Context, uuid.UUID, int) ([]MaintenanceCheckpoint, error)
	BindMaintenanceEpoch(context.Context, MaintenanceGuard, string) (MaintenanceCheckpoint, error)
	RequireMaintenanceRecovery(context.Context, MaintenanceGuard, ErrorCode) (MaintenanceCheckpoint, error)
	CompleteMaintenance(context.Context, MaintenanceGuard) (MaintenanceCheckpoint, error)
}

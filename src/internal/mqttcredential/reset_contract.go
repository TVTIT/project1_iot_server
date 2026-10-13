package mqttcredential

import (
	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// ResetReason represents the explicit operator intent for a manual reset.
// A reason is operator intent, NOT proof that a response was lost or quiescence achieved.
type ResetReason string

const (
	// ResetReasonRecoveryAfterUnknown is for recovering after an unresolved,
	// interrupted or unknown mutation attempt (pending or recovery_needed).
	ResetReasonRecoveryAfterUnknown ResetReason = "recovery_after_unknown"

	// ResetReasonLostSecretRecovery is for recovering when a password-issuing
	// operation succeeded in DB and on Mosquitto, but the one-time secret was lost
	// before disclosure to the operator.
	ResetReasonLostSecretRecovery ResetReason = "lost_secret_recovery"
)

// Validate rejects empty, unknown, normalized, or arbitrary operator reasons.
func (r ResetReason) Validate() error {
	switch r {
	case ResetReasonRecoveryAfterUnknown, ResetReasonLostSecretRecovery:
		return nil
	default:
		return &DomainError{Code: CodeInvalidRequest}
	}
}

// ResetInput conveys parameters for a planned dedicated operator-supervised reset use case.
// It contains GatewayID, IdempotencyKey, and Reason.
// No client password, version, native commands, broker epoch, verified/force-ready, or quiescence boolean.
// Actor is passed separately from the trusted principal.
type ResetInput struct {
	GatewayID      string
	IdempotencyKey uuid.UUID
	Reason         ResetReason
}

// ValidateResetInput checks identity and reason syntax only.
// Syntax validation does NOT establish platform-admin authority, Gateway existence,
// operation ownership, or broker quiescence.
func ValidateResetInput(actorUserID uuid.UUID, input ResetInput) error {
	if actorUserID == uuid.Nil || input.IdempotencyKey == uuid.Nil || gateway.ValidateGatewayID(input.GatewayID) != nil {
		return &DomainError{Code: CodeInvalidRequest}
	}
	return input.Reason.Validate()
}

// CheckResetEligibility evaluates the minimum pure eligibility check for manual reset
// against current metadata and the current operation record.
//
// Eligibility rules:
//  1. ResetReason must be valid.
//  2. Operation and Metadata identities must be non-nil, non-empty, and match each other
//     (matching GatewayID, operation ID, status-appropriate credential version, and consistent operation status).
//     Note on version semantics: For an unresolved attempt (e.g. recovery_needed), current metadata projection
//     retains the previous credential version while the operation record has the attempted version.
//     For lost_secret_recovery, the operation succeeded and metadata version must match op.CredentialVersion.
//  3. Operation record must be valid historically and not already superseded (RecoveryID == nil).
//  4. For ResetReasonRecoveryAfterUnknown:
//     - The current operation must be an unresolved pending or recovery-needed attempt
//     (OperationPending or OperationRecoveryNeeded).
//     - Metadata status must be transient (provisioning, rotating, revoking) or recovery_needed.
//     - If op.Previous != nil && op.Status == OperationRecoveryNeeded, metadata.CredentialVersion matches op.Previous.CredentialVersion.
//     Otherwise metadata.CredentialVersion matches op.CredentialVersion.
//     - Succeeded, failed, or revoked operations are NOT eligible for recovery_after_unknown.
//  5. For ResetReasonLostSecretRecovery:
//     - The current operation must be a succeeded password-issuing action (ActionProvision, ActionRotate, or ActionReset)
//     with OperationSucceeded. Succeeded ActionRevoke is strictly rejected.
//     - Metadata status must be CredentialActive and OperationStatus must be OperationSucceeded.
//     - CredentialVersion must match exactly.
//     - Revoked, failed, pending, or recovery-needed credentials are NOT eligible for lost_secret_recovery.
//
// Important architectural boundary:
// Eligibility is NOT execution admission. Future service and repository must recheck
// conditions atomically under a row lock and separately establish platform-admin authority
// and broker quiescence.
func CheckResetEligibility(current Metadata, op Operation, reason ResetReason) error {
	if err := reason.Validate(); err != nil {
		return err
	}

	if op.OperationID == uuid.Nil || current.LastOperationID == uuid.Nil {
		return &DomainError{Code: CodeInvalidRequest}
	}

	if current.LastOperationID != op.OperationID ||
		current.GatewayID != op.GatewayID ||
		current.OperationStatus != op.Status {
		return &DomainError{Code: CodeCredentialConflict}
	}

	// Version consistency:
	// If op has failed or ended in recovery_needed with a previous projection,
	// current metadata retains previous_credential_version.
	expectedVersion := op.CredentialVersion
	if op.Status == OperationRecoveryNeeded && op.Previous != nil {
		expectedVersion = op.Previous.CredentialVersion
	}
	if current.CredentialVersion != expectedVersion {
		return &DomainError{Code: CodeCredentialConflict}
	}

	if err := op.ValidateHistorical(); err != nil {
		return err
	}

	// If the operation was already superseded by a previous recovery disposition,
	// it cannot be reset again.
	if op.RecoveryID != nil {
		return &DomainError{Code: CodeCredentialConflict}
	}

	switch reason {
	case ResetReasonRecoveryAfterUnknown:
		// Target must be an unresolved attempt (pending or recovery_needed)
		if op.Status != OperationPending && op.Status != OperationRecoveryNeeded {
			return &DomainError{Code: CodeCredentialConflict}
		}
		// Metadata status must be transient or recovery_needed, never terminal active, failed, or revoked
		switch current.Status {
		case CredentialProvisioning, CredentialRotating, CredentialRevoking, CredentialRecoveryNeeded:
			return nil
		default:
			return &DomainError{Code: CodeCredentialConflict}
		}

	case ResetReasonLostSecretRecovery:
		// Target must be a succeeded password-issuing operation (provision, rotate, or reset)
		if op.Status != OperationSucceeded || (op.Action != ActionProvision && op.Action != ActionRotate && op.Action != ActionReset) {
			return &DomainError{Code: CodeCredentialConflict}
		}
		// Target credential must currently be active
		if current.Status != CredentialActive || current.OperationStatus != OperationSucceeded {
			return &DomainError{Code: CodeCredentialConflict}
		}
		return nil

	default:
		return &DomainError{Code: CodeInvalidRequest}
	}
}

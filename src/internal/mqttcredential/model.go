package mqttcredential

import (
	"encoding/json"
	"math"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// Action is a server-selected credential mutation, never a broker command.
type Action string

// Credential actions do not include device delivery or recovery checkpoints.
const (
	ActionProvision Action = "provision"
	ActionRotate    Action = "rotate"
	ActionRevoke    Action = "revoke"
)

// CredentialStatus describes business metadata, not a device handoff receipt.
type CredentialStatus string

// Credential statuses preserve the legacy schema and add explicit uncertainty.
const (
	CredentialProvisioning   CredentialStatus = "provisioning"
	CredentialActive         CredentialStatus = "active"
	CredentialRotating       CredentialStatus = "rotating"
	CredentialRevoking       CredentialStatus = "revoking"
	CredentialRevoked        CredentialStatus = "revoked"
	CredentialFailed         CredentialStatus = "failed"
	CredentialRecoveryNeeded CredentialStatus = "recovery_needed"
)

// OperationStatus is the outcome of an audited operation, not credential state.
type OperationStatus string

// Operation statuses keep unresolved uncertainty separate from terminal failure.
const (
	OperationPending        OperationStatus = "pending"
	OperationSucceeded      OperationStatus = "succeeded"
	OperationFailed         OperationStatus = "failed"
	OperationRecoveryNeeded OperationStatus = "recovery_needed"
)

// OperationPhase is a checkpoint only; it never proves a broker state change.
type OperationPhase string

// Operation checkpoints follow the DB/DynSec/finalization choreography.
const (
	PhaseIntent           OperationPhase = "intent"
	PhaseDynSecMutation   OperationPhase = "dynsec_mutation"
	PhaseSnapshotReadback OperationPhase = "snapshot_readback"
	PhaseFinalize         OperationPhase = "finalize"
	PhaseRecovery         OperationPhase = "recovery"
)

// DeliveryStatus tracks manual USB handoff independently of broker activation.
type DeliveryStatus string

// Delivery remains unknown until a separately verified device update.
const (
	DeliveryUnknown       DeliveryStatus = "unknown"
	DeliveryDeviceUpdated DeliveryStatus = "device_updated"
)

// ExecutionOutcome classifies adapter results, not durable DB operation status.
type ExecutionOutcome string

// Execution classifications do not imply rollback of the previous password.
const (
	ExecutionVerifiedSuccess   ExecutionOutcome = "verified_success"
	ExecutionUnchangedFailure  ExecutionOutcome = "unchanged_failure"
	ExecutionRecoveryRequired  ExecutionOutcome = "recovery_required"
	ExecutionMaintenanceClosed ExecutionOutcome = "maintenance_closed"
)

// Validate rejects unknown actions without normalizing input.
func (a Action) Validate() error {
	switch a {
	case ActionProvision, ActionRotate, ActionRevoke:
		return nil
	default:
		return &DomainError{Code: CodeInvalidRequest}
	}
}

// Validate rejects unknown credential states; a version alone is not active.
func (s CredentialStatus) Validate() error {
	switch s {
	case CredentialProvisioning, CredentialActive, CredentialRotating, CredentialRevoking, CredentialRevoked, CredentialFailed, CredentialRecoveryNeeded:
		return nil
	default:
		return &DomainError{Code: CodeInvalidRequest}
	}
}

// Validate rejects credential states and delivery outcomes used as operation status.
func (s OperationStatus) Validate() error {
	switch s {
	case OperationPending, OperationSucceeded, OperationFailed, OperationRecoveryNeeded:
		return nil
	default:
		return &DomainError{Code: CodeInvalidRequest}
	}
}

// Validate rejects unknown checkpoints, including snapshot evidence as a phase.
func (p OperationPhase) Validate() error {
	switch p {
	case PhaseIntent, PhaseDynSecMutation, PhaseSnapshotReadback, PhaseFinalize, PhaseRecovery:
		return nil
	default:
		return &DomainError{Code: CodeInvalidRequest}
	}
}

// Validate rejects unknown device handoff states.
func (s DeliveryStatus) Validate() error {
	switch s {
	case DeliveryUnknown, DeliveryDeviceUpdated:
		return nil
	default:
		return &DomainError{Code: CodeInvalidRequest}
	}
}

// Validate rejects unknown runtime classifications.
func (o ExecutionOutcome) Validate() error {
	switch o {
	case ExecutionVerifiedSuccess, ExecutionUnchangedFailure, ExecutionRecoveryRequired, ExecutionMaintenanceClosed:
		return nil
	default:
		return &DomainError{Code: CodeInvalidRequest}
	}
}

// MutationInput is an internal use-case input, not a client JSON body. The
// transport selects Action, parses the UUID key, and gets actor from Principal.
// No password, username, version or operation ID is client-selectable.
type MutationInput struct {
	GatewayID      string
	IdempotencyKey uuid.UUID
	Action         Action
}

// ValidateMutationInput checks identity syntax only. Platform-admin authority
// and Gateway existence must still be verified in PostgreSQL for every request.
// UUID parsing belongs at the transport boundary using the existing uuid library;
// no UUID version restriction or text normalization is added here.
func ValidateMutationInput(actorUserID uuid.UUID, input MutationInput) error {
	if actorUserID == uuid.Nil || input.IdempotencyKey == uuid.Nil || gateway.ValidateGatewayID(input.GatewayID) != nil {
		return &DomainError{Code: CodeInvalidRequest}
	}
	return input.Action.Validate()
}

// VerificationEvidence records observations for a specific operation only.
// SnapshotObserved is a bool, not a status or proof of a whole active generation,
// and is not an fsync/power-loss receipt. FreshPositiveVerified concerns the new
// password on a freshly loaded broker; none of these flags proves USB delivery.
type VerificationEvidence struct {
	RAMApplied            bool `json:"ram_applied"`
	SnapshotObserved      bool `json:"snapshot_observed"`
	FreshPositiveVerified bool `json:"fresh_positive_verified"`
}

// Operation carries safe audit identity and checkpoints, never a secret or hash.
// Historical DB actors may become NULL after deletion; live operation validation
// rejects a nil actor and is not intended to validate such historical rows.
type Operation struct {
	OperationID       uuid.UUID            `json:"operation_id"`
	ActorUserID       uuid.UUID            `json:"actor_user_id"`
	GatewayID         string               `json:"gateway_id"`
	IdempotencyKey    uuid.UUID            `json:"idempotency_key"`
	Action            Action               `json:"action"`
	CredentialVersion int64                `json:"credential_version"`
	Status            OperationStatus      `json:"status"`
	Phase             OperationPhase       `json:"phase"`
	Evidence          VerificationEvidence `json:"evidence"`
	DeliveryStatus    DeliveryStatus       `json:"delivery_status"`
	CreatedAt         time.Time            `json:"created_at"`
	UpdatedAt         time.Time            `json:"updated_at"`
	CompletedAt       *time.Time           `json:"completed_at"`
	Previous          *CredentialSnapshot  `json:"previous,omitempty"`
	ErrorCode         *ErrorCode           `json:"error_code,omitempty"`
	RecoveryID        *uuid.UUID           `json:"recovery_id,omitempty"`
}

// Validate checks a live operation's UUID identities and semantic enum groups.
func (o Operation) Validate() error {
	if o.ActorUserID == uuid.Nil {
		return &DomainError{Code: CodeInvalidRequest}
	}
	return o.ValidateHistorical()
}

// ValidateHistorical permits a deleted actor (ON DELETE SET NULL), not an API
// caller. Legacy recover events without an idempotency key require a separate
// migration projection; this validates the new operation contract only.
func (o Operation) ValidateHistorical() error {
	if o.OperationID == uuid.Nil {
		return &DomainError{Code: CodeInvalidRequest}
	}
	// Use the nonnil operation identity solely for syntax validation, not authority.
	if err := ValidateMutationInput(o.OperationID, MutationInput{GatewayID: o.GatewayID, IdempotencyKey: o.IdempotencyKey, Action: o.Action}); err != nil {
		return err
	}
	for _, err := range []error{o.Status.Validate(), o.Phase.Validate(), o.DeliveryStatus.Validate()} {
		if err != nil {
			return err
		}
	}
	if o.RecoveryID != nil && (*o.RecoveryID == uuid.Nil || (o.Status != OperationPending && o.Status != OperationRecoveryNeeded)) {
		return &DomainError{Code: CodeInvalidRequest}
	}
	if o.CredentialVersion <= 0 || (o.Status == OperationSucceeded || o.Status == OperationFailed) != (o.CompletedAt != nil) {
		return &DomainError{Code: CodeInvalidRequest}
	}
	if o.CompletedAt != nil && (o.CompletedAt.IsZero() || o.CompletedAt.Before(o.CreatedAt)) {
		return &DomainError{Code: CodeInvalidRequest}
	}
	// A terminal success must retain the operation-specific proof. Legacy audit
	// rows lacking this evidence need a separate projection, not invented proof.
	if o.Status == OperationSucceeded && (!o.Evidence.RAMApplied || !o.Evidence.SnapshotObserved || (o.Action != ActionRevoke && !o.Evidence.FreshPositiveVerified)) {
		return &DomainError{Code: CodeInvalidRequest}
	}
	if o.Previous != nil && (o.Previous.CredentialVersion <= 0 || o.Previous.Status.Validate() != nil) {
		return &DomainError{Code: CodeInvalidRequest}
	}
	return nil
}

// Terminal events are immutable, including evidence, identity and delivery.
// Later device delivery belongs to a separate audit record, not a terminal edit.
// RecoveryID closes a nonterminal historical event via linked disposition without
// modifying its original status, evidence, Previous, timestamps or error decision.
func (o Operation) Terminal() bool {
	return o.Status == OperationSucceeded || o.Status == OperationFailed || o.RecoveryID != nil
}

// CredentialSnapshot is the safe pre-intent state saved for known unchanged
// failures. It is not an old password, rollback capability or broker receipt.
type CredentialSnapshot struct {
	Status            CredentialStatus `json:"status"`
	CredentialVersion int64            `json:"credential_version"`
	ActivatedAt       *time.Time       `json:"activated_at"`
	RevokedAt         *time.Time       `json:"revoked_at"`
}

// AllocateVersion is for NEW provision/rotate attempts under a Gateway row lock.
// Zero means no metadata/history, never a stored generation. Revoke, no-op and
// replay must reuse their existing version and must not call this allocator.
func AllocateVersion(metadataVersion, maxAttemptedVersion int64) (int64, error) {
	if metadataVersion < 0 || maxAttemptedVersion < 0 {
		return 0, &DomainError{Code: CodeInvalidRequest}
	}
	maximum := max(metadataVersion, maxAttemptedVersion)
	if maximum == math.MaxInt64 {
		return 0, &DomainError{Code: CodeInvalidRequest}
	}
	return maximum + 1, nil
}

// BeginCredentialStatus admits ordinary new mutations, not recovery overrides.
// Reprovision after revoke/failed is permitted only after repository authorization;
// recovery_needed blocks new jobs until its existing operation is resolved.
// A revoke of an already revoked credential is a repository no-op, not an intent.
func BeginCredentialStatus(current CredentialStatus, action Action) (CredentialStatus, error) {
	if action.Validate() != nil || (current != "" && current.Validate() != nil) {
		return "", &DomainError{Code: CodeInvalidRequest}
	}
	switch current {
	case CredentialRecoveryNeeded:
		return "", &DomainError{Code: CodeRecoveryRequired}
	case CredentialProvisioning, CredentialRotating, CredentialRevoking:
		return "", &DomainError{Code: CodeOperationInProgress}
	}
	switch action {
	case ActionProvision:
		if current == "" || current == CredentialRevoked || current == CredentialFailed {
			return CredentialProvisioning, nil
		}
	case ActionRotate:
		if current == CredentialActive {
			return CredentialRotating, nil
		}
	case ActionRevoke:
		if current == CredentialActive {
			return CredentialRevoking, nil
		}
	}
	return "", &DomainError{Code: CodeCredentialConflict}
}

// Completion is a proposed atomic event + current metadata update. The repository
// must check LastOperationID under lock before persisting it; domain proof flags
// are trusted adapter observations, not client input or independent verification.
type Completion struct {
	Operation  Operation
	Credential CredentialSnapshot
}

// CompleteOperation never claims restoration of the original unknown password.
// Known failure is allowed only before any observed side effect and uncertainty.
// Resolving uncertainty to failure needs a separately verified recovery decision,
// not this unchanged-failure path. Maintenance/phase checkpoints are not receipts.
func CompleteOperation(o Operation, outcome ExecutionOutcome, evidence VerificationEvidence, at time.Time) (Completion, error) {
	invalid := func() (Completion, error) { return Completion{}, &DomainError{Code: CodeInvalidRequest} }
	if o.ValidateHistorical() != nil || o.Terminal() || at.IsZero() || at.Before(o.CreatedAt) || at.Before(o.UpdatedAt) {
		return invalid()
	}
	if o.Action != ActionProvision && (o.Previous == nil || o.Previous.Status != CredentialActive) {
		return invalid()
	}
	if o.Previous != nil {
		if o.Action == ActionProvision && o.Previous.Status != CredentialRevoked && o.Previous.Status != CredentialFailed {
			return invalid()
		}
		if o.Action == ActionRevoke && o.CredentialVersion != o.Previous.CredentialVersion {
			return invalid()
		}
		if o.Action != ActionRevoke && o.CredentialVersion <= o.Previous.CredentialVersion {
			return invalid()
		}
	}
	state := CredentialSnapshot{CredentialVersion: o.CredentialVersion}
	if o.Previous != nil {
		state = *o.Previous
	}
	switch outcome {
	case ExecutionVerifiedSuccess:
		if !evidence.RAMApplied || !evidence.SnapshotObserved || (o.Action != ActionRevoke && !evidence.FreshPositiveVerified) {
			return invalid()
		}
		state.CredentialVersion = o.CredentialVersion
		if o.Action == ActionRevoke {
			state.Status, state.RevokedAt = CredentialRevoked, &at
		} else {
			state.Status, state.ActivatedAt, state.RevokedAt = CredentialActive, &at, nil
		}
		o.Status, o.Phase, o.CompletedAt, o.ErrorCode = OperationSucceeded, PhaseFinalize, &at, nil
	case ExecutionUnchangedFailure:
		if o.Status != OperationPending || o.Evidence != (VerificationEvidence{}) || evidence != (VerificationEvidence{}) {
			return invalid()
		}
		if o.Previous == nil {
			state.Status = CredentialFailed
		}
		o.Status, o.Phase, o.CompletedAt = OperationFailed, PhaseFinalize, &at
	case ExecutionRecoveryRequired:
		state.Status = CredentialRecoveryNeeded
		o.Status, o.Phase, o.CompletedAt = OperationRecoveryNeeded, PhaseRecovery, nil
		// Never discard observations made earlier in this same operation.
		evidence.RAMApplied = evidence.RAMApplied || o.Evidence.RAMApplied
		evidence.SnapshotObserved = evidence.SnapshotObserved || o.Evidence.SnapshotObserved
		evidence.FreshPositiveVerified = evidence.FreshPositiveVerified || o.Evidence.FreshPositiveVerified
	default:
		return invalid()
	}
	o.Evidence, o.UpdatedAt = evidence, at
	// Completion is not a USB/device receipt.
	return Completion{Operation: o, Credential: state}, nil
}

// ReplayOperation is pure matching and safe output, never admission to execute.
// Service/repository MUST recheck current PostgreSQL admin authority before using
// it. A deleted historical actor cannot replay. Metadata is CURRENT, not a stale
// audited revoke decision; OperationID identifies the original request outcome.
func ReplayOperation(o Operation, actor uuid.UUID, input MutationInput, current Metadata) (MutationResult, error) {
	if ValidateMutationInput(actor, input) != nil {
		return MutationResult{}, &DomainError{Code: CodeInvalidRequest}
	}
	if o.ActorUserID != actor || o.IdempotencyKey != input.IdempotencyKey || o.GatewayID != input.GatewayID || o.Action != input.Action {
		return MutationResult{}, &DomainError{Code: CodeIdempotencyConflict}
	}
	if err := o.Validate(); err != nil {
		return MutationResult{}, err
	}
	if o.RecoveryID != nil {
		if current.GatewayID != o.GatewayID {
			return MutationResult{}, &DomainError{Code: CodeInvalidRequest}
		}
		return MutationResult{Metadata: current, OperationID: o.OperationID, ReplayedStatus: OperationResolvedByRecovery, NextAction: "provision_with_new_idempotency_key"}, nil
	}
	switch o.Status {
	case OperationPending:
		return MutationResult{}, &DomainError{Code: CodeOperationInProgress}
	case OperationRecoveryNeeded:
		return MutationResult{}, &DomainError{Code: CodeRecoveryRequired}
	}
	if current.GatewayID != o.GatewayID {
		return MutationResult{}, &DomainError{Code: CodeInvalidRequest}
	}
	result := MutationResult{Metadata: current, OperationID: o.OperationID, ReplayedStatus: o.Status, ErrorCode: o.ErrorCode}
	if o.Status == OperationSucceeded && o.Action != ActionRevoke && current.Status != CredentialRevoked {
		result.NextAction = "rotate_with_new_idempotency_key"
	} else if current.Status == CredentialRevoked {
		result.NextAction = "provision_with_new_idempotency_key"
	}
	return result, nil
}

// Metadata is the explicit safe read DTO. A provisioning version may be intended,
// not active: consumers must inspect Status, never infer activation from version.
// Missing metadata is not synthesized as active by this model.
type Metadata struct {
	GatewayID         string           `json:"gateway_id"`
	Username          string           `json:"username"`
	CredentialVersion int64            `json:"credential_version"`
	Status            CredentialStatus `json:"status"`
	LastOperationID   uuid.UUID        `json:"last_operation_id"`
	OperationStatus   OperationStatus  `json:"operation_status"`
	ActivatedAt       *time.Time       `json:"activated_at"`
	RevokedAt         *time.Time       `json:"revoked_at"`
	LastErrorCode     *ErrorCode       `json:"last_error_code"`
}

// MutationResult is safe metadata-only output. Replay policy is defined later;
// the model alone does not grant permission to return or regenerate a secret.
type MutationResult struct {
	Metadata
	OperationID    uuid.UUID       `json:"operation_id"`
	SecretReturned bool            `json:"secret_returned"`
	NextAction     string          `json:"next_action,omitempty"`
	ReplayedStatus OperationStatus `json:"replayed_status,omitempty"`
	ErrorCode      *ErrorCode      `json:"error_code,omitempty"`
}

// SecretResult is transient first-response output, never persistence input.
// Its private password prevents accidental field logging; fmt is redacted while
// explicit JSON marshaling is intentionally secret-bearing. Do not log JSON or
// copy/retain this value in queues, audit, replay records or long-lived closures.
type SecretResult struct {
	Metadata
	OperationID uuid.UUID
	password    string
}

// NewSecretResult packages a server-generated secret only after verified DB
// finalization. This constructor does not generate, validate or verify passwords.
func NewSecretResult(metadata Metadata, operationID uuid.UUID, password string) SecretResult {
	return SecretResult{Metadata: metadata, OperationID: operationID, password: password}
}

// MarshalJSON explicitly emits the first-response secret, separate from metadata.
func (r SecretResult) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Metadata
		OperationID    uuid.UUID `json:"operation_id"`
		Password       string    `json:"password,omitempty"`
		SecretReturned bool      `json:"secret_returned"`
	}{r.Metadata, r.OperationID, r.password, r.password != ""})
}

// String prevents routine fmt/log formatting from disclosing the secret.
func (r SecretResult) String() string { return "mqttcredential.SecretResult{redacted}" }

// GoString also redacts Go-syntax formatting (%#v).
func (r SecretResult) GoString() string { return r.String() }

// ClearSecret drops this value's reference after response handling. It does not
// promise heap zeroization or erase copies previously made by Go or the caller.
func (r *SecretResult) ClearSecret() { r.password = "" }

// ErrorCode is a safe API/business classification, not a raw adapter diagnostic.
type ErrorCode string

// Domain error codes match the planned HTTP contract without importing HTTP.
const (
	CodeInvalidRequest          ErrorCode = "invalid_request"
	CodeForbidden               ErrorCode = "forbidden"
	CodeNotFound                ErrorCode = "not_found"
	CodeCredentialConflict      ErrorCode = "credential_conflict"
	CodeOperationInProgress     ErrorCode = "operation_in_progress"
	CodeIdempotencyConflict     ErrorCode = "idempotency_conflict"
	CodeRuntimeDisabled         ErrorCode = "credential_runtime_disabled"
	CodeRuntimeBusy             ErrorCode = "credential_runtime_busy"
	CodeVerificationUnavailable ErrorCode = "credential_verification_unavailable"
	CodeRecoveryRequired        ErrorCode = "credential_recovery_required"
	CodeFinalizationPending     ErrorCode = "credential_finalization_pending"
	CodeServiceUnavailable      ErrorCode = "service_unavailable"
	CodeInternalError           ErrorCode = "internal_error"
)

// DomainError keeps typed business classification distinct from legacy runtime
// errors, with no caller input, password, raw cause or error message attached.
type DomainError struct {
	Code ErrorCode
}

// Error returns only a safe code, never adapter/DB diagnostics.
func (e *DomainError) Error() string { return string(e.Code) }

// Unwrap preserves established sentinels where semantics actually match.
func (e *DomainError) Unwrap() error {
	switch e.Code {
	case CodeInvalidRequest:
		return ErrInvalidInput
	case CodeRuntimeBusy:
		return ErrRuntimeBusy
	case CodeRecoveryRequired:
		return ErrRecoveryRequired
	default:
		return nil
	}
}

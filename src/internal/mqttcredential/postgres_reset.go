package mqttcredential

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// BeginResetRequest conveys parameters for BeginReset repository admission.
type BeginResetRequest struct {
	ActorUserID           uuid.UUID
	OperationID           uuid.UUID
	Input                 ResetInput
	ExpectedPredecessorID uuid.UUID
	ExpectedTargetGuard   *OperationGuard
}

// BeginReset admits a dedicated operator-supervised reset operation in Postgres.
// It atomically locks the gateway and platform admin, verifies reset reason eligibility under locks,
// allocates credential version strictly greater than current and all attempted history,
// and inserts the immutable reset disposition, reset event (execution_contract='simplified'),
// and updated gateway_mqtt_credentials projection.
func (r *PostgresRepository) BeginReset(ctx context.Context, req BeginResetRequest) (BeginResult, error) {
	if err := ValidateResetInput(req.ActorUserID, req.Input); err != nil {
		return BeginResult{}, err
	}
	if req.OperationID == uuid.Nil {
		return BeginResult{}, &DomainError{Code: CodeInvalidRequest}
	}
	if req.ExpectedTargetGuard != nil && (req.ExpectedTargetGuard.GatewayID != req.Input.GatewayID || req.ExpectedTargetGuard.ExpectedOperationID == uuid.Nil) {
		return BeginResult{}, &DomainError{Code: CodeInvalidRequest}
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return BeginResult{}, safeReadError(err)
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), r.timeout)
		defer done()
		_ = tx.Rollback(cleanup)
	}()

	// Advisory lock on actor and idempotency key
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, req.ActorUserID.String()+":"+req.Input.IdempotencyKey.String()); err != nil {
		return BeginResult{}, safeReadError(err)
	}

	// Gateway lock for mutual exclusion
	var parent string
	if err = tx.QueryRow(ctx, `SELECT gateway_id FROM public.gateways WHERE gateway_id=$1 FOR UPDATE`, req.Input.GatewayID).Scan(&parent); err != nil {
		return BeginResult{}, safeReadError(err)
	}

	// Verify platform admin authority using lock_mqtt_reset_admin
	var admin bool
	if err = tx.QueryRow(ctx, `SELECT public.lock_mqtt_reset_admin($1)`, req.ActorUserID).Scan(&admin); err != nil {
		return BeginResult{}, safeReadError(err)
	}
	if !admin {
		return BeginResult{}, &DomainError{Code: CodeForbidden}
	}

	// Load current metadata under lock
	m, readErr := scanMetadata(tx.QueryRow(ctx, metadataQuery, parent))
	if readErr != nil {
		return BeginResult{}, readErr
	}

	// Replay check: scan existing event for this actor and idempotency key
	p, replayErr := scanOperation(tx.QueryRow(ctx, `SELECT `+operationColumns+` FROM public.gateway_mqtt_credential_events WHERE actor_user_id=$1 AND idempotency_key=$2 AND NOT legacy_projection`, req.ActorUserID, req.Input.IdempotencyKey))
	if replayErr == nil {
		// Existing event found for same key
		if p.GatewayID != parent || p.Action != ActionReset {
			return BeginResult{}, &DomainError{Code: CodeIdempotencyConflict}
		}
		if p.ResetReason == nil || *p.ResetReason != req.Input.Reason {
			return BeginResult{}, &DomainError{Code: CodeIdempotencyConflict}
		}
		// In-progress or recovery checks
		switch p.Status {
		case OperationPending:
			return BeginResult{}, &DomainError{Code: CodeOperationInProgress}
		case OperationRecoveryNeeded:
			return BeginResult{}, &DomainError{Code: CodeRecoveryRequired}
		}
		result := MutationResult{Metadata: m, OperationID: p.OperationID, ReplayedStatus: p.Status}
		return BeginResult{Operation: p.Operation, Metadata: m, Replay: &result}, nil
	}
	var de *DomainError
	if !errors.As(replayErr, &de) || de.Code != CodeNotFound {
		return BeginResult{}, replayErr
	}

	// Check absence of OTHER unresolved effective operations (outside the current predecessor)
	var otherUnresolved bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM public.gateway_mqtt_credential_events
		WHERE gateway_id=$1 AND operation_id<>$2 AND status IN ('pending','recovery_needed')
		  AND recovery_id IS NULL AND public.mqtt_operation_effective(operation_id)
		UNION ALL
		SELECT 1 FROM public.mqtt_credential_recovery
		WHERE gateway_id=$1 AND status='pending'
	)`, parent, m.LastOperationID).Scan(&otherUnresolved); err != nil {
		return BeginResult{}, safeReadError(err)
	}
	if otherUnresolved {
		return BeginResult{}, &DomainError{Code: CodeOperationInProgress}
	}

	// Load exact current operation record (predecessor)
	pred, predErr := scanOperation(tx.QueryRow(ctx, `SELECT `+operationColumns+` FROM public.gateway_mqtt_credential_events WHERE operation_id=$1 AND gateway_id=$2 FOR UPDATE`, m.LastOperationID, parent))
	if predErr != nil {
		return BeginResult{}, predErr
	}
	if pred.legacy {
		return BeginResult{}, &DomainError{Code: CodeCredentialConflict}
	}
	if req.ExpectedPredecessorID != uuid.Nil && pred.OperationID != req.ExpectedPredecessorID {
		return BeginResult{}, &DomainError{Code: CodeCredentialConflict}
	}
	if req.ExpectedTargetGuard != nil {
		if pred.OperationID != req.ExpectedTargetGuard.ExpectedOperationID ||
			pred.GatewayID != req.ExpectedTargetGuard.GatewayID ||
			pred.Status != req.ExpectedTargetGuard.ExpectedOperationStatus ||
			m.Status != req.ExpectedTargetGuard.ExpectedCredentialStatus ||
			m.CredentialVersion != req.ExpectedTargetGuard.ExpectedCredentialVersion ||
			pred.ExecutionContract != ContractSimplified {
			return BeginResult{}, &DomainError{Code: CodeCredentialConflict}
		}
	}

	// Check pure eligibility under lock
	if err = CheckResetEligibility(m, pred.Operation, req.Input.Reason); err != nil {
		return BeginResult{}, err
	}

	// Allocate version strictly above current metadata and ALL attempted history
	var attempted int64
	if err = tx.QueryRow(ctx, `SELECT COALESCE(max(credential_version),0) FROM public.gateway_mqtt_credential_events WHERE gateway_id=$1`, parent).Scan(&attempted); err != nil {
		return BeginResult{}, safeReadError(err)
	}
	version, err := AllocateVersion(m.CredentialVersion, attempted)
	if err != nil {
		return BeginResult{}, err
	}

	// 1. Insert reset disposition sidecar
	_, err = tx.Exec(ctx, `INSERT INTO public.mqtt_reset_dispositions
(predecessor_id, successor_id, gateway_id, reason)
VALUES ($1, $2, $3, $4)`, pred.OperationID, req.OperationID, parent, req.Input.Reason)
	if err != nil {
		return BeginResult{}, safeReadError(err)
	}

	// 2. Insert reset event with execution_contract = 'simplified'
	previous := CredentialSnapshot{
		Status:            m.Status,
		CredentialVersion: m.CredentialVersion,
		ActivatedAt:       m.ActivatedAt,
		RevokedAt:         m.RevokedAt,
	}
	_, err = tx.Exec(ctx, `INSERT INTO public.gateway_mqtt_credential_events
(operation_id, gateway_id, credential_version, action, status, actor_user_id, idempotency_key,
previous_status, previous_credential_version, previous_activated_at, previous_revoked_at,
execution_contract, reset_predecessor_id, reset_reason)
VALUES ($1, $2, $3, 'reset', 'pending', $4, $5, $6, $7, $8, $9, 'simplified', $10, $11)`,
		req.OperationID, parent, version, req.ActorUserID, req.Input.IdempotencyKey,
		previous.Status, previous.CredentialVersion, previous.ActivatedAt, previous.RevokedAt,
		pred.OperationID, req.Input.Reason)
	if err != nil {
		return BeginResult{}, safeReadError(err)
	}

	// 3. Update gateway_mqtt_credentials projection: status becomes rotating
	_, err = tx.Exec(ctx, `UPDATE public.gateway_mqtt_credentials SET
credential_version=$1, status='rotating', last_operation_id=$2, changed_by=$3,
last_error_code=NULL, updated_at=now()
WHERE gateway_id=$4 AND last_operation_id=$5`,
		version, req.OperationID, req.ActorUserID, parent, pred.OperationID)
	if err != nil {
		return BeginResult{}, safeReadError(err)
	}

	newOp, err := scanOperation(tx.QueryRow(ctx, `SELECT `+operationColumns+` FROM public.gateway_mqtt_credential_events WHERE operation_id=$1`, req.OperationID))
	if err != nil {
		return BeginResult{}, err
	}
	newMeta, err := scanMetadata(tx.QueryRow(ctx, metadataQuery, parent))
	if err != nil {
		return BeginResult{}, err
	}

	if err = tx.Commit(ctx); err != nil {
		return BeginResult{}, &CommitOutcomeUnknown{OperationID: req.OperationID}
	}

	return BeginResult{Operation: newOp.Operation, Metadata: newMeta, Execute: true}, nil
}

// LookupResetReplay performs a read-only actor/idempotency-key reset lookup in Postgres.
// It verifies platform admin authority, retrieves any existing event matching the actor and key,
// validates matching Gateway, Action=reset, and ResetReason, distinguishes pending/recovery statuses,
// and returns the historical operation and current metadata without any secret disclosure,
// operation allocation, prerequisite side effects, or database mutation.
func (r *PostgresRepository) LookupResetReplay(ctx context.Context, actor uuid.UUID, in ResetInput) (Operation, Metadata, bool, error) {
	if err := ValidateResetInput(actor, in); err != nil {
		return Operation{}, Metadata{}, false, err
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// Verify platform admin authority (read-only)
	var admin bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.platform_admins WHERE user_id=$1)`, actor).Scan(&admin); err != nil {
		return Operation{}, Metadata{}, false, safeReadError(err)
	}
	if !admin {
		return Operation{}, Metadata{}, false, &DomainError{Code: CodeForbidden}
	}

	// Verify gateway exists (read-only)
	var gwExists bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.gateways WHERE gateway_id=$1)`, in.GatewayID).Scan(&gwExists); err != nil {
		return Operation{}, Metadata{}, false, safeReadError(err)
	}
	if !gwExists {
		return Operation{}, Metadata{}, false, &DomainError{Code: CodeNotFound}
	}

	// Query event by actor and idempotency key
	p, err := scanOperation(r.pool.QueryRow(ctx, `SELECT `+operationColumns+` FROM public.gateway_mqtt_credential_events WHERE actor_user_id=$1 AND idempotency_key=$2 AND NOT legacy_projection`, actor, in.IdempotencyKey))
	if err != nil {
		var de *DomainError
		if errors.As(err, &de) && de.Code == CodeNotFound {
			return Operation{}, Metadata{}, false, nil
		}
		return Operation{}, Metadata{}, false, err
	}

	// Conflict checks: wrong Gateway, wrong action, or wrong reset reason
	if p.GatewayID != in.GatewayID || p.Action != ActionReset {
		return Operation{}, Metadata{}, false, &DomainError{Code: CodeIdempotencyConflict}
	}
	if p.ResetReason == nil || *p.ResetReason != in.Reason {
		return Operation{}, Metadata{}, false, &DomainError{Code: CodeIdempotencyConflict}
	}

	// Distinguish pending / recovery statuses
	switch p.Status {
	case OperationPending:
		return Operation{}, Metadata{}, false, &DomainError{Code: CodeOperationInProgress}
	case OperationRecoveryNeeded:
		return Operation{}, Metadata{}, false, &DomainError{Code: CodeRecoveryRequired}
	}

	// Load current metadata projection
	m, err := scanMetadata(r.pool.QueryRow(ctx, metadataQuery, in.GatewayID))
	if err != nil {
		return Operation{}, Metadata{}, false, err
	}

	return p.Operation, m, true, nil
}

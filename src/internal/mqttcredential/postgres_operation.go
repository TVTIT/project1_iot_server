package mqttcredential

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// BeginOperation starts a credential mutation operation transaction in Postgres.
func (r *PostgresRepository) BeginOperation(ctx context.Context, req BeginRequest) (BeginResult, error) {
	if err := ValidateMutationInput(req.ActorUserID, req.Input); err != nil {
		return BeginResult{}, err
	}
	if req.OperationID == [16]byte{} {
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
		_ = tx.Rollback(cleanup) // Never expose driver diagnostics.
	}()
	// Actor/key serializes even across Gateways, before Gateway/metadata locks.
	// Hash collisions only serialize unrelated requests; identity remains exact SQL.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, req.ActorUserID.String()+":"+req.Input.IdempotencyKey.String()); err != nil {
		return BeginResult{}, safeReadError(err)
	}
	var parent string
	if err = tx.QueryRow(ctx, `SELECT gateway_id FROM public.gateways WHERE gateway_id=$1 FOR UPDATE`, req.Input.GatewayID).Scan(&parent); err != nil {
		return BeginResult{}, safeReadError(err)
	}
	var admin bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM public.platform_admins WHERE user_id=$1)`, req.ActorUserID).Scan(&admin); err != nil {
		return BeginResult{}, safeReadError(err)
	}
	if !admin {
		return BeginResult{}, &DomainError{Code: CodeForbidden}
	}
	m, readErr := scanMetadata(tx.QueryRow(ctx, metadataQuery, parent))
	var de *DomainError
	missing := errors.As(readErr, &de) && de.Code == CodeNotFound
	if readErr != nil && !missing {
		return BeginResult{}, readErr
	}
	if missing && req.Input.Action == ActionRevoke {
		return BeginResult{}, &DomainError{Code: CodeNotFound}
	}
	p, replayErr := scanOperation(tx.QueryRow(ctx, `SELECT `+operationColumns+` FROM public.gateway_mqtt_credential_events WHERE actor_user_id=$1 AND idempotency_key=$2 AND NOT legacy_projection`, req.ActorUserID, req.Input.IdempotencyKey))
	if replayErr == nil {
		if p.GatewayID != parent || p.Action != req.Input.Action {
			return BeginResult{}, &DomainError{Code: CodeIdempotencyConflict}
		}
		if missing {
			return BeginResult{}, &DomainError{Code: CodeInternalError}
		}
		replay, err := ReplayOperation(p.Operation, req.ActorUserID, req.Input, m)
		if err != nil {
			return BeginResult{}, err
		}
		return BeginResult{Operation: p.Operation, Metadata: m, Replay: &replay}, nil
	}
	if !errors.As(replayErr, &de) || de.Code != CodeNotFound {
		return BeginResult{}, replayErr
	}
	var pending, recovery bool
	// Replay above is metadata-only. New intents (including revoke no-ops below)
	// must wait for all prior maintenance, even when its audit event is terminal.
	// The Gateway lock also serializes this check with checkpoint completion.
	if err = tx.QueryRow(ctx, `SELECT COALESCE(bool_or(status IN ('pending','in_progress')),false), COALESCE(bool_or(status='recovery_needed'),false) FROM (
SELECT status FROM public.gateway_mqtt_credential_events WHERE gateway_id=$1 AND status IN ('pending','recovery_needed') AND recovery_id IS NULL
UNION ALL SELECT status FROM public.mqtt_credential_maintenance WHERE gateway_id=$1 AND status <> 'completed'
UNION ALL SELECT 'recovery_needed' FROM public.mqtt_credential_recovery WHERE gateway_id=$1 AND status='pending'
) unresolved`, parent).Scan(&pending, &recovery); err != nil {
		return BeginResult{}, safeReadError(err)
	}
	if recovery {
		return BeginResult{}, &DomainError{Code: CodeRecoveryRequired}
	}
	if pending {
		return BeginResult{}, &DomainError{Code: CodeOperationInProgress}
	}
	if req.Input.Action == ActionRevoke && m.Status == CredentialRevoked {
		result := MutationResult{Metadata: m}
		return BeginResult{Metadata: m, Replay: &result}, nil
	}
	status, err := BeginCredentialStatus(m.Status, req.Input.Action)
	if err != nil {
		return BeginResult{}, err
	}
	version := m.CredentialVersion
	if req.Input.Action != ActionRevoke {
		var attempted int64
		if err = tx.QueryRow(ctx, `SELECT COALESCE(max(credential_version),0) FROM public.gateway_mqtt_credential_events WHERE gateway_id=$1`, parent).Scan(&attempted); err != nil {
			return BeginResult{}, safeReadError(err)
		}
		version, err = AllocateVersion(version, attempted)
		if err != nil {
			return BeginResult{}, err
		}
	}
	var previous *CredentialSnapshot
	var previousStatus any
	var previousVersion any
	if !missing {
		previous = &CredentialSnapshot{Status: m.Status, CredentialVersion: m.CredentialVersion, ActivatedAt: m.ActivatedAt, RevokedAt: m.RevokedAt}
		previousStatus, previousVersion = previous.Status, previous.CredentialVersion
	}
	// Event first: v11 allocation trigger compares against pre-intent metadata.
	_, err = tx.Exec(ctx, `INSERT INTO public.gateway_mqtt_credential_events
(operation_id,gateway_id,credential_version,action,status,actor_user_id,idempotency_key,
previous_status,previous_credential_version,previous_activated_at,previous_revoked_at)
VALUES ($1,$2,$3,$4,'pending',$5,$6,$7,$8,$9,$10)`, req.OperationID, parent, version,
		req.Input.Action, req.ActorUserID, req.Input.IdempotencyKey, previousStatus, previousVersion, m.ActivatedAt, m.RevokedAt)
	if err != nil {
		return BeginResult{}, safeReadError(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO public.gateway_mqtt_credentials
(gateway_id,credential_version,status,last_operation_id,changed_by,activated_at)
VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT(gateway_id) DO UPDATE SET
credential_version=EXCLUDED.credential_version,status=EXCLUDED.status,last_operation_id=EXCLUDED.last_operation_id,
changed_by=EXCLUDED.changed_by,activated_at=EXCLUDED.activated_at,revoked_at=NULL,last_error_code=NULL,updated_at=now()`, parent, version, status, req.OperationID, req.ActorUserID, m.ActivatedAt)
	if err != nil {
		return BeginResult{}, safeReadError(err)
	}
	p, err = scanOperation(tx.QueryRow(ctx, `SELECT `+operationColumns+` FROM public.gateway_mqtt_credential_events WHERE operation_id=$1`, req.OperationID))
	if err != nil {
		return BeginResult{}, err
	}
	m, err = scanMetadata(tx.QueryRow(ctx, metadataQuery, parent))
	if err != nil {
		return BeginResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return BeginResult{}, &CommitOutcomeUnknown{OperationID: req.OperationID}
	}
	return BeginResult{Operation: p.Operation, Metadata: m, Execute: true}, nil
}

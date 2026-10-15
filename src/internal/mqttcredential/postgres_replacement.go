package mqttcredential

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var _ ReplacementRepository = (*PostgresRepository)(nil)

// BeginReplacement transfers only the current DB execution authority. It never
// changes predecessor evidence/checkpoints or asserts a native execution result.
func (r *PostgresRepository) BeginReplacement(ctx context.Context, q ReplacementRequest) (BeginResult, error) {
	g := q.Guard
	if ValidateMutationInput(q.ActorUserID, MutationInput{GatewayID: g.GatewayID, IdempotencyKey: q.IdempotencyKey, Action: ActionRotate}) != nil || q.OperationID == uuid.Nil || g.ExpectedOperationID == uuid.Nil || q.OperationID == g.ExpectedOperationID || g.ExpectedCredentialVersion <= 0 || g.ExpectedCredentialStatus.Validate() != nil || g.ExpectedOperationStatus.Validate() != nil {
		return BeginResult{}, &DomainError{Code: CodeInvalidRequest}
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return BeginResult{}, safeReadError(err)
	}
	defer r.rollbackRecovery(tx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, q.ActorUserID.String()+":"+q.IdempotencyKey.String()); err != nil {
		return BeginResult{}, safeReadError(err)
	}
	var parent string
	if err = tx.QueryRow(ctx, `SELECT gateway_id FROM public.gateways WHERE gateway_id=$1 FOR UPDATE`, g.GatewayID).Scan(&parent); err != nil {
		return BeginResult{}, safeReadError(err)
	}
	actor := q.ActorUserID
	var allowed bool
	if err = tx.QueryRow(ctx, `SELECT public.lock_mqtt_replacement_admin($1)`, actor).Scan(&allowed); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return BeginResult{}, &DomainError{Code: CodeForbidden}
		}
		return BeginResult{}, safeReadError(err)
	}
	if !allowed {
		return BeginResult{}, &DomainError{Code: CodeForbidden}
	}
	m, err := scanMetadata(tx.QueryRow(ctx, metadataQuery, parent))
	if err != nil {
		return BeginResult{}, err
	}
	p, replayErr := scanOperation(tx.QueryRow(ctx, `SELECT `+operationColumns+` FROM public.gateway_mqtt_credential_events WHERE actor_user_id=$1 AND idempotency_key=$2 AND NOT legacy_projection`, actor, q.IdempotencyKey))
	if replayErr == nil {
		if p.GatewayID != parent || p.ReplacementPredecessorID == nil || *p.ReplacementPredecessorID != g.ExpectedOperationID {
			return BeginResult{}, &DomainError{Code: CodeIdempotencyConflict}
		}
		replay := MutationResult{Metadata: m, OperationID: p.OperationID, ReplayedStatus: p.Status, ErrorCode: p.ErrorCode}
		return BeginResult{Operation: p.Operation, Metadata: m, Replay: &replay}, nil
	}
	var de *DomainError
	if !errors.As(replayErr, &de) || de.Code != CodeNotFound {
		return BeginResult{}, replayErr
	}
	p, err = scanOperation(tx.QueryRow(ctx, `SELECT `+operationColumns+` FROM public.gateway_mqtt_credential_events WHERE operation_id=$1 AND gateway_id=$2 FOR UPDATE`, g.ExpectedOperationID, parent))
	if err != nil {
		return BeginResult{}, err
	}
	if p.legacy || p.Action != ActionRotate || p.RecoveryID != nil || m.LastOperationID != g.ExpectedOperationID || p.Status != g.ExpectedOperationStatus || m.Status != g.ExpectedCredentialStatus || m.CredentialVersion != g.ExpectedCredentialVersion {
		return BeginResult{}, &DomainError{Code: CodeCredentialConflict}
	}
	var attempted int64
	if err = tx.QueryRow(ctx, `SELECT COALESCE(max(credential_version),0) FROM public.gateway_mqtt_credential_events WHERE gateway_id=$1`, parent).Scan(&attempted); err != nil {
		return BeginResult{}, safeReadError(err)
	}
	version, err := AllocateVersion(m.CredentialVersion, attempted)
	if err != nil {
		return BeginResult{}, err
	}
	// INSERT guard independently validates actual projection, checkpoint, admin,
	// effective inventory, and exact predecessor; deferred checks forbid orphans.
	_, err = tx.Exec(ctx, `INSERT INTO public.mqtt_rotation_replacements(predecessor_id,successor_id,gateway_id) VALUES($1,$2,$3)`, p.OperationID, q.OperationID, parent)
	if err != nil {
		return BeginResult{}, &DomainError{Code: CodeCredentialConflict}
	}
	_, err = tx.Exec(ctx, `INSERT INTO public.gateway_mqtt_credential_events(operation_id,gateway_id,credential_version,action,status,actor_user_id,idempotency_key,previous_status,previous_credential_version,previous_activated_at,previous_revoked_at,replacement_predecessor_id) VALUES($1,$2,$3,'rotate','pending',$4,$5,$6,$7,$8,$9,$10)`, q.OperationID, parent, version, actor, q.IdempotencyKey, m.Status, m.CredentialVersion, m.ActivatedAt, m.RevokedAt, p.OperationID)
	if err != nil {
		return BeginResult{}, safeReadError(err)
	}
	_, err = tx.Exec(ctx, `UPDATE public.gateway_mqtt_credentials SET credential_version=$2,status='rotating',last_operation_id=$3,changed_by=$4,revoked_at=NULL,last_error_code=NULL,updated_at=now() WHERE gateway_id=$1`, parent, version, q.OperationID, actor)
	if err != nil {
		return BeginResult{}, safeReadError(err)
	}
	p, err = scanOperation(tx.QueryRow(ctx, `SELECT `+operationColumns+` FROM public.gateway_mqtt_credential_events WHERE operation_id=$1`, q.OperationID))
	if err != nil {
		return BeginResult{}, err
	}
	m, err = scanMetadata(tx.QueryRow(ctx, metadataQuery, parent))
	if err != nil {
		return BeginResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return BeginResult{}, &CommitOutcomeUnknown{OperationID: q.OperationID}
	}
	return BeginResult{Operation: p.Operation, Metadata: m, Execute: true}, nil
}

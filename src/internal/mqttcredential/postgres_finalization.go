package mqttcredential

import (
	"context"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"iot-platform/internal/gateway"
)

var _ Repository = (*PostgresRepository)(nil)

func canonicalSnapshot(s CredentialSnapshot) CredentialSnapshot {
	utc := func(t *time.Time) *time.Time {
		if t == nil {
			return nil
		}
		v := t.UTC().Truncate(time.Microsecond)
		return &v
	}
	s.ActivatedAt, s.RevokedAt = utc(s.ActivatedAt), utc(s.RevokedAt)
	return s
}

func sameCompletion(a, b Completion) bool {
	canonical := func(c Completion) Completion {
		c.Credential = canonicalSnapshot(c.Credential)
		c.Operation.CreatedAt = c.Operation.CreatedAt.UTC().Truncate(time.Microsecond)
		c.Operation.UpdatedAt = c.Operation.UpdatedAt.UTC().Truncate(time.Microsecond)
		if c.Operation.CompletedAt != nil {
			at := c.Operation.CompletedAt.UTC().Truncate(time.Microsecond)
			c.Operation.CompletedAt = &at
		}
		if c.Operation.Previous != nil {
			s := canonicalSnapshot(*c.Operation.Previous)
			c.Operation.Previous = &s
		}
		return c
	}
	return reflect.DeepEqual(canonical(a), canonical(b))
}

func (r *PostgresRepository) ConditionalFinalize(ctx context.Context, u OperationUpdate) (Metadata, error) {
	return r.complete(ctx, u, ExecutionVerifiedSuccess)
}

func (r *PostgresRepository) FailKnown(ctx context.Context, u OperationUpdate) (Metadata, error) {
	return r.complete(ctx, u, ExecutionUnchangedFailure)
}

func (r *PostgresRepository) RequireRecovery(ctx context.Context, u OperationUpdate) (Metadata, error) {
	return r.complete(ctx, u, ExecutionRecoveryRequired)
}

func safeCompletionCode(code *ErrorCode) bool {
	if code == nil {
		return true
	}
	switch *code {
	case CodeInvalidRequest, CodeForbidden, CodeNotFound, CodeCredentialConflict, CodeOperationInProgress, CodeIdempotencyConflict, CodeRuntimeDisabled, CodeRuntimeBusy, CodeVerificationUnavailable, CodeRecoveryRequired, CodeFinalizationPending, CodeServiceUnavailable, CodeInternalError:
		return true
	}
	return false
}

// Completion is checked against the immutable DB operation, not caller-supplied
// Previous/identity. Evidence flags are trusted adapter assertions only.
func (r *PostgresRepository) complete(ctx context.Context, u OperationUpdate, outcome ExecutionOutcome) (Metadata, error) {
	g, proposed := u.Guard, u.Completion
	invalid := func() (Metadata, error) { return Metadata{}, &DomainError{Code: CodeInvalidRequest} }
	if gateway.ValidateGatewayID(g.GatewayID) != nil || g.ExpectedOperationID == uuid.Nil || g.ExpectedOperationStatus.Validate() != nil || g.ExpectedCredentialStatus.Validate() != nil || g.ExpectedCredentialVersion <= 0 || !safeCompletionCode(proposed.Operation.ErrorCode) {
		return invalid()
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Metadata{}, safeReadError(err)
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), r.timeout)
		defer done()
		_ = tx.Rollback(cleanup)
	}()
	var parent string
	if err = tx.QueryRow(ctx, `SELECT gateway_id FROM public.gateways WHERE gateway_id=$1 FOR UPDATE`, g.GatewayID).Scan(&parent); err != nil {
		return Metadata{}, safeReadError(err)
	}
	m, err := scanMetadata(tx.QueryRow(ctx, metadataQuery, parent))
	if err != nil {
		return Metadata{}, err
	}
	p, err := scanOperation(tx.QueryRow(ctx, `SELECT `+operationColumns+` FROM public.gateway_mqtt_credential_events WHERE operation_id=$1 AND gateway_id=$2 FOR UPDATE`, g.ExpectedOperationID, parent))
	if err != nil {
		return Metadata{}, err
	}
	if p.legacy {
		return invalid()
	}
	if p.RecoveryID != nil {
		return Metadata{}, &DomainError{Code: CodeCredentialConflict}
	}
	// Exact terminal retry returns CURRENT metadata even after a newer operation.
	// Never update a terminal event or use it to restore historical metadata.
	if p.Terminal() {
		base := p.Operation
		base.Status, base.CompletedAt, base.UpdatedAt, base.Evidence = OperationPending, nil, base.CreatedAt, VerificationEvidence{}
		terminalOutcome := ExecutionVerifiedSuccess
		if p.Status == OperationFailed {
			terminalOutcome = ExecutionUnchangedFailure
		}
		terminal, checkErr := CompleteOperation(base, terminalOutcome, p.Evidence, p.UpdatedAt)
		if checkErr == nil && outcome == terminalOutcome && sameCompletion(Completion{p.Operation, terminal.Credential}, proposed) {
			return m, nil
		}
		return Metadata{}, &DomainError{Code: CodeCredentialConflict}
	}
	if m.LastOperationID != g.ExpectedOperationID || p.Status != g.ExpectedOperationStatus || m.Status != g.ExpectedCredentialStatus || m.CredentialVersion != g.ExpectedCredentialVersion {
		return Metadata{}, &DomainError{Code: CodeCredentialConflict}
	}
	// The operation may become terminal while maintenance remains unresolved.
	// Never erase or complete that checkpoint as a side effect of finalization.
	// Lock it in this same transaction so a missing/corrupt checkpoint cannot
	// authorize a successful business finalization.
	var maintenanceStatus MaintenanceStatus
	if err = tx.QueryRow(ctx, `SELECT status FROM public.mqtt_credential_maintenance WHERE operation_id=$1 AND gateway_id=$2 FOR UPDATE`, p.OperationID, parent).Scan(&maintenanceStatus); err != nil {
		return Metadata{}, safeReadError(err)
	}
	if maintenanceStatus == MaintenanceCompleted {
		return Metadata{}, &DomainError{Code: CodeCredentialConflict}
	}
	// A caller cannot legitimize corrupt/current foreign generation metadata by
	// merely echoing its status and version in the guard.
	status, version := CredentialRecoveryNeeded, p.CredentialVersion
	if p.Status == OperationPending {
		switch p.Action {
		case ActionProvision:
			status = CredentialProvisioning
		case ActionRotate:
			status = CredentialRotating
		case ActionRevoke:
			status = CredentialRevoking
		}
	} else if p.Previous != nil {
		version = p.Previous.CredentialVersion
	}
	if m.Status != status || m.CredentialVersion != version {
		return Metadata{}, &DomainError{Code: CodeCredentialConflict}
	}
	expected, err := CompleteOperation(p.Operation, outcome, proposed.Operation.Evidence, proposed.Operation.UpdatedAt)
	if err != nil {
		return Metadata{}, err
	}
	if (p.Evidence.RAMApplied && !expected.Operation.Evidence.RAMApplied) || (p.Evidence.SnapshotObserved && !expected.Operation.Evidence.SnapshotObserved) || (p.Evidence.FreshPositiveVerified && !expected.Operation.Evidence.FreshPositiveVerified) {
		return invalid()
	}
	if outcome != ExecutionVerifiedSuccess {
		expected.Operation.ErrorCode = proposed.Operation.ErrorCode
	}
	if !sameCompletion(expected, proposed) {
		return invalid()
	}
	o, c := expected.Operation, expected.Credential
	tag, err := tx.Exec(ctx, `UPDATE public.gateway_mqtt_credential_events SET status=$3,phase=$4,ram_applied=$5,snapshot_observed=$6,fresh_positive_verified=$7,updated_at=$8,completed_at=$9,error_code=$10 WHERE operation_id=$1 AND status=$2 AND NOT legacy_projection`, o.OperationID, g.ExpectedOperationStatus, o.Status, o.Phase, o.Evidence.RAMApplied, o.Evidence.SnapshotObserved, o.Evidence.FreshPositiveVerified, o.UpdatedAt, o.CompletedAt, o.ErrorCode)
	if err != nil {
		return Metadata{}, safeReadError(err)
	}
	if tag.RowsAffected() != 1 {
		return Metadata{}, &DomainError{Code: CodeCredentialConflict}
	}
	tag, err = tx.Exec(ctx, `UPDATE public.gateway_mqtt_credentials SET status=$5,credential_version=$6,activated_at=$7,revoked_at=$8,last_error_code=$9,updated_at=$10 WHERE gateway_id=$1 AND last_operation_id=$2 AND status=$3 AND credential_version=$4`, parent, g.ExpectedOperationID, g.ExpectedCredentialStatus, g.ExpectedCredentialVersion, c.Status, c.CredentialVersion, c.ActivatedAt, c.RevokedAt, o.ErrorCode, o.UpdatedAt)
	if err != nil {
		return Metadata{}, safeReadError(err)
	}
	if tag.RowsAffected() != 1 {
		return Metadata{}, &DomainError{Code: CodeCredentialConflict}
	}
	m, err = scanMetadata(tx.QueryRow(ctx, metadataQuery, parent))
	if err != nil {
		return Metadata{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Metadata{}, &CommitOutcomeUnknown{OperationID: o.OperationID}
	}
	return m, nil
}

// Bound may be reduced by the caller; it can never be disabled by a large limit.
const maxRepositoryScanLimit = 1000

func (r *PostgresRepository) ListUnresolved(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
	if limit <= 0 || limit > r.scanLimit {
		return nil, &DomainError{Code: CodeInvalidRequest}
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	rows, err := r.pool.Query(ctx, `SELECT `+operationColumns+` FROM public.gateway_mqtt_credential_events WHERE status IN ('pending','recovery_needed') AND recovery_id IS NULL AND operation_id>$1 ORDER BY operation_id LIMIT $2`, cursor, limit)
	if err != nil {
		return nil, safeReadError(err)
	}
	defer rows.Close()
	result := make([]Operation, 0)
	for rows.Next() {
		p, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		// Missing key/phase/version and action recover explicitly identify legacy
		// projections in the fixed domain contract; do not invent modern proof.
		result = append(result, p.Operation)
	}
	if rows.Err() != nil {
		return nil, safeReadError(rows.Err())
	}
	return result, nil
}

func (r *PostgresRepository) ListDurableRevocations(ctx context.Context, cursor string, limit int) ([]RevocationDecision, error) {
	if limit <= 0 || limit > r.scanLimit || (cursor != "" && gateway.ValidateGatewayID(cursor) != nil) {
		return nil, &DomainError{Code: CodeInvalidRequest}
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	// Current metadata supersedes historical revokes. Unresolved revoke intents
	// also count (including legacy rows); one deterministic decision per Gateway.
	rows, err := r.pool.Query(ctx, `SELECT g.gateway_id, COALESCE(e.operation_id,c.last_operation_id), COALESCE(e.credential_version,c.credential_version,0) FROM public.gateways g LEFT JOIN public.gateway_mqtt_credentials c ON c.gateway_id=g.gateway_id LEFT JOIN LATERAL (SELECT operation_id,credential_version FROM public.gateway_mqtt_credential_events WHERE gateway_id=g.gateway_id AND action='revoke' AND status IN ('pending','recovery_needed') AND recovery_id IS NULL ORDER BY operation_id LIMIT 1) e ON true WHERE g.gateway_id>$1 AND (c.status='revoked' OR e.operation_id IS NOT NULL) ORDER BY g.gateway_id LIMIT $2`, cursor, limit)
	if err != nil {
		return nil, safeReadError(err)
	}
	defer rows.Close()
	result := make([]RevocationDecision, 0)
	for rows.Next() {
		var d RevocationDecision
		var id *uuid.UUID
		if err := rows.Scan(&d.GatewayID, &id, &d.CredentialVersion); err != nil {
			return nil, safeReadError(err)
		}
		if id != nil {
			d.OperationID = *id
		}
		result = append(result, d)
	}
	if rows.Err() != nil {
		return nil, safeReadError(rows.Err())
	}
	return result, nil
}

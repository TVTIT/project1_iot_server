package mqttcredential

import (
	"context"
	"reflect"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"iot-platform/internal/gateway"
)

var _ RecoveryRepository = (*PostgresRepository)(nil)

const recoveryColumns = `recovery_id,operation_id,gateway_id,attempted_version,origin,status,broker_epoch,ram_disabled,snapshot_observed,created_at,updated_at,completed_at`

func scanRecovery(row pgx.Row) (RecoveryRecord, error) {
	var r RecoveryRecord
	if err := row.Scan(&r.RecoveryID, &r.OperationID, &r.GatewayID, &r.AttemptedVersion, &r.Origin, &r.Status, &r.BrokerEpoch, &r.Evidence.RAMDisabled, &r.Evidence.SnapshotObserved, &r.CreatedAt, &r.UpdatedAt, &r.CompletedAt); err != nil {
		return r, safeReadError(err)
	}
	if r.Status == RecoveryDisabled {
		r.Evidence.BrokerEpoch = r.BrokerEpoch
	}
	if r.Validate() != nil {
		return RecoveryRecord{}, &DomainError{Code: CodeInternalError}
	}
	return r, nil
}

// ResolveRecovery loads a recovery record by recovery ID.
func (r *PostgresRepository) ResolveRecovery(ctx context.Context, id uuid.UUID) (RecoveryRecord, error) {
	if id == uuid.Nil {
		return RecoveryRecord{}, &DomainError{Code: CodeInvalidRequest}
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return RecoveryRecord{}, &CommitOutcomeUnknown{OperationID: id}
	}
	defer conn.Release()
	record, err := scanRecovery(conn.QueryRow(ctx, `SELECT `+recoveryColumns+` FROM public.mqtt_credential_recovery WHERE recovery_id=$1`, id))
	if err != nil {
		return RecoveryRecord{}, &CommitOutcomeUnknown{OperationID: id}
	}
	return record, nil
}

// ListPendingRecovery lists pending recovery records ordered by recovery ID.
func (r *PostgresRepository) ListPendingRecovery(ctx context.Context, cursor uuid.UUID, limit int) ([]RecoveryRecord, error) {
	if limit <= 0 || limit > r.scanLimit {
		return nil, &DomainError{Code: CodeInvalidRequest}
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	rows, err := r.pool.Query(ctx, `SELECT `+recoveryColumns+` FROM public.mqtt_credential_recovery WHERE status='pending' AND recovery_id>$1 ORDER BY recovery_id LIMIT $2`, cursor, limit)
	if err != nil {
		return nil, safeReadError(err)
	}
	defer rows.Close()
	result := make([]RecoveryRecord, 0)
	for rows.Next() {
		record, e := scanRecovery(rows)
		if e != nil {
			return nil, e
		}
		result = append(result, record)
	}
	if rows.Err() != nil {
		return nil, safeReadError(rows.Err())
	}
	return result, nil
}

func validRecoveryRequest(q RecoveryRequest) bool {
	g := q.Guard
	return q.RecoveryID != uuid.Nil && gateway.ValidateGatewayID(g.GatewayID) == nil && g.ExpectedOperationID != uuid.Nil && g.ExpectedOperationStatus.Validate() == nil && g.ExpectedCredentialStatus.Validate() == nil && g.ExpectedCredentialVersion > 0 && (g.ExpectedStatus == MaintenanceInProgress || g.ExpectedStatus == MaintenanceRecoveryNeeded) && (g.ExpectedEpoch == nil || validEpoch(*g.ExpectedEpoch)) && validEpoch(q.BrokerEpoch)
}

func (r *PostgresRepository) recoveryTx(ctx context.Context, gatewayID string) (pgx.Tx, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, safeReadError(err)
	}
	var parent string
	if err = tx.QueryRow(ctx, `SELECT gateway_id FROM public.gateways WHERE gateway_id=$1 FOR UPDATE`, gatewayID).Scan(&parent); err != nil {
		r.rollbackRecovery(tx)
		return nil, safeReadError(err)
	}
	return tx, nil
}

func (r *PostgresRepository) rollbackRecovery(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (r *PostgresRepository) checkRecoveryGuard(ctx context.Context, tx pgx.Tx, g MaintenanceGuard) (Operation, Metadata, error) {
	m, err := scanMetadata(tx.QueryRow(ctx, metadataQuery, g.GatewayID))
	if err != nil {
		return Operation{}, m, err
	}
	p, err := scanOperation(tx.QueryRow(ctx, `SELECT `+operationColumns+` FROM public.gateway_mqtt_credential_events WHERE operation_id=$1 AND gateway_id=$2 FOR UPDATE`, g.ExpectedOperationID, g.GatewayID))
	if err != nil {
		return Operation{}, m, err
	}
	c, err := scanMaintenance(tx.QueryRow(ctx, `SELECT `+maintenanceColumns+` FROM public.mqtt_credential_maintenance WHERE operation_id=$1 AND gateway_id=$2 FOR UPDATE`, g.ExpectedOperationID, g.GatewayID))
	if err != nil {
		return Operation{}, m, err
	}
	if p.legacy || p.RecoveryID != nil || m.LastOperationID != g.ExpectedOperationID || p.Status != g.ExpectedOperationStatus || m.Status != g.ExpectedCredentialStatus || m.CredentialVersion != g.ExpectedCredentialVersion || c.Status != g.ExpectedStatus || !reflect.DeepEqual(c.BrokerEpoch, g.ExpectedEpoch) {
		return Operation{}, m, &DomainError{Code: CodeCredentialConflict}
	}
	return p.Operation, m, nil
}

// BeginRecovery inserts or verifies an in-progress recovery record for an operation.
func (r *PostgresRepository) BeginRecovery(ctx context.Context, q RecoveryRequest) (RecoveryRecord, error) {
	if !validRecoveryRequest(q) {
		return RecoveryRecord{}, &DomainError{Code: CodeInvalidRequest}
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.recoveryTx(ctx, q.Guard.GatewayID)
	if err != nil {
		return RecoveryRecord{}, err
	}
	defer r.rollbackRecovery(tx)
	// Existing identity is lookup-only, including after reprovision. Never resume
	// broker execution merely because a completed record is returned.
	existing, e := scanRecovery(tx.QueryRow(ctx, `SELECT `+recoveryColumns+` FROM public.mqtt_credential_recovery WHERE recovery_id=$1`, q.RecoveryID))
	if e == nil {
		if existing.OperationID != q.Guard.ExpectedOperationID || existing.GatewayID != q.Guard.GatewayID || existing.BrokerEpoch != q.BrokerEpoch {
			return RecoveryRecord{}, &DomainError{Code: CodeCredentialConflict}
		}
		if existing.Status == RecoveryPending {
			if _, _, err := r.checkRecoveryGuard(ctx, tx, q.Guard); err != nil {
				return RecoveryRecord{}, err
			}
		}
		return existing, nil
	}
	if de, ok := e.(*DomainError); !ok || de.Code != CodeNotFound {
		return RecoveryRecord{}, e
	}
	o, _, err := r.checkRecoveryGuard(ctx, tx, q.Guard)
	if err != nil {
		return RecoveryRecord{}, err
	}
	if q.Guard.ExpectedEpoch != nil && *q.Guard.ExpectedEpoch == q.BrokerEpoch {
		return RecoveryRecord{}, &DomainError{Code: CodeInvalidRequest}
	}
	record, err := scanRecovery(tx.QueryRow(ctx, `INSERT INTO public.mqtt_credential_recovery(recovery_id,operation_id,gateway_id,attempted_version,broker_epoch) VALUES($1,$2,$3,$4,$5) RETURNING `+recoveryColumns, q.RecoveryID, o.OperationID, o.GatewayID, o.CredentialVersion, q.BrokerEpoch))
	if err != nil {
		return RecoveryRecord{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return RecoveryRecord{}, &CommitOutcomeUnknown{OperationID: q.RecoveryID}
	}
	return record, nil
}

// BindRecoveryEpoch rebinds a pending recovery record to a new broker epoch.
func (r *PostgresRepository) BindRecoveryEpoch(ctx context.Context, id uuid.UUID, expected, epoch string) (RecoveryRecord, error) {
	if id == uuid.Nil || !validEpoch(expected) || !validEpoch(epoch) {
		return RecoveryRecord{}, &DomainError{Code: CodeInvalidRequest}
	}
	// Independent identity read precedes Gateway locking; no write/row lock here.
	record, err := r.ResolveRecovery(ctx, id)
	if err != nil {
		return RecoveryRecord{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.recoveryTx(ctx, record.GatewayID)
	if err != nil {
		return RecoveryRecord{}, err
	}
	defer r.rollbackRecovery(tx)
	record, err = scanRecovery(tx.QueryRow(ctx, `UPDATE public.mqtt_credential_recovery SET broker_epoch=$3,updated_at=now() WHERE recovery_id=$1 AND broker_epoch=$2 AND status='pending' RETURNING `+recoveryColumns, id, expected, epoch))
	if err != nil {
		return RecoveryRecord{}, recoveryConflict(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return RecoveryRecord{}, &CommitOutcomeUnknown{OperationID: id}
	}
	return record, nil
}

func recoveryConflict(err error) error {
	if de, ok := err.(*DomainError); ok && de.Code == CodeNotFound {
		return &DomainError{Code: CodeCredentialConflict}
	}
	return err
}

// CompleteRecovery finalizes a recovery record with disable evidence and updates credential metadata.
func (r *PostgresRepository) CompleteRecovery(ctx context.Context, q RecoveryRequest, evidence RecoveryDisableEvidence) (Metadata, error) {
	if !validRecoveryRequest(q) || evidence.Validate(q.BrokerEpoch) != nil {
		return Metadata{}, &DomainError{Code: CodeInvalidRequest}
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.recoveryTx(ctx, q.Guard.GatewayID)
	if err != nil {
		return Metadata{}, err
	}
	defer r.rollbackRecovery(tx)
	record, err := scanRecovery(tx.QueryRow(ctx, `SELECT `+recoveryColumns+` FROM public.mqtt_credential_recovery WHERE recovery_id=$1`, q.RecoveryID))
	if err != nil {
		return Metadata{}, recoveryConflict(err)
	}
	if record.OperationID != q.Guard.ExpectedOperationID || record.GatewayID != q.Guard.GatewayID || record.BrokerEpoch != q.BrokerEpoch {
		return Metadata{}, &DomainError{Code: CodeCredentialConflict}
	}
	if record.Status == RecoveryDisabled {
		return scanMetadata(tx.QueryRow(ctx, metadataQuery, record.GatewayID))
	}
	o, _, err := r.checkRecoveryGuard(ctx, tx, q.Guard)
	if err != nil {
		return Metadata{}, err
	}
	record, err = scanRecovery(tx.QueryRow(ctx, `UPDATE public.mqtt_credential_recovery SET status='disabled',ram_disabled=true,snapshot_observed=true,updated_at=now(),completed_at=now() WHERE recovery_id=$1 AND status='pending' AND broker_epoch=$2 RETURNING `+recoveryColumns, q.RecoveryID, q.BrokerEpoch))
	if err != nil {
		return Metadata{}, recoveryConflict(err)
	}
	_, err = tx.Exec(ctx, `UPDATE public.gateway_mqtt_credentials SET status='revoked',credential_version=$3,revoked_at=$4,last_error_code='credential_recovery_required',updated_at=$4 WHERE gateway_id=$1 AND last_operation_id=$2`, o.GatewayID, o.OperationID, record.AttemptedVersion, record.CompletedAt)
	if err != nil {
		return Metadata{}, safeReadError(err)
	}
	if !o.Terminal() {
		_, err = tx.Exec(ctx, `UPDATE public.gateway_mqtt_credential_events SET recovery_id=$2 WHERE operation_id=$1`, o.OperationID, q.RecoveryID)
		if err != nil {
			return Metadata{}, safeReadError(err)
		}
	}
	_, err = tx.Exec(ctx, `UPDATE public.mqtt_credential_maintenance SET status='completed',broker_epoch=$2,recovery_id=$3,error_code=NULL,updated_at=$4,completed_at=$4 WHERE operation_id=$1`, o.OperationID, q.BrokerEpoch, q.RecoveryID, record.CompletedAt)
	if err != nil {
		return Metadata{}, safeReadError(err)
	}
	m, err := scanMetadata(tx.QueryRow(ctx, metadataQuery, o.GatewayID))
	if err != nil {
		return Metadata{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Metadata{}, &CommitOutcomeUnknown{OperationID: q.RecoveryID}
	}
	return m, nil
}

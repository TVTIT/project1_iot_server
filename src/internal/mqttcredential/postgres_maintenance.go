package mqttcredential

import (
	"context"
	"encoding/hex"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"iot-platform/internal/gateway"
)

var _ MaintenanceRepository = (*PostgresRepository)(nil)

const maintenanceColumns = `operation_id,gateway_id,status,broker_epoch::text,error_code,created_at,updated_at,completed_at,recovery_id`

func scanMaintenance(row pgx.Row) (MaintenanceCheckpoint, error) {
	var c MaintenanceCheckpoint
	if err := row.Scan(&c.OperationID, &c.GatewayID, &c.Status, &c.BrokerEpoch, &c.ErrorCode, &c.CreatedAt, &c.UpdatedAt, &c.CompletedAt, &c.RecoveryID); err != nil {
		return c, safeReadError(err)
	}
	return c, nil
}

// ResolveMaintenance queries a maintenance checkpoint by operation ID.
func (r *PostgresRepository) ResolveMaintenance(ctx context.Context, id uuid.UUID) (MaintenanceCheckpoint, error) {
	if id == uuid.Nil {
		return MaintenanceCheckpoint{}, &DomainError{Code: CodeInvalidRequest}
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return MaintenanceCheckpoint{}, &CommitOutcomeUnknown{OperationID: id}
	}
	defer conn.Release()
	c, err := scanMaintenance(conn.QueryRow(ctx, `SELECT `+maintenanceColumns+` FROM public.mqtt_credential_maintenance WHERE operation_id=$1`, id))
	if err != nil {
		return MaintenanceCheckpoint{}, &CommitOutcomeUnknown{OperationID: id}
	}
	return c, nil
}

// ListPendingMaintenance lists incomplete maintenance checkpoints ordered by operation ID.
func (r *PostgresRepository) ListPendingMaintenance(ctx context.Context, cursor uuid.UUID, limit int) ([]MaintenanceCheckpoint, error) {
	if limit <= 0 || limit > r.scanLimit {
		return nil, &DomainError{Code: CodeInvalidRequest}
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	rows, err := r.pool.Query(ctx, `SELECT `+maintenanceColumns+` FROM public.mqtt_credential_maintenance WHERE status <> 'completed' AND operation_id>$1 ORDER BY operation_id LIMIT $2`, cursor, limit)
	if err != nil {
		return nil, safeReadError(err)
	}
	defer rows.Close()
	result := make([]MaintenanceCheckpoint, 0)
	for rows.Next() {
		c, e := scanMaintenance(rows)
		if e != nil {
			return nil, e
		}
		result = append(result, c)
	}
	if rows.Err() != nil {
		return nil, safeReadError(rows.Err())
	}
	return result, nil
}

func validEpoch(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}

// BindMaintenanceEpoch binds a broker lifetime epoch to an in-progress maintenance checkpoint.
func (r *PostgresRepository) BindMaintenanceEpoch(ctx context.Context, g MaintenanceGuard, epoch string) (MaintenanceCheckpoint, error) {
	if !validEpoch(epoch) {
		return MaintenanceCheckpoint{}, &DomainError{Code: CodeInvalidRequest}
	}
	return r.updateMaintenance(ctx, g, "bind", &epoch, nil)
}

// RequireMaintenanceRecovery marks a maintenance checkpoint as requiring recovery with an error code.
func (r *PostgresRepository) RequireMaintenanceRecovery(ctx context.Context, g MaintenanceGuard, code ErrorCode) (MaintenanceCheckpoint, error) {
	switch code {
	case CodeRecoveryRequired, CodeServiceUnavailable, CodeFinalizationPending, CodeVerificationUnavailable, CodeInternalError, CodeRuntimeBusy, CodeRuntimeDisabled:
	default:
		return MaintenanceCheckpoint{}, &DomainError{Code: CodeInvalidRequest}
	}
	return r.updateMaintenance(ctx, g, "recovery", nil, &code)
}

// CompleteMaintenance marks a maintenance checkpoint as completed after verified commit.
func (r *PostgresRepository) CompleteMaintenance(ctx context.Context, g MaintenanceGuard) (MaintenanceCheckpoint, error) {
	if g.ExpectedEpoch == nil {
		return MaintenanceCheckpoint{}, &DomainError{Code: CodeInvalidRequest}
	}
	return r.updateMaintenance(ctx, g, "complete", nil, nil)
}

func (r *PostgresRepository) updateMaintenance(ctx context.Context, g MaintenanceGuard, action string, epoch *string, code *ErrorCode) (MaintenanceCheckpoint, error) {
	if gateway.ValidateGatewayID(g.GatewayID) != nil || g.ExpectedOperationID == uuid.Nil || g.ExpectedOperationStatus.Validate() != nil || g.ExpectedCredentialStatus.Validate() != nil || g.ExpectedCredentialVersion <= 0 || (g.ExpectedStatus != MaintenanceInProgress && g.ExpectedStatus != MaintenanceRecoveryNeeded) || (g.ExpectedEpoch != nil && !validEpoch(*g.ExpectedEpoch)) {
		return MaintenanceCheckpoint{}, &DomainError{Code: CodeInvalidRequest}
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return MaintenanceCheckpoint{}, safeReadError(err)
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), r.timeout)
		defer done()
		_ = tx.Rollback(cleanup)
	}()
	var parent string
	if err = tx.QueryRow(ctx, `SELECT gateway_id FROM public.gateways WHERE gateway_id=$1 FOR UPDATE`, g.GatewayID).Scan(&parent); err != nil {
		return MaintenanceCheckpoint{}, safeReadError(err)
	}
	status := g.ExpectedStatus
	switch action {
	case "complete":
		status = MaintenanceCompleted
	case "recovery":
		status = MaintenanceRecoveryNeeded
	}
	c, err := scanMaintenance(tx.QueryRow(ctx, `UPDATE public.mqtt_credential_maintenance p SET status=$8, broker_epoch=CASE WHEN $9='bind' THEN $10::text ELSE p.broker_epoch END,error_code=$11,updated_at=now(),completed_at=CASE WHEN $9='complete' THEN now() ELSE NULL END
WHERE p.operation_id=$1 AND p.gateway_id=$2 AND p.status=$3 AND p.broker_epoch IS NOT DISTINCT FROM $4::text
AND EXISTS(SELECT 1 FROM public.gateway_mqtt_credentials c JOIN public.gateway_mqtt_credential_events e ON e.operation_id=c.last_operation_id AND e.gateway_id=c.gateway_id WHERE c.gateway_id=p.gateway_id AND c.last_operation_id=p.operation_id AND e.status=$5 AND c.status=$6 AND c.credential_version=$7 AND NOT e.legacy_projection AND ($9<>'complete' OR e.status IN ('succeeded','failed')))
RETURNING `+maintenanceColumns, g.ExpectedOperationID, g.GatewayID, g.ExpectedStatus, g.ExpectedEpoch, g.ExpectedOperationStatus, g.ExpectedCredentialStatus, g.ExpectedCredentialVersion, status, action, epoch, code))
	if err != nil {
		if de, ok := err.(*DomainError); ok && de.Code == CodeNotFound {
			return MaintenanceCheckpoint{}, &DomainError{Code: CodeCredentialConflict}
		}
		return MaintenanceCheckpoint{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return MaintenanceCheckpoint{}, &CommitOutcomeUnknown{OperationID: g.ExpectedOperationID}
	}
	return c, nil
}

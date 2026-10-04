package mqttcredential

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"iot-platform/internal/gateway"
)

// PostgresRepository persists safe business intent, observations and completion.
type PostgresRepository struct {
	pool      *pgxpool.Pool
	timeout   time.Duration
	scanLimit int
}

// Optional scanLimit bounds reconciliation pages without changing existing callers.
func NewPostgresRepository(pool *pgxpool.Pool, timeout time.Duration, scanLimit ...int) (*PostgresRepository, error) {
	limit := maxRepositoryScanLimit
	if len(scanLimit) == 1 {
		limit = scanLimit[0]
	}
	if pool == nil || timeout <= 0 || len(scanLimit) > 1 || limit <= 0 || limit > maxRepositoryScanLimit {
		return nil, &DomainError{Code: CodeInvalidRequest}
	}
	return &PostgresRepository{pool: pool, timeout: timeout, scanLimit: limit}, nil
}

func safeReadError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return &DomainError{Code: CodeNotFound}
	}
	return &DomainError{Code: CodeServiceUnavailable}
}

func (r *PostgresRepository) IsPlatformAdmin(ctx context.Context, actor uuid.UUID) (bool, error) {
	if actor == uuid.Nil {
		return false, &DomainError{Code: CodeInvalidRequest}
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	var allowed bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.platform_admins WHERE user_id=$1)`, actor).Scan(&allowed)
	if err != nil {
		return false, safeReadError(err)
	}
	return allowed, nil
}

const metadataQuery = `SELECT c.gateway_id, c.gateway_id, c.credential_version, c.status,
c.last_operation_id, CASE WHEN e.recovery_id IS NOT NULL THEN 'resolved_by_recovery' ELSE COALESCE(e.status,'') END, c.activated_at, c.revoked_at, c.last_error_code
FROM public.gateway_mqtt_credentials c LEFT JOIN public.gateway_mqtt_credential_events e
ON e.operation_id=c.last_operation_id AND e.gateway_id=c.gateway_id WHERE c.gateway_id=$1`

func scanMetadata(row pgx.Row) (Metadata, error) {
	var m Metadata
	var id *uuid.UUID
	err := row.Scan(&m.GatewayID, &m.Username, &m.CredentialVersion, &m.Status, &id,
		&m.OperationStatus, &m.ActivatedAt, &m.RevokedAt, &m.LastErrorCode)
	if err != nil {
		return Metadata{}, safeReadError(err)
	}
	if id != nil {
		m.LastOperationID = *id
	}
	return m, nil
}

// GetMetadata is an internal read; public callers must perform a fresh admin check.
func (r *PostgresRepository) GetMetadata(ctx context.Context, id string) (Metadata, error) {
	if gateway.ValidateGatewayID(id) != nil {
		return Metadata{}, &DomainError{Code: CodeInvalidRequest}
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	return scanMetadata(r.pool.QueryRow(ctx, metadataQuery, id))
}

// CommitOutcomeUnknown never grants execution, even when a later lookup is absent.
// OperationID is server-generated and permits a bounded reconciliation lookup.
type CommitOutcomeUnknown struct{ OperationID uuid.UUID }

func (e *CommitOutcomeUnknown) Error() string { return string(CodeFinalizationPending) }
func (e *CommitOutcomeUnknown) Unwrap() error { return &DomainError{Code: CodeFinalizationPending} }

const operationColumns = `operation_id, actor_user_id, gateway_id, idempotency_key, action,
credential_version, status, phase, ram_applied, snapshot_observed, fresh_positive_verified,
delivery_status, created_at, updated_at, completed_at, previous_status,
previous_credential_version, previous_activated_at, previous_revoked_at, error_code, legacy_projection, recovery_id`

// operationProjection preserves legacy recover, NULL actors/versions and missing
// proof verbatim. Legacy is not a modern operation and must never enter replay.
type operationProjection struct {
	Operation
	legacy bool
}

func scanOperation(row pgx.Row) (operationProjection, error) {
	var p operationProjection
	var actor, key *uuid.UUID
	var version, previousVersion *int64
	var phase *OperationPhase
	var updated *time.Time
	var previousStatus *CredentialStatus
	var previous CredentialSnapshot
	err := row.Scan(&p.OperationID, &actor, &p.GatewayID, &key, &p.Action, &version,
		&p.Status, &phase, &p.Evidence.RAMApplied, &p.Evidence.SnapshotObserved,
		&p.Evidence.FreshPositiveVerified, &p.DeliveryStatus, &p.CreatedAt, &updated,
		&p.CompletedAt, &previousStatus, &previousVersion, &previous.ActivatedAt,
		&previous.RevokedAt, &p.ErrorCode, &p.legacy, &p.RecoveryID)
	if err != nil {
		return p, safeReadError(err)
	}
	if actor != nil {
		p.ActorUserID = *actor
	}
	if key != nil {
		p.IdempotencyKey = *key
	}
	if version != nil {
		p.CredentialVersion = *version
	}
	if phase != nil {
		p.Phase = *phase
	}
	if updated != nil {
		p.UpdatedAt = *updated
	}
	if previousStatus != nil && previousVersion != nil {
		previous.Status, previous.CredentialVersion = *previousStatus, *previousVersion
		p.Previous = &previous
	}
	if !p.legacy && p.ValidateHistorical() != nil {
		return p, &DomainError{Code: CodeInternalError}
	}
	return p, nil
}

// ResolveCommitAmbiguity uses an independent pool checkout, never the uncertain
// transaction. Absence is still uncertainty, not evidence of rollback.
func (r *PostgresRepository) ResolveCommitAmbiguity(ctx context.Context, id uuid.UUID) (Operation, error) {
	if id == uuid.Nil {
		return Operation{}, &DomainError{Code: CodeInvalidRequest}
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return Operation{}, &CommitOutcomeUnknown{OperationID: id}
	}
	defer conn.Release()
	p, err := scanOperation(conn.QueryRow(ctx, `SELECT `+operationColumns+` FROM public.gateway_mqtt_credential_events WHERE operation_id=$1`, id))
	if err != nil || p.legacy {
		return Operation{}, &CommitOutcomeUnknown{OperationID: id}
	}
	return p.Operation, nil
}

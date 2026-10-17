package digitaltwin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"iot-platform/internal/auth"
	"iot-platform/internal/digitaltwin/mapper"
	"iot-platform/internal/gateway"
)

// TxDatabase is satisfied by the application's existing pgx connection pool.
type TxDatabase interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

// PostgresGatewayMetadataPatcher implements GatewayMetadataPatcher using PostgreSQL transactions.
type PostgresGatewayMetadataPatcher struct {
	db TxDatabase
}

var _ GatewayMetadataPatcher = (*PostgresGatewayMetadataPatcher)(nil)

// NewPostgresGatewayMetadataPatcher constructs a new PostgresGatewayMetadataPatcher.
// It verifies that db is non-nil (rejecting typed nils via reflection).
func NewPostgresGatewayMetadataPatcher(db TxDatabase) (*PostgresGatewayMetadataPatcher, error) {
	if isNilDependency(db) {
		return nil, fmt.Errorf("database is required")
	}
	return &PostgresGatewayMetadataPatcher{db: db}, nil
}

// PatchGatewayMetadata executes an atomic Gateway metadata PATCH in one bounded transaction.
// It enforces platform-admin authorization FIRST before looking up target resources,
// locks the Gateway row and canonical Twin entity in strict order, validates graph and state
// consistency without repairing corrupt data, performs same-value no-op detection,
// and atomically updates relational and Twin metadata.
func (p *PostgresGatewayMetadataPatcher) PatchGatewayMetadata(ctx context.Context, actor uuid.UUID, gatewayID string, input PatchGatewayInput) (PatchGatewayResult, error) {
	if isNilDependency(p) || isNilDependency(p.db) {
		return PatchGatewayResult{}, ErrDependencyUnavailable
	}
	if ctx == nil {
		return PatchGatewayResult{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return PatchGatewayResult{}, classifyAvailabilityError(err)
	}
	if actor == uuid.Nil {
		return PatchGatewayResult{}, ErrNilActor
	}
	if err := gateway.ValidateGatewayID(gatewayID); err != nil {
		return PatchGatewayResult{}, ErrInvalidInput
	}
	if err := ValidatePatchGatewayInput(input); err != nil {
		return PatchGatewayResult{}, err
	}

	tx, err := p.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return PatchGatewayResult{}, classifyAvailabilityError(fmt.Errorf("begin gateway patch tx: %w", err))
	}
	defer rollbackTx(tx)

	// Step 1: Platform admin DB check FIRST (non-admin -> forbidden before target lookup).
	checker, err := auth.NewPostgresPlatformAdminChecker(tx)
	if err != nil {
		return PatchGatewayResult{}, classifyAvailabilityError(fmt.Errorf("admin checker init: %w", err))
	}
	isAdmin, err := checker.IsPlatformAdmin(ctx, actor)
	if err != nil {
		return PatchGatewayResult{}, classifyAvailabilityError(fmt.Errorf("admin check: %w", err))
	}
	if !isAdmin {
		return PatchGatewayResult{}, ErrForbidden
	}

	// Step 2: Lock Gateway row FOR UPDATE.
	var currentName string
	var currentDescription *string
	err = tx.QueryRow(ctx, `SELECT name, description FROM public.gateways WHERE gateway_id = $1 FOR UPDATE`, gatewayID).
		Scan(&currentName, &currentDescription)
	if errors.Is(err, pgx.ErrNoRows) {
		return PatchGatewayResult{}, ErrNotFound
	}
	if err != nil {
		return PatchGatewayResult{}, classifyAvailabilityError(fmt.Errorf("lock gateway row: %w", err))
	}

	// Step 3: Verify exact canonical Gateway Twin and lock Twin metadata.
	// Only lock Twin metadata after Gateway; do not update or lock state as a metadata write target.
	expectedEntityID := mapper.GatewayEntityID(gatewayID)
	var twinInternalID uuid.UUID
	var twinName string
	var twinUpdatedAt time.Time
	var hasState bool
	var gwEntityCount int

	err = tx.QueryRow(ctx, `
		SELECT e.id, e.name, e.updated_at,
		       EXISTS(SELECT 1 FROM public.twin_states s WHERE s.entity_id = e.id) AS has_state,
		       (SELECT count(*) FROM public.twin_entities WHERE gateway_id = $2 AND entity_type = 'Gateway') AS gw_entity_count
		FROM public.twin_entities e
		WHERE e.entity_id = $1 AND e.entity_type = 'Gateway' AND e.gateway_id = $2
		FOR UPDATE OF e
	`, expectedEntityID, gatewayID).Scan(&twinInternalID, &twinName, &twinUpdatedAt, &hasState, &gwEntityCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return PatchGatewayResult{}, ErrInconsistentGraph
	}
	if err != nil {
		return PatchGatewayResult{}, classifyAvailabilityError(fmt.Errorf("lock twin entity: %w", err))
	}

	// Verify graph invariants: single twin entity, state exists, and name matches relational name.
	if gwEntityCount != 1 || !hasState || twinName != currentName {
		return PatchGatewayResult{}, ErrInconsistentGraph
	}

	// Step 4: Compare exact nullable/string values under locks.
	targetName := currentName
	nameChanged := false
	if input.Name != nil {
		targetName = *input.Name
		nameChanged = (targetName != currentName)
	}

	targetDescription := currentDescription
	descChanged := false
	if input.DescriptionSet {
		targetDescription = input.Description
		descChanged = !sameNullableString(targetDescription, currentDescription)
	}

	hasActualChange := nameChanged || descChanged

	// Step 5: Same-value PATCH is a no-op: executes no metadata UPDATE and returns unchanged timestamp.
	if !hasActualChange {
		if err := tx.Commit(ctx); err != nil {
			return PatchGatewayResult{}, classifyCommitError(err)
		}
		return PatchGatewayResult{
			ID:          expectedEntityID,
			Type:        EntityTypeGateway,
			GatewayID:   gatewayID,
			Name:        currentName,
			Description: currentDescription,
			UpdatedAt:   twinUpdatedAt,
		}, nil
	}

	// Step 6: Atomic metadata writes for actual changes.
	if nameChanged && descChanged {
		if _, err := tx.Exec(ctx, `UPDATE public.gateways SET name = $1, description = $2 WHERE gateway_id = $3`, targetName, targetDescription, gatewayID); err != nil {
			return PatchGatewayResult{}, classifyAvailabilityError(fmt.Errorf("update gateway name and desc: %w", err))
		}
	} else if nameChanged {
		if _, err := tx.Exec(ctx, `UPDATE public.gateways SET name = $1 WHERE gateway_id = $2`, targetName, gatewayID); err != nil {
			return PatchGatewayResult{}, classifyAvailabilityError(fmt.Errorf("update gateway name: %w", err))
		}
	} else if descChanged {
		if _, err := tx.Exec(ctx, `UPDATE public.gateways SET description = $1 WHERE gateway_id = $2`, targetDescription, gatewayID); err != nil {
			return PatchGatewayResult{}, classifyAvailabilityError(fmt.Errorf("update gateway desc: %w", err))
		}
	}

	// Server-owned metadata time must reflect the write statement that runs AFTER
	// the Gateway and Twin row locks are acquired, not the transaction start.
	// CURRENT_TIMESTAMP/transaction_timestamp is fixed at transaction start, so a
	// transaction that began earlier but only acquired the target lock after a
	// later competing PATCH committed could store/return an updated_at that
	// regressed behind the committed value. statement_timestamp() is evaluated
	// when this UPDATE statement begins, which is necessarily at or after the
	// competing commit because the row lock was already held by the preceding
	// SELECT ... FOR UPDATE statements.
	var newUpdatedAt time.Time
	if nameChanged {
		err = tx.QueryRow(ctx, `UPDATE public.twin_entities SET name = $1, updated_at = statement_timestamp() WHERE id = $2 RETURNING updated_at`, targetName, twinInternalID).Scan(&newUpdatedAt)
	} else {
		err = tx.QueryRow(ctx, `UPDATE public.twin_entities SET updated_at = statement_timestamp() WHERE id = $1 RETURNING updated_at`, twinInternalID).Scan(&newUpdatedAt)
	}
	if err != nil {
		return PatchGatewayResult{}, classifyAvailabilityError(fmt.Errorf("update twin entity: %w", err))
	}

	if err := tx.Commit(ctx); err != nil {
		return PatchGatewayResult{}, classifyCommitError(err)
	}

	return PatchGatewayResult{
		ID:          expectedEntityID,
		Type:        EntityTypeGateway,
		GatewayID:   gatewayID,
		Name:        targetName,
		Description: targetDescription,
		UpdatedAt:   newUpdatedAt,
	}, nil
}

func rollbackTx(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func sameNullableString(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a != nil && b != nil {
		return *a == *b
	}
	return false
}

func classifyAvailabilityError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(ErrDependencyUnavailable, err)
	}
	classified := gateway.ClassifyPostgresAvailabilityError(err, ErrDependencyUnavailable)
	if errors.Is(classified, ErrDependencyUnavailable) {
		return classified
	}
	return fmt.Errorf("%w: %w", ErrUnexpected, err)
}

func classifyCommitError(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrMetadataPatchUnavailable, err)
}

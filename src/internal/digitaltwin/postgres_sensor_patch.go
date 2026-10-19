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

// PostgresSensorMetadataPatcher implements SensorMetadataPatcher using PostgreSQL transactions.
type PostgresSensorMetadataPatcher struct {
	db TxDatabase
}

var _ SensorMetadataPatcher = (*PostgresSensorMetadataPatcher)(nil)

// NewPostgresSensorMetadataPatcher constructs a new PostgresSensorMetadataPatcher.
// It verifies that db is non-nil (rejecting typed nils via reflection).
func NewPostgresSensorMetadataPatcher(db TxDatabase) (*PostgresSensorMetadataPatcher, error) {
	if isNilDependency(db) {
		return nil, fmt.Errorf("database is required")
	}
	return &PostgresSensorMetadataPatcher{db: db}, nil
}

// PatchSensorMetadata executes an atomic Sensor metadata PATCH in one bounded transaction.
// It enforces platform-admin authorization or Gateway-scoped owner membership, locks canonical
// parent Gateway, membership (when needed), Sensor, and Twin metadata in strict order,
// verifies parent and Sensor graph/state consistency without repairing corrupt data,
// performs same-value no-op detection, and atomically updates relational and Twin metadata.
func (p *PostgresSensorMetadataPatcher) PatchSensorMetadata(ctx context.Context, actor uuid.UUID, gatewayID string, sensorID string, input PatchSensorInput) (PatchSensorResult, error) {
	if isNilDependency(p) || isNilDependency(p.db) {
		return PatchSensorResult{}, ErrDependencyUnavailable
	}
	if ctx == nil {
		return PatchSensorResult{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return PatchSensorResult{}, classifyAvailabilityError(err)
	}
	if actor == uuid.Nil {
		return PatchSensorResult{}, ErrNilActor
	}
	if err := gateway.ValidateGatewayID(gatewayID); err != nil {
		return PatchSensorResult{}, ErrInvalidInput
	}
	if err := gateway.ValidateSensorID(sensorID); err != nil {
		return PatchSensorResult{}, ErrInvalidInput
	}
	if err := ValidatePatchSensorInput(input); err != nil {
		return PatchSensorResult{}, err
	}

	tx, err := p.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return PatchSensorResult{}, classifyAvailabilityError(fmt.Errorf("begin sensor patch tx: %w", err))
	}
	defer rollbackTx(tx)

	// Step 1: Platform admin DB check FIRST.
	checker, err := auth.NewPostgresPlatformAdminChecker(tx)
	if err != nil {
		return PatchSensorResult{}, classifyAvailabilityError(fmt.Errorf("admin checker init: %w", err))
	}
	isAdmin, err := checker.IsPlatformAdmin(ctx, actor)
	if err != nil {
		return PatchSensorResult{}, classifyAvailabilityError(fmt.Errorf("admin check: %w", err))
	}

	// Step 2: Lock canonical parent Gateway resource FOR SHARE.
	var parentName string
	err = tx.QueryRow(ctx, `SELECT name FROM public.gateways WHERE gateway_id = $1 FOR SHARE`, gatewayID).Scan(&parentName)
	if errors.Is(err, pgx.ErrNoRows) {
		return PatchSensorResult{}, ErrNotFound
	}
	if err != nil {
		return PatchSensorResult{}, classifyAvailabilityError(fmt.Errorf("lock gateway parent: %w", err))
	}

	// Step 3: If not admin, verify and lock membership FOR SHARE (absent -> ErrNotFound, non-owner -> ErrForbidden).
	if !isAdmin {
		var role string
		err = tx.QueryRow(ctx, `SELECT role FROM public.user_gateways WHERE user_id = $1 AND gateway_id = $2 FOR SHARE`, actor, gatewayID).Scan(&role)
		if errors.Is(err, pgx.ErrNoRows) {
			return PatchSensorResult{}, ErrNotFound
		}
		if err != nil {
			return PatchSensorResult{}, classifyAvailabilityError(fmt.Errorf("lookup membership: %w", err))
		}
		if role != string(gateway.RoleOwner) {
			return PatchSensorResult{}, ErrForbidden
		}
	}

	// Step 4: Lock exact Sensor (gateway_id, sensor_id) FOR UPDATE.
	var currentName string
	var currentUnit *string
	err = tx.QueryRow(ctx, `SELECT name, unit FROM public.sensors WHERE gateway_id = $1 AND sensor_id = $2 FOR UPDATE`, gatewayID, sensorID).Scan(&currentName, &currentUnit)
	if errors.Is(err, pgx.ErrNoRows) {
		return PatchSensorResult{}, ErrNotFound
	}
	if err != nil {
		return PatchSensorResult{}, classifyAvailabilityError(fmt.Errorf("lock sensor row: %w", err))
	}

	// Step 5: Verify canonical parent Gateway Twin and resolve parent internal UUID.
	expectedParentEntityID := mapper.GatewayEntityID(gatewayID)
	var parentTwinID uuid.UUID
	err = tx.QueryRow(ctx, `
		SELECT e.id
		FROM public.twin_entities e
		JOIN public.twin_states s ON s.entity_id = e.id
		WHERE e.entity_id = $1 AND e.entity_type = 'Gateway' AND e.gateway_id = $2 AND e.name = $3
		  AND (SELECT count(*) FROM public.twin_entities WHERE gateway_id = $2 AND entity_type = 'Gateway') = 1
	`, expectedParentEntityID, gatewayID, parentName).Scan(&parentTwinID)
	if errors.Is(err, pgx.ErrNoRows) {
		return PatchSensorResult{}, ErrInconsistentGraph
	}
	if err != nil {
		return PatchSensorResult{}, classifyAvailabilityError(fmt.Errorf("resolve parent twin: %w", err))
	}

	// Step 6: Lock Sensor Twin metadata and verify existing hasSensor relationship and state.
	expectedSensorEntityID := mapper.SensorEntityID(gatewayID, sensorID)
	var sensorTwinInternalID uuid.UUID
	var sensorTwinName string
	var sensorTwinUpdatedAt time.Time
	var hasState bool
	var hasRelationship bool

	err = tx.QueryRow(ctx, `
		SELECT e.id, e.name, e.updated_at,
		       EXISTS(SELECT 1 FROM public.twin_states s WHERE s.entity_id = e.id) AS has_state,
		       EXISTS(SELECT 1 FROM public.twin_relationships r WHERE r.source_entity_id = $3 AND r.relationship_type = 'hasSensor' AND r.target_entity_id = e.id) AS has_relationship
		FROM public.twin_entities e
		WHERE e.entity_id = $1 AND e.entity_type = 'Sensor' AND e.gateway_id = $2
		FOR UPDATE OF e
	`, expectedSensorEntityID, gatewayID, parentTwinID).Scan(&sensorTwinInternalID, &sensorTwinName, &sensorTwinUpdatedAt, &hasState, &hasRelationship)
	if errors.Is(err, pgx.ErrNoRows) {
		return PatchSensorResult{}, ErrInconsistentGraph
	}
	if err != nil {
		return PatchSensorResult{}, classifyAvailabilityError(fmt.Errorf("lock sensor twin: %w", err))
	}

	// Verify graph invariants: state exists, hasSensor relationship exists, and Twin name matches relational name.
	if !hasState || !hasRelationship || sensorTwinName != currentName {
		return PatchSensorResult{}, ErrInconsistentGraph
	}

	targetName := *input.Name
	nameChanged := (targetName != currentName)

	// Step 7: Same-value PATCH is a no-op: executes no metadata UPDATE and preserves old timestamp.
	if !nameChanged {
		if err := tx.Commit(ctx); err != nil {
			return PatchSensorResult{}, classifyCommitError(err)
		}
		return PatchSensorResult{
			ID:        expectedSensorEntityID,
			Type:      EntityTypeSensor,
			GatewayID: gatewayID,
			SensorID:  sensorID,
			Name:      currentName,
			Unit:      currentUnit,
			UpdatedAt: sensorTwinUpdatedAt,
		}, nil
	}

	// Step 8: Atomic metadata writes for name change.
	if _, err := tx.Exec(ctx, `UPDATE public.sensors SET name = $1 WHERE gateway_id = $2 AND sensor_id = $3`, targetName, gatewayID, sensorID); err != nil {
		return PatchSensorResult{}, classifyAvailabilityError(fmt.Errorf("update sensor name: %w", err))
	}

	// Server-owned metadata time must reflect the write statement that runs AFTER
	// the parent Gateway, membership, Sensor, and Twin locks are acquired, not the
	// transaction start. CURRENT_TIMESTAMP/transaction_timestamp is fixed at
	// transaction start, so a transaction that began earlier but only acquired the
	// Sensor lock after a later competing PATCH committed could store/return an
	// updated_at that regressed behind the committed value. statement_timestamp()
	// is evaluated when this UPDATE statement begins, which is necessarily at or
	// after the competing commit because the Sensor row lock was already held by
	// the preceding SELECT ... FOR UPDATE statement.
	var newUpdatedAt time.Time
	err = tx.QueryRow(ctx, `UPDATE public.twin_entities SET name = $1, updated_at = statement_timestamp() WHERE id = $2 RETURNING updated_at`, targetName, sensorTwinInternalID).Scan(&newUpdatedAt)
	if err != nil {
		return PatchSensorResult{}, classifyAvailabilityError(fmt.Errorf("update sensor twin entity: %w", err))
	}

	if err := tx.Commit(ctx); err != nil {
		return PatchSensorResult{}, classifyCommitError(err)
	}

	return PatchSensorResult{
		ID:        expectedSensorEntityID,
		Type:      EntityTypeSensor,
		GatewayID: gatewayID,
		SensorID:  sensorID,
		Name:      targetName,
		Unit:      currentUnit,
		UpdatedAt: newUpdatedAt,
	}, nil
}

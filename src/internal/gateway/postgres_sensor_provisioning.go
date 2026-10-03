package gateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"iot-platform/internal/auth"
	"iot-platform/internal/digitaltwin/mapper"
)

func (r *postgresProvisioningRepository) ProvisionSensor(ctx context.Context, actor uuid.UUID, input ProvisionSensorInput) (ProvisionSensorResult, error) {
	if err := ValidateProvisionSensorInput(actor, input); err != nil {
		return ProvisionSensorResult{}, err
	}
	tx, err := r.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ProvisionSensorResult{}, fmt.Errorf("begin Sensor provisioning: %w", err)
	}
	defer rollbackProvisioning(tx)
	checker, err := auth.NewPostgresPlatformAdminChecker(tx)
	if err != nil {
		return ProvisionSensorResult{}, fmt.Errorf("%w: %w", ErrProvisioningAdminLookup, err)
	}
	admin, err := checker.IsPlatformAdmin(ctx, actor)
	if err != nil {
		return ProvisionSensorResult{}, fmt.Errorf("%w: %w", ErrProvisioningAdminLookup, err)
	}
	if !admin {
		return ProvisionSensorResult{}, ErrProvisioningForbidden
	}
	// All same-parent provisioning takes this lock first. Subsequent statements
	// get fresh READ COMMITTED snapshots after a competing operation commits.
	var parentName string
	err = tx.QueryRow(ctx, `SELECT name FROM public.gateways WHERE gateway_id=$1 FOR UPDATE`, input.GatewayID).Scan(&parentName)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProvisionSensorResult{}, ErrProvisioningNotFound
	}
	if err != nil {
		return ProvisionSensorResult{}, fmt.Errorf("lock Sensor parent: %w", err)
	}
	var parent uuid.UUID
	err = tx.QueryRow(ctx, `SELECT e.id FROM public.twin_entities e JOIN public.twin_states s ON s.entity_id=e.id
	WHERE e.entity_id=$1 AND e.entity_type='Gateway' AND e.gateway_id=$2 AND e.name=$3
	AND (SELECT count(*) FROM public.twin_entities WHERE gateway_id=$2 AND entity_type='Gateway')=1`,
		mapper.GatewayEntityID(input.GatewayID), input.GatewayID, parentName).Scan(&parent)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProvisionSensorResult{}, ErrProvisioningInconsistent
	}
	if err != nil {
		return ProvisionSensorResult{}, fmt.Errorf("resolve Sensor parent twin: %w", err)
	}
	result := ProvisionSensorResult{GatewayID: input.GatewayID, SensorID: input.SensorID, Name: input.Name, Unit: input.Unit,
		EntityID: mapper.SensorEntityID(input.GatewayID, input.SensorID)}
	err = tx.QueryRow(ctx, `INSERT INTO public.sensors(gateway_id,sensor_id,name,unit) VALUES ($1,$2,$3,$4)
	ON CONFLICT (gateway_id,sensor_id) DO NOTHING RETURNING created_at`, input.GatewayID, input.SensorID, input.Name, input.Unit).Scan(&result.CreatedAt)
	switch {
	case err == nil:
		result.Created = true
		var entity uuid.UUID
		// A conflicting orphan twin is an inconsistent graph, never repaired.
		err = tx.QueryRow(ctx, `INSERT INTO public.twin_entities(entity_id,entity_type,name,gateway_id)
		VALUES ($1,'Sensor',$2,$3) ON CONFLICT DO NOTHING RETURNING id`, result.EntityID, input.Name, input.GatewayID).Scan(&entity)
		if errors.Is(err, pgx.ErrNoRows) {
			return ProvisionSensorResult{}, ErrProvisioningInconsistent
		}
		if err != nil {
			return ProvisionSensorResult{}, fmt.Errorf("insert Sensor twin: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO public.twin_states(entity_id) VALUES ($1)`, entity); err != nil {
			return ProvisionSensorResult{}, fmt.Errorf("insert Sensor state: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO public.twin_relationships(source_entity_id,relationship_type,target_entity_id)
		VALUES ($1,'hasSensor',$2)`, parent, entity); err != nil {
			return ProvisionSensorResult{}, fmt.Errorf("insert hasSensor: %w", err)
		}
	case errors.Is(err, pgx.ErrNoRows):
		if err := verifySensorRetry(ctx, tx, parent, input, &result); err != nil {
			return ProvisionSensorResult{}, err
		}
	default:
		return ProvisionSensorResult{}, fmt.Errorf("insert Sensor: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ProvisionSensorResult{}, fmt.Errorf("commit Sensor provisioning: %w", err)
	}
	return result, nil
}

func verifySensorRetry(ctx context.Context, tx pgx.Tx, parent uuid.UUID, input ProvisionSensorInput, result *ProvisionSensorResult) error {
	var name string
	var unit *string
	err := tx.QueryRow(ctx, `SELECT name,unit,created_at FROM public.sensors WHERE gateway_id=$1 AND sensor_id=$2 FOR UPDATE`, input.GatewayID, input.SensorID).Scan(&name, &unit, &result.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrProvisioningInconsistent
	}
	if err != nil {
		return fmt.Errorf("read existing Sensor: %w", err)
	}
	if name != input.Name || !sameNullableString(unit, input.Unit) {
		return ErrProvisioningConflict
	}
	// State may have advanced since creation. Only require its presence, without
	// comparing/resetting values, versions, actors or timestamps.
	var valid bool
	err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.twin_entities e
	JOIN public.twin_states s ON s.entity_id=e.id
	JOIN public.twin_relationships r ON r.target_entity_id=e.id
	WHERE e.entity_id=$1 AND e.entity_type='Sensor' AND e.gateway_id=$2 AND e.name=$3
	AND r.source_entity_id=$4 AND r.relationship_type='hasSensor')`, result.EntityID, input.GatewayID, input.Name, parent).Scan(&valid)
	if err != nil {
		return fmt.Errorf("verify Sensor twin graph: %w", err)
	}
	if !valid {
		return ErrProvisioningInconsistent
	}
	return nil
}

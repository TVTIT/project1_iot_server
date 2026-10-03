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

func (r *postgresProvisioningRepository) ProvisionGateway(ctx context.Context, actor uuid.UUID, input ProvisionGatewayInput) (ProvisionGatewayResult, error) {
	var result ProvisionGatewayResult
	if err := ValidateProvisionGatewayInput(actor, input); err != nil {
		return result, err
	}
	tx, err := r.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return result, fmt.Errorf("begin provisioning: %w", err)
	}
	defer rollbackProvisioning(tx)
	checker, err := auth.NewPostgresPlatformAdminChecker(tx)
	if err != nil {
		return result, fmt.Errorf("%w: %w", ErrProvisioningAdminLookup, err)
	}
	admin, err := checker.IsPlatformAdmin(ctx, actor)
	if err != nil {
		return result, fmt.Errorf("%w: %w", ErrProvisioningAdminLookup, err)
	}
	if !admin {
		return result, ErrProvisioningForbidden
	}
	// profiles is SELECT-only for appDB; the membership FK protects insertion.
	var owner uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM public.profiles WHERE id=$1`, input.OwnerUserID).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, ErrProvisioningNotFound
	}
	if err != nil {
		return result, fmt.Errorf("resolve provisioning owner: %w", err)
	}
	result = ProvisionGatewayResult{GatewayID: input.GatewayID, Name: input.Name, Description: input.Description,
		OwnerUserID: input.OwnerUserID, EntityID: mapper.GatewayEntityID(input.GatewayID)}
	err = tx.QueryRow(ctx, `INSERT INTO public.gateways(gateway_id,name,description) VALUES ($1,$2,$3)
	ON CONFLICT (gateway_id) DO NOTHING RETURNING created_at`, input.GatewayID, input.Name, input.Description).Scan(&result.CreatedAt)
	switch {
	case err == nil:
		result.Created = true
		if _, err := tx.Exec(ctx, `INSERT INTO public.user_gateways(user_id,gateway_id,role) VALUES ($1,$2,'owner')`, owner, input.GatewayID); err != nil {
			return ProvisionGatewayResult{}, fmt.Errorf("insert provisioning owner: %w", err)
		}
		var entity uuid.UUID
		if err := tx.QueryRow(ctx, `INSERT INTO public.twin_entities(entity_id,entity_type,name,gateway_id)
		VALUES ($1,'Gateway',$2,$3) RETURNING id`, result.EntityID, input.Name, input.GatewayID).Scan(&entity); err != nil {
			return ProvisionGatewayResult{}, fmt.Errorf("insert Gateway twin: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO public.twin_states(entity_id) VALUES ($1)`, entity); err != nil {
			return ProvisionGatewayResult{}, fmt.Errorf("insert Gateway state: %w", err)
		}
	case errors.Is(err, pgx.ErrNoRows):
		// A separate READ COMMITTED statement sees the winner after ON CONFLICT
		// waits for its commit. Never use a single-statement snapshot fallback.
		if err := verifyGatewayRetry(ctx, tx, input, &result); err != nil {
			return ProvisionGatewayResult{}, err
		}
	default:
		return ProvisionGatewayResult{}, fmt.Errorf("insert Gateway: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ProvisionGatewayResult{}, fmt.Errorf("commit Gateway provisioning: %w", err)
	}
	return result, nil
}

func verifyGatewayRetry(ctx context.Context, tx pgx.Tx, input ProvisionGatewayInput, result *ProvisionGatewayResult) error {
	var name string
	var description *string
	if err := tx.QueryRow(ctx, `SELECT name,description,created_at FROM public.gateways WHERE gateway_id=$1 FOR UPDATE`, input.GatewayID).
		Scan(&name, &description, &result.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrProvisioningInconsistent
		}
		return fmt.Errorf("read existing Gateway: %w", err)
	}
	if name != input.Name || !sameNullableString(description, input.Description) {
		return ErrProvisioningConflict
	}
	var owners, matchingOwners int
	if err := tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE user_id=$2)
	FROM public.user_gateways WHERE gateway_id=$1 AND role='owner'`, input.GatewayID, input.OwnerUserID).Scan(&owners, &matchingOwners); err != nil {
		return fmt.Errorf("verify Gateway owner: %w", err)
	}
	if owners == 0 || owners > 1 {
		return ErrProvisioningInconsistent
	}
	if matchingOwners != 1 {
		return ErrProvisioningConflict
	}
	// Resolve the expected URN, verify type/scope/name and require current state.
	// State contents and counters are intentionally not compared or modified.
	var valid bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
	SELECT 1 FROM public.twin_entities e JOIN public.twin_states s ON s.entity_id=e.id
	WHERE e.entity_id=$1 AND e.entity_type='Gateway' AND e.gateway_id=$2 AND e.name=$3
	) AND (SELECT count(*) FROM public.twin_entities WHERE gateway_id=$2 AND entity_type='Gateway')=1`,
		result.EntityID, input.GatewayID, input.Name).Scan(&valid); err != nil {
		return fmt.Errorf("verify Gateway twin graph: %w", err)
	}
	if !valid {
		return ErrProvisioningInconsistent
	}
	return nil
}

func sameNullableString(a, b *string) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

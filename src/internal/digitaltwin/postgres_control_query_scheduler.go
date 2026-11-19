package digitaltwin

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	maxControlQueryScanBatch = 16
	// MaxControlQueryPageSize bounds one repository scheduling call.
	MaxControlQueryPageSize = 16
)

// ControlQueryContext is the minimum database context captured by a transient
// query. It grants no authority and is rechecked before a snapshot is accepted.
type ControlQueryContext struct {
	EntityID       uuid.UUID
	EntityURN      string
	GatewayID      string
	SensorID       string
	DesiredVersion Version
}

type controlQueryCandidate struct {
	entityID  uuid.UUID
	urn       string
	gatewayID string
	sensorID  string
	version   Version
}

// ReserveDueControlQueries durably advances cooldown before returning any
// publishable work. Network publication must happen after this method returns.
// The scan and each reservation are bounded; offline Sensors become due again
// after cooldown and cannot pin the front of a future pass.
func (r *PostgresControlRepository) ReserveDueControlQueries(ctx context.Context, limit int, cooldown time.Duration) ([]ControlQueryContext, error) {
	if ctx == nil || limit < 1 || limit > MaxControlQueryPageSize ||
		cooldown < time.Second || cooldown > time.Hour {
		return nil, ErrInvalidInput
	}
	if r == nil || isNilDependency(r.db) || !r.config.Stage6Enabled {
		return nil, ErrDependencyUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, r.config.DBTimeout)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, classifyAvailabilityError(err)
	}
	defer rollbackTx(tx)
	rows, err := tx.Query(ctx, `SELECT e.id,e.entity_id,e.gateway_id,s.sensor_id,st.desired_version
		FROM public.sensors s
		JOIN public.twin_entities e ON e.gateway_id=s.gateway_id
			AND e.entity_id='urn:ngsi-ld:Sensor:'||s.gateway_id||':'||s.sensor_id
			AND e.entity_type='Sensor'
		JOIN public.twin_states st ON st.entity_id=e.id
		WHERE st.next_query_at IS NULL OR st.next_query_at<=clock_timestamp()
		ORDER BY st.next_query_at NULLS FIRST,e.entity_id
		LIMIT $1`, maxControlQueryScanBatch)
	if err != nil {
		return nil, classifyAvailabilityError(err)
	}
	candidates := make([]controlQueryCandidate, 0, maxControlQueryScanBatch)
	for rows.Next() {
		var candidate controlQueryCandidate
		if err := rows.Scan(&candidate.entityID, &candidate.urn, &candidate.gatewayID, &candidate.sensorID, &candidate.version); err != nil {
			rows.Close()
			return nil, classifyAvailabilityError(err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, classifyAvailabilityError(err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, classifyAvailabilityError(err)
	}

	reserved := make([]ControlQueryContext, 0, limit)
	for _, candidate := range candidates {
		if len(reserved) >= limit {
			break
		}
		queryCtx, queryCancel := context.WithTimeout(ctx, r.config.DBTimeout)
		tx, err := r.db.BeginTx(queryCtx, pgx.TxOptions{})
		if err != nil {
			queryCancel()
			return nil, classifyAvailabilityError(err)
		}
		keep := false
		err = reserveOneControlQuery(queryCtx, tx, candidate, cooldown, &keep)
		if err == nil {
			err = tx.Commit(queryCtx)
		} else {
			rollbackTx(tx)
		}
		queryCancel()
		if err != nil {
			return nil, classifyAvailabilityError(err)
		}
		if keep {
			reserved = append(reserved, ControlQueryContext{
				EntityID: candidate.entityID, EntityURN: candidate.urn,
				GatewayID: candidate.gatewayID, SensorID: candidate.sensorID,
				DesiredVersion: candidate.version,
			})
		}
	}
	return reserved, nil
}

func reserveOneControlQuery(ctx context.Context, tx pgx.Tx, candidate controlQueryCandidate, cooldown time.Duration, reserve *bool) error {
	var value string
	if err := tx.QueryRow(ctx, `SELECT gateway_id FROM public.gateways WHERE gateway_id=$1 FOR SHARE`, candidate.gatewayID).Scan(&value); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	if err := tx.QueryRow(ctx, `SELECT sensor_id FROM public.sensors WHERE gateway_id=$1 AND sensor_id=$2 FOR UPDATE`, candidate.gatewayID, candidate.sensorID).Scan(&value); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	var entityID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM public.twin_entities WHERE id=$1 AND entity_id=$2 AND gateway_id=$3 AND entity_type='Sensor' FOR UPDATE`,
		candidate.entityID, candidate.urn, candidate.gatewayID).Scan(&entityID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	var version Version
	var due bool
	if err := tx.QueryRow(ctx, `SELECT desired_version,(next_query_at IS NULL OR next_query_at<=clock_timestamp())
		FROM public.twin_states WHERE entity_id=$1 FOR UPDATE`, entityID).Scan(&version, &due); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	if !due {
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE public.twin_states SET next_query_at=clock_timestamp()+$2::interval
		WHERE entity_id=$1`, entityID, cooldown.String()); err != nil {
		return err
	}
	var active bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM public.twin_commands
		WHERE entity_id=$1 AND status IN ('pending','published','acknowledged'))`, entityID).Scan(&active); err != nil {
		return err
	}
	*reserve = !active && version == candidate.version
	return nil
}

// ControlQueryContextCurrent rejects snapshots after deprovisioning or desired
// state/version changes. It does not persist or mutate the snapshot.
func (r *PostgresControlRepository) ControlQueryContextCurrent(ctx context.Context, query ControlQueryContext) (bool, error) {
	if ctx == nil || query.EntityID == uuid.Nil || query.EntityURN == "" ||
		query.GatewayID == "" || query.SensorID == "" {
		return false, ErrInvalidInput
	}
	if r == nil || isNilDependency(r.db) {
		return false, ErrDependencyUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, r.config.DBTimeout)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, classifyAvailabilityError(err)
	}
	defer rollbackTx(tx)
	var current bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM public.gateways g
		JOIN public.sensors s ON s.gateway_id=g.gateway_id
		JOIN public.twin_entities e ON e.gateway_id=s.gateway_id
			AND e.entity_id='urn:ngsi-ld:Sensor:'||s.gateway_id||':'||s.sensor_id
			AND e.entity_type='Sensor'
		JOIN public.twin_states st ON st.entity_id=e.id
		WHERE g.gateway_id=$1 AND s.sensor_id=$2 AND e.id=$3 AND e.entity_id=$4
			AND st.desired_version=$5
			AND NOT EXISTS (SELECT 1 FROM public.twin_commands c WHERE c.entity_id=e.id
				AND c.status IN ('pending','published','acknowledged'))
	)`, query.GatewayID, query.SensorID, query.EntityID, query.EntityURN, query.DesiredVersion).Scan(&current)
	if err != nil {
		return false, classifyAvailabilityError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, classifyAvailabilityError(err)
	}
	return current, nil
}

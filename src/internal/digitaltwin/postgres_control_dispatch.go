package digitaltwin

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ControlDispatch is a validated durable command, never a regenerated payload.
type ControlDispatch struct {
	CommandID uuid.UUID
	Topic     string
	Payload   []byte
	Attempt   int
	Stage6    bool
	ExpiresAt time.Time
	LeaseAt   time.Time
}

// ContextTimeout returns the bounded execution context timeout for this dispatch attempt,
// ensuring the actual publish call cannot overrun the reservation lease or command TTL.
// Returns ok=false if the authority has already expired at start or if authority metadata is missing.
func (d ControlDispatch) ContextTimeout(publishTimeout time.Duration, now time.Time) (time.Duration, bool) {
	if !d.Stage6 {
		// Legacy dispatch preserves existing behavior
		if !d.ExpiresAt.IsZero() {
			remTTL := d.ExpiresAt.Sub(now)
			if remTTL <= 0 {
				return 0, false
			}
			if remTTL < publishTimeout {
				return remTTL, true
			}
		}
		return publishTimeout, true
	}

	// Managed dispatch (Stage6 = true): MUST have positive attempt and both ExpiresAt and LeaseAt bounds!
	if d.Attempt <= 0 || d.ExpiresAt.IsZero() || d.LeaseAt.IsZero() {
		return 0, false
	}
	remTTL := d.ExpiresAt.Sub(now)
	remLease := d.LeaseAt.Sub(now)
	if remTTL <= 0 || remLease <= 0 {
		return 0, false
	}
	timeout := publishTimeout
	if remTTL < timeout {
		timeout = remTTL
	}
	if remLease < timeout {
		timeout = remLease
	}
	return timeout, true
}

// PendingControl returns a bounded discovery set, not dispatch authority.
func (r *PostgresControlRepository) PendingControl(ctx context.Context, limit int) ([]uuid.UUID, error) {
	if ctx == nil || limit < 1 || limit > 1024 {
		return nil, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, r.config.DBTimeout)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer rollbackTx(tx)

	query := `SELECT c.command_id FROM public.twin_commands c JOIN public.twin_outbox o USING(command_id)
	 WHERE c.control_version=1 AND c.status='pending' AND o.status='pending' AND o.payload_bytes IS NOT NULL
	 AND octet_length(o.payload_bytes)<=8192 AND c.expires_at>clock_timestamp() AND o.next_attempt_at<=clock_timestamp()
	 ORDER BY o.next_attempt_at,c.command_id LIMIT $1`

	if r.config.Stage6Enabled {
		query = `SELECT c.command_id FROM public.twin_commands c JOIN public.twin_outbox o USING(command_id)
	 WHERE c.control_version=1 AND o.payload_bytes IS NOT NULL AND octet_length(o.payload_bytes)<=8192
	 AND c.expires_at>clock_timestamp() AND o.next_attempt_at<=clock_timestamp()
	 AND (
	   (c.stage6_managed=false AND c.status='pending' AND o.status='pending')
	   OR
	   (c.stage6_managed=true AND c.status IN ('pending','published') AND o.status IN ('pending','published')
	    AND c.status <> 'acknowledged' AND c.retry_stop_reason IS NULL)
	 )
	 ORDER BY o.next_attempt_at,c.command_id LIMIT $1`
	}

	rows, err := tx.Query(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// PrepareControl rechecks status/expiry under the shared resource lock order.
func (r *PostgresControlRepository) PrepareControl(ctx context.Context, id uuid.UUID) (*ControlDispatch, error) {
	var dispatch *ControlDispatch
	err := r.withControlLock(ctx, id, func(ctx context.Context, tx pgx.Tx, entity uuid.UUID, urn string, now time.Time) error {
		if err := ExpireSensorCommandsTx(ctx, tx, entity, now); err != nil {
			return err
		}
		var c Command
		var stage6Managed bool
		var status, outboxStatus string
		var cmdAttempts, outboxAttempts int
		var firstReservedAt *time.Time
		var retryStopReason *string
		var nextAttemptAt time.Time
		var topic string
		var payload []byte
		err := tx.QueryRow(ctx, `SELECT c.command_id::text, c.desired_version, c.issued_at, c.expires_at,
		 c.stage6_managed, c.status, c.attempt_count, c.first_reserved_at, c.retry_stop_reason,
		 o.topic, o.payload_bytes, o.status, o.attempt_count, o.next_attempt_at
		 FROM public.twin_commands c JOIN public.twin_outbox o USING(command_id)
		 WHERE c.command_id=$1 AND c.control_version=1 FOR UPDATE OF c,o`, id).Scan(
			&c.CommandID, &c.DesiredVersion, &c.IssuedAt, &c.ExpiresAt,
			&stage6Managed, &status, &cmdAttempts, &firstReservedAt, &retryStopReason,
			&topic, &payload, &outboxStatus, &outboxAttempts, &nextAttemptAt,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if !now.Before(c.ExpiresAt) || now.Before(nextAttemptAt) {
			return nil
		}
		var currentDesiredVersion int64
		if err = tx.QueryRow(ctx, `SELECT desired_version FROM public.twin_states WHERE entity_id=$1`, entity).Scan(&currentDesiredVersion); err != nil {
			return err
		}
		if currentDesiredVersion != int64(c.DesiredVersion) {
			return nil
		}
		parsed, err := ParseMQTTCommand(topic, payload, false)
		if err != nil || parsed.EntityID != urn || parsed.CommandID != c.CommandID || parsed.DesiredVersion != c.DesiredVersion || !parsed.IssuedAt.Equal(c.IssuedAt) || !parsed.ExpiresAt.Equal(c.ExpiresAt) {
			return ErrInvalidInput
		}
		var interval int64
		if err = tx.QueryRow(ctx, `SELECT (parameters->>'sample_interval_us')::bigint FROM public.twin_commands WHERE command_id=$1 AND action='update_configuration'`, id).Scan(&interval); err != nil {
			return err
		}
		if interval != parsed.Parameters.SampleIntervalUS {
			return ErrInvalidInput
		}

		if !stage6Managed {
			// Stage 5 legacy path:
			if status != "pending" || outboxStatus != "pending" {
				return nil
			}
			dispatch = &ControlDispatch{CommandID: id, Topic: topic, Payload: payload, Attempt: cmdAttempts, ExpiresAt: c.ExpiresAt}
			return nil
		}

		// Stage 6 managed path:
		if !r.config.Stage6Enabled {
			// A managed row must not be silently dispatched by unfenced legacy path.
			return nil
		}
		if (status != "pending" && status != "published") || status == string(CommandAcknowledged) ||
			(outboxStatus != "pending" && outboxStatus != "published") || retryStopReason != nil {
			return nil
		}

		if outboxAttempts >= r.config.RetryPolicy.MaxAttempts || cmdAttempts >= r.config.RetryPolicy.MaxAttempts {
			reason := "retry_exhausted"
			if _, err = tx.Exec(ctx, `UPDATE public.twin_commands SET retry_stop_reason=$2 WHERE command_id=$1`, id, reason); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, `UPDATE public.twin_outbox SET next_attempt_at=$2 WHERE command_id=$1`, id, c.ExpiresAt); err != nil {
				return err
			}
			return nil
		}

		// A grant is valid only when the complete finite lease (publish timeout
		// plus safety margin) fits strictly before the original command expiry.
		// When no valid grant fits, no attempt is consumed and no attempt cap is
		// claimed: the command keeps its active slot until the existing expiry.
		leaseDuration := r.config.RetryPolicy.PublishTimeout + r.config.RetryPolicy.SafetyMargin
		if !now.Add(leaseDuration).Before(c.ExpiresAt) {
			if _, err = tx.Exec(ctx, `UPDATE public.twin_outbox SET next_attempt_at=$2 WHERE command_id=$1`, id, c.ExpiresAt); err != nil {
				return err
			}
			return nil
		}

		newAttempt := outboxAttempts + 1
		nextAt, _, stopReason := r.config.RetryPolicy.NextAttemptScheduleWithReason(now, newAttempt, r.jitterFactor(), c.ExpiresAt)

		var newStopReason *string
		if stopReason != "" {
			newStopReason = &stopReason
		}

		if firstReservedAt == nil {
			if _, err = tx.Exec(ctx, `UPDATE public.twin_commands SET attempt_count=$2, first_reserved_at=$3, retry_stop_reason=$4 WHERE command_id=$1`, id, newAttempt, now, newStopReason); err != nil {
				return err
			}
		} else {
			if _, err = tx.Exec(ctx, `UPDATE public.twin_commands SET attempt_count=$2, retry_stop_reason=$3 WHERE command_id=$1`, id, newAttempt, newStopReason); err != nil {
				return err
			}
		}

		if _, err = tx.Exec(ctx, `UPDATE public.twin_outbox SET attempt_count=$2, next_attempt_at=$3 WHERE command_id=$1`, id, newAttempt, nextAt); err != nil {
			return err
		}

		dispatch = &ControlDispatch{
			CommandID: id,
			Topic:     topic,
			Payload:   payload,
			Attempt:   newAttempt,
			Stage6:    true,
			ExpiresAt: c.ExpiresAt,
			LeaseAt:   now.Add(leaseDuration),
		}
		return nil
	})
	if err != nil {
		dispatch = nil
		return nil, err
	}
	return dispatch, nil
}

// MarkControlPublished conditionally records broker delivery, never execution.
func (r *PostgresControlRepository) MarkControlPublished(ctx context.Context, id uuid.UUID) error {
	return r.withControlLock(ctx, id, func(ctx context.Context, tx pgx.Tx, entity uuid.UUID, _ string, now time.Time) error {
		if err := ExpireSensorCommandsTx(ctx, tx, entity, now); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE public.twin_commands SET status='published' WHERE command_id=$1 AND control_version=1 AND status='pending' AND stage6_managed=false AND expires_at>$2`, id, now)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE public.twin_outbox SET status='published' WHERE command_id=$1 AND status='pending'`, id)
		return err
	})
}

// MarkControlReservationPublished records broker delivery for a specific Stage 6 attempt reservation.
func (r *PostgresControlRepository) MarkControlReservationPublished(ctx context.Context, id uuid.UUID, attempt int) error {
	if attempt <= 0 {
		return ErrInvalidInput
	}
	return r.withControlLock(ctx, id, func(ctx context.Context, tx pgx.Tx, entity uuid.UUID, _ string, now time.Time) error {
		if err := ExpireSensorCommandsTx(ctx, tx, entity, now); err != nil {
			return err
		}
		var status string
		var attemptCount int
		var managed bool
		err := tx.QueryRow(ctx, `SELECT status, attempt_count, stage6_managed FROM public.twin_commands WHERE command_id=$1 AND control_version=1`, id).
			Scan(&status, &attemptCount, &managed)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if !managed {
			return nil
		}
		if status == string(CommandAcknowledged) || status == string(CommandSucceeded) ||
			status == string(CommandFailed) || status == string(CommandTimeout) || status == "cancelled" {
			return nil
		}
		if attemptCount != attempt {
			return nil
		}
		if status == string(CommandPending) {
			if _, err = tx.Exec(ctx, `UPDATE public.twin_commands SET status='published' WHERE command_id=$1 AND status='pending' AND attempt_count=$2 AND expires_at>$3`, id, attempt, now); err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `UPDATE public.twin_outbox SET status='published' WHERE command_id=$1 AND status='pending' AND attempt_count=$2`, id, attempt)
		return err
	})
}

// ExpireControl scans at most limit expired Sensor commands using admission expiry.
func (r *PostgresControlRepository) ExpireControl(ctx context.Context, limit int) error {
	if ctx == nil || limit < 1 || limit > 1024 {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, r.config.DBTimeout)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer rollbackTx(tx)
	rows, err := tx.Query(ctx, `SELECT command_id FROM public.twin_commands WHERE target_entity_type='Sensor' AND status IN('pending','published','acknowledged') AND expires_at<=clock_timestamp() ORDER BY expires_at,command_id LIMIT $1`, limit)
	if err != nil {
		return err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	rollbackTx(tx) // Release discovery transaction/connection before acquiring resource locks.
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err = r.withControlLock(ctx, id, func(ctx context.Context, tx pgx.Tx, entity uuid.UUID, _ string, now time.Time) error {
			return ExpireSensorCommandsTx(ctx, tx, entity, now)
		}); err != nil {
			return err
		}
	}
	return nil
}

// Discovery conveys no authority: every mutation reacquires Gateway SHARE ->
// Sensor UPDATE -> Entity UPDATE -> state UPDATE -> command/outbox locks.
// No actor membership applies to already accepted work.
func (r *PostgresControlRepository) withControlLock(ctx context.Context, id uuid.UUID, work func(context.Context, pgx.Tx, uuid.UUID, string, time.Time) error) error {
	if ctx == nil || id == uuid.Nil {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, r.config.DBTimeout)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer rollbackTx(tx)
	var urn string
	err = tx.QueryRow(ctx, `SELECT e.entity_id FROM public.twin_entities e JOIN public.twin_commands c ON c.entity_id=e.id WHERE c.command_id=$1 AND e.entity_type='Sensor'`, id).Scan(&urn)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	p, err := ParseEntityURN(urn)
	if err != nil {
		return err
	}
	var locked string
	if err = tx.QueryRow(ctx, `SELECT gateway_id FROM public.gateways WHERE gateway_id=$1 FOR SHARE`, p.GatewayID).Scan(&locked); err != nil {
		return err
	}
	if err = tx.QueryRow(ctx, `SELECT sensor_id FROM public.sensors WHERE gateway_id=$1 AND sensor_id=$2 FOR UPDATE`, p.GatewayID, p.SensorID).Scan(&locked); err != nil {
		return err
	}
	var entity uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT id FROM public.twin_entities WHERE entity_id=$1 AND entity_type='Sensor' AND gateway_id=$2 FOR UPDATE`, urn, p.GatewayID).Scan(&entity); err != nil {
		return err
	}
	if err = tx.QueryRow(ctx, `SELECT entity_id FROM public.twin_states WHERE entity_id=$1 FOR UPDATE`, entity).Scan(&entity); err != nil {
		return err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return err
	}
	if err = work(ctx, tx, entity, urn, now.UTC()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

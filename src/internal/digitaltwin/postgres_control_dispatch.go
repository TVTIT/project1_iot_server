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
	rows, err := tx.Query(ctx, `SELECT c.command_id FROM public.twin_commands c JOIN public.twin_outbox o USING(command_id)
	 WHERE c.control_version=1 AND c.status='pending' AND o.status='pending' AND o.payload_bytes IS NOT NULL
	 AND octet_length(o.payload_bytes)<=8192 AND c.expires_at>clock_timestamp() AND o.next_attempt_at<=clock_timestamp()
	 ORDER BY o.next_attempt_at,c.command_id LIMIT $1`, limit)
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
		var topic string
		var payload []byte
		err := tx.QueryRow(ctx, `SELECT c.command_id::text,c.desired_version,c.issued_at,c.expires_at,o.topic,o.payload_bytes
		 FROM public.twin_commands c JOIN public.twin_outbox o USING(command_id)
		 WHERE c.command_id=$1 AND c.control_version=1 AND c.status='pending' AND o.status='pending'
		 AND c.expires_at>$2 AND o.next_attempt_at<=$2 FOR UPDATE OF c,o`, id, now).Scan(&c.CommandID, &c.DesiredVersion, &c.IssuedAt, &c.ExpiresAt, &topic, &payload)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
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
		dispatch = &ControlDispatch{id, topic, payload}
		return nil
	})
	if err != nil {
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
		tag, err := tx.Exec(ctx, `UPDATE public.twin_commands SET status='published' WHERE command_id=$1 AND control_version=1 AND status='pending' AND expires_at>$2`, id, now)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE public.twin_outbox SET status='published' WHERE command_id=$1 AND status='pending'`, id)
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

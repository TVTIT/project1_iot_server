package digitaltwin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ControlReceiptOutcome classifies durable receipt acceptance.
type ControlReceiptOutcome string

const (
	// ControlReceiptAccepted means a new durable receipt was committed.
	ControlReceiptAccepted ControlReceiptOutcome = "accepted"
	// ControlReceiptDuplicate means the exact canonical receipt was already stored.
	ControlReceiptDuplicate ControlReceiptOutcome = "duplicate"
	// ControlReceiptLate means expiry won before the receipt transaction.
	ControlReceiptLate ControlReceiptOutcome = "late"
)

// AcceptControlReceipt accepts one canonical Gateway durable-store receipt.
// Receipt evidence stops retries but never changes desired/reported state.
func (r *PostgresControlRepository) AcceptControlReceipt(ctx context.Context, topic string, payload []byte, receivedAt time.Time) (ControlReceiptOutcome, error) {
	if ctx == nil || receivedAt.IsZero() {
		return "", ErrInvalidInput
	}
	receipt, err := ParseMQTTReceipt(topic, payload, false)
	if err != nil {
		return "", err
	}
	if r == nil || isNilDependency(r.db) {
		return "", ErrDependencyUnavailable
	}
	if !r.config.Stage6Enabled {
		return "", ErrCommandReceiptUnavailable
	}
	id, err := uuid.Parse(receipt.CommandID)
	if err != nil {
		return "", ErrInvalidInput
	}
	found := false
	var outcome ControlReceiptOutcome
	err = r.withControlLock(ctx, id, func(ctx context.Context, tx pgx.Tx, entity uuid.UUID, urn string, now time.Time) error {
		found = true
		var status CommandStatus
		var version Version
		var issued, expires time.Time
		var stage6Managed bool
		var commandTopic string
		var commandBytes, storedReceipt []byte
		var storedReceiptHash *string
		if err := tx.QueryRow(ctx, `SELECT c.status,c.desired_version,c.issued_at,c.expires_at,c.stage6_managed,
			o.topic,o.payload_bytes,c.receipt_bytes,c.receipt_hash
			FROM public.twin_commands c JOIN public.twin_outbox o USING(command_id)
			WHERE c.command_id=$1 AND c.control_version=1 AND c.action='update_configuration'
			FOR UPDATE OF c,o`, id).Scan(&status, &version, &issued, &expires, &stage6Managed,
			&commandTopic, &commandBytes, &storedReceipt, &storedReceiptHash); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		identity, err := ParseEntityURN(urn)
		if err != nil || !stage6Managed || receipt.GatewayID != identity.GatewayID || receipt.EntityID != urn || receipt.DesiredVersion != version {
			return ErrInvalidInput
		}
		command, err := ParseMQTTCommand(commandTopic, commandBytes, false)
		if err != nil || command.CommandID != receipt.CommandID || command.GatewayID != receipt.GatewayID ||
			command.EntityID != urn || command.DesiredVersion != version || !command.IssuedAt.Equal(issued) || !command.ExpiresAt.Equal(expires) {
			return ErrInvalidInput
		}
		hash := fmt.Sprintf("%x", sha256.Sum256(commandBytes))
		if receipt.CommandHash != hash {
			return ErrInvalidInput
		}
		if len(storedReceipt) != 0 {
			if bytes.Equal(storedReceipt, payload) && storedReceiptHash != nil && *storedReceiptHash == fmt.Sprintf("%x", sha256.Sum256(payload)) {
				outcome = ControlReceiptDuplicate
				return nil
			}
			return ErrControlResultConflict
		}
		if status.Terminal() || status == CommandAcknowledged {
			return ErrControlResultConflict
		}
		if !status.Active() {
			return ErrControlResultConflict
		}
		if !now.Before(expires) {
			if err := ExpireSensorCommandsTx(ctx, tx, entity, now); err != nil {
				return err
			}
			outcome = ControlReceiptLate
			return nil
		}
		hashReceipt := fmt.Sprintf("%x", sha256.Sum256(payload))
		tag, err := tx.Exec(ctx, `UPDATE public.twin_commands SET status='acknowledged',acknowledged_at=$2,
			receipt_bytes=$3,receipt_hash=$4,receipt_received_at=$5,retry_stop_reason='acknowledged'
			WHERE command_id=$1 AND stage6_managed=true AND status IN('pending','published')
			AND expires_at>$2 AND receipt_bytes IS NULL`, id, now, payload, hashReceipt, receivedAt.UTC())
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrControlResultConflict
		}
		if _, err = tx.Exec(ctx, `UPDATE public.twin_outbox SET status='failed'
			WHERE command_id=$1 AND status IN('pending','published')`, id); err != nil {
			return err
		}
		outcome = ControlReceiptAccepted
		return nil
	})
	if err != nil {
		return "", err
	}
	if !found {
		return "", ErrNotFound
	}
	return outcome, nil
}

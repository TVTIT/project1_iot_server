package digitaltwin

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// CommandCancellation is the stable outcome of a safe cancellation.
//
// Every field is derived from persisted cancellation evidence, never
// regenerated from current state, so repeating the request returns exactly the
// same result. It is domain data: the HTTP body and routing are transport
// concerns and stay outside this package.
type CommandCancellation struct {
	EntityID       string        `json:"entity_id"`
	CommandID      string        `json:"command_id"`
	Status         CommandStatus `json:"status"`
	DesiredVersion Version       `json:"desired_version"`
	CancelledAt    time.Time     `json:"cancelled_at"`
	StatusURL      string        `json:"status_url"`
}

// CancelControl cancels one command whose safe boundary is provable in the
// database: a Stage 6 managed command that is still pending, unexpired and
// has never been reserved for dispatch.
//
// The transaction follows the shared resource order (Gateway SHARE ->
// membership SHARE -> Sensor UPDATE -> Entity UPDATE -> state UPDATE ->
// command/outbox), performs no network I/O, never reverts desired or reported
// state and never resurrects a terminal command. A repeated request against an
// already cancelled command returns the same persisted outcome after the
// current authorization check.
func (r *PostgresControlRepository) CancelControl(ctx context.Context, actor uuid.UUID, urn, commandID string) (CommandCancellation, error) {
	if ctx == nil {
		return CommandCancellation{}, ErrInvalidInput
	}
	if actor == uuid.Nil {
		return CommandCancellation{}, ErrNilActor
	}
	identity, err := ParseEntityURN(urn)
	if err != nil {
		return CommandCancellation{}, err
	}
	id, err := uuid.Parse(commandID)
	if err != nil || id.String() != commandID {
		return CommandCancellation{}, ErrInvalidInput
	}
	if r == nil || isNilDependency(r.db) {
		return CommandCancellation{}, ErrDependencyUnavailable
	}
	if !r.config.Stage6Enabled {
		// The safe cancellation surface exists only with Stage 6 enabled. A
		// Stage 5 repository fails closed instead of cancelling silently.
		return CommandCancellation{}, ErrCommandNotCancellable
	}
	ctx, cancel := context.WithTimeout(ctx, r.config.DBTimeout)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return CommandCancellation{}, classifyAvailabilityError(err)
	}
	defer rollbackTx(tx)
	if _, err = tx.Exec(ctx, `SELECT set_config('lock_timeout',$1,true)`, fmt.Sprintf("%dms", r.config.DBTimeout.Milliseconds())); err != nil {
		return CommandCancellation{}, classifyAvailabilityError(err)
	}
	// Gateway SHARE -> membership SHARE -> Sensor UPDATE -> Entity UPDATE.
	// A platform admin never bypasses membership, and a command of another
	// Gateway is not observable through this path.
	entity, err := lockControlTarget(ctx, tx, actor, identity, true)
	if err != nil {
		return CommandCancellation{}, err
	}
	// state UPDATE
	var stateVersion int64
	if err = tx.QueryRow(ctx, `SELECT desired_version FROM public.twin_states WHERE entity_id=$1 FOR UPDATE`, entity).Scan(&stateVersion); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return CommandCancellation{}, ErrInconsistentGraph
		}
		return CommandCancellation{}, classifyAvailabilityError(err)
	}
	// command lock
	var (
		status          string
		commandVersion  int64
		stage6Managed   bool
		firstReservedAt *time.Time
		retryStopReason *string
		cancelledAt     *time.Time
		cancelledBy     *uuid.UUID
		commandAttempts int
		rootCommandID   *uuid.UUID
		expiresAt       time.Time
	)
	err = tx.QueryRow(ctx, `SELECT c.status,c.desired_version,c.stage6_managed,c.first_reserved_at,
		c.retry_stop_reason,c.cancelled_at,c.cancelled_by,c.attempt_count,c.root_command_id,c.expires_at
		FROM public.twin_commands c WHERE c.command_id=$1 AND c.entity_id=$2 FOR UPDATE`, id, entity).Scan(
		&status, &commandVersion, &stage6Managed, &firstReservedAt,
		&retryStopReason, &cancelledAt, &cancelledBy, &commandAttempts, &rootCommandID, &expiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		// The command does not belong to the authorized Sensor. Existence of a
		// command in another scope is never disclosed.
		return CommandCancellation{}, ErrNotFound
	}
	if err != nil {
		return CommandCancellation{}, classifyAvailabilityError(err)
	}
	// outbox lock
	var outboxStatus *string
	var outboxAttempts *int
	err = tx.QueryRow(ctx, `SELECT o.status,o.attempt_count FROM public.twin_outbox o WHERE o.command_id=$1 FOR UPDATE`, id).Scan(&outboxStatus, &outboxAttempts)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return CommandCancellation{}, classifyAvailabilityError(err)
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return CommandCancellation{}, classifyAvailabilityError(err)
	}
	now = now.UTC()

	if status == string(CommandCancelled) {
		// A repeated request returns the original outcome, after the current
		// authorization check and even after the original deadline passed. The
		// expiry path never turns a cancelled command into a timeout.
		if !stage6Managed || cancelledAt == nil || cancelledBy == nil {
			// Legacy rows carry no immutable cancellation evidence.
			return CommandCancellation{}, ErrCommandNotCancellable
		}
		outcome := cancellationOutcome(urn, id, commandVersion, *cancelledAt)
		if err = tx.Commit(ctx); err != nil {
			return CommandCancellation{}, controlCommitError(err)
		}
		return outcome, nil
	}
	if status != string(CommandPending) {
		// published, acknowledged, succeeded, failed and timeout are not
		// cancellable; only a pending command can be cancelled safely.
		return CommandCancellation{}, ErrCommandNotCancellable
	}
	if commandVersion > stateVersion {
		// The command claims a desired version the server never committed.
		return CommandCancellation{}, ErrCommandNotCancellable
	}
	if !stage6Managed {
		// Legacy rows have UNKNOWN dispatch history: attempt_count = 0 never
		// proves that the command was never sent.
		return CommandCancellation{}, ErrCommandNotCancellable
	}
	if firstReservedAt != nil || commandAttempts != 0 {
		// The first reservation already happened. The device may have received
		// the command, so the outcome of the dispatch is unknown.
		return CommandCancellation{}, ErrCommandNotCancellable
	}
	if retryStopReason != nil {
		return CommandCancellation{}, ErrCommandNotCancellable
	}
	if outboxStatus == nil || *outboxStatus != "pending" {
		// A missing or already stopped outbox row is unknown dispatch state.
		return CommandCancellation{}, ErrCommandNotCancellable
	}
	if outboxAttempts == nil || *outboxAttempts != 0 || *outboxAttempts != commandAttempts {
		// The mirrored counters must agree and both be zero. Any inconsistency
		// fails closed instead of guessing that the command was never sent.
		return CommandCancellation{}, ErrCommandNotCancellable
	}
	if !now.Before(expiresAt) {
		// The deadline passed: the existing expiry path owns the transition.
		return CommandCancellation{}, ErrCommandNotCancellable
	}
	tag, err := tx.Exec(ctx, `UPDATE public.twin_commands SET status='cancelled',completed_at=$2,cancelled_at=$2,cancelled_by=$3,
		retry_stop_reason='cancelled',reconcile_blocked=true
		WHERE command_id=$1 AND status='pending' AND stage6_managed=true AND first_reserved_at IS NULL AND attempt_count=0`, id, now, actor)
	if err != nil {
		return CommandCancellation{}, classifyAvailabilityError(err)
	}
	if tag.RowsAffected() != 1 {
		return CommandCancellation{}, ErrCommandNotCancellable
	}
	tag, err = tx.Exec(ctx, `UPDATE public.twin_outbox SET status='failed' WHERE command_id=$1 AND status='pending'`, id)
	if err != nil {
		return CommandCancellation{}, classifyAvailabilityError(err)
	}
	if tag.RowsAffected() != 1 {
		// The stopped outbox is part of the guarantee. Without it the whole
		// cancellation is rolled back.
		return CommandCancellation{}, ErrCommandNotCancellable
	}
	if rootCommandID != nil && *rootCommandID != id {
		// A cancelled successor blocks the same user root intent, so an
		// automatic recovery cannot republish the cancelled intent.
		if _, err = tx.Exec(ctx, `UPDATE public.twin_commands SET reconcile_blocked=true WHERE command_id=$1 AND reconcile_blocked=false`, *rootCommandID); err != nil {
			return CommandCancellation{}, classifyAvailabilityError(err)
		}
	}
	outcome := cancellationOutcome(urn, id, commandVersion, now)
	if err = tx.Commit(ctx); err != nil {
		// An ambiguous commit never reports a successful cancellation.
		return CommandCancellation{}, controlCommitError(err)
	}
	return outcome, nil
}

func cancellationOutcome(urn string, id uuid.UUID, desiredVersion int64, cancelledAt time.Time) CommandCancellation {
	return CommandCancellation{
		EntityID:       urn,
		CommandID:      id.String(),
		Status:         CommandCancelled,
		DesiredVersion: Version(desiredVersion),
		CancelledAt:    cancelledAt.UTC().Truncate(time.Microsecond),
		StatusURL:      "/v1/digital-twins/" + url.PathEscape(urn) + "/commands/" + id.String(),
	}
}

// currentIntentBlockedTx reports whether the current desired lineage is
// cancelled, spent, or unverifiable. It matches the current desired version
// only, never a historical block belonging to an older root.
//
// The caller must already hold the Sensor, Entity and state locks.
func currentIntentBlockedTx(ctx context.Context, tx pgx.Tx, entity uuid.UUID, desiredVersion int64) (bool, error) {
	var blocked bool
	err := tx.QueryRow(ctx, `SELECT CASE
		WHEN c.stage6_managed AND c.target_entity_type='Sensor' AND c.action='update_configuration'
			AND c.origin='client' AND c.root_command_id=c.command_id
			THEN c.reconcile_blocked OR c.auto_successors_used>=1
		WHEN c.stage6_managed AND c.target_entity_type='Sensor' AND c.action='update_configuration'
			AND c.origin='reconciliation' AND c.parent_command_id IS NOT NULL
			AND r.command_id IS NOT NULL AND c.root_command_id=r.command_id
			AND c.parent_command_id=r.command_id AND c.desired_version=r.desired_version+1
			AND c.original_actor_id=r.original_actor_id
			AND r.stage6_managed AND r.target_entity_type='Sensor' AND r.action='update_configuration'
			AND r.origin='client' AND r.root_command_id=r.command_id
			THEN c.reconcile_blocked OR r.reconcile_blocked OR r.auto_successors_used>=1
		ELSE true
		END
		FROM public.twin_commands c
		LEFT JOIN public.twin_commands r ON r.command_id=c.root_command_id AND r.entity_id=c.entity_id
		WHERE c.entity_id=$1 AND c.desired_version=$2 AND c.control_version=1
		ORDER BY c.issued_at DESC,c.command_id DESC LIMIT 1`, entity, desiredVersion).Scan(&blocked)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return blocked, nil
}

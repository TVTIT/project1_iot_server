package digitaltwin_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"iot-platform/internal/digitaltwin"
)

// stage6Repository builds the Stage 6 managed repository used by the
// cancellation contract. A Stage 5 repository never creates stage6_managed
// rows, so every safe cancellation must use this one.
func (f *controlFixture) stage6Repository(t *testing.T) *digitaltwin.PostgresControlRepository {
	t.Helper()
	config := digitaltwin.DefaultControlConfig()
	config.Stage6Enabled = true
	repo, err := digitaltwin.NewPostgresControlRepository(f.app, config)
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

// admitStage6 performs a real admission and returns the committed command id.
func admitStage6(t *testing.T, repo *digitaltwin.PostgresControlRepository, f *controlFixture, actor uuid.UUID, urn, hz, version, key string) uuid.UUID {
	t.Helper()
	receipt, err := repo.Admit(f.ctx, actor, urn, controlRequest(t, hz, version, key))
	if err != nil || receipt.StatusCode != 202 || receipt.CommandID == nil {
		t.Fatalf("stage6 admission: %+v %v", receipt, err)
	}
	return uuid.MustParse(*receipt.CommandID)
}

// acceptRealResult commits a device result through the existing result
// transaction. It never seeds reported state or a terminal status by SQL.
func acceptRealResult(t *testing.T, repo *digitaltwin.PostgresControlRepository, f *controlFixture, urn string, id uuid.UUID, status digitaltwin.CommandStatus) {
	t.Helper()
	var issued, expires time.Time
	var interval, version int64
	if err := f.admin.QueryRow(f.ctx, `SELECT c.issued_at,c.expires_at,(c.parameters->>'sample_interval_us')::bigint,c.desired_version
		FROM twin_commands c WHERE c.command_id=$1`, id).Scan(&issued, &expires, &interval, &version); err != nil {
		t.Fatal(err)
	}
	result := digitaltwin.CommandResult{
		ProtocolVersion: 1,
		MessageType:     "command_result",
		CommandID:       id.String(),
		GatewayID:       f.gw,
		EntityID:        urn,
		DesiredVersion:  digitaltwin.Version(version),
		Status:          status,
		CompletedAt:     issued.UTC(),
	}
	if status == digitaltwin.CommandSucceeded {
		result.ReportedState = &digitaltwin.SamplingParameters{SampleIntervalUS: interval}
	} else {
		result.ErrorCode = "APPLY_FAILED"
		result.ErrorMessage = digitaltwin.SafeCommandErrorMessage("APPLY_FAILED")
	}
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	topic := "gateways/" + f.gw + "/responses/" + id.String()
	outcome, err := repo.AcceptControlResult(f.ctx, topic, payload, time.Now().UTC())
	if err != nil || outcome != digitaltwin.ControlResultAccepted {
		t.Fatalf("result admission: %+v %v", outcome, err)
	}
}

// parkDueCommands moves the discovery window of the commands created by a
// cancellation test into the future. The rows keep their real status and
// evidence in the shared fixture database, but the periodic discovery of
// later tests keeps its own bounded expectations.
func parkDueCommands(t *testing.T, f *controlFixture, gateways ...string) {
	t.Helper()
	for _, gateway := range gateways {
		f.exec(t, `UPDATE twin_outbox SET next_attempt_at=clock_timestamp()+interval '1 hour'
			WHERE command_id IN (SELECT c.command_id FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id WHERE e.gateway_id=$1)`, gateway)
	}
}

func TestNativeControlCancelPermissions(t *testing.T) {
	f := newControlFixture(t)
	repo := f.stage6Repository(t)
	other := newControlFixture(t)
	defer parkDueCommands(t, f, f.gw, other.gw)

	ownerURN, _ := f.sensor(t, "cancel_owner")
	ownerID := admitStage6(t, repo, f, f.owner, ownerURN, "1", "0", "cancel-owner")
	if _, err := repo.CancelControl(f.ctx, f.owner, ownerURN, ownerID.String()); err != nil {
		t.Fatalf("owner cancel: %v", err)
	}

	operatorURN, _ := f.sensor(t, "cancel_operator")
	operatorID := admitStage6(t, repo, f, f.owner, operatorURN, "1", "0", "cancel-operator")
	if _, err := repo.CancelControl(f.ctx, f.operator, operatorURN, operatorID.String()); err != nil {
		t.Fatalf("operator cancel: %v", err)
	}

	viewerURN, _ := f.sensor(t, "cancel_viewer")
	viewerID := admitStage6(t, repo, f, f.owner, viewerURN, "1", "0", "cancel-viewer")
	if _, err := repo.CancelControl(f.ctx, f.viewer, viewerURN, viewerID.String()); !errors.Is(err, digitaltwin.ErrForbidden) {
		t.Fatalf("viewer cancel: %v", err)
	}

	nonmemberURN, _ := f.sensor(t, "cancel_nonmember")
	nonmemberID := admitStage6(t, repo, f, f.owner, nonmemberURN, "1", "0", "cancel-nonmember")
	if _, err := repo.CancelControl(f.ctx, uuid.New(), nonmemberURN, nonmemberID.String()); !errors.Is(err, digitaltwin.ErrNotFound) {
		t.Fatalf("nonmember cancel: %v", err)
	}

	adminURN, _ := f.sensor(t, "cancel_admin")
	adminID := admitStage6(t, repo, f, f.owner, adminURN, "1", "0", "cancel-admin")
	if _, err := repo.CancelControl(f.ctx, f.outsider, adminURN, adminID.String()); !errors.Is(err, digitaltwin.ErrNotFound) {
		t.Fatalf("platform admin without membership: %v", err)
	}

	revokedURN, _ := f.sensor(t, "cancel_revoked")
	revokedID := admitStage6(t, repo, f, f.owner, revokedURN, "1", "0", "cancel-revoked")
	f.exec(t, `DELETE FROM user_gateways WHERE user_id=$1`, f.owner)
	if _, err := repo.CancelControl(f.ctx, f.owner, revokedURN, revokedID.String()); !errors.Is(err, digitaltwin.ErrNotFound) {
		t.Fatalf("revoked membership: %v", err)
	}

	// A command of another Sensor or another Gateway is never observable.
	otherSensorURN, _ := f.sensor(t, "cancel_other_sensor")
	if _, err := repo.CancelControl(f.ctx, f.operator, otherSensorURN, adminID.String()); !errors.Is(err, digitaltwin.ErrNotFound) {
		t.Fatalf("wrong sensor: %v", err)
	}
	otherRepo := other.stage6Repository(t)
	otherID := admitStage6(t, otherRepo, other, other.owner, other.urn, "1", "0", "cancel-other-gateway")
	if _, err := repo.CancelControl(f.ctx, f.operator, f.urn, otherID.String()); !errors.Is(err, digitaltwin.ErrNotFound) {
		t.Fatalf("wrong gateway: %v", err)
	}

	if _, err := repo.CancelControl(f.ctx, f.operator, "not-a-urn", adminID.String()); !errors.Is(err, digitaltwin.ErrInvalidInput) {
		t.Fatalf("invalid target: %v", err)
	}
	if _, err := repo.CancelControl(f.ctx, f.operator, f.urn, "not-a-command-id"); !errors.Is(err, digitaltwin.ErrInvalidInput) {
		t.Fatalf("invalid command id: %v", err)
	}
	if _, err := repo.CancelControl(f.ctx, uuid.Nil, f.urn, adminID.String()); !errors.Is(err, digitaltwin.ErrNilActor) {
		t.Fatalf("nil actor: %v", err)
	}
	var nilContext context.Context // Deliberately exercise defensive API validation.
	if _, err := repo.CancelControl(nilContext, f.operator, f.urn, adminID.String()); !errors.Is(err, digitaltwin.ErrInvalidInput) {
		t.Fatalf("nil context: %v", err)
	}
	if _, err := repo.CancelControl(f.ctx, f.operator, f.urn, uuid.NewString()); !errors.Is(err, digitaltwin.ErrNotFound) {
		t.Fatalf("unknown command: %v", err)
	}
}

func TestNativeControlCancelSafeBoundaryAndStableRepeat(t *testing.T) {
	f := newControlFixture(t)
	repo := f.stage6Repository(t)
	defer parkDueCommands(t, f, f.gw)

	urn, entity := f.sensor(t, "cancel_boundary")
	id := admitStage6(t, repo, f, f.owner, urn, "1", "0", "cancel-boundary")
	var before string
	if err := f.admin.QueryRow(f.ctx, `SELECT to_jsonb(s)::text FROM twin_states s WHERE entity_id=$1`, entity).Scan(&before); err != nil {
		t.Fatal(err)
	}

	outcome, err := repo.CancelControl(f.ctx, f.owner, urn, id.String())
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if outcome.Status != digitaltwin.CommandCancelled || outcome.CommandID != id.String() || outcome.EntityID != urn || outcome.DesiredVersion != 1 {
		t.Fatalf("outcome: %+v", outcome)
	}
	if outcome.CancelledAt.IsZero() || !strings.HasSuffix(outcome.StatusURL, "/commands/"+id.String()) {
		t.Fatalf("outcome fields: %+v", outcome)
	}

	var (
		dbStatus       string
		dbCancelledAt  *time.Time
		dbCancelledBy  *uuid.UUID
		dbCompletedAt  *time.Time
		dbStopReason   *string
		dbBlocked      bool
		dbAttempts     int
		dbFirstReserve *time.Time
		outboxStatus   string
	)
	if err = f.admin.QueryRow(f.ctx, `SELECT c.status,c.cancelled_at,c.cancelled_by,c.completed_at,c.retry_stop_reason,
		c.reconcile_blocked,c.attempt_count,c.first_reserved_at,o.status
		FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE c.command_id=$1`, id).Scan(
		&dbStatus, &dbCancelledAt, &dbCancelledBy, &dbCompletedAt, &dbStopReason,
		&dbBlocked, &dbAttempts, &dbFirstReserve, &outboxStatus); err != nil {
		t.Fatal(err)
	}
	if dbStatus != "cancelled" || dbCancelledAt == nil || dbCancelledBy == nil || *dbCancelledBy != f.owner ||
		dbCompletedAt == nil || dbStopReason == nil || *dbStopReason != "cancelled" || !dbBlocked ||
		dbAttempts != 0 || dbFirstReserve != nil || outboxStatus != "failed" {
		t.Fatalf("cancellation evidence: status=%s cancelled_at=%v cancelled_by=%v completed_at=%v stop=%v blocked=%t attempts=%d first=%v outbox=%s",
			dbStatus, dbCancelledAt, dbCancelledBy, dbCompletedAt, dbStopReason, dbBlocked, dbAttempts, dbFirstReserve, outboxStatus)
	}
	if !dbCancelledAt.Equal(outcome.CancelledAt) {
		t.Fatalf("persisted cancellation timestamp %v differs from the outcome %v", dbCancelledAt, outcome.CancelledAt)
	}
	var after string
	if err = f.admin.QueryRow(f.ctx, `SELECT to_jsonb(s)::text FROM twin_states s WHERE entity_id=$1`, entity).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("cancellation changed desired or reported state")
	}

	repeat, err := repo.CancelControl(f.ctx, f.owner, urn, id.String())
	if err != nil {
		t.Fatalf("repeat cancel: %v", err)
	}
	if repeat.Status != outcome.Status || repeat.CommandID != outcome.CommandID || repeat.EntityID != outcome.EntityID ||
		repeat.DesiredVersion != outcome.DesiredVersion || repeat.StatusURL != outcome.StatusURL ||
		!repeat.CancelledAt.Equal(outcome.CancelledAt) {
		t.Fatalf("repeat outcome differs: %+v vs %+v", repeat, outcome)
	}

	// The command projection parses the cancelled status.
	view, err := repo.GetCommand(f.ctx, f.viewer, urn, id.String())
	if err != nil {
		t.Fatalf("cancelled command projection: %v", err)
	}
	if view.Status != digitaltwin.CommandCancelled || view.Result != nil {
		t.Fatalf("cancelled command projection: %+v", view)
	}
	page, err := repo.ListCommands(f.ctx, f.viewer, urn, 20, "")
	if err != nil || len(page.Commands) != 1 || page.Commands[0].Status != digitaltwin.CommandCancelled {
		t.Fatalf("cancelled command list: %+v %v", page, err)
	}

	// The expiry path never turns a cancelled command into a timeout, and the
	// repeated request stays stable after the original deadline passed.
	f.exec(t, `UPDATE twin_commands SET issued_at=clock_timestamp()-interval '2 minutes',expires_at=clock_timestamp()-interval '1 minute' WHERE command_id=$1`, id)
	if err = repo.ExpireControl(f.ctx, 32); err != nil {
		t.Fatal(err)
	}
	var expiredStatus string
	if err = f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id).Scan(&expiredStatus); err != nil {
		t.Fatal(err)
	}
	if expiredStatus != "cancelled" {
		t.Fatalf("cancelled command became %q", expiredStatus)
	}
	afterExpiry, err := repo.CancelControl(f.ctx, f.owner, urn, id.String())
	if err != nil {
		t.Fatalf("repeat cancel after expiry: %v", err)
	}
	if !afterExpiry.CancelledAt.Equal(outcome.CancelledAt) || afterExpiry.DesiredVersion != outcome.DesiredVersion {
		t.Fatalf("repeat after expiry differs: %+v vs %+v", afterExpiry, outcome)
	}

	// An expired pending command is not cancellable.
	expiredURN, _ := f.sensor(t, "cancel_expired")
	expiredID := admitStage6(t, repo, f, f.owner, expiredURN, "1", "0", "cancel-expired")
	f.exec(t, `UPDATE twin_commands SET issued_at=clock_timestamp()-interval '2 minutes',expires_at=clock_timestamp()-interval '1 minute' WHERE command_id=$1`, expiredID)
	if _, err = repo.CancelControl(f.ctx, f.owner, expiredURN, expiredID.String()); !errors.Is(err, digitaltwin.ErrCommandNotCancellable) {
		t.Fatalf("expired cancel: %v", err)
	}

	// A reserved command is not cancellable: the device may have received it.
	reservedURN, _ := f.sensor(t, "cancel_reserved")
	reservedID := admitStage6(t, repo, f, f.owner, reservedURN, "1", "0", "cancel-reserved")
	if dispatch, dispatchErr := repo.PrepareControl(f.ctx, reservedID); dispatchErr != nil || dispatch == nil {
		t.Fatalf("reservation: %+v %v", dispatch, dispatchErr)
	}
	if _, err = repo.CancelControl(f.ctx, f.owner, reservedURN, reservedID.String()); !errors.Is(err, digitaltwin.ErrCommandNotCancellable) {
		t.Fatalf("reserved cancel: %v", err)
	}

	// A published command is not cancellable.
	publishedURN, _ := f.sensor(t, "cancel_published")
	publishedID := admitStage6(t, repo, f, f.owner, publishedURN, "1", "0", "cancel-published")
	if _, err = repo.PrepareControl(f.ctx, publishedID); err != nil {
		t.Fatal(err)
	}
	if err = repo.MarkControlReservationPublished(f.ctx, publishedID, 1); err != nil {
		t.Fatal(err)
	}
	if _, err = repo.CancelControl(f.ctx, f.owner, publishedURN, publishedID.String()); !errors.Is(err, digitaltwin.ErrCommandNotCancellable) {
		t.Fatalf("published cancel: %v", err)
	}

	// The acknowledged slot is a reserved legacy status without a runtime
	// producer in this stage, so it is set explicitly as fixture state.
	acknowledgedURN, _ := f.sensor(t, "cancel_acknowledged")
	acknowledgedID := admitStage6(t, repo, f, f.owner, acknowledgedURN, "1", "0", "cancel-acknowledged")
	f.exec(t, `UPDATE twin_commands SET status='acknowledged' WHERE command_id=$1`, acknowledgedID)
	if _, err = repo.CancelControl(f.ctx, f.owner, acknowledgedURN, acknowledgedID.String()); !errors.Is(err, digitaltwin.ErrCommandNotCancellable) {
		t.Fatalf("acknowledged cancel: %v", err)
	}

	// A succeeded command is not cancellable.
	succeededURN, _ := f.sensor(t, "cancel_succeeded")
	succeededID := admitStage6(t, repo, f, f.owner, succeededURN, "1", "0", "cancel-succeeded")
	acceptRealResult(t, repo, f, succeededURN, succeededID, digitaltwin.CommandSucceeded)
	if _, err = repo.CancelControl(f.ctx, f.owner, succeededURN, succeededID.String()); !errors.Is(err, digitaltwin.ErrCommandNotCancellable) {
		t.Fatalf("succeeded cancel: %v", err)
	}

	// A failed command is not cancellable.
	failedURN, _ := f.sensor(t, "cancel_failed")
	failedID := admitStage6(t, repo, f, f.owner, failedURN, "1", "0", "cancel-failed")
	acceptRealResult(t, repo, f, failedURN, failedID, digitaltwin.CommandFailed)
	if _, err = repo.CancelControl(f.ctx, f.owner, failedURN, failedID.String()); !errors.Is(err, digitaltwin.ErrCommandNotCancellable) {
		t.Fatalf("failed cancel: %v", err)
	}

	// A timed out command is not cancellable.
	timedOutURN, _ := f.sensor(t, "cancel_timed_out")
	timedOutID := admitStage6(t, repo, f, f.owner, timedOutURN, "1", "0", "cancel-timed-out")
	f.exec(t, `UPDATE twin_commands SET issued_at=clock_timestamp()-interval '2 minutes',expires_at=clock_timestamp()-interval '1 minute' WHERE command_id=$1`, timedOutID)
	if err = repo.ExpireControl(f.ctx, 32); err != nil {
		t.Fatal(err)
	}
	if _, err = repo.CancelControl(f.ctx, f.owner, timedOutURN, timedOutID.String()); !errors.Is(err, digitaltwin.ErrCommandNotCancellable) {
		t.Fatalf("timed out cancel: %v", err)
	}

	// A legacy row has UNKNOWN dispatch history: attempt_count = 0 never
	// proves that the command was never sent.
	legacyURN, _ := f.sensor(t, "cancel_legacy")
	legacyID := admitStage6(t, f.repo, f, f.owner, legacyURN, "1", "0", "cancel-legacy")
	if _, err = repo.CancelControl(f.ctx, f.owner, legacyURN, legacyID.String()); !errors.Is(err, digitaltwin.ErrCommandNotCancellable) {
		t.Fatalf("legacy cancel: %v", err)
	}

	// Inconsistent mirrored counters fail closed instead of guessing.
	inconsistentURN, _ := f.sensor(t, "cancel_inconsistent")
	inconsistentID := admitStage6(t, repo, f, f.owner, inconsistentURN, "1", "0", "cancel-inconsistent")
	f.exec(t, `UPDATE twin_outbox SET attempt_count=1 WHERE command_id=$1`, inconsistentID)
	if _, err = repo.CancelControl(f.ctx, f.owner, inconsistentURN, inconsistentID.String()); !errors.Is(err, digitaltwin.ErrCommandNotCancellable) {
		t.Fatalf("inconsistent counters cancel: %v", err)
	}

	// The canonical cancellation evidence is immutable.
	if _, err = f.admin.Exec(f.ctx, `UPDATE twin_commands SET cancelled_at=clock_timestamp()+interval '1 second' WHERE command_id=$1`, id); err == nil {
		t.Fatal("cancellation evidence is mutable")
	}

	// A repeated request still requires the current membership.
	revokedURN, _ := f.sensor(t, "cancel_repeat_revoked")
	revokedID := admitStage6(t, repo, f, f.owner, revokedURN, "1", "0", "cancel-repeat-revoked")
	if _, err = repo.CancelControl(f.ctx, f.owner, revokedURN, revokedID.String()); err != nil {
		t.Fatalf("cancel before revocation: %v", err)
	}
	f.exec(t, `DELETE FROM user_gateways WHERE user_id=$1`, f.owner)
	if _, err = repo.CancelControl(f.ctx, f.owner, revokedURN, revokedID.String()); !errors.Is(err, digitaltwin.ErrNotFound) {
		t.Fatalf("repeated cancel after revocation: %v", err)
	}
}

// A late result for a cancelled command must not change the status and must
// not write the reported configuration.
func TestNativeControlCancelRejectsLateResult(t *testing.T) {
	f := newControlFixture(t)
	repo := f.stage6Repository(t)
	defer parkDueCommands(t, f, f.gw)
	urn, entity := f.sensor(t, "cancel_late_result")
	id := admitStage6(t, repo, f, f.owner, urn, "1", "0", "cancel-late-result")
	if _, err := repo.CancelControl(f.ctx, f.owner, urn, id.String()); err != nil {
		t.Fatal(err)
	}
	var before string
	if err := f.admin.QueryRow(f.ctx, `SELECT to_jsonb(s)::text FROM twin_states s WHERE entity_id=$1`, entity).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := acceptRealResultExpectingFailure(t, repo, f, urn, id, digitaltwin.CommandSucceeded); !errors.Is(err, digitaltwin.ErrControlResultConflict) {
		t.Fatalf("late result for a cancelled command: %v", err)
	}
	var status string
	if err := f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "cancelled" {
		t.Fatalf("cancelled command became %q", status)
	}
	var after string
	if err := f.admin.QueryRow(f.ctx, `SELECT to_jsonb(s)::text FROM twin_states s WHERE entity_id=$1`, entity).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("late result changed desired or reported state")
	}
}

// acceptRealResultExpectingFailure commits a result that the repository must
// reject. It returns the repository error for the caller to assert.
func acceptRealResultExpectingFailure(t *testing.T, repo *digitaltwin.PostgresControlRepository, f *controlFixture, urn string, id uuid.UUID, status digitaltwin.CommandStatus) (digitaltwin.ControlResultOutcome, error) {
	t.Helper()
	var issued time.Time
	var interval, version int64
	if err := f.admin.QueryRow(f.ctx, `SELECT c.issued_at,(c.parameters->>'sample_interval_us')::bigint,c.desired_version
		FROM twin_commands c WHERE c.command_id=$1`, id).Scan(&issued, &interval, &version); err != nil {
		t.Fatal(err)
	}
	result := digitaltwin.CommandResult{
		ProtocolVersion: 1,
		MessageType:     "command_result",
		CommandID:       id.String(),
		GatewayID:       f.gw,
		EntityID:        urn,
		DesiredVersion:  digitaltwin.Version(version),
		Status:          status,
		CompletedAt:     issued.UTC(),
		ReportedState:   &digitaltwin.SamplingParameters{SampleIntervalUS: interval},
	}
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	topic := "gateways/" + f.gw + "/responses/" + id.String()
	return repo.AcceptControlResult(f.ctx, topic, payload, time.Now().UTC())
}

func TestNativeControlCancelVersusReservation(t *testing.T) {
	f := newControlFixture(t)
	repo := f.stage6Repository(t)
	defer parkDueCommands(t, f, f.gw)

	// The cancellation wins: the worker must find no dispatch authority.
	cancelURN, _ := f.sensor(t, "race_cancel_wins")
	cancelID := admitStage6(t, repo, f, f.owner, cancelURN, "1", "0", "race-cancel-wins")
	if _, err := repo.CancelControl(f.ctx, f.owner, cancelURN, cancelID.String()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if dispatch, err := repo.PrepareControl(f.ctx, cancelID); err != nil || dispatch != nil {
		t.Fatalf("cancelled command obtained dispatch authority: %+v %v", dispatch, err)
	}

	// The reservation wins: the cancellation is refused afterwards.
	reserveURN, _ := f.sensor(t, "race_reserve_wins")
	reserveID := admitStage6(t, repo, f, f.owner, reserveURN, "1", "0", "race-reserve-wins")
	dispatch, err := repo.PrepareControl(f.ctx, reserveID)
	if err != nil || dispatch == nil {
		t.Fatalf("reservation: %+v %v", dispatch, err)
	}
	if _, err = repo.CancelControl(f.ctx, f.owner, reserveURN, reserveID.String()); !errors.Is(err, digitaltwin.ErrCommandNotCancellable) {
		t.Fatalf("cancel after reservation: %v", err)
	}

	// Concurrent: both operations serialize on the shared Sensor lock. The
	// barrier holds that lock until both operations are queued on it, so the
	// race is real without any guessed sleep.
	for attempt := range 3 {
		sensor := fmt.Sprintf("race_concurrent_%d", attempt)
		urn, _ := f.sensor(t, sensor)
		id := admitStage6(t, repo, f, f.owner, urn, "1", "0", fmt.Sprintf("race-concurrent-%d", attempt))
		barrier, err := f.app.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		var barrierPID int
		if err = barrier.QueryRow(f.ctx, `SELECT pg_backend_pid()`).Scan(&barrierPID); err != nil {
			t.Fatal(err)
		}
		if _, err = barrier.Exec(f.ctx, `SELECT sensor_id FROM public.sensors WHERE gateway_id=$1 AND sensor_id=$2 FOR UPDATE`, f.gw, sensor); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		ready := make(chan struct{}, 2)
		cancelOutcome := make(chan error, 1)
		reserveOutcome := make(chan error, 1)
		wg.Add(2)
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			_, cancelErr := repo.CancelControl(f.ctx, f.owner, urn, id.String())
			cancelOutcome <- cancelErr
		}()
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			_, reserveErr := repo.PrepareControl(f.ctx, id)
			reserveOutcome <- reserveErr
		}()
		<-ready
		<-ready
		waitForLockWaiters(t, f, barrierPID, 2)
		if err = barrier.Rollback(f.ctx); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		close(cancelOutcome)
		close(reserveOutcome)
		var cancelErr, reserveErr error
		for err := range cancelOutcome {
			cancelErr = err
		}
		for err := range reserveOutcome {
			reserveErr = err
		}
		var status string
		var attempts int
		var firstReserve *time.Time
		if err = f.admin.QueryRow(f.ctx, `SELECT status,attempt_count,first_reserved_at FROM twin_commands WHERE command_id=$1`, id).Scan(&status, &attempts, &firstReserve); err != nil {
			t.Fatal(err)
		}
		switch {
		case cancelErr == nil:
			if status != "cancelled" {
				t.Fatalf("accepted cancellation left status %q", status)
			}
			if reserveErr != nil {
				t.Fatalf("reservation failed after the cancellation won: %v", reserveErr)
			}
			if dispatch, err := repo.PrepareControl(f.ctx, id); err != nil || dispatch != nil {
				t.Fatalf("cancelled command obtained dispatch authority: %+v %v", dispatch, err)
			}
		case errors.Is(cancelErr, digitaltwin.ErrCommandNotCancellable):
			if attempts < 1 || firstReserve == nil {
				t.Fatalf("refused cancellation without a reservation: attempts=%d first=%v", attempts, firstReserve)
			}
		default:
			t.Fatalf("unexpected cancellation error: %v", cancelErr)
		}
	}
}

// waitForLockWaiters waits until the requested number of backends wait for a
// lock. A transaction blocked on the barrier row lock appears as an ungranted
// tuple lock, and a transaction queued behind it waits for the transaction id
// of the first waiter. It is a bounded condition wait, never a guessed sleep.
func waitForLockWaiters(t *testing.T, f *controlFixture, barrierPID int, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		err := f.admin.QueryRow(f.ctx, `SELECT count(DISTINCT pid) FROM pg_locks WHERE NOT granted AND pid<>$1`, barrierPID).Scan(&waiting)
		if err == nil && waiting >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("lock waiters did not reach %d", want)
}

func TestNativeControlCancelRollbackAndAmbiguousCommit(t *testing.T) {
	f := newControlFixture(t)
	repo := f.stage6Repository(t)
	defer parkDueCommands(t, f, f.gw)
	urn, _ := f.sensor(t, "cancel_rollback")
	id := admitStage6(t, repo, f, f.owner, urn, "1", "0", "cancel-rollback")

	injected := false
	faulty, err := digitaltwin.NewPostgresControlRepository(&faultInjectingTxDatabase{
		underlying: f.app,
		onTxBegin: func(tx pgx.Tx) pgx.Tx {
			return &faultInjectingTx{Tx: tx, onExec: func(_ context.Context, _ pgx.Tx, sql string, _ ...any) (pgconn.CommandTag, error, bool) {
				if strings.Contains(sql, "SET status='cancelled'") {
					injected = true
					return pgconn.CommandTag{}, errors.New("injected cancellation failure"), true
				}
				return pgconn.CommandTag{}, nil, false
			}}
		},
	}, stage6Config())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = faulty.CancelControl(f.ctx, f.owner, urn, id.String()); err == nil || !injected {
		t.Fatalf("cancellation fault not exercised: injected=%t err=%v", injected, err)
	}
	var status, outbox string
	if err = f.admin.QueryRow(f.ctx, `SELECT c.status,o.status FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE c.command_id=$1`, id).Scan(&status, &outbox); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || outbox != "pending" {
		t.Fatalf("rolled back cancellation left %s/%s", status, outbox)
	}

	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("ambiguous_commit_%t", committed), func(t *testing.T) {
			g := newControlFixture(t)
			gRepo := g.stage6Repository(t)
			defer parkDueCommands(t, g, g.gw)
			gURN, _ := g.sensor(t, "cancel_uncertain")
			gID := admitStage6(t, gRepo, g, g.owner, gURN, "1", "0", "cancel-uncertain")
			ambiguous, err := digitaltwin.NewPostgresControlRepository(&faultInjectingTxDatabase{
				underlying: g.app,
				onTxBegin:  func(tx pgx.Tx) pgx.Tx { return &controlCommitTx{Tx: tx, after: committed} },
			}, stage6Config())
			if err != nil {
				t.Fatal(err)
			}
			outcome, err := ambiguous.CancelControl(g.ctx, g.owner, gURN, gID.String())
			if err == nil {
				t.Fatalf("ambiguous commit reported a successful cancellation: %+v", outcome)
			}
			if !errors.Is(err, digitaltwin.ErrControlCommitUncertain) || !errors.Is(err, digitaltwin.ErrDependencyUnavailable) {
				t.Fatalf("ambiguous commit error: %v", err)
			}
			if outcome.Status != "" {
				t.Fatalf("ambiguous commit returned an outcome: %+v", outcome)
			}
			// The client may read the state or repeat the request safely.
			repeat, err := gRepo.CancelControl(g.ctx, g.owner, gURN, gID.String())
			if err != nil || repeat.Status != digitaltwin.CommandCancelled {
				t.Fatalf("repeated cancellation after an ambiguous commit: %+v %v", repeat, err)
			}
		})
	}
}

func stage6Config() digitaltwin.ControlConfig {
	config := digitaltwin.DefaultControlConfig()
	config.Stage6Enabled = true
	return config
}

// The safe cancellation surface exists only with Stage 6 enabled. A Stage 5
// repository refuses even a managed row instead of cancelling it silently.
func TestNativeControlCancelFeatureOffGuard(t *testing.T) {
	f := newControlFixture(t)
	stage6 := f.stage6Repository(t)
	urn, _ := f.sensor(t, "cancel_feature_off")
	id := admitStage6(t, stage6, f, f.owner, urn, "1", "0", "cancel-feature-off")
	if _, err := f.repo.CancelControl(f.ctx, f.owner, urn, id.String()); !errors.Is(err, digitaltwin.ErrCommandNotCancellable) {
		t.Fatalf("feature off cancellation: %v", err)
	}
	var status string
	if err := f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id).Scan(&status); err != nil || status != "pending" {
		t.Fatalf("feature off cancellation changed the command: %s %v", status, err)
	}
}

// The command projection exposes safe Stage 6 metadata and never claims a
// dispatch history it cannot prove.
func TestNativeControlCancelProjectionMetadata(t *testing.T) {
	f := newControlFixture(t)
	repo := f.stage6Repository(t)

	managedURN, _ := f.sensor(t, "projection_managed")
	managedID := admitStage6(t, repo, f, f.owner, managedURN, "1", "0", "projection-managed")
	view, err := repo.GetCommand(f.ctx, f.viewer, managedURN, managedID.String())
	if err != nil {
		t.Fatal(err)
	}
	if view.AttemptCount == nil || *view.AttemptCount != 0 || view.CancelledAt != nil || view.RetryStopReason != nil || view.AcknowledgedAt != nil {
		t.Fatalf("managed projection: %+v", view)
	}
	if view.Origin == nil || *view.Origin != "client" || view.RootCommandID == nil || *view.RootCommandID != managedID.String() || view.ParentCommandID != nil {
		t.Fatalf("managed lineage: %+v", view)
	}

	// After a reservation the mirrored attempt count is proven.
	if dispatch, dispatchErr := repo.PrepareControl(f.ctx, managedID); dispatchErr != nil || dispatch == nil {
		t.Fatalf("reservation: %+v %v", dispatch, dispatchErr)
	}
	view, err = repo.GetCommand(f.ctx, f.viewer, managedURN, managedID.String())
	if err != nil {
		t.Fatal(err)
	}
	if view.AttemptCount == nil || *view.AttemptCount != 1 {
		t.Fatalf("reserved projection: %+v", view)
	}

	// A Stage 5 row has unknown dispatch history even with control_version 1.
	// Migration 27 gives it the default origin value, but that default is not
	// proof of provenance and must not be exposed as a known client origin.
	stage5URN, stage5Entity := f.sensor(t, "projection_stage5")
	stage5ID := admitStage6(t, f.repo, f, f.owner, stage5URN, "1", "0", "projection-stage5")
	view, err = repo.GetCommand(f.ctx, f.viewer, stage5URN, stage5ID.String())
	if err != nil {
		t.Fatal(err)
	}
	if view.AttemptCount != nil {
		t.Fatalf("stage 5 attempt history must stay unknown: %+v", view)
	}
	if view.Origin != nil || view.RootCommandID != nil || view.ParentCommandID != nil {
		t.Fatalf("stage 5 provenance must stay unknown: %+v", view)
	}

	// A legacy sentinel row has no proven origin at all.
	legacyID := uuid.New()
	f.exec(t, `INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,issued_at,expires_at)
		VALUES($1,$2,'legacy_sentinel','{"sampling_interval_seconds":5}','failed',1,'2026-01-01Z','2026-01-02Z')`, legacyID, stage5Entity)
	view, err = repo.GetCommand(f.ctx, f.viewer, stage5URN, legacyID.String())
	if err != nil {
		t.Fatal(err)
	}
	if view.AttemptCount != nil || view.Origin != nil {
		t.Fatalf("legacy projection: %+v", view)
	}
	if !view.Legacy {
		t.Fatalf("legacy marker lost: %+v", view)
	}

	// A cancelled command exposes the immutable cancellation evidence.
	cancelledURN, _ := f.sensor(t, "projection_cancelled")
	cancelledID := admitStage6(t, repo, f, f.owner, cancelledURN, "1", "0", "projection-cancelled")
	if _, err = repo.CancelControl(f.ctx, f.owner, cancelledURN, cancelledID.String()); err != nil {
		t.Fatal(err)
	}
	view, err = repo.GetCommand(f.ctx, f.viewer, cancelledURN, cancelledID.String())
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != digitaltwin.CommandCancelled || view.CancelledAt == nil || view.RetryStopReason == nil || *view.RetryStopReason != "cancelled" {
		t.Fatalf("cancelled projection: %+v", view)
	}
	if view.CancelledAt.Location() != time.UTC || view.CancelledAt.Nanosecond()%1000 != 0 {
		t.Fatalf("cancellation timestamp is not UTC microsecond: %v", view.CancelledAt)
	}
}

func TestNativeControlCancelKeepsBlockedIntentAndAllowsNewIntent(t *testing.T) {
	f := newControlFixture(t)
	repo := f.stage6Repository(t)
	defer parkDueCommands(t, f, f.gw)
	urn, entity := f.sensor(t, "cancel_block")
	id := admitStage6(t, repo, f, f.owner, urn, "10", "0", "cancel-block")
	if _, err := repo.CancelControl(f.ctx, f.owner, urn, id.String()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	var blocked bool
	if err := f.admin.QueryRow(f.ctx, `SELECT reconcile_blocked FROM twin_commands WHERE command_id=$1`, id).Scan(&blocked); err != nil {
		t.Fatal(err)
	}
	if !blocked {
		t.Fatal("cancelled intent is not blocked")
	}

	// A replayed key returns the original receipt and never clears the block.
	replay, err := repo.Admit(f.ctx, f.owner, urn, controlRequest(t, "10", "0", "cancel-block"))
	if err != nil || replay.StatusCode != 202 || replay.CommandID == nil || *replay.CommandID != id.String() {
		t.Fatalf("replayed admission: %+v %v", replay, err)
	}
	if err = f.admin.QueryRow(f.ctx, `SELECT reconcile_blocked FROM twin_commands WHERE command_id=$1`, id).Scan(&blocked); err != nil {
		t.Fatal(err)
	}
	if !blocked {
		t.Fatal("replay cleared the cancellation block")
	}

	// Fixture state of an already converged device: the reported interval
	// equals the desired interval of the cancelled intent.
	f.exec(t, `UPDATE twin_states SET reported_state=jsonb_set(reported_state,'{configuration}','{"sample_interval_us":100000,"desired_version":"1"}'::jsonb,true) WHERE entity_id=$1`, entity)

	// A new user intent re-asserts the cancelled intent and creates a new
	// root command instead of a silent no-op.
	reassert, err := repo.Admit(f.ctx, f.owner, urn, controlRequest(t, "10", "1", "cancel-reassert"))
	if err != nil || reassert.StatusCode != 202 || reassert.CommandID == nil {
		t.Fatalf("re-asserted admission: %+v %v", reassert, err)
	}
	if *reassert.CommandID == id.String() {
		t.Fatal("re-assertion reused the cancelled command")
	}
	var newRoot *uuid.UUID
	var newBlocked bool
	var newOrigin string
	if err = f.admin.QueryRow(f.ctx, `SELECT root_command_id,reconcile_blocked,origin FROM twin_commands WHERE command_id=$1`, *reassert.CommandID).Scan(&newRoot, &newBlocked, &newOrigin); err != nil {
		t.Fatal(err)
	}
	if newRoot == nil || *newRoot != uuid.MustParse(*reassert.CommandID) || newBlocked || newOrigin != "client" {
		t.Fatalf("re-asserted command lineage: root=%v blocked=%t origin=%s", newRoot, newBlocked, newOrigin)
	}
	if err = f.admin.QueryRow(f.ctx, `SELECT reconcile_blocked FROM twin_commands WHERE command_id=$1`, id).Scan(&blocked); err != nil {
		t.Fatal(err)
	}
	if !blocked {
		t.Fatal("re-assertion cleared the cancelled intent block")
	}

	// The cancelled command is never dispatched again, the new intent is.
	pending, err := repo.PendingControl(f.ctx, 32)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range pending {
		if candidate == id {
			t.Fatal("cancelled command is dispatchable again")
		}
	}
	found := false
	for _, candidate := range pending {
		if candidate == uuid.MustParse(*reassert.CommandID) {
			found = true
		}
	}
	if !found {
		t.Fatal("re-asserted command is not dispatchable")
	}
}

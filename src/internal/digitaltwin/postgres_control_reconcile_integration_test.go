package digitaltwin_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/metrics"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestNativeControlReconciliationSuccessorAtomicAndBudget(t *testing.T) {
	if os.Getenv("CONTROL_TEST_ISOLATED") != "1" {
		t.Skip("requires owned isolated PostgreSQL fixture")
	}
	f := newControlFixture(t)
	config := digitaltwin.DefaultControlConfig()
	config.Stage6Enabled = true
	repo, err := digitaltwin.NewPostgresControlRepository(f.app, config)
	if err != nil {
		t.Fatal(err)
	}
	f.repo = repo
	timeline, err := metrics.NewControlTimeline(8, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	repo.SetControlTimeline(timeline)

	receipt, err := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.5", "0", "r71-root-intent"))
	if err != nil || receipt.CommandID == nil {
		t.Fatalf("commit original user intent: receipt=%+v err=%v", receipt, err)
	}
	rootID := uuid.MustParse(*receipt.CommandID)
	successorNotifications := 0
	repo.SetAdmissionNotifier(func() { successorNotifications++ })
	f.exec(t, `UPDATE public.twin_commands SET issued_at=clock_timestamp()-interval '2 hours',
		expires_at=clock_timestamp()-interval '1 hour' WHERE command_id=$1`, rootID)

	query := digitaltwin.ControlQueryContext{
		EntityID: f.entity, EntityURN: f.urn, GatewayID: f.gw, SensorID: "s1", DesiredVersion: 1,
	}
	actual := int64(10_000_000)
	snapshot := digitaltwin.ConfigurationSnapshot{
		ProtocolVersion: 1, MessageType: "configuration_snapshot", RequestID: uuid.NewString(),
		GatewayID: f.gw, EntityID: f.urn, WatermarkVersion: 0, ConfigurationState: "unconfigured",
		ActualSampleIntervalUS: &actual, ReadbackAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	deadline := time.Now().Add(5 * time.Second)
	timelineBeforeSuccessor := timeline.Snapshot()
	outcome, err := repo.ReconcileSnapshot(f.ctx, query, snapshot, deadline)
	if err != nil || outcome.Decision != digitaltwin.ReconciliationSuccessorCreated ||
		outcome.CommandID == "" || outcome.DesiredVersion != 2 {
		t.Fatalf("create successor for expired intent without reported confirmation: outcome=%+v err=%v", outcome, err)
	}
	if successorNotifications != 1 {
		t.Fatalf("confirmed successor commit notifications=%d want=1", successorNotifications)
	}
	if timing := timeline.Snapshot(); timing.MissingOrigin != timelineBeforeSuccessor.MissingOrigin+1 ||
		timing.Entries != timelineBeforeSuccessor.Entries || timing.MissingResult != timelineBeforeSuccessor.MissingResult {
		t.Fatalf("system successor fabricated timing origin/result evidence: %+v", timing)
	}
	successorID := uuid.MustParse(outcome.CommandID)
	if successorID == rootID {
		t.Fatal("successor reused root command ID")
	}
	rootView, err := repo.GetCommand(f.ctx, f.owner, f.urn, rootID.String())
	if err != nil {
		t.Fatal(err)
	}
	successorView, err := repo.GetCommand(f.ctx, f.viewer, f.urn, successorID.String())
	if err != nil {
		t.Fatal("viewer could not read in-scope successor:", err)
	}
	if rootView.Origin == nil || *rootView.Origin != "client" ||
		rootView.RootCommandID == nil || *rootView.RootCommandID != rootID.String() ||
		rootView.ParentCommandID != nil || rootView.RootAutoSuccessorsUsed == nil ||
		*rootView.RootAutoSuccessorsUsed != 1 || rootView.CurrentReconciliation != nil {
		t.Fatalf("root projection lost provenance/budget or attached child decision: %+v", rootView)
	}
	if successorView.Origin == nil || *successorView.Origin != "reconciliation" ||
		successorView.RootCommandID == nil || *successorView.RootCommandID != rootID.String() ||
		successorView.ParentCommandID == nil || *successorView.ParentCommandID != rootID.String() ||
		successorView.RootAutoSuccessorsUsed == nil || *successorView.RootAutoSuccessorsUsed != 1 ||
		successorView.ReconcileBlocked == nil || *successorView.ReconcileBlocked ||
		successorView.RootReconcileBlocked == nil || *successorView.RootReconcileBlocked ||
		successorView.CurrentReconciliation == nil ||
		successorView.CurrentReconciliation.Decision != string(digitaltwin.ReconciliationSuccessorCreated) ||
		successorView.CurrentReconciliation.DesiredVersion != 2 {
		t.Fatalf("successor projection lacks safe current lineage context: %+v", successorView)
	}
	page, err := repo.ListCommands(f.ctx, f.viewer, f.urn, 10, "")
	if err != nil {
		t.Fatal("viewer could not list in-scope commands:", err)
	}
	listedChild := false
	for _, listed := range page.Commands {
		if listed.CommandID == successorID.String() {
			listedChild = listed.Origin != nil && *listed.Origin == "reconciliation" &&
				listed.RootAutoSuccessorsUsed != nil && *listed.RootAutoSuccessorsUsed == 1 &&
				listed.CurrentReconciliation != nil &&
				listed.CurrentReconciliation.Decision == string(digitaltwin.ReconciliationSuccessorCreated)
		}
	}
	if !listedChild {
		t.Fatalf("command list omitted safe successor projection: %+v", page.Commands)
	}
	if _, err := repo.GetCommand(f.ctx, f.outsider, f.urn, successorID.String()); !errors.Is(err, digitaltwin.ErrNotFound) {
		t.Fatalf("cross-Gateway outsider command read was not hidden: %v", err)
	}
	legacyURN, _ := f.sensor(t, "r72_legacy_projection")
	legacyRepo, err := digitaltwin.NewPostgresControlRepository(f.app, digitaltwin.DefaultControlConfig())
	if err != nil {
		t.Fatal(err)
	}
	legacyReceipt, err := legacyRepo.Admit(f.ctx, f.owner, legacyURN, controlRequest(t, "0.5", "0", "r72-legacy"))
	if err != nil || legacyReceipt.CommandID == nil {
		t.Fatalf("create legacy-provenance command: receipt=%+v err=%v", legacyReceipt, err)
	}
	legacyView, err := legacyRepo.GetCommand(f.ctx, f.owner, legacyURN, *legacyReceipt.CommandID)
	if err != nil {
		t.Fatal(err)
	}
	if legacyView.Origin != nil || legacyView.RootCommandID != nil || legacyView.ParentCommandID != nil ||
		legacyView.ReconcileBlocked != nil || legacyView.RootReconcileBlocked != nil ||
		legacyView.RootAutoSuccessorsUsed != nil || legacyView.CurrentReconciliation != nil {
		t.Fatalf("unknown Stage 5 provenance was guessed as Stage 6: %+v", legacyView)
	}

	var rootStatus, rootOrigin string
	var rootVersion int64
	var rootBudget int
	var rootBlocked bool
	var rootActor string
	if err := f.admin.QueryRow(f.ctx, `SELECT status,origin,desired_version,auto_successors_used,reconcile_blocked,original_actor_id::text
		FROM twin_commands WHERE command_id=$1`, rootID).Scan(&rootStatus, &rootOrigin, &rootVersion, &rootBudget, &rootBlocked, &rootActor); err != nil {
		t.Fatal(err)
	}
	if rootStatus != string(digitaltwin.CommandTimeout) || rootOrigin != "client" || rootVersion != 1 ||
		rootBudget != 1 || rootBlocked || rootActor != f.owner.String() {
		t.Fatalf("root evidence/budget not preserved atomically: status=%s origin=%s version=%d budget=%d blocked=%t actor=%s",
			rootStatus, rootOrigin, rootVersion, rootBudget, rootBlocked, rootActor)
	}
	var origin, rootLink, parentLink string
	var actor, issuedBy, idempotencyKey sql.NullString
	var successorStatus string
	var successorVersion int64
	var successorBudget int
	if err := f.admin.QueryRow(f.ctx, `SELECT origin,root_command_id::text,parent_command_id::text,original_actor_id::text,
		issued_by::text,idempotency_key,status,desired_version,auto_successors_used FROM twin_commands WHERE command_id=$1`,
		successorID).Scan(&origin, &rootLink, &parentLink, &actor, &issuedBy, &idempotencyKey,
		&successorStatus, &successorVersion, &successorBudget); err != nil {
		t.Fatal(err)
	}
	if origin != "reconciliation" || rootLink != rootID.String() || parentLink != rootID.String() ||
		!actor.Valid || actor.String != f.owner.String() || issuedBy.Valid || idempotencyKey.Valid ||
		successorStatus != string(digitaltwin.CommandPending) || successorVersion != 2 || successorBudget != 0 {
		t.Fatalf("successor provenance or system identity incorrect: origin=%s root=%s parent=%s actor=%v issued_by=%v key=%v status=%s version=%d budget=%d",
			origin, rootLink, parentLink, actor, issuedBy, idempotencyKey, successorStatus, successorVersion, successorBudget)
	}
	var topic string
	var payloadBytes []byte
	if err := f.admin.QueryRow(f.ctx, `SELECT topic,payload_bytes FROM twin_outbox WHERE command_id=$1`, successorID).Scan(&topic, &payloadBytes); err != nil {
		t.Fatal("successor outbox missing:", err)
	}
	command, err := digitaltwin.ParseMQTTCommand(topic, payloadBytes, false)
	if err != nil || command.CommandID != successorID.String() || command.DesiredVersion != 2 ||
		command.Parameters.SampleIntervalUS != 2_000_000 || !command.ExpiresAt.After(command.IssuedAt) {
		t.Fatalf("successor outbox is not a new canonical command: command=%+v err=%v", command, err)
	}
	var desiredVersion int64
	var desiredInterval, reportedInterval int64
	if err := f.admin.QueryRow(f.ctx, `SELECT desired_version,(desired_state->>'sample_interval_us')::bigint,
		COALESCE((reported_state->'configuration'->>'sample_interval_us')::bigint,0)
		FROM twin_states WHERE entity_id=$1`, f.entity).Scan(&desiredVersion, &desiredInterval, &reportedInterval); err != nil {
		t.Fatal(err)
	}
	if desiredVersion != 2 || desiredInterval != 2_000_000 || reportedInterval != 0 {
		t.Fatalf("successor changed state incorrectly: version=%d desired=%d reported=%d", desiredVersion, desiredInterval, reportedInterval)
	}
	var receipts int
	if err := f.admin.QueryRow(f.ctx, `SELECT count(*) FROM twin_command_requests WHERE entity_id=$1`, f.entity).Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("system successor created an HTTP request receipt: count=%d err=%v", receipts, err)
	}

	if _, err := repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second)); !errors.Is(err, digitaltwin.ErrControlQueryContextChanged) {
		t.Fatalf("old desired-version candidate was not discarded: %v", err)
	}
	restarted, err := digitaltwin.NewPostgresControlRepository(f.app, config)
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, `UPDATE public.twin_commands SET issued_at=clock_timestamp()-interval '2 hours',
		expires_at=clock_timestamp()-interval '1 hour' WHERE command_id=$1`, successorID)
	current := query
	current.DesiredVersion = 2
	snapshot.WatermarkVersion = 1
	snapshot.ConfigurationState = "configured"
	durable := int64(2_000_000)
	snapshot.DurableSampleIntervalUS = &durable
	snapshot.ActualSampleIntervalUS = &actual
	afterRestart, err := restarted.ReconcileSnapshot(f.ctx, current, snapshot, time.Now().Add(5*time.Second))
	if err != nil || afterRestart.Decision != digitaltwin.ReconciliationBlockedBudget || afterRestart.CommandID != "" {
		t.Fatalf("spent root budget did not survive repository reconstruction: outcome=%+v err=%v", afterRestart, err)
	}
	var commandCount int
	if err := f.admin.QueryRow(f.ctx, `SELECT count(*) FROM twin_commands WHERE entity_id=$1`, f.entity).Scan(&commandCount); err != nil || commandCount != 2 {
		t.Fatalf("repeated candidate created another successor: commands=%d err=%v", commandCount, err)
	}
}

func TestNativeControlReconciliationDecisionGuards(t *testing.T) {
	if os.Getenv("CONTROL_TEST_ISOLATED") != "1" {
		t.Skip("requires owned isolated PostgreSQL fixture")
	}
	f := newControlFixture(t)
	config := digitaltwin.DefaultControlConfig()
	config.Stage6Enabled = true
	repo, err := digitaltwin.NewPostgresControlRepository(f.app, config)
	if err != nil {
		t.Fatal(err)
	}
	f.repo = repo

	candidate := func(urn string, entity uuid.UUID, version, watermark digitaltwin.Version, state string, interval *int64) (digitaltwin.ControlQueryContext, digitaltwin.ConfigurationSnapshot) {
		identity, parseErr := digitaltwin.ParseEntityURN(urn)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		query := digitaltwin.ControlQueryContext{
			EntityID: entity, EntityURN: urn, GatewayID: identity.GatewayID,
			SensorID: identity.SensorID, DesiredVersion: version,
		}
		snapshot := digitaltwin.ConfigurationSnapshot{
			ProtocolVersion: 1, MessageType: "configuration_snapshot", RequestID: uuid.NewString(),
			GatewayID: identity.GatewayID, EntityID: urn, WatermarkVersion: watermark,
			ConfigurationState: state, ActualSampleIntervalUS: interval,
			ReadbackAt: time.Now().UTC().Truncate(time.Microsecond),
		}
		if state == "configured" {
			snapshot.DurableSampleIntervalUS = interval
		}
		return query, snapshot
	}

	// Provisioned defaults without a committed user command are never adopted.
	query, snapshot := candidate(f.urn, f.entity, 0, 0, "unconfigured", nil)
	outcome, err := repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
	if err != nil || outcome.Decision != digitaltwin.ReconciliationPreconditionUnmet || outcome.CommandID != "" {
		t.Fatalf("uncommitted default was treated as intent: outcome=%+v err=%v", outcome, err)
	}

	// Active commands take precedence and do not create a competing successor.
	activeURN, activeEntity := f.sensor(t, "r71_active")
	activeReceipt, err := repo.Admit(f.ctx, f.owner, activeURN, controlRequest(t, "0.5", "0", "r71-active"))
	if err != nil || activeReceipt.CommandID == nil {
		t.Fatalf("create active command: receipt=%+v err=%v", activeReceipt, err)
	}
	query, snapshot = candidate(activeURN, activeEntity, 1, 0, "unconfigured", nil)
	outcome, err = repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
	if err != nil || outcome.Decision != digitaltwin.ReconciliationActiveBusy {
		t.Fatalf("active command did not block successor: outcome=%+v err=%v", outcome, err)
	}

	// Cancellation's durable root block prevents reasserting the same intent.
	cancelURN, cancelEntity := f.sensor(t, "r71_cancelled")
	cancelReceipt, err := repo.Admit(f.ctx, f.owner, cancelURN, controlRequest(t, "0.5", "0", "r71-cancel"))
	if err != nil || cancelReceipt.CommandID == nil {
		t.Fatalf("create cancellable root: receipt=%+v err=%v", cancelReceipt, err)
	}
	if _, err := repo.CancelControl(f.ctx, f.owner, cancelURN, *cancelReceipt.CommandID); err != nil {
		t.Fatal("cancel root intent:", err)
	}
	query, snapshot = candidate(cancelURN, cancelEntity, 1, 0, "unconfigured", nil)
	outcome, err = repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
	if err != nil || outcome.Decision != digitaltwin.ReconciliationBlockedCancelled || outcome.CommandID != "" {
		t.Fatalf("cancelled root was not blocked: outcome=%+v err=%v", outcome, err)
	}

	// Only a committed command result can establish reported configuration;
	// exact agreement is a no-op and must not consume a successor.
	convergedURN, convergedEntity := f.sensor(t, "r71_converged")
	convergedReceipt, err := repo.Admit(f.ctx, f.owner, convergedURN, controlRequest(t, "0.5", "0", "r71-converged"))
	if err != nil || convergedReceipt.CommandID == nil {
		t.Fatalf("create convergence intent: receipt=%+v err=%v", convergedReceipt, err)
	}
	var topic string
	var payload []byte
	if err := f.admin.QueryRow(f.ctx, `SELECT topic,payload_bytes FROM twin_outbox WHERE command_id=$1`,
		*convergedReceipt.CommandID).Scan(&topic, &payload); err != nil {
		t.Fatal(err)
	}
	command, err := digitaltwin.ParseMQTTCommand(topic, payload, false)
	if err != nil {
		t.Fatal(err)
	}
	resultBytes, err := json.Marshal(resultEnvelope(command))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptControlResult(f.ctx, "gateways/"+f.gw+"/responses/"+command.CommandID, resultBytes, time.Now()); err != nil {
		t.Fatal("commit real command result:", err)
	}
	// Model ordinary measurement reports that advance only the aggregate
	// reported_version counter; the configuration branch still came from the
	// actual successful result transaction above.
	f.exec(t, `UPDATE twin_states SET reported_version=reported_version+3 WHERE entity_id=$1`, convergedEntity)
	convergedInterval := int64(2_000_000)
	query, snapshot = candidate(convergedURN, convergedEntity, 1, 1, "configured", &convergedInterval)
	outcome, err = repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
	if err != nil || outcome.Decision != digitaltwin.ReconciliationConverged || outcome.CommandID != "" ||
		outcome.DesiredVersion != 1 {
		t.Fatalf("confirmed state did not converge without successor: outcome=%+v err=%v", outcome, err)
	}
	var commandCount int
	var desiredVersion int64
	if err := f.admin.QueryRow(f.ctx, `SELECT count(*),(SELECT desired_version FROM twin_states WHERE entity_id=$1)
		FROM twin_commands WHERE entity_id=$1`, convergedEntity).Scan(&commandCount, &desiredVersion); err != nil ||
		commandCount != 1 || desiredVersion != 1 {
		t.Fatalf("converged decision changed command/version: commands=%d version=%d err=%v", commandCount, desiredVersion, err)
	}

	// A second actual success puts confirmed configuration version 2 in JSON,
	// while the measurement counter is deliberately a much larger value.
	gapReceipt, err := repo.Admit(f.ctx, f.owner, convergedURN, controlRequest(t, "1", "1", "r71-version-gap"))
	if err != nil || gapReceipt.CommandID == nil {
		t.Fatalf("create version-gap intent: receipt=%+v err=%v", gapReceipt, err)
	}
	if err := f.admin.QueryRow(f.ctx, `SELECT topic,payload_bytes FROM twin_outbox WHERE command_id=$1`,
		*gapReceipt.CommandID).Scan(&topic, &payload); err != nil {
		t.Fatal(err)
	}
	command, err = digitaltwin.ParseMQTTCommand(topic, payload, false)
	if err != nil {
		t.Fatal(err)
	}
	resultBytes, err = json.Marshal(resultEnvelope(command))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptControlResult(f.ctx, "gateways/"+f.gw+"/responses/"+command.CommandID, resultBytes, time.Now()); err != nil {
		t.Fatal("commit version-gap result:", err)
	}
	f.exec(t, `UPDATE twin_states SET reported_version=reported_version+5 WHERE entity_id=$1`, convergedEntity)
	gapInterval := int64(1_000_000)
	query, snapshot = candidate(convergedURN, convergedEntity, 2, 2, "configured", &gapInterval)
	outcome, err = repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
	if err != nil || outcome.Decision != digitaltwin.ReconciliationConverged || outcome.CommandID != "" {
		t.Fatalf("configuration version was confused with telemetry counter: outcome=%+v err=%v", outcome, err)
	}
	var confirmedVersion string
	var aggregateVersion int64
	if err := f.admin.QueryRow(f.ctx, `SELECT reported_state->'configuration'->>'desired_version',reported_version
		FROM twin_states WHERE entity_id=$1`, convergedEntity).Scan(&confirmedVersion, &aggregateVersion); err != nil ||
		confirmedVersion != "2" || aggregateVersion == 2 {
		t.Fatalf("test did not establish distinct version sources: configuration=%s aggregate=%d err=%v",
			confirmedVersion, aggregateVersion, err)
	}

	// A high Gateway watermark or unavailable store is never adopted as state.
	manualURN, manualEntity := f.sensor(t, "r71_manual")
	manualReceipt, err := repo.Admit(f.ctx, f.owner, manualURN, controlRequest(t, "0.5", "0", "r71-manual"))
	if err != nil || manualReceipt.CommandID == nil {
		t.Fatalf("create manual intent: receipt=%+v err=%v", manualReceipt, err)
	}
	f.exec(t, `UPDATE public.twin_commands SET issued_at=clock_timestamp()-interval '2 hours',
		expires_at=clock_timestamp()-interval '1 hour' WHERE command_id=$1`, *manualReceipt.CommandID)
	query, snapshot = candidate(manualURN, manualEntity, 1, 2, "configured", pointerInt64(2_000_000))
	outcome, err = repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
	if err != nil || outcome.Decision != digitaltwin.ReconciliationManualUnconfirmed || outcome.CommandID != "" {
		t.Fatalf("higher Gateway watermark was not kept manual: outcome=%+v err=%v", outcome, err)
	}
	query, snapshot = candidate(manualURN, manualEntity, 1, 0, "unavailable", nil)
	outcome, err = repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
	if err != nil || outcome.Decision != digitaltwin.ReconciliationCorruptUnavailable || outcome.CommandID != "" {
		t.Fatalf("unavailable Gateway store did not fail closed: outcome=%+v err=%v", outcome, err)
	}
	if _, err := repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(-time.Second)); !errors.Is(err, digitaltwin.ErrControlQueryExpired) {
		t.Fatalf("expired local query deadline was accepted: %v", err)
	}

	for _, code := range []string{"UNKNOWN_SENSOR", "UNSUPPORTED_CONFIGURATION"} {
		failedURN, failedEntity := f.sensor(t, "r71_failed_"+strings.ToLower(code))
		failedReceipt, err := repo.Admit(f.ctx, f.owner, failedURN, controlRequest(t, "0.5", "0", "r71-failed-"+strings.ToLower(code)))
		if err != nil || failedReceipt.CommandID == nil {
			t.Fatalf("create %s intent: receipt=%+v err=%v", code, failedReceipt, err)
		}
		var failedTopic string
		var failedPayload []byte
		if err := f.admin.QueryRow(f.ctx, `SELECT topic,payload_bytes FROM twin_outbox WHERE command_id=$1`,
			*failedReceipt.CommandID).Scan(&failedTopic, &failedPayload); err != nil {
			t.Fatal(err)
		}
		failedCommand, err := digitaltwin.ParseMQTTCommand(failedTopic, failedPayload, false)
		if err != nil {
			t.Fatal(err)
		}
		resultBytes, err := json.Marshal(digitaltwin.CommandResult{
			ProtocolVersion: 1, MessageType: "command_result", CommandID: failedCommand.CommandID,
			GatewayID: failedCommand.GatewayID, EntityID: failedCommand.EntityID,
			DesiredVersion: failedCommand.DesiredVersion, Status: digitaltwin.CommandFailed,
			CompletedAt: time.Now().UTC().Truncate(time.Microsecond), ErrorCode: code,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repo.AcceptControlResult(f.ctx, "gateways/"+f.gw+"/responses/"+failedCommand.CommandID, resultBytes, time.Now()); err != nil {
			t.Fatalf("commit real %s failure result: %v", code, err)
		}
		query, snapshot = candidate(failedURN, failedEntity, 1, 0, "unconfigured", nil)
		outcome, err = repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
		if err != nil || outcome.Decision != digitaltwin.ReconciliationManualUnconfirmed || outcome.CommandID != "" {
			t.Fatalf("permanent %s result was retried automatically: outcome=%+v err=%v", code, outcome, err)
		}
	}

	unreadableURN, unreadableEntity := f.sensor(t, "r71_no_actual")
	unreadableReceipt, err := repo.Admit(f.ctx, f.owner, unreadableURN, controlRequest(t, "0.5", "0", "r71-no-actual"))
	if err != nil || unreadableReceipt.CommandID == nil {
		t.Fatalf("create unreadable-sampler intent: receipt=%+v err=%v", unreadableReceipt, err)
	}
	f.exec(t, `UPDATE twin_commands SET issued_at=clock_timestamp()-interval '2 hours',
		expires_at=clock_timestamp()-interval '1 hour' WHERE command_id=$1`, *unreadableReceipt.CommandID)
	query, snapshot = candidate(unreadableURN, unreadableEntity, 1, 0, "configured", pointerInt64(2_000_000))
	snapshot.ActualSampleIntervalUS = nil
	outcome, err = repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
	if err != nil || outcome.Decision != digitaltwin.ReconciliationManualUnconfirmed || outcome.CommandID != "" {
		t.Fatalf("configured snapshot without actual readback triggered automatic apply: outcome=%+v err=%v", outcome, err)
	}

	overflowURN, overflowEntity := f.sensor(t, "r71_overflow")
	overflowReceipt, err := repo.Admit(f.ctx, f.owner, overflowURN, controlRequest(t, "0.5", "0", "r71-overflow"))
	if err != nil || overflowReceipt.CommandID == nil {
		t.Fatalf("create overflow intent: receipt=%+v err=%v", overflowReceipt, err)
	}
	maxVersion := int64(^uint64(0) >> 1)
	f.exec(t, `UPDATE twin_states SET desired_version=$2 WHERE entity_id=$1`, overflowEntity, maxVersion)
	f.exec(t, `UPDATE twin_commands SET desired_version=$2,issued_at=clock_timestamp()-interval '2 hours',
		expires_at=clock_timestamp()-interval '1 hour' WHERE command_id=$1`, *overflowReceipt.CommandID, maxVersion)
	overflowActual := int64(2_000_000)
	query, snapshot = candidate(overflowURN, overflowEntity, digitaltwin.Version(maxVersion), 0, "unconfigured", &overflowActual)
	if _, err := repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second)); !errors.Is(err, digitaltwin.ErrDesiredVersionOverflow) {
		t.Fatalf("successor version overflow was not rejected: %v", err)
	}

	rollbackURN, rollbackEntity := f.sensor(t, "r71_rollback")
	rollbackReceipt, err := repo.Admit(f.ctx, f.owner, rollbackURN, controlRequest(t, "0.5", "0", "r71-rollback"))
	if err != nil || rollbackReceipt.CommandID == nil {
		t.Fatalf("create rollback intent: receipt=%+v err=%v", rollbackReceipt, err)
	}
	f.exec(t, `UPDATE twin_commands SET issued_at=clock_timestamp()-interval '2 hours',
		expires_at=clock_timestamp()-interval '1 hour' WHERE command_id=$1`, *rollbackReceipt.CommandID)
	faulty, err := digitaltwin.NewPostgresControlRepository(&faultInjectingTxDatabase{
		underlying: f.app,
		onTxBegin: func(tx pgx.Tx) pgx.Tx {
			return &faultInjectingTx{Tx: tx, onExec: func(_ context.Context, _ pgx.Tx, query string, _ ...any) (pgconn.CommandTag, error, bool) {
				if strings.Contains(query, "INSERT INTO public.twin_outbox") {
					return pgconn.CommandTag{}, errors.New("injected successor outbox failure"), true
				}
				return pgconn.CommandTag{}, nil, false
			}}
		},
	}, config)
	if err != nil {
		t.Fatal(err)
	}
	rollbackActual := int64(2_000_000)
	query, snapshot = candidate(rollbackURN, rollbackEntity, 1, 0, "unconfigured", &rollbackActual)
	if _, err := faulty.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second)); err == nil {
		t.Fatal("injected outbox failure committed a partial successor")
	}
	var rollbackVersion int64
	var rollbackBudget int
	var rollbackCommandCount int
	var rollbackStatus string
	if err := f.admin.QueryRow(f.ctx, `SELECT st.desired_version,c.auto_successors_used,c.status,
		(SELECT count(*) FROM twin_commands WHERE entity_id=st.entity_id)
		FROM twin_states st JOIN twin_commands c ON c.entity_id=st.entity_id
		WHERE st.entity_id=$1 AND c.command_id=$2`, rollbackEntity, *rollbackReceipt.CommandID).
		Scan(&rollbackVersion, &rollbackBudget, &rollbackStatus, &rollbackCommandCount); err != nil {
		t.Fatal(err)
	}
	if rollbackVersion != 1 || rollbackBudget != 0 || rollbackStatus != string(digitaltwin.CommandPending) || rollbackCommandCount != 1 {
		t.Fatalf("failed successor transaction partially committed: version=%d budget=%d status=%s commands=%d",
			rollbackVersion, rollbackBudget, rollbackStatus, rollbackCommandCount)
	}

	uncertainURN, uncertainEntity := f.sensor(t, "r71_uncertain")
	uncertainReceipt, err := repo.Admit(f.ctx, f.owner, uncertainURN, controlRequest(t, "0.5", "0", "r71-uncertain"))
	if err != nil || uncertainReceipt.CommandID == nil {
		t.Fatalf("create uncertain-commit intent: receipt=%+v err=%v", uncertainReceipt, err)
	}
	f.exec(t, `UPDATE twin_commands SET issued_at=clock_timestamp()-interval '2 hours',
		expires_at=clock_timestamp()-interval '1 hour' WHERE command_id=$1`, *uncertainReceipt.CommandID)
	uncertainNotifications := 0
	uncertainTimeline, err := metrics.NewControlTimeline(8, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	uncertainRepo, err := digitaltwin.NewPostgresControlRepository(&faultInjectingTxDatabase{
		underlying: f.app,
		onTxBegin:  func(tx pgx.Tx) pgx.Tx { return &controlCommitTx{Tx: tx, after: true} },
	}, config)
	if err != nil {
		t.Fatal(err)
	}
	uncertainRepo.SetAdmissionNotifier(func() { uncertainNotifications++ })
	uncertainRepo.SetControlTimeline(uncertainTimeline)
	uncertainActual := int64(2_000_000)
	query, snapshot = candidate(uncertainURN, uncertainEntity, 1, 0, "unconfigured", &uncertainActual)
	if _, err := uncertainRepo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second)); err == nil ||
		!errors.Is(err, digitaltwin.ErrControlCommitUncertain) || !errors.Is(err, digitaltwin.ErrDependencyUnavailable) {
		t.Fatalf("ambiguous successor commit was not reported as uncertain: %v", err)
	}
	if uncertainNotifications != 0 {
		t.Fatalf("ambiguous commit notified dispatcher %d times", uncertainNotifications)
	}
	if timing := uncertainTimeline.Snapshot(); timing.MissingOrigin != 0 {
		t.Fatalf("ambiguous commit recorded unconfirmed system timeline origin: %+v", timing)
	}
	var uncertainBudget, uncertainCommands int
	var uncertainVersion int64
	if err := f.admin.QueryRow(f.ctx, `SELECT st.desired_version,c.auto_successors_used,
		(SELECT count(*) FROM twin_commands WHERE entity_id=st.entity_id)
		FROM twin_states st JOIN twin_commands c ON c.entity_id=st.entity_id
		WHERE st.entity_id=$1 AND c.command_id=$2`, uncertainEntity, *uncertainReceipt.CommandID).
		Scan(&uncertainVersion, &uncertainBudget, &uncertainCommands); err != nil {
		t.Fatal(err)
	}
	if uncertainVersion != 2 || uncertainBudget != 1 || uncertainCommands != 2 {
		t.Fatalf("uncertain commit did not persist one atomic successor: version=%d budget=%d commands=%d",
			uncertainVersion, uncertainBudget, uncertainCommands)
	}
}

func pointerInt64(value int64) *int64 { return &value }

func TestNativeControlReconciliationBudgetRequiresFreshUserRoot(t *testing.T) {
	if os.Getenv("CONTROL_TEST_ISOLATED") != "1" {
		t.Skip("requires owned isolated PostgreSQL fixture")
	}
	f := newControlFixture(t)
	config := digitaltwin.DefaultControlConfig()
	config.Stage6Enabled = true
	repo, err := digitaltwin.NewPostgresControlRepository(f.app, config)
	if err != nil {
		t.Fatal(err)
	}
	f.repo = repo

	rootKey := "r71-budget-reassert-root"
	rootReceipt, err := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.5", "0", rootKey))
	if err != nil || rootReceipt.StatusCode != 202 || rootReceipt.CommandID == nil {
		t.Fatalf("create root intent: receipt=%+v err=%v", rootReceipt, err)
	}
	f.exec(t, `UPDATE twin_commands SET issued_at=clock_timestamp()-interval '2 hours',
		expires_at=clock_timestamp()-interval '1 hour' WHERE command_id=$1`, *rootReceipt.CommandID)
	query := digitaltwin.ControlQueryContext{
		EntityID: f.entity, EntityURN: f.urn, GatewayID: f.gw, SensorID: "s1", DesiredVersion: 1,
	}
	actual := int64(2_000_000)
	snapshot := digitaltwin.ConfigurationSnapshot{
		ProtocolVersion: 1, MessageType: "configuration_snapshot", RequestID: uuid.NewString(),
		GatewayID: f.gw, EntityID: f.urn, WatermarkVersion: 0, ConfigurationState: "unconfigured",
		ActualSampleIntervalUS: &actual, ReadbackAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	successor, err := repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
	if err != nil || successor.Decision != digitaltwin.ReconciliationSuccessorCreated || successor.DesiredVersion != 2 {
		t.Fatalf("create budgeted successor: outcome=%+v err=%v", successor, err)
	}
	var topic string
	var payload []byte
	if err := f.admin.QueryRow(f.ctx, `SELECT topic,payload_bytes FROM twin_outbox WHERE command_id=$1`, successor.CommandID).
		Scan(&topic, &payload); err != nil {
		t.Fatal(err)
	}
	command, err := digitaltwin.ParseMQTTCommand(topic, payload, false)
	if err != nil {
		t.Fatal(err)
	}
	resultBytes, err := json.Marshal(resultEnvelope(command))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptControlResult(f.ctx, "gateways/"+f.gw+"/responses/"+command.CommandID, resultBytes, time.Now()); err != nil {
		t.Fatal("commit successor result:", err)
	}

	// Contract precedence deliberately reports budget before convergence. The
	// user must open a new root even when the prior successor actually succeeded.
	confirmed := int64(2_000_000)
	query.DesiredVersion = 2
	snapshot.RequestID = uuid.NewString()
	snapshot.WatermarkVersion = 2
	snapshot.ConfigurationState = "configured"
	snapshot.DurableSampleIntervalUS = &confirmed
	snapshot.ActualSampleIntervalUS = &confirmed
	blocked, err := repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
	if err != nil || blocked.Decision != digitaltwin.ReconciliationBlockedBudget || blocked.CommandID != "" {
		t.Fatalf("spent root budget decision changed precedence: outcome=%+v err=%v", blocked, err)
	}

	newKey := "r71-budget-reassert-new-root"
	reassert, err := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.5", "2", newKey))
	if err != nil || reassert.StatusCode != 202 || reassert.CommandID == nil ||
		*reassert.CommandID == successor.CommandID {
		t.Fatalf("same-value PATCH did not create fresh root after exhausted budget: receipt=%+v err=%v", reassert, err)
	}
	var origin string
	var newRoot, oldRoot uuid.UUID
	var budget int
	if err := f.admin.QueryRow(f.ctx, `SELECT origin,root_command_id,auto_successors_used
		FROM twin_commands WHERE command_id=$1`, *reassert.CommandID).Scan(&origin, &newRoot, &budget); err != nil {
		t.Fatal(err)
	}
	if origin != "client" || newRoot.String() != *reassert.CommandID || budget != 0 {
		t.Fatalf("fresh PATCH did not start an independent root: origin=%s root=%s budget=%d", origin, newRoot, budget)
	}
	newRootView, err := repo.GetCommand(f.ctx, f.owner, f.urn, *reassert.CommandID)
	if err != nil {
		t.Fatal("read fresh root projection:", err)
	}
	if newRootView.CurrentReconciliation != nil {
		t.Fatalf("fresh root inherited previous desired version's reconciliation decision: %+v",
			newRootView.CurrentReconciliation)
	}
	newRootPage, err := repo.ListCommands(f.ctx, f.owner, f.urn, 10, "")
	if err != nil {
		t.Fatal("list fresh root projection:", err)
	}
	for _, listed := range newRootPage.Commands {
		if listed.CommandID == *reassert.CommandID && listed.CurrentReconciliation != nil {
			t.Fatalf("command list attributed previous decision to fresh root: %+v", listed.CurrentReconciliation)
		}
	}
	if err := f.admin.QueryRow(f.ctx, `SELECT auto_successors_used FROM twin_commands WHERE command_id=$1`,
		*rootReceipt.CommandID).Scan(&budget); err != nil || budget != 1 {
		t.Fatalf("fresh PATCH reset historical root budget: budget=%d err=%v", budget, err)
	}
	oldReplay, err := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.5", "0", rootKey))
	if err != nil || oldReplay.StatusCode != rootReceipt.StatusCode || oldReplay.CommandID == nil ||
		*oldReplay.CommandID != *rootReceipt.CommandID || !bytes.Equal(oldReplay.Response, rootReceipt.Response) {
		t.Fatalf("old idempotency replay changed after fresh root: original=%+v replay=%+v err=%v",
			rootReceipt, oldReplay, err)
	}
	if err := f.admin.QueryRow(f.ctx, `SELECT root_command_id FROM twin_commands WHERE command_id=$1`,
		*rootReceipt.CommandID).Scan(&oldRoot); err != nil || oldRoot.String() != *rootReceipt.CommandID {
		t.Fatalf("old root lineage was modified: root=%s err=%v", oldRoot, err)
	}
	if err := f.admin.QueryRow(f.ctx, `SELECT topic,payload_bytes FROM twin_outbox WHERE command_id=$1`,
		*reassert.CommandID).Scan(&topic, &payload); err != nil {
		t.Fatal(err)
	}
	command, err = digitaltwin.ParseMQTTCommand(topic, payload, false)
	if err != nil {
		t.Fatal(err)
	}
	resultBytes, err = json.Marshal(resultEnvelope(command))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AcceptControlResult(f.ctx, "gateways/"+f.gw+"/responses/"+command.CommandID, resultBytes, time.Now()); err != nil {
		t.Fatal("commit fresh-root result:", err)
	}
	healthyNoop, err := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.5", "3", "r71-fresh-root-noop"))
	if err != nil || healthyNoop.StatusCode != 200 || healthyNoop.CommandID != nil {
		t.Fatalf("historical spent root affected fresh healthy no-op: receipt=%+v err=%v", healthyNoop, err)
	}
}

func TestNativeControlReconciliationLateFailureAndMissingActualGuards(t *testing.T) {
	if os.Getenv("CONTROL_TEST_ISOLATED") != "1" {
		t.Skip("requires owned isolated PostgreSQL fixture")
	}

	newRepository := func(t *testing.T) (*controlFixture, *digitaltwin.PostgresControlRepository) {
		t.Helper()
		f := newControlFixture(t)
		config := digitaltwin.DefaultControlConfig()
		config.Stage6Enabled = true
		config.TTL = time.Second
		repo, err := digitaltwin.NewPostgresControlRepository(f.app, config)
		if err != nil {
			t.Fatal(err)
		}
		f.repo = repo
		return f, repo
	}
	admitIntent := func(t *testing.T, f *controlFixture, repo *digitaltwin.PostgresControlRepository, key string) digitaltwin.Command {
		t.Helper()
		receipt, err := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.5", "0", key))
		if err != nil || receipt.StatusCode != 202 || receipt.CommandID == nil {
			t.Fatalf("admit recoverable intent: receipt=%+v err=%v", receipt, err)
		}
		var topic string
		var payload []byte
		if err := f.admin.QueryRow(f.ctx, `SELECT topic,payload_bytes FROM twin_outbox WHERE command_id=$1`,
			*receipt.CommandID).Scan(&topic, &payload); err != nil {
			t.Fatal(err)
		}
		command, err := digitaltwin.ParseMQTTCommand(topic, payload, false)
		if err != nil {
			t.Fatal(err)
		}
		return command
	}
	expireIntent := func(t *testing.T, f *controlFixture, repo *digitaltwin.PostgresControlRepository, key string) digitaltwin.Command {
		t.Helper()
		command := admitIntent(t, f, repo, key)
		time.Sleep(time.Until(command.ExpiresAt) + 20*time.Millisecond)
		return command
	}
	snapshotFor := func(f *controlFixture, actual *int64) (digitaltwin.ControlQueryContext, digitaltwin.ConfigurationSnapshot) {
		return digitaltwin.ControlQueryContext{
			EntityID: f.entity, EntityURN: f.urn, GatewayID: f.gw, SensorID: "s1", DesiredVersion: 1,
		}, digitaltwin.ConfigurationSnapshot{
			ProtocolVersion: 1, MessageType: "configuration_snapshot", RequestID: uuid.NewString(),
			GatewayID: f.gw, EntityID: f.urn, WatermarkVersion: 0, ConfigurationState: "unconfigured",
			ActualSampleIntervalUS: actual, ReadbackAt: time.Now().UTC().Truncate(time.Microsecond),
		}
	}

	t.Run("pre-expiry arrival processed after TTL remains recoverable", func(t *testing.T) {
		f, repo := newRepository(t)
		command := admitIntent(t, f, repo, "pre-expiry-arrival-after-ttl-processing")
		arrivalAt := time.Now().UTC().Truncate(time.Microsecond)
		if !arrivalAt.Before(command.ExpiresAt) {
			t.Fatalf("test arrival timestamp is not before expiry: arrival=%s expiry=%s", arrivalAt, command.ExpiresAt)
		}
		result := resultEnvelope(command)
		result.CompletedAt = arrivalAt
		resultBytes, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Until(command.ExpiresAt) + 20*time.Millisecond)
		outcome, err := repo.AcceptControlResult(f.ctx,
			"gateways/"+f.gw+"/responses/"+command.CommandID, resultBytes, arrivalAt)
		if err != nil || outcome != digitaltwin.ControlResultLate {
			t.Fatalf("process queued pre-expiry success after timeout: outcome=%q err=%v", outcome, err)
		}
		var status, errorCode string
		var lateBytes, resultStored []byte
		var persistedArrival time.Time
		if err := f.admin.QueryRow(f.ctx, `SELECT status,error_code,late_result_bytes,result_bytes,late_result_received_at
			FROM twin_commands WHERE command_id=$1`, command.CommandID).
			Scan(&status, &errorCode, &lateBytes, &resultStored, &persistedArrival); err != nil {
			t.Fatal(err)
		}
		if status != string(digitaltwin.CommandTimeout) || errorCode != "COMMAND_EXPIRED" ||
			!bytes.Equal(lateBytes, resultBytes) || resultStored != nil ||
			!persistedArrival.Equal(arrivalAt) || !persistedArrival.Before(command.ExpiresAt) {
			t.Fatalf("processing-time timeout or original arrival evidence changed: status=%s error=%s arrival=%s expiry=%s",
				status, errorCode, persistedArrival, command.ExpiresAt)
		}
		var reportedBefore, reportedAfter []byte
		if err := f.admin.QueryRow(f.ctx, `SELECT reported_state FROM twin_states WHERE entity_id=$1`, f.entity).
			Scan(&reportedBefore); err != nil {
			t.Fatal(err)
		}
		query, snapshot := snapshotFor(f, pointerInt64(500_000))
		snapshot.ConfigurationState = "configured"
		snapshot.DurableSampleIntervalUS = pointerInt64(500_000)
		snapshot.ActualSampleIntervalUS = pointerInt64(500_000)
		decision, err := repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
		if err != nil || decision.Decision != digitaltwin.ReconciliationSuccessorCreated ||
			decision.CommandID == "" || decision.DesiredVersion != 2 {
			t.Fatalf("valid queued success forced manual or failed to recover: outcome=%+v err=%v", decision, err)
		}
		var budget int
		var desiredVersion, commandCount int64
		if err := f.admin.QueryRow(f.ctx, `SELECT s.reported_state,s.desired_version,c.auto_successors_used,
			(SELECT count(*) FROM twin_commands WHERE entity_id=s.entity_id)
			FROM twin_states s JOIN twin_commands c ON c.entity_id=s.entity_id
			WHERE s.entity_id=$1 AND c.command_id=$2`, f.entity, command.CommandID).
			Scan(&reportedAfter, &desiredVersion, &budget, &commandCount); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(reportedBefore, reportedAfter) || desiredVersion != 2 || budget != 1 || commandCount != 2 {
			t.Fatalf("late success wrote configuration or failed to spend one successor: changed=%t desired_version=%d budget=%d commands=%d",
				!bytes.Equal(reportedBefore, reportedAfter), desiredVersion, budget, commandCount)
		}
	})

	t.Run("permanent failure received after TTL stays manual", func(t *testing.T) {
		f, repo := newRepository(t)
		command := expireIntent(t, f, repo, "late-permanent-failure")
		resultBytes, err := json.Marshal(digitaltwin.CommandResult{
			ProtocolVersion: 1, MessageType: "command_result", CommandID: command.CommandID,
			GatewayID: command.GatewayID, EntityID: command.EntityID, DesiredVersion: command.DesiredVersion,
			Status: digitaltwin.CommandFailed, CompletedAt: time.Now().UTC().Truncate(time.Microsecond),
			ErrorCode: "UNSUPPORTED_CONFIGURATION",
		})
		if err != nil {
			t.Fatal(err)
		}
		outcome, err := repo.AcceptControlResult(f.ctx,
			"gateways/"+f.gw+"/responses/"+command.CommandID, resultBytes, time.Now())
		if err != nil || outcome != digitaltwin.ControlResultLate {
			t.Fatalf("accept late permanent result: outcome=%q err=%v", outcome, err)
		}
		var status, storedError string
		var lateBytes, resultStored []byte
		var budget int
		var desiredVersion, commandCount int64
		var reportedBefore []byte
		if err := f.admin.QueryRow(f.ctx, `SELECT status,error_code,late_result_bytes,result_bytes,auto_successors_used
			FROM twin_commands WHERE command_id=$1`, command.CommandID).
			Scan(&status, &storedError, &lateBytes, &resultStored, &budget); err != nil {
			t.Fatal(err)
		}
		if status != string(digitaltwin.CommandTimeout) || storedError != "COMMAND_EXPIRED" ||
			!bytes.Equal(lateBytes, resultBytes) || resultStored != nil || budget != 0 {
			t.Fatalf("late evidence did not preserve terminal root: status=%s error=%s late=%s result=%s budget=%d",
				status, storedError, lateBytes, resultStored, budget)
		}
		if err := f.admin.QueryRow(f.ctx, `SELECT reported_state FROM twin_states WHERE entity_id=$1`, f.entity).
			Scan(&reportedBefore); err != nil {
			t.Fatal(err)
		}
		actual := int64(2_000_000)
		query, snapshot := snapshotFor(f, &actual)
		decision, err := repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
		if err != nil || decision.Decision != digitaltwin.ReconciliationManualUnconfirmed || decision.CommandID != "" {
			t.Fatalf("permanent late failure triggered %s command=%s version=%d; want manual_unconfirmed: err=%v",
				decision.Decision, decision.CommandID, decision.DesiredVersion, err)
		}
		var reportedAfter []byte
		if err := f.admin.QueryRow(f.ctx, `SELECT s.reported_state,s.desired_version,
			(SELECT count(*) FROM twin_commands WHERE entity_id=s.entity_id)
			FROM twin_states s WHERE s.entity_id=$1`, f.entity).
			Scan(&reportedAfter, &desiredVersion, &commandCount); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(reportedBefore, reportedAfter) || desiredVersion != 1 || commandCount != 1 {
			t.Fatalf("manual decision mutated configuration/intent: desired_version=%d commands=%d reported_changed=%t",
				desiredVersion, commandCount, !bytes.Equal(reportedBefore, reportedAfter))
		}
	})

	t.Run("permanent failure received before TTL but processed after stays manual", func(t *testing.T) {
		f, repo := newRepository(t)
		command := admitIntent(t, f, repo, "pre-expiry-arrival-late-permanent-failure")
		arrivalAt := time.Now().UTC().Truncate(time.Microsecond)
		if !arrivalAt.Before(command.ExpiresAt) {
			t.Fatalf("test arrival timestamp is not before expiry: arrival=%s expiry=%s", arrivalAt, command.ExpiresAt)
		}
		resultBytes, err := json.Marshal(digitaltwin.CommandResult{
			ProtocolVersion: 1, MessageType: "command_result", CommandID: command.CommandID,
			GatewayID: command.GatewayID, EntityID: command.EntityID, DesiredVersion: command.DesiredVersion,
			Status: digitaltwin.CommandFailed, CompletedAt: arrivalAt, ErrorCode: "UNSUPPORTED_CONFIGURATION",
		})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Until(command.ExpiresAt) + 20*time.Millisecond)
		outcome, err := repo.AcceptControlResult(f.ctx,
			"gateways/"+f.gw+"/responses/"+command.CommandID, resultBytes, arrivalAt)
		if err != nil || outcome != digitaltwin.ControlResultLate {
			t.Fatalf("process queued pre-expiry permanent failure after timeout: outcome=%q err=%v", outcome, err)
		}
		actual := int64(2_000_000)
		query, snapshot := snapshotFor(f, &actual)
		decision, err := repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
		if err != nil || decision.Decision != digitaltwin.ReconciliationManualUnconfirmed || decision.CommandID != "" {
			t.Fatalf("pre-expiry permanent failure bypassed manual guard: outcome=%+v err=%v", decision, err)
		}
		var status, storedError string
		var lateBytes []byte
		var desiredVersion, commandCount int64
		var budget int
		if err := f.admin.QueryRow(f.ctx, `SELECT c.status,c.error_code,c.late_result_bytes,c.auto_successors_used,
			s.desired_version,(SELECT count(*) FROM twin_commands WHERE entity_id=s.entity_id)
			FROM twin_commands c JOIN twin_states s USING(entity_id)
			WHERE c.command_id=$1`, command.CommandID).
			Scan(&status, &storedError, &lateBytes, &budget, &desiredVersion, &commandCount); err != nil {
			t.Fatal(err)
		}
		if status != string(digitaltwin.CommandTimeout) || storedError != "COMMAND_EXPIRED" ||
			!bytes.Equal(lateBytes, resultBytes) || budget != 0 || desiredVersion != 1 || commandCount != 1 {
			t.Fatalf("pre-expiry permanent failure changed terminal root: status=%s error=%s late_match=%t budget=%d desired=%d commands=%d",
				status, storedError, bytes.Equal(lateBytes, resultBytes), budget, desiredVersion, commandCount)
		}
	})

	t.Run("late success remains recoverable but unconfirmed", func(t *testing.T) {
		f, repo := newRepository(t)
		command := expireIntent(t, f, repo, "late-success-remains-recoverable")
		result := resultEnvelope(command)
		result.CompletedAt = command.ExpiresAt
		resultBytes, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		outcome, err := repo.AcceptControlResult(f.ctx,
			"gateways/"+f.gw+"/responses/"+command.CommandID, resultBytes, time.Now())
		if err != nil || outcome != digitaltwin.ControlResultLate {
			t.Fatalf("accept late successful result: outcome=%q err=%v", outcome, err)
		}
		var reportedBefore []byte
		if err := f.admin.QueryRow(f.ctx, `SELECT reported_state FROM twin_states WHERE entity_id=$1`, f.entity).
			Scan(&reportedBefore); err != nil {
			t.Fatal(err)
		}
		actual := int64(2_000_000)
		query, snapshot := snapshotFor(f, &actual)
		decision, err := repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
		if err != nil || decision.Decision != digitaltwin.ReconciliationSuccessorCreated ||
			decision.CommandID == "" || decision.DesiredVersion != 2 {
			t.Fatalf("valid late success blocked eligible lost-result recovery: outcome=%+v err=%v", decision, err)
		}
		var status string
		var lateBytes, resultStored, reportedAfter []byte
		var budget int
		var desiredVersion int64
		if err := f.admin.QueryRow(f.ctx, `SELECT c.status,c.late_result_bytes,c.result_bytes,c.auto_successors_used,
			s.desired_version,s.reported_state FROM twin_commands c JOIN twin_states s USING(entity_id)
			WHERE c.command_id=$1`, command.CommandID).
			Scan(&status, &lateBytes, &resultStored, &budget, &desiredVersion, &reportedAfter); err != nil {
			t.Fatal(err)
		}
		if status != string(digitaltwin.CommandTimeout) || !bytes.Equal(lateBytes, resultBytes) ||
			resultStored != nil || budget != 1 || desiredVersion != 2 ||
			!bytes.Equal(reportedBefore, reportedAfter) {
			t.Fatalf("late success changed root/configuration unexpectedly: status=%s late_match=%t result=%s budget=%d desired_version=%d",
				status, bytes.Equal(lateBytes, resultBytes), resultStored, budget, desiredVersion)
		}
	})

	t.Run("unconfigured without actual readback stays manual", func(t *testing.T) {
		f, repo := newRepository(t)
		command := expireIntent(t, f, repo, "unconfigured-missing-actual")
		var reportedBefore []byte
		if err := f.admin.QueryRow(f.ctx, `SELECT reported_state FROM twin_states WHERE entity_id=$1`, f.entity).
			Scan(&reportedBefore); err != nil {
			t.Fatal(err)
		}
		query, snapshot := snapshotFor(f, nil)
		decision, err := repo.ReconcileSnapshot(f.ctx, query, snapshot, time.Now().Add(5*time.Second))
		if err != nil || decision.Decision != digitaltwin.ReconciliationManualUnconfirmed || decision.CommandID != "" {
			t.Fatalf("unconfigured snapshot without actual readback triggered %s command=%s version=%d; want manual_unconfirmed: err=%v",
				decision.Decision, decision.CommandID, decision.DesiredVersion, err)
		}
		var status, errorCode string
		var budget int
		var desiredVersion, commandCount int64
		var reportedAfter []byte
		if err := f.admin.QueryRow(f.ctx, `SELECT c.status,c.error_code,c.auto_successors_used,
			s.desired_version,s.reported_state,
			(SELECT count(*) FROM twin_commands WHERE entity_id=s.entity_id)
			FROM twin_commands c JOIN twin_states s USING(entity_id) WHERE c.command_id=$1`, command.CommandID).
			Scan(&status, &errorCode, &budget, &desiredVersion, &reportedAfter, &commandCount); err != nil {
			t.Fatal(err)
		}
		if status != string(digitaltwin.CommandTimeout) || errorCode != "COMMAND_EXPIRED" || budget != 0 ||
			desiredVersion != 1 || commandCount != 1 || !bytes.Equal(reportedBefore, reportedAfter) {
			t.Fatalf("manual decision mutated timeout/budget/configuration: status=%s error=%s budget=%d desired_version=%d commands=%d",
				status, errorCode, budget, desiredVersion, commandCount)
		}
	})
}

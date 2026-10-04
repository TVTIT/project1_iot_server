package mqttcredential

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresMaintenanceIntegration(t *testing.T) {
	if os.Getenv("TASK263A_APP_DSN") == "" {
		t.Skip("isolated PostgreSQL fixture required")
	}
	ctx := context.Background()
	app, err := pgxpool.New(ctx, os.Getenv("TASK263A_APP_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	admin, err := pgxpool.New(ctx, os.Getenv("TASK263A_ADMIN_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	r, _ := NewPostgresRepository(app, 3*time.Second, 1)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, e := admin.Exec(ctx, sql, args...); e != nil {
			t.Fatal(e)
		}
	}
	actor, g := uuid.New(), "test_"+uuid.New().String()
	exec(`INSERT INTO profiles(id) VALUES($1)`, actor)
	exec(`INSERT INTO platform_admins(user_id) VALUES($1)`, actor)
	exec(`INSERT INTO gateways(gateway_id,name) VALUES($1,'Maintenance fixture')`, g)
	q := BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionProvision}}
	b, err := r.BeginOperation(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := r.ResolveMaintenance(ctx, q.OperationID)
	if err != nil || cp.Status != MaintenanceInProgress || cp.BrokerEpoch != nil {
		t.Fatal("atomic unbound checkpoint", err)
	}
	guard := MaintenanceGuard{OperationGuard{g, q.OperationID, OperationPending, CredentialProvisioning, 1}, MaintenanceInProgress, nil}
	epoch, _ := freshIdentity()
	cp, err = r.BindMaintenanceEpoch(ctx, guard, epoch)
	if err != nil || cp.BrokerEpoch == nil {
		t.Fatal(err)
	}
	guard.ExpectedEpoch = &epoch
	c, err := CompleteOperation(b.Operation, ExecutionVerifiedSuccess, VerificationEvidence{true, true, true}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	m, err := r.ConditionalFinalize(ctx, OperationUpdate{guard.OperationGuard, c})
	if err != nil {
		t.Fatal(err)
	}
	guard.ExpectedOperationStatus, guard.ExpectedCredentialStatus = OperationSucceeded, m.Status
	cp, err = r.ResolveMaintenance(ctx, q.OperationID)
	if err != nil || cp.Status != MaintenanceInProgress {
		t.Fatal("finalize erased unfinished maintenance")
	}
	checkAdmission := func(code ErrorCode) {
		t.Helper()
		before, e := r.GetMetadata(ctx, g)
		if e != nil {
			t.Fatal(e)
		}
		var countBefore int
		if e = admin.QueryRow(ctx, `SELECT count(*) FROM gateway_mqtt_credential_events WHERE gateway_id=$1`, g).Scan(&countBefore); e != nil {
			t.Fatal(e)
		}
		for _, action := range []Action{ActionRotate, ActionProvision, ActionRevoke} {
			_, e = r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), action}})
			requireCode(t, e, code)
		}
		after, e := r.GetMetadata(ctx, g)
		if e != nil || after.LastOperationID != before.LastOperationID || after.CredentialVersion != before.CredentialVersion || after.Status != before.Status {
			t.Fatal("denied admission changed metadata", e)
		}
		var countAfter int
		if e = admin.QueryRow(ctx, `SELECT count(*) FROM gateway_mqtt_credential_events WHERE gateway_id=$1`, g).Scan(&countAfter); e != nil || countAfter != countBefore {
			t.Fatal("denied admission inserted event", e)
		}
		replay, e := r.BeginOperation(ctx, q)
		if e != nil || replay.Execute || replay.Replay == nil {
			t.Fatal("terminal replay must stay metadata-only", e)
		}
	}
	checkAdmission(CodeOperationInProgress)
	cp, err = r.RequireMaintenanceRecovery(ctx, guard, CodeRecoveryRequired)
	if err != nil || cp.Status != MaintenanceRecoveryNeeded {
		t.Fatal(err)
	}
	checkAdmission(CodeRecoveryRequired)
	o, err := r.ResolveCommitAmbiguity(ctx, q.OperationID)
	if err != nil || o.Status != OperationSucceeded {
		t.Fatal("terminal event edited")
	}
	guard.ExpectedStatus = MaintenanceRecoveryNeeded
	stale := guard
	wrong, _ := freshIdentity()
	stale.ExpectedEpoch = &wrong
	_, err = r.CompleteMaintenance(ctx, stale)
	requireCode(t, err, CodeCredentialConflict)
	_, err = r.ListPendingMaintenance(ctx, uuid.Nil, 2)
	requireCode(t, err, CodeInvalidRequest)
	found := false
	cursor := uuid.Nil
	for {
		page, e := r.ListPendingMaintenance(ctx, cursor, 1)
		if e != nil || page == nil || len(page) > 1 {
			t.Fatal("bounded scan", e)
		}
		if len(page) == 0 {
			break
		}
		cursor = page[0].OperationID
		found = found || cursor == q.OperationID
	}
	if !found {
		t.Fatal("succeeded maintenance omitted")
	}
	cp, err = r.CompleteMaintenance(ctx, guard)
	if err != nil || cp.Status != MaintenanceCompleted {
		t.Fatal(err)
	}
	_, err = r.RequireMaintenanceRecovery(ctx, guard, CodeRecoveryRequired)
	requireCode(t, err, CodeCredentialConflict)
	if _, err := app.Exec(ctx, `DELETE FROM mqtt_credential_maintenance WHERE operation_id=$1`, q.OperationID); err == nil {
		t.Fatal("audit deletion allowed")
	}
	if _, err := admin.Exec(ctx, `UPDATE mqtt_credential_maintenance SET status='in_progress',completed_at=NULL WHERE operation_id=$1`, q.OperationID); err == nil {
		t.Fatal("completed checkpoint mutable")
	}
	replay, err := r.BeginOperation(ctx, q)
	if err != nil || replay.Execute {
		t.Fatal("replay", err)
	}
	// Absence on an independent checkout remains uncertainty, not rollback proof.
	_, err = r.ResolveMaintenance(ctx, uuid.New())
	requireCode(t, err, CodeFinalizationPending)
	t.Run("intent checkpoint failure rolls back all writes", func(t *testing.T) {
		g2 := "test_" + uuid.New().String()
		exec(`INSERT INTO gateways(gateway_id,name) VALUES($1,'Rollback fixture')`, g2)
		exec(`CREATE FUNCTION task266_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture failure'; END $$`)
		exec(`CREATE TRIGGER task266_reject BEFORE INSERT ON mqtt_credential_maintenance FOR EACH ROW EXECUTE FUNCTION task266_reject()`)
		q2 := BeginRequest{actor, uuid.New(), MutationInput{g2, uuid.New(), ActionProvision}}
		_, e := r.BeginOperation(ctx, q2)
		requireCode(t, e, CodeServiceUnavailable)
		exec(`DROP TRIGGER task266_reject ON mqtt_credential_maintenance; DROP FUNCTION task266_reject()`)
		var n int
		if e = admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM gateway_mqtt_credential_events WHERE operation_id=$1)+(SELECT count(*) FROM mqtt_credential_maintenance WHERE operation_id=$1)+(SELECT count(*) FROM gateway_mqtt_credentials WHERE gateway_id=$2)`, q2.OperationID, g2).Scan(&n); e != nil || n != 0 {
			t.Fatal("non-atomic intent", e)
		}
	})
	t.Run("later current operation and broker epoch fence stale completion", func(t *testing.T) {
		q2 := BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionRotate}}
		b2, e := r.BeginOperation(ctx, q2)
		if e != nil {
			t.Fatal(e)
		}
		g2 := MaintenanceGuard{OperationGuard{g, q2.OperationID, OperationPending, CredentialRotating, 2}, MaintenanceInProgress, nil}
		e1, _ := freshIdentity()
		e2, _ := freshIdentity()
		if _, e = r.BindMaintenanceEpoch(ctx, g2, e1); e != nil {
			t.Fatal(e)
		}
		g2.ExpectedEpoch = &e1
		if _, e = r.BindMaintenanceEpoch(ctx, g2, e2); e != nil {
			t.Fatal(e)
		}
		_, e = r.RequireMaintenanceRecovery(ctx, g2, CodeRecoveryRequired)
		requireCode(t, e, CodeCredentialConflict)
		g2.ExpectedEpoch = &e2
		c2, e := CompleteOperation(b2.Operation, ExecutionVerifiedSuccess, VerificationEvidence{true, true, true}, time.Now().UTC())
		if e != nil {
			t.Fatal(e)
		}
		if _, e = r.ConditionalFinalize(ctx, OperationUpdate{g2.OperationGuard, c2}); e != nil {
			t.Fatal(e)
		}
		g2.ExpectedOperationStatus = OperationSucceeded
		g2.ExpectedCredentialStatus = CredentialActive
		q3 := BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionRevoke}}
		_, e = r.BeginOperation(ctx, q3)
		requireCode(t, e, CodeOperationInProgress)
		p, e := r.ResolveMaintenance(ctx, q2.OperationID)
		if e != nil || p.Status != MaintenanceInProgress {
			t.Fatal("stale writer erased checkpoint")
		}
		if _, e = r.CompleteMaintenance(ctx, g2); e != nil {
			t.Fatal(e)
		}
		if _, e = r.BeginOperation(ctx, q3); e != nil {
			t.Fatal("completed checkpoint must permit admission", e)
		}
		_, e = r.CompleteMaintenance(ctx, g2)
		requireCode(t, e, CodeCredentialConflict)
	})
	t.Run("checkpoint commit ambiguity uses independent read", func(t *testing.T) {
		g2 := "test_" + uuid.New().String()
		exec(`INSERT INTO gateways(gateway_id,name) VALUES($1,'Commit fixture')`, g2)
		q2 := BeginRequest{actor, uuid.New(), MutationInput{g2, uuid.New(), ActionProvision}}
		_, e := r.BeginOperation(ctx, q2)
		if e != nil {
			t.Fatal(e)
		}
		guard2 := MaintenanceGuard{OperationGuard{g2, q2.OperationID, OperationPending, CredentialProvisioning, 1}, MaintenanceInProgress, nil}
		exec(`CREATE FUNCTION task266_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.3); RETURN NEW; END $$`)
		exec(`CREATE CONSTRAINT TRIGGER task266_delay AFTER UPDATE ON mqtt_credential_maintenance DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION task266_delay()`)
		defer exec(`DROP TRIGGER task266_delay ON mqtt_credential_maintenance; DROP FUNCTION task266_delay()`)
		short, _ := NewPostgresRepository(app, 100*time.Millisecond)
		epoch2, _ := freshIdentity()
		result, e := short.BindMaintenanceEpoch(ctx, guard2, epoch2)
		var unknown *CommitOutcomeUnknown
		if !errors.As(e, &unknown) || unknown.OperationID != q2.OperationID || result.OperationID != uuid.Nil {
			t.Fatal("unsafe commit outcome", e)
		}
		cp2, e := r.ResolveMaintenance(ctx, q2.OperationID)
		if e != nil || cp2.Status != MaintenanceInProgress {
			t.Fatal("independent lookup", e)
		}
		if cp2.BrokerEpoch != nil && *cp2.BrokerEpoch != epoch2 {
			t.Fatal("unexpected durable epoch")
		}
	})
	t.Run("completion versus new admission serializes and revoked no-op is fenced", func(t *testing.T) {
		g2 := "test_" + uuid.New().String()
		exec(`INSERT INTO gateways(gateway_id,name) VALUES($1,'Completion race')`, g2)
		q2 := BeginRequest{actor, uuid.New(), MutationInput{g2, uuid.New(), ActionProvision}}
		b2, e := r.BeginOperation(ctx, q2)
		if e != nil {
			t.Fatal(e)
		}
		epoch2, _ := freshIdentity()
		guard2 := MaintenanceGuard{operationGuard(b2.Operation, b2.Metadata), MaintenanceInProgress, nil}
		if _, e = r.BindMaintenanceEpoch(ctx, guard2, epoch2); e != nil {
			t.Fatal(e)
		}
		c2, e := CompleteOperation(b2.Operation, ExecutionVerifiedSuccess, VerificationEvidence{true, true, true}, time.Now().UTC())
		if e != nil {
			t.Fatal(e)
		}
		m2, e := r.ConditionalFinalize(ctx, OperationUpdate{guard2.OperationGuard, c2})
		if e != nil {
			t.Fatal(e)
		}
		guard2.OperationGuard = operationGuard(c2.Operation, m2)
		guard2.ExpectedEpoch = &epoch2
		q3 := BeginRequest{actor, uuid.New(), MutationInput{g2, uuid.New(), ActionRevoke}}
		start := make(chan struct{})
		var wg sync.WaitGroup
		var completionErr, admissionErr error
		var b3 BeginResult
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _, completionErr = r.CompleteMaintenance(ctx, guard2) }()
		go func() { defer wg.Done(); <-start; b3, admissionErr = r.BeginOperation(ctx, q3) }()
		close(start)
		wg.Wait()
		if completionErr != nil {
			t.Fatal("completion lost current operation", completionErr)
		}
		if admissionErr != nil {
			requireCode(t, admissionErr, CodeOperationInProgress)
			b3, e = r.BeginOperation(ctx, q3)
			if e != nil {
				t.Fatal(e)
			}
		}
		c3, e := CompleteOperation(b3.Operation, ExecutionVerifiedSuccess, VerificationEvidence{true, true, false}, time.Now().UTC())
		if e != nil {
			t.Fatal(e)
		}
		m3, e := r.ConditionalFinalize(ctx, OperationUpdate{operationGuard(b3.Operation, b3.Metadata), c3})
		if e != nil {
			t.Fatal(e)
		}
		q4 := BeginRequest{actor, uuid.New(), MutationInput{g2, uuid.New(), ActionRevoke}}
		_, e = r.BeginOperation(ctx, q4)
		requireCode(t, e, CodeOperationInProgress)
		replay, e := r.BeginOperation(ctx, q3)
		if e != nil || replay.Execute || replay.Replay == nil {
			t.Fatal("revoke replay", e)
		}
		g3 := MaintenanceGuard{operationGuard(c3.Operation, m3), MaintenanceInProgress, nil}
		if _, e = r.RequireMaintenanceRecovery(ctx, g3, CodeRecoveryRequired); e != nil {
			t.Fatal(e)
		}
		_, e = r.BeginOperation(ctx, q4)
		requireCode(t, e, CodeRecoveryRequired)
		g3.ExpectedStatus = MaintenanceRecoveryNeeded
		if _, e = r.BindMaintenanceEpoch(ctx, g3, epoch2); e != nil {
			t.Fatal(e)
		}
		g3.ExpectedEpoch = &epoch2
		if _, e = r.CompleteMaintenance(ctx, g3); e != nil {
			t.Fatal(e)
		}
		noop, e := r.BeginOperation(ctx, q4)
		if e != nil || noop.Execute || noop.Replay == nil {
			t.Fatal("completed revoke no-op", e)
		}
	})
}

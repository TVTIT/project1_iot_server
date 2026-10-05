package mqttcredential

import (
	"context"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRecoveryIntegration(t *testing.T) {
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
	r, _ := NewPostgresRepository(app, 3*time.Second)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, e := admin.Exec(ctx, sql, args...); e != nil {
			t.Fatal(e)
		}
	}
	actor := uuid.New()
	exec(`INSERT INTO profiles(id) VALUES($1)`, actor)
	exec(`INSERT INTO platform_admins(user_id) VALUES($1)`, actor)
	for _, scenario := range []string{"provision_uncertain", "rotate_uncertain", "succeeded_unfinished"} {
		t.Run(scenario, func(t *testing.T) {
			g := "test_" + uuid.New().String()
			exec(`INSERT INTO gateways(gateway_id,name) VALUES($1,'Recovery fixture')`, g)
			q := BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionProvision}}
			b, e := r.BeginOperation(ctx, q)
			if e != nil {
				t.Fatal(e)
			}
			if scenario == "rotate_uncertain" {
				c, e := CompleteOperation(b.Operation, ExecutionVerifiedSuccess, VerificationEvidence{true, true, true}, time.Now().UTC())
				if e != nil {
					t.Fatal(e)
				}
				m, e := r.ConditionalFinalize(ctx, OperationUpdate{operationGuard(b.Operation, b.Metadata), c})
				if e != nil {
					t.Fatal(e)
				}
				ep, _ := freshIdentity()
				mg := MaintenanceGuard{operationGuard(c.Operation, m), MaintenanceInProgress, nil}
				if _, e = r.BindMaintenanceEpoch(ctx, mg, ep); e != nil {
					t.Fatal(e)
				}
				mg.ExpectedEpoch = &ep
				if _, e = r.CompleteMaintenance(ctx, mg); e != nil {
					t.Fatal(e)
				}
				q = BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionRotate}}
				b, e = r.BeginOperation(ctx, q)
				if e != nil {
					t.Fatal(e)
				}
			}
			o, m := b.Operation, b.Metadata
			outcome, evidence := ExecutionRecoveryRequired, VerificationEvidence{true, true, false}
			if scenario == "succeeded_unfinished" {
				outcome, evidence = ExecutionVerifiedSuccess, VerificationEvidence{true, true, true}
			}
			c, e := CompleteOperation(o, outcome, evidence, time.Now().UTC())
			if e != nil {
				t.Fatal(e)
			}
			if outcome == ExecutionVerifiedSuccess {
				m, e = r.ConditionalFinalize(ctx, OperationUpdate{operationGuard(o, m), c})
			} else {
				m, e = r.RequireRecovery(ctx, OperationUpdate{operationGuard(o, m), c})
			}
			if e != nil {
				t.Fatal(e)
			}
			o = c.Operation
			var before string
			if e = admin.QueryRow(ctx, `SELECT to_jsonb(e)::text FROM gateway_mqtt_credential_events e WHERE operation_id=$1`, o.OperationID).Scan(&before); e != nil {
				t.Fatal(e)
			}
			ep, _ := freshIdentity()
			req := RecoveryRequest{uuid.New(), MaintenanceGuard{operationGuard(o, m), MaintenanceInProgress, nil}, ep}
			record, e := r.BeginRecovery(ctx, req)
			if e != nil {
				t.Fatal(e)
			}
			if record.Status != RecoveryPending || record.AttemptedVersion != o.CredentialVersion || record.Origin != RecoveryStartup {
				t.Fatal("intent mismatch")
			}
			again, e := r.BeginRecovery(ctx, req)
			if e != nil || !reflect.DeepEqual(record, again) {
				t.Fatal("non-idempotent intent", e)
			}
			_, e = r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionProvision}})
			requireCode(t, e, CodeRecoveryRequired)
			_, e = r.CompleteRecovery(ctx, req, RecoveryDisableEvidence{true, false, ep})
			requireCode(t, e, CodeInvalidRequest)
			stale := req
			stale.Guard.ExpectedCredentialVersion++
			_, e = r.CompleteRecovery(ctx, stale, RecoveryDisableEvidence{true, true, ep})
			requireCode(t, e, CodeCredentialConflict)
			other, _ := freshIdentity()
			stale = req
			stale.BrokerEpoch = other
			_, e = r.CompleteRecovery(ctx, stale, RecoveryDisableEvidence{true, true, other})
			requireCode(t, e, CodeCredentialConflict)
			// Partial direct app SQL proof may not commit without authority/disposition/checkpoint.
			if _, e = app.Exec(ctx, `UPDATE mqtt_credential_recovery SET status='disabled',ram_disabled=true,snapshot_observed=true,completed_at=now(),updated_at=now() WHERE recovery_id=$1`, req.RecoveryID); e == nil {
				t.Fatal("partial completion committed")
			}
			if _, e = app.Exec(ctx, `UPDATE gateway_mqtt_credential_events SET recovery_id=$2 WHERE operation_id=$1`, o.OperationID, req.RecoveryID); e == nil {
				t.Fatal("unverified disposition forged")
			}
			if _, e = app.Exec(ctx, `UPDATE mqtt_credential_maintenance SET status='completed',broker_epoch=$2,completed_at=now(),updated_at=now() WHERE operation_id=$1`, o.OperationID, ep); e == nil {
				t.Fatal("ordinary completion bypassed recovery")
			}
			// Force last write failure; all earlier writes must roll back.
			exec(`CREATE FUNCTION recovery_fixture_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER recovery_fixture_reject BEFORE UPDATE ON mqtt_credential_maintenance FOR EACH ROW EXECUTE FUNCTION recovery_fixture_reject()`)
			_, e = r.CompleteRecovery(ctx, req, RecoveryDisableEvidence{true, true, ep})
			requireCode(t, e, CodeServiceUnavailable)
			exec(`DROP TRIGGER recovery_fixture_reject ON mqtt_credential_maintenance; DROP FUNCTION recovery_fixture_reject()`)
			record, e = r.ResolveRecovery(ctx, req.RecoveryID)
			if e != nil || record.Status != RecoveryPending {
				t.Fatal("partial writes survived", e)
			}
			// Broker lifetime rebinding rejects old evidence and never alters original checkpoint.
			record, e = r.BindRecoveryEpoch(ctx, req.RecoveryID, ep, other)
			if e != nil {
				t.Fatal(e)
			}
			_, e = r.CompleteRecovery(ctx, req, RecoveryDisableEvidence{true, true, ep})
			requireCode(t, e, CodeCredentialConflict)
			req.BrokerEpoch = other
			var wg sync.WaitGroup
			errs := make(chan error, 2)
			wg.Add(2)
			for i := 0; i < 2; i++ {
				go func() {
					defer wg.Done()
					_, e := r.CompleteRecovery(ctx, req, RecoveryDisableEvidence{true, true, other})
					errs <- e
				}()
			}
			wg.Wait()
			close(errs)
			for e := range errs {
				if e != nil {
					t.Fatal("concurrent idempotent completion", e)
				}
			}
			m, e = r.GetMetadata(ctx, g)
			if e != nil || m.Status != CredentialRevoked || m.CredentialVersion != o.CredentialVersion || m.LastOperationID != o.OperationID {
				t.Fatal("authority not revoked", e, m)
			}
			cp, e := r.ResolveMaintenance(ctx, o.OperationID)
			if e != nil || cp.Status != MaintenanceCompleted || cp.RecoveryID == nil || *cp.RecoveryID != req.RecoveryID {
				t.Fatal("fence unresolved", e)
			}
			var after string
			_ = admin.QueryRow(ctx, `SELECT to_jsonb(e)::text FROM gateway_mqtt_credential_events e WHERE operation_id=$1`, o.OperationID).Scan(&after)
			if scenario == "succeeded_unfinished" && before != after {
				t.Fatal("terminal historical event mutated")
			}
			replay, e := r.BeginOperation(ctx, q)
			if e != nil || replay.Execute || replay.Replay == nil || replay.Replay.SecretReturned || replay.Replay.Status != CredentialRevoked {
				t.Fatal("unsafe replay", e)
			}
			if scenario != "succeeded_unfinished" && replay.Replay.ReplayedStatus != OperationResolvedByRecovery {
				t.Fatal("false original outcome")
			}
			decisions, e := r.ListDurableRevocations(ctx, "", 1000)
			if e != nil {
				t.Fatal(e)
			}
			found := false
			for _, d := range decisions {
				if d.GatewayID == g {
					found = true
				}
			}
			if !found {
				t.Fatal("revoked target omitted")
			}
			if _, e = app.Exec(ctx, `UPDATE mqtt_credential_recovery SET broker_epoch=$2 WHERE recovery_id=$1`, req.RecoveryID, ep); e == nil {
				t.Fatal("terminal recovery mutable")
			}
			if _, e = app.Exec(ctx, `DELETE FROM mqtt_credential_recovery WHERE recovery_id=$1`, req.RecoveryID); e == nil {
				t.Fatal("audit deletion granted")
			}
			next, e := r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionProvision}})
			if e != nil || !next.Execute || next.Operation.CredentialVersion != o.CredentialVersion+1 {
				t.Fatal("authorized reprovision blocked", e)
			}
			_, e = r.CompleteRecovery(ctx, req, RecoveryDisableEvidence{true, true, other})
			if e != nil {
				t.Fatal("terminal retry metadata", e)
			}
			current, e := r.GetMetadata(ctx, g)
			if e != nil || current.LastOperationID != next.Operation.OperationID {
				t.Fatal("stale recovery overwrote new intent")
			}
		})
	}
	_, err = r.ResolveRecovery(ctx, uuid.New())
	requireCode(t, err, CodeFinalizationPending)
	t.Run("startup legacy and unknown authority diagnosis", func(t *testing.T) {
		for _, variable := range []string{"TASK263A_LEGACY_PENDING", "TASK263A_LEGACY_TERMINAL"} {
			g := os.Getenv(variable)
			if g == "" {
				t.Fatal("legacy fixture required")
			}
			// A strict prefix immediately preceding this key selects its current
			// projection without choosing any historical winner for the Gateway.
			cursor := g[:len(g)-1]
			rows, lookupErr := r.ListStartupInventory(ctx, cursor, 1)
			if lookupErr == nil {
				t.Fatalf("legacy projection silently accepted: count=%d", len(rows))
			}
		}
		if _, lookupErr := r.ListStartupInventory(ctx, "", 0); lookupErr == nil {
			t.Fatal("unbounded scan accepted")
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, lookupErr := r.ListStartupInventory(cancelled, "", 1); lookupErr == nil {
			t.Fatal("cancelled DB read became empty inventory")
		}
	})
	t.Run("bounded startup pool exhaustion", func(t *testing.T) {
		config, parseErr := pgxpool.ParseConfig(os.Getenv("TASK263A_APP_DSN"))
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		config.MaxConns = 1
		pool, poolErr := pgxpool.NewWithConfig(ctx, config)
		if poolErr != nil {
			t.Fatal(poolErr)
		}
		defer pool.Close()
		conn, acquireErr := pool.Acquire(ctx)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		defer conn.Release()
		bounded, constructorErr := NewPostgresRepository(pool, 100*time.Millisecond, 2)
		if constructorErr != nil {
			t.Fatal(constructorErr)
		}
		start := time.Now()
		rows, readErr := bounded.ListStartupInventory(ctx, "", 1)
		if readErr == nil || len(rows) != 0 || time.Since(start) > time.Second {
			t.Fatal("pool exhaustion not bounded uncertainty", readErr)
		}
	})
	t.Run("completion versus new admission", func(t *testing.T) {
		for i := 0; i < 12; i++ {
			g := "test_" + uuid.NewString()
			exec(`INSERT INTO gateways(gateway_id,name) VALUES($1,'Recovery race fixture')`, g)
			b, e := r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionProvision}})
			if e != nil {
				t.Fatal(e)
			}
			ep, _ := freshIdentity()
			q := RecoveryRequest{uuid.New(), maintenanceGuard(b.Operation, b.Metadata, MaintenanceCheckpoint{Status: MaintenanceInProgress}), ep}
			if _, e = r.BeginRecovery(ctx, q); e != nil {
				t.Fatal(e)
			}
			start := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				<-start
				_, e := r.CompleteRecovery(ctx, q, RecoveryDisableEvidence{true, true, ep})
				done <- e
			}()
			close(start)
			request := BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionProvision}}
			next, admissionErr := r.BeginOperation(ctx, request)
			if admissionErr == nil {
				cp, lookupErr := r.ResolveMaintenance(ctx, b.Operation.OperationID)
				if lookupErr != nil || cp.Status != MaintenanceCompleted || cp.RecoveryID == nil {
					t.Fatal("admitted before durable cleanup", lookupErr)
				}
			} else {
				requireCode(t, admissionErr, CodeRecoveryRequired)
			}
			if e = <-done; e != nil {
				t.Fatal(e)
			}
			if admissionErr != nil {
				next, e = r.BeginOperation(ctx, request)
				if e != nil {
					t.Fatal(e)
				}
			}
			if !next.Execute || next.Operation.CredentialVersion != b.Operation.CredentialVersion+1 {
				t.Fatal("nonmonotonic admission")
			}
		}
	})
	t.Run("commit uncertainty uses independent identity read", func(t *testing.T) {
		g := "test_" + uuid.New().String()
		exec(`INSERT INTO gateways(gateway_id,name) VALUES($1,'Commit recovery fixture')`, g)
		b, e := r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionProvision}})
		if e != nil {
			t.Fatal(e)
		}
		ep, _ := freshIdentity()
		q := RecoveryRequest{uuid.New(), MaintenanceGuard{operationGuard(b.Operation, b.Metadata), MaintenanceInProgress, nil}, ep}
		if _, e = r.BeginRecovery(ctx, q); e != nil {
			t.Fatal(e)
		}
		exec(`CREATE FUNCTION recovery_fixture_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.3); RETURN NEW; END $$; CREATE CONSTRAINT TRIGGER recovery_fixture_delay AFTER UPDATE ON mqtt_credential_recovery DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION recovery_fixture_delay()`)
		defer exec(`DROP TRIGGER recovery_fixture_delay ON mqtt_credential_recovery; DROP FUNCTION recovery_fixture_delay()`)
		short, _ := NewPostgresRepository(app, 100*time.Millisecond)
		_, e = short.CompleteRecovery(ctx, q, RecoveryDisableEvidence{true, true, ep})
		requireCode(t, e, CodeFinalizationPending)
		record, e := r.ResolveRecovery(ctx, q.RecoveryID)
		if e != nil || (record.Status != RecoveryPending && record.Status != RecoveryDisabled) {
			t.Fatal("independent uncertainty lookup", e)
		}
	})
}

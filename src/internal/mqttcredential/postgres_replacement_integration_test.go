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

func TestPostgresReplacementIsolated(t *testing.T) {
	if os.Getenv("TASK284_REPLACEMENT_APP_DSN") == "" {
		t.Skip("owned replacement fixture required")
	}
	ctx := context.Background()
	open := func(env string) *pgxpool.Pool {
		p, err := pgxpool.New(ctx, os.Getenv(env))
		if err != nil {
			t.Fatal("fixture connection")
		}
		t.Cleanup(p.Close)
		return p
	}
	app, admin := open("TASK284_REPLACEMENT_APP_DSN"), open("TASK284_REPLACEMENT_ADMIN_DSN")
	r, _ := NewPostgresRepository(app, 5*time.Second)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	actor := uuid.New()
	exec(`INSERT INTO profiles(id,full_name) VALUES($1,'Replacement fixture')`, actor)
	exec(`INSERT INTO platform_admins(user_id) VALUES($1)`, actor)
	guard := func(b BeginResult) OperationGuard {
		return OperationGuard{b.Metadata.GatewayID, b.Operation.OperationID, b.Operation.Status, b.Metadata.Status, b.Metadata.CredentialVersion}
	}
	finish := func(b BeginResult, outcome ExecutionOutcome) BeginResult {
		t.Helper()
		evidence := VerificationEvidence{}
		if outcome == ExecutionVerifiedSuccess {
			evidence = VerificationEvidence{true, true, true}
		}
		c, err := CompleteOperation(b.Operation, outcome, evidence, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		u := OperationUpdate{guard(b), c}
		var m Metadata
		switch outcome {
		case ExecutionVerifiedSuccess:
			m, err = r.ConditionalFinalize(ctx, u)
		case ExecutionRecoveryRequired:
			m, err = r.RequireRecovery(ctx, u)
		default:
			m, err = r.FailKnown(ctx, u)
		}
		if err != nil {
			t.Fatal(err)
		}
		b.Operation, b.Metadata = c.Operation, m
		return b
	}
	completeCheckpoint := func(b BeginResult) {
		t.Helper()
		mg := MaintenanceGuard{OperationGuard: guard(b), ExpectedStatus: MaintenanceInProgress}
		c, err := r.BindMaintenanceEpoch(ctx, mg, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
		if err != nil {
			t.Fatal(err)
		}
		mg.ExpectedEpoch = c.BrokerEpoch
		if _, err = r.CompleteMaintenance(ctx, mg); err != nil {
			t.Fatal(err)
		}
	}
	seed := func() BeginResult {
		t.Helper()
		g := "test_" + uuid.New().String()
		exec(`INSERT INTO gateways(gateway_id,name) VALUES($1,'Replacement fixture')`, g)
		b, err := r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionProvision}})
		if err != nil {
			t.Fatal(err)
		}
		b = finish(b, ExecutionVerifiedSuccess)
		completeCheckpoint(b)
		b, err = r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionRotate}})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	req := func(b BeginResult) ReplacementRequest {
		return ReplacementRequest{actor, uuid.New(), uuid.New(), guard(b)}
	}
	admit := func(q ReplacementRequest) BeginResult {
		t.Helper()
		b, err := r.BeginReplacement(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		if !b.Execute {
			t.Fatal("fresh admission not executable")
		}
		return b
	}
	denySQL := func(sql string, args ...any) {
		t.Helper()
		tx, err := app.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer rollbackTestTransaction(t, tx)
		_, err = tx.Exec(ctx, sql, args...)
		if err == nil {
			err = tx.Commit(ctx)
		}
		if err == nil {
			t.Fatal("spoof/partial transaction committed")
		}
	}
	t.Run("invalid input unknown gateway cancelled DB and exact CAS", func(t *testing.T) {
		old := seed()
		q := req(old)
		invalid := q
		invalid.ActorUserID = uuid.Nil
		_, err := r.BeginReplacement(ctx, invalid)
		requireCode(t, err, CodeInvalidRequest)
		unknown := q
		unknown.Guard.GatewayID = "unknown_" + uuid.New().String()
		_, err = r.BeginReplacement(ctx, unknown)
		requireCode(t, err, CodeNotFound)
		missing := q
		missing.Guard.ExpectedOperationID = uuid.New()
		_, err = r.BeginReplacement(ctx, missing)
		requireCode(t, err, CodeNotFound)
		wrong := q
		wrong.Guard.ExpectedCredentialVersion++
		_, err = r.BeginReplacement(ctx, wrong)
		requireCode(t, err, CodeCredentialConflict)
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err = r.BeginReplacement(cancelled, q)
		requireCode(t, err, CodeServiceUnavailable)
		// A reused ordinary key cannot become a manual capability.
		wrong = q
		wrong.IdempotencyKey = old.Operation.IdempotencyKey
		_, err = r.BeginReplacement(ctx, wrong)
		requireCode(t, err, CodeIdempotencyConflict)
	})
	t.Run("admission replay repeated uncertainty finalization effective scans", func(t *testing.T) {
		old := finish(seed(), ExecutionRecoveryRequired)
		q := req(old)
		b := admit(q)
		if b.Operation.Previous.Status != old.Metadata.Status || b.Operation.CredentialVersion != 3 {
			t.Fatal("forged previous or generation")
		}
		replay, err := r.BeginReplacement(ctx, q)
		if err != nil || replay.Execute || replay.Replay == nil || replay.Replay.SecretReturned {
			t.Fatal("replay must be metadata-only", err)
		}
		wrong := q
		wrong.Guard.ExpectedOperationID = uuid.New()
		_, err = r.BeginReplacement(ctx, wrong)
		requireCode(t, err, CodeIdempotencyConflict)
		_, err = r.BeginReplacement(ctx, req(old))
		requireCode(t, err, CodeCredentialConflict)
		denySQL(`UPDATE gateway_mqtt_credential_events SET updated_at=now() WHERE operation_id=$1`, old.Operation.OperationID)
		denySQL(`UPDATE mqtt_credential_maintenance SET updated_at=now() WHERE operation_id=$1`, old.Operation.OperationID)
		denySQL(`UPDATE gateway_mqtt_credentials SET last_operation_id=$1 WHERE gateway_id=$2`, old.Operation.OperationID, old.Metadata.GatewayID)
		b = finish(b, ExecutionRecoveryRequired)
		next := admit(req(b))
		next = finish(next, ExecutionVerifiedSuccess)
		completeCheckpoint(next)
		if next.Metadata.CredentialVersion != 4 {
			t.Fatal("generation not reserved")
		}
		// Ordinary revoke/provision remain available after effective maintenance
		// clears; no broad admission bypass or generation reset.
		revoked, err := r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{next.Metadata.GatewayID, uuid.New(), ActionRevoke}})
		if err != nil {
			t.Fatal(err)
		}
		revoked = finish(revoked, ExecutionVerifiedSuccess)
		completeCheckpoint(revoked)
		provisioned, err := r.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{next.Metadata.GatewayID, uuid.New(), ActionProvision}})
		if err != nil {
			t.Fatal(err)
		}
		provisioned = finish(provisioned, ExecutionVerifiedSuccess)
		completeCheckpoint(provisioned)
		if provisioned.Metadata.CredentialVersion != 5 {
			t.Fatal("ordinary generation regression")
		}
		ops, err := r.ListUnresolved(ctx, uuid.Nil, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range ops {
			if o.GatewayID == old.Metadata.GatewayID {
				t.Fatal("superseded unresolved event still effective")
			}
		}
		cps, err := r.ListPendingMaintenance(ctx, uuid.Nil, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cps {
			if c.GatewayID == old.Metadata.GatewayID {
				t.Fatal("superseded checkpoint still effective")
			}
		}
		resolved, err := r.ResolveCommitAmbiguity(ctx, next.Operation.OperationID)
		if err != nil || resolved.ReplacementPredecessorID == nil {
			t.Fatal("exact ambiguity lookup")
		}
		_, err = r.ResolveCommitAmbiguity(ctx, uuid.New())
		var unknown *CommitOutcomeUnknown
		if !errors.As(err, &unknown) {
			t.Fatal("absence permits rerun")
		}
	})
	t.Run("concurrent successors exactly one current", func(t *testing.T) {
		old := seed()
		start := make(chan struct{})
		var wg sync.WaitGroup
		results := make(chan error, 6)
		for i := 0; i < 6; i++ {
			q := req(old)
			wg.Add(1)
			go func() { defer wg.Done(); <-start; _, err := r.BeginReplacement(ctx, q); results <- err }()
		}
		close(start)
		wg.Wait()
		close(results)
		wins := 0
		for err := range results {
			if err == nil {
				wins++
			} else {
				requireCode(t, err, CodeCredentialConflict)
			}
		}
		if wins != 1 {
			t.Fatal("duplicate effective admission")
		}
	})
	t.Run("link partial orphan cross gateway cycle missing checkpoint", func(t *testing.T) {
		old := seed()
		other := seed()
		denySQL(`INSERT INTO mqtt_rotation_replacements(predecessor_id,successor_id,gateway_id) VALUES($1,$2,$3)`, old.Operation.OperationID, uuid.New(), old.Metadata.GatewayID)
		denySQL(`INSERT INTO mqtt_rotation_replacements(predecessor_id,successor_id,gateway_id) VALUES($1,$2,$3)`, old.Operation.OperationID, other.Operation.OperationID, old.Metadata.GatewayID)
		denySQL(`INSERT INTO mqtt_rotation_replacements(predecessor_id,successor_id,gateway_id) VALUES($1,$1,$2)`, old.Operation.OperationID, old.Metadata.GatewayID)
		denySQL(`INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,credential_version,action,status,actor_user_id,idempotency_key,previous_status,previous_credential_version,previous_activated_at,replacement_predecessor_id) VALUES($1,$2,3,'rotate','pending',$3,$4,'rotating',2,now(),$5)`, uuid.New(), old.Metadata.GatewayID, actor, uuid.New(), old.Operation.OperationID)
		denySQL(`DELETE FROM mqtt_credential_maintenance WHERE operation_id=$1`, old.Operation.OperationID)
		// Remove only fixture checkpoint with migration-owner bypass to exercise
		// repository corruption fail-closed; no runtime/public guard is disabled.
		exec(`ALTER TABLE mqtt_credential_maintenance DISABLE TRIGGER USER`)
		exec(`DELETE FROM mqtt_credential_maintenance WHERE operation_id=$1`, old.Operation.OperationID)
		exec(`ALTER TABLE mqtt_credential_maintenance ENABLE TRIGGER USER`)
		_, err := r.BeginReplacement(ctx, req(old))
		requireCode(t, err, CodeCredentialConflict)
		exec(`INSERT INTO mqtt_credential_maintenance(operation_id,gateway_id) VALUES($1,$2)`, old.Operation.OperationID, old.Metadata.GatewayID)
	})
	t.Run("all historical generations and overflow reserved", func(t *testing.T) {
		for _, version := range []int64{97, 9223372036854775807} {
			old := seed()
			// Owned migration-role corruption fixture only: historical attempted
			// generation, never a runtime reset or fake execution disposition.
			exec(`ALTER TABLE gateway_mqtt_credential_events DISABLE TRIGGER USER`)
			exec(`INSERT INTO gateway_mqtt_credential_events(operation_id,gateway_id,credential_version,action,status,legacy_projection,completed_at) VALUES($1,$2,$3,'rotate','failed',true,now())`, uuid.New(), old.Metadata.GatewayID, version)
			exec(`ALTER TABLE gateway_mqtt_credential_events ENABLE TRIGGER USER`)
			b, err := r.BeginReplacement(ctx, req(old))
			if version == 97 {
				if err != nil || b.Operation.CredentialVersion != 98 {
					t.Fatal("all history reservation", err)
				}
			} else {
				requireCode(t, err, CodeInvalidRequest)
			}
		}
	})
	t.Run("grant lock serializes concurrent revoke", func(t *testing.T) {
		tx, err := app.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer rollbackTestTransaction(t, tx)
		var allowed bool
		if err = tx.QueryRow(ctx, `SELECT lock_mqtt_replacement_admin($1)`, actor).Scan(&allowed); err != nil || !allowed {
			t.Fatal("grant lock", err)
		}
		started := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			close(started)
			_, e := admin.Exec(ctx, `DELETE FROM platform_admins WHERE user_id=$1`, actor)
			done <- e
		}()
		<-started
		select {
		case <-done:
			t.Fatal("revoke bypassed grant lock")
		case <-time.After(100 * time.Millisecond):
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err = <-done; err != nil {
			t.Fatal(err)
		}
		exec(`INSERT INTO platform_admins(user_id) VALUES($1)`, actor)
	})
	t.Run("admin revoked and deleted actors retained", func(t *testing.T) {
		old := seed()
		q := req(old)
		exec(`DELETE FROM platform_admins WHERE user_id=$1`, actor)
		_, err := r.BeginReplacement(ctx, q)
		requireCode(t, err, CodeForbidden)
		exec(`INSERT INTO platform_admins(user_id) VALUES($1)`, actor)
		b := admit(q)
		exec(`DELETE FROM profiles WHERE id=$1`, actor)
		o, err := r.ResolveCommitAmbiguity(ctx, old.Operation.OperationID)
		if err != nil || o.ActorUserID != uuid.Nil {
			t.Fatal("historical actor-delete exception", err)
		}
		o, err = r.ResolveCommitAmbiguity(ctx, b.Operation.OperationID)
		if err != nil || o.ActorUserID != uuid.Nil {
			t.Fatal("current actor-delete exception", err)
		}
		_, err = r.BeginReplacement(ctx, q)
		requireCode(t, err, CodeForbidden)
	})
}

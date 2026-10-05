package mqttcredential

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresFinalizationIntegration(t *testing.T) {
	if os.Getenv("TASK263A_APP_DSN") == "" {
		t.Skip("isolated PostgreSQL fixture required")
	}
	ctx := context.Background()
	open := func(dsn string) *pgxpool.Pool {
		p, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatal("fixture unavailable")
		}
		t.Cleanup(p.Close)
		return p
	}
	app, admin := open(os.Getenv("TASK263A_APP_DSN")), open(os.Getenv("TASK263A_ADMIN_DSN"))
	r, err := NewPostgresRepository(app, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatal("fixture SQL failed")
		}
	}
	actor := uuid.New()
	exec(`INSERT INTO profiles(id) VALUES($1)`, actor)
	exec(`INSERT INTO platform_admins(user_id) VALUES($1)`, actor)
	allowed, err := r.IsPlatformAdmin(ctx, actor)
	if err != nil || !allowed {
		t.Fatal("app role admin read failed")
	}
	allowed, err = r.IsPlatformAdmin(ctx, uuid.New())
	if err != nil || allowed {
		t.Fatal("unknown actor privileged")
	}
	bounded, err := NewPostgresRepository(app, time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = bounded.ListUnresolved(ctx, uuid.Nil, 2)
	requireCode(t, err, CodeInvalidRequest)
	_, err = bounded.ListDurableRevocations(ctx, "", 2)
	requireCode(t, err, CodeInvalidRequest)
	newGateway := func() string {
		g := "test_" + uuid.New().String()
		exec(`INSERT INTO gateways(gateway_id,name) VALUES($1,'Finalization fixture')`, g)
		return g
	}
	begin := func(g string, a Action) (BeginRequest, BeginResult) {
		t.Helper()
		// Finalization-only fixtures simulate the separate successful OPEN receipt
		// before moving to a new operation; terminal audit status alone is insufficient.
		if _, e := app.Exec(ctx, `UPDATE mqtt_credential_maintenance p SET status='completed',broker_epoch=repeat('a',64),error_code=NULL,completed_at=now(),updated_at=now()
FROM gateway_mqtt_credentials c JOIN gateway_mqtt_credential_events e ON e.operation_id=c.last_operation_id
WHERE c.gateway_id=$1 AND p.operation_id=c.last_operation_id AND e.status IN ('succeeded','failed') AND p.status <> 'completed'`, g); e != nil {
			t.Fatal(e)
		}
		q := BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), a}}
		b, e := r.BeginOperation(ctx, q)
		if e != nil {
			t.Fatal(e)
		}
		return q, b
	}
	update := func(b BeginResult, outcome ExecutionOutcome, evidence VerificationEvidence) OperationUpdate {
		t.Helper()
		c, e := CompleteOperation(b.Operation, outcome, evidence, time.Now().UTC().Truncate(time.Microsecond))
		if e != nil {
			t.Fatal(e)
		}
		return OperationUpdate{OperationGuard{b.Metadata.GatewayID, b.Operation.OperationID, b.Operation.Status, b.Metadata.Status, b.Metadata.CredentialVersion}, c}
	}
	proof := VerificationEvidence{true, true, true}
	t.Run("separate finalization budget and short admission reads", func(t *testing.T) {
		shortDB, e := NewPostgresRepositoryWithFinalizationTimeout(app, 100*time.Millisecond, 2*time.Second)
		if e != nil {
			t.Fatal(e)
		}
		for _, outcome := range []ExecutionOutcome{ExecutionVerifiedSuccess, ExecutionUnchangedFailure, ExecutionRecoveryRequired} {
			g := newGateway()
			_, b := begin(g, ActionProvision)
			evidence := VerificationEvidence{}
			if outcome == ExecutionVerifiedSuccess {
				evidence = proof
			}
			u := update(b, outcome, evidence)
			exec(`CREATE FUNCTION task2613_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.gateway_id=TG_ARGV[0] THEN PERFORM pg_sleep(0.3); END IF; RETURN NEW; END $$`)
			exec(`CREATE TRIGGER task2613_delay BEFORE UPDATE ON gateway_mqtt_credential_events FOR EACH ROW EXECUTE FUNCTION task2613_delay('` + g + `')`)
			m, err := shortDB.complete(ctx, u, outcome)
			exec(`DROP TRIGGER task2613_delay ON gateway_mqtt_credential_events; DROP FUNCTION task2613_delay()`)
			if err != nil || m.GatewayID != g {
				t.Fatal("finalization incorrectly used short query budget", err)
			}
		}
		g := newGateway()
		_, b := begin(g, ActionProvision)
		lock, e := admin.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		defer func() { _ = lock.Rollback(ctx) }()
		if _, e = lock.Exec(ctx, `LOCK TABLE gateway_mqtt_credentials IN ACCESS EXCLUSIVE MODE`); e != nil {
			t.Fatal(e)
		}
		started := time.Now()
		_, e = shortDB.GetMetadata(ctx, g)
		requireCode(t, e, CodeServiceUnavailable)
		if time.Since(started) > time.Second {
			t.Fatal("read used finalization budget")
		}
		started = time.Now()
		_, e = shortDB.BeginOperation(ctx, BeginRequest{actor, uuid.New(), MutationInput{g, uuid.New(), ActionRotate}})
		requireCode(t, e, CodeServiceUnavailable)
		if time.Since(started) > time.Second {
			t.Fatal("admission used finalization budget")
		}
		_ = lock.Rollback(ctx)
		// A separate short finalization budget must interrupt a real delayed COMMIT.
		shortFinalize, e := NewPostgresRepositoryWithFinalizationTimeout(app, 2*time.Second, 100*time.Millisecond)
		if e != nil {
			t.Fatal(e)
		}
		exec(`CREATE FUNCTION task2613_commit_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.gateway_id=TG_ARGV[0] THEN PERFORM pg_sleep(0.3); END IF; RETURN NEW; END $$`)
		exec(`CREATE CONSTRAINT TRIGGER task2613_commit_delay AFTER UPDATE ON gateway_mqtt_credential_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION task2613_commit_delay('` + g + `')`)
		m, e := shortFinalize.ConditionalFinalize(ctx, update(b, ExecutionVerifiedSuccess, proof))
		var unknown *CommitOutcomeUnknown
		if !errors.As(e, &unknown) || unknown.OperationID != b.Operation.OperationID || m.GatewayID != "" {
			t.Fatal("expired commit must remain unknown and return no metadata", e)
		}
		exec(`DROP TRIGGER task2613_commit_delay ON gateway_mqtt_credential_events; DROP FUNCTION task2613_commit_delay()`)
		o, e := r.ResolveCommitAmbiguity(ctx, b.Operation.OperationID)
		if e != nil || (o.Status != OperationPending && o.Status != OperationSucceeded) {
			t.Fatal("exact detached lookup unavailable", e)
		}
		if _, e = r.ConditionalFinalize(ctx, update(b, ExecutionVerifiedSuccess, proof)); e != nil {
			t.Fatal("database-only completion retry failed", e)
		}
	})
	success := func(b BeginResult) Metadata {
		t.Helper()
		m, e := r.ConditionalFinalize(ctx, update(b, ExecutionVerifiedSuccess, proof))
		if e != nil {
			t.Fatal(e)
		}
		return m
	}
	t.Run("CAS proof and immutable previous", func(t *testing.T) {
		_, b := begin(newGateway(), ActionProvision)
		u := update(b, ExecutionVerifiedSuccess, proof)
		for _, change := range []func(*OperationUpdate){
			func(v *OperationUpdate) { v.Guard.ExpectedOperationID = uuid.New() },
			func(v *OperationUpdate) { v.Guard.ExpectedCredentialVersion++ },
			func(v *OperationUpdate) { v.Guard.ExpectedCredentialStatus = CredentialActive },
			func(v *OperationUpdate) { v.Guard.ExpectedOperationStatus = OperationRecoveryNeeded },
			func(v *OperationUpdate) { v.Completion.Operation.Evidence.FreshPositiveVerified = false },
			func(v *OperationUpdate) { v.Completion.Operation.Evidence.RAMApplied = false },
			func(v *OperationUpdate) { v.Completion.Operation.Evidence.SnapshotObserved = false },
			func(v *OperationUpdate) { v.Completion.Credential.CredentialVersion++ },
			func(v *OperationUpdate) {
				v.Completion.Operation.Previous = &CredentialSnapshot{Status: CredentialActive, CredentialVersion: 1}
			},
			func(v *OperationUpdate) {
				code := ErrorCode("unsafe SQL diagnostic")
				v.Completion.Operation.ErrorCode = &code
			},
		} {
			v := u
			change(&v)
			if _, e := r.ConditionalFinalize(ctx, v); e == nil {
				t.Fatal("invalid completion accepted")
			}
			m, e := r.GetMetadata(ctx, b.Metadata.GatewayID)
			if e != nil || m.Status != CredentialProvisioning {
				t.Fatal("invalid proposal changed metadata")
			}
		}
		if _, e := admin.Exec(ctx, `UPDATE gateway_mqtt_credentials SET last_operation_id=NULL WHERE gateway_id=$1`, b.Metadata.GatewayID); e == nil {
			t.Fatal("schema permitted corrupt operation projection")
		}
		m, e := r.ConditionalFinalize(ctx, u)
		if e != nil || m.Status != CredentialActive {
			t.Fatal("finalize failed", e)
		}
		m2, e := r.ConditionalFinalize(ctx, u)
		if e != nil || !reflect.DeepEqual(m, m2) {
			t.Fatal("terminal retry failed", e)
		}
		_, e = r.RequireRecovery(ctx, u)
		requireCode(t, e, CodeCredentialConflict)
		_, newer := begin(b.Metadata.GatewayID, ActionRotate)
		m2, e = r.ConditionalFinalize(ctx, u)
		if e != nil || m2.LastOperationID != newer.Operation.OperationID {
			t.Fatal("terminal retry overwrote future operation")
		}
		v := u
		v.Completion.Operation.UpdatedAt = v.Completion.Operation.UpdatedAt.Add(time.Second)
		_, e = r.ConditionalFinalize(ctx, v)
		requireCode(t, e, CodeCredentialConflict)
	})
	t.Run("known unchanged restore and reserve attempted generation", func(t *testing.T) {
		g := newGateway()
		_, b := begin(g, ActionProvision)
		old := success(b)
		q, b := begin(g, ActionRotate)
		u := update(b, ExecutionUnchangedFailure, VerificationEvidence{})
		code := CodeRuntimeBusy
		u.Completion.Operation.ErrorCode = &code
		m, e := r.FailKnown(ctx, u)
		if e != nil || m.Status != CredentialActive || m.CredentialVersion != old.CredentialVersion || !m.ActivatedAt.Equal(*old.ActivatedAt) {
			t.Fatal("known unchanged not restored", e)
		}
		if _, e = r.FailKnown(ctx, u); e != nil {
			t.Fatal("failed retry", e)
		}
		replay, e := r.BeginOperation(ctx, q)
		if e != nil || replay.Execute || replay.Replay == nil || replay.Replay.ReplayedStatus != OperationFailed {
			t.Fatal("failure replay", e)
		}
		_, next := begin(g, ActionRotate)
		if next.Operation.CredentialVersion != 3 {
			t.Fatal("generation reused")
		}
		_, b = begin(newGateway(), ActionProvision)
		m, e = r.FailKnown(ctx, update(b, ExecutionUnchangedFailure, VerificationEvidence{}))
		if e != nil || m.Status != CredentialFailed {
			t.Fatal("failed provision", e)
		}
	})
	t.Run("recovery evidence monotonic no fake rollback", func(t *testing.T) {
		_, b := begin(newGateway(), ActionProvision)
		u := update(b, ExecutionRecoveryRequired, VerificationEvidence{RAMApplied: true})
		m, e := r.RequireRecovery(ctx, u)
		if e != nil || m.Status != CredentialRecoveryNeeded {
			t.Fatal("recovery failed", e)
		}
		o, e := r.ResolveCommitAmbiguity(ctx, b.Operation.OperationID)
		if e != nil || o.CompletedAt != nil {
			t.Fatal("recovery became terminal")
		}
		b.Operation, b.Metadata = o, m
		u = update(b, ExecutionRecoveryRequired, VerificationEvidence{})
		m, e = r.RequireRecovery(ctx, u)
		if e != nil || !u.Completion.Operation.Evidence.RAMApplied {
			t.Fatal("evidence discarded")
		}
		bad := u
		bad.Completion.Operation.Status = OperationFailed
		_, e = r.FailKnown(ctx, bad)
		requireCode(t, e, CodeInvalidRequest)
		b.Operation, _ = r.ResolveCommitAmbiguity(ctx, o.OperationID)
		b.Metadata = m
		success(b)
	})
	t.Run("concurrent finalize versus recovery", func(t *testing.T) {
		_, b := begin(newGateway(), ActionProvision)
		a := update(b, ExecutionVerifiedSuccess, proof)
		z := update(b, ExecutionRecoveryRequired, VerificationEvidence{RAMApplied: true})
		start := make(chan struct{})
		result := make(chan error, 2)
		var wg sync.WaitGroup
		for i, u := range []OperationUpdate{a, z} {
			wg.Add(1)
			go func(i int, u OperationUpdate) {
				defer wg.Done()
				<-start
				var e error
				if i == 0 {
					_, e = r.ConditionalFinalize(ctx, u)
				} else {
					_, e = r.RequireRecovery(ctx, u)
				}
				result <- e
			}(i, u)
		}
		close(start)
		wg.Wait()
		close(result)
		n := 0
		for e := range result {
			if e == nil {
				n++
			} else {
				requireCode(t, e, CodeCredentialConflict)
			}
		}
		if n != 1 {
			t.Fatal("both CAS writers won")
		}
	})
	t.Run("atomic rollback event or metadata error", func(t *testing.T) {
		for _, table := range []string{"gateway_mqtt_credentials", "gateway_mqtt_credential_events"} {
			_, b := begin(newGateway(), ActionProvision)
			u := update(b, ExecutionVerifiedSuccess, proof)
			exec(`CREATE FUNCTION task263b_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.gateway_id=TG_ARGV[0] THEN RAISE EXCEPTION 'fixture failure'; END IF; RETURN NEW; END $$`)
			exec(`CREATE TRIGGER task263b_reject BEFORE UPDATE ON ` + table + ` FOR EACH ROW EXECUTE FUNCTION task263b_reject('` + b.Metadata.GatewayID + `')`)
			_, e := r.ConditionalFinalize(ctx, u)
			requireCode(t, e, CodeServiceUnavailable)
			exec(`DROP TRIGGER task263b_reject ON ` + table + `; DROP FUNCTION task263b_reject()`)
			o, e := r.ResolveCommitAmbiguity(ctx, b.Operation.OperationID)
			if e != nil || o.Status != OperationPending {
				t.Fatal("event did not roll back")
			}
			m, e := r.GetMetadata(ctx, b.Metadata.GatewayID)
			if e != nil || m.Status != CredentialProvisioning {
				t.Fatal("metadata did not roll back")
			}
			success(b)
		}
	})
	t.Run("success replay current revoke and authorized reprovision", func(t *testing.T) {
		g := newGateway()
		q, b := begin(g, ActionProvision)
		success(b)
		replay, e := r.BeginOperation(ctx, q)
		if e != nil || replay.Execute || replay.Replay == nil || replay.Replay.SecretReturned {
			t.Fatal("unsafe success replay")
		}
		_, b = begin(g, ActionRevoke)
		contains := func() bool {
			ds, e := r.ListDurableRevocations(ctx, "", 1000)
			if e != nil {
				t.Fatal(e)
			}
			for _, d := range ds {
				if d.GatewayID == g {
					return true
				}
			}
			return false
		}
		if !contains() {
			t.Fatal("pending revoke omitted")
		}
		success(b)
		if !contains() {
			t.Fatal("durable revoke omitted")
		}
		_, b = begin(g, ActionProvision)
		if contains() {
			t.Fatal("reprovision did not supersede")
		}
		success(b)
		if contains() {
			t.Fatal("historical revoke reapplied")
		}
	})
	t.Run("bounded cursor legacy duplicates deleted actor", func(t *testing.T) {
		var cursor uuid.UUID
		seen := map[uuid.UUID]bool{}
		legacy := 0
		recoverSeen := false
		for {
			page, e := r.ListUnresolved(ctx, cursor, 1)
			if e != nil {
				t.Fatal(e)
			}
			if page == nil || len(page) > 1 {
				t.Fatal("unbounded or nil")
			}
			if len(page) == 0 {
				break
			}
			o := page[0]
			if seen[o.OperationID] || o.OperationID.String() <= cursor.String() {
				t.Fatal("cursor not exclusive ordered")
			}
			seen[o.OperationID] = true
			cursor = o.OperationID
			if o.GatewayID == os.Getenv("TASK263A_LEGACY_PENDING") {
				legacy++
				if o.Action == Action("recover") {
					recoverSeen = true
					if o.CredentialVersion != 0 || o.ActorUserID != uuid.Nil || o.IdempotencyKey != uuid.Nil {
						t.Fatal("legacy NULL invented")
					}
				}
			}
		}
		if legacy != 2 || !recoverSeen {
			t.Fatal("legacy unresolved lost")
		}
		last := ""
		for {
			page, e := r.ListDurableRevocations(ctx, last, 1)
			if e != nil {
				t.Fatal(e)
			}
			if page == nil || len(page) > 1 {
				t.Fatal("revocation unbounded")
			}
			if len(page) == 0 {
				break
			}
			if page[0].GatewayID <= last {
				t.Fatal("revocation cursor")
			}
			last = page[0].GatewayID
		}
		old := uuid.New()
		exec(`INSERT INTO profiles(id) VALUES($1)`, old)
		exec(`INSERT INTO platform_admins(user_id) VALUES($1)`, old)
		g := newGateway()
		q := BeginRequest{old, uuid.New(), MutationInput{g, uuid.New(), ActionProvision}}
		b, e := r.BeginOperation(ctx, q)
		if e != nil {
			t.Fatal(e)
		}
		exec(`DELETE FROM profiles WHERE id=$1`, old)
		o, e := r.ResolveCommitAmbiguity(ctx, q.OperationID)
		if e != nil || o.ActorUserID != uuid.Nil {
			t.Fatal("deleted actor inaccessible")
		}
		b.Operation = o
		success(b)
	})
	t.Run("finalization commit delay safely unknown", func(t *testing.T) {
		_, b := begin(newGateway(), ActionProvision)
		u := update(b, ExecutionVerifiedSuccess, proof)
		exec(`CREATE FUNCTION task263b_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.gateway_id=TG_ARGV[0] THEN PERFORM pg_sleep(0.3); END IF; RETURN NEW; END $$`)
		exec(`CREATE CONSTRAINT TRIGGER task263b_delay AFTER UPDATE ON gateway_mqtt_credential_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION task263b_delay('` + b.Metadata.GatewayID + `')`)
		defer exec(`DROP TRIGGER task263b_delay ON gateway_mqtt_credential_events; DROP FUNCTION task263b_delay()`)
		short, _ := NewPostgresRepository(app, 100*time.Millisecond)
		m, e := short.ConditionalFinalize(ctx, u)
		var unknown *CommitOutcomeUnknown
		if !errors.As(e, &unknown) || unknown.OperationID != b.Operation.OperationID || m.GatewayID != "" {
			t.Fatal("ambiguity unsafe", e)
		}
		o, e := r.ResolveCommitAmbiguity(ctx, b.Operation.OperationID)
		if e != nil {
			t.Fatal(e)
		}
		if o.Status != OperationPending && o.Status != OperationSucceeded {
			t.Fatal("unexpected durable outcome")
		}
		// Retry only the DB completion; no runtime adapter exists in this fixture.
		m, e = r.ConditionalFinalize(ctx, u)
		if e != nil || m.Status != CredentialActive {
			t.Fatal("ambiguity DB-only retry failed", e)
		}
	})
	t.Run("database outage safe errors", func(t *testing.T) {
		_, b := begin(newGateway(), ActionProvision)
		u := update(b, ExecutionVerifiedSuccess, proof)
		closed := open(os.Getenv("TASK263A_APP_DSN"))
		dead, e := NewPostgresRepository(closed, time.Second)
		if e != nil {
			t.Fatal(e)
		}
		closed.Close()
		_, e = dead.ConditionalFinalize(ctx, u)
		requireCode(t, e, CodeServiceUnavailable)
		_, e = dead.ListUnresolved(ctx, uuid.Nil, 1)
		requireCode(t, e, CodeServiceUnavailable)
		_, e = dead.ListDurableRevocations(ctx, "", 1)
		requireCode(t, e, CodeServiceUnavailable)
		_, e = dead.ResolveCommitAmbiguity(ctx, b.Operation.OperationID)
		requireCode(t, e, CodeFinalizationPending)
	})
}

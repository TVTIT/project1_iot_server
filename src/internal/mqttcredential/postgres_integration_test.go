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

func TestPostgresAdmissionIntegration(t *testing.T) {
	dsn := os.Getenv("TASK263A_APP_DSN")
	if dsn == "" {
		t.Skip("isolated PostgreSQL fixture required")
	}
	ctx := context.Background()
	open := func(dsn string) *pgxpool.Pool {
		c, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatal("fixture config invalid")
		}
		c.MaxConns = 4
		p, err := pgxpool.NewWithConfig(ctx, c)
		if err != nil {
			t.Fatal("fixture connection unavailable")
		}
		t.Cleanup(p.Close)
		return p
	}
	app, admin := open(dsn), open(os.Getenv("TASK263A_ADMIN_DSN"))
	r, err := NewPostgresRepository(app, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatal("fixture write failed")
		}
	}
	finalizeFixture := func(eventSQL string, operationID uuid.UUID, metadataSQL, gateway string) {
		t.Helper()
		tx, err := app.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err = tx.Exec(ctx, eventSQL, operationID); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, metadataSQL, gateway); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	actor := uuid.New()
	exec(`INSERT INTO profiles(id,full_name) VALUES($1,'Admission fixture')`, actor)
	exec(`INSERT INTO platform_admins(user_id) VALUES($1)`, actor)
	newGateway := func() string {
		id := "test_" + uuid.New().String()
		exec(`INSERT INTO gateways(gateway_id,name) VALUES($1,'Admission fixture')`, id)
		return id
	}
	request := func(g string, a Action) BeginRequest {
		return BeginRequest{ActorUserID: actor, OperationID: uuid.New(), Input: MutationInput{GatewayID: g, IdempotencyKey: uuid.New(), Action: a}}
	}
	count := func(sql string) int64 {
		var n int64
		if err := admin.QueryRow(ctx, sql).Scan(&n); err != nil {
			t.Fatal("fixture count failed")
		}
		return n
	}
	before := []int64{count(`SELECT count(*) FROM sensors`), count(`SELECT count(*) FROM twin_entities`), count(`SELECT count(*) FROM user_gateways`)}
	t.Run("admission and authoritative read", func(t *testing.T) {
		q := request(newGateway(), ActionProvision)
		result, err := r.BeginOperation(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		if !result.Execute || result.Metadata.CredentialVersion != 1 || result.Operation.Previous != nil || result.Metadata.LastOperationID != q.OperationID {
			t.Fatal("invalid intent")
		}
		m, err := r.GetMetadata(ctx, q.Input.GatewayID)
		if err != nil || m != result.Metadata {
			t.Fatal("metadata mismatch")
		}
		o, err := r.ResolveCommitAmbiguity(ctx, q.OperationID)
		if err != nil || o.OperationID != q.OperationID {
			t.Fatal("lookup failed")
		}
		_, err = r.BeginOperation(ctx, q)
		requireCode(t, err, CodeOperationInProgress)
		cross := q
		cross.Input.GatewayID = newGateway()
		_, err = r.BeginOperation(ctx, cross)
		requireCode(t, err, CodeIdempotencyConflict)
		changed := q
		changed.Input.Action = ActionRevoke
		_, err = r.BeginOperation(ctx, changed)
		requireCode(t, err, CodeIdempotencyConflict)
		exec(`DELETE FROM platform_admins WHERE user_id=$1`, actor)
		_, err = r.BeginOperation(ctx, q)
		requireCode(t, err, CodeForbidden)
		exec(`INSERT INTO platform_admins(user_id) VALUES($1)`, actor)
	})
	t.Run("unknown and invalid no writes", func(t *testing.T) {
		n := count(`SELECT count(*) FROM gateway_mqtt_credential_events`)
		_, err := r.BeginOperation(ctx, request("unknown_"+uuid.New().String(), ActionProvision))
		requireCode(t, err, CodeNotFound)
		for _, q := range []BeginRequest{request("backend_service", ActionProvision), request("bad/id", ActionProvision), request(newGateway(), Action("raw")), {ActorUserID: actor, OperationID: uuid.New(), Input: MutationInput{GatewayID: newGateway(), Action: ActionProvision}}} {
			_, err = r.BeginOperation(ctx, q)
			requireCode(t, err, CodeInvalidRequest)
		}
		if count(`SELECT count(*) FROM gateway_mqtt_credential_events`) != n {
			t.Fatal("invalid request wrote intent")
		}
		_, err = r.ResolveCommitAmbiguity(ctx, uuid.New())
		var unknown *CommitOutcomeUnknown
		if !errorsAsUnknown(err, &unknown) {
			t.Fatal("absence claimed rollback")
		}
	})
	t.Run("actual races", func(t *testing.T) {
		for _, same := range []bool{true, false} {
			g := newGateway()
			q := request(g, ActionProvision)
			start := make(chan struct{})
			var wg sync.WaitGroup
			results := make(chan error, 8)
			for i := 0; i < 8; i++ {
				next := q
				next.OperationID = uuid.New()
				if !same {
					next.Input.IdempotencyKey = uuid.New()
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					v, e := r.BeginOperation(ctx, next)
					if e == nil && !v.Execute {
						e = &DomainError{Code: CodeInternalError}
					}
					results <- e
				}()
			}
			close(start)
			wg.Wait()
			close(results)
			success := 0
			for e := range results {
				if e == nil {
					success++
				} else {
					requireCode(t, e, CodeOperationInProgress)
				}
			}
			if success != 1 {
				t.Fatalf("race admitted %d", success)
			}
		}
	})
	t.Run("cross gateway simultaneous key conflict", func(t *testing.T) {
		q := request(newGateway(), ActionProvision)
		other := q
		other.OperationID, other.Input.GatewayID = uuid.New(), newGateway()
		start := make(chan struct{})
		results := make(chan error, 2)
		for _, next := range []BeginRequest{q, other} {
			go func() { <-start; _, err := r.BeginOperation(ctx, next); results <- err }()
		}
		close(start)
		success := 0
		for i := 0; i < 2; i++ {
			err := <-results
			if err == nil {
				success++
			} else {
				requireCode(t, err, CodeIdempotencyConflict)
			}
		}
		if success != 1 {
			t.Fatal("cross gateway key admitted twice")
		}
	})
	t.Run("active revoke preserves generation and atomic collision", func(t *testing.T) {
		g := newGateway()
		q := request(g, ActionProvision)
		_, err := r.BeginOperation(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		finalizeFixture(`UPDATE gateway_mqtt_credential_events SET status='succeeded',phase='finalize',ram_applied=true,snapshot_observed=true,fresh_positive_verified=true,updated_at=now(),completed_at=now() WHERE operation_id=$1`, q.OperationID,
			`UPDATE gateway_mqtt_credentials c SET status='active',activated_at=e.updated_at,updated_at=e.updated_at FROM gateway_mqtt_credential_events e WHERE c.gateway_id=$1 AND e.operation_id=c.last_operation_id`, g)
		exec(`UPDATE mqtt_credential_maintenance SET status='completed',broker_epoch=repeat('a',64),completed_at=now(),updated_at=now() WHERE operation_id=$1`, q.OperationID)
		v, err := r.BeginOperation(ctx, request(g, ActionRevoke))
		if err != nil || v.Operation.CredentialVersion != 1 || v.Operation.Previous.Status != CredentialActive || v.Metadata.Status != CredentialRevoking {
			t.Fatal("revoke allocation changed", err)
		}
		collision := request(newGateway(), ActionProvision)
		collision.OperationID = q.OperationID
		_, err = r.BeginOperation(ctx, collision)
		requireCode(t, err, CodeServiceUnavailable)
		_, err = r.GetMetadata(ctx, collision.Input.GatewayID)
		requireCode(t, err, CodeNotFound)
	})
	t.Run("commit timeout gives no execute permission", func(t *testing.T) {
		g := newGateway()
		// Deferred trigger delays COMMIT only, after all intent reads/writes succeeded.
		// Fixture-only DDL is performed by the isolated administrator, not app role.
		exec(`CREATE FUNCTION task263a_delay_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.gateway_id = TG_ARGV[0] THEN PERFORM pg_sleep(0.3); END IF; RETURN NEW; END $$`)
		// Identifier and argument are fixture-owned; no request text is interpolated.
		exec(`CREATE CONSTRAINT TRIGGER task263a_commit_delay AFTER INSERT ON gateway_mqtt_credential_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION task263a_delay_commit('` + g + `')`)
		defer exec(`DROP TRIGGER task263a_commit_delay ON gateway_mqtt_credential_events; DROP FUNCTION task263a_delay_commit()`)
		short, err := NewPostgresRepository(app, 100*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		q := request(g, ActionProvision)
		v, err := short.BeginOperation(ctx, q)
		var unknown *CommitOutcomeUnknown
		if !errors.As(err, &unknown) || unknown.OperationID != q.OperationID || v.Execute {
			t.Fatal("commit timeout granted execution", err)
		}
		_, err = r.ResolveCommitAmbiguity(ctx, q.OperationID)
		if err != nil {
			requireCode(t, err, CodeFinalizationPending)
		}
	})
	t.Run("previous generation history replay failure and current metadata", func(t *testing.T) {
		g := newGateway()
		q := request(g, ActionProvision)
		v, err := r.BeginOperation(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		finalizeFixture(`UPDATE gateway_mqtt_credential_events SET status='succeeded',phase='finalize',ram_applied=true,snapshot_observed=true,fresh_positive_verified=true,updated_at=now(),completed_at=now() WHERE operation_id=$1`, q.OperationID,
			`UPDATE gateway_mqtt_credentials c SET status='active',activated_at=e.updated_at,updated_at=e.updated_at FROM gateway_mqtt_credential_events e WHERE c.gateway_id=$1 AND e.operation_id=c.last_operation_id`, g)
		v, err = r.BeginOperation(ctx, q)
		if err != nil || v.Execute || v.Replay == nil || v.Replay.SecretReturned || v.Replay.NextAction == "" {
			t.Fatal("unsafe replay", err)
		}
		rotate := request(g, ActionRotate)
		exec(`UPDATE mqtt_credential_maintenance SET status='completed',broker_epoch=repeat('a',64),completed_at=now(),updated_at=now() WHERE operation_id=$1`, q.OperationID)
		v, err = r.BeginOperation(ctx, rotate)
		if err != nil || v.Operation.Previous == nil || v.Operation.Previous.Status != CredentialActive || v.Operation.CredentialVersion != 2 {
			t.Fatal("snapshot failure", err)
		}
		finalizeFixture(`UPDATE gateway_mqtt_credential_events SET status='failed',phase='finalize',error_code='credential_runtime_busy',updated_at=now(),completed_at=now() WHERE operation_id=$1`, rotate.OperationID,
			`UPDATE gateway_mqtt_credentials c SET status='active',credential_version=1,updated_at=e.updated_at,last_error_code=e.error_code FROM gateway_mqtt_credential_events e WHERE c.gateway_id=$1 AND e.operation_id=c.last_operation_id`, g)
		next := request(g, ActionRotate)
		exec(`UPDATE mqtt_credential_maintenance SET status='completed',broker_epoch=repeat('a',64),completed_at=now(),updated_at=now() WHERE operation_id=$1`, rotate.OperationID)
		v, err = r.BeginOperation(ctx, next)
		if err != nil || v.Operation.CredentialVersion != 3 {
			t.Fatal("attempt reused", err)
		}
		v, err = r.BeginOperation(ctx, rotate)
		if err != nil || v.Replay == nil || v.Replay.ReplayedStatus != OperationFailed || v.Replay.ErrorCode == nil || *v.Replay.ErrorCode != CodeRuntimeBusy || v.Metadata.CredentialVersion != 3 || v.Metadata.Status != CredentialRotating {
			t.Fatal("terminal replay lost outcome/current metadata", err)
		}
		finalizeFixture(`UPDATE gateway_mqtt_credential_events SET status='recovery_needed',phase='recovery',updated_at=now() WHERE operation_id=$1`, next.OperationID,
			`UPDATE gateway_mqtt_credentials c SET status='recovery_needed',credential_version=e.previous_credential_version,updated_at=e.updated_at FROM gateway_mqtt_credential_events e WHERE c.gateway_id=$1 AND e.operation_id=c.last_operation_id`, g)
		_, err = r.BeginOperation(ctx, next)
		requireCode(t, err, CodeRecoveryRequired)
		_, err = r.BeginOperation(ctx, request(g, ActionProvision))
		requireCode(t, err, CodeRecoveryRequired)
	})
	t.Run("legacy duplicates and missing proof", func(t *testing.T) {
		g := os.Getenv("TASK263A_LEGACY_PENDING")
		_, err := r.BeginOperation(ctx, request(g, ActionProvision))
		requireCode(t, err, CodeOperationInProgress)
		rows, err := app.Query(ctx, `SELECT `+operationColumns+` FROM gateway_mqtt_credential_events WHERE gateway_id=$1 ORDER BY action`, g)
		if err != nil {
			t.Fatal("legacy read failed")
		}
		defer rows.Close()
		n := 0
		recoverSeen := false
		for rows.Next() {
			p, err := scanOperation(rows)
			if err != nil || !p.legacy {
				t.Fatal("legacy lost", err)
			}
			n++
			if p.Action == Action("recover") {
				recoverSeen = true
				if p.ActorUserID != uuid.Nil || p.CredentialVersion != 0 {
					t.Fatal("NULL fabricated")
				}
			}
		}
		if rows.Err() != nil || n != 2 || !recoverSeen {
			t.Fatal("legacy duplicates lost")
		}
		g = os.Getenv("TASK263A_LEGACY_TERMINAL")
		m, err := r.GetMetadata(ctx, g)
		if err != nil || m.Status != CredentialRevoked {
			t.Fatal("legacy metadata failed")
		}
		p, err := scanOperation(app.QueryRow(ctx, `SELECT `+operationColumns+` FROM gateway_mqtt_credential_events WHERE operation_id=$1`, m.LastOperationID))
		if err != nil || !p.legacy || p.Evidence != (VerificationEvidence{}) {
			t.Fatal("legacy proof invented")
		}
		_, err = r.ResolveCommitAmbiguity(ctx, m.LastOperationID)
		requireCode(t, err, CodeFinalizationPending)
		q := request(g, ActionRevoke)
		v, err := r.BeginOperation(ctx, q)
		if err != nil || v.Execute || v.Metadata.CredentialVersion != 3 {
			t.Fatal("revoked no-op", err)
		}
		exec(`DELETE FROM platform_admins WHERE user_id=$1`, actor)
		_, err = r.BeginOperation(ctx, q)
		requireCode(t, err, CodeForbidden)
		exec(`INSERT INTO platform_admins(user_id) VALUES($1)`, actor)
		v, err = r.BeginOperation(ctx, request(g, ActionProvision))
		if err != nil || v.Operation.CredentialVersion != 4 || v.Operation.Previous.Status != CredentialRevoked {
			t.Fatal("reprovision failed", err)
		}
	})
	t.Run("deleted actor remains readable not replayable", func(t *testing.T) {
		old := uuid.New()
		exec(`INSERT INTO profiles(id) VALUES($1)`, old)
		exec(`INSERT INTO platform_admins(user_id) VALUES($1)`, old)
		q := request(newGateway(), ActionProvision)
		q.ActorUserID = old
		_, err := r.BeginOperation(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		exec(`DELETE FROM profiles WHERE id=$1`, old)
		o, err := r.ResolveCommitAmbiguity(ctx, q.OperationID)
		if err != nil || o.ActorUserID != uuid.Nil {
			t.Fatal("NULL actor scan failed", err)
		}
		_, err = r.BeginOperation(ctx, q)
		requireCode(t, err, CodeForbidden)
	})
	for i, sql := range []string{`SELECT count(*) FROM sensors`, `SELECT count(*) FROM twin_entities`, `SELECT count(*) FROM user_gateways`} {
		if count(sql) != before[i] {
			t.Fatal("unrelated resource mutated")
		}
	}
}

func errorsAsUnknown(err error, target **CommitOutcomeUnknown) bool { return errors.As(err, target) }

package digitaltwin_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/digitaltwin"
)

type controlFixture struct {
	admin, app                        *pgxpool.Pool
	ctx                               context.Context
	owner, operator, viewer, outsider uuid.UUID
	gw, urn                           string
	entity                            uuid.UUID
	repo                              *digitaltwin.PostgresControlRepository
}

func newControlFixture(t *testing.T) *controlFixture {
	return newControlFixtureGateway(t, "")
}

func newControlFixtureGateway(t *testing.T, gateway string) *controlFixture {
	t.Helper()
	if os.Getenv("CONTROL_TEST_ISOLATED") != "1" {
		t.Skip("requires owned native PostgreSQL fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	t.Cleanup(cancel)
	f := &controlFixture{ctx: ctx, owner: uuid.New(), operator: uuid.New(), viewer: uuid.New(), outsider: uuid.New(), gw: "c" + strings.ReplaceAll(uuid.NewString(), "-", "")}
	if gateway != "" {
		f.gw = gateway
	}
	var err error
	f.admin, err = pgxpool.New(ctx, os.Getenv("CONTROL_TEST_SETUP_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.admin.Close)
	f.app, err = pgxpool.New(ctx, os.Getenv("CONTROL_TEST_BACKEND_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.app.Close)
	var principal string
	if err = f.app.QueryRow(ctx, `SELECT current_user`).Scan(&principal); err != nil || principal != "iot_backend_app" {
		t.Fatalf("runtime principal: %q %v", principal, err)
	}
	for _, actor := range []uuid.UUID{f.owner, f.operator, f.viewer, f.outsider} {
		f.exec(t, `INSERT INTO auth.users(id,email) VALUES($1,$2)`, actor, actor.String()+"@fixture.invalid")
	}
	f.exec(t, `INSERT INTO gateways(gateway_id,name) VALUES($1,'control fixture')`, f.gw)
	for i, actor := range []uuid.UUID{f.owner, f.operator, f.viewer} {
		f.exec(t, `INSERT INTO user_gateways(user_id,gateway_id,role) VALUES($1,$2,$3)`, actor, f.gw, []string{"owner", "operator", "viewer"}[i])
	}
	f.exec(t, `INSERT INTO platform_admins(user_id) VALUES($1)`, f.outsider)
	f.exec(t, `INSERT INTO twin_entities(entity_id,entity_type,gateway_id,name) VALUES($1,'Gateway',$2,'control fixture')`, digitaltwin.GatewayURNPrefix+f.gw, f.gw)
	f.exec(t, `INSERT INTO twin_states(entity_id) SELECT id FROM twin_entities WHERE gateway_id=$1`, f.gw)
	f.urn, f.entity = f.sensor(t, "s1")
	f.repo, err = digitaltwin.NewPostgresControlRepository(f.app, digitaltwin.DefaultControlConfig())
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *controlFixture) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := f.admin.Exec(f.ctx, sql, args...); err != nil {
		t.Fatal(err)
	}
}

func (f *controlFixture) sensor(t *testing.T, sensor string) (string, uuid.UUID) {
	t.Helper()
	urn := digitaltwin.SensorURNPrefix + f.gw + ":" + sensor
	f.exec(t, `INSERT INTO sensors(gateway_id,sensor_id,name,unit) VALUES($1,$2,'sensor','degC')`, f.gw, sensor)
	var id uuid.UUID
	if err := f.admin.QueryRow(f.ctx, `INSERT INTO twin_entities(entity_id,entity_type,gateway_id,name) VALUES($1,'Sensor',$2,'sensor') RETURNING id`, urn, f.gw).Scan(&id); err != nil {
		t.Fatal(err)
	}
	f.exec(t, `INSERT INTO twin_states(entity_id) VALUES($1)`, id)
	f.exec(t, `INSERT INTO twin_relationships(source_entity_id,relationship_type,target_entity_id) SELECT id,'hasSensor',$2 FROM twin_entities WHERE entity_id=$1`, digitaltwin.GatewayURNPrefix+f.gw, id)
	return urn, id
}

func controlRequest(t *testing.T, hz, version, key string) digitaltwin.DesiredRequest {
	t.Helper()
	r, err := digitaltwin.ParseDesiredRequest([]byte(fmt.Sprintf(`{"frequency_hz":%s,"expected_desired_version":%q}`, hz, version)), key)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (f *controlFixture) counts(t *testing.T) string {
	t.Helper()
	var s string
	err := f.admin.QueryRow(f.ctx, `SELECT jsonb_build_array(desired_state,desired_version,last_desired_by,last_desired_at,reported_state,reported_version,last_reported_at,
	 (SELECT count(*) FROM twin_commands WHERE entity_id=$1),
	 (SELECT count(*) FROM twin_outbox WHERE command_id IN(SELECT command_id FROM twin_commands WHERE entity_id=$1)),
	 (SELECT count(*) FROM twin_command_requests WHERE entity_id=$1))::text FROM twin_states WHERE entity_id=$1`, f.entity).Scan(&s)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestNativeControlPermissionsAndReplay(t *testing.T) {
	for _, role := range []string{"owner", "operator", "viewer", "nonmember", "admin_without_membership", "revoked"} {
		t.Run(role, func(t *testing.T) {
			f := newControlFixture(t)
			actor := f.owner
			switch role {
			case "operator":
				actor = f.operator
			case "viewer":
				actor = f.viewer
			case "nonmember":
				actor = uuid.New()
			case "admin_without_membership":
				actor = f.outsider
			case "revoked":
				f.exec(t, `DELETE FROM user_gateways WHERE user_id=$1`, f.owner)
			}
			before := f.counts(t)
			req := controlRequest(t, "0.1", "0", "permission-key")
			r, err := f.repo.Admit(f.ctx, actor, f.urn, req)
			if role != "owner" && role != "operator" {
				want := digitaltwin.ErrNotFound
				if role == "viewer" {
					want = digitaltwin.ErrForbidden
				}
				if !errors.Is(err, want) {
					t.Fatalf("want %v got %v", want, err)
				}
				if f.counts(t) != before {
					t.Fatal("denial changed state/commands/outbox")
				}
				return
			}
			if err != nil || r.StatusCode != 202 || r.CommandID == nil {
				t.Fatalf("admission: %+v %v", r, err)
			}
			replay, err := f.repo.Admit(f.ctx, actor, f.urn, controlRequest(t, "1e-1", "0", "permission-key"))
			if err != nil || string(replay.Response) != string(r.Response) {
				t.Fatalf("exact replay: %+v %v", replay, err)
			}
			for _, hz := range []string{"0.2", "0.10000000001"} {
				_, err = f.repo.Admit(f.ctx, actor, f.urn, controlRequest(t, hz, "0", "permission-key"))
				if !errors.Is(err, digitaltwin.ErrIdempotencyConflict) {
					t.Fatalf("exact rational fingerprint %s: %v", hz, err)
				}
			}
			_, err = f.repo.Admit(f.ctx, actor, f.urn, controlRequest(t, "1", "0", "stale"))
			if !errors.Is(err, digitaltwin.ErrDesiredVersionConflict) {
				t.Fatalf("stale: %v", err)
			}
			_, err = f.repo.Admit(f.ctx, actor, f.urn, controlRequest(t, "1", "1", "busy"))
			if !errors.Is(err, digitaltwin.ErrCommandInProgress) {
				t.Fatalf("busy: %v", err)
			}
			var payload []byte
			var topic string
			var projection bool
			if err = f.admin.QueryRow(f.ctx, `SELECT topic,payload_bytes,convert_from(payload_bytes,'UTF8')::jsonb=payload FROM twin_outbox WHERE command_id=$1`, *r.CommandID).Scan(&topic, &payload, &projection); err != nil || !projection {
				t.Fatalf("stable payload projection: %v", err)
			}
			if !json.Valid(payload) {
				t.Fatal("invalid payload")
			}
			if _, err = digitaltwin.ParseMQTTCommand(topic, payload, false); err != nil {
				t.Fatalf("stored command violates domain envelope: %v", err)
			}
			f.exec(t, `UPDATE twin_commands SET status='failed',completed_at=clock_timestamp() WHERE command_id=$1`, *r.CommandID)
			replay, err = f.repo.Admit(f.ctx, actor, f.urn, req)
			if err != nil || string(replay.Response) != string(r.Response) {
				t.Fatalf("202 must remain original after failure: %v", err)
			}
			other := f.operator
			if actor == f.operator {
				other = f.owner
			}
			if _, err = f.repo.Admit(f.ctx, other, f.urn, controlRequest(t, "0.2", "1", "permission-key")); err != nil {
				t.Fatalf("same key belongs to a different actor scope: %v", err)
			}
			replay, err = f.repo.Admit(f.ctx, actor, f.urn, req)
			if err != nil || string(replay.Response) != string(r.Response) {
				t.Fatalf("original replay after newer version: %v", err)
			}
			f.exec(t, `DELETE FROM user_gateways WHERE user_id=$1`, actor)
			_, err = f.repo.Admit(f.ctx, actor, f.urn, req)
			if !errors.Is(err, digitaltwin.ErrNotFound) {
				t.Fatalf("revoked replay: %v", err)
			}
		})
	}
}

func TestNativeControlActiveConstraint(t *testing.T) {
	f := newControlFixture(t)
	for _, status := range []string{"pending", "published", "acknowledged"} {
		t.Run(status, func(t *testing.T) {
			f.exec(t, `DELETE FROM twin_commands WHERE entity_id=$1`, f.entity)
			f.exec(t, `INSERT INTO twin_commands(command_id,entity_id,action,status,issued_at,expires_at) VALUES($1,$2,'legacy_fixture',$3,clock_timestamp(),clock_timestamp()+interval '1 minute')`, uuid.New(), f.entity, status)
			_, err := f.app.Exec(f.ctx, `INSERT INTO twin_commands(command_id,entity_id,action,status,issued_at,expires_at) VALUES($1,$2,'legacy_fixture','pending',clock_timestamp(),clock_timestamp()+interval '1 minute')`, uuid.New(), f.entity)
			var pe *pgconn.PgError
			if !errors.As(err, &pe) || pe.Code != "23505" {
				t.Fatalf("one-active Sensor DB protection missing: %v", err)
			}
		})
	}
}

func TestNativeControlNoopReexecuteExpiry(t *testing.T) {
	for _, status := range []string{"pending", "published", "acknowledged"} {
		t.Run(status, func(t *testing.T) {
			f := newControlFixture(t)
			f.exec(t, `UPDATE twin_states SET desired_state='{"sample_interval_us":10000000,"keep":true}',reported_state='{"value":42,"configuration":{"sample_interval_us":10000000,"desired_version":"0"}}' WHERE entity_id=$1`, f.entity)
			r, err := f.repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.1", "0", "noop"))
			if err != nil || r.StatusCode != 200 || r.CommandID != nil {
				t.Fatalf("no-op: %+v %v", r, err)
			}
			before := f.counts(t)
			f.exec(t, `UPDATE twin_states SET reported_state='{"value":42}' WHERE entity_id=$1`, f.entity)
			rp, err := f.repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.10", "0", "noop"))
			if err != nil || string(rp.Response) != string(r.Response) {
				t.Fatalf("200 exact replay %v", err)
			}
			if before == f.counts(t) {
				t.Fatal("test setup did not change confirmation")
			}
			old := uuid.New()
			f.exec(t, `INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,issued_at,expires_at) VALUES($1,$2,'update_configuration','{"sampling_interval_seconds":5}',$3,0,clock_timestamp()-interval '2 minute',clock_timestamp()-interval '1 minute')`, old, f.entity, status)
			f.exec(t, `INSERT INTO twin_outbox(command_id,topic,payload,status) VALUES($1,'legacy-fixture','{"sentinel":true}','pending')`, old)
			r, err = f.repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.1", "0", "after-expiry"))
			if err != nil || r.StatusCode != 202 {
				t.Fatalf("reuse expired %s: %+v %v", status, r, err)
			}
			var commandStatus, outboxStatus, params string
			if err = f.admin.QueryRow(f.ctx, `SELECT c.status,o.status,c.parameters::text FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE command_id=$1`, old).Scan(&commandStatus, &outboxStatus, &params); err != nil || commandStatus != "timeout" || outboxStatus != "failed" || !strings.Contains(params, "sampling_interval_seconds") {
				t.Fatalf("expiry/preservation: %s %s %s %v", commandStatus, outboxStatus, params, err)
			}
			f.exec(t, `UPDATE twin_commands SET status='failed',completed_at=clock_timestamp() WHERE command_id=$1`, *r.CommandID)
			r, err = f.repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.1", "1", "explicit-reexecute"))
			if err != nil || r.StatusCode != 202 {
				t.Fatalf("equal desired after failed must execute: %v", err)
			}
			var preserved bool
			if err = f.admin.QueryRow(f.ctx, `SELECT desired_version=2 AND desired_state @> '{"keep":true,"sample_interval_us":10000000}'::jsonb AND reported_state='{"value":42}'::jsonb AND reported_version=0 AND last_reported_at IS NULL FROM twin_states WHERE entity_id=$1`, f.entity).Scan(&preserved); err != nil || !preserved {
				t.Fatalf("desired merge / observation separation: %v", err)
			}
		})
	}
}

func TestNativeControlConcurrent(t *testing.T) {
	for _, sameKey := range []bool{true, false} {
		t.Run(fmt.Sprint(sameKey), func(t *testing.T) {
			f := newControlFixture(t)
			start := make(chan struct{})
			var wg sync.WaitGroup
			results := make([]digitaltwin.RequestReceipt, 2)
			errs := make([]error, 2)
			for i := range 2 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					key := "same"
					if !sameKey {
						key = fmt.Sprint(i)
					}
					results[i], errs[i] = f.repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "60", "0", key))
				}()
			}
			close(start)
			wg.Wait()
			if sameKey {
				if errs[0] != nil || errs[1] != nil || string(results[0].Response) != string(results[1].Response) {
					t.Fatalf("concurrent same key: %v", errs)
				}
			} else {
				n := 0
				for _, err := range errs {
					if err == nil {
						n++
					} else if !errors.Is(err, digitaltwin.ErrDesiredVersionConflict) {
						t.Fatal(err)
					}
				}
				if n != 1 {
					t.Fatalf("two-key accepted=%d", n)
				}
			}
			var n int
			if err := f.admin.QueryRow(f.ctx, `SELECT count(*) FROM twin_commands WHERE entity_id=$1`, f.entity).Scan(&n); err != nil || n != 1 {
				t.Fatalf("commands %d %v", n, err)
			}
		})
	}
	f := newControlFixture(t)
	urn2, _ := f.sensor(t, "s2")
	for _, urn := range []string{f.urn, urn2} {
		if _, err := f.repo.Admit(f.ctx, f.owner, urn, controlRequest(t, "1", "0", urn)); err != nil {
			t.Fatalf("independent Sensor on same Gateway: %v", err)
		}
	}
}

func TestNativeControlRollback(t *testing.T) {
	for _, write := range []string{"UPDATE public.twin_states", "INSERT INTO public.twin_commands", "INSERT INTO public.twin_outbox", "INSERT INTO public.twin_command_requests"} {
		t.Run(write, func(t *testing.T) {
			f := newControlFixture(t)
			old := uuid.New()
			f.exec(t, `INSERT INTO twin_commands(command_id,entity_id,action,status,issued_at,expires_at) VALUES($1,$2,'legacy_fixture','acknowledged',clock_timestamp()-interval '2 minute',clock_timestamp()-interval '1 minute')`, old, f.entity)
			f.exec(t, `INSERT INTO twin_outbox(command_id,topic,payload) VALUES($1,'fixture/legacy','{}')`, old)
			before := f.counts(t)
			injected := false
			db := &faultInjectingTxDatabase{underlying: f.app, onTxBegin: func(tx pgx.Tx) pgx.Tx {
				return &faultInjectingTx{Tx: tx, onExec: func(ctx context.Context, tx pgx.Tx, sql string, _ ...any) (pgconn.CommandTag, error, bool) {
					if strings.Contains(sql, write) {
						injected = true
						_, err := tx.Exec(ctx, `SELECT 1/0`)
						return pgconn.CommandTag{}, err, true
					}
					return pgconn.CommandTag{}, nil, false
				}}
			}}
			repo, err := digitaltwin.NewPostgresControlRepository(db, digitaltwin.DefaultControlConfig())
			if err != nil {
				t.Fatal(err)
			}
			_, err = repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "1", "0", "rollback"))
			if err == nil || !injected {
				t.Fatalf("fault not exercised %v %v", injected, err)
			}
			if f.counts(t) != before {
				t.Fatal("partial intent survived rollback")
			}
			var expiryRolledBack bool
			if err = f.admin.QueryRow(f.ctx, `SELECT c.status='acknowledged' AND c.completed_at IS NULL AND o.status='pending' FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE command_id=$1`, old).Scan(&expiryRolledBack); err != nil || !expiryRolledBack {
				t.Fatalf("expiry/outbox stop survived admission rollback: %v", err)
			}
			var receipts int
			if err = f.admin.QueryRow(f.ctx, `SELECT count(*) FROM twin_command_requests WHERE entity_id=$1`, f.entity).Scan(&receipts); err != nil || receipts != 0 {
				t.Fatalf("receipt survived: %d %v", receipts, err)
			}
		})
	}
}

func TestNativeControlScopedQueries(t *testing.T) {
	f := newControlFixture(t)
	r, err := f.repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "60", "0", "query"))
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, `UPDATE twin_commands SET status='failed',completed_at=clock_timestamp(),error_code='APPLY_FAILED',error_message='SECRET raw diagnostic' WHERE command_id=$1`, *r.CommandID)
	for range 104 {
		f.exec(t, `INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,issued_at,expires_at,error_code,error_message) VALUES($1,$2,'legacy_fixture','{"secret":"never expose"}','failed',1,'2026-01-01Z','2026-01-02Z','UNTRUSTED','SECRET')`, uuid.New(), f.entity)
	}
	page, err := f.repo.ListCommands(f.ctx, f.viewer, f.urn, 0, "")
	if err != nil || len(page.Commands) != 20 || page.NextCursor == "" {
		t.Fatalf("default page %+v %v", page, err)
	}
	all := map[string]bool{}
	cursor := ""
	for {
		page, err = f.repo.ListCommands(f.ctx, f.viewer, f.urn, 20, cursor)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range page.Commands {
			if all[c.CommandID] {
				t.Fatal("duplicate keyset row")
			}
			all[c.CommandID] = true
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(all) != 105 {
		t.Fatalf("missing keyset rows: %d", len(all))
	}
	page, err = f.repo.ListCommands(f.ctx, f.viewer, f.urn, 100, "")
	if err != nil || len(page.Commands) != 100 {
		t.Fatalf("max page %v", err)
	}
	for _, limit := range []int{-1, 101} {
		if _, err = f.repo.ListCommands(f.ctx, f.viewer, f.urn, limit, ""); !errors.Is(err, digitaltwin.ErrInvalidInput) {
			t.Fatalf("limit %d: %v", limit, err)
		}
	}
	for _, cursor := range []string{"garbage", strings.Repeat("x", 513)} {
		if _, err = f.repo.ListCommands(f.ctx, f.viewer, f.urn, 20, cursor); !errors.Is(err, digitaltwin.ErrInvalidInput) {
			t.Fatalf("cursor %v", err)
		}
	}
	view, err := f.repo.GetCommand(f.ctx, f.viewer, f.urn, *r.CommandID)
	if err != nil || view.ErrorMessage != "configuration apply failed" || view.Parameters == nil {
		t.Fatalf("safe detail %+v %v", view, err)
	}
	data, _ := json.Marshal(page)
	if strings.Contains(string(data), "SECRET") || strings.Contains(string(data), "never expose") {
		t.Fatal("raw diagnostics/parameters leaked")
	}
	urn2, _ := f.sensor(t, "s2")
	if _, err = f.repo.GetCommand(f.ctx, f.viewer, urn2, *r.CommandID); !errors.Is(err, digitaltwin.ErrNotFound) {
		t.Fatalf("cross Sensor detail %v", err)
	}
	if _, err = f.repo.ListCommands(f.ctx, f.viewer, urn2, 20, page.NextCursor); !errors.Is(err, digitaltwin.ErrInvalidInput) {
		t.Fatalf("cross Sensor cursor %v", err)
	}
	for _, actor := range []uuid.UUID{uuid.New(), f.outsider} {
		if _, err = f.repo.GetCommand(f.ctx, actor, f.urn, *r.CommandID); !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("nonmember detail %v", err)
		}
		if _, err = f.repo.ListCommands(f.ctx, actor, f.urn, 20, ""); !errors.Is(err, digitaltwin.ErrNotFound) {
			t.Fatalf("nonmember list %v", err)
		}
	}
	// Seed canonical evidence to exercise the read projection only. No runtime
	// result admission/ingress/state merge is implemented by this task.
	confirmedURN, _ := f.sensor(t, "confirmed")
	accepted, err := f.repo.Admit(f.ctx, f.owner, confirmedURN, controlRequest(t, "60", "0", "confirmed-query"))
	if err != nil {
		t.Fatal(err)
	}
	confirmed, err := f.repo.GetCommand(f.ctx, f.viewer, confirmedURN, *accepted.CommandID)
	if err != nil {
		t.Fatal(err)
	}
	completed := confirmed.IssuedAt.Add(time.Millisecond)
	result := digitaltwin.CommandResult{ProtocolVersion: 1, MessageType: "command_result", CommandID: confirmed.CommandID, GatewayID: f.gw, EntityID: confirmedURN, DesiredVersion: *confirmed.DesiredVersion, Status: digitaltwin.CommandSucceeded, ReportedState: confirmed.Parameters, CompletedAt: completed}
	resultBytes, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, `UPDATE twin_commands SET status='succeeded',completed_at=$3,result_bytes=$2,result_hash=encode(sha256($2),'hex'),result_received_at=$3 WHERE command_id=$1`, confirmed.CommandID, resultBytes, completed)
	confirmed, err = f.repo.GetCommand(f.ctx, f.viewer, confirmedURN, confirmed.CommandID)
	if err != nil || confirmed.Result == nil || confirmed.Result.ReportedState.SampleIntervalUS != 16667 {
		t.Fatalf("safe canonical result read %+v %v", confirmed, err)
	}
	f.exec(t, `DELETE FROM user_gateways WHERE user_id=$1`, f.viewer)
	if _, err = f.repo.GetCommand(f.ctx, f.viewer, f.urn, *r.CommandID); !errors.Is(err, digitaltwin.ErrNotFound) {
		t.Fatalf("revoked detail %v", err)
	}
}

type controlCommitTx struct {
	pgx.Tx
	after bool
}

func (tx *controlCommitTx) Commit(ctx context.Context) error {
	if tx.after {
		if err := tx.Tx.Commit(ctx); err != nil {
			return err
		}
	}
	return context.DeadlineExceeded
}

func TestNativeControlCommitAndOverflow(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			f := newControlFixture(t)
			req := controlRequest(t, "1", "0", "uncertain")
			db := &faultInjectingTxDatabase{underlying: f.app, onTxBegin: func(tx pgx.Tx) pgx.Tx { return &controlCommitTx{Tx: tx, after: after} }}
			repo, _ := digitaltwin.NewPostgresControlRepository(db, digitaltwin.DefaultControlConfig())
			if _, err := repo.Admit(f.ctx, f.owner, f.urn, req); !errors.Is(err, digitaltwin.ErrControlCommitUncertain) || !errors.Is(err, digitaltwin.ErrDependencyUnavailable) {
				t.Fatalf("ambiguous commit %v", err)
			}
			r, err := f.repo.Admit(f.ctx, f.owner, f.urn, req)
			if err != nil || r.StatusCode != 202 {
				t.Fatalf("safe retry %+v %v", r, err)
			}
			var n int
			if err = f.admin.QueryRow(f.ctx, `SELECT count(*) FROM twin_commands WHERE entity_id=$1`, f.entity).Scan(&n); err != nil || n != 1 {
				t.Fatalf("ambiguous retry duplicate %d %v", n, err)
			}
		})
	}
	f := newControlFixture(t)
	f.exec(t, `UPDATE twin_states SET desired_version=9223372036854775807 WHERE entity_id=$1`, f.entity)
	before := f.counts(t)
	if _, err := f.repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "1", "9223372036854775807", "overflow")); !errors.Is(err, digitaltwin.ErrDesiredVersionOverflow) {
		t.Fatalf("overflow: %v", err)
	}
	if before != f.counts(t) {
		t.Fatal("overflow mutated state")
	}
	f.exec(t, `UPDATE twin_states SET desired_state='{"sample_interval_us":1000000}',reported_state='{"configuration":{"sample_interval_us":1000000}}' WHERE entity_id=$1`, f.entity)
	r, err := f.repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "1", "9223372036854775807", "max-noop"))
	if err != nil || r.StatusCode != 200 || !strings.Contains(string(r.Response), `"desired_version":"9223372036854775807"`) {
		t.Fatalf("no increment at max, precision-safe no-op: %+v %v", r, err)
	}
}

func TestNativeControlCapacityAndBounds(t *testing.T) {
	f := newControlFixture(t)
	config := digitaltwin.DefaultControlConfig()
	// Capacity is deliberately store-global. Other test-owned graphs remain in
	// this isolated database until its owned container is destroyed by the runner.
	var pending, receipts int
	if err := f.admin.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM twin_outbox WHERE status='pending'),(SELECT count(*) FROM twin_command_requests)`).Scan(&pending, &receipts); err != nil {
		t.Fatal(err)
	}
	config.MaxPendingOutbox = pending + 1
	config.MaxReceipts = receipts + 2
	repo, err := digitaltwin.NewPostgresControlRepository(f.app, config)
	if err != nil {
		t.Fatal(err)
	}
	r, err := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "1", "0", "capacity"))
	if err != nil {
		t.Fatal(err)
	}
	urn2, _ := f.sensor(t, "s2")
	if _, err = repo.Admit(f.ctx, f.owner, urn2, controlRequest(t, "1", "0", "full")); !errors.Is(err, digitaltwin.ErrControlCapacity) {
		t.Fatalf("pending quota %v", err)
	}
	if _, err = repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "1", "0", "capacity")); err != nil {
		t.Fatalf("quota must not block replay %v", err)
	}
	f.exec(t, `UPDATE twin_outbox SET status='failed' WHERE command_id=$1`, *r.CommandID)
	if _, err = repo.Admit(f.ctx, f.owner, urn2, controlRequest(t, "1", "0", "second")); err != nil {
		t.Fatal(err)
	}
	urn3, _ := f.sensor(t, "s3")
	f.exec(t, `UPDATE twin_outbox SET status='failed' WHERE command_id IN(SELECT command_id FROM twin_commands WHERE entity_id IN(SELECT id FROM twin_entities WHERE gateway_id=$1))`, f.gw)
	if _, err = repo.Admit(f.ctx, f.owner, urn3, controlRequest(t, "1", "0", "receipt-full")); !errors.Is(err, digitaltwin.ErrControlCapacity) {
		t.Fatalf("receipt quota %v", err)
	}
	for _, hz := range []string{"100.00000000000000001", "0", "1e-128"} {
		if _, err = digitaltwin.ParseDesiredRequest([]byte(`{"frequency_hz":`+hz+`,"expected_desired_version":"0"}`), "invalid"); err == nil {
			t.Fatalf("bounds accepted %s", hz)
		}
	}
	// The representable 2^53 boundary must survive the actual command/outbox JSON.
	max := newControlFixture(t)
	req := controlRequest(t, "0.00000000011102230246251565404236316680908203125", "0", "max-interval")
	if req.Frequency.SampleIntervalUS() != 1<<53 {
		t.Fatalf("boundary setup %d", req.Frequency.SampleIntervalUS())
	}
	r, err = max.repo.Admit(max.ctx, max.owner, max.urn, req)
	if err != nil {
		t.Fatal(err)
	}
	var interval int64
	if err = max.admin.QueryRow(max.ctx, `SELECT (payload->'parameters'->>'sample_interval_us')::bigint FROM twin_outbox WHERE command_id=$1`, *r.CommandID).Scan(&interval); err != nil || interval != 1<<53 {
		t.Fatalf("max interval %d %v", interval, err)
	}
}

func TestNativeControlRevocationAndIndependentLocks(t *testing.T) {
	f := newControlFixture(t)
	urn2, _ := f.sensor(t, "s2")
	paused := make(chan struct{}, 1)
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	db := newPauseBeforeLockDB(f.app, func(sql string) bool { return strings.Contains(sql, "SELECT sensor_id FROM public.sensors") }, paused, release)
	repo, _ := digitaltwin.NewPostgresControlRepository(db, digitaltwin.DefaultControlConfig())
	done := make(chan error, 1)
	go func() { _, err := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "1", "0", "held")); done <- err }()
	select {
	case <-paused:
	case <-f.ctx.Done():
		t.Fatal("never reached Sensor lock")
	}
	// Both parent/membership SHARE locks coexist: a second Sensor is not busy.
	if _, err := f.repo.Admit(f.ctx, f.owner, urn2, controlRequest(t, "1", "0", "independent")); err != nil {
		t.Fatalf("same Gateway second Sensor %v", err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 100*time.Millisecond)
	defer cancel()
	tx, err := f.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `SELECT gateway_id FROM gateways WHERE gateway_id=$1 FOR UPDATE`, f.gw)
	_ = tx.Rollback(context.Background())
	if err == nil {
		t.Fatal("membership mutator's parent UPDATE bypassed admission SHARE")
	}
	close(release)
	released = true
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	// A mutation that obtains the Gateway lock first must be observed on admission.
	f2 := newControlFixture(t)
	tx, err = f2.admin.Begin(f2.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err = tx.Exec(f2.ctx, `SELECT gateway_id FROM gateways WHERE gateway_id=$1 FOR UPDATE`, f2.gw); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(f2.ctx, `DELETE FROM user_gateways WHERE user_id=$1 AND gateway_id=$2`, f2.owner, f2.gw); err != nil {
		t.Fatal(err)
	}
	done2 := make(chan error, 1)
	go func() {
		_, err := f2.repo.Admit(f2.ctx, f2.owner, f2.urn, controlRequest(t, "1", "0", "revocation-race"))
		done2 <- err
	}()
	if err = tx.Commit(f2.ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done2; !errors.Is(err, digitaltwin.ErrNotFound) {
		t.Fatalf("revocation before admission %v", err)
	}
}

func TestNativeControlSchemaGrantsAndEvidence(t *testing.T) {
	f := newControlFixture(t)
	r, err := f.repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "1", "0", "schema"))
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"anon", "authenticated"} {
		for _, table := range []string{"twin_states", "twin_commands", "twin_outbox", "twin_command_requests"} {
			column := map[string]string{"twin_states": "desired_state", "twin_commands": "parameters", "twin_outbox": "payload", "twin_command_requests": "idempotency_key"}[table]
			for _, sql := range []string{`DELETE FROM public.` + table + ` WHERE false`, `INSERT INTO public.` + table + ` DEFAULT VALUES`, `UPDATE public.` + table + ` SET ` + column + `=` + column + ` WHERE false`} {
				tx, err := f.admin.Begin(f.ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = tx.Exec(f.ctx, `SET LOCAL ROLE `+role); err != nil {
					t.Fatal(err)
				}
				_, err = tx.Exec(f.ctx, sql)
				_ = tx.Rollback(f.ctx)
				var pe *pgconn.PgError
				if !errors.As(err, &pe) || pe.Code != "42501" {
					t.Fatalf("human direct-write %s %s: %v", role, sql, err)
				}
			}
		}
	}
	for _, sql := range []string{
		`UPDATE twin_outbox SET payload='{"tampered":true}',payload_bytes=convert_to('{"tampered":true}','UTF8') WHERE command_id=$1`,
		`UPDATE twin_commands SET result_bytes=decode('00','hex'),result_hash=repeat('a',64),result_received_at=clock_timestamp() WHERE command_id=$1`,
		`UPDATE twin_commands SET late_result_bytes=repeat('a',8193)::bytea,late_result_hash=repeat('a',64),late_result_received_at=clock_timestamp() WHERE command_id=$1`,
	} {
		if _, err = f.app.Exec(f.ctx, sql, *r.CommandID); err == nil {
			t.Fatalf("storage invariant missing: %s", sql)
		}
	}
	// Real SQL establishes the first bounded canonical/late evidence, permits an
	// identical duplicate, then rejects replacement of either accepted payload.
	for _, prefix := range []string{"result", "late_result"} {
		sql := `UPDATE twin_commands SET ` + prefix + `_bytes=convert_to('{"fixture":true}','UTF8'),` + prefix + `_hash=encode(sha256(convert_to('{"fixture":true}','UTF8')),'hex'),` + prefix + `_received_at='2026-01-01Z' WHERE command_id=$1`
		for range 2 {
			if _, err = f.app.Exec(f.ctx, sql, *r.CommandID); err != nil {
				t.Fatalf("canonical duplicate %s %v", prefix, err)
			}
		}
		sql = `UPDATE twin_commands SET ` + prefix + `_bytes=convert_to('{"different":true}','UTF8'),` + prefix + `_hash=encode(sha256(convert_to('{"different":true}','UTF8')),'hex') WHERE command_id=$1`
		if _, err = f.app.Exec(f.ctx, sql, *r.CommandID); err == nil {
			t.Fatalf("canonical %s overwritten", prefix)
		}
	}
	for _, table := range []string{"twin_states", "twin_commands", "twin_outbox", "twin_command_requests"} {
		var denied bool
		if err = f.admin.QueryRow(f.ctx, `SELECT NOT(has_table_privilege('anon',$1,'INSERT') OR has_table_privilege('anon',$1,'UPDATE') OR has_table_privilege('authenticated',$1,'INSERT') OR has_table_privilege('authenticated',$1,'UPDATE'))`, table).Scan(&denied); err != nil || !denied {
			t.Fatalf("human grants %s %v", table, err)
		}
	}
}

func TestNativeControlNoopProof(t *testing.T) {
	for _, reported := range []string{`{"sample_interval_us":10000000}`, `{"sample_interval_us":10000000,"configuration":{}}`, `{"configuration":{"sample_interval_us":20000}}`} {
		t.Run(reported, func(t *testing.T) {
			f := newControlFixture(t)
			f.exec(t, `UPDATE twin_states SET desired_state='{"sample_interval_us":10000000}',reported_state=$2::jsonb WHERE entity_id=$1`, f.entity, reported)
			r, err := f.repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.1", "0", "not-confirmed"))
			if err != nil || r.StatusCode != 202 {
				t.Fatalf("observation is not confirmation: %+v %v", r, err)
			}
		})
	}
	f := newControlFixture(t)
	f.exec(t, `UPDATE twin_states SET desired_state='{"sample_interval_us":10000000}',reported_state='{"configuration":{"sample_interval_us":10000000}}' WHERE entity_id=$1`, f.entity)
	f.exec(t, `INSERT INTO twin_commands(command_id,entity_id,action,status,expires_at) VALUES($1,$2,'legacy_fixture','acknowledged',clock_timestamp()+interval '1 minute')`, uuid.New(), f.entity)
	if _, err := f.repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.1", "0", "busy-not-noop")); !errors.Is(err, digitaltwin.ErrCommandInProgress) {
		t.Fatalf("no-op bypassed active command: %v", err)
	}
}

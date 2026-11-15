package digitaltwin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/mqttcontrol"

	"github.com/google/uuid"
)

func TestNativeControlDispatchStableConditionalAndExpiry(t *testing.T) {
	f := newControlFixture(t)
	r, err := f.repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.1", "0", "dispatch"))
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.MustParse(*r.CommandID)
	ids, err := f.repo.PendingControl(f.ctx, 32)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, got := range ids {
		if got == id {
			found = true
		}
	}
	if !found {
		t.Fatal("durable pending command not discovered")
	}
	first, err := f.repo.PrepareControl(f.ctx, id)
	if err != nil || first == nil {
		t.Fatalf("prepare missing: %v", err)
	}
	second, err := f.repo.PrepareControl(f.ctx, id)
	if err != nil || second == nil || first.Topic != second.Topic || !bytes.Equal(first.Payload, second.Payload) {
		t.Fatalf("resend mutated payload: %v", err)
	}
	// A real DB mark failure leaves the same durable bytes/ID pending.
	f.exec(t, `CREATE FUNCTION stage54_mark_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='published' THEN RAISE EXCEPTION 'isolated mark failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER stage54_mark_failure BEFORE UPDATE ON twin_commands FOR EACH ROW EXECUTE FUNCTION stage54_mark_failure()`)
	if err = f.repo.MarkControlPublished(f.ctx, id); err == nil {
		t.Fatal("expected database mark failure")
	}
	f.exec(t, `DROP TRIGGER stage54_mark_failure ON twin_commands; DROP FUNCTION stage54_mark_failure()`)
	again, err := f.repo.PrepareControl(f.ctx, id)
	if err != nil || again == nil || !bytes.Equal(first.Payload, again.Payload) {
		t.Fatalf("ambiguous mark lost same-ID resend: %v", err)
	}
	// Simulated early result; no result receiver/transaction is implemented here.
	f.exec(t, `UPDATE twin_commands SET status='succeeded',completed_at=clock_timestamp() WHERE command_id=$1`, id)
	if err = f.repo.MarkControlPublished(f.ctx, id); err != nil {
		t.Fatal(err)
	}
	var status string
	if err = f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id).Scan(&status); err != nil || status != "succeeded" {
		t.Fatalf("terminal regressed %s %v", status, err)
	}
	if d, e := f.repo.PrepareControl(f.ctx, id); e != nil || d != nil {
		t.Fatalf("terminal dispatch %v %v", d, e)
	}
	for _, active := range []string{"pending", "published", "acknowledged"} {
		urn, entity := f.sensor(t, "expiry_"+active)
		r, e := f.repo.Admit(f.ctx, f.owner, urn, controlRequest(t, "1", "0", "expiry"))
		if e != nil {
			t.Fatal(e)
		}
		cid := uuid.MustParse(*r.CommandID)
		// Keep immutable bytes and issued/expires intact: shorten TTL at admission
		// in the separate expiry test rather than rewrite an accepted payload.
		f.exec(t, `UPDATE twin_commands SET status=$2 WHERE command_id=$1`, cid, active)
		f.exec(t, `UPDATE twin_commands SET expires_at=clock_timestamp()-interval '1 second',issued_at=clock_timestamp()-interval '2 seconds' WHERE command_id=$1`, cid)
		var before string
		if e = f.admin.QueryRow(f.ctx, `SELECT to_jsonb(s)::text FROM twin_states s WHERE entity_id=$1`, entity).Scan(&before); e != nil {
			t.Fatal(e)
		}
		if e = f.repo.ExpireControl(f.ctx, 32); e != nil {
			t.Fatal(e)
		}
		if e = f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, cid).Scan(&status); e != nil || status != "timeout" {
			t.Fatalf("%s expiry: %s %v", active, status, e)
		}
		var after string
		_ = f.admin.QueryRow(f.ctx, `SELECT to_jsonb(s)::text FROM twin_states s WHERE entity_id=$1`, entity).Scan(&after)
		if before != after {
			t.Fatal("expiry changed desired/reported")
		}
		if d, e := f.repo.PrepareControl(f.ctx, cid); e != nil || d != nil {
			t.Fatalf("expired dispatch %v %v", d, e)
		}
		if _, e = f.repo.Admit(f.ctx, f.owner, urn, controlRequest(t, "2", "1", "released")); e != nil {
			t.Fatal("slot not released", e)
		}
	}
}

func TestNativeControlDispatchBoundsLegacyAndConcurrentExpiry(t *testing.T) {
	f := newControlFixture(t)
	for _, limit := range []int{0, 1025} {
		if _, e := f.repo.PendingControl(f.ctx, limit); e == nil {
			t.Fatal("unbounded discovery")
		}
		if e := f.repo.ExpireControl(f.ctx, limit); e == nil {
			t.Fatal("unbounded expiry")
		}
	}
	for i, name := range []string{"legacy_pending", "legacy_published", "legacy_acknowledged"} {
		_, entity := f.sensor(t, name)
		status := []string{"pending", "published", "acknowledged"}[i]
		f.exec(t, `INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,issued_at,expires_at) VALUES(gen_random_uuid(),$1,'legacy_sentinel','{"sampling_interval_seconds":5}',$2,'1949-01-01Z','1950-01-01Z')`, entity, status)
		f.exec(t, `INSERT INTO twin_outbox(command_id,topic,payload) SELECT command_id,'fixture/legacy','{"sentinel":true}' FROM twin_commands WHERE entity_id=$1`, entity)
	}
	if e := f.repo.ExpireControl(f.ctx, 1); e != nil {
		t.Fatal(e)
	}
	var count int
	if e := f.admin.QueryRow(f.ctx, `SELECT count(*) FROM twin_commands c JOIN twin_entities e ON e.id=c.entity_id WHERE e.gateway_id=$1 AND c.status='timeout'`, f.gw).Scan(&count); e != nil || count != 1 {
		t.Fatalf("expiry scan not bounded %d %v", count, e)
	}
	// A live legacy sentinel never obtains dispatch authority.
	_, legacy := f.sensor(t, "legacy_live")
	legacyID := uuid.New()
	f.exec(t, `INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,issued_at,expires_at) VALUES($1,$2,'update_configuration','{"sampling_interval_seconds":5}','pending',clock_timestamp(),clock_timestamp()+interval '1 hour')`, legacyID, legacy)
	f.exec(t, `INSERT INTO twin_outbox(command_id,topic,payload) VALUES($1,'fixture/legacy','{"sentinel":true}')`, legacyID)
	if d, e := f.repo.PrepareControl(f.ctx, legacyID); e != nil || d != nil {
		t.Fatalf("legacy dispatch %v %v", d, e)
	}
	ids, e := f.repo.PendingControl(f.ctx, 1024)
	if e != nil {
		t.Fatal(e)
	}
	for _, id := range ids {
		if id == legacyID {
			t.Fatal("legacy discovered")
		}
	}
	// Real TTL, no stored payload/deadline rewrite. Expiry before dispatch, and
	// concurrent admission/mark/expiry all use resource->state->command locks.
	cfg := digitaltwin.DefaultControlConfig()
	cfg.TTL = time.Second
	repo, e := digitaltwin.NewPostgresControlRepository(f.app, cfg)
	if e != nil {
		t.Fatal(e)
	}
	r, e := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "1", "0", "expiring"))
	if e != nil {
		t.Fatal(e)
	}
	old := uuid.MustParse(*r.CommandID)
	time.Sleep(1100 * time.Millisecond)
	if d, e := repo.PrepareControl(f.ctx, old); e != nil || d != nil {
		t.Fatalf("expired bytes dispatched %v %v", d, e)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	start := make(chan struct{})
	for _, fn := range []func() error{func() error { return repo.ExpireControl(f.ctx, 32) }, func() error { return repo.MarkControlPublished(f.ctx, old) }, func() error { _, e := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "2", "1", "next")); return e }} {
		wg.Add(1)
		go func(fn func() error) { defer wg.Done(); <-start; errs <- fn() }(fn)
	}
	close(start)
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var status, outbox string
	var version int64
	if e := f.admin.QueryRow(f.ctx, `SELECT c.status,o.status FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE c.command_id=$1`, old).Scan(&status, &outbox); e != nil || status != "timeout" || outbox != "failed" {
		t.Fatalf("terminal/outbox %s %s %v", status, outbox, e)
	}
	if e := f.admin.QueryRow(f.ctx, `SELECT desired_version FROM twin_states WHERE entity_id=$1`, f.entity).Scan(&version); e != nil || version != 2 {
		t.Fatalf("expiry admission slot %d %v", version, e)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, e := repo.PendingControl(ctx, 1); e == nil {
		t.Fatal("cancelled scan")
	}
}

func TestNativeControlStage6ReservationAndFencing(t *testing.T) {
	f := newControlFixture(t)
	cfg := digitaltwin.DefaultControlConfig()
	cfg.Stage6Enabled = true
	stage6Repo, err := digitaltwin.NewPostgresControlRepository(f.app, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Stage 6 admission explicitly sets stage6_managed = true, client origin, root_command_id
	r, err := stage6Repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "1.5", "0", "s6_res"))
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.MustParse(*r.CommandID)

	var managed bool
	var origin, rootID string
	var attemptCount int
	var firstReservedAt *time.Time
	err = f.admin.QueryRow(f.ctx, `SELECT stage6_managed, origin, root_command_id::text, attempt_count, first_reserved_at FROM twin_commands WHERE command_id = $1`, id).
		Scan(&managed, &origin, &rootID, &attemptCount, &firstReservedAt)
	if err != nil || !managed || origin != "client" || rootID != id.String() || attemptCount != 0 || firstReservedAt != nil {
		t.Fatalf("managed admission mismatch: managed=%v origin=%s root=%s attempts=%d reservedAt=%v err=%v", managed, origin, rootID, attemptCount, firstReservedAt, err)
	}

	// 2. First grant: increments both command and outbox counters atomically, sets first_reserved_at
	first, err := stage6Repo.PrepareControl(f.ctx, id)
	if err != nil || first == nil {
		t.Fatalf("first prepare failed: %v", err)
	}
	if first.Attempt != 1 {
		t.Fatalf("first attempt = %d, want 1", first.Attempt)
	}
	var cmdAttempts, outboxAttempts int
	var outboxNextAt time.Time
	err = f.admin.QueryRow(f.ctx, `SELECT c.attempt_count, o.attempt_count, c.first_reserved_at, o.next_attempt_at FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE c.command_id = $1`, id).
		Scan(&cmdAttempts, &outboxAttempts, &firstReservedAt, &outboxNextAt)
	if err != nil || cmdAttempts != 1 || outboxAttempts != 1 || firstReservedAt == nil || !outboxNextAt.After(time.Now()) {
		t.Fatalf("first grant db mismatch: cmd=%d outbox=%d reservedAt=%v nextAt=%v err=%v", cmdAttempts, outboxAttempts, firstReservedAt, outboxNextAt, err)
	}

	// 3. Before due: no second grant even when queue/wakeup repeats
	immediate, err := stage6Repo.PrepareControl(f.ctx, id)
	if err != nil || immediate != nil {
		t.Fatalf("immediate prepare before due granted authority: %v, %v", immediate, err)
	}

	// 4. Legacy Mark call omitting attempt identity must NOT mutate managed command
	if err := f.repo.MarkControlPublished(f.ctx, id); err != nil {
		t.Fatal(err)
	}
	var currentStatus string
	if err := f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id = $1`, id).Scan(&currentStatus); err != nil || currentStatus != "pending" {
		t.Fatalf("legacy mark altered managed command: status=%s err=%v", currentStatus, err)
	}

	// 5. Proper Stage 6 completion on attempt 1 marks published
	if err := stage6Repo.MarkControlReservationPublished(f.ctx, id, 1); err != nil {
		t.Fatal(err)
	}
	if err := f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id = $1`, id).Scan(&currentStatus); err != nil || currentStatus != "published" {
		t.Fatalf("mark reservation 1 status=%s err=%v", currentStatus, err)
	}

	// 6. Fast-forward due time to past: due published command can grant again with original bytes
	f.exec(t, `UPDATE twin_outbox SET next_attempt_at = clock_timestamp() - interval '1 second' WHERE command_id = $1`, id)
	second, err := stage6Repo.PrepareControl(f.ctx, id)
	if err != nil || second == nil {
		t.Fatalf("second prepare failed: %v", err)
	}
	if second.Attempt != 2 {
		t.Fatalf("second attempt = %d, want 2", second.Attempt)
	}
	if !bytes.Equal(first.Payload, second.Payload) || first.Topic != second.Topic || first.CommandID != second.CommandID {
		t.Fatalf("second grant mutated bytes or topic: %v vs %v", first, second)
	}

	// 7. Stale completion on attempt 1 does NOT mutate or regress attempt 2
	if err := stage6Repo.MarkControlReservationPublished(f.ctx, id, 1); err != nil {
		t.Fatal(err)
	}
	_ = f.admin.QueryRow(f.ctx, `SELECT attempt_count FROM twin_commands WHERE command_id = $1`, id).Scan(&cmdAttempts)
	if cmdAttempts != 2 {
		t.Fatalf("stale mark regressed attempt count: %d, want 2", cmdAttempts)
	}

	// 8. Max attempts and retry exhaustion:
	// Fast-forward attempts to 4 and make due
	f.exec(t, `UPDATE twin_commands SET attempt_count = 4 WHERE command_id = $1`, id)
	f.exec(t, `UPDATE twin_outbox SET attempt_count = 4, next_attempt_at = clock_timestamp() - interval '1 second' WHERE command_id = $1`, id)
	fifth, err := stage6Repo.PrepareControl(f.ctx, id)
	if err != nil || fifth == nil {
		t.Fatalf("fifth prepare failed: %v", err)
	}
	if fifth.Attempt != 5 {
		t.Fatalf("fifth attempt = %d, want 5", fifth.Attempt)
	}
	var retryStop *string
	_ = f.admin.QueryRow(f.ctx, `SELECT status, retry_stop_reason FROM twin_commands WHERE command_id = $1`, id).Scan(&currentStatus, &retryStop)
	if currentStatus != "published" || retryStop == nil || *retryStop != "retry_exhausted" {
		t.Fatalf("exhausted command state: status=%s reason=%v", currentStatus, retryStop)
	}

	// Once exhausted: no further grants even if made due
	f.exec(t, `UPDATE twin_outbox SET next_attempt_at = clock_timestamp() - interval '1 second' WHERE command_id = $1`, id)
	sixth, err := stage6Repo.PrepareControl(f.ctx, id)
	if err != nil || sixth != nil {
		t.Fatalf("exhausted command granted: %v, %v", sixth, err)
	}

	// 9. Acknowledged suppresses resend
	urn2, _ := f.sensor(t, "s2_ack")
	r2, err := stage6Repo.Admit(f.ctx, f.owner, urn2, controlRequest(t, "2.0", "0", "s6_ack"))
	if err != nil {
		t.Fatal(err)
	}
	id2 := uuid.MustParse(*r2.CommandID)
	f.exec(t, `UPDATE twin_commands SET status = 'acknowledged' WHERE command_id = $1`, id2)
	ackPrep, err := stage6Repo.PrepareControl(f.ctx, id2)
	if err != nil || ackPrep != nil {
		t.Fatalf("acknowledged command granted: %v, %v", ackPrep, err)
	}

	// 10. Recreating repository against SAME DB preserves attempt count and schedule (does not dispatch early)
	repoRecreated, err := digitaltwin.NewPostgresControlRepository(f.app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	urn3, _ := f.sensor(t, "s3_recreate")
	r3, err := repoRecreated.Admit(f.ctx, f.owner, urn3, controlRequest(t, "1.0", "0", "s6_rec"))
	if err != nil {
		t.Fatal(err)
	}
	id3 := uuid.MustParse(*r3.CommandID)
	g1, err := repoRecreated.PrepareControl(f.ctx, id3)
	if err != nil || g1 == nil || g1.Attempt != 1 {
		t.Fatalf("g1 prepare failed: %v", err)
	}
	// A new instance of repository against same DB must not dispatch early
	repoAnother, err := digitaltwin.NewPostgresControlRepository(f.app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	early, err := repoAnother.PrepareControl(f.ctx, id3)
	if err != nil || early != nil {
		t.Fatalf("recreated repo dispatched early before due: %v, %v", early, err)
	}

	// 11. Remaining TTL < publish timeout refuses grant before incrementing attempts
	cfgTight := cfg
	cfgTight.TTL = 2 * time.Second // less than PublishTimeout (3s)
	repoTight, err := digitaltwin.NewPostgresControlRepository(f.app, cfgTight)
	if err != nil {
		t.Fatal(err)
	}
	urn4, _ := f.sensor(t, "s4_tight")
	r4, err := repoTight.Admit(f.ctx, f.owner, urn4, controlRequest(t, "1.0", "0", "s6_tight"))
	if err != nil {
		t.Fatal(err)
	}
	id4 := uuid.MustParse(*r4.CommandID)
	tightGrant, err := repoTight.PrepareControl(f.ctx, id4)
	if err != nil || tightGrant != nil {
		t.Fatalf("tight TTL granted authority: %v, %v", tightGrant, err)
	}
	var tightAttempts int
	var tightReason *string
	_ = f.admin.QueryRow(f.ctx, `SELECT attempt_count, retry_stop_reason FROM twin_commands WHERE command_id = $1`, id4).
		Scan(&tightAttempts, &tightReason)
	if tightAttempts != 0 || tightReason != nil {
		t.Fatalf("tight TTL incremented attempts or misreported exhaustion: attempts=%d reason=%v", tightAttempts, tightReason)
	}

	// 12. Jitter hook varies persisted next_attempt_at when backoff exceeds lease (attempt 4, backoff 8s > lease 4s)
	stage6Repo.SetJitterFunc(func() float64 { return 1.0 }) // max jitter: 8s
	f.exec(t, `UPDATE twin_commands SET attempt_count = 3 WHERE command_id = $1`, id3)
	f.exec(t, `UPDATE twin_outbox SET attempt_count = 3, next_attempt_at = clock_timestamp() - interval '1 second' WHERE command_id = $1`, id3)
	g4Max, err := stage6Repo.PrepareControl(f.ctx, id3)
	if err != nil || g4Max == nil || g4Max.Attempt != 4 {
		t.Fatalf("g4Max prepare failed: %v", err)
	}
	var nextAtMax time.Time
	_ = f.admin.QueryRow(f.ctx, `SELECT next_attempt_at FROM twin_outbox WHERE command_id = $1`, id3).Scan(&nextAtMax)

	stage6Repo.SetJitterFunc(func() float64 { return 0.0 }) // min jitter: 4s
	f.exec(t, `UPDATE twin_commands SET attempt_count = 3 WHERE command_id = $1`, id3)
	f.exec(t, `UPDATE twin_outbox SET attempt_count = 3, next_attempt_at = clock_timestamp() - interval '1 second' WHERE command_id = $1`, id3)
	g4Min, err := stage6Repo.PrepareControl(f.ctx, id3)
	if err != nil || g4Min == nil || g4Min.Attempt != 4 {
		t.Fatalf("g4Min prepare failed: %v", err)
	}
	var nextAtMin time.Time
	_ = f.admin.QueryRow(f.ctx, `SELECT next_attempt_at FROM twin_outbox WHERE command_id = $1`, id3).Scan(&nextAtMin)
	if !nextAtMax.After(nextAtMin) {
		t.Fatalf("jitter did not vary schedule: nextAtMax=%v nextAtMin=%v", nextAtMax, nextAtMin)
	}

	// 13. Periodic scan finds due command without wakeup
	f.exec(t, `UPDATE twin_outbox SET next_attempt_at = clock_timestamp() - interval '1 second' WHERE command_id = $1`, id3)
	dueIDs, err := stage6Repo.PendingControl(f.ctx, 10)
	if err != nil || len(dueIDs) == 0 {
		t.Fatalf("pending control failed: ids=%v err=%v", dueIDs, err)
	}
	foundDue := false
	for _, did := range dueIDs {
		if did == id3 {
			foundDue = true
			break
		}
	}
	if !foundDue {
		t.Fatalf("due ID %v not found in periodic discovery: %v", id3, dueIDs)
	}

	// 14. Stale completion after terminal state does not regress terminal timeout
	f.exec(t, `UPDATE twin_commands SET status = 'timeout' WHERE command_id = $1`, id3)
	if err := stage6Repo.MarkControlReservationPublished(f.ctx, id3, 4); err != nil {
		t.Fatal(err)
	}
	var postTerminalStatus string
	_ = f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id = $1`, id3).Scan(&postTerminalStatus)
	if postTerminalStatus != "timeout" {
		t.Fatalf("stale mark regressed terminal timeout: %s", postTerminalStatus)
	}

	// 15. Valid Gateway command result remains receivable and transitions state
	urn5, entity5 := f.sensor(t, "s5_result")
	r5, err := stage6Repo.Admit(f.ctx, f.owner, urn5, controlRequest(t, "2.0", "0", "s6_res_res"))
	if err != nil {
		t.Fatal(err)
	}
	id5 := uuid.MustParse(*r5.CommandID)
	g5, err := stage6Repo.PrepareControl(f.ctx, id5)
	if err != nil || g5 == nil {
		t.Fatal(err)
	}
	// Protocol timestamps carry microsecond precision only; nanosecond text is
	// rejected by the strict wire parser, so the builder must truncate.
	completedAt := time.Now().UTC().Truncate(time.Microsecond)
	resPayload := []byte(`{"protocol_version":1,"message_type":"command_result","command_id":"` + id5.String() + `","gateway_id":"` + f.gw + `","entity_id":"` + urn5 + `","desired_version":"1","status":"succeeded","reported_state":{"sample_interval_us":500000},"completed_at":"` + completedAt.Format(time.RFC3339Nano) + `"}`)
	outcome, err := f.repo.AcceptControlResult(f.ctx, "gateways/"+f.gw+"/responses/"+id5.String(), resPayload, time.Now())
	if err != nil || outcome != digitaltwin.ControlResultAccepted {
		t.Fatalf("accept result failed: outcome=%s err=%v", outcome, err)
	}
	var resStatus string
	var repInterval int64
	_ = f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id = $1`, id5).Scan(&resStatus)
	_ = f.admin.QueryRow(f.ctx, `SELECT (reported_state->'configuration'->>'sample_interval_us')::bigint FROM twin_states WHERE entity_id = $1`, entity5).Scan(&repInterval)
	if resStatus != "succeeded" || repInterval != 500000 {
		t.Fatalf("result transition mismatch: status=%s interval=%d", resStatus, repInterval)
	}
}

// stage6Policy is a small, finite policy so native rehearsal stays inside the
// fixture timeout while keeping the same lease/backoff semantics.
func stage6Policy() digitaltwin.RetryPolicy {
	return digitaltwin.RetryPolicy{
		BaseDelay:      100 * time.Millisecond,
		CapDelay:       200 * time.Millisecond,
		MaxAttempts:    5,
		PublishTimeout: 100 * time.Millisecond,
		SafetyMargin:   50 * time.Millisecond,
	}
}

func stage6ControlConfig(ttl time.Duration) digitaltwin.ControlConfig {
	cfg := digitaltwin.DefaultControlConfig()
	cfg.Stage6Enabled = true
	cfg.RetryPolicy = stage6Policy()
	cfg.TTL = ttl
	return cfg
}

func stage6Command(t *testing.T, f *controlFixture, id uuid.UUID) digitaltwin.Command {
	t.Helper()
	var topic string
	var payload []byte
	if err := f.admin.QueryRow(f.ctx, `SELECT topic,payload_bytes FROM twin_outbox WHERE command_id=$1`, id).Scan(&topic, &payload); err != nil {
		t.Fatal(err)
	}
	c, err := digitaltwin.ParseMQTTCommand(topic, payload, false)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// The complete finite lease (publish timeout plus safety margin) must fit
// before the original expiry before any attempt is granted, and a short TTL
// must not be reported as an exhausted attempt cap.
func TestNativeControlStage6LeaseExpiryAndAttemptStop(t *testing.T) {
	f := newControlFixture(t)

	// 1. A 1s command TTL cannot hold the complete 2s lease (1s publish timeout
	// plus 1s safety margin), so no valid grant fits before the expiry.
	leaseCfg := stage6ControlConfig(time.Second)
	leaseCfg.RetryPolicy = digitaltwin.RetryPolicy{
		BaseDelay:      100 * time.Millisecond,
		CapDelay:       200 * time.Millisecond,
		MaxAttempts:    5,
		PublishTimeout: time.Second,
		SafetyMargin:   time.Second,
	}
	shortRepo, err := digitaltwin.NewPostgresControlRepository(f.app, leaseCfg)
	if err != nil {
		t.Fatal(err)
	}
	urn, _ := f.sensor(t, "s6_lease")
	r, err := shortRepo.Admit(f.ctx, f.owner, urn, controlRequest(t, "1.0", "0", "s6_lease"))
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.MustParse(*r.CommandID)
	grant, err := shortRepo.PrepareControl(f.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if grant != nil {
		t.Fatalf("grant issued without a complete lease: %+v", grant)
	}
	var attempts int
	var reason *string
	var nextAt, expiresAt time.Time
	var status string
	if err = f.admin.QueryRow(f.ctx, `SELECT c.attempt_count,c.retry_stop_reason,o.next_attempt_at,c.expires_at,c.status
		FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE c.command_id=$1`, id).
		Scan(&attempts, &reason, &nextAt, &expiresAt, &status); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 {
		t.Fatalf("refused grant consumed an attempt: %d", attempts)
	}
	if reason != nil {
		t.Fatalf("refused grant claimed an exhausted cap: %q", *reason)
	}
	if !nextAt.Equal(expiresAt) {
		t.Fatalf("schedule not parked at the original expiry: %v vs %v", nextAt, expiresAt)
	}
	if status != "pending" {
		t.Fatalf("short TTL changed the active status: %s", status)
	}

	// 2. A TTL that fits the complete lease grants normally and keeps the bytes.
	repo, err := digitaltwin.NewPostgresControlRepository(f.app, stage6ControlConfig(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	urn2, _ := f.sensor(t, "s6_grant")
	r2, err := repo.Admit(f.ctx, f.owner, urn2, controlRequest(t, "2.0", "0", "s6_grant"))
	if err != nil {
		t.Fatal(err)
	}
	id2 := uuid.MustParse(*r2.CommandID)
	first, err := repo.PrepareControl(f.ctx, id2)
	if err != nil || first == nil || first.Attempt != 1 {
		t.Fatalf("first grant failed: %+v %v", first, err)
	}
	if first.LeaseAt.Sub(first.ExpiresAt) >= 0 {
		t.Fatalf("lease must stay inside the command expiry: lease=%v expiry=%v", first.LeaseAt, first.ExpiresAt)
	}

	// 3. The attempt cap stops automatic retries without inventing a failure.
	f.exec(t, `UPDATE twin_commands SET attempt_count=4 WHERE command_id=$1`, id2)
	f.exec(t, `UPDATE twin_outbox SET attempt_count=4, next_attempt_at=clock_timestamp()-interval '1 second' WHERE command_id=$1`, id2)
	last, err := repo.PrepareControl(f.ctx, id2)
	if err != nil || last == nil || last.Attempt != 5 {
		t.Fatalf("last permitted grant failed: %+v %v", last, err)
	}
	var stopReason *string
	var cmdStatus string
	if err = f.admin.QueryRow(f.ctx, `SELECT retry_stop_reason,status FROM twin_commands WHERE command_id=$1`, id2).Scan(&stopReason, &cmdStatus); err != nil {
		t.Fatal(err)
	}
	if stopReason == nil || *stopReason != "retry_exhausted" {
		t.Fatalf("retry stop reason = %v, want retry_exhausted", stopReason)
	}
	if cmdStatus != "pending" && cmdStatus != "published" {
		t.Fatalf("exhausted command invented a terminal state: %s", cmdStatus)
	}
	// No further grant is possible even when the row is forced due again.
	f.exec(t, `UPDATE twin_outbox SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE command_id=$1`, id2)
	again, err := repo.PrepareControl(f.ctx, id2)
	if err != nil || again != nil {
		t.Fatalf("exhausted command granted again: %+v %v", again, err)
	}
	// The exhausted command is no longer discovered as due work.
	ids, err := repo.PendingControl(f.ctx, 32)
	if err != nil {
		t.Fatal(err)
	}
	for _, got := range ids {
		if got == id2 {
			t.Fatal("exhausted command is still discovered as due work")
		}
	}

	// 4. A valid Gateway result still commits inside the TTL after the retry stop.
	c := stage6Command(t, f, id2)
	result := digitaltwin.CommandResult{
		ProtocolVersion: 1,
		MessageType:     "command_result",
		CommandID:       c.CommandID,
		GatewayID:       c.GatewayID,
		EntityID:        c.EntityID,
		DesiredVersion:  c.DesiredVersion,
		Status:          digitaltwin.CommandSucceeded,
		ReportedState:   &c.Parameters,
		CompletedAt:     time.Now().UTC().Truncate(time.Microsecond),
	}
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := repo.AcceptControlResult(f.ctx, "gateways/"+c.GatewayID+"/responses/"+c.CommandID, payload, time.Now())
	if err != nil || outcome != digitaltwin.ControlResultAccepted {
		t.Fatalf("result after retry stop: outcome=%s err=%v", outcome, err)
	}
	var reportedInterval int64
	if err = f.admin.QueryRow(f.ctx, `SELECT (reported_state->'configuration'->>'sample_interval_us')::bigint FROM twin_states WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, c.EntityID).Scan(&reportedInterval); err != nil {
		t.Fatal(err)
	}
	if reportedInterval != c.Parameters.SampleIntervalUS {
		t.Fatalf("reported configuration = %d, want %d", reportedInterval, c.Parameters.SampleIntervalUS)
	}
	var finalStatus string
	if err = f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id2).Scan(&finalStatus); err != nil || finalStatus != "succeeded" {
		t.Fatalf("final status = %s err=%v", finalStatus, err)
	}

	// 5. A stale completion for an older attempt cannot regress the terminal state.
	if err = repo.MarkControlReservationPublished(f.ctx, id2, 3); err != nil {
		t.Fatal(err)
	}
	var afterStale string
	if err = f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id2).Scan(&afterStale); err != nil || afterStale != "succeeded" {
		t.Fatalf("stale completion regressed terminal state: %s %v", afterStale, err)
	}

	// 6. A stale completion cannot regress an acknowledged command either.
	f.exec(t, `UPDATE twin_commands SET status='acknowledged' WHERE command_id=$1`, id)
	if err = repo.MarkControlReservationPublished(f.ctx, id, 1); err != nil {
		t.Fatal(err)
	}
	var ackStatus string
	if err = f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id).Scan(&ackStatus); err != nil || ackStatus != "acknowledged" {
		t.Fatalf("stale completion regressed acknowledged state: %s %v", ackStatus, err)
	}
	// An acknowledged command is never dispatched again.
	ackGrant, err := repo.PrepareControl(f.ctx, id)
	if err != nil || ackGrant != nil {
		t.Fatalf("acknowledged command granted authority: %+v %v", ackGrant, err)
	}
}

// The durable schedule must survive a repository/runtime reconstruction against
// the same database, and a stale queued ID must never bypass the due time or
// the status checks.
func TestNativeControlStage6RestartKeepsDurableSchedule(t *testing.T) {
	f := newControlFixture(t)
	cfg := stage6ControlConfig(time.Minute)
	repo, err := digitaltwin.NewPostgresControlRepository(f.app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	urn, _ := f.sensor(t, "s6_restart")
	r, err := repo.Admit(f.ctx, f.owner, urn, controlRequest(t, "1.0", "0", "s6_restart"))
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.MustParse(*r.CommandID)
	first, err := repo.PrepareControl(f.ctx, id)
	if err != nil || first == nil || first.Attempt != 1 {
		t.Fatalf("first grant failed: %+v %v", first, err)
	}
	var attempts int
	var nextAt time.Time
	if err = f.admin.QueryRow(f.ctx, `SELECT c.attempt_count,o.next_attempt_at FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE c.command_id=$1`, id).Scan(&attempts, &nextAt); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("attempt count after grant = %d, want 1", attempts)
	}

	// Reconstruct the repository against the same database.
	restarted, err := digitaltwin.NewPostgresControlRepository(f.app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var attemptsAfter int
	var nextAfter time.Time
	if err = f.admin.QueryRow(f.ctx, `SELECT c.attempt_count,o.next_attempt_at FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE c.command_id=$1`, id).Scan(&attemptsAfter, &nextAfter); err != nil {
		t.Fatal(err)
	}
	if attemptsAfter != attempts || !nextAfter.Equal(nextAt) {
		t.Fatalf("durable schedule did not survive reconstruction: attempts=%d/%d next=%v/%v", attemptsAfter, attempts, nextAfter, nextAt)
	}
	// The consumed grant is not handed out again before the due time.
	early, err := restarted.PrepareControl(f.ctx, id)
	if err != nil || early != nil {
		t.Fatalf("reconstructed repository granted before the due time: %+v %v", early, err)
	}
	// After the persisted schedule becomes due, the next grant continues the
	// same attempt sequence with identical bytes, topic and command identity.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if time.Now().After(nextAt) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	second, err := restarted.PrepareControl(f.ctx, id)
	if err != nil || second == nil || second.Attempt != 2 {
		t.Fatalf("due grant after reconstruction failed: %+v %v", second, err)
	}
	if !bytes.Equal(first.Payload, second.Payload) || first.Topic != second.Topic || first.CommandID != second.CommandID {
		t.Fatal("retry after reconstruction changed the durable bytes, topic or identity")
	}
	if first.ExpiresAt != second.ExpiresAt {
		t.Fatalf("retry after reconstruction changed the command expiry: %v vs %v", first.ExpiresAt, second.ExpiresAt)
	}

	// A stale queued ID must not bypass the status check after the natural expiry.
	urn2, _ := f.sensor(t, "s6_expire")
	shortRepo, err := digitaltwin.NewPostgresControlRepository(f.app, stage6ControlConfig(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := shortRepo.Admit(f.ctx, f.owner, urn2, controlRequest(t, "1.0", "0", "s6_expire"))
	if err != nil {
		t.Fatal(err)
	}
	id2 := uuid.MustParse(*r2.CommandID)
	if grant, err := shortRepo.PrepareControl(f.ctx, id2); err != nil || grant == nil {
		t.Fatalf("short TTL grant failed: %+v %v", grant, err)
	}
	time.Sleep(1300 * time.Millisecond)
	stale, err := shortRepo.PrepareControl(f.ctx, id2)
	if err != nil {
		t.Fatal(err)
	}
	if stale != nil {
		t.Fatalf("expired command granted authority: %+v", stale)
	}
	var expiredStatus string
	if err = f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id2).Scan(&expiredStatus); err != nil {
		t.Fatal(err)
	}
	if expiredStatus != "timeout" {
		t.Fatalf("expired command status = %s, want timeout", expiredStatus)
	}
}

type countingControlRepository struct {
	*digitaltwin.PostgresControlRepository
	prepareCalls atomic.Int64
}

func (r *countingControlRepository) PrepareControl(ctx context.Context, id uuid.UUID) (*digitaltwin.ControlDispatch, error) {
	r.prepareCalls.Add(1)
	return r.PostgresControlRepository.PrepareControl(ctx, id)
}

type countingControlPublisher struct {
	mu           sync.Mutex
	ready        bool
	publishCalls atomic.Int64
}

func (p *countingControlPublisher) Ready() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ready
}

func (p *countingControlPublisher) Publish(_ context.Context, _ digitaltwin.ControlDispatch) error {
	p.publishCalls.Add(1)
	return nil
}

func TestNativeControlStage6PolicyDecreaseParksExhausted(t *testing.T) {
	f := newControlFixture(t)
	// Park any leftover pending outbox rows from prior serial tests into the future so prefix is owned
	f.exec(t, `UPDATE twin_outbox SET next_attempt_at = clock_timestamp() + interval '1 hour' WHERE status IN ('pending', 'published')`)

	cfg := stage6ControlConfig(time.Minute)
	cfg.RetryPolicy.BaseDelay = 20 * time.Millisecond
	cfg.RetryPolicy.CapDelay = 50 * time.Millisecond
	cfg.RetryPolicy.PublishTimeout = 20 * time.Millisecond
	cfg.RetryPolicy.SafetyMargin = 10 * time.Millisecond
	cfg.RetryPolicy.MaxAttempts = 5

	repo, err := digitaltwin.NewPostgresControlRepository(f.app, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Admit real user intent for Sensor 1 under MaxAttempts = 5
	r1, err := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "1.0", "0", "pol_dec_1"))
	if err != nil {
		t.Fatal(err)
	}
	id1 := uuid.MustParse(*r1.CommandID)

	// 2. Reserve attempt 1 under cap 5
	grant1, err := repo.PrepareControl(f.ctx, id1)
	if err != nil || grant1 == nil || grant1.Attempt != 1 {
		t.Fatalf("first grant failed: %+v %v", grant1, err)
	}

	var due1 time.Time
	if err = f.admin.QueryRow(f.ctx, `SELECT next_attempt_at FROM twin_outbox WHERE command_id=$1`, id1).Scan(&due1); err != nil {
		t.Fatal(err)
	}

	// 3. Natural wait until due1 has elapsed
	if wait := time.Until(due1.Add(10 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}

	// 4. Reconstruct repository on SAME database with MaxAttempts = 1
	cfgLowered := cfg
	cfgLowered.RetryPolicy.MaxAttempts = 1
	loweredRepo, err := digitaltwin.NewPostgresControlRepository(f.app, cfgLowered)
	if err != nil {
		t.Fatal(err)
	}

	// 5. Admit another Sensor's valid work behind it
	urn2, _ := f.sensor(t, "pol_dec_2")
	r2, err := loweredRepo.Admit(f.ctx, f.owner, urn2, controlRequest(t, "2.0", "0", "pol_dec_2"))
	if err != nil {
		t.Fatal(err)
	}
	id2 := uuid.MustParse(*r2.CommandID)

	// Verify before Prepare: PendingControl(1) sees id1 because next_attempt_at is due
	// and retry_stop_reason was not yet updated.
	prefixBefore, err := loweredRepo.PendingControl(f.ctx, 1)
	if err != nil || len(prefixBefore) != 1 || prefixBefore[0] != id1 {
		t.Fatalf("expected due id1 (%v) in prefix before prepare: got %v (id2 is %v), %v", id1, prefixBefore, id2, err)
	}

	// 6. Call PrepareControl on id1 with loweredRepo
	dispatch, err := loweredRepo.PrepareControl(f.ctx, id1)
	if err != nil {
		t.Fatalf("PrepareControl unexpected error: %v", err)
	}
	if dispatch != nil {
		t.Fatalf("PrepareControl granted attempt beyond cap: %+v", dispatch)
	}

	// 7. Verify id1 is durably stopped and parked at expires_at
	var attempts1 int
	var stopReason1 *string
	var nextAt1, expiresAt1 time.Time
	var status1 string
	if err = f.admin.QueryRow(f.ctx, `SELECT c.attempt_count, c.retry_stop_reason, c.status, c.expires_at, o.next_attempt_at FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE c.command_id=$1`, id1).
		Scan(&attempts1, &stopReason1, &status1, &expiresAt1, &nextAt1); err != nil {
		t.Fatal(err)
	}

	if stopReason1 == nil || *stopReason1 != "retry_exhausted" {
		t.Fatalf("id1 retry_stop_reason = %v, want 'retry_exhausted'", stopReason1)
	}
	if !nextAt1.Equal(expiresAt1) {
		t.Fatalf("id1 next_attempt_at = %v, want expires_at %v", nextAt1, expiresAt1)
	}
	if attempts1 != 1 {
		t.Fatalf("id1 attempts = %d, want 1", attempts1)
	}
	if status1 != "pending" && status1 != "published" {
		t.Fatalf("id1 status = %q, want active pending/published", status1)
	}

	// 8. Verify discovery: PendingControl(1) must now return id2, not id1!
	ids, err := loweredRepo.PendingControl(f.ctx, 1)
	if err != nil {
		t.Fatalf("PendingControl error: %v", err)
	}
	if len(ids) != 1 || ids[0] != id2 {
		t.Fatalf("PendingControl returned %v, want [id2=%v] (id1 must not monopolize prefix)", ids, id2)
	}

	// 9. Later work progresses: PrepareControl on id2 succeeds
	grant2, err := loweredRepo.PrepareControl(f.ctx, id2)
	if err != nil || grant2 == nil || grant2.Attempt != 1 {
		t.Fatalf("id2 grant failed: %+v %v", grant2, err)
	}

	// 10. Valid result commits within TTL for stopped command id1
	completedAt := time.Now().UTC().Truncate(time.Microsecond)
	resultPayload := fmt.Sprintf(`{"protocol_version":1,"message_type":"command_result","command_id":%q,"gateway_id":%q,"entity_id":%q,"desired_version":"1","status":"succeeded","reported_state":{"sample_interval_us":1000000},"completed_at":%q}`, id1.String(), f.gw, f.urn, completedAt.Format(time.RFC3339Nano))
	topic := fmt.Sprintf("gateways/%s/responses/%s", f.gw, id1.String())
	outcome, err := loweredRepo.AcceptControlResult(f.ctx, topic, []byte(resultPayload), time.Now().UTC())
	if err != nil || outcome != digitaltwin.ControlResultAccepted {
		t.Fatalf("AcceptControlResult on exhausted command = %v, %v; want accepted", outcome, err)
	}

	var finalStatus string
	if err = f.admin.QueryRow(f.ctx, `SELECT status FROM twin_commands WHERE command_id=$1`, id1).Scan(&finalStatus); err != nil {
		t.Fatal(err)
	}
	if finalStatus != "succeeded" {
		t.Fatalf("final status = %q, want 'succeeded'", finalStatus)
	}
}

func TestNativeControlStage6PolicyDecreaseRuntimePacing(t *testing.T) {
	for _, wakeup := range []bool{true, false} {
		t.Run(fmt.Sprintf("WakeupEnabled=%v", wakeup), func(t *testing.T) {
			f := newControlFixture(t)
			f.exec(t, `UPDATE twin_outbox SET next_attempt_at = clock_timestamp() + interval '1 hour' WHERE status IN ('pending', 'published')`)

			cfg := stage6ControlConfig(time.Minute)
			cfg.RetryPolicy.BaseDelay = 20 * time.Millisecond
			cfg.RetryPolicy.CapDelay = 50 * time.Millisecond
			cfg.RetryPolicy.PublishTimeout = 20 * time.Millisecond
			cfg.RetryPolicy.SafetyMargin = 10 * time.Millisecond
			cfg.RetryPolicy.MaxAttempts = 5

			repo, err := digitaltwin.NewPostgresControlRepository(f.app, cfg)
			if err != nil {
				t.Fatal(err)
			}

			// Admit and grant attempt 1 under cap 5
			r1, err := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "1.0", "0", "pol_rt_1"))
			if err != nil {
				t.Fatal(err)
			}
			id1 := uuid.MustParse(*r1.CommandID)
			grant1, err := repo.PrepareControl(f.ctx, id1)
			if err != nil || grant1 == nil {
				t.Fatalf("first grant failed: %+v %v", grant1, err)
			}

			var due1 time.Time
			if err = f.admin.QueryRow(f.ctx, `SELECT next_attempt_at FROM twin_outbox WHERE command_id=$1`, id1).Scan(&due1); err != nil {
				t.Fatal(err)
			}

			// Wait until id1 is naturally due before admitting later work
			if wait := time.Until(due1.Add(10 * time.Millisecond)); wait > 0 {
				time.Sleep(wait)
			}

			// Reconstruct repository with MaxAttempts = 1 on SAME database
			cfgLowered := cfg
			cfgLowered.RetryPolicy.MaxAttempts = 1
			loweredRepo, err := digitaltwin.NewPostgresControlRepository(f.app, cfgLowered)
			if err != nil {
				t.Fatal(err)
			}

			// Admit later work on a second sensor
			urn2, _ := f.sensor(t, "pol_rt_2")
			r2, err := loweredRepo.Admit(f.ctx, f.owner, urn2, controlRequest(t, "2.0", "0", "pol_rt_2"))
			if err != nil {
				t.Fatal(err)
			}
			id2 := uuid.MustParse(*r2.CommandID)

			// Runtime with BatchSize = 1, ScanInterval = 100ms
			wrapped := &countingControlRepository{PostgresControlRepository: loweredRepo}
			pub := &countingControlPublisher{ready: true}

			rtCfg := mqttcontrol.DefaultConfig()
			rtCfg.Stage6Enabled = true
			rtCfg.WakeupEnabled = wakeup
			rtCfg.RetryPolicy = cfgLowered.RetryPolicy
			rtCfg.PublishTimeout = cfgLowered.RetryPolicy.PublishTimeout
			rtCfg.BatchSize = 1
			rtCfg.ScanInterval = 100 * time.Millisecond

			runtime, err := mqttcontrol.New(rtCfg, wrapped, pub)
			if err != nil {
				t.Fatal(err)
			}
			if err = runtime.Start(); err != nil {
				t.Fatal(err)
			}

			// Wait 350ms (3-4 scan intervals):
			// - id1 should be normalized on the first scan (prepare call 1)
			// - on scan 2, id2 should be discovered and published (prepare call 2, publish call 1)
			// - subsequent scans find nothing, calls stay bounded (<= 8 calls, NOT ~150!)
			time.Sleep(350 * time.Millisecond)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err = runtime.Close(ctx); err != nil {
				t.Fatal(err)
			}

			var attempts1 int
			var stopReason1 *string
			var dueAfter1 time.Time
			if err = f.admin.QueryRow(f.ctx, `SELECT c.attempt_count, c.retry_stop_reason, o.next_attempt_at FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE command_id=$1`, id1).
				Scan(&attempts1, &stopReason1, &dueAfter1); err != nil {
				t.Fatal(err)
			}

			var attempts2 int
			if err = f.admin.QueryRow(f.ctx, `SELECT attempt_count FROM twin_commands WHERE command_id=$1`, id2).
				Scan(&attempts2); err != nil {
				t.Fatal(err)
			}

			prepareCalls := wrapped.prepareCalls.Load()
			publishCalls := pub.publishCalls.Load()

			// Assertions with reversed oracle (acceptance success):
			// 1. Hot loop is prevented: prepare calls bounded (<= 8 over 350ms, not 150+)
			if prepareCalls > 8 {
				t.Fatalf("hot loop detected: prepare calls = %d in 350ms (scan interval 100ms, wakeup=%v)", prepareCalls, wakeup)
			}
			// 2. id1 is durably stopped with retry_exhausted
			if stopReason1 == nil || *stopReason1 != "retry_exhausted" {
				t.Fatalf("id1 retry_stop_reason = %v, want 'retry_exhausted'", stopReason1)
			}
			// 3. id1 next_attempt_at was parked in the future (not unchanged due1)
			if dueAfter1.Equal(due1) {
				t.Fatalf("id1 next_attempt_at unchanged: %v", dueAfter1)
			}
			// 4. later work progressed and was granted/published!
			if attempts2 != 1 {
				t.Fatalf("id2 attempts = %d, want 1 (later work starved)", attempts2)
			}
			if publishCalls != 1 {
				t.Fatalf("publish calls = %d, want 1", publishCalls)
			}
		})
	}
}

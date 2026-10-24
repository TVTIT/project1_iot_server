package digitaltwin_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"

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

package digitaltwin_test

import (
	"fmt"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"
)

func TestNativeControlQueryReservationCooldownAndContextRecheck(t *testing.T) {
	f := newControlFixture(t)
	cfg := digitaltwin.DefaultControlConfig()
	cfg.Stage6Enabled = true
	repo, err := digitaltwin.NewPostgresControlRepository(f.app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.repo = repo

	urn2, _ := f.sensor(t, "query_sensor_2")
	urn3, _ := f.sensor(t, "query_sensor_3")
	f.exec(t, `UPDATE public.twin_states st SET next_query_at=clock_timestamp()+interval '1 hour'
		FROM public.twin_entities e WHERE st.entity_id=e.id AND e.entity_type='Sensor'`)
	f.exec(t, `UPDATE public.twin_states st SET next_query_at=NULL FROM public.twin_entities e
		WHERE st.entity_id=e.id AND e.entity_id=ANY($1::text[])`, []string{f.urn, urn2, urn3})
	// An active Sensor is visited and cooled down but must not monopolize the
	// first page; later due Sensors are still returned on subsequent bounded passes.
	if _, err := repo.Admit(f.ctx, f.owner, f.urn, controlRequest(t, "0.1", "0", "query-active")); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{urn2: true, urn3: true}
	var candidates []digitaltwin.ControlQueryContext
	for pass := 0; pass < 3; pass++ {
		batch, err := repo.ReserveDueControlQueries(f.ctx, 1, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) > 1 {
			t.Fatalf("reservation returned %d targets for page size one", len(batch))
		}
		if len(batch) == 1 {
			if !want[batch[0].EntityURN] {
				t.Fatalf("active/duplicate Sensor reserved: %+v", batch[0])
			}
			delete(want, batch[0].EntityURN)
			candidates = append(candidates, batch[0])
		}
	}
	if len(candidates) != 2 || len(want) != 0 {
		t.Fatalf("bounded due traversal starved later Sensors: candidates=%+v remaining=%v", candidates, want)
	}
	var scheduled time.Time
	if err := f.admin.QueryRow(f.ctx, `SELECT next_query_at FROM twin_states WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, candidates[0].EntityURN).Scan(&scheduled); err != nil {
		t.Fatal(err)
	}
	if !scheduled.After(time.Now()) {
		t.Fatalf("cooldown was not durably advanced: %v", scheduled)
	}

	// A reconstructed repository observes the persisted cooldown and cannot
	// rediscover already reserved Sensors until it becomes due again.
	restarted, err := digitaltwin.NewPostgresControlRepository(f.app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := restarted.ReserveDueControlQueries(f.ctx, 16, 30*time.Second); err != nil || len(again) != 0 {
		t.Fatalf("restart ignored durable cooldown: %+v/%v", again, err)
	}
	current, err := restarted.ControlQueryContextCurrent(f.ctx, candidates[0])
	if err != nil || !current {
		t.Fatalf("unchanged query context not current: %v/%v", current, err)
	}
	if _, err := restarted.Admit(f.ctx, f.owner, candidates[0].EntityURN,
		controlRequest(t, "0.2", fmt.Sprintf("%d", candidates[0].DesiredVersion), "query-context-change")); err != nil {
		t.Fatal(err)
	}
	current, err = restarted.ControlQueryContextCurrent(f.ctx, candidates[0])
	if err != nil || current {
		t.Fatalf("changed desired context remained current: %v/%v", current, err)
	}
}

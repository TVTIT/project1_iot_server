package gateway

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Destructive fixtures are enabled only by the isolated-container harness.
func TestPostgresProvisioningIntegration(t *testing.T) {
	if os.Getenv("PROVISIONING_TEST_ISOLATED") != "1" {
		t.Skip("run sh scripts/test-stage2-provisioning.sh")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	setup, err := pgxpool.New(ctx, os.Getenv("PROVISIONING_TEST_SETUP_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close()
	app, err := pgxpool.New(ctx, os.Getenv("PROVISIONING_TEST_BACKEND_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	var role string
	if err := app.QueryRow(ctx, "SELECT current_user").Scan(&role); err != nil || role != "iot_backend_app" {
		t.Fatal("integration must use backend app role")
	}
	repo, err := NewPostgresProvisioningRepository(app)
	if err != nil {
		t.Fatal(err)
	}
	admin, owner, other := uuid.New(), uuid.New(), uuid.New()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := setup.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec("INSERT INTO profiles(id) VALUES ($1),($2),($3)", admin, owner, other)
	exec("INSERT INTO platform_admins(user_id) VALUES ($1)", admin)
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		// This database belongs exclusively to the harness.
		if _, err := setup.Exec(cleanupCtx, "DELETE FROM twin_entities"); err != nil {
			t.Error(err)
		}
		if _, err := setup.Exec(cleanupCtx, "DELETE FROM gateways"); err != nil {
			t.Error(err)
		}
		if _, err := setup.Exec(cleanupCtx, "DELETE FROM profiles WHERE id IN ($1,$2,$3)", admin, owner, other); err != nil {
			t.Error(err)
		}
	}()
	input := ProvisionGatewayInput{GatewayID: "test_" + uuid.NewString(), Name: "Gateway", OwnerUserID: owner}
	assertNoGateway := func(id string) {
		t.Helper()
		var n int
		if err := setup.QueryRow(ctx, "SELECT count(*) FROM gateways WHERE gateway_id=$1", id).Scan(&n); err != nil || n != 0 {
			t.Fatalf("unexpected gateway: count=%d error=%v", n, err)
		}
	}
	t.Run("missing owner and nonadmin leave no rows", func(t *testing.T) {
		missing := input
		missing.OwnerUserID = uuid.New()
		if _, err := repo.ProvisionGateway(ctx, admin, missing); !errors.Is(err, ErrProvisioningNotFound) {
			t.Fatalf("missing owner: %v", err)
		}
		if _, err := repo.ProvisionGateway(ctx, other, input); !errors.Is(err, ErrProvisioningForbidden) {
			t.Fatalf("nonadmin: %v", err)
		}
		assertNoGateway(input.GatewayID)
	})
	var first ProvisionGatewayResult
	var initialSnapshot string
	snapshot := func() string {
		t.Helper()
		var value string
		if err := setup.QueryRow(ctx, `SELECT jsonb_build_array(to_jsonb(g),to_jsonb(ug),to_jsonb(e),to_jsonb(s))::text
		FROM gateways g JOIN user_gateways ug USING(gateway_id)
		JOIN twin_entities e USING(gateway_id) JOIN twin_states s ON s.entity_id=e.id
		WHERE g.gateway_id=$1`, input.GatewayID).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	t.Run("create complete empty graph", func(t *testing.T) {
		first, err = repo.ProvisionGateway(ctx, admin, input)
		if err != nil || !first.Created || first.CreatedAt == nil || first.EntityID != "urn:ngsi-ld:Gateway:"+input.GatewayID {
			t.Fatalf("create: %#v %v", first, err)
		}
		var valid bool
		if err := setup.QueryRow(ctx, `SELECT e.entity_type='Gateway' AND e.name=$2 AND e.attributes='{}'::jsonb
		AND s.reported_state='{}'::jsonb AND s.desired_state='{}'::jsonb
		AND s.reported_version=0 AND s.desired_version=0 AND s.last_reported_at IS NULL
		AND s.last_desired_at IS NULL AND s.last_desired_by IS NULL
		AND (SELECT count(*) FROM user_gateways WHERE gateway_id=$1 AND role='owner' AND user_id=$3)=1
		FROM twin_entities e JOIN twin_states s ON s.entity_id=e.id WHERE e.gateway_id=$1`, input.GatewayID, input.Name, owner).Scan(&valid); err != nil || !valid {
			t.Fatalf("initial graph: %v %v", valid, err)
		}
		initialSnapshot = snapshot()
	})
	t.Run("retry preserves full representation IDs timestamps and advanced state", func(t *testing.T) {
		got, err := repo.ProvisionGateway(ctx, admin, input)
		want := first
		want.Created = false
		if err != nil || !reflect.DeepEqual(got, want) || snapshot() != initialSnapshot {
			t.Fatalf("retry: %#v %v", got, err)
		}
		exec(`UPDATE twin_states SET reported_state='{"value":42}',desired_state='{"interval":5}',
		reported_version=9,desired_version=4,last_reported_at=now(),last_desired_at=now(),last_desired_by=$2
		WHERE entity_id=(SELECT id FROM twin_entities WHERE gateway_id=$1)`, input.GatewayID, owner)
		advanced := snapshot()
		if _, err := repo.ProvisionGateway(ctx, admin, input); err != nil {
			t.Fatal(err)
		}
		if snapshot() != advanced {
			t.Fatal("retry reset advanced state or timestamps")
		}
	})
	t.Run("changed metadata nullable description and owner conflict", func(t *testing.T) {
		for _, mutate := range []func(*ProvisionGatewayInput){func(i *ProvisionGatewayInput) { i.Name = "changed" }, func(i *ProvisionGatewayInput) { i.Description = new(string) }, func(i *ProvisionGatewayInput) { i.OwnerUserID = other }} {
			changed := input
			mutate(&changed)
			before := snapshot()
			if _, err := repo.ProvisionGateway(ctx, admin, changed); !errors.Is(err, ErrProvisioningConflict) {
				t.Fatalf("conflict: %v", err)
			}
			if snapshot() != before {
				t.Fatal("conflict mutated graph")
			}
		}
	})
	t.Run("missing state never repaired", func(t *testing.T) {
		exec("DELETE FROM twin_states WHERE entity_id=(SELECT id FROM twin_entities WHERE gateway_id=$1)", input.GatewayID)
		if _, err := repo.ProvisionGateway(ctx, admin, input); !errors.Is(err, ErrProvisioningInconsistent) {
			t.Fatalf("incomplete graph: %v", err)
		}
		var n int
		if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_states").Scan(&n); err != nil || n != 0 {
			t.Fatalf("state restored: %d %v", n, err)
		}
	})
	t.Run("removed owner never restored", func(t *testing.T) {
		exec("DELETE FROM user_gateways WHERE gateway_id=$1", input.GatewayID)
		if _, err := repo.ProvisionGateway(ctx, admin, input); !errors.Is(err, ErrProvisioningInconsistent) {
			t.Fatalf("removed owner: %v", err)
		}
		var n int
		if err := setup.QueryRow(ctx, "SELECT count(*) FROM user_gateways WHERE gateway_id=$1", input.GatewayID).Scan(&n); err != nil || n != 0 {
			t.Fatalf("owner restored: %d %v", n, err)
		}
	})
	t.Run("concurrent identical creates one graph", func(t *testing.T) {
		concurrent := input
		concurrent.GatewayID = "test_" + uuid.NewString()
		var wg sync.WaitGroup
		results := make(chan ProvisionGatewayResult, 2)
		failures := make(chan error, 2)
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				got, err := repo.ProvisionGateway(ctx, admin, concurrent)
				results <- got
				failures <- err
			}()
		}
		wg.Wait()
		close(results)
		close(failures)
		for err := range failures {
			if err != nil {
				t.Fatal(err)
			}
		}
		created := 0
		for got := range results {
			if got.Created {
				created++
			}
		}
		if created != 1 {
			t.Fatalf("created %d times", created)
		}
	})
	t.Run("concurrent differing requests have one winner", func(t *testing.T) {
		a := input
		a.GatewayID = "test_" + uuid.NewString()
		b := a
		b.Name = "different"
		var wg sync.WaitGroup
		failures := make(chan error, 2)
		for _, request := range []ProvisionGatewayInput{a, b} {
			wg.Add(1)
			go func(in ProvisionGatewayInput) {
				defer wg.Done()
				_, err := repo.ProvisionGateway(ctx, admin, in)
				failures <- err
			}(request)
		}
		wg.Wait()
		close(failures)
		success, conflict := 0, 0
		for err := range failures {
			switch {
			case err == nil:
				success++
			case errors.Is(err, ErrProvisioningConflict):
				conflict++
			default:
				t.Fatal(err)
			}
		}
		if success != 1 || conflict != 1 {
			t.Fatalf("success=%d conflict=%d", success, conflict)
		}
	})
	t.Run("legacy incomplete graph no automatic repair", func(t *testing.T) {
		legacy := input
		legacy.GatewayID = "test_" + uuid.NewString()
		exec("INSERT INTO gateways(gateway_id,name) VALUES ($1,$2)", legacy.GatewayID, legacy.Name)
		exec("INSERT INTO user_gateways(user_id,gateway_id,role) VALUES ($1,$2,'owner')", owner, legacy.GatewayID)
		if _, err := repo.ProvisionGateway(ctx, admin, legacy); !errors.Is(err, ErrProvisioningInconsistent) {
			t.Fatalf("missing twin: %v", err)
		}
		var n int
		if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_entities WHERE gateway_id=$1", legacy.GatewayID).Scan(&n); err != nil || n != 0 {
			t.Fatalf("twin repaired: %d %v", n, err)
		}
	})
	t.Run("state insertion failure rolls back every write", func(t *testing.T) {
		// Fault injection is local to this disposable database, not a migration.
		exec(`CREATE FUNCTION test_fail_state() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'injected state failure'; END $$`)
		exec(`CREATE TRIGGER test_fail_state BEFORE INSERT ON twin_states FOR EACH ROW EXECUTE FUNCTION test_fail_state()`)
		defer exec("DROP FUNCTION test_fail_state() CASCADE")
		failed := input
		failed.GatewayID = "test_" + uuid.NewString()
		if _, err := repo.ProvisionGateway(ctx, admin, failed); err == nil {
			t.Fatal("injected failure succeeded")
		}
		assertNoGateway(failed.GatewayID)
		var n int
		if err := setup.QueryRow(ctx, `SELECT (SELECT count(*) FROM user_gateways WHERE gateway_id=$1)
		+(SELECT count(*) FROM twin_entities WHERE gateway_id=$1)`, failed.GatewayID).Scan(&n); err != nil || n != 0 {
			t.Fatalf("partial graph: %d %v", n, err)
		}
	})
	t.Run("locked mutation deadline rolls back", func(t *testing.T) {
		tx, err := setup.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer rollbackProvisioning(tx)
		if _, err := tx.Exec(ctx, "LOCK TABLE user_gateways IN ACCESS EXCLUSIVE MODE"); err != nil {
			t.Fatal(err)
		}
		blocked := input
		blocked.GatewayID = "test_" + uuid.NewString()
		deadline, stop := context.WithTimeout(ctx, 150*time.Millisecond)
		_, err = repo.ProvisionGateway(deadline, admin, blocked)
		stop()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline: %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		assertNoGateway(blocked.GatewayID)
		if _, err := repo.ProvisionGateway(ctx, admin, blocked); err != nil {
			t.Fatalf("pool unusable after rollback: %v", err)
		}
	})
	t.Run("admin lookup deadline is distinct", func(t *testing.T) {
		tx, err := setup.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer rollbackProvisioning(tx)
		if _, err := tx.Exec(ctx, "LOCK TABLE platform_admins IN ACCESS EXCLUSIVE MODE"); err != nil {
			t.Fatal(err)
		}
		deadline, stop := context.WithTimeout(ctx, 100*time.Millisecond)
		defer stop()
		_, err = repo.ProvisionGateway(deadline, admin, input)
		if !errors.Is(err, ErrProvisioningAdminLookup) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("lookup: %v", err)
		}
	})
	t.Run("cancelled caller", func(t *testing.T) {
		cancelled, stop := context.WithCancel(ctx)
		stop()
		if _, err := repo.ProvisionGateway(cancelled, admin, input); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel: %v", err)
		}
	})
	t.Run("Sensor transactional provisioning", func(t *testing.T) {
		parent := ProvisionGatewayInput{GatewayID: "test_" + uuid.NewString(), Name: "Parent", OwnerUserID: owner}
		if _, err := repo.ProvisionGateway(ctx, admin, parent); err != nil {
			t.Fatal(err)
		}
		in := ProvisionSensorInput{GatewayID: parent.GatewayID, SensorID: "backend_service", Name: " Cảm biến ' ; -- "}
		noSensor := func(i ProvisionSensorInput) {
			t.Helper()
			var n int
			if err := setup.QueryRow(ctx, `SELECT (SELECT count(*) FROM sensors WHERE gateway_id=$1 AND sensor_id=$2)
			+(SELECT count(*) FROM twin_entities WHERE entity_id=$3)`, i.GatewayID, i.SensorID, "urn:ngsi-ld:Sensor:"+i.GatewayID+":"+i.SensorID).Scan(&n); err != nil || n != 0 {
				t.Fatalf("partial Sensor graph: %d %v", n, err)
			}
		}
		if _, err := repo.ProvisionSensor(ctx, other, in); !errors.Is(err, ErrProvisioningForbidden) {
			t.Fatalf("nonadmin: %v", err)
		}
		noSensor(in)
		missing := in
		missing.GatewayID = "test_" + uuid.NewString()
		if _, err := repo.ProvisionSensor(ctx, admin, missing); !errors.Is(err, ErrProvisioningNotFound) {
			t.Fatalf("missing parent: %v", err)
		}
		noSensor(missing)
		first, err := repo.ProvisionSensor(ctx, admin, in)
		if err != nil || !first.Created || first.CreatedAt == nil || first.EntityID != "urn:ngsi-ld:Sensor:"+in.GatewayID+":"+in.SensorID {
			t.Fatalf("create Sensor: %#v %v", first, err)
		}
		var valid bool
		if err := setup.QueryRow(ctx, `SELECT e.entity_type='Sensor' AND e.gateway_id=$1 AND e.name=$3 AND e.attributes='{}'::jsonb
		AND s.reported_state='{}'::jsonb AND s.desired_state='{}'::jsonb AND s.reported_version=0 AND s.desired_version=0
		AND s.last_reported_at IS NULL AND s.last_desired_at IS NULL AND s.last_desired_by IS NULL
		AND (SELECT count(*) FROM twin_relationships r JOIN twin_entities p ON p.id=r.source_entity_id
		WHERE r.target_entity_id=e.id AND r.relationship_type='hasSensor' AND p.entity_id=$4)=1
		FROM twin_entities e JOIN twin_states s ON s.entity_id=e.id WHERE e.entity_id=$2`, in.GatewayID, first.EntityID, in.Name, "urn:ngsi-ld:Gateway:"+in.GatewayID).Scan(&valid); err != nil || !valid {
			t.Fatalf("empty graph: %v %v", valid, err)
		}
		snapshot := func() string {
			t.Helper()
			var value string
			if err := setup.QueryRow(ctx, `SELECT jsonb_build_array(to_jsonb(se),to_jsonb(e),to_jsonb(s),to_jsonb(r))::text
			FROM sensors se JOIN twin_entities e ON e.entity_id=$3 JOIN twin_states s ON s.entity_id=e.id
			JOIN twin_relationships r ON r.target_entity_id=e.id AND r.relationship_type='hasSensor'
			WHERE se.gateway_id=$1 AND se.sensor_id=$2`, in.GatewayID, in.SensorID, first.EntityID).Scan(&value); err != nil {
				t.Fatal(err)
			}
			return value
		}
		before := snapshot()
		got, err := repo.ProvisionSensor(ctx, admin, in)
		want := first
		want.Created = false
		if err != nil || !reflect.DeepEqual(got, want) || snapshot() != before {
			t.Fatalf("retry Sensor: %#v %v", got, err)
		}
		exec(`UPDATE twin_states SET reported_state='{"value":42}',desired_state='{"interval":5}',reported_version=8,desired_version=3,
		last_reported_at=now(),last_desired_at=now(),last_desired_by=$2 WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, first.EntityID, owner)
		before = snapshot()
		if _, err := repo.ProvisionSensor(ctx, admin, in); err != nil || snapshot() != before {
			t.Fatalf("advanced retry: %v", err)
		}
		for _, change := range []func(*ProvisionSensorInput){func(i *ProvisionSensorInput) { i.Unit = new(string) }, func(i *ProvisionSensorInput) { i.Name = "changed" }} {
			conflict := in
			change(&conflict)
			if _, err := repo.ProvisionSensor(ctx, admin, conflict); !errors.Is(err, ErrProvisioningConflict) || snapshot() != before {
				t.Fatalf("metadata conflict: %v", err)
			}
		}
		second := parent
		second.GatewayID = "test_" + uuid.NewString()
		if _, err := repo.ProvisionGateway(ctx, admin, second); err != nil {
			t.Fatal(err)
		}
		isolated := in
		isolated.GatewayID = second.GatewayID
		unit := "°C ' --"
		isolated.Unit = &unit
		otherSensor, err := repo.ProvisionSensor(ctx, admin, isolated)
		if err != nil || !otherSensor.Created || otherSensor.EntityID == first.EntityID || snapshot() != before {
			t.Fatalf("two Gateway isolation: %#v %v", otherSensor, err)
		}
		// Retry requires the relationship and state; neither is silently repaired.
		exec(`DELETE FROM twin_relationships WHERE target_entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, first.EntityID)
		if _, err := repo.ProvisionSensor(ctx, admin, in); !errors.Is(err, ErrProvisioningInconsistent) {
			t.Fatalf("missing relationship: %v", err)
		}
		var n int
		if err := setup.QueryRow(ctx, `SELECT count(*) FROM twin_relationships WHERE target_entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, first.EntityID).Scan(&n); err != nil || n != 0 {
			t.Fatalf("repaired relationship: %d %v", n, err)
		}
		exec(`DELETE FROM twin_states WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, otherSensor.EntityID)
		if _, err := repo.ProvisionSensor(ctx, admin, isolated); !errors.Is(err, ErrProvisioningInconsistent) {
			t.Fatalf("missing Sensor state: %v", err)
		}
		if err := setup.QueryRow(ctx, `SELECT count(*) FROM twin_states WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, otherSensor.EntityID).Scan(&n); err != nil || n != 0 {
			t.Fatalf("repaired Sensor state: %d %v", n, err)
		}
		// Parent identity/type/scope/state must be coherent, not merely present.
		badParent := parent
		badParent.GatewayID = "test_" + uuid.NewString()
		if _, err := repo.ProvisionGateway(ctx, admin, badParent); err != nil {
			t.Fatal(err)
		}
		badChild := in
		badChild.GatewayID = badParent.GatewayID
		parentURN := "urn:ngsi-ld:Gateway:" + badParent.GatewayID
		for index, query := range []string{
			`UPDATE twin_entities SET entity_id=entity_id || ':wrong' WHERE entity_id=$1`,
			`UPDATE twin_entities SET entity_type='Device' WHERE entity_id=$1`,
			`UPDATE twin_entities SET gateway_id=$2 WHERE entity_id=$1`,
		} {
			if index == 2 {
				exec(query, parentURN, parent.GatewayID)
			} else {
				exec(query, parentURN)
			}
			if _, err := repo.ProvisionSensor(ctx, admin, badChild); !errors.Is(err, ErrProvisioningInconsistent) {
				t.Fatalf("inconsistent parent: %v", err)
			}
			noSensor(badChild)
			exec(`UPDATE twin_entities SET entity_id=$1,entity_type='Gateway',gateway_id=$2 WHERE entity_id=$1 OR entity_id=$1 || ':wrong'`, parentURN, badParent.GatewayID)
		}
		// A parent with no state cannot acquire a new Sensor.
		exec(`DELETE FROM twin_states WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, "urn:ngsi-ld:Gateway:"+second.GatewayID)
		bad := isolated
		bad.SensorID = "new_sensor"
		if _, err := repo.ProvisionSensor(ctx, admin, bad); !errors.Is(err, ErrProvisioningInconsistent) {
			t.Fatalf("missing parent state: %v", err)
		}
		noSensor(bad)
		legacy := parent
		legacy.GatewayID = "test_" + uuid.NewString()
		exec(`INSERT INTO gateways(gateway_id,name) VALUES ($1,$2)`, legacy.GatewayID, legacy.Name)
		bad.GatewayID = legacy.GatewayID
		if _, err := repo.ProvisionSensor(ctx, admin, bad); !errors.Is(err, ErrProvisioningInconsistent) {
			t.Fatalf("missing parent twin: %v", err)
		}
		noSensor(bad)
		// Inject failure at the final write to prove every preceding write rolls back.
		exec(`CREATE FUNCTION test_fail_relationship() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected relationship failure'; END $$`)
		exec(`CREATE TRIGGER test_fail_relationship BEFORE INSERT ON twin_relationships FOR EACH ROW EXECUTE FUNCTION test_fail_relationship()`)
		bad.GatewayID = parent.GatewayID
		_, err = repo.ProvisionSensor(ctx, admin, bad)
		exec(`DROP FUNCTION test_fail_relationship() CASCADE`)
		if err == nil {
			t.Fatal("injected failure succeeded")
		}
		noSensor(bad)
		// Same parent requests are serialized by its row lock, with one creator.
		for _, differing := range []bool{false, true} {
			a := in
			a.SensorID = "test_" + uuid.NewString()
			b := a
			if differing {
				b.Name = "different"
			}
			type outcome struct {
				result ProvisionSensorResult
				err    error
			}
			outcomes := make(chan outcome, 2)
			var wg sync.WaitGroup
			for _, request := range []ProvisionSensorInput{a, b} {
				wg.Add(1)
				go func(i ProvisionSensorInput) {
					defer wg.Done()
					r, e := repo.ProvisionSensor(ctx, admin, i)
					outcomes <- outcome{r, e}
				}(request)
			}
			wg.Wait()
			close(outcomes)
			created, retries, conflicts := 0, 0, 0
			for o := range outcomes {
				switch {
				case errors.Is(o.err, ErrProvisioningConflict):
					conflicts++
				case o.err != nil:
					t.Fatal(o.err)
				case o.result.Created:
					created++
				default:
					retries++
				}
			}
			if created != 1 || (!differing && retries != 1) || (differing && conflicts != 1) {
				t.Fatalf("concurrent Sensor created=%d retry=%d conflicts=%d", created, retries, conflicts)
			}
		}
	})
	t.Run("privilege and side effect boundaries", func(t *testing.T) {
		for _, query := range []string{"INSERT INTO profiles(id) VALUES ($1)", "INSERT INTO platform_admins(user_id) VALUES ($1)"} {
			_, err := app.Exec(ctx, query, uuid.New())
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
				t.Fatalf("privilege: %v", err)
			}
		}
		var n int
		if err := setup.QueryRow(ctx, `SELECT (SELECT count(*) FROM gateway_mqtt_credentials)+(SELECT count(*) FROM gateway_mqtt_credential_events)
		+(SELECT count(*) FROM twin_commands)+(SELECT count(*) FROM twin_outbox)+(SELECT count(*) FROM twin_temporal_values)
		`).Scan(&n); err != nil || n != 0 {
			t.Fatalf("unexpected side effects: %d %v", n, err)
		}
	})
	t.Run("every insert and deferred commit failure is atomic", func(t *testing.T) {
		parent := input
		parent.GatewayID = "test_" + uuid.NewString()
		if _, err := repo.ProvisionGateway(ctx, admin, parent); err != nil {
			t.Fatal("create fault-test parent failed")
		}
		for _, sensor := range []bool{false, true} {
			tables := []string{"gateways", "user_gateways", "twin_entities", "twin_states"}
			if sensor {
				tables = []string{"sensors", "twin_entities", "twin_states", "twin_relationships"}
			}
			for _, table := range tables {
				for _, deferred := range []bool{false, true} {
					t.Run(fmt.Sprintf("sensor=%t/%s/deferred=%t", sensor, table, deferred), func(t *testing.T) {
						exec(`CREATE FUNCTION test_insert_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test-only insert fault'; END $$`)
						trigger := "CREATE TRIGGER test_insert_fault BEFORE INSERT ON " + table
						if deferred {
							trigger = "CREATE CONSTRAINT TRIGGER test_insert_fault AFTER INSERT ON " + table + " DEFERRABLE INITIALLY DEFERRED"
						}
						exec(trigger + " FOR EACH ROW EXECUTE FUNCTION test_insert_fault()")
						defer exec("DROP FUNCTION test_insert_fault() CASCADE")
						id := "test_" + uuid.NewString()
						var operationErr error
						if sensor {
							got, e := repo.ProvisionSensor(ctx, admin, ProvisionSensorInput{GatewayID: parent.GatewayID, SensorID: id, Name: "fault"})
							operationErr = e
							if got.Created {
								t.Fatal("failed operation claimed creation")
							}
						} else {
							in := input
							in.GatewayID = id
							got, e := repo.ProvisionGateway(ctx, admin, in)
							operationErr = e
							if got.Created {
								t.Fatal("failed operation claimed creation")
							}
						}
						if operationErr == nil {
							t.Fatal("injected failure succeeded")
						}
						var n int
						// Count all affected graph pieces, including state/relationships.
						if err := setup.QueryRow(ctx, `SELECT
						(SELECT count(*) FROM gateways WHERE gateway_id=$1)+
						(SELECT count(*) FROM user_gateways WHERE gateway_id=$1)+
						(SELECT count(*) FROM sensors WHERE sensor_id=$1)+
						(SELECT count(*) FROM twin_entities WHERE gateway_id=$1 OR entity_id=$2)+
						(SELECT count(*) FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.gateway_id=$1 OR e.entity_id=$2)+
						(SELECT count(*) FROM twin_relationships r JOIN twin_entities e ON e.id=r.target_entity_id WHERE e.gateway_id=$1 OR e.entity_id=$2)`, id, "urn:ngsi-ld:Sensor:"+parent.GatewayID+":"+id).Scan(&n); err != nil || n != 0 {
							t.Fatal("partial graph after injected rollback")
						}
					})
				}
			}
		}
	})
	t.Run("Sensor parent lock timeout and caller cancellation", func(t *testing.T) {
		parent := input
		parent.GatewayID = "test_" + uuid.NewString()
		if _, err := repo.ProvisionGateway(ctx, admin, parent); err != nil {
			t.Fatal("parent creation failed")
		}
		tx, err := setup.Begin(ctx)
		if err != nil {
			t.Fatal("lock transaction failed")
		}
		defer rollbackProvisioning(tx)
		if _, err := tx.Exec(ctx, "SELECT 1 FROM gateways WHERE gateway_id=$1 FOR UPDATE", parent.GatewayID); err != nil {
			t.Fatal("lock failed")
		}
		in := ProvisionSensorInput{GatewayID: parent.GatewayID, SensorID: "blocked", Name: "blocked"}
		deadline, stop := context.WithTimeout(ctx, 150*time.Millisecond)
		got, err := repo.ProvisionSensor(deadline, admin, in)
		stop()
		if !errors.Is(err, context.DeadlineExceeded) || got.Created {
			t.Fatal("Sensor timeout not fail closed")
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if got, err := repo.ProvisionSensor(cancelled, admin, in); !errors.Is(err, context.Canceled) || got.Created {
			t.Fatal("Sensor cancellation not fail closed")
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal("unlock failed")
		}
		var n int
		if err := setup.QueryRow(ctx, "SELECT count(*) FROM sensors WHERE gateway_id=$1", parent.GatewayID).Scan(&n); err != nil || n != 0 {
			t.Fatal("cancelled Sensor persisted")
		}
		if got, err := repo.ProvisionSensor(ctx, admin, in); err != nil || !got.Created {
			t.Fatal("pool unusable after cancellation")
		}
	})
	t.Run("distinct concurrent Sensors preserve both relationships", func(t *testing.T) {
		parent := input
		parent.GatewayID = "test_" + uuid.NewString()
		if _, err := repo.ProvisionGateway(ctx, admin, parent); err != nil {
			t.Fatal("parent creation failed")
		}
		start := make(chan struct{})
		failures := make(chan error, 2)
		for _, id := range []string{"first", "second"} {
			go func(id string) {
				<-start
				got, err := repo.ProvisionSensor(ctx, admin, ProvisionSensorInput{GatewayID: parent.GatewayID, SensorID: id, Name: id})
				if err == nil && !got.Created {
					err = errors.New("distinct Sensor was not created")
				}
				failures <- err
			}(id)
		}
		close(start)
		for range 2 {
			select {
			case err := <-failures:
				if err != nil {
					t.Fatal("concurrent distinct Sensor failed")
				}
			case <-ctx.Done():
				t.Fatal("concurrency test exceeded deadline")
			}
		}
		var n int
		if err := setup.QueryRow(ctx, `SELECT count(*) FROM twin_relationships r JOIN twin_entities p ON p.id=r.source_entity_id JOIN twin_entities e ON e.id=r.target_entity_id JOIN twin_states s ON s.entity_id=e.id WHERE p.gateway_id=$1 AND p.entity_type='Gateway' AND e.gateway_id=$1 AND e.entity_type='Sensor' AND r.relationship_type='hasSensor'`, parent.GatewayID).Scan(&n); err != nil || n != 2 {
			t.Fatal("lost concurrent relationship")
		}
	})
	t.Run("closed pool denies both operations", func(t *testing.T) {
		closed, err := pgxpool.New(ctx, os.Getenv("PROVISIONING_TEST_BACKEND_URL"))
		if err != nil {
			t.Fatal("pool configuration failed")
		}
		closed.Close()
		r, err := NewPostgresProvisioningRepository(closed)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := r.ProvisionGateway(ctx, admin, input); err == nil || got.Created {
			t.Fatal("closed pool Gateway succeeded")
		}
		if got, err := r.ProvisionSensor(ctx, admin, ProvisionSensorInput{GatewayID: input.GatewayID, SensorID: "closed", Name: "closed"}); err == nil || got.Created {
			t.Fatal("closed pool Sensor succeeded")
		}
	})
	t.Run("lost commit confirmation is not a rollback guarantee", func(t *testing.T) {
		lost, err := NewPostgresProvisioningRepository(lostCommitDatabase{app})
		if err != nil {
			t.Fatal(err)
		}
		in := input
		in.GatewayID = "test_" + uuid.NewString()
		if got, err := lost.ProvisionGateway(ctx, admin, in); err == nil || got.Created {
			t.Fatal("ambiguous Gateway commit claimed success")
		}
		if got, err := repo.ProvisionGateway(ctx, admin, in); err != nil || got.Created {
			t.Fatal("committed Gateway not resolved as retry")
		}
		sensor := ProvisionSensorInput{GatewayID: in.GatewayID, SensorID: "ambiguous", Name: "ambiguous"}
		if got, err := lost.ProvisionSensor(ctx, admin, sensor); err == nil || got.Created {
			t.Fatal("ambiguous Sensor commit claimed success")
		}
		if got, err := repo.ProvisionSensor(ctx, admin, sensor); err != nil || got.Created {
			t.Fatal("committed Sensor not resolved as retry")
		}
	})
}

// A network error from COMMIT is ambiguous: the server may already have
// committed. Do not promise rollback; return no success and let an identical
// retry resolve the authoritative graph. This adapter deliberately commits and
// then loses the confirmation to exercise that boundary without network sleeps.
type lostCommitDatabase struct{ ProvisioningDatabase }

func (d lostCommitDatabase) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	tx, err := d.ProvisioningDatabase.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return lostCommitTx{Tx: tx}, nil
}

type lostCommitTx struct{ pgx.Tx }

func (tx lostCommitTx) Commit(ctx context.Context) error {
	if err := tx.Tx.Commit(ctx); err != nil {
		return err
	}
	return errors.New("test-only lost commit confirmation")
}

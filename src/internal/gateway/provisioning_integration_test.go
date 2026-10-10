package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
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
	t.Run("owner sensor provisioning real postgres tests", func(t *testing.T) {
		testPostgresOwnerSensorProvisioning(ctx, t, setup, app, input)
	})
}

func testPostgresOwnerSensorProvisioning(ctx context.Context, t *testing.T, setup, app *pgxpool.Pool, baseInput ProvisionGatewayInput) {
	adminRepo, err := NewPostgresProvisioningRepository(app)
	if err != nil {
		t.Fatal(err)
	}
	ownerRepo, err := NewPostgresOwnerSensorProvisioningRepository(app)
	if err != nil {
		t.Fatal(err)
	}

	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := setup.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("setup fixture: %v", err)
		}
	}

	admin, owner, operator, viewer, nonMember, otherOwner := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{admin, owner, operator, viewer, nonMember, otherOwner} {
		exec("INSERT INTO profiles(id) VALUES ($1)", id)
	}
	exec("INSERT INTO platform_admins(user_id) VALUES ($1)", admin)

	gwID := "ownergw_" + uuid.NewString()[:8]
	otherGwID := "othergw_" + uuid.NewString()[:8]

	gwInput := baseInput
	gwInput.GatewayID = gwID
	gwInput.OwnerUserID = owner
	if _, err := adminRepo.ProvisionGateway(ctx, admin, gwInput); err != nil {
		t.Fatalf("provision owner parent gateway: %v", err)
	}

	otherGwInput := baseInput
	otherGwInput.GatewayID = otherGwID
	otherGwInput.OwnerUserID = otherOwner
	if _, err := adminRepo.ProvisionGateway(ctx, admin, otherGwInput); err != nil {
		t.Fatalf("provision other parent gateway: %v", err)
	}

	exec("INSERT INTO user_gateways(user_id, gateway_id, role) VALUES ($1, $2, 'operator')", operator, gwID)
	exec("INSERT INTO user_gateways(user_id, gateway_id, role) VALUES ($1, $2, 'viewer')", viewer, gwID)

	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = setup.Exec(cleanup, "DELETE FROM gateways WHERE gateway_id IN ($1, $2)", gwID, otherGwID)
		_, _ = setup.Exec(cleanup, "DELETE FROM platform_admins WHERE user_id=$1", admin)
		for _, id := range []uuid.UUID{admin, owner, operator, viewer, nonMember, otherOwner} {
			_, _ = setup.Exec(cleanup, "DELETE FROM profiles WHERE id=$1", id)
		}
	}()

	sensorInput := ProvisionSensorInput{
		GatewayID: gwID,
		SensorID:  "temp_01",
		Name:      "Temperature Sensor",
		Unit:      func(s string) *string { return &s }("°C"),
	}

	// 1. Role denials on owner route: operator (403), viewer (403), non-member (404), other owner (404), admin without owner (404)
	roleDenialBaseline := snapshotOwnerGraph(t, ctx, setup)
	if _, err := ownerRepo.ProvisionSensorForOwner(ctx, operator, sensorInput); !errors.Is(err, ErrProvisioningForbidden) {
		t.Fatalf("operator expected ErrProvisioningForbidden, got %v", err)
	}
	assertOwnerGraphEqual(t, roleDenialBaseline, snapshotOwnerGraph(t, ctx, setup))
	if _, err := ownerRepo.ProvisionSensorForOwner(ctx, viewer, sensorInput); !errors.Is(err, ErrProvisioningForbidden) {
		t.Fatalf("viewer expected ErrProvisioningForbidden, got %v", err)
	}
	assertOwnerGraphEqual(t, roleDenialBaseline, snapshotOwnerGraph(t, ctx, setup))
	if _, err := ownerRepo.ProvisionSensorForOwner(ctx, nonMember, sensorInput); !errors.Is(err, ErrProvisioningNotFound) {
		t.Fatalf("non-member expected ErrProvisioningNotFound, got %v", err)
	}
	assertOwnerGraphEqual(t, roleDenialBaseline, snapshotOwnerGraph(t, ctx, setup))
	if _, err := ownerRepo.ProvisionSensorForOwner(ctx, otherOwner, sensorInput); !errors.Is(err, ErrProvisioningNotFound) {
		t.Fatalf("cross-gateway owner expected ErrProvisioningNotFound, got %v", err)
	}
	assertOwnerGraphEqual(t, roleDenialBaseline, snapshotOwnerGraph(t, ctx, setup))
	if _, err := ownerRepo.ProvisionSensorForOwner(ctx, admin, sensorInput); !errors.Is(err, ErrProvisioningNotFound) {
		t.Fatalf("admin without owner membership expected ErrProvisioningNotFound, got %v", err)
	}
	assertOwnerGraphEqual(t, roleDenialBaseline, snapshotOwnerGraph(t, ctx, setup))
	// Missing gateway parent -> ErrProvisioningNotFound
	missingGwInput := sensorInput
	missingGwInput.GatewayID = "missing_gw"
	if _, err := ownerRepo.ProvisionSensorForOwner(ctx, owner, missingGwInput); !errors.Is(err, ErrProvisioningNotFound) {
		t.Fatalf("missing parent expected ErrProvisioningNotFound, got %v", err)
	}
	assertOwnerGraphEqual(t, roleDenialBaseline, snapshotOwnerGraph(t, ctx, setup))

	// 2. Owner success: creates Sensor, Twin entity, Twin state, and hasSensor atomically
	created, err := ownerRepo.ProvisionSensorForOwner(ctx, owner, sensorInput)
	if err != nil || !created.Created {
		t.Fatalf("owner create failed: res=%+v err=%v", created, err)
	}
	if created.CreatedAt == nil {
		t.Fatal("created_at must be populated on create")
	}

	// Verify database rows exist and cohere
	var sensorCount, twinCount, stateCount, relCount int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM sensors WHERE gateway_id=$1 AND sensor_id=$2", gwID, sensorInput.SensorID).Scan(&sensorCount); err != nil || sensorCount != 1 {
		t.Fatalf("sensors table count = %d, err = %v", sensorCount, err)
	}
	sensorEntityURN := "urn:ngsi-ld:Sensor:" + gwID + ":" + sensorInput.SensorID
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_entities WHERE entity_id=$1 AND entity_type='Sensor'", sensorEntityURN).Scan(&twinCount); err != nil || twinCount != 1 {
		t.Fatalf("twin_entities count = %d", twinCount)
	}
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=$1", sensorEntityURN).Scan(&stateCount); err != nil || stateCount != 1 {
		t.Fatalf("twin_states count = %d", stateCount)
	}
	parentGatewayURN := "urn:ngsi-ld:Gateway:" + gwID
	if err := setup.QueryRow(ctx, `SELECT count(*) FROM twin_relationships r
		JOIN twin_entities p ON p.id=r.source_entity_id
		JOIN twin_entities c ON c.id=r.target_entity_id
		WHERE p.entity_id=$1 AND c.entity_id=$2 AND r.relationship_type='hasSensor'`, parentGatewayURN, sensorEntityURN).Scan(&relCount); err != nil || relCount != 1 {
		t.Fatalf("hasSensor relationship count = %d", relCount)
	}

	// Advance twin state to verify retry does NOT reset state
	var entityUUID uuid.UUID
	if err := setup.QueryRow(ctx, "SELECT id FROM twin_entities WHERE entity_id=$1", sensorEntityURN).Scan(&entityUUID); err != nil {
		t.Fatal(err)
	}
	exec("UPDATE twin_states SET reported_version=42, reported_state='{\"temp\":25.5}'::jsonb WHERE entity_id=$1", entityUUID)
	exec(`UPDATE twin_states SET desired_state='{"sampling_interval_seconds":7,"enabled":false}'::jsonb,
	 desired_version=23, last_desired_by=$2, last_reported_at='2026-09-01T01:02:03Z',
	 last_desired_at='2026-09-02T02:03:04Z', updated_at='2026-09-03T03:04:05Z' WHERE entity_id=$1`, entityUUID, owner)
	exec(`UPDATE twin_entities SET attributes='{"fixture":"preserve","nested":{"n":9}}'::jsonb,
	 created_at='2026-08-01T01:02:03Z', updated_at='2026-08-02T02:03:04Z' WHERE id=$1`, entityUUID)
	exec("UPDATE twin_relationships SET created_at='2026-08-03T03:04:05Z' WHERE target_entity_id=$1", entityUUID)
	advancedGraph := snapshotOwnerGraph(t, ctx, setup)

	// 3. Identical retry: returns 200 (created=false), preserves timestamps, IDs, and does NOT reset advanced twin state
	retried, err := ownerRepo.ProvisionSensorForOwner(ctx, owner, sensorInput)
	if err != nil || retried.Created {
		t.Fatalf("owner retry failed: res=%+v err=%v", retried, err)
	}
	if retried.CreatedAt == nil || !retried.CreatedAt.Equal(*created.CreatedAt) {
		t.Fatalf("retry createdAt mismatch: got %v, want %v", retried.CreatedAt, created.CreatedAt)
	}
	if retried.GatewayID != created.GatewayID || retried.SensorID != created.SensorID || retried.EntityID != created.EntityID {
		t.Fatalf("retry returned identity changed: %+v -> %+v", created, retried)
	}
	assertOwnerGraphEqual(t, advancedGraph, snapshotOwnerGraph(t, ctx, setup))
	t.Log("owner identical retry preserved full four-table snapshot: reported+desired JSON, versions, actor, all timestamps, Sensor metadata, internal/public identities and exact hasSensor endpoints")
	var repVer int64
	var repState string
	if err := setup.QueryRow(ctx, "SELECT reported_version, reported_state::text FROM twin_states WHERE entity_id=$1", entityUUID).Scan(&repVer, &repState); err != nil || repVer != 42 || !strings.Contains(repState, "25.5") {
		t.Fatalf("retry reset advanced twin state: ver=%d state=%s", repVer, repState)
	}

	// 4. Same ID with different metadata -> ErrProvisioningConflict (409)
	diffInput := sensorInput
	diffInput.Name = "Different Name"
	if _, err := ownerRepo.ProvisionSensorForOwner(ctx, owner, diffInput); !errors.Is(err, ErrProvisioningConflict) {
		t.Fatalf("expected ErrProvisioningConflict on name change, got %v", err)
	}
	assertOwnerGraphEqual(t, advancedGraph, snapshotOwnerGraph(t, ctx, setup))
	t.Log("owner name conflict preserved full four-table snapshot")
	diffUnitInput := sensorInput
	diffUnitInput.Unit = func(s string) *string { return &s }("Kelvin")
	if _, err := ownerRepo.ProvisionSensorForOwner(ctx, owner, diffUnitInput); !errors.Is(err, ErrProvisioningConflict) {
		t.Fatalf("expected ErrProvisioningConflict on unit change, got %v", err)
	}
	assertOwnerGraphEqual(t, advancedGraph, snapshotOwnerGraph(t, ctx, setup))
	t.Log("owner unit conflict preserved full four-table snapshot")

	// 5. Inconsistent parent / orphan twin -> ErrProvisioningInconsistent (no auto-heal)
	orphanSensorID := "orphan_s"
	orphanURN := "urn:ngsi-ld:Sensor:" + gwID + ":" + orphanSensorID
	exec("INSERT INTO twin_entities(entity_id, entity_type, name, gateway_id) VALUES ($1, 'Sensor', 'Orphan', $2)", orphanURN, gwID)
	orphanBaseline := snapshotOwnerGraph(t, ctx, setup)
	orphanInput := sensorInput
	orphanInput.SensorID = orphanSensorID
	if _, err := ownerRepo.ProvisionSensorForOwner(ctx, owner, orphanInput); !errors.Is(err, ErrProvisioningInconsistent) {
		t.Fatalf("expected ErrProvisioningInconsistent for orphan twin, got %v", err)
	}
	assertOwnerGraphEqual(t, orphanBaseline, snapshotOwnerGraph(t, ctx, setup))

	// 6. Deleted profile -> ErrProvisioningNotFound
	deletedOwnerID := uuid.New()
	exec("INSERT INTO profiles(id) VALUES ($1)", deletedOwnerID)
	exec("INSERT INTO user_gateways(user_id, gateway_id, role) VALUES ($1, $2, 'owner')", deletedOwnerID, gwID)
	exec("DELETE FROM profiles WHERE id=$1", deletedOwnerID) // Cascades user_gateways
	deletedProfileBaseline := snapshotOwnerGraph(t, ctx, setup)
	if _, err := ownerRepo.ProvisionSensorForOwner(ctx, deletedOwnerID, ProvisionSensorInput{GatewayID: gwID, SensorID: "deleted_s", Name: "temp"}); !errors.Is(err, ErrProvisioningNotFound) {
		t.Fatalf("deleted profile expected ErrProvisioningNotFound, got %v", err)
	}
	assertOwnerGraphEqual(t, deletedProfileBaseline, snapshotOwnerGraph(t, ctx, setup))

	// 7. Concurrent same-payload (exactly one create, one retry) and different-metadata (exactly one create, one conflict)
	t.Run("concurrent same-payload exactly one create one retry", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
		var wg sync.WaitGroup
		defer cleanupOwnerWorkers(t, cancel, func() {}, &wg)
		before := snapshotOwnerGraph(t, ctx, setup)
		concurrentSameID := "conc_same_s"
		sameIn := sensorInput
		sameIn.SensorID = concurrentSameID
		sameIn.Name = "Concurrent Same"
		type resType struct {
			res ProvisionSensorResult
			err error
		}
		chRes := make(chan resType, 2)
		startCh := make(chan struct{})
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-startCh
				r, err := ownerRepo.ProvisionSensorForOwner(ctx, owner, sameIn)
				chRes <- resType{res: r, err: err}
			}()
		}
		close(startCh)
		createdCount := 0
		retriedCount := 0
		var firstCreatedAt time.Time
		var winner, retry ProvisionSensorResult
		for range 2 {
			var out resType
			select {
			case out = <-chRes:
			case <-ctx.Done():
				t.Fatal("concurrent owner completion deadline")
			}
			if out.err != nil {
				t.Fatalf("concurrent same payload failed: %v", out.err)
			}
			if out.res.Created {
				winner = out.res
				createdCount++
				firstCreatedAt = *out.res.CreatedAt
			} else {
				retry = out.res
				retriedCount++
			}
		}
		if createdCount != 1 || retriedCount != 1 {
			t.Fatalf("expected exactly 1 create and 1 retry, got created=%d retried=%d", createdCount, retriedCount)
		}
		if retry.CreatedAt == nil || winner.CreatedAt == nil || !retry.CreatedAt.Equal(*winner.CreatedAt) || retry.EntityID != winner.EntityID || retry.GatewayID != winner.GatewayID || retry.SensorID != winner.SensorID || retry.Name != winner.Name || !reflect.DeepEqual(retry.Unit, winner.Unit) {
			t.Fatal("concurrent retry representation differs from winner")
		}
		assertPersistedOwnerWinner(t, before, snapshotOwnerGraph(t, ctx, setup), winner)
		var countSame int
		if err := setup.QueryRow(ctx, "SELECT count(*) FROM sensors WHERE gateway_id=$1 AND sensor_id=$2", gwID, concurrentSameID).Scan(&countSame); err != nil || countSame != 1 {
			t.Fatalf("concurrent same created %d rows", countSame)
		}
		var dbCreatedAt time.Time
		if err := setup.QueryRow(ctx, "SELECT created_at FROM sensors WHERE gateway_id=$1 AND sensor_id=$2", gwID, concurrentSameID).Scan(&dbCreatedAt); err != nil || !dbCreatedAt.Equal(firstCreatedAt) {
			t.Fatalf("stable created_at mismatch: db=%v first=%v", dbCreatedAt, firstCreatedAt)
		}
	})

	t.Run("concurrent different-metadata exactly one create one conflict", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
		var wg sync.WaitGroup
		defer cleanupOwnerWorkers(t, cancel, func() {}, &wg)
		before := snapshotOwnerGraph(t, ctx, setup)
		concurrentDiffID := "conc_diff_s"
		diffInA := sensorInput
		diffInA.SensorID = concurrentDiffID
		diffInA.Name = "Payload A"
		diffInB := sensorInput
		diffInB.SensorID = concurrentDiffID
		diffInB.Name = "Payload B"

		type resType struct {
			res ProvisionSensorResult
			err error
		}
		chRes := make(chan resType, 2)
		startCh := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-startCh
			r, err := ownerRepo.ProvisionSensorForOwner(ctx, owner, diffInA)
			chRes <- resType{res: r, err: err}
		}()
		go func() {
			defer wg.Done()
			<-startCh
			r, err := ownerRepo.ProvisionSensorForOwner(ctx, owner, diffInB)
			chRes <- resType{res: r, err: err}
		}()
		close(startCh)

		createdCount := 0
		conflictCount := 0
		var winner ProvisionSensorResult
		for range 2 {
			var out resType
			select {
			case out = <-chRes:
			case <-ctx.Done():
				t.Fatal("concurrent owner completion deadline")
			}
			if out.err == nil && out.res.Created {
				winner = out.res
				createdCount++
			} else if errors.Is(out.err, ErrProvisioningConflict) {
				conflictCount++
			} else {
				t.Fatalf("unexpected concurrent outcome: %+v %v", out.res, out.err)
			}
		}
		if createdCount != 1 || conflictCount != 1 {
			t.Fatalf("expected exactly 1 create and 1 conflict, got created=%d conflict=%d", createdCount, conflictCount)
		}
		assertPersistedOwnerWinner(t, before, snapshotOwnerGraph(t, ctx, setup), winner)
	})

	// RDB-01: exercise the actual owner entrypoint in both serial orders.
	for _, firstAdmin := range []bool{false, true} {
		for _, different := range []bool{false, true} {
			t.Run(fmt.Sprintf("mixed admin owner admin-first=%t different=%t", firstAdmin, different), func(t *testing.T) {
				c, cancel := context.WithTimeout(ctx, 8*time.Second)
				reached, started := make(chan uint32, 1), make(chan uint32, 1)
				releaseCh := make(chan struct{})
				var once sync.Once
				release := func() { once.Do(func() { close(releaseCh) }) }
				var wg sync.WaitGroup
				defer cleanupOwnerWorkers(t, cancel, release, &wg)
				before := snapshotOwnerGraph(t, c, setup)
				firstDB := barrierSensorDatabase{ProvisioningDatabase: app, onParentAcquired: func(c context.Context, pid uint32) error {
					reached <- pid
					select {
					case <-releaseCh:
						return nil
					case <-c.Done():
						return c.Err()
					}
				}}
				secondDB := barrierSensorDatabase{ProvisioningDatabase: app, onBegin: func(pid uint32) { started <- pid }}
				firstRepo, err := NewPostgresProvisioningRepository(firstDB)
				if err != nil {
					t.Fatal(err)
				}
				secondRepo, err := NewPostgresProvisioningRepository(secondDB)
				if err != nil {
					t.Fatal(err)
				}
				firstOwnerRepo, err := NewPostgresOwnerSensorProvisioningRepository(firstDB)
				if err != nil {
					t.Fatal(err)
				}
				secondOwnerRepo, err := NewPostgresOwnerSensorProvisioningRepository(secondDB)
				if err != nil {
					t.Fatal(err)
				}
				input := sensorInput
				input.SensorID = fmt.Sprintf("mixed_%t_%t", firstAdmin, different)
				otherInput := input
				if different {
					otherInput.Name = "Mixed loser metadata"
				}
				var winner, second ProvisionSensorResult
				firstDone, secondDone := make(chan error, 1), make(chan error, 1)
				wg.Add(1)
				go func() {
					defer wg.Done()
					var e error
					if firstAdmin {
						winner, e = firstRepo.ProvisionSensor(c, admin, input)
					} else {
						winner, e = firstOwnerRepo.ProvisionSensorForOwner(c, owner, input)
					}
					firstDone <- e
				}()
				blocker := awaitProvisioningPID(t, c, reached)
				wg.Add(1)
				go func() {
					defer wg.Done()
					var e error
					if firstAdmin {
						second, e = secondOwnerRepo.ProvisionSensorForOwner(c, owner, otherInput)
					} else {
						second, e = secondRepo.ProvisionSensor(c, admin, otherInput)
					}
					secondDone <- e
				}()
				waiter := awaitProvisioningPID(t, c, started)
				assertProvisioningLockWait(t, c, setup, waiter, blocker, "SELECT name FROM public.gateways WHERE gateway_id=$1 FOR UPDATE", secondDone)
				release()
				if err := awaitProvisioningWorker(t, c, firstDone); err != nil {
					t.Fatal(err)
				}
				err = awaitProvisioningWorker(t, c, secondDone)
				if different {
					if !errors.Is(err, ErrProvisioningConflict) {
						t.Fatalf("mixed conflict: %v", err)
					}
				} else if err != nil || second.Created || second.EntityID != winner.EntityID || second.CreatedAt == nil || !second.CreatedAt.Equal(*winner.CreatedAt) {
					t.Fatalf("mixed retry: %+v %v", second, err)
				}
				assertPersistedOwnerWinner(t, before, snapshotOwnerGraph(t, c, setup), winner)
			})
		}
	}
	t.Run("absent membership grant after lookup does not authorize in flight", func(t *testing.T) {
		actor := uuid.New()
		exec("INSERT INTO profiles(id) VALUES ($1)", actor)
		defer exec("DELETE FROM profiles WHERE id=$1", actor)
		c, cancel := context.WithTimeout(ctx, 8*time.Second)
		reached := make(chan uint32, 1)
		releaseCh := make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(releaseCh) }) }
		var wg sync.WaitGroup
		defer cleanupOwnerWorkers(t, cancel, release, &wg)
		db := barrierSensorDatabase{ProvisioningDatabase: app, onMembershipAbsent: func(c context.Context, pid uint32) error {
			reached <- pid
			select {
			case <-releaseCh:
				return nil
			case <-c.Done():
				return c.Err()
			}
		}}
		repo, err := NewPostgresOwnerSensorProvisioningRepository(db)
		if err != nil {
			t.Fatal(err)
		}
		input := sensorInput
		input.SensorID = "absent_grant_s"
		before := snapshotOwnerGraph(t, c, setup)
		done := make(chan error, 1)
		wg.Add(1)
		go func() { defer wg.Done(); _, e := repo.ProvisionSensorForOwner(c, actor, input); done <- e }()
		pid := awaitProvisioningPID(t, c, reached)
		// Scan returned ErrNoRows: no nonexistent membership-row lock is claimed.
		// The INSERT's FK parent check instead waits on the held Gateway lock.
		const grantSQL = "INSERT INTO user_gateways(user_id,gateway_id,role) VALUES ($1,$2,'owner')"
		conn, err := setup.Acquire(c)
		if err != nil {
			t.Fatal(err)
		}
		grantPID := conn.Conn().PgConn().PID()
		grantDone := make(chan error, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.Release()
			_, e := conn.Exec(c, grantSQL, actor, gwID)
			grantDone <- e
		}()
		assertProvisioningLockWait(t, c, setup, grantPID, pid, grantSQL, grantDone)
		release()
		if err := awaitProvisioningWorker(t, c, done); !errors.Is(err, ErrProvisioningNotFound) {
			t.Fatalf("in-flight absent membership: %v", err)
		}
		if err := awaitProvisioningWorker(t, c, grantDone); err != nil {
			t.Fatal(err)
		}
		assertOwnerGraphEqual(t, before, snapshotOwnerGraph(t, c, setup))
		winner, err := ownerRepo.ProvisionSensorForOwner(c, actor, input)
		if err != nil {
			t.Fatal(err)
		}
		assertPersistedOwnerWinner(t, before, snapshotOwnerGraph(t, c, setup), winner)
		t.Logf("exact owner pid=%d observed ErrNoRows before committed grant; denied unchanged; fresh request created", pid)
	})

	for _, mutation := range []struct {
		name, sql string
		denied    error
	}{
		{"UPDATE", "UPDATE user_gateways SET role='viewer' WHERE user_id=$1 AND gateway_id=$2", ErrProvisioningForbidden},
		{"DELETE", "DELETE FROM user_gateways WHERE user_id=$1 AND gateway_id=$2", ErrProvisioningNotFound},
		{"PROFILE_DELETE", "DELETE FROM profiles WHERE id=$1 AND $2::text IS NOT NULL", ErrProvisioningNotFound},
	} {
		for _, authFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("lock ordering %s auth-first=%t", mutation.name, authFirst), func(t *testing.T) {
				actor := owner
				if mutation.name == "PROFILE_DELETE" {
					actor = uuid.New()
					exec("INSERT INTO profiles(id) VALUES ($1)", actor)
					defer exec("DELETE FROM profiles WHERE id=$1", actor)
					exec("INSERT INTO user_gateways(user_id,gateway_id,role) VALUES ($1,$2,'owner')", actor, gwID)
				}
				exec("INSERT INTO user_gateways(user_id,gateway_id,role) VALUES ($1,$2,'owner') ON CONFLICT (user_id,gateway_id) DO UPDATE SET role='owner'", owner, gwID)
				defer exec("INSERT INTO user_gateways(user_id,gateway_id,role) VALUES ($1,$2,'owner') ON CONFLICT (user_id,gateway_id) DO UPDATE SET role='owner'", owner, gwID)
				runCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
				releaseCh := make(chan struct{})
				var once sync.Once
				release := func() { once.Do(func() { close(releaseCh) }) }
				var workers sync.WaitGroup
				var lockTx pgx.Tx
				// Registered before starting any worker: Fatal/timeout releases all locks,
				// cancels SQL and the barrier, and joins before fixture pools close.
				defer func() {
					cancel()
					release()
					if lockTx != nil {
						rollbackProvisioning(lockTx)
					}
					joined := make(chan struct{})
					go func() { workers.Wait(); close(joined) }()
					select {
					case <-joined:
					case <-time.After(5 * time.Second):
						t.Error("lock workers failed to join after cancellation")
					}
				}()
				started := make(chan uint32, 1)
				acquired := make(chan uint32, 1)
				db := barrierSensorDatabase{ProvisioningDatabase: app,
					onBegin: func(pid uint32) { started <- pid },
					onMembershipAcquired: func(c context.Context, pid uint32) error {
						if !authFirst {
							return nil
						}
						acquired <- pid
						select {
						case <-releaseCh:
							return nil
						case <-c.Done():
							return c.Err()
						}
					},
				}
				repo, err := NewPostgresOwnerSensorProvisioningRepository(db)
				if err != nil {
					t.Fatal(err)
				}
				if !authFirst {
					lockTx, err = setup.Begin(runCtx)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = lockTx.Exec(runCtx, "SELECT name FROM gateways WHERE gateway_id=$1 FOR UPDATE", gwID); err != nil {
						t.Fatal(err)
					}
				}
				input := sensorInput
				input.SensorID = fmt.Sprintf("lock_%s_%t", mutation.name, authFirst)
				snapshotBeforeProvision := snapshotOwnerGraph(t, runCtx, setup)
				var winner ProvisionSensorResult // Read only after provDone synchronizes worker completion.
				provDone := make(chan error, 1)
				workers.Add(1)
				go func() {
					defer workers.Done()
					var e error
					winner, e = repo.ProvisionSensorForOwner(runCtx, actor, input)
					provDone <- e
				}()
				ownerPID := awaitProvisioningPID(t, runCtx, started)
				if !authFirst {
					assertProvisioningLockWait(t, runCtx, setup, ownerPID, lockTx.Conn().PgConn().PID(), "SELECT name FROM public.gateways WHERE gateway_id=$1 FOR UPDATE", provDone)
					beforeMutation := snapshotOwnerGraph(t, runCtx, setup)
					if _, err := setup.Exec(runCtx, mutation.sql, actor, gwID); err != nil {
						t.Fatal(err)
					}
					// profiles/user_gateways cascade is intended; graph tables have no
					// disposable actor references and must remain exactly unchanged.
					deniedBaseline := snapshotOwnerGraph(t, runCtx, setup)
					assertOwnerGraphEqual(t, beforeMutation, deniedBaseline)
					if mutation.name == "PROFILE_DELETE" {
						assertDeletedOwnerProfile(t, runCtx, setup, actor)
					}
					if err := lockTx.Rollback(runCtx); err != nil {
						t.Fatal(err)
					}
					if err := awaitProvisioningWorker(t, runCtx, provDone); !errors.Is(err, mutation.denied) {
						t.Fatalf("denial after committed %s: %v", mutation.name, err)
					}
					assertOwnerGraphEqual(t, deniedBaseline, snapshotOwnerGraph(t, runCtx, setup))
					t.Logf("%s revoke-first denial: full four-table graph unchanged across intended authority mutation and denied owner call", mutation.name)
					var count int
					if err := setup.QueryRow(runCtx, "SELECT count(*) FROM sensors WHERE gateway_id=$1 AND sensor_id=$2", gwID, input.SensorID).Scan(&count); err != nil || count != 0 {
						t.Fatalf("denied sensor count=%d error=%v", count, err)
					}
					return
				}
				if pid := awaitProvisioningPID(t, runCtx, acquired); pid != ownerPID {
					t.Fatal("membership barrier changed backend")
				}
				idle, err := observeProvisioningLock(runCtx, setup, ownerPID)
				if err != nil {
					t.Fatal(err)
				}
				if idle.waitEvent != "ClientRead" || idle.matches(ownerPID, idle.query) {
					t.Fatalf("idle negative witness: %+v", idle)
				}
				t.Logf("native negative idle witness pid=%d state=%s type=%s event=%s rejected", ownerPID, idle.state, idle.waitType, idle.waitEvent)
				conn, err := setup.Acquire(runCtx)
				if err != nil {
					t.Fatal(err)
				}
				// Worker owns/relinquishes this exact competing connection.
				mutationPID := conn.Conn().PgConn().PID()
				mutationDone := make(chan error, 1)
				workers.Add(1)
				go func() {
					defer workers.Done()
					defer conn.Release()
					_, e := conn.Exec(runCtx, mutation.sql, actor, gwID)
					mutationDone <- e
				}()
				assertProvisioningLockWait(t, runCtx, setup, mutationPID, ownerPID, mutation.sql, mutationDone)
				unrelated, err := observeProvisioningLock(runCtx, setup, mutationPID)
				if err != nil {
					t.Fatal(err)
				}
				if unrelated.matches(mutationPID, mutation.sql) {
					t.Fatal("unrelated blocker accepted")
				}
				t.Logf("native negative unrelated lock witness waiter=%d actual_blockers=%v expected_wrong_blocker=%d rejected", mutationPID, unrelated.blockers, mutationPID)
				release()
				if err := awaitProvisioningWorker(t, runCtx, provDone); err != nil {
					t.Fatal(err)
				}
				if err := awaitProvisioningWorker(t, runCtx, mutationDone); err != nil {
					t.Fatal(err)
				}
				assertPersistedOwnerWinner(t, snapshotBeforeProvision, snapshotOwnerGraph(t, runCtx, setup), winner)
				if mutation.name == "PROFILE_DELETE" {
					assertDeletedOwnerProfile(t, runCtx, setup, actor)
					var complete bool
					if err := setup.QueryRow(runCtx, `SELECT EXISTS (
					 SELECT 1 FROM sensors s JOIN twin_entities e ON e.entity_id=$3 AND e.gateway_id=s.gateway_id AND e.entity_type='Sensor'
					 JOIN twin_states st ON st.entity_id=e.id
					 JOIN twin_relationships r ON r.target_entity_id=e.id AND r.relationship_type='hasSensor'
					 JOIN twin_entities p ON p.id=r.source_entity_id AND p.entity_id=$4 AND p.gateway_id=s.gateway_id
					 WHERE s.gateway_id=$1 AND s.sensor_id=$2 AND s.name=$5 AND s.unit IS NOT DISTINCT FROM $6
					 AND e.name=$5)`, gwID, input.SensorID, "urn:ngsi-ld:Sensor:"+gwID+":"+input.SensorID, parentGatewayURN, input.Name, input.Unit).Scan(&complete); err != nil || !complete {
						t.Fatalf("profile auth-first complete committed graph=%t error=%v", complete, err)
					}
					t.Log("profile auth-first: complete committed Sensor/entity/state/exact parent-hasSensor graph survives intended profile/membership cascade")
				}
				postMutationGraph := snapshotOwnerGraph(t, runCtx, setup)
				if _, err := ownerRepo.ProvisionSensorForOwner(runCtx, actor, ProvisionSensorInput{GatewayID: gwID, SensorID: "after_lock_s", Name: "temp"}); !errors.Is(err, mutation.denied) {
					t.Fatalf("subsequent denial: %v", err)
				}
				assertOwnerGraphEqual(t, postMutationGraph, snapshotOwnerGraph(t, runCtx, setup))
			})
		}
	}
	t.Run("lock barrier cancellation rolls back and joins", func(t *testing.T) {
		runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		reached := make(chan uint32, 1)
		db := barrierSensorDatabase{ProvisioningDatabase: app, onMembershipAcquired: func(c context.Context, pid uint32) error {
			reached <- pid
			<-c.Done()
			return c.Err()
		}}
		repo, err := NewPostgresOwnerSensorProvisioningRepository(db)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		joined := make(chan struct{})
		defer func() {
			cancel()
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Error("cancelled barrier worker failed to join")
			}
		}()
		input := sensorInput
		input.SensorID = "cancelled_barrier_s"
		go func() {
			defer close(joined)
			_, err := repo.ProvisionSensorForOwner(runCtx, owner, input)
			done <- err
		}()
		pid := awaitProvisioningPID(t, runCtx, reached)
		cancel()
		cleanupCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		if err := awaitProvisioningWorker(t, cleanupCtx, done); !errors.Is(err, context.Canceled) {
			t.Fatalf("barrier cancellation: %v", err)
		}
		// A fresh mutation must finish after rollback; no leaked membership lock.
		if _, err := setup.Exec(cleanupCtx, "UPDATE user_gateways SET role='owner' WHERE user_id=$1 AND gateway_id=$2", owner, gwID); err != nil {
			t.Fatal(err)
		}
		t.Logf("cancelled real owner barrier pid=%d returned, rolled back and joined; fresh membership UPDATE completed", pid)
	})

	// 9. Actual owner-path fault rollback and parent-corruption assertions
	t.Run("owner-path fault rollback and parent corruption", func(t *testing.T) {
		faultSensorID := "fault_rollback_s"
		faultInput := sensorInput
		faultInput.SensorID = faultSensorID

		// Injected fault trigger on twin_relationships
		exec(`CREATE FUNCTION test_owner_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owner-path injected relationship fault'; END $$`)
		exec("CREATE TRIGGER test_owner_fault BEFORE INSERT ON twin_relationships FOR EACH ROW EXECUTE FUNCTION test_owner_fault()")
		defer exec("DROP FUNCTION test_owner_fault() CASCADE")
		faultBaseline := snapshotOwnerGraph(t, ctx, setup)

		_, err := ownerRepo.ProvisionSensorForOwner(ctx, owner, faultInput)
		if err == nil {
			t.Fatal("expected failure on injected trigger, got success")
		}
		assertOwnerGraphEqual(t, faultBaseline, snapshotOwnerGraph(t, ctx, setup))

		// Verify complete rollback: zero rows in sensors, twin_entities, twin_states, twin_relationships
		var sCnt, eCnt, stCnt, rCnt int
		if err := setup.QueryRow(ctx, "SELECT count(*) FROM sensors WHERE gateway_id=$1 AND sensor_id=$2", gwID, faultSensorID).Scan(&sCnt); err != nil || sCnt != 0 {
			t.Fatalf("sensor table was not rolled back: %d", sCnt)
		}
		faultURN := "urn:ngsi-ld:Sensor:" + gwID + ":" + faultSensorID
		if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_entities WHERE entity_id=$1", faultURN).Scan(&eCnt); err != nil || eCnt != 0 {
			t.Fatalf("twin_entities was not rolled back: %d", eCnt)
		}
		if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=$1", faultURN).Scan(&stCnt); err != nil || stCnt != 0 {
			t.Fatalf("twin_states was not rolled back: %d", stCnt)
		}
		if err := setup.QueryRow(ctx, "SELECT count(*) FROM twin_relationships r JOIN twin_entities e ON e.id=r.target_entity_id WHERE e.entity_id=$1", faultURN).Scan(&rCnt); err != nil || rCnt != 0 {
			t.Fatalf("twin_relationships was not rolled back: %d", rCnt)
		}

		// Parent gateway twin corruption: duplicate parent gateway twin entities
		dupParentURN := "urn:ngsi-ld:Gateway:" + gwID + "_dup"
		exec("INSERT INTO twin_entities(entity_id, entity_type, name, gateway_id) VALUES ($1, 'Gateway', 'Corrupted Gateway', $2)", dupParentURN, gwID)
		defer exec("DELETE FROM twin_entities WHERE entity_id=$1", dupParentURN)
		corruptBaseline := snapshotOwnerGraph(t, ctx, setup)

		_, err = ownerRepo.ProvisionSensorForOwner(ctx, owner, ProvisionSensorInput{GatewayID: gwID, SensorID: "corrupted_parent_s", Name: "temp"})
		if !errors.Is(err, ErrProvisioningInconsistent) {
			t.Fatalf("expected ErrProvisioningInconsistent on corrupted parent gateway twin, got %v", err)
		}
		assertOwnerGraphEqual(t, corruptBaseline, snapshotOwnerGraph(t, ctx, setup))
	})
}

// Whole-table snapshots are safe only in this owned, serial isolated fixture.
// One statement/snapshot captures every column (including nullable actor,
// identities, timestamps and exact relationship endpoints), not joined counts
// which could hide an orphan. Canonical JSONB ordering makes equality exact.
type ownerGraphSnapshot [4]string

func cleanupOwnerWorkers(t *testing.T, cancel context.CancelFunc, release func(), wg *sync.WaitGroup) {
	t.Helper()
	cancel()
	release()
	joined := make(chan struct{})
	go func() { wg.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Error("owner evidence workers failed bounded join")
	}
}

// Validate exactly one complete new graph and byte-semantic preservation of
// every pre-existing row. Missing/orphan/extra rows cannot hide in an inner join.
func comparePersistedOwnerWinner(before, after ownerGraphSnapshot, winner ProvisionSensorResult) error {
	if !winner.Created || winner.CreatedAt == nil || winner.EntityID != "urn:ngsi-ld:Sensor:"+winner.GatewayID+":"+winner.SensorID {
		return errors.New("invalid winner representation")
	}
	var added [4]map[string]any
	for table := range before {
		var oldRows, newRows []map[string]any
		if err := json.Unmarshal([]byte(before[table]), &oldRows); err != nil {
			return err
		}
		if err := json.Unmarshal([]byte(after[table]), &newRows); err != nil {
			return err
		}
		remaining := append([]map[string]any(nil), newRows...)
		for _, old := range oldRows {
			found := -1
			for i, row := range remaining {
				if reflect.DeepEqual(old, row) {
					found = i
					break
				}
			}
			if found < 0 {
				return fmt.Errorf("existing graph row changed in table %d", table)
			}
			remaining = append(remaining[:found], remaining[found+1:]...)
		}
		if len(remaining) != 1 {
			return fmt.Errorf("expected one added graph row in table %d, got %d", table, len(remaining))
		}
		added[table] = remaining[0]
	}
	s, e, st, r := added[0], added[1], added[2], added[3]
	var unit any
	if winner.Unit != nil {
		unit = *winner.Unit
	}
	stamp, err := time.Parse(time.RFC3339Nano, fmt.Sprint(s["created_at"]))
	if err != nil || !stamp.Equal(*winner.CreatedAt) || s["gateway_id"] != winner.GatewayID || s["sensor_id"] != winner.SensorID || s["name"] != winner.Name || s["unit"] != unit {
		return errors.New("persisted Sensor does not match actual winner")
	}
	if e["entity_id"] != winner.EntityID || e["entity_type"] != "Sensor" || e["gateway_id"] != winner.GatewayID || e["name"] != winner.Name || st["entity_id"] != e["id"] || r["target_entity_id"] != e["id"] || r["relationship_type"] != "hasSensor" {
		return errors.New("winner entity/state/relationship identities mismatch")
	}
	if st["reported_version"] != float64(0) || st["desired_version"] != float64(0) || !reflect.DeepEqual(st["reported_state"], map[string]any{}) || !reflect.DeepEqual(st["desired_state"], map[string]any{}) {
		return errors.New("unexpected new winner state")
	}
	var entities []map[string]any
	if err := json.Unmarshal([]byte(before[1]), &entities); err != nil {
		return err
	}
	for _, parent := range entities {
		if parent["entity_id"] == "urn:ngsi-ld:Gateway:"+winner.GatewayID && parent["entity_type"] == "Gateway" && parent["id"] == r["source_entity_id"] {
			return nil
		}
	}
	return errors.New("hasSensor source is not exact parent")
}

func assertPersistedOwnerWinner(t *testing.T, before, after ownerGraphSnapshot, winner ProvisionSensorResult) {
	t.Helper()
	if err := comparePersistedOwnerWinner(before, after, winner); err != nil {
		t.Fatal(err)
	}
	t.Logf("full four-table winner graph and unchanged existing rows: %s/%s name=%q entity=%s", winner.GatewayID, winner.SensorID, winner.Name, winner.EntityID)
}

func TestPersistedOwnerWinnerCounterexamples(t *testing.T) {
	stamp := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	winner := ProvisionSensorResult{Created: true, GatewayID: "fixture", SensorID: "s", Name: "winner", EntityID: "urn:ngsi-ld:Sensor:fixture:s", CreatedAt: &stamp}
	before := ownerGraphSnapshot{"[]", `[{"id":"parent","entity_id":"urn:ngsi-ld:Gateway:fixture","entity_type":"Gateway"}]`, "[]", "[]"}
	after := ownerGraphSnapshot{
		`[{"gateway_id":"fixture","sensor_id":"s","name":"winner","unit":null,"created_at":"2026-09-01T00:00:00Z"}]`,
		`[{"id":"parent","entity_id":"urn:ngsi-ld:Gateway:fixture","entity_type":"Gateway"},{"id":"child","entity_id":"urn:ngsi-ld:Sensor:fixture:s","entity_type":"Sensor","gateway_id":"fixture","name":"winner"}]`,
		`[{"entity_id":"child","reported_version":0,"desired_version":0,"reported_state":{},"desired_state":{}}]`,
		`[{"source_entity_id":"parent","target_entity_id":"child","relationship_type":"hasSensor"}]`,
	}
	if err := comparePersistedOwnerWinner(before, after, winner); err != nil {
		t.Fatal(err)
	}
	for table := range after {
		partial := after
		partial[table] = before[table]
		if err := comparePersistedOwnerWinner(before, partial, winner); err == nil {
			t.Fatalf("partial graph table%d accepted", table)
		}
	}
	wrong := winner
	wrong.Name = "loser"
	if err := comparePersistedOwnerWinner(before, after, wrong); err == nil {
		t.Fatal("wrong winner accepted")
	}
	for _, replacement := range []struct {
		table    int
		old, new string
	}{
		{3, `"source_entity_id":"parent"`, `"source_entity_id":"other"`},
		{2, `"desired_version":0`, `"desired_version":9`},
		{1, `"id":"parent"`, `"id":"changed-parent"`},
	} {
		changed := after
		changed[replacement.table] = strings.ReplaceAll(changed[replacement.table], replacement.old, replacement.new)
		if err := comparePersistedOwnerWinner(before, changed, winner); err == nil {
			t.Fatal("endpoint/state/existing row mutation accepted")
		}
	}
}

//nolint:revive // Match the existing test-helper convention: testing.T first.
func assertDeletedOwnerProfile(t *testing.T, ctx context.Context, pool *pgxpool.Pool, actor uuid.UUID) {
	t.Helper()
	var profiles, memberships int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM profiles WHERE id=$1),
	 (SELECT count(*) FROM user_gateways WHERE user_id=$1)`, actor).Scan(&profiles, &memberships); err != nil || profiles != 0 || memberships != 0 {
		t.Fatalf("profile cascade incomplete: profiles=%d memberships=%d err=%v", profiles, memberships, err)
	}
}

//nolint:revive // Match the existing test-helper convention: testing.T first.
func snapshotOwnerGraph(t *testing.T, ctx context.Context, pool *pgxpool.Pool) ownerGraphSnapshot {
	t.Helper()
	var s ownerGraphSnapshot
	err := pool.QueryRow(ctx, `SELECT
	 (SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY gateway_id,sensor_id),'[]'::jsonb)::text FROM sensors s),
	 (SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY id),'[]'::jsonb)::text FROM twin_entities e),
	 (SELECT COALESCE(jsonb_agg(to_jsonb(st) ORDER BY entity_id),'[]'::jsonb)::text FROM twin_states st),
	 (SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY source_entity_id,relationship_type,target_entity_id),'[]'::jsonb)::text FROM twin_relationships r)`).Scan(&s[0], &s[1], &s[2], &s[3])
	if err != nil {
		t.Fatalf("full owner graph snapshot: %v", err)
	}
	return s
}

func compareOwnerGraph(before, after ownerGraphSnapshot) error {
	for i, table := range []string{"sensors", "twin_entities", "twin_states", "twin_relationships"} {
		if before[i] != after[i] {
			return fmt.Errorf("persisted owner graph changed: %s", table)
		}
	}
	return nil
}

func assertOwnerGraphEqual(t *testing.T, before, after ownerGraphSnapshot) {
	t.Helper()
	if err := compareOwnerGraph(before, after); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerGraphSnapshotCounterexamples(t *testing.T) {
	baseline := ownerGraphSnapshot{
		`[{"gateway_id":"fixture","sensor_id":"s","name":"Temperature","unit":"C","created_at":"time1"}]`,
		`[{"id":"internal","entity_id":"urn:ngsi-ld:Sensor:fixture:s","entity_type":"Sensor","gateway_id":"fixture","name":"Temperature","attributes":{"n":9},"created_at":"time2","updated_at":"time3"}]`,
		`[{"entity_id":"internal","reported_state":{"temp":25.5},"desired_state":{"enabled":false},"reported_version":42,"desired_version":23,"last_desired_by":"actor","last_reported_at":"time4","last_desired_at":"time5","updated_at":"time6"}]`,
		`[{"source_entity_id":"parent","target_entity_id":"internal","relationship_type":"hasSensor","created_at":"time7"}]`,
	}
	assertOwnerGraphEqual(t, baseline, baseline)
	for table, serialized := range baseline {
		var rows []map[string]any
		if err := json.Unmarshal([]byte(serialized), &rows); err != nil {
			t.Fatal(err)
		}
		canonical, err := json.Marshal(rows)
		if err != nil {
			t.Fatal(err)
		}
		baseline[table] = string(canonical)
		serialized = baseline[table]
		for field := range rows[0] {
			t.Run(fmt.Sprintf("table%d/%s", table, field), func(t *testing.T) {
				var changedRows []map[string]any
				if err := json.Unmarshal([]byte(serialized), &changedRows); err != nil {
					t.Fatal(err)
				}
				changedRows[0][field] = "mutated"
				encoded, err := json.Marshal(changedRows)
				if err != nil {
					t.Fatal(err)
				}
				changed := baseline
				changed[table] = string(encoded)
				// compareOwnerGraph is the exact assertion engine used above.
				if err := compareOwnerGraph(baseline, changed); err == nil {
					t.Fatal("snapshot mutation escaped assertion helper")
				}
			})
		}
		removed := baseline
		removed[table] = "[]"
		if err := compareOwnerGraph(baseline, removed); err == nil {
			t.Fatal("row deletion escaped assertion helper")
		}
	}
}

// Query only the backend captured from the real connection. SQL text is an
// additional operation check, never a substitute for PID and blocker identity.
const provisioningLockObservationSQL = `SELECT state, COALESCE(wait_event_type,''), COALESCE(wait_event,''), query, pg_blocking_pids(pid)
 FROM pg_stat_activity WHERE pid=$1`

type provisioningLockObservation struct {
	state, waitType, waitEvent, query string
	blockers                          []int32
}

func (o provisioningLockObservation) matches(blocker uint32, statement string) bool {
	if o.state != "active" || o.waitType != "Lock" || o.query != statement {
		return false
	}
	for _, pid := range o.blockers {
		if uint32(pid) == blocker {
			return true
		}
	}
	return false
}

func observeProvisioningLock(ctx context.Context, pool *pgxpool.Pool, pid uint32) (provisioningLockObservation, error) {
	var o provisioningLockObservation
	err := pool.QueryRow(ctx, provisioningLockObservationSQL, pid).Scan(&o.state, &o.waitType, &o.waitEvent, &o.query, &o.blockers)
	return o, err
}

//nolint:revive // Match the existing test-helper convention: testing.T first.
func assertProvisioningLockWait(t *testing.T, ctx context.Context, pool *pgxpool.Pool, waiter, blocker uint32, statement string, done <-chan error) {
	t.Helper()
	deadline, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		o, err := observeProvisioningLock(deadline, pool, waiter)
		if err != nil {
			t.Fatalf("observe exact lock waiter PID=%d: %v", waiter, err)
		}
		select {
		case err := <-done:
			t.Fatalf("operation completed before lock release: %v", err)
		default:
		}
		if o.matches(blocker, statement) {
			t.Logf("native lock evidence waiter=%d blocker=%d state=%s wait_event_type=%s wait_event=%s blocking_pids=%v statement=%q", waiter, blocker, o.state, o.waitType, o.waitEvent, o.blockers, o.query)
			return
		}
		select {
		case <-deadline.Done():
			t.Fatalf("no exact lock evidence waiter=%d blocker=%d last=%+v", waiter, blocker, o)
		case err := <-done:
			t.Fatalf("operation completed before lock evidence: %v", err)
		case <-tick.C: // Sampling cadence only; passage of time is not lock proof.
		}
	}
}

//nolint:revive // Match the existing test-helper convention: testing.T first.
func awaitProvisioningPID(t *testing.T, ctx context.Context, ch <-chan uint32) uint32 {
	t.Helper()
	select {
	case pid := <-ch:
		return pid
	case <-ctx.Done():
		t.Fatal("backend/barrier deadline")
		return 0
	}
}

//nolint:revive // Match the existing test-helper convention: testing.T first.
func awaitProvisioningWorker(t *testing.T, ctx context.Context, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		t.Fatal("worker deadline")
		return ctx.Err()
	}
}

func TestProvisioningLockOracle(t *testing.T) {
	const statement = "UPDATE user_gateways SET role='viewer' WHERE user_id=$1 AND gateway_id=$2"
	for _, o := range []provisioningLockObservation{
		{state: "idle", waitType: "Client", waitEvent: "ClientRead", query: statement, blockers: []int32{42}},
		{state: "active", waitType: "Lock", query: statement, blockers: []int32{99}},
		{state: "active", waitType: "Lock", query: "unrelated statement", blockers: []int32{42}},
	} {
		if o.matches(42, statement) {
			t.Fatalf("false lock witness accepted: %+v", o)
		}
	}
	if !(provisioningLockObservation{state: "active", waitType: "Lock", query: statement, blockers: []int32{42}}).matches(42, statement) {
		t.Fatal("real witness rejected")
	}
}

type barrierSensorDatabase struct {
	ProvisioningDatabase
	onBegin              func(uint32)
	onMembershipAcquired func(context.Context, uint32) error
	onMembershipAbsent   func(context.Context, uint32) error
	onParentAcquired     func(context.Context, uint32) error
}

func (d barrierSensorDatabase) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	tx, err := d.ProvisioningDatabase.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	if d.onBegin != nil {
		d.onBegin(tx.Conn().PgConn().PID())
	}
	return &barrierSensorTx{
		Tx:                   tx,
		onMembershipAcquired: d.onMembershipAcquired,
		onMembershipAbsent:   d.onMembershipAbsent,
		onParentAcquired:     d.onParentAcquired,
	}, nil
}

type barrierSensorTx struct {
	pgx.Tx
	onMembershipAcquired func(context.Context, uint32) error
	onMembershipAbsent   func(context.Context, uint32) error
	onParentAcquired     func(context.Context, uint32) error
}

func (tx *barrierSensorTx) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	row := tx.Tx.QueryRow(ctx, query, args...)
	if strings.Contains(query, "SELECT role FROM public.user_gateways WHERE user_id=$1 AND gateway_id=$2 FOR SHARE") {
		return &barrierRow{
			Row:                  row,
			ctx:                  ctx,
			pid:                  tx.Conn().PgConn().PID(),
			onMembershipAcquired: tx.onMembershipAcquired,
			onAbsent:             tx.onMembershipAbsent,
		}
	}
	if query == "SELECT name FROM public.gateways WHERE gateway_id=$1 FOR UPDATE" && tx.onParentAcquired != nil {
		return &barrierRow{Row: row, ctx: ctx, pid: tx.Conn().PgConn().PID(), onMembershipAcquired: tx.onParentAcquired}
	}
	return row
}

type barrierRow struct {
	pgx.Row
	ctx                  context.Context
	pid                  uint32
	onMembershipAcquired func(context.Context, uint32) error
	onAbsent             func(context.Context, uint32) error
}

func (r *barrierRow) Scan(dest ...any) error {
	err := r.Row.Scan(dest...)
	if errors.Is(err, pgx.ErrNoRows) && r.onAbsent != nil {
		if pauseErr := r.onAbsent(r.ctx, r.pid); pauseErr != nil {
			return pauseErr
		}
	}
	if err == nil && r.onMembershipAcquired != nil {
		return r.onMembershipAcquired(r.ctx, r.pid)
	}
	return err
}

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

package digitaltwin_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// emptyGatewayCleanupTimeout bounds the fixture cleanup so a stuck connection
// cannot block test teardown indefinitely.
const emptyGatewayCleanupTimeout = 10 * time.Second

// fixtureCleanupRegistrar is the subset of *testing.T that the fixture helper
// needs to register and report its cleanup. It is an interface so registration
// ordering and cleanup behavior can be exercised by a fake without a live
// database.
type fixtureCleanupRegistrar interface {
	Helper()
	Cleanup(func())
	Errorf(format string, args ...any)
}

// fixtureDB is the subset of *pgxpool.Pool used by the fixture helper.
type fixtureDB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// emptyGatewayFixture owns exactly the rows created for the empty-Gateway
// sub-scenario. Cleanup targets only these identifiers so it can never delete
// unrelated fixture data.
type emptyGatewayFixture struct {
	GatewaysTableID string
	EntityUUID      uuid.UUID
	EntityURN       string
	OwnerUserID     uuid.UUID
}

// seedEmptyGatewayFixture registers an exact-scoped cleanup BEFORE performing
// any INSERT, then seeds the Gateway, its owner membership, and its Twin
// entity. Registering cleanup first guarantees that an early t.Fatal from a
// later statement (or a partial seed) still removes whichever rows were already
// written. It returns the first insert error, if any; the registered cleanup
// remains responsible for removing the partial rows.
func seedEmptyGatewayFixture(ctx context.Context, reg fixtureCleanupRegistrar, db fixtureDB, fixture emptyGatewayFixture) error {
	reg.Helper()
	registerEmptyGatewayCleanup(reg, db, fixture)

	if _, err := db.Exec(ctx, `
		INSERT INTO public.gateways (gateway_id, name, description)
		VALUES ($1, 'Gateway Empty', 'Gateway with no sensors')`,
		fixture.GatewaysTableID,
	); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO public.user_gateways (user_id, gateway_id, role)
		VALUES ($1, $2, 'owner')`,
		fixture.OwnerUserID, fixture.GatewaysTableID,
	); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO public.twin_entities (id, entity_id, entity_type, name, gateway_id, attributes)
		VALUES ($1, $2, 'Gateway', 'Gateway Empty', $3, '{}')`,
		fixture.EntityUUID, fixture.EntityURN, fixture.GatewaysTableID,
	); err != nil {
		return err
	}
	return nil
}

// registerEmptyGatewayCleanup registers a fresh-context, exact-scoped,
// best-effort cleanup that removes the fixture rows in reverse foreign-key
// order and verifies the removal with checked count queries. It never calls
// t.Fatal, so it is safe to run from a test cleanup even when the test has
// already failed. Repeated invocation is safe because every statement is an
// idempotent scoped DELETE.
func registerEmptyGatewayCleanup(reg fixtureCleanupRegistrar, db fixtureDB, fixture emptyGatewayFixture) {
	reg.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), emptyGatewayCleanupTimeout)
		defer cancel()

		// Reverse foreign-key order; best-effort (continue past errors).
		execFixtureCleanup(cleanupCtx, reg, db, "twin_temporal_values", fixture.EntityUUID,
			"DELETE FROM public.twin_temporal_values WHERE entity_id = $1")
		execFixtureCleanup(cleanupCtx, reg, db, "twin_states", fixture.EntityUUID,
			"DELETE FROM public.twin_states WHERE entity_id = $1")
		execFixtureCleanup(cleanupCtx, reg, db, "twin_relationships", fixture.EntityUUID,
			"DELETE FROM public.twin_relationships WHERE source_entity_id = $1 OR target_entity_id = $1")
		execFixtureCleanup(cleanupCtx, reg, db, "twin_entities", fixture.EntityUUID,
			"DELETE FROM public.twin_entities WHERE id = $1")
		execFixtureCleanup(cleanupCtx, reg, db, "sensors", fixture.GatewaysTableID,
			"DELETE FROM public.sensors WHERE gateway_id = $1")
		execFixtureCleanup(cleanupCtx, reg, db, "user_gateways", fixture.GatewaysTableID,
			"DELETE FROM public.user_gateways WHERE gateway_id = $1")
		execFixtureCleanup(cleanupCtx, reg, db, "gateways", fixture.GatewaysTableID,
			"DELETE FROM public.gateways WHERE gateway_id = $1")

		// Checked verification: never claim success when the query fails.
		verifyFixtureCleanupCount(cleanupCtx, reg, db, "gateways", fixture.GatewaysTableID,
			"SELECT COUNT(*) FROM public.gateways WHERE gateway_id = $1")
		verifyFixtureCleanupCount(cleanupCtx, reg, db, "twin_entities", fixture.EntityUUID,
			"SELECT COUNT(*) FROM public.twin_entities WHERE id = $1")
		verifyFixtureCleanupCount(cleanupCtx, reg, db, "user_gateways", fixture.GatewaysTableID,
			"SELECT COUNT(*) FROM public.user_gateways WHERE gateway_id = $1")
	})
}

func execFixtureCleanup(ctx context.Context, reg fixtureCleanupRegistrar, db fixtureDB, table string, arg any, query string) {
	if _, err := db.Exec(ctx, query, arg); err != nil {
		reg.Errorf("empty-gateway fixture cleanup of %s failed: %v", table, err)
	}
}

func verifyFixtureCleanupCount(ctx context.Context, reg fixtureCleanupRegistrar, db fixtureDB, table string, arg any, query string) {
	var remaining int
	if err := db.QueryRow(ctx, query, arg).Scan(&remaining); err != nil {
		reg.Errorf("empty-gateway fixture cleanup verification for %s failed: %v", table, err)
		return
	}
	if remaining != 0 {
		reg.Errorf("empty-gateway fixture cleanup incomplete: %d %s row(s) remain", remaining, table)
	}
}

// ---------------------------------------------------------------------------
// Regression: cleanup registration must precede the first fixture INSERT, and
// a partial seed must still be fully removed by the registered cleanup.
// ---------------------------------------------------------------------------

type fakeCleanupRegistrar struct {
	order        *[]string
	cleanupFuncs []func()
	errors       []string
}

func (f *fakeCleanupRegistrar) Helper() {}

func (f *fakeCleanupRegistrar) Cleanup(fn func()) {
	*f.order = append(*f.order, "register")
	f.cleanupFuncs = append(f.cleanupFuncs, fn)
}

func (f *fakeCleanupRegistrar) Errorf(format string, args ...any) {
	f.errors = append(f.errors, fmt.Sprintf(format, args...))
}

type fakeFixtureRow struct{ value int }

func (r fakeFixtureRow) Scan(dest ...any) error {
	if len(dest) == 1 {
		if p, ok := dest[0].(*int); ok {
			*p = r.value
		}
	}
	return nil
}

type fakeFixtureDB struct {
	order      *[]string
	execs      []string
	failExecAt int // 1-based index of the Exec call to fail; 0 means never
	execCount  int
}

func (d *fakeFixtureDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	d.execCount++
	trimmed := strings.Join(strings.Fields(sql), " ")
	*d.order = append(*d.order, "exec:"+trimmed)
	d.execs = append(d.execs, trimmed)
	if d.failExecAt != 0 && d.execCount == d.failExecAt {
		return pgconn.CommandTag{}, fmt.Errorf("injected fixture insert failure at exec #%d", d.execCount)
	}
	return pgconn.CommandTag{}, nil
}

func (d *fakeFixtureDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	*d.order = append(*d.order, "query:"+strings.Join(strings.Fields(sql), " "))
	return fakeFixtureRow{value: 0}
}

func (d *fakeFixtureDB) sawExecPrefix(prefix string) bool {
	for _, e := range d.execs {
		if strings.HasPrefix(e, prefix) {
			return true
		}
	}
	return false
}

func TestEmptyGatewayFixtureCleanupRegisteredBeforeInsert(t *testing.T) {
	var order []string
	reg := &fakeCleanupRegistrar{order: &order}
	// Fail on the second INSERT to model a partial seed after the first write.
	db := &fakeFixtureDB{order: &order, failExecAt: 2}

	fixture := emptyGatewayFixture{
		GatewaysTableID: "gw_empty_reg",
		EntityUUID:      uuid.New(),
		EntityURN:       "urn:ngsi-ld:Gateway:gw_empty_reg",
		OwnerUserID:     uuid.New(),
	}

	if err := seedEmptyGatewayFixture(context.Background(), reg, db, fixture); err == nil {
		t.Fatalf("expected injected insert failure to be returned")
	}

	// Registration must happen before the first INSERT.
	if len(order) == 0 || order[0] != "register" {
		t.Fatalf("expected cleanup registration before any insert, got order %v", order)
	}
	if len(reg.cleanupFuncs) != 1 {
		t.Fatalf("expected exactly one registered cleanup, got %d", len(reg.cleanupFuncs))
	}
	// First executed statement must be an INSERT issued after registration.
	if !db.sawExecPrefix("INSERT INTO public.gateways") {
		t.Fatalf("expected the first exec to be the gateway insert, got %v", db.execs)
	}
	firstExecIdx := -1
	for i, ev := range order {
		if strings.HasPrefix(ev, "exec:") {
			firstExecIdx = i
			break
		}
	}
	if firstExecIdx <= 0 {
		t.Fatalf("expected an exec event after registration, got order %v", order)
	}

	// Simulate early test exit: run the registered cleanup after the failed seed.
	reg.cleanupFuncs[0]()

	for _, want := range []string{
		"DELETE FROM public.twin_temporal_values",
		"DELETE FROM public.twin_states",
		"DELETE FROM public.twin_relationships",
		"DELETE FROM public.twin_entities",
		"DELETE FROM public.sensors",
		"DELETE FROM public.user_gateways",
		"DELETE FROM public.gateways",
	} {
		if !db.sawExecPrefix(want) {
			t.Errorf("cleanup did not issue %q; execs=%v", want, db.execs)
		}
	}
	if len(reg.errors) != 0 {
		t.Errorf("unexpected cleanup errors: %v", reg.errors)
	}

	// Repeated cleanup must stay safe (idempotent scoped deletes).
	reg.cleanupFuncs[0]()
	if len(reg.errors) != 0 {
		t.Errorf("unexpected cleanup errors on repeated cleanup: %v", reg.errors)
	}
}

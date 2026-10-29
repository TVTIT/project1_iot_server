package gateway

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type fakeRows struct {
	pgx.Rows
	next    bool
	scanErr error
	endErr  error
	closed  bool
}

func (r *fakeRows) Next() bool        { next := r.next; r.next = false; return next }
func (r *fakeRows) Scan(...any) error { return r.scanErr }
func (r *fakeRows) Err() error        { return r.endErr }
func (r *fakeRows) Close()            { r.closed = true }

type fakeRow struct{ err error }

func (r fakeRow) Scan(...any) error { return r.err }

type fakeDatabase struct {
	query  string
	args   []any
	ctx    context.Context
	rows   *fakeRows
	err    error
	rowErr error
	calls  int
}

func (d *fakeDatabase) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	d.ctx, d.query, d.args = ctx, query, args
	d.calls++
	return d.rows, d.err
}
func (d *fakeDatabase) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	d.ctx, d.query, d.args = ctx, query, args
	d.calls++
	return fakeRow{d.rowErr}
}

func TestRepositoryRejectsNilDatabase(t *testing.T) {
	var typedNil *fakeDatabase
	for _, db := range []Database{nil, typedNil} {
		if _, err := NewPostgresRepository(db); err == nil {
			t.Fatal("accepted nil database")
		}
	}
}

func TestRepositoryErrorPaths(t *testing.T) {
	failure := errors.New("fixture failure")
	for _, operation := range []string{"gateways", "sensors"} {
		for _, kind := range []string{"query", "scan", "iteration", "empty"} {
			t.Run(operation+"/"+kind, func(t *testing.T) {
				rows := &fakeRows{}
				db := &fakeDatabase{rows: rows}
				switch kind {
				case "query":
					db.err = failure
				case "scan":
					rows.next, rows.scanErr = true, failure
				case "iteration":
					rows.endErr = failure
				}
				repo, err := NewPostgresRepository(db)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				id := uuid.New()
				if operation == "gateways" {
					items, readErr := repo.ListGatewaysForUser(ctx, id)
					err = readErr
					if kind == "empty" && (items == nil || len(items) != 0 || err != nil) {
						t.Fatal("empty gateway list must be nonnil")
					}
				} else {
					_, err = repo.ListSensorsForUserAndGateway(ctx, id, "fixture")
				}
				if kind != "empty" && !errors.Is(err, failure) {
					t.Fatalf("error=%v", err)
				}
				if kind == "empty" && operation == "sensors" && !errors.Is(err, ErrNotFound) {
					t.Fatalf("error=%v", err)
				}
				if kind != "query" && !rows.closed {
					t.Fatal("rows not closed")
				}
				wantArgs := []any{id}
				if operation == "sensors" {
					wantArgs = append(wantArgs, "fixture")
				}
				if db.ctx != ctx || !reflect.DeepEqual(db.args, wantArgs) || db.calls != 1 {
					t.Fatal("context/parameters/query count mismatch")
				}
			})
		}
	}
}

func TestRoleErrors(t *testing.T) {
	failure := errors.New("fixture failure")
	for _, cause := range []error{pgx.ErrNoRows, failure} {
		db := &fakeDatabase{rowErr: cause}
		repo, err := NewPostgresRepository(db)
		if err != nil {
			t.Fatal(err)
		}
		id := uuid.New()
		_, err = repo.GetGatewayRole(context.Background(), id, "fixture")
		want := cause
		if cause == pgx.ErrNoRows {
			want = ErrNotFound
		}
		if !errors.Is(err, want) || !reflect.DeepEqual(db.args, []any{id, "fixture"}) {
			t.Fatal("role error/parameters mismatch")
		}
	}
}

type fakeAdminDatabase struct {
	adminErr   error
	isAdmin    bool
	rows       pgx.Rows
	queryErr   error
	lastQuery  string
	lastArgs   []any
	queryCalls int
}

func (d *fakeAdminDatabase) QueryRow(_ context.Context, query string, args ...any) pgx.Row {
	d.lastQuery = query
	d.lastArgs = args
	return fakeAdminRow{admin: d.isAdmin, err: d.adminErr}
}

func (d *fakeAdminDatabase) Query(_ context.Context, query string, args ...any) (pgx.Rows, error) {
	d.queryCalls++
	d.lastQuery = query
	d.lastArgs = args
	return d.rows, d.queryErr
}

type fakeAdminRow struct {
	admin bool
	err   error
}

func (r fakeAdminRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) > 0 {
		if b, ok := dest[0].(*bool); ok {
			*b = r.admin
		}
	}
	return nil
}

func TestPostgresAdminGatewayRepository(t *testing.T) {
	ctx := context.Background()
	actor := uuid.New()

	t.Run("nil actor", func(t *testing.T) {
		repo, _ := NewPostgresAdminGatewayRepository(&fakeAdminDatabase{})
		_, err := repo.ListAdminGateways(ctx, uuid.Nil, AdminGatewayInventoryQuery{Limit: 10})
		if !errors.Is(err, ErrInvalidUser) {
			t.Fatalf("expected ErrInvalidUser, got %v", err)
		}
	})

	t.Run("admin lookup failure", func(t *testing.T) {
		db := &fakeAdminDatabase{adminErr: errors.New("db down")}
		repo, _ := NewPostgresAdminGatewayRepository(db)
		_, err := repo.ListAdminGateways(ctx, actor, AdminGatewayInventoryQuery{Limit: 10})
		if !errors.Is(err, ErrAdminLookupFailed) {
			t.Fatalf("expected ErrAdminLookupFailed, got %v", err)
		}
	})

	t.Run("non-admin forbidden", func(t *testing.T) {
		db := &fakeAdminDatabase{isAdmin: false}
		repo, _ := NewPostgresAdminGatewayRepository(db)
		_, err := repo.ListAdminGateways(ctx, actor, AdminGatewayInventoryQuery{Limit: 10})
		if !errors.Is(err, ErrAdminForbidden) {
			t.Fatalf("expected ErrAdminForbidden, got %v", err)
		}
	})

	t.Run("query failure", func(t *testing.T) {
		db := &fakeAdminDatabase{isAdmin: true, queryErr: errors.New("query failed")}
		repo, _ := NewPostgresAdminGatewayRepository(db)
		_, err := repo.ListAdminGateways(ctx, actor, AdminGatewayInventoryQuery{Limit: 10})
		if err == nil || !errors.Is(err, db.queryErr) {
			t.Fatalf("expected query error, got %v", err)
		}
	})

	t.Run("rows scan error", func(t *testing.T) {
		rows := &fakeRows{next: true, scanErr: errors.New("scan failed")}
		db := &fakeAdminDatabase{isAdmin: true, rows: rows}
		repo, _ := NewPostgresAdminGatewayRepository(db)
		_, err := repo.ListAdminGateways(ctx, actor, AdminGatewayInventoryQuery{Limit: 10})
		if err == nil {
			t.Fatal("expected scan error, got nil")
		}
		if !rows.closed {
			t.Fatal("rows not closed on scan error")
		}
	})

	t.Run("rows iterate error", func(t *testing.T) {
		rows := &fakeRows{next: false, endErr: errors.New("iteration failed")}
		db := &fakeAdminDatabase{isAdmin: true, rows: rows}
		repo, _ := NewPostgresAdminGatewayRepository(db)
		_, err := repo.ListAdminGateways(ctx, actor, AdminGatewayInventoryQuery{Limit: 10})
		if err == nil {
			t.Fatal("expected iteration error, got nil")
		}
	})

	t.Run("nil database rejected", func(t *testing.T) {
		if _, err := NewPostgresAdminGatewayRepository(nil); err == nil {
			t.Fatal("expected error on nil database")
		}
	})

	t.Run("first page with cursor", func(t *testing.T) {
		rows := &fakeGatewayRows{
			items: []AdminGateway{
				{GatewayID: "gw-1", Name: "Gateway 1"},
				{GatewayID: "gw-2", Name: "Gateway 2"},
				{GatewayID: "gw-3", Name: "Gateway 3"},
			},
		}
		db := &fakeAdminDatabase{isAdmin: true, rows: rows}
		repo, _ := NewPostgresAdminGatewayRepository(db)
		page, err := repo.ListAdminGateways(ctx, actor, AdminGatewayInventoryQuery{Limit: 2})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(page.Items) != 2 || page.NextCursor == nil || *page.NextCursor != "gw-2" {
			t.Fatalf("unexpected page: %+v", page)
		}
		if db.lastQuery != listAdminGatewaysFirstPageQuery || len(db.lastArgs) != 1 || db.lastArgs[0] != 3 {
			t.Fatalf("unexpected query %s or args %v", db.lastQuery, db.lastArgs)
		}
	})

	t.Run("after page without cursor", func(t *testing.T) {
		rows := &fakeGatewayRows{
			items: []AdminGateway{
				{GatewayID: "gw-3", Name: "Gateway 3"},
			},
		}
		db := &fakeAdminDatabase{isAdmin: true, rows: rows}
		repo, _ := NewPostgresAdminGatewayRepository(db)
		page, err := repo.ListAdminGateways(ctx, actor, AdminGatewayInventoryQuery{Limit: 2, After: "gw-2"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(page.Items) != 1 || page.NextCursor != nil {
			t.Fatalf("unexpected page: %+v", page)
		}
		if db.lastQuery != listAdminGatewaysAfterQuery || len(db.lastArgs) != 2 || db.lastArgs[0] != "gw-2" || db.lastArgs[1] != 3 {
			t.Fatalf("unexpected query %s or args %v", db.lastQuery, db.lastArgs)
		}
	})
}

type fakeGatewayRows struct {
	pgx.Rows
	items  []AdminGateway
	idx    int
	closed bool
}

func (r *fakeGatewayRows) Next() bool {
	if r.idx < len(r.items) {
		r.idx++
		return true
	}
	return false
}

func (r *fakeGatewayRows) Scan(dest ...any) error {
	item := r.items[r.idx-1]
	if len(dest) >= 4 {
		if id, ok := dest[0].(*string); ok {
			*id = item.GatewayID
		}
		if name, ok := dest[1].(*string); ok {
			*name = item.Name
		}
		if desc, ok := dest[2].(**string); ok {
			*desc = item.Description
		}
	}
	return nil
}

func (r *fakeGatewayRows) Err() error { return nil }
func (r *fakeGatewayRows) Close()     { r.closed = true }

type fakeRoleDatabase struct {
	query string
	args  []any
	role  Role
	err   error
}

func (d *fakeRoleDatabase) QueryRow(_ context.Context, query string, args ...any) pgx.Row {
	d.query = query
	d.args = args
	return fakeRoleRow{role: d.role, err: d.err}
}

func (d *fakeRoleDatabase) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return nil, errors.New("unexpected Query call")
}

type fakeRoleRow struct {
	role Role
	err  error
}

func (r fakeRoleRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) > 0 {
		if ptr, ok := dest[0].(*Role); ok {
			*ptr = r.role
		}
	}
	return nil
}

func TestAuthorizeSubscriptionTarget(t *testing.T) {
	ctx := context.Background()
	user := uuid.New()

	t.Run("input validation", func(t *testing.T) {
		db := &fakeRoleDatabase{}
		repo, _ := NewPostgresRepository(db)

		// nil user
		if _, err := repo.AuthorizeSubscriptionTarget(ctx, uuid.Nil, "gw-1", ""); !errors.Is(err, ErrInvalidUser) {
			t.Fatalf("nil user: err = %v, want ErrInvalidUser", err)
		}

		// invalid gateway ID
		if _, err := repo.AuthorizeSubscriptionTarget(ctx, user, "bad gw ID!", ""); !errors.Is(err, ErrInvalidGatewayID) {
			t.Fatalf("bad gateway ID: err = %v, want ErrInvalidGatewayID", err)
		}
		if _, err := repo.AuthorizeSubscriptionTarget(ctx, user, "backend_service", ""); !errors.Is(err, ErrInvalidGatewayID) {
			t.Fatalf("reserved gateway ID: err = %v, want ErrInvalidGatewayID", err)
		}

		// invalid sensor ID
		if _, err := repo.AuthorizeSubscriptionTarget(ctx, user, "gw-1", "bad sensor ID!"); !errors.Is(err, ErrInvalidSensorID) {
			t.Fatalf("bad sensor ID: err = %v, want ErrInvalidSensorID", err)
		}
	})

	t.Run("gateway target queries and argument correctness", func(t *testing.T) {
		roles := []Role{RoleOwner, RoleOperator, RoleViewer}
		for _, wantRole := range roles {
			db := &fakeRoleDatabase{role: wantRole}
			repo, _ := NewPostgresRepository(db)

			role, err := repo.AuthorizeSubscriptionTarget(ctx, user, "gw-valid-01", "")
			if err != nil {
				t.Fatalf("unexpected error for %s: %v", wantRole, err)
			}
			if role != wantRole {
				t.Fatalf("role = %v, want %v", role, wantRole)
			}
			if !reflect.DeepEqual(db.args, []any{user, "gw-valid-01"}) {
				t.Fatalf("args = %v, want [%v, gw-valid-01]", db.args, user)
			}
			if !strings.Contains(db.query, "FROM public.user_gateways") ||
				!strings.Contains(db.query, "JOIN public.gateways") ||
				strings.Contains(db.query, "sensors") {
				t.Fatalf("query for gateway target must join user_gateways and gateways, but not sensors: %s", db.query)
			}
		}
	})

	t.Run("sensor target queries and argument correctness", func(t *testing.T) {
		roles := []Role{RoleOwner, RoleOperator, RoleViewer}
		for _, wantRole := range roles {
			db := &fakeRoleDatabase{role: wantRole}
			repo, _ := NewPostgresRepository(db)

			role, err := repo.AuthorizeSubscriptionTarget(ctx, user, "gw-valid-01", "sensor-01")
			if err != nil {
				t.Fatalf("unexpected error for %s: %v", wantRole, err)
			}
			if role != wantRole {
				t.Fatalf("role = %v, want %v", role, wantRole)
			}
			if !reflect.DeepEqual(db.args, []any{user, "gw-valid-01", "sensor-01"}) {
				t.Fatalf("args = %v, want [%v, gw-valid-01, sensor-01]", db.args, user)
			}
			if !strings.Contains(db.query, "FROM public.user_gateways") ||
				!strings.Contains(db.query, "JOIN public.gateways") ||
				!strings.Contains(db.query, "JOIN public.sensors") {
				t.Fatalf("query for sensor target must join user_gateways, gateways, and sensors: %s", db.query)
			}
		}
	})

	t.Run("not found (no rows) conflation for nonmember or absent entity", func(t *testing.T) {
		db := &fakeRoleDatabase{err: pgx.ErrNoRows}
		repo, _ := NewPostgresRepository(db)

		// Gateway target
		if _, err := repo.AuthorizeSubscriptionTarget(ctx, user, "gw-missing", ""); !errors.Is(err, ErrNotFound) {
			t.Fatalf("gateway not found: err = %v, want ErrNotFound", err)
		}

		// Sensor target
		if _, err := repo.AuthorizeSubscriptionTarget(ctx, user, "gw-mine", "sen-missing"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("sensor not found: err = %v, want ErrNotFound", err)
		}
	})

	t.Run("database failure returns wrapped internal error", func(t *testing.T) {
		dbFailure := errors.New("pgx: connection reset by peer")
		db := &fakeRoleDatabase{err: dbFailure}
		repo, _ := NewPostgresRepository(db)

		_, err := repo.AuthorizeSubscriptionTarget(ctx, user, "gw-valid", "")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if errors.Is(err, ErrNotFound) {
			t.Fatal("DB failure must not be conflated with ErrNotFound")
		}
		if !errors.Is(err, dbFailure) {
			t.Fatalf("err = %v, want to unwrap dbFailure", err)
		}
	})
}

type batchAuthRow struct {
	user   uuid.UUID
	gw     string
	sensor string
	role   Role
}

type batchAuthRows struct {
	pgx.Rows
	items   []batchAuthRow
	index   int
	scanErr error
	endErr  error
}

func (r *batchAuthRows) Next() bool { return r.index < len(r.items) }
func (r *batchAuthRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	item := r.items[r.index]
	r.index++
	*dest[0].(*uuid.UUID) = item.user
	*dest[1].(*string) = item.gw
	*dest[2].(*string) = item.sensor
	*dest[3].(*Role) = item.role
	return nil
}
func (r *batchAuthRows) Err() error { return r.endErr }
func (r *batchAuthRows) Close()     {}

type batchAuthDB struct {
	rows  pgx.Rows
	err   error
	query string
	args  []any
	calls int
}

func (d *batchAuthDB) Query(_ context.Context, query string, args ...any) (pgx.Rows, error) {
	d.calls++
	d.query, d.args = query, args
	return d.rows, d.err
}
func (d *batchAuthDB) QueryRow(context.Context, string, ...any) pgx.Row { return fakeRow{} }

func TestAuthorizeSubscriptionTargetsBatch(t *testing.T) {
	userA, userB := uuid.New(), uuid.New()
	keys := []SubscriptionTargetKey{
		{UserID: userA, GatewayID: "gateway_A", SensorID: "shared_sensor"},
		{UserID: userB, GatewayID: "gateway_B", SensorID: "shared_sensor"},
		{UserID: userA, GatewayID: "gateway_A", SensorID: "missing_sensor"},
	}
	db := &batchAuthDB{rows: &batchAuthRows{items: []batchAuthRow{
		{user: userA, gw: "gateway_A", sensor: "shared_sensor", role: RoleViewer},
		{user: userB, gw: "gateway_B", sensor: "shared_sensor", role: RoleOwner},
	}}}
	repo, _ := NewPostgresRepository(db)
	got, err := repo.(BatchSubscriptionTargetRepository).AuthorizeSubscriptionTargets(context.Background(), keys)
	if err != nil || len(got) != 3 || !got[0].Authorized || !got[1].Authorized || got[0].Key != keys[0] || got[1].Key != keys[1] || got[2].Authorized {
		t.Fatalf("results=%#v err=%v", got, err)
	}
	if !strings.Contains(db.query, "requested.user_id") || !strings.Contains(db.query, "requested.gateway_id") || !strings.Contains(db.query, "requested.sensor_id") || !strings.Contains(db.query, "public.user_gateways") || !strings.Contains(db.query, "public.sensors") {
		t.Fatalf("batch query does not preserve full composite identity/membership/existence: %s", db.query)
	}

	beforeCalls := db.calls
	overbound := make([]SubscriptionTargetKey, MaxSubscriptionTargetBatchSize+1)
	for i := range overbound {
		overbound[i] = keys[0]
	}
	if _, err := repo.(BatchSubscriptionTargetRepository).AuthorizeSubscriptionTargets(context.Background(), overbound); err == nil || db.calls != beforeCalls {
		t.Fatal("overbound batch must be rejected before querying")
	}
	invalid := append([]SubscriptionTargetKey(nil), keys[:1]...)
	invalid[0].GatewayID = "bad gateway"
	if _, err := repo.(BatchSubscriptionTargetRepository).AuthorizeSubscriptionTargets(context.Background(), invalid); !errors.Is(err, ErrInvalidGatewayID) || db.calls != beforeCalls {
		t.Fatal("invalid key must be rejected before querying")
	}

	db.err = errors.New("db unavailable")
	if got, err := repo.(BatchSubscriptionTargetRepository).AuthorizeSubscriptionTargets(context.Background(), keys); err == nil || got != nil {
		t.Fatal("query error must return no partial results")
	}
	db.err = nil
	db.rows = &batchAuthRows{items: []batchAuthRow{{user: userA, gw: "gateway_A", sensor: "shared_sensor", role: RoleViewer}}, scanErr: errors.New("scan failure")}
	if got, err := repo.(BatchSubscriptionTargetRepository).AuthorizeSubscriptionTargets(context.Background(), keys); err == nil || got != nil {
		t.Fatal("row scan error must return no partial results")
	}
}

package gateway

import (
	"context"
	"errors"
	"reflect"
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

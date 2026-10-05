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

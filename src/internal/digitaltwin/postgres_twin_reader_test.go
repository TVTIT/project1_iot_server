package digitaltwin

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"
)

// fakeTwinRows implements pgx.Rows for testing row scanning and lifecycle.
type fakeTwinRows struct {
	pgx.Rows
	records []func(dest ...any) error
	idx     int
	scanErr error
	endErr  error
	closed  bool
}

func (r *fakeTwinRows) Next() bool {
	if r.idx < len(r.records) {
		r.idx++
		return true
	}
	return false
}

func (r *fakeTwinRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.idx-1 >= 0 && r.idx-1 < len(r.records) {
		return r.records[r.idx-1](dest...)
	}
	return nil
}

func (r *fakeTwinRows) Err() error {
	return r.endErr
}

func (r *fakeTwinRows) Close() {
	r.closed = true
}

// fakeTwinDatabase implements Database interface for testing PostgresTwinReader.
type fakeTwinDatabase struct {
	lastCtx     context.Context
	lastQuery   string
	lastArgs    []any
	queries     []string
	argsHistory [][]any
	rows        *fakeTwinRows
	row         pgx.Row
	queryRowFn  func(ctx context.Context, query string, args ...any) pgx.Row
	queryFn     func(ctx context.Context, query string, args ...any) (pgx.Rows, error)
	queryErr    error
	calls       int
}

func (d *fakeTwinDatabase) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	d.lastCtx, d.lastQuery, d.lastArgs = ctx, query, args
	d.queries = append(d.queries, query)
	d.argsHistory = append(d.argsHistory, args)
	d.calls++
	if d.queryFn != nil {
		return d.queryFn(ctx, query, args...)
	}
	if d.queryErr != nil {
		return nil, d.queryErr
	}
	if d.rows != nil {
		return d.rows, nil
	}
	return &fakeTwinRows{}, nil
}

func (d *fakeTwinDatabase) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	d.lastCtx, d.lastQuery, d.lastArgs = ctx, query, args
	d.queries = append(d.queries, query)
	d.argsHistory = append(d.argsHistory, args)
	d.calls++
	if d.queryRowFn != nil {
		return d.queryRowFn(ctx, query, args...)
	}
	if d.row != nil {
		return d.row
	}
	return fakeRow{}
}

func TestNewPostgresTwinReaderRejectsNilDatabase(t *testing.T) {
	var typedNil *fakeTwinDatabase
	for _, db := range []Database{nil, typedNil} {
		if _, err := NewPostgresTwinReader(db); err == nil {
			t.Fatal("expected error on nil database")
		}
	}

	validDB := &fakeTwinDatabase{}
	reader, err := NewPostgresTwinReader(validDB)
	if err != nil {
		t.Fatalf("unexpected error on valid database: %v", err)
	}
	if reader == nil {
		t.Fatal("expected non-nil reader")
	}
}

func TestListEntitiesRejectsNilActor(t *testing.T) {
	db := &fakeTwinDatabase{}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.ListEntities(context.Background(), uuid.Nil, TwinListQuery{Limit: 50})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for nil actor, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls, got: %d", db.calls)
	}
}

func TestListEntitiesValidatesInput(t *testing.T) {
	db := &fakeTwinDatabase{}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()
	ctx := context.Background()

	// 1. Limit exceeding ContractMaxLimit (200)
	_, err = reader.ListEntities(ctx, actor, TwinListQuery{Limit: 201})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for limit > 200, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls, got: %d", db.calls)
	}

	// 2. Invalid GatewayID
	invalidGW := "invalid@gateway!"
	_, err = reader.ListEntities(ctx, actor, TwinListQuery{GatewayID: &invalidGW, Limit: 50})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for invalid gateway_id, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls, got: %d", db.calls)
	}

	// 3. Invalid EntityType
	invalidType := EntityType("Device")
	_, err = reader.ListEntities(ctx, actor, TwinListQuery{EntityType: &invalidType, Limit: 50})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for invalid entity_type, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls, got: %d", db.calls)
	}
}

func TestListEntitiesCursorValidationAndTampering(t *testing.T) {
	db := &fakeTwinDatabase{}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()
	ctx := context.Background()

	gw1 := "gw_01"
	gw2 := "gw_02"
	typeSensor := EntityTypeSensor
	typeGateway := EntityTypeGateway

	// 1. Malformed cursor string
	malformed := "v1.invalidbase64!!!"
	_, err = reader.ListEntities(ctx, actor, TwinListQuery{After: &malformed, Limit: 50})
	if !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor for malformed cursor, got: %v", err)
	}

	// 2. Cursor created with gw1, but query asks for gw2 (gateway filter tampering)
	c1, err := EncodeListCursor("urn:ngsi-ld:Sensor:gw_01:s1", &gw1, &typeSensor)
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.ListEntities(ctx, actor, TwinListQuery{
		GatewayID:  &gw2,
		EntityType: &typeSensor,
		After:      &c1,
		Limit:      50,
	})
	if !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor for mismatched gateway_id, got: %v", err)
	}

	// 3. Cursor created with EntityTypeSensor, but query asks for EntityTypeGateway (type filter tampering)
	_, err = reader.ListEntities(ctx, actor, TwinListQuery{
		GatewayID:  &gw1,
		EntityType: &typeGateway,
		After:      &c1,
		Limit:      50,
	})
	if !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor for mismatched entity_type, got: %v", err)
	}

	// 4. Cursor created without filters, but query specifies filters
	cNoFilter, err := EncodeListCursor("urn:ngsi-ld:Gateway:gw_01", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.ListEntities(ctx, actor, TwinListQuery{
		GatewayID: &gw1,
		After:     &cNoFilter,
		Limit:     50,
	})
	if !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor when query specifies filter not in cursor, got: %v", err)
	}

	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls on invalid cursors, got: %d", db.calls)
	}
}

func TestListEntitiesEmptyResult(t *testing.T) {
	fakeRows := &fakeTwinRows{records: nil}
	db := &fakeTwinDatabase{rows: fakeRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()

	result, err := reader.ListEntities(context.Background(), actor, TwinListQuery{Limit: 50})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Items == nil {
		t.Fatal("expected non-nil Items slice")
	}
	if len(result.Items) != 0 {
		t.Fatalf("expected 0 items, got: %d", len(result.Items))
	}
	if result.NextCursor != nil {
		t.Fatalf("expected nil NextCursor, got: %v", *result.NextCursor)
	}
	if !fakeRows.closed {
		t.Fatal("expected rows to be closed")
	}
}

func TestListEntitiesExactLimit(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	name1 := "GW 1"
	name2 := "GW 2"

	fakeRows := &fakeTwinRows{
		records: []func(dest ...any) error{
			func(dest ...any) error {
				*dest[0].(*string) = "urn:ngsi-ld:Gateway:gw_01"
				*dest[1].(*string) = "Gateway"
				*dest[2].(**string) = &name1
				*dest[3].(*string) = "gw_01"
				*dest[4].(*time.Time) = now
				return nil
			},
			func(dest ...any) error {
				*dest[0].(*string) = "urn:ngsi-ld:Gateway:gw_02"
				*dest[1].(*string) = "Gateway"
				*dest[2].(**string) = &name2
				*dest[3].(*string) = "gw_02"
				*dest[4].(*time.Time) = now
				return nil
			},
		},
	}
	db := &fakeTwinDatabase{rows: fakeRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	result, err := reader.ListEntities(context.Background(), uuid.New(), TwinListQuery{Limit: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Items) != 2 {
		t.Fatalf("expected 2 items, got: %d", len(result.Items))
	}
	if result.NextCursor != nil {
		t.Fatalf("expected nil NextCursor on exact limit, got: %s", *result.NextCursor)
	}
	if result.Items[0].ID != "urn:ngsi-ld:Gateway:gw_01" || *result.Items[0].Name != name1 {
		t.Errorf("unexpected item 0: %+v", result.Items[0])
	}
	if result.Items[1].ID != "urn:ngsi-ld:Gateway:gw_02" || *result.Items[1].Name != name2 {
		t.Errorf("unexpected item 1: %+v", result.Items[1])
	}
	if !fakeRows.closed {
		t.Fatal("expected rows to be closed")
	}
}

func TestListEntitiesLimitPlusOneTruncationAndCursor(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	gwID := "gw_01"
	sensorType := EntityTypeSensor

	fakeRows := &fakeTwinRows{
		records: []func(dest ...any) error{
			func(dest ...any) error {
				*dest[0].(*string) = "urn:ngsi-ld:Sensor:gw_01:s1"
				*dest[1].(*string) = "Sensor"
				*dest[2].(**string) = nil
				*dest[3].(*string) = gwID
				*dest[4].(*time.Time) = now
				return nil
			},
			func(dest ...any) error {
				*dest[0].(*string) = "urn:ngsi-ld:Sensor:gw_01:s2"
				*dest[1].(*string) = "Sensor"
				*dest[2].(**string) = nil
				*dest[3].(*string) = gwID
				*dest[4].(*time.Time) = now
				return nil
			},
			func(dest ...any) error {
				// Third row (limit + 1)
				*dest[0].(*string) = "urn:ngsi-ld:Sensor:gw_01:s3"
				*dest[1].(*string) = "Sensor"
				*dest[2].(**string) = nil
				*dest[3].(*string) = gwID
				*dest[4].(*time.Time) = now
				return nil
			},
		},
	}
	db := &fakeTwinDatabase{rows: fakeRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	result, err := reader.ListEntities(context.Background(), uuid.New(), TwinListQuery{
		GatewayID:  &gwID,
		EntityType: &sensorType,
		Limit:      2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Items) != 2 {
		t.Fatalf("expected truncated 2 items, got: %d", len(result.Items))
	}
	if result.NextCursor == nil {
		t.Fatal("expected non-nil NextCursor on limit + 1 rows")
	}

	// Validate next_cursor content
	decodedCursor, err := DecodeListCursor(*result.NextCursor)
	if err != nil {
		t.Fatalf("failed to decode generated NextCursor: %v", err)
	}
	if decodedCursor.EntityID != "urn:ngsi-ld:Sensor:gw_01:s2" {
		t.Errorf("expected cursor k = s2, got: %s", decodedCursor.EntityID)
	}
	if decodedCursor.GatewayID == nil || *decodedCursor.GatewayID != gwID {
		t.Errorf("expected cursor g = %s, got: %v", gwID, decodedCursor.GatewayID)
	}
	if decodedCursor.EntityType == nil || *decodedCursor.EntityType != sensorType {
		t.Errorf("expected cursor t = %s, got: %v", sensorType, decodedCursor.EntityType)
	}

	// Nullable name scanned as nil
	if result.Items[0].Name != nil {
		t.Errorf("expected nil Name for item 0, got: %v", result.Items[0].Name)
	}
	if !fakeRows.closed {
		t.Fatal("expected rows to be closed")
	}
}

func TestListEntitiesSQLScopingAndCollation(t *testing.T) {
	gwID := "gw_01"
	sensorType := EntityTypeSensor
	cursorStr, err := EncodeListCursor("urn:ngsi-ld:Sensor:gw_01:s1", &gwID, &sensorType)
	if err != nil {
		t.Fatal(err)
	}

	fakeRows := &fakeTwinRows{records: nil}
	db := &fakeTwinDatabase{rows: fakeRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")

	_, err = reader.ListEntities(context.Background(), actor, TwinListQuery{
		GatewayID:  &gwID,
		EntityType: &sensorType,
		After:      &cursorStr,
		Limit:      10,
	})
	if err != nil {
		t.Fatal(err)
	}

	sql := db.lastQuery

	// 1. SELECT columns
	if !strings.Contains(sql, "SELECT e.entity_id, e.entity_type, e.name, e.gateway_id, e.updated_at") {
		t.Errorf("SQL missing required SELECT columns: %s", sql)
	}

	// 2. FROM and JOIN user_gateways scoped by actor $1
	if !strings.Contains(sql, "FROM public.twin_entities AS e") {
		t.Errorf("SQL missing 'FROM public.twin_entities AS e': %s", sql)
	}
	if !strings.Contains(sql, "JOIN public.user_gateways AS ug ON ug.gateway_id = e.gateway_id AND ug.user_id = $1") {
		t.Errorf("SQL missing scoped user_gateways JOIN: %s", sql)
	}

	// 3. Filters
	if !strings.Contains(sql, "e.entity_type IN ('Gateway', 'Sensor')") {
		t.Errorf("SQL missing server-owned predicate e.entity_type IN ('Gateway', 'Sensor'): %s", sql)
	}
	if !strings.Contains(sql, "e.gateway_id = $2") {
		t.Errorf("SQL missing e.gateway_id filter $2: %s", sql)
	}
	if !strings.Contains(sql, "e.entity_type = $3") {
		t.Errorf("SQL missing e.entity_type filter $3: %s", sql)
	}
	if !strings.Contains(sql, `e.entity_id COLLATE "C" > $4`) {
		t.Errorf("SQL missing e.entity_id COLLATE \"C\" > $4: %s", sql)
	}

	// 4. ORDER BY and LIMIT
	if !strings.Contains(sql, `ORDER BY e.entity_id COLLATE "C" ASC`) {
		t.Errorf("SQL missing required ORDER BY collation \"C\" ASC: %s", sql)
	}
	if !strings.Contains(sql, "LIMIT $5") {
		t.Errorf("SQL missing LIMIT $5: %s", sql)
	}

	// 5. Arguments verification
	if len(db.lastArgs) != 5 {
		t.Fatalf("expected 5 arguments, got %d: %v", len(db.lastArgs), db.lastArgs)
	}
	if db.lastArgs[0] != actor {
		t.Errorf("arg 0 (actor) = %v, want %v", db.lastArgs[0], actor)
	}
	if db.lastArgs[1] != gwID {
		t.Errorf("arg 1 (gateway_id) = %v, want %v", db.lastArgs[1], gwID)
	}
	if db.lastArgs[2] != string(sensorType) {
		t.Errorf("arg 2 (entity_type) = %v, want %v", db.lastArgs[2], sensorType)
	}
	if db.lastArgs[3] != "urn:ngsi-ld:Sensor:gw_01:s1" {
		t.Errorf("arg 3 (last_entity_id) = %v, want urn:ngsi-ld:Sensor:gw_01:s1", db.lastArgs[3])
	}
	if db.lastArgs[4] != 11 { // limit 10 + 1
		t.Errorf("arg 4 (limit) = %v, want 11", db.lastArgs[4])
	}
}

func TestListEntitiesAlwaysRestrictsSupportedTypes(t *testing.T) {
	fakeRows := &fakeTwinRows{records: nil}
	db := &fakeTwinDatabase{rows: fakeRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()

	// 1. Unfiltered query (no GatewayID, no EntityType, no cursor)
	_, err = reader.ListEntities(context.Background(), actor, TwinListQuery{Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	unfilteredSQL := db.lastQuery
	if !strings.Contains(unfilteredSQL, "WHERE e.entity_type IN ('Gateway', 'Sensor')") {
		t.Errorf("unfiltered list missing 'WHERE e.entity_type IN (''Gateway'', ''Sensor'')': %s", unfilteredSQL)
	}

	// 2. Query with GatewayID only
	gwID := "gw_01"
	_, err = reader.ListEntities(context.Background(), actor, TwinListQuery{GatewayID: &gwID, Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	gwFilteredSQL := db.lastQuery
	if !strings.Contains(gwFilteredSQL, "e.entity_type IN ('Gateway', 'Sensor')") {
		t.Errorf("gateway-filtered list missing server-owned type predicate: %s", gwFilteredSQL)
	}
}

func TestListEntitiesDefaultLimitApplied(t *testing.T) {
	fakeRows := &fakeTwinRows{records: nil}
	db := &fakeTwinDatabase{rows: fakeRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	// Limit <= 0 should use default 50 (fetch 51)
	_, err = reader.ListEntities(context.Background(), uuid.New(), TwinListQuery{Limit: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(db.lastArgs) != 2 {
		t.Fatalf("expected 2 args, got: %d", len(db.lastArgs))
	}
	if db.lastArgs[1] != 51 {
		t.Errorf("expected limit arg 51 (ContractDefaultLimit + 1), got: %v", db.lastArgs[1])
	}
}

func TestListEntitiesErrorClassifications(t *testing.T) {
	actor := uuid.New()
	q := TwinListQuery{Limit: 50}

	// 1. Context canceled before call
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	db := &fakeTwinDatabase{}
	reader, _ := NewPostgresTwinReader(db)
	_, err := reader.ListEntities(canceledCtx, actor, q)
	if !errors.Is(err, ErrDependencyUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ErrDependencyUnavailable wrapping context.Canceled, got: %v", err)
	}

	// 2. Query returns net.Error
	db = &fakeTwinDatabase{queryErr: &net.OpError{Op: "dial", Err: errors.New("connection refused")}}
	reader, _ = NewPostgresTwinReader(db)
	_, err = reader.ListEntities(context.Background(), actor, q)
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on net.Error, got: %v", err)
	}

	// 3. Query returns puddle.ErrClosedPool
	db = &fakeTwinDatabase{queryErr: puddle.ErrClosedPool}
	reader, _ = NewPostgresTwinReader(db)
	_, err = reader.ListEntities(context.Background(), actor, q)
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on puddle.ErrClosedPool, got: %v", err)
	}

	// 4. Query returns PostgreSQL connection error (code 08006)
	db = &fakeTwinDatabase{queryErr: &pgconn.PgError{Code: "08006", Message: "connection failure"}}
	reader, _ = NewPostgresTwinReader(db)
	_, err = reader.ListEntities(context.Background(), actor, q)
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on pgconn 08006, got: %v", err)
	}

	// 5. Query returns unexpected syntax error
	db = &fakeTwinDatabase{queryErr: errors.New("syntax error at or near FROM")}
	reader, _ = NewPostgresTwinReader(db)
	_, err = reader.ListEntities(context.Background(), actor, q)
	if !errors.Is(err, ErrUnexpected) {
		t.Fatalf("expected ErrUnexpected on arbitrary DB error, got: %v", err)
	}

	// 6. Scan error
	fakeRows := &fakeTwinRows{
		records: []func(dest ...any) error{
			func(_ ...any) error { return errors.New("scan failure") },
		},
		scanErr: errors.New("scan failure"),
	}
	db = &fakeTwinDatabase{rows: fakeRows}
	reader, _ = NewPostgresTwinReader(db)
	_, err = reader.ListEntities(context.Background(), actor, q)
	if !errors.Is(err, ErrUnexpected) {
		t.Fatalf("expected ErrUnexpected on scan error, got: %v", err)
	}
	if !fakeRows.closed {
		t.Fatal("expected rows to be closed on scan error")
	}

	// 7. Iteration endErr (rows.Err()) with dependency error
	fakeRows = &fakeTwinRows{
		records: nil,
		endErr:  &pgconn.PgError{Code: "57P01", Message: "admin shutdown"},
	}
	db = &fakeTwinDatabase{rows: fakeRows}
	reader, _ = NewPostgresTwinReader(db)
	_, err = reader.ListEntities(context.Background(), actor, q)
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on iteration 57P01, got: %v", err)
	}
	if !fakeRows.closed {
		t.Fatal("expected rows to be closed on iteration error")
	}
}

func TestPostgresTwinReaderUnimplementedMethods(t *testing.T) {
	db := &fakeTwinDatabase{}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()
	entity := ResolvedEntity{}

	if _, err := reader.ReadTemporalHistory(context.Background(), actor, entity, TwinHistoryQuery{}); !errors.Is(err, ErrUnexpected) {
		t.Errorf("expected ErrUnexpected from ReadTemporalHistory, got: %v", err)
	}
}

func TestGetDetailRejectsInvalidInputs(t *testing.T) {
	db := &fakeTwinDatabase{}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	validActor := uuid.New()
	validGWEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}

	// 1. actor == uuid.Nil
	if _, err := reader.GetDetail(ctx, uuid.Nil, validGWEntity, TwinDetailQuery{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for nil actor, got: %v", err)
	}

	// 2. entity.PublicURN is empty
	invalidEntity := validGWEntity
	invalidEntity.PublicURN = ""
	if _, err := reader.GetDetail(ctx, validActor, invalidEntity, TwinDetailQuery{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for empty PublicURN, got: %v", err)
	}

	// 3. entity.GatewayID is empty
	invalidEntity = validGWEntity
	invalidEntity.GatewayID = ""
	if _, err := reader.GetDetail(ctx, validActor, invalidEntity, TwinDetailQuery{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for empty GatewayID, got: %v", err)
	}

	// 4. entity.Type is invalid
	invalidEntity = validGWEntity
	invalidEntity.Type = EntityType("Unknown")
	if _, err := reader.GetDetail(ctx, validActor, invalidEntity, TwinDetailQuery{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for invalid EntityType, got: %v", err)
	}

	// 5. entity.InternalID is nil
	invalidEntity = validGWEntity
	invalidEntity.InternalID = uuid.Nil
	if _, err := reader.GetDetail(ctx, validActor, invalidEntity, TwinDetailQuery{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for nil InternalID, got: %v", err)
	}

	// 6. Sensor entity with empty SensorID
	invalidSensor := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Sensor:gw_01:s1",
		Type:       EntityTypeSensor,
		GatewayID:  "gw_01",
		SensorID:   "",
	}
	if _, err := reader.GetDetail(ctx, validActor, invalidSensor, TwinDetailQuery{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for sensor with empty SensorID, got: %v", err)
	}

	// 7. q.EntityID provided but does not match entity.PublicURN
	mismatchedQuery := TwinDetailQuery{
		EntityID: "urn:ngsi-ld:Gateway:different_gw",
	}
	if _, err := reader.GetDetail(ctx, validActor, validGWEntity, mismatchedQuery); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for mismatched EntityID, got: %v", err)
	}

	// 8. q.RelationshipLimit > ContractMaxRelationshipLimit (200)
	overLimitQuery := TwinDetailQuery{
		RelationshipLimit: 201,
	}
	if _, err := reader.GetDetail(ctx, validActor, validGWEntity, overLimitQuery); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for RelationshipLimit > 200, got: %v", err)
	}

	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls on validation failures, got: %d", db.calls)
	}
}

func TestGetDetailCursorValidationAndTampering(t *testing.T) {
	db := &fakeTwinDatabase{}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	actor := uuid.New()
	gwEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}

	// 1. Malformed cursor string
	malformed := "v1.invalidbase64!!!"
	_, err = reader.GetDetail(ctx, actor, gwEntity, TwinDetailQuery{
		RelationshipAfter: &malformed,
	})
	if !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor for malformed cursor, got: %v", err)
	}

	// 2. Cursor created for another Gateway (source URN tampering)
	diffSourceCursor, err := EncodeRelationshipCursor("urn:ngsi-ld:Gateway:gw_02", "hasSensor", "urn:ngsi-ld:Sensor:gw_02:s1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.GetDetail(ctx, actor, gwEntity, TwinDetailQuery{
		RelationshipAfter: &diffSourceCursor,
	})
	if !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor for mismatched source URN in cursor, got: %v", err)
	}

	// 3. Sensor query with relationship cursor
	sensorEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Sensor:gw_01:s1",
		Type:       EntityTypeSensor,
		GatewayID:  "gw_01",
		SensorID:   "s1",
	}
	validGWCursor, err := EncodeRelationshipCursor("urn:ngsi-ld:Gateway:gw_01", "hasSensor", "urn:ngsi-ld:Sensor:gw_01:s1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.GetDetail(ctx, actor, sensorEntity, TwinDetailQuery{
		RelationshipAfter: &validGWCursor,
	})
	if !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor when passing relationship cursor to Sensor, got: %v", err)
	}

	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls on invalid cursors, got: %d", db.calls)
	}
}

func makeGatewayDetailRows(
	urn, entityType, name string,
	desc *string,
	gwID string,
	updatedAt time.Time,
	relationships []struct {
		relType   string
		targetURN string
		createdAt time.Time
	},
) *fakeTwinRows {
	if len(relationships) == 0 {
		return &fakeTwinRows{
			records: []func(dest ...any) error{
				func(dest ...any) error {
					*dest[0].(*string) = urn
					*dest[1].(*string) = entityType
					*dest[2].(*string) = name
					*dest[3].(**string) = desc
					*dest[4].(*string) = gwID
					*dest[5].(*time.Time) = updatedAt
					*dest[6].(**string) = nil
					*dest[7].(**string) = nil
					*dest[8].(**time.Time) = nil
					return nil
				},
			},
		}
	}

	records := make([]func(dest ...any) error, len(relationships))
	for i, rel := range relationships {
		r := rel
		records[i] = func(dest ...any) error {
			*dest[0].(*string) = urn
			*dest[1].(*string) = entityType
			*dest[2].(*string) = name
			*dest[3].(**string) = desc
			*dest[4].(*string) = gwID
			*dest[5].(*time.Time) = updatedAt
			relType := r.relType
			targetURN := r.targetURN
			createdAt := r.createdAt
			*dest[6].(**string) = &relType
			*dest[7].(**string) = &targetURN
			*dest[8].(**time.Time) = &createdAt
			return nil
		}
	}
	return &fakeTwinRows{records: records}
}

func TestGetDetailGatewayExactLimitRelationships(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	gwName := "Gateway 01"
	gwDesc := "First floor gateway"
	actor := uuid.New()
	gwEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}

	rows := makeGatewayDetailRows(
		gwEntity.PublicURN,
		string(EntityTypeGateway),
		gwName,
		&gwDesc,
		gwEntity.GatewayID,
		now,
		[]struct {
			relType   string
			targetURN string
			createdAt time.Time
		}{
			{relType: "hasSensor", targetURN: "urn:ngsi-ld:Sensor:gw_01:s1", createdAt: now},
			{relType: "hasSensor", targetURN: "urn:ngsi-ld:Sensor:gw_01:s2", createdAt: now},
		},
	)

	db := &fakeTwinDatabase{
		rows: rows,
	}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	res, err := reader.GetDetail(context.Background(), actor, gwEntity, TwinDetailQuery{
		EntityID:          gwEntity.PublicURN,
		RelationshipLimit: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result")
	}

	// Verify gateway metadata
	if res.ID != gwEntity.PublicURN || res.Type != EntityTypeGateway {
		t.Errorf("unexpected ID/Type: %s / %s", res.ID, res.Type)
	}
	if res.Name != gwName {
		t.Errorf("name = %s, want %s", res.Name, gwName)
	}
	if res.Description == nil || *res.Description != gwDesc {
		t.Errorf("description = %v, want %s", res.Description, gwDesc)
	}
	if res.Unit != nil {
		t.Errorf("expected nil Unit for gateway, got %v", res.Unit)
	}
	if res.GatewayID != gwEntity.GatewayID {
		t.Errorf("gatewayID = %s, want %s", res.GatewayID, gwEntity.GatewayID)
	}
	if !res.UpdatedAt.Equal(now) {
		t.Errorf("updatedAt = %v, want %v", res.UpdatedAt, now)
	}

	// Verify relationships and pagination page
	if len(res.Relationships) != 2 {
		t.Fatalf("expected 2 relationships, got: %d", len(res.Relationships))
	}
	if res.Relationships[0].TargetURN != "urn:ngsi-ld:Sensor:gw_01:s1" || res.Relationships[0].Type != "Relationship" {
		t.Errorf("unexpected rel 0: %+v", res.Relationships[0])
	}
	if res.Relationships[1].TargetURN != "urn:ngsi-ld:Sensor:gw_01:s2" || res.Relationships[1].RelationshipType != "hasSensor" {
		t.Errorf("unexpected rel 1: %+v", res.Relationships[1])
	}

	if res.RelationshipPage == nil {
		t.Fatal("expected non-nil RelationshipPage")
	}
	if res.RelationshipPage.RelationshipType != "hasSensor" {
		t.Errorf("RelationshipType = %s, want hasSensor", res.RelationshipPage.RelationshipType)
	}
	if res.RelationshipPage.Limit != 2 {
		t.Errorf("Limit = %d, want 2", res.RelationshipPage.Limit)
	}
	if res.RelationshipPage.Count != 2 {
		t.Errorf("Count = %d, want 2", res.RelationshipPage.Count)
	}
	if res.RelationshipPage.Partial {
		t.Error("expected Partial = false for exact limit")
	}
	if res.RelationshipPage.NextCursor != nil {
		t.Errorf("expected nil NextCursor for exact limit, got: %s", *res.RelationshipPage.NextCursor)
	}
	if !rows.closed {
		t.Fatal("expected rows to be closed")
	}
}

func TestGetDetailGatewayLimitPlusOneRelationships(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	gwName := "Gateway 01"
	actor := uuid.New()
	gwEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}

	rows := makeGatewayDetailRows(
		gwEntity.PublicURN,
		string(EntityTypeGateway),
		gwName,
		nil,
		gwEntity.GatewayID,
		now,
		[]struct {
			relType   string
			targetURN string
			createdAt time.Time
		}{
			{relType: "hasSensor", targetURN: "urn:ngsi-ld:Sensor:gw_01:s1", createdAt: now},
			{relType: "hasSensor", targetURN: "urn:ngsi-ld:Sensor:gw_01:s2", createdAt: now},
			{relType: "hasSensor", targetURN: "urn:ngsi-ld:Sensor:gw_01:s3", createdAt: now},
		},
	)

	db := &fakeTwinDatabase{
		rows: rows,
	}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	res, err := reader.GetDetail(context.Background(), actor, gwEntity, TwinDetailQuery{
		RelationshipLimit: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify truncation to 2 items
	if len(res.Relationships) != 2 {
		t.Fatalf("expected truncated 2 relationships, got: %d", len(res.Relationships))
	}
	if res.RelationshipPage.Count != 2 {
		t.Errorf("page.Count = %d, want 2", res.RelationshipPage.Count)
	}
	if !res.RelationshipPage.Partial {
		t.Error("expected Partial = true for limit + 1 rows")
	}
	if res.RelationshipPage.NextCursor == nil {
		t.Fatal("expected non-nil NextCursor on partial page")
	}

	// Decode and verify NextCursor
	cursor, err := DecodeRelationshipCursor(*res.RelationshipPage.NextCursor)
	if err != nil {
		t.Fatalf("failed to decode relationship cursor: %v", err)
	}
	if cursor.SourceURN != gwEntity.PublicURN {
		t.Errorf("cursor.SourceURN = %s, want %s", cursor.SourceURN, gwEntity.PublicURN)
	}
	if cursor.RelType != "hasSensor" {
		t.Errorf("cursor.RelType = %s, want hasSensor", cursor.RelType)
	}
	if cursor.TargetURN != "urn:ngsi-ld:Sensor:gw_01:s2" {
		t.Errorf("cursor.TargetURN = %s, want s2", cursor.TargetURN)
	}
	if !rows.closed {
		t.Fatal("expected rows to be closed")
	}
}

func TestGetDetailGatewayZeroRelationships(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	actor := uuid.New()
	gwEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}

	rows := makeGatewayDetailRows(
		gwEntity.PublicURN,
		string(EntityTypeGateway),
		"Gateway 01",
		nil,
		gwEntity.GatewayID,
		now,
		nil,
	)

	db := &fakeTwinDatabase{
		rows: rows,
	}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	res, err := reader.GetDetail(context.Background(), actor, gwEntity, TwinDetailQuery{
		RelationshipLimit: 50,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Relationships MUST be []RelationshipItem{} (empty slice, not nil)
	if res.Relationships == nil {
		t.Fatal("expected non-nil Relationships slice for zero items")
	}
	if len(res.Relationships) != 0 {
		t.Fatalf("expected 0 relationships, got: %d", len(res.Relationships))
	}
	if res.RelationshipPage.Count != 0 {
		t.Errorf("Count = %d, want 0", res.RelationshipPage.Count)
	}
	if res.RelationshipPage.Partial {
		t.Error("expected Partial = false")
	}
	if res.RelationshipPage.NextCursor != nil {
		t.Errorf("expected nil NextCursor, got: %v", *res.RelationshipPage.NextCursor)
	}
	if !rows.closed {
		t.Fatal("expected rows to be closed")
	}
}

func TestGetDetailSensorSuccess(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	sensorName := "Temperature Sensor"
	sensorUnit := "degC"
	actor := uuid.New()
	sensorEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Sensor:gw_01:temp",
		Type:       EntityTypeSensor,
		GatewayID:  "gw_01",
		SensorID:   "temp",
	}

	sensorRow := fakeRow{
		scanFn: func(dest ...any) error {
			*dest[0].(*string) = sensorEntity.PublicURN
			*dest[1].(*string) = string(EntityTypeSensor)
			*dest[2].(*string) = sensorName
			*dest[3].(**string) = &sensorUnit
			*dest[4].(*string) = sensorEntity.GatewayID
			*dest[5].(*time.Time) = now
			return nil
		},
	}

	db := &fakeTwinDatabase{row: sensorRow}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	res, err := reader.GetDetail(context.Background(), actor, sensorEntity, TwinDetailQuery{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result")
	}

	if res.ID != sensorEntity.PublicURN || res.Type != EntityTypeSensor {
		t.Errorf("unexpected ID/Type: %s / %s", res.ID, res.Type)
	}
	if res.Name != sensorName {
		t.Errorf("name = %s, want %s", res.Name, sensorName)
	}
	if res.Description != nil {
		t.Errorf("expected nil Description for Sensor, got: %v", res.Description)
	}
	if res.Unit == nil || *res.Unit != sensorUnit {
		t.Errorf("unit = %v, want %s", res.Unit, sensorUnit)
	}
	if res.GatewayID != sensorEntity.GatewayID {
		t.Errorf("gatewayID = %s, want %s", res.GatewayID, sensorEntity.GatewayID)
	}
	if !res.UpdatedAt.Equal(now) {
		t.Errorf("updatedAt = %v, want %v", res.UpdatedAt, now)
	}
	if res.Relationships != nil {
		t.Errorf("expected nil Relationships for Sensor, got: %v", res.Relationships)
	}
	if res.RelationshipPage != nil {
		t.Errorf("expected nil RelationshipPage for Sensor, got: %v", res.RelationshipPage)
	}
}

func TestGetDetailSensorBrokenGraphReturnsNotFound(t *testing.T) {
	actor := uuid.New()
	sensorEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Sensor:gw_01:orphan_sensor",
		Type:       EntityTypeSensor,
		GatewayID:  "gw_01",
		SensorID:   "orphan_sensor",
	}

	// Graph broken: parent gateway or hasSensor relationship does not exist -> pgx.ErrNoRows
	db := &fakeTwinDatabase{
		row: fakeRow{
			scanFn: func(_ ...any) error {
				return pgx.ErrNoRows
			},
		},
	}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetDetail(context.Background(), actor, sensorEntity, TwinDetailQuery{})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for broken graph, got: %v", err)
	}
}

func TestGetDetailActorScopingNonMemberReturnsNotFound(t *testing.T) {
	nonMemberActor := uuid.New()
	gwEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}
	sensorEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Sensor:gw_01:s1",
		Type:       EntityTypeSensor,
		GatewayID:  "gw_01",
		SensorID:   "s1",
	}

	// 1. Gateway query with non-member actor -> 0 rows -> ErrNotFound
	db := &fakeTwinDatabase{
		rows: &fakeTwinRows{records: nil},
	}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetDetail(context.Background(), nonMemberActor, gwEntity, TwinDetailQuery{})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for non-member on Gateway, got: %v", err)
	}

	// 2. Sensor query with non-member actor -> pgx.ErrNoRows -> ErrNotFound
	db = &fakeTwinDatabase{
		row: fakeRow{
			scanFn: func(_ ...any) error {
				return pgx.ErrNoRows
			},
		},
	}
	reader, err = NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.GetDetail(context.Background(), nonMemberActor, sensorEntity, TwinDetailQuery{})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for non-member on Sensor, got: %v", err)
	}
}

func TestGetDetailTargetSensorIsolationInSQL(t *testing.T) {
	actor := uuid.New()
	gwEntity := ResolvedEntity{
		InternalID: uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2"),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}

	now := time.Now().UTC()
	afterCursor, err := EncodeRelationshipCursor("urn:ngsi-ld:Gateway:gw_01", "hasSensor", "urn:ngsi-ld:Sensor:gw_01:s1")
	if err != nil {
		t.Fatal(err)
	}

	rows := makeGatewayDetailRows(
		gwEntity.PublicURN,
		string(EntityTypeGateway),
		"GW 01",
		nil,
		gwEntity.GatewayID,
		now,
		nil,
	)

	db := &fakeTwinDatabase{
		rows: rows,
	}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetDetail(context.Background(), actor, gwEntity, TwinDetailQuery{
		RelationshipLimit: 10,
		RelationshipAfter: &afterCursor,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(db.queries) != 1 {
		t.Fatalf("expected exactly 1 parameterized query for Gateway detail, got: %d", len(db.queries))
	}

	// Gateway single query verification with CTE and LATERAL
	query := db.queries[0]
	if !strings.Contains(query, "WITH authorized_source AS (") {
		t.Errorf("query missing WITH authorized_source CTE: %s", query)
	}
	if !strings.Contains(query, "JOIN public.user_gateways AS ug ON ug.gateway_id = g.gateway_id AND ug.user_id = $1") {
		t.Errorf("query missing user_gateways scoped by actor $1 in CTE: %s", query)
	}
	if !strings.Contains(query, "WHERE e.id = $2") || !strings.Contains(query, "e.entity_id = $3") || !strings.Contains(query, "e.entity_type = $4") || !strings.Contains(query, "e.gateway_id = $5") {
		t.Errorf("query missing source parameter binding in CTE: %s", query)
	}
	if !strings.Contains(query, "LEFT JOIN LATERAL (") {
		t.Errorf("query missing LEFT JOIN LATERAL: %s", query)
	}
	if !strings.Contains(query, "tgt.entity_type = 'Sensor'") || !strings.Contains(query, "tgt.gateway_id = src.source_gateway_id") {
		t.Errorf("query missing target sensor and gateway isolation in LATERAL: %s", query)
	}
	if !strings.Contains(query, "s.gateway_id = tgt.gateway_id") || !strings.Contains(query, "('urn:ngsi-ld:Sensor:' || s.gateway_id || ':' || s.sensor_id) = tgt.entity_id") {
		t.Errorf("query missing canonical Sensor resource verification: %s", query)
	}
	if !strings.Contains(query, "r.source_entity_id = src.source_id") || !strings.Contains(query, "r.relationship_type = 'hasSensor'") {
		t.Errorf("query missing source relation check: %s", query)
	}
	if !strings.Contains(query, `tgt.entity_id COLLATE "C" > $6`) {
		t.Errorf("query missing cursor continuation with collation \"C\": %s", query)
	}
	if !strings.Contains(query, `ORDER BY tgt.entity_id COLLATE "C" ASC`) {
		t.Errorf("query missing ORDER BY collation \"C\" ASC: %s", query)
	}
	if !strings.Contains(query, "LIMIT $7") {
		t.Errorf("query missing LIMIT $7: %s", query)
	}

	// Verify query arguments
	args := db.argsHistory[0]
	if len(args) != 7 {
		t.Fatalf("expected 7 args for Gateway detail query, got %d: %v", len(args), args)
	}
	if args[0] != actor {
		t.Errorf("arg 0 (actor) = %v, want %v", args[0], actor)
	}
	if args[1] != gwEntity.InternalID {
		t.Errorf("arg 1 (internal_id) = %v, want %v", args[1], gwEntity.InternalID)
	}
	if args[2] != gwEntity.PublicURN {
		t.Errorf("arg 2 (public_urn) = %v, want %s", args[2], gwEntity.PublicURN)
	}
	if args[3] != string(EntityTypeGateway) {
		t.Errorf("arg 3 (type) = %v, want Gateway", args[3])
	}
	if args[4] != gwEntity.GatewayID {
		t.Errorf("arg 4 (gateway_id) = %v, want %s", args[4], gwEntity.GatewayID)
	}
	if args[5] != "urn:ngsi-ld:Sensor:gw_01:s1" {
		t.Errorf("arg 5 (cursor target_urn) = %v, want s1", args[5])
	}
	if args[6] != 11 { // limit 10 + 1
		t.Errorf("arg 6 (limit) = %v, want 11", args[6])
	}
}

func TestGetDetailErrorClassifications(t *testing.T) {
	actor := uuid.New()
	gwEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}

	// 1. Context canceled before call
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	db := &fakeTwinDatabase{}
	reader, _ := NewPostgresTwinReader(db)
	_, err := reader.GetDetail(canceledCtx, actor, gwEntity, TwinDetailQuery{})
	if !errors.Is(err, ErrDependencyUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ErrDependencyUnavailable wrapping context.Canceled, got: %v", err)
	}

	// 2. Query returns net.Error
	db = &fakeTwinDatabase{
		queryErr: &net.OpError{Op: "read", Err: errors.New("network unreachable")},
	}
	reader, _ = NewPostgresTwinReader(db)
	_, err = reader.GetDetail(context.Background(), actor, gwEntity, TwinDetailQuery{})
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on net.Error, got: %v", err)
	}

	// 3. Query returns pgconn 08006
	db = &fakeTwinDatabase{
		queryErr: &pgconn.PgError{Code: "08006", Message: "connection failure"},
	}
	reader, _ = NewPostgresTwinReader(db)
	_, err = reader.GetDetail(context.Background(), actor, gwEntity, TwinDetailQuery{})
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on pgconn 08006, got: %v", err)
	}

	// 4. Query scan error
	scanErrRows := &fakeTwinRows{
		records: []func(dest ...any) error{
			func(_ ...any) error { return errors.New("scan failure") },
		},
		scanErr: errors.New("scan failure"),
	}
	db = &fakeTwinDatabase{
		rows: scanErrRows,
	}
	reader, _ = NewPostgresTwinReader(db)
	_, err = reader.GetDetail(context.Background(), actor, gwEntity, TwinDetailQuery{})
	if !errors.Is(err, ErrUnexpected) {
		t.Fatalf("expected ErrUnexpected on scan error, got: %v", err)
	}
	if !scanErrRows.closed {
		t.Fatal("expected scanErrRows to be closed")
	}

	// 5. Query rows.Err() returns pgconn 57P01
	endErrRows := &fakeTwinRows{
		records: nil,
		endErr:  &pgconn.PgError{Code: "57P01", Message: "admin shutdown"},
	}
	db = &fakeTwinDatabase{
		rows: endErrRows,
	}
	reader, _ = NewPostgresTwinReader(db)
	_, err = reader.GetDetail(context.Background(), actor, gwEntity, TwinDetailQuery{})
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on 57P01, got: %v", err)
	}
	if !endErrRows.closed {
		t.Fatal("expected endErrRows to be closed")
	}
}

func TestGetStateRejectsInvalidInputs(t *testing.T) {
	db := &fakeTwinDatabase{}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	validActor := uuid.New()
	validGWEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}

	// 1. actor == uuid.Nil
	if _, err := reader.GetState(ctx, uuid.Nil, validGWEntity); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for nil actor, got: %v", err)
	}

	// 2. entity.PublicURN is empty
	invalidEntity := validGWEntity
	invalidEntity.PublicURN = ""
	if _, err := reader.GetState(ctx, validActor, invalidEntity); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for empty PublicURN, got: %v", err)
	}

	// 3. entity.GatewayID is empty
	invalidEntity = validGWEntity
	invalidEntity.GatewayID = ""
	if _, err := reader.GetState(ctx, validActor, invalidEntity); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for empty GatewayID, got: %v", err)
	}

	// 4. entity.Type is invalid
	invalidEntity = validGWEntity
	invalidEntity.Type = EntityType("Unknown")
	if _, err := reader.GetState(ctx, validActor, invalidEntity); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for invalid EntityType, got: %v", err)
	}

	// 5. entity.InternalID is nil
	invalidEntity = validGWEntity
	invalidEntity.InternalID = uuid.Nil
	if _, err := reader.GetState(ctx, validActor, invalidEntity); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for nil InternalID, got: %v", err)
	}

	// 6. Sensor entity with empty SensorID
	invalidSensor := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Sensor:gw_01:s1",
		Type:       EntityTypeSensor,
		GatewayID:  "gw_01",
		SensorID:   "",
	}
	if _, err := reader.GetState(ctx, validActor, invalidSensor); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for sensor with empty SensorID, got: %v", err)
	}

	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls on validation failures, got: %d", db.calls)
	}
}

type fakeTerminalErrorRows struct {
	pgx.Rows
	records []func(dest ...any) error
	idx     int
	closed  bool
	termErr error
}

func (r *fakeTerminalErrorRows) Next() bool {
	if r.idx < len(r.records) {
		r.idx++
		return true
	}
	return false
}

func (r *fakeTerminalErrorRows) Scan(dest ...any) error {
	if r.idx-1 >= 0 && r.idx-1 < len(r.records) {
		return r.records[r.idx-1](dest...)
	}
	return nil
}

func (r *fakeTerminalErrorRows) Close() {
	r.closed = true
}

func (r *fakeTerminalErrorRows) Err() error {
	if r.closed {
		return r.termErr
	}
	return nil
}

func makeStateRow(
	urn, entityType, gwID string,
	hasState bool,
	reportedJSON, desiredJSON []byte,
	reportedVer, desiredVer *int64,
	lastReported, lastDesired *time.Time,
	updatedAt *time.Time,
) *fakeTwinRows {
	return &fakeTwinRows{
		records: []func(dest ...any) error{
			func(dest ...any) error {
				*dest[0].(*string) = urn
				*dest[1].(*string) = entityType
				*dest[2].(*string) = gwID
				*dest[3].(*bool) = hasState
				if hasState {
					*dest[4].(*[]byte) = reportedJSON
					*dest[5].(*[]byte) = desiredJSON
					*dest[6].(**int64) = reportedVer
					*dest[7].(**int64) = desiredVer
					*dest[8].(**time.Time) = lastReported
					*dest[9].(**time.Time) = lastDesired
					*dest[10].(**time.Time) = updatedAt
				} else {
					*dest[4].(*[]byte) = nil
					*dest[5].(*[]byte) = nil
					*dest[6].(**int64) = nil
					*dest[7].(**int64) = nil
					*dest[8].(**time.Time) = nil
					*dest[9].(**time.Time) = nil
					*dest[10].(**time.Time) = nil
				}
				return nil
			},
		},
	}
}

func TestGetStateGatewaySuccess(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	lastReported := now.Add(-10 * time.Second)
	lastDesired := now.Add(-5 * time.Second)
	actor := uuid.New()
	gwEntity := ResolvedEntity{
		InternalID: uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2"),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}

	reportedJSON := []byte(`{"sampling_interval_seconds":5,"online":true}`)
	desiredJSON := []byte(`{"sampling_interval_seconds":10}`)
	reportedVer := int64(12)
	desiredVer := int64(14)

	stateRows := makeStateRow(
		gwEntity.PublicURN,
		string(EntityTypeGateway),
		gwEntity.GatewayID,
		true,
		reportedJSON,
		desiredJSON,
		&reportedVer,
		&desiredVer,
		&lastReported,
		&lastDesired,
		&now,
	)

	db := &fakeTwinDatabase{rows: stateRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	res, err := reader.GetState(context.Background(), actor, gwEntity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result")
	}

	// Verify returned fields
	if res.ID != gwEntity.PublicURN {
		t.Errorf("ID = %s, want %s", res.ID, gwEntity.PublicURN)
	}
	if res.Type != EntityTypeGateway {
		t.Errorf("Type = %s, want %s", res.Type, EntityTypeGateway)
	}
	if res.GatewayID != gwEntity.GatewayID {
		t.Errorf("GatewayID = %s, want %s", res.GatewayID, gwEntity.GatewayID)
	}
	if string(res.ReportedState) != string(reportedJSON) {
		t.Errorf("ReportedState = %s, want %s", string(res.ReportedState), string(reportedJSON))
	}
	if string(res.DesiredState) != string(desiredJSON) {
		t.Errorf("DesiredState = %s, want %s", string(res.DesiredState), string(desiredJSON))
	}
	if res.ReportedVersion != reportedVer {
		t.Errorf("ReportedVersion = %d, want %d", res.ReportedVersion, reportedVer)
	}
	if res.DesiredVersion != desiredVer {
		t.Errorf("DesiredVersion = %d, want %d", res.DesiredVersion, desiredVer)
	}
	if res.LastReportedAt == nil || !res.LastReportedAt.Equal(lastReported) {
		t.Errorf("LastReportedAt = %v, want %v", res.LastReportedAt, lastReported)
	}
	if res.LastDesiredAt == nil || !res.LastDesiredAt.Equal(lastDesired) {
		t.Errorf("LastDesiredAt = %v, want %v", res.LastDesiredAt, lastDesired)
	}
	if !res.UpdatedAt.Equal(now) {
		t.Errorf("UpdatedAt = %v, want %v", res.UpdatedAt, now)
	}
	if !stateRows.closed {
		t.Fatal("expected rows to be closed")
	}

	// Verify query string and arguments
	if db.calls != 1 {
		t.Fatalf("expected 1 DB call, got %d", db.calls)
	}
	query := db.lastQuery
	if !strings.Contains(query, "SELECT e.entity_id, e.entity_type, e.gateway_id") {
		t.Errorf("query missing entity columns: %s", query)
	}
	if !strings.Contains(query, "s.entity_id IS NOT NULL AS has_state") {
		t.Errorf("query missing has_state column: %s", query)
	}
	if !strings.Contains(query, "s.reported_state, s.desired_state") {
		t.Errorf("query missing state columns: %s", query)
	}
	if !strings.Contains(query, "s.reported_version, s.desired_version") {
		t.Errorf("query missing version columns: %s", query)
	}
	if !strings.Contains(query, "s.last_reported_at, s.last_desired_at, s.updated_at") {
		t.Errorf("query missing timestamp columns: %s", query)
	}
	if strings.Contains(query, "last_desired_by") {
		t.Errorf("query must NOT select last_desired_by: %s", query)
	}
	if !strings.Contains(query, "FROM public.twin_entities AS e") {
		t.Errorf("query missing FROM public.twin_entities: %s", query)
	}
	if !strings.Contains(query, "LEFT JOIN public.twin_states AS s ON s.entity_id = e.id") {
		t.Errorf("query missing LEFT JOIN twin_states: %s", query)
	}
	if !strings.Contains(query, "JOIN public.gateways AS g ON g.gateway_id = e.gateway_id") {
		t.Errorf("query missing JOIN gateways: %s", query)
	}
	if !strings.Contains(query, "JOIN public.user_gateways AS ug ON ug.gateway_id = g.gateway_id AND ug.user_id = $1") {
		t.Errorf("query missing JOIN user_gateways scoped by actor $1: %s", query)
	}
	if !strings.Contains(query, "WHERE e.id = $2") || !strings.Contains(query, "e.entity_id = $3") || !strings.Contains(query, "e.entity_type = $4") || !strings.Contains(query, "e.gateway_id = $5") {
		t.Errorf("query missing Gateway WHERE predicates: %s", query)
	}

	// Verify query arguments
	if len(db.lastArgs) != 5 {
		t.Fatalf("expected 5 arguments, got %d: %v", len(db.lastArgs), db.lastArgs)
	}
	if db.lastArgs[0] != actor {
		t.Errorf("arg 0 (actor) = %v, want %v", db.lastArgs[0], actor)
	}
	if db.lastArgs[1] != gwEntity.InternalID {
		t.Errorf("arg 1 (internal_id) = %v, want %v", db.lastArgs[1], gwEntity.InternalID)
	}
	if db.lastArgs[2] != gwEntity.PublicURN {
		t.Errorf("arg 2 (public_urn) = %v, want %s", db.lastArgs[2], gwEntity.PublicURN)
	}
	if db.lastArgs[3] != string(EntityTypeGateway) {
		t.Errorf("arg 3 (type) = %v, want %s", db.lastArgs[3], EntityTypeGateway)
	}
	if db.lastArgs[4] != gwEntity.GatewayID {
		t.Errorf("arg 4 (gateway_id) = %v, want %s", db.lastArgs[4], gwEntity.GatewayID)
	}
}

func TestGetStateSensorSuccess(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	lastReported := now.Add(-1 * time.Minute)
	actor := uuid.New()
	sensorEntity := ResolvedEntity{
		InternalID: uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf3"),
		PublicURN:  "urn:ngsi-ld:Sensor:gw_01:temp",
		Type:       EntityTypeSensor,
		GatewayID:  "gw_01",
		SensorID:   "temp",
	}

	reportedJSON := []byte(`{"sample_interval_us":10000}`)
	desiredJSON := []byte(`{}`)
	reportedVer := int64(100)
	desiredVer := int64(0)

	stateRows := makeStateRow(
		sensorEntity.PublicURN,
		string(EntityTypeSensor),
		sensorEntity.GatewayID,
		true,
		reportedJSON,
		desiredJSON,
		&reportedVer,
		&desiredVer,
		&lastReported,
		nil, // nullable lastDesiredAt null
		&now,
	)

	db := &fakeTwinDatabase{rows: stateRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	res, err := reader.GetState(context.Background(), actor, sensorEntity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result")
	}

	if res.ID != sensorEntity.PublicURN || res.Type != EntityTypeSensor {
		t.Errorf("unexpected ID/Type: %s / %s", res.ID, res.Type)
	}
	if res.GatewayID != sensorEntity.GatewayID {
		t.Errorf("GatewayID = %s, want %s", res.GatewayID, sensorEntity.GatewayID)
	}
	if string(res.ReportedState) != string(reportedJSON) {
		t.Errorf("ReportedState = %s, want %s", string(res.ReportedState), string(reportedJSON))
	}
	if string(res.DesiredState) != string(desiredJSON) {
		t.Errorf("DesiredState = %s, want %s", string(res.DesiredState), string(desiredJSON))
	}
	if res.ReportedVersion != reportedVer || res.DesiredVersion != desiredVer {
		t.Errorf("Versions: reported=%d, desired=%d", res.ReportedVersion, res.DesiredVersion)
	}
	if res.LastReportedAt == nil || !res.LastReportedAt.Equal(lastReported) {
		t.Errorf("LastReportedAt = %v, want %v", res.LastReportedAt, lastReported)
	}
	if res.LastDesiredAt != nil {
		t.Errorf("expected nil LastDesiredAt, got %v", res.LastDesiredAt)
	}
	if !res.UpdatedAt.Equal(now) {
		t.Errorf("UpdatedAt = %v, want %v", res.UpdatedAt, now)
	}
	if !stateRows.closed {
		t.Fatal("expected rows to be closed")
	}

	// Verify query string and arguments
	if db.calls != 1 {
		t.Fatalf("expected 1 DB call, got %d", db.calls)
	}
	query := db.lastQuery
	if !strings.Contains(query, "FROM public.twin_entities AS e") {
		t.Errorf("query missing FROM twin_entities: %s", query)
	}
	if !strings.Contains(query, "JOIN public.sensors AS sens ON sens.gateway_id = e.gateway_id AND sens.sensor_id = $1") {
		t.Errorf("query missing JOIN sensors on $1: %s", query)
	}
	if !strings.Contains(query, "JOIN public.twin_entities AS parent_e ON parent_e.gateway_id = e.gateway_id AND parent_e.entity_type = 'Gateway'") {
		t.Errorf("query missing JOIN parent_e: %s", query)
	}
	if !strings.Contains(query, "JOIN public.twin_relationships AS r ON r.source_entity_id = parent_e.id AND r.target_entity_id = e.id AND r.relationship_type = 'hasSensor'") {
		t.Errorf("query missing JOIN twin_relationships hasSensor: %s", query)
	}
	if !strings.Contains(query, "JOIN public.gateways AS g ON g.gateway_id = e.gateway_id") {
		t.Errorf("query missing JOIN gateways: %s", query)
	}
	if !strings.Contains(query, "JOIN public.user_gateways AS ug ON ug.gateway_id = g.gateway_id AND ug.user_id = $2") {
		t.Errorf("query missing JOIN user_gateways scoped by actor $2: %s", query)
	}
	if !strings.Contains(query, "LEFT JOIN public.twin_states AS s ON s.entity_id = e.id") {
		t.Errorf("query missing LEFT JOIN twin_states: %s", query)
	}
	if !strings.Contains(query, "WHERE e.id = $3") || !strings.Contains(query, "e.entity_id = $4") || !strings.Contains(query, "e.entity_type = $5") || !strings.Contains(query, "e.gateway_id = $6") {
		t.Errorf("query missing Sensor WHERE predicates: %s", query)
	}

	// Verify query arguments
	if len(db.lastArgs) != 6 {
		t.Fatalf("expected 6 arguments, got %d: %v", len(db.lastArgs), db.lastArgs)
	}
	if db.lastArgs[0] != sensorEntity.SensorID {
		t.Errorf("arg 0 (sensor_id) = %v, want %s", db.lastArgs[0], sensorEntity.SensorID)
	}
	if db.lastArgs[1] != actor {
		t.Errorf("arg 1 (actor) = %v, want %v", db.lastArgs[1], actor)
	}
	if db.lastArgs[2] != sensorEntity.InternalID {
		t.Errorf("arg 2 (internal_id) = %v, want %v", db.lastArgs[2], sensorEntity.InternalID)
	}
	if db.lastArgs[3] != sensorEntity.PublicURN {
		t.Errorf("arg 3 (public_urn) = %v, want %s", db.lastArgs[3], sensorEntity.PublicURN)
	}
	if db.lastArgs[4] != string(EntityTypeSensor) {
		t.Errorf("arg 4 (type) = %v, want %s", db.lastArgs[4], EntityTypeSensor)
	}
	if db.lastArgs[5] != sensorEntity.GatewayID {
		t.Errorf("arg 5 (gateway_id) = %v, want %s", db.lastArgs[5], sensorEntity.GatewayID)
	}
}

func TestGetStateActorScopingNonMemberReturnsNotFound(t *testing.T) {
	nonMemberActor := uuid.New()
	gwEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}
	sensorEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Sensor:gw_01:s1",
		Type:       EntityTypeSensor,
		GatewayID:  "gw_01",
		SensorID:   "s1",
	}

	// 1. Gateway state query with non-member actor -> 0 rows -> ErrNotFound
	emptyRows := &fakeTwinRows{records: nil}
	db := &fakeTwinDatabase{rows: emptyRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetState(context.Background(), nonMemberActor, gwEntity)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for non-member on Gateway, got: %v", err)
	}
	if !emptyRows.closed {
		t.Fatal("expected emptyRows to be closed")
	}

	// 2. Sensor state query with non-member actor -> 0 rows -> ErrNotFound
	emptySensorRows := &fakeTwinRows{records: nil}
	db.rows = emptySensorRows
	_, err = reader.GetState(context.Background(), nonMemberActor, sensorEntity)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for non-member on Sensor, got: %v", err)
	}
	if !emptySensorRows.closed {
		t.Fatal("expected emptySensorRows to be closed")
	}
}

func TestGetStateMissingStateRowReturnsUnexpected(t *testing.T) {
	actor := uuid.New()
	gwEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}

	// Authorized entity exists, but twin_states row is missing -> has_state is false
	missingStateRows := makeStateRow(
		gwEntity.PublicURN,
		string(EntityTypeGateway),
		gwEntity.GatewayID,
		false,
		nil, nil, nil, nil, nil, nil, nil,
	)
	db := &fakeTwinDatabase{rows: missingStateRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	// MUST return ErrUnexpected for data fault, must NOT invent empty state JSON
	_, err = reader.GetState(context.Background(), actor, gwEntity)
	if !errors.Is(err, ErrUnexpected) {
		t.Fatalf("expected ErrUnexpected for missing state row on authorized entity, got: %v", err)
	}
	if !missingStateRows.closed {
		t.Fatal("expected missingStateRows to be closed")
	}
}

func TestGetStateSensorBrokenGraphReturnsNotFound(t *testing.T) {
	actor := uuid.New()
	sensorEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Sensor:gw_01:orphan_sensor",
		Type:       EntityTypeSensor,
		GatewayID:  "gw_01",
		SensorID:   "orphan_sensor",
	}

	// Graph broken: parent gateway or hasSensor relationship does not exist -> 0 rows returned
	brokenGraphRows := &fakeTwinRows{records: nil}
	db := &fakeTwinDatabase{rows: brokenGraphRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.GetState(context.Background(), actor, sensorEntity)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for broken sensor graph, got: %v", err)
	}
	if !brokenGraphRows.closed {
		t.Fatal("expected brokenGraphRows to be closed")
	}
}

func TestGetStateJSONBPreservationAndBigNumbers(t *testing.T) {
	now := time.Now().UTC()
	actor := uuid.New()
	gwEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}

	// Complex JSON with big integers, arrays, booleans, nested structures, nulls
	reportedJSON := []byte(`{"big_int":9223372036854775807,"huge_counter":18446744073709551615,"nested":{"arr":[1,true,null,"str"]},"zero":0}`)
	desiredJSON := []byte(`{"float_val":12345.6789012345,"nullable":null,"str_val":"exact \"quotes\" & unicode \u00a9"}`)
	repVer := int64(1)
	desVer := int64(2)

	stateRows := makeStateRow(
		gwEntity.PublicURN,
		string(EntityTypeGateway),
		gwEntity.GatewayID,
		true,
		reportedJSON,
		desiredJSON,
		&repVer,
		&desVer,
		nil,
		nil,
		&now,
	)

	db := &fakeTwinDatabase{rows: stateRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	res, err := reader.GetState(context.Background(), actor, gwEntity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Assert byte-for-byte exact preservation
	if string(res.ReportedState) != string(reportedJSON) {
		t.Errorf("ReportedState not preserved: got %s, want %s", string(res.ReportedState), string(reportedJSON))
	}
	if string(res.DesiredState) != string(desiredJSON) {
		t.Errorf("DesiredState not preserved: got %s, want %s", string(res.DesiredState), string(desiredJSON))
	}
}

func TestGetStateNullableTimestamps(t *testing.T) {
	now := time.Now().UTC()
	actor := uuid.New()
	gwEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}

	repVer := int64(0)
	desVer := int64(0)
	stateRows := makeStateRow(
		gwEntity.PublicURN,
		string(EntityTypeGateway),
		gwEntity.GatewayID,
		true,
		[]byte("{}"),
		[]byte("{}"),
		&repVer,
		&desVer,
		nil, // LastReportedAt is NULL
		nil, // LastDesiredAt is NULL
		&now,
	)

	db := &fakeTwinDatabase{rows: stateRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	res, err := reader.GetState(context.Background(), actor, gwEntity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.LastReportedAt != nil {
		t.Errorf("expected nil LastReportedAt, got: %v", res.LastReportedAt)
	}
	if res.LastDesiredAt != nil {
		t.Errorf("expected nil LastDesiredAt, got: %v", res.LastDesiredAt)
	}
}

func TestGetStateStreamCompletionTerminalErrorExposedOnClose(t *testing.T) {
	now := time.Now().UTC()
	actor := uuid.New()
	gwEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}

	reportedJSON := []byte(`{"online":true}`)
	desiredJSON := []byte(`{}`)
	repVer := int64(1)
	desVer := int64(0)

	// Rows that only expose terminal error AFTER Close() is called
	termRows := &fakeTerminalErrorRows{
		records: []func(dest ...any) error{
			func(dest ...any) error {
				*dest[0].(*string) = gwEntity.PublicURN
				*dest[1].(*string) = string(EntityTypeGateway)
				*dest[2].(*string) = gwEntity.GatewayID
				*dest[3].(*bool) = true
				*dest[4].(*[]byte) = reportedJSON
				*dest[5].(*[]byte) = desiredJSON
				*dest[6].(**int64) = &repVer
				*dest[7].(**int64) = &desVer
				*dest[8].(**time.Time) = nil
				*dest[9].(**time.Time) = nil
				*dest[10].(**time.Time) = &now
				return nil
			},
		},
		termErr: &pgconn.PgError{Code: "57P01", Message: "admin shutdown"},
	}

	db := &fakeTwinDatabase{
		queryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			return termRows, nil
		},
	}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	res, err := reader.GetState(context.Background(), actor, gwEntity)
	if res != nil {
		t.Errorf("expected nil result on terminal stream error, got: %+v", res)
	}
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on terminal 57P01 error, got: %v", err)
	}
	if !termRows.closed {
		t.Fatal("expected termRows to be closed")
	}
}

func TestGetStateErrorClassifications(t *testing.T) {
	actor := uuid.New()
	gwEntity := ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
		Type:       EntityTypeGateway,
		GatewayID:  "gw_01",
	}

	// 1. Context canceled before call
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	db := &fakeTwinDatabase{}
	reader, _ := NewPostgresTwinReader(db)
	_, err := reader.GetState(canceledCtx, actor, gwEntity)
	if !errors.Is(err, ErrDependencyUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ErrDependencyUnavailable wrapping context.Canceled, got: %v", err)
	}

	// 2. Query returns net.Error
	db = &fakeTwinDatabase{
		queryErr: &net.OpError{Op: "read", Err: errors.New("network unreachable")},
	}
	reader, _ = NewPostgresTwinReader(db)
	_, err = reader.GetState(context.Background(), actor, gwEntity)
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on net.Error, got: %v", err)
	}

	// 3. Query returns pgconn error 08006
	db = &fakeTwinDatabase{
		queryErr: &pgconn.PgError{Code: "08006", Message: "connection failure"},
	}
	reader, _ = NewPostgresTwinReader(db)
	_, err = reader.GetState(context.Background(), actor, gwEntity)
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on pgconn 08006, got: %v", err)
	}

	// 4. Rows scan error
	scanErrRows := &fakeTwinRows{
		records: []func(dest ...any) error{
			func(_ ...any) error { return errors.New("scan failure") },
		},
		scanErr: errors.New("scan failure"),
	}
	db = &fakeTwinDatabase{rows: scanErrRows}
	reader, _ = NewPostgresTwinReader(db)
	_, err = reader.GetState(context.Background(), actor, gwEntity)
	if !errors.Is(err, ErrUnexpected) {
		t.Fatalf("expected ErrUnexpected on scan error, got: %v", err)
	}
	if !scanErrRows.closed {
		t.Fatal("expected scanErrRows to be closed")
	}

	// 5. rows.Err() returns pgconn 57P01
	repVer := int64(0)
	desVer := int64(0)
	now := time.Now().UTC()
	endErrRows := makeStateRow(
		gwEntity.PublicURN,
		string(EntityTypeGateway),
		gwEntity.GatewayID,
		true,
		[]byte("{}"),
		[]byte("{}"),
		&repVer,
		&desVer,
		nil,
		nil,
		&now,
	)
	endErrRows.endErr = &pgconn.PgError{Code: "57P01", Message: "admin shutdown"}
	db = &fakeTwinDatabase{rows: endErrRows}
	reader, _ = NewPostgresTwinReader(db)
	_, err = reader.GetState(context.Background(), actor, gwEntity)
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on 57P01, got: %v", err)
	}
	if !endErrRows.closed {
		t.Fatal("expected endErrRows to be closed")
	}
}

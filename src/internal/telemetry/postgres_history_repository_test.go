package telemetry_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"iot-platform/internal/telemetry"
)

// fakeHistoryRows implements pgx.Rows for testing row scanning and lifecycle.
type fakeHistoryRows struct {
	pgx.Rows
	records []func(dest ...any) error
	idx     int
	scanErr error
	endErr  error
	closed  bool
}

func (r *fakeHistoryRows) Next() bool {
	if r.idx < len(r.records) {
		r.idx++
		return true
	}
	return false
}

func (r *fakeHistoryRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.idx-1 >= 0 && r.idx-1 < len(r.records) {
		return r.records[r.idx-1](dest...)
	}
	return nil
}

func (r *fakeHistoryRows) Err() error {
	return r.endErr
}

func (r *fakeHistoryRows) Close() {
	r.closed = true
}

// fakeHistoryDatabase implements telemetry.HistoryDatabase for unit tests.
type fakeHistoryDatabase struct {
	calls    int
	lastCtx  context.Context
	lastSQL  string
	lastArgs []any
	rows     *fakeHistoryRows
	queryErr error
}

func (d *fakeHistoryDatabase) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	d.calls++
	d.lastCtx = ctx
	d.lastSQL = sql
	d.lastArgs = args
	if d.queryErr != nil {
		return nil, d.queryErr
	}
	if d.rows != nil {
		return d.rows, nil
	}
	return &fakeHistoryRows{}, nil
}

func validRawQuery() telemetry.TelemetryHistoryQuery {
	from := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	return telemetry.TelemetryHistoryQuery{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		Range:      telemetry.TimeRange{From: from, To: to},
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  100,
	}
}

func TestNewPostgresHistoryRepository(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()

	// 1. Untyped nil DB
	if _, err := telemetry.NewPostgresHistoryRepository(nil, cfg); err == nil {
		t.Fatal("expected error on untyped nil DB")
	}

	// 2. Typed nil DB
	var typedNil *fakeHistoryDatabase
	if _, err := telemetry.NewPostgresHistoryRepository(typedNil, cfg); err == nil {
		t.Fatal("expected error on typed nil DB")
	}

	// 3. Invalid config
	invalidCfg := cfg
	invalidCfg.QueryTimeout = 0
	db := &fakeHistoryDatabase{}
	if _, err := telemetry.NewPostgresHistoryRepository(db, invalidCfg); err == nil {
		t.Fatal("expected error on invalid config (QueryTimeout=0)")
	}

	invalidCfg = cfg
	invalidCfg.MinPoints = 50 // below lower limit 100
	if _, err := telemetry.NewPostgresHistoryRepository(db, invalidCfg); err == nil {
		t.Fatal("expected error on invalid config (MinPoints < 100)")
	}

	// 4. Valid constructor
	repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatalf("unexpected error creating repository: %v", err)
	}
	if repo == nil {
		t.Fatal("expected non-nil repository")
	}

	// Verify implements HistoryReader
	var _ telemetry.HistoryReader = repo
}

func TestReadTelemetryHistory_PreDBValidation(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	db := &fakeHistoryDatabase{}
	repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")

	// 1. Nil actor
	q := validRawQuery()
	if _, err := repo.ReadTelemetryHistory(ctx, uuid.Nil, q); !errors.Is(err, telemetry.ErrHistoryNilActor) {
		t.Fatalf("expected ErrHistoryNilActor, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls on nil actor, got %d", db.calls)
	}

	// 2. Invalid Gateway ID
	q = validRawQuery()
	q.GatewayID = "Invalid GW!"
	if _, err := repo.ReadTelemetryHistory(ctx, actor, q); !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
		t.Fatalf("expected ErrHistoryInvalidInput on invalid gateway ID, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls on invalid gateway ID, got %d", db.calls)
	}

	// 3. Invalid Sensor ID
	q = validRawQuery()
	q.SensorID = ""
	if _, err := repo.ReadTelemetryHistory(ctx, actor, q); !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
		t.Fatalf("expected ErrHistoryInvalidInput on empty sensor ID, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls on empty sensor ID, got %d", db.calls)
	}

	// 4. Unsupported resolution (ResolutionAuto and unknown resolution tokens)
	for _, res := range []telemetry.Resolution{telemetry.ResolutionAuto, "unknown", "2s"} {
		q = validRawQuery()
		q.Resolution = res
		if _, err := repo.ReadTelemetryHistory(ctx, actor, q); !errors.Is(err, telemetry.ErrHistoryInvalidResolution) {
			t.Fatalf("expected ErrHistoryInvalidResolution for unsupported resolution %q, got: %v", res, err)
		}
		if db.calls != 0 {
			t.Fatalf("expected 0 DB calls for resolution %q, got %d", res, db.calls)
		}
	}

	// 5. MaxPoints below bounds
	q = validRawQuery()
	q.MaxPoints = 50 // cfg.MinPoints is 100
	if _, err := repo.ReadTelemetryHistory(ctx, actor, q); !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
		t.Fatalf("expected ErrHistoryInvalidInput for MaxPoints below min, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls on MaxPoints below min, got %d", db.calls)
	}

	// 6. MaxPoints above bounds
	q = validRawQuery()
	q.MaxPoints = 3000 // cfg.MaxPoints is 2000
	if _, err := repo.ReadTelemetryHistory(ctx, actor, q); !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
		t.Fatalf("expected ErrHistoryInvalidInput for MaxPoints above max, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls on MaxPoints above max, got %d", db.calls)
	}

	// 7. Time range invalid: from not before to
	q = validRawQuery()
	q.Range.From = q.Range.To
	if _, err := repo.ReadTelemetryHistory(ctx, actor, q); !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
		t.Fatalf("expected ErrHistoryInvalidInput for from == to, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls on from == to, got %d", db.calls)
	}

	// 8. Time range invalid: nanoseconds not microsecond aligned
	q = validRawQuery()
	q.Range.From = q.Range.From.Add(500 * time.Nanosecond)
	if _, err := repo.ReadTelemetryHistory(ctx, actor, q); !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
		t.Fatalf("expected ErrHistoryInvalidInput for unaligned ns, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls on unaligned ns, got %d", db.calls)
	}

	// 9. Time range exceeds raw window (e.g. 11s > 10s raw window)
	q = validRawQuery()
	q.Range.To = q.Range.From.Add(11 * time.Second)
	if _, err := repo.ReadTelemetryHistory(ctx, actor, q); !errors.Is(err, telemetry.ErrHistoryRawLimitExceeded) {
		t.Fatalf("expected ErrHistoryRawLimitExceeded for raw window > 10s, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls on raw window > 10s, got %d", db.calls)
	}

	// 10. Pre-canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	q = validRawQuery()
	if _, err := repo.ReadTelemetryHistory(canceledCtx, actor, q); !errors.Is(err, telemetry.ErrHistoryDependency) || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ErrHistoryDependency wrapping context.Canceled, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls on pre-canceled context, got %d", db.calls)
	}
}

func TestReadTelemetryHistory_SQL_ScopesBindsOrderLimit(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	db := &fakeHistoryDatabase{
		rows: &fakeHistoryRows{}, // returns 0 rows
	}
	repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
	q := validRawQuery()
	q.MaxPoints = 500

	_, _ = repo.ReadTelemetryHistory(ctx, actor, q)

	if db.calls != 1 {
		t.Fatalf("expected 1 DB call, got %d", db.calls)
	}

	// Verify SQL query contains required CTE, joins, ordering and limit
	sql := db.lastSQL

	// 1. Authorized source CTE from sensors JOIN user_gateways
	if !strings.Contains(sql, "WITH authorized_sensor AS") {
		t.Errorf("SQL missing CTE 'authorized_sensor': %s", sql)
	}
	if !strings.Contains(sql, "FROM public.sensors") {
		t.Errorf("SQL missing 'FROM public.sensors': %s", sql)
	}
	if !strings.Contains(sql, "JOIN public.user_gateways") {
		t.Errorf("SQL missing 'JOIN public.user_gateways': %s", sql)
	}
	if !strings.Contains(sql, "user_id = $1") {
		t.Errorf("SQL missing user_id binding $1 in user_gateways join/where: %s", sql)
	}
	if !strings.Contains(sql, "gateway_id = $2") {
		t.Errorf("SQL missing gateway_id binding $2: %s", sql)
	}
	if !strings.Contains(sql, "sensor_id = $3") {
		t.Errorf("SQL missing sensor_id binding $3: %s", sql)
	}

	// 2. LEFT JOIN with raw telemetry on composite identity and time predicates
	if !strings.Contains(sql, "LEFT JOIN public.telemetry") {
		t.Errorf("SQL missing 'LEFT JOIN public.telemetry': %s", sql)
	}
	if !strings.Contains(sql, "measured_at >= $4") || !strings.Contains(sql, "measured_at < $5") {
		t.Errorf("SQL missing measured_at time predicates >= $4 and < $5: %s", sql)
	}

	// 3. ORDER BY measured_at, sample_id
	if !strings.Contains(sql, "ORDER BY") || !strings.Contains(sql, "measured_at ASC") || !strings.Contains(sql, "sample_id ASC") {
		t.Errorf("SQL missing required ORDER BY measured_at ASC, sample_id ASC: %s", sql)
	}

	// 4. LIMIT max_points + 1
	if !strings.Contains(sql, "LIMIT $6") {
		t.Errorf("SQL missing LIMIT $6: %s", sql)
	}

	// Verify parameter bindings
	if len(db.lastArgs) != 6 {
		t.Fatalf("expected 6 SQL arguments, got %d", len(db.lastArgs))
	}
	if db.lastArgs[0] != actor {
		t.Errorf("arg 0 (actor) = %v, want %v", db.lastArgs[0], actor)
	}
	if db.lastArgs[1] != q.GatewayID {
		t.Errorf("arg 1 (gateway_id) = %v, want %v", db.lastArgs[1], q.GatewayID)
	}
	if db.lastArgs[2] != q.SensorID {
		t.Errorf("arg 2 (sensor_id) = %v, want %v", db.lastArgs[2], q.SensorID)
	}
	if db.lastArgs[3] != q.Range.From.UTC() {
		t.Errorf("arg 3 (from) = %v, want %v", db.lastArgs[3], q.Range.From.UTC())
	}
	if db.lastArgs[4] != q.Range.To.UTC() {
		t.Errorf("arg 4 (to) = %v, want %v", db.lastArgs[4], q.Range.To.UTC())
	}
	if db.lastArgs[5] != 501 { // MaxPoints + 1
		t.Errorf("arg 5 (limit) = %v, want %v (MaxPoints+1)", db.lastArgs[5], 501)
	}
}

func TestReadTelemetryHistory_ZeroRows_NotFound(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	fakeRows := &fakeHistoryRows{records: nil} // 0 rows
	db := &fakeHistoryDatabase{rows: fakeRows}
	repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
	q := validRawQuery()

	res, err := repo.ReadTelemetryHistory(context.Background(), actor, q)
	if !errors.Is(err, telemetry.ErrHistoryNotFound) {
		t.Fatalf("expected ErrHistoryNotFound for zero source rows, got: %v", err)
	}
	if res != nil {
		t.Fatalf("expected nil result on not found, got: %#v", res)
	}
	if !fakeRows.closed {
		t.Fatal("expected rows.Close() to be called")
	}
}

func TestReadTelemetryHistory_AuthorizedEmpty_Sentinel(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	unit := "degC"
	// Sentinel row: gw, sensor, unit, all telemetry columns NULL
	sentinelRecord := func(dest ...any) error {
		*(dest[0].(*string)) = "gateway_001"
		*(dest[1].(*string)) = "sensor_001"
		*(dest[2].(**string)) = &unit
		*(dest[3].(**time.Time)) = nil
		*(dest[4].(**float64)) = nil
		*(dest[5].(**int64)) = nil
		*(dest[6].(**uuid.UUID)) = nil
		*(dest[7].(**uuid.UUID)) = nil
		*(dest[8].(**int32)) = nil
		return nil
	}
	fakeRows := &fakeHistoryRows{
		records: []func(dest ...any) error{sentinelRecord},
	}
	db := &fakeHistoryDatabase{rows: fakeRows}
	repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
	q := validRawQuery()

	res, err := repo.ReadTelemetryHistory(context.Background(), actor, q)
	if err != nil {
		t.Fatalf("unexpected error on authorized empty sentinel: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result on authorized empty sentinel")
	}
	if res.GatewayID != "gateway_001" || res.SensorID != "sensor_001" {
		t.Errorf("unexpected resource identity: %s/%s", res.GatewayID, res.SensorID)
	}
	if res.Unit == nil || *res.Unit != "degC" {
		t.Errorf("expected unit 'degC', got: %v", res.Unit)
	}
	if res.PointCount != 0 {
		t.Errorf("expected PointCount 0, got %d", res.PointCount)
	}
	if len(res.RawPoints) != 0 || res.RawPoints == nil {
		t.Errorf("expected empty non-nil RawPoints slice, got: %#v", res.RawPoints)
	}
	if res.Mode != "raw" || res.Resolution != "raw" {
		t.Errorf("expected mode=raw resolution=raw, got mode=%s res=%s", res.Mode, res.Resolution)
	}
	if !res.From.Equal(q.Range.From.UTC()) || !res.To.Equal(q.Range.To.UTC()) {
		t.Errorf("expected time range [%v, %v), got [%v, %v)", q.Range.From.UTC(), q.Range.To.UTC(), res.From, res.To)
	}
	if !fakeRows.closed {
		t.Fatal("expected rows.Close() to be called")
	}
}

func TestReadTelemetryHistory_UnitNULL(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	// Sentinel row where unit is SQL NULL
	sentinelRecord := func(dest ...any) error {
		*(dest[0].(*string)) = "gateway_001"
		*(dest[1].(*string)) = "sensor_001"
		*(dest[2].(**string)) = nil
		*(dest[3].(**time.Time)) = nil
		*(dest[4].(**float64)) = nil
		*(dest[5].(**int64)) = nil
		*(dest[6].(**uuid.UUID)) = nil
		*(dest[7].(**uuid.UUID)) = nil
		*(dest[8].(**int32)) = nil
		return nil
	}
	fakeRows := &fakeHistoryRows{
		records: []func(dest ...any) error{sentinelRecord},
	}
	db := &fakeHistoryDatabase{rows: fakeRows}
	repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
	q := validRawQuery()

	res, err := repo.ReadTelemetryHistory(context.Background(), actor, q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Unit != nil {
		t.Fatalf("expected nil Unit when SQL column is NULL, got: %v", *res.Unit)
	}
	if !fakeRows.closed {
		t.Fatal("expected rows.Close() to be called")
	}
}

func TestReadTelemetryHistory_ExactMaxPoints(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	baseTime := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
	unit := "degC"
	bootID := uuid.MustParse("7f2c45f7-0a76-4e28-8a37-7793d7d85b04")
	msgID := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")

	// Generate exactly 100 sample rows
	records := make([]func(dest ...any) error, 100)
	for i := 0; i < 100; i++ {
		idx := int32(i)
		seq := int64(1000 + i)
		val := float64(20 + i)
		mTime := baseTime.Add(time.Duration(i) * 10 * time.Millisecond)
		bID := bootID
		mID := msgID

		records[i] = func(dest ...any) error {
			*(dest[0].(*string)) = "gateway_001"
			*(dest[1].(*string)) = "sensor_001"
			*(dest[2].(**string)) = &unit
			*(dest[3].(**time.Time)) = &mTime
			*(dest[4].(**float64)) = &val
			*(dest[5].(**int64)) = &seq
			*(dest[6].(**uuid.UUID)) = &bID
			*(dest[7].(**uuid.UUID)) = &mID
			*(dest[8].(**int32)) = &idx
			return nil
		}
	}

	fakeRows := &fakeHistoryRows{records: records}
	db := &fakeHistoryDatabase{rows: fakeRows}
	repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
	q := validRawQuery()
	q.MaxPoints = 100

	res, err := repo.ReadTelemetryHistory(context.Background(), actor, q)
	if err != nil {
		t.Fatalf("unexpected error on exact max points: %v", err)
	}
	if res.PointCount != 100 {
		t.Fatalf("expected PointCount 100, got %d", res.PointCount)
	}
	if len(res.RawPoints) != 100 {
		t.Fatalf("expected 100 raw points, got %d", len(res.RawPoints))
	}
	if res.RawPoints[0].SequenceNum != 1000 || res.RawPoints[99].SequenceNum != 1099 {
		t.Errorf("sequence range mismatch: first=%d last=%d", res.RawPoints[0].SequenceNum, res.RawPoints[99].SequenceNum)
	}
	if !fakeRows.closed {
		t.Fatal("expected rows.Close() to be called")
	}
}

func TestReadTelemetryHistory_Overflow_MaxPlusOne(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	baseTime := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
	unit := "degC"
	bootID := uuid.MustParse("7f2c45f7-0a76-4e28-8a37-7793d7d85b04")
	msgID := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")

	// Generate 101 sample rows when max_points = 100
	records := make([]func(dest ...any) error, 101)
	for i := 0; i < 101; i++ {
		idx := int32(i)
		seq := int64(1000 + i)
		val := float64(20 + i)
		mTime := baseTime.Add(time.Duration(i) * 10 * time.Millisecond)
		bID := bootID
		mID := msgID

		records[i] = func(dest ...any) error {
			*(dest[0].(*string)) = "gateway_001"
			*(dest[1].(*string)) = "sensor_001"
			*(dest[2].(**string)) = &unit
			*(dest[3].(**time.Time)) = &mTime
			*(dest[4].(**float64)) = &val
			*(dest[5].(**int64)) = &seq
			*(dest[6].(**uuid.UUID)) = &bID
			*(dest[7].(**uuid.UUID)) = &mID
			*(dest[8].(**int32)) = &idx
			return nil
		}
	}

	fakeRows := &fakeHistoryRows{records: records}
	db := &fakeHistoryDatabase{rows: fakeRows}
	repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
	q := validRawQuery()
	q.MaxPoints = 100

	res, err := repo.ReadTelemetryHistory(context.Background(), actor, q)
	if !errors.Is(err, telemetry.ErrHistoryRawLimitExceeded) {
		t.Fatalf("expected ErrHistoryRawLimitExceeded on overflow, got: %v", err)
	}
	if res != nil {
		t.Fatalf("expected nil result on overflow (no partial successful truncation), got: %#v", res)
	}
	if !fakeRows.closed {
		t.Fatal("expected rows.Close() to be called on overflow")
	}
}

func TestReadTelemetryHistory_NullableLegacy_BigintOver2To53(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	baseTime := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
	unit := "degC"
	bigSeq := int64(1) << 54 // 18014398509481984 > 2^53 (9007199254740992)
	val := 42.5

	legacyRecord := func(dest ...any) error {
		*(dest[0].(*string)) = "gateway_001"
		*(dest[1].(*string)) = "sensor_001"
		*(dest[2].(**string)) = &unit
		*(dest[3].(**time.Time)) = &baseTime
		*(dest[4].(**float64)) = &val
		*(dest[5].(**int64)) = &bigSeq
		*(dest[6].(**uuid.UUID)) = nil // legacy NULL
		*(dest[7].(**uuid.UUID)) = nil // legacy NULL
		*(dest[8].(**int32)) = nil     // legacy NULL
		return nil
	}

	fakeRows := &fakeHistoryRows{records: []func(dest ...any) error{legacyRecord}}
	db := &fakeHistoryDatabase{rows: fakeRows}
	repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
	q := validRawQuery()

	res, err := repo.ReadTelemetryHistory(context.Background(), actor, q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.PointCount != 1 {
		t.Fatalf("expected 1 point, got %d", res.PointCount)
	}
	pt := res.RawPoints[0]
	if pt.SequenceNum != bigSeq {
		t.Errorf("SequenceNum precision lost: got %d, want %d", pt.SequenceNum, bigSeq)
	}
	if pt.BootID != nil {
		t.Errorf("expected nil BootID for legacy row, got %v", pt.BootID)
	}
	if pt.MessageID != nil {
		t.Errorf("expected nil MessageID for legacy row, got %v", pt.MessageID)
	}
	if pt.SampleIndex != nil {
		t.Errorf("expected nil SampleIndex for legacy row, got %v", pt.SampleIndex)
	}
	if !fakeRows.closed {
		t.Fatal("expected rows.Close() to be called")
	}
}

func TestReadTelemetryHistory_TwoBootsEqualTimestampsRetained(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	sameTime := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
	unit := "degC"
	boot1 := uuid.MustParse("7f2c45f7-0a76-4e28-8a37-7793d7d85b01")
	boot2 := uuid.MustParse("7f2c45f7-0a76-4e28-8a37-7793d7d85b02")
	seq := int64(10)
	val1 := 25.1
	val2 := 25.2
	idx := int32(0)

	r1 := func(dest ...any) error {
		*(dest[0].(*string)) = "gateway_001"
		*(dest[1].(*string)) = "sensor_001"
		*(dest[2].(**string)) = &unit
		*(dest[3].(**time.Time)) = &sameTime
		*(dest[4].(**float64)) = &val1
		*(dest[5].(**int64)) = &seq
		*(dest[6].(**uuid.UUID)) = &boot1
		*(dest[7].(**uuid.UUID)) = nil
		*(dest[8].(**int32)) = &idx
		return nil
	}
	r2 := func(dest ...any) error {
		*(dest[0].(*string)) = "gateway_001"
		*(dest[1].(*string)) = "sensor_001"
		*(dest[2].(**string)) = &unit
		*(dest[3].(**time.Time)) = &sameTime
		*(dest[4].(**float64)) = &val2
		*(dest[5].(**int64)) = &seq
		*(dest[6].(**uuid.UUID)) = &boot2
		*(dest[7].(**uuid.UUID)) = nil
		*(dest[8].(**int32)) = &idx
		return nil
	}

	fakeRows := &fakeHistoryRows{records: []func(dest ...any) error{r1, r2}}
	db := &fakeHistoryDatabase{rows: fakeRows}
	repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
	q := validRawQuery()

	res, err := repo.ReadTelemetryHistory(context.Background(), actor, q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.PointCount != 2 {
		t.Fatalf("expected both points retained, got %d", res.PointCount)
	}
	if *res.RawPoints[0].BootID != boot1 || *res.RawPoints[1].BootID != boot2 {
		t.Errorf("boot identity mismatch: %v vs %v", res.RawPoints[0].BootID, res.RawPoints[1].BootID)
	}
	if !fakeRows.closed {
		t.Fatal("expected rows.Close() to be called")
	}
}

func TestReadTelemetryHistory_Errors(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
	q := validRawQuery()

	// 1. Query error (connection closed) -> ErrHistoryDependency and cause
	connErr := pgconn.ErrConnClosed
	db := &fakeHistoryDatabase{queryErr: connErr}
	repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := repo.ReadTelemetryHistory(context.Background(), actor, q)
	if !errors.Is(err, telemetry.ErrHistoryDependency) || !errors.Is(err, connErr) {
		t.Fatalf("expected ErrHistoryDependency preserving connErr, got: %v", err)
	}
	if res != nil {
		t.Fatal("expected nil result on query error")
	}

	// 2. Query error (unexpected SQL error) -> ErrHistoryUnexpected and cause
	syntaxErr := errors.New("sql syntax error")
	db = &fakeHistoryDatabase{queryErr: syntaxErr}
	repo, err = telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err = repo.ReadTelemetryHistory(context.Background(), actor, q)
	if !errors.Is(err, telemetry.ErrHistoryUnexpected) || !errors.Is(err, syntaxErr) {
		t.Fatalf("expected ErrHistoryUnexpected preserving syntaxErr, got: %v", err)
	}
	if res != nil {
		t.Fatal("expected nil result on query error")
	}

	// 3. Scan error -> ErrHistoryUnexpected and cause, rows closed
	scanErr := errors.New("cannot scan column into type")
	fakeRows := &fakeHistoryRows{
		records: []func(dest ...any) error{func(_ ...any) error { return scanErr }},
		scanErr: scanErr,
	}
	db = &fakeHistoryDatabase{rows: fakeRows}
	repo, err = telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err = repo.ReadTelemetryHistory(context.Background(), actor, q)
	if !errors.Is(err, telemetry.ErrHistoryUnexpected) || !errors.Is(err, scanErr) {
		t.Fatalf("expected ErrHistoryUnexpected preserving scanErr, got: %v", err)
	}
	if res != nil {
		t.Fatal("expected nil result on scan error (no partial result)")
	}
	if !fakeRows.closed {
		t.Fatal("expected rows.Close() on scan error")
	}

	// 4. Iteration error (rows.Err()) -> ErrHistoryDependency / cause, rows closed
	fakeRows = &fakeHistoryRows{
		records: []func(dest ...any) error{},
		endErr:  connErr,
	}
	db = &fakeHistoryDatabase{rows: fakeRows}
	repo, err = telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err = repo.ReadTelemetryHistory(context.Background(), actor, q)
	if !errors.Is(err, telemetry.ErrHistoryDependency) || !errors.Is(err, connErr) {
		t.Fatalf("expected ErrHistoryDependency preserving connErr on rows.Err(), got: %v", err)
	}
	if res != nil {
		t.Fatal("expected nil result on iteration error")
	}
	if !fakeRows.closed {
		t.Fatal("expected rows.Close() on iteration error")
	}

	// 5. Context canceled during scan/iteration -> ErrHistoryDependency, rows closed
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancelRows := &fakeHistoryRows{
		records: []func(dest ...any) error{
			func(dest ...any) error {
				cancel() // cancel during scanning
				unit := "degC"
				now := time.Now().UTC()
				seq := int64(1)
				val := 1.0
				*(dest[0].(*string)) = "gateway_001"
				*(dest[1].(*string)) = "sensor_001"
				*(dest[2].(**string)) = &unit
				*(dest[3].(**time.Time)) = &now
				*(dest[4].(**float64)) = &val
				*(dest[5].(**int64)) = &seq
				*(dest[6].(**uuid.UUID)) = nil
				*(dest[7].(**uuid.UUID)) = nil
				*(dest[8].(**int32)) = nil
				return nil
			},
			func(_ ...any) error {
				return nil
			},
		},
	}
	db = &fakeHistoryDatabase{rows: cancelRows}
	repo, err = telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err = repo.ReadTelemetryHistory(cancelCtx, actor, q)
	if !errors.Is(err, telemetry.ErrHistoryDependency) || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ErrHistoryDependency wrapping context.Canceled, got: %v", err)
	}
	if res != nil {
		t.Fatal("expected nil result on context cancellation (no partial result)")
	}
	if !cancelRows.closed {
		t.Fatal("expected rows.Close() on cancellation")
	}

	// 6. Fake returns nil under cancellation: check ctx even if fake returns nil
	cancelCtx2, cancel2 := context.WithCancel(context.Background())
	cancel2() // pre-canceled but passed to query
	fakeSilentRows := &fakeHistoryRows{records: nil}
	db = &fakeHistoryDatabase{rows: fakeSilentRows}
	repo, err = telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err = repo.ReadTelemetryHistory(cancelCtx2, actor, q)
	if !errors.Is(err, telemetry.ErrHistoryDependency) || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ErrHistoryDependency wrapping context.Canceled when fake returns nil, got: %v", err)
	}
	if res != nil {
		t.Fatal("expected nil result on canceled context")
	}
}

func makeFakeBucketRow(gwID, sID string, unit *string, bucketStart *time.Time, avg, min, max *float64, count *int64) func(dest ...any) error {
	return func(dest ...any) error {
		*(dest[0].(*string)) = gwID
		*(dest[1].(*string)) = sID
		*(dest[2].(**string)) = unit
		*(dest[3].(**time.Time)) = bucketStart
		*(dest[4].(**float64)) = avg
		*(dest[5].(**float64)) = min
		*(dest[6].(**float64)) = max
		*(dest[7].(**int64)) = count
		return nil
	}
}

func TestPostgresHistoryRepository_Bucket_AllowedIntervalBindings(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf1")

	testCases := []struct {
		res            telemetry.Resolution
		expectedMicros int64
	}{
		{telemetry.Resolution100ms, 100 * 1000},
		{telemetry.Resolution1s, 1 * 1000 * 1000},
		{telemetry.Resolution10s, 10 * 1000 * 1000},
		{telemetry.Resolution1m, 60 * 1000 * 1000},
		{telemetry.Resolution5m, 300 * 1000 * 1000},
		{telemetry.Resolution15m, 900 * 1000 * 1000},
		{telemetry.Resolution1h, 3600 * 1000 * 1000},
		{telemetry.Resolution6h, 21600 * 1000 * 1000},
		{telemetry.Resolution1d, 86400 * 1000 * 1000},
	}

	for _, tc := range testCases {
		t.Run(string(tc.res), func(t *testing.T) {
			dur, ok := telemetry.ResolutionDuration(tc.res)
			if !ok {
				t.Fatalf("expected ResolutionDuration to be ok for %s", tc.res)
			}
			from := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
			to := from.Add(dur)

			q := telemetry.TelemetryHistoryQuery{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				Range:      telemetry.TimeRange{From: from, To: to},
				Resolution: tc.res,
				MaxPoints:  100,
			}

			unit := "degC"
			bucketStart := from
			avg := 25.5
			min := 20.0
			max := 30.0
			count := int64(10)

			rows := &fakeHistoryRows{
				records: []func(dest ...any) error{
					makeFakeBucketRow(q.GatewayID, q.SensorID, &unit, &bucketStart, &avg, &min, &max, &count),
				},
			}
			db := &fakeHistoryDatabase{rows: rows}
			repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
			if err != nil {
				t.Fatal(err)
			}

			res, err := repo.ReadTelemetryHistory(context.Background(), actor, q)
			if err != nil {
				t.Fatalf("unexpected error for resolution %s: %v", tc.res, err)
			}
			if res == nil {
				t.Fatal("expected non-nil result")
			}

			if db.calls != 1 {
				t.Fatalf("expected 1 DB call, got %d", db.calls)
			}
			if len(db.lastArgs) != 7 {
				t.Fatalf("expected 7 query args, got %d", len(db.lastArgs))
			}

			// Arg 0: actor ($1)
			if db.lastArgs[0] != actor {
				t.Errorf("arg 0 (actor) = %v, want %v", db.lastArgs[0], actor)
			}
			// Arg 1: gateway_id ($2)
			if db.lastArgs[1] != q.GatewayID {
				t.Errorf("arg 1 (gateway_id) = %v, want %v", db.lastArgs[1], q.GatewayID)
			}
			// Arg 2: sensor_id ($3)
			if db.lastArgs[2] != q.SensorID {
				t.Errorf("arg 2 (sensor_id) = %v, want %v", db.lastArgs[2], q.SensorID)
			}
			// Arg 3: interval ($4)
			interval, ok := db.lastArgs[3].(pgtype.Interval)
			if !ok {
				t.Fatalf("arg 3 (interval) is type %T, want pgtype.Interval", db.lastArgs[3])
			}
			if !interval.Valid {
				t.Errorf("arg 3 (interval) Valid = false, want true")
			}
			if interval.Microseconds != tc.expectedMicros {
				t.Errorf("arg 3 (interval) Microseconds = %d, want %d", interval.Microseconds, tc.expectedMicros)
			}
			// Arg 4: from ($5)
			if db.lastArgs[4] != from.UTC() {
				t.Errorf("arg 4 (from) = %v, want %v", db.lastArgs[4], from.UTC())
			}
			// Arg 5: to ($6)
			if db.lastArgs[5] != to.UTC() {
				t.Errorf("arg 5 (to) = %v, want %v", db.lastArgs[5], to.UTC())
			}
			// Arg 6: limit ($7)
			if db.lastArgs[6] != q.MaxPoints+1 {
				t.Errorf("arg 6 (limit) = %v, want %v", db.lastArgs[6], q.MaxPoints+1)
			}

			// SQL query assertions
			if !strings.Contains(db.lastSQL, "time_bucket($4::interval, t.measured_at, TIMESTAMPTZ '1970-01-01 00:00:00+00')") {
				t.Errorf("SQL missing explicit epoch origin time_bucket: %s", db.lastSQL)
			}
			if !strings.Contains(db.lastSQL, "t.measured_at >= $5") || !strings.Contains(db.lastSQL, "t.measured_at < $6") {
				t.Errorf("SQL missing direct measured_at clipping predicates: %s", db.lastSQL)
			}
			if !strings.Contains(db.lastSQL, "ORDER BY b.bucket_start ASC") {
				t.Errorf("SQL missing ORDER BY b.bucket_start ASC: %s", db.lastSQL)
			}
			if !strings.Contains(db.lastSQL, "LIMIT $7") {
				t.Errorf("SQL missing LIMIT $7: %s", db.lastSQL)
			}

			// Result assertions
			if res.GatewayID != q.GatewayID {
				t.Errorf("res.GatewayID = %s, want %s", res.GatewayID, q.GatewayID)
			}
			if res.SensorID != q.SensorID {
				t.Errorf("res.SensorID = %s, want %s", res.SensorID, q.SensorID)
			}
			if res.Unit == nil || *res.Unit != unit {
				t.Errorf("res.Unit = %v, want %s", res.Unit, unit)
			}
			if res.Mode != "bucket" {
				t.Errorf("res.Mode = %s, want bucket", res.Mode)
			}
			if res.Resolution != string(tc.res) {
				t.Errorf("res.Resolution = %s, want %s", res.Resolution, tc.res)
			}
			if res.PointCount != 1 {
				t.Errorf("res.PointCount = %d, want 1", res.PointCount)
			}
			if len(res.BucketPoints) != 1 {
				t.Fatalf("len(res.BucketPoints) = %d, want 1", len(res.BucketPoints))
			}
			if res.RawPoints != nil {
				t.Errorf("res.RawPoints should be nil for bucket queries, got %v", res.RawPoints)
			}
			bp := res.BucketPoints[0]
			if !bp.BucketStart.Equal(bucketStart) {
				t.Errorf("bp.BucketStart = %v, want %v", bp.BucketStart, bucketStart)
			}
			if bp.Avg != avg || bp.Min != min || bp.Max != max || bp.Count != count {
				t.Errorf("bp values mismatch: got avg=%f min=%f max=%f count=%d", bp.Avg, bp.Min, bp.Max, bp.Count)
			}
		})
	}
}

func TestPostgresHistoryRepository_Bucket_Validation_NoDBCalls(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf1")
	ctx := context.Background()
	from := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)

	// 1. Resolution too fine (e.g. 100ms across 1 hour with maxPoints=100 -> ~36000 buckets > 100)
	{
		db := &fakeHistoryDatabase{}
		repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
		if err != nil {
			t.Fatal(err)
		}
		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: from, To: from.Add(time.Hour)},
			Resolution: telemetry.Resolution100ms,
			MaxPoints:  100,
		}
		_, err = repo.ReadTelemetryHistory(ctx, actor, q)
		if !errors.Is(err, telemetry.ErrHistoryResolutionTooFine) {
			t.Fatalf("expected ErrHistoryResolutionTooFine, got: %v", err)
		}
		if db.calls != 0 {
			t.Fatalf("expected 0 DB calls on resolution too fine, got %d", db.calls)
		}
	}

	// 2. Unknown resolution token -> ErrHistoryInvalidResolution, 0 DB calls
	{
		db := &fakeHistoryDatabase{}
		repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
		if err != nil {
			t.Fatal(err)
		}
		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: from, To: from.Add(time.Minute)},
			Resolution: telemetry.Resolution("2s"),
			MaxPoints:  100,
		}
		_, err = repo.ReadTelemetryHistory(ctx, actor, q)
		if !errors.Is(err, telemetry.ErrHistoryInvalidResolution) {
			t.Fatalf("expected ErrHistoryInvalidResolution for 2s, got: %v", err)
		}
		if db.calls != 0 {
			t.Fatalf("expected 0 DB calls on unknown resolution, got %d", db.calls)
		}
	}

	// 3. ResolutionAuto -> ErrHistoryInvalidResolution, 0 DB calls (auto belongs to service layer)
	{
		db := &fakeHistoryDatabase{}
		repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
		if err != nil {
			t.Fatal(err)
		}
		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: from, To: from.Add(time.Minute)},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		_, err = repo.ReadTelemetryHistory(ctx, actor, q)
		if !errors.Is(err, telemetry.ErrHistoryInvalidResolution) {
			t.Fatalf("expected ErrHistoryInvalidResolution for auto, got: %v", err)
		}
		if db.calls != 0 {
			t.Fatalf("expected 0 DB calls on auto resolution, got %d", db.calls)
		}
	}

	// 4. Bucket resolution over window > 10s does NOT fail with ErrHistoryRawLimitExceeded
	{
		// 5 days of 1d resolution is 5 buckets <= maxPoints 100.
		db := &fakeHistoryDatabase{}
		repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
		if err != nil {
			t.Fatal(err)
		}
		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: from, To: from.Add(5 * 24 * time.Hour)},
			Resolution: telemetry.Resolution1d,
			MaxPoints:  100,
		}
		// Empty response from DB to test validation passed and DB was called
		unit := "degC"
		rows := &fakeHistoryRows{
			records: []func(dest ...any) error{
				makeFakeBucketRow(q.GatewayID, q.SensorID, &unit, nil, nil, nil, nil, nil),
			},
		}
		db.rows = rows
		res, err := repo.ReadTelemetryHistory(ctx, actor, q)
		if err != nil {
			t.Fatalf("unexpected error for 5d bucket query: %v", err)
		}
		if res == nil || res.PointCount != 0 {
			t.Fatalf("expected empty bucket result, got %v", res)
		}
		if db.calls != 1 {
			t.Fatalf("expected 1 DB call for valid 5d bucket query, got %d", db.calls)
		}
	}

	// 5. Window exceeds MaxRange (744h) -> ErrHistoryRangeTooLarge, 0 DB calls
	{
		db := &fakeHistoryDatabase{}
		repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
		if err != nil {
			t.Fatal(err)
		}
		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: from, To: from.Add(745 * time.Hour)},
			Resolution: telemetry.Resolution1d,
			MaxPoints:  100,
		}
		_, err = repo.ReadTelemetryHistory(ctx, actor, q)
		if !errors.Is(err, telemetry.ErrHistoryRangeTooLarge) {
			t.Fatalf("expected ErrHistoryRangeTooLarge, got: %v", err)
		}
		if db.calls != 0 {
			t.Fatalf("expected 0 DB calls on range too large, got %d", db.calls)
		}
	}

	// 6. Non-microsecond aligned timestamps -> ErrHistoryInvalidInput, 0 DB calls
	{
		db := &fakeHistoryDatabase{}
		repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
		if err != nil {
			t.Fatal(err)
		}
		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: from.Add(123 * time.Nanosecond), To: from.Add(time.Hour)},
			Resolution: telemetry.Resolution1h,
			MaxPoints:  100,
		}
		_, err = repo.ReadTelemetryHistory(ctx, actor, q)
		if !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
			t.Fatalf("expected ErrHistoryInvalidInput for unaligned ns, got: %v", err)
		}
		if db.calls != 0 {
			t.Fatalf("expected 0 DB calls on unaligned ns, got %d", db.calls)
		}
	}
}

func TestPostgresHistoryRepository_Bucket_AuthorizedEmptyVsMissing(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf1")
	ctx := context.Background()
	from := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	to := from.Add(10 * time.Minute)

	q := telemetry.TelemetryHistoryQuery{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		Range:      telemetry.TimeRange{From: from, To: to},
		Resolution: telemetry.Resolution1m,
		MaxPoints:  100,
	}

	// 1. Missing resource or unauthorized: 0 rows returned from DB -> ErrHistoryNotFound
	{
		rows := &fakeHistoryRows{records: []func(dest ...any) error{}}
		db := &fakeHistoryDatabase{rows: rows}
		repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
		if err != nil {
			t.Fatal(err)
		}
		res, err := repo.ReadTelemetryHistory(ctx, actor, q)
		if !errors.Is(err, telemetry.ErrHistoryNotFound) {
			t.Fatalf("expected ErrHistoryNotFound for 0 source rows, got: %v", err)
		}
		if res != nil {
			t.Fatal("expected nil result on not found")
		}
		if !rows.closed {
			t.Fatal("expected rows.Close() on not found")
		}
	}

	// 2. Authorized empty: 1 sentinel row where bucket_start is NULL -> success with 0 points
	{
		unit := "m/s"
		rows := &fakeHistoryRows{
			records: []func(dest ...any) error{
				makeFakeBucketRow(q.GatewayID, q.SensorID, &unit, nil, nil, nil, nil, nil),
			},
		}
		db := &fakeHistoryDatabase{rows: rows}
		repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
		if err != nil {
			t.Fatal(err)
		}
		res, err := repo.ReadTelemetryHistory(ctx, actor, q)
		if err != nil {
			t.Fatalf("unexpected error on authorized empty: %v", err)
		}
		if res == nil {
			t.Fatal("expected non-nil result on authorized empty")
		}
		if res.GatewayID != q.GatewayID || res.SensorID != q.SensorID {
			t.Errorf("identity mismatch: gw=%s sens=%s", res.GatewayID, res.SensorID)
		}
		if res.Unit == nil || *res.Unit != unit {
			t.Errorf("unit mismatch: got %v, want %s", res.Unit, unit)
		}
		if res.Mode != "bucket" || res.Resolution != "1m" {
			t.Errorf("mode/resolution mismatch: mode=%s res=%s", res.Mode, res.Resolution)
		}
		if res.PointCount != 0 {
			t.Errorf("res.PointCount = %d, want 0", res.PointCount)
		}
		if len(res.BucketPoints) != 0 {
			t.Errorf("len(res.BucketPoints) = %d, want 0", len(res.BucketPoints))
		}
		if res.RawPoints != nil {
			t.Errorf("res.RawPoints should be nil, got %v", res.RawPoints)
		}
		if !rows.closed {
			t.Fatal("expected rows.Close() on authorized empty")
		}
	}

	// 3. Authorized empty with NULL unit on sensor
	{
		rows := &fakeHistoryRows{
			records: []func(dest ...any) error{
				makeFakeBucketRow(q.GatewayID, q.SensorID, nil, nil, nil, nil, nil, nil),
			},
		}
		db := &fakeHistoryDatabase{rows: rows}
		repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
		if err != nil {
			t.Fatal(err)
		}
		res, err := repo.ReadTelemetryHistory(ctx, actor, q)
		if err != nil {
			t.Fatalf("unexpected error on authorized empty with null unit: %v", err)
		}
		if res.Unit != nil {
			t.Errorf("expected nil Unit, got %v", res.Unit)
		}
		if res.PointCount != 0 {
			t.Errorf("res.PointCount = %d, want 0", res.PointCount)
		}
	}
}

func TestPostgresHistoryRepository_Bucket_MultipleBucketsAndCounts(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf1")
	ctx := context.Background()
	from := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	to := from.Add(3 * time.Second)

	q := telemetry.TelemetryHistoryQuery{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		Range:      telemetry.TimeRange{From: from, To: to},
		Resolution: telemetry.Resolution1s,
		MaxPoints:  100,
	}

	unit := "Pa"
	t0 := from
	t1 := from.Add(time.Second)
	t2 := from.Add(2 * time.Second)

	avg0, min0, max0, cnt0 := 101.3, 100.0, 102.5, int64(5)
	avg1, min1, max1, cnt1 := 103.0, 103.0, 103.0, int64(1)
	avg2, min2, max2, cnt2 := 99.8, 98.0, 101.0, int64(10)

	rows := &fakeHistoryRows{
		records: []func(dest ...any) error{
			makeFakeBucketRow(q.GatewayID, q.SensorID, &unit, &t0, &avg0, &min0, &max0, &cnt0),
			makeFakeBucketRow(q.GatewayID, q.SensorID, &unit, &t1, &avg1, &min1, &max1, &cnt1),
			makeFakeBucketRow(q.GatewayID, q.SensorID, &unit, &t2, &avg2, &min2, &max2, &cnt2),
		},
	}
	db := &fakeHistoryDatabase{rows: rows}
	repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	res, err := repo.ReadTelemetryHistory(ctx, actor, q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.PointCount != 3 {
		t.Fatalf("res.PointCount = %d, want 3", res.PointCount)
	}
	if len(res.BucketPoints) != 3 {
		t.Fatalf("len(res.BucketPoints) = %d, want 3", len(res.BucketPoints))
	}

	// Verify each point
	pts := res.BucketPoints
	if !pts[0].BucketStart.Equal(t0) || pts[0].Avg != avg0 || pts[0].Min != min0 || pts[0].Max != max0 || pts[0].Count != cnt0 {
		t.Errorf("bucket 0 mismatch: %+v", pts[0])
	}
	if !pts[1].BucketStart.Equal(t1) || pts[1].Avg != avg1 || pts[1].Min != min1 || pts[1].Max != max1 || pts[1].Count != cnt1 {
		t.Errorf("bucket 1 mismatch: %+v", pts[1])
	}
	if !pts[2].BucketStart.Equal(t2) || pts[2].Avg != avg2 || pts[2].Min != min2 || pts[2].Max != max2 || pts[2].Count != cnt2 {
		t.Errorf("bucket 2 mismatch: %+v", pts[2])
	}
}

func TestPostgresHistoryRepository_Bucket_CapOverflow(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf1")
	ctx := context.Background()
	from := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	to := from.Add(100 * time.Second)

	q := telemetry.TelemetryHistoryQuery{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		Range:      telemetry.TimeRange{From: from, To: to},
		Resolution: telemetry.Resolution1s,
		MaxPoints:  100, // 100 maxPoints, so limit is 101
	}

	unit := "degC"
	// Generate 101 rows (maxPoints + 1)
	records := make([]func(dest ...any) error, 101)
	for i := 0; i < 101; i++ {
		tBucket := from.Add(time.Duration(i) * time.Second)
		avg := float64(i)
		min := float64(i)
		max := float64(i)
		cnt := int64(1)
		records[i] = makeFakeBucketRow(q.GatewayID, q.SensorID, &unit, &tBucket, &avg, &min, &max, &cnt)
	}

	rows := &fakeHistoryRows{records: records}
	db := &fakeHistoryDatabase{rows: rows}
	repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	res, err := repo.ReadTelemetryHistory(ctx, actor, q)
	if !errors.Is(err, telemetry.ErrHistoryResolutionTooFine) {
		t.Fatalf("expected ErrHistoryResolutionTooFine on cap overflow, got: %v", err)
	}
	if res != nil {
		t.Fatal("expected nil result on cap overflow (no truncated success)")
	}
	if !rows.closed {
		t.Fatal("expected rows.Close() on cap overflow")
	}
}

func TestPostgresHistoryRepository_Bucket_LifecycleAndErrors(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	actor := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf1")
	from := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)

	q := telemetry.TelemetryHistoryQuery{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		Range:      telemetry.TimeRange{From: from, To: to},
		Resolution: telemetry.Resolution1s,
		MaxPoints:  100,
	}

	connErr := &pgconn.PgError{Code: "08006", Message: "connection failure"}
	syntaxErr := &pgconn.PgError{Code: "42601", Message: "syntax error"}
	scanErr := errors.New("scan failure")

	// 1. Query error (connection failure) -> ErrHistoryDependency
	{
		db := &fakeHistoryDatabase{queryErr: connErr}
		repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
		if err != nil {
			t.Fatal(err)
		}
		res, err := repo.ReadTelemetryHistory(context.Background(), actor, q)
		if !errors.Is(err, telemetry.ErrHistoryDependency) || !errors.Is(err, connErr) {
			t.Fatalf("expected ErrHistoryDependency preserving connErr, got: %v", err)
		}
		if res != nil {
			t.Fatal("expected nil result on query error")
		}
	}

	// 2. Query error (syntax error) -> ErrHistoryUnexpected
	{
		db := &fakeHistoryDatabase{queryErr: syntaxErr}
		repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
		if err != nil {
			t.Fatal(err)
		}
		res, err := repo.ReadTelemetryHistory(context.Background(), actor, q)
		if !errors.Is(err, telemetry.ErrHistoryUnexpected) || !errors.Is(err, syntaxErr) {
			t.Fatalf("expected ErrHistoryUnexpected preserving syntaxErr, got: %v", err)
		}
		if res != nil {
			t.Fatal("expected nil result on query error")
		}
	}

	// 3. Scan error -> ErrHistoryUnexpected, rows closed, nil result
	{
		rows := &fakeHistoryRows{records: []func(dest ...any) error{func(_ ...any) error { return nil }}, scanErr: scanErr}
		db := &fakeHistoryDatabase{rows: rows}
		repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
		if err != nil {
			t.Fatal(err)
		}
		res, err := repo.ReadTelemetryHistory(context.Background(), actor, q)
		if !errors.Is(err, telemetry.ErrHistoryUnexpected) || !errors.Is(err, scanErr) {
			t.Fatalf("expected ErrHistoryUnexpected preserving scanErr, got: %v", err)
		}
		if res != nil {
			t.Fatal("expected nil result on scan error")
		}
		if !rows.closed {
			t.Fatal("expected rows.Close() on scan error")
		}
	}

	// 4. Rows.Err() -> ErrHistoryDependency, rows closed
	{
		rows := &fakeHistoryRows{records: []func(dest ...any) error{}, endErr: connErr}
		db := &fakeHistoryDatabase{rows: rows}
		repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
		if err != nil {
			t.Fatal(err)
		}
		res, err := repo.ReadTelemetryHistory(context.Background(), actor, q)
		if !errors.Is(err, telemetry.ErrHistoryDependency) || !errors.Is(err, connErr) {
			t.Fatalf("expected ErrHistoryDependency preserving connErr, got: %v", err)
		}
		if res != nil {
			t.Fatal("expected nil result on rows.Err()")
		}
		if !rows.closed {
			t.Fatal("expected rows.Close() on rows.Err()")
		}
	}

	// 5. Incomplete row (bucket_start != nil but avg == nil) -> ErrHistoryUnexpected
	{
		unit := "degC"
		tBucket := from
		rows := &fakeHistoryRows{
			records: []func(dest ...any) error{
				makeFakeBucketRow(q.GatewayID, q.SensorID, &unit, &tBucket, nil, nil, nil, nil),
			},
		}
		db := &fakeHistoryDatabase{rows: rows}
		repo, err := telemetry.NewPostgresHistoryRepository(db, cfg)
		if err != nil {
			t.Fatal(err)
		}
		res, err := repo.ReadTelemetryHistory(context.Background(), actor, q)
		if !errors.Is(err, telemetry.ErrHistoryUnexpected) {
			t.Fatalf("expected ErrHistoryUnexpected on incomplete row, got: %v", err)
		}
		if res != nil {
			t.Fatal("expected nil result on incomplete row")
		}
		if !rows.closed {
			t.Fatal("expected rows.Close() on incomplete row")
		}
	}
}

package digitaltwin

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

func makeTestSensorEntity() ResolvedEntity {
	return ResolvedEntity{
		InternalID: uuid.MustParse("00000000-0000-0000-0000-0000000000aa"),
		PublicURN:  "urn:ngsi-ld:Sensor:gw_01:sensor_01",
		Type:       EntityTypeSensor,
		GatewayID:  "gw_01",
		SensorID:   "sensor_01",
	}
}

func makeTestRawQuery(from, to time.Time, maxPoints int) TwinHistoryQuery {
	return TwinHistoryQuery{
		EntityID: "urn:ngsi-ld:Sensor:gw_01:sensor_01",
		Property: SelectorSampleIntervalUS,
		Range: telemetry.TimeRange{
			From: from,
			To:   to,
		},
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  maxPoints,
	}
}

func makeBucketRecord(
	entity ResolvedEntity,
	bucketStart *time.Time,
	avg, min, max *float64,
	count *int64,
	summary *TemporalWindowSummary,
) func(dest ...any) error {
	return func(dest ...any) error {
		*dest[0].(*uuid.UUID) = entity.InternalID
		*dest[1].(*string) = entity.PublicURN
		*dest[2].(**time.Time) = bucketStart
		*dest[3].(**float64) = avg
		*dest[4].(**float64) = min
		*dest[5].(**float64) = max
		*dest[6].(**int64) = count
		if summary != nil {
			*dest[7].(*int64) = summary.TotalCount
			*dest[8].(*int64) = summary.NumberCount
			*dest[9].(*int64) = summary.TextCount
			*dest[10].(*int64) = summary.BooleanCount
			*dest[11].(*int64) = summary.InvalidTypeCount
			*dest[12].(*int64) = summary.InvalidIntervalCount
			*dest[13].(*int64) = summary.UnknownUnitCount
			*dest[14].(*int64) = summary.MalformedUnitCount
			*dest[15].(*int64) = summary.DistinctUnitCount
			*dest[16].(**string) = summary.MinUnit
		}
		return nil
	}
}

func TestReadTemporalHistory_MissingVsEmpty(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	q := makeTestRawQuery(from, to, 100)

	t.Run("source missing or unauthorized returns ErrNotFound", func(t *testing.T) {
		// 0 rows returned by DB because authorized_sensor CTE produced 0 rows
		fakeRows := &fakeTwinRows{records: nil}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, err := NewPostgresTwinReader(db)
		if err != nil {
			t.Fatal(err)
		}

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound for missing/unauthorized source, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on ErrNotFound, got %+v", res)
		}
		if !fakeRows.closed {
			t.Fatal("expected rows to be closed")
		}
	})

	t.Run("source authorized with zero observations returns valid empty series", func(t *testing.T) {
		// 1 row returned where v.observed_at is NULL (outer-join sentinel)
		sentinelRecord := func(dest ...any) error {
			// dest: &srcUUID, &srcEntityID, &observedAt, &valueNumber, &valueText, &valueBoolean, &metadataRaw, &messageID
			*dest[0].(*uuid.UUID) = entity.InternalID
			*dest[1].(*string) = entity.PublicURN
			*dest[2].(**time.Time) = nil
			*dest[3].(**float64) = nil
			*dest[4].(**string) = nil
			*dest[5].(**bool) = nil
			*dest[6].(*[]byte) = nil
			*dest[7].(**uuid.UUID) = nil
			return nil
		}

		fakeRows := &fakeTwinRows{
			records: []func(dest ...any) error{sentinelRecord},
		}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, err := NewPostgresTwinReader(db)
		if err != nil {
			t.Fatal(err)
		}

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if err != nil {
			t.Fatalf("expected nil error for authorized empty series, got %v", err)
		}
		if res == nil {
			t.Fatal("expected non-nil result for authorized empty series")
		}
		if res.PointCount != 0 {
			t.Fatalf("expected PointCount 0, got %d", res.PointCount)
		}
		if len(res.TemporalPoints) != 0 {
			t.Fatalf("expected empty TemporalPoints, got %d", len(res.TemporalPoints))
		}
		if res.Unit != nil {
			t.Fatalf("expected nil Unit for empty series, got %v", *res.Unit)
		}
		if res.Mode != "raw" || res.Resolution != "raw" {
			t.Fatalf("expected mode=raw, resolution=raw, got mode=%s, resolution=%s", res.Mode, res.Resolution)
		}
		if !res.From.Equal(from) || !res.To.Equal(to) {
			t.Fatalf("expected range [%v, %v), got [%v, %v)", from, to, res.From, res.To)
		}
		if !fakeRows.closed {
			t.Fatal("expected rows to be closed")
		}
	})
}

func TestReadTemporalHistory_QueryMembershipAndGraphScope(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	q := makeTestRawQuery(from, to, 500)

	fakeRows := &fakeTwinRows{records: nil}
	db := &fakeTwinDatabase{rows: fakeRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = reader.ReadTemporalHistory(context.Background(), actor, entity, q)

	sql := db.lastQuery
	// Verify CTE authorized_sensor joins
	if !strings.Contains(sql, "WITH authorized_sensor AS (") {
		t.Errorf("query missing authorized_sensor CTE: %s", sql)
	}
	if !strings.Contains(sql, "JOIN public.sensors AS sens") {
		t.Errorf("query missing sensors join: %s", sql)
	}
	if !strings.Contains(sql, "sens.gateway_id = e.gateway_id AND sens.sensor_id = $1") {
		t.Errorf("query missing same-gateway sensor join predicate: %s", sql)
	}
	if !strings.Contains(sql, "parent_e.entity_type = 'Gateway'") {
		t.Errorf("query missing Gateway parent predicate: %s", sql)
	}
	if !strings.Contains(sql, "r.relationship_type = 'hasSensor'") {
		t.Errorf("query missing hasSensor relationship predicate: %s", sql)
	}
	if !strings.Contains(sql, "JOIN public.user_gateways AS ug") || !strings.Contains(sql, "ug.gateway_id = g.gateway_id AND ug.user_id = $2") {
		t.Errorf("query missing actor user_gateways membership check: %s", sql)
	}
	if !strings.Contains(sql, "e.entity_type = 'Sensor'") {
		t.Errorf("query missing Sensor entity_type check: %s", sql)
	}
	if !strings.Contains(sql, "parent_e.entity_id = 'urn:ngsi-ld:Gateway:' || e.gateway_id") {
		t.Errorf("query missing canonical parent Gateway URN predicate: %s", sql)
	}
	if !strings.Contains(sql, "e.entity_id = 'urn:ngsi-ld:Sensor:' || sens.gateway_id || ':' || sens.sensor_id") {
		t.Errorf("query missing canonical Sensor URN predicate: %s", sql)
	}
	if !strings.Contains(sql, "window_summary AS (") {
		t.Errorf("query missing window_summary CTE: %s", sql)
	}
	if !strings.Contains(sql, "bounded_observations AS (") {
		t.Errorf("query missing bounded_observations CTE: %s", sql)
	}

	// Verify parameterized arguments
	if len(db.lastArgs) != 9 {
		t.Fatalf("expected 9 arguments, got %d: %v", len(db.lastArgs), db.lastArgs)
	}
	if db.lastArgs[0] != entity.SensorID {
		t.Errorf("arg[0] (sensor_id) = %v, want %s", db.lastArgs[0], entity.SensorID)
	}
	if db.lastArgs[1] != actor {
		t.Errorf("arg[1] (actor) = %v, want %s", db.lastArgs[1], actor)
	}
	if db.lastArgs[2] != entity.InternalID {
		t.Errorf("arg[2] (internal_id) = %v, want %s", db.lastArgs[2], entity.InternalID)
	}
	if db.lastArgs[3] != entity.PublicURN {
		t.Errorf("arg[3] (public_urn) = %v, want %s", db.lastArgs[3], entity.PublicURN)
	}
	if db.lastArgs[4] != entity.GatewayID {
		t.Errorf("arg[4] (gateway_id) = %v, want %s", db.lastArgs[4], entity.GatewayID)
	}
	if db.lastArgs[5] != "sample_interval_us" {
		t.Errorf("arg[5] (property_name) = %v, want sample_interval_us", db.lastArgs[5])
	}
	if db.lastArgs[6] != from.UTC() {
		t.Errorf("arg[6] (from) = %v, want %v", db.lastArgs[6], from.UTC())
	}
	if db.lastArgs[7] != to.UTC() {
		t.Errorf("arg[7] (to) = %v, want %v", db.lastArgs[7], to.UTC())
	}
	if db.lastArgs[8] != 501 {
		t.Errorf("arg[8] (limit) = %v, want 501 (maxPoints+1)", db.lastArgs[8])
	}
}

func TestReadTemporalHistory_TimeBoundsAndHalfOpen(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	q := makeTestRawQuery(from, to, 100)

	fakeRows := &fakeTwinRows{records: nil}
	db := &fakeTwinDatabase{rows: fakeRows}
	reader, _ := NewPostgresTwinReader(db)

	_, _ = reader.ReadTemporalHistory(context.Background(), actor, entity, q)

	sql := db.lastQuery
	if !strings.Contains(sql, "v.observed_at >= $7") || !strings.Contains(sql, "v.observed_at < $8") {
		t.Errorf("query missing half-open [from, to) filtering: %s", sql)
	}
}

func TestReadTemporalHistory_TiesPreserved(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	q := makeTestRawQuery(from, to, 100)

	tiedTime := from.Add(1 * time.Second)
	val1 := 10000.0
	val2 := 20000.0
	msgID1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	msgID2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	meta := []byte(`{"unit":"us"}`)

	rec1 := func(dest ...any) error {
		*dest[0].(*uuid.UUID) = entity.InternalID
		*dest[1].(*string) = entity.PublicURN
		*dest[2].(**time.Time) = &tiedTime
		*dest[3].(**float64) = &val1
		*dest[4].(**string) = nil
		*dest[5].(**bool) = nil
		*dest[6].(*[]byte) = meta
		*dest[7].(**uuid.UUID) = &msgID1
		return nil
	}
	rec2 := func(dest ...any) error {
		*dest[0].(*uuid.UUID) = entity.InternalID
		*dest[1].(*string) = entity.PublicURN
		*dest[2].(**time.Time) = &tiedTime
		*dest[3].(**float64) = &val2
		*dest[4].(**string) = nil
		*dest[5].(**bool) = nil
		*dest[6].(*[]byte) = meta
		*dest[7].(**uuid.UUID) = &msgID2
		return nil
	}

	fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec1, rec2}}
	db := &fakeTwinDatabase{rows: fakeRows}
	reader, _ := NewPostgresTwinReader(db)

	res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.PointCount != 2 {
		t.Fatalf("expected PointCount 2, got %d", res.PointCount)
	}
	if len(res.TemporalPoints) != 2 {
		t.Fatalf("expected 2 temporal points, got %d", len(res.TemporalPoints))
	}
	// Verify both tied points are preserved
	if !res.TemporalPoints[0].ObservedAt.Equal(tiedTime) || !res.TemporalPoints[1].ObservedAt.Equal(tiedTime) {
		t.Errorf("both points must preserve tied timestamp %v", tiedTime)
	}
	if *res.TemporalPoints[0].ValueNumber != val1 || *res.TemporalPoints[1].ValueNumber != val2 {
		t.Errorf("values must match without deduplication: got %v and %v", *res.TemporalPoints[0].ValueNumber, *res.TemporalPoints[1].ValueNumber)
	}
}

func TestReadTemporalHistory_NullableProvenanceAndMetadataPrecision(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	q := makeTestRawQuery(from, to, 100)

	obsTime1 := from.Add(1 * time.Second)
	obsTime2 := from.Add(2 * time.Second)
	val := 10000.0
	msgID := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")

	// Metadata with integer above 2^53 (e.g. 9007199254740993)
	hugeIntMeta := []byte(`{"unit":"us","large_seq":9007199254740993,"boot_id":"boot-1"}`)
	// Second record has NULL message_id and consistent unit "us"
	consistentMeta := []byte(`{"unit":"us"}`)

	rec1 := func(dest ...any) error {
		*dest[0].(*uuid.UUID) = entity.InternalID
		*dest[1].(*string) = entity.PublicURN
		*dest[2].(**time.Time) = &obsTime1
		*dest[3].(**float64) = &val
		*dest[4].(**string) = nil
		*dest[5].(**bool) = nil
		*dest[6].(*[]byte) = hugeIntMeta
		*dest[7].(**uuid.UUID) = &msgID
		return nil
	}
	rec2 := func(dest ...any) error {
		*dest[0].(*uuid.UUID) = entity.InternalID
		*dest[1].(*string) = entity.PublicURN
		*dest[2].(**time.Time) = &obsTime2
		*dest[3].(**float64) = &val
		*dest[4].(**string) = nil
		*dest[5].(**bool) = nil
		*dest[6].(*[]byte) = consistentMeta
		*dest[7].(**uuid.UUID) = nil
		return nil
	}

	fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec1, rec2}}
	db := &fakeTwinDatabase{rows: fakeRows}
	reader, _ := NewPostgresTwinReader(db)

	res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	p1 := res.TemporalPoints[0]
	if p1.MessageID == nil || *p1.MessageID != msgID {
		t.Fatalf("expected MessageID %v, got %v", msgID, p1.MessageID)
	}
	// Verify huge integer preserved as string in json.RawMessage without float64 distortion
	rawMeta1 := string(p1.Metadata)
	if !strings.Contains(rawMeta1, "9007199254740993") {
		t.Errorf("expected metadata to preserve 9007199254740993 verbatim, got: %s", rawMeta1)
	}

	p2 := res.TemporalPoints[1]
	if p2.MessageID != nil {
		t.Fatalf("expected nil MessageID for rec2, got %v", p2.MessageID)
	}
	rawMeta2 := string(p2.Metadata)
	if rawMeta2 != `{"unit":"us"}` {
		t.Errorf("expected metadata '{\"unit\":\"us\"}', got: %s", rawMeta2)
	}

	// Verify derived unit is "us" (from rec1)
	if res.Unit == nil || *res.Unit != "us" {
		t.Errorf("expected Unit 'us', got %v", res.Unit)
	}
}

func TestReadTemporalHistory_TypedFalseAndEmptyText(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)

	obsTime1 := from.Add(1 * time.Second)
	obsTime2 := from.Add(2 * time.Second)
	boolFalse := false
	emptyText := ""
	meta := []byte(`{}`)

	recBool := func(dest ...any) error {
		*dest[0].(*uuid.UUID) = entity.InternalID
		*dest[1].(*string) = entity.PublicURN
		*dest[2].(**time.Time) = &obsTime1
		*dest[3].(**float64) = nil
		*dest[4].(**string) = nil
		*dest[5].(**bool) = &boolFalse
		*dest[6].(*[]byte) = meta
		*dest[7].(**uuid.UUID) = nil
		return nil
	}
	recText := func(dest ...any) error {
		*dest[0].(*uuid.UUID) = entity.InternalID
		*dest[1].(*string) = entity.PublicURN
		*dest[2].(**time.Time) = &obsTime2
		*dest[3].(**float64) = nil
		*dest[4].(**string) = &emptyText
		*dest[5].(**bool) = nil
		*dest[6].(*[]byte) = meta
		*dest[7].(**uuid.UUID) = nil
		return nil
	}

	fakeRows := &fakeTwinRows{records: []func(dest ...any) error{recBool, recText}}
	db := &fakeTwinDatabase{rows: fakeRows}
	reader, _ := NewPostgresTwinReader(db)

	// Call the unexported typed observations helper directly (private seam for tests)
	points, err := reader.readTemporalObservationsInternal(
		context.Background(),
		actor,
		entity,
		"reserved_test_property",
		from,
		to,
		100,
	)
	if err != nil {
		t.Fatalf("unexpected error from readTemporalObservationsInternal: %v", err)
	}
	if len(points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(points))
	}

	pBool := points[0]
	if pBool.ValueType != "boolean" {
		t.Errorf("expected ValueType boolean, got %s", pBool.ValueType)
	}
	if pBool.ValueBoolean == nil {
		t.Fatal("expected non-nil ValueBoolean for false value")
	}
	if *pBool.ValueBoolean != false {
		t.Errorf("expected false, got %v", *pBool.ValueBoolean)
	}
	if pBool.ValueNumber != nil || pBool.ValueText != nil {
		t.Errorf("other value pointers must be nil, got num=%v, text=%v", pBool.ValueNumber, pBool.ValueText)
	}

	pText := points[1]
	if pText.ValueType != "text" {
		t.Errorf("expected ValueType text, got %s", pText.ValueType)
	}
	if pText.ValueText == nil {
		t.Fatal("expected non-nil ValueText for empty string value")
	}
	if *pText.ValueText != "" {
		t.Errorf("expected empty string, got %q", *pText.ValueText)
	}
	if pText.ValueNumber != nil || pText.ValueBoolean != nil {
		t.Errorf("other value pointers must be nil, got num=%v, bool=%v", pText.ValueNumber, pText.ValueBoolean)
	}
}

func TestReadTemporalHistory_NumericIntervalInvariant(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	q := makeTestRawQuery(from, to, 100)
	obsTime := from.Add(1 * time.Second)
	meta := []byte(`{}`)

	testCases := []struct {
		name   string
		record func(dest ...any) error
	}{
		{
			name: "zero interval value",
			record: func(dest ...any) error {
				zeroVal := 0.0
				*dest[0].(*uuid.UUID) = entity.InternalID
				*dest[1].(*string) = entity.PublicURN
				*dest[2].(**time.Time) = &obsTime
				*dest[3].(**float64) = &zeroVal
				*dest[4].(**string) = nil
				*dest[5].(**bool) = nil
				*dest[6].(*[]byte) = meta
				*dest[7].(**uuid.UUID) = nil
				return nil
			},
		},
		{
			name: "negative interval value",
			record: func(dest ...any) error {
				negVal := -100.0
				*dest[0].(*uuid.UUID) = entity.InternalID
				*dest[1].(*string) = entity.PublicURN
				*dest[2].(**time.Time) = &obsTime
				*dest[3].(**float64) = &negVal
				*dest[4].(**string) = nil
				*dest[5].(**bool) = nil
				*dest[6].(*[]byte) = meta
				*dest[7].(**uuid.UUID) = nil
				return nil
			},
		},
		{
			name: "fractional interval value",
			record: func(dest ...any) error {
				fracVal := 10000.5
				*dest[0].(*uuid.UUID) = entity.InternalID
				*dest[1].(*string) = entity.PublicURN
				*dest[2].(**time.Time) = &obsTime
				*dest[3].(**float64) = &fracVal
				*dest[4].(**string) = nil
				*dest[5].(**bool) = nil
				*dest[6].(*[]byte) = meta
				*dest[7].(**uuid.UUID) = nil
				return nil
			},
		},
		{
			name: "interval exceeding 2^53",
			record: func(dest ...any) error {
				hugeVal := float64(maxExactFloat64Integer + 1000)
				*dest[0].(*uuid.UUID) = entity.InternalID
				*dest[1].(*string) = entity.PublicURN
				*dest[2].(**time.Time) = &obsTime
				*dest[3].(**float64) = &hugeVal
				*dest[4].(**string) = nil
				*dest[5].(**bool) = nil
				*dest[6].(*[]byte) = meta
				*dest[7].(**uuid.UUID) = nil
				return nil
			},
		},
		{
			name: "stored as text instead of number",
			record: func(dest ...any) error {
				textVal := "10000"
				*dest[0].(*uuid.UUID) = entity.InternalID
				*dest[1].(*string) = entity.PublicURN
				*dest[2].(**time.Time) = &obsTime
				*dest[3].(**float64) = nil
				*dest[4].(**string) = &textVal
				*dest[5].(**bool) = nil
				*dest[6].(*[]byte) = meta
				*dest[7].(**uuid.UUID) = nil
				return nil
			},
		},
		{
			name: "all value columns null (0 non-null)",
			record: func(dest ...any) error {
				*dest[0].(*uuid.UUID) = entity.InternalID
				*dest[1].(*string) = entity.PublicURN
				*dest[2].(**time.Time) = &obsTime
				*dest[3].(**float64) = nil
				*dest[4].(**string) = nil
				*dest[5].(**bool) = nil
				*dest[6].(*[]byte) = meta
				*dest[7].(**uuid.UUID) = nil
				return nil
			},
		},
		{
			name: "multiple value columns non-null (>1 non-null)",
			record: func(dest ...any) error {
				numVal := 10000.0
				textVal := "10000"
				*dest[0].(*uuid.UUID) = entity.InternalID
				*dest[1].(*string) = entity.PublicURN
				*dest[2].(**time.Time) = &obsTime
				*dest[3].(**float64) = &numVal
				*dest[4].(**string) = &textVal
				*dest[5].(**bool) = nil
				*dest[6].(*[]byte) = meta
				*dest[7].(**uuid.UUID) = nil
				return nil
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeRows := &fakeTwinRows{records: []func(dest ...any) error{tc.record}}
			db := &fakeTwinDatabase{rows: fakeRows}
			reader, _ := NewPostgresTwinReader(db)

			res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
			if !errors.Is(err, ErrUnexpected) {
				t.Fatalf("expected ErrUnexpected for invalid interval invariant (%s), got %v", tc.name, err)
			}
			if res != nil {
				t.Fatalf("expected nil result on error, got %+v", res)
			}
		})
	}
}

func TestReadTemporalHistory_ExactCapAndOverflow(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	maxPoints := 3
	q := makeTestRawQuery(from, to, maxPoints)

	val := 10000.0
	meta := []byte(`{"unit":"us"}`)
	makeRec := func(sec int) func(dest ...any) error {
		obs := from.Add(time.Duration(sec) * time.Second)
		return func(dest ...any) error {
			*dest[0].(*uuid.UUID) = entity.InternalID
			*dest[1].(*string) = entity.PublicURN
			*dest[2].(**time.Time) = &obs
			*dest[3].(**float64) = &val
			*dest[4].(**string) = nil
			*dest[5].(**bool) = nil
			*dest[6].(*[]byte) = meta
			*dest[7].(**uuid.UUID) = nil
			return nil
		}
	}

	t.Run("exact cap (maxPoints rows) succeeds", func(t *testing.T) {
		records := []func(dest ...any) error{makeRec(1), makeRec(2), makeRec(3)}
		fakeRows := &fakeTwinRows{records: records}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.PointCount != 3 {
			t.Fatalf("expected PointCount 3, got %d", res.PointCount)
		}
	})

	t.Run("overflow (maxPoints+1 rows) returns ErrRawLimitExceeded without truncation", func(t *testing.T) {
		records := []func(dest ...any) error{makeRec(1), makeRec(2), makeRec(3), makeRec(4)}
		fakeRows := &fakeTwinRows{records: records}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrRawLimitExceeded) {
			t.Fatalf("expected ErrRawLimitExceeded on overflow, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on overflow, got %+v", res)
		}
	})
}

func TestReadTemporalHistory_ScanIterationAndCloseErrors(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	q := makeTestRawQuery(from, to, 100)

	t.Run("scan error returns ErrUnexpected and closes rows", func(t *testing.T) {
		fakeRows := &fakeTwinRows{
			records: []func(dest ...any) error{
				func(_ ...any) error { return errors.New("scan failed") },
			},
			scanErr: errors.New("scan failed"),
		}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected on scan error, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %+v", res)
		}
		if !fakeRows.closed {
			t.Fatal("expected rows to be closed on scan error")
		}
	})

	t.Run("rows.Err() iteration error returns classified error and closes rows", func(t *testing.T) {
		fakeRows := &fakeTwinRows{
			records: nil,
			endErr:  &pgconn.PgError{Code: "08006", Message: "connection failure"},
		}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable on 08006 pgconn, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %+v", res)
		}
		if !fakeRows.closed {
			t.Fatal("expected rows to be closed on iteration error")
		}
	})

	t.Run("database.Query error returns classified error", func(t *testing.T) {
		db := &fakeTwinDatabase{queryErr: errors.New("connection broken")}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected on arbitrary DB error, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %+v", res)
		}
	})
}

func TestReadTemporalHistory_ContextCancellation(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	q := makeTestRawQuery(from, to, 100)

	t.Run("pre-canceled context returns ErrDependencyUnavailable", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		db := &fakeTwinDatabase{}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(ctx, actor, entity, q)
		if !errors.Is(err, ErrDependencyUnavailable) || !errors.Is(err, context.Canceled) {
			t.Fatalf("expected ErrDependencyUnavailable wrapping context.Canceled, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on pre-canceled context, got %+v", res)
		}
	})

	t.Run("context canceled during row scan closes rows and returns error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		obsTime := from.Add(1 * time.Second)
		val := 10000.0

		rec := func(dest ...any) error {
			cancel() // cancel during scanning
			*dest[0].(*uuid.UUID) = entity.InternalID
			*dest[1].(*string) = entity.PublicURN
			*dest[2].(**time.Time) = &obsTime
			*dest[3].(**float64) = &val
			*dest[4].(**string) = nil
			*dest[5].(**bool) = nil
			*dest[6].(*[]byte) = []byte(`{"unit":"us"}`)
			*dest[7].(**uuid.UUID) = nil
			return nil
		}

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec, rec}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(ctx, actor, entity, q)
		if !errors.Is(err, ErrDependencyUnavailable) || !errors.Is(err, context.Canceled) {
			t.Fatalf("expected ErrDependencyUnavailable wrapping context.Canceled, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result when canceled during scan, got %+v", res)
		}
		if !fakeRows.closed {
			t.Fatal("expected rows to be closed when canceled")
		}
	})
}

func TestReadTemporalHistory_ResolutionHandling(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	obsTime := from.Add(1 * time.Second)
	val := 10000.0
	meta := []byte(`{"unit":"us"}`)

	rec := func(dest ...any) error {
		*dest[0].(*uuid.UUID) = entity.InternalID
		*dest[1].(*string) = entity.PublicURN
		*dest[2].(**time.Time) = &obsTime
		*dest[3].(**float64) = &val
		*dest[4].(**string) = nil
		*dest[5].(**bool) = nil
		*dest[6].(*[]byte) = meta
		*dest[7].(**uuid.UUID) = nil
		return nil
	}

	t.Run("explicit raw window > 10s returns ErrRawLimitExceeded", func(t *testing.T) {
		db := &fakeTwinDatabase{}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, from.Add(11*time.Second), 100)
		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrRawLimitExceeded) {
			t.Fatalf("expected ErrRawLimitExceeded for raw window > 10s, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %+v", res)
		}
	})

	t.Run("short auto window <= 10s fitting raw bounds succeeds", func(t *testing.T) {
		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, from.Add(5*time.Second), 100)
		q.Resolution = telemetry.ResolutionAuto

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if err != nil {
			t.Fatalf("unexpected error for short auto: %v", err)
		}
		if res.Mode != "raw" || res.Resolution != "raw" {
			t.Fatalf("expected mode=raw, resolution=raw for short auto, got mode=%s, res=%s", res.Mode, res.Resolution)
		}
		if res.PointCount != 1 {
			t.Fatalf("expected PointCount 1, got %d", res.PointCount)
		}
	})

	t.Run("short auto window with raw overflow executes bucket aggregation", func(t *testing.T) {
		// 3 raw records when maxPoints=2 triggers bucket fallback
		rawRecords := []func(dest ...any) error{rec, rec, rec}
		fakeRawRows := &fakeTwinRows{records: rawRecords}

		bStart := from.Add(1 * time.Second)
		bAvg := 15000.0
		bMin := 10000.0
		bMax := 20000.0
		bCount := int64(3)
		unitStr := "us"
		sum := &TemporalWindowSummary{
			TotalCount:        3,
			NumberCount:       3,
			DistinctUnitCount: 1,
			MinUnit:           &unitStr,
		}
		bRec := makeBucketRecord(entity, &bStart, &bAvg, &bMin, &bMax, &bCount, sum)
		fakeBucketRows := &fakeTwinRows{records: []func(dest ...any) error{bRec}}

		db := &fakeTwinDatabase{
			queryFn: func(_ context.Context, query string, _ ...any) (pgx.Rows, error) {
				if strings.Contains(query, "bounded_observations AS (") {
					return fakeRawRows, nil
				}
				if strings.Contains(query, "bucket_aggregates AS (") {
					return fakeBucketRows, nil
				}
				return &fakeTwinRows{}, nil
			},
		}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, from.Add(5*time.Second), 2)
		q.Resolution = telemetry.ResolutionAuto

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if err != nil {
			t.Fatalf("unexpected error for auto overflow bucket fallback: %v", err)
		}
		if res.Mode != "bucket" {
			t.Fatalf("expected mode=bucket, got %s", res.Mode)
		}
		if res.PointCount != 1 {
			t.Fatalf("expected PointCount 1, got %d", res.PointCount)
		}
		if len(res.TemporalBuckets) != 1 {
			t.Fatalf("expected 1 bucket, got %d", len(res.TemporalBuckets))
		}
		if res.TemporalBuckets[0].Avg != 15000.0 || res.TemporalBuckets[0].Count != 3 {
			t.Fatalf("unexpected bucket values: %+v", res.TemporalBuckets[0])
		}
		if res.Unit == nil || *res.Unit != "us" {
			t.Fatalf("expected unit 'us', got %v", res.Unit)
		}
		if res.TemporalPoints != nil {
			t.Fatalf("raw probe points must be discarded on bucket fallback, got %v", res.TemporalPoints)
		}
	})

	t.Run("long auto window > 10s selects bucket resolution directly", func(t *testing.T) {
		bStart := from
		bAvg := 10000.0
		bMin := 10000.0
		bMax := 10000.0
		bCount := int64(10)
		unitStr := "us"
		sum := &TemporalWindowSummary{
			TotalCount:        10,
			NumberCount:       10,
			DistinctUnitCount: 1,
			MinUnit:           &unitStr,
		}
		bRec := makeBucketRecord(entity, &bStart, &bAvg, &bMin, &bMax, &bCount, sum)
		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{bRec}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, from.Add(60*time.Second), 100)
		q.Resolution = telemetry.ResolutionAuto

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if err != nil {
			t.Fatalf("unexpected error for long auto: %v", err)
		}
		if res.Mode != "bucket" {
			t.Fatalf("expected mode=bucket for long auto, got %s", res.Mode)
		}
		if res.Resolution != "1s" {
			t.Fatalf("expected chosen resolution 1s, got %s", res.Resolution)
		}
		if res.PointCount != 1 {
			t.Fatalf("expected PointCount 1, got %d", res.PointCount)
		}
	})

	t.Run("explicit bucket resolutions execute bucket aggregation", func(t *testing.T) {
		bStart := from
		bAvg := 25000.0
		bMin := 20000.0
		bMax := 30000.0
		bCount := int64(2)
		unitStr := "us"
		sum := &TemporalWindowSummary{
			TotalCount:        2,
			NumberCount:       2,
			DistinctUnitCount: 1,
			MinUnit:           &unitStr,
		}
		bRec := makeBucketRecord(entity, &bStart, &bAvg, &bMin, &bMax, &bCount, sum)

		bucketResolutions := []telemetry.Resolution{
			telemetry.Resolution100ms,
			telemetry.Resolution1s,
			telemetry.Resolution10s,
			telemetry.Resolution1m,
			telemetry.Resolution5m,
			telemetry.Resolution15m,
			telemetry.Resolution1h,
			telemetry.Resolution6h,
			telemetry.Resolution1d,
		}
		for _, bRes := range bucketResolutions {
			dur, _ := telemetry.ResolutionDuration(bRes)
			// Choose window such that IntersectingBucketCount <= maxPoints
			windowEnd := from.Add(dur * 2)
			q := makeTestRawQuery(from, windowEnd, 100)
			q.Resolution = bRes

			fakeRows := &fakeTwinRows{records: []func(dest ...any) error{bRec}}
			db := &fakeTwinDatabase{rows: fakeRows}
			reader, _ := NewPostgresTwinReader(db)

			res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
			if err != nil {
				t.Fatalf("unexpected error for bucket resolution %q: %v", bRes, err)
			}
			if res.Mode != "bucket" || res.Resolution != string(bRes) {
				t.Fatalf("expected mode=bucket res=%s, got mode=%s res=%s", bRes, res.Mode, res.Resolution)
			}
			if res.PointCount != 1 {
				t.Fatalf("expected 1 point, got %d", res.PointCount)
			}
		}
	})

	t.Run("unsupported resolution returns ErrInvalidResolution", func(t *testing.T) {
		db := &fakeTwinDatabase{}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, from.Add(5*time.Second), 100)
		q.Resolution = telemetry.Resolution("2s") // not an allowlisted resolution

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrInvalidResolution) {
			t.Fatalf("expected ErrInvalidResolution for unsupported resolution, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %+v", res)
		}
	})
}

func TestReadTemporalHistory_InputValidations(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	validQ := makeTestRawQuery(from, to, 100)

	db := &fakeTwinDatabase{}
	reader, _ := NewPostgresTwinReader(db)
	ctx := context.Background()

	t.Run("nil actor returns ErrNilActor", func(t *testing.T) {
		_, err := reader.ReadTemporalHistory(ctx, uuid.Nil, entity, validQ)
		if !errors.Is(err, ErrNilActor) {
			t.Fatalf("expected ErrNilActor, got %v", err)
		}
	})

	t.Run("non-sensor entity returns ErrInvalidProperty", func(t *testing.T) {
		gwEntity := ResolvedEntity{
			InternalID: uuid.New(),
			PublicURN:  "urn:ngsi-ld:Gateway:gw_01",
			Type:       EntityTypeGateway,
			GatewayID:  "gw_01",
		}
		_, err := reader.ReadTemporalHistory(ctx, actor, gwEntity, validQ)
		if !errors.Is(err, ErrInvalidProperty) {
			t.Fatalf("expected ErrInvalidProperty for Gateway entity, got %v", err)
		}
	})

	t.Run("invalid property selector returns ErrInvalidProperty", func(t *testing.T) {
		q := validQ
		q.Property = HistorySelector("value") // value must not be sent to ReadTemporalHistory
		_, err := reader.ReadTemporalHistory(ctx, actor, entity, q)
		if !errors.Is(err, ErrInvalidProperty) {
			t.Fatalf("expected ErrInvalidProperty for property=value in ReadTemporalHistory, got %v", err)
		}
	})

	t.Run("maxPoints <= 0 returns ErrInvalidInput", func(t *testing.T) {
		q := validQ
		q.MaxPoints = 0
		_, err := reader.ReadTemporalHistory(ctx, actor, entity, q)
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput for maxPoints <= 0, got %v", err)
		}
	})

	t.Run("from not before to returns ErrInvalidInput", func(t *testing.T) {
		q := validQ
		q.Range.From = to
		q.Range.To = from
		_, err := reader.ReadTemporalHistory(ctx, actor, entity, q)
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput when from >= to, got %v", err)
		}
	})
}

func makeSummaryRecord(
	entity ResolvedEntity,
	s TemporalWindowSummary,
	obsTime *time.Time,
	valNum *float64,
	valText *string,
	valBool *bool,
	metadata []byte,
	msgID *uuid.UUID,
) func(dest ...any) error {
	return func(dest ...any) error {
		*dest[0].(*uuid.UUID) = entity.InternalID
		*dest[1].(*string) = entity.PublicURN
		*dest[2].(**time.Time) = obsTime
		*dest[3].(**float64) = valNum
		*dest[4].(**string) = valText
		*dest[5].(**bool) = valBool
		*dest[6].(*[]byte) = metadata
		*dest[7].(**uuid.UUID) = msgID

		if len(dest) >= 18 {
			*dest[8].(*int64) = s.TotalCount
			*dest[9].(*int64) = s.NumberCount
			*dest[10].(*int64) = s.TextCount
			*dest[11].(*int64) = s.BooleanCount
			*dest[12].(*int64) = s.InvalidTypeCount
			*dest[13].(*int64) = s.InvalidIntervalCount
			*dest[14].(*int64) = s.UnknownUnitCount
			*dest[15].(*int64) = s.MalformedUnitCount
			*dest[16].(*int64) = s.DistinctUnitCount
			*dest[17].(**string) = s.MinUnit
		}
		return nil
	}
}

func TestReadTemporalHistory_WholeWindowValidation(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	q := makeTestRawQuery(from, to, 10)

	t.Run("numeric with one known unit succeeds and returns homogeneous unit", func(t *testing.T) {
		unitUS := "us"
		val := 10000.0
		obs1 := from.Add(1 * time.Second)
		obs2 := from.Add(2 * time.Second)

		summary := TemporalWindowSummary{
			TotalCount:           2,
			NumberCount:          2,
			TextCount:            0,
			BooleanCount:         0,
			InvalidTypeCount:     0,
			InvalidIntervalCount: 0,
			UnknownUnitCount:     0,
			MalformedUnitCount:   0,
			DistinctUnitCount:    1,
			MinUnit:              &unitUS,
		}

		rec1 := makeSummaryRecord(entity, summary, &obs1, &val, nil, nil, []byte(`{"unit":"us"}`), nil)
		rec2 := makeSummaryRecord(entity, summary, &obs2, &val, nil, nil, []byte(`{"unit":"us"}`), nil)

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec1, rec2}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res == nil {
			t.Fatal("expected non-nil result")
		}
		if res.Unit == nil || *res.Unit != "us" {
			t.Fatalf("expected unit 'us', got %v", res.Unit)
		}
		if res.PointCount != 2 {
			t.Fatalf("expected PointCount 2, got %d", res.PointCount)
		}
	})

	t.Run("all units unknown yields nil unit without fabricating us default", func(t *testing.T) {
		val := 10000.0
		obs1 := from.Add(1 * time.Second)
		obs2 := from.Add(2 * time.Second)

		summary := TemporalWindowSummary{
			TotalCount:           2,
			NumberCount:          2,
			TextCount:            0,
			BooleanCount:         0,
			InvalidTypeCount:     0,
			InvalidIntervalCount: 0,
			UnknownUnitCount:     2,
			MalformedUnitCount:   0,
			DistinctUnitCount:    0,
			MinUnit:              nil,
		}

		rec1 := makeSummaryRecord(entity, summary, &obs1, &val, nil, nil, []byte(`{}`), nil)
		rec2 := makeSummaryRecord(entity, summary, &obs2, &val, nil, nil, []byte(`{}`), nil)

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec1, rec2}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res == nil {
			t.Fatal("expected non-nil result")
		}
		if res.Unit != nil {
			t.Fatalf("expected nil unit for all unknown, got %q", *res.Unit)
		}
	})

	t.Run("missing unit and JSON null both count as unknown unit", func(t *testing.T) {
		val := 10000.0
		obs1 := from.Add(1 * time.Second)
		obs2 := from.Add(2 * time.Second)

		summary := TemporalWindowSummary{
			TotalCount:           2,
			NumberCount:          2,
			TextCount:            0,
			BooleanCount:         0,
			InvalidTypeCount:     0,
			InvalidIntervalCount: 0,
			UnknownUnitCount:     2,
			MalformedUnitCount:   0,
			DistinctUnitCount:    0,
			MinUnit:              nil,
		}

		// rec1: missing unit key ({}); rec2: JSON null ({"unit": null})
		rec1 := makeSummaryRecord(entity, summary, &obs1, &val, nil, nil, []byte(`{}`), nil)
		rec2 := makeSummaryRecord(entity, summary, &obs2, &val, nil, nil, []byte(`{"unit":null}`), nil)

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec1, rec2}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Unit != nil {
			t.Fatalf("expected nil unit for missing + JSON null mix, got %q", *res.Unit)
		}
	})

	t.Run("multiple known units in numeric series produces ErrInconsistentUnit", func(t *testing.T) {
		unitUS := "us"
		val := 10000.0
		obs1 := from.Add(1 * time.Second)

		summary := TemporalWindowSummary{
			TotalCount:           2,
			NumberCount:          2,
			TextCount:            0,
			BooleanCount:         0,
			InvalidTypeCount:     0,
			InvalidIntervalCount: 0,
			UnknownUnitCount:     0,
			MalformedUnitCount:   0,
			DistinctUnitCount:    2, // e.g. "us" and "ms"
			MinUnit:              &unitUS,
		}

		rec1 := makeSummaryRecord(entity, summary, &obs1, &val, nil, nil, []byte(`{"unit":"us"}`), nil)

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec1}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrInconsistentUnit) {
			t.Fatalf("expected ErrInconsistentUnit for multiple distinct units, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("known mixed with unknown unit produces ErrInconsistentUnit", func(t *testing.T) {
		unitUS := "us"
		val := 10000.0
		obs1 := from.Add(1 * time.Second)

		summary := TemporalWindowSummary{
			TotalCount:           2,
			NumberCount:          2,
			TextCount:            0,
			BooleanCount:         0,
			InvalidTypeCount:     0,
			InvalidIntervalCount: 0,
			UnknownUnitCount:     1, // 1 known ("us"), 1 unknown (missing/null)
			MalformedUnitCount:   0,
			DistinctUnitCount:    1,
			MinUnit:              &unitUS,
		}

		rec1 := makeSummaryRecord(entity, summary, &obs1, &val, nil, nil, []byte(`{"unit":"us"}`), nil)

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec1}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrInconsistentUnit) {
			t.Fatalf("expected ErrInconsistentUnit for known/unknown mix, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("mixed stored types in temporal window produces ErrUnexpected", func(t *testing.T) {
		val := 10000.0
		obs1 := from.Add(1 * time.Second)

		summary := TemporalWindowSummary{
			TotalCount:           2,
			NumberCount:          1,
			TextCount:            1, // mixed numbers and text in window
			BooleanCount:         0,
			InvalidTypeCount:     0,
			InvalidIntervalCount: 0,
			UnknownUnitCount:     0,
			MalformedUnitCount:   0,
			DistinctUnitCount:    1,
			MinUnit:              nil,
		}

		rec1 := makeSummaryRecord(entity, summary, &obs1, &val, nil, nil, []byte(`{"unit":"us"}`), nil)

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec1}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for mixed stored types, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("malformed public numeric interval in window produces ErrUnexpected", func(t *testing.T) {
		unitUS := "us"
		val := 10000.0
		obs1 := from.Add(1 * time.Second)

		summary := TemporalWindowSummary{
			TotalCount:           2,
			NumberCount:          2,
			TextCount:            0,
			BooleanCount:         0,
			InvalidTypeCount:     0,
			InvalidIntervalCount: 1, // e.g. interval <= 0 or > 2^53 in window
			UnknownUnitCount:     0,
			MalformedUnitCount:   0,
			DistinctUnitCount:    1,
			MinUnit:              &unitUS,
		}

		rec1 := makeSummaryRecord(entity, summary, &obs1, &val, nil, nil, []byte(`{"unit":"us"}`), nil)

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec1}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for invalid interval in window, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("malformed non-string unit in metadata fails safely with ErrUnexpected", func(t *testing.T) {
		val := 10000.0
		obs1 := from.Add(1 * time.Second)

		summary := TemporalWindowSummary{
			TotalCount:           2,
			NumberCount:          2,
			TextCount:            0,
			BooleanCount:         0,
			InvalidTypeCount:     0,
			InvalidIntervalCount: 0,
			UnknownUnitCount:     0,
			MalformedUnitCount:   1, // e.g. {"unit": 123}
			DistinctUnitCount:    1,
			MinUnit:              nil,
		}

		rec1 := makeSummaryRecord(entity, summary, &obs1, &val, nil, nil, []byte(`{"unit":123}`), nil)

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec1}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for non-string unit, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("validation observes full-window error beyond bounded raw prefix", func(t *testing.T) {
		// maxPoints is 2; the returned observation rows (prefix) are 2 valid points with unit "us".
		// But the window summary has DistinctUnitCount = 2 (e.g. observation #50 has unit "ms").
		unitUS := "us"
		val := 10000.0
		obs1 := from.Add(1 * time.Second)
		obs2 := from.Add(2 * time.Second)

		prefixQ := makeTestRawQuery(from, to, 2) // maxPoints = 2

		summaryWithInconsistentUnit := TemporalWindowSummary{
			TotalCount:           50,
			NumberCount:          50,
			TextCount:            0,
			BooleanCount:         0,
			InvalidTypeCount:     0,
			InvalidIntervalCount: 0,
			UnknownUnitCount:     0,
			MalformedUnitCount:   0,
			DistinctUnitCount:    2, // inconsistent unit exists at row 50!
			MinUnit:              &unitUS,
		}

		rec1 := makeSummaryRecord(entity, summaryWithInconsistentUnit, &obs1, &val, nil, nil, []byte(`{"unit":"us"}`), nil)
		rec2 := makeSummaryRecord(entity, summaryWithInconsistentUnit, &obs2, &val, nil, nil, []byte(`{"unit":"us"}`), nil)

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec1, rec2}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, prefixQ)
		if !errors.Is(err, ErrInconsistentUnit) {
			t.Fatalf("expected ErrInconsistentUnit from full-window summary even though prefix is homogeneous, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}

		// Similarly, invalid interval beyond prefix must be caught
		summaryWithInvalidInterval := TemporalWindowSummary{
			TotalCount:           50,
			NumberCount:          50,
			TextCount:            0,
			BooleanCount:         0,
			InvalidTypeCount:     0,
			InvalidIntervalCount: 1, // invalid interval exists at row 50!
			UnknownUnitCount:     0,
			MalformedUnitCount:   0,
			DistinctUnitCount:    1,
			MinUnit:              &unitUS,
		}

		rec1Invalid := makeSummaryRecord(entity, summaryWithInvalidInterval, &obs1, &val, nil, nil, []byte(`{"unit":"us"}`), nil)
		rec2Invalid := makeSummaryRecord(entity, summaryWithInvalidInterval, &obs2, &val, nil, nil, []byte(`{"unit":"us"}`), nil)

		fakeRowsInvalid := &fakeTwinRows{records: []func(dest ...any) error{rec1Invalid, rec2Invalid}}
		dbInvalid := &fakeTwinDatabase{rows: fakeRowsInvalid}
		readerInvalid, _ := NewPostgresTwinReader(dbInvalid)

		resInv, errInv := readerInvalid.ReadTemporalHistory(context.Background(), actor, entity, prefixQ)
		if !errors.Is(errInv, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected from full-window summary for interval breach beyond prefix, got %v", errInv)
		}
		if resInv != nil {
			t.Fatalf("expected nil result on error, got %+v", resInv)
		}
	})

	t.Run("classifyTemporalWindow classification seam for non-numeric properties", func(t *testing.T) {
		unitStr := "raw_text"
		textSummary := &TemporalWindowSummary{
			TotalCount:        5,
			NumberCount:       0,
			TextCount:         5,
			BooleanCount:      0,
			UnknownUnitCount:  0,
			DistinctUnitCount: 1,
			MinUnit:           &unitStr,
		}
		class, err := classifyTemporalWindow(textSummary, "reserved_text_prop")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if class.ValueType != "text" {
			t.Errorf("expected ValueType 'text', got %s", class.ValueType)
		}
		if class.Unit == nil || *class.Unit != "raw_text" {
			t.Errorf("expected unit 'raw_text', got %v", class.Unit)
		}

		// Text property with multiple units should not fail with ErrInconsistentUnit; unit is nil
		textMultiUnit := &TemporalWindowSummary{
			TotalCount:        5,
			NumberCount:       0,
			TextCount:         5,
			BooleanCount:      0,
			UnknownUnitCount:  0,
			DistinctUnitCount: 2,
		}
		classMulti, err := classifyTemporalWindow(textMultiUnit, "reserved_text_prop")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if classMulti.Unit != nil {
			t.Errorf("expected nil unit for multi-unit text property, got %v", classMulti.Unit)
		}
	})
}

func TestReadTemporalHistory_BucketSQLStructure(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(30 * time.Second)
	q := makeTestRawQuery(from, to, 500)
	q.Resolution = telemetry.Resolution1s

	fakeRows := &fakeTwinRows{records: nil}
	db := &fakeTwinDatabase{rows: fakeRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = reader.ReadTemporalHistory(context.Background(), actor, entity, q)

	sql := db.lastQuery
	// Verify authorized_sensor CTE
	if !strings.Contains(sql, "WITH authorized_sensor AS (") {
		t.Errorf("query missing authorized_sensor CTE: %s", sql)
	}
	if !strings.Contains(sql, "JOIN public.sensors AS sens") {
		t.Errorf("query missing sensors join: %s", sql)
	}
	if !strings.Contains(sql, "sens.gateway_id = e.gateway_id AND sens.sensor_id = $1") {
		t.Errorf("query missing same-gateway sensor join predicate: %s", sql)
	}
	if !strings.Contains(sql, "parent_e.entity_type = 'Gateway'") {
		t.Errorf("query missing Gateway parent predicate: %s", sql)
	}
	if !strings.Contains(sql, "parent_e.entity_id = 'urn:ngsi-ld:Gateway:' || e.gateway_id") {
		t.Errorf("query missing canonical parent Gateway URN predicate: %s", sql)
	}
	if !strings.Contains(sql, "e.entity_id = 'urn:ngsi-ld:Sensor:' || sens.gateway_id || ':' || sens.sensor_id") {
		t.Errorf("query missing canonical Sensor URN predicate: %s", sql)
	}
	if !strings.Contains(sql, "r.relationship_type = 'hasSensor'") {
		t.Errorf("query missing hasSensor relationship predicate: %s", sql)
	}
	if !strings.Contains(sql, "JOIN public.user_gateways AS ug") || !strings.Contains(sql, "ug.gateway_id = g.gateway_id AND ug.user_id = $2") {
		t.Errorf("query missing actor user_gateways membership check: %s", sql)
	}
	if !strings.Contains(sql, "e.entity_type = 'Sensor'") {
		t.Errorf("query missing Sensor entity_type check: %s", sql)
	}

	// Verify window_summary CTE
	if !strings.Contains(sql, "window_summary AS (") {
		t.Errorf("query missing window_summary CTE: %s", sql)
	}

	// Verify bucket_aggregates CTE with explicit UTC epoch origin and pre-aggregation clipping
	if !strings.Contains(sql, "bucket_aggregates AS (") {
		t.Errorf("query missing bucket_aggregates CTE: %s", sql)
	}
	if !strings.Contains(sql, "time_bucket($9::interval, v.observed_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS bucket_start") {
		t.Errorf("query missing explicit epoch origin time_bucket: %s", sql)
	}
	if !strings.Contains(sql, "AVG(v.value_number)::double precision AS avg_value") {
		t.Errorf("query missing AVG aggregation: %s", sql)
	}
	if !strings.Contains(sql, "MIN(v.value_number)::double precision AS min_value") {
		t.Errorf("query missing MIN aggregation: %s", sql)
	}
	if !strings.Contains(sql, "MAX(v.value_number)::double precision AS max_value") {
		t.Errorf("query missing MAX aggregation: %s", sql)
	}
	if !strings.Contains(sql, "COUNT(*)::bigint AS sample_count") {
		t.Errorf("query missing COUNT aggregation: %s", sql)
	}
	if !strings.Contains(sql, "v.observed_at >= $7") || !strings.Contains(sql, "v.observed_at < $8") {
		t.Errorf("query missing pre-aggregation clipping to [from, to): %s", sql)
	}
	if !strings.Contains(sql, "GROUP BY bucket_start") {
		t.Errorf("query missing GROUP BY bucket_start: %s", sql)
	}
	if !strings.Contains(sql, "ORDER BY b.bucket_start ASC NULLS LAST") {
		t.Errorf("query missing ORDER BY bucket_start ASC: %s", sql)
	}
	if !strings.Contains(sql, "LIMIT $10") {
		t.Errorf("query missing LIMIT $10: %s", sql)
	}

	// Verify arguments
	if len(db.lastArgs) != 10 {
		t.Fatalf("expected 10 arguments, got %d: %v", len(db.lastArgs), db.lastArgs)
	}
	if db.lastArgs[0] != entity.SensorID {
		t.Errorf("arg[0] = %v, want %s", db.lastArgs[0], entity.SensorID)
	}
	if db.lastArgs[1] != actor {
		t.Errorf("arg[1] = %v, want %s", db.lastArgs[1], actor)
	}
	if db.lastArgs[2] != entity.InternalID {
		t.Errorf("arg[2] = %v, want %s", db.lastArgs[2], entity.InternalID)
	}
	if db.lastArgs[3] != entity.PublicURN {
		t.Errorf("arg[3] = %v, want %s", db.lastArgs[3], entity.PublicURN)
	}
	if db.lastArgs[4] != entity.GatewayID {
		t.Errorf("arg[4] = %v, want %s", db.lastArgs[4], entity.GatewayID)
	}
	if db.lastArgs[5] != "sample_interval_us" {
		t.Errorf("arg[5] = %v, want sample_interval_us", db.lastArgs[5])
	}
	if db.lastArgs[6] != from.UTC() {
		t.Errorf("arg[6] = %v, want %v", db.lastArgs[6], from.UTC())
	}
	if db.lastArgs[7] != to.UTC() {
		t.Errorf("arg[7] = %v, want %v", db.lastArgs[7], to.UTC())
	}
	// Interval argument must be pgtype.Interval with 1,000,000 microseconds (1s)
	intervalArg, ok := db.lastArgs[8].(pgtype.Interval)
	if !ok || intervalArg.Microseconds != 1_000_000 || !intervalArg.Valid {
		t.Errorf("arg[8] expected pgtype.Interval(1s), got %v", db.lastArgs[8])
	}
	if db.lastArgs[9] != 501 {
		t.Errorf("arg[9] = %v, want 501 (maxPoints+1)", db.lastArgs[9])
	}
}

func TestReadTemporalHistory_BucketArithmeticAndNoGapfill(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	q := makeTestRawQuery(from, to, 100)
	q.Resolution = telemetry.Resolution1s

	unitUS := "us"
	summary := &TemporalWindowSummary{
		TotalCount:        3,
		NumberCount:       3,
		DistinctUnitCount: 1,
		MinUnit:           &unitUS,
	}

	// Bucket 0: [10:15:00, 10:15:01) with 2 samples: 10.0 and 20.0 -> avg 15.0, min 10.0, max 20.0, count 2
	b0Start := from
	b0Avg := 15.0
	b0Min := 10.0
	b0Max := 20.0
	b0Count := int64(2)
	rec0 := makeBucketRecord(entity, &b0Start, &b0Avg, &b0Min, &b0Max, &b0Count, summary)

	// Bucket 2: [10:15:02, 10:15:03) with 1 sample: 30.0 -> avg 30.0, min 30.0, max 30.0, count 1
	// Bucket 1 at [10:15:01, 10:15:02) has 0 samples, so it is ABSENT (no gap-fill).
	b2Start := from.Add(2 * time.Second)
	b2Avg := 30.0
	b2Min := 30.0
	b2Max := 30.0
	b2Count := int64(1)
	rec2 := makeBucketRecord(entity, &b2Start, &b2Avg, &b2Min, &b2Max, &b2Count, summary)

	fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec0, rec2}}
	db := &fakeTwinDatabase{rows: fakeRows}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatal(err)
	}

	res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result")
	}

	if res.Mode != "bucket" || res.Resolution != "1s" {
		t.Fatalf("expected mode=bucket resolution=1s, got %s/%s", res.Mode, res.Resolution)
	}
	if res.PointCount != 2 {
		t.Fatalf("expected PointCount 2, got %d", res.PointCount)
	}
	if len(res.TemporalBuckets) != 2 {
		t.Fatalf("expected exactly 2 buckets (gap preserved, no gap-fill), got %d", len(res.TemporalBuckets))
	}

	// Bucket 0 assertions
	b0 := res.TemporalBuckets[0]
	if !b0.BucketStart.Equal(b0Start) {
		t.Errorf("bucket[0] start = %v, want %v", b0.BucketStart, b0Start)
	}
	if b0.Avg != 15.0 || b0.Min != 10.0 || b0.Max != 20.0 || b0.Count != 2 {
		t.Errorf("bucket[0] values = %+v, want avg=15 min=10 max=20 count=2", b0)
	}

	// Bucket 1 (which represents timestamp from+2s) assertions
	b1 := res.TemporalBuckets[1]
	if !b1.BucketStart.Equal(b2Start) {
		t.Errorf("bucket[1] start = %v, want %v", b1.BucketStart, b2Start)
	}
	if b1.Avg != 30.0 || b1.Min != 30.0 || b1.Max != 30.0 || b1.Count != 1 {
		t.Errorf("bucket[1] values = %+v, want avg=30 min=30 max=30 count=1", b1)
	}

	if res.Unit == nil || *res.Unit != "us" {
		t.Errorf("expected unit 'us', got %v", res.Unit)
	}
}

func TestReadTemporalHistory_BucketWholeWindowValidation(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	q := makeTestRawQuery(from, to, 100)
	q.Resolution = telemetry.Resolution1s

	bStart := from
	avgVal := 10000.0
	minVal := 10000.0
	maxVal := 10000.0
	countVal := int64(2)
	unitUS := "us"

	t.Run("inconsistent unit in bucket query window returns ErrInconsistentUnit", func(t *testing.T) {
		summary := &TemporalWindowSummary{
			TotalCount:        2,
			NumberCount:       2,
			DistinctUnitCount: 2, // inconsistent unit!
			MinUnit:           &unitUS,
		}
		rec := makeBucketRecord(entity, &bStart, &avgVal, &minVal, &maxVal, &countVal, summary)
		db := &fakeTwinDatabase{rows: &fakeTwinRows{records: []func(dest ...any) error{rec}}}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrInconsistentUnit) {
			t.Fatalf("expected ErrInconsistentUnit, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("invalid interval in bucket query window returns ErrUnexpected", func(t *testing.T) {
		summary := &TemporalWindowSummary{
			TotalCount:           2,
			NumberCount:          2,
			InvalidIntervalCount: 1, // <= 0 or > 2^53
			DistinctUnitCount:    1,
			MinUnit:              &unitUS,
		}
		rec := makeBucketRecord(entity, &bStart, &avgVal, &minVal, &maxVal, &countVal, summary)
		db := &fakeTwinDatabase{rows: &fakeTwinRows{records: []func(dest ...any) error{rec}}}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for invalid interval in bucket window, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("mixed types in bucket query window returns ErrUnexpected", func(t *testing.T) {
		summary := &TemporalWindowSummary{
			TotalCount:        2,
			NumberCount:       1,
			TextCount:         1, // mixed
			DistinctUnitCount: 1,
			MinUnit:           &unitUS,
		}
		rec := makeBucketRecord(entity, &bStart, &avgVal, &minVal, &maxVal, &countVal, summary)
		db := &fakeTwinDatabase{rows: &fakeTwinRows{records: []func(dest ...any) error{rec}}}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for mixed types in bucket window, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})
}

func TestReadTemporalHistory_ResolutionTooFine(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	to := from.Add(1 * time.Hour) // 1h with 100ms = 36000 intersecting buckets

	t.Run("intersecting bucket count exceeding maxPoints returns ErrResolutionTooFine without DB query", func(t *testing.T) {
		db := &fakeTwinDatabase{}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, to, 100) // maxPoints = 100
		q.Resolution = telemetry.Resolution100ms

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrResolutionTooFine) {
			t.Fatalf("expected ErrResolutionTooFine, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %+v", res)
		}
		if db.calls != 0 {
			t.Fatalf("expected 0 DB calls when resolution too fine check fails upfront, got %d", db.calls)
		}
	})

	t.Run("DB returning buckets exceeding maxPoints returns ErrResolutionTooFine without silent truncation", func(t *testing.T) {
		// Window where IntersectingBucketCount <= maxPoints upfront
		shortTo := from.Add(3 * time.Second)
		q := makeTestRawQuery(from, shortTo, 2) // maxPoints = 2
		q.Resolution = telemetry.Resolution1s

		bStart := from
		val := 10.0
		cnt := int64(1)
		unitStr := "us"
		sum := &TemporalWindowSummary{TotalCount: 3, NumberCount: 3, DistinctUnitCount: 1, MinUnit: &unitStr}
		rec := makeBucketRecord(entity, &bStart, &val, &val, &val, &cnt, sum)

		// 3 records returned from DB when maxPoints is 2
		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec, rec, rec}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrResolutionTooFine) {
			t.Fatalf("expected ErrResolutionTooFine when DB returns > maxPoints, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %+v", res)
		}
	})
}

func TestReadTemporalHistory_AutoModeFallbackDetails(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	q := makeTestRawQuery(from, to, 2) // maxPoints = 2
	q.Resolution = telemetry.ResolutionAuto

	obsTime := from.Add(1 * time.Second)
	val := 10000.0
	rawRec := func(dest ...any) error {
		*dest[0].(*uuid.UUID) = entity.InternalID
		*dest[1].(*string) = entity.PublicURN
		*dest[2].(**time.Time) = &obsTime
		*dest[3].(**float64) = &val
		*dest[4].(**string) = nil
		*dest[5].(**bool) = nil
		*dest[6].(*[]byte) = []byte(`{"unit":"us"}`)
		*dest[7].(**uuid.UUID) = nil
		return nil
	}

	t.Run("fallback query failure discards raw probe and returns classified error", func(t *testing.T) {
		// 3 raw records cause auto to trigger bucket fallback
		fakeRawRows := &fakeTwinRows{records: []func(dest ...any) error{rawRec, rawRec, rawRec}}

		db := &fakeTwinDatabase{
			queryFn: func(_ context.Context, query string, _ ...any) (pgx.Rows, error) {
				if strings.Contains(query, "bounded_observations AS (") {
					return fakeRawRows, nil
				}
				if strings.Contains(query, "bucket_aggregates AS (") {
					return nil, errors.New("bucket query connection dropped")
				}
				return &fakeTwinRows{}, nil
			},
		}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected on fallback query failure, got %v", err)
		}
		if res != nil {
			t.Fatalf("no partial raw probe points may escape failed fallback, got %+v", res)
		}
	})

	t.Run("context cancellation during bucket fallback returns classified error with nil result", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		fakeRawRows := &fakeTwinRows{records: []func(dest ...any) error{rawRec, rawRec, rawRec}}

		db := &fakeTwinDatabase{
			queryFn: func(c context.Context, query string, _ ...any) (pgx.Rows, error) {
				if strings.Contains(query, "bounded_observations AS (") {
					return fakeRawRows, nil
				}
				cancel() // cancel during fallback query execution
				return nil, c.Err()
			},
		}
		reader, _ := NewPostgresTwinReader(db)

		res, err := reader.ReadTemporalHistory(ctx, actor, entity, q)
		if !errors.Is(err, ErrDependencyUnavailable) || !errors.Is(err, context.Canceled) {
			t.Fatalf("expected ErrDependencyUnavailable wrapping context.Canceled, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on cancel during fallback, got %+v", res)
		}
	})
}

func TestReadTemporalHistory_PrivateTypedExecutionSeam(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)

	// Test with private text property
	t.Run("private text property explicit raw succeeds with real empty string value", func(t *testing.T) {
		obsTime := from.Add(1 * time.Second)
		emptyStr := ""
		textRec := func(dest ...any) error {
			*dest[0].(*uuid.UUID) = entity.InternalID
			*dest[1].(*string) = entity.PublicURN
			*dest[2].(**time.Time) = &obsTime
			*dest[3].(**float64) = nil
			*dest[4].(**string) = &emptyStr
			*dest[5].(**bool) = nil
			*dest[6].(*[]byte) = []byte(`{}`)
			*dest[7].(**uuid.UUID) = nil
			return nil
		}

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{textRec}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, to, 10)
		res, err := reader.readTemporalHistoryInternal(context.Background(), actor, entity, "private_text_prop", q)
		if err != nil {
			t.Fatalf("unexpected error for private text raw: %v", err)
		}
		if res.PointCount != 1 || len(res.TemporalPoints) != 1 {
			t.Fatalf("expected 1 point, got %d", res.PointCount)
		}
		pt := res.TemporalPoints[0]
		if pt.ValueType != "text" || pt.ValueText == nil || *pt.ValueText != "" {
			t.Errorf("expected text point with empty string, got type=%s val=%v", pt.ValueType, pt.ValueText)
		}
		if pt.ValueNumber != nil || pt.ValueBoolean != nil {
			t.Errorf("other value pointers must be nil, got num=%v bool=%v", pt.ValueNumber, pt.ValueBoolean)
		}
	})

	t.Run("private boolean property explicit raw succeeds with real false value", func(t *testing.T) {
		obsTime := from.Add(1 * time.Second)
		falseVal := false
		boolRec := func(dest ...any) error {
			*dest[0].(*uuid.UUID) = entity.InternalID
			*dest[1].(*string) = entity.PublicURN
			*dest[2].(**time.Time) = &obsTime
			*dest[3].(**float64) = nil
			*dest[4].(**string) = nil
			*dest[5].(**bool) = &falseVal
			*dest[6].(*[]byte) = []byte(`{}`)
			*dest[7].(**uuid.UUID) = nil
			return nil
		}

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{boolRec}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, to, 10)
		res, err := reader.readTemporalHistoryInternal(context.Background(), actor, entity, "private_bool_prop", q)
		if err != nil {
			t.Fatalf("unexpected error for private bool raw: %v", err)
		}
		if res.PointCount != 1 || len(res.TemporalPoints) != 1 {
			t.Fatalf("expected 1 point, got %d", res.PointCount)
		}
		pt := res.TemporalPoints[0]
		if pt.ValueType != "boolean" || pt.ValueBoolean == nil || *pt.ValueBoolean != false {
			t.Errorf("expected boolean point with false, got type=%s val=%v", pt.ValueType, pt.ValueBoolean)
		}
		if pt.ValueNumber != nil || pt.ValueText != nil {
			t.Errorf("other value pointers must be nil, got num=%v text=%v", pt.ValueNumber, pt.ValueText)
		}
	})

	t.Run("private numeric property explicit raw succeeds with fractional value", func(t *testing.T) {
		obsTime := from.Add(1 * time.Second)
		fracVal := 0.75
		numRec := func(dest ...any) error {
			*dest[0].(*uuid.UUID) = entity.InternalID
			*dest[1].(*string) = entity.PublicURN
			*dest[2].(**time.Time) = &obsTime
			*dest[3].(**float64) = &fracVal
			*dest[4].(**string) = nil
			*dest[5].(**bool) = nil
			*dest[6].(*[]byte) = []byte(`{"unit":"ratio"}`)
			*dest[7].(**uuid.UUID) = nil
			return nil
		}

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{numRec}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, to, 10)
		res, err := reader.readTemporalHistoryInternal(context.Background(), actor, entity, "reconstruction_loss", q)
		if err != nil {
			t.Fatalf("unexpected error for private numeric raw with fractional value: %v", err)
		}
		if res.PointCount != 1 || len(res.TemporalPoints) != 1 {
			t.Fatalf("expected 1 point, got %d", res.PointCount)
		}
		pt := res.TemporalPoints[0]
		if pt.ValueType != "number" || pt.ValueNumber == nil || *pt.ValueNumber != 0.75 {
			t.Errorf("expected numeric point with 0.75, got type=%s val=%v", pt.ValueType, pt.ValueNumber)
		}
	})

	t.Run("explicit bucket on private text property returns ErrInvalidResolution", func(t *testing.T) {
		bStart := from
		val := 10.0
		cnt := int64(2)
		textSummary := &TemporalWindowSummary{TotalCount: 2, TextCount: 2}
		rec := makeBucketRecord(entity, &bStart, &val, &val, &val, &cnt, textSummary)

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, to, 10)
		q.Resolution = telemetry.Resolution1s

		res, err := reader.readTemporalHistoryInternal(context.Background(), actor, entity, "private_text_prop", q)
		if !errors.Is(err, ErrInvalidResolution) {
			t.Fatalf("expected ErrInvalidResolution for bucket on text property, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("explicit bucket on private boolean property returns ErrInvalidResolution with null numeric aggregates", func(t *testing.T) {
		bStart := from
		cnt := int64(2)
		boolSummary := &TemporalWindowSummary{TotalCount: 2, BooleanCount: 2}
		// In real SQL, AVG/MIN/MAX of value_number on non-numeric series are NULL.
		rec := makeBucketRecord(entity, &bStart, nil, nil, nil, &cnt, boolSummary)

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, to, 10)
		q.Resolution = telemetry.Resolution1s

		res, err := reader.readTemporalHistoryInternal(context.Background(), actor, entity, "private_bool_prop", q)
		if !errors.Is(err, ErrInvalidResolution) {
			t.Fatalf("expected ErrInvalidResolution for bucket on boolean property, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("auto mode with overflow on private text property returns ErrRawLimitExceeded without bucketing", func(t *testing.T) {
		obsTime := from.Add(1 * time.Second)
		txt := "hello"
		// 3 text records with maxPoints=2
		textRec := func(dest ...any) error {
			*dest[0].(*uuid.UUID) = entity.InternalID
			*dest[1].(*string) = entity.PublicURN
			*dest[2].(**time.Time) = &obsTime
			*dest[3].(**float64) = nil
			*dest[4].(**string) = &txt
			*dest[5].(**bool) = nil
			*dest[6].(*[]byte) = []byte(`{}`)
			*dest[7].(**uuid.UUID) = nil
			return nil
		}

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{textRec, textRec, textRec}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, to, 2) // maxPoints = 2
		q.Resolution = telemetry.ResolutionAuto

		res, err := reader.readTemporalHistoryInternal(context.Background(), actor, entity, "private_text_prop", q)
		if !errors.Is(err, ErrRawLimitExceeded) {
			t.Fatalf("expected ErrRawLimitExceeded for text overflow in auto mode, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("auto mode long window > 10s on private text property returns ErrRawLimitExceeded with null numeric aggregates", func(t *testing.T) {
		bStart := from
		cnt := int64(10)
		textSummary := &TemporalWindowSummary{TotalCount: 10, TextCount: 10}
		// In real SQL, AVG/MIN/MAX of value_number on non-numeric series are NULL.
		rec := makeBucketRecord(entity, &bStart, nil, nil, nil, &cnt, textSummary)

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, from.Add(60*time.Second), 100)
		q.Resolution = telemetry.ResolutionAuto

		res, err := reader.readTemporalHistoryInternal(context.Background(), actor, entity, "private_text_prop", q)
		if !errors.Is(err, ErrRawLimitExceeded) {
			t.Fatalf("expected ErrRawLimitExceeded for long auto window on text property, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("public ReadTemporalHistory strictly rejects private text/bool property names", func(t *testing.T) {
		db := &fakeTwinDatabase{}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, to, 100)
		q.Property = HistorySelector("private_text_prop")

		_, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrInvalidProperty) {
			t.Fatalf("expected ErrInvalidProperty for public ReadTemporalHistory with private property, got %v", err)
		}
	})
}

func TestReadTemporalHistory_EmptyAuthorizedSeries(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)

	sentinelRec := func(dest ...any) error {
		*dest[0].(*uuid.UUID) = entity.InternalID
		*dest[1].(*string) = entity.PublicURN
		*dest[2].(**time.Time) = nil // sentinel: 0 observations
		*dest[3].(**float64) = nil
		*dest[4].(**float64) = nil
		*dest[5].(**float64) = nil
		*dest[6].(**int64) = nil
		*dest[7].(*int64) = 0
		*dest[8].(*int64) = 0
		*dest[9].(*int64) = 0
		*dest[10].(*int64) = 0
		*dest[11].(*int64) = 0
		*dest[12].(*int64) = 0
		*dest[13].(*int64) = 0
		*dest[14].(*int64) = 0
		*dest[15].(*int64) = 0
		*dest[16].(**string) = nil
		return nil
	}

	t.Run("explicit bucket empty series returns mode=bucket and empty slice", func(t *testing.T) {
		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{sentinelRec}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, to, 100)
		q.Resolution = telemetry.Resolution1s

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if err != nil {
			t.Fatalf("unexpected error for empty bucket series: %v", err)
		}
		if res.Mode != "bucket" || res.Resolution != "1s" {
			t.Errorf("expected mode=bucket res=1s, got mode=%s res=%s", res.Mode, res.Resolution)
		}
		if res.PointCount != 0 {
			t.Errorf("expected PointCount 0, got %d", res.PointCount)
		}
		if res.TemporalBuckets == nil || len(res.TemporalBuckets) != 0 {
			t.Errorf("expected non-nil empty slice for TemporalBuckets, got %v", res.TemporalBuckets)
		}
		if res.Unit != nil {
			t.Errorf("expected nil unit for empty series, got %v", *res.Unit)
		}
		if !res.From.Equal(from) || !res.To.Equal(to) {
			t.Errorf("expected range [%v, %v), got [%v, %v)", from, to, res.From, res.To)
		}
		if res.EntityID != entity.PublicURN || res.Property != SelectorSampleIntervalUS {
			t.Errorf("identity/property mismatch: entity=%s prop=%s", res.EntityID, res.Property)
		}
	})

	t.Run("auto long window empty series returns mode=bucket and empty slice", func(t *testing.T) {
		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{sentinelRec}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		longTo := from.Add(60 * time.Second)
		q := makeTestRawQuery(from, longTo, 100)
		q.Resolution = telemetry.ResolutionAuto

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if err != nil {
			t.Fatalf("unexpected error for empty auto long series: %v", err)
		}
		if res.Mode != "bucket" || res.Resolution != "1s" {
			t.Errorf("expected mode=bucket res=1s, got mode=%s res=%s", res.Mode, res.Resolution)
		}
		if res.PointCount != 0 {
			t.Errorf("expected PointCount 0, got %d", res.PointCount)
		}
		if res.TemporalBuckets == nil || len(res.TemporalBuckets) != 0 {
			t.Errorf("expected non-nil empty slice for TemporalBuckets, got %v", res.TemporalBuckets)
		}
		if res.Unit != nil {
			t.Errorf("expected nil unit for empty series, got %v", *res.Unit)
		}
	})
}

func TestReadTemporalHistory_EffectiveRawWindowHandling(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)

	obsTime := from.Add(100 * time.Millisecond)
	val := 10000.0
	rawRec := func(dest ...any) error {
		*dest[0].(*uuid.UUID) = entity.InternalID
		*dest[1].(*string) = entity.PublicURN
		*dest[2].(**time.Time) = &obsTime
		*dest[3].(**float64) = &val
		*dest[4].(**string) = nil
		*dest[5].(**bool) = nil
		*dest[6].(*[]byte) = []byte(`{"unit":"us"}`)
		*dest[7].(**uuid.UUID) = nil
		return nil
	}

	bStart := from
	bAvg := 10000.0
	bMin := 10000.0
	bMax := 10000.0
	bCount := int64(1)
	unitStr := "us"
	sum := &TemporalWindowSummary{
		TotalCount:        1,
		NumberCount:       1,
		DistinctUnitCount: 1,
		MinUnit:           &unitStr,
	}
	bRec := makeBucketRecord(entity, &bStart, &bAvg, &bMin, &bMax, &bCount, sum)

	var lastExecutedQuery string
	db := &fakeTwinDatabase{
		queryFn: func(_ context.Context, query string, _ ...any) (pgx.Rows, error) {
			lastExecutedQuery = query
			if strings.Contains(query, "bounded_observations AS (") {
				return &fakeTwinRows{records: []func(dest ...any) error{rawRec}}, nil
			}
			if strings.Contains(query, "bucket_aggregates AS (") {
				return &fakeTwinRows{records: []func(dest ...any) error{bRec}}, nil
			}
			return &fakeTwinRows{}, nil
		},
	}
	reader, _ := NewPostgresTwinReader(db)

	t.Run("missing/zero EffectiveRawWindow safely defaults to ContractMaxRawWindow (10s)", func(t *testing.T) {
		lastExecutedQuery = ""
		q := makeTestRawQuery(from, from.Add(5*time.Second), 100)
		q.Resolution = telemetry.ResolutionAuto
		q.EffectiveRawWindow = 0 // missing / unset

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Mode != "raw" || res.Resolution != "raw" {
			t.Fatalf("expected 5s auto with missing EffectiveRawWindow to return raw, got mode=%s res=%s", res.Mode, res.Resolution)
		}
		if !strings.Contains(lastExecutedQuery, "bounded_observations AS (") {
			t.Fatalf("expected raw query execution")
		}
	})

	t.Run("configured narrower EffectiveRawWindow (1s) governs auto and raw branching without replacing with 10s", func(t *testing.T) {
		lastExecutedQuery = ""
		q5s := makeTestRawQuery(from, from.Add(5*time.Second), 100)
		q5s.Resolution = telemetry.ResolutionAuto
		q5s.EffectiveRawWindow = 1 * time.Second

		res5s, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q5s)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res5s.Mode != "bucket" {
			t.Fatalf("expected 5s auto with 1s EffectiveRawWindow to select bucket, got mode=%s", res5s.Mode)
		}
		if !strings.Contains(lastExecutedQuery, "bucket_aggregates AS (") {
			t.Fatalf("expected bucket query execution")
		}

		lastExecutedQuery = ""
		q800ms := makeTestRawQuery(from, from.Add(800*time.Millisecond), 100)
		q800ms.Resolution = telemetry.ResolutionAuto
		q800ms.EffectiveRawWindow = 1 * time.Second

		res800ms, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q800ms)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res800ms.Mode != "raw" || res800ms.Resolution != "raw" {
			t.Fatalf("expected 800ms auto with 1s EffectiveRawWindow to return raw, got mode=%s res=%s", res800ms.Mode, res800ms.Resolution)
		}
		if !strings.Contains(lastExecutedQuery, "bounded_observations AS (") {
			t.Fatalf("expected raw query execution")
		}

		q5sRaw := makeTestRawQuery(from, from.Add(5*time.Second), 100)
		q5sRaw.Resolution = telemetry.ResolutionRaw
		q5sRaw.EffectiveRawWindow = 1 * time.Second

		_, errRaw := reader.ReadTemporalHistory(context.Background(), actor, entity, q5sRaw)
		if !errors.Is(errRaw, ErrRawLimitExceeded) {
			t.Fatalf("expected ErrRawLimitExceeded on explicit raw > 1s, got %v", errRaw)
		}
	})

	t.Run("caller cannot enlarge EffectiveRawWindow beyond ContractMaxRawWindow (10s)", func(t *testing.T) {
		lastExecutedQuery = ""
		// Query spanning 15s with caller attempting EffectiveRawWindow = 20s
		q15s := makeTestRawQuery(from, from.Add(15*time.Second), 100)
		q15s.Resolution = telemetry.ResolutionAuto
		q15s.EffectiveRawWindow = 20 * time.Second // exceeds ContractMaxRawWindow

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q15s)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Clamped to 10s: 15s > 10s, so bucket must be selected directly
		if res.Mode != "bucket" {
			t.Fatalf("expected caller-enlarged 20s bound to be clamped to 10s and select bucket for 15s, got mode=%s", res.Mode)
		}
		if !strings.Contains(lastExecutedQuery, "bucket_aggregates AS (") {
			t.Fatalf("expected bucket query execution")
		}
	})

	t.Run("private typed text property in auto mode respects narrower raw window", func(t *testing.T) {
		textVal := "active"
		textRec := func(dest ...any) error {
			*dest[0].(*uuid.UUID) = entity.InternalID
			*dest[1].(*string) = entity.PublicURN
			*dest[2].(**time.Time) = &obsTime
			*dest[3].(**float64) = nil
			*dest[4].(**string) = &textVal
			*dest[5].(**bool) = nil
			*dest[6].(*[]byte) = []byte(`{}`)
			*dest[7].(**uuid.UUID) = nil
			return nil
		}
		textSum := &TemporalWindowSummary{
			TotalCount: 1,
			TextCount:  1,
		}
		bTextRec := makeBucketRecord(entity, &bStart, nil, nil, nil, &bCount, textSum)

		dbText := &fakeTwinDatabase{
			queryFn: func(_ context.Context, query string, _ ...any) (pgx.Rows, error) {
				if strings.Contains(query, "bounded_observations AS (") {
					return &fakeTwinRows{records: []func(dest ...any) error{textRec}}, nil
				}
				if strings.Contains(query, "bucket_aggregates AS (") {
					return &fakeTwinRows{records: []func(dest ...any) error{bTextRec}}, nil
				}
				return &fakeTwinRows{}, nil
			},
		}
		readerText, _ := NewPostgresTwinReader(dbText)

		// 5s auto query with EffectiveRawWindow = 1s:
		// Cannot be bucketed because it is text; falls into bucket branch and returns ErrRawLimitExceeded
		qText5s := makeTestRawQuery(from, from.Add(5*time.Second), 100)
		qText5s.Resolution = telemetry.ResolutionAuto
		qText5s.EffectiveRawWindow = 1 * time.Second

		_, err := readerText.readTemporalHistoryInternal(context.Background(), actor, entity, "status_text", qText5s)
		if !errors.Is(err, ErrRawLimitExceeded) {
			t.Fatalf("expected ErrRawLimitExceeded for private text in auto mode when window > 1s, got %v", err)
		}

		// 800ms auto query with EffectiveRawWindow = 1s:
		// Probes raw and returns raw text points
		qText800ms := makeTestRawQuery(from, from.Add(800*time.Millisecond), 100)
		qText800ms.Resolution = telemetry.ResolutionAuto
		qText800ms.EffectiveRawWindow = 1 * time.Second

		resText, err := readerText.readTemporalHistoryInternal(context.Background(), actor, entity, "status_text", qText800ms)
		if err != nil {
			t.Fatalf("unexpected error for 800ms private text: %v", err)
		}
		if resText.Mode != "raw" || resText.Resolution != "raw" {
			t.Fatalf("expected mode=raw res=raw for 800ms text, got mode=%s res=%s", resText.Mode, resText.Resolution)
		}
	})
}

func TestReadTemporalHistory_FractionalIntervalRegressions(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	entity := makeTestSensorEntity()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	obsTime := from.Add(1 * time.Second)
	unitUS := "us"

	t.Run("raw mode rejects fractional interval within prefix with ErrUnexpected and nil result", func(t *testing.T) {
		fracVal := 10000.5
		fracRec := func(dest ...any) error {
			*dest[0].(*uuid.UUID) = entity.InternalID
			*dest[1].(*string) = entity.PublicURN
			*dest[2].(**time.Time) = &obsTime
			*dest[3].(**float64) = &fracVal
			*dest[4].(**string) = nil
			*dest[5].(**bool) = nil
			*dest[6].(*[]byte) = []byte(`{"unit":"us"}`)
			*dest[7].(**uuid.UUID) = nil
			return nil
		}

		db := &fakeTwinDatabase{rows: &fakeTwinRows{records: []func(dest ...any) error{fracRec}}}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, to, 100)
		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for fractional interval in raw mode, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("raw mode rejects fractional interval beyond bounded raw prefix with ErrUnexpected and nil result", func(t *testing.T) {
		validVal := 10000.0
		summaryWithFracBeyondPrefix := TemporalWindowSummary{
			TotalCount:           50,
			NumberCount:          50,
			InvalidIntervalCount: 1, // fractional observation exists beyond prefix in window!
			DistinctUnitCount:    1,
			MinUnit:              &unitUS,
		}

		obs1 := from.Add(100 * time.Millisecond)
		obs2 := from.Add(200 * time.Millisecond)
		rec1 := makeSummaryRecord(entity, summaryWithFracBeyondPrefix, &obs1, &validVal, nil, nil, []byte(`{"unit":"us"}`), nil)
		rec2 := makeSummaryRecord(entity, summaryWithFracBeyondPrefix, &obs2, &validVal, nil, nil, []byte(`{"unit":"us"}`), nil)

		fakeRows := &fakeTwinRows{records: []func(dest ...any) error{rec1, rec2}}
		db := &fakeTwinDatabase{rows: fakeRows}
		reader, _ := NewPostgresTwinReader(db)

		prefixQ := makeTestRawQuery(from, to, 2)
		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, prefixQ)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for fractional interval beyond prefix, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("auto mode short window rejects fractional interval with ErrUnexpected and nil result", func(t *testing.T) {
		fracVal := 10000.5
		fracRec := func(dest ...any) error {
			*dest[0].(*uuid.UUID) = entity.InternalID
			*dest[1].(*string) = entity.PublicURN
			*dest[2].(**time.Time) = &obsTime
			*dest[3].(**float64) = &fracVal
			*dest[4].(**string) = nil
			*dest[5].(**bool) = nil
			*dest[6].(*[]byte) = []byte(`{"unit":"us"}`)
			*dest[7].(**uuid.UUID) = nil
			return nil
		}

		db := &fakeTwinDatabase{rows: &fakeTwinRows{records: []func(dest ...any) error{fracRec}}}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, to, 100)
		q.Resolution = telemetry.ResolutionAuto

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for fractional interval in auto mode, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("auto mode long window rejects fractional interval in bucket summary with ErrUnexpected and nil result", func(t *testing.T) {
		bStart := from
		avgVal := 10000.0
		minVal := 10000.0
		maxVal := 10000.0
		countVal := int64(10)
		summaryWithFrac := &TemporalWindowSummary{
			TotalCount:           10,
			NumberCount:          10,
			InvalidIntervalCount: 1, // fractional observation in window
			DistinctUnitCount:    1,
			MinUnit:              &unitUS,
		}
		rec := makeBucketRecord(entity, &bStart, &avgVal, &minVal, &maxVal, &countVal, summaryWithFrac)

		db := &fakeTwinDatabase{rows: &fakeTwinRows{records: []func(dest ...any) error{rec}}}
		reader, _ := NewPostgresTwinReader(db)

		longQ := makeTestRawQuery(from, from.Add(60*time.Second), 100)
		longQ.Resolution = telemetry.ResolutionAuto

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, longQ)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for fractional interval in auto long window, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("explicit bucket mode rejects fractional interval with ErrUnexpected and nil result", func(t *testing.T) {
		bStart := from
		avgVal := 10000.0
		minVal := 10000.0
		maxVal := 10000.0
		countVal := int64(5)
		summaryWithFrac := &TemporalWindowSummary{
			TotalCount:           5,
			NumberCount:          5,
			InvalidIntervalCount: 1, // fractional observation in window
			DistinctUnitCount:    1,
			MinUnit:              &unitUS,
		}
		rec := makeBucketRecord(entity, &bStart, &avgVal, &minVal, &maxVal, &countVal, summaryWithFrac)

		db := &fakeTwinDatabase{rows: &fakeTwinRows{records: []func(dest ...any) error{rec}}}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, to, 100)
		q.Resolution = telemetry.Resolution1s

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected for fractional interval in explicit bucket mode, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %+v", res)
		}
	})

	t.Run("explicit bucket mode accepts valid integer observations with fractional AVG", func(t *testing.T) {
		bStart := from
		avgVal := 10000.5 // (10000 + 10001) / 2 = 10000.5
		minVal := 10000.0
		maxVal := 10001.0
		countVal := int64(2)
		summaryValid := &TemporalWindowSummary{
			TotalCount:           2,
			NumberCount:          2,
			InvalidIntervalCount: 0, // all integer observations
			DistinctUnitCount:    1,
			MinUnit:              &unitUS,
		}
		rec := makeBucketRecord(entity, &bStart, &avgVal, &minVal, &maxVal, &countVal, summaryValid)

		db := &fakeTwinDatabase{rows: &fakeTwinRows{records: []func(dest ...any) error{rec}}}
		reader, _ := NewPostgresTwinReader(db)

		q := makeTestRawQuery(from, to, 100)
		q.Resolution = telemetry.Resolution1s

		res, err := reader.ReadTemporalHistory(context.Background(), actor, entity, q)
		if err != nil {
			t.Fatalf("unexpected error for valid integer observations with fractional AVG: %v", err)
		}
		if res.Mode != "bucket" || res.Resolution != "1s" {
			t.Errorf("expected mode=bucket res=1s, got mode=%s res=%s", res.Mode, res.Resolution)
		}
		if res.PointCount != 1 || len(res.TemporalBuckets) != 1 {
			t.Fatalf("expected 1 bucket point, got %d", res.PointCount)
		}
		b := res.TemporalBuckets[0]
		if b.Avg != 10000.5 {
			t.Errorf("expected bucket Avg = 10000.5, got %v", b.Avg)
		}
		if b.Min != 10000.0 || b.Max != 10001.0 || b.Count != 2 {
			t.Errorf("expected Min=10000 Max=10001 Count=2, got Min=%v Max=%v Count=%d", b.Min, b.Max, b.Count)
		}
	})
}

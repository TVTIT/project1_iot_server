package digitaltwin

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"

	"iot-platform/internal/telemetry"
)

type fakeRow struct {
	scanFn func(dest ...any) error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}

type fakeDatabase struct {
	query string
	args  []any
	ctx   context.Context
	row   pgx.Row
	calls int
}

func (d *fakeDatabase) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	d.ctx, d.query, d.args = ctx, query, args
	d.calls++
	return nil, errors.New("Query not implemented")
}

func (d *fakeDatabase) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	d.ctx, d.query, d.args = ctx, query, args
	d.calls++
	if d.row != nil {
		return d.row
	}
	return fakeRow{}
}

func TestNewPostgresResolverRejectsNilDatabase(t *testing.T) {
	var typedNil *fakeDatabase
	for _, db := range []Database{nil, typedNil} {
		if _, err := NewPostgresResolver(db); err == nil {
			t.Fatal("expected error on nil database")
		}
	}

	validDB := &fakeDatabase{}
	resolver, err := NewPostgresResolver(validDB)
	if err != nil {
		t.Fatalf("unexpected error on valid database: %v", err)
	}
	if resolver == nil {
		t.Fatal("expected non-nil resolver")
	}
}

func TestResolveRejectsNilActorBeforeDB(t *testing.T) {
	db := &fakeDatabase{}
	resolver, err := NewPostgresResolver(db)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// 1. ResolveEntity
	parsedGW := ParsedIdentity{
		Type:      EntityTypeGateway,
		GatewayID: "gw1",
		URN:       GatewayURNPrefix + "gw1",
	}
	_, err = resolver.ResolveEntity(ctx, uuid.Nil, parsedGW)
	if !errors.Is(err, ErrNilActor) {
		t.Fatalf("expected ErrNilActor, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls, got: %d", db.calls)
	}

	// 2. ResolveGateway
	_, err = resolver.ResolveGateway(ctx, uuid.Nil, "gw1")
	if !errors.Is(err, ErrNilActor) {
		t.Fatalf("expected ErrNilActor, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls, got: %d", db.calls)
	}

	// 3. ResolveSensor
	_, err = resolver.ResolveSensor(ctx, uuid.Nil, "gw1", "s1")
	if !errors.Is(err, telemetry.ErrHistoryNilActor) {
		t.Fatalf("expected ErrHistoryNilActor, got: %v", err)
	}
	if db.calls != 0 {
		t.Fatalf("expected 0 DB calls, got: %d", db.calls)
	}
}

func TestResolveRejectsInvalidInputBeforeDB(t *testing.T) {
	db := &fakeDatabase{}
	resolver, err := NewPostgresResolver(db)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	actor := uuid.New()

	testCasesEntity := []struct {
		name   string
		parsed ParsedIdentity
	}{
		{
			name: "invalid type",
			parsed: ParsedIdentity{
				Type:      "Unknown",
				GatewayID: "gw1",
				URN:       "urn:ngsi-ld:Unknown:gw1",
			},
		},
		{
			name: "reserved gatewayID",
			parsed: ParsedIdentity{
				Type:      EntityTypeGateway,
				GatewayID: "backend_service",
				URN:       GatewayURNPrefix + "backend_service",
			},
		},
		{
			name: "invalid gatewayID chars",
			parsed: ParsedIdentity{
				Type:      EntityTypeGateway,
				GatewayID: "-invalid",
				URN:       GatewayURNPrefix + "-invalid",
			},
		},
		{
			name: "gateway with sensorID",
			parsed: ParsedIdentity{
				Type:      EntityTypeGateway,
				GatewayID: "gw1",
				SensorID:  "s1",
				URN:       GatewayURNPrefix + "gw1",
			},
		},
		{
			name: "gateway URN mismatch",
			parsed: ParsedIdentity{
				Type:      EntityTypeGateway,
				GatewayID: "gw1",
				URN:       GatewayURNPrefix + "gw2",
			},
		},
		{
			name: "sensor invalid sensorID",
			parsed: ParsedIdentity{
				Type:      EntityTypeSensor,
				GatewayID: "gw1",
				SensorID:  "!bad",
				URN:       SensorURNPrefix + "gw1:!bad",
			},
		},
		{
			name: "sensor URN mismatch",
			parsed: ParsedIdentity{
				Type:      EntityTypeSensor,
				GatewayID: "gw1",
				SensorID:  "s1",
				URN:       SensorURNPrefix + "gw1:s2",
			},
		},
	}

	for _, tc := range testCasesEntity {
		t.Run("Entity/"+tc.name, func(t *testing.T) {
			db.calls = 0
			_, err := resolver.ResolveEntity(ctx, actor, tc.parsed)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("expected ErrInvalidInput, got: %v", err)
			}
			if db.calls != 0 {
				t.Fatalf("expected 0 DB calls, got: %d", db.calls)
			}
		})
	}

	// ResolveGateway invalid input
	for _, badGW := range []string{"", "backend_service", "-bad", "gw:colon", strings.Repeat("a", 65)} {
		t.Run("Gateway/"+badGW, func(t *testing.T) {
			db.calls = 0
			_, err := resolver.ResolveGateway(ctx, actor, badGW)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("expected ErrInvalidInput, got: %v", err)
			}
			if db.calls != 0 {
				t.Fatalf("expected 0 DB calls, got: %d", db.calls)
			}
		})
	}

	// ResolveSensor invalid input
	for _, tc := range []struct {
		gw string
		s  string
	}{
		{"", "s1"},
		{"backend_service", "s1"},
		{"gw1", ""},
		{"gw1", "-invalid"},
		{"gw1", "sensor:colon"},
	} {
		t.Run("Sensor/"+tc.gw+"/"+tc.s, func(t *testing.T) {
			db.calls = 0
			_, err := resolver.ResolveSensor(ctx, actor, tc.gw, tc.s)
			if !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
				t.Fatalf("expected ErrHistoryInvalidInput, got: %v", err)
			}
			if db.calls != 0 {
				t.Fatalf("expected 0 DB calls, got: %d", db.calls)
			}
		})
	}
}

func TestResolveEntityGatewaySuccessAndSQLPredicates(t *testing.T) {
	actor := uuid.New()
	entityUUID := uuid.New()
	gwID := "gw_001"
	expectedURN := GatewayURNPrefix + gwID

	db := &fakeDatabase{
		row: fakeRow{
			scanFn: func(dest ...any) error {
				*dest[0].(*uuid.UUID) = entityUUID
				*dest[1].(*string) = expectedURN
				*dest[2].(*string) = string(EntityTypeGateway)
				*dest[3].(*string) = gwID
				return nil
			},
		},
	}

	resolver, err := NewPostgresResolver(db)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	parsed := ParsedIdentity{
		Type:      EntityTypeGateway,
		GatewayID: gwID,
		URN:       expectedURN,
	}

	res, err := resolver.ResolveEntity(ctx, actor, parsed)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.InternalID != entityUUID {
		t.Errorf("InternalID = %v, want %v", res.InternalID, entityUUID)
	}
	if res.PublicURN != expectedURN {
		t.Errorf("PublicURN = %s, want %s", res.PublicURN, expectedURN)
	}
	if res.Type != EntityTypeGateway {
		t.Errorf("Type = %s, want %s", res.Type, EntityTypeGateway)
	}
	if res.GatewayID != gwID {
		t.Errorf("GatewayID = %s, want %s", res.GatewayID, gwID)
	}
	if res.SensorID != "" {
		t.Errorf("SensorID = %s, want empty", res.SensorID)
	}

	// Verify SQL assertions
	if !strings.Contains(db.query, "public.twin_entities") {
		t.Errorf("query missing twin_entities: %s", db.query)
	}
	if !strings.Contains(db.query, "public.gateways") {
		t.Errorf("query missing gateways: %s", db.query)
	}
	if !strings.Contains(db.query, "public.user_gateways") {
		t.Errorf("query missing user_gateways: %s", db.query)
	}
	if !strings.Contains(db.query, "ug.user_id = $1") {
		t.Errorf("query missing ug.user_id predicate: %s", db.query)
	}
	if strings.Contains(db.query, "FOR UPDATE") {
		t.Errorf("read query must not use FOR UPDATE: %s", db.query)
	}

	// Verify arguments
	if len(db.args) != 3 {
		t.Fatalf("expected 3 arguments, got %d", len(db.args))
	}
	if db.args[0] != actor {
		t.Errorf("arg[0] = %v, want actor %v", db.args[0], actor)
	}
	if db.args[1] != expectedURN {
		t.Errorf("arg[1] = %v, want URN %s", db.args[1], expectedURN)
	}
	if db.args[2] != gwID {
		t.Errorf("arg[2] = %v, want gatewayID %s", db.args[2], gwID)
	}
}

func TestResolveEntitySensorSuccessAndGraphJoins(t *testing.T) {
	actor := uuid.New()
	sensorEntityUUID := uuid.New()
	gwID := "gw_001"
	sensorID := "sensor_temp"
	expectedURN := SensorURNPrefix + gwID + ":" + sensorID
	parentURN := GatewayURNPrefix + gwID

	db := &fakeDatabase{
		row: fakeRow{
			scanFn: func(dest ...any) error {
				*dest[0].(*uuid.UUID) = sensorEntityUUID
				*dest[1].(*string) = expectedURN
				*dest[2].(*string) = string(EntityTypeSensor)
				*dest[3].(*string) = gwID
				*dest[4].(*string) = sensorID
				return nil
			},
		},
	}

	resolver, err := NewPostgresResolver(db)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	parsed := ParsedIdentity{
		Type:      EntityTypeSensor,
		GatewayID: gwID,
		SensorID:  sensorID,
		URN:       expectedURN,
	}

	res, err := resolver.ResolveEntity(ctx, actor, parsed)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.InternalID != sensorEntityUUID {
		t.Errorf("InternalID = %v, want %v", res.InternalID, sensorEntityUUID)
	}
	if res.PublicURN != expectedURN {
		t.Errorf("PublicURN = %s, want %s", res.PublicURN, expectedURN)
	}
	if res.Type != EntityTypeSensor {
		t.Errorf("Type = %s, want %s", res.Type, EntityTypeSensor)
	}
	if res.GatewayID != gwID {
		t.Errorf("GatewayID = %s, want %s", res.GatewayID, gwID)
	}
	if res.SensorID != sensorID {
		t.Errorf("SensorID = %s, want %s", res.SensorID, sensorID)
	}

	// Verify SQL graph assertions
	if !strings.Contains(db.query, "public.sensors") {
		t.Errorf("query missing public.sensors: %s", db.query)
	}
	if !strings.Contains(db.query, "public.twin_relationships") {
		t.Errorf("query missing public.twin_relationships: %s", db.query)
	}
	if !strings.Contains(db.query, "hasSensor") {
		t.Errorf("query missing hasSensor relationship type: %s", db.query)
	}
	if !strings.Contains(db.query, "parent_e") {
		t.Errorf("query missing parent_e Gateway entity join: %s", db.query)
	}
	if !strings.Contains(db.query, "public.user_gateways") {
		t.Errorf("query missing public.user_gateways: %s", db.query)
	}
	if !strings.Contains(db.query, "ug.user_id = $3") {
		t.Errorf("query missing ug.user_id predicate: %s", db.query)
	}
	if strings.Contains(db.query, "FOR UPDATE") {
		t.Errorf("read query must not use FOR UPDATE: %s", db.query)
	}

	// Verify arguments: parsed.SensorID, parentURN, actor, parsed.URN, parsed.GatewayID
	if len(db.args) != 5 {
		t.Fatalf("expected 5 arguments, got %d", len(db.args))
	}
	if db.args[0] != sensorID {
		t.Errorf("arg[0] = %v, want sensorID %s", db.args[0], sensorID)
	}
	if db.args[1] != parentURN {
		t.Errorf("arg[1] = %v, want parentURN %s", db.args[1], parentURN)
	}
	if db.args[2] != actor {
		t.Errorf("arg[2] = %v, want actor %v", db.args[2], actor)
	}
	if db.args[3] != expectedURN {
		t.Errorf("arg[3] = %v, want URN %s", db.args[3], expectedURN)
	}
	if db.args[4] != gwID {
		t.Errorf("arg[4] = %v, want gatewayID %s", db.args[4], gwID)
	}
}

func TestResolveGatewaySuccessAndSQLPredicates(t *testing.T) {
	actor := uuid.New()
	gwEntityUUID := uuid.New()
	gwID := "gw_main"
	expectedURN := GatewayURNPrefix + gwID

	db := &fakeDatabase{
		row: fakeRow{
			scanFn: func(dest ...any) error {
				*dest[0].(*uuid.UUID) = gwEntityUUID
				*dest[1].(*string) = gwID
				return nil
			},
		},
	}

	resolver, err := NewPostgresResolver(db)
	if err != nil {
		t.Fatal(err)
	}

	res, err := resolver.ResolveGateway(context.Background(), actor, gwID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.InternalID != gwEntityUUID {
		t.Errorf("InternalID = %v, want %v", res.InternalID, gwEntityUUID)
	}
	if res.GatewayID != gwID {
		t.Errorf("GatewayID = %s, want %s", res.GatewayID, gwID)
	}

	if !strings.Contains(db.query, "public.user_gateways") {
		t.Errorf("query missing user_gateways: %s", db.query)
	}
	if !strings.Contains(db.query, "ug.user_id = $1") {
		t.Errorf("query missing ug.user_id = $1: %s", db.query)
	}
	if len(db.args) != 3 || db.args[0] != actor || db.args[1] != expectedURN || db.args[2] != gwID {
		t.Errorf("unexpected args: %v", db.args)
	}
}

func TestResolveSensorSuccessAndSQLPredicates(t *testing.T) {
	actor := uuid.New()
	gwID := "gw_001"
	sensorID := "sensor_humidity"
	unitVal := "percent"

	for _, tc := range []struct {
		name string
		unit *string
	}{
		{"with unit", &unitVal},
		{"nullable unit", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &fakeDatabase{
				row: fakeRow{
					scanFn: func(dest ...any) error {
						*dest[0].(*string) = gwID
						*dest[1].(*string) = sensorID
						*dest[2].(**string) = tc.unit
						return nil
					},
				},
			}

			resolver, err := NewPostgresResolver(db)
			if err != nil {
				t.Fatal(err)
			}

			res, err := resolver.ResolveSensor(context.Background(), actor, gwID, sensorID)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if res.GatewayID != gwID {
				t.Errorf("GatewayID = %s, want %s", res.GatewayID, gwID)
			}
			if res.SensorID != sensorID {
				t.Errorf("SensorID = %s, want %s", res.SensorID, sensorID)
			}
			if tc.unit == nil {
				if res.Unit != nil {
					t.Errorf("Unit = %v, want nil", *res.Unit)
				}
			} else {
				if res.Unit == nil || *res.Unit != *tc.unit {
					t.Errorf("Unit = %v, want %v", res.Unit, tc.unit)
				}
			}

			if !strings.Contains(db.query, "public.sensors") {
				t.Errorf("query missing public.sensors: %s", db.query)
			}
			if !strings.Contains(db.query, "public.user_gateways") {
				t.Errorf("query missing public.user_gateways: %s", db.query)
			}
			if !strings.Contains(db.query, "ug.user_id = $1") {
				t.Errorf("query missing ug.user_id predicate: %s", db.query)
			}
			if len(db.args) != 3 || db.args[0] != actor || db.args[1] != gwID || db.args[2] != sensorID {
				t.Errorf("unexpected args: %v", db.args)
			}
		})
	}
}

func TestNotFoundForMissingAndNonMemberAlike(t *testing.T) {
	actor := uuid.New()
	db := &fakeDatabase{
		row: fakeRow{
			scanFn: func(...any) error {
				return pgx.ErrNoRows
			},
		},
	}

	resolver, err := NewPostgresResolver(db)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// 1. ResolveEntity Gateway
	parsedGW := ParsedIdentity{
		Type:      EntityTypeGateway,
		GatewayID: "gw1",
		URN:       GatewayURNPrefix + "gw1",
	}
	_, err = resolver.ResolveEntity(ctx, actor, parsedGW)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing/nonmember gateway entity, got: %v", err)
	}

	// 2. ResolveEntity Sensor
	parsedSensor := ParsedIdentity{
		Type:      EntityTypeSensor,
		GatewayID: "gw1",
		SensorID:  "s1",
		URN:       SensorURNPrefix + "gw1:s1",
	}
	_, err = resolver.ResolveEntity(ctx, actor, parsedSensor)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing/nonmember/orphan sensor entity, got: %v", err)
	}

	// 3. ResolveGateway
	_, err = resolver.ResolveGateway(ctx, actor, "gw1")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing/nonmember gateway, got: %v", err)
	}

	// 4. ResolveSensor
	_, err = resolver.ResolveSensor(ctx, actor, "gw1", "s1")
	if !errors.Is(err, telemetry.ErrHistoryNotFound) {
		t.Fatalf("expected telemetry.ErrHistoryNotFound for missing/nonmember sensor, got: %v", err)
	}
}

func TestDistinctWrappingForScanAndQueryErrors(t *testing.T) {
	actor := uuid.New()
	ctx := context.Background()
	parsedGW := ParsedIdentity{
		Type:      EntityTypeGateway,
		GatewayID: "gw1",
		URN:       GatewayURNPrefix + "gw1",
	}

	testCases := []struct {
		name          string
		cause         error
		isDependency  bool
		isUnexpected  bool
		preserveCause bool
	}{
		{
			name:          "context canceled",
			cause:         context.Canceled,
			isDependency:  true,
			isUnexpected:  false,
			preserveCause: true,
		},
		{
			name:          "context deadline exceeded",
			cause:         context.DeadlineExceeded,
			isDependency:  true,
			isUnexpected:  false,
			preserveCause: true,
		},
		{
			name:          "pgconn closed connection",
			cause:         pgconn.ErrConnClosed,
			isDependency:  true,
			isUnexpected:  false,
			preserveCause: true,
		},
		{
			name:          "puddle closed pool",
			cause:         puddle.ErrClosedPool,
			isDependency:  true,
			isUnexpected:  false,
			preserveCause: true,
		},
		{
			name:          "io.EOF",
			cause:         io.EOF,
			isDependency:  true,
			isUnexpected:  false,
			preserveCause: true,
		},
		{
			name:          "io.ErrUnexpectedEOF",
			cause:         io.ErrUnexpectedEOF,
			isDependency:  true,
			isUnexpected:  false,
			preserveCause: true,
		},
		{
			name:          "network op error",
			cause:         &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET},
			isDependency:  true,
			isUnexpected:  false,
			preserveCause: true,
		},
		{
			name:          "server shutdown 57P01",
			cause:         &pgconn.PgError{Code: "57P01"},
			isDependency:  true,
			isUnexpected:  false,
			preserveCause: true,
		},
		{
			name:          "connection failure 08006",
			cause:         &pgconn.PgError{Code: "08006"},
			isDependency:  true,
			isUnexpected:  false,
			preserveCause: true,
		},
		{
			name:          "scan error",
			cause:         &pgx.ScanArgError{ColumnIndex: 0, Err: errors.New("bad scan conversion")},
			isDependency:  false,
			isUnexpected:  true,
			preserveCause: true,
		},
		{
			name:          "SQL syntax error 42601",
			cause:         &pgconn.PgError{Code: "42601"},
			isDependency:  false,
			isUnexpected:  true,
			preserveCause: true,
		},
		{
			name:          "arbitrary error",
			cause:         errors.New("some arbitrary DB fault"),
			isDependency:  false,
			isUnexpected:  true,
			preserveCause: true,
		},
	}

	for _, tc := range testCases {
		t.Run("Entity/"+tc.name, func(t *testing.T) {
			db := &fakeDatabase{
				row: fakeRow{
					scanFn: func(...any) error {
						return tc.cause
					},
				},
			}
			resolver, err := NewPostgresResolver(db)
			if err != nil {
				t.Fatal(err)
			}

			_, err = resolver.ResolveEntity(ctx, actor, parsedGW)
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if tc.isDependency && !errors.Is(err, ErrDependencyUnavailable) {
				t.Errorf("expected ErrDependencyUnavailable, got: %v", err)
			}
			if tc.isUnexpected && !errors.Is(err, ErrUnexpected) {
				t.Errorf("expected ErrUnexpected, got: %v", err)
			}
			if tc.preserveCause && !errors.Is(err, tc.cause) {
				t.Errorf("expected errors.Is cause %v, got: %v", tc.cause, err)
			}
		})

		t.Run("Gateway/"+tc.name, func(t *testing.T) {
			db := &fakeDatabase{
				row: fakeRow{
					scanFn: func(...any) error {
						return tc.cause
					},
				},
			}
			resolver, err := NewPostgresResolver(db)
			if err != nil {
				t.Fatal(err)
			}

			_, err = resolver.ResolveGateway(ctx, actor, "gw1")
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if tc.isDependency && !errors.Is(err, ErrDependencyUnavailable) {
				t.Errorf("expected ErrDependencyUnavailable, got: %v", err)
			}
			if tc.isUnexpected && !errors.Is(err, ErrUnexpected) {
				t.Errorf("expected ErrUnexpected, got: %v", err)
			}
			if tc.preserveCause && !errors.Is(err, tc.cause) {
				t.Errorf("expected errors.Is cause %v, got: %v", tc.cause, err)
			}
		})

		t.Run("Sensor/"+tc.name, func(t *testing.T) {
			db := &fakeDatabase{
				row: fakeRow{
					scanFn: func(...any) error {
						return tc.cause
					},
				},
			}
			resolver, err := NewPostgresResolver(db)
			if err != nil {
				t.Fatal(err)
			}

			_, err = resolver.ResolveSensor(ctx, actor, "gw1", "s1")
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if tc.isDependency && !errors.Is(err, telemetry.ErrHistoryDependency) {
				t.Errorf("expected telemetry.ErrHistoryDependency, got: %v", err)
			}
			if tc.isUnexpected && !errors.Is(err, telemetry.ErrHistoryUnexpected) {
				t.Errorf("expected telemetry.ErrHistoryUnexpected, got: %v", err)
			}
			if tc.preserveCause && !errors.Is(err, tc.cause) {
				t.Errorf("expected errors.Is cause %v, got: %v", tc.cause, err)
			}
		})
	}
}

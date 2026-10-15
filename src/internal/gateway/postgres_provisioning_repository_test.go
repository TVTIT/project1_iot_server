package gateway

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type provisioningBeginFunc func(context.Context, pgx.TxOptions) (pgx.Tx, error)

func (f provisioningBeginFunc) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	return f(ctx, options)
}

func TestPostgresProvisioningConstructorAndSensorValidation(t *testing.T) {
	var typedNil *provisioningBeginFunc
	for _, db := range []ProvisioningDatabase{nil, typedNil} {
		if _, err := NewPostgresProvisioningRepository(db); err == nil {
			t.Fatal("accepted missing database")
		}
	}
	repo, err := NewPostgresProvisioningRepository(provisioningBeginFunc(func(context.Context, pgx.TxOptions) (pgx.Tx, error) {
		t.Fatal("invalid Sensor must not start transaction")
		return nil, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := repo.ProvisionSensor(context.Background(), uuid.New(), ProvisionSensorInput{GatewayID: "fixture", SensorID: "fixture"}); !errors.Is(err, ErrInvalidProvisioningInput) || got.Created {
		t.Fatalf("Sensor not fail closed: %#v %v", got, err)
	}
}

func TestPostgresProvisioningValidationAndIsolation(t *testing.T) {
	wantErr := errors.New("begin unavailable")
	calls := 0
	repo, err := NewPostgresProvisioningRepository(provisioningBeginFunc(func(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
		calls++
		if options.IsoLevel != pgx.ReadCommitted {
			t.Fatalf("isolation: %v", options)
		}
		return nil, wantErr
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ProvisionGateway(context.Background(), uuid.Nil, ProvisionGatewayInput{}); err == nil || calls != 0 {
		t.Fatal("invalid input touched database")
	}
	if _, err := repo.ProvisionGateway(context.Background(), uuid.New(), ProvisionGatewayInput{GatewayID: "test_gateway", Name: "test", OwnerUserID: uuid.New()}); !errors.Is(err, wantErr) || calls != 1 {
		t.Fatalf("begin failure: %v", err)
	}
	if _, err := repo.ProvisionSensor(context.Background(), uuid.New(), ProvisionSensorInput{GatewayID: "test_gateway", SensorID: "backend_service", Name: "test"}); !errors.Is(err, wantErr) || calls != 2 {
		t.Fatalf("Sensor begin failure: %v", err)
	}

	ownerRepo, err := NewPostgresOwnerSensorProvisioningRepository(provisioningBeginFunc(func(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
		calls++
		if options.IsoLevel != pgx.ReadCommitted {
			t.Fatalf("isolation: %v", options)
		}
		return nil, wantErr
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ownerRepo.ProvisionSensorForOwner(context.Background(), uuid.Nil, ProvisionSensorInput{}); err == nil || calls != 2 {
		t.Fatal("invalid input touched database for owner")
	}
	if _, err := ownerRepo.ProvisionSensorForOwner(context.Background(), uuid.New(), ProvisionSensorInput{GatewayID: "test_gateway", SensorID: "test_sensor", Name: "test"}); !errors.Is(err, wantErr) || calls != 3 {
		t.Fatalf("owner Sensor begin failure: %v", err)
	}
	if _, err := NewPostgresOwnerSensorProvisioningRepository(nil); err == nil {
		t.Fatal("accepted nil database")
	}
}

type fakeOwnerSensorTxDatabase struct {
	parentNotFound bool
	parentErr      error
	parentName     string
	roleNotFound   bool
	roleErr        error
	role           Role
}

func (d *fakeOwnerSensorTxDatabase) BeginTx(_ context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	return &fakeOwnerSensorTx{db: d}, nil
}

type fakeOwnerSensorTx struct {
	pgx.Tx
	db *fakeOwnerSensorTxDatabase
}

func (tx *fakeOwnerSensorTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	if query == `SELECT name FROM public.gateways WHERE gateway_id=$1 FOR UPDATE` {
		if tx.db.parentNotFound {
			return fakeRow{err: pgx.ErrNoRows}
		}
		if tx.db.parentErr != nil {
			return fakeRow{err: tx.db.parentErr}
		}
		return fakeScanRow{val: tx.db.parentName}
	}
	if query == `SELECT role FROM public.user_gateways WHERE user_id=$1 AND gateway_id=$2 FOR SHARE` {
		if tx.db.roleNotFound {
			return fakeRow{err: pgx.ErrNoRows}
		}
		if tx.db.roleErr != nil {
			return fakeRow{err: tx.db.roleErr}
		}
		return fakeScanRow{val: string(tx.db.role)}
	}
	return fakeRow{err: errors.New("unexpected query")}
}

func (tx *fakeOwnerSensorTx) Rollback(_ context.Context) error {
	return nil
}

type fakeScanRow struct {
	val any
}

func (r fakeScanRow) Scan(dest ...any) error {
	if len(dest) > 0 {
		switch v := dest[0].(type) {
		case *string:
			if s, ok := r.val.(string); ok {
				*v = s
			}
		case *Role:
			if s, ok := r.val.(string); ok {
				*v = Role(s)
			}
		}
	}
	return nil
}

func TestPostgresOwnerSensorProvisioningUnitAuthorizationBranches(t *testing.T) {
	ctx := context.Background()
	actor := uuid.New()
	in := ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: "temp"}

	t.Run("parent not found", func(t *testing.T) {
		db := &fakeOwnerSensorTxDatabase{parentNotFound: true}
		repo, _ := NewPostgresOwnerSensorProvisioningRepository(db)
		_, err := repo.ProvisionSensorForOwner(ctx, actor, in)
		if !errors.Is(err, ErrProvisioningNotFound) {
			t.Fatalf("expected ErrProvisioningNotFound, got %v", err)
		}
	})

	t.Run("parent query error", func(t *testing.T) {
		db := &fakeOwnerSensorTxDatabase{parentErr: errors.New("parent query failed")}
		repo, _ := NewPostgresOwnerSensorProvisioningRepository(db)
		_, err := repo.ProvisionSensorForOwner(ctx, actor, in)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("membership not found", func(t *testing.T) {
		db := &fakeOwnerSensorTxDatabase{parentName: "gw1", roleNotFound: true}
		repo, _ := NewPostgresOwnerSensorProvisioningRepository(db)
		_, err := repo.ProvisionSensorForOwner(ctx, actor, in)
		if !errors.Is(err, ErrProvisioningNotFound) {
			t.Fatalf("expected ErrProvisioningNotFound, got %v", err)
		}
	})

	t.Run("membership lookup failure", func(t *testing.T) {
		db := &fakeOwnerSensorTxDatabase{parentName: "gw1", roleErr: errors.New("membership query failed")}
		repo, _ := NewPostgresOwnerSensorProvisioningRepository(db)
		_, err := repo.ProvisionSensorForOwner(ctx, actor, in)
		if !errors.Is(err, db.roleErr) || errors.Is(err, ErrOwnerProvisioningUnavailable) || errors.Is(err, ErrProvisioningAdminLookup) {
			t.Fatalf("expected preserved logical membership error without availability tag, got %v", err)
		}
	})

	t.Run("role not owner (operator/viewer)", func(t *testing.T) {
		db := &fakeOwnerSensorTxDatabase{parentName: "gw1", role: RoleOperator}
		repo, _ := NewPostgresOwnerSensorProvisioningRepository(db)
		_, err := repo.ProvisionSensorForOwner(ctx, actor, in)
		if !errors.Is(err, ErrProvisioningForbidden) {
			t.Fatalf("expected ErrProvisioningForbidden for operator, got %v", err)
		}

		db.role = RoleViewer
		_, err = repo.ProvisionSensorForOwner(ctx, actor, in)
		if !errors.Is(err, ErrProvisioningForbidden) {
			t.Fatalf("expected ErrProvisioningForbidden for viewer, got %v", err)
		}
	})
}

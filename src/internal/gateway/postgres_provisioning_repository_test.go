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
}

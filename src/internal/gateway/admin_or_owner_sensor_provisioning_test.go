package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

type adminOrOwnerSensorProvisionerRepoFunc func(context.Context, uuid.UUID, ProvisionSensorInput) (ProvisionSensorResult, error)

func (f adminOrOwnerSensorProvisionerRepoFunc) ProvisionSensorForAdminOrOwner(ctx context.Context, actor uuid.UUID, input ProvisionSensorInput) (ProvisionSensorResult, error) {
	return f(ctx, actor, input)
}

func TestAdminOrOwnerSensorProvisioningServiceValidation(t *testing.T) {
	actor := uuid.New()
	validRepo := adminOrOwnerSensorProvisionerRepoFunc(func(_ context.Context, _ uuid.UUID, _ ProvisionSensorInput) (ProvisionSensorResult, error) {
		return ProvisionSensorResult{Created: true}, nil
	})

	if _, err := NewAdminOrOwnerSensorProvisioningService(nil, time.Second); err == nil {
		t.Fatal("expected error on nil repo")
	}
	if _, err := NewAdminOrOwnerSensorProvisioningService(validRepo, 0); err == nil {
		t.Fatal("expected error on zero timeout")
	}
	if _, err := NewAdminOrOwnerSensorProvisioningService(validRepo, -time.Second); err == nil {
		t.Fatal("expected error on negative timeout")
	}

	svc, err := NewAdminOrOwnerSensorProvisioningService(validRepo, 2*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Nil actor
	if _, err := svc.ProvisionSensorForAdminOrOwner(context.Background(), uuid.Nil, ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: "temp"}); !errors.Is(err, ErrInvalidProvisioningInput) {
		t.Fatalf("expected ErrInvalidProvisioningInput on nil actor, got %v", err)
	}

	// Invalid gateway ID
	if _, err := svc.ProvisionSensorForAdminOrOwner(context.Background(), actor, ProvisionSensorInput{GatewayID: "invalid*gw", SensorID: "s1", Name: "temp"}); !errors.Is(err, ErrInvalidGatewayID) {
		t.Fatalf("expected ErrInvalidGatewayID, got %v", err)
	}

	// Invalid sensor ID
	if _, err := svc.ProvisionSensorForAdminOrOwner(context.Background(), actor, ProvisionSensorInput{GatewayID: "gw1", SensorID: "invalid*s", Name: "temp"}); !errors.Is(err, ErrInvalidSensorID) {
		t.Fatalf("expected ErrInvalidSensorID, got %v", err)
	}

	// Empty name
	if _, err := svc.ProvisionSensorForAdminOrOwner(context.Background(), actor, ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: "   "}); !errors.Is(err, ErrInvalidProvisioningInput) {
		t.Fatalf("expected ErrInvalidProvisioningInput on whitespace name, got %v", err)
	}

	// Unit exceeding maximum allowed length (64 bytes)
	tooLongUnit := string(make([]byte, 65))
	if _, err := svc.ProvisionSensorForAdminOrOwner(context.Background(), actor, ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: "temp", Unit: &tooLongUnit}); !errors.Is(err, ErrInvalidProvisioningInput) {
		t.Fatalf("expected ErrInvalidProvisioningInput on unit > 64 bytes, got %v", err)
	}

	// Context timeout propagation
	timeoutRepo := adminOrOwnerSensorProvisionerRepoFunc(func(ctx context.Context, _ uuid.UUID, _ ProvisionSensorInput) (ProvisionSensorResult, error) {
		<-ctx.Done()
		return ProvisionSensorResult{}, ctx.Err()
	})
	timeoutSvc, err := NewAdminOrOwnerSensorProvisioningService(timeoutRepo, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := timeoutSvc.ProvisionSensorForAdminOrOwner(context.Background(), actor, ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: "temp"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	// Successful delegation
	res, err := svc.ProvisionSensorForAdminOrOwner(context.Background(), actor, ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: "temp"})
	if err != nil || !res.Created {
		t.Fatalf("expected success, got res=%+v err=%v", res, err)
	}
}

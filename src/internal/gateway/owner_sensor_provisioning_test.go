package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

type ownerSensorProvisionerRepoFunc func(context.Context, uuid.UUID, ProvisionSensorInput) (ProvisionSensorResult, error)

func (f ownerSensorProvisionerRepoFunc) ProvisionSensorForOwner(ctx context.Context, actor uuid.UUID, input ProvisionSensorInput) (ProvisionSensorResult, error) {
	return f(ctx, actor, input)
}

func TestOwnerSensorProvisioningServiceValidation(t *testing.T) {
	actor := uuid.New()
	validRepo := ownerSensorProvisionerRepoFunc(func(_ context.Context, _ uuid.UUID, _ ProvisionSensorInput) (ProvisionSensorResult, error) {
		return ProvisionSensorResult{Created: true}, nil
	})

	if _, err := NewOwnerSensorProvisioningService(nil, time.Second); err == nil {
		t.Fatal("expected error on nil repo")
	}
	if _, err := NewOwnerSensorProvisioningService(validRepo, 0); err == nil {
		t.Fatal("expected error on zero timeout")
	}

	svc, err := NewOwnerSensorProvisioningService(validRepo, 2*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Nil actor
	if _, err := svc.ProvisionSensorForOwner(context.Background(), uuid.Nil, ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: "temp"}); !errors.Is(err, ErrInvalidProvisioningInput) {
		t.Fatalf("expected ErrInvalidProvisioningInput on nil actor, got %v", err)
	}

	// Invalid gateway ID
	if _, err := svc.ProvisionSensorForOwner(context.Background(), actor, ProvisionSensorInput{GatewayID: "invalid*gw", SensorID: "s1", Name: "temp"}); !errors.Is(err, ErrInvalidGatewayID) {
		t.Fatalf("expected ErrInvalidGatewayID, got %v", err)
	}

	// Invalid sensor ID
	if _, err := svc.ProvisionSensorForOwner(context.Background(), actor, ProvisionSensorInput{GatewayID: "gw1", SensorID: "invalid*s", Name: "temp"}); !errors.Is(err, ErrInvalidSensorID) {
		t.Fatalf("expected ErrInvalidSensorID, got %v", err)
	}

	// Empty name
	if _, err := svc.ProvisionSensorForOwner(context.Background(), actor, ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: "   "}); !errors.Is(err, ErrInvalidProvisioningInput) {
		t.Fatalf("expected ErrInvalidProvisioningInput on whitespace name, got %v", err)
	}

	// Successful delegation
	res, err := svc.ProvisionSensorForOwner(context.Background(), actor, ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: "temp"})
	if err != nil || !res.Created {
		t.Fatalf("expected success, got res=%+v err=%v", res, err)
	}
}

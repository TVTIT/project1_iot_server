package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

type provisioningRepoFake struct {
	calls       int
	actor       uuid.UUID
	input       ProvisionGatewayInput
	sensorInput ProvisionSensorInput
}

func (r *provisioningRepoFake) ProvisionGateway(ctx context.Context, a uuid.UUID, i ProvisionGatewayInput) (ProvisionGatewayResult, error) {
	r.calls++
	r.actor = a
	r.input = i
	if _, ok := ctx.Deadline(); !ok {
		return ProvisionGatewayResult{}, errors.New("missing deadline")
	}
	<-ctx.Done()
	return ProvisionGatewayResult{}, ctx.Err()
}
func (r *provisioningRepoFake) ProvisionSensor(ctx context.Context, actor uuid.UUID, input ProvisionSensorInput) (ProvisionSensorResult, error) {
	r.calls++
	r.actor = actor
	r.sensorInput = input
	if _, ok := ctx.Deadline(); !ok {
		return ProvisionSensorResult{}, errors.New("missing deadline")
	}
	<-ctx.Done()
	return ProvisionSensorResult{}, ctx.Err()
}

func TestProvisioningServiceSensorValidationAndDeadline(t *testing.T) {
	repo := &provisioningRepoFake{}
	s, err := NewProvisioningService(repo, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()
	unit := ""
	input := ProvisionSensorInput{GatewayID: "fixture", SensorID: "backend_service", Name: " raw ", Unit: &unit}
	invalid := input
	invalid.Name = " "
	if _, err := s.ProvisionSensor(context.Background(), actor, invalid); !errors.Is(err, ErrInvalidProvisioningInput) || repo.calls != 0 {
		t.Fatal("invalid Sensor reached repository")
	}
	if _, err := s.ProvisionSensor(context.Background(), actor, input); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Sensor deadline: %v", err)
	}
	if repo.actor != actor || repo.sensorInput != input {
		t.Fatal("Sensor values changed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ProvisionSensor(ctx, actor, input); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sensor cancellation: %v", err)
	}
}

func TestProvisioningServiceValidationAndDeadline(t *testing.T) {
	repo := &provisioningRepoFake{}
	s, err := NewProvisioningService(repo, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()
	input := ProvisionGatewayInput{GatewayID: "fixture", Name: " raw ", OwnerUserID: uuid.New()}
	invalid := input
	invalid.Name = " "
	if _, err := s.ProvisionGateway(context.Background(), actor, invalid); !errors.Is(err, ErrInvalidProvisioningInput) || repo.calls != 0 {
		t.Fatal("invalid input reached repository")
	}
	if _, err := s.ProvisionGateway(context.Background(), actor, input); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline error=%v", err)
	}
	if repo.actor != actor || repo.input.Name != input.Name {
		t.Fatal("values changed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ProvisionGateway(ctx, actor, input); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
}
func TestProvisioningServiceRequiresDependencies(t *testing.T) {
	var typedNil *provisioningRepoFake
	for _, repo := range []ProvisioningRepository{nil, typedNil} {
		if _, err := NewProvisioningService(repo, time.Second); err == nil {
			t.Fatal("nil accepted")
		}
	}
	if _, err := NewProvisioningService(&provisioningRepoFake{}, 0); err == nil {
		t.Fatal("invalid timeout accepted")
	}
}

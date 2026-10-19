package digitaltwin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

type fakeGatewayProvisioner struct {
	calls      int
	lastActor  uuid.UUID
	lastInput  gateway.ProvisionGatewayInput
	stubResult gateway.ProvisionGatewayResult
	stubErr    error
}

func (f *fakeGatewayProvisioner) ProvisionGateway(_ context.Context, actor uuid.UUID, input gateway.ProvisionGatewayInput) (gateway.ProvisionGatewayResult, error) {
	f.calls++
	f.lastActor = actor
	f.lastInput = input
	return f.stubResult, f.stubErr
}

type fakeSensorProvisioner struct {
	calls      int
	lastActor  uuid.UUID
	lastInput  gateway.ProvisionSensorInput
	stubResult gateway.ProvisionSensorResult
	stubErr    error
}

func (f *fakeSensorProvisioner) ProvisionSensorForAdminOrOwner(_ context.Context, actor uuid.UUID, input gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error) {
	f.calls++
	f.lastActor = actor
	f.lastInput = input
	return f.stubResult, f.stubErr
}

func TestNewCreateService_DependencyValidation(t *testing.T) {
	gw := &fakeGatewayProvisioner{}
	sensor := &fakeSensorProvisioner{}

	var typedNilGw *fakeGatewayProvisioner
	var typedNilSensor *fakeSensorProvisioner

	cases := []struct {
		name    string
		gw      GatewayProvisioner
		sensor  SensorProvisioner
		wantErr bool
	}{
		{"nil gateway provisioner", nil, sensor, true},
		{"typed-nil gateway provisioner", typedNilGw, sensor, true},
		{"nil sensor provisioner", gw, nil, true},
		{"typed-nil sensor provisioner", gw, typedNilSensor, true},
		{"both valid", gw, sensor, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, err := NewCreateService(tc.gw, tc.sensor)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if svc != nil {
					t.Fatal("expected nil service on error")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if svc == nil {
					t.Fatal("expected non-nil service")
				}
			}
		})
	}
}

func TestCreateEntity_ReceiverAndContextValidation(t *testing.T) {
	actor := uuid.New()
	owner := uuid.New()
	validGWInput := CreateEntityInput{
		Type: EntityTypeGateway,
		Gateway: &gateway.ProvisionGatewayInput{
			GatewayID:   "gw1",
			Name:        "GW 1",
			OwnerUserID: owner,
		},
	}

	t.Run("nil receiver returns ErrDependencyUnavailable", func(t *testing.T) {
		var s *createService
		_, err := s.CreateEntity(context.Background(), actor, validGWInput)
		if !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable, got: %v", err)
		}
	})

	t.Run("nil dependencies return ErrDependencyUnavailable", func(t *testing.T) {
		gw := &fakeGatewayProvisioner{}
		sensor := &fakeSensorProvisioner{}

		s1 := &createService{gatewayProvisioner: nil, sensorProvisioner: sensor}
		if _, err := s1.CreateEntity(context.Background(), actor, validGWInput); !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable, got: %v", err)
		}

		s2 := &createService{gatewayProvisioner: gw, sensorProvisioner: nil}
		if _, err := s2.CreateEntity(context.Background(), actor, validGWInput); !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable, got: %v", err)
		}
	})

	t.Run("nil context returns ErrInvalidInput", func(t *testing.T) {
		gw := &fakeGatewayProvisioner{}
		sensor := &fakeSensorProvisioner{}
		svc, _ := NewCreateService(gw, sensor)

		var nilCtx context.Context
		_, err := svc.CreateEntity(nilCtx, actor, validGWInput)
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput, got: %v", err)
		}
		if gw.calls != 0 || sensor.calls != 0 {
			t.Fatal("provisioners must not be invoked on nil context")
		}
	})

	t.Run("cancelled context before call returns context.Canceled", func(t *testing.T) {
		gw := &fakeGatewayProvisioner{}
		sensor := &fakeSensorProvisioner{}
		svc, _ := NewCreateService(gw, sensor)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := svc.CreateEntity(ctx, actor, validGWInput)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
		if gw.calls != 0 || sensor.calls != 0 {
			t.Fatal("provisioners must not be invoked on cancelled context")
		}
	})
}

func TestCreateEntity_InputValidationDoesNotInvokeProvisioners(t *testing.T) {
	gw := &fakeGatewayProvisioner{}
	sensor := &fakeSensorProvisioner{}
	svc, _ := NewCreateService(gw, sensor)
	ctx := context.Background()
	owner := uuid.New()

	cases := []struct {
		name    string
		actor   uuid.UUID
		input   CreateEntityInput
		wantErr error
	}{
		{
			name:  "nil actor returns ErrNilActor",
			actor: uuid.Nil,
			input: CreateEntityInput{
				Type: EntityTypeGateway,
				Gateway: &gateway.ProvisionGatewayInput{
					GatewayID:   "gw1",
					Name:        "GW 1",
					OwnerUserID: owner,
				},
			},
			wantErr: ErrNilActor,
		},
		{
			name:  "unsupported type returns ErrInvalidInput",
			actor: uuid.New(),
			input: CreateEntityInput{
				Type: "Device",
				Gateway: &gateway.ProvisionGatewayInput{
					GatewayID:   "gw1",
					Name:        "GW 1",
					OwnerUserID: owner,
				},
			},
			wantErr: ErrInvalidInput,
		},
		{
			name:  "inconsistent tagged branches returns ErrInvalidInput",
			actor: uuid.New(),
			input: CreateEntityInput{
				Type: EntityTypeGateway,
				Gateway: &gateway.ProvisionGatewayInput{
					GatewayID:   "gw1",
					Name:        "GW 1",
					OwnerUserID: owner,
				},
				Sensor: &gateway.ProvisionSensorInput{
					GatewayID: "gw1",
					SensorID:  "s1",
					Name:      "Sensor",
				},
			},
			wantErr: ErrInvalidInput,
		},
		{
			name:  "invalid gateway fields returns ErrInvalidInput",
			actor: uuid.New(),
			input: CreateEntityInput{
				Type: EntityTypeGateway,
				Gateway: &gateway.ProvisionGatewayInput{
					GatewayID:   "INVALID_ID!",
					Name:        "GW 1",
					OwnerUserID: owner,
				},
			},
			wantErr: ErrInvalidInput,
		},
		{
			name:  "invalid sensor fields returns ErrInvalidInput",
			actor: uuid.New(),
			input: CreateEntityInput{
				Type: EntityTypeSensor,
				Sensor: &gateway.ProvisionSensorInput{
					GatewayID: "gw1",
					SensorID:  "INVALID!",
					Name:      "Sensor",
				},
			},
			wantErr: ErrInvalidInput,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gw.calls = 0
			sensor.calls = 0
			res, err := svc.CreateEntity(ctx, tc.actor, tc.input)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got: %v", tc.wantErr, err)
			}
			if res != (CreateEntityResult{}) {
				t.Fatalf("expected empty result on validation error, got: %+v", res)
			}
			if gw.calls != 0 || sensor.calls != 0 {
				t.Fatalf("provisioners must not be invoked on validation error: gw=%d sensor=%d", gw.calls, sensor.calls)
			}
		})
	}
}

func TestCreateEntity_GatewayDispatchAndOutcomes(t *testing.T) {
	ctx := context.Background()
	actor := uuid.New()
	targetOwner := uuid.New()
	desc := "A test gateway"
	now := time.Now().UTC()

	in := CreateEntityInput{
		Type: EntityTypeGateway,
		Gateway: &gateway.ProvisionGatewayInput{
			GatewayID:   "gw_dispatch_test",
			Name:        "Dispatch Test GW",
			Description: &desc,
			OwnerUserID: targetOwner,
		},
	}

	t.Run("creation outcome created=true", func(t *testing.T) {
		gw := &fakeGatewayProvisioner{
			stubResult: gateway.ProvisionGatewayResult{
				Created:     true,
				GatewayID:   "gw_dispatch_test",
				Name:        "Dispatch Test GW",
				Description: &desc,
				OwnerUserID: targetOwner,
				EntityID:    "urn:ngsi-ld:Gateway:gw_dispatch_test",
				CreatedAt:   &now,
			},
		}
		sensor := &fakeSensorProvisioner{}
		svc, _ := NewCreateService(gw, sensor)

		res, err := svc.CreateEntity(ctx, actor, in)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gw.calls != 1 {
			t.Fatalf("expected 1 gateway provisioner call, got %d", gw.calls)
		}
		if sensor.calls != 0 {
			t.Fatalf("sensor provisioner must not be called, got %d", sensor.calls)
		}
		if gw.lastActor != actor {
			t.Fatalf("expected actor %v passed to provisioner, got %v", actor, gw.lastActor)
		}
		if gw.lastActor == targetOwner {
			t.Fatal("actor must be separate from target owner_user_id")
		}

		if !res.Created {
			t.Fatal("expected Created=true")
		}
		if res.Type != EntityTypeGateway {
			t.Fatalf("expected Type=EntityTypeGateway, got %v", res.Type)
		}
		if res.EntityID != "urn:ngsi-ld:Gateway:gw_dispatch_test" {
			t.Fatalf("expected EntityID urn:ngsi-ld:Gateway:gw_dispatch_test, got %v", res.EntityID)
		}
		if res.Gateway == nil || res.Sensor != nil {
			t.Fatal("expected non-nil Gateway and nil Sensor")
		}
		if res.Gateway.Description == nil || *res.Gateway.Description != desc {
			t.Fatalf("expected description %q, got %v", desc, res.Gateway.Description)
		}
		if res.Gateway.CreatedAt == nil || *res.Gateway.CreatedAt != now {
			t.Fatalf("expected CreatedAt %v, got %v", now, res.Gateway.CreatedAt)
		}
	})

	t.Run("exact replay outcome created=false", func(t *testing.T) {
		gw := &fakeGatewayProvisioner{
			stubResult: gateway.ProvisionGatewayResult{
				Created:     false,
				GatewayID:   "gw_dispatch_test",
				Name:        "Dispatch Test GW",
				Description: &desc,
				OwnerUserID: targetOwner,
				EntityID:    "urn:ngsi-ld:Gateway:gw_dispatch_test",
				CreatedAt:   &now,
			},
		}
		sensor := &fakeSensorProvisioner{}
		svc, _ := NewCreateService(gw, sensor)

		res, err := svc.CreateEntity(ctx, actor, in)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Created {
			t.Fatal("expected Created=false on replay")
		}
		if res.EntityID != "urn:ngsi-ld:Gateway:gw_dispatch_test" {
			t.Fatalf("expected EntityID urn:ngsi-ld:Gateway:gw_dispatch_test, got %v", res.EntityID)
		}
		if res.Gateway.CreatedAt == nil || *res.Gateway.CreatedAt != now {
			t.Fatalf("expected preserved CreatedAt %v, got %v", now, res.Gateway.CreatedAt)
		}
	})

	t.Run("error propagation preserves causes and returns empty result", func(t *testing.T) {
		errCases := []struct {
			name    string
			stubErr error
		}{
			{"forbidden", gateway.ErrProvisioningForbidden},
			{"not found", gateway.ErrProvisioningNotFound},
			{"conflict", gateway.ErrProvisioningConflict},
			{"inconsistent", gateway.ErrProvisioningInconsistent},
			{"admin lookup", gateway.ErrProvisioningAdminLookup},
			{"gateway unavailable", ErrGatewayProvisioningUnavailable},
			{"deadline exceeded", context.DeadlineExceeded},
			{"canceled", context.Canceled},
		}

		for _, tc := range errCases {
			t.Run(tc.name, func(t *testing.T) {
				gw := &fakeGatewayProvisioner{stubErr: tc.stubErr}
				sensor := &fakeSensorProvisioner{}
				svc, _ := NewCreateService(gw, sensor)

				res, err := svc.CreateEntity(ctx, actor, in)
				if !errors.Is(err, tc.stubErr) {
					t.Fatalf("expected error %v, got: %v", tc.stubErr, err)
				}
				if res != (CreateEntityResult{}) {
					t.Fatalf("expected empty result on error, got: %+v", res)
				}
			})
		}
	})
}

func TestCreateEntity_SensorDispatchAndOutcomes(t *testing.T) {
	ctx := context.Background()
	actor := uuid.New()
	unit := "celsius"
	now := time.Now().UTC()

	in := CreateEntityInput{
		Type: EntityTypeSensor,
		Sensor: &gateway.ProvisionSensorInput{
			GatewayID: "gw1",
			SensorID:  "s1",
			Name:      "Temperature Sensor",
			Unit:      &unit,
		},
	}

	t.Run("creation outcome created=true", func(t *testing.T) {
		sensor := &fakeSensorProvisioner{
			stubResult: gateway.ProvisionSensorResult{
				Created:   true,
				GatewayID: "gw1",
				SensorID:  "s1",
				Name:      "Temperature Sensor",
				Unit:      &unit,
				EntityID:  "urn:ngsi-ld:Sensor:gw1:s1",
				CreatedAt: &now,
			},
		}
		gw := &fakeGatewayProvisioner{}
		svc, _ := NewCreateService(gw, sensor)

		res, err := svc.CreateEntity(ctx, actor, in)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sensor.calls != 1 {
			t.Fatalf("expected 1 sensor provisioner call, got %d", sensor.calls)
		}
		if gw.calls != 0 {
			t.Fatalf("gateway provisioner must not be called, got %d", gw.calls)
		}
		if sensor.lastActor != actor {
			t.Fatalf("expected actor %v passed to sensor provisioner, got %v", actor, sensor.lastActor)
		}

		if !res.Created {
			t.Fatal("expected Created=true")
		}
		if res.Type != EntityTypeSensor {
			t.Fatalf("expected Type=EntityTypeSensor, got %v", res.Type)
		}
		if res.EntityID != "urn:ngsi-ld:Sensor:gw1:s1" {
			t.Fatalf("expected EntityID urn:ngsi-ld:Sensor:gw1:s1, got %v", res.EntityID)
		}
		if res.Sensor == nil || res.Gateway != nil {
			t.Fatal("expected non-nil Sensor and nil Gateway")
		}
		if res.Sensor.Unit == nil || *res.Sensor.Unit != unit {
			t.Fatalf("expected unit %q, got %v", unit, res.Sensor.Unit)
		}
		if res.Sensor.CreatedAt == nil || *res.Sensor.CreatedAt != now {
			t.Fatalf("expected CreatedAt %v, got %v", now, res.Sensor.CreatedAt)
		}
	})

	t.Run("exact replay outcome created=false", func(t *testing.T) {
		sensor := &fakeSensorProvisioner{
			stubResult: gateway.ProvisionSensorResult{
				Created:   false,
				GatewayID: "gw1",
				SensorID:  "s1",
				Name:      "Temperature Sensor",
				Unit:      &unit,
				EntityID:  "urn:ngsi-ld:Sensor:gw1:s1",
				CreatedAt: &now,
			},
		}
		gw := &fakeGatewayProvisioner{}
		svc, _ := NewCreateService(gw, sensor)

		res, err := svc.CreateEntity(ctx, actor, in)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Created {
			t.Fatal("expected Created=false on replay")
		}
		if res.EntityID != "urn:ngsi-ld:Sensor:gw1:s1" {
			t.Fatalf("expected EntityID urn:ngsi-ld:Sensor:gw1:s1, got %v", res.EntityID)
		}
		if res.Sensor.CreatedAt == nil || *res.Sensor.CreatedAt != now {
			t.Fatalf("expected preserved CreatedAt %v, got %v", now, res.Sensor.CreatedAt)
		}
	})

	t.Run("error propagation preserves causes and returns empty result", func(t *testing.T) {
		errCases := []struct {
			name    string
			stubErr error
		}{
			{"forbidden", gateway.ErrProvisioningForbidden},
			{"not found", gateway.ErrProvisioningNotFound},
			{"conflict", gateway.ErrProvisioningConflict},
			{"inconsistent", gateway.ErrProvisioningInconsistent},
			{"admin lookup", gateway.ErrProvisioningAdminLookup},
			{"admin or owner sensor unavailable", gateway.ErrAdminOrOwnerProvisioningUnavailable},
			{"deadline exceeded", context.DeadlineExceeded},
			{"canceled", context.Canceled},
		}

		for _, tc := range errCases {
			t.Run(tc.name, func(t *testing.T) {
				sensor := &fakeSensorProvisioner{stubErr: tc.stubErr}
				gw := &fakeGatewayProvisioner{}
				svc, _ := NewCreateService(gw, sensor)

				res, err := svc.CreateEntity(ctx, actor, in)
				if !errors.Is(err, tc.stubErr) {
					t.Fatalf("expected error %v, got: %v", tc.stubErr, err)
				}
				if res != (CreateEntityResult{}) {
					t.Fatalf("expected empty result on error, got: %+v", res)
				}
			})
		}
	})
}

package digitaltwin

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

func TestValidateCreateEntityInput(t *testing.T) {
	actor := uuid.New()
	owner := uuid.New()
	desc := "A gateway description"
	unit := "degC"

	t.Run("nil actor returns ErrNilActor", func(t *testing.T) {
		input := CreateEntityInput{
			Type: EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   "gw1",
				Name:        "GW 1",
				OwnerUserID: owner,
			},
		}
		err := ValidateCreateEntityInput(uuid.Nil, input)
		if !errors.Is(err, ErrNilActor) {
			t.Fatalf("expected ErrNilActor, got: %v", err)
		}
	})

	t.Run("valid gateway input passes", func(t *testing.T) {
		input := CreateEntityInput{
			Type: EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   "gw1",
				Name:        "GW 1",
				Description: &desc,
				OwnerUserID: owner,
			},
		}
		if err := ValidateCreateEntityInput(actor, input); err != nil {
			t.Fatalf("expected valid gateway input, got: %v", err)
		}
	})

	t.Run("valid sensor input passes", func(t *testing.T) {
		input := CreateEntityInput{
			Type: EntityTypeSensor,
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: "gw1",
				SensorID:  "s1",
				Name:      "Sensor 1",
				Unit:      &unit,
			},
		}
		if err := ValidateCreateEntityInput(actor, input); err != nil {
			t.Fatalf("expected valid sensor input, got: %v", err)
		}
	})

	t.Run("unsupported entity type rejected", func(t *testing.T) {
		for _, badType := range []EntityType{"Device", "Unknown", "", "gateway", "sensor"} {
			input := CreateEntityInput{
				Type: badType,
				Gateway: &gateway.ProvisionGatewayInput{
					GatewayID:   "gw1",
					Name:        "GW 1",
					OwnerUserID: owner,
				},
			}
			err := ValidateCreateEntityInput(actor, input)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("expected ErrInvalidInput for type %q, got: %v", badType, err)
			}
		}
	})

	t.Run("gateway branch nil rejected", func(t *testing.T) {
		input := CreateEntityInput{
			Type:    EntityTypeGateway,
			Gateway: nil,
		}
		err := ValidateCreateEntityInput(actor, input)
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput for nil gateway branch, got: %v", err)
		}
	})

	t.Run("gateway with both gateway and sensor rejected", func(t *testing.T) {
		input := CreateEntityInput{
			Type: EntityTypeGateway,
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   "gw1",
				Name:        "GW 1",
				OwnerUserID: owner,
			},
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: "gw1",
				SensorID:  "s1",
				Name:      "Sensor 1",
			},
		}
		err := ValidateCreateEntityInput(actor, input)
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput for inconsistent tagged branches, got: %v", err)
		}
	})

	t.Run("sensor branch nil rejected", func(t *testing.T) {
		input := CreateEntityInput{
			Type:   EntityTypeSensor,
			Sensor: nil,
		}
		err := ValidateCreateEntityInput(actor, input)
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput for nil sensor branch, got: %v", err)
		}
	})

	t.Run("sensor with both sensor and gateway rejected", func(t *testing.T) {
		input := CreateEntityInput{
			Type: EntityTypeSensor,
			Sensor: &gateway.ProvisionSensorInput{
				GatewayID: "gw1",
				SensorID:  "s1",
				Name:      "Sensor 1",
			},
			Gateway: &gateway.ProvisionGatewayInput{
				GatewayID:   "gw1",
				Name:        "GW 1",
				OwnerUserID: owner,
			},
		}
		err := ValidateCreateEntityInput(actor, input)
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput for inconsistent tagged branches, got: %v", err)
		}
	})

	t.Run("invalid gateway fields rejected", func(t *testing.T) {
		cases := []struct {
			name  string
			input gateway.ProvisionGatewayInput
		}{
			{"invalid gateway id", gateway.ProvisionGatewayInput{GatewayID: "INVALID_ID!", Name: "GW", OwnerUserID: owner}},
			{"empty name", gateway.ProvisionGatewayInput{GatewayID: "gw1", Name: "   ", OwnerUserID: owner}},
			{"name too long", gateway.ProvisionGatewayInput{GatewayID: "gw1", Name: strings.Repeat("a", 257), OwnerUserID: owner}},
			{"nil owner user id", gateway.ProvisionGatewayInput{GatewayID: "gw1", Name: "GW", OwnerUserID: uuid.Nil}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				in := CreateEntityInput{
					Type:    EntityTypeGateway,
					Gateway: &tc.input,
				}
				err := ValidateCreateEntityInput(actor, in)
				if !errors.Is(err, ErrInvalidInput) {
					t.Fatalf("expected ErrInvalidInput, got: %v", err)
				}
			})
		}
	})

	t.Run("invalid sensor fields rejected", func(t *testing.T) {
		tooLongUnit := strings.Repeat("u", 65)
		cases := []struct {
			name  string
			input gateway.ProvisionSensorInput
		}{
			{"invalid gateway id", gateway.ProvisionSensorInput{GatewayID: "INVALID!", SensorID: "s1", Name: "S"}},
			{"invalid sensor id", gateway.ProvisionSensorInput{GatewayID: "gw1", SensorID: "INVALID!", Name: "S"}},
			{"empty name", gateway.ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: "   "}},
			{"name too long", gateway.ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: strings.Repeat("a", 257)}},
			{"unit too long", gateway.ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: "S", Unit: &tooLongUnit}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				in := CreateEntityInput{
					Type:   EntityTypeSensor,
					Sensor: &tc.input,
				}
				err := ValidateCreateEntityInput(actor, in)
				if !errors.Is(err, ErrInvalidInput) {
					t.Fatalf("expected ErrInvalidInput, got: %v", err)
				}
			})
		}
	})
}

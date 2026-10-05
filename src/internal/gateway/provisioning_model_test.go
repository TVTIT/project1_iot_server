package gateway

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestProvisioningInputValidation(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	owner := uuid.MustParse("00000000-0000-4000-8000-000000000002")
	empty := ""
	name := "  Cảm biến  "
	g := ProvisionGatewayInput{GatewayID: "Gateway_A", Name: name, Description: &empty, OwnerUserID: owner}
	s := ProvisionSensorInput{GatewayID: "Gateway_A", SensorID: "backend_service", Name: name, Unit: &empty}
	if err := ValidateProvisionGatewayInput(actor, g); err != nil {
		t.Fatal(err)
	}
	if err := ValidateProvisionSensorInput(actor, s); err != nil {
		t.Fatal(err)
	}
	if g.Name != name || g.Description == nil || *g.Description != "" || s.Unit == nil || *s.Unit != "" {
		t.Fatal("metadata was normalized")
	}
	g.Description, s.Unit = nil, nil
	if err := ValidateProvisionGatewayInput(actor, g); err != nil {
		t.Fatal(err)
	}
	if err := ValidateProvisionSensorInput(actor, s); err != nil {
		t.Fatal(err)
	}
	if err := ValidateProvisionGatewayInput(uuid.Nil, g); !errors.Is(err, ErrInvalidProvisioningInput) {
		t.Errorf("zero actor: %v", err)
	}
	if err := ValidateProvisionSensorInput(uuid.Nil, s); !errors.Is(err, ErrInvalidProvisioningInput) {
		t.Errorf("zero actor: %v", err)
	}
	g.OwnerUserID = uuid.Nil
	if err := ValidateProvisionGatewayInput(actor, g); !errors.Is(err, ErrInvalidProvisioningInput) {
		t.Errorf("zero owner: %v", err)
	}
	g.OwnerUserID, g.GatewayID = owner, "backend_service"
	if err := ValidateProvisionGatewayInput(actor, g); !errors.Is(err, ErrInvalidGatewayID) {
		t.Errorf("Gateway ID: %v", err)
	}
	s.GatewayID = "../a"
	if err := ValidateProvisionSensorInput(actor, s); !errors.Is(err, ErrInvalidGatewayID) {
		t.Errorf("parent ID: %v", err)
	}
	s.GatewayID, s.SensorID = "Gateway_A", "a/#"
	if err := ValidateProvisionSensorInput(actor, s); !errors.Is(err, ErrInvalidSensorID) {
		t.Errorf("Sensor ID: %v", err)
	}
}

func TestProvisioningMetadataBoundaries(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	for _, tc := range []struct {
		field string
		limit int
	}{{"name", 256}, {"description", 4096}, {"unit", 64}} {
		t.Run(tc.field, func(t *testing.T) {
			valid := []string{strings.Repeat("a", tc.limit), strings.Repeat("é", tc.limit/2), " x "}
			invalid := []string{strings.Repeat("a", tc.limit+1), strings.Repeat("é", tc.limit/2) + "a", "a\x00b", string([]byte{0xff})}
			if tc.field == "name" {
				invalid = append(invalid, "", " \t\n", "\u2003")
			} else {
				valid = append(valid, "", " \t")
			}
			check := func(value string) error {
				g := ProvisionGatewayInput{GatewayID: "g", Name: "name", OwnerUserID: actor}
				s := ProvisionSensorInput{GatewayID: "g", SensorID: "s", Name: "name"}
				switch tc.field {
				case "name":
					g.Name, s.Name = value, value
					if err := ValidateProvisionGatewayInput(actor, g); err != nil {
						return err
					}
					return ValidateProvisionSensorInput(actor, s)
				case "description":
					g.Description = &value
					return ValidateProvisionGatewayInput(actor, g)
				default:
					s.Unit = &value
					return ValidateProvisionSensorInput(actor, s)
				}
			}
			for _, value := range valid {
				if err := check(value); err != nil {
					t.Errorf("valid value (%d bytes): %v", len(value), err)
				}
			}
			for _, value := range invalid {
				if err := check(value); !errors.Is(err, ErrInvalidProvisioningInput) {
					t.Errorf("invalid value (%d bytes): %v", len(value), err)
				}
			}
		})
	}
}

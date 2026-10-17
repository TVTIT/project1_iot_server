package digitaltwin

import (
	"errors"
	"strings"
	"testing"
)

func TestValidatePatchGatewayInput(t *testing.T) {
	validName := "AM5728 Gateway 001"
	untrimmedName := "  AM5728 Gateway 001  "
	validDesc := "Bàn giao phòng lab IoT"
	emptyStr := ""
	oversizedName := strings.Repeat("a", 257)
	oversizedDesc := strings.Repeat("b", 4097)

	tests := []struct {
		name    string
		input   PatchGatewayInput
		wantErr bool
	}{
		{
			name: "name only",
			input: PatchGatewayInput{
				Name:           &validName,
				DescriptionSet: false,
				Description:    nil,
			},
			wantErr: false,
		},
		{
			name: "untrimmed name preserved",
			input: PatchGatewayInput{
				Name:           &untrimmedName,
				DescriptionSet: false,
				Description:    nil,
			},
			wantErr: false,
		},
		{
			name: "description only string",
			input: PatchGatewayInput{
				Name:           nil,
				DescriptionSet: true,
				Description:    &validDesc,
			},
			wantErr: false,
		},
		{
			name: "description only explicit null clear",
			input: PatchGatewayInput{
				Name:           nil,
				DescriptionSet: true,
				Description:    nil,
			},
			wantErr: false,
		},
		{
			name: "both name and description string",
			input: PatchGatewayInput{
				Name:           &validName,
				DescriptionSet: true,
				Description:    &validDesc,
			},
			wantErr: false,
		},
		{
			name: "both name and description explicit null clear",
			input: PatchGatewayInput{
				Name:           &validName,
				DescriptionSet: true,
				Description:    nil,
			},
			wantErr: false,
		},
		{
			name: "empty patch - neither name nor description supplied",
			input: PatchGatewayInput{
				Name:           nil,
				DescriptionSet: false,
				Description:    nil,
			},
			wantErr: true,
		},
		{
			name: "blank name after trim",
			input: PatchGatewayInput{
				Name:           &emptyStr,
				DescriptionSet: false,
				Description:    nil,
			},
			wantErr: true,
		},
		{
			name: "spaces-only name",
			input: PatchGatewayInput{
				Name:           func() *string { s := "   \t\n  "; return &s }(),
				DescriptionSet: false,
				Description:    nil,
			},
			wantErr: true,
		},
		{
			name: "oversized name > 256 bytes",
			input: PatchGatewayInput{
				Name:           &oversizedName,
				DescriptionSet: false,
				Description:    nil,
			},
			wantErr: true,
		},
		{
			name: "name contains NUL byte",
			input: PatchGatewayInput{
				Name:           func() *string { s := "GW\x0001"; return &s }(),
				DescriptionSet: false,
				Description:    nil,
			},
			wantErr: true,
		},
		{
			name: "name contains invalid UTF-8",
			input: PatchGatewayInput{
				Name:           func() *string { s := "GW\xff\xfe"; return &s }(),
				DescriptionSet: false,
				Description:    nil,
			},
			wantErr: true,
		},
		{
			name: "oversized description > 4096 bytes",
			input: PatchGatewayInput{
				Name:           nil,
				DescriptionSet: true,
				Description:    &oversizedDesc,
			},
			wantErr: true,
		},
		{
			name: "description contains NUL byte",
			input: PatchGatewayInput{
				Name:           nil,
				DescriptionSet: true,
				Description:    func() *string { s := "Desc\x00Bad"; return &s }(),
			},
			wantErr: true,
		},
		{
			name: "inconsistent presence - DescriptionSet false but Description non-nil",
			input: PatchGatewayInput{
				Name:           &validName,
				DescriptionSet: false,
				Description:    &validDesc,
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePatchGatewayInput(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidatePatchGatewayInput() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("expected ErrInvalidInput, got %v", err)
			}
		})
	}
}

func TestValidatePatchSensorInput(t *testing.T) {
	validName := "Temperature Sensor"
	untrimmedName := "  Temperature Sensor  "
	emptyStr := ""
	oversizedName := strings.Repeat("a", 257)

	tests := []struct {
		name    string
		input   PatchSensorInput
		wantErr bool
	}{
		{
			name: "valid name",
			input: PatchSensorInput{
				Name: &validName,
			},
			wantErr: false,
		},
		{
			name: "untrimmed name preserved",
			input: PatchSensorInput{
				Name: &untrimmedName,
			},
			wantErr: false,
		},
		{
			name: "name omitted (nil)",
			input: PatchSensorInput{
				Name: nil,
			},
			wantErr: true,
		},
		{
			name: "blank name after trim",
			input: PatchSensorInput{
				Name: &emptyStr,
			},
			wantErr: true,
		},
		{
			name: "spaces-only name",
			input: PatchSensorInput{
				Name: func() *string { s := "   \t\n  "; return &s }(),
			},
			wantErr: true,
		},
		{
			name: "oversized name > 256 bytes",
			input: PatchSensorInput{
				Name: &oversizedName,
			},
			wantErr: true,
		},
		{
			name: "name contains NUL byte",
			input: PatchSensorInput{
				Name: func() *string { s := "Sensor\x0001"; return &s }(),
			},
			wantErr: true,
		},
		{
			name: "name contains invalid UTF-8",
			input: PatchSensorInput{
				Name: func() *string { s := "Sensor\xff\xfe"; return &s }(),
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePatchSensorInput(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidatePatchSensorInput() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("expected ErrInvalidInput, got %v", err)
			}
		})
	}
}

func TestValidatePatchEntityInput(t *testing.T) {
	validName := "AM5728 Gateway"
	sensorName := "Temperature Sensor"

	gwTarget := ParsedIdentity{
		Type:      EntityTypeGateway,
		GatewayID: "gw_01",
		URN:       "urn:ngsi-ld:Gateway:gw_01",
	}
	sensorTarget := ParsedIdentity{
		Type:      EntityTypeSensor,
		GatewayID: "gw_01",
		SensorID:  "temp_01",
		URN:       "urn:ngsi-ld:Sensor:gw_01:temp_01",
	}

	t.Run("valid Gateway entity input", func(t *testing.T) {
		input := PatchEntityInput{
			Target: gwTarget,
			Gateway: &PatchGatewayInput{
				Name:           &validName,
				DescriptionSet: false,
				Description:    nil,
			},
			Sensor: nil,
		}
		if err := ValidatePatchEntityInput(input); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("Gateway entity input with Sensor field set", func(t *testing.T) {
		input := PatchEntityInput{
			Target: gwTarget,
			Gateway: &PatchGatewayInput{
				Name: &validName,
			},
			Sensor: &PatchSensorInput{
				Name: &sensorName,
			},
		}
		if err := ValidatePatchEntityInput(input); err == nil {
			t.Fatalf("expected error, got nil")
		}
	})

	t.Run("valid Sensor entity input", func(t *testing.T) {
		input := PatchEntityInput{
			Target:  sensorTarget,
			Gateway: nil,
			Sensor: &PatchSensorInput{
				Name: &sensorName,
			},
		}
		if err := ValidatePatchEntityInput(input); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("Sensor entity input with Gateway field set", func(t *testing.T) {
		input := PatchEntityInput{
			Target: sensorTarget,
			Gateway: &PatchGatewayInput{
				Name: &validName,
			},
			Sensor: &PatchSensorInput{
				Name: &sensorName,
			},
		}
		if err := ValidatePatchEntityInput(input); err == nil {
			t.Fatalf("expected error, got nil")
		}
	})

	t.Run("invalid target type", func(t *testing.T) {
		input := PatchEntityInput{
			Target: ParsedIdentity{
				Type: EntityType("Device"),
			},
			Gateway: &PatchGatewayInput{
				Name: &validName,
			},
		}
		if err := ValidatePatchEntityInput(input); err == nil {
			t.Fatalf("expected error, got nil")
		}
	})
}

package digitaltwin_test

import (
	"errors"
	"testing"

	"iot-platform/internal/digitaltwin"
)

func TestValidateTwinListQuery(t *testing.T) {
	gwID := "gw_01"
	sensorType := digitaltwin.EntityTypeSensor
	invalidType := digitaltwin.EntityType("Device")
	validCursor, err := digitaltwin.EncodeListCursor("urn:ngsi-ld:Sensor:gw_01:s1", &gwID, &sensorType)
	if err != nil {
		t.Fatalf("failed to encode valid cursor: %v", err)
	}

	tests := []struct {
		name         string
		query        digitaltwin.TwinListQuery
		maxLimit     int
		wantErr      bool
		wantSentinel error
	}{
		{
			name: "valid query without filters",
			query: digitaltwin.TwinListQuery{
				Limit: 50,
			},
			maxLimit: 200,
			wantErr:  false,
		},
		{
			name: "valid query with filters and cursor",
			query: digitaltwin.TwinListQuery{
				GatewayID:  &gwID,
				EntityType: &sensorType,
				Limit:      100,
				After:      &validCursor,
			},
			maxLimit: 200,
			wantErr:  false,
		},
		{
			name: "reject zero limit",
			query: digitaltwin.TwinListQuery{
				Limit: 0,
			},
			maxLimit:     200,
			wantErr:      true,
			wantSentinel: digitaltwin.ErrInvalidInput,
		},
		{
			name: "reject negative limit",
			query: digitaltwin.TwinListQuery{
				Limit: -5,
			},
			maxLimit:     200,
			wantErr:      true,
			wantSentinel: digitaltwin.ErrInvalidInput,
		},
		{
			name: "reject limit exceeding maxLimit",
			query: digitaltwin.TwinListQuery{
				Limit: 201,
			},
			maxLimit:     200,
			wantErr:      true,
			wantSentinel: digitaltwin.ErrInvalidInput,
		},
		{
			name: "reject invalid gateway_id",
			query: digitaltwin.TwinListQuery{
				GatewayID: func() *string { s := "invalid@gw"; return &s }(),
				Limit:     50,
			},
			maxLimit:     200,
			wantErr:      true,
			wantSentinel: digitaltwin.ErrInvalidInput,
		},
		{
			name: "reject invalid entity_type",
			query: digitaltwin.TwinListQuery{
				EntityType: &invalidType,
				Limit:      50,
			},
			maxLimit:     200,
			wantErr:      true,
			wantSentinel: digitaltwin.ErrInvalidInput,
		},
		{
			name: "reject invalid cursor (mismatched filter)",
			query: digitaltwin.TwinListQuery{
				GatewayID:  nil, // cursor has gw_01
				EntityType: &sensorType,
				Limit:      50,
				After:      &validCursor,
			},
			maxLimit:     200,
			wantErr:      true,
			wantSentinel: digitaltwin.ErrInvalidCursor,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := digitaltwin.ValidateTwinListQuery(tt.query, tt.maxLimit)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.wantSentinel != nil && !errors.Is(err, tt.wantSentinel) {
					t.Errorf("got error %v, want sentinel %v", err, tt.wantSentinel)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestValidateTwinDetailQuery(t *testing.T) {
	srcURN := "urn:ngsi-ld:Gateway:gw_01"
	targetURN := "urn:ngsi-ld:Sensor:gw_01:s1"
	validCursor, err := digitaltwin.EncodeRelationshipCursor(srcURN, "hasSensor", targetURN)
	if err != nil {
		t.Fatalf("failed to encode valid cursor: %v", err)
	}

	tests := []struct {
		name         string
		query        digitaltwin.TwinDetailQuery
		maxLimit     int
		wantErr      bool
		wantSentinel error
	}{
		{
			name: "valid detail query without cursor",
			query: digitaltwin.TwinDetailQuery{
				EntityID:          srcURN,
				RelationshipLimit: 50,
			},
			maxLimit: 200,
			wantErr:  false,
		},
		{
			name: "valid detail query with cursor",
			query: digitaltwin.TwinDetailQuery{
				EntityID:          srcURN,
				RelationshipLimit: 100,
				RelationshipAfter: &validCursor,
			},
			maxLimit: 200,
			wantErr:  false,
		},
		{
			name: "reject invalid entityID",
			query: digitaltwin.TwinDetailQuery{
				EntityID:          "not-a-urn",
				RelationshipLimit: 50,
			},
			maxLimit:     200,
			wantErr:      true,
			wantSentinel: digitaltwin.ErrInvalidInput,
		},
		{
			name: "reject zero limit",
			query: digitaltwin.TwinDetailQuery{
				EntityID:          srcURN,
				RelationshipLimit: 0,
			},
			maxLimit:     200,
			wantErr:      true,
			wantSentinel: digitaltwin.ErrInvalidInput,
		},
		{
			name: "reject limit exceeding maxLimit",
			query: digitaltwin.TwinDetailQuery{
				EntityID:          srcURN,
				RelationshipLimit: 201,
			},
			maxLimit:     200,
			wantErr:      true,
			wantSentinel: digitaltwin.ErrInvalidInput,
		},
		{
			name: "reject cursor source mismatch",
			query: digitaltwin.TwinDetailQuery{
				EntityID:          "urn:ngsi-ld:Gateway:gw_02", // cursor has gw_01
				RelationshipLimit: 50,
				RelationshipAfter: &validCursor,
			},
			maxLimit:     200,
			wantErr:      true,
			wantSentinel: digitaltwin.ErrInvalidCursor,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := digitaltwin.ValidateTwinDetailQuery(tt.query, tt.maxLimit)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.wantSentinel != nil && !errors.Is(err, tt.wantSentinel) {
					t.Errorf("got error %v, want sentinel %v", err, tt.wantSentinel)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
)

func TestPostGatewayResponseDTO_ExpectedJSON_Independent(t *testing.T) {
	ownerID := uuid.MustParse("3f2a1b4c-0000-4000-8000-000000000001")
	createdAt := time.Date(2026, 10, 17, 8, 0, 0, 0, time.UTC)
	desc := "Bàn giao tại phòng lab AM5728"

	t.Run("Create 201 with all fields", func(t *testing.T) {
		res := &gateway.ProvisionGatewayResult{
			Created:     true,
			GatewayID:   "gateway_001",
			Name:        "AM5728 Gateway 001",
			Description: &desc,
			OwnerUserID: ownerID,
			EntityID:    "urn:ngsi-ld:Gateway:gateway_001",
			CreatedAt:   &createdAt,
		}

		status, body, err := NewCreateEntityResponseDTO(digitaltwin.CreateEntityResult{
			Created:  true,
			Type:     digitaltwin.EntityTypeGateway,
			EntityID: "urn:ngsi-ld:Gateway:gateway_001",
			Gateway:  res,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status != http.StatusCreated {
			t.Fatalf("expected status 201, got %d", status)
		}

		wireBytes, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}

		expectedJSON := `{"gateway_id":"gateway_001","name":"AM5728 Gateway 001","description":"Bàn giao tại phòng lab AM5728","owner_user_id":"3f2a1b4c-0000-4000-8000-000000000001","entity_id":"urn:ngsi-ld:Gateway:gateway_001","created_at":"2026-10-17T08:00:00.000000Z"}`
		if string(wireBytes) != expectedJSON {
			t.Fatalf("wire JSON mismatch:\nexpected: %s\ngot:      %s", expectedJSON, string(wireBytes))
		}

		// Verify no internal fields leaked
		var rawMap map[string]any
		if err := json.Unmarshal(wireBytes, &rawMap); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}
		if len(rawMap) != 6 {
			t.Fatalf("expected exactly 6 fields, got %d: %v", len(rawMap), rawMap)
		}
		for _, forbidden := range []string{"id", "type", "attributes", "relationships", "reported_state", "desired_state", "@context"} {
			if _, exists := rawMap[forbidden]; exists {
				t.Fatalf("forbidden field %q leaked in response", forbidden)
			}
		}
	})

	t.Run("Replay 200 outcome", func(t *testing.T) {
		res := &gateway.ProvisionGatewayResult{
			Created:     false,
			GatewayID:   "gateway_001",
			Name:        "AM5728 Gateway 001",
			Description: &desc,
			OwnerUserID: ownerID,
			EntityID:    "urn:ngsi-ld:Gateway:gateway_001",
			CreatedAt:   &createdAt,
		}

		status, body, err := NewCreateEntityResponseDTO(digitaltwin.CreateEntityResult{
			Created:  false,
			Type:     digitaltwin.EntityTypeGateway,
			EntityID: "urn:ngsi-ld:Gateway:gateway_001",
			Gateway:  res,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected status 200, got %d", status)
		}

		wireBytes, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}
		expectedJSON := `{"gateway_id":"gateway_001","name":"AM5728 Gateway 001","description":"Bàn giao tại phòng lab AM5728","owner_user_id":"3f2a1b4c-0000-4000-8000-000000000001","entity_id":"urn:ngsi-ld:Gateway:gateway_001","created_at":"2026-10-17T08:00:00.000000Z"}`
		if string(wireBytes) != expectedJSON {
			t.Fatalf("wire JSON mismatch:\nexpected: %s\ngot:      %s", expectedJSON, string(wireBytes))
		}
	})

	t.Run("Null description and legacy null created_at", func(t *testing.T) {
		res := &gateway.ProvisionGatewayResult{
			Created:     true,
			GatewayID:   "gateway_001",
			Name:        "AM5728 Gateway 001",
			Description: nil,
			OwnerUserID: ownerID,
			EntityID:    "urn:ngsi-ld:Gateway:gateway_001",
			CreatedAt:   nil, // Legacy NULL
		}

		dto, err := NewPostGatewayResponseDTO(res)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		wireBytes, err := json.Marshal(dto)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}
		expectedJSON := `{"gateway_id":"gateway_001","name":"AM5728 Gateway 001","description":null,"owner_user_id":"3f2a1b4c-0000-4000-8000-000000000001","entity_id":"urn:ngsi-ld:Gateway:gateway_001","created_at":null}`
		if string(wireBytes) != expectedJSON {
			t.Fatalf("wire JSON mismatch:\nexpected: %s\ngot:      %s", expectedJSON, string(wireBytes))
		}
	})

	t.Run("Non-UTC timestamp normalization", func(t *testing.T) {
		loc := time.FixedZone("UTC+7", 7*3600)
		localTime := time.Date(2026, 10, 17, 15, 0, 0, 0, loc) // 15:00 +07 == 08:00 UTC

		res := &gateway.ProvisionGatewayResult{
			Created:     true,
			GatewayID:   "gateway_001",
			Name:        "AM5728 Gateway 001",
			Description: nil,
			OwnerUserID: ownerID,
			EntityID:    "urn:ngsi-ld:Gateway:gateway_001",
			CreatedAt:   &localTime,
		}

		dto, err := NewPostGatewayResponseDTO(res)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dto.CreatedAt == nil || *dto.CreatedAt != "2026-10-17T08:00:00.000000Z" {
			t.Fatalf("expected UTC microsecond string 2026-10-17T08:00:00.000000Z, got %v", dto.CreatedAt)
		}
	})
}

func TestPostSensorResponseDTO_ExpectedJSON_Independent(t *testing.T) {
	createdAt := time.Date(2026, 10, 17, 8, 5, 0, 0, time.UTC)
	unit := "degC"

	t.Run("Create 201 with all fields", func(t *testing.T) {
		res := &gateway.ProvisionSensorResult{
			Created:   true,
			GatewayID: "gateway_001",
			SensorID:  "sensor_001",
			Name:      "Temperature",
			Unit:      &unit,
			EntityID:  "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
			CreatedAt: &createdAt,
		}

		status, body, err := NewCreateEntityResponseDTO(digitaltwin.CreateEntityResult{
			Created:  true,
			Type:     digitaltwin.EntityTypeSensor,
			EntityID: "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
			Sensor:   res,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status != http.StatusCreated {
			t.Fatalf("expected status 201, got %d", status)
		}

		wireBytes, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}

		expectedJSON := `{"gateway_id":"gateway_001","sensor_id":"sensor_001","name":"Temperature","unit":"degC","entity_id":"urn:ngsi-ld:Sensor:gateway_001:sensor_001","created_at":"2026-10-17T08:05:00.000000Z"}`
		if string(wireBytes) != expectedJSON {
			t.Fatalf("wire JSON mismatch:\nexpected: %s\ngot:      %s", expectedJSON, string(wireBytes))
		}

		// Verify no description or internal fields
		var rawMap map[string]any
		if err := json.Unmarshal(wireBytes, &rawMap); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}
		if len(rawMap) != 6 {
			t.Fatalf("expected exactly 6 fields, got %d: %v", len(rawMap), rawMap)
		}
		for _, forbidden := range []string{"description", "owner_user_id", "id", "type", "attributes", "relationships", "reported_state", "desired_state", "@context"} {
			if _, exists := rawMap[forbidden]; exists {
				t.Fatalf("forbidden field %q leaked in sensor response", forbidden)
			}
		}
	})

	t.Run("Replay 200 outcome", func(t *testing.T) {
		res := &gateway.ProvisionSensorResult{
			Created:   false,
			GatewayID: "gateway_001",
			SensorID:  "sensor_001",
			Name:      "Temperature",
			Unit:      &unit,
			EntityID:  "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
			CreatedAt: &createdAt,
		}

		status, body, err := NewCreateEntityResponseDTO(digitaltwin.CreateEntityResult{
			Created:  false,
			Type:     digitaltwin.EntityTypeSensor,
			EntityID: "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
			Sensor:   res,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected status 200, got %d", status)
		}
		wireBytes, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}
		expectedJSON := `{"gateway_id":"gateway_001","sensor_id":"sensor_001","name":"Temperature","unit":"degC","entity_id":"urn:ngsi-ld:Sensor:gateway_001:sensor_001","created_at":"2026-10-17T08:05:00.000000Z"}`
		if string(wireBytes) != expectedJSON {
			t.Fatalf("wire JSON mismatch:\nexpected: %s\ngot:      %s", expectedJSON, string(wireBytes))
		}
	})

	t.Run("Null unit and legacy null created_at", func(t *testing.T) {
		res := &gateway.ProvisionSensorResult{
			Created:   true,
			GatewayID: "gateway_001",
			SensorID:  "sensor_001",
			Name:      "Temperature",
			Unit:      nil,
			EntityID:  "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
			CreatedAt: nil,
		}

		dto, err := NewPostSensorResponseDTO(res)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		wireBytes, err := json.Marshal(dto)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}
		expectedJSON := `{"gateway_id":"gateway_001","sensor_id":"sensor_001","name":"Temperature","unit":null,"entity_id":"urn:ngsi-ld:Sensor:gateway_001:sensor_001","created_at":null}`
		if string(wireBytes) != expectedJSON {
			t.Fatalf("wire JSON mismatch:\nexpected: %s\ngot:      %s", expectedJSON, string(wireBytes))
		}
	})

	t.Run("Non-UTC timestamp normalization", func(t *testing.T) {
		loc := time.FixedZone("UTC+7", 7*3600)
		localTime := time.Date(2026, 10, 17, 15, 5, 0, 0, loc)

		res := &gateway.ProvisionSensorResult{
			Created:   true,
			GatewayID: "gateway_001",
			SensorID:  "sensor_001",
			Name:      "Temperature",
			Unit:      nil,
			EntityID:  "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
			CreatedAt: &localTime,
		}

		dto, err := NewPostSensorResponseDTO(res)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dto.CreatedAt == nil || *dto.CreatedAt != "2026-10-17T08:05:00.000000Z" {
			t.Fatalf("expected UTC microsecond string 2026-10-17T08:05:00.000000Z, got %v", dto.CreatedAt)
		}
	})
}

func TestPatchGatewayResponseDTO_ExpectedJSON_Independent(t *testing.T) {
	updatedAt := time.Date(2026, 10, 17, 8, 12, 0, 0, time.UTC)
	desc := "Cập nhật lab description"

	t.Run("Gateway patch with non-nil description", func(t *testing.T) {
		res := digitaltwin.PatchGatewayResult{
			ID:          "urn:ngsi-ld:Gateway:gateway_001",
			Type:        digitaltwin.EntityTypeGateway,
			GatewayID:   "gateway_001",
			Name:        "AM5728 Gateway 001",
			Description: &desc,
			UpdatedAt:   updatedAt,
		}

		dto, err := NewPatchGatewayResponseDTO(res)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		wireBytes, err := json.Marshal(dto)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}

		expectedJSON := `{"id":"urn:ngsi-ld:Gateway:gateway_001","type":"Gateway","gateway_id":"gateway_001","name":"AM5728 Gateway 001","description":"Cập nhật lab description","updated_at":"2026-10-17T08:12:00.000000Z"}`
		if string(wireBytes) != expectedJSON {
			t.Fatalf("wire JSON mismatch:\nexpected: %s\ngot:      %s", expectedJSON, string(wireBytes))
		}

		var rawMap map[string]any
		if err := json.Unmarshal(wireBytes, &rawMap); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}
		if len(rawMap) != 6 {
			t.Fatalf("expected exactly 6 fields, got %d: %v", len(rawMap), rawMap)
		}
		for _, forbidden := range []string{"unit", "sensor_id", "created_at", "attributes", "relationships", "reported_state", "desired_state", "@context"} {
			if _, exists := rawMap[forbidden]; exists {
				t.Fatalf("forbidden field %q leaked in patch gateway response", forbidden)
			}
		}
	})

	t.Run("Gateway patch with explicit null description (cleared)", func(t *testing.T) {
		res := digitaltwin.PatchGatewayResult{
			ID:          "urn:ngsi-ld:Gateway:gateway_001",
			Type:        digitaltwin.EntityTypeGateway,
			GatewayID:   "gateway_001",
			Name:        "AM5728 Gateway 001",
			Description: nil,
			UpdatedAt:   updatedAt,
		}

		dto, err := NewPatchGatewayResponseDTO(res)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		wireBytes, err := json.Marshal(dto)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}

		expectedJSON := `{"id":"urn:ngsi-ld:Gateway:gateway_001","type":"Gateway","gateway_id":"gateway_001","name":"AM5728 Gateway 001","description":null,"updated_at":"2026-10-17T08:12:00.000000Z"}`
		if string(wireBytes) != expectedJSON {
			t.Fatalf("wire JSON mismatch:\nexpected: %s\ngot:      %s", expectedJSON, string(wireBytes))
		}
	})

	t.Run("Preserve no-op timestamp and non-UTC normalization", func(t *testing.T) {
		loc := time.FixedZone("UTC-4", -4*3600)
		pastNoOpTime := time.Date(2026, 10, 16, 19, 30, 0, 0, loc) // 19:30 -04 == 23:30 UTC

		res := digitaltwin.PatchGatewayResult{
			ID:          "urn:ngsi-ld:Gateway:gateway_001",
			Type:        digitaltwin.EntityTypeGateway,
			GatewayID:   "gateway_001",
			Name:        "AM5728 Gateway 001",
			Description: nil,
			UpdatedAt:   pastNoOpTime,
		}

		dto, err := NewPatchGatewayResponseDTO(res)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dto.UpdatedAt != "2026-10-16T23:30:00.000000Z" {
			t.Fatalf("expected preserved no-op timestamp 2026-10-16T23:30:00.000000Z, got %s", dto.UpdatedAt)
		}
	})
}

func TestPatchSensorResponseDTO_ExpectedJSON_Independent(t *testing.T) {
	updatedAt := time.Date(2026, 10, 17, 8, 10, 0, 0, time.UTC)
	unit := "degC"

	t.Run("Sensor patch with non-nil unit", func(t *testing.T) {
		res := digitaltwin.PatchSensorResult{
			ID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
			Type:      digitaltwin.EntityTypeSensor,
			GatewayID: "gateway_001",
			SensorID:  "sensor_001",
			Name:      "Temperature (phòng lab)",
			Unit:      &unit,
			UpdatedAt: updatedAt,
		}

		dto, err := NewPatchSensorResponseDTO(res)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		wireBytes, err := json.Marshal(dto)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}

		expectedJSON := `{"id":"urn:ngsi-ld:Sensor:gateway_001:sensor_001","type":"Sensor","gateway_id":"gateway_001","sensor_id":"sensor_001","name":"Temperature (phòng lab)","unit":"degC","updated_at":"2026-10-17T08:10:00.000000Z"}`
		if string(wireBytes) != expectedJSON {
			t.Fatalf("wire JSON mismatch:\nexpected: %s\ngot:      %s", expectedJSON, string(wireBytes))
		}

		var rawMap map[string]any
		if err := json.Unmarshal(wireBytes, &rawMap); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}
		if len(rawMap) != 7 {
			t.Fatalf("expected exactly 7 fields, got %d: %v", len(rawMap), rawMap)
		}
		for _, forbidden := range []string{"description", "created_at", "owner_user_id", "attributes", "relationships", "reported_state", "desired_state", "@context"} {
			if _, exists := rawMap[forbidden]; exists {
				t.Fatalf("forbidden field %q leaked in patch sensor response", forbidden)
			}
		}
	})

	t.Run("Sensor patch with null unit", func(t *testing.T) {
		res := digitaltwin.PatchSensorResult{
			ID:        "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
			Type:      digitaltwin.EntityTypeSensor,
			GatewayID: "gateway_001",
			SensorID:  "sensor_001",
			Name:      "Temperature (phòng lab)",
			Unit:      nil,
			UpdatedAt: updatedAt,
		}

		dto, err := NewPatchSensorResponseDTO(res)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		wireBytes, err := json.Marshal(dto)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}

		expectedJSON := `{"id":"urn:ngsi-ld:Sensor:gateway_001:sensor_001","type":"Sensor","gateway_id":"gateway_001","sensor_id":"sensor_001","name":"Temperature (phòng lab)","unit":null,"updated_at":"2026-10-17T08:10:00.000000Z"}`
		if string(wireBytes) != expectedJSON {
			t.Fatalf("wire JSON mismatch:\nexpected: %s\ngot:      %s", expectedJSON, string(wireBytes))
		}
	})
}

func TestNewPatchEntityResponseDTO_Dispatcher(t *testing.T) {
	updatedAt := time.Date(2026, 10, 17, 8, 10, 0, 0, time.UTC)

	t.Run("Gateway dispatch", func(t *testing.T) {
		gwRes := &digitaltwin.PatchGatewayResult{
			ID:        "urn:ngsi-ld:Gateway:gw1",
			Type:      digitaltwin.EntityTypeGateway,
			GatewayID: "gw1",
			Name:      "GW1",
			UpdatedAt: updatedAt,
		}
		body, err := NewPatchEntityResponseDTO(digitaltwin.EntityTypeGateway, gwRes, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		dto, ok := body.(*PatchGatewayResponseDTO)
		if !ok || dto.GatewayID != "gw1" {
			t.Fatalf("expected *PatchGatewayResponseDTO with gw1, got %T %v", body, body)
		}
	})

	t.Run("Sensor dispatch", func(t *testing.T) {
		sensorRes := &digitaltwin.PatchSensorResult{
			ID:        "urn:ngsi-ld:Sensor:gw1:s1",
			Type:      digitaltwin.EntityTypeSensor,
			GatewayID: "gw1",
			SensorID:  "s1",
			Name:      "S1",
			UpdatedAt: updatedAt,
		}
		body, err := NewPatchEntityResponseDTO(digitaltwin.EntityTypeSensor, nil, sensorRes)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		dto, ok := body.(*PatchSensorResponseDTO)
		if !ok || dto.SensorID != "s1" {
			t.Fatalf("expected *PatchSensorResponseDTO with s1, got %T %v", body, body)
		}
	})

	t.Run("Branch mismatch or unsupported type", func(t *testing.T) {
		if _, err := NewPatchEntityResponseDTO(digitaltwin.EntityTypeGateway, nil, nil); err == nil {
			t.Fatal("expected error on missing gateway result")
		}
		if _, err := NewPatchEntityResponseDTO(digitaltwin.EntityTypeSensor, nil, nil); err == nil {
			t.Fatal("expected error on missing sensor result")
		}
		if _, err := NewPatchEntityResponseDTO(digitaltwin.EntityType("Other"), nil, nil); err == nil {
			t.Fatal("expected error on unsupported entity type")
		}
	})
}

func TestNewCreateEntityResponseDTO_Rejections(t *testing.T) {
	ownerID := uuid.MustParse("3f2a1b4c-0000-4000-8000-000000000001")
	createdAt := time.Date(2026, 10, 17, 8, 0, 0, 0, time.UTC)

	validGW := &gateway.ProvisionGatewayResult{
		Created:     true,
		GatewayID:   "gw1",
		Name:        "GW1",
		OwnerUserID: ownerID,
		EntityID:    "urn:ngsi-ld:Gateway:gw1",
		CreatedAt:   &createdAt,
	}

	validSensor := &gateway.ProvisionSensorResult{
		Created:   true,
		GatewayID: "gw1",
		SensorID:  "s1",
		Name:      "S1",
		EntityID:  "urn:ngsi-ld:Sensor:gw1:s1",
		CreatedAt: &createdAt,
	}

	tests := []struct {
		name string
		res  digitaltwin.CreateEntityResult
	}{
		{
			name: "gateway with nil gateway result",
			res: digitaltwin.CreateEntityResult{
				Created:  true,
				Type:     digitaltwin.EntityTypeGateway,
				EntityID: "urn:ngsi-ld:Gateway:gw1",
			},
		},
		{
			name: "gateway with unexpected sensor result",
			res: digitaltwin.CreateEntityResult{
				Created:  true,
				Type:     digitaltwin.EntityTypeGateway,
				EntityID: "urn:ngsi-ld:Gateway:gw1",
				Gateway:  validGW,
				Sensor:   validSensor,
			},
		},
		{
			name: "gateway with mismatched created flag",
			res: digitaltwin.CreateEntityResult{
				Created:  false, // mismatches validGW.Created (true)
				Type:     digitaltwin.EntityTypeGateway,
				EntityID: "urn:ngsi-ld:Gateway:gw1",
				Gateway:  validGW,
			},
		},
		{
			name: "gateway with mismatched entity ID",
			res: digitaltwin.CreateEntityResult{
				Created:  true,
				Type:     digitaltwin.EntityTypeGateway,
				EntityID: "urn:ngsi-ld:Gateway:other_gw",
				Gateway:  validGW,
			},
		},
		{
			name: "sensor with nil sensor result",
			res: digitaltwin.CreateEntityResult{
				Created:  true,
				Type:     digitaltwin.EntityTypeSensor,
				EntityID: "urn:ngsi-ld:Sensor:gw1:s1",
			},
		},
		{
			name: "sensor with unexpected gateway result",
			res: digitaltwin.CreateEntityResult{
				Created:  true,
				Type:     digitaltwin.EntityTypeSensor,
				EntityID: "urn:ngsi-ld:Sensor:gw1:s1",
				Gateway:  validGW,
				Sensor:   validSensor,
			},
		},
		{
			name: "sensor with mismatched created flag",
			res: digitaltwin.CreateEntityResult{
				Created:  false,
				Type:     digitaltwin.EntityTypeSensor,
				EntityID: "urn:ngsi-ld:Sensor:gw1:s1",
				Sensor:   validSensor,
			},
		},
		{
			name: "unsupported entity type",
			res: digitaltwin.CreateEntityResult{
				Created:  true,
				Type:     digitaltwin.EntityType("Device"),
				EntityID: "urn:ngsi-ld:Device:dev1",
			},
		},
		{
			name: "nil provision result pointer in post gateway",
			res: digitaltwin.CreateEntityResult{
				Created: true,
				Type:    digitaltwin.EntityTypeGateway,
				Gateway: nil,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := NewCreateEntityResponseDTO(tc.res)
			if err == nil {
				t.Fatalf("expected error for case %q, got nil", tc.name)
			}
		})
	}
}

func TestNewPostGatewayResponseDTO_ValidationRejections(t *testing.T) {
	ownerID := uuid.MustParse("3f2a1b4c-0000-4000-8000-000000000001")
	createdAt := time.Date(2026, 10, 17, 8, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		res  *gateway.ProvisionGatewayResult
	}{
		{name: "nil result", res: nil},
		{
			name: "invalid gateway ID",
			res: &gateway.ProvisionGatewayResult{
				GatewayID:   "invalid gateway!",
				Name:        "GW",
				OwnerUserID: ownerID,
				EntityID:    "urn:ngsi-ld:Gateway:invalid_gw",
				CreatedAt:   &createdAt,
			},
		},
		{
			name: "empty name",
			res: &gateway.ProvisionGatewayResult{
				GatewayID:   "gw1",
				Name:        "",
				OwnerUserID: ownerID,
				EntityID:    "urn:ngsi-ld:Gateway:gw1",
				CreatedAt:   &createdAt,
			},
		},
		{
			name: "nil owner user ID",
			res: &gateway.ProvisionGatewayResult{
				GatewayID:   "gw1",
				Name:        "GW1",
				OwnerUserID: uuid.Nil,
				EntityID:    "urn:ngsi-ld:Gateway:gw1",
				CreatedAt:   &createdAt,
			},
		},
		{
			name: "invalid entity URN syntax",
			res: &gateway.ProvisionGatewayResult{
				GatewayID:   "gw1",
				Name:        "GW1",
				OwnerUserID: ownerID,
				EntityID:    "not-a-urn",
				CreatedAt:   &createdAt,
			},
		},
		{
			name: "entity URN wrong type",
			res: &gateway.ProvisionGatewayResult{
				GatewayID:   "gw1",
				Name:        "GW1",
				OwnerUserID: ownerID,
				EntityID:    "urn:ngsi-ld:Sensor:gw1:s1",
				CreatedAt:   &createdAt,
			},
		},
		{
			name: "entity URN gateway_id mismatch",
			res: &gateway.ProvisionGatewayResult{
				GatewayID:   "gw1",
				Name:        "GW1",
				OwnerUserID: ownerID,
				EntityID:    "urn:ngsi-ld:Gateway:gw2",
				CreatedAt:   &createdAt,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewPostGatewayResponseDTO(tc.res)
			if err == nil {
				t.Fatalf("expected error for case %q, got nil", tc.name)
			}
		})
	}
}

func TestNewPostSensorResponseDTO_ValidationRejections(t *testing.T) {
	createdAt := time.Date(2026, 10, 17, 8, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		res  *gateway.ProvisionSensorResult
	}{
		{name: "nil result", res: nil},
		{
			name: "invalid gateway ID",
			res: &gateway.ProvisionSensorResult{
				GatewayID: "invalid gateway!",
				SensorID:  "s1",
				Name:      "S1",
				EntityID:  "urn:ngsi-ld:Sensor:invalid:s1",
				CreatedAt: &createdAt,
			},
		},
		{
			name: "invalid sensor ID",
			res: &gateway.ProvisionSensorResult{
				GatewayID: "gw1",
				SensorID:  "invalid/sensor",
				Name:      "S1",
				EntityID:  "urn:ngsi-ld:Sensor:gw1:invalid/sensor",
				CreatedAt: &createdAt,
			},
		},
		{
			name: "empty name",
			res: &gateway.ProvisionSensorResult{
				GatewayID: "gw1",
				SensorID:  "s1",
				Name:      "",
				EntityID:  "urn:ngsi-ld:Sensor:gw1:s1",
				CreatedAt: &createdAt,
			},
		},
		{
			name: "entity URN wrong type",
			res: &gateway.ProvisionSensorResult{
				GatewayID: "gw1",
				SensorID:  "s1",
				Name:      "S1",
				EntityID:  "urn:ngsi-ld:Gateway:gw1",
				CreatedAt: &createdAt,
			},
		},
		{
			name: "entity URN gateway mismatch",
			res: &gateway.ProvisionSensorResult{
				GatewayID: "gw1",
				SensorID:  "s1",
				Name:      "S1",
				EntityID:  "urn:ngsi-ld:Sensor:gw2:s1",
				CreatedAt: &createdAt,
			},
		},
		{
			name: "entity URN sensor mismatch",
			res: &gateway.ProvisionSensorResult{
				GatewayID: "gw1",
				SensorID:  "s1",
				Name:      "S1",
				EntityID:  "urn:ngsi-ld:Sensor:gw1:s2",
				CreatedAt: &createdAt,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewPostSensorResponseDTO(tc.res)
			if err == nil {
				t.Fatalf("expected error for case %q, got nil", tc.name)
			}
		})
	}
}

func TestNewPatchResponseDTO_ValidationRejections(t *testing.T) {
	updatedAt := time.Date(2026, 10, 17, 8, 12, 0, 0, time.UTC)

	t.Run("Gateway rejections", func(t *testing.T) {
		// Wrong entity type
		_, err := NewPatchGatewayResponseDTO(digitaltwin.PatchGatewayResult{
			ID:        "urn:ngsi-ld:Gateway:gw1",
			Type:      digitaltwin.EntityTypeSensor,
			GatewayID: "gw1",
			Name:      "GW1",
			UpdatedAt: updatedAt,
		})
		if err == nil {
			t.Fatal("expected error on wrong entity type")
		}

		// Empty name
		_, err = NewPatchGatewayResponseDTO(digitaltwin.PatchGatewayResult{
			ID:        "urn:ngsi-ld:Gateway:gw1",
			Type:      digitaltwin.EntityTypeGateway,
			GatewayID: "gw1",
			Name:      "",
			UpdatedAt: updatedAt,
		})
		if err == nil {
			t.Fatal("expected error on empty name")
		}

		// URN mismatch
		_, err = NewPatchGatewayResponseDTO(digitaltwin.PatchGatewayResult{
			ID:        "urn:ngsi-ld:Gateway:gw2",
			Type:      digitaltwin.EntityTypeGateway,
			GatewayID: "gw1",
			Name:      "GW1",
			UpdatedAt: updatedAt,
		})
		if err == nil {
			t.Fatal("expected error on URN gateway mismatch")
		}
	})

	t.Run("Sensor rejections", func(t *testing.T) {
		// Wrong entity type
		_, err := NewPatchSensorResponseDTO(digitaltwin.PatchSensorResult{
			ID:        "urn:ngsi-ld:Sensor:gw1:s1",
			Type:      digitaltwin.EntityTypeGateway,
			GatewayID: "gw1",
			SensorID:  "s1",
			Name:      "S1",
			UpdatedAt: updatedAt,
		})
		if err == nil {
			t.Fatal("expected error on wrong entity type")
		}

		// Empty name
		_, err = NewPatchSensorResponseDTO(digitaltwin.PatchSensorResult{
			ID:        "urn:ngsi-ld:Sensor:gw1:s1",
			Type:      digitaltwin.EntityTypeSensor,
			GatewayID: "gw1",
			SensorID:  "s1",
			Name:      "",
			UpdatedAt: updatedAt,
		})
		if err == nil {
			t.Fatal("expected error on empty name")
		}

		// URN sensor mismatch
		_, err = NewPatchSensorResponseDTO(digitaltwin.PatchSensorResult{
			ID:        "urn:ngsi-ld:Sensor:gw1:s2",
			Type:      digitaltwin.EntityTypeSensor,
			GatewayID: "gw1",
			SensorID:  "s1",
			Name:      "S1",
			UpdatedAt: updatedAt,
		})
		if err == nil {
			t.Fatal("expected error on URN sensor mismatch")
		}
	})
}

func TestClassifyMutationError(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantStatus  int
		wantCode    string
		wantMessage string
	}{
		{
			name:        "nil error",
			err:         nil,
			wantStatus:  http.StatusOK,
			wantCode:    "",
			wantMessage: "",
		},
		// 503 Service Unavailable family
		{
			name:        "context.Canceled",
			err:         context.Canceled,
			wantStatus:  http.StatusServiceUnavailable,
			wantCode:    "service_unavailable",
			wantMessage: "service unavailable",
		},
		{
			name:        "context.DeadlineExceeded",
			err:         context.DeadlineExceeded,
			wantStatus:  http.StatusServiceUnavailable,
			wantCode:    "service_unavailable",
			wantMessage: "service unavailable",
		},
		{
			name:        "digitaltwin.ErrDependencyUnavailable",
			err:         digitaltwin.ErrDependencyUnavailable,
			wantStatus:  http.StatusServiceUnavailable,
			wantCode:    "service_unavailable",
			wantMessage: "service unavailable",
		},
		{
			name:        "digitaltwin.ErrMetadataPatchUnavailable",
			err:         digitaltwin.ErrMetadataPatchUnavailable,
			wantStatus:  http.StatusServiceUnavailable,
			wantCode:    "service_unavailable",
			wantMessage: "service unavailable",
		},
		{
			name:        "gateway.ErrGatewayProvisioningUnavailable",
			err:         gateway.ErrGatewayProvisioningUnavailable,
			wantStatus:  http.StatusServiceUnavailable,
			wantCode:    "service_unavailable",
			wantMessage: "service unavailable",
		},
		{
			name:        "gateway.ErrAdminOrOwnerProvisioningUnavailable",
			err:         gateway.ErrAdminOrOwnerProvisioningUnavailable,
			wantStatus:  http.StatusServiceUnavailable,
			wantCode:    "service_unavailable",
			wantMessage: "service unavailable",
		},
		{
			name:        "gateway.ErrOwnerProvisioningUnavailable",
			err:         gateway.ErrOwnerProvisioningUnavailable,
			wantStatus:  http.StatusServiceUnavailable,
			wantCode:    "service_unavailable",
			wantMessage: "service unavailable",
		},
		{
			name:        "gateway.ErrProvisioningAdminLookup",
			err:         gateway.ErrProvisioningAdminLookup,
			wantStatus:  http.StatusServiceUnavailable,
			wantCode:    "service_unavailable",
			wantMessage: "service unavailable",
		},
		{
			name:        "wrapped 503 error",
			err:         fmt.Errorf("connection lost during commit: %w", digitaltwin.ErrMetadataPatchUnavailable),
			wantStatus:  http.StatusServiceUnavailable,
			wantCode:    "service_unavailable",
			wantMessage: "service unavailable",
		},
		// 400 Bad Request family
		{
			name:        "digitaltwin.ErrInvalidInput",
			err:         digitaltwin.ErrInvalidInput,
			wantStatus:  http.StatusBadRequest,
			wantCode:    "invalid_request",
			wantMessage: "invalid request",
		},
		{
			name:        "digitaltwin.ErrNilActor",
			err:         digitaltwin.ErrNilActor,
			wantStatus:  http.StatusBadRequest,
			wantCode:    "invalid_request",
			wantMessage: "invalid request",
		},
		{
			name:        "gateway.ErrInvalidProvisioningInput",
			err:         gateway.ErrInvalidProvisioningInput,
			wantStatus:  http.StatusBadRequest,
			wantCode:    "invalid_request",
			wantMessage: "invalid request",
		},
		{
			name:        "gateway.ErrInvalidGatewayID",
			err:         gateway.ErrInvalidGatewayID,
			wantStatus:  http.StatusBadRequest,
			wantCode:    "invalid_request",
			wantMessage: "invalid request",
		},
		{
			name:        "gateway.ErrInvalidSensorID",
			err:         gateway.ErrInvalidSensorID,
			wantStatus:  http.StatusBadRequest,
			wantCode:    "invalid_request",
			wantMessage: "invalid request",
		},
		{
			name:        "wrapped 400 error",
			err:         errors.Join(digitaltwin.ErrInvalidInput, errors.New("name is blank")),
			wantStatus:  http.StatusBadRequest,
			wantCode:    "invalid_request",
			wantMessage: "invalid request",
		},
		// 403 Forbidden family
		{
			name:        "digitaltwin.ErrForbidden",
			err:         digitaltwin.ErrForbidden,
			wantStatus:  http.StatusForbidden,
			wantCode:    "forbidden",
			wantMessage: "access denied",
		},
		{
			name:        "digitaltwin.ErrProvisioningForbidden",
			err:         digitaltwin.ErrProvisioningForbidden,
			wantStatus:  http.StatusForbidden,
			wantCode:    "forbidden",
			wantMessage: "access denied",
		},
		{
			name:        "gateway.ErrAdminForbidden",
			err:         gateway.ErrAdminForbidden,
			wantStatus:  http.StatusForbidden,
			wantCode:    "forbidden",
			wantMessage: "access denied",
		},
		{
			name:        "wrapped 403 error",
			err:         fmt.Errorf("operator role cannot mutate: %w", digitaltwin.ErrForbidden),
			wantStatus:  http.StatusForbidden,
			wantCode:    "forbidden",
			wantMessage: "access denied",
		},
		// 404 Not Found family
		{
			name:        "digitaltwin.ErrNotFound",
			err:         digitaltwin.ErrNotFound,
			wantStatus:  http.StatusNotFound,
			wantCode:    "not_found",
			wantMessage: "resource not found",
		},
		{
			name:        "digitaltwin.ErrProvisioningNotFound",
			err:         digitaltwin.ErrProvisioningNotFound,
			wantStatus:  http.StatusNotFound,
			wantCode:    "not_found",
			wantMessage: "resource not found",
		},
		{
			name:        "wrapped 404 error",
			err:         fmt.Errorf("parent gateway not found: %w", digitaltwin.ErrNotFound),
			wantStatus:  http.StatusNotFound,
			wantCode:    "not_found",
			wantMessage: "resource not found",
		},
		// 409 Conflict family
		{
			name:        "digitaltwin.ErrProvisioningConflict",
			err:         digitaltwin.ErrProvisioningConflict,
			wantStatus:  http.StatusConflict,
			wantCode:    "conflict",
			wantMessage: "resource conflict",
		},
		{
			name:        "gateway.ErrProvisioningConflict",
			err:         gateway.ErrProvisioningConflict,
			wantStatus:  http.StatusConflict,
			wantCode:    "conflict",
			wantMessage: "resource conflict",
		},
		{
			name:        "wrapped 409 error",
			err:         fmt.Errorf("mismatched description on replay: %w", digitaltwin.ErrProvisioningConflict),
			wantStatus:  http.StatusConflict,
			wantCode:    "conflict",
			wantMessage: "resource conflict",
		},
		// 500 Internal Error family
		{
			name:        "digitaltwin.ErrInconsistentGraph",
			err:         digitaltwin.ErrInconsistentGraph,
			wantStatus:  http.StatusInternalServerError,
			wantCode:    "internal_error",
			wantMessage: "internal server error",
		},
		{
			name:        "digitaltwin.ErrProvisioningInconsistent",
			err:         digitaltwin.ErrProvisioningInconsistent,
			wantStatus:  http.StatusInternalServerError,
			wantCode:    "internal_error",
			wantMessage: "internal server error",
		},
		{
			name:        "digitaltwin.ErrUnexpected",
			err:         digitaltwin.ErrUnexpected,
			wantStatus:  http.StatusInternalServerError,
			wantCode:    "internal_error",
			wantMessage: "internal server error",
		},
		{
			name:        "arbitrary database error without leak",
			err:         errors.New("pq: syntax error at or near SELECT secret_token FROM users"),
			wantStatus:  http.StatusInternalServerError,
			wantCode:    "internal_error",
			wantMessage: "internal server error", // Crucial: secret_token not leaked
		},
		{
			name:        "arbitrary scan error",
			err:         errors.New("scan error: cannot decode into string"),
			wantStatus:  http.StatusInternalServerError,
			wantCode:    "internal_error",
			wantMessage: "internal server error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, code, msg := ClassifyMutationError(tc.err)
			if status != tc.wantStatus {
				t.Errorf("status = %d, want %d", status, tc.wantStatus)
			}
			if code != tc.wantCode {
				t.Errorf("code = %q, want %q", code, tc.wantCode)
			}
			if msg != tc.wantMessage {
				t.Errorf("message = %q, want %q", msg, tc.wantMessage)
			}
		})
	}
}

func TestClassifyMutationParserStatus(t *testing.T) {
	tests := []struct {
		status      int
		wantCode    string
		wantMessage string
	}{
		{
			status:      http.StatusRequestEntityTooLarge,
			wantCode:    "payload_too_large",
			wantMessage: "payload too large",
		},
		{
			status:      http.StatusUnsupportedMediaType,
			wantCode:    "unsupported_media_type",
			wantMessage: "unsupported media type",
		},
		{
			status:      http.StatusBadRequest,
			wantCode:    "invalid_request",
			wantMessage: "invalid request",
		},
		{
			status:      500,
			wantCode:    "invalid_request",
			wantMessage: "invalid request",
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("status_%d", tc.status), func(t *testing.T) {
			code, msg := ClassifyMutationParserStatus(tc.status)
			if code != tc.wantCode {
				t.Errorf("code = %q, want %q", code, tc.wantCode)
			}
			if msg != tc.wantMessage {
				t.Errorf("message = %q, want %q", msg, tc.wantMessage)
			}
		})
	}
}

// Package httpserver provides HTTP boundary handlers and router wiring.
//
// STRICTLY TEST-ONLY SCAFFOLDING NOTICE:
// The types and helpers in this file (AttachTestStage4Dependencies, inert readers/creators/patchers)
// exist strictly as test scaffolding for non-Stage-4 test suites (e.g. credential, provisioning,
// and authorization integration tests) that require default Stage 4 router fields to be present.
// They MUST NEVER be imported, referenced, or used as a fallback in production server composition
// or runtime code. Real server composition is performed exclusively via composeStage4Dependencies.
package httpserver

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/config"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetry"
)

type inertTelemetryHistoryReader struct{}

func (inertTelemetryHistoryReader) GetTelemetryHistory(context.Context, uuid.UUID, telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
	now := time.Now().UTC()
	return &telemetry.TelemetryHistoryResult{
		GatewayID:    "test-gw",
		SensorID:     "test-s",
		From:         now.Add(-time.Hour),
		To:           now,
		Mode:         "raw",
		Resolution:   "raw",
		PointCount:   0,
		RawPoints:    []telemetry.RawSamplePoint{},
		BucketPoints: []telemetry.BucketSamplePoint{},
	}, nil
}

type inertTwinListReader struct{}

func (inertTwinListReader) ListTwins(context.Context, uuid.UUID, digitaltwin.TwinListQuery) (*digitaltwin.TwinListResult, error) {
	return &digitaltwin.TwinListResult{Items: []digitaltwin.TwinSummary{}}, nil
}

type inertTwinDetailReader struct{}

func (inertTwinDetailReader) GetTwinDetail(_ context.Context, _ uuid.UUID, q digitaltwin.TwinDetailQuery) (*digitaltwin.TwinDetailResult, error) {
	id := q.EntityID
	if id == "" {
		id = "urn:ngsi-ld:Gateway:test-gw"
	}
	return &digitaltwin.TwinDetailResult{
		ID:            id,
		Type:          digitaltwin.EntityTypeGateway,
		Name:          "Test Gateway",
		GatewayID:     "test-gw",
		Relationships: []digitaltwin.RelationshipItem{},
	}, nil
}

type inertTwinStateReader struct{}

func (inertTwinStateReader) GetTwinState(_ context.Context, _ uuid.UUID, q digitaltwin.TwinStateQuery) (*digitaltwin.TwinStateResult, error) {
	id := q.EntityID
	if id == "" {
		id = "urn:ngsi-ld:Gateway:test-gw"
	}
	return &digitaltwin.TwinStateResult{
		ID:              id,
		Type:            digitaltwin.EntityTypeGateway,
		GatewayID:       "test-gw",
		ReportedState:   json.RawMessage(`{}`),
		DesiredState:    json.RawMessage(`{}`),
		ReportedVersion: 1,
		DesiredVersion:  1,
	}, nil
}

type inertTwinHistoryReader struct{}

func (inertTwinHistoryReader) GetTwinHistory(_ context.Context, _ uuid.UUID, q digitaltwin.TwinHistoryQuery) (*digitaltwin.TwinHistoryResult, error) {
	now := time.Now().UTC()
	id := q.EntityID
	if id == "" {
		id = "urn:ngsi-ld:Gateway:test-gw"
	}
	prop := q.Property
	if prop == "" {
		prop = digitaltwin.SelectorValue
	}
	return &digitaltwin.TwinHistoryResult{
		EntityID:   id,
		Property:   prop,
		From:       now.Add(-time.Hour),
		To:         now,
		Mode:       "raw",
		Resolution: "raw",
		PointCount: 0,
		TelemetryResult: &telemetry.TelemetryHistoryResult{
			GatewayID:    "test-gw",
			SensorID:     "test-s",
			From:         now.Add(-time.Hour),
			To:           now,
			Mode:         "raw",
			Resolution:   "raw",
			PointCount:   0,
			RawPoints:    []telemetry.RawSamplePoint{},
			BucketPoints: []telemetry.BucketSamplePoint{},
		},
	}, nil
}

type inertEntityCreator struct{}

func (inertEntityCreator) CreateEntity(_ context.Context, _ uuid.UUID, _ digitaltwin.CreateEntityInput) (digitaltwin.CreateEntityResult, error) {
	now := time.Now().UTC()
	return digitaltwin.CreateEntityResult{
		Created:  true,
		Type:     digitaltwin.EntityTypeGateway,
		EntityID: "urn:ngsi-ld:Gateway:test-gw",
		Gateway: &gateway.ProvisionGatewayResult{
			Created:     true,
			GatewayID:   "test-gw",
			Name:        "Test Gateway",
			EntityID:    "urn:ngsi-ld:Gateway:test-gw",
			OwnerUserID: uuid.New(),
			CreatedAt:   &now,
		},
	}, nil
}

type inertGatewayPatcher struct{}

func (inertGatewayPatcher) PatchGateway(_ context.Context, _ uuid.UUID, gatewayID string, _ digitaltwin.PatchGatewayInput) (digitaltwin.PatchGatewayResult, error) {
	if gatewayID == "" {
		gatewayID = "test-gw"
	}
	return digitaltwin.PatchGatewayResult{
		ID:        "urn:ngsi-ld:Gateway:" + gatewayID,
		Type:      digitaltwin.EntityTypeGateway,
		GatewayID: gatewayID,
		Name:      "Test Gateway",
		UpdatedAt: time.Now().UTC(),
	}, nil
}

type inertSensorPatcher struct{}

func (inertSensorPatcher) PatchSensor(_ context.Context, _ uuid.UUID, gatewayID string, sensorID string, _ digitaltwin.PatchSensorInput) (digitaltwin.PatchSensorResult, error) {
	if gatewayID == "" {
		gatewayID = "test-gw"
	}
	if sensorID == "" {
		sensorID = "test-s"
	}
	unit := "celsius"
	return digitaltwin.PatchSensorResult{
		ID:        "urn:ngsi-ld:Sensor:" + gatewayID + ":" + sensorID,
		Type:      digitaltwin.EntityTypeSensor,
		GatewayID: gatewayID,
		SensorID:  sensorID,
		Name:      "Test Sensor",
		Unit:      &unit,
		UpdatedAt: time.Now().UTC(),
	}, nil
}

// AttachTestStage4Dependencies populates default Stage 4 mock use-cases and parsers
// into a RouterDependencies struct for testing purposes if they are not already set.
func AttachTestStage4Dependencies(deps RouterDependencies) RouterDependencies {
	if deps.HistoryQueryParser == nil {
		cfg := config.DefaultHistoryConfig()
		deps.HistoryQueryParser, _ = NewHistoryQueryParser(&cfg)
	}
	if deps.TwinQueryParser == nil {
		cfg := config.DefaultTwinConfig()
		deps.TwinQueryParser, _ = NewTwinQueryParser(&cfg)
	}
	if deps.TwinMaxBodyBytes == 0 {
		deps.TwinMaxBodyBytes = 16384
	}
	if deps.TelemetryHistoryReader == nil {
		deps.TelemetryHistoryReader = inertTelemetryHistoryReader{}
	}
	if deps.TwinListReader == nil {
		deps.TwinListReader = inertTwinListReader{}
	}
	if deps.TwinDetailReader == nil {
		deps.TwinDetailReader = inertTwinDetailReader{}
	}
	if deps.TwinStateReader == nil {
		deps.TwinStateReader = inertTwinStateReader{}
	}
	if deps.TwinHistoryReader == nil {
		deps.TwinHistoryReader = inertTwinHistoryReader{}
	}
	if deps.EntityCreator == nil {
		deps.EntityCreator = inertEntityCreator{}
	}
	if deps.GatewayPatcher == nil {
		deps.GatewayPatcher = inertGatewayPatcher{}
	}
	if deps.SensorPatcher == nil {
		deps.SensorPatcher = inertSensorPatcher{}
	}
	return deps
}

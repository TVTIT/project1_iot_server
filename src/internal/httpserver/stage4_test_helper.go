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
	"iot-platform/internal/usermanagement"
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

type inertReadinessChecker struct{}

func (inertReadinessChecker) Ping(context.Context) error { return nil }

type inertSensorProvisioner struct{}

func (inertSensorProvisioner) ProvisionSensor(context.Context, uuid.UUID, gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error) {
	return gateway.ProvisionSensorResult{}, gateway.ErrProvisioningInconsistent
}

type inertOwnerSensorProvisioner struct{}

func (inertOwnerSensorProvisioner) ProvisionSensorForOwner(context.Context, uuid.UUID, gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error) {
	return gateway.ProvisionSensorResult{}, gateway.ErrProvisioningInconsistent
}

type inertGatewayProvisioner struct{}

func (inertGatewayProvisioner) ProvisionGateway(context.Context, uuid.UUID, gateway.ProvisionGatewayInput) (gateway.ProvisionGatewayResult, error) {
	return gateway.ProvisionGatewayResult{}, gateway.ErrProvisioningInconsistent
}

type inertSensorReader struct{}

func (inertSensorReader) ListSensorsForUserAndGateway(context.Context, uuid.UUID, string) ([]gateway.Sensor, error) {
	return nil, nil
}

type inertGatewayReader struct{}

func (inertGatewayReader) ListGatewaysForUser(context.Context, uuid.UUID) ([]gateway.Gateway, error) {
	return nil, nil
}

type inertAdminGatewayLister struct{}

func (inertAdminGatewayLister) ListAdminGatewayInventory(context.Context, uuid.UUID, gateway.AdminGatewayInventoryQuery) (gateway.AdminGatewayInventoryPage, error) {
	return gateway.AdminGatewayInventoryPage{Items: []gateway.AdminGateway{}}, nil
}

type inertGatewayOwnerTransferer struct{}

func (inertGatewayOwnerTransferer) TransferGatewayOwner(context.Context, uuid.UUID, string, uuid.UUID) error {
	return nil
}

type inertGatewayUserAssigner struct{}

func (inertGatewayUserAssigner) AssignGatewayUser(context.Context, uuid.UUID, string, uuid.UUID, string) error {
	return nil
}

type inertUserUpdater struct{}

func (inertUserUpdater) UpdateUser(context.Context, uuid.UUID, uuid.UUID, usermanagement.UpdateUserInput) (usermanagement.UpdatedUser, error) {
	return usermanagement.UpdatedUser{}, nil
}

type inertUserLister struct{}

func (inertUserLister) ListUsers(context.Context, uuid.UUID) ([]usermanagement.UserSummary, error) {
	return []usermanagement.UserSummary{}, nil
}

type inertUserCreator struct{}

func (inertUserCreator) CreateUser(context.Context, uuid.UUID, usermanagement.CreateUserInput) (usermanagement.CreatedUser, error) {
	return usermanagement.CreatedUser{}, nil
}

// AttachTestUserManagementDependencies populates default user management mock use-cases
// into a RouterDependencies struct for testing purposes if they are not already set.
func AttachTestUserManagementDependencies(deps RouterDependencies) RouterDependencies {
	if deps.UserCreator == nil {
		deps.UserCreator = inertUserCreator{}
	}
	if deps.UserLister == nil {
		deps.UserLister = inertUserLister{}
	}
	if deps.UserUpdater == nil {
		deps.UserUpdater = inertUserUpdater{}
	}
	if deps.GatewayUserAssigner == nil {
		deps.GatewayUserAssigner = inertGatewayUserAssigner{}
	}
	if deps.GatewayOwnerTransferer == nil {
		deps.GatewayOwnerTransferer = inertGatewayOwnerTransferer{}
	}
	return deps
}

// DefaultTestRouterDependencies returns RouterDependencies populated with inert mocks suitable for testing.
func DefaultTestRouterDependencies() RouterDependencies {
	deps := RouterDependencies{
		ReadinessChecker:           inertReadinessChecker{},
		ReadinessTimeout:           time.Second,
		AuthorizationTimeout:       time.Second,
		AdminMaxBodyBytes:          16384,
		AdminInventoryDefaultLimit: 100,
		AdminInventoryMaxLimit:     200,
		SensorProvisioner:          inertSensorProvisioner{},
		OwnerSensorProvisioner:     inertOwnerSensorProvisioner{},
		GatewayProvisioner:         inertGatewayProvisioner{},
		SensorReader:               inertSensorReader{},
		GatewayReader:              inertGatewayReader{},
		AdminGatewayLister:         inertAdminGatewayLister{},
	}
	deps = AttachTestStage4Dependencies(deps)
	return AttachTestUserManagementDependencies(deps)
}

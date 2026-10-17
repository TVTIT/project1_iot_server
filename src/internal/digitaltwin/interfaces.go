package digitaltwin

import (
	"context"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetry"
)

// ResolvedGateway is an authorized internal representation of a Gateway.
type ResolvedGateway struct {
	InternalID uuid.UUID
	GatewayID  string
}

// GatewayResolver resolves Gateway existence and authorization under the actor's membership.
type GatewayResolver interface {
	ResolveGateway(ctx context.Context, actor uuid.UUID, gatewayID string) (*ResolvedGateway, error)
}

// EntityResolver resolves entity existence and authorization under the actor's membership.
// Implementations MUST enforce that the caller (actor) holds access in user_gateways.
type EntityResolver interface {
	ResolveEntity(ctx context.Context, actor uuid.UUID, parsed ParsedIdentity) (*ResolvedEntity, error)
}

// ResourceResolver unifies Gateway, Sensor, and Digital Twin entity resolution under actor membership.
type ResourceResolver interface {
	EntityResolver
	GatewayResolver
	telemetry.SensorResolver
}

// TwinReader executes authorized data reads for Digital Twin resources.
// Every method requires the actor uuid.UUID to enforce query-level membership scoping.
type TwinReader interface {
	ListEntities(ctx context.Context, actor uuid.UUID, q TwinListQuery) (*TwinListResult, error)
	GetDetail(ctx context.Context, actor uuid.UUID, entity ResolvedEntity, q TwinDetailQuery) (*TwinDetailResult, error)
	GetState(ctx context.Context, actor uuid.UUID, entity ResolvedEntity) (*TwinStateResult, error)
	ReadTemporalHistory(ctx context.Context, actor uuid.UUID, entity ResolvedEntity, q TwinHistoryQuery) (*TwinHistoryResult, error)
}

// TelemetryHistoryDelegator executes delegated history queries within an existing scoped context.
// Reused by Digital Twin history service when delegating property=value to the telemetry history engine.
type TelemetryHistoryDelegator interface {
	ExecuteEffectiveHistoryQuery(ctx context.Context, actor uuid.UUID, q telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error)
}

// ReadService defines the application use-case boundary for Digital Twin read operations.
// Actor identity is passed explicitly and must not be uuid.Nil.
type ReadService interface {
	ListTwins(ctx context.Context, actor uuid.UUID, q TwinListQuery) (*TwinListResult, error)
	GetTwinDetail(ctx context.Context, actor uuid.UUID, q TwinDetailQuery) (*TwinDetailResult, error)
	GetTwinState(ctx context.Context, actor uuid.UUID, q TwinStateQuery) (*TwinStateResult, error)
	GetTwinHistory(ctx context.Context, actor uuid.UUID, q TwinHistoryQuery) (*TwinHistoryResult, error)
}

// GatewayProvisioner defines the gateway provisioning dependency needed by Digital Twin creation.
type GatewayProvisioner interface {
	ProvisionGateway(ctx context.Context, actor uuid.UUID, input gateway.ProvisionGatewayInput) (gateway.ProvisionGatewayResult, error)
}

// SensorProvisioner defines the admin-or-owner sensor provisioning dependency needed by Digital Twin creation.
type SensorProvisioner interface {
	ProvisionSensorForAdminOrOwner(ctx context.Context, actor uuid.UUID, input gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error)
}

// EntityCreateService defines the application use-case boundary for Digital Twin entity creation.
// It delegates to transactional Gateway and Sensor provisioners without out-of-transaction role prechecks.
type EntityCreateService interface {
	CreateEntity(ctx context.Context, actor uuid.UUID, input CreateEntityInput) (CreateEntityResult, error)
}

// GatewayMetadataPatcher defines the persistence boundary for patching Gateway metadata in a single transaction.
type GatewayMetadataPatcher interface {
	PatchGatewayMetadata(ctx context.Context, actor uuid.UUID, gatewayID string, input PatchGatewayInput) (PatchGatewayResult, error)
}

// GatewayPatchService defines the application use-case boundary for Gateway metadata PATCH.
type GatewayPatchService interface {
	PatchGateway(ctx context.Context, actor uuid.UUID, gatewayID string, input PatchGatewayInput) (PatchGatewayResult, error)
}

// SensorMetadataPatcher defines the persistence boundary for patching Sensor metadata in a single transaction.
type SensorMetadataPatcher interface {
	PatchSensorMetadata(ctx context.Context, actor uuid.UUID, gatewayID string, sensorID string, input PatchSensorInput) (PatchSensorResult, error)
}

// SensorPatchService defines the application use-case boundary for Sensor metadata PATCH.
type SensorPatchService interface {
	PatchSensor(ctx context.Context, actor uuid.UUID, gatewayID string, sensorID string, input PatchSensorInput) (PatchSensorResult, error)
}

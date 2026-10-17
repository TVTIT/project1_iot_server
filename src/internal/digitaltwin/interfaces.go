package digitaltwin

import (
	"context"

	"github.com/google/uuid"

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

// ReadService defines the application use-case boundary for Digital Twin read operations.
// Actor identity is passed explicitly and must not be uuid.Nil.
type ReadService interface {
	ListTwins(ctx context.Context, actor uuid.UUID, q TwinListQuery) (*TwinListResult, error)
	GetTwinDetail(ctx context.Context, actor uuid.UUID, q TwinDetailQuery) (*TwinDetailResult, error)
	GetTwinState(ctx context.Context, actor uuid.UUID, q TwinStateQuery) (*TwinStateResult, error)
	GetTwinHistory(ctx context.Context, actor uuid.UUID, q TwinHistoryQuery) (*TwinHistoryResult, error)
}

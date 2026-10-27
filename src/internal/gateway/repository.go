package gateway

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// ErrNotFound intentionally conflates missing and inaccessible Gateways.
var ErrNotFound = errors.New("gateway not found")

// SubscriptionTargetRepository checks membership access and entity existence for subscription targets.
type SubscriptionTargetRepository interface {
	AuthorizeSubscriptionTarget(ctx context.Context, userID uuid.UUID, gatewayID string, sensorID string) (Role, error)
}

// Repository reads metadata under the caller's PostgreSQL membership.
// Empty lists are nonnil. Implementations must propagate query errors.
type Repository interface {
	ListGatewaysForUser(context.Context, uuid.UUID) ([]Gateway, error)
	ListSensorsForUserAndGateway(context.Context, uuid.UUID, string) ([]Sensor, error)
	GetGatewayRole(context.Context, uuid.UUID, string) (Role, error)
	AuthorizeSubscriptionTarget(context.Context, uuid.UUID, string, string) (Role, error)
}

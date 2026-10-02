package gateway

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// ErrNotFound intentionally conflates missing and inaccessible Gateways.
var ErrNotFound = errors.New("gateway not found")

// Repository reads metadata under the caller's PostgreSQL membership.
// Empty lists are nonnil. Implementations must propagate query errors.
type Repository interface {
	ListGatewaysForUser(context.Context, uuid.UUID) ([]Gateway, error)
	ListSensorsForUserAndGateway(context.Context, uuid.UUID, string) ([]Sensor, error)
	GetGatewayRole(context.Context, uuid.UUID, string) (Role, error)
}

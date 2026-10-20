package usermanagement

import (
	"context"

	"github.com/google/uuid"
)

// UserCreator creates a new user account.
type UserCreator interface {
	CreateUser(ctx context.Context, actorID uuid.UUID, input CreateUserInput) (CreatedUser, error)
}

// UserLister lists all users with their memberships.
type UserLister interface {
	ListUsers(ctx context.Context, actorID uuid.UUID) ([]UserSummary, error)
}

// UserUpdater updates a user's full name and/or platform role.
type UserUpdater interface {
	UpdateUser(ctx context.Context, actorID, targetID uuid.UUID, input UpdateUserInput) (UpdatedUser, error)
}

// GatewayUserAssigner assigns an operator or viewer role on a gateway.
type GatewayUserAssigner interface {
	AssignGatewayUser(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID, role string) error
}

// GatewayUserRemover removes only an operator/viewer membership.
type GatewayUserRemover interface {
	RemoveGatewayUser(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID) error
}

// GatewayOwnerTransferer transfers ownership of a gateway atomically.
type GatewayOwnerTransferer interface {
	TransferGatewayOwner(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID) error
}

// AuthAdminClient performs administrative identity operations against the Auth service.
type AuthAdminClient interface {
	CreateUser(ctx context.Context, input AuthAdminCreateUserInput) (AuthAdminUser, error)
}

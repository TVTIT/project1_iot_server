package usermanagement

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// UserProjectionRow represents a single row returned from the bounded user projection query.
type UserProjectionRow struct {
	UserID        uuid.UUID
	Email         string
	FullName      *string
	PlatformRole  string
	CreatedAt     time.Time
	ProfileExists bool
}

// UserGatewayRoleRow represents a single membership association.
type UserGatewayRoleRow struct {
	UserID    uuid.UUID
	GatewayID string
	Role      string
}

// Repository defines database operations for admin user management.
type Repository interface {
	VerifyActorIsAdmin(ctx context.Context, actorID uuid.UUID) error
	VerifyCreatedUserProfile(ctx context.Context, targetID uuid.UUID, expectedFullName string) error
	ListUsersProjection(ctx context.Context, actorID uuid.UUID, limit int) ([]UserProjectionRow, error)
	ListUserGatewayRoles(ctx context.Context, userIDs []uuid.UUID, limit int) ([]UserGatewayRoleRow, error)
	UpdateUserProfileAndRole(ctx context.Context, actorID, targetID uuid.UUID, fullName *string, platformRole *string) (UpdatedUser, error)
	AssignGatewayUser(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID, role string) error
	RemoveGatewayUser(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID) error
	TransferGatewayOwner(ctx context.Context, actorID uuid.UUID, gatewayID string, targetID uuid.UUID) error
}

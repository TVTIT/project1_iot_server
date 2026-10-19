package usermanagement

import (
	"time"

	"github.com/google/uuid"
)

// CreateUserInput carries validated parameters for creating an account.
type CreateUserInput struct {
	FullName string `json:"full_name"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

// CreatedUser represents the response after successful account creation.
type CreatedUser struct {
	ID           uuid.UUID `json:"id"`
	Email        string    `json:"email"`
	FullName     string    `json:"full_name"`
	PlatformRole string    `json:"platform_role"`
	CreatedAt    time.Time `json:"created_at"`
}

// UpdateUserInput represents requested changes to user profile and global role.
type UpdateUserInput struct {
	FullName     *string `json:"full_name,omitempty"`
	PlatformRole *string `json:"platform_role,omitempty"`
}

// UpdatedUser represents the confirmed state after user update.
type UpdatedUser struct {
	ID           uuid.UUID `json:"id"`
	FullName     *string   `json:"full_name"`
	PlatformRole string    `json:"platform_role"`
}

// UserSummary carries projection information for the admin user listing.
type UserSummary struct {
	ID           uuid.UUID         `json:"id"`
	Email        string            `json:"email"`
	FullName     *string           `json:"full_name"`
	PlatformRole string            `json:"platform_role"`
	CreatedAt    time.Time         `json:"created_at"`
	GatewayRoles []UserGatewayRole `json:"gateway_roles"`
}

// UserGatewayRole describes a membership role for a user on a specific gateway.
type UserGatewayRole struct {
	GatewayID string `json:"gateway_id"`
	Role      string `json:"role"`
}

// UserListConfig holds capacity bounds for admin user listing.
type UserListConfig struct {
	MaxUsers         int // default 1000
	MaxMemberships   int // default 10000
	MaxResponseBytes int // default 2 * 1024 * 1024 (2 MiB)
}

// DefaultUserListConfig returns the standard defaults from the specification.
func DefaultUserListConfig() UserListConfig {
	return UserListConfig{
		MaxUsers:         1000,
		MaxMemberships:   10000,
		MaxResponseBytes: 2 * 1024 * 1024,
	}
}

// AuthAdminUser represents the user created by the upstream Auth Admin API.
type AuthAdminUser struct {
	ID        uuid.UUID `json:"id"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}

// AuthAdminCreateUserInput carries the payload for the upstream Auth admin creation request.
type AuthAdminCreateUserInput struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	FullName string `json:"full_name"`
}

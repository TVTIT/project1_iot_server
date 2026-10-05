// Package gateway defines Gateway metadata and membership-scoped reads.
package gateway

import "time"

// Role is the per-Gateway membership role, not a JWT or platform-admin role.
type Role string

// Membership roles match the user_gateways CHECK constraint.
const (
	RoleOwner    Role = "owner"
	RoleOperator Role = "operator"
	RoleViewer   Role = "viewer"
)

// Gateway contains only readable metadata and the caller's membership role.
type Gateway struct {
	GatewayID   string
	Name        string
	Description *string
	Role        Role
	CreatedAt   *time.Time
}

// Sensor inherits authorization exclusively from its parent Gateway.
type Sensor struct {
	SensorID  string
	Name      string
	Unit      *string
	CreatedAt *time.Time
}

package gateway

import (
	"context"

	"github.com/google/uuid"
)

// ProvisioningRepository is separate from membership-scoped read Repository.
// Each operation owns one complete atomic transaction and rechecks actorUserID
// against platform_admins inside that transaction; middleware is not sufficient.
// Inputs must pass the corresponding ValidateProvision*Input function first.
// Implementations use the supplied context for every transaction operation and
// return results only after commit, propagating cancellation/deadline errors.
//
// Missing owner/parent returns ErrProvisioningNotFound; a non-admin actor returns
// ErrProvisioningForbidden; changed metadata/owner returns ErrProvisioningConflict.
// Missing or incorrect existing graph returns ErrProvisioningInconsistent, never
// silent repair. Other database failures remain internal errors for safe mapping.
type ProvisioningRepository interface {
	// ProvisionGateway creates Gateway, owner membership, Gateway Twin and empty
	// state together. A matching retry is a no-op, not an ownership/state reset.
	ProvisionGateway(context.Context, uuid.UUID, ProvisionGatewayInput) (ProvisionGatewayResult, error)
	// ProvisionSensor verifies and locks its parent, then creates Sensor, Twin,
	// empty state and hasSensor together. A matching retry verifies the graph
	// without restoring a removed relationship or resetting current state.
	ProvisionSensor(context.Context, uuid.UUID, ProvisionSensorInput) (ProvisionSensorResult, error)
}

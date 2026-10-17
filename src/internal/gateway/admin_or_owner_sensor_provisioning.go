package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrAdminOrOwnerProvisioningUnavailable marks infrastructure unavailability, not a
// definitive rollback: loss of commit confirmation can leave the outcome unknown.
var ErrAdminOrOwnerProvisioningUnavailable = errors.New("admin or owner sensor provisioning unavailable")

// AdminOrOwnerSensorProvisioningRepository defines the trusted capability for
// Sensor provisioning initiated by either a platform admin or a Gateway owner.
type AdminOrOwnerSensorProvisioningRepository interface {
	ProvisionSensorForAdminOrOwner(context.Context, uuid.UUID, ProvisionSensorInput) (ProvisionSensorResult, error)
}

// AdminOrOwnerSensorProvisioningService validates requests and bounds admin-or-owner sensor provisioning.
type AdminOrOwnerSensorProvisioningService struct {
	repository AdminOrOwnerSensorProvisioningRepository
	timeout    time.Duration
}

// NewAdminOrOwnerSensorProvisioningService creates a service for admin-or-owner sensor provisioning.
func NewAdminOrOwnerSensorProvisioningService(repo AdminOrOwnerSensorProvisioningRepository, timeout time.Duration) (*AdminOrOwnerSensorProvisioningService, error) {
	if isNilDependency(repo) {
		return nil, fmt.Errorf("admin or owner sensor provisioning repository is required")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("admin or owner sensor provisioning timeout must be positive")
	}
	return &AdminOrOwnerSensorProvisioningService{
		repository: repo,
		timeout:    timeout,
	}, nil
}

// ProvisionSensorForAdminOrOwner validates input and executes atomic admin-or-owner sensor provisioning.
func (s *AdminOrOwnerSensorProvisioningService) ProvisionSensorForAdminOrOwner(ctx context.Context, actor uuid.UUID, input ProvisionSensorInput) (ProvisionSensorResult, error) {
	if err := ValidateProvisionSensorInput(actor, input); err != nil {
		return ProvisionSensorResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.repository.ProvisionSensorForAdminOrOwner(ctx, actor, input)
}

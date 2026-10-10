package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrOwnerProvisioningUnavailable marks infrastructure unavailability, not a
// definitive rollback: loss of commit confirmation can leave the outcome unknown.
var ErrOwnerProvisioningUnavailable = errors.New("owner provisioning unavailable")

// OwnerSensorProvisioningRepository defines the trusted capability for owner-initiated
// Sensor provisioning in PostgreSQL.
type OwnerSensorProvisioningRepository interface {
	ProvisionSensorForOwner(context.Context, uuid.UUID, ProvisionSensorInput) (ProvisionSensorResult, error)
}

// OwnerSensorProvisioningService validates requests and bounds owner sensor provisioning.
type OwnerSensorProvisioningService struct {
	repository OwnerSensorProvisioningRepository
	timeout    time.Duration
}

// NewOwnerSensorProvisioningService creates a service for owner sensor provisioning.
func NewOwnerSensorProvisioningService(repo OwnerSensorProvisioningRepository, timeout time.Duration) (*OwnerSensorProvisioningService, error) {
	if isNilDependency(repo) {
		return nil, fmt.Errorf("owner sensor provisioning repository is required")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("owner sensor provisioning timeout must be positive")
	}
	return &OwnerSensorProvisioningService{
		repository: repo,
		timeout:    timeout,
	}, nil
}

// ProvisionSensorForOwner validates input and executes atomic owner sensor provisioning.
func (s *OwnerSensorProvisioningService) ProvisionSensorForOwner(ctx context.Context, actor uuid.UUID, input ProvisionSensorInput) (ProvisionSensorResult, error) {
	if err := ValidateProvisionSensorInput(actor, input); err != nil {
		return ProvisionSensorResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.repository.ProvisionSensorForOwner(ctx, actor, input)
}

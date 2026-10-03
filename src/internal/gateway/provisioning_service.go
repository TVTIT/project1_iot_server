package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ProvisioningService validates requests and bounds atomic repository operations.
type ProvisioningService struct {
	repository ProvisioningRepository
	timeout    time.Duration
}

// NewProvisioningService requires a repository and a positive operation timeout.
func NewProvisioningService(repository ProvisioningRepository, timeout time.Duration) (*ProvisioningService, error) {
	if isNilDependency(repository) {
		return nil, fmt.Errorf("provisioning repository is required")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("provisioning timeout must be positive")
	}
	return &ProvisioningService{repository: repository, timeout: timeout}, nil
}

// ProvisionGateway validates and atomically provisions the Gateway graph.
func (s *ProvisioningService) ProvisionGateway(ctx context.Context, actor uuid.UUID, input ProvisionGatewayInput) (ProvisionGatewayResult, error) {
	if err := ValidateProvisionGatewayInput(actor, input); err != nil {
		return ProvisionGatewayResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.repository.ProvisionGateway(ctx, actor, input)
}

// ProvisionSensor validates and atomically provisions the Sensor graph.
func (s *ProvisioningService) ProvisionSensor(ctx context.Context, actor uuid.UUID, input ProvisionSensorInput) (ProvisionSensorResult, error) {
	if err := ValidateProvisionSensorInput(actor, input); err != nil {
		return ProvisionSensorResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.repository.ProvisionSensor(ctx, actor, input)
}

package digitaltwin

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// createService coordinates Digital Twin entity creation by validating inputs
// and dispatching to transactional provisioners without out-of-transaction role prechecks.
type createService struct {
	gatewayProvisioner GatewayProvisioner
	sensorProvisioner  SensorProvisioner
}

var _ EntityCreateService = (*createService)(nil)

// NewCreateService constructs a new EntityCreateService implementation.
// Both gatewayProvisioner and sensorProvisioner are required non-nil dependencies.
func NewCreateService(gw GatewayProvisioner, sensor SensorProvisioner) (EntityCreateService, error) {
	if isNilDependency(gw) {
		return nil, fmt.Errorf("gateway provisioner is required")
	}
	if isNilDependency(sensor) {
		return nil, fmt.Errorf("sensor provisioner is required")
	}
	return &createService{
		gatewayProvisioner: gw,
		sensorProvisioner:  sensor,
	}, nil
}

// CreateEntity validates input and dispatches to the appropriate transactional provisioner.
// The operation timeout is enforced by the underlying provisioner service budget;
// no nested timeout is applied here to preserve a single coherent budget.
func (s *createService) CreateEntity(ctx context.Context, actor uuid.UUID, input CreateEntityInput) (CreateEntityResult, error) {
	if isNilDependency(s) || isNilDependency(s.gatewayProvisioner) || isNilDependency(s.sensorProvisioner) {
		return CreateEntityResult{}, ErrDependencyUnavailable
	}
	if ctx == nil {
		return CreateEntityResult{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return CreateEntityResult{}, err
	}
	if err := ValidateCreateEntityInput(actor, input); err != nil {
		return CreateEntityResult{}, err
	}

	switch input.Type {
	case EntityTypeGateway:
		res, err := s.gatewayProvisioner.ProvisionGateway(ctx, actor, *input.Gateway)
		if err != nil {
			return CreateEntityResult{}, err
		}
		return CreateEntityResult{
			Created:  res.Created,
			Type:     EntityTypeGateway,
			EntityID: res.EntityID,
			Gateway:  &res,
		}, nil

	case EntityTypeSensor:
		res, err := s.sensorProvisioner.ProvisionSensorForAdminOrOwner(ctx, actor, *input.Sensor)
		if err != nil {
			return CreateEntityResult{}, err
		}
		return CreateEntityResult{
			Created:  res.Created,
			Type:     EntityTypeSensor,
			EntityID: res.EntityID,
			Sensor:   &res,
		}, nil

	default:
		return CreateEntityResult{}, ErrInvalidInput
	}
}

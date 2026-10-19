package digitaltwin

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// gatewayPatchService coordinates Gateway metadata PATCH operations by enforcing
// input validation and bounding execution to a single finite timeout budget.
type gatewayPatchService struct {
	patcher GatewayMetadataPatcher
	timeout time.Duration
}

var _ GatewayPatchService = (*gatewayPatchService)(nil)

// NewGatewayPatchService constructs a new GatewayPatchService.
// Both patcher and a positive timeout are required dependencies.
func NewGatewayPatchService(patcher GatewayMetadataPatcher, timeout time.Duration) (GatewayPatchService, error) {
	if isNilDependency(patcher) {
		return nil, fmt.Errorf("gateway metadata patcher is required")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("operation timeout must be positive")
	}
	return &gatewayPatchService{
		patcher: patcher,
		timeout: timeout,
	}, nil
}

// PatchGateway validates the actor, gatewayID, and patch input before delegating
// to the transactional metadata patcher under a single finite timeout context.
// When validation fails, the patcher is never invoked.
func (s *gatewayPatchService) PatchGateway(ctx context.Context, actor uuid.UUID, gatewayID string, input PatchGatewayInput) (PatchGatewayResult, error) {
	if isNilDependency(s) || isNilDependency(s.patcher) {
		return PatchGatewayResult{}, ErrDependencyUnavailable
	}
	if ctx == nil {
		return PatchGatewayResult{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return PatchGatewayResult{}, err
	}
	if actor == uuid.Nil {
		return PatchGatewayResult{}, ErrNilActor
	}
	if err := gateway.ValidateGatewayID(gatewayID); err != nil {
		return PatchGatewayResult{}, ErrInvalidInput
	}
	if err := ValidatePatchGatewayInput(input); err != nil {
		return PatchGatewayResult{}, err
	}

	opCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	return s.patcher.PatchGatewayMetadata(opCtx, actor, gatewayID, input)
}

// sensorPatchService coordinates Sensor metadata PATCH operations by enforcing
// input validation and bounding execution to a single finite timeout budget.
type sensorPatchService struct {
	patcher SensorMetadataPatcher
	timeout time.Duration
}

var _ SensorPatchService = (*sensorPatchService)(nil)

// NewSensorPatchService constructs a new SensorPatchService.
// Both patcher and a positive timeout are required dependencies.
func NewSensorPatchService(patcher SensorMetadataPatcher, timeout time.Duration) (SensorPatchService, error) {
	if isNilDependency(patcher) {
		return nil, fmt.Errorf("sensor metadata patcher is required")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("operation timeout must be positive")
	}
	return &sensorPatchService{
		patcher: patcher,
		timeout: timeout,
	}, nil
}

// PatchSensor validates the actor, gatewayID, sensorID, and patch input before delegating
// to the transactional metadata patcher under a single finite timeout context.
// When validation fails, the patcher is never invoked.
func (s *sensorPatchService) PatchSensor(ctx context.Context, actor uuid.UUID, gatewayID string, sensorID string, input PatchSensorInput) (PatchSensorResult, error) {
	if isNilDependency(s) || isNilDependency(s.patcher) {
		return PatchSensorResult{}, ErrDependencyUnavailable
	}
	if ctx == nil {
		return PatchSensorResult{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return PatchSensorResult{}, err
	}
	if actor == uuid.Nil {
		return PatchSensorResult{}, ErrNilActor
	}
	if err := gateway.ValidateGatewayID(gatewayID); err != nil {
		return PatchSensorResult{}, ErrInvalidInput
	}
	if err := gateway.ValidateSensorID(sensorID); err != nil {
		return PatchSensorResult{}, ErrInvalidInput
	}
	if err := ValidatePatchSensorInput(input); err != nil {
		return PatchSensorResult{}, err
	}

	opCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	return s.patcher.PatchSensorMetadata(opCtx, actor, gatewayID, sensorID, input)
}

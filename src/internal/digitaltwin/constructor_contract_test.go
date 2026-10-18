package digitaltwin

import "time"

// Constructors expose application interfaces, not private implementation types.
var (
	_ func(GatewayProvisioner, SensorProvisioner) (EntityCreateService, error)           = NewCreateService
	_ func(GatewayMetadataPatcher, time.Duration) (GatewayPatchService, error)           = NewGatewayPatchService
	_ func(SensorMetadataPatcher, time.Duration) (SensorPatchService, error)             = NewSensorPatchService
	_ func(*ReadPreparation, TwinReader, TelemetryHistoryDelegator) (ReadService, error) = NewReadService
)

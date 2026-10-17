package digitaltwin

import (
	"errors"
	"fmt"

	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetry"
)

// Domain errors for Digital Twin read operations.
// These errors deliberately avoid HTTP dependencies so that domain layers remain decoupled from transport.
var (
	ErrNilActor              = errors.New("actor must not be nil")
	ErrNotFound              = errors.New("digital twin resource not found")
	ErrInvalidInput          = errors.New("invalid input")
	ErrInvalidCursor         = errors.New("invalid cursor")
	ErrInvalidProperty       = errors.New("invalid property selector")
	ErrInconsistentUnit      = errors.New("inconsistent unit in temporal series")
	ErrDependencyUnavailable = errors.New("dependency unavailable")
	ErrUnexpected            = errors.New("unexpected digital twin error")

	// Delegated history error sentinels from telemetry
	ErrRangeTooLarge     = telemetry.ErrHistoryRangeTooLarge
	ErrInvalidResolution = telemetry.ErrHistoryInvalidResolution
	ErrRawLimitExceeded  = telemetry.ErrHistoryRawLimitExceeded
	ErrResolutionTooFine = telemetry.ErrHistoryResolutionTooFine

	// Delegated provisioning error sentinels from gateway
	ErrProvisioningForbidden               = gateway.ErrProvisioningForbidden
	ErrProvisioningConflict                = gateway.ErrProvisioningConflict
	ErrProvisioningInconsistent            = gateway.ErrProvisioningInconsistent
	ErrProvisioningNotFound                = gateway.ErrProvisioningNotFound
	ErrGatewayProvisioningUnavailable      = gateway.ErrGatewayProvisioningUnavailable
	ErrAdminOrOwnerProvisioningUnavailable = gateway.ErrAdminOrOwnerProvisioningUnavailable

	// Metadata patch sentinels
	ErrForbidden                = gateway.ErrProvisioningForbidden
	ErrInconsistentGraph        = gateway.ErrProvisioningInconsistent
	ErrMetadataPatchUnavailable = fmt.Errorf("metadata patch unavailable: %w", ErrDependencyUnavailable)
)

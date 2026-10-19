package gateway

import (
	"errors"

	"github.com/jackc/pgx/v5"
)

// ErrGatewayProvisioningUnavailable marks infrastructure unavailability during Gateway provisioning,
// including lost commit confirmation where the outcome is uncertain.
var ErrGatewayProvisioningUnavailable = errors.New("gateway provisioning unavailable")

func classifyGatewayProvisioningError(err error) error {
	// pgx ScanArgError is decoding a returned value, not reading the socket.
	// Its decoder may wrap EOF/net errors; those must not disguise bad data as
	// infrastructure failure. QueryRow.Scan's actual transport error is unwrapped.
	var scan pgx.ScanArgError
	var scanPointer *pgx.ScanArgError
	if errors.As(err, &scan) || errors.As(err, &scanPointer) {
		return err
	}
	return ClassifyPostgresAvailabilityError(err, ErrGatewayProvisioningUnavailable)
}

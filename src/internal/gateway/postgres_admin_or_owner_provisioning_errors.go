package gateway

import (
	"errors"

	"github.com/jackc/pgx/v5"
)

func classifyAdminOrOwnerProvisioningError(err error) error {
	// pgx ScanArgError is decoding a returned value, not reading the socket.
	// Its decoder may wrap EOF/net errors; those must not disguise bad data as
	// infrastructure failure. QueryRow.Scan's actual transport error is unwrapped.
	var scan pgx.ScanArgError
	var scanPointer *pgx.ScanArgError
	if errors.As(err, &scan) || errors.As(err, &scanPointer) {
		return err
	}
	return ClassifyPostgresAvailabilityError(err, ErrAdminOrOwnerProvisioningUnavailable)
}

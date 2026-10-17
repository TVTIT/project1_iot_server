package gateway

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"
)

// classifyInventoryReadError is only used at Query/rows.Err boundaries, not Scan:
// malformed stored values and programming errors must remain unexpected failures.
// Preserve the original cause for errors.Is/As; HTTP and audit expose only safe codes.
func classifyInventoryReadError(err error) error {
	return ClassifyPostgresAvailabilityError(err, ErrInventoryUnavailable)
}

// ClassifyPostgresAvailabilityError tags only typed transport/server availability
// failures. Callers choose their own domain sentinel; SQL/scan faults stay intact.
func ClassifyPostgresAvailabilityError(err, unavailable error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var server *pgconn.PgError
	if errors.As(err, &server) && server != nil {
		// Explicit availability codes, not all class 08: 08P01 is a protocol bug.
		// Server SQL/permission/constraint errors stay 500 even inside ConnectError.
		switch server.Code {
		case "08000", "08001", "08003", "08006", "53300", "57P01", "57P02", "57P03":
			return errors.Join(unavailable, err)
		default:
			return err
		}
	}
	var connection *pgconn.ConnectError
	var transport net.Error
	if errors.Is(err, pgconn.ErrConnClosed) || errors.Is(err, puddle.ErrClosedPool) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		(errors.As(err, &connection) && connection != nil) || errors.As(err, &transport) {
		return errors.Join(unavailable, err)
	}
	return err
}

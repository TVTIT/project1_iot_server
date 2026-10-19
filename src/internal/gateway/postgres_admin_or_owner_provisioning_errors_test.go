package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestAdminOrOwnerProvisioningClassification(t *testing.T) {
	for _, cause := range []error{
		pgx.ScanArgError{ColumnIndex: 0, Err: io.EOF},
		&pgx.ScanArgError{ColumnIndex: 0, Err: io.ErrUnexpectedEOF},
		context.Canceled, context.DeadlineExceeded,
		ErrProvisioningConflict, ErrProvisioningInconsistent, ErrProvisioningNotFound, ErrProvisioningForbidden, ErrProvisioningAdminLookup,
	} {
		wrapped := fmt.Errorf("fixture: %w", cause)
		got := classifyAdminOrOwnerProvisioningError(wrapped)
		if got != wrapped || !errors.Is(got, cause) {
			t.Fatalf("logical/context error changed: %v", got)
		}
	}
	cause := &pgconn.PgError{Code: "57P01"}
	err := classifyAdminOrOwnerProvisioningError(fmt.Errorf("fixture: %w", cause))
	var preserved *pgconn.PgError
	if !errors.Is(err, ErrAdminOrOwnerProvisioningUnavailable) || !errors.As(err, &preserved) || preserved != cause {
		t.Fatal("availability/cause lost")
	}
	if errors.Is(err, ErrInventoryUnavailable) || errors.Is(err, ErrOwnerProvisioningUnavailable) {
		t.Fatal("wrong capability sentinel")
	}
	if classifyAdminOrOwnerProvisioningError(nil) != nil {
		t.Fatal("nil became failure")
	}
}

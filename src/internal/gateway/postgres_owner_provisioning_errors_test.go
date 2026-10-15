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

func TestOwnerProvisioningClassification(t *testing.T) {
	for _, cause := range []error{
		pgx.ScanArgError{ColumnIndex: 0, Err: io.EOF},
		&pgx.ScanArgError{ColumnIndex: 0, Err: io.ErrUnexpectedEOF},
		context.Canceled, context.DeadlineExceeded,
		ErrProvisioningConflict, ErrProvisioningInconsistent, ErrProvisioningNotFound, ErrProvisioningForbidden,
	} {
		wrapped := fmt.Errorf("fixture: %w", cause)
		got := classifyOwnerProvisioningError(wrapped)
		if got != wrapped || !errors.Is(got, cause) {
			t.Fatalf("logical/context error changed: %v", got)
		}
	}
	cause := &pgconn.PgError{Code: "57P01"}
	err := classifyOwnerProvisioningError(fmt.Errorf("fixture: %w", cause))
	var preserved *pgconn.PgError
	if !errors.Is(err, ErrOwnerProvisioningUnavailable) || !errors.As(err, &preserved) || preserved != cause {
		t.Fatal("availability/cause lost")
	}
	if errors.Is(err, ErrInventoryUnavailable) || errors.Is(err, ErrProvisioningAdminLookup) {
		t.Fatal("wrong capability sentinel")
	}
	if classifyOwnerProvisioningError(nil) != nil {
		t.Fatal("nil became failure")
	}
}

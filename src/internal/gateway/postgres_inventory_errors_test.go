package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"
)

func TestInventoryReadErrorClassificationAndPreservation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cause       error
		unavailable bool
	}{
		{"closed connection", pgconn.ErrConnClosed, true},
		{"closed pool", puddle.ErrClosedPool, true},
		{"eof", io.EOF, true},
		{"unexpected eof", io.ErrUnexpectedEOF, true},
		{"network", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, true},
		{"dns", &net.DNSError{Err: "fixture", Name: "fixture.invalid"}, true},
		{"shutdown", &pgconn.PgError{Code: "57P01"}, true},
		{"crash", &pgconn.PgError{Code: "57P02"}, true},
		{"starting", &pgconn.PgError{Code: "57P03"}, true},
		{"capacity", &pgconn.PgError{Code: "53300"}, true},
		{"connection exception", &pgconn.PgError{Code: "08000"}, true},
		{"cannot connect", &pgconn.PgError{Code: "08001"}, true},
		{"connection missing", &pgconn.PgError{Code: "08003"}, true},
		{"connection failure", &pgconn.PgError{Code: "08006"}, true},
		{"timeout", context.DeadlineExceeded, false},
		{"cancel", context.Canceled, false},
		{"syntax", &pgconn.PgError{Code: "42601"}, false},
		{"missing column", &pgconn.PgError{Code: "42703"}, false},
		{"constraint", &pgconn.PgError{Code: "23514"}, false},
		{"permission", &pgconn.PgError{Code: "42501"}, false},
		{"db authentication", &pgconn.PgError{Code: "28P01"}, false},
		{"protocol", &pgconn.PgError{Code: "08P01"}, false},
		{"no rows", pgx.ErrNoRows, false},
		{"arbitrary", errors.New("conn closed"), false},
	} {
		for _, phase := range []string{"query", "iteration"} {
			t.Run(tc.name+"/"+phase, func(t *testing.T) {
				cause := fmt.Errorf("wrapped: %w", tc.cause)
				rows := &fakeRows{next: true, endErr: cause}
				db := &fakeAdminDatabase{isAdmin: true, rows: rows}
				if phase == "query" {
					db.queryErr = cause
				}
				repo, err := NewPostgresAdminGatewayRepository(db)
				if err != nil {
					t.Fatal(err)
				}
				page, err := repo.ListAdminGateways(context.Background(), uuid.New(), AdminGatewayInventoryQuery{Limit: 10})
				if errors.Is(err, ErrInventoryUnavailable) != tc.unavailable {
					t.Fatalf("availability mismatch: %v", err)
				}
				if !errors.Is(err, cause) || !errors.Is(err, tc.cause) {
					t.Fatal("original error not preserved")
				}
				if pg, ok := tc.cause.(*pgconn.PgError); ok {
					var preserved *pgconn.PgError
					if !errors.As(err, &preserved) || preserved != pg {
						t.Fatal("driver type not preserved")
					}
				}
				if page.Items != nil || page.NextCursor != nil {
					t.Fatal("partial page exposed")
				}
				if phase == "iteration" && !rows.closed {
					t.Fatal("rows not closed")
				}
			})
		}
	}
	if classifyInventoryReadError(nil) != nil {
		t.Fatal("nil became failure")
	}
}

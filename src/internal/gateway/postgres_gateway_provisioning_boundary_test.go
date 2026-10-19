package gateway

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"
)

// boundaryFakeDB fails BeginTx with the configured error (when set) and otherwise
// delegates every statement to a boundaryFakeTx whose individual boundaries can be
// faulted. It exercises the real repository transaction flow, not the classifier in
// isolation.
type boundaryFakeDB struct {
	beginErr error
	tx       *boundaryFakeTx
}

func (d *boundaryFakeDB) BeginTx(_ context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	if d.beginErr != nil {
		return nil, d.beginErr
	}
	return d.tx, nil
}

type boundaryFakeTx struct {
	pgx.Tx
	platformAdminScanErr error
	profileScanErr       error
	execErr              error
	gatewayInsertErr     error
	replayReadErr        error
	twinEntityScanErr    error
}

func (tx *boundaryFakeTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	switch {
	case strings.Contains(query, "platform_admins"):
		return fakeGatewayRow{scanFn: func(dest ...any) error {
			if tx.platformAdminScanErr != nil {
				return tx.platformAdminScanErr
			}
			if len(dest) > 0 {
				if b, ok := dest[0].(*bool); ok {
					*b = true
				}
			}
			return nil
		}}
	case strings.Contains(query, "public.profiles"):
		return fakeGatewayRow{scanFn: func(dest ...any) error {
			if tx.profileScanErr != nil {
				return tx.profileScanErr
			}
			if len(dest) > 0 {
				if u, ok := dest[0].(*uuid.UUID); ok {
					*u = uuid.New()
				}
			}
			return nil
		}}
	case strings.Contains(query, "ON CONFLICT"):
		return fakeGatewayRow{scanFn: func(dest ...any) error {
			if tx.gatewayInsertErr != nil {
				return tx.gatewayInsertErr
			}
			if len(dest) > 0 {
				if t, ok := dest[0].(**time.Time); ok {
					now := time.Now().UTC()
					*t = &now
				}
			}
			return nil
		}}
	case strings.Contains(query, "FOR UPDATE"):
		return fakeGatewayRow{scanFn: func(_ ...any) error {
			if tx.replayReadErr != nil {
				return tx.replayReadErr
			}
			return nil
		}}
	case strings.Contains(query, "user_gateways"):
		return fakeGatewayRow{scanFn: func(dest ...any) error {
			if len(dest) >= 2 {
				if a, ok := dest[0].(*int); ok {
					*a = 1
				}
				if b, ok := dest[1].(*int); ok {
					*b = 1
				}
			}
			return nil
		}}
	case strings.Contains(query, "twin_entities"):
		return fakeGatewayRow{scanFn: func(dest ...any) error {
			if tx.twinEntityScanErr != nil {
				return tx.twinEntityScanErr
			}
			if len(dest) > 0 {
				switch target := dest[0].(type) {
				case *uuid.UUID:
					*target = uuid.New()
				case *bool:
					*target = true
				}
			}
			return nil
		}}
	default:
		return fakeGatewayRow{scanFn: func(...any) error {
			return errors.New("unexpected query: " + query)
		}}
	}
}

func (tx *boundaryFakeTx) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	if tx.execErr != nil {
		return pgconn.CommandTag{}, tx.execErr
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (tx *boundaryFakeTx) Rollback(_ context.Context) error { return nil }

func (tx *boundaryFakeTx) Commit(_ context.Context) error { return nil }

func newBoundaryProvisioningRepo(t *testing.T, db ProvisioningDatabase) ProvisioningRepository {
	t.Helper()
	repo, err := NewPostgresProvisioningRepository(db)
	if err != nil {
		t.Fatalf("NewPostgresProvisioningRepository: %v", err)
	}
	return repo
}

func TestPostgresGatewayProvisioningBoundaryAvailability(t *testing.T) {
	ctx := context.Background()
	actor := uuid.New()
	input := ProvisionGatewayInput{GatewayID: "gw_boundary", Name: "Boundary Gateway", OwnerUserID: uuid.New()}
	serverCode := &pgconn.PgError{Code: "08006"}

	cases := []struct {
		name  string
		db    *boundaryFakeDB
		cause error
	}{
		{
			name:  "begin closed pool",
			db:    &boundaryFakeDB{beginErr: puddle.ErrClosedPool},
			cause: puddle.ErrClosedPool,
		},
		{
			name:  "begin transport eof",
			db:    &boundaryFakeDB{beginErr: io.EOF},
			cause: io.EOF,
		},
		{
			name:  "owner profile lookup transport",
			db:    &boundaryFakeDB{tx: &boundaryFakeTx{profileScanErr: io.EOF}},
			cause: io.EOF,
		},
		{
			name:  "owner membership write transport",
			db:    &boundaryFakeDB{tx: &boundaryFakeTx{execErr: io.ErrUnexpectedEOF}},
			cause: io.ErrUnexpectedEOF,
		},
		{
			name:  "twin entity write transport",
			db:    &boundaryFakeDB{tx: &boundaryFakeTx{twinEntityScanErr: io.EOF}},
			cause: io.EOF,
		},
		{
			name: "replay Gateway read transport server code",
			db: &boundaryFakeDB{tx: &boundaryFakeTx{
				gatewayInsertErr: pgx.ErrNoRows,
				replayReadErr:    serverCode,
			}},
			cause: serverCode,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := newBoundaryProvisioningRepo(t, tc.db)
			_, err := repo.ProvisionGateway(ctx, actor, input)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, ErrGatewayProvisioningUnavailable) {
				t.Fatalf("missing ErrGatewayProvisioningUnavailable: %v", err)
			}
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Fatalf("cause lost: %v", err)
			}
			if errors.Is(err, ErrProvisioningForbidden) || errors.Is(err, ErrProvisioningNotFound) ||
				errors.Is(err, ErrProvisioningConflict) || errors.Is(err, ErrProvisioningInconsistent) {
				t.Fatalf("domain sentinel leaked into availability error: %v", err)
			}
		})
	}
}

func TestPostgresGatewayProvisioningBoundaryProgrammingFaults(t *testing.T) {
	ctx := context.Background()
	actor := uuid.New()
	input := ProvisionGatewayInput{GatewayID: "gw_boundary_fault", Name: "Boundary Fault", OwnerUserID: uuid.New()}

	cases := []struct {
		name     string
		db       *boundaryFakeDB
		wantCode string
	}{
		{
			name:     "undefined table profile lookup stays internal",
			db:       &boundaryFakeDB{tx: &boundaryFakeTx{profileScanErr: &pgconn.PgError{Code: "42P01"}}},
			wantCode: "42P01",
		},
		{
			name:     "unique violation write stays internal",
			db:       &boundaryFakeDB{tx: &boundaryFakeTx{execErr: &pgconn.PgError{Code: "23505"}}},
			wantCode: "23505",
		},
		{
			name: "value scan fault is not availability",
			db:   &boundaryFakeDB{tx: &boundaryFakeTx{profileScanErr: pgx.ScanArgError{ColumnIndex: 0, Err: io.EOF}}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := newBoundaryProvisioningRepo(t, tc.db)
			_, err := repo.ProvisionGateway(ctx, actor, input)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if errors.Is(err, ErrGatewayProvisioningUnavailable) {
				t.Fatalf("programming/data fault misclassified as unavailable: %v", err)
			}
			if tc.wantCode != "" {
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != tc.wantCode {
					t.Fatalf("expected PgError code %s preserved, got %v", tc.wantCode, err)
				}
			} else {
				var scan pgx.ScanArgError
				if !errors.As(err, &scan) {
					t.Fatalf("expected scan fault preserved, got %v", err)
				}
			}
		})
	}
}

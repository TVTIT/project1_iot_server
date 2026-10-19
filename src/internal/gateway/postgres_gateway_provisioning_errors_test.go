package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestGatewayProvisioningClassification(t *testing.T) {
	for _, cause := range []error{
		pgx.ScanArgError{ColumnIndex: 0, Err: io.EOF},
		&pgx.ScanArgError{ColumnIndex: 0, Err: io.ErrUnexpectedEOF},
		context.Canceled, context.DeadlineExceeded,
		ErrProvisioningConflict, ErrProvisioningInconsistent, ErrProvisioningNotFound, ErrProvisioningForbidden, ErrProvisioningAdminLookup,
	} {
		wrapped := fmt.Errorf("fixture: %w", cause)
		got := classifyGatewayProvisioningError(wrapped)
		if got != wrapped || !errors.Is(got, cause) {
			t.Fatalf("logical/context error changed: %v", got)
		}
	}
	cause := &pgconn.PgError{Code: "57P01"}
	err := classifyGatewayProvisioningError(fmt.Errorf("fixture: %w", cause))
	var preserved *pgconn.PgError
	if !errors.Is(err, ErrGatewayProvisioningUnavailable) || !errors.As(err, &preserved) || preserved != cause {
		t.Fatal("availability/cause lost")
	}
	if errors.Is(err, ErrInventoryUnavailable) || errors.Is(err, ErrOwnerProvisioningUnavailable) || errors.Is(err, ErrAdminOrOwnerProvisioningUnavailable) {
		t.Fatal("wrong capability sentinel")
	}
	if classifyGatewayProvisioningError(nil) != nil {
		t.Fatal("nil became failure")
	}
}

type fakeGatewayCommitFailureDB struct {
	commitErr error
}

func (d *fakeGatewayCommitFailureDB) BeginTx(_ context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	return &fakeGatewayCommitFailureTx{db: d}, nil
}

type fakeGatewayCommitFailureTx struct {
	pgx.Tx
	db *fakeGatewayCommitFailureDB
}

type fakeGatewayRow struct {
	scanFn func(dest ...any) error
}

func (r fakeGatewayRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}

func (tx *fakeGatewayCommitFailureTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	switch {
	case strings.Contains(query, "platform_admins"):
		return fakeGatewayRow{scanFn: func(dest ...any) error {
			if len(dest) > 0 {
				if b, ok := dest[0].(*bool); ok {
					*b = true
				}
			}
			return nil
		}}
	case strings.Contains(query, "profiles"):
		return fakeGatewayRow{scanFn: func(dest ...any) error {
			if len(dest) > 0 {
				if u, ok := dest[0].(*uuid.UUID); ok {
					*u = uuid.New()
				}
			}
			return nil
		}}
	case strings.Contains(query, "gateways"):
		return fakeGatewayRow{scanFn: func(dest ...any) error {
			if len(dest) > 0 {
				if t, ok := dest[0].(**time.Time); ok {
					now := time.Now().UTC()
					*t = &now
				}
			}
			return nil
		}}
	case strings.Contains(query, "twin_entities"):
		return fakeGatewayRow{scanFn: func(dest ...any) error {
			if len(dest) > 0 {
				if u, ok := dest[0].(*uuid.UUID); ok {
					*u = uuid.New()
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

func (tx *fakeGatewayCommitFailureTx) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (tx *fakeGatewayCommitFailureTx) Rollback(_ context.Context) error {
	return nil
}

func (tx *fakeGatewayCommitFailureTx) Commit(_ context.Context) error {
	return tx.db.commitErr
}

func TestPostgresGatewayProvisioningCommitFailureClassification(t *testing.T) {
	ctx := context.Background()
	admin := uuid.New()
	owner := uuid.New()
	in := ProvisionGatewayInput{
		GatewayID:   "gw_commit_test",
		Name:        "Test Gateway",
		OwnerUserID: owner,
	}

	t.Run("eof commit failure wraps ErrGatewayProvisioningUnavailable", func(t *testing.T) {
		db := &fakeGatewayCommitFailureDB{commitErr: io.EOF}
		repo, err := NewPostgresProvisioningRepository(db)
		if err != nil {
			t.Fatal(err)
		}
		res, err := repo.ProvisionGateway(ctx, admin, in)
		if err == nil {
			t.Fatal("expected error on commit failure, got nil")
		}
		if !errors.Is(err, ErrGatewayProvisioningUnavailable) {
			t.Fatalf("expected ErrGatewayProvisioningUnavailable on EOF commit failure, got %v", err)
		}
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected cause io.EOF preserved, got %v", err)
		}
		if res.Created {
			t.Fatal("result must not claim Created on commit failure")
		}
	})

	t.Run("non availability commit error does not wrap ErrGatewayProvisioningUnavailable", func(t *testing.T) {
		customErr := errors.New("custom non-availability commit error")
		db := &fakeGatewayCommitFailureDB{commitErr: customErr}
		repo, err := NewPostgresProvisioningRepository(db)
		if err != nil {
			t.Fatal(err)
		}
		res, err := repo.ProvisionGateway(ctx, admin, in)
		if err == nil {
			t.Fatal("expected error on commit failure, got nil")
		}
		if errors.Is(err, ErrGatewayProvisioningUnavailable) {
			t.Fatalf("did not expect ErrGatewayProvisioningUnavailable on non-availability commit error, got %v", err)
		}
		if !errors.Is(err, customErr) {
			t.Fatalf("expected custom error preserved, got %v", err)
		}
		if res.Created {
			t.Fatal("result must not claim Created on commit failure")
		}
	})
}

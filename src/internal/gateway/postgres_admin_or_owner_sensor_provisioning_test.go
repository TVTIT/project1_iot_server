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

	"iot-platform/internal/digitaltwin/mapper"
)

type fakeAdminOrOwnerTxDatabase struct {
	admin        bool
	adminErr     error
	parentName   string
	parentErr    error
	role         Role
	roleErr      error
	graphErr     error
	commitErr    error
	isReplay     bool
	conflict     bool
	inconsistent bool

	queriesExecuted []string
	rollbackCalled  int
	commitCalled    int
}

func (d *fakeAdminOrOwnerTxDatabase) BeginTx(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	if options.IsoLevel != pgx.ReadCommitted {
		return nil, errors.New("expected ReadCommitted isolation")
	}
	return &fakeAdminOrOwnerTx{db: d}, nil
}

type fakeAdminOrOwnerTx struct {
	pgx.Tx
	db *fakeAdminOrOwnerTxDatabase
}

type fakeSingleScanRow struct {
	val any
}

func (r fakeSingleScanRow) Scan(dest ...any) error {
	if len(dest) > 0 {
		switch v := dest[0].(type) {
		case *string:
			if s, ok := r.val.(string); ok {
				*v = s
			}
		case *Role:
			if s, ok := r.val.(string); ok {
				*v = Role(s)
			}
		case *bool:
			if b, ok := r.val.(bool); ok {
				*v = b
			}
		case *uuid.UUID:
			if u, ok := r.val.(uuid.UUID); ok {
				*v = u
			}
		case **time.Time:
			if t, ok := r.val.(*time.Time); ok {
				*v = t
			}
		}
	}
	return nil
}

func (tx *fakeAdminOrOwnerTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	tx.db.queriesExecuted = append(tx.db.queriesExecuted, query)

	switch {
	case strings.Contains(query, "platform_admins"):
		if tx.db.adminErr != nil {
			return fakeRow{err: tx.db.adminErr}
		}
		return fakeSingleScanRow{val: tx.db.admin}

	case strings.Contains(query, "FROM public.gateways") && (strings.Contains(query, "FOR UPDATE") || strings.Contains(query, "FOR SHARE")):
		if tx.db.parentErr != nil {
			return fakeRow{err: tx.db.parentErr}
		}
		if tx.db.parentName == "" {
			return fakeRow{err: pgx.ErrNoRows}
		}
		return fakeSingleScanRow{val: tx.db.parentName}

	case strings.Contains(query, "FROM public.user_gateways") && strings.Contains(query, "FOR SHARE"):
		if tx.db.roleErr != nil {
			return fakeRow{err: tx.db.roleErr}
		}
		if tx.db.role == "" {
			return fakeRow{err: pgx.ErrNoRows}
		}
		return fakeSingleScanRow{val: string(tx.db.role)}

	case strings.Contains(query, "SELECT e.id FROM public.twin_entities e JOIN public.twin_states s"):
		if tx.db.inconsistent {
			return fakeRow{err: pgx.ErrNoRows}
		}
		if tx.db.graphErr != nil {
			return fakeRow{err: tx.db.graphErr}
		}
		return fakeSingleScanRow{val: uuid.New()}

	case strings.Contains(query, "INSERT INTO public.sensors"):
		if tx.db.isReplay || tx.db.conflict {
			return fakeRow{err: pgx.ErrNoRows}
		}
		now := time.Now().UTC()
		return fakeSingleScanRow{val: &now}

	case strings.Contains(query, "INSERT INTO public.twin_entities") && strings.Contains(query, "RETURNING id"):
		return fakeSingleScanRow{val: uuid.New()}

	case strings.Contains(query, "SELECT name,unit,created_at FROM public.sensors"):
		if tx.db.conflict {
			now := time.Now().UTC()
			var differentUnit *string
			diff := "different_unit"
			differentUnit = &diff
			return fakeMultiScanRow{values: []any{"different_name", differentUnit, &now}}
		}
		now := time.Now().UTC()
		return fakeMultiScanRow{values: []any{"temp", nil, &now}}

	case strings.Contains(query, "SELECT EXISTS (SELECT 1 FROM public.twin_entities e"):
		if tx.db.inconsistent {
			return fakeSingleScanRow{val: false}
		}
		return fakeSingleScanRow{val: true}

	default:
		return fakeRow{err: errors.New("unexpected query: " + query)}
	}
}

func (tx *fakeAdminOrOwnerTx) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	tx.db.queriesExecuted = append(tx.db.queriesExecuted, query)
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (tx *fakeAdminOrOwnerTx) Commit(_ context.Context) error {
	tx.db.commitCalled++
	return tx.db.commitErr
}

func (tx *fakeAdminOrOwnerTx) Rollback(_ context.Context) error {
	tx.db.rollbackCalled++
	return nil
}

type fakeMultiScanRow struct {
	values []any
}

func (r fakeMultiScanRow) Scan(dest ...any) error {
	for i, d := range dest {
		if i >= len(r.values) {
			break
		}
		val := r.values[i]
		switch target := d.(type) {
		case *string:
			if s, ok := val.(string); ok {
				*target = s
			}
		case **string:
			if s, ok := val.(*string); ok {
				*target = s
			}
		case **time.Time:
			if t, ok := val.(*time.Time); ok {
				*target = t
			}
		case *bool:
			if b, ok := val.(bool); ok {
				*target = b
			}
		case *uuid.UUID:
			if u, ok := val.(uuid.UUID); ok {
				*target = u
			}
		}
	}
	return nil
}

func TestPostgresAdminOrOwnerSensorProvisioningConstructor(t *testing.T) {
	var typedNil *provisioningBeginFunc
	for _, db := range []ProvisioningDatabase{nil, typedNil} {
		if _, err := NewPostgresAdminOrOwnerSensorProvisioningRepository(db); err == nil {
			t.Fatal("accepted nil or typed-nil database")
		}
	}

	repo, err := NewPostgresAdminOrOwnerSensorProvisioningRepository(provisioningBeginFunc(func(context.Context, pgx.TxOptions) (pgx.Tx, error) {
		return nil, nil
	}))
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}
	if repo == nil {
		t.Fatal("expected non-nil repo")
	}
}

func TestPostgresAdminOrOwnerSensorProvisioningInputValidation(t *testing.T) {
	called := false
	db := provisioningBeginFunc(func(context.Context, pgx.TxOptions) (pgx.Tx, error) {
		called = true
		return nil, nil
	})
	repo, err := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
	if err != nil {
		t.Fatal(err)
	}

	// Nil actor
	if _, err := repo.ProvisionSensorForAdminOrOwner(context.Background(), uuid.Nil, ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: "temp"}); !errors.Is(err, ErrInvalidProvisioningInput) || called {
		t.Fatalf("expected ErrInvalidProvisioningInput on nil actor, got %v", err)
	}

	// Invalid gateway ID
	if _, err := repo.ProvisionSensorForAdminOrOwner(context.Background(), uuid.New(), ProvisionSensorInput{GatewayID: "invalid/gw", SensorID: "s1", Name: "temp"}); !errors.Is(err, ErrInvalidGatewayID) || called {
		t.Fatalf("expected ErrInvalidGatewayID, got %v", err)
	}

	// Invalid sensor ID
	if _, err := repo.ProvisionSensorForAdminOrOwner(context.Background(), uuid.New(), ProvisionSensorInput{GatewayID: "gw1", SensorID: "invalid/sensor", Name: "temp"}); !errors.Is(err, ErrInvalidSensorID) || called {
		t.Fatalf("expected ErrInvalidSensorID, got %v", err)
	}

	// Empty name
	if _, err := repo.ProvisionSensorForAdminOrOwner(context.Background(), uuid.New(), ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: ""}); !errors.Is(err, ErrInvalidProvisioningInput) || called {
		t.Fatalf("expected ErrInvalidProvisioningInput on empty name, got %v", err)
	}
}

func TestPostgresAdminOrOwnerSensorProvisioningAuthorizationBranches(t *testing.T) {
	ctx := context.Background()
	actor := uuid.New()
	in := ProvisionSensorInput{GatewayID: "gw1", SensorID: "s1", Name: "temp"}

	t.Run("admin lookup failure does not fall back to owner check", func(t *testing.T) {
		db := &fakeAdminOrOwnerTxDatabase{
			adminErr:   errors.New("admin table error"),
			parentName: "gw1",
			role:       RoleOwner, // Even if actor is an owner in user_gateways, admin lookup failure must fail closed!
		}
		repo, err := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
		if err != nil {
			t.Fatal(err)
		}
		_, err = repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if !errors.Is(err, ErrProvisioningAdminLookup) {
			t.Fatalf("expected ErrProvisioningAdminLookup, got %v", err)
		}
		if len(db.queriesExecuted) != 1 {
			t.Fatalf("expected exactly 1 query (admin lookup), got %d queries: %v", len(db.queriesExecuted), db.queriesExecuted)
		}
		if db.rollbackCalled == 0 {
			t.Fatal("expected rollback on admin lookup failure")
		}
	})

	t.Run("missing parent returns not found for admin", func(t *testing.T) {
		db := &fakeAdminOrOwnerTxDatabase{
			admin:      true,
			parentName: "", // missing parent
		}
		repo, _ := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
		_, err := repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if !errors.Is(err, ErrProvisioningNotFound) {
			t.Fatalf("expected ErrProvisioningNotFound, got %v", err)
		}
		if len(db.queriesExecuted) != 2 {
			t.Fatalf("expected 2 queries (admin check, then parent lock), got %d: %v", len(db.queriesExecuted), db.queriesExecuted)
		}
		if !strings.Contains(db.queriesExecuted[0], "platform_admins") {
			t.Fatalf("first query must be admin check, got %s", db.queriesExecuted[0])
		}
		if !strings.Contains(db.queriesExecuted[1], "FROM public.gateways") || !strings.Contains(db.queriesExecuted[1], "FOR SHARE") {
			t.Fatalf("second query must be parent lock FOR SHARE, got %s", db.queriesExecuted[1])
		}
	})

	t.Run("missing parent returns not found for non-admin without membership check", func(t *testing.T) {
		db := &fakeAdminOrOwnerTxDatabase{
			admin:      false,
			parentName: "", // missing parent
			role:       RoleOwner,
		}
		repo, _ := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
		_, err := repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if !errors.Is(err, ErrProvisioningNotFound) {
			t.Fatalf("expected ErrProvisioningNotFound, got %v", err)
		}
		// When parent is missing, membership check must NOT be executed
		if len(db.queriesExecuted) != 2 {
			t.Fatalf("expected 2 queries (admin check, parent lock), got %d: %v", len(db.queriesExecuted), db.queriesExecuted)
		}
	})

	t.Run("non-admin with absent membership returns not found", func(t *testing.T) {
		db := &fakeAdminOrOwnerTxDatabase{
			admin:      false,
			parentName: "gw1",
			role:       "", // missing membership (pgx.ErrNoRows)
		}
		repo, _ := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
		_, err := repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if !errors.Is(err, ErrProvisioningNotFound) {
			t.Fatalf("expected ErrProvisioningNotFound, got %v", err)
		}
		if len(db.queriesExecuted) != 3 {
			t.Fatalf("expected 3 queries (admin check, parent lock, membership check), got %d: %v", len(db.queriesExecuted), db.queriesExecuted)
		}
		if !strings.Contains(db.queriesExecuted[2], "FROM public.user_gateways") || !strings.Contains(db.queriesExecuted[2], "FOR SHARE") {
			t.Fatalf("third query must be membership check FOR SHARE, got %s", db.queriesExecuted[2])
		}
	})

	t.Run("non-admin with operator role is forbidden", func(t *testing.T) {
		db := &fakeAdminOrOwnerTxDatabase{
			admin:      false,
			parentName: "gw1",
			role:       RoleOperator,
		}
		repo, _ := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
		_, err := repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if !errors.Is(err, ErrProvisioningForbidden) {
			t.Fatalf("expected ErrProvisioningForbidden for operator, got %v", err)
		}
	})

	t.Run("non-admin with viewer role is forbidden", func(t *testing.T) {
		db := &fakeAdminOrOwnerTxDatabase{
			admin:      false,
			parentName: "gw1",
			role:       RoleViewer,
		}
		repo, _ := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
		_, err := repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if !errors.Is(err, ErrProvisioningForbidden) {
			t.Fatalf("expected ErrProvisioningForbidden for viewer, got %v", err)
		}
	})

	t.Run("admin may provision without membership", func(t *testing.T) {
		db := &fakeAdminOrOwnerTxDatabase{
			admin:      true,
			parentName: "gw1",
			role:       "", // no membership at all
		}
		repo, _ := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
		res, err := repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if err != nil {
			t.Fatalf("expected success for admin without membership, got %v", err)
		}
		if !res.Created {
			t.Fatal("expected Created: true")
		}
		if res.EntityID != mapper.SensorEntityID("gw1", "s1") {
			t.Fatalf("expected entity ID %s, got %s", mapper.SensorEntityID("gw1", "s1"), res.EntityID)
		}
		// Confirm membership query was NEVER executed for admin
		for _, q := range db.queriesExecuted {
			if strings.Contains(q, "user_gateways") {
				t.Fatalf("admin must not query user_gateways, found query: %s", q)
			}
		}
		if db.commitCalled != 1 {
			t.Fatalf("expected 1 commit call, got %d", db.commitCalled)
		}
	})

	t.Run("owner is allowed to provision", func(t *testing.T) {
		db := &fakeAdminOrOwnerTxDatabase{
			admin:      false,
			parentName: "gw1",
			role:       RoleOwner,
		}
		repo, _ := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
		res, err := repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if err != nil {
			t.Fatalf("expected success for owner, got %v", err)
		}
		if !res.Created {
			t.Fatal("expected Created: true")
		}
		if db.commitCalled != 1 {
			t.Fatalf("expected 1 commit call, got %d", db.commitCalled)
		}
	})

	t.Run("exact replay on identical metadata returns Created: false", func(t *testing.T) {
		db := &fakeAdminOrOwnerTxDatabase{
			admin:      true,
			parentName: "gw1",
			isReplay:   true,
		}
		repo, _ := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
		res, err := repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if err != nil {
			t.Fatalf("expected replay success, got %v", err)
		}
		if res.Created {
			t.Fatal("expected Created: false on exact replay")
		}
		if db.commitCalled != 1 {
			t.Fatalf("expected 1 commit call, got %d", db.commitCalled)
		}
	})

	t.Run("conflict on modified metadata", func(t *testing.T) {
		db := &fakeAdminOrOwnerTxDatabase{
			admin:      true,
			parentName: "gw1",
			conflict:   true,
		}
		repo, _ := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
		_, err := repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if !errors.Is(err, ErrProvisioningConflict) {
			t.Fatalf("expected ErrProvisioningConflict on metadata mismatch, got %v", err)
		}
		if db.commitCalled != 0 {
			t.Fatal("must not commit on conflict")
		}
		if db.rollbackCalled == 0 {
			t.Fatal("expected rollback on conflict")
		}
	})

	t.Run("inconsistent graph returns ErrProvisioningInconsistent", func(t *testing.T) {
		db := &fakeAdminOrOwnerTxDatabase{
			admin:        true,
			parentName:   "gw1",
			inconsistent: true,
		}
		repo, _ := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
		_, err := repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if !errors.Is(err, ErrProvisioningInconsistent) {
			t.Fatalf("expected ErrProvisioningInconsistent, got %v", err)
		}
	})

	t.Run("commit failure returns classified error and no result", func(t *testing.T) {
		db := &fakeAdminOrOwnerTxDatabase{
			admin:      true,
			parentName: "gw1",
			commitErr:  io.EOF,
		}
		repo, _ := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
		res, err := repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if err == nil {
			t.Fatal("expected error on commit failure, got nil")
		}
		if !errors.Is(err, ErrAdminOrOwnerProvisioningUnavailable) {
			t.Fatalf("expected ErrAdminOrOwnerProvisioningUnavailable on EOF commit failure, got %v", err)
		}
		if res.Created {
			t.Fatal("result must not claim Created on commit failure")
		}
	})

	t.Run("begin tx failure classification", func(t *testing.T) {
		repo, err := NewPostgresAdminOrOwnerSensorProvisioningRepository(provisioningBeginFunc(func(_ context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
			return nil, io.EOF
		}))
		if err != nil {
			t.Fatal(err)
		}
		_, err = repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if !errors.Is(err, ErrAdminOrOwnerProvisioningUnavailable) || !errors.Is(err, io.EOF) {
			t.Fatalf("expected ErrAdminOrOwnerProvisioningUnavailable joined with EOF, got %v", err)
		}
	})

	t.Run("query order and lock types", func(t *testing.T) {
		db := &fakeAdminOrOwnerTxDatabase{
			admin:      false,
			parentName: "gw1",
			role:       RoleOwner,
		}
		repo, _ := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
		_, err := repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if err != nil {
			t.Fatal(err)
		}
		if len(db.queriesExecuted) < 3 {
			t.Fatalf("expected at least 3 queries, got %d", len(db.queriesExecuted))
		}
		if !strings.Contains(db.queriesExecuted[0], "platform_admins") {
			t.Errorf("query 0 must check platform_admins, got %s", db.queriesExecuted[0])
		}
		if !strings.Contains(db.queriesExecuted[1], "FROM public.gateways") || !strings.Contains(db.queriesExecuted[1], "FOR SHARE") {
			t.Errorf("query 1 must lock parent FOR SHARE, got %s", db.queriesExecuted[1])
		}
		if strings.Contains(db.queriesExecuted[1], "FOR UPDATE") {
			t.Errorf("query 1 must not take a parent FOR UPDATE lock; it conflicts with telemetry FK KEY SHARE, got %s", db.queriesExecuted[1])
		}
		if !strings.Contains(db.queriesExecuted[2], "FROM public.user_gateways") || !strings.Contains(db.queriesExecuted[2], "FOR SHARE") {
			t.Errorf("query 2 must lock user_gateways FOR SHARE, got %s", db.queriesExecuted[2])
		}
	})

	t.Run("parent lock FOR SHARE avoids telemetry FK KEY SHARE cycle", func(t *testing.T) {
		// Focused regression for the POST Sensor exact-replay versus telemetry
		// Persist deadlock: replay must never hold the parent Gateway FOR UPDATE,
		// because telemetry Persist holds the Sensor row and then needs a foreign-key
		// KEY SHARE lock on the same Gateway row. The combined admin-or-owner entry
		// must take FOR SHARE, which is compatible with KEY SHARE while still
		// conflicting with a Gateway metadata FOR UPDATE.
		db := &fakeAdminOrOwnerTxDatabase{
			admin:      true,
			parentName: "gw1",
			isReplay:   true,
		}
		repo, _ := NewPostgresAdminOrOwnerSensorProvisioningRepository(db)
		res, err := repo.ProvisionSensorForAdminOrOwner(ctx, actor, in)
		if err != nil {
			t.Fatalf("exact replay failed: %v", err)
		}
		if res.Created {
			t.Fatal("exact replay must return Created: false")
		}
		if len(db.queriesExecuted) < 2 {
			t.Fatalf("expected a parent lock query, got %d queries: %v", len(db.queriesExecuted), db.queriesExecuted)
		}
		parentLock := db.queriesExecuted[1]
		if !strings.Contains(parentLock, "FROM public.gateways") || !strings.Contains(parentLock, "FOR SHARE") || strings.Contains(parentLock, "FOR UPDATE") {
			t.Fatalf("parent lock must be FOR SHARE and never FOR UPDATE, got %s", parentLock)
		}
	})
}

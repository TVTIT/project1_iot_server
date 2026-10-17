package digitaltwin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakePatchTxDatabase struct {
	beginErr error
	tx       pgx.Tx
}

func (d *fakePatchTxDatabase) BeginTx(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	if d.beginErr != nil {
		return nil, d.beginErr
	}
	if options.IsoLevel != pgx.ReadCommitted {
		return nil, fmt.Errorf("expected ReadCommitted isolation, got %v", options.IsoLevel)
	}
	return d.tx, nil
}

type fakePatchTx struct {
	pgx.Tx
	queriesExecuted []string
	execExecuted    []string
	rollbackCalled  int
	commitCalled    int

	isAdmin  bool
	adminErr error

	gwFound bool
	gwName  string
	gwDesc  *string
	gwErr   error

	twinFound      bool
	twinInternalID uuid.UUID
	twinName       string
	twinUpdatedAt  time.Time
	hasState       bool
	gwEntityCount  int
	twinErr        error

	updateGwErr   error
	updateTwinErr error
	commitErr     error

	newUpdatedAt time.Time

	// txStartTime / statementTime model PostgreSQL's timestamp semantics for the
	// final Twin metadata UPDATE. Like the real server, CURRENT_TIMESTAMP /
	// transaction_timestamp() resolve to the transaction-start time, while
	// statement_timestamp() resolves to the statement-start time (which, after
	// the row locks are held, is later than a competing transaction's commit).
	// When both are zero the fake falls back to newUpdatedAt so existing tests
	// remain unchanged.
	txStartTime   time.Time
	statementTime time.Time
}

type fakePatchScanRow struct {
	values []any
	err    error
}

func (r fakePatchScanRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, val := range r.values {
		if i >= len(dest) {
			break
		}
		switch d := dest[i].(type) {
		case *bool:
			if b, ok := val.(bool); ok {
				*d = b
			}
		case *string:
			if s, ok := val.(string); ok {
				*d = s
			}
		case **string:
			if s, ok := val.(*string); ok {
				*d = s
			} else if s, ok := val.(string); ok {
				*d = &s
			} else if val == nil {
				*d = nil
			}
		case *uuid.UUID:
			if u, ok := val.(uuid.UUID); ok {
				*d = u
			}
		case *time.Time:
			if t, ok := val.(time.Time); ok {
				*d = t
			}
		case *int:
			if n, ok := val.(int); ok {
				*d = n
			}
		}
	}
	return nil
}

func (tx *fakePatchTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	tx.queriesExecuted = append(tx.queriesExecuted, query)

	switch {
	case strings.Contains(query, "platform_admins"):
		if tx.adminErr != nil {
			return fakePatchScanRow{err: tx.adminErr}
		}
		return fakePatchScanRow{values: []any{tx.isAdmin}}

	case strings.Contains(query, "FROM public.gateways") && strings.Contains(query, "FOR UPDATE"):
		if tx.gwErr != nil {
			return fakePatchScanRow{err: tx.gwErr}
		}
		if !tx.gwFound {
			return fakePatchScanRow{err: pgx.ErrNoRows}
		}
		return fakePatchScanRow{values: []any{tx.gwName, tx.gwDesc}}

	case strings.Contains(query, "FROM public.twin_entities") && strings.Contains(query, "FOR UPDATE"):
		if tx.twinErr != nil {
			return fakePatchScanRow{err: tx.twinErr}
		}
		if !tx.twinFound {
			return fakePatchScanRow{err: pgx.ErrNoRows}
		}
		return fakePatchScanRow{values: []any{tx.twinInternalID, tx.twinName, tx.twinUpdatedAt, tx.hasState, tx.gwEntityCount}}

	case strings.Contains(query, "UPDATE public.twin_entities") && strings.Contains(query, "RETURNING updated_at"):
		if tx.updateTwinErr != nil {
			return fakePatchScanRow{err: tx.updateTwinErr}
		}
		return fakePatchScanRow{values: []any{tx.resolveTwinUpdatedAt(query)}}

	default:
		return fakePatchScanRow{err: fmt.Errorf("unhandled query in fake: %s", query)}
	}
}

func (tx *fakePatchTx) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	tx.execExecuted = append(tx.execExecuted, query)
	if strings.Contains(query, "UPDATE public.gateways") {
		if tx.updateGwErr != nil {
			return pgconn.CommandTag{}, tx.updateGwErr
		}
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
	return pgconn.CommandTag{}, fmt.Errorf("unhandled exec in fake: %s", query)
}

// resolveTwinUpdatedAt emulates PostgreSQL's expression evaluation for the final
// Twin metadata UPDATE: it returns the transaction-start time when the statement
// uses CURRENT_TIMESTAMP/transaction_timestamp()/now(), and the statement-start
// time when it uses statement_timestamp(). This lets a unit test assert the real
// timestamp semantics rather than merely matching a SQL substring.
func (tx *fakePatchTx) resolveTwinUpdatedAt(query string) time.Time {
	usesTransactionStart := strings.Contains(query, "CURRENT_TIMESTAMP") ||
		strings.Contains(query, "transaction_timestamp") ||
		strings.Contains(query, "now()")
	if usesTransactionStart {
		if !tx.txStartTime.IsZero() {
			return tx.txStartTime
		}
		return tx.newUpdatedAt
	}
	if strings.Contains(query, "statement_timestamp") {
		if !tx.statementTime.IsZero() {
			return tx.statementTime
		}
		return tx.newUpdatedAt
	}
	return tx.newUpdatedAt
}

func (tx *fakePatchTx) Commit(_ context.Context) error {
	tx.commitCalled++
	return tx.commitErr
}

func (tx *fakePatchTx) Rollback(_ context.Context) error {
	tx.rollbackCalled++
	return nil
}

func setupValidGatewayTx() *fakePatchTx {
	initialTime := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	newTime := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	desc := "Old Description"
	return &fakePatchTx{
		isAdmin:        true,
		gwFound:        true,
		gwName:         "Gateway Alpha",
		gwDesc:         &desc,
		twinFound:      true,
		twinInternalID: uuid.New(),
		twinName:       "Gateway Alpha",
		twinUpdatedAt:  initialTime,
		hasState:       true,
		gwEntityCount:  1,
		newUpdatedAt:   newTime,
	}
}

func TestPostgresGatewayMetadataPatcher_Constructor(t *testing.T) {
	var typedNil *fakePatchTxDatabase
	if _, err := NewPostgresGatewayMetadataPatcher(nil); err == nil {
		t.Fatal("expected error for nil db")
	}
	if _, err := NewPostgresGatewayMetadataPatcher(typedNil); err == nil {
		t.Fatal("expected error for typed nil db")
	}

	db := &fakePatchTxDatabase{}
	patcher, err := NewPostgresGatewayMetadataPatcher(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if patcher == nil {
		t.Fatal("expected non-nil patcher")
	}
}

func TestPostgresGatewayMetadataPatcher_AdminAuthorization(t *testing.T) {
	adminID := uuid.New()
	nonAdminID := uuid.New()
	gatewayID := "gw_001"
	newName := "New Gateway Name"
	input := PatchGatewayInput{Name: &newName}

	// 1. Non-admin: rejected with ErrForbidden BEFORE looking up gateway
	tx := setupValidGatewayTx()
	tx.isAdmin = false
	db := &fakePatchTxDatabase{tx: tx}
	patcher, _ := NewPostgresGatewayMetadataPatcher(db)

	_, err := patcher.PatchGatewayMetadata(context.Background(), nonAdminID, gatewayID, input)
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected ErrForbidden for non-admin, got %v", err)
	}
	if tx.rollbackCalled == 0 {
		t.Fatal("expected rollback called on forbidden")
	}
	// Verify that gateways table was NEVER touched!
	for _, q := range tx.queriesExecuted {
		if strings.Contains(q, "public.gateways") {
			t.Fatalf("gateways table must NOT be queried for non-admin: %s", q)
		}
	}

	// 2. Admin lookup error: classified availability/dependency error
	tx2 := setupValidGatewayTx()
	tx2.adminErr = errors.New("admin table lock error")
	db2 := &fakePatchTxDatabase{tx: tx2}
	patcher2, _ := NewPostgresGatewayMetadataPatcher(db2)

	_, err = patcher2.PatchGatewayMetadata(context.Background(), adminID, gatewayID, input)
	if !errors.Is(err, ErrUnexpected) && !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected dependency/unexpected error on admin lookup failure, got %v", err)
	}

	// 3. Platform admin succeeds
	tx3 := setupValidGatewayTx()
	db3 := &fakePatchTxDatabase{tx: tx3}
	patcher3, _ := NewPostgresGatewayMetadataPatcher(db3)

	res, err := patcher3.PatchGatewayMetadata(context.Background(), adminID, gatewayID, input)
	if err != nil {
		t.Fatalf("unexpected error for admin: %v", err)
	}
	if res.Name != newName {
		t.Fatalf("res.Name = %q, want %q", res.Name, newName)
	}
	if tx3.commitCalled != 1 {
		t.Fatalf("commitCalled = %d, want 1", tx3.commitCalled)
	}
}

func TestPostgresGatewayMetadataPatcher_MissingGateway(t *testing.T) {
	adminID := uuid.New()
	gatewayID := "gw_nonexistent"
	newName := "New Name"
	input := PatchGatewayInput{Name: &newName}

	tx := setupValidGatewayTx()
	tx.gwFound = false // Gateway not in database
	db := &fakePatchTxDatabase{tx: tx}
	patcher, _ := NewPostgresGatewayMetadataPatcher(db)

	_, err := patcher.PatchGatewayMetadata(context.Background(), adminID, gatewayID, input)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing gateway, got %v", err)
	}
	if tx.rollbackCalled == 0 {
		t.Fatal("expected rollback on missing gateway")
	}
}

func TestPostgresGatewayMetadataPatcher_InconsistentGraph(t *testing.T) {
	adminID := uuid.New()
	gatewayID := "gw_001"
	newName := "New Name"
	input := PatchGatewayInput{Name: &newName}

	cases := []struct {
		name     string
		mutateTx func(tx *fakePatchTx)
	}{
		{
			name: "twin entity missing",
			mutateTx: func(tx *fakePatchTx) {
				tx.twinFound = false
			},
		},
		{
			name: "twin states missing",
			mutateTx: func(tx *fakePatchTx) {
				tx.hasState = false
			},
		},
		{
			name: "twin name does not match relational name",
			mutateTx: func(tx *fakePatchTx) {
				tx.twinName = "Different Twin Name"
			},
		},
		{
			name: "gateway twin entity count != 1",
			mutateTx: func(tx *fakePatchTx) {
				tx.gwEntityCount = 2
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := setupValidGatewayTx()
			tc.mutateTx(tx)
			db := &fakePatchTxDatabase{tx: tx}
			patcher, _ := NewPostgresGatewayMetadataPatcher(db)

			res, err := patcher.PatchGatewayMetadata(context.Background(), adminID, gatewayID, input)
			if !errors.Is(err, ErrInconsistentGraph) {
				t.Fatalf("expected ErrInconsistentGraph, got %v", err)
			}
			if res != (PatchGatewayResult{}) {
				t.Fatalf("expected empty result, got %#v", res)
			}
			if tx.rollbackCalled == 0 {
				t.Fatal("expected rollback called on inconsistency")
			}
			// Verify NO repair / insert / update was attempted
			if len(tx.execExecuted) != 0 {
				t.Fatalf("expected 0 exec statements on inconsistency (safe data fault), got %d", len(tx.execExecuted))
			}
		})
	}
}

func TestPostgresGatewayMetadataPatcher_FieldVariations(t *testing.T) {
	adminID := uuid.New()
	gatewayID := "gw_001"
	newName := "Updated Name Only"
	newDesc := "Updated Description Only"

	// 1. Name only change
	t.Run("name only", func(t *testing.T) {
		tx := setupValidGatewayTx()
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresGatewayMetadataPatcher(db)

		input := PatchGatewayInput{Name: &newName}
		res, err := patcher.PatchGatewayMetadata(context.Background(), adminID, gatewayID, input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Name != newName {
			t.Fatalf("res.Name = %q, want %q", res.Name, newName)
		}
		if res.Description == nil || *res.Description != "Old Description" {
			t.Fatalf("res.Description = %v, want 'Old Description' preserved", res.Description)
		}
		if !res.UpdatedAt.Equal(tx.newUpdatedAt) {
			t.Fatalf("res.UpdatedAt = %v, want %v", res.UpdatedAt, tx.newUpdatedAt)
		}
		if len(tx.execExecuted) != 1 || !strings.Contains(tx.execExecuted[0], "SET name = $1 WHERE gateway_id = $2") {
			t.Fatalf("expected gateways name update only, got %v", tx.execExecuted)
		}
	})

	// 2. Description only change
	t.Run("description only", func(t *testing.T) {
		tx := setupValidGatewayTx()
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresGatewayMetadataPatcher(db)

		input := PatchGatewayInput{DescriptionSet: true, Description: &newDesc}
		res, err := patcher.PatchGatewayMetadata(context.Background(), adminID, gatewayID, input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Name != "Gateway Alpha" {
			t.Fatalf("res.Name = %q, want 'Gateway Alpha' preserved", res.Name)
		}
		if res.Description == nil || *res.Description != newDesc {
			t.Fatalf("res.Description = %v, want %q", res.Description, newDesc)
		}
		if !res.UpdatedAt.Equal(tx.newUpdatedAt) {
			t.Fatalf("res.UpdatedAt = %v, want %v", res.UpdatedAt, tx.newUpdatedAt)
		}
		if len(tx.execExecuted) != 1 || !strings.Contains(tx.execExecuted[0], "SET description = $1 WHERE gateway_id = $2") {
			t.Fatalf("expected gateways description update only, got %v", tx.execExecuted)
		}
	})

	// 3. Description explicit null clear
	t.Run("description null clear", func(t *testing.T) {
		tx := setupValidGatewayTx()
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresGatewayMetadataPatcher(db)

		input := PatchGatewayInput{DescriptionSet: true, Description: nil}
		res, err := patcher.PatchGatewayMetadata(context.Background(), adminID, gatewayID, input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Name != "Gateway Alpha" {
			t.Fatalf("res.Name = %q, want 'Gateway Alpha' preserved", res.Name)
		}
		if res.Description != nil {
			t.Fatalf("res.Description = %v, want nil", res.Description)
		}
		if !res.UpdatedAt.Equal(tx.newUpdatedAt) {
			t.Fatalf("res.UpdatedAt = %v, want %v", res.UpdatedAt, tx.newUpdatedAt)
		}
		if len(tx.execExecuted) != 1 || !strings.Contains(tx.execExecuted[0], "SET description = $1 WHERE gateway_id = $2") {
			t.Fatalf("expected gateways description update to null, got %v", tx.execExecuted)
		}
	})

	// 4. Both name and description change
	t.Run("both name and description", func(t *testing.T) {
		tx := setupValidGatewayTx()
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresGatewayMetadataPatcher(db)

		input := PatchGatewayInput{Name: &newName, DescriptionSet: true, Description: &newDesc}
		res, err := patcher.PatchGatewayMetadata(context.Background(), adminID, gatewayID, input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Name != newName || res.Description == nil || *res.Description != newDesc {
			t.Fatalf("res mismatch: %#v", res)
		}
		if !res.UpdatedAt.Equal(tx.newUpdatedAt) {
			t.Fatalf("res.UpdatedAt = %v, want %v", res.UpdatedAt, tx.newUpdatedAt)
		}
		if len(tx.execExecuted) != 1 || !strings.Contains(tx.execExecuted[0], "SET name = $1, description = $2") {
			t.Fatalf("expected both columns updated in gateways, got %v", tx.execExecuted)
		}
	})
}

func TestPostgresGatewayMetadataPatcher_SameValueNoOp(t *testing.T) {
	adminID := uuid.New()
	gatewayID := "gw_001"
	sameName := "Gateway Alpha"
	sameDesc := "Old Description"

	tests := []struct {
		name  string
		input PatchGatewayInput
	}{
		{
			name:  "same name only",
			input: PatchGatewayInput{Name: &sameName},
		},
		{
			name:  "same description only",
			input: PatchGatewayInput{DescriptionSet: true, Description: &sameDesc},
		},
		{
			name:  "same name and same description",
			input: PatchGatewayInput{Name: &sameName, DescriptionSet: true, Description: &sameDesc},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := setupValidGatewayTx()
			db := &fakePatchTxDatabase{tx: tx}
			patcher, _ := NewPostgresGatewayMetadataPatcher(db)

			res, err := patcher.PatchGatewayMetadata(context.Background(), adminID, gatewayID, tc.input)
			if err != nil {
				t.Fatalf("unexpected error on same-value patch: %v", err)
			}
			// Must return unchanged timestamp!
			if !res.UpdatedAt.Equal(tx.twinUpdatedAt) {
				t.Fatalf("res.UpdatedAt = %v, want unchanged %v", res.UpdatedAt, tx.twinUpdatedAt)
			}
			// Zero UPDATE statements in execExecuted!
			if len(tx.execExecuted) != 0 {
				t.Fatalf("expected 0 exec statements on same-value no-op, got %d: %v", len(tx.execExecuted), tx.execExecuted)
			}
			// Zero UPDATE statements in queriesExecuted!
			for _, q := range tx.queriesExecuted {
				if strings.HasPrefix(strings.TrimSpace(q), "UPDATE") {
					t.Fatalf("unexpected UPDATE query on same-value no-op: %s", q)
				}
			}
			// Commit must still be called
			if tx.commitCalled != 1 {
				t.Fatalf("commitCalled = %d, want 1", tx.commitCalled)
			}
		})
	}

	// Also test description already null and explicit null clear requested
	t.Run("description already null same-value no-op", func(t *testing.T) {
		tx := setupValidGatewayTx()
		tx.gwDesc = nil // already null
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresGatewayMetadataPatcher(db)

		input := PatchGatewayInput{DescriptionSet: true, Description: nil}
		res, err := patcher.PatchGatewayMetadata(context.Background(), adminID, gatewayID, input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Description != nil {
			t.Fatalf("expected nil description, got %v", res.Description)
		}
		if !res.UpdatedAt.Equal(tx.twinUpdatedAt) {
			t.Fatalf("expected unchanged timestamp, got %v", res.UpdatedAt)
		}
		if len(tx.execExecuted) != 0 {
			t.Fatalf("expected 0 exec statements, got %d", len(tx.execExecuted))
		}
	})
}

func TestPostgresGatewayMetadataPatcher_FailuresAndRollback(t *testing.T) {
	adminID := uuid.New()
	gatewayID := "gw_001"
	newName := "Updated Name"
	input := PatchGatewayInput{Name: &newName}

	// 1. BeginTx failure
	t.Run("BeginTx failure", func(t *testing.T) {
		db := &fakePatchTxDatabase{beginErr: errors.New("db pool exhausted")}
		patcher, _ := NewPostgresGatewayMetadataPatcher(db)

		_, err := patcher.PatchGatewayMetadata(context.Background(), adminID, gatewayID, input)
		if !errors.Is(err, ErrUnexpected) && !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected dependency error on BeginTx failure, got %v", err)
		}
	})

	// 2. Gateway lock query error
	t.Run("gateway lock error", func(t *testing.T) {
		tx := setupValidGatewayTx()
		tx.gwErr = errors.New("deadlock or query timeout on gateway lock")
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresGatewayMetadataPatcher(db)

		_, err := patcher.PatchGatewayMetadata(context.Background(), adminID, gatewayID, input)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if tx.rollbackCalled == 0 {
			t.Fatal("expected rollback called")
		}
	})

	// 3. Relational update error
	t.Run("relational update error", func(t *testing.T) {
		tx := setupValidGatewayTx()
		tx.updateGwErr = errors.New("disk full or constraint violation")
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresGatewayMetadataPatcher(db)

		_, err := patcher.PatchGatewayMetadata(context.Background(), adminID, gatewayID, input)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if tx.rollbackCalled == 0 {
			t.Fatal("expected rollback called")
		}
	})

	// 4. Twin update error (no partial success!)
	t.Run("twin update error - no partial success", func(t *testing.T) {
		tx := setupValidGatewayTx()
		tx.updateTwinErr = errors.New("twin update failed")
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresGatewayMetadataPatcher(db)

		res, err := patcher.PatchGatewayMetadata(context.Background(), adminID, gatewayID, input)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if res != (PatchGatewayResult{}) {
			t.Fatalf("expected empty result on partial failure, got %#v", res)
		}
		if tx.rollbackCalled == 0 {
			t.Fatal("expected rollback called")
		}
	})

	// 5. Commit failure (commit uncertain / unavailable)
	t.Run("commit failure", func(t *testing.T) {
		tx := setupValidGatewayTx()
		tx.commitErr = errors.New("connection reset by peer during commit")
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresGatewayMetadataPatcher(db)

		res, err := patcher.PatchGatewayMetadata(context.Background(), adminID, gatewayID, input)
		if !errors.Is(err, ErrMetadataPatchUnavailable) {
			t.Fatalf("expected ErrMetadataPatchUnavailable on commit failure, got %v", err)
		}
		if res != (PatchGatewayResult{}) {
			t.Fatalf("expected empty result on commit failure, got %#v", res)
		}
	})
}

// TestPostgresGatewayMetadataPatcher_UpdatedAtIsPostLockStatementTime proves the
// final Twin metadata UPDATE reports/returns the post-lock write statement time
// rather than the earlier transaction-start time, for BOTH metadata UPDATE
// branches (name change and description-only change). It asserts the real
// timestamp semantics via a fake that resolves CURRENT_TIMESTAMP to the
// transaction-start time and statement_timestamp() to the statement-start time.
func TestPostgresGatewayMetadataPatcher_UpdatedAtIsPostLockStatementTime(t *testing.T) {
	adminID := uuid.New()
	gatewayID := "gw_001"

	txStart := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	stmtTime := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		input PatchGatewayInput
	}{
		{
			name:  "name branch",
			input: PatchGatewayInput{Name: strPtrTest("Updated Name Under Locks")},
		},
		{
			name:  "description-only branch",
			input: PatchGatewayInput{DescriptionSet: true, Description: strPtrTest("Updated Description Under Locks")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := setupValidGatewayTx()
			tx.txStartTime = txStart
			tx.statementTime = stmtTime
			db := &fakePatchTxDatabase{tx: tx}
			patcher, _ := NewPostgresGatewayMetadataPatcher(db)

			res, err := patcher.PatchGatewayMetadata(context.Background(), adminID, gatewayID, tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.UpdatedAt.Equal(txStart) {
				t.Fatalf("Gateway metadata updated_at reflects transaction start %v instead of post-lock statement time %v", txStart, stmtTime)
			}
			if !res.UpdatedAt.Equal(stmtTime) {
				t.Fatalf("Gateway metadata updated_at = %v, want post-lock statement time %v", res.UpdatedAt, stmtTime)
			}
		})
	}
}

func strPtrTest(s string) *string { return &s }

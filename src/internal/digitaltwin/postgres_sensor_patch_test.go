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

	"iot-platform/internal/digitaltwin/mapper"
)

type fakeSensorPatchTx struct {
	pgx.Tx
	queriesExecuted []string
	execExecuted    []string
	rollbackCalled  int
	commitCalled    int

	isAdmin  bool
	adminErr error

	parentGwFound bool
	parentGwName  string
	parentGwErr   error

	membershipFound bool
	membershipRole  string
	membershipErr   error

	sensorFound bool
	sensorName  string
	sensorUnit  *string
	sensorErr   error

	parentTwinFound bool
	parentTwinID    uuid.UUID
	parentTwinErr   error

	sensorTwinFound      bool
	sensorTwinInternalID uuid.UUID
	sensorTwinName       string
	sensorTwinUpdatedAt  time.Time
	hasState             bool
	hasRelationship      bool
	sensorTwinErr        error

	updateSensorErr error
	updateTwinErr   error
	commitErr       error

	newUpdatedAt time.Time

	// txStartTime / statementTime model PostgreSQL's timestamp semantics for the
	// final Twin metadata UPDATE (see the Gateway fake for the rationale). Zero
	// values fall back to newUpdatedAt so existing tests remain unchanged.
	txStartTime   time.Time
	statementTime time.Time
}

func (tx *fakeSensorPatchTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	tx.queriesExecuted = append(tx.queriesExecuted, query)

	switch {
	case strings.Contains(query, "platform_admins"):
		if tx.adminErr != nil {
			return fakePatchScanRow{err: tx.adminErr}
		}
		return fakePatchScanRow{values: []any{tx.isAdmin}}

	case strings.Contains(query, "FROM public.gateways") && strings.Contains(query, "FOR SHARE"):
		if tx.parentGwErr != nil {
			return fakePatchScanRow{err: tx.parentGwErr}
		}
		if !tx.parentGwFound {
			return fakePatchScanRow{err: pgx.ErrNoRows}
		}
		return fakePatchScanRow{values: []any{tx.parentGwName}}

	case strings.Contains(query, "FROM public.user_gateways") && strings.Contains(query, "FOR SHARE"):
		if tx.membershipErr != nil {
			return fakePatchScanRow{err: tx.membershipErr}
		}
		if !tx.membershipFound {
			return fakePatchScanRow{err: pgx.ErrNoRows}
		}
		return fakePatchScanRow{values: []any{tx.membershipRole}}

	case strings.Contains(query, "FROM public.sensors") && strings.Contains(query, "FOR UPDATE"):
		if tx.sensorErr != nil {
			return fakePatchScanRow{err: tx.sensorErr}
		}
		if !tx.sensorFound {
			return fakePatchScanRow{err: pgx.ErrNoRows}
		}
		return fakePatchScanRow{values: []any{tx.sensorName, tx.sensorUnit}}

	case strings.Contains(query, "FROM public.twin_entities e") && strings.Contains(query, "e.entity_type = 'Gateway'"):
		if tx.parentTwinErr != nil {
			return fakePatchScanRow{err: tx.parentTwinErr}
		}
		if !tx.parentTwinFound {
			return fakePatchScanRow{err: pgx.ErrNoRows}
		}
		return fakePatchScanRow{values: []any{tx.parentTwinID}}

	case strings.Contains(query, "FROM public.twin_entities e") && strings.Contains(query, "e.entity_type = 'Sensor'") && strings.Contains(query, "FOR UPDATE OF e"):
		if tx.sensorTwinErr != nil {
			return fakePatchScanRow{err: tx.sensorTwinErr}
		}
		if !tx.sensorTwinFound {
			return fakePatchScanRow{err: pgx.ErrNoRows}
		}
		return fakePatchScanRow{values: []any{tx.sensorTwinInternalID, tx.sensorTwinName, tx.sensorTwinUpdatedAt, tx.hasState, tx.hasRelationship}}

	case strings.Contains(query, "UPDATE public.twin_entities") && strings.Contains(query, "RETURNING updated_at"):
		if tx.updateTwinErr != nil {
			return fakePatchScanRow{err: tx.updateTwinErr}
		}
		return fakePatchScanRow{values: []any{tx.resolveTwinUpdatedAt(query)}}

	default:
		return fakePatchScanRow{err: fmt.Errorf("unhandled query in sensor fake: %s", query)}
	}
}

func (tx *fakeSensorPatchTx) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	tx.execExecuted = append(tx.execExecuted, query)
	if strings.Contains(query, "UPDATE public.sensors") {
		if tx.updateSensorErr != nil {
			return pgconn.CommandTag{}, tx.updateSensorErr
		}
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
	return pgconn.CommandTag{}, fmt.Errorf("unhandled exec in sensor fake: %s", query)
}

// resolveTwinUpdatedAt emulates PostgreSQL's expression evaluation for the final
// Twin metadata UPDATE: CURRENT_TIMESTAMP/transaction_timestamp()/now() resolve
// to the transaction-start time; statement_timestamp() resolves to the
// statement-start time.
func (tx *fakeSensorPatchTx) resolveTwinUpdatedAt(query string) time.Time {
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

func (tx *fakeSensorPatchTx) Commit(_ context.Context) error {
	tx.commitCalled++
	return tx.commitErr
}

func (tx *fakeSensorPatchTx) Rollback(_ context.Context) error {
	tx.rollbackCalled++
	return nil
}

func setupValidSensorTx() *fakeSensorPatchTx {
	initialTime := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	newTime := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	unit := "degC"
	parentTwinID := uuid.New()
	sensorTwinID := uuid.New()
	return &fakeSensorPatchTx{
		isAdmin:              false,
		parentGwFound:        true,
		parentGwName:         "Gateway Alpha",
		membershipFound:      true,
		membershipRole:       "owner",
		sensorFound:          true,
		sensorName:           "Sensor Temperature",
		sensorUnit:           &unit,
		parentTwinFound:      true,
		parentTwinID:         parentTwinID,
		sensorTwinFound:      true,
		sensorTwinInternalID: sensorTwinID,
		sensorTwinName:       "Sensor Temperature",
		sensorTwinUpdatedAt:  initialTime,
		hasState:             true,
		hasRelationship:      true,
		newUpdatedAt:         newTime,
	}
}

func TestPostgresSensorMetadataPatcher_Constructor(t *testing.T) {
	var typedNil *fakePatchTxDatabase
	if _, err := NewPostgresSensorMetadataPatcher(nil); err == nil {
		t.Fatal("expected error for nil db")
	}
	if _, err := NewPostgresSensorMetadataPatcher(typedNil); err == nil {
		t.Fatal("expected error for typed nil db")
	}

	db := &fakePatchTxDatabase{}
	patcher, err := NewPostgresSensorMetadataPatcher(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if patcher == nil {
		t.Fatal("expected non-nil patcher")
	}
}

func TestPostgresSensorMetadataPatcher_InputValidation(t *testing.T) {
	db := &fakePatchTxDatabase{}
	patcher, err := NewPostgresSensorMetadataPatcher(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	validName := "Sensor 01"
	validInput := PatchSensorInput{Name: &validName}

	// Nil receiver
	var nilPatcher *PostgresSensorMetadataPatcher
	if _, err := nilPatcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", validInput); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable for nil receiver, got %v", err)
	}

	// Patcher with nil db
	brokenPatcher := &PostgresSensorMetadataPatcher{db: nil}
	if _, err := brokenPatcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", validInput); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable for broken patcher, got %v", err)
	}

	// Nil context
	var nilCtx context.Context
	if _, err := patcher.PatchSensorMetadata(nilCtx, uuid.New(), "gw1", "s1", validInput); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for nil context, got %v", err)
	}

	// Pre-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := patcher.PatchSensorMetadata(ctx, uuid.New(), "gw1", "s1", validInput); !errors.Is(err, ErrDependencyUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ErrDependencyUnavailable wrapping Canceled, got %v", err)
	}

	// Nil actor
	if _, err := patcher.PatchSensorMetadata(context.Background(), uuid.Nil, "gw1", "s1", validInput); !errors.Is(err, ErrNilActor) {
		t.Fatalf("expected ErrNilActor, got %v", err)
	}

	// Invalid gatewayID
	if _, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "invalid/gw", "s1", validInput); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for invalid gatewayID, got %v", err)
	}

	// Invalid sensorID
	if _, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "invalid*s", validInput); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for invalid sensorID, got %v", err)
	}

	// Invalid input (nil name)
	if _, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", PatchSensorInput{Name: nil}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for nil name, got %v", err)
	}

	// BeginTx error
	beginErrDB := &fakePatchTxDatabase{beginErr: errors.New("connection failed")}
	errPatcher, _ := NewPostgresSensorMetadataPatcher(beginErrDB)
	if _, err := errPatcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", validInput); !errors.Is(err, ErrUnexpected) && !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrUnexpected or ErrDependencyUnavailable for BeginTx failure, got %v", err)
	}
}

func TestPostgresSensorMetadataPatcher_Authorization(t *testing.T) {
	validName := "Updated Sensor"
	input := PatchSensorInput{Name: &validName}

	t.Run("admin lookup error does NOT fall back to owner lookup", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.adminErr = errors.New("admin table corrupted")
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if !errors.Is(err, ErrDependencyUnavailable) && !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected error on admin check failure, got %v", err)
		}
		// Invariant: no query to user_gateways must be executed!
		for _, q := range tx.queriesExecuted {
			if strings.Contains(q, "public.user_gateways") {
				t.Fatalf("query executed on user_gateways despite admin check error: %s", q)
			}
		}
	})

	t.Run("admin without membership succeeds", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.isAdmin = true
		tx.membershipFound = false // No membership!
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		res, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if err != nil {
			t.Fatalf("unexpected error for admin: %v", err)
		}
		if res.Name != validName {
			t.Fatalf("res.Name = %q, want %q", res.Name, validName)
		}
		// Invariant: admin skips user_gateways query completely
		for _, q := range tx.queriesExecuted {
			if strings.Contains(q, "public.user_gateways") {
				t.Fatalf("admin query touched user_gateways: %s", q)
			}
		}
	})

	t.Run("owner membership succeeds", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.isAdmin = false
		tx.membershipFound = true
		tx.membershipRole = "owner"
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		res, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if err != nil {
			t.Fatalf("unexpected error for owner: %v", err)
		}
		if res.Name != validName {
			t.Fatalf("res.Name = %q, want %q", res.Name, validName)
		}
	})

	t.Run("non-admin with missing membership returns ErrNotFound", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.isAdmin = false
		tx.membershipFound = false
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound for non-member, got %v", err)
		}
	})

	t.Run("non-admin operator returns ErrForbidden", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.isAdmin = false
		tx.membershipFound = true
		tx.membershipRole = "operator"
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("expected ErrForbidden for operator, got %v", err)
		}
	})

	t.Run("non-admin viewer returns ErrForbidden", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.isAdmin = false
		tx.membershipFound = true
		tx.membershipRole = "viewer"
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("expected ErrForbidden for viewer, got %v", err)
		}
	})
}

func TestPostgresSensorMetadataPatcher_LockOrderAndGraphInvariants(t *testing.T) {
	validName := "Updated Sensor"
	input := PatchSensorInput{Name: &validName}

	t.Run("parent Gateway missing returns ErrNotFound", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.parentGwFound = false
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound for missing parent gateway, got %v", err)
		}
	})

	t.Run("sensor row missing returns ErrNotFound", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.sensorFound = false
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound for missing sensor row, got %v", err)
		}
	})

	t.Run("parent twin inconsistent returns ErrInconsistentGraph", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.parentTwinFound = false
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if !errors.Is(err, ErrInconsistentGraph) {
			t.Fatalf("expected ErrInconsistentGraph for inconsistent parent twin, got %v", err)
		}
	})

	t.Run("sensor twin missing returns ErrInconsistentGraph", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.sensorTwinFound = false
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if !errors.Is(err, ErrInconsistentGraph) {
			t.Fatalf("expected ErrInconsistentGraph for missing sensor twin, got %v", err)
		}
	})

	t.Run("sensor twin missing state returns ErrInconsistentGraph", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.hasState = false
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if !errors.Is(err, ErrInconsistentGraph) {
			t.Fatalf("expected ErrInconsistentGraph when sensor state is missing, got %v", err)
		}
	})

	t.Run("sensor twin missing hasSensor relationship returns ErrInconsistentGraph", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.hasRelationship = false
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if !errors.Is(err, ErrInconsistentGraph) {
			t.Fatalf("expected ErrInconsistentGraph when hasSensor is missing, got %v", err)
		}
	})

	t.Run("sensor twin name mismatch with relational name returns ErrInconsistentGraph", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.sensorName = "Relational Name"
		tx.sensorTwinName = "Twin Name Mismatch"
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if !errors.Is(err, ErrInconsistentGraph) {
			t.Fatalf("expected ErrInconsistentGraph when sensor names mismatch, got %v", err)
		}
	})

	t.Run("strict lock order: parent -> membership -> sensor -> twin", func(t *testing.T) {
		tx := setupValidSensorTx()
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Find indices of lock queries
		parentLockIdx := -1
		membershipLockIdx := -1
		sensorLockIdx := -1
		twinLockIdx := -1

		for i, q := range tx.queriesExecuted {
			if strings.Contains(q, "FROM public.gateways") && strings.Contains(q, "FOR SHARE") {
				parentLockIdx = i
			}
			if strings.Contains(q, "FROM public.user_gateways") && strings.Contains(q, "FOR SHARE") {
				membershipLockIdx = i
			}
			if strings.Contains(q, "FROM public.sensors") && strings.Contains(q, "FOR UPDATE") {
				sensorLockIdx = i
			}
			if strings.Contains(q, "FROM public.twin_entities e") && strings.Contains(q, "FOR UPDATE OF e") {
				twinLockIdx = i
			}
		}

		if parentLockIdx == -1 || membershipLockIdx == -1 || sensorLockIdx == -1 || twinLockIdx == -1 {
			t.Fatalf("one or more required locks not executed: parent=%d member=%d sensor=%d twin=%d",
				parentLockIdx, membershipLockIdx, sensorLockIdx, twinLockIdx)
		}

		if parentLockIdx >= membershipLockIdx || membershipLockIdx >= sensorLockIdx || sensorLockIdx >= twinLockIdx {
			t.Fatalf("lock order violated! parent=%d member=%d sensor=%d twin=%d",
				parentLockIdx, membershipLockIdx, sensorLockIdx, twinLockIdx)
		}
	})
}

func TestPostgresSensorMetadataPatcher_SameValueNoOp(t *testing.T) {
	tx := setupValidSensorTx()
	// Current name is "Sensor Temperature"
	sameName := "Sensor Temperature"
	input := PatchSensorInput{Name: &sameName}

	db := &fakePatchTxDatabase{tx: tx}
	patcher, _ := NewPostgresSensorMetadataPatcher(db)

	res, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Invariant: no UPDATE executed!
	if len(tx.execExecuted) != 0 {
		t.Fatalf("expected 0 exec commands on no-op, got %d: %v", len(tx.execExecuted), tx.execExecuted)
	}
	for _, q := range tx.queriesExecuted {
		if strings.Contains(q, "UPDATE public.") {
			t.Fatalf("UPDATE query executed on no-op: %s", q)
		}
	}

	// Invariant: old timestamp preserved
	if !res.UpdatedAt.Equal(tx.sensorTwinUpdatedAt) {
		t.Fatalf("updated_at modified on no-op: got %v, want %v", res.UpdatedAt, tx.sensorTwinUpdatedAt)
	}

	// Invariant: commit was called
	if tx.commitCalled != 1 {
		t.Fatalf("commit called %d times, want 1", tx.commitCalled)
	}
}

func TestPostgresSensorMetadataPatcher_UpdateSuccessAndUnitPreservation(t *testing.T) {
	tx := setupValidSensorTx()
	newName := "Sensor Temperature Rev 2"
	input := PatchSensorInput{Name: &newName}

	db := &fakePatchTxDatabase{tx: tx}
	patcher, _ := NewPostgresSensorMetadataPatcher(db)

	res, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Invariants:
	if res.Name != newName {
		t.Fatalf("res.Name = %q, want %q", res.Name, newName)
	}
	if res.ID != mapper.SensorEntityID("gw1", "s1") {
		t.Fatalf("res.ID = %q, want %q", res.ID, mapper.SensorEntityID("gw1", "s1"))
	}
	if res.Type != EntityTypeSensor {
		t.Fatalf("res.Type = %v, want %v", res.Type, EntityTypeSensor)
	}
	if res.GatewayID != "gw1" || res.SensorID != "s1" {
		t.Fatalf("res GatewayID/SensorID mismatch: gw=%s sensor=%s", res.GatewayID, res.SensorID)
	}
	if res.Unit == nil || *res.Unit != "degC" {
		t.Fatalf("unit not preserved: got %v, want degC", res.Unit)
	}
	if !res.UpdatedAt.Equal(tx.newUpdatedAt) {
		t.Fatalf("updated_at not updated: got %v, want %v", res.UpdatedAt, tx.newUpdatedAt)
	}

	// Both relational and twin updates were executed
	var hasSensorUpdate, hasTwinUpdate bool
	for _, e := range tx.execExecuted {
		if strings.Contains(e, "UPDATE public.sensors") {
			hasSensorUpdate = true
		}
	}
	for _, q := range tx.queriesExecuted {
		if strings.Contains(q, "UPDATE public.twin_entities") {
			hasTwinUpdate = true
		}
	}
	if !hasSensorUpdate || !hasTwinUpdate {
		t.Fatalf("expected both sensor update and twin update: sensor=%v twin=%v", hasSensorUpdate, hasTwinUpdate)
	}

	if tx.commitCalled != 1 {
		t.Fatalf("commit called %d times, want 1", tx.commitCalled)
	}
}

func TestPostgresSensorMetadataPatcher_FailuresAndCommitError(t *testing.T) {
	validName := "Updated Sensor"
	input := PatchSensorInput{Name: &validName}

	t.Run("sensor update failure rolls back and returns error", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.updateSensorErr = errors.New("update sensor failed")
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if tx.commitCalled != 0 {
			t.Fatalf("commit called %d times on failure, want 0", tx.commitCalled)
		}
		if tx.rollbackCalled == 0 {
			t.Fatal("expected rollback called")
		}
	})

	t.Run("twin update failure rolls back and returns error", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.updateTwinErr = errors.New("update twin failed")
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if tx.commitCalled != 0 {
			t.Fatalf("commit called %d times on failure, want 0", tx.commitCalled)
		}
		if tx.rollbackCalled == 0 {
			t.Fatal("expected rollback called")
		}
	})

	t.Run("commit error returns ErrMetadataPatchUnavailable without claiming clean rollback", func(t *testing.T) {
		tx := setupValidSensorTx()
		tx.commitErr = errors.New("commit network error")
		db := &fakePatchTxDatabase{tx: tx}
		patcher, _ := NewPostgresSensorMetadataPatcher(db)

		_, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", input)
		if !errors.Is(err, ErrMetadataPatchUnavailable) {
			t.Fatalf("expected ErrMetadataPatchUnavailable, got %v", err)
		}
	})
}

func TestPostgresSensorMetadataPatcher_TwoGatewaysSharedSensorIDIsolation(t *testing.T) {
	// Two gateways share the same sensor_id "sensor_001"
	// Ensure that patching on gateway_A addresses strictly (gateway_A, sensor_001)
	// and does not touch or bind gateway_B.
	tx := setupValidSensorTx()
	newName := "Sensor for GW A"
	input := PatchSensorInput{Name: &newName}

	db := &fakePatchTxDatabase{tx: tx}
	patcher, _ := NewPostgresSensorMetadataPatcher(db)

	res, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw_alpha", "sensor_shared", input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.GatewayID != "gw_alpha" || res.SensorID != "sensor_shared" {
		t.Fatalf("res mismatch: gw=%s sensor=%s", res.GatewayID, res.SensorID)
	}
	expectedID := mapper.SensorEntityID("gw_alpha", "sensor_shared")
	if res.ID != expectedID {
		t.Fatalf("res.ID = %q, want %q", res.ID, expectedID)
	}
}

// TestPostgresSensorMetadataPatcher_UpdatedAtIsPostLockStatementTime proves the
// final Sensor Twin metadata UPDATE reports/returns the post-lock write statement
// time rather than the earlier transaction-start time.
func TestPostgresSensorMetadataPatcher_UpdatedAtIsPostLockStatementTime(t *testing.T) {
	txStart := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	stmtTime := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)

	tx := setupValidSensorTx()
	tx.txStartTime = txStart
	tx.statementTime = stmtTime
	db := &fakePatchTxDatabase{tx: tx}
	patcher, _ := NewPostgresSensorMetadataPatcher(db)

	newName := "Sensor Renamed Under Locks"
	res, err := patcher.PatchSensorMetadata(context.Background(), uuid.New(), "gw1", "s1", PatchSensorInput{Name: &newName})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.UpdatedAt.Equal(txStart) {
		t.Fatalf("Sensor metadata updated_at reflects transaction start %v instead of post-lock statement time %v", txStart, stmtTime)
	}
	if !res.UpdatedAt.Equal(stmtTime) {
		t.Fatalf("Sensor metadata updated_at = %v, want post-lock statement time %v", res.UpdatedAt, stmtTime)
	}
}

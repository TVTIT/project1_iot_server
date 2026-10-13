package mqttcredential

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

type mockClassifierRepo struct {
	hasUnfinishedFunc    func(ctx context.Context) (bool, error)
	listUnresolvedFunc   func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error)
	getMetadataFunc      func(ctx context.Context, gatewayID string) (Metadata, error)
	requireRecoveryFunc  func(ctx context.Context, u OperationUpdate) (Metadata, error)
	resolveAmbiguityFunc func(ctx context.Context, opID uuid.UUID) (Operation, error)

	hasUnfinishedCalls   int
	requireRecoveryCalls []OperationUpdate
}

func (m *mockClassifierRepo) HasUnfinishedLifecycleAuthority(ctx context.Context) (bool, error) {
	m.hasUnfinishedCalls++
	if m.hasUnfinishedFunc != nil {
		return m.hasUnfinishedFunc(ctx)
	}
	return false, nil
}

func (m *mockClassifierRepo) ListUnresolved(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
	if m.listUnresolvedFunc != nil {
		return m.listUnresolvedFunc(ctx, cursor, limit)
	}
	return []Operation{}, nil
}

func (m *mockClassifierRepo) GetMetadata(ctx context.Context, gatewayID string) (Metadata, error) {
	if m.getMetadataFunc != nil {
		return m.getMetadataFunc(ctx, gatewayID)
	}
	return Metadata{}, nil
}

func (m *mockClassifierRepo) RequireRecovery(ctx context.Context, u OperationUpdate) (Metadata, error) {
	m.requireRecoveryCalls = append(m.requireRecoveryCalls, u)
	if m.requireRecoveryFunc != nil {
		return m.requireRecoveryFunc(ctx, u)
	}
	return Metadata{}, nil
}

func (m *mockClassifierRepo) ResolveCommitAmbiguity(ctx context.Context, opID uuid.UUID) (Operation, error) {
	if m.resolveAmbiguityFunc != nil {
		return m.resolveAmbiguityFunc(ctx, opID)
	}
	return Operation{}, nil
}

func validClassifierOptions() SimplifiedStartupClassifierOptions {
	return SimplifiedStartupClassifierOptions{
		Timeout:  5 * time.Second,
		PageSize: 10,
		MaxPages: 5,
	}
}

// 1. Invalid options / dependencies
func TestSimplifiedStartupClassifier_InvalidOptions(t *testing.T) {
	repo := &mockClassifierRepo{}
	opts := validClassifierOptions()

	tests := []struct {
		name string
		r    SimplifiedStartupRepository
		opts SimplifiedStartupClassifierOptions
	}{
		{"nil repo", nil, opts},
		{"zero timeout", repo, SimplifiedStartupClassifierOptions{Timeout: 0, PageSize: 10, MaxPages: 5}},
		{"excessive timeout", repo, SimplifiedStartupClassifierOptions{Timeout: 11 * time.Minute, PageSize: 10, MaxPages: 5}},
		{"negative timeout", repo, SimplifiedStartupClassifierOptions{Timeout: -1, PageSize: 10, MaxPages: 5}},
		{"zero page size", repo, SimplifiedStartupClassifierOptions{Timeout: 5 * time.Second, PageSize: 0, MaxPages: 5}},
		{"too large page size", repo, SimplifiedStartupClassifierOptions{Timeout: 5 * time.Second, PageSize: 1025, MaxPages: 5}},
		{"zero max pages", repo, SimplifiedStartupClassifierOptions{Timeout: 5 * time.Second, PageSize: 10, MaxPages: 0}},
		{"too large max pages", repo, SimplifiedStartupClassifierOptions{Timeout: 5 * time.Second, PageSize: 10, MaxPages: 1025}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewSimplifiedStartupClassifier(tc.r, tc.opts)
			if err == nil || c != nil {
				t.Fatalf("expected error and nil classifier, got c=%v err=%v", c, err)
			}
		})
	}
}

// 2. Empty scan -> Completed with zero targets
func TestSimplifiedStartupClassifier_EmptyScan(t *testing.T) {
	repo := &mockClassifierRepo{
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			return []Operation{}, nil
		},
	}
	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}
	res, err := c.Classify(context.Background())
	if err != nil {
		t.Fatalf("unexpected Classify err: %v", err)
	}
	if res.Status != ClassificationCompleted {
		t.Fatalf("expected completed status, got %v", res.Status)
	}
	if len(res.RecoveryRequiredTargets) != 0 {
		t.Fatalf("expected 0 recovery required targets, got %d", len(res.RecoveryRequiredTargets))
	}
	if len(repo.requireRecoveryCalls) != 0 {
		t.Fatalf("expected 0 calls to RequireRecovery, got %d", len(repo.requireRecoveryCalls))
	}
}

// 3. Pending simplified -> RequireRecovery exactly once with exact guarded identity/evidence
func TestSimplifiedStartupClassifier_PendingSimplified_RequiresRecovery(t *testing.T) {
	opID := uuid.New()
	actorID := uuid.New()
	idemKey := uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)
	gatewayID := "gw-001"

	pendingOp := Operation{
		OperationID:       opID,
		ActorUserID:       actorID,
		GatewayID:         gatewayID,
		IdempotencyKey:    idemKey,
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		Evidence:          VerificationEvidence{},
		CreatedAt:         now.Add(-time.Minute),
		UpdatedAt:         now.Add(-time.Minute),
	}

	meta := Metadata{
		GatewayID:         gatewayID,
		Username:          gatewayID,
		CredentialVersion: 1,
		Status:            CredentialProvisioning,
		LastOperationID:   opID,
		OperationStatus:   OperationPending,
	}

	repo := &mockClassifierRepo{}
	repo.listUnresolvedFunc = func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
		if cursor == uuid.Nil {
			return []Operation{pendingOp}, nil
		}
		return []Operation{}, nil
	}
	repo.getMetadataFunc = func(ctx context.Context, gid string) (Metadata, error) {
		if gid == gatewayID {
			return meta, nil
		}
		return Metadata{}, &DomainError{Code: CodeNotFound}
	}
	repo.requireRecoveryFunc = func(ctx context.Context, u OperationUpdate) (Metadata, error) {
		return Metadata{
			GatewayID:         gatewayID,
			Username:          gatewayID,
			CredentialVersion: u.Completion.Credential.CredentialVersion,
			Status:            CredentialRecoveryNeeded,
			LastOperationID:   opID,
			OperationStatus:   OperationRecoveryNeeded,
			ActivatedAt:       u.Completion.Credential.ActivatedAt,
			RevokedAt:         u.Completion.Credential.RevokedAt,
		}, nil
	}
	repo.resolveAmbiguityFunc = func(ctx context.Context, id uuid.UUID) (Operation, error) {
		if id == opID {
			if len(repo.requireRecoveryCalls) > 0 {
				return repo.requireRecoveryCalls[len(repo.requireRecoveryCalls)-1].Completion.Operation, nil
			}
			return pendingOp, nil
		}
		return Operation{}, &DomainError{Code: CodeNotFound}
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(context.Background())
	if err != nil {
		t.Fatalf("unexpected Classify err: %v", err)
	}

	if res.Status != ClassificationCompleted {
		t.Fatalf("expected ClassificationCompleted, got %v", res.Status)
	}
	if len(res.RecoveryRequiredTargets) != 1 {
		t.Fatalf("expected 1 recovery target, got %d", len(res.RecoveryRequiredTargets))
	}
	if res.RecoveryRequiredTargets[0].GatewayID != gatewayID || res.RecoveryRequiredTargets[0].OperationID != opID {
		t.Fatalf("target mismatch: got %v", res.RecoveryRequiredTargets[0])
	}
	if len(repo.requireRecoveryCalls) != 1 {
		t.Fatalf("expected exactly 1 RequireRecovery call, got %d", len(repo.requireRecoveryCalls))
	}

	call := repo.requireRecoveryCalls[0]
	// Verify guard
	expectedGuard := OperationGuard{
		GatewayID:                 gatewayID,
		ExpectedOperationID:       opID,
		ExpectedOperationStatus:   OperationPending,
		ExpectedCredentialStatus:  CredentialProvisioning,
		ExpectedCredentialVersion: 1,
	}
	if call.Guard != expectedGuard {
		t.Fatalf("guard mismatch: expected %+v, got %+v", expectedGuard, call.Guard)
	}
	// Verify completion: must be created via CompleteOperation with ExecutionRecoveryRequired
	if call.Completion.Operation.Status != OperationRecoveryNeeded {
		t.Fatalf("expected OperationRecoveryNeeded, got %v", call.Completion.Operation.Status)
	}
	if call.Completion.Credential.Status != CredentialRecoveryNeeded {
		t.Fatalf("expected CredentialRecoveryNeeded, got %v", call.Completion.Credential.Status)
	}
	// Verify no fabricated evidence: evidence was empty, must remain empty
	if call.Completion.Operation.Evidence != (VerificationEvidence{}) {
		t.Fatalf("expected empty evidence, got %+v", call.Completion.Operation.Evidence)
	}
}

// 4. Pending simplified with partial existing evidence (e.g. RAMApplied already true): preserved, never fabricated
func TestSimplifiedStartupClassifier_PendingSimplified_PreservesExistingEvidence(t *testing.T) {
	opID := uuid.New()
	actorID := uuid.New()
	idemKey := uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)
	gatewayID := "gw-002"

	pendingOp := Operation{
		OperationID:       opID,
		ActorUserID:       actorID,
		GatewayID:         gatewayID,
		IdempotencyKey:    idemKey,
		Action:            ActionRotate,
		CredentialVersion: 2,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		Evidence:          VerificationEvidence{RAMApplied: true}, // already had RAMApplied
		Previous: &CredentialSnapshot{
			Status:            CredentialActive,
			CredentialVersion: 1,
		},
		CreatedAt: now.Add(-time.Minute),
		UpdatedAt: now.Add(-time.Minute),
	}

	meta := Metadata{
		GatewayID:         gatewayID,
		Username:          gatewayID,
		CredentialVersion: 2,
		Status:            CredentialRotating,
		LastOperationID:   opID,
		OperationStatus:   OperationPending,
	}

	repo := &mockClassifierRepo{}
	repo.listUnresolvedFunc = func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
		if cursor == uuid.Nil {
			return []Operation{pendingOp}, nil
		}
		return []Operation{}, nil
	}
	repo.getMetadataFunc = func(ctx context.Context, gid string) (Metadata, error) {
		return meta, nil
	}
	repo.resolveAmbiguityFunc = func(ctx context.Context, id uuid.UUID) (Operation, error) {
		if len(repo.requireRecoveryCalls) > 0 {
			return repo.requireRecoveryCalls[len(repo.requireRecoveryCalls)-1].Completion.Operation, nil
		}
		return pendingOp, nil
	}
	repo.requireRecoveryFunc = func(ctx context.Context, u OperationUpdate) (Metadata, error) {
		return Metadata{
			GatewayID:         gatewayID,
			Username:          gatewayID,
			CredentialVersion: u.Completion.Credential.CredentialVersion,
			Status:            CredentialRecoveryNeeded,
			LastOperationID:   opID,
			OperationStatus:   OperationRecoveryNeeded,
			ActivatedAt:       u.Completion.Credential.ActivatedAt,
			RevokedAt:         u.Completion.Credential.RevokedAt,
		}, nil
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(context.Background())
	if err != nil {
		t.Fatalf("unexpected Classify err: %v", err)
	}

	if res.Status != ClassificationCompleted {
		t.Fatalf("expected ClassificationCompleted, got %v", res.Status)
	}
	if len(repo.requireRecoveryCalls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(repo.requireRecoveryCalls))
	}
	ev := repo.requireRecoveryCalls[0].Completion.Operation.Evidence
	if !ev.RAMApplied || ev.SnapshotObserved || ev.FreshPositiveVerified {
		t.Fatalf("evidence should preserve RAMApplied=true without fabricating others, got %+v", ev)
	}
}

// 5. Already recovery-needed -> zero writes, target recorded in result
func TestSimplifiedStartupClassifier_AlreadyRecoveryNeeded_ZeroWrites(t *testing.T) {
	opID := uuid.New()
	actorID := uuid.New()
	idemKey := uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)
	gatewayID := "gw-003"

	recOp := Operation{
		OperationID:       opID,
		ActorUserID:       actorID,
		GatewayID:         gatewayID,
		IdempotencyKey:    idemKey,
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationRecoveryNeeded,
		Phase:             PhaseRecovery,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		Evidence:          VerificationEvidence{},
		CreatedAt:         now.Add(-time.Minute),
		UpdatedAt:         now.Add(-time.Minute),
	}

	meta := Metadata{
		GatewayID:         gatewayID,
		Username:          gatewayID,
		CredentialVersion: 1,
		Status:            CredentialRecoveryNeeded,
		LastOperationID:   opID,
		OperationStatus:   OperationRecoveryNeeded,
	}

	repo := &mockClassifierRepo{
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			if cursor == uuid.Nil {
				return []Operation{recOp}, nil
			}
			return []Operation{}, nil
		},
		getMetadataFunc: func(ctx context.Context, gid string) (Metadata, error) {
			return meta, nil
		},
		resolveAmbiguityFunc: func(ctx context.Context, id uuid.UUID) (Operation, error) {
			return recOp, nil
		},
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(context.Background())
	if err != nil {
		t.Fatalf("unexpected Classify err: %v", err)
	}

	if res.Status != ClassificationCompleted {
		t.Fatalf("expected completed status, got %v", res.Status)
	}
	if len(res.RecoveryRequiredTargets) != 1 {
		t.Fatalf("expected 1 recovery target, got %d", len(res.RecoveryRequiredTargets))
	}
	if res.RecoveryRequiredTargets[0].GatewayID != gatewayID || res.RecoveryRequiredTargets[0].OperationID != opID {
		t.Fatalf("target mismatch: got %v", res.RecoveryRequiredTargets[0])
	}
	if len(repo.requireRecoveryCalls) != 0 {
		t.Fatalf("expected zero RequireRecovery calls for already recovery-needed op, got %d", len(repo.requireRecoveryCalls))
	}
}

// 6. Stale / mismatched target -> no write, returns incomplete/unavailable
func TestSimplifiedStartupClassifier_MismatchedTarget_ReturnsIncomplete(t *testing.T) {
	opID := uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)
	gatewayID := "gw-004"

	pendingOp := Operation{
		OperationID:       opID,
		ActorUserID:       uuid.New(),
		GatewayID:         gatewayID,
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         now.Add(-time.Minute),
		UpdatedAt:         now.Add(-time.Minute),
	}

	// Metadata points to different last_operation_id!
	staleMeta := Metadata{
		GatewayID:         gatewayID,
		Username:          gatewayID,
		CredentialVersion: 2,
		Status:            CredentialActive,
		LastOperationID:   uuid.New(), // mismatch
		OperationStatus:   OperationSucceeded,
	}

	repo := &mockClassifierRepo{
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			if cursor == uuid.Nil {
				return []Operation{pendingOp}, nil
			}
			return []Operation{}, nil
		},
		getMetadataFunc: func(ctx context.Context, gid string) (Metadata, error) {
			return staleMeta, nil
		},
		resolveAmbiguityFunc: func(ctx context.Context, id uuid.UUID) (Operation, error) {
			return pendingOp, nil
		},
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(context.Background())
	if err == nil {
		t.Fatal("expected error on mismatched metadata")
	}
	if res.Status != ClassificationIncomplete {
		t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
	}
	if len(repo.requireRecoveryCalls) != 0 {
		t.Fatalf("expected 0 writes, got %d", len(repo.requireRecoveryCalls))
	}
}

// 7. Legacy unresolved operation -> explicit blocker / unavailable
func TestSimplifiedStartupClassifier_LegacyUnresolved_ExplicitBlocker(t *testing.T) {
	opID := uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)
	gatewayID := "gw-005"

	legacyOp := Operation{
		OperationID:       opID,
		ActorUserID:       uuid.New(),
		GatewayID:         gatewayID,
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractLifecycle, // NOT simplified!
		CreatedAt:         now.Add(-time.Minute),
		UpdatedAt:         now.Add(-time.Minute),
	}

	repo := &mockClassifierRepo{
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			if cursor == uuid.Nil {
				return []Operation{legacyOp}, nil
			}
			return []Operation{}, nil
		},
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(context.Background())
	if err == nil {
		t.Fatal("expected error on legacy unresolved operation")
	}
	if res.Status != ClassificationIncomplete {
		t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
	}
	if !errors.Is(err, ErrLegacyContractBlocked) {
		t.Fatalf("expected ErrLegacyContractBlocked, got %v", err)
	}
	if len(repo.requireRecoveryCalls) != 0 {
		t.Fatalf("expected 0 writes, got %d", len(repo.requireRecoveryCalls))
	}
}

// 7b. HasUnfinishedLifecycleAuthority returns true (e.g. terminal lifecycle with unfinished checkpoint) -> ClassificationIncomplete + ErrLegacyContractBlocked, zero list/conversion calls
func TestSimplifiedStartupClassifier_UnfinishedLifecycleAuthority_BlocksStartup(t *testing.T) {
	listCalled := false
	repo := &mockClassifierRepo{
		hasUnfinishedFunc: func(ctx context.Context) (bool, error) {
			return true, nil
		},
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			listCalled = true
			return []Operation{}, nil
		},
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(context.Background())
	if err == nil {
		t.Fatal("expected error on unfinished lifecycle authority")
	}
	if res.Status != ClassificationIncomplete {
		t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
	}
	if !errors.Is(err, ErrLegacyContractBlocked) {
		t.Fatalf("expected ErrLegacyContractBlocked, got %v", err)
	}
	if listCalled {
		t.Fatal("expected zero ListUnresolved calls when lifecycle authority is unfinished")
	}
	if len(repo.requireRecoveryCalls) != 0 {
		t.Fatalf("expected 0 writes, got %d", len(repo.requireRecoveryCalls))
	}
}

// 7c. HasUnfinishedLifecycleAuthority returns read error -> ClassificationIncomplete, zero conversion calls
func TestSimplifiedStartupClassifier_UnfinishedLifecycleAuthority_ReadError(t *testing.T) {
	dbErr := errors.New("read error on unfinished check")
	listCalled := false
	repo := &mockClassifierRepo{
		hasUnfinishedFunc: func(ctx context.Context) (bool, error) {
			return false, dbErr
		},
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			listCalled = true
			return []Operation{}, nil
		},
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(context.Background())
	if err == nil {
		t.Fatal("expected error on guard read failure")
	}
	if res.Status != ClassificationIncomplete {
		t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
	}
	if !errors.Is(err, dbErr) {
		t.Fatalf("expected dbErr, got %v", err)
	}
	if listCalled {
		t.Fatal("expected zero ListUnresolved calls when guard check fails")
	}
	if len(repo.requireRecoveryCalls) != 0 {
		t.Fatalf("expected 0 writes, got %d", len(repo.requireRecoveryCalls))
	}
}

// 8. DB failure / read error -> incomplete/unavailable, no write
func TestSimplifiedStartupClassifier_DBReadError(t *testing.T) {
	dbErr := errors.New("connection reset")
	repo := &mockClassifierRepo{
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			return nil, dbErr
		},
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(context.Background())
	if err == nil {
		t.Fatal("expected error on db failure")
	}
	if res.Status != ClassificationIncomplete {
		t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
	}
	if len(repo.requireRecoveryCalls) != 0 {
		t.Fatalf("expected 0 writes, got %d", len(repo.requireRecoveryCalls))
	}
}

// 9. Update / RequireRecovery error -> incomplete/unavailable
func TestSimplifiedStartupClassifier_RequireRecoveryError(t *testing.T) {
	opID := uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)
	gatewayID := "gw-006"

	pendingOp := Operation{
		OperationID:       opID,
		ActorUserID:       uuid.New(),
		GatewayID:         gatewayID,
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         now.Add(-time.Minute),
		UpdatedAt:         now.Add(-time.Minute),
	}

	meta := Metadata{
		GatewayID:         gatewayID,
		Username:          gatewayID,
		CredentialVersion: 1,
		Status:            CredentialProvisioning,
		LastOperationID:   opID,
		OperationStatus:   OperationPending,
	}

	writeErr := errors.New("deadlock / commit failed")
	repo := &mockClassifierRepo{
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			if cursor == uuid.Nil {
				return []Operation{pendingOp}, nil
			}
			return []Operation{}, nil
		},
		getMetadataFunc: func(ctx context.Context, gid string) (Metadata, error) {
			return meta, nil
		},
		resolveAmbiguityFunc: func(ctx context.Context, id uuid.UUID) (Operation, error) {
			return pendingOp, nil
		},
		requireRecoveryFunc: func(ctx context.Context, u OperationUpdate) (Metadata, error) {
			return Metadata{}, writeErr
		},
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(context.Background())
	if err == nil {
		t.Fatal("expected error on RequireRecovery failure")
	}
	if res.Status != ClassificationIncomplete {
		t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
	}
}

// 10. Malformed page: unordered cursor or repeated cursor -> bounded failure
func TestSimplifiedStartupClassifier_MalformedPageOrder(t *testing.T) {
	op1 := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	op2 := uuid.MustParse("00000000-0000-0000-0000-000000000001") // out of order!
	now := time.Now().UTC()

	repo := &mockClassifierRepo{
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			return []Operation{
				{OperationID: op1, ExecutionContract: ContractSimplified, Status: OperationPending, CreatedAt: now, UpdatedAt: now},
				{OperationID: op2, ExecutionContract: ContractSimplified, Status: OperationPending, CreatedAt: now, UpdatedAt: now},
			}, nil
		},
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(context.Background())
	if err == nil {
		t.Fatal("expected error on out-of-order operations")
	}
	if res.Status != ClassificationIncomplete {
		t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
	}
}

// 11. Repeated cursor / duplicate opID in page -> bounded failure
func TestSimplifiedStartupClassifier_RepeatedCursor(t *testing.T) {
	op1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	now := time.Now().UTC()

	repo := &mockClassifierRepo{
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			return []Operation{
				{OperationID: op1, ExecutionContract: ContractSimplified, Status: OperationPending, CreatedAt: now, UpdatedAt: now},
				{OperationID: op1, ExecutionContract: ContractSimplified, Status: OperationPending, CreatedAt: now, UpdatedAt: now},
			}, nil
		},
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(context.Background())
	if err == nil {
		t.Fatal("expected error on duplicate opID")
	}
	if res.Status != ClassificationIncomplete {
		t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
	}
}

// 12. Max-page exhaustion not false success
func TestSimplifiedStartupClassifier_MaxPageExhaustion(t *testing.T) {
	now := time.Now().UTC()
	// Return a full page on every call
	var allOps []Operation
	repo := &mockClassifierRepo{
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			ops := make([]Operation, limit)
			for i := 0; i < limit; i++ {
				id := uuid.New()
				// Ensure strictly increasing
				for id.String() <= cursor.String() {
					id = uuid.New()
				}
				gid := "gw-" + id.String()[:8]
				ops[i] = Operation{
					OperationID:       id,
					GatewayID:         gid,
					ActorUserID:       uuid.New(),
					IdempotencyKey:    uuid.New(),
					Action:            ActionProvision,
					CredentialVersion: 1,
					Phase:             PhaseRecovery,
					DeliveryStatus:    DeliveryUnknown,
					Status:            OperationRecoveryNeeded,
					ExecutionContract: ContractSimplified,
					CreatedAt:         now,
					UpdatedAt:         now,
				}
				cursor = id
				allOps = append(allOps, ops[i])
			}
			return ops, nil
		},
		getMetadataFunc: func(ctx context.Context, gid string) (Metadata, error) {
			var matchingID uuid.UUID
			for _, o := range allOps {
				if o.GatewayID == gid {
					matchingID = o.OperationID
					break
				}
			}
			return Metadata{
				GatewayID:         gid,
				Username:          gid,
				CredentialVersion: 1,
				Status:            CredentialRecoveryNeeded,
				LastOperationID:   matchingID,
				OperationStatus:   OperationRecoveryNeeded,
			}, nil
		},
		resolveAmbiguityFunc: func(ctx context.Context, id uuid.UUID) (Operation, error) {
			for _, o := range allOps {
				if o.OperationID == id {
					return o, nil
				}
			}
			return Operation{}, &DomainError{Code: CodeNotFound}
		},
	}

	opts := validClassifierOptions()
	opts.PageSize = 2
	opts.MaxPages = 2

	c, err := NewSimplifiedStartupClassifier(repo, opts)
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(context.Background())
	if err == nil {
		t.Fatal("expected error on max page budget exhaustion")
	}
	if res.Status != ClassificationIncomplete {
		t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
	}
	if !errors.Is(err, ErrScanBudgetExhausted) {
		t.Fatalf("expected ErrScanBudgetExhausted, got %v", err)
	}
}

// 13. Context cancellation -> returns incomplete with context error
func TestSimplifiedStartupClassifier_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled

	repo := &mockClassifierRepo{
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			return nil, ctx.Err()
		},
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(ctx)
	if err == nil {
		t.Fatal("expected error on cancelled context")
	}
	if res.Status != ClassificationIncomplete {
		t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
	}
}

// 14. Paging works across multiple pages without exceeding max pages
func TestSimplifiedStartupClassifier_MultiPageSuccess(t *testing.T) {
	opID1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	opID2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	now := time.Now().UTC()

	op1 := Operation{
		OperationID:       opID1,
		GatewayID:         "gw-1",
		ActorUserID:       uuid.New(),
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationRecoveryNeeded,
		Phase:             PhaseRecovery,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	op2 := Operation{
		OperationID:       opID2,
		GatewayID:         "gw-2",
		ActorUserID:       uuid.New(),
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationRecoveryNeeded,
		Phase:             PhaseRecovery,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	repo := &mockClassifierRepo{
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			if cursor == uuid.Nil {
				return []Operation{op1}, nil
			}
			if cursor == opID1 {
				return []Operation{op2}, nil
			}
			return []Operation{}, nil
		},
		getMetadataFunc: func(ctx context.Context, gid string) (Metadata, error) {
			id := opID1
			if gid == "gw-2" {
				id = opID2
			}
			return Metadata{
				GatewayID:         gid,
				Username:          gid,
				CredentialVersion: 1,
				Status:            CredentialRecoveryNeeded,
				LastOperationID:   id,
				OperationStatus:   OperationRecoveryNeeded,
			}, nil
		},
		resolveAmbiguityFunc: func(ctx context.Context, id uuid.UUID) (Operation, error) {
			if id == opID1 {
				return op1, nil
			}
			return op2, nil
		},
	}

	opts := validClassifierOptions()
	opts.PageSize = 1 // 1 per page -> will take 2 full pages + 1 empty page (3 pages)
	opts.MaxPages = 5

	c, err := NewSimplifiedStartupClassifier(repo, opts)
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(context.Background())
	if err != nil {
		t.Fatalf("unexpected Classify err: %v", err)
	}
	if res.Status != ClassificationCompleted {
		t.Fatalf("expected completed, got %v", res.Status)
	}
	if len(res.RecoveryRequiredTargets) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(res.RecoveryRequiredTargets))
	}
}

// 15. Disallowed or mismatched contract in ResolveCommitAmbiguity
func TestSimplifiedStartupClassifier_AmbiguityResolvesMismatch(t *testing.T) {
	opID := uuid.New()
	now := time.Now().UTC()
	gatewayID := "gw-007"

	pendingOp := Operation{
		OperationID:       opID,
		ActorUserID:       uuid.New(),
		GatewayID:         gatewayID,
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         now.Add(-time.Minute),
		UpdatedAt:         now.Add(-time.Minute),
	}

	meta := Metadata{
		GatewayID:         gatewayID,
		Username:          gatewayID,
		CredentialVersion: 1,
		Status:            CredentialProvisioning,
		LastOperationID:   opID,
		OperationStatus:   OperationPending,
	}

	// ResolveCommitAmbiguity returns an operation with modified ActorUserID (corrupt/mismatched)
	corruptOp := pendingOp
	corruptOp.ActorUserID = uuid.New()

	repo := &mockClassifierRepo{
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			if cursor == uuid.Nil {
				return []Operation{pendingOp}, nil
			}
			return []Operation{}, nil
		},
		getMetadataFunc: func(ctx context.Context, gid string) (Metadata, error) {
			return meta, nil
		},
		resolveAmbiguityFunc: func(ctx context.Context, id uuid.UUID) (Operation, error) {
			return corruptOp, nil
		},
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}

	res, err := c.Classify(context.Background())
	if err == nil {
		t.Fatal("expected error on mismatched ambiguity resolution")
	}
	if res.Status != ClassificationIncomplete {
		t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
	}
	if len(repo.requireRecoveryCalls) != 0 {
		t.Fatalf("expected 0 writes, got %d", len(repo.requireRecoveryCalls))
	}
}

// 16. Regression: Already recovery-needed with mismatched currentOp from ResolveCommitAmbiguity
func TestSimplifiedStartupClassifier_AlreadyRecoveryNeeded_AmbiguityMismatch_Fails(t *testing.T) {
	opID := uuid.New()
	gatewayID := "gw-corrupt-rec"
	now := time.Now().UTC()
	recOp := Operation{
		OperationID:       opID,
		GatewayID:         gatewayID,
		ActorUserID:       uuid.New(),
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationRecoveryNeeded,
		Phase:             PhaseRecovery,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	meta := Metadata{
		GatewayID:         gatewayID,
		Username:          gatewayID,
		CredentialVersion: 1,
		Status:            CredentialRecoveryNeeded,
		LastOperationID:   opID,
		OperationStatus:   OperationRecoveryNeeded,
	}
	// ResolveCommitAmbiguity returns an operation with a different GatewayID
	mismatchedOp := recOp
	mismatchedOp.GatewayID = "gw-other"

	repo := &mockClassifierRepo{
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			if cursor == uuid.Nil {
				return []Operation{recOp}, nil
			}
			return []Operation{}, nil
		},
		getMetadataFunc: func(ctx context.Context, gid string) (Metadata, error) {
			return meta, nil
		},
		resolveAmbiguityFunc: func(ctx context.Context, id uuid.UUID) (Operation, error) {
			return mismatchedOp, nil
		},
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}
	res, err := c.Classify(context.Background())
	if err == nil {
		t.Fatal("expected error on mismatched resolved operation for already recovery-needed target")
	}
	if res.Status != ClassificationIncomplete {
		t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
	}
}

// 17. Regression: RequireRecovery returns mismatched metadata -> ClassificationIncomplete
func TestSimplifiedStartupClassifier_RequireRecovery_ReturnedWrongMetadata_Fails(t *testing.T) {
	opID := uuid.New()
	gatewayID := "gw-wrong-meta"
	now := time.Now().UTC()
	pendingOp := Operation{
		OperationID:       opID,
		GatewayID:         gatewayID,
		ActorUserID:       uuid.New(),
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	meta := Metadata{
		GatewayID:         gatewayID,
		Username:          gatewayID,
		CredentialVersion: 1,
		Status:            CredentialProvisioning,
		LastOperationID:   opID,
		OperationStatus:   OperationPending,
	}
	// RequireRecovery returns wrong version or status
	corruptReturnedMeta := meta
	corruptReturnedMeta.CredentialVersion = 999

	repo := &mockClassifierRepo{
		listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			if cursor == uuid.Nil {
				return []Operation{pendingOp}, nil
			}
			return []Operation{}, nil
		},
		getMetadataFunc: func(ctx context.Context, gid string) (Metadata, error) {
			return meta, nil
		},
		resolveAmbiguityFunc: func(ctx context.Context, id uuid.UUID) (Operation, error) {
			return pendingOp, nil
		},
		requireRecoveryFunc: func(ctx context.Context, u OperationUpdate) (Metadata, error) {
			return corruptReturnedMeta, nil
		},
	}

	c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
	if err != nil {
		t.Fatalf("unexpected New err: %v", err)
	}
	res, err := c.Classify(context.Background())
	if err == nil {
		t.Fatal("expected error when RequireRecovery returns corrupted metadata")
	}
	if res.Status != ClassificationIncomplete {
		t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
	}
}

// 19. Regression: Readback pending / mismatch / read error after RequireRecovery -> ClassificationIncomplete
func TestSimplifiedStartupClassifier_RequireRecovery_ReadbackFailure(t *testing.T) {
	opID := uuid.New()
	gatewayID := "gw-readback-fail"
	now := time.Now().UTC()
	pendingOp := Operation{
		OperationID:       opID,
		GatewayID:         gatewayID,
		ActorUserID:       uuid.New(),
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	meta := Metadata{
		GatewayID:         gatewayID,
		Username:          gatewayID,
		CredentialVersion: 1,
		Status:            CredentialProvisioning,
		LastOperationID:   opID,
		OperationStatus:   OperationPending,
	}

	t.Run("ReadbackError", func(t *testing.T) {
		repo := &mockClassifierRepo{}
		repo.listUnresolvedFunc = func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			if cursor == uuid.Nil {
				return []Operation{pendingOp}, nil
			}
			return []Operation{}, nil
		}
		repo.getMetadataFunc = func(ctx context.Context, gid string) (Metadata, error) {
			return meta, nil
		}
		repo.requireRecoveryFunc = func(ctx context.Context, u OperationUpdate) (Metadata, error) {
			return Metadata{
				GatewayID:         gatewayID,
				Username:          gatewayID,
				CredentialVersion: u.Completion.Credential.CredentialVersion,
				Status:            CredentialRecoveryNeeded,
				LastOperationID:   opID,
				OperationStatus:   OperationRecoveryNeeded,
				ActivatedAt:       u.Completion.Credential.ActivatedAt,
				RevokedAt:         u.Completion.Credential.RevokedAt,
			}, nil
		}
		readbackCalls := 0
		repo.resolveAmbiguityFunc = func(ctx context.Context, id uuid.UUID) (Operation, error) {
			readbackCalls++
			if readbackCalls == 1 {
				return pendingOp, nil
			}
			return Operation{}, errors.New("readback commit ambiguity failed")
		}

		c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
		if err != nil {
			t.Fatalf("unexpected New err: %v", err)
		}
		res, err := c.Classify(context.Background())
		if err == nil {
			t.Fatal("expected error on readback failure")
		}
		if res.Status != ClassificationIncomplete {
			t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
		}
	})

	t.Run("ReadbackMismatchStillPending", func(t *testing.T) {
		repo := &mockClassifierRepo{}
		repo.listUnresolvedFunc = func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
			if cursor == uuid.Nil {
				return []Operation{pendingOp}, nil
			}
			return []Operation{}, nil
		}
		repo.getMetadataFunc = func(ctx context.Context, gid string) (Metadata, error) {
			return meta, nil
		}
		repo.requireRecoveryFunc = func(ctx context.Context, u OperationUpdate) (Metadata, error) {
			return Metadata{
				GatewayID:         gatewayID,
				Username:          gatewayID,
				CredentialVersion: u.Completion.Credential.CredentialVersion,
				Status:            CredentialRecoveryNeeded,
				LastOperationID:   opID,
				OperationStatus:   OperationRecoveryNeeded,
				ActivatedAt:       u.Completion.Credential.ActivatedAt,
				RevokedAt:         u.Completion.Credential.RevokedAt,
			}, nil
		}
		// Returns pendingOp even after RequireRecovery (commit did not stick / mismatch)
		repo.resolveAmbiguityFunc = func(ctx context.Context, id uuid.UUID) (Operation, error) {
			return pendingOp, nil
		}

		c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
		if err != nil {
			t.Fatalf("unexpected New err: %v", err)
		}
		res, err := c.Classify(context.Background())
		if err == nil {
			t.Fatal("expected error on readback mismatch (still pending)")
		}
		if res.Status != ClassificationIncomplete {
			t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
		}
	})
}

// 20. Regression: Corrupt recovery Gateway / username / status / version / action
func TestSimplifiedStartupClassifier_CorruptMetadataAndResolvedOp(t *testing.T) {
	opID := uuid.New()
	gatewayID := "gw-corrupt-checks"
	now := time.Now().UTC()
	pendingOp := Operation{
		OperationID:       opID,
		GatewayID:         gatewayID,
		ActorUserID:       uuid.New(),
		IdempotencyKey:    uuid.New(),
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	tests := []struct {
		name       string
		meta       Metadata
		resolvedOp Operation
	}{
		{
			name: "MismatchedUsername",
			meta: Metadata{
				GatewayID:         gatewayID,
				Username:          "wrong-user",
				CredentialVersion: 1,
				Status:            CredentialProvisioning,
				LastOperationID:   opID,
				OperationStatus:   OperationPending,
			},
			resolvedOp: pendingOp,
		},
		{
			name: "CorruptVersionZero",
			meta: Metadata{
				GatewayID:         gatewayID,
				Username:          gatewayID,
				CredentialVersion: 0,
				Status:            CredentialProvisioning,
				LastOperationID:   opID,
				OperationStatus:   OperationPending,
			},
			resolvedOp: pendingOp,
		},
		{
			name: "ResolvedOpCorruptAction",
			meta: Metadata{
				GatewayID:         gatewayID,
				Username:          gatewayID,
				CredentialVersion: 1,
				Status:            CredentialProvisioning,
				LastOperationID:   opID,
				OperationStatus:   OperationPending,
			},
			resolvedOp: func() Operation {
				o := pendingOp
				o.Action = Action("invalid-action")
				return o
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := &mockClassifierRepo{
				listUnresolvedFunc: func(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
					if cursor == uuid.Nil {
						return []Operation{pendingOp}, nil
					}
					return []Operation{}, nil
				},
				getMetadataFunc: func(ctx context.Context, gid string) (Metadata, error) {
					return tc.meta, nil
				},
				resolveAmbiguityFunc: func(ctx context.Context, id uuid.UUID) (Operation, error) {
					return tc.resolvedOp, nil
				},
			}

			c, err := NewSimplifiedStartupClassifier(repo, validClassifierOptions())
			if err != nil {
				t.Fatalf("unexpected New err: %v", err)
			}
			res, err := c.Classify(context.Background())
			if err == nil {
				t.Fatalf("expected error for case %s", tc.name)
			}
			if res.Status != ClassificationIncomplete {
				t.Fatalf("expected ClassificationIncomplete, got %v", res.Status)
			}
		})
	}
}

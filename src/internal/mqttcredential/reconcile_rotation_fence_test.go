package mqttcredential

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fenceSpyRuntime struct {
	mu           sync.Mutex
	closed       bool
	opened       bool
	begun        bool
	disabled     []string
	beginCalls   int
	disableCalls int
	openCalls    int
	drainCalls   int
}

func (f *fenceSpyRuntime) BeginStartup(ctx context.Context) (VerificationReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.begun = true
	f.closed = true
	f.beginCalls++
	ep, _ := freshIdentity()
	nonce, _ := freshIdentity()
	return VerificationReceipt{Epoch: ep, Nonce: nonce}, nil
}

func (f *fenceSpyRuntime) CloseDrain(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	f.opened = false
	f.drainCalls++
	return nil
}

func (f *fenceSpyRuntime) DisableStartup(ctx context.Context, r VerificationReceipt, u string) (RecoveryDisableEvidence, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.disabled = append(f.disabled, u)
	f.disableCalls++
	return RecoveryDisableEvidence{RAMDisabled: true, SnapshotObserved: true, BrokerEpoch: r.Epoch}, nil
}

func (f *fenceSpyRuntime) VerifyStartupInventory(ctx context.Context, r VerificationReceipt, rows []Metadata) error {
	return nil
}

func (f *fenceSpyRuntime) OpenStartup(ctx context.Context, r VerificationReceipt) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = false
	f.opened = true
	f.openCalls++
	return nil
}

type fenceSpyRepo struct {
	Repository
	MaintenanceRepository
	RecoveryRepository

	mu                  sync.Mutex
	unresolved          []Operation
	checkpoints         []MaintenanceCheckpoint
	pendingRecovery     []RecoveryRecord
	inventory           []Metadata
	operationsByID      map[uuid.UUID]Operation
	checkpointsByID     map[uuid.UUID]MaintenanceCheckpoint
	metadataByGW        map[string]Metadata
	resolveOpCalls      int
	resolveCPCalls      int
	getMetadataCalls    int
	beginRecoveryCalls  int
	bindRecoveryCalls   int
	completeRecCalls    int
	listUnresolvedCalls int
	listPendingCPCalls  int
	listPendingRecCalls int
	resolveOpErr        map[uuid.UUID]error
	resolveOpFn         func(ctx context.Context, id uuid.UUID, callCount int) (Operation, error)
}

func newFenceSpyRepo() *fenceSpyRepo {
	return &fenceSpyRepo{
		operationsByID:  make(map[uuid.UUID]Operation),
		checkpointsByID: make(map[uuid.UUID]MaintenanceCheckpoint),
		metadataByGW:    make(map[string]Metadata),
		resolveOpErr:    make(map[uuid.UUID]error),
	}
}

func (r *fenceSpyRepo) ListUnresolved(ctx context.Context, cursor uuid.UUID, limit int) ([]Operation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listUnresolvedCalls++
	var res []Operation
	for _, o := range r.unresolved {
		if cursor == uuid.Nil || o.OperationID.String() > cursor.String() {
			res = append(res, o)
			if len(res) >= limit {
				break
			}
		}
	}
	return res, nil
}

func (r *fenceSpyRepo) ListPendingMaintenance(ctx context.Context, cursor uuid.UUID, limit int) ([]MaintenanceCheckpoint, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listPendingCPCalls++
	var res []MaintenanceCheckpoint
	for _, cp := range r.checkpoints {
		if cursor == uuid.Nil || cp.OperationID.String() > cursor.String() {
			res = append(res, cp)
			if len(res) >= limit {
				break
			}
		}
	}
	return res, nil
}

func (r *fenceSpyRepo) ListPendingRecovery(ctx context.Context, cursor uuid.UUID, limit int) ([]RecoveryRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listPendingRecCalls++
	var res []RecoveryRecord
	for _, rec := range r.pendingRecovery {
		if cursor == uuid.Nil || rec.RecoveryID.String() > cursor.String() {
			res = append(res, rec)
			if len(res) >= limit {
				break
			}
		}
	}
	return res, nil
}

func (r *fenceSpyRepo) ListStartupInventory(ctx context.Context, key string, limit int) ([]Metadata, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var res []Metadata
	for _, m := range r.inventory {
		if m.GatewayID > key {
			res = append(res, m)
			if len(res) >= limit {
				break
			}
		}
	}
	return res, nil
}

func (r *fenceSpyRepo) ResolveCommitAmbiguity(ctx context.Context, id uuid.UUID) (Operation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolveOpCalls++
	if r.resolveOpFn != nil {
		return r.resolveOpFn(ctx, id, r.resolveOpCalls)
	}
	if err, ok := r.resolveOpErr[id]; ok {
		return Operation{}, err
	}
	if op, ok := r.operationsByID[id]; ok {
		return op, nil
	}
	return Operation{}, ErrRecoveryRequired
}

func (r *fenceSpyRepo) ResolveMaintenance(ctx context.Context, id uuid.UUID) (MaintenanceCheckpoint, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolveCPCalls++
	if cp, ok := r.checkpointsByID[id]; ok {
		return cp, nil
	}
	return MaintenanceCheckpoint{}, ErrRecoveryRequired
}

func (r *fenceSpyRepo) GetMetadata(ctx context.Context, gw string) (Metadata, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.getMetadataCalls++
	if m, ok := r.metadataByGW[gw]; ok {
		return m, nil
	}
	return Metadata{}, ErrRecoveryRequired
}

func (r *fenceSpyRepo) BeginRecovery(ctx context.Context, q RecoveryRequest) (RecoveryRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.beginRecoveryCalls++
	rec := RecoveryRecord{
		RecoveryID:  q.RecoveryID,
		OperationID: q.Guard.ExpectedOperationID,
		GatewayID:   q.Guard.GatewayID,
		BrokerEpoch: q.BrokerEpoch,
		Status:      RecoveryPending,
	}
	r.pendingRecovery = append(r.pendingRecovery, rec)
	return rec, nil
}

func (r *fenceSpyRepo) BindRecoveryEpoch(ctx context.Context, id uuid.UUID, old, next string) (RecoveryRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bindRecoveryCalls++
	return RecoveryRecord{RecoveryID: id, BrokerEpoch: next, Status: RecoveryPending}, nil
}

func (r *fenceSpyRepo) CompleteRecovery(ctx context.Context, q RecoveryRequest, proof RecoveryDisableEvidence) (Metadata, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completeRecCalls++
	r.unresolved = nil
	r.checkpoints = nil
	r.pendingRecovery = nil
	m := Metadata{GatewayID: q.Guard.GatewayID, Status: CredentialRevoked, LastOperationID: q.Guard.ExpectedOperationID}
	r.inventory = []Metadata{m}
	r.metadataByGW[q.Guard.GatewayID] = m
	return m, nil
}

func (r *fenceSpyRepo) ResolveRecovery(ctx context.Context, id uuid.UUID) (RecoveryRecord, error) {
	return RecoveryRecord{RecoveryID: id, Status: RecoveryDisabled, Evidence: RecoveryDisableEvidence{RAMDisabled: true, SnapshotObserved: true, BrokerEpoch: "epoch"}}, nil
}

// Unresolved ordinary rotate must block startup and trigger 0 mutations.
func TestStartupRotationFenceUnresolvedRotate(t *testing.T) {
	rotateID := uuid.New()
	repo := newFenceSpyRepo()
	runtime := &fenceSpyRuntime{}

	op := Operation{
		OperationID:    rotateID,
		ActorUserID:    uuid.New(),
		GatewayID:      "gw_rotate",
		IdempotencyKey: uuid.New(),
		Action:         ActionRotate,
		Status:         OperationPending,
		Phase:          PhaseIntent,
	}
	cp := MaintenanceCheckpoint{
		OperationID: rotateID,
		GatewayID:   "gw_rotate",
		Status:      MaintenanceInProgress,
	}
	meta := Metadata{
		GatewayID:       "gw_rotate",
		Status:          CredentialRotating,
		LastOperationID: rotateID,
	}

	repo.unresolved = []Operation{op}
	repo.operationsByID[rotateID] = op
	repo.checkpointsByID[rotateID] = cp
	repo.metadataByGW["gw_rotate"] = meta

	s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
		Timeout:       time.Second,
		PageSize:      10,
		MaxPages:      5,
		VerifyBackend: func(context.Context, VerificationReceipt) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Run(context.Background())
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("expected ErrRecoveryRequired, got: %v", err)
	}
	if s.Ready() {
		t.Fatal("readiness must be false")
	}
	if runtime.opened {
		t.Fatal("runtime must remain closed")
	}
	if runtime.disableCalls != 0 {
		t.Fatalf("DisableStartup called %d times; want 0", runtime.disableCalls)
	}
	if repo.beginRecoveryCalls != 0 {
		t.Fatalf("BeginRecovery called %d times; want 0", repo.beginRecoveryCalls)
	}
	if repo.bindRecoveryCalls != 0 {
		t.Fatalf("BindRecoveryEpoch called %d times; want 0", repo.bindRecoveryCalls)
	}
	if repo.completeRecCalls != 0 {
		t.Fatalf("CompleteRecovery called %d times; want 0", repo.completeRecCalls)
	}
}

// Rotate replacement successor must block startup with 0 mutations.
func TestStartupRotationFenceReplacementSuccessor(t *testing.T) {
	predecessorID := uuid.New()
	successorID := uuid.New()
	repo := newFenceSpyRepo()
	runtime := &fenceSpyRuntime{}

	op := Operation{
		OperationID:              successorID,
		ActorUserID:              uuid.New(),
		GatewayID:                "gw_replacement",
		IdempotencyKey:           uuid.New(),
		Action:                   ActionRotate,
		Status:                   OperationPending,
		Phase:                    PhaseIntent,
		ReplacementPredecessorID: &predecessorID,
	}
	cp := MaintenanceCheckpoint{
		OperationID: successorID,
		GatewayID:   "gw_replacement",
		Status:      MaintenanceInProgress,
	}
	meta := Metadata{
		GatewayID:       "gw_replacement",
		Status:          CredentialRotating,
		LastOperationID: successorID,
	}

	repo.unresolved = []Operation{op}
	repo.operationsByID[successorID] = op
	repo.checkpointsByID[successorID] = cp
	repo.metadataByGW["gw_replacement"] = meta

	s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
		Timeout:       time.Second,
		PageSize:      10,
		MaxPages:      5,
		VerifyBackend: func(context.Context, VerificationReceipt) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Run(context.Background())
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("expected ErrRecoveryRequired, got: %v", err)
	}
	if s.Ready() {
		t.Fatal("readiness must be false")
	}
	if runtime.opened {
		t.Fatal("runtime must remain closed")
	}
	if runtime.disableCalls != 0 || repo.beginRecoveryCalls != 0 || repo.completeRecCalls != 0 {
		t.Fatal("zero mutations expected")
	}
}

// Successful rotate with an incomplete maintenance checkpoint must block startup with 0 mutations.
func TestStartupRotationFenceSucceededRotateIncompleteCheckpoint(t *testing.T) {
	rotateID := uuid.New()
	repo := newFenceSpyRepo()
	runtime := &fenceSpyRuntime{}

	now := time.Now().UTC()
	op := Operation{
		OperationID:    rotateID,
		ActorUserID:    uuid.New(),
		GatewayID:      "gw_succ_rotate",
		IdempotencyKey: uuid.New(),
		Action:         ActionRotate,
		Status:         OperationSucceeded,
		Phase:          PhaseFinalize,
		CompletedAt:    &now,
	}
	cp := MaintenanceCheckpoint{
		OperationID: rotateID,
		GatewayID:   "gw_succ_rotate",
		Status:      MaintenanceInProgress,
	}
	meta := Metadata{
		GatewayID:       "gw_succ_rotate",
		Status:          CredentialActive,
		LastOperationID: rotateID,
	}

	repo.checkpoints = []MaintenanceCheckpoint{cp}
	repo.operationsByID[rotateID] = op
	repo.checkpointsByID[rotateID] = cp
	repo.metadataByGW["gw_succ_rotate"] = meta

	s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
		Timeout:       time.Second,
		PageSize:      10,
		MaxPages:      5,
		VerifyBackend: func(context.Context, VerificationReceipt) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Run(context.Background())
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("expected ErrRecoveryRequired, got: %v", err)
	}
	if s.Ready() {
		t.Fatal("readiness must be false")
	}
	if runtime.opened {
		t.Fatal("runtime must remain closed")
	}
	if runtime.disableCalls != 0 || repo.beginRecoveryCalls != 0 || repo.completeRecCalls != 0 {
		t.Fatal("zero mutations expected")
	}
}

// Pending recovery attached to a rotate operation must block startup with 0 mutations.
func TestStartupRotationFencePendingRecoveryForRotate(t *testing.T) {
	rotateID := uuid.New()
	recID := uuid.New()
	repo := newFenceSpyRepo()
	runtime := &fenceSpyRuntime{}

	op := Operation{
		OperationID:    rotateID,
		ActorUserID:    uuid.New(),
		GatewayID:      "gw_rec_rotate",
		IdempotencyKey: uuid.New(),
		Action:         ActionRotate,
		Status:         OperationPending,
	}
	cp := MaintenanceCheckpoint{
		OperationID: rotateID,
		GatewayID:   "gw_rec_rotate",
		Status:      MaintenanceInProgress,
	}
	meta := Metadata{
		GatewayID:       "gw_rec_rotate",
		Status:          CredentialRotating,
		LastOperationID: rotateID,
	}
	rec := RecoveryRecord{
		RecoveryID:       recID,
		OperationID:      rotateID,
		GatewayID:        "gw_rec_rotate",
		Status:           RecoveryPending,
		AttemptedVersion: 2,
		Origin:           RecoveryStartup,
		BrokerEpoch:      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}

	repo.pendingRecovery = []RecoveryRecord{rec}
	repo.operationsByID[rotateID] = op
	repo.checkpointsByID[rotateID] = cp
	repo.metadataByGW["gw_rec_rotate"] = meta

	s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
		Timeout:       time.Second,
		PageSize:      10,
		MaxPages:      5,
		VerifyBackend: func(context.Context, VerificationReceipt) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Run(context.Background())
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("expected ErrRecoveryRequired, got: %v", err)
	}
	if s.Ready() {
		t.Fatal("readiness must be false")
	}
	if runtime.opened {
		t.Fatal("runtime must remain closed")
	}
	if runtime.disableCalls != 0 || repo.beginRecoveryCalls != 0 || repo.bindRecoveryCalls != 0 || repo.completeRecCalls != 0 {
		t.Fatal("zero mutations expected")
	}
}

// Mixed provision/revoke jobs plus a blocking rotation: zero recovery mutations regardless of ordering.
func TestStartupRotationFenceMixedJobsZeroMutations(t *testing.T) {
	provID := uuid.New()
	rotateID := uuid.New()
	revokeID := uuid.New()

	repo := newFenceSpyRepo()
	runtime := &fenceSpyRuntime{}

	provOp := Operation{
		OperationID:    provID,
		ActorUserID:    uuid.New(),
		GatewayID:      "gw_prov",
		IdempotencyKey: uuid.New(),
		Action:         ActionProvision,
		Status:         OperationPending,
	}
	rotateOp := Operation{
		OperationID:    rotateID,
		ActorUserID:    uuid.New(),
		GatewayID:      "gw_rot",
		IdempotencyKey: uuid.New(),
		Action:         ActionRotate,
		Status:         OperationPending,
	}
	revokeOp := Operation{
		OperationID:    revokeID,
		ActorUserID:    uuid.New(),
		GatewayID:      "gw_rev",
		IdempotencyKey: uuid.New(),
		Action:         ActionRevoke,
		Status:         OperationPending,
	}

	// Order them by ID to ensure valid cursor pagination
	ops := []Operation{provOp, rotateOp, revokeOp}
	if ops[0].OperationID.String() > ops[1].OperationID.String() {
		ops[0], ops[1] = ops[1], ops[0]
	}
	if ops[1].OperationID.String() > ops[2].OperationID.String() {
		ops[1], ops[2] = ops[2], ops[1]
	}
	if ops[0].OperationID.String() > ops[1].OperationID.String() {
		ops[0], ops[1] = ops[1], ops[0]
	}

	provMeta := Metadata{GatewayID: "gw_prov", Status: CredentialProvisioning, LastOperationID: provID}
	rotMeta := Metadata{GatewayID: "gw_rot", Status: CredentialRotating, LastOperationID: rotateID}
	revMeta := Metadata{GatewayID: "gw_rev", Status: CredentialActive, LastOperationID: revokeID}

	provCP := MaintenanceCheckpoint{OperationID: provID, GatewayID: "gw_prov", Status: MaintenanceInProgress}
	rotCP := MaintenanceCheckpoint{OperationID: rotateID, GatewayID: "gw_rot", Status: MaintenanceInProgress}
	revCP := MaintenanceCheckpoint{OperationID: revokeID, GatewayID: "gw_rev", Status: MaintenanceInProgress}

	repo.unresolved = ops
	repo.operationsByID[provID] = provOp
	repo.operationsByID[rotateID] = rotateOp
	repo.operationsByID[revokeID] = revokeOp

	repo.checkpointsByID[provID] = provCP
	repo.checkpointsByID[rotateID] = rotCP
	repo.checkpointsByID[revokeID] = revCP

	repo.metadataByGW["gw_prov"] = provMeta
	repo.metadataByGW["gw_rot"] = rotMeta
	repo.metadataByGW["gw_rev"] = revMeta

	s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
		Timeout:       time.Second,
		PageSize:      10,
		MaxPages:      5,
		VerifyBackend: func(context.Context, VerificationReceipt) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Run(context.Background())
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("expected ErrRecoveryRequired, got: %v", err)
	}
	if s.Ready() {
		t.Fatal("readiness must be false")
	}
	if runtime.opened {
		t.Fatal("runtime must remain closed")
	}
	// Crucial assertion: provision/revoke jobs must NOT have been recovered!
	if runtime.disableCalls != 0 {
		t.Fatalf("DisableStartup called %d times; want 0", runtime.disableCalls)
	}
	if repo.beginRecoveryCalls != 0 || repo.bindRecoveryCalls != 0 || repo.completeRecCalls != 0 {
		t.Fatalf("zero recovery mutations expected (begin=%d bind=%d complete=%d)", repo.beginRecoveryCalls, repo.bindRecoveryCalls, repo.completeRecCalls)
	}
}

// Safe failure when operation resolution is unavailable or inconsistent.
func TestStartupRotationFenceResolutionError(t *testing.T) {
	jobID := uuid.New()
	repo := newFenceSpyRepo()
	runtime := &fenceSpyRuntime{}

	cp := MaintenanceCheckpoint{
		OperationID: jobID,
		GatewayID:   "gw_err",
		Status:      MaintenanceInProgress,
	}
	repo.checkpoints = []MaintenanceCheckpoint{cp}
	repo.checkpointsByID[jobID] = cp
	// Simulate DB error on ResolveCommitAmbiguity
	repo.resolveOpErr[jobID] = errors.New("db error resolving operation")

	s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
		Timeout:       time.Second,
		PageSize:      10,
		MaxPages:      5,
		VerifyBackend: func(context.Context, VerificationReceipt) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Run(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if s.Ready() {
		t.Fatal("readiness must be false")
	}
	if runtime.opened {
		t.Fatal("runtime must remain closed")
	}
	if runtime.disableCalls != 0 || repo.beginRecoveryCalls != 0 || repo.completeRecCalls != 0 {
		t.Fatal("zero mutations expected")
	}
}

// Clean startup succeeds without unresolved jobs.
func TestStartupRotationFenceCleanStartupSucceeds(t *testing.T) {
	repo := newFenceSpyRepo()
	runtime := &fenceSpyRuntime{}

	repo.inventory = []Metadata{
		{GatewayID: "gw_active", Status: CredentialActive},
	}

	s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
		Timeout:       time.Second,
		PageSize:      10,
		MaxPages:      5,
		VerifyBackend: func(context.Context, VerificationReceipt) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Run(context.Background())
	if err != nil {
		t.Fatalf("clean startup failed: %v", err)
	}
	if !s.Ready() {
		t.Fatal("readiness must be true")
	}
	if !runtime.opened {
		t.Fatal("runtime must be opened")
	}
}

// Provision/revoke recovery remains functional when no rotation is present.
func TestStartupRotationFenceProvisionRevokeRecoveryWorks(t *testing.T) {
	provID := uuid.New()
	repo := newFenceSpyRepo()
	runtime := &fenceSpyRuntime{}

	provOp := Operation{
		OperationID:    provID,
		ActorUserID:    uuid.New(),
		GatewayID:      "gw_prov",
		IdempotencyKey: uuid.New(),
		Action:         ActionProvision,
		Status:         OperationPending,
		Phase:          PhaseIntent,
	}
	cp := MaintenanceCheckpoint{
		OperationID: provID,
		GatewayID:   "gw_prov",
		Status:      MaintenanceInProgress,
	}
	meta := Metadata{
		GatewayID:       "gw_prov",
		Status:          CredentialProvisioning,
		LastOperationID: provID,
	}

	repo.unresolved = []Operation{provOp}
	repo.operationsByID[provID] = provOp
	repo.checkpointsByID[provID] = cp
	repo.metadataByGW["gw_prov"] = meta

	s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
		Timeout:  time.Second,
		PageSize: 10,
		MaxPages: 5,
		VerifyBackend: func(ctx context.Context, r VerificationReceipt) error {
			t.Logf("VerifyBackend called")
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Run(context.Background())
	t.Logf("Run returned: %v", err)
	if err != nil {
		t.Fatalf("expected recovery to succeed, got: %v", err)
	}
	if !s.Ready() {
		t.Fatal("expected ready after provision recovery")
	}
	if !runtime.opened {
		t.Fatal("expected runtime opened after recovery")
	}
	if runtime.disableCalls != 2 {
		t.Fatalf("DisableStartup called %d times; want 2", runtime.disableCalls)
	}
	if repo.beginRecoveryCalls != 1 {
		t.Fatalf("BeginRecovery called %d times; want 1", repo.beginRecoveryCalls)
	}
	if repo.completeRecCalls != 1 {
		t.Fatalf("CompleteRecovery called %d times; want 1", repo.completeRecCalls)
	}
}

// Inconsistent resolution: resolver returns a different (foreign) OperationID.
// Must fail closed with ErrRecoveryRequired and ZERO recovery mutations.
func TestStartupResolutionIntegrityForeignID(t *testing.T) {
	jobID := uuid.New()
	foreignID := uuid.New()
	repo := newFenceSpyRepo()
	runtime := &fenceSpyRuntime{}

	repo.checkpoints = []MaintenanceCheckpoint{{
		OperationID: jobID,
		GatewayID:   "gw_foreign",
		Status:      MaintenanceInProgress,
	}}
	repo.checkpointsByID[jobID] = repo.checkpoints[0]

	// Resolver returns foreignID instead of jobID
	repo.operationsByID[jobID] = Operation{
		OperationID:    foreignID,
		ActorUserID:    uuid.New(),
		GatewayID:      "gw_foreign",
		IdempotencyKey: uuid.New(),
		Action:         ActionProvision,
		Status:         OperationPending,
	}
	repo.checkpointsByID[jobID] = MaintenanceCheckpoint{
		OperationID: jobID,
		GatewayID:   "gw_foreign",
		Status:      MaintenanceInProgress,
	}
	repo.checkpointsByID[foreignID] = MaintenanceCheckpoint{
		OperationID: foreignID,
		GatewayID:   "gw_foreign",
		Status:      MaintenanceInProgress,
	}
	repo.metadataByGW["gw_foreign"] = Metadata{
		GatewayID:       "gw_foreign",
		LastOperationID: jobID,
		Status:          CredentialProvisioning,
	}

	s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
		Timeout:       time.Second,
		PageSize:      10,
		MaxPages:      5,
		VerifyBackend: func(context.Context, VerificationReceipt) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Run(context.Background())
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("expected ErrRecoveryRequired, got: %v", err)
	}
	if s.Ready() {
		t.Fatal("readiness must be false")
	}
	if runtime.opened {
		t.Fatal("runtime must remain closed")
	}
	if runtime.beginCalls != 1 {
		t.Fatalf("BeginStartup calls: %d; want 1", runtime.beginCalls)
	}
	if runtime.drainCalls == 0 {
		t.Fatal("expected defensive CloseDrain call")
	}
	if runtime.disableCalls != 0 || runtime.openCalls != 0 || repo.beginRecoveryCalls != 0 || repo.bindRecoveryCalls != 0 || repo.completeRecCalls != 0 {
		t.Fatalf("zero recovery mutations expected (disable=%d open=%d beginRec=%d bindRec=%d completeRec=%d)",
			runtime.disableCalls, runtime.openCalls, repo.beginRecoveryCalls, repo.bindRecoveryCalls, repo.completeRecCalls)
	}
}

// Inconsistent resolution: resolver returns uuid.Nil as OperationID.
// Must fail closed with ErrRecoveryRequired and ZERO recovery mutations.
func TestStartupResolutionIntegrityNilID(t *testing.T) {
	jobID := uuid.New()
	repo := newFenceSpyRepo()
	runtime := &fenceSpyRuntime{}

	repo.checkpoints = []MaintenanceCheckpoint{{
		OperationID: jobID,
		GatewayID:   "gw_nil",
		Status:      MaintenanceInProgress,
	}}
	repo.checkpointsByID[jobID] = repo.checkpoints[0]

	repo.operationsByID[jobID] = Operation{
		OperationID:    uuid.Nil,
		ActorUserID:    uuid.New(),
		GatewayID:      "gw_nil",
		IdempotencyKey: uuid.New(),
		Action:         ActionProvision,
		Status:         OperationPending,
	}
	repo.metadataByGW["gw_nil"] = Metadata{
		GatewayID:       "gw_nil",
		LastOperationID: jobID,
		Status:          CredentialProvisioning,
	}

	s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
		Timeout:       time.Second,
		PageSize:      10,
		MaxPages:      5,
		VerifyBackend: func(context.Context, VerificationReceipt) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Run(context.Background())
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("expected ErrRecoveryRequired, got: %v", err)
	}
	if s.Ready() {
		t.Fatal("readiness must be false")
	}
	if runtime.opened {
		t.Fatal("runtime must remain closed")
	}
	if runtime.disableCalls != 0 || runtime.openCalls != 0 || repo.beginRecoveryCalls != 0 || repo.bindRecoveryCalls != 0 || repo.completeRecCalls != 0 {
		t.Fatalf("zero recovery mutations expected (disable=%d open=%d beginRec=%d bindRec=%d completeRec=%d)",
			runtime.disableCalls, runtime.openCalls, repo.beginRecoveryCalls, repo.bindRecoveryCalls, repo.completeRecCalls)
	}
}

// Inconsistent resolution: resolver returns invalid or empty action.
// Must fail closed with ErrRecoveryRequired and ZERO recovery mutations.
func TestStartupResolutionIntegrityInvalidAction(t *testing.T) {
	for _, invalidAction := range []Action{"", "unknown", "recover", "drain"} {
		t.Run(string(invalidAction), func(t *testing.T) {
			jobID := uuid.New()
			repo := newFenceSpyRepo()
			runtime := &fenceSpyRuntime{}

			repo.checkpoints = []MaintenanceCheckpoint{{
				OperationID: jobID,
				GatewayID:   "gw_action",
				Status:      MaintenanceInProgress,
			}}
			repo.checkpointsByID[jobID] = repo.checkpoints[0]

			repo.operationsByID[jobID] = Operation{
				OperationID:    jobID,
				ActorUserID:    uuid.New(),
				GatewayID:      "gw_action",
				IdempotencyKey: uuid.New(),
				Action:         invalidAction,
				Status:         OperationPending,
			}
			repo.metadataByGW["gw_action"] = Metadata{
				GatewayID:       "gw_action",
				LastOperationID: jobID,
				Status:          CredentialProvisioning,
			}

			s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
				Timeout:       time.Second,
				PageSize:      10,
				MaxPages:      5,
				VerifyBackend: func(context.Context, VerificationReceipt) error { return nil },
			})
			if err != nil {
				t.Fatal(err)
			}

			err = s.Run(context.Background())
			if !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("expected ErrRecoveryRequired, got: %v", err)
			}
			if s.Ready() {
				t.Fatal("readiness must be false")
			}
			if runtime.opened {
				t.Fatal("runtime must remain closed")
			}
			if runtime.disableCalls != 0 || runtime.openCalls != 0 || repo.beginRecoveryCalls != 0 || repo.bindRecoveryCalls != 0 || repo.completeRecCalls != 0 {
				t.Fatalf("zero recovery mutations expected (disable=%d open=%d beginRec=%d bindRec=%d completeRec=%d)",
					runtime.disableCalls, runtime.openCalls, repo.beginRecoveryCalls, repo.bindRecoveryCalls, repo.completeRecCalls)
			}
		})
	}
}

// Inconsistent resolution: second resolution during recovery mutates action to rotate or changes identity.
// Must fail closed before any recovery mutation (zero BeginRecovery/DisableStartup).
func TestStartupResolutionIntegritySecondResolutionMutated(t *testing.T) {
	t.Run("action-changes-to-rotate", func(t *testing.T) {
		jobID := uuid.New()
		repo := newFenceSpyRepo()
		runtime := &fenceSpyRuntime{}

		provOp := Operation{
			OperationID:    jobID,
			ActorUserID:    uuid.New(),
			GatewayID:      "gw_mutate",
			IdempotencyKey: uuid.New(),
			Action:         ActionProvision,
			Status:         OperationPending,
		}
		rotateOp := Operation{
			OperationID:    jobID,
			ActorUserID:    uuid.New(),
			GatewayID:      "gw_mutate",
			IdempotencyKey: uuid.New(),
			Action:         ActionRotate,
			Status:         OperationPending,
		}

		repo.checkpoints = []MaintenanceCheckpoint{{
			OperationID: jobID,
			GatewayID:   "gw_mutate",
			Status:      MaintenanceInProgress,
		}}
		repo.checkpointsByID[jobID] = repo.checkpoints[0]
		repo.metadataByGW["gw_mutate"] = Metadata{GatewayID: "gw_mutate", LastOperationID: jobID, Status: CredentialProvisioning}

		repo.resolveOpFn = func(ctx context.Context, id uuid.UUID, callCount int) (Operation, error) {
			if callCount == 1 {
				return provOp, nil // preflight sees provision
			}
			return rotateOp, nil // recovery-stage resolution sees rotate
		}

		s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
			Timeout:       time.Second,
			PageSize:      10,
			MaxPages:      5,
			VerifyBackend: func(context.Context, VerificationReceipt) error { return nil },
		})
		if err != nil {
			t.Fatal(err)
		}

		err = s.Run(context.Background())
		if !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("expected ErrRecoveryRequired, got: %v", err)
		}
		if s.Ready() {
			t.Fatal("readiness must be false")
		}
		if runtime.opened {
			t.Fatal("runtime must remain closed")
		}
		if runtime.disableCalls != 0 || runtime.openCalls != 0 || repo.beginRecoveryCalls != 0 || repo.bindRecoveryCalls != 0 || repo.completeRecCalls != 0 {
			t.Fatalf("zero recovery mutations expected (disable=%d open=%d beginRec=%d bindRec=%d completeRec=%d)",
				runtime.disableCalls, runtime.openCalls, repo.beginRecoveryCalls, repo.bindRecoveryCalls, repo.completeRecCalls)
		}
	})

	t.Run("identity-changes-on-second-resolve", func(t *testing.T) {
		jobID := uuid.New()
		foreignID := uuid.New()
		repo := newFenceSpyRepo()
		runtime := &fenceSpyRuntime{}

		provOp := Operation{
			OperationID:    jobID,
			ActorUserID:    uuid.New(),
			GatewayID:      "gw_mutate2",
			IdempotencyKey: uuid.New(),
			Action:         ActionProvision,
			Status:         OperationPending,
		}
		foreignOp := Operation{
			OperationID:    foreignID,
			ActorUserID:    uuid.New(),
			GatewayID:      "gw_mutate2",
			IdempotencyKey: uuid.New(),
			Action:         ActionProvision,
			Status:         OperationPending,
		}

		repo.checkpoints = []MaintenanceCheckpoint{{
			OperationID: jobID,
			GatewayID:   "gw_mutate2",
			Status:      MaintenanceInProgress,
		}}
		repo.checkpointsByID[jobID] = repo.checkpoints[0]
		repo.metadataByGW["gw_mutate2"] = Metadata{GatewayID: "gw_mutate2", LastOperationID: jobID, Status: CredentialProvisioning}

		repo.resolveOpFn = func(ctx context.Context, id uuid.UUID, callCount int) (Operation, error) {
			if callCount == 1 {
				return provOp, nil // preflight sees jobID
			}
			return foreignOp, nil // recovery-stage resolution sees foreignID
		}

		s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
			Timeout:       time.Second,
			PageSize:      10,
			MaxPages:      5,
			VerifyBackend: func(context.Context, VerificationReceipt) error { return nil },
		})
		if err != nil {
			t.Fatal(err)
		}

		err = s.Run(context.Background())
		if !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("expected ErrRecoveryRequired, got: %v", err)
		}
		if s.Ready() {
			t.Fatal("readiness must be false")
		}
		if runtime.opened {
			t.Fatal("runtime must remain closed")
		}
		if runtime.disableCalls != 0 || runtime.openCalls != 0 || repo.beginRecoveryCalls != 0 || repo.bindRecoveryCalls != 0 || repo.completeRecCalls != 0 {
			t.Fatalf("zero recovery mutations expected (disable=%d open=%d beginRec=%d bindRec=%d completeRec=%d)",
				runtime.disableCalls, runtime.openCalls, repo.beginRecoveryCalls, repo.bindRecoveryCalls, repo.completeRecCalls)
		}
	})
}

// Mixed safe jobs plus an inconsistent job must fail closed with ZERO recovery mutations.
func TestStartupResolutionIntegrityMixedSafePlusInconsistentZeroMutations(t *testing.T) {
	provID := uuid.New()
	inconsistentID := uuid.New()
	foreignID := uuid.New()
	revokeID := uuid.New()

	repo := newFenceSpyRepo()
	runtime := &fenceSpyRuntime{}

	provOp := Operation{
		OperationID:    provID,
		ActorUserID:    uuid.New(),
		GatewayID:      "gw_prov",
		IdempotencyKey: uuid.New(),
		Action:         ActionProvision,
		Status:         OperationPending,
	}
	foreignOp := Operation{
		OperationID:    foreignID,
		ActorUserID:    uuid.New(),
		GatewayID:      "gw_inconsistent",
		IdempotencyKey: uuid.New(),
		Action:         ActionProvision,
		Status:         OperationPending,
	}
	revokeOp := Operation{
		OperationID:    revokeID,
		ActorUserID:    uuid.New(),
		GatewayID:      "gw_rev",
		IdempotencyKey: uuid.New(),
		Action:         ActionRevoke,
		Status:         OperationPending,
	}

	ops := []Operation{provOp, {OperationID: inconsistentID, ActorUserID: uuid.New(), GatewayID: "gw_inconsistent", IdempotencyKey: uuid.New(), Action: ActionProvision, Status: OperationPending}, revokeOp}
	// Sort by ID for valid pagination cursor
	if ops[0].OperationID.String() > ops[1].OperationID.String() {
		ops[0], ops[1] = ops[1], ops[0]
	}
	if ops[1].OperationID.String() > ops[2].OperationID.String() {
		ops[1], ops[2] = ops[2], ops[1]
	}
	if ops[0].OperationID.String() > ops[1].OperationID.String() {
		ops[0], ops[1] = ops[1], ops[0]
	}

	repo.unresolved = ops
	repo.operationsByID[provID] = provOp
	repo.operationsByID[inconsistentID] = foreignOp // foreign ID returned!
	repo.operationsByID[revokeID] = revokeOp

	repo.checkpointsByID[provID] = MaintenanceCheckpoint{OperationID: provID, GatewayID: "gw_prov", Status: MaintenanceInProgress}
	repo.checkpointsByID[inconsistentID] = MaintenanceCheckpoint{OperationID: inconsistentID, GatewayID: "gw_inconsistent", Status: MaintenanceInProgress}
	repo.checkpointsByID[revokeID] = MaintenanceCheckpoint{OperationID: revokeID, GatewayID: "gw_rev", Status: MaintenanceInProgress}

	repo.metadataByGW["gw_prov"] = Metadata{GatewayID: "gw_prov", Status: CredentialProvisioning, LastOperationID: provID}
	repo.metadataByGW["gw_inconsistent"] = Metadata{GatewayID: "gw_inconsistent", Status: CredentialProvisioning, LastOperationID: inconsistentID}
	repo.metadataByGW["gw_rev"] = Metadata{GatewayID: "gw_rev", Status: CredentialActive, LastOperationID: revokeID}

	s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
		Timeout:       time.Second,
		PageSize:      10,
		MaxPages:      5,
		VerifyBackend: func(context.Context, VerificationReceipt) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Run(context.Background())
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("expected ErrRecoveryRequired, got: %v", err)
	}
	if s.Ready() {
		t.Fatal("readiness must be false")
	}
	if runtime.opened {
		t.Fatal("runtime must remain closed")
	}
	if runtime.disableCalls != 0 || runtime.openCalls != 0 || repo.beginRecoveryCalls != 0 || repo.bindRecoveryCalls != 0 || repo.completeRecCalls != 0 {
		t.Fatalf("zero recovery mutations expected (disable=%d open=%d beginRec=%d bindRec=%d completeRec=%d)",
			runtime.disableCalls, runtime.openCalls, repo.beginRecoveryCalls, repo.bindRecoveryCalls, repo.completeRecCalls)
	}
}

// Provision and Revoke recovery both work cleanly when valid and no rotation is present.
func TestStartupRotationFenceRevokeRecoveryWorks(t *testing.T) {
	revokeID := uuid.New()
	repo := newFenceSpyRepo()
	runtime := &fenceSpyRuntime{}

	revokeOp := Operation{
		OperationID:    revokeID,
		ActorUserID:    uuid.New(),
		GatewayID:      "gw_rev",
		IdempotencyKey: uuid.New(),
		Action:         ActionRevoke,
		Status:         OperationPending,
		Phase:          PhaseIntent,
	}
	cp := MaintenanceCheckpoint{
		OperationID: revokeID,
		GatewayID:   "gw_rev",
		Status:      MaintenanceInProgress,
	}
	meta := Metadata{
		GatewayID:       "gw_rev",
		Status:          CredentialActive,
		LastOperationID: revokeID,
	}

	repo.unresolved = []Operation{revokeOp}
	repo.operationsByID[revokeID] = revokeOp
	repo.checkpointsByID[revokeID] = cp
	repo.metadataByGW["gw_rev"] = meta

	s, err := NewStartupReconciler(repo, repo, repo, runtime, StartupOptions{
		Timeout:  time.Second,
		PageSize: 10,
		MaxPages: 5,
		VerifyBackend: func(ctx context.Context, r VerificationReceipt) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Run(context.Background())
	if err != nil {
		t.Fatalf("expected recovery to succeed, got: %v", err)
	}
	if !s.Ready() {
		t.Fatal("expected ready after revoke recovery")
	}
	if !runtime.opened {
		t.Fatal("expected runtime opened after recovery")
	}
	if runtime.disableCalls != 2 {
		t.Fatalf("DisableStartup called %d times; want 2", runtime.disableCalls)
	}
	if repo.beginRecoveryCalls != 1 {
		t.Fatalf("BeginRecovery called %d times; want 1", repo.beginRecoveryCalls)
	}
	if repo.completeRecCalls != 1 {
		t.Fatalf("CompleteRecovery called %d times; want 1", repo.completeRecCalls)
	}
}

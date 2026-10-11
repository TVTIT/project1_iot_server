package mqttcredential

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Spy implementations for Simplified TDD RED phase
type spySimplifiedRepo struct {
	admin            bool
	adminErr         error
	beginResult      BeginResult
	beginErr         error
	beginResetResult BeginResult
	beginResetErr    error
	finalizeMeta     Metadata
	finalizeErr      error
	completion       Completion
	autoComplete     bool
	failKnownMeta    Metadata
	failKnownErr     error
	requireRecMeta   Metadata
	requireRecErr    error
	getMeta          Metadata
	getMetaErr       error
	ambiguityOp      Operation
	ambiguityErr     error
	ambiguityHook    func(uuid.UUID) (Operation, error)
	lookupOp         Operation
	lookupMeta       Metadata
	lookupFound      bool
	lookupErr        error

	calls []string
}

func (r *spySimplifiedRepo) IsPlatformAdmin(ctx context.Context, actor uuid.UUID) (bool, error) {
	r.calls = append(r.calls, "IsPlatformAdmin")
	return r.admin, r.adminErr
}

func (r *spySimplifiedRepo) BeginSimplifiedOperation(ctx context.Context, req BeginRequest) (BeginResult, error) {
	r.calls = append(r.calls, "BeginSimplifiedOperation:"+string(req.Input.Action))
	return r.beginResult, r.beginErr
}

func (r *spySimplifiedRepo) BeginReset(ctx context.Context, req BeginResetRequest) (BeginResult, error) {
	r.calls = append(r.calls, "BeginReset:"+string(req.Input.Reason))
	return r.beginResetResult, r.beginResetErr
}

func (r *spySimplifiedRepo) ConditionalFinalize(ctx context.Context, u OperationUpdate) (Metadata, error) {
	r.calls = append(r.calls, "ConditionalFinalize")
	r.completion = u.Completion
	if r.autoComplete {
		r.ambiguityOp = u.Completion.Operation
		m := r.beginResult.Metadata
		if m.GatewayID == "" {
			m = r.beginResetResult.Metadata
		}
		m.Status = u.Completion.Credential.Status
		m.OperationStatus = OperationSucceeded
		m.ActivatedAt = u.Completion.Credential.ActivatedAt
		m.RevokedAt = u.Completion.Credential.RevokedAt
		r.getMeta = m
		r.finalizeMeta = m
	}
	return r.finalizeMeta, r.finalizeErr
}

func (r *spySimplifiedRepo) FailKnown(ctx context.Context, u OperationUpdate) (Metadata, error) {
	r.calls = append(r.calls, "FailKnown")
	return r.failKnownMeta, r.failKnownErr
}

func (r *spySimplifiedRepo) RequireRecovery(ctx context.Context, u OperationUpdate) (Metadata, error) {
	r.calls = append(r.calls, "RequireRecovery")
	return r.requireRecMeta, r.requireRecErr
}

func (r *spySimplifiedRepo) GetMetadata(ctx context.Context, g string) (Metadata, error) {
	r.calls = append(r.calls, "GetMetadata")
	return r.getMeta, r.getMetaErr
}

func (r *spySimplifiedRepo) ResolveCommitAmbiguity(ctx context.Context, id uuid.UUID) (Operation, error) {
	r.calls = append(r.calls, "ResolveCommitAmbiguity")
	if r.ambiguityHook != nil {
		return r.ambiguityHook(id)
	}
	return r.ambiguityOp, r.ambiguityErr
}

func (r *spySimplifiedRepo) LookupResetReplay(ctx context.Context, actor uuid.UUID, in ResetInput) (Operation, Metadata, bool, error) {
	r.calls = append(r.calls, "LookupResetReplay:"+string(in.Reason))
	return r.lookupOp, r.lookupMeta, r.lookupFound, r.lookupErr
}

type spySimplifiedExecutor struct {
	execResult SimplifiedExecutionResult
	execErr    error
	revokedErr error
	calls      []string
	generate   bool
	observe    func()
}

func (e *spySimplifiedExecutor) Execute(ctx context.Context, o Operation, gen func(context.Context) (string, error)) (SimplifiedExecutionResult, error) {
	e.calls = append(e.calls, "Execute:"+string(o.Action))
	if e.observe != nil {
		e.observe()
	}
	if e.generate && gen != nil {
		if _, err := gen(ctx); err != nil {
			return SimplifiedExecutionResult{OperationID: o.OperationID, Outcome: ExecutionUnchangedFailure}, err
		}
	}
	return e.execResult, e.execErr
}

func simplifiedServiceFixture(t *testing.T, action Action) (*ProvisionService, *spySimplifiedRepo, *spySimplifiedExecutor, uuid.UUID, MutationInput) {
	t.Helper()
	actor, id, key := uuid.New(), uuid.New(), uuid.New()
	in := MutationInput{GatewayID: "test_gateway", IdempotencyKey: key, Action: action}
	o := Operation{OperationID: id, ActorUserID: actor, GatewayID: in.GatewayID, IdempotencyKey: key, Action: action, CredentialVersion: 2, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown, ExecutionContract: ContractSimplified, CreatedAt: time.Now().Add(-time.Second), UpdatedAt: time.Now().Add(-time.Second)}
	status := CredentialProvisioning
	if action != ActionProvision {
		o.Previous = &CredentialSnapshot{Status: CredentialActive, CredentialVersion: 1}
		status = CredentialRotating
	}
	if action == ActionRevoke {
		o.CredentialVersion = 1
		status = CredentialRevoking
	}
	m := Metadata{GatewayID: in.GatewayID, Username: in.GatewayID, CredentialVersion: o.CredentialVersion, Status: status, LastOperationID: id, OperationStatus: OperationPending}
	r := &spySimplifiedRepo{admin: true, beginResult: BeginResult{Execute: true, Operation: o, Metadata: m}}
	// SYNTHETIC CONTRACT FIXTURE (mock-only): When action == ActionRevoke, SessionHandlingVerified
	// is explicitly set to true ONLY to test the service-layer qualified success contract.
	// This is a unit test spy assertion and does NOT constitute native session proof or qualification.
	e := &spySimplifiedExecutor{generate: true, execResult: SimplifiedExecutionResult{OperationID: id, Outcome: ExecutionVerifiedSuccess, Mutated: true, Evidence: SimplifiedExecutionEvidence{RAMApplied: true, FreshPositiveVerified: action != ActionRevoke, ProtocolDenialVerified: action == ActionRevoke, SessionHandlingVerified: action == ActionRevoke}}}
	s, err := NewSimplifiedProvisionService(r, e, &spyResetPrerequisite{}, SimplifiedProvisionServiceOptions{RecoveryTimeout: time.Second, NewOperationID: func() (uuid.UUID, error) { return id, nil }, GeneratePassword: func() (string, error) { return strings.Repeat("a", 48), nil }})
	if err != nil {
		t.Fatal(err)
	}
	return s, r, e, actor, in
}

func TestSimplifiedService_Orchestration(t *testing.T) {
	for _, action := range []Action{ActionProvision, ActionRotate, ActionRevoke} {
		t.Run(string(action), func(t *testing.T) {
			s, r, e, actor, in := simplifiedServiceFixture(t, action)
			e.observe = func() {
				if len(r.calls) != 2 || !strings.HasPrefix(r.calls[1], "BeginSimplifiedOperation:") {
					t.Fatal("native before intent")
				}
			}
			// Repository readback must contain the actual completion generated by service.
			r.ambiguityErr = errors.New("not completed")
			out, err := s.mutate(context.Background(), actor, in, action)
			if err == nil || out.Secret != nil {
				t.Fatal("disclosed without readback")
			}
			if r.completion.Operation.Status != OperationSucceeded || r.completion.Operation.Evidence.SnapshotObserved {
				t.Fatal("completion evidence")
			}
		})
	}
}

func TestSimplifiedService_FinalizeBeforeDisclosure(t *testing.T) {
	for _, action := range []Action{ActionProvision, ActionRotate, ActionRevoke} {
		for _, ambiguous := range []bool{false, true} {
			t.Run(string(action)+"/"+fmt.Sprint(ambiguous), func(t *testing.T) {
				s, r, _, a, in := simplifiedServiceFixture(t, action)
				r.autoComplete = true
				if ambiguous {
					r.finalizeErr = errors.New("lost commit acknowledgement")
				}
				out, err := s.mutate(context.Background(), a, in, action)
				if err != nil || out.SecretReturned != (action != ActionRevoke) || (out.Secret != nil) != (action != ActionRevoke) {
					t.Fatalf("completion %v", err)
				}
				if !reflect.DeepEqual(r.calls, []string{"IsPlatformAdmin", "BeginSimplifiedOperation:" + string(action), "ConditionalFinalize", "ResolveCommitAmbiguity", "GetMetadata"}) {
					t.Fatal(r.calls)
				}
			})
		}
	}
}

func TestSimplifiedService_RejectAdmission(t *testing.T) {
	for _, change := range []func(*BeginResult){
		func(b *BeginResult) { b.Operation.ActorUserID = uuid.New() }, func(b *BeginResult) { b.Operation.OperationID = uuid.New() }, func(b *BeginResult) { b.Operation.IdempotencyKey = uuid.New() }, func(b *BeginResult) { b.Operation.GatewayID = "other_gateway" }, func(b *BeginResult) { b.Operation.Action = ActionRotate }, func(b *BeginResult) { b.Operation.ExecutionContract = ContractLifecycle }, func(b *BeginResult) { b.Operation.Status = OperationRecoveryNeeded }, func(b *BeginResult) { b.Metadata.Username = "other_gateway" }, func(b *BeginResult) { b.Metadata.CredentialVersion++ }, func(b *BeginResult) { b.Metadata.Status = CredentialActive }, func(b *BeginResult) { b.Execute = false },
	} {
		s, r, e, a, in := simplifiedServiceFixture(t, ActionProvision)
		change(&r.beginResult)
		out, err := s.Provision(context.Background(), a, in)
		if err == nil || out.Secret != nil || len(e.calls) != 0 || !s.poisoned {
			t.Fatal("bad admission accepted")
		}
	}
}

func TestSimplifiedService_FailureClassification(t *testing.T) {
	for _, name := range []string{"known", "partial", "identity", "evidence", "generator", "db"} {
		t.Run(name, func(t *testing.T) {
			s, r, e, a, in := simplifiedServiceFixture(t, ActionRotate)
			switch name {
			case "known", "db":
				e.execResult = SimplifiedExecutionResult{OperationID: r.beginResult.Operation.OperationID, Outcome: ExecutionUnchangedFailure}
				e.execErr = errors.New("private")
				if name == "db" {
					r.failKnownErr = errors.New("db")
				}
			case "partial":
				e.execErr = errors.New("private")
			case "identity":
				e.execResult.OperationID = uuid.New()
			case "evidence":
				e.execResult.Evidence.FreshPositiveVerified = false
			case "generator":
				s.simplifiedCfg.GeneratePassword = func() (string, error) { return "", errors.New("private") }
			}
			out, err := s.Rotate(context.Background(), a, in)
			if err == nil || out.Secret != nil || strings.Contains(err.Error(), "private") {
				t.Fatal("unsafe failure")
			}
			known := name == "known" || name == "db" || name == "generator"
			want := "RequireRecovery"
			if known {
				want = "FailKnown"
			}
			if r.calls[len(r.calls)-1] != want {
				t.Fatalf("calls %v", r.calls)
			}
			if s.poisoned != (!known || name == "db") {
				t.Fatal("poison classification")
			}
		})
	}
}

func TestSimplifiedService_ReplayAndRevoked(t *testing.T) {
	s, r, e, a, in := simplifiedServiceFixture(t, ActionProvision)
	c, _ := CompleteOperation(r.beginResult.Operation, ExecutionVerifiedSuccess, VerificationEvidence{RAMApplied: true, FreshPositiveVerified: true}, time.Now())
	r.beginResult.Operation = c.Operation
	r.beginResult.Execute = false
	m := r.beginResult.Metadata
	m.Status = CredentialActive
	m.OperationStatus = OperationSucceeded
	r.beginResult.Metadata = m
	replay, _ := ReplayOperation(c.Operation, a, in, m)
	r.beginResult.Replay = &replay
	out, err := s.Provision(context.Background(), a, in)
	if err != nil || out.Secret != nil || out.SecretReturned || len(e.calls) != 0 {
		t.Fatalf("replay %v", err)
	}
	s, r, e, a, in = simplifiedServiceFixture(t, ActionRevoke)
	m = r.beginResult.Metadata
	m.Status = CredentialRevoked
	m.OperationStatus = OperationSucceeded
	r.beginResult = BeginResult{Metadata: m, Replay: &MutationResult{Metadata: m}}
	r.getMeta = m
	out, err = s.mutate(context.Background(), a, in, ActionRevoke)
	if err != nil || out.Secret != nil || len(e.calls) != 1 || !strings.HasPrefix(e.calls[0], "CheckRevoked:") {
		t.Fatalf("noop %v", err)
	}
	e.revokedErr = errors.New("native unknown")
	if _, err = s.Revoke(context.Background(), a, in); err == nil || !s.poisoned {
		t.Fatal("historic DB trusted")
	}
}

func TestSimplifiedService_AuthorizationAndResetBlocker(t *testing.T) {
	s, r, e, a, in := simplifiedServiceFixture(t, ActionProvision)
	r.admin = false
	if _, err := s.Provision(context.Background(), a, in); err == nil || len(e.calls) != 0 || len(r.calls) != 1 {
		t.Fatal("authorization")
	}
	reset := ResetInput{GatewayID: in.GatewayID, IdempotencyKey: in.IdempotencyKey, Reason: ResetReasonRecoveryAfterUnknown}
	if _, err := s.Reset(context.Background(), a, reset); err == nil || err.Error() != string(CodeForbidden) {
		t.Fatal("reset auth")
	}
	r.admin = true
	s.busy = true
	if _, err := s.Provision(context.Background(), a, in); err == nil || err.Error() != string(CodeRuntimeBusy) {
		t.Fatal("busy")
	}
	if _, err := s.Reset(context.Background(), a, reset); err == nil || err.Error() != string(CodeRuntimeBusy) {
		t.Fatal("reset busy")
	}
	s.busy = false
	// When LookupResetReplay returns an error (e.g. database failure), Reset returns service error without admission
	r.lookupErr = errors.New("lookup database unavailable")
	if _, err := s.Reset(context.Background(), a, reset); err == nil || err.Error() != string(CodeServiceUnavailable) {
		t.Fatal("lookup error must propagate as service unavailable")
	}
	for _, call := range r.calls {
		if strings.HasPrefix(call, "BeginReset") {
			t.Fatal("reset admitted")
		}
	}
	if len(s.resetPrereq.(*spyResetPrerequisite).calls) != 0 {
		t.Fatal("replay prerequisite side effect")
	}
}

func (e *spySimplifiedExecutor) CheckRevoked(ctx context.Context, u string) error {
	e.calls = append(e.calls, "CheckRevoked:"+u)
	return e.revokedErr
}

type spyResetPrerequisite struct {
	checkErr error
	calls    []string
}

func (p *spyResetPrerequisite) SupervisedQuiescencePrerequisite(ctx context.Context, g string) error {
	p.calls = append(p.calls, "SupervisedQuiescencePrerequisite:"+g)
	return p.checkErr
}

// Compile check to fail RED before service implementation
func TestSimplifiedService_CompileRED(t *testing.T) {
	repo := &spySimplifiedRepo{admin: true}
	exec := &spySimplifiedExecutor{}
	prereq := &spyResetPrerequisite{}

	opts := SimplifiedProvisionServiceOptions{
		RecoveryTimeout: time.Second,
	}

	svc, err := NewSimplifiedProvisionService(repo, exec, prereq, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_ = svc
}

// RED contract test: Revoke executor without SessionHandlingVerified must fail closed
// and require recovery (poisoning the service), NOT finalize succeeded.
func TestSimplifiedService_Revoke_MissingSessionEvidence_MustRequireRecovery(t *testing.T) {
	s, r, e, a, in := simplifiedServiceFixture(t, ActionRevoke)
	r.autoComplete = true
	// Executor returns Mutated=true, ExecutionVerifiedSuccess, RAMApplied=true, ProtocolDenialVerified=true,
	// but SessionHandlingVerified=false (missing active session handling evidence).
	e.execResult = SimplifiedExecutionResult{
		OperationID: r.beginResult.Operation.OperationID,
		Outcome:     ExecutionVerifiedSuccess,
		Mutated:     true,
		Evidence: SimplifiedExecutionEvidence{
			RAMApplied:              true,
			FreshPositiveVerified:   false,
			ProtocolDenialVerified:  true,
			SessionHandlingVerified: false,
		},
	}

	res, err := s.mutate(context.Background(), a, in, ActionRevoke)
	if err == nil {
		t.Fatal("expected error when SessionHandlingVerified is false, got nil")
	}
	if res.SecretReturned || res.Secret != nil {
		t.Fatal("secret returned on failed revoke")
	}
	// Service must require recovery and poison
	if !s.poisoned {
		t.Fatal("service must be poisoned when executor reports Mutated+VerifiedSuccess without active session evidence")
	}
	if len(r.calls) == 0 || r.calls[len(r.calls)-1] != "RequireRecovery" {
		t.Fatalf("expected last call to be RequireRecovery, got calls: %v", r.calls)
	}
	for _, call := range r.calls {
		if call == "ConditionalFinalize" {
			t.Fatal("ConditionalFinalize must NOT be called when session evidence is missing")
		}
	}
}

// Contract tests for all predicate boundary conditions on simplified operations.
func TestSimplifiedService_EvidenceContractMatrix(t *testing.T) {
	t.Run("Revoke: synthetic contract test with all evidence verified succeeds", func(t *testing.T) {
		s, r, e, a, in := simplifiedServiceFixture(t, ActionRevoke)
		r.autoComplete = true
		// SYNTHETIC CONTRACT TEST (explicitly mock/spy only, NOT native session proof):
		e.execResult = SimplifiedExecutionResult{
			OperationID: r.beginResult.Operation.OperationID,
			Outcome:     ExecutionVerifiedSuccess,
			Mutated:     true,
			Evidence: SimplifiedExecutionEvidence{
				RAMApplied:              true,
				FreshPositiveVerified:   false,
				ProtocolDenialVerified:  true,
				SessionHandlingVerified: true,
			},
		}
		out, err := s.mutate(context.Background(), a, in, ActionRevoke)
		if err != nil {
			t.Fatalf("synthetic complete evidence must succeed in service unit test, got: %v", err)
		}
		if out.SecretReturned || out.Secret != nil {
			t.Fatal("revoke must not return secret")
		}
		if s.poisoned {
			t.Fatal("service should not be poisoned on valid synthetic completion")
		}
	})

	t.Run("Revoke: missing RAMApplied fails closed with recovery", func(t *testing.T) {
		s, r, e, a, in := simplifiedServiceFixture(t, ActionRevoke)
		r.autoComplete = true
		e.execResult = SimplifiedExecutionResult{
			OperationID: r.beginResult.Operation.OperationID,
			Outcome:     ExecutionVerifiedSuccess,
			Mutated:     true,
			Evidence: SimplifiedExecutionEvidence{
				RAMApplied:              false,
				FreshPositiveVerified:   false,
				ProtocolDenialVerified:  true,
				SessionHandlingVerified: true,
			},
		}
		_, err := s.mutate(context.Background(), a, in, ActionRevoke)
		if err == nil || !s.poisoned {
			t.Fatal("missing RAMApplied must poison and fail closed")
		}
	})

	t.Run("Revoke: missing ProtocolDenialVerified fails closed with recovery", func(t *testing.T) {
		s, r, e, a, in := simplifiedServiceFixture(t, ActionRevoke)
		r.autoComplete = true
		e.execResult = SimplifiedExecutionResult{
			OperationID: r.beginResult.Operation.OperationID,
			Outcome:     ExecutionVerifiedSuccess,
			Mutated:     true,
			Evidence: SimplifiedExecutionEvidence{
				RAMApplied:              true,
				FreshPositiveVerified:   false,
				ProtocolDenialVerified:  false,
				SessionHandlingVerified: true,
			},
		}
		_, err := s.mutate(context.Background(), a, in, ActionRevoke)
		if err == nil || !s.poisoned {
			t.Fatal("missing ProtocolDenialVerified must poison and fail closed")
		}
	})

	t.Run("Revoke: contradictory FreshPositiveVerified true fails closed with recovery", func(t *testing.T) {
		s, r, e, a, in := simplifiedServiceFixture(t, ActionRevoke)
		r.autoComplete = true
		e.execResult = SimplifiedExecutionResult{
			OperationID: r.beginResult.Operation.OperationID,
			Outcome:     ExecutionVerifiedSuccess,
			Mutated:     true,
			Evidence: SimplifiedExecutionEvidence{
				RAMApplied:              true,
				FreshPositiveVerified:   true,
				ProtocolDenialVerified:  true,
				SessionHandlingVerified: true,
			},
		}
		_, err := s.mutate(context.Background(), a, in, ActionRevoke)
		if err == nil || !s.poisoned {
			t.Fatal("contradictory FreshPositiveVerified on revoke must poison")
		}
	})

	t.Run("Provision: contradictory SessionHandlingVerified true fails closed with recovery", func(t *testing.T) {
		s, r, e, a, in := simplifiedServiceFixture(t, ActionProvision)
		r.autoComplete = true
		e.execResult = SimplifiedExecutionResult{
			OperationID: r.beginResult.Operation.OperationID,
			Outcome:     ExecutionVerifiedSuccess,
			Mutated:     true,
			Evidence: SimplifiedExecutionEvidence{
				RAMApplied:              true,
				FreshPositiveVerified:   true,
				ProtocolDenialVerified:  false,
				SessionHandlingVerified: true, // Contradictory on password flow
			},
		}
		_, err := s.Provision(context.Background(), a, in)
		if err == nil || !s.poisoned {
			t.Fatal("contradictory SessionHandlingVerified on provision must poison and fail closed")
		}
	})

	t.Run("Provision: contradictory ProtocolDenialVerified true fails closed with recovery", func(t *testing.T) {
		s, r, e, a, in := simplifiedServiceFixture(t, ActionProvision)
		r.autoComplete = true
		e.execResult = SimplifiedExecutionResult{
			OperationID: r.beginResult.Operation.OperationID,
			Outcome:     ExecutionVerifiedSuccess,
			Mutated:     true,
			Evidence: SimplifiedExecutionEvidence{
				RAMApplied:              true,
				FreshPositiveVerified:   true,
				ProtocolDenialVerified:  true, // Contradictory on password flow
				SessionHandlingVerified: false,
			},
		}
		_, err := s.Provision(context.Background(), a, in)
		if err == nil || !s.poisoned {
			t.Fatal("contradictory ProtocolDenialVerified on provision must poison and fail closed")
		}
	})

	t.Run("Rotate: contradictory SessionHandlingVerified true fails closed with recovery", func(t *testing.T) {
		s, r, e, a, in := simplifiedServiceFixture(t, ActionRotate)
		r.autoComplete = true
		e.execResult = SimplifiedExecutionResult{
			OperationID: r.beginResult.Operation.OperationID,
			Outcome:     ExecutionVerifiedSuccess,
			Mutated:     true,
			Evidence: SimplifiedExecutionEvidence{
				RAMApplied:              true,
				FreshPositiveVerified:   true,
				ProtocolDenialVerified:  false,
				SessionHandlingVerified: true, // Contradictory on rotate
			},
		}
		_, err := s.Rotate(context.Background(), a, in)
		if err == nil || !s.poisoned {
			t.Fatal("contradictory SessionHandlingVerified on rotate must poison and fail closed")
		}
	})

	t.Run("Reset: contradictory SessionHandlingVerified true fails closed with recovery", func(t *testing.T) {
		gwID := "test_gateway"
		actor := uuid.New()
		key := uuid.New()
		predOpID := uuid.New()
		resetOpID := uuid.New()

		now := time.Now().UTC()
		predecessorMeta := Metadata{
			GatewayID:         gwID,
			Username:          gwID,
			CredentialVersion: 2,
			Status:            CredentialRecoveryNeeded,
			LastOperationID:   predOpID,
			OperationStatus:   OperationRecoveryNeeded,
		}
		predecessorOp := Operation{
			OperationID:       predOpID,
			ActorUserID:       actor,
			GatewayID:         gwID,
			IdempotencyKey:    uuid.New(),
			Action:            ActionRotate,
			CredentialVersion: 2,
			Status:            OperationRecoveryNeeded,
			Phase:             PhaseRecovery,
			DeliveryStatus:    DeliveryUnknown,
			ExecutionContract: ContractSimplified,
			CreatedAt:         now.Add(-time.Minute),
			UpdatedAt:         now,
			Evidence: VerificationEvidence{
				RAMApplied: true,
			},
		}
		r := &spySimplifiedRepo{
			admin:        true,
			getMeta:      predecessorMeta,
			ambiguityOp:  predecessorOp,
			autoComplete: true,
		}
		e := &spySimplifiedExecutor{
			generate: true,
			execResult: SimplifiedExecutionResult{
				OperationID: resetOpID,
				Outcome:     ExecutionVerifiedSuccess,
				Mutated:     true,
				Evidence: SimplifiedExecutionEvidence{
					RAMApplied:              true,
					FreshPositiveVerified:   true,
					ProtocolDenialVerified:  false,
					SessionHandlingVerified: true, // Contradictory on reset
				},
			},
		}
		resetOp := Operation{
			OperationID:        resetOpID,
			ActorUserID:        actor,
			GatewayID:          gwID,
			IdempotencyKey:     key,
			Action:             ActionReset,
			CredentialVersion:  2,
			Status:             OperationPending,
			Phase:              PhaseIntent,
			ExecutionContract:  ContractSimplified,
			ResetReason:        &[]ResetReason{ResetReasonRecoveryAfterUnknown}[0],
			ResetPredecessorID: &predOpID,
		}
		admittedMeta := Metadata{
			GatewayID:         gwID,
			Username:          gwID,
			CredentialVersion: 2,
			Status:            CredentialRotating,
			LastOperationID:   resetOpID,
			OperationStatus:   OperationPending,
		}
		r.beginResetResult = BeginResult{
			Execute:   true,
			Operation: resetOp,
			Metadata:  admittedMeta,
		}
		s, err := NewSimplifiedProvisionService(r, e, &spyResetPrerequisite{}, SimplifiedProvisionServiceOptions{
			RecoveryTimeout:  time.Second,
			NewOperationID:   func() (uuid.UUID, error) { return resetOpID, nil },
			GeneratePassword: func() (string, error) { return strings.Repeat("a", 48), nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.Reset(context.Background(), actor, ResetInput{
			GatewayID:      gwID,
			IdempotencyKey: key,
			Reason:         ResetReasonRecoveryAfterUnknown,
		})
		if err == nil || !s.poisoned {
			t.Fatalf("contradictory SessionHandlingVerified on reset must poison and fail closed, got err=%v, poisoned=%v, calls=%v", err, s.poisoned, r.calls)
		}
	})

	t.Run("Unchanged failure with non-empty evidence (e.g. SessionHandlingVerified=true) fails closed as recovery not FailKnown", func(t *testing.T) {
		s, r, e, a, in := simplifiedServiceFixture(t, ActionRotate)
		e.execResult = SimplifiedExecutionResult{
			OperationID: r.beginResult.Operation.OperationID,
			Outcome:     ExecutionUnchangedFailure,
			Mutated:     false,
			Evidence: SimplifiedExecutionEvidence{
				SessionHandlingVerified: true, // Non-empty evidence!
			},
		}
		e.execErr = errors.New("error")
		_, err := s.Rotate(context.Background(), a, in)
		if err == nil {
			t.Fatal("expected error")
		}
		if !s.poisoned {
			t.Fatal("non-empty evidence on unchanged failure must NOT qualify as known unchanged failure; must poison")
		}
		if r.calls[len(r.calls)-1] != "RequireRecovery" {
			t.Fatalf("expected RequireRecovery, got calls: %v", r.calls)
		}
	})
}

func TestSimplifiedService_MetadataDispatch(t *testing.T) {
	r := &spySimplifiedRepo{admin: true, getMeta: Metadata{GatewayID: "test_gateway"}}
	s, _ := NewSimplifiedProvisionService(r, &spySimplifiedExecutor{}, &spyResetPrerequisite{}, SimplifiedProvisionServiceOptions{RecoveryTimeout: time.Second})
	m, err := s.Metadata(context.Background(), uuid.New(), "test_gateway")
	if err != nil || m.GatewayID != "test_gateway" || !reflect.DeepEqual(r.calls, []string{"IsPlatformAdmin", "GetMetadata"}) {
		t.Fatalf("dispatch: %v %v", m, err)
	}
	r.adminErr = errors.New("private diagnostic")
	if _, err = s.Metadata(context.Background(), uuid.New(), "test_gateway"); err == nil || err.Error() != string(CodeServiceUnavailable) {
		t.Fatal("unsafe admin error")
	}
}

func TestSimplifiedService_ConstructorAndGuards(t *testing.T) {
	r, e, p := &spySimplifiedRepo{}, &spySimplifiedExecutor{}, &spyResetPrerequisite{}
	for _, timeout := range []time.Duration{0, -time.Second, 2 * time.Minute} {
		if _, err := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{RecoveryTimeout: timeout}); err == nil {
			t.Fatal("timeout")
		}
	}
	var nilRepo *spySimplifiedRepo
	if _, err := NewSimplifiedProvisionService(nilRepo, e, p, SimplifiedProvisionServiceOptions{RecoveryTimeout: time.Second}); err == nil {
		t.Fatal("typed nil")
	}
	s, r, _, a, in := simplifiedServiceFixture(t, ActionProvision)
	if _, err := s.Metadata(context.Background(), uuid.Nil, in.GatewayID); err == nil {
		t.Fatal("invalid metadata")
	}
	r.admin = false
	if _, err := s.Metadata(context.Background(), a, in.GatewayID); err == nil {
		t.Fatal("denied metadata")
	}
	r.admin = true
	r.getMetaErr = &DomainError{Code: CodeNotFound}
	if _, err := s.Metadata(context.Background(), a, in.GatewayID); err == nil {
		t.Fatal("metadata failure")
	}
	if _, err := s.Provision(context.Background(), uuid.Nil, in); err == nil {
		t.Fatal("invalid")
	}
	s.simplifiedCfg.StartupReady = func() bool { return false }
	if _, err := s.Provision(context.Background(), a, in); err == nil {
		t.Fatal("startup")
	}
	s.simplifiedCfg.StartupReady = nil
	r.adminErr = errors.New("private")
	if _, err := s.Provision(context.Background(), a, in); err == nil {
		t.Fatal("admin outage")
	}
	r.adminErr = nil
	s.simplifiedCfg.NewOperationID = func() (uuid.UUID, error) { return uuid.Nil, nil }
	if _, err := s.Provision(context.Background(), a, in); err == nil {
		t.Fatal("id")
	}
	s.simplifiedCfg.NewOperationID = uuid.NewRandom
	r.beginErr = errors.New("private")
	if _, err := s.Provision(context.Background(), a, in); err == nil || !s.poisoned {
		t.Fatal("begin uncertainty")
	}
	if _, err := s.Reset(context.Background(), uuid.Nil, ResetInput{}); err == nil {
		t.Fatal("reset invalid")
	}
	if _, err := (&ProvisionService{}).Reset(context.Background(), a, ResetInput{GatewayID: in.GatewayID, IdempotencyKey: in.IdempotencyKey, Reason: ResetReasonRecoveryAfterUnknown}); err == nil {
		t.Fatal("legacy reset")
	}
}

func TestSimplifiedService_ResetFlows(t *testing.T) {
	actor := uuid.New()
	key := uuid.New()
	gwID := "test_gateway"
	predOpID := uuid.New()
	resetOpID := uuid.New()

	resetInput := ResetInput{
		GatewayID:      gwID,
		IdempotencyKey: key,
		Reason:         ResetReasonRecoveryAfterUnknown,
	}

	predecessorMeta := Metadata{
		GatewayID:         gwID,
		Username:          gwID,
		CredentialVersion: 2,
		Status:            CredentialRecoveryNeeded,
		LastOperationID:   predOpID,
		OperationStatus:   OperationRecoveryNeeded,
	}

	now := time.Now().UTC()
	predecessorOp := Operation{
		OperationID:       predOpID,
		ActorUserID:       actor,
		GatewayID:         gwID,
		IdempotencyKey:    uuid.New(),
		Action:            ActionRotate,
		CredentialVersion: 2,
		Status:            OperationRecoveryNeeded,
		Phase:             PhaseRecovery,
		DeliveryStatus:    DeliveryUnknown,
		ExecutionContract: ContractSimplified,
		CreatedAt:         now.Add(-time.Minute),
		UpdatedAt:         now,
		Evidence: VerificationEvidence{
			RAMApplied: true,
		},
	}

	t.Run("Historical same-key replay returns metadata with ZERO admission, native, or prerequisite effects", func(t *testing.T) {
		r := &spySimplifiedRepo{admin: true}
		e := &spySimplifiedExecutor{}
		p := &spyResetPrerequisite{}
		s, err := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{RecoveryTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}

		completedAt := time.Now().Add(-10 * time.Minute)
		histOp := Operation{
			OperationID:        resetOpID,
			ActorUserID:        actor,
			GatewayID:          gwID,
			IdempotencyKey:     key,
			Action:             ActionReset,
			CredentialVersion:  3,
			Status:             OperationSucceeded,
			Phase:              PhaseFinalize,
			DeliveryStatus:     DeliveryUnknown,
			ExecutionContract:  ContractSimplified,
			ResetReason:        &resetInput.Reason,
			ResetPredecessorID: &predOpID,
			Evidence: VerificationEvidence{
				RAMApplied:            true,
				FreshPositiveVerified: true,
			},
			CreatedAt:   time.Now().Add(-15 * time.Minute),
			UpdatedAt:   completedAt,
			CompletedAt: &completedAt,
		}
		currentMeta := Metadata{
			GatewayID:         gwID,
			Username:          gwID,
			CredentialVersion: 3,
			Status:            CredentialActive,
			LastOperationID:   resetOpID,
			OperationStatus:   OperationSucceeded,
		}

		r.lookupFound = true
		r.lookupOp = histOp
		r.lookupMeta = currentMeta

		resp, err := s.Reset(context.Background(), actor, resetInput)
		if err != nil {
			t.Fatalf("unexpected reset replay error: %v", err)
		}
		if resp.Secret != nil || resp.SecretReturned {
			t.Fatal("secret must NOT be disclosed on replay")
		}
		if resp.OperationID != resetOpID {
			t.Fatalf("expected opID %v, got %v", resetOpID, resp.OperationID)
		}
		if resp.ReplayedStatus != OperationSucceeded {
			t.Fatalf("expected status %v, got %v", OperationSucceeded, resp.ReplayedStatus)
		}
		if len(p.calls) != 0 {
			t.Fatalf("prerequisite must NOT be called on historical replay: %v", p.calls)
		}
		if len(e.calls) != 0 {
			t.Fatalf("executor must NOT be called on historical replay: %v", e.calls)
		}
		for _, call := range r.calls {
			if strings.HasPrefix(call, "BeginReset") {
				t.Fatalf("BeginReset must NOT be called on historical replay: %v", r.calls)
			}
		}
	})

	t.Run("Pending or recovery-needed same-key returns in-progress error without execution", func(t *testing.T) {
		r := &spySimplifiedRepo{admin: true}
		e := &spySimplifiedExecutor{}
		p := &spyResetPrerequisite{}
		s, _ := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{RecoveryTimeout: time.Second})

		r.lookupErr = &DomainError{Code: CodeOperationInProgress}
		_, err := s.Reset(context.Background(), actor, resetInput)
		if err == nil || err.Error() != string(CodeOperationInProgress) {
			t.Fatalf("expected CodeOperationInProgress, got %v", err)
		}

		r.lookupErr = &DomainError{Code: CodeRecoveryRequired}
		_, err = s.Reset(context.Background(), actor, resetInput)
		if err == nil || err.Error() != string(CodeRecoveryRequired) {
			t.Fatalf("expected CodeRecoveryRequired, got %v", err)
		}
		if len(p.calls) != 0 || len(e.calls) != 0 {
			t.Fatal("prerequisite/executor must not run on in-progress lookup")
		}
	})

	t.Run("Missing or failing prerequisite yields zero admission and zero native mutation", func(t *testing.T) {
		r := &spySimplifiedRepo{admin: true, getMeta: predecessorMeta, ambiguityOp: predecessorOp}
		e := &spySimplifiedExecutor{}
		p := &spyResetPrerequisite{checkErr: errors.New("quiescence failed")}
		s, _ := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{RecoveryTimeout: time.Second})

		_, err := s.Reset(context.Background(), actor, resetInput)
		if err == nil || err.Error() != string(CodeServiceUnavailable) {
			t.Fatalf("expected CodeServiceUnavailable on prerequisite failure, got %v", err)
		}
		if len(p.calls) != 1 {
			t.Fatalf("expected 1 prerequisite call, got %v", p.calls)
		}
		if len(e.calls) != 0 {
			t.Fatal("executor must not run when prerequisite fails")
		}
		for _, call := range r.calls {
			if strings.HasPrefix(call, "BeginReset") {
				t.Fatal("BeginReset must not be called when prerequisite fails")
			}
		}
	})

	t.Run("Target changes during prerequisite rejects without mutation", func(t *testing.T) {
		r := &spySimplifiedRepo{admin: true, getMeta: predecessorMeta, ambiguityOp: predecessorOp}
		e := &spySimplifiedExecutor{}
		p := &spyResetPrerequisite{}
		s, _ := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
			RecoveryTimeout: time.Second,
			NewOperationID:  func() (uuid.UUID, error) { return resetOpID, nil },
		})

		// BeginReset rejects if expected predecessor does not match current DB predecessor
		r.beginResetErr = &DomainError{Code: CodeCredentialConflict}

		_, err := s.Reset(context.Background(), actor, resetInput)
		if err == nil || err.Error() != string(CodeCredentialConflict) {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
		if len(e.calls) != 0 {
			t.Fatal("executor must not run when target changes")
		}
	})

	t.Run("Invalid current predecessor eligibility prevents prerequisite and execution", func(t *testing.T) {
		// If predecessor is currently active (lost_secret_recovery needed, but request has recovery_after_unknown),
		// or conversely, if predecessor is recovery_needed but request is lost_secret_recovery,
		// CheckResetEligibility BEFORE prerequisite must fail closed with CodeCredentialConflict and ZERO prerequisite calls.
		activeMeta := Metadata{
			GatewayID:         gwID,
			Username:          gwID,
			CredentialVersion: 1,
			Status:            CredentialActive,
			LastOperationID:   predOpID,
			OperationStatus:   OperationSucceeded,
		}
		completedAt := time.Now().UTC()
		activeOp := Operation{
			OperationID:       predOpID,
			ActorUserID:       actor,
			GatewayID:         gwID,
			IdempotencyKey:    uuid.New(),
			Action:            ActionProvision,
			CredentialVersion: 1,
			Status:            OperationSucceeded,
			Phase:             PhaseFinalize,
			DeliveryStatus:    DeliveryUnknown,
			CompletedAt:       &completedAt,
			ExecutionContract: ContractSimplified,
			CreatedAt:         completedAt.Add(-time.Minute),
			UpdatedAt:         completedAt,
			Evidence: VerificationEvidence{
				RAMApplied:            true,
				FreshPositiveVerified: true,
			},
		}

		r := &spySimplifiedRepo{
			admin:       true,
			getMeta:     activeMeta,
			ambiguityOp: activeOp,
		}
		e := &spySimplifiedExecutor{}
		p := &spyResetPrerequisite{}
		s, _ := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
			RecoveryTimeout: time.Second,
			NewOperationID:  func() (uuid.UUID, error) { return resetOpID, nil },
		})

		// Request recovery_after_unknown on active credential:
		invalidReq := ResetInput{
			GatewayID:      gwID,
			IdempotencyKey: uuid.New(),
			Reason:         ResetReasonRecoveryAfterUnknown,
		}
		_, err := s.Reset(context.Background(), actor, invalidReq)
		if err == nil || err.Error() != string(CodeCredentialConflict) {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
		if len(p.calls) != 0 {
			t.Fatalf("prerequisite must NOT run when predecessor is ineligible: %v", p.calls)
		}
		if len(e.calls) != 0 {
			t.Fatalf("executor must NOT run when predecessor is ineligible: %v", e.calls)
		}
		for _, call := range r.calls {
			if strings.HasPrefix(call, "BeginReset") {
				t.Fatalf("BeginReset must NOT be called when predecessor is ineligible: %v", r.calls)
			}
		}
	})

	t.Run("SAME-ID state change during prerequisite rejects admission and leaves zero writes", func(t *testing.T) {
		// Test that if the predecessor's status/version changes during prerequisite evaluation,
		// BeginReset rejects under lock with CodeCredentialConflict and zero native mutation.
		r := &spySimplifiedRepo{
			admin:       true,
			getMeta:     predecessorMeta,
			ambiguityOp: predecessorOp,
		}
		e := &spySimplifiedExecutor{}
		p := &spyResetPrerequisite{}
		s, _ := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
			RecoveryTimeout: time.Second,
			NewOperationID:  func() (uuid.UUID, error) { return resetOpID, nil },
		})

		r.beginResetErr = &DomainError{Code: CodeCredentialConflict}
		_, err := s.Reset(context.Background(), actor, resetInput)
		if err == nil || err.Error() != string(CodeCredentialConflict) {
			t.Fatalf("expected CodeCredentialConflict on same-ID state change: %v", err)
		}
		if len(e.calls) != 0 {
			t.Fatal("executor must not run on same-ID state change")
		}
	})

	t.Run("Valid admitted reset executes, finalizes before disclosure, and returns secret", func(t *testing.T) {
		r := &spySimplifiedRepo{
			admin:        true,
			getMeta:      predecessorMeta,
			ambiguityOp:  predecessorOp,
			autoComplete: true,
		}
		e := &spySimplifiedExecutor{
			generate: true,
			execResult: SimplifiedExecutionResult{
				OperationID: resetOpID,
				Outcome:     ExecutionVerifiedSuccess,
				Mutated:     true,
				Evidence: SimplifiedExecutionEvidence{
					RAMApplied:            true,
					FreshPositiveVerified: true,
				},
			},
		}
		p := &spyResetPrerequisite{}

		resetOp := Operation{
			OperationID:        resetOpID,
			ActorUserID:        actor,
			GatewayID:          gwID,
			IdempotencyKey:     key,
			Action:             ActionReset,
			CredentialVersion:  4,
			Status:             OperationPending,
			Phase:              PhaseIntent,
			DeliveryStatus:     DeliveryUnknown,
			ExecutionContract:  ContractSimplified,
			ResetReason:        &resetInput.Reason,
			ResetPredecessorID: &predOpID,
			Previous: &CredentialSnapshot{
				Status:            predecessorMeta.Status,
				CredentialVersion: predecessorMeta.CredentialVersion,
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		admittedMeta := Metadata{
			GatewayID:         gwID,
			Username:          gwID,
			CredentialVersion: 4,
			Status:            CredentialRotating,
			LastOperationID:   resetOpID,
			OperationStatus:   OperationPending,
		}

		r.beginResetResult = BeginResult{
			Execute:   true,
			Operation: resetOp,
			Metadata:  admittedMeta,
		}

		s, err := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
			RecoveryTimeout:  time.Second,
			NewOperationID:   func() (uuid.UUID, error) { return resetOpID, nil },
			GeneratePassword: func() (string, error) { return strings.Repeat("x", 48), nil },
		})
		if err != nil {
			t.Fatal(err)
		}

		resp, err := s.Reset(context.Background(), actor, resetInput)
		if err != nil {
			t.Fatalf("unexpected reset error: %v", err)
		}
		if resp.Secret == nil || !resp.SecretReturned {
			t.Fatal("expected secret disclosure after verified completion")
		}
		rawJSON, err := json.Marshal(resp.Secret)
		if err != nil || !strings.Contains(string(rawJSON), strings.Repeat("x", 48)) {
			t.Fatalf("expected marshaled secret to contain password, got %s", string(rawJSON))
		}
		if resp.OperationID != resetOpID {
			t.Fatalf("expected opID %v, got %v", resetOpID, resp.OperationID)
		}
		if len(p.calls) != 1 {
			t.Fatalf("expected prerequisite called once, got %v", p.calls)
		}
		if len(e.calls) != 1 {
			t.Fatalf("expected executor called once, got %v", e.calls)
		}
	})

	t.Run("Failed execution requires recovery and does NOT disclose secret", func(t *testing.T) {
		r := &spySimplifiedRepo{
			admin:       true,
			getMeta:     predecessorMeta,
			ambiguityOp: predecessorOp,
		}
		e := &spySimplifiedExecutor{
			execResult: SimplifiedExecutionResult{
				OperationID: resetOpID,
				Outcome:     ExecutionRecoveryRequired,
				Mutated:     true,
			},
		}
		p := &spyResetPrerequisite{}

		resetOp := Operation{
			OperationID:        resetOpID,
			ActorUserID:        actor,
			GatewayID:          gwID,
			IdempotencyKey:     key,
			Action:             ActionReset,
			CredentialVersion:  4,
			Status:             OperationPending,
			Phase:              PhaseIntent,
			DeliveryStatus:     DeliveryUnknown,
			ExecutionContract:  ContractSimplified,
			ResetReason:        &resetInput.Reason,
			ResetPredecessorID: &predOpID,
			CreatedAt:          time.Now(),
			UpdatedAt:          time.Now(),
		}
		admittedMeta := Metadata{
			GatewayID:         gwID,
			Username:          gwID,
			CredentialVersion: 4,
			Status:            CredentialRotating,
			LastOperationID:   resetOpID,
			OperationStatus:   OperationPending,
		}

		r.beginResetResult = BeginResult{
			Execute:   true,
			Operation: resetOp,
			Metadata:  admittedMeta,
		}

		s, _ := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
			RecoveryTimeout:  time.Second,
			NewOperationID:   func() (uuid.UUID, error) { return resetOpID, nil },
			GeneratePassword: func() (string, error) { return strings.Repeat("x", 48), nil },
		})

		_, err := s.Reset(context.Background(), actor, resetInput)
		if err == nil || err.Error() != string(CodeRecoveryRequired) {
			t.Fatalf("expected CodeRecoveryRequired, got %v", err)
		}
		if !s.poisoned {
			t.Fatal("service owner must be poisoned on recovery required")
		}
	})

	t.Run("Known unchanged execution failure calls FailKnown without poisoning", func(t *testing.T) {
		r := &spySimplifiedRepo{
			admin:       true,
			getMeta:     predecessorMeta,
			ambiguityOp: predecessorOp,
		}
		e := &spySimplifiedExecutor{
			execResult: SimplifiedExecutionResult{
				OperationID: resetOpID,
				Outcome:     ExecutionUnchangedFailure,
			},
			execErr: errors.New("unchanged private error"),
		}
		p := &spyResetPrerequisite{}

		resetOp := Operation{
			OperationID:        resetOpID,
			ActorUserID:        actor,
			GatewayID:          gwID,
			IdempotencyKey:     key,
			Action:             ActionReset,
			CredentialVersion:  4,
			Status:             OperationPending,
			Phase:              PhaseIntent,
			DeliveryStatus:     DeliveryUnknown,
			ExecutionContract:  ContractSimplified,
			ResetReason:        &resetInput.Reason,
			ResetPredecessorID: &predOpID,
			Previous: &CredentialSnapshot{
				Status:            predecessorMeta.Status,
				CredentialVersion: predecessorMeta.CredentialVersion,
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		admittedMeta := Metadata{
			GatewayID:         gwID,
			Username:          gwID,
			CredentialVersion: 4,
			Status:            CredentialRotating,
			LastOperationID:   resetOpID,
			OperationStatus:   OperationPending,
		}

		r.beginResetResult = BeginResult{
			Execute:   true,
			Operation: resetOp,
			Metadata:  admittedMeta,
		}

		s, _ := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
			RecoveryTimeout:  time.Second,
			NewOperationID:   func() (uuid.UUID, error) { return resetOpID, nil },
			GeneratePassword: func() (string, error) { return strings.Repeat("x", 48), nil },
		})

		_, err := s.Reset(context.Background(), actor, resetInput)
		if err == nil || err.Error() != string(CodeRecoveryRequired) {
			t.Fatalf("expected CodeRecoveryRequired, got %v", err)
		}
		if s.poisoned {
			t.Fatal("known failure must NOT poison service")
		}
		var calledFailKnown bool
		for _, call := range r.calls {
			if call == "FailKnown" {
				calledFailKnown = true
			}
		}
		if !calledFailKnown {
			t.Fatal("expected FailKnown called on unchanged failure")
		}
	})

	t.Run("Mismatched completion readback causes finalization pending and poisons service", func(t *testing.T) {
		r := &spySimplifiedRepo{
			admin:        true,
			getMeta:      predecessorMeta,
			ambiguityOp:  predecessorOp,
			autoComplete: false,
		}
		e := &spySimplifiedExecutor{
			generate: true,
			execResult: SimplifiedExecutionResult{
				OperationID: resetOpID,
				Outcome:     ExecutionVerifiedSuccess,
				Mutated:     true,
				Evidence: SimplifiedExecutionEvidence{
					RAMApplied:            true,
					FreshPositiveVerified: true,
				},
			},
		}
		p := &spyResetPrerequisite{}

		resetOp := Operation{
			OperationID:        resetOpID,
			ActorUserID:        actor,
			GatewayID:          gwID,
			IdempotencyKey:     key,
			Action:             ActionReset,
			CredentialVersion:  4,
			Status:             OperationPending,
			Phase:              PhaseIntent,
			DeliveryStatus:     DeliveryUnknown,
			ExecutionContract:  ContractSimplified,
			ResetReason:        &resetInput.Reason,
			ResetPredecessorID: &predOpID,
			Previous: &CredentialSnapshot{
				Status:            predecessorMeta.Status,
				CredentialVersion: predecessorMeta.CredentialVersion,
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		admittedMeta := Metadata{
			GatewayID:         gwID,
			Username:          gwID,
			CredentialVersion: 4,
			Status:            CredentialRotating,
			LastOperationID:   resetOpID,
			OperationStatus:   OperationPending,
		}

		r.beginResetResult = BeginResult{
			Execute:   true,
			Operation: resetOp,
			Metadata:  admittedMeta,
		}
		// Finalize returns meta, but ambiguity/readback returns error or mismatch
		r.finalizeMeta = Metadata{
			GatewayID:         gwID,
			Username:          gwID,
			CredentialVersion: 4,
			Status:            CredentialActive,
			LastOperationID:   resetOpID,
			OperationStatus:   OperationSucceeded,
		}
		// After conditional finalize, ambiguity lookup is for newOpID
		origAmbiguityOp := predecessorOp
		r.ambiguityHook = func(id uuid.UUID) (Operation, error) {
			if id == resetOpID {
				return Operation{}, errors.New("ambiguity readback failed")
			}
			return origAmbiguityOp, nil
		}

		s, _ := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
			RecoveryTimeout:  time.Second,
			NewOperationID:   func() (uuid.UUID, error) { return resetOpID, nil },
			GeneratePassword: func() (string, error) { return strings.Repeat("x", 48), nil },
		})

		_, err := s.Reset(context.Background(), actor, resetInput)
		if err == nil || err.Error() != string(CodeFinalizationPending) {
			t.Fatalf("expected CodeFinalizationPending on readback mismatch, got %v", err)
		}
		if !s.poisoned {
			t.Fatal("service owner must be poisoned on ambiguous completion")
		}
	})

	t.Run("ReplayOperation unit edge cases for reset", func(t *testing.T) {
		// Valid reset op
		completedAt := time.Now().Add(-10 * time.Minute)
		resetOp := Operation{
			OperationID:        resetOpID,
			ActorUserID:        actor,
			GatewayID:          gwID,
			IdempotencyKey:     key,
			Action:             ActionReset,
			CredentialVersion:  3,
			Status:             OperationSucceeded,
			Phase:              PhaseFinalize,
			DeliveryStatus:     DeliveryUnknown,
			ExecutionContract:  ContractSimplified,
			ResetReason:        &resetInput.Reason,
			ResetPredecessorID: &predOpID,
			Evidence: VerificationEvidence{
				RAMApplied:            true,
				FreshPositiveVerified: true,
			},
			CreatedAt:   time.Now().Add(-15 * time.Minute),
			UpdatedAt:   completedAt,
			CompletedAt: &completedAt,
		}
		curMeta := Metadata{
			GatewayID:         gwID,
			Username:          gwID,
			CredentialVersion: 3,
			Status:            CredentialActive,
			LastOperationID:   resetOpID,
			OperationStatus:   OperationSucceeded,
		}

		// Wrong reason in input
		diffReasonInput := ResetInput{GatewayID: gwID, IdempotencyKey: key, Reason: ResetReasonLostSecretRecovery}
		if _, err := replayResetOperation(resetOp, actor, diffReasonInput, curMeta); err == nil {
			t.Fatal("expected conflict on different reason")
		}

		// Wrong actor
		if _, err := replayResetOperation(resetOp, uuid.New(), resetInput, curMeta); err == nil {
			t.Fatal("expected conflict on different actor")
		}

		// Wrong gateway
		diffGWInput := ResetInput{GatewayID: "other_gw", IdempotencyKey: key, Reason: resetInput.Reason}
		if _, err := replayResetOperation(resetOp, actor, diffGWInput, curMeta); err == nil {
			t.Fatal("expected conflict on different gateway")
		}

		// Resolved by recovery
		recOp := resetOp
		recOp.Status = OperationRecoveryNeeded
		recOp.CompletedAt = nil // recovery-needed has no CompletedAt
		recOp.Phase = PhaseRecovery
		if _, err := replayResetOperation(recOp, actor, resetInput, curMeta); err == nil {
			t.Fatal("expected recovery required error")
		}

		// Current gateway mismatch
		diffMeta := curMeta
		diffMeta.GatewayID = "mismatch"
		if _, err := replayResetOperation(recOp, actor, resetInput, diffMeta); err == nil {
			t.Fatal("expected invalid request on current gateway mismatch")
		}
		if _, err := replayResetOperation(resetOp, actor, resetInput, diffMeta); err == nil {
			t.Fatal("expected invalid request on current gateway mismatch")
		}

		// In-progress status
		pendOp := resetOp
		pendOp.Status = OperationPending
		if _, err := replayResetOperation(pendOp, actor, resetInput, curMeta); err == nil {
			t.Fatal("expected in-progress error")
		}

		// Recovery-needed status
		recNeedOp := resetOp
		recNeedOp.Status = OperationRecoveryNeeded
		if _, err := replayResetOperation(recNeedOp, actor, resetInput, curMeta); err == nil {
			t.Fatal("expected recovery required error")
		}

		// Revoked current metadata
		revMeta := curMeta
		revMeta.Status = CredentialRevoked
		res, err := replayResetOperation(resetOp, actor, resetInput, revMeta)
		if err != nil || res.NextAction != "provision_with_new_idempotency_key" {
			t.Fatalf("expected provision_with_new_idempotency_key on revoked, got %+v", res)
		}
	})

	t.Run("Startup not ready blocks reset", func(t *testing.T) {
		r := &spySimplifiedRepo{admin: true}
		e := &spySimplifiedExecutor{}
		p := &spyResetPrerequisite{}
		s, _ := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
			RecoveryTimeout: time.Second,
			StartupReady:    func() bool { return false },
		})
		_, err := s.Reset(context.Background(), actor, resetInput)
		if err == nil || err.Error() != string(CodeServiceUnavailable) {
			t.Fatalf("expected CodeServiceUnavailable when startup not ready, got %v", err)
		}
	})

	t.Run("Additional reset error and branch coverage", func(t *testing.T) {
		// 1. Invalid input to replayResetOperation
		if _, err := replayResetOperation(Operation{}, uuid.Nil, ResetInput{}, Metadata{}); err == nil {
			t.Fatal("expected error for nil actor in replayResetOperation")
		}

		// 2. Admin error in Reset
		r := &spySimplifiedRepo{adminErr: errors.New("admin db down")}
		e := &spySimplifiedExecutor{}
		p := &spyResetPrerequisite{}
		s, _ := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{RecoveryTimeout: time.Second})
		if _, err := s.Reset(context.Background(), actor, resetInput); err == nil || err.Error() != string(CodeServiceUnavailable) {
			t.Fatal("expected service unavailable on admin error")
		}

		// 3. NewOperationID failure in Reset
		r = &spySimplifiedRepo{admin: true, getMeta: predecessorMeta, ambiguityOp: predecessorOp}
		s, _ = NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
			RecoveryTimeout: time.Second,
			NewOperationID:  func() (uuid.UUID, error) { return uuid.Nil, errors.New("entropy error") },
		})
		if _, err := s.Reset(context.Background(), actor, resetInput); err == nil || err.Error() != string(CodeInternalError) {
			t.Fatal("expected internal error on NewOperationID error")
		}

		// 4. BeginReset failure poisons service
		r = &spySimplifiedRepo{admin: true, getMeta: predecessorMeta, ambiguityOp: predecessorOp, beginResetErr: errors.New("tx begin error")}
		s, _ = NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
			RecoveryTimeout: time.Second,
			NewOperationID:  func() (uuid.UUID, error) { return resetOpID, nil },
		})
		if _, err := s.Reset(context.Background(), actor, resetInput); err == nil || !s.poisoned {
			t.Fatal("expected poison on BeginReset failure")
		}

		// 5. BeginReset returns Replay branch (simulated)
		histOp := Operation{
			OperationID:        resetOpID,
			ActorUserID:        actor,
			GatewayID:          gwID,
			IdempotencyKey:     key,
			Action:             ActionReset,
			CredentialVersion:  3,
			Status:             OperationSucceeded,
			Phase:              PhaseFinalize,
			DeliveryStatus:     DeliveryUnknown,
			ExecutionContract:  ContractSimplified,
			ResetReason:        &resetInput.Reason,
			ResetPredecessorID: &predOpID,
			Evidence: VerificationEvidence{
				RAMApplied:            true,
				FreshPositiveVerified: true,
			},
			CreatedAt: time.Now().Add(-time.Hour),
			UpdatedAt: time.Now().Add(-time.Hour),
		}
		completedAt := histOp.UpdatedAt
		histOp.CompletedAt = &completedAt
		curMeta := Metadata{
			GatewayID:         gwID,
			Username:          gwID,
			CredentialVersion: 3,
			Status:            CredentialActive,
			LastOperationID:   resetOpID,
			OperationStatus:   OperationSucceeded,
		}
		expectedReplay, _ := replayResetOperation(histOp, actor, resetInput, curMeta)

		r = &spySimplifiedRepo{
			admin:       true,
			getMeta:     predecessorMeta,
			ambiguityOp: predecessorOp,
			beginResetResult: BeginResult{
				Execute:   false,
				Operation: histOp,
				Metadata:  curMeta,
				Replay:    &expectedReplay,
			},
		}
		s, _ = NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
			RecoveryTimeout: time.Second,
			NewOperationID:  func() (uuid.UUID, error) { return resetOpID, nil },
		})
		res, err := s.Reset(context.Background(), actor, resetInput)
		if err != nil || res.MutationResult.OperationID != resetOpID {
			t.Fatalf("expected replay from BeginReset branch, got %v, err=%v", res, err)
		}

		// 6. GeneratePassword error during Reset execution
		resetOp := Operation{
			OperationID:        resetOpID,
			ActorUserID:        actor,
			GatewayID:          gwID,
			IdempotencyKey:     key,
			Action:             ActionReset,
			CredentialVersion:  4,
			Status:             OperationPending,
			Phase:              PhaseIntent,
			DeliveryStatus:     DeliveryUnknown,
			ExecutionContract:  ContractSimplified,
			ResetReason:        &resetInput.Reason,
			ResetPredecessorID: &predOpID,
			Previous: &CredentialSnapshot{
				Status:            predecessorMeta.Status,
				CredentialVersion: predecessorMeta.CredentialVersion,
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		admittedMeta := Metadata{
			GatewayID:         gwID,
			Username:          gwID,
			CredentialVersion: 4,
			Status:            CredentialRotating,
			LastOperationID:   resetOpID,
			OperationStatus:   OperationPending,
		}
		r = &spySimplifiedRepo{
			admin:   true,
			getMeta: predecessorMeta,
			beginResetResult: BeginResult{
				Execute:   true,
				Operation: resetOp,
				Metadata:  admittedMeta,
			},
		}
		e = &spySimplifiedExecutor{
			generate: true,
			execResult: SimplifiedExecutionResult{
				OperationID: resetOpID,
				Outcome:     ExecutionUnchangedFailure,
			},
		}
		s, _ = NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
			RecoveryTimeout:  time.Second,
			NewOperationID:   func() (uuid.UUID, error) { return resetOpID, nil },
			GeneratePassword: func() (string, error) { return "", errors.New("gen failed") },
		})
		if _, err := s.Reset(context.Background(), actor, resetInput); err == nil || err.Error() != string(CodeRecoveryRequired) {
			t.Fatal("expected recovery required on password generator error")
		}

		// 7. CompleteOperation error in recovery path
		r = &spySimplifiedRepo{
			admin:   true,
			getMeta: predecessorMeta,
			beginResetResult: BeginResult{
				Execute:   true,
				Operation: resetOp,
				Metadata:  admittedMeta,
			},
			failKnownErr: errors.New("failKnown db error"),
		}
		s, _ = NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
			RecoveryTimeout:  time.Second,
			NewOperationID:   func() (uuid.UUID, error) { return resetOpID, nil },
			GeneratePassword: func() (string, error) { return strings.Repeat("x", 48), nil },
		})
		if _, err := s.Reset(context.Background(), actor, resetInput); err == nil || !s.poisoned {
			t.Fatal("expected poison when FailKnown fails")
		}

		// 8. ResetPrerequisite nil
		sWithoutPrereq, _ := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
			RecoveryTimeout: time.Second,
		})
		sWithoutPrereq.resetPrereq = nil
		r.getMeta = predecessorMeta
		r.ambiguityOp = predecessorOp
		if _, err := sWithoutPrereq.Reset(context.Background(), actor, resetInput); err == nil || err.Error() != string(CodeServiceUnavailable) {
			t.Fatal("expected service unavailable when resetPrereq is nil")
		}
	})
}

func TestSimplifiedService_AdmissionErrorPoisonRemediation(t *testing.T) {
	// Table of safe domain error codes that must NOT poison the service when admission result is zero.
	allowedSafeCodes := []ErrorCode{
		CodeInvalidRequest,
		CodeForbidden,
		CodeNotFound,
		CodeCredentialConflict,
		CodeIdempotencyConflict,
		CodeOperationInProgress,
	}

	t.Run("Allowed codes do not poison on mutateSimplified and allow subsequent valid request", func(t *testing.T) {
		for _, code := range allowedSafeCodes {
			t.Run(string(code), func(t *testing.T) {
				s, r, e, a, in := simplifiedServiceFixture(t, ActionProvision)
				r.beginErr = &DomainError{Code: code}
				r.beginResult = BeginResult{}

				// First request rejected with safe domain error
				_, err := s.Provision(context.Background(), a, in)
				if err == nil || err.Error() != string(code) {
					t.Fatalf("expected error code %s, got %v", code, err)
				}
				if s.poisoned {
					t.Fatalf("service must NOT be poisoned on trusted admission error %s", code)
				}
				if len(e.calls) != 0 {
					t.Fatalf("executor must not run on denied admission: %v", e.calls)
				}

				// SAME service instance executes an independent valid request successfully
				validKey := uuid.New()
				validIn := MutationInput{
					GatewayID:      in.GatewayID,
					IdempotencyKey: validKey,
					Action:         ActionProvision,
				}
				validOpID := uuid.New()
				s.simplifiedCfg.NewOperationID = func() (uuid.UUID, error) { return validOpID, nil }
				validOp := Operation{
					OperationID:       validOpID,
					ActorUserID:       a,
					GatewayID:         in.GatewayID,
					IdempotencyKey:    validKey,
					Action:            ActionProvision,
					CredentialVersion: 2,
					Status:            OperationPending,
					Phase:             PhaseIntent,
					DeliveryStatus:    DeliveryUnknown,
					ExecutionContract: ContractSimplified,
					CreatedAt:         time.Now(),
					UpdatedAt:         time.Now(),
				}
				validMeta := Metadata{
					GatewayID:         in.GatewayID,
					Username:          in.GatewayID,
					CredentialVersion: 2,
					Status:            CredentialProvisioning,
					LastOperationID:   validOpID,
					OperationStatus:   OperationPending,
				}
				r.beginErr = nil
				r.autoComplete = true
				r.beginResult = BeginResult{
					Execute:   true,
					Operation: validOp,
					Metadata:  validMeta,
				}
				e.execResult = SimplifiedExecutionResult{
					OperationID: validOpID,
					Outcome:     ExecutionVerifiedSuccess,
					Mutated:     true,
					Evidence: SimplifiedExecutionEvidence{
						RAMApplied:            true,
						FreshPositiveVerified: true,
					},
				}

				out, err := s.Provision(context.Background(), a, validIn)
				if err != nil {
					t.Fatalf("subsequent valid request must succeed, got err: %v", err)
				}
				if out.Secret == nil {
					t.Fatal("expected secret on valid provision")
				}
			})
		}
	})

	t.Run("Allowed codes do not poison on BeginReset and allow subsequent valid reset", func(t *testing.T) {
		gwID := "test_gateway"
		actor := uuid.New()
		key1 := uuid.New()
		predOpID := uuid.New()
		resetOpID1 := uuid.New()
		resetOpID2 := uuid.New()

		predecessorMeta := Metadata{
			GatewayID:         gwID,
			Username:          gwID,
			CredentialVersion: 1,
			Status:            CredentialActive,
			LastOperationID:   predOpID,
			OperationStatus:   OperationSucceeded,
		}
		completedAt := time.Now().UTC()
		predecessorOp := Operation{
			OperationID:       predOpID,
			ActorUserID:       actor,
			GatewayID:         gwID,
			IdempotencyKey:    uuid.New(),
			Action:            ActionProvision,
			CredentialVersion: 1,
			Status:            OperationSucceeded,
			Phase:             PhaseFinalize,
			DeliveryStatus:    DeliveryUnknown,
			CompletedAt:       &completedAt,
			ExecutionContract: ContractSimplified,
			CreatedAt:         completedAt.Add(-time.Minute),
			UpdatedAt:         completedAt,
			Evidence: VerificationEvidence{
				RAMApplied:            true,
				FreshPositiveVerified: true,
			},
		}

		for _, code := range allowedSafeCodes {
			t.Run(string(code), func(t *testing.T) {
				resetInput1 := ResetInput{
					GatewayID:      gwID,
					IdempotencyKey: key1,
					Reason:         ResetReasonLostSecretRecovery,
				}

				opIDs := []uuid.UUID{resetOpID1, resetOpID2}
				opIdx := 0
				newOpIDFunc := func() (uuid.UUID, error) {
					if opIdx < len(opIDs) {
						id := opIDs[opIdx]
						opIdx++
						return id, nil
					}
					return uuid.New(), nil
				}

				r := &spySimplifiedRepo{
					admin:         true,
					getMeta:       predecessorMeta,
					ambiguityOp:   predecessorOp,
					beginResetErr: &DomainError{Code: code},
				}
				e := &spySimplifiedExecutor{generate: true}
				p := &spyResetPrerequisite{}

				s, err := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
					RecoveryTimeout:  time.Second,
					NewOperationID:   newOpIDFunc,
					GeneratePassword: func() (string, error) { return strings.Repeat("s", 48), nil },
				})
				if err != nil {
					t.Fatal(err)
				}

				// Attempt 1: Denied by BeginReset with allowed code
				_, err = s.Reset(context.Background(), actor, resetInput1)
				if err == nil || err.Error() != string(code) {
					t.Fatalf("expected error %s, got %v", code, err)
				}
				if s.poisoned {
					t.Fatalf("service must NOT be poisoned on trusted BeginReset error %s", code)
				}
				if len(e.calls) != 0 {
					t.Fatalf("executor must not run on denied BeginReset: %v", e.calls)
				}

				// Attempt 2: Later valid reset on SAME service instance works
				key2 := uuid.New()
				resetInput2 := ResetInput{
					GatewayID:      gwID,
					IdempotencyKey: key2,
					Reason:         ResetReasonLostSecretRecovery,
				}
				r.beginResetErr = nil
				r.autoComplete = true
				r.beginResetResult = BeginResult{
					Execute: true,
					Operation: Operation{
						OperationID:        resetOpID2,
						ActorUserID:        actor,
						GatewayID:          gwID,
						IdempotencyKey:     key2,
						Action:             ActionReset,
						CredentialVersion:  2,
						Status:             OperationPending,
						Phase:              PhaseIntent,
						DeliveryStatus:     DeliveryUnknown,
						ExecutionContract:  ContractSimplified,
						ResetReason:        &resetInput2.Reason,
						ResetPredecessorID: &predOpID,
						Previous: &CredentialSnapshot{
							Status:            predecessorMeta.Status,
							CredentialVersion: predecessorMeta.CredentialVersion,
						},
						CreatedAt: time.Now(),
						UpdatedAt: time.Now(),
					},
					Metadata: Metadata{
						GatewayID:         gwID,
						Username:          gwID,
						CredentialVersion: 2,
						Status:            CredentialRotating,
						LastOperationID:   resetOpID2,
						OperationStatus:   OperationPending,
					},
				}
				e.execResult = SimplifiedExecutionResult{
					OperationID: resetOpID2,
					Outcome:     ExecutionVerifiedSuccess,
					Mutated:     true,
					Evidence: SimplifiedExecutionEvidence{
						RAMApplied:            true,
						FreshPositiveVerified: true,
					},
				}

				out, err := s.Reset(context.Background(), actor, resetInput2)
				if err != nil {
					t.Fatalf("subsequent valid reset must succeed, got err: %v", err)
				}
				if out.Secret == nil {
					t.Fatal("expected secret on valid reset")
				}
			})
		}
	})

	t.Run("Uncertainty and untrusted errors preserve conservative poison", func(t *testing.T) {
		t.Run("CommitOutcomeUnknown latches poison and second request busy", func(t *testing.T) {
			s, r, e, a, in := simplifiedServiceFixture(t, ActionProvision)
			r.beginErr = &CommitOutcomeUnknown{OperationID: uuid.New()}
			r.beginResult = BeginResult{}

			_, err := s.Provision(context.Background(), a, in)
			if err == nil {
				t.Fatal("expected error")
			}
			if !s.poisoned {
				t.Fatal("service must be poisoned on CommitOutcomeUnknown")
			}
			if len(e.calls) != 0 {
				t.Fatal("executor must not run")
			}

			// Second request must fail with RuntimeBusy and zero native execution
			_, err2 := s.Provision(context.Background(), a, MutationInput{
				GatewayID:      in.GatewayID,
				IdempotencyKey: uuid.New(),
				Action:         ActionProvision,
			})
			if err2 == nil || err2.Error() != string(CodeRuntimeBusy) {
				t.Fatalf("expected CodeRuntimeBusy on poisoned service, got %v", err2)
			}
			if len(e.calls) != 0 {
				t.Fatal("executor must still not run on poisoned service")
			}
		})

		t.Run("Wrapped CommitOutcomeUnknown dominates conflict and poisons", func(t *testing.T) {
			s, r, _, a, in := simplifiedServiceFixture(t, ActionProvision)
			// Wrapped CommitOutcomeUnknown joined with a DomainError
			wrappedUnknown := fmt.Errorf("admission failed: %w with conflict: %w",
				&CommitOutcomeUnknown{OperationID: uuid.New()},
				&DomainError{Code: CodeCredentialConflict},
			)
			r.beginErr = wrappedUnknown
			r.beginResult = BeginResult{}

			_, err := s.Provision(context.Background(), a, in)
			if err == nil {
				t.Fatal("expected error")
			}
			if !s.poisoned {
				t.Fatal("wrapped CommitOutcomeUnknown must dominate and poison service")
			}
		})

		t.Run("Generic unavailable/DB error poisons service and second request busy", func(t *testing.T) {
			s, r, _, a, in := simplifiedServiceFixture(t, ActionProvision)
			r.beginErr = errors.New("connection reset by peer")
			r.beginResult = BeginResult{}

			_, err := s.Provision(context.Background(), a, in)
			if err == nil {
				t.Fatal("expected error")
			}
			if !s.poisoned {
				t.Fatal("generic DB/network error must poison service")
			}

			// Second request blocked by poison
			_, err2 := s.Provision(context.Background(), a, in)
			if err2 == nil || err2.Error() != string(CodeRuntimeBusy) {
				t.Fatalf("expected CodeRuntimeBusy, got %v", err2)
			}
		})

		t.Run("Inconsistent non-zero result with denied error remains poison", func(t *testing.T) {
			s, r, _, a, in := simplifiedServiceFixture(t, ActionProvision)
			// Return a safe error BUT with a non-zero BeginResult (implying authority/state change)
			r.beginErr = &DomainError{Code: CodeCredentialConflict}
			r.beginResult = BeginResult{
				Execute: true,
				Operation: Operation{
					OperationID: uuid.New(),
					GatewayID:   in.GatewayID,
				},
			}

			_, err := s.Provision(context.Background(), a, in)
			if err == nil {
				t.Fatal("expected error")
			}
			if !s.poisoned {
				t.Fatal("non-zero BeginResult accompanied by error must poison service")
			}
		})

		t.Run("Inconsistent non-zero metadata with denied error remains poison", func(t *testing.T) {
			s, r, _, a, in := simplifiedServiceFixture(t, ActionProvision)
			r.beginErr = &DomainError{Code: CodeIdempotencyConflict}
			r.beginResult = BeginResult{
				Metadata: Metadata{
					GatewayID: in.GatewayID,
				},
			}

			_, err := s.Provision(context.Background(), a, in)
			if err == nil {
				t.Fatal("expected error")
			}
			if !s.poisoned {
				t.Fatal("non-zero Metadata accompanied by error must poison service")
			}
		})

		t.Run("Inconsistent non-zero replay with denied error remains poison", func(t *testing.T) {
			s, r, _, a, in := simplifiedServiceFixture(t, ActionProvision)
			r.beginErr = &DomainError{Code: CodeNotFound}
			r.beginResult = BeginResult{
				Replay: &MutationResult{},
			}

			_, err := s.Provision(context.Background(), a, in)
			if err == nil {
				t.Fatal("expected error")
			}
			if !s.poisoned {
				t.Fatal("non-zero Replay accompanied by error must poison service")
			}
		})

		t.Run("CodeRecoveryRequired or CodeInternalError are not in safe whitelist and poison", func(t *testing.T) {
			for _, code := range []ErrorCode{CodeRecoveryRequired, CodeInternalError, CodeServiceUnavailable, CodeFinalizationPending} {
				s, r, _, a, in := simplifiedServiceFixture(t, ActionProvision)
				r.beginErr = &DomainError{Code: code}
				r.beginResult = BeginResult{}

				_, err := s.Provision(context.Background(), a, in)
				if err == nil {
					t.Fatal("expected error")
				}
				if !s.poisoned {
					t.Fatalf("code %s must poison service", code)
				}
			}
		})

		t.Run("DB04 conservative error tree validation matrix", func(t *testing.T) {
			type customAsSpoof struct {
				code ErrorCode
			}
			customAs := func(c ErrorCode) error {
				return &spoofDomainError{code: c}
			}

			// Matrix of ambiguous / invalid error trees that MUST poison service
			// Both for ordinary mutateSimplified (e.g. Provision) and BeginReset (Reset)
			poisonCases := []struct {
				name string
				err  error
			}{
				{
					name: "Join conflict and generic error",
					err:  errors.Join(&DomainError{Code: CodeCredentialConflict}, errors.New("connection reset by peer")),
				},
				{
					name: "Join generic error and conflict (reversed order)",
					err:  errors.Join(errors.New("connection reset by peer"), &DomainError{Code: CodeCredentialConflict}),
				},
				{
					name: "Nested wrapper around join with generic error",
					err:  fmt.Errorf("outer wrap: %w", errors.Join(&DomainError{Code: CodeIdempotencyConflict}, errors.New("timeout"))),
				},
				{
					name: "Join conflict and context.Canceled",
					err:  errors.Join(&DomainError{Code: CodeCredentialConflict}, context.Canceled),
				},
				{
					name: "Join conflict and context.DeadlineExceeded",
					err:  errors.Join(&DomainError{Code: CodeCredentialConflict}, context.DeadlineExceeded),
				},
				{
					name: "Join conflict and CodeServiceUnavailable",
					err:  errors.Join(&DomainError{Code: CodeCredentialConflict}, &DomainError{Code: CodeServiceUnavailable}),
				},
				{
					name: "Join CodeInternalError and conflict (reversed order)",
					err:  errors.Join(&DomainError{Code: CodeInternalError}, &DomainError{Code: CodeCredentialConflict}),
				},
				{
					name: "Nested wrapper with CodeInternalError and conflict",
					err:  fmt.Errorf("wrap: %w", errors.Join(&DomainError{Code: CodeInternalError}, &DomainError{Code: CodeCredentialConflict})),
				},
				{
					name: "Custom As spoof matching DomainError code but concrete type is not DomainError",
					err:  customAs(CodeCredentialConflict),
				},
				{
					name: "Join conflict with custom As spoof",
					err:  errors.Join(&DomainError{Code: CodeCredentialConflict}, customAs(CodeInvalidRequest)),
				},
				{
					name: "Nested CommitOutcomeUnknown inside join",
					err:  errors.Join(&DomainError{Code: CodeCredentialConflict}, fmt.Errorf("nested: %w", &CommitOutcomeUnknown{OperationID: uuid.New()})),
				},
				{
					name: "Nil error",
					err:  nil,
				},
				{
					name: "Malformed multi-error with nil child",
					err:  &multiErrWithNilChild{errs: []error{&DomainError{Code: CodeCredentialConflict}, nil}},
				},
				{
					name: "Malformed multi-error with empty slice",
					err:  &multiErrWithNilChild{errs: []error{}},
				},
				{
					name: "Typed nil DomainError direct",
					err:  func() error { var d *DomainError; return d }(),
				},
				{
					name: "Joined typed nil DomainError",
					err:  func() error { var d *DomainError; return errors.Join(&DomainError{Code: CodeCredentialConflict}, d) }(),
				},
				{
					name: "Wrapped typed nil DomainError",
					err:  func() error { var d *DomainError; return fmt.Errorf("wrap: %w", d) }(),
				},
				{
					name: "Typed nil single unwrap receiver",
					err:  (*panicOnUnwrapSingle)(nil),
				},
				{
					name: "Typed nil multi unwrap receiver",
					err:  (*panicOnUnwrapMulti)(nil),
				},
				{
					name: "Single unwrap child is typed nil wrapper",
					err:  &wrapperError{child: (*panicOnUnwrapSingle)(nil)},
				},
				{
					name: "Multi unwrap child is typed nil wrapper",
					err:  &multiWrapperError{children: []error{&DomainError{Code: CodeCredentialConflict}, (*panicOnUnwrapSingle)(nil)}},
				},
				{
					name: "Spoofed As returning true with nil DomainError",
					err:  &spoofNilDomainError{},
				},
				{
					name: "Permanent single cycle",
					err:  newPermanentCycle(),
				},
				{
					name: "Permanent multi cycle",
					err:  newPermanentMultiCycle(),
				},
				{
					name: "Cycle before safe sibling in multi unwrap",
					err:  &multiWrapperError{children: []error{newPermanentCycle(), &DomainError{Code: CodeCredentialConflict}}},
				},
				{
					name: "Shared DAG budget exhaustion",
					err:  makeSharedDAG(10),
				},
				{
					name: "Exact budget negative 65 nodes",
					err:  newDeepError(65, &DomainError{Code: CodeCredentialConflict}),
				},
				{
					name: "Excessive depth exceeding limit",
					err:  newDeepError(70, &DomainError{Code: CodeCredentialConflict}),
				},
			}

			for _, tc := range poisonCases {
				tc := tc
				t.Run("Provision/"+tc.name, func(t *testing.T) {
					s, r, e, a, in := simplifiedServiceFixture(t, ActionProvision)
					r.beginErr = tc.err
					r.beginResult = BeginResult{}

					_, err := s.Provision(context.Background(), a, in)
					if tc.err != nil && err == nil {
						t.Fatal("expected error on non-nil beginErr")
					}
					if !s.poisoned {
						t.Fatalf("case %q MUST poison service", tc.name)
					}

					// Initial executor calls must be ZERO
					if len(e.calls) != 0 {
						t.Fatalf("expected initial executor calls to be 0, got %d", len(e.calls))
					}
					callsBefore := len(e.calls)
					_, err2 := s.Provision(context.Background(), a, MutationInput{
						GatewayID:      in.GatewayID,
						IdempotencyKey: uuid.New(),
						Action:         ActionProvision,
					})
					if err2 == nil || err2.Error() != string(CodeRuntimeBusy) {
						t.Fatalf("expected CodeRuntimeBusy on poisoned service, got %v", err2)
					}
					if len(e.calls) != callsBefore {
						t.Fatalf("executor calls changed on second request: before %d, after %d (MUST be zero calls)", callsBefore, len(e.calls))
					}
				})

				t.Run("Reset/"+tc.name, func(t *testing.T) {
					gwID := "gw-db04-reset-" + uuid.New().String()[:8]
					predOpID := uuid.New()
					resetOpID1 := uuid.New()
					resetOpID2 := uuid.New()
					actor := uuid.New()

					predecessorMeta := Metadata{
						GatewayID:         gwID,
						Username:          gwID,
						CredentialVersion: 1,
						Status:            CredentialActive,
						LastOperationID:   predOpID,
						OperationStatus:   OperationSucceeded,
					}
					completedAt := time.Now().UTC()
					predecessorOp := Operation{
						OperationID:       predOpID,
						ActorUserID:       actor,
						GatewayID:         gwID,
						IdempotencyKey:    uuid.New(),
						Action:            ActionProvision,
						CredentialVersion: 1,
						Status:            OperationSucceeded,
						Phase:             PhaseFinalize,
						DeliveryStatus:    DeliveryUnknown,
						CompletedAt:       &completedAt,
						ExecutionContract: ContractSimplified,
						CreatedAt:         completedAt.Add(-time.Minute),
						UpdatedAt:         completedAt,
						Evidence: VerificationEvidence{
							RAMApplied:            true,
							FreshPositiveVerified: true,
						},
					}

					opIDs := []uuid.UUID{resetOpID1, resetOpID2}
					opIdx := 0
					newOpIDFunc := func() (uuid.UUID, error) {
						if opIdx < len(opIDs) {
							id := opIDs[opIdx]
							opIdx++
							return id, nil
						}
						return uuid.New(), nil
					}

					r := &spySimplifiedRepo{
						admin:         true,
						getMeta:       predecessorMeta,
						ambiguityOp:   predecessorOp,
						beginResetErr: tc.err,
					}
					e := &spySimplifiedExecutor{generate: true}
					p := &spyResetPrerequisite{}

					s, err := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
						RecoveryTimeout:  time.Second,
						NewOperationID:   newOpIDFunc,
						GeneratePassword: func() (string, error) { return strings.Repeat("s", 48), nil },
					})
					if err != nil {
						t.Fatal(err)
					}

					resetInput := ResetInput{
						GatewayID:      gwID,
						IdempotencyKey: uuid.New(),
						Reason:         ResetReasonLostSecretRecovery,
					}

					_, err = s.Reset(context.Background(), actor, resetInput)
					if tc.err != nil && err == nil {
						t.Fatal("expected error on non-nil beginErr")
					}
					if !s.poisoned {
						t.Fatalf("Reset case %q MUST poison service", tc.name)
					}

					// Initial executor calls must be ZERO
					if len(e.calls) != 0 {
						t.Fatalf("expected initial executor calls to be 0 on Reset, got %d", len(e.calls))
					}
					callsBefore := len(e.calls)
					_, err2 := s.Reset(context.Background(), actor, ResetInput{
						GatewayID:      gwID,
						IdempotencyKey: uuid.New(),
						Reason:         ResetReasonLostSecretRecovery,
					})
					if err2 == nil || err2.Error() != string(CodeRuntimeBusy) {
						t.Fatalf("expected CodeRuntimeBusy on poisoned service, got %v", err2)
					}
					if len(e.calls) != callsBefore {
						t.Fatalf("executor calls changed on second request: before %d, after %d (MUST be zero calls)", callsBefore, len(e.calls))
					}
				})
			}

			// Positive cases: single wrapped allowed DomainError AND joined all-allowed DomainErrors
			// MUST NOT poison service, and subsequent valid request MUST succeed.
			positiveCases := []struct {
				name string
				err  error
			}{
				{
					name: "Exact budget positive 64 nodes",
					err:  newDeepError(63, &DomainError{Code: CodeCredentialConflict}), // 63 deep nodes + 1 leaf = 64 nodes
				},
				{
					name: "Single wrapped allowed DomainError (fmt.Errorf %w)",
					err:  fmt.Errorf("admission failed: %w", &DomainError{Code: CodeCredentialConflict}),
				},
				{
					name: "Double wrapped allowed DomainError",
					err:  fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", &DomainError{Code: CodeIdempotencyConflict})),
				},
				{
					name: "Joined all-allowed DomainErrors (two leaves)",
					err:  errors.Join(&DomainError{Code: CodeCredentialConflict}, &DomainError{Code: CodeForbidden}),
				},
				{
					name: "Joined all-allowed DomainErrors (three leaves)",
					err:  errors.Join(&DomainError{Code: CodeNotFound}, &DomainError{Code: CodeInvalidRequest}, &DomainError{Code: CodeOperationInProgress}),
				},
				{
					name: "Joined wrapped all-allowed DomainErrors",
					err:  errors.Join(fmt.Errorf("wrap1: %w", &DomainError{Code: CodeCredentialConflict}), fmt.Errorf("wrap2: %w", &DomainError{Code: CodeIdempotencyConflict})),
				},
			}

			for _, tc := range positiveCases {
				tc := tc
				t.Run("Positive/Provision/"+tc.name, func(t *testing.T) {
					s, r, e, a, in := simplifiedServiceFixture(t, ActionProvision)
					r.beginErr = tc.err
					r.beginResult = BeginResult{}

					_, err := s.Provision(context.Background(), a, in)
					if err == nil {
						t.Fatal("expected error on non-nil beginErr")
					}
					if s.poisoned {
						t.Fatalf("positive case %q MUST NOT poison service", tc.name)
					}

					// Subsequent request MUST succeed
					validKey := uuid.New()
					in2 := MutationInput{
						GatewayID:      in.GatewayID,
						IdempotencyKey: validKey,
						Action:         ActionProvision,
					}
					opID2 := uuid.New()
					s.simplifiedCfg.NewOperationID = func() (uuid.UUID, error) { return opID2, nil }
					validOp := Operation{
						OperationID:       opID2,
						ActorUserID:       a,
						GatewayID:         in.GatewayID,
						IdempotencyKey:    validKey,
						Action:            ActionProvision,
						CredentialVersion: 2,
						Status:            OperationPending,
						Phase:             PhaseIntent,
						DeliveryStatus:    DeliveryUnknown,
						ExecutionContract: ContractSimplified,
						CreatedAt:         time.Now(),
						UpdatedAt:         time.Now(),
					}
					validMeta := Metadata{
						GatewayID:         in.GatewayID,
						Username:          in.GatewayID,
						CredentialVersion: 2,
						Status:            CredentialProvisioning,
						LastOperationID:   opID2,
						OperationStatus:   OperationPending,
					}
					r.beginErr = nil
					r.autoComplete = true
					r.beginResult = BeginResult{
						Execute:   true,
						Operation: validOp,
						Metadata:  validMeta,
					}
					e.execResult = SimplifiedExecutionResult{
						OperationID: opID2,
						Outcome:     ExecutionVerifiedSuccess,
						Mutated:     true,
						Evidence: SimplifiedExecutionEvidence{
							RAMApplied:            true,
							FreshPositiveVerified: true,
						},
					}
					out, err2 := s.Provision(context.Background(), a, in2)
					if err2 != nil {
						t.Fatalf("subsequent valid request must succeed, got err: %v", err2)
					}
					if out.Secret == nil {
						t.Fatal("expected secret on valid request")
					}
				})

				t.Run("Positive/Reset/"+tc.name, func(t *testing.T) {
					gwID := "gw-db04-posreset-" + uuid.New().String()[:8]
					predOpID := uuid.New()
					resetOpID1 := uuid.New()
					resetOpID2 := uuid.New()
					actor := uuid.New()

					predecessorMeta := Metadata{
						GatewayID:         gwID,
						Username:          gwID,
						CredentialVersion: 1,
						Status:            CredentialActive,
						LastOperationID:   predOpID,
						OperationStatus:   OperationSucceeded,
					}
					completedAt := time.Now().UTC()
					predecessorOp := Operation{
						OperationID:       predOpID,
						ActorUserID:       actor,
						GatewayID:         gwID,
						IdempotencyKey:    uuid.New(),
						Action:            ActionProvision,
						CredentialVersion: 1,
						Status:            OperationSucceeded,
						Phase:             PhaseFinalize,
						DeliveryStatus:    DeliveryUnknown,
						CompletedAt:       &completedAt,
						ExecutionContract: ContractSimplified,
						CreatedAt:         completedAt.Add(-time.Minute),
						UpdatedAt:         completedAt,
						Evidence: VerificationEvidence{
							RAMApplied:            true,
							FreshPositiveVerified: true,
						},
					}

					opIDs := []uuid.UUID{resetOpID1, resetOpID2}
					opIdx := 0
					newOpIDFunc := func() (uuid.UUID, error) {
						if opIdx < len(opIDs) {
							id := opIDs[opIdx]
							opIdx++
							return id, nil
						}
						return uuid.New(), nil
					}

					r := &spySimplifiedRepo{
						admin:         true,
						getMeta:       predecessorMeta,
						ambiguityOp:   predecessorOp,
						beginResetErr: tc.err,
					}
					e := &spySimplifiedExecutor{generate: true}
					p := &spyResetPrerequisite{}

					s, err := NewSimplifiedProvisionService(r, e, p, SimplifiedProvisionServiceOptions{
						RecoveryTimeout:  time.Second,
						NewOperationID:   newOpIDFunc,
						GeneratePassword: func() (string, error) { return strings.Repeat("s", 48), nil },
					})
					if err != nil {
						t.Fatal(err)
					}

					resetInput := ResetInput{
						GatewayID:      gwID,
						IdempotencyKey: uuid.New(),
						Reason:         ResetReasonLostSecretRecovery,
					}

					_, err = s.Reset(context.Background(), actor, resetInput)
					if err == nil {
						t.Fatal("expected error on non-nil beginResetErr")
					}
					if s.poisoned {
						t.Fatalf("positive Reset case %q MUST NOT poison service", tc.name)
					}

					// Subsequent request MUST succeed
					validResetKey := uuid.New()
					resetInput2 := ResetInput{
						GatewayID:      gwID,
						IdempotencyKey: validResetKey,
						Reason:         ResetReasonLostSecretRecovery,
					}
					r.beginResetErr = nil
					r.autoComplete = true
					r.beginResetResult = BeginResult{
						Execute: true,
						Operation: Operation{
							OperationID:        resetOpID2,
							ActorUserID:        actor,
							GatewayID:          gwID,
							IdempotencyKey:     validResetKey,
							Action:             ActionReset,
							CredentialVersion:  2,
							Status:             OperationPending,
							Phase:              PhaseIntent,
							DeliveryStatus:     DeliveryUnknown,
							ExecutionContract:  ContractSimplified,
							ResetReason:        &resetInput2.Reason,
							ResetPredecessorID: &predOpID,
							Previous: &CredentialSnapshot{
								Status:            predecessorMeta.Status,
								CredentialVersion: predecessorMeta.CredentialVersion,
							},
							CreatedAt: time.Now(),
							UpdatedAt: time.Now(),
						},
						Metadata: Metadata{
							GatewayID:         gwID,
							Username:          gwID,
							CredentialVersion: 2,
							Status:            CredentialRotating,
							LastOperationID:   resetOpID2,
							OperationStatus:   OperationPending,
						},
					}
					e.execResult = SimplifiedExecutionResult{
						OperationID: resetOpID2,
						Outcome:     ExecutionVerifiedSuccess,
						Mutated:     true,
						Evidence: SimplifiedExecutionEvidence{
							RAMApplied:            true,
							FreshPositiveVerified: true,
						},
					}

					out, err2 := s.Reset(context.Background(), actor, resetInput2)
					if err2 != nil {
						t.Fatalf("subsequent valid reset must succeed, got err: %v", err2)
					}
					if out.Secret == nil {
						t.Fatal("expected secret on valid reset")
					}
				})
			}
		})
	})
}

// Custom error types for testing DB04 error tree boundary cases

type permanentCycleError struct{}

func (p *permanentCycleError) Error() string { return "permanent cycle" }
func (p *permanentCycleError) Unwrap() error { return p }

func newPermanentCycle() error {
	return &permanentCycleError{}
}

type permanentMultiCycleNode struct {
	next *permanentMultiCycleNode
}

func (m *permanentMultiCycleNode) Error() string { return "permanent multi cycle" }
func (m *permanentMultiCycleNode) Unwrap() error { return m.next }

func newPermanentMultiCycle() error {
	a := &permanentMultiCycleNode{}
	b := &permanentMultiCycleNode{}
	a.next = b
	b.next = a
	return a
}

type panicOnUnwrapSingle struct{}

func (p *panicOnUnwrapSingle) Error() string { return "panic single" }
func (p *panicOnUnwrapSingle) Unwrap() error {
	if p == nil {
		panic("panicOnUnwrapSingle called on nil receiver")
	}
	return nil
}

type panicOnUnwrapMulti struct{}

func (p *panicOnUnwrapMulti) Error() string { return "panic multi" }
func (p *panicOnUnwrapMulti) Unwrap() []error {
	if p == nil {
		panic("panicOnUnwrapMulti called on nil receiver")
	}
	return nil
}

type wrapperError struct {
	child error
}

func (w *wrapperError) Error() string { return "wrapper" }
func (w *wrapperError) Unwrap() error { return w.child }

type multiWrapperError struct {
	children []error
}

func (m *multiWrapperError) Error() string   { return "multi wrapper" }
func (m *multiWrapperError) Unwrap() []error { return m.children }

type spoofNilDomainError struct{}

func (s *spoofNilDomainError) Error() string { return "spoof nil" }
func (s *spoofNilDomainError) As(target any) bool {
	if d, ok := target.(**DomainError); ok {
		*d = nil
		return true
	}
	return false
}

type sharedDAGNode struct {
	child error
}

func (n *sharedDAGNode) Error() string   { return "shared DAG" }
func (n *sharedDAGNode) Unwrap() []error { return []error{n.child, n.child} }

func makeSharedDAG(levels int) error {
	var cur error = &DomainError{Code: CodeCredentialConflict}
	for i := 0; i < levels; i++ {
		cur = &sharedDAGNode{child: cur}
	}
	return cur
}

type spoofDomainError struct {
	code ErrorCode
}

func (s *spoofDomainError) Error() string { return string(s.code) }

func (s *spoofDomainError) As(target any) bool {
	if d, ok := target.(**DomainError); ok {
		*d = &DomainError{Code: s.code}
		return true
	}
	return false
}

type multiErrWithNilChild struct {
	errs []error
}

func (m *multiErrWithNilChild) Error() string   { return "multi error" }
func (m *multiErrWithNilChild) Unwrap() []error { return m.errs }

type cycleError struct {
	depth int
	next  *cycleError
}

func newCycleError(child error) *cycleError {
	c := &cycleError{}
	c.next = c // self-cycle
	return c
}

func (c *cycleError) Error() string { return "cycle error" }
func (c *cycleError) Unwrap() error {
	// Standard library errors.As traverses Unwrap() without cycle detection.
	// Allow a bounded number of unwraps (e.g. 100) before terminating so errors.As doesn't hang forever,
	// while our helper's maxNodes=64 limit will detect cycle/excessive depth and fail closed first!
	c.depth++
	if c.depth > 100 {
		return nil
	}
	return c.next
}

type deepError struct {
	depth int
	child error
}

func newDeepError(depth int, leaf error) error {
	var cur error = leaf
	for i := 0; i < depth; i++ {
		cur = &deepError{depth: i, child: cur}
	}
	return cur
}

func (d *deepError) Error() string { return fmt.Sprintf("deep_%d", d.depth) }
func (d *deepError) Unwrap() error { return d.child }

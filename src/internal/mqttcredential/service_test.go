package mqttcredential

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// These doubles model committed state, not unconditional OPEN callbacks.
type provisionRepo struct {
	Repository
	MaintenanceRepository
	admin  bool
	op     Operation
	meta   Metadata
	cp     MaintenanceCheckpoint
	replay bool
	fail   string
	queued Operation
	calls  []string
}

func (r *provisionRepo) IsPlatformAdmin(context.Context, uuid.UUID) (bool, error) {
	return r.admin, nil
}
func (r *provisionRepo) BeginOperation(_ context.Context, q BeginRequest) (BeginResult, error) {
	r.calls = append(r.calls, "intent")
	if r.fail == "begin" {
		return BeginResult{}, &DomainError{Code: CodeNotFound}
	}
	if r.replay {
		if r.op.OperationID == uuid.Nil {
			r.op.OperationID = uuid.New()
		}
		v := MutationResult{Metadata: r.meta, OperationID: r.op.OperationID}
		return BeginResult{Replay: &v}, nil
	}
	at := time.Now().UTC()
	r.op = Operation{OperationID: q.OperationID, ActorUserID: q.ActorUserID, GatewayID: q.Input.GatewayID, IdempotencyKey: q.Input.IdempotencyKey, Action: q.Input.Action, CredentialVersion: 1, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown, CreatedAt: at, UpdatedAt: at}
	r.meta = Metadata{GatewayID: q.Input.GatewayID, Username: q.Input.GatewayID, CredentialVersion: 1, Status: CredentialProvisioning, LastOperationID: q.OperationID, OperationStatus: OperationPending}
	if q.Input.Action == ActionRotate {
		r.op.Previous = &CredentialSnapshot{Status: CredentialActive, CredentialVersion: 1}
		r.op.CredentialVersion = 2
		r.meta.CredentialVersion, r.meta.Status = 2, CredentialRotating
	}
	if q.Input.Action == ActionRevoke {
		r.op.Previous = &CredentialSnapshot{Status: CredentialActive, CredentialVersion: 1}
		r.meta.Status = CredentialRevoking
	}
	r.cp = MaintenanceCheckpoint{OperationID: q.OperationID, GatewayID: q.Input.GatewayID, Status: MaintenanceInProgress}
	return BeginResult{Operation: r.op, Metadata: r.meta, Execute: true}, nil
}
func (r *provisionRepo) GetMetadata(context.Context, string) (Metadata, error) { return r.meta, nil }
func (r *provisionRepo) ResolveCommitAmbiguity(context.Context, uuid.UUID) (Operation, error) {
	if r.fail == "lookup" {
		return Operation{}, errors.New("unavailable")
	}
	return r.op, nil
}
func (r *provisionRepo) ResolveMaintenance(context.Context, uuid.UUID) (MaintenanceCheckpoint, error) {
	return r.cp, nil
}
func (r *provisionRepo) BindMaintenanceEpoch(_ context.Context, g MaintenanceGuard, e string) (MaintenanceCheckpoint, error) {
	r.calls = append(r.calls, "bind")
	r.cp.BrokerEpoch = &e
	return r.cp, nil
}
func (r *provisionRepo) ConditionalFinalize(_ context.Context, u OperationUpdate) (Metadata, error) {
	r.calls = append(r.calls, "finalize")
	if r.fail == "finalize" {
		return Metadata{}, errors.New("unavailable")
	}
	r.op = u.Completion.Operation
	r.meta.Status = u.Completion.Credential.Status
	r.meta.OperationStatus = r.op.Status
	if r.fail == "finalize_unknown" {
		return Metadata{}, &CommitOutcomeUnknown{OperationID: r.op.OperationID}
	}
	return r.meta, nil
}
func (r *provisionRepo) RequireRecovery(_ context.Context, u OperationUpdate) (Metadata, error) {
	r.op = u.Completion.Operation
	r.meta.Status = CredentialRecoveryNeeded
	r.meta.OperationStatus = r.op.Status
	return r.meta, nil
}
func (r *provisionRepo) FailKnown(_ context.Context, u OperationUpdate) (Metadata, error) {
	r.op = u.Completion.Operation
	r.meta.Status = u.Completion.Credential.Status
	r.meta.CredentialVersion = u.Completion.Credential.CredentialVersion
	r.meta.OperationStatus = r.op.Status
	return r.meta, nil
}
func (r *provisionRepo) RequireMaintenanceRecovery(_ context.Context, g MaintenanceGuard, e ErrorCode) (MaintenanceCheckpoint, error) {
	r.calls = append(r.calls, "recovery")
	if r.cp.Status == MaintenanceCompleted {
		return MaintenanceCheckpoint{}, errors.New("immutable")
	}
	r.cp.Status = MaintenanceRecoveryNeeded
	return r.cp, nil
}
func (r *provisionRepo) CompleteMaintenance(_ context.Context, g MaintenanceGuard) (MaintenanceCheckpoint, error) {
	r.calls = append(r.calls, "complete")
	if r.op.Status != OperationSucceeded {
		return MaintenanceCheckpoint{}, errors.New("not finalized")
	}
	if r.fail == "complete" {
		return MaintenanceCheckpoint{}, errors.New("unavailable")
	}
	r.cp.Status = MaintenanceCompleted
	if r.fail == "complete_unknown" {
		return MaintenanceCheckpoint{}, &CommitOutcomeUnknown{OperationID: r.op.OperationID}
	}
	return r.cp, nil
}
func (r *provisionRepo) ListUnresolved(_ context.Context, cursor uuid.UUID, _ int) ([]Operation, error) {
	if r.fail == "scan" {
		return nil, errors.New("unavailable")
	}
	if r.queued.OperationID != uuid.Nil && cursor == uuid.Nil {
		return []Operation{r.queued}, nil
	}
	return []Operation{}, nil
}
func (r *provisionRepo) ListPendingMaintenance(_ context.Context, cursor uuid.UUID, _ int) ([]MaintenanceCheckpoint, error) {
	if r.queued.OperationID != uuid.Nil && cursor == uuid.Nil {
		return []MaintenanceCheckpoint{{OperationID: r.queued.OperationID, GatewayID: r.queued.GatewayID, Status: MaintenanceInProgress}}, nil
	}
	return []MaintenanceCheckpoint{}, nil
}
func (r *provisionRepo) ListDurableRevocations(context.Context, string, int) ([]RevocationDecision, error) {
	return []RevocationDecision{}, nil
}

type provisionRuntime struct {
	repo             *provisionRepo
	result           DynSecAdapterResult
	open, closed     bool
	calls, generated int
	fail             string
}

func (a *provisionRuntime) Execute(ctx context.Context, o Operation, g func(context.Context) (string, error)) (DynSecAdapterResult, error) {
	a.calls++
	a.closed = true
	a.open = false
	if a.repo.cp.OperationID != o.OperationID {
		return DynSecAdapterResult{}, errors.New("intent absent")
	}
	if a.fail == "unchanged" {
		return DynSecAdapterResult{OperationID: o.OperationID, Outcome: ExecutionUnchangedFailure}, ErrInvalidInput
	}
	if o.Action != ActionRevoke {
		a.generated++
		if _, e := g(ctx); e != nil {
			return DynSecAdapterResult{OperationID: o.OperationID, Outcome: ExecutionMaintenanceClosed}, e
		}
	}
	a.result = DynSecAdapterResult{OperationID: o.OperationID, Outcome: ExecutionVerifiedSuccess, Evidence: VerificationEvidence{true, true, true}, receipt: VerificationReceipt{Epoch: string(makeEpoch()), Nonce: "private-challenge"}}
	if o.Action == ActionRevoke {
		a.result.Evidence.FreshPositiveVerified = false
	}
	if a.fail == "partial" {
		a.result.Outcome = ExecutionRecoveryRequired
		return a.result, ErrRecoveryRequired
	}
	return a.result, nil
}

func TestRevokeServiceContracts(t *testing.T) {
	for _, fault := range []string{"", "begin", "denied", "replay", "finalize", "finalize_unknown", "complete", "complete_unknown", "open", "partial", "scan"} {
		t.Run(fault, func(t *testing.T) {
			r := &provisionRepo{admin: fault != "denied", fail: fault, replay: fault == "replay"}
			a := &provisionRuntime{repo: r, fail: fault}
			rngCalls := 0
			s, e := NewProvisionService(r, r, a, ProvisionServiceOptions{RecoveryTimeout: time.Second, PageSize: 1, MaxPages: 4, GeneratePassword: func() (string, error) { rngCalls++; return "", errors.New("private entropy diagnostic") }})
			if e != nil {
				t.Fatal(e)
			}
			v, e := s.Revoke(context.Background(), uuid.New(), MutationInput{"fixture", uuid.New(), ActionRevoke})
			good := fault == "" || fault == "replay" || fault == "finalize_unknown" || fault == "complete_unknown"
			if good != (e == nil) {
				t.Fatalf("revoke classification %v", e)
			}
			if v.SecretReturned || a.generated != 0 || rngCalls != 0 {
				t.Fatal("revoke returned/generated secret")
			}
			if fault == "begin" || fault == "denied" || fault == "replay" {
				if a.calls != 0 {
					t.Fatal("denial/replay executed")
				}
				return
			}
			if good {
				if v.Metadata.Status != CredentialRevoked || v.Metadata.CredentialVersion != 1 || !a.open || r.cp.Status != MaintenanceCompleted {
					t.Fatal("revoke finalization/OPEN")
				}
			} else if a.open || !a.closed {
				t.Fatal("revoke failed open")
			}
		})
	}
}
func makeEpoch() []byte {
	b := make([]byte, 64)
	for i := range b {
		b[i] = 'a'
	}
	return b
}
func (a *provisionRuntime) VerifyRevocations(context.Context, DynSecAdapterResult, []RevocationDecision) error {
	return nil
}
func (a *provisionRuntime) OpenAfterFinalization(_ context.Context, r DynSecAdapterResult, id uuid.UUID) error {
	if a.fail == "open" || a.repo.op.Status != OperationSucceeded || a.repo.cp.BrokerEpoch == nil || *a.repo.cp.BrokerEpoch != r.receipt.Epoch || id != r.OperationID {
		return ErrLifecycleUnavailable
	}
	a.open = true
	a.closed = false
	return nil
}
func (a *provisionRuntime) CloseDrain(context.Context) error {
	a.open = false
	a.closed = true
	return nil
}
func (a *provisionRuntime) VerifyOpen(context.Context, DynSecAdapterResult) error {
	if !a.open {
		return ErrLifecycleUnavailable
	}
	return nil
}
func (a *provisionRuntime) ReleaseClosed(context.Context, uuid.UUID) error {
	if !a.closed {
		return ErrLifecycleUnavailable
	}
	return nil
}

func TestProvisionServiceContracts(t *testing.T) {
	for _, fault := range []string{"", "begin", "finalize", "finalize_unknown", "complete", "complete_unknown", "scan", "open", "partial", "rng", "replay", "denied"} {
		t.Run(fault, func(t *testing.T) {
			r := &provisionRepo{admin: fault != "denied", fail: fault, replay: fault == "replay"}
			a := &provisionRuntime{repo: r, fail: fault}
			generated := 0
			s, e := NewProvisionService(r, r, a, ProvisionServiceOptions{RecoveryTimeout: time.Second, PageSize: 1, MaxPages: 4, GeneratePassword: func() (string, error) {
				generated++
				if !a.closed {
					t.Fatal("RNG before CLOSED")
				}
				if fault == "rng" {
					return "", errors.New("secret diagnostic")
				}
				return "test-password-no-logging", nil
			}})
			if e != nil {
				t.Fatal(e)
			}
			actor := uuid.New()
			input := MutationInput{GatewayID: "fixture", IdempotencyKey: uuid.New(), Action: ActionProvision}
			v, e := s.Provision(context.Background(), actor, input)
			good := fault == "" || fault == "finalize_unknown" || fault == "complete_unknown" || fault == "replay"
			if good != (e == nil) {
				t.Fatalf("unexpected classification: %T", e)
			}
			if fault == "replay" || fault == "denied" || fault == "begin" {
				if generated != 0 || a.calls != 0 {
					t.Fatal("replay/denial executed")
				}
				return
			}
			if good {
				if v.Secret == nil || v.Secret.password == "" || !a.open || r.cp.Status != MaintenanceCompleted {
					t.Fatal("secret before committed OPEN choreography")
				}
			} else {
				if v.Secret != nil || a.open || !a.closed {
					t.Fatal("failure did not close/no secret")
				}
			}
			if a.calls != 1 {
				t.Fatal("runtime replay")
			}
			if fault == "open" || fault == "complete" {
				if r.op.Status != OperationSucceeded || r.cp.Status != MaintenanceRecoveryNeeded {
					t.Fatal("rewrote terminal proof or lost maintenance recovery")
				}
			}
		})
	}
}

func TestRotateServiceContracts(t *testing.T) {
	for _, fault := range []string{"", "begin", "finalize", "finalize_unknown", "complete", "complete_unknown", "scan", "open", "partial", "rng", "replay", "denied", "unchanged"} {
		t.Run(fault, func(t *testing.T) {
			r := &provisionRepo{admin: fault != "denied", fail: fault, replay: fault == "replay"}
			a := &provisionRuntime{repo: r, fail: fault}
			generated := 0
			s, e := NewProvisionService(r, r, a, ProvisionServiceOptions{RecoveryTimeout: time.Second, PageSize: 1, MaxPages: 4, GeneratePassword: func() (string, error) {
				generated++
				if !a.closed {
					t.Fatal("rotate RNG before CLOSED")
				}
				if fault == "rng" {
					return "", errors.New("private secret diagnostic")
				}
				return "fixture-new-password-not-logged", nil
			}})
			if e != nil {
				t.Fatal(e)
			}
			v, e := s.Rotate(context.Background(), uuid.New(), MutationInput{"fixture", uuid.New(), ActionRotate})
			good := fault == "" || fault == "finalize_unknown" || fault == "complete_unknown" || fault == "replay"
			if good != (e == nil) {
				t.Fatalf("rotate classification %T", e)
			}
			if fault == "begin" || fault == "denied" || fault == "replay" {
				if generated != 0 || a.calls != 0 || v.Secret != nil {
					t.Fatal("rotate denial/replay executed")
				}
				return
			}
			if good {
				if v.Secret == nil || !a.open || r.cp.Status != MaintenanceCompleted || r.op.Action != ActionRotate {
					t.Fatal("rotate secret before committed OPEN")
				}
			} else if v.Secret != nil || a.open || !a.closed {
				t.Fatal("rotate failure did not close/no secret")
			}
			if a.calls != 1 {
				t.Fatal("rotate runtime replay")
			}
			if fault == "unchanged" && (r.op.Status != OperationFailed || r.meta.Status != CredentialActive || r.meta.CredentialVersion != 1 || generated != 0 || r.cp.Status != MaintenanceRecoveryNeeded) {
				t.Fatal("unchanged rotation failed to restore prior state with durable closed checkpoint")
			}
		})
	}
}

type blockingRotateRuntime struct {
	*provisionRuntime
	entered, resume chan struct{}
}

type revokedNoopRepository struct{ *provisionRepo }

func (r *revokedNoopRepository) BeginOperation(context.Context, BeginRequest) (BeginResult, error) {
	v := MutationResult{Metadata: r.meta}
	return BeginResult{Metadata: r.meta, Replay: &v}, nil
}

type revokedCheckRuntime struct {
	*provisionRuntime
	invalid bool
	checked int
}

func (a *revokedCheckRuntime) CheckRevoked(context.Context, string) error {
	a.checked++
	if a.invalid {
		return ErrRecoveryRequired
	}
	return nil
}
func TestRevokeNewKeyNoopRequiresCurrentNativeProof(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		r := &revokedNoopRepository{&provisionRepo{admin: true, meta: Metadata{Status: CredentialRevoked, LastOperationID: uuid.New(), CredentialVersion: 1}}}
		a := &revokedCheckRuntime{provisionRuntime: &provisionRuntime{repo: r.provisionRepo}, invalid: invalid}
		s, e := NewProvisionService(r, r, a, ProvisionServiceOptions{RecoveryTimeout: time.Second, PageSize: 1, MaxPages: 4, GeneratePassword: func() (string, error) { t.Fatal("no-op RNG"); return "", nil }})
		if e != nil {
			t.Fatal(e)
		}
		_, e = s.Revoke(context.Background(), uuid.New(), MutationInput{"fixture", uuid.New(), ActionRevoke})
		if invalid != (e != nil) || a.checked != 1 || a.calls != 0 || invalid && (!a.closed || !s.poisoned) {
			t.Fatal("no-op borrowed historical proof")
		}
	}
}

func (a *blockingRotateRuntime) Execute(ctx context.Context, o Operation, g func(context.Context) (string, error)) (DynSecAdapterResult, error) {
	close(a.entered)
	<-a.resume
	return a.provisionRuntime.Execute(ctx, o, g)
}

func TestRotateServiceSingleOwner(t *testing.T) {
	r := &provisionRepo{admin: true}
	a := &blockingRotateRuntime{provisionRuntime: &provisionRuntime{repo: r}, entered: make(chan struct{}), resume: make(chan struct{})}
	s, e := NewProvisionService(r, r, a, ProvisionServiceOptions{RecoveryTimeout: time.Second, PageSize: 1, MaxPages: 4})
	if e != nil {
		t.Fatal(e)
	}
	actor := uuid.New()
	done := make(chan error, 1)
	go func() {
		_, x := s.Rotate(context.Background(), actor, MutationInput{"fixture", uuid.New(), ActionRotate})
		done <- x
	}()
	<-a.entered
	for _, action := range []Action{ActionRotate, ActionProvision, ActionRevoke} {
		if action == ActionRevoke {
			_, x := s.Revoke(context.Background(), actor, MutationInput{"other_fixture", uuid.New(), action})
			var d *DomainError
			if !errors.As(x, &d) || d.Code != CodeRuntimeBusy {
				t.Fatal("concurrent revoke bypassed owner")
			}
			continue
		}
		call := s.Rotate
		if action == ActionProvision {
			call = s.Provision
		}
		_, x := call(context.Background(), actor, MutationInput{"other_fixture", uuid.New(), action})
		var d *DomainError
		if !errors.As(x, &d) || d.Code != CodeRuntimeBusy {
			t.Fatal("shared maintenance owner bypassed")
		}
	}
	close(a.resume)
	if x := <-done; x != nil {
		t.Fatal(x)
	}
	if a.calls != 1 || len(r.calls) != 4 {
		t.Fatal("concurrent admission side effects")
	}
}

func TestProvisionServiceUUIDFailureAndMetadataAuthorization(t *testing.T) {
	r := &provisionRepo{admin: true}
	a := &provisionRuntime{repo: r}
	s, e := NewProvisionService(r, r, a, ProvisionServiceOptions{RecoveryTimeout: time.Second, PageSize: 1, MaxPages: 2, NewOperationID: func() (uuid.UUID, error) { return uuid.Nil, errors.New("entropy failure") }})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.Provision(context.Background(), uuid.New(), MutationInput{"fixture", uuid.New(), ActionProvision})
	var d *DomainError
	if !errors.As(e, &d) || d.Code != CodeInternalError || len(r.calls) != 0 {
		t.Fatal("UUID failure not safe before intent")
	}
	r.admin = false
	_, e = s.Metadata(context.Background(), uuid.New(), "fixture")
	if !errors.As(e, &d) || d.Code != CodeForbidden {
		t.Fatal("metadata missing fresh admin check")
	}
}

func TestProvisionAdapterRevocationsRequirePendingReceipt(t *testing.T) {
	a := &DynSecAdapter{}
	r := DynSecAdapterResult{OperationID: uuid.New(), Outcome: ExecutionVerifiedSuccess}
	if a.VerifyRevocations(context.Background(), r, nil) == nil {
		t.Fatal("unowned receipt admitted")
	}
	a.busy = true
	a.pending = r
	if a.VerifyRevocations(context.Background(), r, []RevocationDecision{{GatewayID: "fixture", OperationID: r.OperationID, CredentialVersion: 1}}) == nil {
		t.Fatal("target superseded decision admitted")
	}
}

func TestProvisionAdapterOpenConsumedChallenge(t *testing.T) {
	ctrl := &provisionController{description: LifecycleDescription{Alive: true, Open: true, Epoch: string(makeEpoch())}}
	a := &DynSecAdapter{cfg: DynSecAdapterConfig{Controller: ctrl}}
	r := DynSecAdapterResult{OperationID: uuid.New(), receipt: VerificationReceipt{Epoch: string(makeEpoch()), Nonce: "consumed"}}
	a.opened = r
	if a.VerifyOpen(context.Background(), r) != nil {
		t.Fatal("OPEN consumes challenge, not epoch")
	}
	r.OperationID = uuid.New()
	if a.VerifyOpen(context.Background(), r) == nil {
		t.Fatal("unacknowledged operation accepted")
	}
}

func TestProvisionServiceAdmissionBoundsAndScanRecovery(t *testing.T) {
	r := &provisionRepo{admin: true}
	a := &provisionRuntime{repo: r}
	s, e := NewProvisionService(r, r, a, ProvisionServiceOptions{RecoveryTimeout: time.Second, PageSize: 1, MaxPages: 1})
	if e != nil {
		t.Fatal(e)
	}
	in := MutationInput{"fixture", uuid.New(), ActionProvision}
	actor := uuid.New()
	s.busy = true
	if _, e = s.Provision(context.Background(), actor, in); e == nil || len(r.calls) != 0 {
		t.Fatal("busy intent admitted")
	}
	s.busy = false
	s.poisoned = true
	if _, e = s.Provision(context.Background(), actor, in); e == nil || len(r.calls) != 0 {
		t.Fatal("poison intent admitted")
	}
	s.poisoned = false
	r.queued = Operation{OperationID: uuid.New(), ActorUserID: actor, GatewayID: "other_fixture", IdempotencyKey: uuid.New(), Action: ActionProvision, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown, CredentialVersion: 1}
	if _, e = s.Provision(context.Background(), actor, in); e == nil || a.open || r.cp.Status != MaintenanceRecoveryNeeded {
		t.Fatal("scan budget exhausted OPEN")
	}
	r.queued.Status = OperationRecoveryNeeded
	if s.reconcile(context.Background(), a.result) == nil {
		t.Fatal("unresolved recovery skipped")
	}
	r.queued = Operation{}
	r.fail = "lookup"
	a.fail = "partial"
	if _, e = s.Provision(context.Background(), actor, MutationInput{"fixture", uuid.New(), ActionProvision}); e == nil || !s.poisoned || a.open {
		t.Fatal("unconfirmed recovery released owner")
	}
}

func TestProvisionServiceOptionsAndInvalidInput(t *testing.T) {
	r := &provisionRepo{admin: true}
	a := &provisionRuntime{repo: r}
	if _, e := NewProvisionService(nil, r, a, ProvisionServiceOptions{}); e == nil {
		t.Fatal("nil dependency")
	}
	s, e := NewProvisionService(r, r, a, ProvisionServiceOptions{RecoveryTimeout: time.Second, PageSize: 1, MaxPages: 2})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Provision(context.Background(), uuid.Nil, MutationInput{}); e == nil {
		t.Fatal("invalid mutation")
	}
	if _, e = s.Metadata(context.Background(), uuid.Nil, "fixture"); e == nil {
		t.Fatal("invalid metadata")
	}
	if _, e = s.Metadata(context.Background(), uuid.New(), "fixture"); e != nil {
		t.Fatal(e)
	}
	if serviceErrorOrNil(nil) != nil {
		t.Fatal("nil error changed")
	}
}

type provisionController struct{ description LifecycleDescription }

func (c *provisionController) Describe(context.Context) (LifecycleDescription, error) {
	return c.description, nil
}
func (c *provisionController) CloseDrain(context.Context) error {
	c.description.Open = false
	return nil
}
func (c *provisionController) RestartClosed(context.Context) (LifecycleDescription, error) {
	return c.description, nil
}
func (c *provisionController) OpenVerified(context.Context, VerificationReceipt) error { return nil }

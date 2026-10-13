package mqttcredential

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// ProvisionRuntime is a trusted local capability, never transport input. One
// service and one adapter own the global maintenance window in a process.
type ProvisionRuntime interface {
	Execute(context.Context, Operation, func(context.Context) (string, error)) (DynSecAdapterResult, error)
	VerifyRevocations(context.Context, DynSecAdapterResult, []RevocationDecision) error
	OpenAfterFinalization(context.Context, DynSecAdapterResult, uuid.UUID) error
	VerifyOpen(context.Context, DynSecAdapterResult) error
	CloseDrain(context.Context) error
	ReleaseClosed(context.Context, uuid.UUID) error
}

// ProvisionServiceOptions configures credential provisioning service parameters and hooks. Function seams are for trusted composition/tests only. Production entropy
// sources must be secure and bounded; the service never starts a reader goroutine.
type ProvisionServiceOptions struct {
	RecoveryTimeout    time.Duration
	PageSize, MaxPages int
	GeneratePassword   func() (string, error)
	NewOperationID     func() (uuid.UUID, error)
	// Task 11 composition must bind the startup barrier before exposing handlers.
	// This never resets a poisoned service or substitutes for admin authorization.
	StartupReady func() bool
}

// SimplifiedOperationRepository defines the narrow repository capabilities required by
// simplified credential operations (provision, rotate, revoke, reset).
type SimplifiedOperationRepository interface {
	IsPlatformAdmin(context.Context, uuid.UUID) (bool, error)
	BeginSimplifiedOperation(context.Context, BeginRequest) (BeginResult, error)
	BeginReset(context.Context, BeginResetRequest) (BeginResult, error)
	ConditionalFinalize(context.Context, OperationUpdate) (Metadata, error)
	FailKnown(context.Context, OperationUpdate) (Metadata, error)
	RequireRecovery(context.Context, OperationUpdate) (Metadata, error)
	GetMetadata(context.Context, string) (Metadata, error)
	ResolveCommitAmbiguity(context.Context, uuid.UUID) (Operation, error)
	LookupResetReplay(context.Context, uuid.UUID, ResetInput) (Operation, Metadata, bool, error)
}

// SimplifiedExecutor defines the narrow executor capabilities for simplified DynSec operations.
type SimplifiedExecutor interface {
	Execute(context.Context, Operation, func(context.Context) (string, error)) (SimplifiedExecutionResult, error)
	CheckRevoked(context.Context, string) error
}

// ResetPrerequisite defines the operator-supervised quiescence check required before admitting a reset.
type ResetPrerequisite interface {
	SupervisedQuiescencePrerequisite(ctx context.Context, gatewayID string) error
}

// SimplifiedProvisionServiceOptions configures simplified credential provisioning options.
type SimplifiedProvisionServiceOptions struct {
	RecoveryTimeout  time.Duration
	GeneratePassword func() (string, error)
	NewOperationID   func() (uuid.UUID, error)
	StartupReady     func() bool
}

// ProvisionResponse holds the mutation result and optional plaintext secret.
type ProvisionResponse struct {
	MutationResult
	Secret *SecretResult
}

// ProvisionService executes credential management operations and coordinates broker runtime.
type ProvisionService struct {
	repo           Repository
	maintenance    MaintenanceRepository
	runtime        ProvisionRuntime
	cfg            ProvisionServiceOptions
	mu             sync.Mutex
	busy, poisoned bool

	// Simplified dependencies (nil when operating on legacy lifecycle path)
	simplifiedRepo     SimplifiedOperationRepository
	simplifiedExecutor SimplifiedExecutor
	resetPrereq        ResetPrerequisite
	simplifiedCfg      SimplifiedProvisionServiceOptions
}

// NewProvisionService constructs a ProvisionService after validating dependencies and options.
func NewProvisionService(r Repository, m MaintenanceRepository, a ProvisionRuntime, c ProvisionServiceOptions) (*ProvisionService, error) {
	if dynSecNil(r) || dynSecNil(m) || dynSecNil(a) || c.RecoveryTimeout <= 0 || c.RecoveryTimeout > time.Minute || c.PageSize < 1 || c.PageSize > 1024 || c.MaxPages < 1 || c.MaxPages > 1024 {
		return nil, &DomainError{Code: CodeInvalidRequest}
	}
	if c.GeneratePassword == nil {
		c.GeneratePassword = generatePassword
	}
	if c.NewOperationID == nil {
		c.NewOperationID = uuid.NewRandom
	}
	return &ProvisionService{repo: r, maintenance: m, runtime: a, cfg: c}, nil
}

// NewSimplifiedProvisionService is a transitional explicit constructor returning the SAME
// *ProvisionService with simplified dependencies to preserve current callers/tests until next wiring cutover.
func NewSimplifiedProvisionService(
	r SimplifiedOperationRepository,
	e SimplifiedExecutor,
	p ResetPrerequisite,
	c SimplifiedProvisionServiceOptions,
) (*ProvisionService, error) {
	if dynSecNil(r) || dynSecNil(e) || dynSecNil(p) || c.RecoveryTimeout <= 0 || c.RecoveryTimeout > time.Minute {
		return nil, &DomainError{Code: CodeInvalidRequest}
	}
	if c.GeneratePassword == nil {
		c.GeneratePassword = generatePassword
	}
	if c.NewOperationID == nil {
		c.NewOperationID = uuid.NewRandom
	}
	return &ProvisionService{
		simplifiedRepo:     r,
		simplifiedExecutor: e,
		resetPrereq:        p,
		simplifiedCfg:      c,
	}, nil
}
func serviceError(e error) error {
	var d *DomainError
	if errors.As(e, &d) {
		return &DomainError{Code: d.Code}
	}
	return &DomainError{Code: CodeServiceUnavailable}
}

func isNilKind(err error) bool {
	if err == nil {
		return true
	}
	val := reflect.ValueOf(err)
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		return val.IsNil()
	default:
		return false
	}
}

// inspectAdmissionError walks the admission error tree within a strict total-work budget (64 nodes).
// It verifies that all leaves are concrete non-nil *DomainError with allowed safe codes.
// It never invokes arbitrary As() or Error(), rejecting typed nil receivers and children prior to Unwrap().
// It returns safeRejection=true and the first safe ErrorCode only if all leaves are valid and safe.
func inspectAdmissionError(err error) (safeRejection bool, publicCode ErrorCode) {
	if isNilKind(err) {
		return false, CodeServiceUnavailable
	}

	remainingBudget := 64
	var firstSafeCode ErrorCode
	allLeavesSafe := true

	type singleUnwrap interface {
		Unwrap() error
	}
	type multiUnwrap interface {
		Unwrap() []error
	}

	var traverse func(curr error)
	traverse = func(curr error) {
		if remainingBudget <= 0 {
			allLeavesSafe = false
			return
		}
		remainingBudget--

		if isNilKind(curr) {
			allLeavesSafe = false
			return
		}

		// Concrete CommitOutcomeUnknown always indicates uncertainty
		if _, isUnknown := curr.(*CommitOutcomeUnknown); isUnknown {
			allLeavesSafe = false
			return
		}

		// Check multi-error unwrap: Unwrap() []error
		if mu, ok := curr.(multiUnwrap); ok {
			children := mu.Unwrap()
			if len(children) == 0 || len(children) > remainingBudget {
				allLeavesSafe = false
				return
			}
			for _, child := range children {
				traverse(child)
				if !allLeavesSafe {
					return
				}
			}
			return
		}

		// Leaf inspection: MUST be concrete *DomainError with allowed safe code
		if d, ok := curr.(*DomainError); ok {
			switch d.Code {
			case CodeInvalidRequest,
				CodeForbidden,
				CodeNotFound,
				CodeCredentialConflict,
				CodeIdempotencyConflict,
				CodeOperationInProgress:
				if firstSafeCode == "" {
					firstSafeCode = d.Code
				}
				return
			default:
				allLeavesSafe = false
				return
			}
		}

		// Check single error unwrap: Unwrap() error
		if su, ok := curr.(singleUnwrap); ok {
			child := su.Unwrap()
			traverse(child)
			return
		}

		allLeavesSafe = false
	}

	traverse(err)

	if allLeavesSafe && firstSafeCode != "" {
		return true, firstSafeCode
	}
	return false, CodeServiceUnavailable
}

// evaluateAdmissionError evaluates admission rejection safely and returns whether it is a safe rejection
// and the corresponding public DomainError. Non-zero BeginResult or unsafe errors poison and fail closed.
func evaluateAdmissionError(b BeginResult, e error) (safeRejection bool, publicErr error) {
	safe, code := inspectAdmissionError(e)
	if !reflect.DeepEqual(b, BeginResult{}) {
		return false, &DomainError{Code: CodeServiceUnavailable}
	}
	if !safe {
		return false, &DomainError{Code: CodeServiceUnavailable}
	}
	return true, &DomainError{Code: code}
}

// Metadata retrieves credential metadata for a gateway if the actor is a platform admin.
func (s *ProvisionService) Metadata(ctx context.Context, actor uuid.UUID, g string) (Metadata, error) {
	if actor == uuid.Nil || gateway.ValidateGatewayID(g) != nil {
		return Metadata{}, &DomainError{Code: CodeInvalidRequest}
	}
	var adminChecker func(context.Context, uuid.UUID) (bool, error)
	var metaGetter func(context.Context, string) (Metadata, error)
	if s.simplifiedRepo != nil {
		adminChecker = s.simplifiedRepo.IsPlatformAdmin
		metaGetter = s.simplifiedRepo.GetMetadata
	} else {
		adminChecker = s.repo.IsPlatformAdmin
		metaGetter = s.repo.GetMetadata
	}
	ok, e := adminChecker(ctx, actor)
	if e != nil {
		return Metadata{}, serviceError(e)
	}
	if !ok {
		return Metadata{}, &DomainError{Code: CodeForbidden}
	}
	m, e := metaGetter(ctx, g)
	return m, serviceErrorOrNil(e)
}
func serviceErrorOrNil(e error) error {
	if e == nil {
		return nil
	}
	return serviceError(e)
}
func operationGuard(o Operation, m Metadata) OperationGuard {
	return OperationGuard{o.GatewayID, o.OperationID, o.Status, m.Status, m.CredentialVersion}
}
func maintenanceGuard(o Operation, m Metadata, c MaintenanceCheckpoint) MaintenanceGuard {
	return MaintenanceGuard{operationGuard(o, m), c.Status, c.BrokerEpoch}
}
func (s *ProvisionService) poison() { s.mu.Lock(); s.poisoned = true; s.mu.Unlock() }

// Provision returns plaintext only in the current request after all committed
// evidence and the exact OPEN receipt. A lost response is metadata-only replay.
// It does not acknowledge USB delivery, fsync, or multi-process/HA ownership.
func (s *ProvisionService) Provision(ctx context.Context, actor uuid.UUID, in MutationInput) (out ProvisionResponse, err error) {
	return s.mutate(ctx, actor, in, ActionProvision)
}

// Rotate replaces only an active Gateway credential. actor is the validated
// principal from trusted composition, never a client-supplied administrative ID.
// Repository admission enforces active status, monotonic attempts and the prior
// maintenance fence. A replay returns current metadata, never a previous secret.
func (s *ProvisionService) Rotate(ctx context.Context, actor uuid.UUID, in MutationInput) (ProvisionResponse, error) {
	return s.mutate(ctx, actor, in, ActionRotate)
}

// Revoke has no secret response and never invokes the password generator.
func (s *ProvisionService) Revoke(ctx context.Context, actor uuid.UUID, in MutationInput) (MutationResult, error) {
	v, e := s.mutate(ctx, actor, in, ActionRevoke)
	return v.MutationResult, e
}

func (s *ProvisionService) mutate(ctx context.Context, actor uuid.UUID, in MutationInput, action Action) (out ProvisionResponse, err error) {
	if s.simplifiedRepo != nil {
		return s.mutateSimplified(ctx, actor, in, action)
	}
	if s.cfg.StartupReady != nil && !s.cfg.StartupReady() {
		return out, &DomainError{Code: CodeServiceUnavailable}
	}
	if ValidateMutationInput(actor, in) != nil || in.Action != action {
		return out, &DomainError{Code: CodeInvalidRequest}
	}
	ok, e := s.repo.IsPlatformAdmin(ctx, actor)
	if e != nil {
		return out, serviceError(e)
	}
	if !ok {
		return out, &DomainError{Code: CodeForbidden}
	}
	s.mu.Lock()
	if s.busy || s.poisoned {
		s.mu.Unlock()
		return out, &DomainError{Code: CodeRuntimeBusy}
	}
	s.busy = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.busy = false; s.mu.Unlock() }()
	id, e := s.cfg.NewOperationID()
	if e != nil || id == uuid.Nil {
		return out, &DomainError{Code: CodeInternalError}
	}
	b, e := s.repo.BeginOperation(ctx, BeginRequest{actor, id, in})
	if e != nil {
		// Even a confirmed intent after ambiguous Begin is NOT permission to rerun
		// runtime. Its atomic unbound checkpoint remains for explicit recovery.
		return out, serviceError(e)
	}
	if b.Replay != nil {
		// A terminal replay describes historical execution only. A new-key no-op
		// (no operation identity) must prove CURRENT native state, not borrow it.
		if action == ActionRevoke && b.Replay.OperationID == uuid.Nil {
			checker, ok := s.runtime.(interface {
				CheckRevoked(context.Context, string) error
			})
			if !ok || b.Metadata.Status != CredentialRevoked || checker.CheckRevoked(ctx, in.GatewayID) != nil {
				recovery, stop := context.WithTimeout(context.Background(), s.cfg.RecoveryTimeout)
				defer stop()
				_ = s.runtime.CloseDrain(recovery)
				s.poison()
				return out, &DomainError{Code: CodeRecoveryRequired}
			}
			latest, x := s.repo.GetMetadata(ctx, in.GatewayID)
			if x != nil || latest.LastOperationID != b.Metadata.LastOperationID || latest.Status != CredentialRevoked || latest.CredentialVersion != b.Metadata.CredentialVersion {
				_ = s.runtime.CloseDrain(ctx)
				s.poison()
				return out, &DomainError{Code: CodeRecoveryRequired}
			}
		}
		out.MutationResult = *b.Replay
		return out, nil
	}
	o, m := b.Operation, b.Metadata
	if !b.Execute || o.Validate() != nil || o.OperationID != id || o.ActorUserID != actor || o.GatewayID != in.GatewayID || o.IdempotencyKey != in.IdempotencyKey || o.Action != action || o.Status != OperationPending || m.LastOperationID != id {
		s.poison()
		return out, &DomainError{Code: CodeInternalError}
	}
	cp, e := s.maintenance.ResolveMaintenance(ctx, id)
	if e != nil || cp.OperationID != id || cp.GatewayID != o.GatewayID || cp.Status != MaintenanceInProgress || cp.BrokerEpoch != nil {
		s.poison()
		return out, &DomainError{Code: CodeRecoveryRequired}
	}
	var secret string
	var runtimeEvidence VerificationEvidence
	unchanged := false
	defer func() { secret = "" }()
	executed := false
	success := false
	defer func() {
		if success {
			return
		}
		// Detached bounded recovery is mandatory even when OPEN might have succeeded
		// and request cancellation/commit uncertainty prevented the next response.
		recovery, stop := context.WithTimeout(context.Background(), s.cfg.RecoveryTimeout)
		defer stop()
		closed := s.runtime.CloseDrain(recovery) == nil
		current, oe := s.repo.ResolveCommitAmbiguity(recovery, id)
		latest, me := s.repo.GetMetadata(recovery, o.GatewayID)
		checkpoint, ce := s.maintenance.ResolveMaintenance(recovery, id)
		if oe != nil || me != nil || ce != nil || current.OperationID != id || latest.LastOperationID != id || checkpoint.OperationID != id {
			s.poison()
			return
		}
		// Never rewrite a terminal event. CLOSED with completed history remains an
		// explicit deployment recovery problem, not fictitious checkpoint rollback.
		if !current.Terminal() {
			outcome := ExecutionRecoveryRequired
			if unchanged {
				outcome = ExecutionUnchangedFailure
			}
			c, x := CompleteOperation(current, outcome, runtimeEvidence, time.Now().UTC())
			if x != nil {
				s.poison()
				return
			}
			if unchanged {
				latest, x = s.repo.FailKnown(recovery, OperationUpdate{operationGuard(current, latest), c})
			} else {
				latest, x = s.repo.RequireRecovery(recovery, OperationUpdate{operationGuard(current, latest), c})
			}
			if x != nil {
				s.poison()
				return
			}
			current = c.Operation
		}
		if checkpoint.Status != MaintenanceCompleted {
			_, x := s.maintenance.RequireMaintenanceRecovery(recovery, maintenanceGuard(current, latest, checkpoint), CodeRecoveryRequired)
			if x != nil {
				s.poison()
				return
			}
		}
		if !closed {
			s.poison()
			return
		}
		if executed && s.runtime.ReleaseClosed(recovery, id) != nil {
			s.poison()
		}
	}()
	executed = true
	var generate func(context.Context) (string, error)
	if action != ActionRevoke {
		generate = func(ctx context.Context) (string, error) {
			if ctx.Err() != nil {
				return "", &DomainError{Code: CodeInternalError}
			}
			p, x := s.cfg.GeneratePassword()
			if x != nil {
				return "", &DomainError{Code: CodeInternalError}
			}
			secret = p
			return p, nil
		}
	}
	result, e := s.runtime.Execute(ctx, o, generate)
	if result.OperationID == id {
		runtimeEvidence = result.Evidence
		// Only explicit adapter proof of no mutation permits restoring the prior
		// version. MaintenanceClosed alone is NOT proof of an unchanged native state.
		unchanged = action == ActionRotate && result.Outcome == ExecutionUnchangedFailure && result.Evidence == (VerificationEvidence{})
	}
	if e != nil || result.OperationID != id || result.Outcome != ExecutionVerifiedSuccess || !validEpoch(result.receipt.Epoch) {
		return out, &DomainError{Code: CodeRecoveryRequired}
	}
	// Execute cold-restarts provision/rotate, but revoke preserves OPEN. Bind the
	// verified lifetime, not
	// an earlier Describe receipt invalidated by CloseDrain/RestartClosed.
	cp, e = s.maintenance.BindMaintenanceEpoch(ctx, maintenanceGuard(o, m, cp), result.receipt.Epoch)
	if e != nil {
		return out, &DomainError{Code: CodeFinalizationPending}
	}
	c, e := CompleteOperation(o, result.Outcome, result.Evidence, time.Now().UTC())
	if e != nil {
		return out, &DomainError{Code: CodeInternalError}
	}
	finalized, e := s.repo.ConditionalFinalize(ctx, OperationUpdate{operationGuard(o, m), c})
	expectedStatus := c.Credential.Status
	if e != nil {
		recovery, stop := context.WithTimeout(context.Background(), s.cfg.RecoveryTimeout)
		confirmed, x := s.repo.ResolveCommitAmbiguity(recovery, id)
		latest, y := s.repo.GetMetadata(recovery, o.GatewayID)
		stop()
		if x != nil || y != nil || !sameCompletion(Completion{Operation: confirmed, Credential: c.Credential}, c) || latest.LastOperationID != id || latest.Status != expectedStatus || latest.OperationStatus != OperationSucceeded {
			return out, &DomainError{Code: CodeFinalizationPending}
		}
		finalized = latest
	}
	m, o = finalized, c.Operation
	if m.LastOperationID != id || m.CredentialVersion != o.CredentialVersion || m.Status != expectedStatus || m.OperationStatus != OperationSucceeded {
		return out, &DomainError{Code: CodeFinalizationPending}
	}
	if e = s.reconcile(ctx, result); e != nil {
		return out, &DomainError{Code: CodeRecoveryRequired}
	}
	if e = s.runtime.OpenAfterFinalization(ctx, result, id); e != nil {
		return out, &DomainError{Code: CodeRecoveryRequired}
	}
	cp, e = s.maintenance.CompleteMaintenance(ctx, maintenanceGuard(o, m, cp))
	if e != nil {
		recovery, stop := context.WithTimeout(context.Background(), s.cfg.RecoveryTimeout)
		confirmed, x := s.maintenance.ResolveMaintenance(recovery, id)
		latest, y := s.repo.GetMetadata(recovery, o.GatewayID)
		stop()
		if x != nil || y != nil || confirmed.OperationID != id || confirmed.GatewayID != o.GatewayID || confirmed.Status != MaintenanceCompleted || confirmed.BrokerEpoch == nil || *confirmed.BrokerEpoch != result.receipt.Epoch || latest.LastOperationID != id || latest.Status != expectedStatus || latest.OperationStatus != OperationSucceeded {
			return out, &DomainError{Code: CodeFinalizationPending}
		}
		cp = confirmed
		m = latest
	}
	if cp.OperationID != id || cp.Status != MaintenanceCompleted || cp.BrokerEpoch == nil || *cp.BrokerEpoch != result.receipt.Epoch || (action != ActionRevoke && secret == "") || s.runtime.VerifyOpen(ctx, result) != nil {
		return out, &DomainError{Code: CodeRecoveryRequired}
	}
	out.MutationResult = MutationResult{Metadata: m, OperationID: id}
	if action != ActionRevoke {
		v := NewSecretResult(m, id, secret)
		out.Secret = &v
		out.SecretReturned = true
	}
	success = true
	return out, nil
}

// Reset deliberately fails closed until the repository offers an actor/key
// read-only reset lookup. BeginReset is mutation admission, not that lookup:
// calling it before quiescence or creating a prerequisite on replay is unsafe.
// The prerequisite seam is retained for subsequent supervised composition; no
// production quiescence or automatic clearing of poison is claimed here.
func replayResetOperation(o Operation, actor uuid.UUID, in ResetInput, current Metadata) (MutationResult, error) {
	if ValidateResetInput(actor, in) != nil {
		return MutationResult{}, &DomainError{Code: CodeInvalidRequest}
	}
	if o.ActorUserID != actor || o.IdempotencyKey != in.IdempotencyKey || o.GatewayID != in.GatewayID || o.Action != ActionReset {
		return MutationResult{}, &DomainError{Code: CodeIdempotencyConflict}
	}
	if o.ResetReason == nil || *o.ResetReason != in.Reason {
		return MutationResult{}, &DomainError{Code: CodeIdempotencyConflict}
	}
	if err := o.Validate(); err != nil {
		return MutationResult{}, err
	}
	switch o.Status {
	case OperationPending:
		return MutationResult{}, &DomainError{Code: CodeOperationInProgress}
	case OperationRecoveryNeeded:
		return MutationResult{}, &DomainError{Code: CodeRecoveryRequired}
	}
	if current.GatewayID != o.GatewayID {
		return MutationResult{}, &DomainError{Code: CodeInvalidRequest}
	}
	result := MutationResult{Metadata: current, OperationID: o.OperationID, ReplayedStatus: o.Status, ErrorCode: o.ErrorCode}
	if o.Status == OperationSucceeded && current.Status != CredentialRevoked {
		result.NextAction = "rotate_with_new_idempotency_key"
	} else if current.Status == CredentialRevoked {
		result.NextAction = "provision_with_new_idempotency_key"
	}
	return result, nil
}

// Reset executes an operator-supervised administrative credential reset for a gateway.
// It enforces platform admin authorization, input validation, service readiness, and
// checks existing replay records before acquiring the serialized service lock.
// When admitted, it verifies predecessor operation eligibility, invokes the configured
// supervised quiescence prerequisite, records intent via BeginReset, executes the
// broker reset sequence, and commits or fails the operation with poisoned recovery handling on ambiguity.
func (s *ProvisionService) Reset(ctx context.Context, actor uuid.UUID, in ResetInput) (ProvisionResponse, error) {
	if ValidateResetInput(actor, in) != nil {
		return ProvisionResponse{}, &DomainError{Code: CodeInvalidRequest}
	}
	if s.simplifiedRepo == nil {
		return ProvisionResponse{}, &DomainError{Code: CodeServiceUnavailable}
	}
	if s.simplifiedCfg.StartupReady != nil && !s.simplifiedCfg.StartupReady() {
		return ProvisionResponse{}, &DomainError{Code: CodeServiceUnavailable}
	}
	ok, err := s.simplifiedRepo.IsPlatformAdmin(ctx, actor)
	if err != nil {
		return ProvisionResponse{}, serviceError(err)
	}
	if !ok {
		return ProvisionResponse{}, &DomainError{Code: CodeForbidden}
	}

	// 1. Check read-only reset lookup before acquiring serialized worker lock.
	// Matching historical replay or pending/recovery status is returned immediately
	// without admission, generator, native execution, or quiescence prerequisite.
	existingOp, currentMeta, found, err := s.simplifiedRepo.LookupResetReplay(ctx, actor, in)
	if err != nil {
		return ProvisionResponse{}, serviceError(err)
	}
	if found {
		if !validSimplifiedMetadata(currentMeta, in.GatewayID) {
			s.poison()
			return ProvisionResponse{}, &DomainError{Code: CodeRecoveryRequired}
		}
		replay, replayErr := replayResetOperation(existingOp, actor, in, currentMeta)
		if replayErr != nil || existingOp.ExecutionContract != ContractSimplified {
			s.poison()
			return ProvisionResponse{}, &DomainError{Code: CodeRecoveryRequired}
		}
		// Safe metadata-only response, zero secret
		return ProvisionResponse{MutationResult: replay}, nil
	}

	// 2. New reset path: acquire serialized service owner lock
	s.mu.Lock()
	if s.busy || s.poisoned {
		s.mu.Unlock()
		return ProvisionResponse{}, &DomainError{Code: CodeRuntimeBusy}
	}
	s.busy = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.busy = false; s.mu.Unlock() }()

	// 3. Inspect current metadata and predecessor operation to bind prerequisite and subsequent admission
	// to exact current predecessor authority and verify pure eligibility BEFORE disruptive prerequisite.
	currentPredecessorMeta, err := s.simplifiedRepo.GetMetadata(ctx, in.GatewayID)
	if err != nil {
		s.poison()
		return ProvisionResponse{}, serviceError(err)
	}
	if !validSimplifiedMetadata(currentPredecessorMeta, in.GatewayID) {
		s.poison()
		return ProvisionResponse{}, &DomainError{Code: CodeRecoveryRequired}
	}
	boundPredecessorID := currentPredecessorMeta.LastOperationID

	// Read exact current operation record via existing read-only capability
	predOp, err := s.simplifiedRepo.ResolveCommitAmbiguity(ctx, boundPredecessorID)
	if err != nil {
		s.poison()
		return ProvisionResponse{}, serviceError(err)
	}
	if predOp.GatewayID != in.GatewayID || predOp.ExecutionContract != ContractSimplified {
		s.poison()
		return ProvisionResponse{}, &DomainError{Code: CodeRecoveryRequired}
	}

	// Validate CheckResetEligibility BEFORE running prerequisite actions
	if err := CheckResetEligibility(currentPredecessorMeta, predOp, in.Reason); err != nil {
		return ProvisionResponse{}, err
	}

	targetGuard := operationGuard(predOp, currentPredecessorMeta)

	// 4. Run operator-supervised quiescence prerequisite dependency
	if s.resetPrereq == nil {
		return ProvisionResponse{}, &DomainError{Code: CodeServiceUnavailable}
	}
	if err := s.resetPrereq.SupervisedQuiescencePrerequisite(ctx, in.GatewayID); err != nil {
		return ProvisionResponse{}, serviceError(err)
	}

	// 5. Admit new reset via BeginReset with expected predecessor and exact target guard
	newOpID, err := s.simplifiedCfg.NewOperationID()
	if err != nil || newOpID == uuid.Nil {
		return ProvisionResponse{}, &DomainError{Code: CodeInternalError}
	}
	b, err := s.simplifiedRepo.BeginReset(ctx, BeginResetRequest{
		ActorUserID:           actor,
		OperationID:           newOpID,
		Input:                 in,
		ExpectedPredecessorID: boundPredecessorID,
		ExpectedTargetGuard:   &targetGuard,
	})
	if err != nil {
		safe, pubErr := evaluateAdmissionError(b, err)
		if !safe {
			s.poison()
		}
		return ProvisionResponse{}, pubErr
	}

	o, m := b.Operation, b.Metadata
	if b.Replay != nil {
		// BeginReset should not return Replay for a new key; if it does, validate strictly
		if b.Execute || !validSimplifiedMetadata(m, in.GatewayID) || b.Replay.SecretReturned || !sameSimplifiedMetadata(b.Replay.Metadata, m) {
			s.poison()
			return ProvisionResponse{}, &DomainError{Code: CodeRecoveryRequired}
		}
		replay, replayErr := replayResetOperation(o, actor, in, m)
		if replayErr != nil || o.ExecutionContract != ContractSimplified || !reflect.DeepEqual(replay, *b.Replay) {
			s.poison()
			return ProvisionResponse{}, &DomainError{Code: CodeRecoveryRequired}
		}
		return ProvisionResponse{MutationResult: *b.Replay}, nil
	}

	// Verify newly admitted reset operation invariants
	if !b.Execute || o.Validate() != nil || o.ExecutionContract != ContractSimplified ||
		o.OperationID != newOpID || o.ActorUserID != actor || o.GatewayID != in.GatewayID ||
		o.IdempotencyKey != in.IdempotencyKey || o.Action != ActionReset || o.Status != OperationPending ||
		o.Phase != PhaseIntent || o.Evidence != (VerificationEvidence{}) ||
		o.ResetReason == nil || *o.ResetReason != in.Reason ||
		o.ResetPredecessorID == nil || *o.ResetPredecessorID != boundPredecessorID ||
		!validSimplifiedMetadata(m, in.GatewayID) || m.LastOperationID != newOpID ||
		m.CredentialVersion != o.CredentialVersion || m.OperationStatus != OperationPending ||
		m.Status != CredentialRotating {
		s.poison()
		return ProvisionResponse{}, &DomainError{Code: CodeRecoveryRequired}
	}

	// 6. Generate CSPRNG password
	var secret string
	defer func() { secret = "" }()
	gen := func(c context.Context) (string, error) {
		if c.Err() != nil || secret != "" {
			return "", &DomainError{Code: CodeInternalError}
		}
		p, err := s.simplifiedCfg.GeneratePassword()
		if err != nil || c.Err() != nil || !validPassword(p) {
			return "", &DomainError{Code: CodeInternalError}
		}
		secret = p
		return p, nil
	}

	// 7. Execute reset on native adapter
	r, execErr := s.simplifiedExecutor.Execute(ctx, o, gen)
	evidence := VerificationEvidence{}
	if r.OperationID == newOpID {
		evidence.RAMApplied, evidence.FreshPositiveVerified = r.Evidence.RAMApplied, r.Evidence.FreshPositiveVerified
	}
	verified := execErr == nil && r.OperationID == newOpID && r.Mutated &&
		r.Outcome == ExecutionVerifiedSuccess && evidence.RAMApplied &&
		evidence.FreshPositiveVerified && !r.Evidence.ProtocolDenialVerified &&
		!r.Evidence.SessionHandlingVerified && validPassword(secret)

	if !verified {
		outcome := ExecutionRecoveryRequired
		known := r.OperationID == newOpID && !r.Mutated && r.Outcome == ExecutionUnchangedFailure && r.Evidence == (SimplifiedExecutionEvidence{})
		if known {
			outcome = ExecutionUnchangedFailure
		} else {
			s.poison()
		}
		c, ce := CompleteOperation(o, outcome, evidence, time.Now().UTC())
		if ce == nil {
			recovery, stop := context.WithTimeout(context.Background(), s.simplifiedCfg.RecoveryTimeout)
			if known {
				_, ce = s.simplifiedRepo.FailKnown(recovery, OperationUpdate{operationGuard(o, m), c})
			} else {
				_, ce = s.simplifiedRepo.RequireRecovery(recovery, OperationUpdate{operationGuard(o, m), c})
			}
			stop()
		}
		if ce != nil {
			s.poison()
		}
		return ProvisionResponse{}, &DomainError{Code: CodeRecoveryRequired}
	}

	// 8. Finalize before secret disclosure
	c, e := CompleteOperation(o, ExecutionVerifiedSuccess, evidence, time.Now().UTC())
	if e != nil {
		s.poison()
		return ProvisionResponse{}, &DomainError{Code: CodeRecoveryRequired}
	}
	finalized, finalizeErr := s.simplifiedRepo.ConditionalFinalize(ctx, OperationUpdate{operationGuard(o, m), c})

	// 9. Exact completion readback before disclosure
	recovery, stop := context.WithTimeout(context.Background(), s.simplifiedCfg.RecoveryTimeout)
	confirmed, oe := s.simplifiedRepo.ResolveCommitAmbiguity(recovery, newOpID)
	latest, me := s.simplifiedRepo.GetMetadata(recovery, in.GatewayID)
	stop()

	if oe != nil || me != nil ||
		!sameCompletion(Completion{Operation: confirmed, Credential: c.Credential}, c) ||
		!validSimplifiedMetadata(latest, in.GatewayID) ||
		latest.LastOperationID != newOpID ||
		latest.CredentialVersion != c.Credential.CredentialVersion ||
		latest.Status != c.Credential.Status ||
		latest.OperationStatus != OperationSucceeded ||
		!reflect.DeepEqual(canonicalSnapshot(CredentialSnapshot{Status: latest.Status, CredentialVersion: latest.CredentialVersion, ActivatedAt: latest.ActivatedAt, RevokedAt: latest.RevokedAt}), canonicalSnapshot(c.Credential)) ||
		latest.LastErrorCode != nil ||
		(finalizeErr == nil && !sameSimplifiedMetadata(finalized, latest)) {
		s.poison()
		return ProvisionResponse{}, &DomainError{Code: CodeFinalizationPending}
	}

	out := ProvisionResponse{
		MutationResult: MutationResult{Metadata: latest, OperationID: newOpID},
	}
	v := NewSecretResult(latest, newOpID, secret)
	out.Secret = &v
	out.SecretReturned = true
	return out, nil
}

func validSimplifiedMetadata(m Metadata, g string) bool {
	return m.GatewayID == g && m.Username == g && m.CredentialVersion > 0 && m.LastOperationID != uuid.Nil && m.Status.Validate() == nil && m.OperationStatus.Validate() == nil
}

func sameSimplifiedMetadata(a, b Metadata) bool {
	canonical := func(m Metadata) Metadata {
		s := canonicalSnapshot(CredentialSnapshot{ActivatedAt: m.ActivatedAt, RevokedAt: m.RevokedAt})
		m.ActivatedAt, m.RevokedAt = s.ActivatedAt, s.RevokedAt
		return m
	}
	return reflect.DeepEqual(canonical(a), canonical(b))
}

func (s *ProvisionService) mutateSimplified(ctx context.Context, actor uuid.UUID, in MutationInput, action Action) (out ProvisionResponse, err error) {
	if ValidateMutationInput(actor, in) != nil || in.Action != action {
		return out, &DomainError{Code: CodeInvalidRequest}
	}
	if s.simplifiedCfg.StartupReady != nil && !s.simplifiedCfg.StartupReady() {
		return out, &DomainError{Code: CodeServiceUnavailable}
	}
	ok, e := s.simplifiedRepo.IsPlatformAdmin(ctx, actor)
	if e != nil {
		return out, serviceError(e)
	}
	if !ok {
		return out, &DomainError{Code: CodeForbidden}
	}
	s.mu.Lock()
	if s.busy || s.poisoned {
		s.mu.Unlock()
		return out, &DomainError{Code: CodeRuntimeBusy}
	}
	s.busy = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.busy = false; s.mu.Unlock() }()
	id, e := s.simplifiedCfg.NewOperationID()
	if e != nil || id == uuid.Nil {
		return out, &DomainError{Code: CodeInternalError}
	}
	b, e := s.simplifiedRepo.BeginSimplifiedOperation(ctx, BeginRequest{actor, id, in})
	if e != nil {
		safe, pubErr := evaluateAdmissionError(b, e)
		if !safe {
			s.poison()
		}
		return out, pubErr
	}
	o, m := b.Operation, b.Metadata
	if b.Replay != nil {
		if b.Execute || !validSimplifiedMetadata(m, in.GatewayID) || b.Replay.SecretReturned || !sameSimplifiedMetadata(b.Replay.Metadata, m) {
			s.poison()
			return out, &DomainError{Code: CodeRecoveryRequired}
		}
		if b.Replay.OperationID == uuid.Nil {
			if action != ActionRevoke || m.Status != CredentialRevoked || s.simplifiedExecutor.CheckRevoked(ctx, in.GatewayID) != nil {
				s.poison()
				return out, &DomainError{Code: CodeRecoveryRequired}
			}
			latest, x := s.simplifiedRepo.GetMetadata(ctx, in.GatewayID)
			if x != nil || !sameSimplifiedMetadata(latest, m) {
				s.poison()
				return out, &DomainError{Code: CodeRecoveryRequired}
			}
		} else {
			replay, x := ReplayOperation(o, actor, in, m)
			if x != nil || o.ExecutionContract != ContractSimplified || !reflect.DeepEqual(replay, *b.Replay) {
				s.poison()
				return out, &DomainError{Code: CodeRecoveryRequired}
			}
		}
		out.MutationResult = *b.Replay
		return out, nil
	}
	expected, x := BeginCredentialStatus("", action)
	if o.Previous != nil {
		expected, x = BeginCredentialStatus(o.Previous.Status, action)
	}
	if !b.Execute || x != nil || o.Validate() != nil || o.ExecutionContract != ContractSimplified || o.OperationID != id || o.ActorUserID != actor || o.GatewayID != in.GatewayID || o.IdempotencyKey != in.IdempotencyKey || o.Action != action || o.Status != OperationPending || o.Phase != PhaseIntent || o.Evidence != (VerificationEvidence{}) || !validSimplifiedMetadata(m, in.GatewayID) || m.LastOperationID != id || m.CredentialVersion != o.CredentialVersion || m.OperationStatus != OperationPending || m.Status != expected {
		s.poison()
		return out, &DomainError{Code: CodeRecoveryRequired}
	}
	var secret string
	defer func() { secret = "" }()
	var gen func(context.Context) (string, error)
	if action != ActionRevoke {
		gen = func(c context.Context) (string, error) {
			if c.Err() != nil || secret != "" {
				return "", &DomainError{Code: CodeInternalError}
			}
			p, err := s.simplifiedCfg.GeneratePassword()
			if err != nil || c.Err() != nil || !validPassword(p) {
				return "", &DomainError{Code: CodeInternalError}
			}
			secret = p
			return p, nil
		}
	}
	r, execErr := s.simplifiedExecutor.Execute(ctx, o, gen)
	evidence := VerificationEvidence{}
	if r.OperationID == id {
		evidence.RAMApplied, evidence.FreshPositiveVerified = r.Evidence.RAMApplied, r.Evidence.FreshPositiveVerified
		// Revoke's protected native-store observation is filesystem evidence,
		// not fsync durability or proof of an exact persisted new password.
		if action == ActionRevoke {
			evidence.SnapshotObserved = r.Evidence.NativePersistedRoleACLObserved
		}
	}
	verified := execErr == nil && r.OperationID == id && r.Mutated && r.Outcome == ExecutionVerifiedSuccess && evidence.RAMApplied
	if action == ActionRevoke {
		verified = verified && r.Evidence.NativeDisabledObserved && r.Evidence.NativePersistedRoleACLObserved && r.Evidence.Qualification.Valid() && !r.Evidence.ProtocolDenialVerified && !r.Evidence.SessionHandlingVerified && !r.Evidence.FreshPositiveVerified && secret == ""
	} else {
		verified = verified && evidence.FreshPositiveVerified && !r.Evidence.ProtocolDenialVerified && !r.Evidence.SessionHandlingVerified && !r.Evidence.NativeDisabledObserved && !r.Evidence.NativePersistedRoleACLObserved && r.Evidence.Qualification.IsZero() && validPassword(secret)
	}
	if !verified {
		outcome := ExecutionRecoveryRequired
		known := r.OperationID == id && !r.Mutated && r.Outcome == ExecutionUnchangedFailure && r.Evidence == (SimplifiedExecutionEvidence{})
		if known {
			outcome = ExecutionUnchangedFailure
		} else {
			s.poison()
		}
		c, ce := CompleteOperation(o, outcome, evidence, time.Now().UTC())
		if ce == nil {
			recovery, stop := context.WithTimeout(context.Background(), s.simplifiedCfg.RecoveryTimeout)
			if known {
				_, ce = s.simplifiedRepo.FailKnown(recovery, OperationUpdate{operationGuard(o, m), c})
			} else {
				_, ce = s.simplifiedRepo.RequireRecovery(recovery, OperationUpdate{operationGuard(o, m), c})
			}
			stop()
		}
		if ce != nil {
			s.poison()
		}
		return out, &DomainError{Code: CodeRecoveryRequired}
	}
	c, e := CompleteOperation(o, ExecutionVerifiedSuccess, evidence, time.Now().UTC())
	if e != nil {
		s.poison()
		return out, &DomainError{Code: CodeRecoveryRequired}
	}
	finalized, finalizeErr := s.simplifiedRepo.ConditionalFinalize(ctx, OperationUpdate{operationGuard(o, m), c})
	// Read back the exact completion, including after a lost COMMIT ACK, without
	// rewriting terminal database truth or retrying broker mutation. An error from
	// finalization still forbids success/secret disclosure for this request.
	recovery, stop := context.WithTimeout(context.Background(), s.simplifiedCfg.RecoveryTimeout)
	confirmed, oe := s.simplifiedRepo.ResolveCommitAmbiguity(recovery, id)
	latest, me := s.simplifiedRepo.GetMetadata(recovery, in.GatewayID)
	stop()
	if finalizeErr != nil || oe != nil || me != nil || !sameCompletion(Completion{Operation: confirmed, Credential: c.Credential}, c) || !validSimplifiedMetadata(latest, in.GatewayID) || latest.LastOperationID != id || latest.CredentialVersion != c.Credential.CredentialVersion || latest.Status != c.Credential.Status || latest.OperationStatus != OperationSucceeded || !reflect.DeepEqual(canonicalSnapshot(CredentialSnapshot{Status: latest.Status, CredentialVersion: latest.CredentialVersion, ActivatedAt: latest.ActivatedAt, RevokedAt: latest.RevokedAt}), canonicalSnapshot(c.Credential)) || latest.LastErrorCode != nil || !sameSimplifiedMetadata(finalized, latest) {
		s.poison()
		return out, &DomainError{Code: CodeFinalizationPending}
	}
	out.MutationResult = MutationResult{Metadata: latest, OperationID: id}
	if action != ActionRevoke {
		v := NewSecretResult(latest, id, secret)
		out.Secret = &v
		out.SecretReturned = true
	}
	return out, nil
}

// Each page is bounded; exhausting the entire scan budget is fail-closed. Other
// pending intents may wait ONLY while still at intent with no runtime evidence.
// Recovery/legacy/unfinished prior maintenance must never be silently skipped.
func (s *ProvisionService) reconcile(ctx context.Context, r DynSecAdapterResult) error {
	cursor := uuid.Nil
	for page := 0; ; page++ {
		if page >= s.cfg.MaxPages {
			return ErrRecoveryRequired
		}
		rows, e := s.repo.ListUnresolved(ctx, cursor, s.cfg.PageSize)
		if e != nil {
			return e
		}
		for _, o := range rows {
			if o.OperationID == r.OperationID {
				return ErrRecoveryRequired
			}
			if o.Status != OperationPending || o.Phase != PhaseIntent || o.Evidence != (VerificationEvidence{}) || o.ValidateHistorical() != nil {
				return ErrRecoveryRequired
			}
			if o.OperationID.String() <= cursor.String() {
				return ErrRecoveryRequired
			}
			cursor = o.OperationID
		}
		if len(rows) < s.cfg.PageSize {
			break
		}
	}
	cursor = uuid.Nil
	for page := 0; ; page++ {
		if page >= s.cfg.MaxPages {
			return ErrRecoveryRequired
		}
		rows, e := s.maintenance.ListPendingMaintenance(ctx, cursor, s.cfg.PageSize)
		if e != nil {
			return e
		}
		for _, cp := range rows {
			if cp.OperationID.String() <= cursor.String() {
				return ErrRecoveryRequired
			}
			cursor = cp.OperationID
			if cp.OperationID == r.OperationID {
				continue
			}
			if cp.Status != MaintenanceInProgress || cp.BrokerEpoch != nil {
				return ErrRecoveryRequired
			}
			o, x := s.repo.ResolveCommitAmbiguity(ctx, cp.OperationID)
			if x != nil || o.Status != OperationPending || o.Phase != PhaseIntent || o.Evidence != (VerificationEvidence{}) {
				return ErrRecoveryRequired
			}
		}
		if len(rows) < s.cfg.PageSize {
			break
		}
	}
	key := ""
	for page := 0; ; page++ {
		if page >= s.cfg.MaxPages {
			return ErrRecoveryRequired
		}
		rows, e := s.repo.ListDurableRevocations(ctx, key, s.cfg.PageSize)
		if e != nil {
			return e
		}
		for _, d := range rows {
			if d.GatewayID <= key {
				return ErrRecoveryRequired
			}
			key = d.GatewayID
			// This operation's current revoke was already verified by Execute.
			// Do not reinterpret its own decision as a superseded reprovision.
		}
		others := make([]RevocationDecision, 0, len(rows))
		for _, d := range rows {
			if d.OperationID != r.OperationID {
				others = append(others, d)
			}
		}
		if e = s.runtime.VerifyRevocations(ctx, r, others); e != nil {
			return e
		}
		if len(rows) < s.cfg.PageSize {
			return nil
		}
	}
}

package mqttcredential

import (
	"context"
	"errors"
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
func serviceError(e error) error {
	var d *DomainError
	if errors.As(e, &d) {
		return &DomainError{Code: d.Code}
	}
	return &DomainError{Code: CodeServiceUnavailable}
}

// Metadata retrieves credential metadata for a gateway if the actor is a platform admin.
func (s *ProvisionService) Metadata(ctx context.Context, actor uuid.UUID, g string) (Metadata, error) {
	if actor == uuid.Nil || gateway.ValidateGatewayID(g) != nil {
		return Metadata{}, &DomainError{Code: CodeInvalidRequest}
	}
	ok, e := s.repo.IsPlatformAdmin(ctx, actor)
	if e != nil {
		return Metadata{}, serviceError(e)
	}
	if !ok {
		return Metadata{}, &DomainError{Code: CodeForbidden}
	}
	m, e := s.repo.GetMetadata(ctx, g)
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
	// Execute cold-restarts the broker. Bind the actual resulting lifetime, not
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

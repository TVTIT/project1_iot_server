package mqttcredential

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

// StartupInventoryRepository is internal; no HTTP authority is granted by it.
// Inventory includes active authorities and rejects ambiguous legacy rows.
type StartupInventoryRepository interface {
	ListStartupInventory(context.Context, string, int) ([]Metadata, error)
}

type StartupRuntime interface {
	BeginStartup(context.Context) (VerificationReceipt, error)
	DisableStartup(context.Context, VerificationReceipt, string) (RecoveryDisableEvidence, error)
	VerifyStartupInventory(context.Context, VerificationReceipt, []Metadata) error
	OpenStartup(context.Context, VerificationReceipt) error
	CloseDrain(context.Context) error
}

type StartupOptions struct {
	Timeout            time.Duration
	PageSize, MaxPages int
	// Server-owned check of non-target backend/manager readiness over private IPC.
	// A DB scan or a TCP liveness probe alone cannot satisfy this capability.
	VerifyBackend func(context.Context, VerificationReceipt) error
}

type StartupReconciler struct {
	repo        Repository
	maintenance MaintenanceRepository
	recovery    RecoveryRepository
	inventory   StartupInventoryRepository
	runtime     StartupRuntime
	cfg         StartupOptions
	mu          sync.Mutex
	busy        bool
	readiness   StartupReadiness
}

func NewStartupReconciler(r Repository, m MaintenanceRepository, recovery RecoveryRepository, runtime StartupRuntime, c StartupOptions) (*StartupReconciler, error) {
	i, ok := r.(StartupInventoryRepository)
	if !ok || dynSecNil(r) || dynSecNil(m) || dynSecNil(recovery) || dynSecNil(runtime) || c.Timeout <= 0 || c.Timeout > 10*time.Minute || c.PageSize < 1 || c.PageSize > 1024 || c.MaxPages < 1 || c.MaxPages > 1024 || c.VerifyBackend == nil {
		return nil, ErrInvalidInput
	}
	return &StartupReconciler{repo: r, maintenance: m, recovery: recovery, inventory: i, runtime: runtime, cfg: c}, nil
}

func (s *StartupReconciler) Ready() bool { return s.readiness.Ready() }

// Run is a single synchronous bounded worker, before exposing any mutation
// service. It cannot override a poisoned in-flight service or an adapter lease.
func (s *StartupReconciler) Run(ctx context.Context) (err error) {
	s.mu.Lock()
	if s.busy {
		s.mu.Unlock()
		return ErrRuntimeBusy
	}
	s.busy = true
	s.mu.Unlock()
	s.readiness.set(false)
	defer func() { s.mu.Lock(); s.busy = false; s.mu.Unlock() }()
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	defer func() {
		if err != nil {
			closeCtx, stop := context.WithTimeout(context.Background(), time.Second*5)
			defer stop()
			_ = s.runtime.CloseDrain(closeCtx)
		}
	}()
	receipt, err := s.runtime.BeginStartup(ctx)
	if err != nil {
		return err
	}
	// Union of unresolved events, unfinished checkpoints (including SUCCESS),
	// and pending recoveries. Cursor exhaustion must be proven, never truncated.
	jobs := map[uuid.UUID]bool{}
	var cursor uuid.UUID
	complete := false
	for page := 0; page < s.cfg.MaxPages; page++ {
		rows, e := s.repo.ListUnresolved(ctx, cursor, s.cfg.PageSize)
		if e != nil {
			return e
		}
		for _, o := range rows {
			if o.OperationID == uuid.Nil || o.OperationID.String() <= cursor.String() {
				return ErrRecoveryRequired
			}
			jobs[o.OperationID] = true
			cursor = o.OperationID
		}
		if len(rows) < s.cfg.PageSize {
			complete = true
			break
		}
	}
	if !complete {
		return ErrRecoveryRequired
	}
	cursor = uuid.Nil
	complete = false
	for page := 0; page < s.cfg.MaxPages; page++ {
		rows, e := s.maintenance.ListPendingMaintenance(ctx, cursor, s.cfg.PageSize)
		if e != nil {
			return e
		}
		for _, cp := range rows {
			if cp.OperationID == uuid.Nil || cp.OperationID.String() <= cursor.String() {
				return ErrRecoveryRequired
			}
			jobs[cp.OperationID] = true
			cursor = cp.OperationID
		}
		if len(rows) < s.cfg.PageSize {
			complete = true
			break
		}
	}
	if !complete {
		return ErrRecoveryRequired
	}
	pending := map[uuid.UUID]RecoveryRecord{}
	cursor = uuid.Nil
	complete = false
	for page := 0; page < s.cfg.MaxPages; page++ {
		rows, e := s.recovery.ListPendingRecovery(ctx, cursor, s.cfg.PageSize)
		if e != nil {
			return e
		}
		for _, r := range rows {
			if r.Validate() != nil || r.RecoveryID.String() <= cursor.String() {
				return ErrRecoveryRequired
			}
			if _, ok := pending[r.OperationID]; ok {
				return ErrRecoveryRequired
			}
			pending[r.OperationID] = r
			jobs[r.OperationID] = true
			cursor = r.RecoveryID
		}
		if len(rows) < s.cfg.PageSize {
			complete = true
			break
		}
	}
	if !complete {
		return ErrRecoveryRequired
	}
	for id := range jobs {
		if e := s.recover(ctx, receipt, id, pending[id]); e != nil {
			return e
		}
	}
	// Re-read all current authority after cleanup, including valid active B.
	key := ""
	complete = false
	for page := 0; page < s.cfg.MaxPages; page++ {
		rows, e := s.inventory.ListStartupInventory(ctx, key, s.cfg.PageSize)
		if e != nil {
			return e
		}
		for _, m := range rows {
			if m.GatewayID <= key || (m.Status != CredentialActive && m.Status != CredentialRevoked) {
				return ErrRecoveryRequired
			}
			key = m.GatewayID
			if m.Status == CredentialRevoked {
				if _, e = s.runtime.DisableStartup(ctx, receipt, m.GatewayID); e != nil {
					return e
				}
			}
		}
		if e = s.runtime.VerifyStartupInventory(ctx, receipt, rows); e != nil {
			return e
		}
		if len(rows) < s.cfg.PageSize {
			complete = true
			break
		}
	}
	if !complete {
		return ErrRecoveryRequired
	}
	// A fresh final fence read prevents OPEN with jobs still unresolved.
	if rows, e := s.repo.ListUnresolved(ctx, uuid.Nil, 1); e != nil {
		return e
	} else if len(rows) != 0 {
		return ErrRecoveryRequired
	}
	if rows, e := s.maintenance.ListPendingMaintenance(ctx, uuid.Nil, 1); e != nil {
		return e
	} else if len(rows) != 0 {
		return ErrRecoveryRequired
	}
	if rows, e := s.recovery.ListPendingRecovery(ctx, uuid.Nil, 1); e != nil {
		return e
	} else if len(rows) != 0 {
		return ErrRecoveryRequired
	}
	if err = s.cfg.VerifyBackend(ctx, receipt); err != nil {
		return err
	}
	if err = s.runtime.OpenStartup(ctx, receipt); err != nil {
		return err
	}
	s.readiness.set(true)
	return nil
}

func (s *StartupReconciler) recover(ctx context.Context, receipt VerificationReceipt, id uuid.UUID, r RecoveryRecord) error {
	o, e := s.repo.ResolveCommitAmbiguity(ctx, id)
	if e != nil {
		return e
	}
	cp, e := s.maintenance.ResolveMaintenance(ctx, id)
	if e != nil {
		return e
	}
	m, e := s.repo.GetMetadata(ctx, o.GatewayID)
	if e != nil {
		return e
	}
	if cp.Status == MaintenanceCompleted || m.LastOperationID != id {
		return ErrRecoveryRequired
	}
	q := RecoveryRequest{Guard: maintenanceGuard(o, m, cp), BrokerEpoch: receipt.Epoch}
	if r.RecoveryID == uuid.Nil {
		q.RecoveryID, e = uuid.NewRandom()
		if e != nil {
			return ErrRecoveryRequired
		}
		r, e = s.recovery.BeginRecovery(ctx, q)
		if e != nil {
			var unknown *CommitOutcomeUnknown
			if !errors.As(e, &unknown) {
				return e
			}
			r, e = s.recovery.ResolveRecovery(ctx, q.RecoveryID)
			if e != nil {
				return e
			}
		}
	} else {
		q.RecoveryID = r.RecoveryID
		if r.BrokerEpoch != receipt.Epoch {
			r, e = s.recovery.BindRecoveryEpoch(ctx, r.RecoveryID, r.BrokerEpoch, receipt.Epoch)
			if e != nil {
				var unknown *CommitOutcomeUnknown
				if !errors.As(e, &unknown) {
					return e
				}
				r, e = s.recovery.ResolveRecovery(ctx, q.RecoveryID)
				if e != nil {
					return e
				}
			}
		}
	}
	if r.Status != RecoveryPending || r.BrokerEpoch != receipt.Epoch || r.OperationID != id || r.GatewayID != o.GatewayID {
		return ErrRecoveryRequired
	}
	proof, e := s.runtime.DisableStartup(ctx, receipt, o.GatewayID)
	if e != nil {
		return e
	}
	if proof.Validate(receipt.Epoch) != nil {
		return ErrRecoveryRequired
	}
	_, e = s.recovery.CompleteRecovery(ctx, q, proof)
	if e != nil {
		var unknown *CommitOutcomeUnknown
		if !errors.As(e, &unknown) {
			return e
		}
		r, e = s.recovery.ResolveRecovery(ctx, q.RecoveryID)
		if e != nil {
			return e
		}
		if r.Status != RecoveryDisabled || r.Evidence.Validate(receipt.Epoch) != nil {
			return ErrRecoveryRequired
		}
	}
	return nil
}

package mqttcredential

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestStartupRejectsInvalidConfiguration(t *testing.T) {
	if _, e := NewStartupReconciler(nil, nil, nil, nil, StartupOptions{}); e == nil {
		t.Fatal("nil startup capabilities accepted")
	}
}

func TestMutationStartupBarrier(t *testing.T) {
	s := &ProvisionService{cfg: ProvisionServiceOptions{StartupReady: func() bool { return false }}}
	_, e := s.Provision(context.Background(), uuid.New(), MutationInput{"fixture_a", uuid.New(), ActionProvision})
	requireCode(t, e, CodeServiceUnavailable)
}

func TestProvisionAbsentRecoveredPrincipal(t *testing.T) {
	if !provisionNativeAdmissible(&CredentialSnapshot{Status: CredentialRevoked, CredentialVersion: 1}, true, DynSecClientMetadata{}, "fixture_a") {
		t.Fatal("recovered absent principal denied new generation")
	}
	if provisionNativeAdmissible(nil, false, DynSecClientMetadata{}, "fixture_a") {
		t.Fatal("unexpected existing principal admitted")
	}
}

func TestStartupReadinessStartsClosed(t *testing.T) {
	var r StartupReadiness
	if r.Ready() {
		t.Fatal("startup is ready without inventory and OPEN")
	}
	r.set(true)
	if !r.Ready() {
		t.Fatal("confirmed startup not ready")
	}
	r.set(false)
	if r.Ready() {
		t.Fatal("failed startup remained ready")
	}
}

type startupRepoFixture struct {
	Repository
	MaintenanceRepository
	RecoveryRepository
	closed    *bool
	inventory []Metadata
	fail      bool
	reads     int
}

func (f *startupRepoFixture) check() error {
	f.reads++
	if !*f.closed {
		return errors.New("DB read before CLOSED")
	}
	if f.fail {
		return errors.New("DB unavailable")
	}
	return nil
}
func (f *startupRepoFixture) ListUnresolved(context.Context, uuid.UUID, int) ([]Operation, error) {
	return nil, f.check()
}
func (f *startupRepoFixture) ListPendingMaintenance(context.Context, uuid.UUID, int) ([]MaintenanceCheckpoint, error) {
	return nil, f.check()
}
func (f *startupRepoFixture) ListPendingRecovery(context.Context, uuid.UUID, int) ([]RecoveryRecord, error) {
	return nil, f.check()
}
func (f *startupRepoFixture) ListStartupInventory(_ context.Context, key string, n int) ([]Metadata, error) {
	if e := f.check(); e != nil {
		return nil, e
	}
	rows := []Metadata{}
	for _, m := range f.inventory {
		if m.GatewayID > key && len(rows) < n {
			rows = append(rows, m)
		}
	}
	return rows, nil
}

type startupRuntimeFixture struct {
	closed, opened                               bool
	failBegin, failDisable, failVerify, failOpen bool
	disabled                                     []string
}

func (f *startupRuntimeFixture) BeginStartup(context.Context) (VerificationReceipt, error) {
	f.closed = true
	if f.failBegin {
		return VerificationReceipt{}, ErrLifecycleUnavailable
	}
	ep, _ := freshIdentity()
	nonce, _ := freshIdentity()
	return VerificationReceipt{Epoch: ep, Nonce: nonce}, nil
}
func (f *startupRuntimeFixture) CloseDrain(context.Context) error {
	f.closed = true
	f.opened = false
	return nil
}
func (f *startupRuntimeFixture) DisableStartup(_ context.Context, r VerificationReceipt, u string) (RecoveryDisableEvidence, error) {
	f.disabled = append(f.disabled, u)
	if f.failDisable {
		return RecoveryDisableEvidence{}, ErrRecoveryRequired
	}
	return RecoveryDisableEvidence{true, true, r.Epoch}, nil
}
func (f *startupRuntimeFixture) VerifyStartupInventory(context.Context, VerificationReceipt, []Metadata) error {
	if f.failVerify {
		return ErrRecoveryRequired
	}
	return nil
}
func (f *startupRuntimeFixture) OpenStartup(context.Context, VerificationReceipt) error {
	if f.failOpen {
		return ErrLifecycleUnavailable
	}
	f.closed = false
	f.opened = true
	return nil
}

func TestStartupInventoryBarrier(t *testing.T) {
	for _, scenario := range []string{"clean", "DBdown", "epoch", "disable", "snapshot", "open", "backend", "truncation", "uncertain"} {
		t.Run(scenario, func(t *testing.T) {
			a := &startupRuntimeFixture{}
			r := &startupRepoFixture{closed: &a.closed, inventory: []Metadata{{GatewayID: "fixture_a", Status: CredentialRevoked}, {GatewayID: "fixture_b", Status: CredentialActive}}}
			cfg := StartupOptions{Timeout: time.Second, PageSize: 1, MaxPages: 3, VerifyBackend: func(context.Context, VerificationReceipt) error { return nil }}
			switch scenario {
			case "DBdown":
				r.fail = true
			case "epoch":
				a.failBegin = true
			case "disable":
				a.failDisable = true
			case "snapshot":
				a.failVerify = true
			case "open":
				a.failOpen = true
			case "backend":
				cfg.VerifyBackend = func(context.Context, VerificationReceipt) error { return ErrLifecycleUnavailable }
			case "truncation":
				cfg.MaxPages = 2
			case "uncertain":
				r.inventory[1].Status = CredentialRecoveryNeeded
			}
			s, e := NewStartupReconciler(r, r, r, a, cfg)
			if e != nil {
				t.Fatal(e)
			}
			e = s.Run(context.Background())
			if scenario == "clean" {
				if e != nil || !s.Ready() || !a.opened {
					t.Fatal("clean startup failed", e)
				}
				if len(a.disabled) != 1 || a.disabled[0] != "fixture_a" {
					t.Fatal("active B was disabled")
				}
			} else {
				if e == nil || s.Ready() || a.opened || !a.closed {
					t.Fatal("failure opened gate", e)
				}
			}
			if scenario == "epoch" && r.reads != 0 {
				t.Fatal("DB read before trusted epoch")
			}
		})
	}
}

type startupScanFault struct {
	*startupRepoFixture
	scan string
	mode string
}

func (f *startupScanFault) ListUnresolved(ctx context.Context, id uuid.UUID, n int) ([]Operation, error) {
	if f.scan != "events" {
		return f.startupRepoFixture.ListUnresolved(ctx, id, n)
	}
	if f.mode == "error" {
		return nil, ErrRecoveryRequired
	}
	return []Operation{{OperationID: uuid.Nil}}, nil
}
func (f *startupScanFault) ListPendingMaintenance(ctx context.Context, id uuid.UUID, n int) ([]MaintenanceCheckpoint, error) {
	if f.scan != "maintenance" {
		return f.startupRepoFixture.ListPendingMaintenance(ctx, id, n)
	}
	if f.mode == "error" {
		return nil, ErrRecoveryRequired
	}
	return []MaintenanceCheckpoint{{OperationID: uuid.Nil}}, nil
}
func (f *startupScanFault) ListPendingRecovery(ctx context.Context, id uuid.UUID, n int) ([]RecoveryRecord, error) {
	if f.scan != "recovery" {
		return f.startupRepoFixture.ListPendingRecovery(ctx, id, n)
	}
	if f.mode == "error" {
		return nil, ErrRecoveryRequired
	}
	return []RecoveryRecord{{RecoveryID: uuid.Nil}}, nil
}
func TestStartupScanRejectsUnknownProjection(t *testing.T) {
	for _, scan := range []string{"events", "maintenance", "recovery"} {
		for _, mode := range []string{"error", "invalid"} {
			t.Run(scan+"/"+mode, func(t *testing.T) {
				a := &startupRuntimeFixture{}
				r := &startupScanFault{startupRepoFixture: &startupRepoFixture{closed: &a.closed}, scan: scan, mode: mode}
				s, err := NewStartupReconciler(r, r, r, a, StartupOptions{Timeout: time.Second, PageSize: 1, MaxPages: 1, VerifyBackend: func(context.Context, VerificationReceipt) error { return nil }})
				if err != nil {
					t.Fatal(err)
				}
				if err = s.Run(context.Background()); err == nil || s.Ready() || a.opened {
					t.Fatal("unknown projection opened gate")
				}
			})
		}
	}
}

// This is a contract counterexample, not a startup implementation. A verified
// disable is not positive authentication of the lost provision/rotate password.
func TestStartupLostPasswordRecoveryContract(t *testing.T) {
	now := time.Now().UTC()
	for _, action := range []Action{ActionProvision, ActionRotate} {
		t.Run(string(action), func(t *testing.T) {
			o := Operation{OperationID: uuid.New(), ActorUserID: uuid.New(), GatewayID: "fixture_recovery", IdempotencyKey: uuid.New(), Action: action, CredentialVersion: 2, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown, CreatedAt: now, UpdatedAt: now}
			if action == ActionRotate {
				o.Previous = &CredentialSnapshot{Status: CredentialActive, CredentialVersion: 1}
			}
			// Hypothetical native disable/readback proof. No random-password probe
			// may manufacture FreshPositiveVerified for the lost generation.
			disabled := VerificationEvidence{RAMApplied: true, SnapshotObserved: true}
			if _, e := CompleteOperation(o, ExecutionVerifiedSuccess, disabled, now); e == nil {
				t.Fatal("disable proof was accepted as provision/rotate success")
			}
			r, e := CompleteOperation(o, ExecutionRecoveryRequired, disabled, now)
			if e != nil || r.Operation.Terminal() || r.Credential.Status != CredentialRecoveryNeeded {
				t.Fatal("uncertainty must remain nonterminal", e)
			}
			if _, e = CompleteOperation(r.Operation, ExecutionUnchangedFailure, VerificationEvidence{}, now); e == nil {
				t.Fatal("cleanup was mislabeled as unchanged failure")
			}
			for _, next := range []Action{ActionProvision, ActionRotate, ActionRevoke} {
				_, e = BeginCredentialStatus(r.Credential.Status, next)
				requireCode(t, e, CodeRecoveryRequired)
			}
			// Existing success remains immutable even if its checkpoint was not
			// completed before process death and the password was never returned.
			success, e := CompleteOperation(o, ExecutionVerifiedSuccess, VerificationEvidence{true, true, true}, now)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = CompleteOperation(success.Operation, ExecutionRecoveryRequired, disabled, now); e == nil {
				t.Fatal("terminal business success rewritten by recovery")
			}
		})
	}
}

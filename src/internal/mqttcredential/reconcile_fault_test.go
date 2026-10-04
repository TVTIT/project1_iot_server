package mqttcredential

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

// These seams exercise application commit ambiguity, not simulated PG COMMIT.
type recoveryFaultFixture struct {
	Repository
	MaintenanceRepository
	RecoveryRepository
	o                   Operation
	m                   Metadata
	cp                  MaintenanceCheckpoint
	r                   RecoveryRecord
	fault               string
	completed, resolved int
}

func (f *recoveryFaultFixture) ResolveCommitAmbiguity(context.Context, uuid.UUID) (Operation, error) {
	if f.fault == "operation" {
		return Operation{}, ErrRecoveryRequired
	}
	return f.o, nil
}
func (f *recoveryFaultFixture) ResolveMaintenance(context.Context, uuid.UUID) (MaintenanceCheckpoint, error) {
	if f.fault == "checkpoint" {
		return MaintenanceCheckpoint{}, ErrRecoveryRequired
	}
	return f.cp, nil
}
func (f *recoveryFaultFixture) GetMetadata(context.Context, string) (Metadata, error) {
	if f.fault == "metadata" {
		return Metadata{}, ErrRecoveryRequired
	}
	return f.m, nil
}
func (f *recoveryFaultFixture) BeginRecovery(_ context.Context, q RecoveryRequest) (RecoveryRecord, error) {
	f.r.RecoveryID, f.r.BrokerEpoch = q.RecoveryID, q.BrokerEpoch
	if f.fault == "begin" {
		return RecoveryRecord{}, ErrRecoveryRequired
	}
	if f.fault == "begin-unknown" || f.fault == "resolve" {
		return RecoveryRecord{}, &CommitOutcomeUnknown{OperationID: q.RecoveryID}
	}
	return f.r, nil
}
func (f *recoveryFaultFixture) BindRecoveryEpoch(_ context.Context, id uuid.UUID, old, next string) (RecoveryRecord, error) {
	if id != f.r.RecoveryID || old != f.r.BrokerEpoch {
		return RecoveryRecord{}, ErrRecoveryRequired
	}
	if f.fault == "bind" {
		return RecoveryRecord{}, ErrRecoveryRequired
	}
	f.r.BrokerEpoch = next
	if f.fault == "bind-unknown" {
		return RecoveryRecord{}, &CommitOutcomeUnknown{OperationID: id}
	}
	return f.r, nil
}
func (f *recoveryFaultFixture) CompleteRecovery(_ context.Context, q RecoveryRequest, proof RecoveryDisableEvidence) (Metadata, error) {
	f.completed++
	if f.fault == "complete" {
		return Metadata{}, ErrRecoveryRequired
	}
	f.r.Status, f.r.Evidence = RecoveryDisabled, proof
	if f.fault == "complete-unconfirmed" {
		f.r.Status = RecoveryPending
	}
	if f.fault == "complete-unknown" || f.fault == "complete-unconfirmed" {
		return Metadata{}, &CommitOutcomeUnknown{OperationID: q.RecoveryID}
	}
	return f.m, nil
}
func (f *recoveryFaultFixture) ResolveRecovery(_ context.Context, id uuid.UUID) (RecoveryRecord, error) {
	f.resolved++
	if id != f.r.RecoveryID || f.fault == "resolve" {
		return RecoveryRecord{}, ErrRecoveryRequired
	}
	return f.r, nil
}

type recoveryProofRuntime struct {
	startupRuntimeFixture
	bad bool
}

func (f *recoveryProofRuntime) DisableStartup(ctx context.Context, r VerificationReceipt, u string) (RecoveryDisableEvidence, error) {
	p, e := f.startupRuntimeFixture.DisableStartup(ctx, r, u)
	if f.bad {
		p.SnapshotObserved = false
	}
	return p, e
}

func TestStartupRecoveryFaultClassification(t *testing.T) {
	for _, fault := range []string{"clean", "operation", "checkpoint", "metadata", "stale", "completed-checkpoint", "begin", "begin-unknown", "resolve", "bind", "bind-unknown", "disable", "invalid-proof", "complete", "complete-unknown", "complete-unconfirmed", "wrong-operation", "wrong-epoch"} {
		t.Run(fault, func(t *testing.T) {
			id := uuid.New()
			epoch, err := freshIdentity()
			if err != nil {
				t.Fatal(err)
			}
			f := &recoveryFaultFixture{fault: fault, o: Operation{OperationID: id, GatewayID: "fixture_recovery"}, m: Metadata{LastOperationID: id}, cp: MaintenanceCheckpoint{Status: MaintenanceInProgress}, r: RecoveryRecord{OperationID: id, GatewayID: "fixture_recovery", Status: RecoveryPending}}
			a := &recoveryProofRuntime{bad: fault == "invalid-proof"}
			a.failDisable = fault == "disable"
			s := &StartupReconciler{repo: f, maintenance: f, recovery: f, runtime: a}
			pending := RecoveryRecord{}
			if fault == "stale" {
				f.m.LastOperationID = uuid.New()
			}
			if fault == "completed-checkpoint" {
				f.cp.Status = MaintenanceCompleted
			}
			if fault == "bind" || fault == "bind-unknown" {
				f.r.RecoveryID, f.r.BrokerEpoch = uuid.New(), uuid.NewString()
				pending = f.r
			}
			if fault == "wrong-operation" {
				f.r.OperationID = uuid.New()
			}
			if fault == "wrong-epoch" {
				f.r.RecoveryID, f.r.BrokerEpoch = uuid.New(), epoch
				pending = f.r
				pending.BrokerEpoch = uuid.NewString()
				f.fault = "bind"
			}
			e := s.recover(context.Background(), VerificationReceipt{Epoch: epoch}, id, pending)
			ok := fault == "clean" || fault == "begin-unknown" || fault == "bind-unknown" || fault == "complete-unknown"
			if (e == nil) != ok {
				t.Fatalf("unexpected recovery result: %v", e)
			}
			if fault == "invalid-proof" && f.completed != 0 {
				t.Fatal("invalid RAM-only evidence reached recovery commit")
			}
			if ok && f.completed != 1 {
				t.Fatal("cleanup must commit once")
			}
			if fault == "complete-unknown" && f.resolved != 1 {
				t.Fatal("recovery UUID ambiguity was not resolved")
			}
			if !ok && e != nil && !errors.Is(e, ErrRecoveryRequired) {
				t.Fatal("unexpected classification")
			}
		})
	}
}

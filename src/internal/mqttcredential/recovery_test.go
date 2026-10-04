package mqttcredential

import (
	"testing"

	"github.com/google/uuid"
)

func TestRecoveryDisableEvidence(t *testing.T) {
	epoch, _ := freshIdentity()
	r := RecoveryRecord{RecoveryID: uuid.New(), OperationID: uuid.New(), GatewayID: "fixture_recovery", AttemptedVersion: 2, Origin: RecoveryStartup, Status: RecoveryPending, BrokerEpoch: epoch}
	if r.Validate() != nil {
		t.Fatal("valid durable internal intent rejected")
	}
	for _, e := range []RecoveryDisableEvidence{{}, {RAMDisabled: true}, {SnapshotObserved: true}, {RAMDisabled: true, SnapshotObserved: true, BrokerEpoch: "bad"}} {
		if e.Validate(epoch) == nil {
			t.Fatal("incomplete disable evidence accepted")
		}
	}
	if (RecoveryDisableEvidence{true, true, epoch}).Validate(epoch) != nil {
		t.Fatal("verified disable rejected")
	}
}

func TestRecoveryDispositionReplay(t *testing.T) {
	o := fixtureOperation(ActionProvision)
	o.RecoveryID = ptrRecoveryUUID(uuid.New())
	if o.Validate() != nil || !o.Terminal() {
		t.Fatal("recovery disposition must resolve without fabricated success")
	}
	m := Metadata{GatewayID: o.GatewayID, Status: CredentialRevoked}
	r, e := ReplayOperation(o, o.ActorUserID, MutationInput{o.GatewayID, o.IdempotencyKey, o.Action}, m)
	if e != nil || r.SecretReturned || r.ReplayedStatus != OperationResolvedByRecovery || r.NextAction != "provision_with_new_idempotency_key" {
		t.Fatal("unsafe recovered replay", e, r)
	}
}

func ptrRecoveryUUID(id uuid.UUID) *uuid.UUID { return &id }

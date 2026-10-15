package mqttcredential

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestReplacementCompletionPreservesActualPrevious(t *testing.T) {
	now := time.Now().UTC()
	o := Operation{OperationID: uuid.New(), ActorUserID: uuid.New(), GatewayID: "fixture_gateway", IdempotencyKey: uuid.New(), Action: ActionRotate, CredentialVersion: 3, Status: OperationPending, Phase: PhaseIntent, DeliveryStatus: DeliveryUnknown, CreatedAt: now, UpdatedAt: now, Previous: &CredentialSnapshot{Status: CredentialRecoveryNeeded, CredentialVersion: 1}, ReplacementPredecessorID: ptrUUID(uuid.New())}
	c, err := CompleteOperation(o, ExecutionVerifiedSuccess, VerificationEvidence{true, true, true}, now.Add(time.Second))
	if err != nil || c.Credential.Status != CredentialActive || c.Credential.CredentialVersion != 3 || c.Operation.Previous.Status != CredentialRecoveryNeeded {
		t.Fatalf("actual previous must not be forged: %v", err)
	}
	c, err = CompleteOperation(o, ExecutionUnchangedFailure, VerificationEvidence{}, now.Add(time.Second))
	if err != nil || c.Credential.Status != CredentialRecoveryNeeded || c.Credential.CredentialVersion != 1 {
		t.Fatalf("unchanged failure preserves uncertainty: %v", err)
	}
	o.ReplacementPredecessorID = nil
	if _, err := CompleteOperation(o, ExecutionVerifiedSuccess, VerificationEvidence{true, true, true}, now.Add(time.Second)); err == nil {
		t.Fatal("ordinary rotation still requires active previous")
	}
}

func ptrUUID(id uuid.UUID) *uuid.UUID { return &id }

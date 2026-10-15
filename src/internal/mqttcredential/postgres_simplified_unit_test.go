package mqttcredential

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Unit tests for Domain & Repository support for ExecutionContract, Simplified Journal, and Reset.

func TestExecutionContract_Validate(t *testing.T) {
	tests := []struct {
		name     string
		contract ExecutionContract
		wantErr  bool
	}{
		{
			name:     "empty contract is invalid",
			contract: "",
			wantErr:  true,
		},
		{
			name:     "lifecycle contract is valid",
			contract: ContractLifecycle,
			wantErr:  false,
		},
		{
			name:     "simplified contract is valid",
			contract: ContractSimplified,
			wantErr:  false,
		},
		{
			name:     "arbitrary contract is invalid",
			contract: "arbitrary",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.contract.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("ExecutionContract.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCompleteOperation_ContractDistinction(t *testing.T) {
	now := time.Now().UTC()
	opID := uuid.New()
	actor := uuid.New()
	key := uuid.New()
	gw := "gateway-001"

	baseLifecycleOp := Operation{
		OperationID:       opID,
		ActorUserID:       actor,
		GatewayID:         gw,
		IdempotencyKey:    key,
		Action:            ActionProvision,
		CredentialVersion: 1,
		Status:            OperationPending,
		Phase:             PhaseIntent,
		DeliveryStatus:    DeliveryUnknown,
		CreatedAt:         now.Add(-time.Minute),
		UpdatedAt:         now.Add(-time.Minute),
		ExecutionContract: ContractLifecycle,
	}

	proofWithSnapshot := VerificationEvidence{
		RAMApplied:            true,
		SnapshotObserved:      true,
		FreshPositiveVerified: true,
	}

	proofWithoutSnapshot := VerificationEvidence{
		RAMApplied:            true,
		SnapshotObserved:      false,
		FreshPositiveVerified: true,
	}

	t.Run("lifecycle success requires snapshot_observed", func(t *testing.T) {
		op := baseLifecycleOp
		// Without snapshot_observed should fail for lifecycle
		_, err := CompleteOperation(op, ExecutionVerifiedSuccess, proofWithoutSnapshot, now)
		if err == nil {
			t.Fatalf("expected error for lifecycle success missing snapshot_observed")
		}

		// With snapshot_observed should succeed
		comp, err := CompleteOperation(op, ExecutionVerifiedSuccess, proofWithSnapshot, now)
		if err != nil {
			t.Fatalf("unexpected error for lifecycle success: %v", err)
		}
		if comp.Operation.Status != OperationSucceeded {
			t.Fatalf("expected OperationSucceeded, got %v", comp.Operation.Status)
		}
	})

	t.Run("simplified success does not require snapshot_observed", func(t *testing.T) {
		op := baseLifecycleOp
		op.ExecutionContract = ContractSimplified

		// Without snapshot_observed should succeed for simplified contract
		comp, err := CompleteOperation(op, ExecutionVerifiedSuccess, proofWithoutSnapshot, now)
		if err != nil {
			t.Fatalf("unexpected error for simplified success without snapshot: %v", err)
		}
		if comp.Operation.Status != OperationSucceeded {
			t.Fatalf("expected OperationSucceeded, got %v", comp.Operation.Status)
		}
		if comp.Operation.Evidence.SnapshotObserved {
			t.Fatalf("expected snapshot_observed to remain false")
		}
	})

	t.Run("simplified revoke success does not require fresh_positive_verified or snapshot_observed", func(t *testing.T) {
		revokeOp := Operation{
			OperationID:       opID,
			ActorUserID:       actor,
			GatewayID:         gw,
			IdempotencyKey:    key,
			Action:            ActionRevoke,
			CredentialVersion: 1,
			Status:            OperationPending,
			Phase:             PhaseIntent,
			DeliveryStatus:    DeliveryUnknown,
			CreatedAt:         now.Add(-time.Minute),
			UpdatedAt:         now.Add(-time.Minute),
			ExecutionContract: ContractSimplified,
			Previous: &CredentialSnapshot{
				Status:            CredentialActive,
				CredentialVersion: 1,
			},
		}

		revokeProof := VerificationEvidence{
			RAMApplied:            true,
			SnapshotObserved:      false,
			FreshPositiveVerified: false,
		}

		comp, err := CompleteOperation(revokeOp, ExecutionVerifiedSuccess, revokeProof, now)
		if err != nil {
			t.Fatalf("unexpected error for simplified revoke success: %v", err)
		}
		if comp.Operation.Status != OperationSucceeded {
			t.Fatalf("expected OperationSucceeded, got %v", comp.Operation.Status)
		}
		if comp.Credential.Status != CredentialRevoked {
			t.Fatalf("expected CredentialRevoked, got %v", comp.Credential.Status)
		}
	})

	t.Run("simplified reset success completes correctly", func(t *testing.T) {
		predID := uuid.New()
		reason := ResetReasonLostSecretRecovery
		resetOp := Operation{
			OperationID:        opID,
			ActorUserID:        actor,
			GatewayID:          gw,
			IdempotencyKey:     key,
			Action:             ActionReset,
			CredentialVersion:  2,
			Status:             OperationPending,
			Phase:              PhaseIntent,
			DeliveryStatus:     DeliveryUnknown,
			CreatedAt:          now.Add(-time.Minute),
			UpdatedAt:          now.Add(-time.Minute),
			ExecutionContract:  ContractSimplified,
			ResetPredecessorID: &predID,
			ResetReason:        &reason,
			Previous: &CredentialSnapshot{
				Status:            CredentialActive,
				CredentialVersion: 1,
			},
		}

		comp, err := CompleteOperation(resetOp, ExecutionVerifiedSuccess, proofWithoutSnapshot, now)
		if err != nil {
			t.Fatalf("unexpected error for simplified reset success: %v", err)
		}
		if comp.Operation.Status != OperationSucceeded {
			t.Fatalf("expected OperationSucceeded, got %v", comp.Operation.Status)
		}
		if comp.Credential.Status != CredentialActive || comp.Credential.CredentialVersion != 2 {
			t.Fatalf("expected CredentialActive version 2, got %v v%d", comp.Credential.Status, comp.Credential.CredentialVersion)
		}
	})

	t.Run("simplified reset without reset metadata fails validation", func(t *testing.T) {
		resetOp := Operation{
			OperationID:       opID,
			ActorUserID:       actor,
			GatewayID:         gw,
			IdempotencyKey:    key,
			Action:            ActionReset,
			CredentialVersion: 2,
			Status:            OperationPending,
			Phase:             PhaseIntent,
			DeliveryStatus:    DeliveryUnknown,
			CreatedAt:         now.Add(-time.Minute),
			UpdatedAt:         now.Add(-time.Minute),
			ExecutionContract: ContractSimplified,
			Previous: &CredentialSnapshot{
				Status:            CredentialActive,
				CredentialVersion: 1,
			},
		}
		if err := resetOp.ValidateHistorical(); err == nil {
			t.Fatalf("expected error for ActionReset without ResetPredecessorID and ResetReason")
		}
	})
}

func TestCheckResetEligibility_UpdatedSemantics(t *testing.T) {
	now := time.Now().UTC()
	gw := "gateway-001"
	opID := uuid.New()
	actor := uuid.New()
	key := uuid.New()

	t.Run("eligible: recovery_after_unknown when metadata retains previous credential version", func(t *testing.T) {
		// Attempted version was 2, but recovery_needed retained v1 in metadata
		prevVer := int64(1)
		op := Operation{
			OperationID:       opID,
			ActorUserID:       actor,
			GatewayID:         gw,
			IdempotencyKey:    key,
			Action:            ActionRotate,
			CredentialVersion: 2,
			Status:            OperationRecoveryNeeded,
			Phase:             PhaseRecovery,
			DeliveryStatus:    DeliveryUnknown,
			CreatedAt:         now.Add(-time.Minute),
			UpdatedAt:         now,
			Evidence:          VerificationEvidence{RAMApplied: true},
			ExecutionContract: ContractSimplified,
			Previous: &CredentialSnapshot{
				Status:            CredentialActive,
				CredentialVersion: prevVer,
			},
		}

		meta := Metadata{
			GatewayID:         gw,
			Username:          gw,
			CredentialVersion: 1, // retained previous version!
			Status:            CredentialRecoveryNeeded,
			LastOperationID:   opID,
			OperationStatus:   OperationRecoveryNeeded,
		}

		err := CheckResetEligibility(meta, op, ResetReasonRecoveryAfterUnknown)
		if err != nil {
			t.Fatalf("unexpected error for recovery_after_unknown with retained previous version: %v", err)
		}
	})

	t.Run("eligible: lost_secret_recovery targeting prior succeeded reset", func(t *testing.T) {
		completedAt := now
		predID := uuid.New()
		reason := ResetReasonRecoveryAfterUnknown
		op := Operation{
			OperationID:        opID,
			ActorUserID:        actor,
			GatewayID:          gw,
			IdempotencyKey:     key,
			Action:             ActionReset,
			CredentialVersion:  3,
			Status:             OperationSucceeded,
			Phase:              PhaseFinalize,
			DeliveryStatus:     DeliveryUnknown,
			CreatedAt:          now.Add(-time.Minute),
			UpdatedAt:          now,
			CompletedAt:        &completedAt,
			Evidence:           VerificationEvidence{RAMApplied: true, FreshPositiveVerified: true},
			ExecutionContract:  ContractSimplified,
			ResetPredecessorID: &predID,
			ResetReason:        &reason,
			Previous: &CredentialSnapshot{
				Status:            CredentialRotating,
				CredentialVersion: 2,
			},
		}

		meta := Metadata{
			GatewayID:         gw,
			Username:          gw,
			CredentialVersion: 3,
			Status:            CredentialActive,
			LastOperationID:   opID,
			OperationStatus:   OperationSucceeded,
			ActivatedAt:       &completedAt,
		}

		err := CheckResetEligibility(meta, op, ResetReasonLostSecretRecovery)
		if err != nil {
			t.Fatalf("unexpected error for lost_secret_recovery targeting prior reset: %v", err)
		}
	})
}

func TestResetInputValidation(t *testing.T) {
	actor := uuid.New()
	key := uuid.New()
	gw := "gateway-001"

	for _, reason := range []ResetReason{ResetReasonRecoveryAfterUnknown, ResetReasonLostSecretRecovery} {
		in := ResetInput{GatewayID: gw, IdempotencyKey: key, Reason: reason}
		if err := ValidateResetInput(actor, in); err != nil {
			t.Fatalf("expected valid reset input for %s, got: %v", reason, err)
		}
	}

	// Nil actor
	if err := ValidateResetInput(uuid.Nil, ResetInput{GatewayID: gw, IdempotencyKey: key, Reason: ResetReasonLostSecretRecovery}); err == nil {
		t.Fatalf("expected error for nil actor")
	}

	// Nil key
	if err := ValidateResetInput(actor, ResetInput{GatewayID: gw, IdempotencyKey: uuid.Nil, Reason: ResetReasonLostSecretRecovery}); err == nil {
		t.Fatalf("expected error for nil key")
	}

	// Bad gateway
	if err := ValidateResetInput(actor, ResetInput{GatewayID: "bad/id", IdempotencyKey: key, Reason: ResetReasonLostSecretRecovery}); err == nil {
		t.Fatalf("expected error for bad gateway")
	}

	// Bad reason
	if err := ValidateResetInput(actor, ResetInput{GatewayID: gw, IdempotencyKey: key, Reason: "arbitrary"}); err == nil {
		t.Fatalf("expected error for bad reason")
	}
}

func TestBeginResetRequestValidation(t *testing.T) {
	// Repository validation check with mock/stub
	r := &PostgresRepository{}
	// Empty operation ID
	_, err := r.BeginReset(context.Background(), BeginResetRequest{
		ActorUserID: uuid.New(),
		OperationID: uuid.Nil,
		Input:       ResetInput{GatewayID: "gateway-001", IdempotencyKey: uuid.New(), Reason: ResetReasonLostSecretRecovery},
	})
	if err == nil {
		t.Fatalf("expected error for nil OperationID in BeginReset")
	}
	var de *DomainError
	if !errors.As(err, &de) || de.Code != CodeInvalidRequest {
		t.Fatalf("expected CodeInvalidRequest, got %v", err)
	}

	// Invalid input
	_, err = r.BeginReset(context.Background(), BeginResetRequest{
		ActorUserID: uuid.Nil,
		OperationID: uuid.New(),
		Input:       ResetInput{GatewayID: "gateway-001", IdempotencyKey: uuid.New(), Reason: ResetReasonLostSecretRecovery},
	})
	if err == nil {
		t.Fatalf("expected error for invalid input in BeginReset")
	}

	// Invalid ExpectedTargetGuard
	_, err = r.BeginReset(context.Background(), BeginResetRequest{
		ActorUserID: uuid.New(),
		OperationID: uuid.New(),
		Input:       ResetInput{GatewayID: "gateway-001", IdempotencyKey: uuid.New(), Reason: ResetReasonLostSecretRecovery},
		ExpectedTargetGuard: &OperationGuard{
			GatewayID: "wrong-gw",
		},
	})
	if err == nil {
		t.Fatalf("expected error for mismatched ExpectedTargetGuard GatewayID")
	}
}

func TestLookupResetReplay_UnitValidation(t *testing.T) {
	r := &PostgresRepository{}
	// Nil actor
	_, _, _, err := r.LookupResetReplay(context.Background(), uuid.Nil, ResetInput{
		GatewayID:      "gateway-001",
		IdempotencyKey: uuid.New(),
		Reason:         ResetReasonLostSecretRecovery,
	})
	if err == nil {
		t.Fatalf("expected error for nil actor in LookupResetReplay")
	}

	// Invalid input (empty gateway)
	_, _, _, err = r.LookupResetReplay(context.Background(), uuid.New(), ResetInput{
		GatewayID:      "",
		IdempotencyKey: uuid.New(),
		Reason:         ResetReasonLostSecretRecovery,
	})
	if err == nil {
		t.Fatalf("expected error for invalid input in LookupResetReplay")
	}

	// Invalid reason
	_, _, _, err = r.LookupResetReplay(context.Background(), uuid.New(), ResetInput{
		GatewayID:      "gateway-001",
		IdempotencyKey: uuid.New(),
		Reason:         "invalid_reason",
	})
	if err == nil {
		t.Fatalf("expected error for invalid reason in LookupResetReplay")
	}
}

func TestBeginSimplifiedOperation_UnitErrors(t *testing.T) {
	r := &PostgresRepository{}
	// Empty operation ID
	_, err := r.BeginSimplifiedOperation(context.Background(), BeginRequest{
		ActorUserID: uuid.New(),
		OperationID: [16]byte{},
		Input: MutationInput{
			GatewayID:      "gateway-001",
			IdempotencyKey: uuid.New(),
			Action:         ActionProvision,
		},
	})
	if err == nil {
		t.Fatalf("expected error for empty OperationID in BeginSimplifiedOperation")
	}

	// Invalid input (empty gateway)
	_, err = r.BeginSimplifiedOperation(context.Background(), BeginRequest{
		ActorUserID: uuid.New(),
		OperationID: uuid.New(),
		Input: MutationInput{
			GatewayID:      "",
			IdempotencyKey: uuid.New(),
			Action:         ActionProvision,
		},
	})
	if err == nil {
		t.Fatalf("expected error for empty GatewayID")
	}

	// Nil actor
	_, err = r.BeginSimplifiedOperation(context.Background(), BeginRequest{
		ActorUserID: uuid.Nil,
		OperationID: uuid.New(),
		Input: MutationInput{
			GatewayID:      "gateway-001",
			IdempotencyKey: uuid.New(),
			Action:         ActionProvision,
		},
	})
	if err == nil {
		t.Fatalf("expected error for nil ActorUserID")
	}
}

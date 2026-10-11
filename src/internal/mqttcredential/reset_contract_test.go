package mqttcredential

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestResetReason_Validate(t *testing.T) {
	tests := []struct {
		name    string
		reason  ResetReason
		wantErr bool
	}{
		{
			name:    "valid recovery_after_unknown",
			reason:  ResetReasonRecoveryAfterUnknown,
			wantErr: false,
		},
		{
			name:    "valid lost_secret_recovery",
			reason:  ResetReasonLostSecretRecovery,
			wantErr: false,
		},
		{
			name:    "empty reason rejected",
			reason:  "",
			wantErr: true,
		},
		{
			name:    "arbitrary operator_intervention rejected",
			reason:  "operator_intervention",
			wantErr: true,
		},
		{
			name:    "free form text rejected",
			reason:  "recovery",
			wantErr: true,
		},
		{
			name:    "normalized uppercase rejected without normalization",
			reason:  "RECOVERY_AFTER_UNKNOWN",
			wantErr: true,
		},
		{
			name:    "whitespace padded rejected",
			reason:  " recovery_after_unknown ",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.reason.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("ResetReason.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				domainErr, ok := err.(*DomainError)
				if !ok || domainErr.Code != CodeInvalidRequest {
					t.Fatalf("expected CodeInvalidRequest, got %v", err)
				}
			}
		})
	}
}

func TestValidateResetInput(t *testing.T) {
	validActor := uuid.New()
	validKey := uuid.New()
	validGW := "gateway-001"

	tests := []struct {
		name    string
		actor   uuid.UUID
		input   ResetInput
		wantErr bool
	}{
		{
			name:  "valid recovery_after_unknown input",
			actor: validActor,
			input: ResetInput{
				GatewayID:      validGW,
				IdempotencyKey: validKey,
				Reason:         ResetReasonRecoveryAfterUnknown,
			},
			wantErr: false,
		},
		{
			name:  "valid lost_secret_recovery input",
			actor: validActor,
			input: ResetInput{
				GatewayID:      validGW,
				IdempotencyKey: validKey,
				Reason:         ResetReasonLostSecretRecovery,
			},
			wantErr: false,
		},
		{
			name:  "nil actor rejected",
			actor: uuid.Nil,
			input: ResetInput{
				GatewayID:      validGW,
				IdempotencyKey: validKey,
				Reason:         ResetReasonRecoveryAfterUnknown,
			},
			wantErr: true,
		},
		{
			name:  "nil idempotency key rejected",
			actor: validActor,
			input: ResetInput{
				GatewayID:      validGW,
				IdempotencyKey: uuid.Nil,
				Reason:         ResetReasonRecoveryAfterUnknown,
			},
			wantErr: true,
		},
		{
			name:  "empty gateway id rejected",
			actor: validActor,
			input: ResetInput{
				GatewayID:      "",
				IdempotencyKey: validKey,
				Reason:         ResetReasonRecoveryAfterUnknown,
			},
			wantErr: true,
		},
		{
			name:  "invalid gateway id syntax rejected",
			actor: validActor,
			input: ResetInput{
				GatewayID:      "bad/gateway/id",
				IdempotencyKey: validKey,
				Reason:         ResetReasonRecoveryAfterUnknown,
			},
			wantErr: true,
		},
		{
			name:  "invalid reason rejected",
			actor: validActor,
			input: ResetInput{
				GatewayID:      validGW,
				IdempotencyKey: validKey,
				Reason:         "operator_intervention",
			},
			wantErr: true,
		},
		{
			name:  "empty reason rejected",
			actor: validActor,
			input: ResetInput{
				GatewayID:      validGW,
				IdempotencyKey: validKey,
				Reason:         "",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateResetInput(tt.actor, tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateResetInput() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				domainErr, ok := err.(*DomainError)
				if !ok || domainErr.Code != CodeInvalidRequest {
					t.Fatalf("expected CodeInvalidRequest, got %v", err)
				}
			}
		})
	}
}

func TestCheckResetEligibility(t *testing.T) {
	now := time.Now().UTC()
	gw := "gateway-001"
	opID := uuid.New()
	actor := uuid.New()
	key := uuid.New()

	validBaseOp := Operation{
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
		Evidence: VerificationEvidence{
			RAMApplied: true,
		},
	}

	validBaseMeta := Metadata{
		GatewayID:         gw,
		Username:          gw,
		CredentialVersion: 2,
		Status:            CredentialRecoveryNeeded,
		LastOperationID:   opID,
		OperationStatus:   OperationRecoveryNeeded,
	}

	t.Run("eligible: recovery_after_unknown on recovery-needed current attempt", func(t *testing.T) {
		op := validBaseOp
		meta := validBaseMeta
		err := CheckResetEligibility(meta, op, ResetReasonRecoveryAfterUnknown)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("eligible: recovery_after_unknown on unresolved pending current attempt", func(t *testing.T) {
		op := validBaseOp
		op.Status = OperationPending
		op.Phase = PhaseDynSecMutation

		meta := validBaseMeta
		meta.Status = CredentialRotating
		meta.OperationStatus = OperationPending

		err := CheckResetEligibility(meta, op, ResetReasonRecoveryAfterUnknown)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("eligible: recovery_after_unknown on unresolved pending provisioning attempt", func(t *testing.T) {
		op := validBaseOp
		op.Action = ActionProvision
		op.CredentialVersion = 1
		op.Status = OperationPending
		op.Phase = PhaseIntent

		meta := validBaseMeta
		meta.CredentialVersion = 1
		meta.Status = CredentialProvisioning
		meta.OperationStatus = OperationPending

		err := CheckResetEligibility(meta, op, ResetReasonRecoveryAfterUnknown)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("eligible: lost_secret_recovery on current succeeded provision", func(t *testing.T) {
		completedAt := now
		op := validBaseOp
		op.Action = ActionProvision
		op.CredentialVersion = 1
		op.Status = OperationSucceeded
		op.Phase = PhaseFinalize
		op.CompletedAt = &completedAt
		op.Evidence = VerificationEvidence{
			RAMApplied:            true,
			SnapshotObserved:      true,
			FreshPositiveVerified: true,
		}

		meta := validBaseMeta
		meta.CredentialVersion = 1
		meta.Status = CredentialActive
		meta.OperationStatus = OperationSucceeded
		meta.ActivatedAt = &completedAt

		err := CheckResetEligibility(meta, op, ResetReasonLostSecretRecovery)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("eligible: lost_secret_recovery on current succeeded rotate", func(t *testing.T) {
		completedAt := now
		prevVer := int64(1)
		op := validBaseOp
		op.Action = ActionRotate
		op.CredentialVersion = 2
		op.Status = OperationSucceeded
		op.Phase = PhaseFinalize
		op.CompletedAt = &completedAt
		op.Evidence = VerificationEvidence{
			RAMApplied:            true,
			SnapshotObserved:      true,
			FreshPositiveVerified: true,
		}
		op.Previous = &CredentialSnapshot{
			Status:            CredentialActive,
			CredentialVersion: prevVer,
		}

		meta := validBaseMeta
		meta.CredentialVersion = 2
		meta.Status = CredentialActive
		meta.OperationStatus = OperationSucceeded
		meta.ActivatedAt = &completedAt

		err := CheckResetEligibility(meta, op, ResetReasonLostSecretRecovery)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("reject: invalid reset reason", func(t *testing.T) {
		err := CheckResetEligibility(validBaseMeta, validBaseOp, "arbitrary")
		if err == nil {
			t.Fatalf("expected error for invalid reason")
		}
		domainErr, ok := err.(*DomainError)
		if !ok || domainErr.Code != CodeInvalidRequest {
			t.Fatalf("expected CodeInvalidRequest, got %v", err)
		}
	})

	t.Run("reject: nil / empty operation identity", func(t *testing.T) {
		op := validBaseOp
		op.OperationID = uuid.Nil
		meta := validBaseMeta
		meta.LastOperationID = uuid.Nil

		err := CheckResetEligibility(meta, op, ResetReasonRecoveryAfterUnknown)
		if err == nil {
			t.Fatalf("expected error for nil operation ID")
		}
	})

	t.Run("reject: foreign operation identity mismatch between metadata and operation", func(t *testing.T) {
		op := validBaseOp
		meta := validBaseMeta
		meta.LastOperationID = uuid.New() // mismatch

		err := CheckResetEligibility(meta, op, ResetReasonRecoveryAfterUnknown)
		if err == nil {
			t.Fatalf("expected error for operation ID mismatch")
		}
		domainErr, ok := err.(*DomainError)
		if !ok || domainErr.Code != CodeCredentialConflict {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
	})

	t.Run("reject: gateway ID mismatch between metadata and operation", func(t *testing.T) {
		op := validBaseOp
		op.GatewayID = "gateway-002"
		meta := validBaseMeta

		err := CheckResetEligibility(meta, op, ResetReasonRecoveryAfterUnknown)
		if err == nil {
			t.Fatalf("expected error for gateway ID mismatch")
		}
		domainErr, ok := err.(*DomainError)
		if !ok || domainErr.Code != CodeCredentialConflict {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
	})

	t.Run("reject: credential version mismatch", func(t *testing.T) {
		op := validBaseOp
		op.CredentialVersion = 3
		meta := validBaseMeta // version is 2

		err := CheckResetEligibility(meta, op, ResetReasonRecoveryAfterUnknown)
		if err == nil {
			t.Fatalf("expected error for version mismatch")
		}
		domainErr, ok := err.(*DomainError)
		if !ok || domainErr.Code != CodeCredentialConflict {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
	})

	t.Run("reject: inconsistent metadata operation status vs operation status", func(t *testing.T) {
		op := validBaseOp // status is OperationRecoveryNeeded
		meta := validBaseMeta
		meta.OperationStatus = OperationPending // inconsistent

		err := CheckResetEligibility(meta, op, ResetReasonRecoveryAfterUnknown)
		if err == nil {
			t.Fatalf("expected error for operation status mismatch")
		}
		domainErr, ok := err.(*DomainError)
		if !ok || domainErr.Code != CodeCredentialConflict {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
	})

	t.Run("reject: malformed historical operation fails validation", func(t *testing.T) {
		op := validBaseOp
		op.CredentialVersion = 0 // invalid version
		meta := validBaseMeta
		meta.CredentialVersion = 0

		err := CheckResetEligibility(meta, op, ResetReasonRecoveryAfterUnknown)
		if err == nil {
			t.Fatalf("expected error for invalid operation")
		}
	})

	t.Run("reject: lost_secret_recovery targeting succeeded revoke", func(t *testing.T) {
		completedAt := now
		op := validBaseOp
		op.Action = ActionRevoke
		op.CredentialVersion = 2
		op.Status = OperationSucceeded
		op.Phase = PhaseFinalize
		op.CompletedAt = &completedAt
		op.Evidence = VerificationEvidence{
			RAMApplied:       true,
			SnapshotObserved: true,
		}
		op.Previous = &CredentialSnapshot{
			Status:            CredentialActive,
			CredentialVersion: 2,
		}

		meta := validBaseMeta
		meta.CredentialVersion = 2
		meta.Status = CredentialRevoked
		meta.OperationStatus = OperationSucceeded
		meta.RevokedAt = &completedAt

		err := CheckResetEligibility(meta, op, ResetReasonLostSecretRecovery)
		if err == nil {
			t.Fatalf("expected error: revoke cannot be target of lost_secret_recovery")
		}
		domainErr, ok := err.(*DomainError)
		if !ok || domainErr.Code != CodeCredentialConflict {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
	})

	t.Run("reject: lost_secret_recovery targeting revoked credential status", func(t *testing.T) {
		completedAt := now
		op := validBaseOp
		op.Action = ActionRotate
		op.Status = OperationSucceeded
		op.CompletedAt = &completedAt
		op.Evidence = VerificationEvidence{
			RAMApplied:            true,
			SnapshotObserved:      true,
			FreshPositiveVerified: true,
		}

		meta := validBaseMeta
		meta.Status = CredentialRevoked // not active!
		meta.OperationStatus = OperationSucceeded

		err := CheckResetEligibility(meta, op, ResetReasonLostSecretRecovery)
		if err == nil {
			t.Fatalf("expected error for revoked credential")
		}
		domainErr, ok := err.(*DomainError)
		if !ok || domainErr.Code != CodeCredentialConflict {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
	})

	t.Run("reject: recovery_after_unknown on active credential", func(t *testing.T) {
		completedAt := now
		op := validBaseOp
		op.Status = OperationSucceeded
		op.CompletedAt = &completedAt
		op.Evidence = VerificationEvidence{
			RAMApplied:            true,
			SnapshotObserved:      true,
			FreshPositiveVerified: true,
		}

		meta := validBaseMeta
		meta.Status = CredentialActive
		meta.OperationStatus = OperationSucceeded

		err := CheckResetEligibility(meta, op, ResetReasonRecoveryAfterUnknown)
		if err == nil {
			t.Fatalf("expected error: recovery_after_unknown cannot target active credential")
		}
		domainErr, ok := err.(*DomainError)
		if !ok || domainErr.Code != CodeCredentialConflict {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
	})

	t.Run("reject: recovery_after_unknown on terminal failed operation", func(t *testing.T) {
		completedAt := now
		op := validBaseOp
		op.Status = OperationFailed
		op.Phase = PhaseFinalize
		op.CompletedAt = &completedAt
		op.Evidence = VerificationEvidence{} // unchanged failure

		meta := validBaseMeta
		meta.Status = CredentialFailed
		meta.OperationStatus = OperationFailed

		err := CheckResetEligibility(meta, op, ResetReasonRecoveryAfterUnknown)
		if err == nil {
			t.Fatalf("expected error: arbitrary failed operation cannot be reset via recovery_after_unknown")
		}
		domainErr, ok := err.(*DomainError)
		if !ok || domainErr.Code != CodeCredentialConflict {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
	})

	t.Run("reject: recovery_after_unknown on revoked credential", func(t *testing.T) {
		completedAt := now
		op := validBaseOp
		op.Action = ActionRevoke
		op.Status = OperationSucceeded
		op.Phase = PhaseFinalize
		op.CompletedAt = &completedAt
		op.Evidence = VerificationEvidence{RAMApplied: true, SnapshotObserved: true}

		meta := validBaseMeta
		meta.Status = CredentialRevoked
		meta.OperationStatus = OperationSucceeded

		err := CheckResetEligibility(meta, op, ResetReasonRecoveryAfterUnknown)
		if err == nil {
			t.Fatalf("expected error: revoked cannot be reset via recovery_after_unknown")
		}
		domainErr, ok := err.(*DomainError)
		if !ok || domainErr.Code != CodeCredentialConflict {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
	})

	t.Run("reject: lost_secret_recovery on unresolved pending attempt", func(t *testing.T) {
		op := validBaseOp
		op.Status = OperationPending
		op.Phase = PhaseDynSecMutation

		meta := validBaseMeta
		meta.Status = CredentialRotating
		meta.OperationStatus = OperationPending

		err := CheckResetEligibility(meta, op, ResetReasonLostSecretRecovery)
		if err == nil {
			t.Fatalf("expected error: lost_secret_recovery requires succeeded attempt")
		}
		domainErr, ok := err.(*DomainError)
		if !ok || domainErr.Code != CodeCredentialConflict {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
	})

	t.Run("reject: lost_secret_recovery on recovery_needed attempt", func(t *testing.T) {
		op := validBaseOp
		meta := validBaseMeta

		err := CheckResetEligibility(meta, op, ResetReasonLostSecretRecovery)
		if err == nil {
			t.Fatalf("expected error: lost_secret_recovery requires succeeded attempt")
		}
		domainErr, ok := err.(*DomainError)
		if !ok || domainErr.Code != CodeCredentialConflict {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
	})

	t.Run("reject: already recovered operation with RecoveryID cannot be reset target", func(t *testing.T) {
		recoveryID := uuid.New()
		op := validBaseOp
		op.RecoveryID = &recoveryID

		meta := validBaseMeta

		err := CheckResetEligibility(meta, op, ResetReasonRecoveryAfterUnknown)
		if err == nil {
			t.Fatalf("expected error: already superseded operation cannot be reset target")
		}
		domainErr, ok := err.(*DomainError)
		if !ok || domainErr.Code != CodeCredentialConflict {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
	})
}

func TestOrdinaryMutationInputValidationPreserved(t *testing.T) {
	actor := uuid.New()
	key := uuid.New()
	gw := "gateway-001"

	// Valid ordinary actions
	for _, action := range []Action{ActionProvision, ActionRotate, ActionRevoke} {
		input := MutationInput{
			GatewayID:      gw,
			IdempotencyKey: key,
			Action:         action,
		}
		if err := ValidateMutationInput(actor, input); err != nil {
			t.Fatalf("ordinary action %s should be valid: %v", action, err)
		}
	}

	// Action.Validate still rejects unknown actions
	if err := Action("reset").Validate(); err == nil {
		t.Fatalf("Action('reset').Validate() should still fail in this increment because Action enum is untouched")
	}
	if err := Action("invalid").Validate(); err == nil {
		t.Fatalf("Action('invalid').Validate() should fail")
	}
}

package mqttcredential

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Real schema019 native PostgreSQL integration tests for simplified journal and reset.
// Only runs when TEST_POSTGRES_DSN or TEST_DATABASE_URL is provided in environment.

func requireCleanSimplifiedTestDB(t *testing.T) (*pgxpool.Pool, *PostgresRepository) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		dsn = os.Getenv("TEST_DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("skipping native integration test: TEST_POSTGRES_DSN/TEST_DATABASE_URL not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to connect to test db: %v", err)
	}

	repo, err := NewPostgresRepository(pool, 5*time.Second)
	if err != nil {
		pool.Close()
		t.Fatalf("failed to create repo: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	return pool, repo
}

func setupTestGatewayAndAdmin(t *testing.T, pool *pgxpool.Pool) (string, uuid.UUID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gwID := "gw-" + uuid.New().String()[:8]
	adminID := uuid.New()

	// Insert profile, platform_admin, and gateway
	_, err := pool.Exec(ctx, `INSERT INTO public.profiles(id, full_name) VALUES ($1, 'Admin')`, adminID)
	if err != nil {
		t.Fatalf("failed to insert profile: %v", err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO public.platform_admins(user_id) VALUES ($1)`, adminID)
	if err != nil {
		t.Fatalf("failed to insert platform_admin: %v", err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO public.gateways(gateway_id, name) VALUES ($1, 'Test GW')`, gwID)
	if err != nil {
		t.Fatalf("failed to insert gateway: %v", err)
	}

	return gwID, adminID
}

func TestSimplifiedRepository_ProvisionRotateRevoke(t *testing.T) {
	pool, repo := requireCleanSimplifiedTestDB(t)
	gwID, adminID := setupTestGatewayAndAdmin(t, pool)
	ctx := context.Background()

	// 1. BeginSimplifiedOperation (Provision)
	provOpID := uuid.New()
	provKey := uuid.New()
	provReq := BeginRequest{
		ActorUserID: adminID,
		OperationID: provOpID,
		Input: MutationInput{
			GatewayID:      gwID,
			IdempotencyKey: provKey,
			Action:         ActionProvision,
		},
	}

	b1, err := repo.BeginSimplifiedOperation(ctx, provReq)
	if err != nil {
		t.Fatalf("BeginSimplifiedOperation(provision) failed: %v", err)
	}
	if b1.Operation.ExecutionContract != ContractSimplified {
		t.Fatalf("expected ContractSimplified, got %v", b1.Operation.ExecutionContract)
	}
	if b1.Operation.CredentialVersion != 1 {
		t.Fatalf("expected version 1, got %d", b1.Operation.CredentialVersion)
	}

	// Verify ZERO rows in mqtt_credential_maintenance
	var maintCount int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM public.mqtt_credential_maintenance WHERE operation_id=$1`, provOpID).Scan(&maintCount)
	if err != nil || maintCount != 0 {
		t.Fatalf("expected ZERO maintenance rows for simplified operation, got %d (err: %v)", maintCount, err)
	}

	// 2. Finalize provision via ConditionalFinalize without snapshot_observed!
	now := time.Now().UTC().Truncate(time.Microsecond)
	evidence := VerificationEvidence{
		RAMApplied:            true,
		SnapshotObserved:      false, // zero snapshot!
		FreshPositiveVerified: true,
	}
	c1, err := CompleteOperation(b1.Operation, ExecutionVerifiedSuccess, evidence, now)
	if err != nil {
		t.Fatalf("CompleteOperation failed: %v", err)
	}

	metaAfterProv, err := repo.ConditionalFinalize(ctx, OperationUpdate{
		Guard: OperationGuard{
			GatewayID:                 gwID,
			ExpectedOperationID:       provOpID,
			ExpectedOperationStatus:   OperationPending,
			ExpectedCredentialStatus:  CredentialProvisioning,
			ExpectedCredentialVersion: 1,
		},
		Completion: c1,
	})
	if err != nil {
		t.Fatalf("ConditionalFinalize failed: %v", err)
	}
	if metaAfterProv.Status != CredentialActive || metaAfterProv.CredentialVersion != 1 {
		t.Fatalf("expected active v1, got %v v%d", metaAfterProv.Status, metaAfterProv.CredentialVersion)
	}

	// 3. BeginSimplifiedOperation (Rotate)
	rotOpID := uuid.New()
	rotKey := uuid.New()
	rotReq := BeginRequest{
		ActorUserID: adminID,
		OperationID: rotOpID,
		Input: MutationInput{
			GatewayID:      gwID,
			IdempotencyKey: rotKey,
			Action:         ActionRotate,
		},
	}

	b2, err := repo.BeginSimplifiedOperation(ctx, rotReq)
	if err != nil {
		t.Fatalf("BeginSimplifiedOperation(rotate) failed: %v", err)
	}
	if b2.Operation.ExecutionContract != ContractSimplified {
		t.Fatalf("expected ContractSimplified, got %v", b2.Operation.ExecutionContract)
	}
	if b2.Operation.CredentialVersion != 2 {
		t.Fatalf("expected version 2, got %d", b2.Operation.CredentialVersion)
	}

	// Finalize rotate via ConditionalFinalize
	now = time.Now().UTC().Truncate(time.Microsecond)
	c2, err := CompleteOperation(b2.Operation, ExecutionVerifiedSuccess, evidence, now)
	if err != nil {
		t.Fatalf("CompleteOperation(rotate) failed: %v", err)
	}

	metaAfterRot, err := repo.ConditionalFinalize(ctx, OperationUpdate{
		Guard: OperationGuard{
			GatewayID:                 gwID,
			ExpectedOperationID:       rotOpID,
			ExpectedOperationStatus:   OperationPending,
			ExpectedCredentialStatus:  CredentialRotating,
			ExpectedCredentialVersion: 2,
		},
		Completion: c2,
	})
	if err != nil {
		t.Fatalf("ConditionalFinalize(rotate) failed: %v", err)
	}
	if metaAfterRot.Status != CredentialActive || metaAfterRot.CredentialVersion != 2 {
		t.Fatalf("expected active v2, got %v v%d", metaAfterRot.Status, metaAfterRot.CredentialVersion)
	}

	// 4. BeginSimplifiedOperation (Revoke)
	revOpID := uuid.New()
	revKey := uuid.New()
	revReq := BeginRequest{
		ActorUserID: adminID,
		OperationID: revOpID,
		Input: MutationInput{
			GatewayID:      gwID,
			IdempotencyKey: revKey,
			Action:         ActionRevoke,
		},
	}

	b3, err := repo.BeginSimplifiedOperation(ctx, revReq)
	if err != nil {
		t.Fatalf("BeginSimplifiedOperation(revoke) failed: %v", err)
	}
	if b3.Operation.CredentialVersion != 2 {
		t.Fatalf("revoke should keep version 2, got %d", b3.Operation.CredentialVersion)
	}

	// Finalize revoke via ConditionalFinalize (no fresh positive verified needed for revoke)
	revokeEvidence := VerificationEvidence{
		RAMApplied:            true,
		SnapshotObserved:      false,
		FreshPositiveVerified: false,
	}
	now = time.Now().UTC().Truncate(time.Microsecond)
	c3, err := CompleteOperation(b3.Operation, ExecutionVerifiedSuccess, revokeEvidence, now)
	if err != nil {
		t.Fatalf("CompleteOperation(revoke) failed: %v", err)
	}

	metaAfterRev, err := repo.ConditionalFinalize(ctx, OperationUpdate{
		Guard: OperationGuard{
			GatewayID:                 gwID,
			ExpectedOperationID:       revOpID,
			ExpectedOperationStatus:   OperationPending,
			ExpectedCredentialStatus:  CredentialRevoking,
			ExpectedCredentialVersion: 2,
		},
		Completion: c3,
	})
	if err != nil {
		t.Fatalf("ConditionalFinalize(revoke) failed: %v", err)
	}
	if metaAfterRev.Status != CredentialRevoked || metaAfterRev.CredentialVersion != 2 {
		t.Fatalf("expected revoked v2, got %v v%d", metaAfterRev.Status, metaAfterRev.CredentialVersion)
	}
}

func TestSimplifiedRepository_ResetFlows(t *testing.T) {
	pool, repo := requireCleanSimplifiedTestDB(t)
	gwID, adminID := setupTestGatewayAndAdmin(t, pool)
	ctx := context.Background()

	// Step 1: Initial provision (pending -> succeeded)
	provOpID := uuid.New()
	b1, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
		ActorUserID: adminID,
		OperationID: provOpID,
		Input: MutationInput{
			GatewayID:      gwID,
			IdempotencyKey: uuid.New(),
			Action:         ActionProvision,
		},
	})
	if err != nil {
		t.Fatalf("provision failed: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	evidence := VerificationEvidence{RAMApplied: true, FreshPositiveVerified: true}
	c1, _ := CompleteOperation(b1.Operation, ExecutionVerifiedSuccess, evidence, now)
	_, err = repo.ConditionalFinalize(ctx, OperationUpdate{
		Guard: OperationGuard{
			GatewayID:                 gwID,
			ExpectedOperationID:       provOpID,
			ExpectedOperationStatus:   OperationPending,
			ExpectedCredentialStatus:  CredentialProvisioning,
			ExpectedCredentialVersion: 1,
		},
		Completion: c1,
	})
	if err != nil {
		t.Fatalf("finalize provision failed: %v", err)
	}

	// Case A: Lost-secret recovery from succeeded active provision
	t.Run("lost_secret_recovery reset admission and finalization", func(t *testing.T) {
		resetOpID := uuid.New()
		resetKey := uuid.New()
		resetReq := BeginResetRequest{
			ActorUserID: adminID,
			OperationID: resetOpID,
			Input: ResetInput{
				GatewayID:      gwID,
				IdempotencyKey: resetKey,
				Reason:         ResetReasonLostSecretRecovery,
			},
		}

		bReset, err := repo.BeginReset(ctx, resetReq)
		if err != nil {
			t.Fatalf("BeginReset failed: %v", err)
		}
		if bReset.Operation.Action != ActionReset {
			t.Fatalf("expected ActionReset, got %v", bReset.Operation.Action)
		}
		if bReset.Operation.ExecutionContract != ContractSimplified {
			t.Fatalf("expected ContractSimplified, got %v", bReset.Operation.ExecutionContract)
		}
		if bReset.Operation.CredentialVersion != 2 {
			t.Fatalf("expected version 2, got %d", bReset.Operation.CredentialVersion)
		}
		if bReset.Metadata.Status != CredentialRotating {
			t.Fatalf("expected rotating status in metadata projection, got %v", bReset.Metadata.Status)
		}

		// Replay same key should return replay
		bReplay, err := repo.BeginReset(ctx, resetReq)
		if err != nil {
			// Pending operation returns OperationInProgress
			var de *DomainError
			if !errors.As(err, &de) || de.Code != CodeOperationInProgress {
				t.Fatalf("expected CodeOperationInProgress on pending replay, got %v", err)
			}
		} else if bReplay.Replay == nil {
			t.Fatalf("expected replay result")
		}

		// Finalize reset
		now = time.Now().UTC().Truncate(time.Microsecond)
		cReset, err := CompleteOperation(bReset.Operation, ExecutionVerifiedSuccess, evidence, now)
		if err != nil {
			t.Fatalf("CompleteOperation(reset) failed: %v", err)
		}

		metaAfterReset, err := repo.ConditionalFinalize(ctx, OperationUpdate{
			Guard: OperationGuard{
				GatewayID:                 gwID,
				ExpectedOperationID:       resetOpID,
				ExpectedOperationStatus:   OperationPending,
				ExpectedCredentialStatus:  CredentialRotating,
				ExpectedCredentialVersion: 2,
			},
			Completion: cReset,
		})
		if err != nil {
			t.Fatalf("ConditionalFinalize(reset) failed: %v", err)
		}
		if metaAfterReset.Status != CredentialActive || metaAfterReset.CredentialVersion != 2 {
			t.Fatalf("expected active v2, got %v v%d", metaAfterReset.Status, metaAfterReset.CredentialVersion)
		}
	})

	// Case B: Recovery-after-unknown on unresolved rotate attempt
	t.Run("recovery_after_unknown on unknown rotate attempt", func(t *testing.T) {
		rotOpID := uuid.New()
		bRot, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: rotOpID,
			Input: MutationInput{
				GatewayID:      gwID,
				IdempotencyKey: uuid.New(),
				Action:         ActionRotate,
			},
		})
		if err != nil {
			t.Fatalf("rotate failed: %v", err)
		}

		// Fail into RequireRecovery (recovery_needed)
		now = time.Now().UTC().Truncate(time.Microsecond)
		cRecovery, _ := CompleteOperation(bRot.Operation, ExecutionRecoveryRequired, VerificationEvidence{RAMApplied: true}, now)
		metaRecovery, err := repo.RequireRecovery(ctx, OperationUpdate{
			Guard: OperationGuard{
				GatewayID:                 gwID,
				ExpectedOperationID:       rotOpID,
				ExpectedOperationStatus:   OperationPending,
				ExpectedCredentialStatus:  CredentialRotating,
				ExpectedCredentialVersion: 3,
			},
			Completion: cRecovery,
		})
		if err != nil {
			t.Fatalf("RequireRecovery failed: %v", err)
		}
		if metaRecovery.Status != CredentialRecoveryNeeded || metaRecovery.CredentialVersion != 2 {
			t.Fatalf("expected recovery_needed retaining v2, got %v v%d", metaRecovery.Status, metaRecovery.CredentialVersion)
		}

		// Now admit reset with recovery_after_unknown
		resetUnknownID := uuid.New()
		bResetUnknown, err := repo.BeginReset(ctx, BeginResetRequest{
			ActorUserID: adminID,
			OperationID: resetUnknownID,
			Input: ResetInput{
				GatewayID:      gwID,
				IdempotencyKey: uuid.New(),
				Reason:         ResetReasonRecoveryAfterUnknown,
			},
		})
		if err != nil {
			t.Fatalf("BeginReset(recovery_after_unknown) failed: %v", err)
		}
		if bResetUnknown.Operation.CredentialVersion != 4 {
			// Version was allocated strictly above attempted (3) and metadata (2): 3 + 1 = 4!
			t.Fatalf("expected allocated version 4, got %d", bResetUnknown.Operation.CredentialVersion)
		}

		// Finalize reset
		now = time.Now().UTC().Truncate(time.Microsecond)
		cResetUnknown, _ := CompleteOperation(bResetUnknown.Operation, ExecutionVerifiedSuccess, evidence, now)
		metaFinal, err := repo.ConditionalFinalize(ctx, OperationUpdate{
			Guard: OperationGuard{
				GatewayID:                 gwID,
				ExpectedOperationID:       resetUnknownID,
				ExpectedOperationStatus:   OperationPending,
				ExpectedCredentialStatus:  CredentialRotating,
				ExpectedCredentialVersion: 4,
			},
			Completion: cResetUnknown,
		})
		if err != nil {
			t.Fatalf("ConditionalFinalize for reset-after-unknown failed: %v", err)
		}
		if metaFinal.Status != CredentialActive || metaFinal.CredentialVersion != 4 {
			t.Fatalf("expected active v4, got %v v%d", metaFinal.Status, metaFinal.CredentialVersion)
		}
	})
}

func TestSimplifiedRepository_SecurityGuards(t *testing.T) {
	pool, repo := requireCleanSimplifiedTestDB(t)
	gwID, _ := setupTestGatewayAndAdmin(t, pool)
	ctx := context.Background()

	// Non-admin actor
	nonAdminID := uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO public.profiles(id, full_name) VALUES ($1, 'NonAdmin')`, nonAdminID)
	if err != nil {
		t.Fatalf("failed to insert profile: %v", err)
	}

	t.Run("non-admin rejected in BeginSimplifiedOperation", func(t *testing.T) {
		_, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: nonAdminID,
			OperationID: uuid.New(),
			Input: MutationInput{
				GatewayID:      gwID,
				IdempotencyKey: uuid.New(),
				Action:         ActionProvision,
			},
		})
		if err == nil {
			t.Fatalf("expected error for non-admin")
		}
		var de *DomainError
		if !errors.As(err, &de) || de.Code != CodeForbidden {
			t.Fatalf("expected CodeForbidden, got %v", err)
		}
	})

	t.Run("non-admin rejected in BeginReset", func(t *testing.T) {
		_, err := repo.BeginReset(ctx, BeginResetRequest{
			ActorUserID: nonAdminID,
			OperationID: uuid.New(),
			Input: ResetInput{
				GatewayID:      gwID,
				IdempotencyKey: uuid.New(),
				Reason:         ResetReasonLostSecretRecovery,
			},
		})
		if err == nil {
			t.Fatalf("expected error for non-admin")
		}
		var de *DomainError
		if !errors.As(err, &de) || de.Code != CodeForbidden {
			t.Fatalf("expected CodeForbidden, got %v", err)
		}
	})
}

func TestSimplifiedRepository_EdgeCasesAndErrors(t *testing.T) {
	pool, repo := requireCleanSimplifiedTestDB(t)
	gwID, adminID := setupTestGatewayAndAdmin(t, pool)
	ctx := context.Background()

	// 1. BeginSimplifiedOperation: non-existent gateway -> NotFound
	t.Run("non-existent gateway rejected", func(t *testing.T) {
		_, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: MutationInput{
				GatewayID:      "gw-not-exist",
				IdempotencyKey: uuid.New(),
				Action:         ActionProvision,
			},
		})
		if err == nil {
			t.Fatalf("expected error for non-existent gateway")
		}
		var de *DomainError
		if !errors.As(err, &de) || de.Code != CodeNotFound {
			t.Fatalf("expected CodeNotFound, got %v", err)
		}
	})

	// 2. BeginSimplifiedOperation: revoke on missing credential -> NotFound
	t.Run("revoke on non-existent credential returns NotFound", func(t *testing.T) {
		_, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: MutationInput{
				GatewayID:      gwID,
				IdempotencyKey: uuid.New(),
				Action:         ActionRevoke,
			},
		})
		if err == nil {
			t.Fatalf("expected error for revoke on missing credential")
		}
		var de *DomainError
		if !errors.As(err, &de) || de.Code != CodeNotFound {
			t.Fatalf("expected CodeNotFound, got %v", err)
		}
	})

	// Provision gateway first
	provOpID := uuid.New()
	provKey := uuid.New()
	bProv, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
		ActorUserID: adminID,
		OperationID: provOpID,
		Input: MutationInput{
			GatewayID:      gwID,
			IdempotencyKey: provKey,
			Action:         ActionProvision,
		},
	})
	if err != nil {
		t.Fatalf("provision failed: %v", err)
	}

	// 3. Replay with conflicting action -> CodeIdempotencyConflict
	t.Run("idempotency key collision with different action", func(t *testing.T) {
		_, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: MutationInput{
				GatewayID:      gwID,
				IdempotencyKey: provKey,      // same key!
				Action:         ActionRotate, // different action!
			},
		})
		if err == nil {
			t.Fatalf("expected error for action conflict")
		}
		var de *DomainError
		if !errors.As(err, &de) || de.Code != CodeIdempotencyConflict {
			t.Fatalf("expected CodeIdempotencyConflict, got %v", err)
		}
	})

	// 4. Concurrent / pending conflict: second operation while provision is pending -> CodeOperationInProgress
	t.Run("operation in progress blocks new operation", func(t *testing.T) {
		_, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: MutationInput{
				GatewayID:      gwID,
				IdempotencyKey: uuid.New(),
				Action:         ActionRotate,
			},
		})
		if err == nil {
			t.Fatalf("expected error while provision is pending")
		}
		var de *DomainError
		if !errors.As(err, &de) || de.Code != CodeOperationInProgress {
			t.Fatalf("expected CodeOperationInProgress, got %v", err)
		}
	})

	// Finalize provision as active
	now := time.Now().UTC().Truncate(time.Microsecond)
	evidence := VerificationEvidence{RAMApplied: true, FreshPositiveVerified: true}
	cProv, _ := CompleteOperation(bProv.Operation, ExecutionVerifiedSuccess, evidence, now)
	_, err = repo.ConditionalFinalize(ctx, OperationUpdate{
		Guard: OperationGuard{
			GatewayID:                 gwID,
			ExpectedOperationID:       provOpID,
			ExpectedOperationStatus:   OperationPending,
			ExpectedCredentialStatus:  CredentialProvisioning,
			ExpectedCredentialVersion: 1,
		},
		Completion: cProv,
	})
	if err != nil {
		t.Fatalf("finalize failed: %v", err)
	}

	// 5. BeginReset: non-existent gateway -> NotFound
	t.Run("BeginReset on non-existent gateway returns NotFound", func(t *testing.T) {
		_, err := repo.BeginReset(ctx, BeginResetRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: ResetInput{
				GatewayID:      "gw-not-exist",
				IdempotencyKey: uuid.New(),
				Reason:         ResetReasonLostSecretRecovery,
			},
		})
		if err == nil {
			t.Fatalf("expected error for non-existent gateway")
		}
		var de *DomainError
		if !errors.As(err, &de) || de.Code != CodeNotFound {
			t.Fatalf("expected CodeNotFound, got %v", err)
		}
	})

	// 6. BeginReset: key conflict with different reason
	resetKey := uuid.New()
	resetOpID := uuid.New()
	bReset, err := repo.BeginReset(ctx, BeginResetRequest{
		ActorUserID: adminID,
		OperationID: resetOpID,
		Input: ResetInput{
			GatewayID:      gwID,
			IdempotencyKey: resetKey,
			Reason:         ResetReasonLostSecretRecovery,
		},
	})
	if err != nil {
		t.Fatalf("BeginReset failed: %v", err)
	}

	t.Run("BeginReset key conflict with different reason", func(t *testing.T) {
		_, err := repo.BeginReset(ctx, BeginResetRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: ResetInput{
				GatewayID:      gwID,
				IdempotencyKey: resetKey,
				Reason:         ResetReasonRecoveryAfterUnknown, // different reason!
			},
		})
		if err == nil {
			t.Fatalf("expected error for reason conflict")
		}
		var de *DomainError
		if !errors.As(err, &de) || de.Code != CodeIdempotencyConflict {
			t.Fatalf("expected CodeIdempotencyConflict, got %v", err)
		}
	})

	// Finalize reset
	now = time.Now().UTC().Truncate(time.Microsecond)
	cReset, _ := CompleteOperation(bReset.Operation, ExecutionVerifiedSuccess, evidence, now)
	_, err = repo.ConditionalFinalize(ctx, OperationUpdate{
		Guard: OperationGuard{
			GatewayID:                 gwID,
			ExpectedOperationID:       resetOpID,
			ExpectedOperationStatus:   OperationPending,
			ExpectedCredentialStatus:  CredentialRotating,
			ExpectedCredentialVersion: 2,
		},
		Completion: cReset,
	})
	if err != nil {
		t.Fatalf("finalize reset failed: %v", err)
	}

	// 7. Replay after completed reset returns MutationResult
	t.Run("BeginReset completed replay returns MutationResult", func(t *testing.T) {
		bReplay, err := repo.BeginReset(ctx, BeginResetRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: ResetInput{
				GatewayID:      gwID,
				IdempotencyKey: resetKey,
				Reason:         ResetReasonLostSecretRecovery,
			},
		})
		if err != nil {
			t.Fatalf("unexpected error for replay: %v", err)
		}
		if bReplay.Execute {
			t.Fatalf("expected Execute=false on replay")
		}
		if bReplay.Replay == nil || bReplay.Replay.OperationID != resetOpID {
			t.Fatalf("expected replayed operation ID %v, got %+v", resetOpID, bReplay.Replay)
		}
		if bReplay.Replay.ReplayedStatus != OperationSucceeded {
			t.Fatalf("expected replayed status succeeded, got %v", bReplay.Replay.ReplayedStatus)
		}
	})

	// 8. Replay with conflicting Gateway for BeginReset
	t.Run("BeginReset key conflict with different gateway", func(t *testing.T) {
		otherGW := "gw-" + uuid.New().String()[:8]
		_, err := pool.Exec(ctx, `INSERT INTO public.gateways(gateway_id, name) VALUES ($1, 'Other GW')`, otherGW)
		if err != nil {
			t.Fatalf("failed to insert other gateway: %v", err)
		}
		// Also provision otherGW so it has a credentials projection and doesn't fail with not_found on metadata
		otherOpID := uuid.New()
		bOther, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: otherOpID,
			Input: MutationInput{
				GatewayID:      otherGW,
				IdempotencyKey: uuid.New(),
				Action:         ActionProvision,
			},
		})
		if err != nil {
			t.Fatalf("failed to provision other gateway: %v", err)
		}
		now = time.Now().UTC().Truncate(time.Microsecond)
		cOther, _ := CompleteOperation(bOther.Operation, ExecutionVerifiedSuccess, evidence, now)
		_, err = repo.ConditionalFinalize(ctx, OperationUpdate{
			Guard: OperationGuard{
				GatewayID:                 otherGW,
				ExpectedOperationID:       otherOpID,
				ExpectedOperationStatus:   OperationPending,
				ExpectedCredentialStatus:  CredentialProvisioning,
				ExpectedCredentialVersion: 1,
			},
			Completion: cOther,
		})
		if err != nil {
			t.Fatalf("failed to finalize other gateway: %v", err)
		}

		_, err = repo.BeginReset(ctx, BeginResetRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: ResetInput{
				GatewayID:      otherGW,
				IdempotencyKey: resetKey, // same key used for gwID
				Reason:         ResetReasonLostSecretRecovery,
			},
		})
		if err == nil {
			t.Fatalf("expected error for gateway conflict on reset replay")
		}
		var de *DomainError
		if !errors.As(err, &de) || de.Code != CodeIdempotencyConflict {
			t.Fatalf("expected CodeIdempotencyConflict, got %v", err)
		}
	})

	// 9. Repeated reset after prior succeeded reset whose secret was lost
	t.Run("repeated reset after prior succeeded reset without secret replay", func(t *testing.T) {
		repeatResetID := uuid.New()
		repeatResetKey := uuid.New()
		bRepeatReset, err := repo.BeginReset(ctx, BeginResetRequest{
			ActorUserID: adminID,
			OperationID: repeatResetID,
			Input: ResetInput{
				GatewayID:      gwID,
				IdempotencyKey: repeatResetKey,
				Reason:         ResetReasonLostSecretRecovery,
			},
		})
		if err != nil {
			t.Fatalf("repeated BeginReset failed: %v", err)
		}
		if bRepeatReset.Operation.CredentialVersion != 3 {
			t.Fatalf("expected monotonic version 3, got %d", bRepeatReset.Operation.CredentialVersion)
		}
		if !bRepeatReset.Execute {
			t.Fatalf("expected Execute=true for new reset")
		}
		// Finalize repeated reset
		now = time.Now().UTC().Truncate(time.Microsecond)
		cRepeatReset, _ := CompleteOperation(bRepeatReset.Operation, ExecutionVerifiedSuccess, evidence, now)
		metaAfterRepeat, err := repo.ConditionalFinalize(ctx, OperationUpdate{
			Guard: OperationGuard{
				GatewayID:                 gwID,
				ExpectedOperationID:       repeatResetID,
				ExpectedOperationStatus:   OperationPending,
				ExpectedCredentialStatus:  CredentialRotating,
				ExpectedCredentialVersion: 3,
			},
			Completion: cRepeatReset,
		})
		if err != nil {
			t.Fatalf("finalize repeated reset failed: %v", err)
		}
		if metaAfterRepeat.Status != CredentialActive || metaAfterRepeat.CredentialVersion != 3 {
			t.Fatalf("expected active v3, got %v v%d", metaAfterRepeat.Status, metaAfterRepeat.CredentialVersion)
		}
	})

	// 10. BeginSimplifiedOperation replay with conflicting Gateway
	t.Run("BeginSimplifiedOperation key conflict with different gateway", func(t *testing.T) {
		otherGW := "gw-" + uuid.New().String()[:8]
		_, err := pool.Exec(ctx, `INSERT INTO public.gateways(gateway_id, name) VALUES ($1, 'Other GW')`, otherGW)
		if err != nil {
			t.Fatalf("failed to insert other gateway: %v", err)
		}
		_, err = repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: MutationInput{
				GatewayID:      otherGW,
				IdempotencyKey: provKey, // same key used for gwID provision
				Action:         ActionProvision,
			},
		})
		if err == nil {
			t.Fatalf("expected error for gateway conflict")
		}
		var de *DomainError
		if !errors.As(err, &de) || de.Code != CodeIdempotencyConflict {
			t.Fatalf("expected CodeIdempotencyConflict, got %v", err)
		}
	})

	// 11. BeginSimplifiedOperation replay on completed provision returns MutationResult with Execute=false
	t.Run("BeginSimplifiedOperation completed replay returns MutationResult", func(t *testing.T) {
		bReplay, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: MutationInput{
				GatewayID:      gwID,
				IdempotencyKey: provKey,
				Action:         ActionProvision,
			},
		})
		if err != nil {
			t.Fatalf("unexpected error for replay provision: %v", err)
		}
		if bReplay.Execute {
			t.Fatalf("expected Execute=false on completed replay")
		}
		if bReplay.Replay == nil || bReplay.Replay.OperationID != provOpID {
			t.Fatalf("expected replayed operation ID %v, got %+v", provOpID, bReplay.Replay)
		}
	})

	// 12. BeginReset rejected when predecessor has foreign status/action or legacy projection
	t.Run("BeginReset rejects ordinary unresolved or foreign status predecessor", func(t *testing.T) {
		// Create a separate gateway in a non-eligible state (e.g. provisioning)
		gwIneligible := "gw-" + uuid.New().String()[:8]
		_, err := pool.Exec(ctx, `INSERT INTO public.gateways(gateway_id, name) VALUES ($1, 'Ineligible GW')`, gwIneligible)
		if err != nil {
			t.Fatalf("failed to insert gateway: %v", err)
		}
		// Start provision so last_operation is pending provisioning
		ineligOpID := uuid.New()
		_, err = repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: ineligOpID,
			Input: MutationInput{
				GatewayID:      gwIneligible,
				IdempotencyKey: uuid.New(),
				Action:         ActionProvision,
			},
		})
		if err != nil {
			t.Fatalf("provision on ineligible gw failed: %v", err)
		}

		// Attempting reset on pending provision should fail eligibility / in-progress
		_, err = repo.BeginReset(ctx, BeginResetRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: ResetInput{
				GatewayID:      gwIneligible,
				IdempotencyKey: uuid.New(),
				Reason:         ResetReasonLostSecretRecovery,
			},
		})
		if err == nil {
			t.Fatalf("expected BeginReset on pending provision to be rejected")
		}
		var de *DomainError
		if !errors.As(err, &de) {
			t.Fatalf("expected domain error, got %v", err)
		}
	})

	// 13. Predecessor event has legacy_projection=true -> CodeCredentialConflict
	t.Run("BeginReset rejects predecessor with legacy_projection", func(t *testing.T) {
		gwLegacy := "gw-" + uuid.New().String()[:8]
		_, err := pool.Exec(ctx, `INSERT INTO public.gateways(gateway_id, name) VALUES ($1, 'Legacy GW')`, gwLegacy)
		if err != nil {
			t.Fatalf("failed to insert gateway: %v", err)
		}
		legOpID := uuid.New()
		// Insert legacy event and credentials projection with disabled trigger to simulate legacy migrated row
		_, err = pool.Exec(ctx, `ALTER TABLE public.gateway_mqtt_credential_events DISABLE TRIGGER ALL`)
		if err != nil {
			t.Fatalf("failed to disable triggers on events: %v", err)
		}
		_, err = pool.Exec(ctx, `ALTER TABLE public.gateway_mqtt_credentials DISABLE TRIGGER ALL`)
		if err != nil {
			_ = pool.QueryRow(ctx, `ALTER TABLE public.gateway_mqtt_credential_events ENABLE TRIGGER ALL`)
			t.Fatalf("failed to disable triggers on credentials: %v", err)
		}
		_, err = pool.Exec(ctx, `INSERT INTO public.gateway_mqtt_credential_events
(operation_id, gateway_id, credential_version, action, status, actor_user_id, idempotency_key, legacy_projection, execution_contract, completed_at)
VALUES ($1, $2, 1, 'provision', 'succeeded', $3, $4, true, 'lifecycle', now())`, legOpID, gwLegacy, adminID, uuid.New())
		if err != nil {
			_ = pool.QueryRow(ctx, `ALTER TABLE public.gateway_mqtt_credential_events ENABLE TRIGGER ALL`)
			_ = pool.QueryRow(ctx, `ALTER TABLE public.gateway_mqtt_credentials ENABLE TRIGGER ALL`)
			t.Fatalf("failed to insert legacy event: %v", err)
		}
		_, err = pool.Exec(ctx, `INSERT INTO public.gateway_mqtt_credentials
(gateway_id, credential_version, status, last_operation_id, changed_by, activated_at)
VALUES ($1, 1, 'active', $2, $3, now())`, gwLegacy, legOpID, adminID)
		if err != nil {
			_ = pool.QueryRow(ctx, `ALTER TABLE public.gateway_mqtt_credential_events ENABLE TRIGGER ALL`)
			_ = pool.QueryRow(ctx, `ALTER TABLE public.gateway_mqtt_credentials ENABLE TRIGGER ALL`)
			t.Fatalf("failed to insert legacy credential projection: %v", err)
		}
		_, err = pool.Exec(ctx, `ALTER TABLE public.gateway_mqtt_credential_events ENABLE TRIGGER ALL`)
		if err != nil {
			t.Fatalf("failed to re-enable triggers on events: %v", err)
		}
		_, err = pool.Exec(ctx, `ALTER TABLE public.gateway_mqtt_credentials ENABLE TRIGGER ALL`)
		if err != nil {
			t.Fatalf("failed to re-enable triggers on credentials: %v", err)
		}

		_, err = repo.BeginReset(ctx, BeginResetRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: ResetInput{
				GatewayID:      gwLegacy,
				IdempotencyKey: uuid.New(),
				Reason:         ResetReasonLostSecretRecovery,
			},
		})
		if err == nil {
			t.Fatalf("expected error for legacy predecessor in BeginReset")
		}
		var de *DomainError
		if !errors.As(err, &de) || de.Code != CodeCredentialConflict {
			t.Fatalf("expected CodeCredentialConflict, got %v", err)
		}
	})

	// 14. Partial reset admission / rollback preserves snapshot
	t.Run("partial reset admission failure rolls back and preserves snapshot", func(t *testing.T) {
		// Snapshot before
		metaBefore, err := repo.GetMetadata(ctx, gwID)
		if err != nil {
			t.Fatalf("failed to get metadata before: %v", err)
		}

		// Attempt BeginReset with duplicate OperationID that collides with an existing event
		_, err = repo.BeginReset(ctx, BeginResetRequest{
			ActorUserID: adminID,
			OperationID: provOpID, // Collides with existing operation_id in gateway_mqtt_credential_events!
			Input: ResetInput{
				GatewayID:      gwID,
				IdempotencyKey: uuid.New(), // fresh key so it passes replay check
				Reason:         ResetReasonLostSecretRecovery,
			},
		})
		if err == nil {
			t.Fatalf("expected error due to duplicate operation_id")
		}

		// Verify snapshot is untouched
		metaAfter, err := repo.GetMetadata(ctx, gwID)
		if err != nil {
			t.Fatalf("failed to get metadata after: %v", err)
		}
		if metaAfter.Status != metaBefore.Status || metaAfter.CredentialVersion != metaBefore.CredentialVersion || metaAfter.LastOperationID != metaBefore.LastOperationID {
			t.Fatalf("snapshot mutated after rolled back reset: before=%+v, after=%+v", metaBefore, metaAfter)
		}
	})

	// 15. Partial simplified intent insert failure rolls back all rows and preserves snapshot
	t.Run("simplified intent insert failure rolls back and preserves snapshot", func(t *testing.T) {
		metaBefore, err := repo.GetMetadata(ctx, gwID)
		if err != nil {
			t.Fatalf("failed to get metadata before: %v", err)
		}

		// Attempt BeginSimplifiedOperation with duplicate OperationID
		_, err = repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: provOpID, // Collides with existing operation_id!
			Input: MutationInput{
				GatewayID:      gwID,
				IdempotencyKey: uuid.New(), // fresh key
				Action:         ActionRotate,
			},
		})
		if err == nil {
			t.Fatalf("expected error due to duplicate operation_id")
		}

		// Verify snapshot is untouched
		metaAfter, err := repo.GetMetadata(ctx, gwID)
		if err != nil {
			t.Fatalf("failed to get metadata after: %v", err)
		}
		if metaAfter.Status != metaBefore.Status || metaAfter.CredentialVersion != metaBefore.CredentialVersion || metaAfter.LastOperationID != metaBefore.LastOperationID {
			t.Fatalf("snapshot mutated after rolled back simplified op: before=%+v, after=%+v", metaBefore, metaAfter)
		}
	})

	// 16. Revoke on already revoked credential returns replay with Execute=false
	t.Run("revoke on already revoked credential returns replay without execution", func(t *testing.T) {
		gwRev := "gw-" + uuid.New().String()[:8]
		_, err := pool.Exec(ctx, `INSERT INTO public.gateways(gateway_id, name) VALUES ($1, 'Revoke GW')`, gwRev)
		if err != nil {
			t.Fatalf("failed to insert gateway: %v", err)
		}
		// Provision
		provID := uuid.New()
		bProv, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: provID,
			Input: MutationInput{
				GatewayID:      gwRev,
				IdempotencyKey: uuid.New(),
				Action:         ActionProvision,
			},
		})
		if err != nil {
			t.Fatalf("provision failed: %v", err)
		}
		now = time.Now().UTC().Truncate(time.Microsecond)
		cProv, _ := CompleteOperation(bProv.Operation, ExecutionVerifiedSuccess, evidence, now)
		_, err = repo.ConditionalFinalize(ctx, OperationUpdate{
			Guard: OperationGuard{
				GatewayID:                 gwRev,
				ExpectedOperationID:       provID,
				ExpectedOperationStatus:   OperationPending,
				ExpectedCredentialStatus:  CredentialProvisioning,
				ExpectedCredentialVersion: 1,
			},
			Completion: cProv,
		})
		if err != nil {
			t.Fatalf("finalize provision failed: %v", err)
		}

		// First Revoke
		revID := uuid.New()
		bRev, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: revID,
			Input: MutationInput{
				GatewayID:      gwRev,
				IdempotencyKey: uuid.New(),
				Action:         ActionRevoke,
			},
		})
		if err != nil {
			t.Fatalf("first revoke failed: %v", err)
		}
		now = time.Now().UTC().Truncate(time.Microsecond)
		revokeEv := VerificationEvidence{RAMApplied: true}
		cRev, _ := CompleteOperation(bRev.Operation, ExecutionVerifiedSuccess, revokeEv, now)
		_, err = repo.ConditionalFinalize(ctx, OperationUpdate{
			Guard: OperationGuard{
				GatewayID:                 gwRev,
				ExpectedOperationID:       revID,
				ExpectedOperationStatus:   OperationPending,
				ExpectedCredentialStatus:  CredentialRevoking,
				ExpectedCredentialVersion: 1,
			},
			Completion: cRev,
		})
		if err != nil {
			t.Fatalf("finalize revoke failed: %v", err)
		}

		// Second Revoke with new idempotency key on already revoked credential
		bRev2, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: MutationInput{
				GatewayID:      gwRev,
				IdempotencyKey: uuid.New(),
				Action:         ActionRevoke,
			},
		})
		if err != nil {
			t.Fatalf("second revoke failed: %v", err)
		}
		if bRev2.Execute {
			t.Fatalf("expected Execute=false for revoke on already revoked credential")
		}
		if bRev2.Replay == nil {
			t.Fatalf("expected replay result for revoke on already revoked credential")
		}
		if bRev2.Metadata.Status != CredentialRevoked {
			t.Fatalf("expected status revoked, got %v", bRev2.Metadata.Status)
		}
	})

	// 17. BeginSimplifiedOperation blocked when recovery is required
	t.Run("BeginSimplifiedOperation blocked when recovery_needed is present", func(t *testing.T) {
		gwRec := "gw-" + uuid.New().String()[:8]
		_, err := pool.Exec(ctx, `INSERT INTO public.gateways(gateway_id, name) VALUES ($1, 'Rec GW')`, gwRec)
		if err != nil {
			t.Fatalf("failed to insert gateway: %v", err)
		}
		provID := uuid.New()
		bProv, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: provID,
			Input: MutationInput{
				GatewayID:      gwRec,
				IdempotencyKey: uuid.New(),
				Action:         ActionProvision,
			},
		})
		if err != nil {
			t.Fatalf("provision failed: %v", err)
		}
		now = time.Now().UTC().Truncate(time.Microsecond)
		cRec, _ := CompleteOperation(bProv.Operation, ExecutionRecoveryRequired, VerificationEvidence{RAMApplied: true}, now)
		_, err = repo.RequireRecovery(ctx, OperationUpdate{
			Guard: OperationGuard{
				GatewayID:                 gwRec,
				ExpectedOperationID:       provID,
				ExpectedOperationStatus:   OperationPending,
				ExpectedCredentialStatus:  CredentialProvisioning,
				ExpectedCredentialVersion: 1,
			},
			Completion: cRec,
		})
		if err != nil {
			t.Fatalf("RequireRecovery failed: %v", err)
		}

		// Attempting a new simplified operation must return CodeRecoveryRequired
		_, err = repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: MutationInput{
				GatewayID:      gwRec,
				IdempotencyKey: uuid.New(),
				Action:         ActionRotate,
			},
		})
		if err == nil {
			t.Fatalf("expected error when recovery_needed is present")
		}
		var de *DomainError
		if !errors.As(err, &de) || de.Code != CodeRecoveryRequired {
			t.Fatalf("expected CodeRecoveryRequired, got %v", err)
		}
	})

	// 18. BeginReset blocked when another unresolved operation exists
	t.Run("BeginReset blocked when another unresolved operation exists", func(t *testing.T) {
		gwUnres := "gw-" + uuid.New().String()[:8]
		_, err := pool.Exec(ctx, `INSERT INTO public.gateways(gateway_id, name) VALUES ($1, 'Unres GW')`, gwUnres)
		if err != nil {
			t.Fatalf("failed to insert gateway: %v", err)
		}
		// First provision succeeded
		provID := uuid.New()
		bProv, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminID,
			OperationID: provID,
			Input: MutationInput{
				GatewayID:      gwUnres,
				IdempotencyKey: uuid.New(),
				Action:         ActionProvision,
			},
		})
		if err != nil {
			t.Fatalf("provision failed: %v", err)
		}
		now = time.Now().UTC().Truncate(time.Microsecond)
		cProv, _ := CompleteOperation(bProv.Operation, ExecutionVerifiedSuccess, evidence, now)
		_, err = repo.ConditionalFinalize(ctx, OperationUpdate{
			Guard: OperationGuard{
				GatewayID:                 gwUnres,
				ExpectedOperationID:       provID,
				ExpectedOperationStatus:   OperationPending,
				ExpectedCredentialStatus:  CredentialProvisioning,
				ExpectedCredentialVersion: 1,
			},
			Completion: cProv,
		})
		if err != nil {
			t.Fatalf("finalize provision failed: %v", err)
		}

		// Insert an extra pending event on the same gateway (simulating other unresolved operation)
		extraOpID := uuid.New()
		_, err = pool.Exec(ctx, `ALTER TABLE public.gateway_mqtt_credential_events DISABLE TRIGGER ALL`)
		if err != nil {
			t.Fatalf("failed to disable triggers: %v", err)
		}
		_, err = pool.Exec(ctx, `INSERT INTO public.gateway_mqtt_credential_events
(operation_id, gateway_id, credential_version, action, status, actor_user_id, idempotency_key, execution_contract)
VALUES ($1, $2, 2, 'rotate', 'pending', $3, $4, 'simplified')`, extraOpID, gwUnres, adminID, uuid.New())
		if err != nil {
			_ = pool.QueryRow(ctx, `ALTER TABLE public.gateway_mqtt_credential_events ENABLE TRIGGER ALL`)
			t.Fatalf("failed to insert extra pending event: %v", err)
		}
		_, err = pool.Exec(ctx, `ALTER TABLE public.gateway_mqtt_credential_events ENABLE TRIGGER ALL`)
		if err != nil {
			t.Fatalf("failed to re-enable triggers: %v", err)
		}

		// BeginReset must fail with CodeOperationInProgress because of other unresolved operation
		_, err = repo.BeginReset(ctx, BeginResetRequest{
			ActorUserID: adminID,
			OperationID: uuid.New(),
			Input: ResetInput{
				GatewayID:      gwUnres,
				IdempotencyKey: uuid.New(),
				Reason:         ResetReasonLostSecretRecovery,
			},
		})
		if err == nil {
			t.Fatalf("expected BeginReset to fail with other unresolved operation")
		}
		var de *DomainError
		if !errors.As(err, &de) || de.Code != CodeOperationInProgress {
			t.Fatalf("expected CodeOperationInProgress, got %v", err)
		}
	})

	// 19. Read-only LookupResetReplay tests
	t.Run("LookupResetReplay distinguishes not found, historical replay, pending, and conflict", func(t *testing.T) {
		// Not found lookup
		op, meta, found, err := repo.LookupResetReplay(ctx, adminID, ResetInput{
			GatewayID:      gwID,
			IdempotencyKey: uuid.New(),
			Reason:         ResetReasonLostSecretRecovery,
		})
		if err != nil || found || op.OperationID != uuid.Nil || meta.GatewayID != "" {
			t.Fatalf("expected not found for unused key: found=%v, err=%v", found, err)
		}

		// Historical matching reset replay
		op, meta, found, err = repo.LookupResetReplay(ctx, adminID, ResetInput{
			GatewayID:      gwID,
			IdempotencyKey: resetKey,
			Reason:         ResetReasonLostSecretRecovery,
		})
		if err != nil || !found {
			t.Fatalf("expected found replay: found=%v, err=%v", found, err)
		}
		if op.OperationID != resetOpID || op.Action != ActionReset || op.Status != OperationSucceeded {
			t.Fatalf("expected historical reset op %v, got %+v", resetOpID, op)
		}
		if meta.GatewayID != gwID || meta.Status != CredentialActive {
			t.Fatalf("expected active metadata for gwID, got %+v", meta)
		}

		// Conflict on wrong gateway
		_, _, _, err = repo.LookupResetReplay(ctx, adminID, ResetInput{
			GatewayID:      "non-existent-gw",
			IdempotencyKey: resetKey,
			Reason:         ResetReasonLostSecretRecovery,
		})
		if err == nil {
			t.Fatalf("expected error for non-existent gateway")
		}

		// Conflict on wrong reason
		_, _, _, err = repo.LookupResetReplay(ctx, adminID, ResetInput{
			GatewayID:      gwID,
			IdempotencyKey: resetKey,
			Reason:         ResetReasonRecoveryAfterUnknown,
		})
		if err == nil {
			t.Fatalf("expected conflict for different reset reason")
		}
		var de *DomainError
		if !errors.As(err, &de) || de.Code != CodeIdempotencyConflict {
			t.Fatalf("expected CodeIdempotencyConflict, got %v", err)
		}

		// Non-admin lookup denied
		nonAdminID := uuid.New()
		_, _, _, err = repo.LookupResetReplay(ctx, nonAdminID, ResetInput{
			GatewayID:      gwID,
			IdempotencyKey: resetKey,
			Reason:         ResetReasonLostSecretRecovery,
		})
		if err == nil || !errors.As(err, &de) || de.Code != CodeForbidden {
			t.Fatalf("expected CodeForbidden for non-admin, got %v", err)
		}
	})

	// 20. Stale expected-target rejection leaves event/disposition/projection counts unchanged
	t.Run("stale ExpectedTargetGuard rejection leaves counts unchanged and valid binding admits exactly once", func(t *testing.T) {
		gwStale, adminStaleID := setupTestGatewayAndAdmin(t, pool)

		// 1. Initial provision to active v1
		provOpID := uuid.New()
		bProv, err := repo.BeginSimplifiedOperation(ctx, BeginRequest{
			ActorUserID: adminStaleID,
			OperationID: provOpID,
			Input: MutationInput{
				GatewayID:      gwStale,
				IdempotencyKey: uuid.New(),
				Action:         ActionProvision,
			},
		})
		if err != nil {
			t.Fatalf("provision failed: %v", err)
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		cProv, _ := CompleteOperation(bProv.Operation, ExecutionVerifiedSuccess, evidence, now)
		_, err = repo.ConditionalFinalize(ctx, OperationUpdate{
			Guard: OperationGuard{
				GatewayID:                 gwStale,
				ExpectedOperationID:       provOpID,
				ExpectedOperationStatus:   OperationPending,
				ExpectedCredentialStatus:  CredentialProvisioning,
				ExpectedCredentialVersion: 1,
			},
			Completion: cProv,
		})
		if err != nil {
			t.Fatalf("finalize provision failed: %v", err)
		}

		// Count baseline events, dispositions, credentials
		var eventCountBefore, dispCountBefore int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.gateway_mqtt_credential_events WHERE gateway_id=$1`, gwStale).Scan(&eventCountBefore); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.mqtt_reset_dispositions WHERE gateway_id=$1`, gwStale).Scan(&dispCountBefore); err != nil {
			t.Fatal(err)
		}
		metaBefore, err := repo.GetMetadata(ctx, gwStale)
		if err != nil {
			t.Fatal(err)
		}

		// Attempt BeginReset with stale ExpectedTargetGuard (e.g. wrong ExpectedOperationStatus or version)
		staleGuards := []*OperationGuard{
			{
				GatewayID:                 gwStale,
				ExpectedOperationID:       provOpID,
				ExpectedOperationStatus:   OperationPending, // currently succeeded!
				ExpectedCredentialStatus:  CredentialActive,
				ExpectedCredentialVersion: 1,
			},
			{
				GatewayID:                 gwStale,
				ExpectedOperationID:       provOpID,
				ExpectedOperationStatus:   OperationSucceeded,
				ExpectedCredentialStatus:  CredentialRotating, // currently active!
				ExpectedCredentialVersion: 1,
			},
			{
				GatewayID:                 gwStale,
				ExpectedOperationID:       provOpID,
				ExpectedOperationStatus:   OperationSucceeded,
				ExpectedCredentialStatus:  CredentialActive,
				ExpectedCredentialVersion: 99, // currently 1!
			},
		}

		for i, sg := range staleGuards {
			_, err := repo.BeginReset(ctx, BeginResetRequest{
				ActorUserID:           adminStaleID,
				OperationID:           uuid.New(),
				ExpectedPredecessorID: provOpID,
				ExpectedTargetGuard:   sg,
				Input: ResetInput{
					GatewayID:      gwStale,
					IdempotencyKey: uuid.New(),
					Reason:         ResetReasonLostSecretRecovery,
				},
			})
			if err == nil {
				t.Fatalf("expected error for stale target guard %d", i)
			}
			var de *DomainError
			if !errors.As(err, &de) || de.Code != CodeCredentialConflict {
				t.Fatalf("expected CodeCredentialConflict for stale target guard %d, got %v", i, err)
			}
		}

		// Verify event, disposition, and projection counts/state remain strictly unchanged
		var eventCountAfter, dispCountAfter int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.gateway_mqtt_credential_events WHERE gateway_id=$1`, gwStale).Scan(&eventCountAfter); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.mqtt_reset_dispositions WHERE gateway_id=$1`, gwStale).Scan(&dispCountAfter); err != nil {
			t.Fatal(err)
		}
		metaAfter, err := repo.GetMetadata(ctx, gwStale)
		if err != nil {
			t.Fatal(err)
		}

		if eventCountBefore != eventCountAfter || dispCountBefore != dispCountAfter {
			t.Fatalf("counts changed on stale rejection: events %d->%d, dispositions %d->%d", eventCountBefore, eventCountAfter, dispCountBefore, dispCountAfter)
		}
		if !sameSimplifiedMetadata(metaBefore, metaAfter) {
			t.Fatalf("metadata projection changed on stale rejection: %+v != %+v", metaBefore, metaAfter)
		}

		// Now valid exact target binding admits exactly once
		validTargetGuard := &OperationGuard{
			GatewayID:                 gwStale,
			ExpectedOperationID:       provOpID,
			ExpectedOperationStatus:   OperationSucceeded,
			ExpectedCredentialStatus:  CredentialActive,
			ExpectedCredentialVersion: 1,
		}
		resetOnceID := uuid.New()
		resetKey := uuid.New()
		bAdmit, err := repo.BeginReset(ctx, BeginResetRequest{
			ActorUserID:           adminStaleID,
			OperationID:           resetOnceID,
			ExpectedPredecessorID: provOpID,
			ExpectedTargetGuard:   validTargetGuard,
			Input: ResetInput{
				GatewayID:      gwStale,
				IdempotencyKey: resetKey,
				Reason:         ResetReasonLostSecretRecovery,
			},
		})
		if err != nil {
			t.Fatalf("valid target guard must admit: %v", err)
		}
		if !bAdmit.Execute || bAdmit.Operation.OperationID != resetOnceID {
			t.Fatalf("unexpected admit result: %+v", bAdmit)
		}

		// Verify event and disposition counts incremented by exactly 1
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.gateway_mqtt_credential_events WHERE gateway_id=$1`, gwStale).Scan(&eventCountAfter); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.mqtt_reset_dispositions WHERE gateway_id=$1`, gwStale).Scan(&dispCountAfter); err != nil {
			t.Fatal(err)
		}
		if eventCountAfter != eventCountBefore+1 || dispCountAfter != dispCountBefore+1 {
			t.Fatalf("expected exactly +1 event and disposition, got events: %d, disp: %d", eventCountAfter, dispCountAfter)
		}
	})
}
